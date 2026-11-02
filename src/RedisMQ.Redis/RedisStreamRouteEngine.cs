using System.Linq;
using System.Text.Json;
using System.Threading.Channels;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using StackExchange.Redis;

namespace RedisMQ.Redis;

/// <summary>
/// M1 route engine: a Redis Streams write-ahead log behind the synchronous
/// <see cref="IRouteEngine"/> seam. <see cref="InMemoryEngine"/> is the system of
/// record for reads and credit gating; every mutating call is mirrored to Redis
/// Streams asynchronously by a single background drainer so the protocol path
/// never blocks on Redis.
/// </summary>
public sealed class RedisStreamRouteEngine : IRouteEngine, IAsyncDisposable
{
    private readonly IDatabase? _redis;
    private readonly string[] _vhosts;
    private readonly InMemoryEngine _memory;
    private readonly Channel<WriteCommand>? _channel;
    private readonly Task? _drainerTask;
    private readonly CancellationTokenSource _stopCts = new();

    // Exclusive queue ownership and auto-delete consumer tracking for connection
    // teardown reconciliation.
    private readonly Dictionary<(string Vhost, string Queue), string> _exclusiveQueues = [];
    private readonly Dictionary<(string Vhost, string Queue), HashSet<string>> _autoDeleteQueues = [];

    /// <summary>
    /// Creates the engine. <paramref name="redis"/> may be null (e.g. Redis
    /// unavailable); in that case all writes go to memory only and no drainer
    /// is started. When <paramref name="replayOnStart"/> is true, Redis topology
    /// and ready-queue streams are replayed into memory before the engine serves.
    /// When <paramref name="enforceEvictionGate"/> is true and Redis is available,
    /// the server's maxmemory-policy is checked at startup; eviction policies that
    /// could discard durable data cause the constructor to throw.
    /// </summary>
    public RedisStreamRouteEngine(IDatabase? redis, IReadOnlyList<string> vhosts, bool replayOnStart = true, int drainerCapacity = 1024, bool enforceEvictionGate = true)
    {
        ArgumentNullException.ThrowIfNull(vhosts);
        if (vhosts.Count == 0)
        {
            throw new ArgumentException("At least one vhost is required", nameof(vhosts));
        }
        _redis = redis;
        _vhosts = [.. vhosts];
        _memory = new InMemoryEngine(_vhosts);

        if (redis is not null)
        {
            _channel = Channel.CreateBounded<WriteCommand>(
                new BoundedChannelOptions(drainerCapacity) { FullMode = BoundedChannelFullMode.Wait });
            _drainerTask = Task.Run(() => DrainAsync(_stopCts.Token));
        }

        if (enforceEvictionGate && redis is not null)
        {
            CheckEvictionPolicy();
        }

        if (replayOnStart && redis is not null)
        {
            ReplayAsync().GetAwaiter().GetResult();
        }
    }

    // ---------- IRouteEngine reads (delegate to memory, synchronous) ----------

    public ExchangeInfo? GetExchange(string vhost, string name) => _memory.GetExchange(vhost, name);

    public QueueInfo? GetQueue(string vhost, string name) => _memory.GetQueue(vhost, name);

    public IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties, string routingKey) =>
        _memory.RouteTo(exchange, properties, routingKey);

    public GetClaimOutcome TryClaimForGet(string vhost, string queue, out InMemoryMessage? message, out int remaining) =>
        _memory.TryClaimForGet(vhost, queue, out message, out remaining);

    public void Nudge(string vhost, string queue) => _memory.Nudge(vhost, queue);

    // ---------- IRouteEngine writes (memory first, then Redis drainer) ----------

    public ExchangeInfo? DeclareExchange(ExchangeInfo exchange)
    {
        var prior = _memory.DeclareExchange(exchange);
        EnqueueCommand(WriteCommand.DeclareExchange(exchange.Vhost, exchange.Name, exchange.Type));
        return prior;
    }

    public QueueInfo? DeclareQueue(QueueInfo queue)
    {
        var prior = _memory.DeclareQueue(queue);
        EnqueueCommand(WriteCommand.DeclareQueue(queue.Vhost, queue.Name, queue.Durable, queue.Exclusive, queue.AutoDelete, queue.MaxLength));

        // Track exclusive queue ownership for connection teardown reconciliation.
        if (queue.Exclusive)
        {
            lock (_exclusiveQueues)
            {
                _exclusiveQueues[(queue.Vhost, queue.Name)] = queue.Name;
            }
        }

        return prior;
    }

    public void Bind(string vhost, string exchange, string queue, string routingKey, FieldTable? arguments = null)
    {
        _memory.Bind(vhost, exchange, queue, routingKey, arguments);
        EnqueueCommand(WriteCommand.Bind(vhost, exchange, queue, routingKey, arguments));
    }

    public void Unbind(string vhost, string exchange, string queue, string routingKey)
    {
        _memory.Unbind(vhost, exchange, queue, routingKey);
        EnqueueCommand(WriteCommand.Unbind(vhost, exchange, queue, routingKey));
    }

    public void Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey)
    {
        _memory.Enqueue(vhost, queue, properties, body, exchange, routingKey);
        EnqueueCommand(WriteCommand.Enqueue(vhost, queue, 0, exchange, routingKey, body, properties));
    }

    public ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer)
    {
        var outcome = _memory.RegisterConsumer(vhost, queue, consumer);
        EnqueueCommand(WriteCommand.RegisterConsumer(vhost, queue, consumer.ConsumerTag));
        return outcome;
    }

    public void UnregisterConsumer(string vhost, string queue, string consumerTag)
    {
        _memory.UnregisterConsumer(vhost, queue, consumerTag);
        EnqueueCommand(WriteCommand.UnregisterConsumer(vhost, queue, consumerTag));
    }

    /// <summary>
    /// Records that <paramref name="connectionId"/> owns an exclusive queue.
    /// Called by <see cref="WireConnection"/> when a channel declares an exclusive
    /// queue on its connection.
    /// </summary>
    public void RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId)
    {
        lock (_exclusiveQueues)
        {
            _exclusiveQueues[(vhost, queue)] = connectionId;
        }
    }

    public void Requeue(string vhost, string queue, InMemoryMessage message)
    {
        _memory.Requeue(vhost, queue, message);
        EnqueueCommand(WriteCommand.Requeue(vhost, queue, message.EnvelopeId, message.Exchange, message.RoutingKey,
            message.Body, message.Props, message.Redelivered));
    }

    /// <summary>
    /// Connection teardown reconciliation: deletes exclusive queues owned by this
    /// connection and auto-delete queues whose last consumer has disconnected.
    /// </summary>
    public void ReconcileConnection(string connectionId)
    {
        lock (_exclusiveQueues)
        {
            var queuesToDelete = _exclusiveQueues.Keys.ToList();
            foreach (var (vhost, queue) in queuesToDelete)
            {
                _memory.DeleteQueue(vhost, queue);
                _exclusiveQueues.Remove((vhost, queue));
                EnqueueCommand(WriteCommand.DeleteQueue(vhost, queue));
            }
        }

        lock (_autoDeleteQueues)
        {
            var queuesToDelete = _autoDeleteQueues
                .Where(kv => kv.Value.Remove(connectionId) && kv.Value.Count == 0)
                .Select(kv => kv.Key)
                .ToList();

            foreach (var (vhost, queue) in queuesToDelete)
            {
                _memory.DeleteQueue(vhost, queue);
                _autoDeleteQueues.Remove((vhost, queue));
                EnqueueCommand(WriteCommand.DeleteQueue(vhost, queue));
            }
        }
    }

    // ---------- drainer ----------

    private void EnqueueCommand(WriteCommand command)
    {
        if (_channel is null)
        {
            return; // no Redis: memory-only
        }

        // Bounded channel with Wait full mode: applies backpressure on the
        // protocol thread when Redis can't keep up, but never blocks on Redis
        // directly.
        _ = _channel.Writer.TryWrite(command);
    }

    private async Task DrainAsync(CancellationToken ct)
    {
        await foreach (var command in _channel!.Reader.ReadAllAsync(ct))
        {
            try
            {
                await WriteCommandToRedisAsync(command);
            }
            catch
            {
                // Drainer is best-effort: memory is the system of record. A
                // transient Redis failure drops the mirrored write; the in-memory
                // state is already updated.
            }
        }
    }

    private async Task WriteCommandToRedisAsync(WriteCommand command)
    {
        ArgumentNullException.ThrowIfNull(_redis);
        var fields = command.ToStreamFields();

        switch (command.Kind)
        {
            case WriteCommandKind.DeclareExchange or WriteCommandKind.DeclareQueue or
                 WriteCommandKind.Bind or WriteCommandKind.Unbind:
                await _redis.StreamAddAsync(RedisKeys.TopologyKey(command.Vhost ?? string.Empty), fields.Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());
                break;

            case WriteCommandKind.Enqueue or WriteCommandKind.Requeue:
                await _redis.StreamAddAsync(
                    RedisKeys.QueueKey(command.Vhost ?? string.Empty, command.Queue ?? string.Empty),
                    fields.Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());
                break;

            case WriteCommandKind.RegisterConsumer or WriteCommandKind.UnregisterConsumer:
                // Consumer registration is in-memory state; mirrored to the
                // topology log for durability.
                await _redis.StreamAddAsync(RedisKeys.TopologyKey(command.Vhost ?? string.Empty), fields.Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());
                break;
        }
    }

    // ---------- startup replay ----------

    private async Task ReplayAsync()
    {
        if (_redis is null)
        {
            return;
        }

        // Replay topology: exchanges, queues, bindings from {mq:top:<vhost>}.
        // Call _memory directly (not this.DeclareExchange etc.) so the drainer
        // is not fed a second copy of each entry.
        foreach (string vhost in _vhosts)
        {
            var topologyResult = await _redis.ExecuteAsync("XRANGE", RedisKeys.TopologyKey(vhost), "-", "+");
            ReplayTopologyEntries(topologyResult);
        }

        // Replay ready queues: for each known queue in each vhost, read its stream
        // and rebuild the ready buffer (without touching the drainer).
        foreach (string vhost in _vhosts)
        {
            foreach (string queueName in _memory.QueueNamesOfVhost(vhost))
            {
                string queueKey = RedisKeys.QueueKey(vhost, queueName);
                var queueResult = await _redis.ExecuteAsync("XRANGE", queueKey, "-", "+");
                ReplayQueueEntries(vhost, queueName, queueResult);
            }
        }
    }

    /// <summary>
    /// Startup eviction-policy gate. Reads Redis CONFIG GET maxmemory-policy and
    /// refuses to start if an eviction policy that could discard durable data is
    /// in effect. noeviction is permitted.
    /// </summary>
    private void CheckEvictionPolicy()
    {
        ArgumentNullException.ThrowIfNull(_redis);
        var result = _redis.Execute("CONFIG", "GET", "maxmemory-policy");
        if (result.Length >= 2)
        {
            var policyValue = (string)result[1];
            if (!string.Equals("noeviction", policyValue, StringComparison.OrdinalIgnoreCase))
            {
                throw new BrokerConfigurationException(
                    $"Redis maxmemory-policy '{policyValue}' may evict durable data; " +
                    "set maxmemory-policy noeviction or disable the eviction gate.");
            }
        }
    }

    private void ReplayTopologyEntries(RedisResult result)
    {
        for (int i = 0; i < result.Length; i++)
        {
            var entry = result[i]; // RedisResult: [id, [field1, value1, field2, value2, ...]]
            var id = (string)entry[0]!;
            var fields = (string[])entry[1]!;
            var dict = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int j = 0; j < fields.Length; j += 2)
            {
                dict[fields[j]!] = fields[j + 1]!;
            }

            string kind = dict.GetValueOrDefault("kind") ?? string.Empty;
            string vhost = dict.GetValueOrDefault("vhost") ?? string.Empty;

            switch (kind)
            {
                case nameof(WriteCommandKind.DeclareExchange):
                    _memory.DeclareExchange(new ExchangeInfo(
                        vhost,
                        dict.GetValueOrDefault("exchange") ?? string.Empty,
                        dict.GetValueOrDefault("type") ?? string.Empty));
                    break;

                case nameof(WriteCommandKind.DeclareQueue):
                    _memory.DeclareQueue(new QueueInfo(
                        vhost,
                        dict.GetValueOrDefault("queue") ?? string.Empty,
                        bool.TryParse(dict.GetValueOrDefault("durable") ?? "0", out var d) && d,
                        bool.TryParse(dict.GetValueOrDefault("exclusive") ?? "0", out var e) && e,
                        bool.TryParse(dict.GetValueOrDefault("auto-delete") ?? "0", out var a) && a,
                        MaxLength: int.TryParse(dict.GetValueOrDefault("x-max-length"), out var ml) ? ml : null));
                    break;

                case nameof(WriteCommandKind.Bind):
                    _memory.Bind(
                        vhost,
                        dict.GetValueOrDefault("exchange") ?? string.Empty,
                        dict.GetValueOrDefault("queue") ?? string.Empty,
                        dict.GetValueOrDefault("routing-key") ?? string.Empty,
                        WriteCommand.DecodeArguments(dict.GetValueOrDefault("arguments")));
                    break;
            }
        }
    }

    private void ReplayQueueEntries(string vhost, string queueName, RedisResult result)
    {
        for (int i = 0; i < result.Length; i++)
        {
            var entry = result[i]; // RedisResult: [id, [field1, value1, field2, value2, ...]]
            var fields = (string[])entry[1]!;
            var dict = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int j = 0; j < fields.Length; j += 2)
            {
                dict[fields[j]!] = fields[j + 1]!;
            }

            var record = StreamCodec.DecodeEnqueue(
                dict.Select(kv => (kv.Key, kv.Value)).ToList());
            ContentProperties? properties = null;
            if (!string.IsNullOrEmpty(record.PropertiesJson))
            {
                properties = JsonSerializer.Deserialize<ContentProperties>(record.PropertiesJson);
            }

            var message = new InMemoryMessage(
                properties ?? new ContentProperties(), record.Body, record.Exchange, record.RoutingKey, record.EnvelopeId);
            _memory.ReplayEnqueue(vhost, queueName, message);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        _stopCts.Cancel();
        if (_drainerTask is not null)
        {
            try
            {
                await _drainerTask;
            }
            catch
            {
                // Drainer may fault on shutdown; ignore.
            }
        }
    }
}
