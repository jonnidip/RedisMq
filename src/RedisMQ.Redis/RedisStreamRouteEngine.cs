using System.Text.Json;
using System.Threading.Channels;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
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
    private readonly string _vhost;
    private readonly InMemoryEngine _memory;
    private readonly Channel<WriteCommand>? _channel;
    private readonly Task? _drainerTask;
    private readonly CancellationTokenSource _stopCts = new();

    /// <summary>
    /// Creates the engine. <paramref name="redis"/> may be null (e.g. Redis
    /// unavailable); in that case all writes go to memory only and no drainer
    /// is started. When <paramref name="replayOnStart"/> is true, Redis topology
    /// and ready-queue streams are replayed into memory before the engine serves.
    /// </summary>
    public RedisStreamRouteEngine(IDatabase? redis, string vhost, bool replayOnStart = true, int drainerCapacity = 1024)
    {
        ArgumentNullException.ThrowIfNull(vhost);
        _redis = redis;
        _vhost = vhost;
        _memory = new InMemoryEngine(new[] { vhost });

        if (redis is not null)
        {
            _channel = Channel.CreateBounded<WriteCommand>(
                new BoundedChannelOptions(drainerCapacity) { FullMode = BoundedChannelFullMode.Wait });
            _drainerTask = Task.Run(() => DrainAsync(_stopCts.Token));
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
        EnqueueCommand(WriteCommand.DeclareQueue(queue.Vhost, queue.Name, queue.Durable, queue.Exclusive, queue.AutoDelete));
        return prior;
    }

    public void Bind(string vhost, string exchange, string queue, string routingKey)
    {
        _memory.Bind(vhost, exchange, queue, routingKey);
        EnqueueCommand(WriteCommand.Bind(vhost, exchange, queue, routingKey));
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

    public void Requeue(string vhost, string queue, InMemoryMessage message)
    {
        _memory.Requeue(vhost, queue, message);
        EnqueueCommand(WriteCommand.Requeue(vhost, queue, message.EnvelopeId, message.Exchange, message.RoutingKey,
            message.Body, message.Props, message.Redelivered));
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
                await _redis.StreamAddAsync(RedisKeys.TopologyKey, fields.Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());
                break;

            case WriteCommandKind.Enqueue or WriteCommandKind.Requeue:
                await _redis.StreamAddAsync(
                    RedisKeys.QueueKey(command.Vhost ?? string.Empty, command.Queue ?? string.Empty),
                    fields.Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());
                break;

            case WriteCommandKind.RegisterConsumer or WriteCommandKind.UnregisterConsumer:
                // Consumer registration is in-memory state; mirrored to the
                // topology log for durability.
                await _redis.StreamAddAsync(RedisKeys.TopologyKey, fields.Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());
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

        // Replay topology: exchanges, queues, bindings from {mq:top}.
        // Call _memory directly (not this.DeclareExchange etc.) so the drainer
        // is not fed a second copy of each entry.
        var topologyResult = await _redis.ExecuteAsync("XRANGE", RedisKeys.TopologyKey, "-", "+");
        ReplayTopologyEntries(topologyResult);

        // Replay ready queues: for each known queue, read its stream and rebuild
        // the ready buffer (without touching the drainer).
        foreach (string queueName in _memory.QueueNamesOfVhost(_vhost))
        {
            string queueKey = RedisKeys.QueueKey(_vhost, queueName);
            var queueResult = await _redis.ExecuteAsync("XRANGE", queueKey, "-", "+");
            ReplayQueueEntries(queueName, queueResult);
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
                        bool.TryParse(dict.GetValueOrDefault("auto-delete") ?? "0", out var a) && a));
                    break;

                case nameof(WriteCommandKind.Bind):
                    _memory.Bind(
                        vhost,
                        dict.GetValueOrDefault("exchange") ?? string.Empty,
                        dict.GetValueOrDefault("queue") ?? string.Empty,
                        dict.GetValueOrDefault("routing-key") ?? string.Empty);
                    break;
            }
        }
    }

    private void ReplayQueueEntries(string queueName, RedisResult result)
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
            _memory.ReplayEnqueue(_vhost, queueName, message);
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
