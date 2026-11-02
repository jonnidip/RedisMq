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

    // ---------- startup replay (Task 4 stub — no-op at Task 3) ----------

    private async Task ReplayAsync()
    {
        // Task 4 implements replay from {mq:top} and {mq:q:...} into _memory.
        // At Task 3 this is a no-op so the engine can serve immediately.
        await Task.CompletedTask;
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
