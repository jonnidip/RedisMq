using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using System.Threading.Channels;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
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
public sealed class RedisStreamRouteEngine : IRouteEngine, IEngineDiagnostics, IAsyncDisposable
{
    private readonly IDatabase? _redis;
    private readonly string[] _vhosts;
    private readonly InMemoryEngine _memory;
    private readonly BrokerDiagnostics _diagnostics;
    private readonly Channel<WriteCommand>? _channel;
    private readonly Task? _drainerTask;
    private readonly SemaphoreSlim? _drainerSlots;
    private readonly CancellationTokenSource _stopCts = new();

    // Mirror health: written by the drainer and the protocol path, read under
    // _healthLock by GetEngineHealth.
    private readonly object _healthLock = new();
    private long _mirrorFailures;
    private long _mirrorDropped;
    private DateTimeOffset? _lastMirrorFailureUtc;
    private DateTimeOffset? _lastMirrorDropUtc;
    private string? _lastError;
    private bool _failureEpisode;
    private IDisposable? _depthGauge;

    // Exclusive queue ownership and auto-delete consumer tracking for connection
    // teardown reconciliation.
    private readonly Dictionary<(string Vhost, string Queue), string> _exclusiveQueues = [];
    private readonly Dictionary<(string Vhost, string Queue), HashSet<string>> _autoDeleteQueues = [];

    // The envelope-id -> stream-id map the settle path resolves against. Written by
    // the drainer after each XADD, read (and stashed) by MirrorDelta before a batch
    // can overwrite an entry for the same envelope id.
    private readonly Dictionary<(string Vhost, string Queue, ulong EnvelopeId), RedisValue> _streamIds = [];
    private readonly object _streamIdsLock = new();

    /// <summary>
    /// Creates the engine. <paramref name="redis"/> may be null (e.g. Redis
    /// unavailable); in that case all writes go to memory only and no drainer
    /// is started. When <paramref name="replayOnStart"/> is true, Redis topology
    /// and ready-queue streams are replayed into memory before the engine serves.
    /// When <paramref name="enforceEvictionGate"/> is true and Redis is available,
    /// the server's maxmemory-policy is checked at startup; eviction policies that
    /// could discard durable data cause the constructor to throw.
    /// </summary>
    public RedisStreamRouteEngine(IDatabase? redis, IReadOnlyList<string> vhosts, bool replayOnStart = true, int drainerCapacity = 1024, bool enforceEvictionGate = true, BrokerDiagnostics? diagnostics = null)
    {
        ArgumentNullException.ThrowIfNull(vhosts);
        if (vhosts.Count == 0)
        {
            throw new ArgumentException("At least one vhost is required", nameof(vhosts));
        }
        _redis = redis;
        _vhosts = [.. vhosts];
        _diagnostics = diagnostics ?? new BrokerDiagnostics();
        _memory = new InMemoryEngine(_vhosts, _diagnostics);

        if (redis is not null)
        {
            _channel = Channel.CreateBounded<WriteCommand>(
                new BoundedChannelOptions(drainerCapacity) { FullMode = BoundedChannelFullMode.Wait });
            _drainerSlots = new SemaphoreSlim(drainerCapacity, drainerCapacity);
            _drainerTask = Task.Run(() => DrainAsync(_stopCts.Token));
            _depthGauge = _diagnostics.RegisterObservableGauge(
                "redismq.redis.drainer.depth",
                "{command}",
                "Commands queued in the Redis drainer.",
                () => [new Measurement<long>(_channel!.Reader.Count)]);
        }

        try
        {
            if (enforceEvictionGate && redis is not null)
            {
                CheckEvictionPolicy();
            }

            if (replayOnStart && redis is not null)
            {
                try
                {
                    ReplayAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    _diagnostics.Count(_diagnostics.RedisReplayFailures, new TagList());
                    if (_diagnostics.IsLogEnabled(BrokerLogLevel.Error))
                    {
                        _diagnostics.WriteLog(
                            BrokerLogLevel.Error,
                            BrokerLogEvent.RedisMirrorFailed,
                            "redis replay failed");
                    }

                    throw;
                }
            }
        }
        catch
        {
            // Construction failed after the drainer and its gauge were created: the
            // caller has no instance to dispose, so unwind them here rather than let
            // the task and the gauge outlive a half-built engine.
            _stopCts.Cancel();
            _depthGauge?.Dispose();
            if (_drainerTask is not null)
            {
                _ = _drainerTask.ContinueWith(
                    static task => _ = task.Exception,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted
                        | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }

            _memory.Dispose();
            throw;
        }
    }

    // ---------- IRouteEngine reads (delegate to memory, synchronous) ----------

    /// <summary>Test seam: the drainer's slot semaphore, exposed so a test can
    /// exhaust it deterministically. Null when Redis is absent.</summary>
    internal SemaphoreSlim? DrainerSlotsForTest => _drainerSlots;

    private Func<WriteCommand, Task>? _writeOverrideForTest;

    /// <summary>Test seam: replaces the Redis write so a failure episode can be
    /// induced deterministically. Null restores the real write path.</summary>
    internal Func<WriteCommand, Task>? WriteOverrideForTest
    {
        get => Volatile.Read(ref _writeOverrideForTest);
        set => Volatile.Write(ref _writeOverrideForTest, value);
    }

    /// <summary>Test seam: completes the drainer's writer so subsequent enqueues are
    /// dropped as if the channel were closed. True when a channel exists.</summary>
    internal bool CompleteDrainerForTest() => _channel?.Writer.TryComplete() ?? false;

    /// <summary>The diagnostics instance this engine emits to (never disposed here).</summary>
    public BrokerDiagnostics Diagnostics => _diagnostics;

    /// <summary>Durability and connection state of the Redis backend, plus the
    /// drainer's mirror-failure/drop counters and last sanitized error.</summary>
    public EngineHealth GetEngineHealth()
    {
        DateTimeOffset? lastFailure;
        DateTimeOffset? lastDrop;
        string? lastError;
        lock (_healthLock)
        {
            lastFailure = _lastMirrorFailureUtc;
            lastDrop = _lastMirrorDropUtc;
            lastError = _lastError;
        }

        return new EngineHealth(
            "redis",
            _redis is not null,
            _redis?.Multiplexer.IsConnected,
            _channel?.Reader.Count ?? 0,
            Interlocked.Read(ref _mirrorFailures),
            Interlocked.Read(ref _mirrorDropped),
            lastFailure,
            lastDrop,
            lastError);
    }

    /// <summary>Queue depth and consumer counts come from the inner memory store; both
    /// deployments report identical per-queue numbers.</summary>
    public IReadOnlyList<QueueSnapshot> QueueSnapshots() => _memory.QueueSnapshots();

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
        EnqueueCommand(WriteCommand.DeclareQueue(
            queue.Vhost,
            queue.Name,
            queue.Durable,
            queue.Exclusive,
            queue.AutoDelete,
            queue.MaxLength,
            queue.Expiration is TimeSpan idle
                ? (int)Math.Clamp(idle.TotalMilliseconds, 0, int.MaxValue)
                : null,
            queue.DeadLetterExchange,
            queue.DeadLetterRoutingKey,
            queue.DeliveryLimit));

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

    public EnqueueReceipt Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey)
    {
        if (_channel is null || _drainerSlots is null)
        {
            return _memory.Enqueue(vhost, queue, properties, body, exchange, routingKey);
        }

        if (!_drainerSlots.Wait(0))
        {
            // Backpressure contract: no slot means the mirror cannot promise
            // durability, so the publish is refused before it touches memory.
            _diagnostics.Count(_diagnostics.RedisDrainerSaturated, new TagList());
            if (_diagnostics.IsLogEnabled(BrokerLogLevel.Warning))
            {
                _diagnostics.WriteLog(
                    BrokerLogLevel.Warning,
                    BrokerLogEvent.DrainerSaturated,
                    "publish refused: redis drainer saturated");
            }

            return new EnqueueReceipt(0, ValueTask.FromResult(false), []);
        }

        EnqueueReceipt memoryReceipt;
        try
        {
            memoryReceipt = _memory.Enqueue(vhost, queue, properties, body, exchange, routingKey);
        }
        catch
        {
            _drainerSlots.Release();
            throw;
        }

        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        WriteCommand command = WriteCommand.Enqueue(
            vhost, queue, memoryReceipt.EnvelopeId, exchange, routingKey, body, properties,
            ExpirationOf(properties))
            with { Completion = completion, ReservedDrainerSlot = true };

        if (!_channel.Writer.TryWrite(command))
        {
            _drainerSlots.Release();
            completion.TrySetResult(false);
            CountMirrorDropped(command);
            return new EnqueueReceipt(memoryReceipt.EnvelopeId, ValueTask.FromResult(false), memoryReceipt.DroppedEnvelopeIds);
        }

        if (memoryReceipt.DroppedEnvelopeIds.Count > 0)
        {
            // x-max-length evictions are mirrored best-effort: a missed removal can
            // resurrect a dropped message on replay, but never lose a live one.
            WriteCommand remove = WriteCommand.Remove(
                vhost, queue, [.. memoryReceipt.DroppedEnvelopeIds]);
            if (!_channel.Writer.TryWrite(remove))
            {
                CountMirrorDropped(remove);
            }
        }

        return new EnqueueReceipt(
            memoryReceipt.EnvelopeId,
            new ValueTask<bool>(completion.Task),
            memoryReceipt.DroppedEnvelopeIds);
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

    public EngineDelta Requeue(string vhost, string queue, InMemoryMessage message)
    {
        EngineDelta delta = _memory.Requeue(vhost, queue, message);
        MirrorDelta(delta);
        return delta;
    }

    public EngineDelta Acknowledge(string vhost, string queue, InMemoryMessage message)
    {
        EngineDelta delta = _memory.Acknowledge(vhost, queue, message);
        MirrorDelta(delta);
        return delta;
    }

    public EngineDelta Reject(string vhost, string queue, InMemoryMessage message)
    {
        EngineDelta delta = _memory.Reject(vhost, queue, message);
        MirrorDelta(delta);
        return delta;
    }

    public EngineDelta Reap(DateTimeOffset now)
    {
        EngineDelta delta = _memory.Reap(now);
        MirrorDelta(delta);
        return delta;
    }

    /// <summary>
    /// Translates one engine delta into a single drainer batch. Removed entries are
    /// resolved to stream ids <b>before</b> the batch is queued: a requeue reuses its
    /// envelope id, so the new XADD would otherwise overwrite the map entry and leak
    /// the old stream entry. The batch order is enqueue first, remove second — a
    /// crash between them duplicates on replay but never loses a message.
    /// </summary>
    private void MirrorDelta(EngineDelta delta)
    {
        if (_channel is null || delta == EngineDelta.Empty)
        {
            return;
        }

        List<WriteCommand> batch = [];

        foreach (QueuedMessage queued in delta.Enqueued)
        {
            InMemoryMessage message = queued.Message;
            batch.Add(WriteCommand.Enqueue(
                queued.Vhost, queued.Queue, message.EnvelopeId, message.Exchange, message.RoutingKey,
                message.Body, message.Props, message.Expiration, message.DeliveryCount, message.Redelivered));
        }

        foreach (IGrouping<(string Vhost, string Queue), QueuedMessageRef> group in
                 delta.Removed.GroupBy(removed => (removed.Vhost, removed.Queue)))
        {
            List<RedisValue> streamIds = [];
            lock (_streamIdsLock)
            {
                foreach (QueuedMessageRef removed in group)
                {
                    if (_streamIds.TryGetValue((group.Key.Vhost, group.Key.Queue, removed.EnvelopeId), out RedisValue streamId))
                    {
                        streamIds.Add(streamId);
                    }
                }
            }

            if (streamIds.Count > 0)
            {
                batch.Add(WriteCommand.RemoveStreamIds(
                    group.Key.Vhost, group.Key.Queue, [.. streamIds.Select(id => id.ToString())]));
            }
        }

        foreach ((string vhost, string name) in delta.DeletedQueues)
        {
            batch.Add(WriteCommand.DeleteQueue(vhost, name));
        }

        if (batch.Count == 0)
        {
            return;
        }

        WriteCommand outgoing = batch.Count == 1 ? batch[0] : WriteCommand.Delta([.. batch]);
        if (!_channel.Writer.TryWrite(outgoing))
        {
            CountMirrorDropped(outgoing);
        }
    }

    /// <summary>The publish path's absolute deadline from the AMQP relative
    /// millisecond <c>expiration</c> property, mirroring the in-memory parse.</summary>
    private static DateTimeOffset? ExpirationOf(ContentProperties properties)
    {
        if (string.IsNullOrEmpty(properties.Expiration)
            || !long.TryParse(properties.Expiration, out long milliseconds)
            || milliseconds < 0)
        {
            return null;
        }

        try
        {
            return DateTimeOffset.UtcNow.AddMilliseconds(milliseconds);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
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
        if (!_channel.Writer.TryWrite(command))
        {
            CountMirrorDropped(command);
        }
    }

    /// <summary>Records one command the drainer channel would not accept. A dropped
    /// command degrades the mirror but not readiness: memory is still the system of
    /// record. <paramref name="reason"/> distinguishes a full channel during normal
    /// operation from commands abandoned at shutdown.</summary>
    private void CountMirrorDropped(WriteCommand command, string reason = "channel_full")
    {
        Interlocked.Increment(ref _mirrorDropped);
        lock (_healthLock)
        {
            _lastMirrorDropUtc = DateTimeOffset.UtcNow;
        }

        _diagnostics.Count(
            _diagnostics.RedisMirrorDropped,
            new TagList
            {
                { "kind", command.Kind.ToString() },
                { "reason", reason },
            });
    }

    private async Task DrainAsync(CancellationToken ct)
    {
        await foreach (var command in _channel!.Reader.ReadAllAsync(ct))
        {
            // Shutdown stops starting new mirror writes. The reader still yields
            // commands that were buffered before cancellation, so account each one
            // here instead of letting the write run after DisposeAsync began.
            if (ct.IsCancellationRequested)
            {
                AbandonWrite(command);
                continue;
            }

            bool succeeded = true;
            long started = Stopwatch.GetTimestamp();
            try
            {
                await WriteCommandToRedisAsync(command);
                if (_failureEpisode)
                {
                    _failureEpisode = false;
                    if (_diagnostics.IsLogEnabled(BrokerLogLevel.Information))
                    {
                        _diagnostics.WriteLog(
                            BrokerLogLevel.Information,
                            BrokerLogEvent.RedisMirrorRecovered,
                            "redis mirror recovered");
                    }
                }
            }
            catch (Exception ex)
            {
                // Drainer is best-effort: memory is the system of record. A
                // transient Redis failure drops the mirrored write; the in-memory
                // state is already updated, and a waiting publisher is told the
                // truth (confirm.nack) rather than a lie.
                succeeded = false;
                Interlocked.Increment(ref _mirrorFailures);
                string error = SanitizeMirrorError(ex);
                lock (_healthLock)
                {
                    _lastMirrorFailureUtc = DateTimeOffset.UtcNow;
                    _lastError = error;
                }

                if (!_failureEpisode)
                {
                    _failureEpisode = true;
                    if (_diagnostics.IsLogEnabled(BrokerLogLevel.Error))
                    {
                        _diagnostics.WriteLog(
                            BrokerLogLevel.Error,
                            BrokerLogEvent.RedisMirrorFailed,
                            "redis mirror write failed",
                            ex,
                            new KeyValuePair<string, object?>("kind", command.Kind.ToString()),
                            new KeyValuePair<string, object?>("error", error));
                    }
                }
            }
            finally
            {
                if (command.ReservedDrainerSlot)
                {
                    _drainerSlots!.Release();
                }

                _diagnostics.Count(
                    _diagnostics.RedisMirrorCommands,
                    new TagList
                    {
                        { "kind", command.Kind.ToString() },
                        { "outcome", succeeded ? "success" : "failure" },
                    });
                _diagnostics.Measure(
                    _diagnostics.RedisMirrorDuration,
                    Stopwatch.GetElapsedTime(started).TotalSeconds,
                    new TagList { { "kind", command.Kind.ToString() } });

                command.Completion?.TrySetResult(succeeded);
            }
        }
    }

    /// <summary>The first line of a failure message, bounded to 200 characters.
    /// Health reports this, never a stack trace or a connection string.</summary>
    private static string SanitizeMirrorError(Exception ex)
    {
        string message = ex.Message.Length > 0 ? ex.Message : ex.GetType().Name;
        int newline = message.IndexOfAny(['\r', '\n']);
        if (newline >= 0)
        {
            message = message[..newline];
        }

        return message.Length <= 200 ? message : message[..200];
    }

    private async Task WriteCommandToRedisAsync(WriteCommand command)
    {
        if (WriteOverrideForTest is { } write)
        {
            await write(command);
            return;
        }

        ArgumentNullException.ThrowIfNull(_redis);
        var fields = command.Kind == WriteCommandKind.Delta ? [] : command.ToStreamFields();

        switch (command.Kind)
        {
            case WriteCommandKind.DeclareExchange or WriteCommandKind.DeclareQueue or
                 WriteCommandKind.Bind or WriteCommandKind.Unbind:
                await _redis.StreamAddAsync(RedisKeys.TopologyKey(command.Vhost ?? string.Empty), ToEntries(fields));
                break;

            case WriteCommandKind.Enqueue:
                string queueKey = RedisKeys.QueueKey(command.Vhost ?? string.Empty, command.Queue ?? string.Empty);
                RedisValue streamId = await _redis.StreamAddAsync(queueKey, ToEntries(fields));
                lock (_streamIdsLock)
                {
                    _streamIds[(command.Vhost ?? string.Empty, command.Queue ?? string.Empty, command.EnvelopeId)] = streamId;
                }

                break;

            case WriteCommandKind.Remove:
                await RemoveEnvelopeIdsAsync(command);
                break;

            case WriteCommandKind.RemoveStreamIds:
                await RemoveStreamIdsAsync(command);
                break;

            case WriteCommandKind.DeleteQueue:
                await _redis.StreamAddAsync(RedisKeys.TopologyKey(command.Vhost ?? string.Empty), ToEntries(fields));
                await _redis.KeyDeleteAsync(RedisKeys.QueueKey(command.Vhost ?? string.Empty, command.Queue ?? string.Empty));
                ForgetQueue(command.Vhost ?? string.Empty, command.Queue ?? string.Empty);
                break;

            case WriteCommandKind.RegisterConsumer or WriteCommandKind.UnregisterConsumer:
                // Consumer registration is in-memory state; mirrored to the
                // topology log for durability.
                await _redis.StreamAddAsync(RedisKeys.TopologyKey(command.Vhost ?? string.Empty), ToEntries(fields));
                break;

            case WriteCommandKind.Delta:
                foreach (WriteCommand child in command.Batch ?? [])
                {
                    await WriteCommandToRedisAsync(child);
                }

                break;
        }
    }

    private static NameValueEntry[] ToEntries((string Field, string Value)[] fields) =>
        fields.Select(field => new NameValueEntry(field.Field, field.Value)).ToArray();

    /// <summary>Resolves envelope ids to stream ids under the map lock, forgets them,
    /// then XDELs the resolved ids.</summary>
    private async Task RemoveEnvelopeIdsAsync(WriteCommand command)
    {
        ArgumentNullException.ThrowIfNull(_redis);
        if (command.EnvelopeIds is not { Length: > 0 } envelopeIds)
        {
            return;
        }

        string vhost = command.Vhost ?? string.Empty;
        string queue = command.Queue ?? string.Empty;
        List<RedisValue> streamIds = [];
        lock (_streamIdsLock)
        {
            foreach (ulong envelopeId in envelopeIds)
            {
                if (_streamIds.Remove((vhost, queue, envelopeId), out RedisValue streamId))
                {
                    streamIds.Add(streamId);
                }
            }
        }

        if (streamIds.Count > 0)
        {
            await _redis.StreamDeleteAsync(RedisKeys.QueueKey(vhost, queue), [.. streamIds]);
        }
    }

    /// <summary>XDELs exact stream ids and drops any map entry that still points at
    /// one of them (a newer entry for the same envelope id is left alone).</summary>
    private async Task RemoveStreamIdsAsync(WriteCommand command)
    {
        ArgumentNullException.ThrowIfNull(_redis);
        if (command.StreamIds is not { Length: > 0 } streamIds)
        {
            return;
        }

        string vhost = command.Vhost ?? string.Empty;
        string queue = command.Queue ?? string.Empty;
        RedisValue[] values = [.. streamIds.Select(id => (RedisValue)id)];
        await _redis.StreamDeleteAsync(RedisKeys.QueueKey(vhost, queue), values);

        lock (_streamIdsLock)
        {
            List<(string, string, ulong)> stale = [];
            foreach (((string Vhost, string Queue, ulong EnvelopeId) key, RedisValue value) in _streamIds)
            {
                if (key.Vhost == vhost && key.Queue == queue && streamIds.Contains(value.ToString()))
                {
                    stale.Add(key);
                }
            }

            foreach ((string, string, ulong) key in stale)
            {
                _streamIds.Remove(key);
            }
        }
    }

    private void ForgetQueue(string vhost, string queue)
    {
        lock (_streamIdsLock)
        {
            List<(string, string, ulong)> stale = _streamIds.Keys
                .Where(key => key.Vhost == vhost && key.Queue == queue)
                .ToList();
            foreach ((string, string, ulong) key in stale)
            {
                _streamIds.Remove(key);
            }
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
                await ReplayQueueEntriesAsync(vhost, queueName, queueResult);
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
                        int.TryParse(dict.GetValueOrDefault("x-expires"), out var ex)
                            ? TimeSpan.FromMilliseconds(ex)
                            : null,
                        int.TryParse(dict.GetValueOrDefault("x-max-length"), out var ml) ? ml : null,
                        dict.GetValueOrDefault("x-dead-letter-exchange"),
                        dict.GetValueOrDefault("x-dead-letter-routing-key"),
                        int.TryParse(dict.GetValueOrDefault("x-delivery-limit"), out var dl) ? dl : null));
                    break;

                case nameof(WriteCommandKind.Bind):
                    _memory.Bind(
                        vhost,
                        dict.GetValueOrDefault("exchange") ?? string.Empty,
                        dict.GetValueOrDefault("queue") ?? string.Empty,
                        dict.GetValueOrDefault("routing-key") ?? string.Empty,
                        WriteCommand.DecodeArguments(dict.GetValueOrDefault("arguments")));
                    break;

                case nameof(WriteCommandKind.Unbind):
                    _memory.Unbind(
                        vhost,
                        dict.GetValueOrDefault("exchange") ?? string.Empty,
                        dict.GetValueOrDefault("queue") ?? string.Empty,
                        dict.GetValueOrDefault("routing-key") ?? string.Empty);
                    break;

                case nameof(WriteCommandKind.DeleteQueue):
                    _memory.DeleteQueue(vhost, dict.GetValueOrDefault("queue") ?? string.Empty);
                    break;
            }
        }
    }

    /// <summary>
    /// Rebuilds one queue's ready buffer and collapses crash-window duplicates:
    /// entries sharing an envelope id keep the higher delivery count (tie: the later
    /// stream id), the stale copies are XDELed, and every survivor's stream id is
    /// recorded so a later ack can remove it.
    /// </summary>
    private async Task ReplayQueueEntriesAsync(string vhost, string queueName, RedisResult result)
    {
        ArgumentNullException.ThrowIfNull(_redis);
        List<(string Id, EnvelopeRecord Record)> entries = [];
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

            entries.Add((id, StreamCodec.DecodeEnqueue(dict.Select(kv => (kv.Key, kv.Value)).ToList())));
        }

        Dictionary<ulong, (string Id, EnvelopeRecord Record)> winners = [];
        List<RedisValue> stale = [];
        foreach ((string id, EnvelopeRecord record) in entries)
        {
            if (!winners.TryGetValue(record.EnvelopeId, out (string Id, EnvelopeRecord Record) current))
            {
                winners[record.EnvelopeId] = (id, record);
                continue;
            }

            if (IsNewer((id, record), current))
            {
                stale.Add(current.Id);
                winners[record.EnvelopeId] = (id, record);
            }
            else
            {
                stale.Add(id);
            }
        }

        if (stale.Count > 0)
        {
            await _redis.StreamDeleteAsync(RedisKeys.QueueKey(vhost, queueName), [.. stale]);
        }

        foreach (KeyValuePair<ulong, (string Id, EnvelopeRecord Record)> winner in winners)
        {
            ulong envelopeId = winner.Key;
            string streamId = winner.Value.Id;
            EnvelopeRecord record = winner.Value.Record;
            var message = new InMemoryMessage(
                record.Properties,
                record.Body,
                record.Exchange,
                record.RoutingKey,
                envelopeId,
                record.Redelivered,
                record.ExpiresAt,
                record.DeliveryCount);
            _memory.ReplayEnqueue(vhost, queueName, message);
            _diagnostics.Count(_diagnostics.RedisReplayMessages, new TagList());

            lock (_streamIdsLock)
            {
                _streamIds[(vhost, queueName, envelopeId)] = streamId;
            }
        }
    }

    private static bool IsNewer((string Id, EnvelopeRecord Record) candidate, (string Id, EnvelopeRecord Record) current)
    {
        if (candidate.Record.DeliveryCount != current.Record.DeliveryCount)
        {
            return candidate.Record.DeliveryCount > current.Record.DeliveryCount;
        }

        return CompareStreamId(candidate.Id, current.Id) > 0;
    }

    private static int CompareStreamId(string left, string right)
    {
        (long LeftMs, long LeftSeq) = ParseStreamId(left);
        (long RightMs, long RightSeq) = ParseStreamId(right);
        int byMillis = LeftMs.CompareTo(RightMs);
        return byMillis != 0 ? byMillis : LeftSeq.CompareTo(RightSeq);
    }

    private static (long Milliseconds, long Sequence) ParseStreamId(string id)
    {
        int dash = id.IndexOf('-');
        return dash < 0
            ? (0, 0)
            : (long.Parse(id[..dash], CultureInfo.InvariantCulture), long.Parse(id[(dash + 1)..], CultureInfo.InvariantCulture));
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

        AbandonQueuedWrites();
        _depthGauge?.Dispose();
        _memory.Dispose();
    }

    /// <summary>Counts and refuses the commands still queued when the drainer stopped.
    /// Each reserved slot is released and each waiting publisher is told the truth
    /// (<c>Durable == false</c>) instead of hanging on a completion the disposed
    /// drainer will never service.</summary>
    private void AbandonQueuedWrites()
    {
        if (_channel is null)
        {
            return;
        }

        while (_channel.Reader.TryRead(out WriteCommand? command) && command is not null)
        {
            AbandonWrite(command);
        }
    }

    /// <summary>Accounts for one command the drainer will never write: release its
    /// admission slot, complete the publisher's durability task false, and count the
    /// drop under <c>reason=shutdown</c>. Called for buffered commands noticed after
    /// cancellation and for whatever is still queued once the drainer returned, so
    /// each command is counted exactly once.</summary>
    private void AbandonWrite(WriteCommand command)
    {
        if (command.ReservedDrainerSlot)
        {
            _drainerSlots?.Release();
        }

        command.Completion?.TrySetResult(false);
        CountMirrorDropped(command, "shutdown");
    }
}
