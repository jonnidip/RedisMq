using System.Globalization;
using System.Linq;
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
    private readonly SemaphoreSlim? _drainerSlots;
    private readonly CancellationTokenSource _stopCts = new();

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
            _drainerSlots = new SemaphoreSlim(drainerCapacity, drainerCapacity);
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

    /// <summary>Test seam: the drainer's slot semaphore, exposed so a test can
    /// exhaust it deterministically. Null when Redis is absent.</summary>
    internal SemaphoreSlim? DrainerSlotsForTest => _drainerSlots;

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
            return new EnqueueReceipt(memoryReceipt.EnvelopeId, ValueTask.FromResult(false), memoryReceipt.DroppedEnvelopeIds);
        }

        if (memoryReceipt.DroppedEnvelopeIds.Count > 0)
        {
            // x-max-length evictions are mirrored best-effort: a missed removal can
            // resurrect a dropped message on replay, but never lose a live one.
            _ = _channel.Writer.TryWrite(WriteCommand.Remove(
                vhost, queue, [.. memoryReceipt.DroppedEnvelopeIds]));
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

        _ = _channel.Writer.TryWrite(batch.Count == 1 ? batch[0] : WriteCommand.Delta([.. batch]));
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
        _ = _channel.Writer.TryWrite(command);
    }

    private async Task DrainAsync(CancellationToken ct)
    {
        await foreach (var command in _channel!.Reader.ReadAllAsync(ct))
        {
            bool succeeded = true;
            try
            {
                await WriteCommandToRedisAsync(command);
            }
            catch
            {
                // Drainer is best-effort: memory is the system of record. A
                // transient Redis failure drops the mirrored write; the in-memory
                // state is already updated, and a waiting publisher is told the
                // truth (confirm.nack) rather than a lie.
                succeeded = false;
            }
            finally
            {
                if (command.ReservedDrainerSlot)
                {
                    _drainerSlots!.Release();
                }

                command.Completion?.TrySetResult(succeeded);
            }
        }
    }

    private async Task WriteCommandToRedisAsync(WriteCommand command)
    {
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
    }
}
