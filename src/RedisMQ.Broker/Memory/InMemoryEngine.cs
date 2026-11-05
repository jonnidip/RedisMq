namespace RedisMQ.Broker.Memory;

using System.Diagnostics;
using System.Diagnostics.Metrics;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;

/// <summary>The outcome of one dead-letter attempt: which clearing rule (if any)
/// prevented a copy. The x-death header reason stays <c>rejected</c>/<c>expired</c>;
/// this only classifies the metric reason for a dropped copy.</summary>
internal enum DeadLetterDisposition
{
    NoDeadLetterExchange,
    Copied,
    MissingExchange,
    Unroutable,
    LoopGuard,
}

/// <summary>
/// The M0 in-memory topology engine: exchanges, queues and direct bindings per
/// vhost, predeclared with the default exchange and <c>amq.direct</c>, plus the
/// per-queue message store the publish path fills. It only stores and routes —
/// the channel (ChannelContext) owns every protocol check — and it is strictly
/// insertion-ordered and thread-safe, so the single <see cref="WireConnection"/>
/// frame pump can share one instance with tests that poke at engine state directly.
/// Queue deletion exists only through the reaper (x-expires) and the Redis
/// engine's exclusive/auto-delete reconciliation.
/// </summary>
public sealed class InMemoryEngine : IRouteEngine, IEngineDiagnostics, IDisposable
{
    private readonly object _lock = new();
    private readonly Dictionary<(string Vhost, string Name), ExchangeInfo> _exchanges = [];
    private readonly Dictionary<(string Vhost, string Name), QueueInfo> _queues = [];
    private readonly List<Binding> _bindings = [];

    // Task 7: per-queue delivery state (ready buffer, consumer registry, pump) keyed
    // like the old message lists; created lazily on first enqueue/consume and removed
    // by a queue delete or the x-expires reaper.
    private readonly Dictionary<(string Vhost, string Queue), InMemoryQueue> _delivery = [];
    private ulong _lastEnvelopeId; // the highest InMemoryMessage.EnvelopeId handed out

    // M4 Task 7: per-queue last-use stamps for x-expires (idle queue expiry). Pure
    // in-memory state: it resets on restart, so a queue restarts its idle clock.
    private readonly Dictionary<(string Vhost, string Queue), DateTimeOffset> _lastUsed = [];

    private readonly BrokerDiagnostics _diagnostics;
    private readonly IDisposable _readyGauge;
    private readonly IDisposable _consumerGauge;
    private bool _disposed;

    /// <summary>Creates the engine and predeclares, per vhost, the default exchange
    /// (empty name, direct type, durable) and <c>amq.direct</c> exactly as the real
    /// broker's internal declarations do. The declares run on empty dictionaries, so
    /// their null returns are the creation contract of the compare-and-set. A null
    /// diagnostics instance becomes a private one owned by this engine's lifetime.</summary>
    public InMemoryEngine(IReadOnlyList<string> vhosts, BrokerDiagnostics? diagnostics = null)
    {
        ArgumentNullException.ThrowIfNull(vhosts);
        _diagnostics = diagnostics ?? new BrokerDiagnostics();
        _readyGauge = _diagnostics.RegisterObservableGauge(
            "redismq.messages.ready",
            "{message}",
            "Ready messages per queue.",
            ObserveReady);
        _consumerGauge = _diagnostics.RegisterObservableGauge(
            "redismq.consumers",
            "{consumer}",
            "Registered consumers per queue.",
            ObserveConsumers);

        foreach (string vhost in vhosts)
        {
            DeclareExchange(new ExchangeInfo(vhost, "", "direct", Durable: true));
            DeclareExchange(new ExchangeInfo(vhost, "amq.direct", "direct", Durable: true));
        }
    }

    /// <summary>The diagnostics instance this engine emits to (never disposed here).</summary>
    public BrokerDiagnostics Diagnostics => _diagnostics;

    /// <summary>A memory engine is never durable and has no Redis connection to lose.</summary>
    public EngineHealth GetEngineHealth() =>
        new("memory", false, null, 0, 0, 0, null, null, null);

    /// <summary>Every declared queue's ready and consumer counts under the engine gate;
    /// unacked deliveries are connection state the server overlays.</summary>
    public IReadOnlyList<QueueSnapshot> QueueSnapshots()
    {
        lock (_lock)
        {
            List<QueueSnapshot> snapshots = [];
            foreach ((string vhost, string name) in _queues.Keys)
            {
                (int ready, int consumers) = CountsOf(vhost, name);
                snapshots.Add(new QueueSnapshot(vhost, name, ready, consumers));
            }

            return snapshots;
        }
    }

    /// <summary>Unregisters this engine's gauges; the shared diagnostics instance is
    /// untouched. Idempotent.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _readyGauge.Dispose();
        _consumerGauge.Dispose();
    }

    private IEnumerable<Measurement<long>> ObserveReady()
    {
        lock (_lock)
        {
            foreach ((string vhost, string name) in _queues.Keys)
            {
                (int ready, _) = CountsOf(vhost, name);
                yield return new Measurement<long>(
                    ready,
                    new TagList { { "vhost", vhost }, { "queue", name } });
            }
        }
    }

    private IEnumerable<Measurement<long>> ObserveConsumers()
    {
        lock (_lock)
        {
            foreach ((string vhost, string name) in _queues.Keys)
            {
                (_, int consumers) = CountsOf(vhost, name);
                yield return new Measurement<long>(
                    consumers,
                    new TagList { { "vhost", vhost }, { "queue", name } });
            }
        }
    }

    private (int Ready, int Consumers) CountsOf(string vhost, string name) =>
        _delivery.TryGetValue((vhost, name), out InMemoryQueue? delivery)
            ? (delivery.ReadyCount, delivery.ConsumerCount)
            : (0, 0);

    public ExchangeInfo? GetExchange(string vhost, string name)
    {
        lock (_lock)
        {
            return _exchanges.TryGetValue((vhost, name), out ExchangeInfo? exchange) ? exchange : null;
        }
    }

    public ExchangeInfo? DeclareExchange(ExchangeInfo exchange)
    {
        lock (_lock)
        {
            // Creation-only compare-and-set: an existing record is returned and never
            // replaced, so two racing first-declares cannot overwrite each other; the
            // loser takes the winner's record into its channel-side equivalence check.
            if (_exchanges.TryGetValue((exchange.Vhost, exchange.Name), out ExchangeInfo? prior))
            {
                return prior;
            }

            _exchanges[(exchange.Vhost, exchange.Name)] = exchange;
            return null;
        }
    }

    public QueueInfo? GetQueue(string vhost, string name)
    {
        lock (_lock)
        {
            return _queues.TryGetValue((vhost, name), out QueueInfo? queue) ? queue : null;
        }
    }

    public QueueInfo? DeclareQueue(QueueInfo queue)
    {
        lock (_lock)
        {
            _lastUsed[(queue.Vhost, queue.Name)] = DateTimeOffset.UtcNow;

            // The same creation-only compare-and-set as DeclareExchange: the winner's
            // record is returned to the loser, never overwritten.
            if (_queues.TryGetValue((queue.Vhost, queue.Name), out QueueInfo? prior))
            {
                return prior;
            }

            _queues[(queue.Vhost, queue.Name)] = queue;
            return null;
        }
    }

    public void Bind(string vhost, string exchange, string queue, string routingKey, FieldTable? arguments = null)
    {
        lock (_lock)
        {
            _lastUsed[(vhost, queue)] = DateTimeOffset.UtcNow;

            foreach (Binding existing in _bindings)
            {
                if (existing.Matches(vhost, exchange, queue, routingKey))
                {
                    return; // duplicate binding: no-op, original position preserved
                }
            }

            _bindings.Add(new Binding(vhost, exchange, queue, routingKey, arguments));
        }
    }

    public void Unbind(string vhost, string exchange, string queue, string routingKey)
    {
        lock (_lock)
        {
            for (int index = _bindings.Count - 1; index >= 0; index--)
            {
                if (_bindings[index].Matches(vhost, exchange, queue, routingKey))
                {
                    _bindings.RemoveAt(index);
                }
            }
        }
    }

    public IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties, string routingKey)
    {
        lock (_lock)
        {
            return RouteToAlreadyLocked(exchange, properties, routingKey);
        }
    }

    /// <summary>The routing pass without taking the engine gate (the caller already
    /// holds it — dead-letter construction and the reaper route under the lock).</summary>
    private List<string> RouteToAlreadyLocked(ExchangeInfo exchange, ContentProperties properties, string routingKey)
    {
        List<string> queues = [];
        foreach (Binding binding in _bindings)
        {
            if (binding.Vhost == exchange.Vhost
                && binding.Exchange == exchange.Name
                && MatchesRoutingKey(binding.RoutingKey, routingKey, exchange.Type))
            {
                // Headers exchange: match message headers against binding arguments
                if (exchange.Type == "headers" && !HeadersMatch(binding.Arguments, properties.Headers))
                {
                    continue;
                }
                queues.Add(binding.Queue);
            }
        }

        return queues;
    }

    /// <summary>
    /// Matches a publish routing key against a binding routing key for the given
    /// exchange type. Direct exchange requires exact match; topic exchange supports
    /// wildcard matching (star = exactly one word, hash = zero or more words).
    /// Headers exchange ignores the routing key; header matching happens separately.
    /// </summary>
    private static bool MatchesRoutingKey(string bindingKey, string routingKey, string exchangeType)
    {
        if (exchangeType == "direct")
        {
            return bindingKey == routingKey;
        }

        if (exchangeType == "topic")
        {
            return TopicMatches(bindingKey, routingKey);
        }

        // Headers exchange: routing key is irrelevant; header matching happens
        // elsewhere. Return true to let the caller decide based on headers.
        return true;
    }

    /// <summary>
    /// Topic wildcard matching per AMQP 0-9-1:
    ///   star matches exactly one word
    ///   hash matches zero or more words
    /// Words are separated by ".".
    /// </summary>
    private static bool TopicMatches(string bindingKey, string routingKey)
    {
        var bindingWords = bindingKey.Split('.');
        var routeWords = routingKey.Split('.');

        return TopicMatch(bindingWords, 0, routeWords, 0);
    }

    private static bool TopicMatch(string[] bindingWords, int bi, string[] routeWords, int ri)
    {
        // Both exhausted: match
        if (bi == bindingWords.Length && ri == routeWords.Length)
        {
            return true;
        }

        // Binding exhausted but route remains: no match (unless binding ended with #,
        // which is handled below)
        if (bi == bindingWords.Length)
        {
            return false;
        }

        // Route exhausted but binding remains: match only if remaining binding
        // words are all #
        if (ri == routeWords.Length)
        {
            for (int i = bi; i < bindingWords.Length; i++)
            {
                if (bindingWords[i] != "#")
                {
                    return false;
                }
            }
            return true;
        }

        string bw = bindingWords[bi];

        if (bw == "#")
        {
            // # matches zero or more words: try consuming 0..N route words
            for (int k = ri; k <= routeWords.Length; k++)
            {
                if (TopicMatch(bindingWords, bi + 1, routeWords, k))
                {
                    return true;
                }
            }
            return false;
        }

        if (ri >= routeWords.Length)
        {
            return false;
        }

        string rw = routeWords[ri];

        if (bw == "*")
        {
            // * matches exactly one word
            return TopicMatch(bindingWords, bi + 1, routeWords, ri + 1);
        }

        // Literal word must match exactly
        if (bw != rw)
        {
            return false;
        }

        return TopicMatch(bindingWords, bi + 1, routeWords, ri + 1);
    }

    /// <summary>
    /// Headers exchange matching: a message matches a binding if all (x-match=all)
    /// or any (x-match=any) of the binding's arguments are present and equal in the
    /// message headers. The x-match key itself is ignored.
    /// </summary>
    private static bool HeadersMatch(FieldTable? bindingArgs, FieldTable? messageHeaders)
    {
        if (bindingArgs is null || bindingArgs.Count == 0)
        {
            return true; // no binding args = match all
        }

        if (messageHeaders is null)
        {
            return false; // no message headers to match against
        }

        // Extract x-match from binding args
        bool matchAll = bindingArgs.TryGetValue("x-match", out var xMatchVal)
            && xMatchVal.TypeLetter == 'S'
            && (string)xMatchVal.Raw == "any";

        if (matchAll)
        {
            // any: at least one binding arg must match a message header
            foreach (var kv in bindingArgs)
            {
                if (kv.Key == "x-match") continue;
                if (messageHeaders.TryGetValue(kv.Key, out var msgVal) && msgVal.Equals(kv.Value))
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            // all: every binding arg must be present and equal in message headers
            foreach (var kv in bindingArgs)
            {
                if (kv.Key == "x-match") continue;
                if (!messageHeaders.TryGetValue(kv.Key, out var msgVal) || !msgVal.Equals(kv.Value))
                {
                    return false;
                }
            }
            return true;
        }
    }

    /// <summary>The declared queue names of a vhost, most-helpful for engine-state
    /// assertions (M1's storage replaces scans like this with real enumeration).</summary>
    public IEnumerable<string> QueueNamesOfVhost(string vhost)
    {
        lock (_lock)
        {
            List<string> names = [];
            foreach ((string Vhost, string Name) key in _queues.Keys)
            {
                if (key.Vhost == vhost)
                {
                    names.Add(key.Name);
                }
            }

            return names;
        }
    }

    /// <summary>Lands one completed publish in the queue's delivery state: one entry
    /// per (queue, enqueue) call with its own defensive body copy and its own
    /// engine-global envelope id, assigned under the engine lock; then the queue's
    /// pump is armed so a registered consumer serves it immediately.</summary>
    public EnqueueReceipt Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey)
    {
        ArgumentNullException.ThrowIfNull(properties);
        ArgumentNullException.ThrowIfNull(body);
        ulong envelopeId;
        List<InMemoryMessage> dropped = [];
        lock (_lock)
        {
            // The AMQP `expiration` property is milliseconds from enqueue, not an
            // absolute Unix timestamp: resolve the deadline now. Negative or
            // unparseable values leave no deadline (M1 behavior, pinned by tests),
            // and an overflowing deadline is dropped rather than thrown.
            DateTimeOffset? expiration = ResolveExpiration(properties.Expiration);

            _lastUsed[(vhost, queue)] = DateTimeOffset.UtcNow;
            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, queue);
            envelopeId = ++_lastEnvelopeId;
            delivery.AppendAlreadyLocked(new InMemoryMessage(properties, DefensiveCopy(body), exchange, routingKey, envelopeId, Expiration: expiration));
            delivery.WakeAlreadyLocked();

            // Enforce x-max-length if set
            if (_queues.TryGetValue((vhost, queue), out var qi) && qi.MaxLength is int maxLen && maxLen > 0)
            {
                dropped = delivery.RemoveOldestMessages(maxLen);
            }
        }

        foreach (InMemoryMessage _ in dropped)
        {
            _diagnostics.Count(
                _diagnostics.MessagesDropped,
                new TagList { { "vhost", vhost }, { "queue", queue }, { "reason", "max_length" } });
        }

        return EnqueueReceipt.Immediate(
            envelopeId,
            dropped.Count == 0 ? [] : dropped.Select(message => message.EnvelopeId).ToList());
    }

    /// <summary>[internal] Test introspection of a queue's stored messages: a fresh
    /// snapshot with freshly copied bodies across every vhost that has a queue with
    /// the name, in storage (first-delivery-state creation) order. Ready-only: messages
    /// claimed by the pump (in flight or unacked) are not visible here — the Task 7
    /// pump admits them by removing them from the ready buffer.</summary>
    public IReadOnlyList<InMemoryMessage> PeekQueue(string queue)
    {
        lock (_lock)
        {
            List<InMemoryMessage> snapshot = [];
            foreach ((string _, string Queue) key in _delivery.Keys)
            {
                if (key.Queue != queue)
                {
                    continue;
                }

                List<InMemoryMessage> raw = [];
                _delivery[key].SnapshotReadyAlreadyLocked(raw);
                foreach (InMemoryMessage message in raw)
                {
                    snapshot.Add(message with { Body = DefensiveCopy(message.Body) });
                }
            }

            return snapshot;
        }
    }

    // ---------- consume seam (Task 7) ----------

    /// <summary>The per-queue delivery state, created under the engine lock on first
    /// touch (enqueue, consume, requeue, or a claim attempt) and removed with the
    /// queue (explicit delete or the x-expires reaper).</summary>
    private InMemoryQueue DeliveryQueueAlreadyLocked(string vhost, string queue)
    {
        (string, string) key = (vhost, queue);
        if (!_delivery.TryGetValue(key, out InMemoryQueue? delivery))
        {
            delivery = new InMemoryQueue(_lock);
            _delivery[key] = delivery;
        }

        return delivery;
    }

    /// <summary>Replays a previously-enqueued message into the ready buffer without
    /// touching the drainer. Used by <see cref="RedisMQ.Redis.RedisStreamRouteEngine"/>
    /// on startup replay. Caller does not need to hold the engine lock.</summary>
    internal void ReplayEnqueue(string vhost, string queue, InMemoryMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_lock)
        {
            _lastEnvelopeId = Math.Max(_lastEnvelopeId, message.EnvelopeId);
            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, queue);
            delivery.AppendAlreadyLocked(message);
        }
    }

    /// <summary>Exclusive arbitration first (an exclusive consumer repels any joining
    /// consumer; a new exclusive consumer needs an empty registry), then registration
    /// and the pump arm — all inside the one engine lock, so a racing competer
    /// deterministically loses.</summary>
    public ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        lock (_lock)
        {
            if (!_queues.TryGetValue((vhost, queue), out QueueInfo? _))
            {
                return ConsumeRegisterOutcome.QueueNotFound;
            }

            _lastUsed[(vhost, queue)] = DateTimeOffset.UtcNow;

            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, queue);
            if (consumer.IsExclusive && delivery.HasConsumers)
            {
                return ConsumeRegisterOutcome.ExclusiveHeld;
            }

            if (!consumer.IsExclusive && delivery.HasExclusiveConsumer)
            {
                return ConsumeRegisterOutcome.ExclusiveHeld;
            }

            delivery.RegisterConsumerAlreadyLocked(consumer);
            return ConsumeRegisterOutcome.Registered;
        }
    }

    /// <summary>Drops the consumer's registration; unknown (queue, tag) pairs are
    /// silent no-ops, matching the cancel-ok-echo contract.</summary>
    public void UnregisterConsumer(string vhost, string queue, string consumerTag)
    {
        lock (_lock)
        {
            if (_delivery.TryGetValue((vhost, queue), out InMemoryQueue? delivery))
            {
                delivery.UnregisterConsumerAlreadyLocked(consumerTag);

                // The idle clock restarts when the last consumer leaves: x-expires
                // measures "no consumers and no use", not time since the queue was
                // ever touched.
                if (!delivery.HasConsumers)
                {
                    _lastUsed[(vhost, queue)] = DateTimeOffset.UtcNow;
                }
            }
        }
    }

    /// <summary>Back to the ready tail with the redelivered flag set, unless the
    /// queue's delivery limit is exhausted: then the message is dead-lettered with
    /// reason <c>rejected</c> (delivery-limit overflow). The delta appends the copy
    /// first, then removes the settled entry.</summary>
    public EngineDelta Requeue(string vhost, string queue, InMemoryMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        List<QueuedMessage> enqueued = [];
        List<QueuedMessageRef> removed = [];
        lock (_lock)
        {
            if (_queues.TryGetValue((vhost, queue), out QueueInfo? info)
                && info.DeliveryLimit is int limit
                && message.DeliveryCount >= limit)
            {
                DeadLetterAlreadyLocked(
                    vhost, queue, message, reason: "rejected", metricReason: "delivery_limit",
                    DateTimeOffset.UtcNow, enqueued, removed);
                return new EngineDelta(enqueued, removed, []);
            }

            InMemoryMessage redelivered = message with { Redelivered = true };
            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, queue);
            delivery.AppendAlreadyLocked(redelivered);
            delivery.WakeAlreadyLocked();
            enqueued.Add(new QueuedMessage(vhost, queue, redelivered));
            removed.Add(new QueuedMessageRef(vhost, queue, message.EnvelopeId));
        }

        return new EngineDelta(enqueued, removed, []);
    }

    /// <summary>The pump popped the message at claim time, so acknowledging only
    /// describes the durable removal. No memory mutation.</summary>
    public EngineDelta Acknowledge(string vhost, string queue, InMemoryMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return RemovedDelta(vhost, queue, message);
    }

    /// <summary>Reject-without-requeue: dead-letter when the queue has a DLX (reason
    /// <c>rejected</c>), otherwise remove only. The copy lands in the same delta as
    /// the removal, enqueue first.</summary>
    public EngineDelta Reject(string vhost, string queue, InMemoryMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        List<QueuedMessage> enqueued = [];
        List<QueuedMessageRef> removed = [];
        lock (_lock)
        {
            if (_queues.TryGetValue((vhost, queue), out QueueInfo? info) && info.DeadLetterExchange is not null)
            {
                DeadLetterAlreadyLocked(
                    vhost, queue, message, reason: "rejected", metricReason: "rejected",
                    DateTimeOffset.UtcNow, enqueued, removed);
            }
            else
            {
                removed.Add(new QueuedMessageRef(vhost, queue, message.EnvelopeId));
            }
        }

        return new EngineDelta(enqueued, removed, []);
    }

    /// <summary>The reaper's sweep: first idle queues (x-expires) are deleted with
    /// their messages, delivery state, bindings and last-use stamp — messages of an
    /// expired queue are dropped, never dead-lettered; then every ready message in
    /// the surviving queues whose deadline passed is dead-lettered or dropped. Keys
    /// are snapshotted first, so a dead-letter copy into another queue (or back into
    /// this one) cannot mutate the set being iterated.</summary>
    public EngineDelta Reap(DateTimeOffset now)
    {
        List<QueuedMessage> enqueued = [];
        List<QueuedMessageRef> removed = [];
        List<(string Vhost, string Name)> deletedQueues = [];
        List<(string Vhost, string Queue)> expiredMessages = [];
        lock (_lock)
        {
            (string Vhost, string Queue)[] queues = [.. _queues.Keys];
            foreach ((string Vhost, string Queue) key in queues)
            {
                if (!_queues.TryGetValue(key, out QueueInfo? info)
                    || info.Expiration is not TimeSpan idle
                    || !_lastUsed.TryGetValue(key, out DateTimeOffset lastUsed)
                    || now < lastUsed + idle)
                {
                    continue;
                }

                if (_delivery.TryGetValue(key, out InMemoryQueue? state) && state.HasConsumers)
                {
                    continue; // an active consumer keeps the queue alive
                }

                _queues.Remove(key);
                _delivery.Remove(key);
                _lastUsed.Remove(key);
                _bindings.RemoveAll(binding => binding.Vhost == key.Vhost && binding.Queue == key.Queue);
                deletedQueues.Add(key);
            }

            (string Vhost, string Queue)[] keys = [.. _delivery.Keys];
            List<InMemoryMessage> expired = [];
            foreach ((string Vhost, string Queue) key in keys)
            {
                if (!_delivery.TryGetValue(key, out InMemoryQueue? delivery))
                {
                    continue; // a dead-letter copy may have deleted nothing yet; be safe
                }

                expired.Clear();
                delivery.CollectExpiredMessages(now, expired);
                foreach (InMemoryMessage message in expired)
                {
                    expiredMessages.Add((key.Vhost, key.Queue));
                    DeadLetterAlreadyLocked(
                        key.Vhost, key.Queue, message, reason: "expired", metricReason: "expired",
                        now, enqueued, removed);
                }
            }
        }

        foreach ((string Vhost, string Queue) key in expiredMessages)
        {
            _diagnostics.Count(
                _diagnostics.MessagesExpired,
                new TagList { { "vhost", key.Vhost }, { "queue", key.Queue } });
            if (_diagnostics.IsLogEnabled(BrokerLogLevel.Debug))
            {
                _diagnostics.WriteLog(
                    BrokerLogLevel.Debug,
                    BrokerLogEvent.MessageExpired,
                    "message expired",
                    data:
                    [
                        new KeyValuePair<string, object?>("vhost", key.Vhost),
                        new KeyValuePair<string, object?>("queue", key.Queue),
                    ]);
            }
        }

        foreach ((string Vhost, string Name) key in deletedQueues)
        {
            _diagnostics.Count(
                _diagnostics.QueuesExpired,
                new TagList { { "vhost", key.Vhost } });
            if (_diagnostics.IsLogEnabled(BrokerLogLevel.Warning))
            {
                _diagnostics.WriteLog(
                    BrokerLogLevel.Warning,
                    BrokerLogEvent.QueueExpired,
                    "queue expired",
                    data:
                    [
                        new KeyValuePair<string, object?>("vhost", key.Vhost),
                        new KeyValuePair<string, object?>("queue", key.Name),
                    ]);
            }
        }

        return new EngineDelta(enqueued, removed, deletedQueues);
    }

    /// <summary>
    /// Builds the dead-letter delta entry for one unacked-but-settled message: the
    /// original is always removed; when the queue names a resolvable DLX with at
    /// least one target, a copy is appended to each target (new envelope id, reset
    /// delivery count, cleared expiry, x-death headers). A missing exchange, an
    /// unroutable copy, or the loop guard leaves the original removed with no copy.
    /// Caller holds the engine gate.
    /// </summary>
    private DeadLetterDisposition DeadLetterAlreadyLocked(
        string vhost,
        string queue,
        InMemoryMessage message,
        string reason,
        string metricReason,
        DateTimeOffset now,
        List<QueuedMessage> enqueued,
        List<QueuedMessageRef> removed)
    {
        removed.Add(new QueuedMessageRef(vhost, queue, message.EnvelopeId));

        if (!_queues.TryGetValue((vhost, queue), out QueueInfo? origin)
            || origin.DeadLetterExchange is not string deadLetterExchange)
        {
            return DeadLetterDisposition.NoDeadLetterExchange; // no DLX: drop, no failure
        }

        if (HasDeathFor(message.Props.Headers, queue, reason))
        {
            CountDeadLetterDropped(vhost, queue, "loop_guard");
            return DeadLetterDisposition.LoopGuard;
        }

        ContentProperties properties = WithDeath(message.Props, queue, message.Exchange, reason, now);
        string routingKey = origin.DeadLetterRoutingKey ?? message.RoutingKey;

        List<string> targets;
        if (deadLetterExchange.Length == 0)
        {
            // The default exchange addresses a queue by name; "" is the default
            // exchange itself, not "no DLX".
            targets = _queues.ContainsKey((vhost, routingKey)) ? [routingKey] : [];
        }
        else if (_exchanges.TryGetValue((vhost, deadLetterExchange), out ExchangeInfo? exchange))
        {
            targets = RouteToAlreadyLocked(exchange, properties, routingKey);
        }
        else
        {
            CountDeadLetterDropped(vhost, queue, "missing_exchange");
            return DeadLetterDisposition.MissingExchange; // named DLX does not exist
        }

        if (targets.Count == 0)
        {
            CountDeadLetterDropped(vhost, queue, "unroutable");
            return DeadLetterDisposition.Unroutable;
        }

        foreach (string target in targets)
        {
            ulong envelopeId = ++_lastEnvelopeId;
            InMemoryMessage copy = new(
                properties,
                DefensiveCopy(message.Body),
                deadLetterExchange,
                routingKey,
                envelopeId,
                DeliveryCount: 0,
                Expiration: null);

            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, target);
            delivery.AppendAlreadyLocked(copy);
            delivery.WakeAlreadyLocked();
            enqueued.Add(new QueuedMessage(vhost, target, copy));
            _diagnostics.Count(
                _diagnostics.MessagesDeadlettered,
                new TagList { { "vhost", vhost }, { "queue", queue }, { "reason", metricReason } });

            if (_queues.TryGetValue((vhost, target), out QueueInfo? targetInfo)
                && targetInfo.MaxLength is int maxLength
                && maxLength > 0)
            {
                foreach (InMemoryMessage evicted in delivery.RemoveOldestMessages(maxLength))
                {
                    removed.Add(new QueuedMessageRef(vhost, target, evicted.EnvelopeId));
                    // A dead-letter copy that evicts an incumbent is a drop like any
                    // other x-max-length eviction; count it so the queue's drop series
                    // does not silently under-report.
                    _diagnostics.Count(
                        _diagnostics.MessagesDropped,
                        new TagList
                        {
                            { "vhost", vhost },
                            { "queue", target },
                            { "reason", "max_length" },
                        });
                }
            }
        }

        return DeadLetterDisposition.Copied;
    }

    private void CountDeadLetterDropped(string vhost, string queue, string reason) =>
        _diagnostics.Count(
            _diagnostics.DeadLettersDropped,
            new TagList { { "vhost", vhost }, { "queue", queue }, { "reason", reason } });

    /// <summary>True when the headers already carry an x-death entry for this
    /// (queue, reason) pair — the loop guard's check.</summary>
    private static bool HasDeathFor(FieldTable? headers, string queue, string reason)
    {
        if (headers is null
            || !headers.TryGetValue("x-death", out FieldValue deaths)
            || deaths.Raw is not FieldValue[] entries)
        {
            return false;
        }

        foreach (FieldValue entry in entries)
        {
            if (entry.Raw is FieldTable table
                && table.TryGetValue("queue", out FieldValue deathQueue)
                && deathQueue.Raw as string == queue
                && table.TryGetValue("reason", out FieldValue deathReason)
                && deathReason.Raw as string == reason)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns the properties a dead-letter copy carries: a cloned header table with
    /// x-death updated (newest first: the matching (queue, reason) entry's count is
    /// incremented and its time refreshed, or a fresh entry is prepended), the
    /// x-first-death fields set once, and the AMQP expiration property cleared.
    /// Internal so the header shape is unit-testable without a routing topology.
    /// </summary>
    internal static ContentProperties WithDeath(
        ContentProperties properties,
        string queue,
        string exchange,
        string reason,
        DateTimeOffset now)
    {
        var headers = new FieldTable();
        if (properties.Headers is not null)
        {
            foreach ((string key, FieldValue value) in properties.Headers)
            {
                headers.Add(key, value);
            }
        }

        List<FieldValue> deaths = [];
        if (headers.TryGetValue("x-death", out FieldValue existing) && existing.Raw is FieldValue[] entries)
        {
            deaths.AddRange(entries);
        }

        int match = -1;
        for (int index = 0; index < deaths.Count; index++)
        {
            if (deaths[index].Raw is FieldTable table
                && table.TryGetValue("queue", out FieldValue deathQueue)
                && deathQueue.Raw as string == queue
                && table.TryGetValue("reason", out FieldValue deathReason)
                && deathReason.Raw as string == reason)
            {
                match = index;
                break;
            }
        }

        if (match >= 0)
        {
            var updated = new FieldTable();
            foreach ((string key, FieldValue value) in (FieldTable)deaths[match].Raw!)
            {
                updated.Add(key, value);
            }

            long count = updated.TryGetValue("count", out FieldValue countValue)
                && countValue.Raw is long parsed
                ? parsed + 1
                : 2;
            updated.Add("count", FieldValue.From(count));
            updated.Add("time", FieldValue.From(now));
            deaths[match] = FieldValue.From(updated);
        }
        else
        {
            deaths.Insert(0, FieldValue.From(new FieldTable([
                new("queue", FieldValue.From(queue)),
                new("reason", FieldValue.From(reason)),
                new("count", FieldValue.From(1L)),
                new("time", FieldValue.From(now)),
                new("exchange", FieldValue.From(exchange)),
            ])));
        }

        headers.Add("x-death", FieldValue.List([.. deaths]));
        if (!headers.ContainsKey("x-first-death-queue"))
        {
            headers.Add("x-first-death-queue", FieldValue.From(queue));
        }

        if (!headers.ContainsKey("x-first-death-reason"))
        {
            headers.Add("x-first-death-reason", FieldValue.From(reason));
        }

        if (!headers.ContainsKey("x-first-death-exchange"))
        {
            headers.Add("x-first-death-exchange", FieldValue.From(exchange));
        }

        return properties with { Headers = headers, Expiration = null };
    }

    private static EngineDelta RemovedDelta(string vhost, string queue, InMemoryMessage message) =>
        new([], [new QueuedMessageRef(vhost, queue, message.EnvelopeId)], []);

    /// <summary>The basic.get claim, fully atomic: existence, exclusive arbitration,
    /// the head pop and the remaining count run inside one engine-lock hold, so a
    /// concurrent pump claim cannot empty the queue between the check and the pop.</summary>
    public GetClaimOutcome TryClaimForGet(string vhost, string queue, out InMemoryMessage? message, out int remaining)
    {
        message = null;
        remaining = 0;
        lock (_lock)
        {
            if (!_queues.TryGetValue((vhost, queue), out QueueInfo? _))
            {
                return GetClaimOutcome.QueueNotFound;
            }

            _lastUsed[(vhost, queue)] = DateTimeOffset.UtcNow;

            // A queue with no delivery state yet has no consumers and no messages.
            if (!_delivery.TryGetValue((vhost, queue), out InMemoryQueue? delivery))
            {
                return GetClaimOutcome.Empty;
            }

            if (delivery.HasExclusiveConsumer)
            {
                return GetClaimOutcome.ExclusiveHeld;
            }

            if (!delivery.TryPopReadyAlreadyLocked(out InMemoryMessage? popped))
            {
                return GetClaimOutcome.Empty;
            }

            message = popped with { DeliveryCount = popped.DeliveryCount + 1 };
            remaining = delivery.ReadyCount;
            return GetClaimOutcome.Claimed;
        }
    }

    /// <summary>A pure pump arm — no storage interaction beyond the wake check.</summary>
    public void Nudge(string vhost, string queue)
    {
        lock (_lock)
        {
            if (_delivery.TryGetValue((vhost, queue), out InMemoryQueue? delivery))
            {
                delivery.WakeAlreadyLocked();
            }
        }
    }

    /// <summary>Deletes a queue and all its messages. Internal: used by M1's
    /// RedisStreamRouteEngine for exclusive/auto-delete reconciliation.</summary>
    internal void DeleteQueue(string vhost, string queue)
    {
        lock (_lock)
        {
            _queues.Remove((vhost, queue));
            _delivery.Remove((vhost, queue));
            _lastUsed.Remove((vhost, queue));
            _bindings.RemoveAll(binding => binding.Vhost == vhost && binding.Queue == queue);
        }
    }

    /// <summary>The queue's configured idle-expiry duration (x-expires), or null
    /// when the queue has none. Whether the idle clock has run out is the reaper's
    /// call (LastUsed + this duration).</summary>
    internal TimeSpan? GetQueueIdleExpiration(string vhost, string queue)
    {
        lock (_lock)
        {
            return _queues.TryGetValue((vhost, queue), out QueueInfo? qi) ? qi.Expiration : null;
        }
    }

    /// <summary>The queue's last-use stamp, or null when the queue does not exist.
    /// In-memory only: a restart forgets it and the idle clock begins afresh.</summary>
    internal DateTimeOffset? GetQueueLastUsed(string vhost, string queue)
    {
        lock (_lock)
        {
            return _lastUsed.TryGetValue((vhost, queue), out DateTimeOffset lastUsed) ? lastUsed : null;
        }
    }

    /// <summary>Connection teardown reconciliation: M0's in-memory engine has no
    /// connection-aware queue ownership, so this is a no-op. M1's
    /// RedisStreamRouteEngine implements it fully.</summary>
    public void ReconcileConnection(string connectionId)
    {
        // M0: no connection-tracking of queues, nothing to clean up.
    }

    /// <summary>Records exclusive queue ownership. M0's in-memory engine has no
    /// connection-aware queue ownership, so this is a no-op. M1's
    /// RedisStreamRouteEngine implements it fully.</summary>
    public void RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId)
    {
        // M0: no connection-tracking of queues, nothing to clean up.
    }

    /// <summary>The body bytes are handed to (or out of) the store solely through
    /// this copy, so no message ever aliases another's array.</summary>
    private static byte[] DefensiveCopy(byte[] body) =>
        body.Length == 0 ? [] : body.AsSpan().ToArray();

    /// <summary>The AMQP expiration property is a relative millisecond count. A
    /// negative or unparseable value means "no deadline"; a deadline that would
    /// overflow the clock is dropped rather than thrown.</summary>
    private static DateTimeOffset? ResolveExpiration(string? expiration)
    {
        if (string.IsNullOrEmpty(expiration)
            || !long.TryParse(expiration, out long milliseconds)
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

    private readonly record struct Binding(string Vhost, string Exchange, string Queue, string RoutingKey, FieldTable? Arguments)
    {
        public bool Matches(string vhost, string exchange, string queue, string routingKey) =>
            Vhost == vhost && Exchange == exchange && Queue == queue && RoutingKey == routingKey;
    }
}
