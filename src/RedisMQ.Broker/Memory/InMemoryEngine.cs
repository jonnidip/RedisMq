namespace RedisMQ.Broker.Memory;

using RedisMQ.Broker.Codec;

/// <summary>
/// The M0 in-memory topology engine: exchanges, queues and direct bindings per
/// vhost, predeclared with the default exchange and <c>amq.direct</c>, plus the
/// per-queue message store the publish path fills. It only stores and routes —
/// the channel (ChannelContext) owns every protocol check — and it is strictly
/// insertion-ordered and thread-safe, so the single <see cref="WireConnection"/>
/// frame pump can share one instance with tests that poke at engine state directly.
/// Queue deletes do not exist in M0, so the message dictionaries only ever grow.
/// </summary>
public sealed class InMemoryEngine : IRouteEngine
{
    private readonly object _lock = new();
    private readonly Dictionary<(string Vhost, string Name), ExchangeInfo> _exchanges = [];
    private readonly Dictionary<(string Vhost, string Name), QueueInfo> _queues = [];
    private readonly List<Binding> _bindings = [];

    // Task 7: per-queue delivery state (ready buffer, consumer registry, pump) keyed
    // like the old message lists; created lazily on first enqueue/consume and, since
    // queue deletes do not exist in M0, never removed again.
    private readonly Dictionary<(string Vhost, string Queue), InMemoryQueue> _delivery = [];
    private ulong _lastEnvelopeId; // the highest InMemoryMessage.EnvelopeId handed out

    /// <summary>Creates the engine and predeclares, per vhost, the default exchange
    /// (empty name, direct type, durable) and <c>amq.direct</c> exactly as the real
    /// broker's internal declarations do. The declares run on empty dictionaries, so
    /// their null returns are the creation contract of the compare-and-set.</summary>
    public InMemoryEngine(IReadOnlyList<string> vhosts)
    {
        ArgumentNullException.ThrowIfNull(vhosts);
        foreach (string vhost in vhosts)
        {
            DeclareExchange(new ExchangeInfo(vhost, "", "direct", Durable: true));
            DeclareExchange(new ExchangeInfo(vhost, "amq.direct", "direct", Durable: true));
        }
    }

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

    public void Bind(string vhost, string exchange, string queue, string routingKey)
    {
        lock (_lock)
        {
            foreach (Binding existing in _bindings)
            {
                if (existing.Matches(vhost, exchange, queue, routingKey))
                {
                    return; // duplicate binding: no-op, original position preserved
                }
            }

            _bindings.Add(new Binding(vhost, exchange, queue, routingKey));
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
            List<string> queues = [];
            foreach (Binding binding in _bindings)
            {
                if (binding.Vhost == exchange.Vhost
                    && binding.Exchange == exchange.Name
                    && binding.RoutingKey == routingKey)
                {
                    queues.Add(binding.Queue);
                }
            }

            return queues;
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
    public void Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey)
    {
        ArgumentNullException.ThrowIfNull(properties);
        ArgumentNullException.ThrowIfNull(body);
        lock (_lock)
        {
            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, queue);
            delivery.AppendAlreadyLocked(new InMemoryMessage(properties, DefensiveCopy(body), exchange, routingKey, ++_lastEnvelopeId));
            delivery.WakeAlreadyLocked();
        }
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
    /// touch (enqueue, consume, requeue, or a claim attempt) and kept forever, since
    /// queue deletes do not exist in M0.</summary>
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
            }
        }
    }

    /// <summary>Back to the ready tail with the redelivered flag set; the pump is
    /// armed under the same lock, so the requeued message can be claimed the moment
    /// the gate is released.</summary>
    public void Requeue(string vhost, string queue, InMemoryMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_lock)
        {
            InMemoryQueue delivery = DeliveryQueueAlreadyLocked(vhost, queue);
            delivery.AppendAlreadyLocked(message with { Redelivered = true });
            delivery.WakeAlreadyLocked();
        }
    }

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

            message = popped;
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

    /// <summary>The body bytes are handed to (or out of) the store solely through
    /// this copy, so no message ever aliases another's array.</summary>
    private static byte[] DefensiveCopy(byte[] body) =>
        body.Length == 0 ? [] : body.AsSpan().ToArray();

    private readonly record struct Binding(string Vhost, string Exchange, string Queue, string RoutingKey)
    {
        public bool Matches(string vhost, string exchange, string queue, string routingKey) =>
            Vhost == vhost && Exchange == exchange && Queue == queue && RoutingKey == routingKey;
    }
}
