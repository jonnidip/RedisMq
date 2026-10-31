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
    private readonly Dictionary<(string Vhost, string Queue), List<InMemoryMessage>> _messages = [];
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

    /// <summary>Lands one completed publish in the queue's message list: one entry
    /// per (queue, enqueue) call with its own defensive body copy and its own
    /// engine-global envelope id, assigned under the engine lock.</summary>
    public void Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey)
    {
        ArgumentNullException.ThrowIfNull(properties);
        ArgumentNullException.ThrowIfNull(body);
        lock (_lock)
        {
            (string, string) key = (vhost, queue);
            if (!_messages.TryGetValue(key, out List<InMemoryMessage>? list))
            {
                list = [];
                _messages[key] = list;
            }

            list.Add(new InMemoryMessage(properties, DefensiveCopy(body), exchange, routingKey, ++_lastEnvelopeId));
        }
    }

    /// <summary>[internal] Test introspection of a queue's stored messages: a fresh
    /// snapshot with freshly copied bodies — lists or body bytes handed out here can
    /// never write back into the engine. The name is matched across every vhost that
    /// has a queue with it, in storage (first-enqueue) order; queue deletes do not
    /// exist in M0, so the whole dict is enumerated as-is.</summary>
    public IReadOnlyList<InMemoryMessage> PeekQueue(string queue)
    {
        lock (_lock)
        {
            List<InMemoryMessage> snapshot = [];
            foreach ((string _, string Queue) key in _messages.Keys)
            {
                if (key.Queue != queue)
                {
                    continue;
                }

                foreach (InMemoryMessage message in _messages[key])
                {
                    snapshot.Add(message with { Body = DefensiveCopy(message.Body) });
                }
            }

            return snapshot;
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
