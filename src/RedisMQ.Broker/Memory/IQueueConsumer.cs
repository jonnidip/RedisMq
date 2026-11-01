namespace RedisMQ.Broker.Memory;

/// <summary>
/// One consumer a queue's delivery pump can serve. Consumers are created by the
/// channel (ChannelContext's per-channel admission bookkeeping) and registered into
/// the engine by <c>IRouteEngine.RegisterConsumer</c>; the pump hands prospective
/// messages to <see cref="TryAdmitForDelivery"/> while the engine's structure lock is
/// held, which by contract performs the consuming channel's credit check and ledger
/// registration synchronously (no awaits under the engine gate) and schedules the
/// deliver's write on the channel's serialized writer chain.
/// </summary>
public interface IQueueConsumer
{
    /// <summary>The consumer tag, unique per channel (server-assigned tags are
    /// <c>amq.ctag-…</c>).</summary>
    string ConsumerTag { get; }

    /// <summary>Whether this consumer claimed exclusive delivery rights on its queue:
    /// while an exclusive consumer is registered, no other consumer may join the queue
    /// (and a new exclusive consumer cannot join an occupied queue either).</summary>
    bool IsExclusive { get; }

    /// <summary>
    /// Attempts to admit <paramref name="message"/> for delivery to this consumer.
    /// Both the decision and the bookkeeping (credit consumption, the
    /// unacked-ledger entry, the writer-chain job) must complete synchronously inside
    /// the call — the engine's lock is held across it.
    /// </summary>
    /// <returns>
    /// true when admitted: the message leaves the queue, the delivery write rides the
    /// channel's chain, and <see cref="DrainDeliveryWritesAsync"/> must drain it after
    /// the engine gate is released. false when the consumer's channel has no credit
    /// (prefetch count or the bytes extension): the message stays at the queue head
    /// undelivered.
    /// </returns>
    bool TryAdmitForDelivery(InMemoryMessage message);

    /// <summary>Flushes the writer jobs a prior <see cref="TryAdmitForDelivery"/>
    /// scheduled (called strictly outside the engine gate — never hold the gate across
    /// a write).</summary>
    Task DrainDeliveryWritesAsync();
}
