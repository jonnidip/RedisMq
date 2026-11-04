namespace RedisMQ.Broker.Server;

using System.Diagnostics;
using RedisMQ.Broker.Memory;

/// <summary>
/// One publish waiting to be confirmed, in the order its sequence number was
/// assigned. The channel builds exactly one entry for every publish on a
/// confirm-mode channel — routed, unroutable and mandatory-unroutable alike — so
/// the connection's single confirm loop emits every outcome in publish order.
/// </summary>
/// <param name="Channel">The channel identity the entry belongs to; a retired or
/// replaced channel number makes the entry stale (sequence numbers are never
/// reused for another channel).</param>
/// <param name="ChannelNumber">The wire channel number the outcome is written on.</param>
/// <param name="Sequence">The publish's confirm sequence (confirm.ack/nack tag).</param>
/// <param name="Receipts">One durability receipt per routed queue; empty for an
/// unroutable publish. No receipt means "nothing to persist".</param>
/// <param name="ReturnFirst">The mandatory-unroutable basic.return, written before
/// the confirm for the same sequence.</param>
/// <param name="StartedTimestamp">The <see cref="Stopwatch"/> timestamp taken when
/// the content dispatch began; the confirm loop turns it into the confirm duration.</param>
/// <param name="Activity">The <c>amqp.publish</c> span this publish owns; the confirm
/// loop stops it exactly once (the channel stops only the non-confirm case).</param>
public sealed record PendingConfirm(
    ChannelContext Channel,
    ushort ChannelNumber,
    long Sequence,
    IReadOnlyList<EnqueueReceipt> Receipts,
    ChannelOutcomeBasicReturn? ReturnFirst,
    long StartedTimestamp = 0,
    Activity? Activity = null);
