namespace RedisMQ.Broker.Memory;

using RedisMQ.Broker.Codec;

/// <summary>
/// Manifest of a declared exchange: the declare flags the channel's equivalence
/// checks compare, verbatim as declared (RabbitMQ stores its own adjusted flags;
/// the M0 deviation is recorded in the Task 5 report).
/// </summary>
public sealed record ExchangeInfo(
    string Vhost,
    string Name,
    string Type,
    bool Durable = false,
    bool AutoDelete = false,
    bool Internal = false);

/// <summary>
/// Manifest of a declared queue. Flags again verbatim as declared: RabbitMQ's
/// channel strips <c>durable</c> when <c>exclusive</c> is declared; M0 does not
/// (Task 5 report deviation), so the stored record is exactly what arrived.
/// </summary>
public sealed record QueueInfo(
    string Vhost,
    string Name,
    bool Durable = false,
    bool Exclusive = false,
    bool AutoDelete = false,
    DateTimeOffset? Expiration = null);

/// <summary>
/// What <c>RegisterConsumer</c> decided about a consume request the channel ran:
/// registered-and-woken, or one of the two protocol refusals the channel maps to
/// not-found (404) and resource-locked (405) closes.
/// </summary>
public enum ConsumeRegisterOutcome
{
    /// <summary>The consumer joined the queue's registry and its pump was armed.</summary>
    Registered,

    /// <summary>The queue does not exist in the vhost.</summary>
    QueueNotFound,

    /// <summary>Exclusive access is unavailable: an exclusive consumer holds the queue,
    /// or the new consumer demanded exclusivity on an occupied one.</summary>
    ExclusiveHeld,
}

/// <summary>
/// What <c>TryClaimForGet</c> decided about a basic.get: the head was popped into the
/// channel's hands, or one of the three refusals/absences the channel maps onto its
/// wire replies.
/// </summary>
public enum GetClaimOutcome
{
    /// <summary>The head message was popped and handed to the caller.</summary>
    Claimed,

    /// <summary>The queue does not exist in the vhost.</summary>
    QueueNotFound,

    /// <summary>An exclusive consumer holds the queue; basic.get is refused like a
    /// competing consume would be.</summary>
    ExclusiveHeld,

    /// <summary>No ready message — the get-empty path.</summary>
    Empty,
}

/// <summary>
/// The seam between the wire server (WireConnection/ChannelContext) and whatever
/// backs topology and routing. The channel owns every protocol check (naming,
/// equivalence, not-found, refused); the engine only stores declarations, holds
/// bindings, and answers routing — this final M0 shape replaces Task 4's stub.
/// </summary>
public interface IRouteEngine
{
    /// <summary>Returns the exchange, or null when it does not exist in the vhost.</summary>
    ExchangeInfo? GetExchange(string vhost, string name);

    /// <summary>
    /// Atomic creation-only declare of an exchange. When the (vhost, name) is absent the
    /// request is stored and null is returned; when a record already exists it is
    /// returned unchanged — an existing record is never replaced, so the acceptance of
    /// an (equivalent) redeclare is the caller's equivalence check, not the engine's.
    /// The compare-and-insert happens inside one engine lock, which makes concurrent
    /// first-declares deterministic: exactly one caller observes null, the other
    /// observes the winner's record.
    /// </summary>
    ExchangeInfo? DeclareExchange(ExchangeInfo exchange);

    /// <summary>Returns the queue, or null when it does not exist in the vhost.</summary>
    QueueInfo? GetQueue(string vhost, string name);

    /// <summary>Atomic creation-only declare of a queue, the same contract as
    /// <see cref="DeclareExchange"/>: store-and-return-null on miss, return the prior
    /// unchanged record on hit, never replace an existing record.</summary>
    QueueInfo? DeclareQueue(QueueInfo queue);

    /// <summary>Records a binding; a duplicate is a no-op keeping the original order.</summary>
    void Bind(string vhost, string exchange, string queue, string routingKey, FieldTable? arguments = null);

    /// <summary>Drops a binding; removing an absent one is a no-op (RabbitMQ semantics).</summary>
    void Unbind(string vhost, string exchange, string queue, string routingKey);

    /// <summary>The queue names bound to the exchange for the given routing key, in
    /// binding-insertion order. The content properties parameter is the seam M1's
    /// topic routing needs; the direct engine ignores it.</summary>
    IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties, string routingKey);

    /// <summary>
    /// Lands one completed publish's content in the named queue: the only way the
    /// channel (ChannelContext) writes through to storage. The body argument is the
    /// fully-assembled content body; the engine stores its own copy per target, so a
    /// fan-out to N queues never aliases one byte array across messages, and callers
    /// owning the assembled bytes are free to hand them over as-is. The engine
    /// assigns the 1-based monotonic <see cref="InMemoryMessage.EnvelopeId"/>
    /// (queue deletes do not exist in M0, so ids never recycle).
    /// </summary>
    void Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey);

    /// <summary>
    /// Registers a consumer on the named queue (Task 7 consume seam). Every call is
    /// atomic under the engine's structure lock: the exclusivity arbitration and the
    /// pump arming (the consumer may immediately start delivering) complete inside
    /// the call.
    /// </summary>
    ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer);

    /// <summary>
    /// Removes a consumer by tag; an unknown tag is a no-op (basic.cancel echoes ok for
    /// any tag). Remaining consumers are armed: a later-credit or fresh-message wake
    /// can serve them.
    /// </summary>
    void UnregisterConsumer(string vhost, string queue, string consumerTag);

    /// <summary>
    /// Puts an unacked message back on its queue's ready tail with
    /// <see cref="InMemoryMessage.Redelivered"/> set (the M0 stream-consonant
    /// requeue-to-tail placement; RabbitMQ distinguishes placements and the M0
    /// simplification is recorded in the Task 7 report). The pump is armed: a waiting
    /// or returning consumer may claim it immediately.
    /// </summary>
    void Requeue(string vhost, string queue, InMemoryMessage message);

    /// <summary>
    /// The basic.get claim: atomically pops the queue head (and reports the messages
    /// remaining after it) under the engine lock, after the same exclusive-consumer
    /// arbitration a competing consume would face. <paramref name="message"/> is only
    /// meaningful on <see cref="GetClaimOutcome.Claimed"/>.
    /// </summary>
    GetClaimOutcome TryClaimForGet(string vhost, string queue, out InMemoryMessage? message, out int remaining);

    /// <summary>Arms the queue's delivery pump without changing storage: the re-check
    /// a credit-freeing or prefetch-raising path on the channel needs (a pump that
    /// idled out on a full gate must be re-spawned by whoever frees credit).</summary>
    void Nudge(string vhost, string queue);

    /// <summary>Connection teardown reconciliation: deletes exclusive queues owned by
    /// <paramref name="connectionId"/> and auto-delete queues whose last consumer
    /// has disconnected. Called by <see cref="WireConnection"/> on connection end.
    /// M0's <see cref="InMemoryEngine"/> implements this as a no-op; M1's
    /// <see cref="RedisStreamRouteEngine"/> implements it fully.
    /// </summary>
    void ReconcileConnection(string connectionId);

    /// <summary>
    /// Records that <paramref name="connectionId"/> owns an exclusive queue.
    /// Called by <see cref="WireConnection"/> when a channel declares an exclusive
    /// queue on its connection.
    /// </summary>
    void RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId);
}
