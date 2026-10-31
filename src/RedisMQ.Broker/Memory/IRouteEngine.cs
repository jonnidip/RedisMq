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
    bool AutoDelete = false);

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
    void Bind(string vhost, string exchange, string queue, string routingKey);

    /// <summary>Drops a binding; removing an absent one is a no-op (RabbitMQ semantics).</summary>
    void Unbind(string vhost, string exchange, string queue, string routingKey);

    /// <summary>The queue names bound to the exchange for the given routing key, in
    /// binding-insertion order. The content properties parameter is the seam M1's
    /// topic routing needs; the direct engine ignores it.</summary>
    IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties, string routingKey);
}
