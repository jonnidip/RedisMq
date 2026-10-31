namespace RedisMQ.Broker.Memory;

/// <summary>
/// Minimal manifest of a declared exchange. Born in Task 4 only so
/// <see cref="IRouteEngine"/>'s <c>FindExchange</c> shape can exist before the engine
/// itself; Task 5 owns the fuller topology records (durable/auto-delete flags,
/// arguments) and may extend this record.
/// </summary>
public sealed record ExchangeInfo(string Vhost, string Name, string Type);

/// <summary>
/// The seam between the wire server (WireConnection) and whatever backs topology and
/// routing. STUB BORN IN TASK 4 (ledger ruling): AmqpBrokerServer's constructor
/// references this interface before Task 5 implements the in-memory engine, so the
/// minimal member set lives here now and Task 5 fills the semantics (and may finalize
/// these signature shapes, e.g. richer declare arguments and QueueInfo records).
/// </summary>
public interface IRouteEngine
{
    /// <summary>Returns the exchange, or null when it does not exist in the vhost.</summary>
    ExchangeInfo? FindExchange(string vhost, string name);

    void DeclareExchange(ExchangeInfo exchange);

    void DeclareQueue(string vhost, string name);

    void Bind(string vhost, string exchange, string queue, string routingKey);

    /// <summary>The queue names bound to the exchange for the given routing key.</summary>
    IEnumerable<string> Route(ExchangeInfo exchange, string routingKey);
}
