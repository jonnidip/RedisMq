namespace RedisMQ.Broker.Memory;

using RedisMQ.Broker.Codec;

/// <summary>
/// One message landed in a queue by a completed basic.publish (the M0 in-memory
/// representation M1's durable storage will replace). <see cref="EnvelopeId"/> is
/// the engine-global, monotonic, 1-based identity assigned at enqueue time —
/// fan-out to N queues creates N messages with N distinct ids. The body storage the
/// engine hands out is a defensive copy: whoever holds a message never aliases the
/// broker's bytes.
/// </summary>
public sealed record InMemoryMessage(
    ContentProperties Props,
    byte[] Body,
    string Exchange,
    string RoutingKey,
    ulong EnvelopeId);
