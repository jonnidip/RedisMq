namespace RedisMQ.Broker.Server;

using System.Threading;
using System.Threading.Tasks;
using RedisMQ.Broker.Codec;

/// <summary>
/// The write surface the channel's consumer paths (deliveries, get replies,
/// consumer-family replies) hand their bytes to — implemented by the connection
/// (WireConnection), which owns the transport and the negotiated frame-max, and which
/// serializes every wire write behind one connection-wide gate. Guest callers run on
/// the queue pumps (outside the connection's frame pump), so implementations must be
/// safe to call concurrently and must absorb transport death the same way the
/// connection's own writes do.
/// </summary>
public interface IChannelDeliveryHost
{
    /// <summary>Encodes and writes one method frame on the channel.</summary>
    ValueTask WriteMethodToWireAsync(ushort channel, AmqpMethod method, CancellationToken ct);

    /// <summary>Encodes and writes the full content sequence (header plus the body
    /// re-chunked per the negotiated frame-max) as one atomic write, so a concurrent
    /// writer can never split a delivery's content.</summary>
    ValueTask WriteContentToWireAsync(ushort channel, ContentProperties properties, byte[] body, CancellationToken ct);

    /// <summary>Encodes and writes a full delivery (basic.deliver method, content
    /// header, body re-chunked) as ONE atomic write — the gated whole that keeps a
    /// deliver's frames interleaving with nothing else but other whole frames.</summary>
    ValueTask WriteDeliveryToWireAsync(ushort channel, AmqpMethod deliver, ContentProperties properties, byte[] body, CancellationToken ct);

    /// <summary>Hands one publish outcome to the connection's ordered confirm loop.
    /// Blocks the frame pump when the bounded confirm queue is full — that is
    /// deliberate backpressure, never a drop and never an invented answer.</summary>
    void EnqueueConfirm(PendingConfirm confirm);

    /// <summary>Writes a mandatory-unroutable basic.return (method, content header,
    /// body re-chunked) as one atomic write, preserving return-before-confirm order.</summary>
    ValueTask WriteReturnToWireAsync(ushort channel, ChannelOutcomeBasicReturn returned, CancellationToken ct);
}
