using System.Buffers;
using System.Buffers.Binary;

namespace RedisMQ.Broker.Codec;

/// <summary>
/// Pulls complete AMQP frames off a <see cref="ReadOnlySequence{T}"/> of buffered stream
/// bytes. Reads are always made from bytes already in the buffer, so the decoder never
/// reads past a frame-end mid-sequence: a frame is only decoded once all of it is
/// present, and a hard error is decided as soon as the evidence appears (an oversized
/// size field is rejected before its payload arrives, a bad frame-end only once the
/// whole frame is buffered). Consumed bytes are removed from the front of the buffer;
/// a partially received frame is left untouched for the next fill. It is the only
/// place that decides AMQP hard errors, so it surfaces them as
/// <see cref="AmqpProtocolException"/> rather than recoverable decode failures.
/// </summary>
public static class FrameDecoder
{
    /// <summary>Decodes 0..n frames from the buffer, appending them to
    /// <paramref name="frames"/> and removing the consumed prefix. Returns
    /// <see cref="DecodeMode.NeedMore"/> when no complete frame remains in the buffer
    /// (empty or partial). Hard wire errors (unknown frame type, frame-end mismatch,
    /// payload size over the negotiated frame-max) throw <see cref="AmqpProtocolException"/>.</summary>
    /// <param name="negotiatedFrameMax">The negotiated frame-max in bytes (long-uint, as
    /// tuned on the wire — e.g. RabbitMQ's default 131072).</param>
    public static DecodeMode ReadFrames(ref ReadOnlySequence<byte> buffer, List<AmqpFrame> frames, uint negotiatedFrameMax)
    {
        ArgumentNullException.ThrowIfNull(frames);

        // Pinned outside the loop (CA2014): the header is the fixed 7-byte prefix
        // type(1) + channel(2) + size(4), copied because its bytes may straddle segments.
        Span<byte> header = stackalloc byte[AmqpFrame.FrameOverhead - 1];
        while (buffer.Length >= AmqpFrame.FrameOverhead - 1) // the 7-byte fixed header
        {
            buffer.Slice(0, header.Length).CopyTo(header);
            byte type = header[0];
            ushort channel = BinaryPrimitives.ReadUInt16BigEndian(header[1..]);
            uint size = BinaryPrimitives.ReadUInt32BigEndian(header[3..]);

            // Decide hard errors from the header alone: this never waits for more bytes.
            if (!IsKnownFrameType(type))
            {
                throw new AmqpProtocolException(
                    $"Unknown AMQP frame type {type} (0x{type:X2})");
            }

            if (size > negotiatedFrameMax)
            {
                throw new AmqpProtocolException(
                    $"Frame payload of {size} byte(s) is larger than the negotiated frame-max {negotiatedFrameMax}");
            }

            long frameLength = AmqpFrame.FrameOverhead + size; // header + payload + frame-end
            if (buffer.Length < frameLength)
            {
                return DecodeMode.NeedMore; // wait for the rest of this frame
            }

            byte frameEnd = ByteAt(buffer, frameLength - 1);
            if (frameEnd != AmqpFrame.FrameEnd)
            {
                throw new AmqpProtocolException(
                    $"Frame ends with byte 0x{frameEnd:X2} instead of the frame-end marker 0x{AmqpFrame.FrameEnd:X2}");
            }

            byte[] payload = new byte[size];
            buffer.Slice(AmqpFrame.FrameOverhead - 1, size).CopyTo(payload);
            frames.Add(new AmqpFrame(type, channel, payload));
            buffer = buffer.Slice(frameLength);
        }

        return DecodeMode.NeedMore;
    }

    private static bool IsKnownFrameType(byte type) => type is AmqpFrame.FrameMethod
        or AmqpFrame.FrameHeader or AmqpFrame.FrameBody or AmqpFrame.FrameHeartbeat;

    private static byte ByteAt(in ReadOnlySequence<byte> sequence, long index) =>
        sequence.Slice(index, 1).FirstSpan[0];
}
