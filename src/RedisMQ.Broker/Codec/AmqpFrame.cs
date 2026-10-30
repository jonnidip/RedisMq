namespace RedisMQ.Broker.Codec;

/// <summary>
/// One decoded AMQP 0-9-1 frame: a 1-byte frame type, the channel it arrived on, and
/// the payload between the frame header and the frame-end octet. Payloads are copied
/// out of the incoming stream into a stable buffer, so decoded frames outlive any
/// reuse of the receive buffer.
/// </summary>
public sealed record AmqpFrame(byte Type, ushort Channel, ReadOnlyMemory<byte> Payload)
{
    public const byte FrameMethod = 1;
    public const byte FrameHeader = 2;
    public const byte FrameBody = 3;
    public const byte FrameHeartbeat = 8;
    /// <summary>The mandatory frame terminator, 0xCE.</summary>
    public const byte FrameEnd = 0xCE;
    /// <summary>Minimum frame-max a connection will negotiate (AMQP frame_min_size).</summary>
    public const int FrameMinSize = 4096;
    /// <summary>Empty-frame size: type(1) + channel(2) + size(4) + frame-end(1); a frame of
    /// payload size <c>n</c> occupies <c>n + FrameOverhead</c> bytes on the wire.</summary>
    public const int FrameOverhead = 8;
}

/// <summary>
/// Protocol state returned by <see cref="FrameDecoder.ReadFrames"/>: the bookkeeping
/// mode of the decode pass. <see cref="NeedMore"/> means the remaining buffer holds no
/// complete frame (it may be empty or partial); feed more bytes and call again.
/// <see cref="Error"/> is part of the protocol-state contract but is never returned by
/// this implementation: every hard wire error the decoder detects is surfaced by
/// throwing <see cref="AmqpProtocolException"/> instead, so connection code handles
/// fatal framing failures through a single channel.
/// </summary>
public enum DecodeMode
{
    NeedMore,
    Error,
}

/// <summary>
/// Frame-level write helpers: each one assembles complete wire frames (frame type,
/// channel, payload-size field, payload, frame-end) onto a <see cref="ByteWriter"/>,
/// the way rabbit_writer produces outgoing frames. Payloads are built with the Task-1
/// primitives (or passed in pre-encoded) and the size field counts payload bytes only.
/// </summary>
public static class FrameWriter
{
    /// <summary>
    /// Writes a method frame whose payload the caller prepared: the argument bytes must
    /// be passed in <paramref name="args"/>; the frame payload is class-id, method-id and
    /// those bytes, in that order, and empty <paramref name="args"/> yields the 4-byte
    /// no-argument method payload.
    /// </summary>
    public static void WriteMethod(ref ByteWriter writer, ushort channel, ushort classId, ushort methodId, ReadOnlySpan<byte> args)
    {
        ArgumentNullException.ThrowIfNull(writer);
        var payload = new ByteWriter();
        payload.WriteUShort(classId);
        payload.WriteUShort(methodId);
        payload.WriteRaw(args);
        WriteFramed(ref writer, AmqpFrame.FrameMethod, channel, payload.ToArray());
    }

    /// <summary>Writes the content-header frame carrying a basic-class header
    /// (class 60): the payload content is produced by <see cref="ContentHeaderCodec.Write"/>.</summary>
    public static void WriteContentHeader(ref ByteWriter writer, ushort channel, ushort classId, long bodySize, ContentProperties properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var payload = new ByteWriter();
        ContentHeaderCodec.Write(payload, classId, bodySize, properties);
        WriteFramed(ref writer, AmqpFrame.FrameHeader, channel, payload.ToArray());
    }

    /// <summary>
    /// Writes content body frames, splitting a body larger than one frame just like the
    /// RabbitMQ clients do: the payload cap per frame is <c>frameMax - FrameOverhead</c>
    /// (AMQCommand.EMPTY_FRAME_SIZE = 8), so a split frame never exceeds the negotiated
    /// frame-max in wire size. A frame-max of 0 means uncapped (one frame); an empty body
    /// produces no frames at all — the content header carries it alone.
    /// </summary>
    public static void WriteBody(ref ByteWriter writer, ushort channel, ReadOnlySpan<byte> body, uint frameMax)
    {
        ArgumentNullException.ThrowIfNull(writer);
        long payloadPerFrame = frameMax == 0
            ? long.MaxValue
            : Math.Clamp((long)frameMax - AmqpFrame.FrameOverhead, min: 1, max: int.MaxValue);
        int offset = 0;
        while (offset < body.Length)
        {
            int chunk = (int)Math.Min(payloadPerFrame, body.Length - offset);
            WriteFramed(ref writer, AmqpFrame.FrameBody, channel, body.Slice(offset, chunk).ToArray());
            offset += chunk;
        }
    }

    /// <summary>Appends one complete frame: type octet, channel, 4-byte payload size,
    /// the payload, and the 0xCE frame-end.</summary>
    private static void WriteFramed(ref ByteWriter writer, byte type, ushort channel, byte[] payload)
    {
        writer.WriteOctet(type);
        writer.WriteUShort(channel);
        writer.WriteLong(payload.Length);
        writer.WriteRaw(payload);
        writer.WriteOctet(AmqpFrame.FrameEnd);
    }
}
