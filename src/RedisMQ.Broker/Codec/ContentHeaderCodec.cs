namespace RedisMQ.Broker.Codec;

/// <summary>
/// A parsed content header: the long-uint content it describes (class id, the unused
/// weight field, the total body size in bytes) plus the properties carried in the
/// property list.
/// </summary>
public sealed record ContentHeader(ushort ClassId, ushort Weight, long BodySize, ContentProperties Properties);

/// <summary>
/// The 0-9-1 basic-class content properties, declared in exact wire order: each nullable
/// field maps to one property-flag bit, and the record's parameter order is the order the
/// values travel on the wire (MSB-first flag word, first property at bit 15).
/// </summary>
public sealed record ContentProperties(
    string? ContentType = null,
    string? ContentEncoding = null,
    FieldTable? Headers = null,
    byte? DeliveryMode = null,
    byte? Priority = null,
    string? CorrelationId = null,
    string? ReplyTo = null,
    string? Expiration = null,
    string? MessageId = null,
    DateTimeOffset? Timestamp = null,
    string? Type = null,
    string? UserId = null,
    string? AppId = null,
    string? ClusterId = null);

/// <summary>
/// Codec for the basic-class (0-9-1 class 60) content header payload — this is the
/// payload content of a frame type 2, without the frame itself (see
/// <see cref="FrameWriter.WriteContentHeader"/> for the framing). The property list is
/// governed by the trailing stop-bit rule: a property-flag word holds the present
/// properties as bits, ordered from the most significant bit, and its least significant
/// bit marks whether another flag word (extension properties) follows. Standard basic
/// properties fill bits 15..2 of the first word, so a first word with a clear low bit is
/// a complete property list
/// </summary>
public static class ContentHeaderCodec
{
    private const ushort BasicClassId = 60;

    // Property-flag masks, first property (content-type) at the most significant bit.
    private const ushort FlagContentType = 0x8000;
    private const ushort FlagContentEncoding = 0x4000;
    private const ushort FlagHeaders = 0x2000;
    private const ushort FlagDeliveryMode = 0x1000;
    private const ushort FlagPriority = 0x0800;
    private const ushort FlagCorrelationId = 0x0400;
    private const ushort FlagReplyTo = 0x0200;
    private const ushort FlagExpiration = 0x0100;
    private const ushort FlagMessageId = 0x0080;
    private const ushort FlagTimestamp = 0x0040;
    private const ushort FlagType = 0x0020;
    private const ushort FlagUserId = 0x0010;
    private const ushort FlagAppId = 0x0008;
    private const ushort FlagClusterId = 0x0004;
    private const ushort FlagContinuation = 0x0001;

    /// <summary>Parses a basic-class content header payload, positioned at its class-id
    /// field and consuming all of it.</summary>
    public static ContentHeader Read(ref ByteReader reader)
    {
        ushort classId = reader.ReadUShort();
        if (classId != BasicClassId)
        {
            throw new AmqpDecodeException(
                $"Content class {classId} is not supported (only basic class {BasicClassId} carries content)");
        }

        ushort weight = reader.ReadUShort();
        long bodySize = reader.ReadLongLong();
        ushort flags = reader.ReadUShort();
        if ((flags & FlagContinuation) != 0)
        {
            throw new AmqpDecodeException(
                "Content header property flags use the continuation bit, so a second flag " +
                "word of extension properties follows; those are not part of the standard " +
                "basic property list this codec speaks");
        }

        string? contentType = FlagSet(flags, FlagContentType) ? reader.ReadShortstr() : null;
        string? contentEncoding = FlagSet(flags, FlagContentEncoding) ? reader.ReadShortstr() : null;
        FieldTable? headers = ReadHeaders(ref reader, flags);
        byte? deliveryMode = FlagSet(flags, FlagDeliveryMode) ? reader.ReadOctet() : null;
        byte? priority = FlagSet(flags, FlagPriority) ? reader.ReadOctet() : null;
        string? correlationId = FlagSet(flags, FlagCorrelationId) ? reader.ReadShortstr() : null;
        string? replyTo = FlagSet(flags, FlagReplyTo) ? reader.ReadShortstr() : null;
        string? expiration = FlagSet(flags, FlagExpiration) ? reader.ReadShortstr() : null;
        string? messageId = FlagSet(flags, FlagMessageId) ? reader.ReadShortstr() : null;
        DateTimeOffset? timestamp = FlagSet(flags, FlagTimestamp) ? reader.ReadTimestamp() : null;
        string? type = FlagSet(flags, FlagType) ? reader.ReadShortstr() : null;
        string? userId = FlagSet(flags, FlagUserId) ? reader.ReadShortstr() : null;
        string? appId = FlagSet(flags, FlagAppId) ? reader.ReadShortstr() : null;
        string? clusterId = FlagSet(flags, FlagClusterId) ? reader.ReadShortstr() : null;

        return new ContentHeader(
            classId,
            weight,
            bodySize,
            new ContentProperties(
                contentType, contentEncoding, headers, deliveryMode, priority,
                correlationId, replyTo, expiration, messageId, timestamp,
                type, userId, appId, clusterId));
    }

    /// <summary>Writes the basic-class content header payload: class-id, the
    /// (always zero) weight field, the long-uint body size, then the property flag
    /// word with its values in wire order.</summary>
    public static void Write(ByteWriter writer, ushort classId, long bodySize, ContentProperties properties)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(properties);
        if (classId != BasicClassId)
        {
            throw new AmqpEncodeException(
                $"Content class {classId} is not supported (only basic class {BasicClassId} carries content)");
        }

        ushort flags = FlagsOf(properties);
        writer.WriteUShort(classId);
        writer.WriteUShort(0); // the weight field is unused and always zero in 0-9-1
        writer.WriteLongLong(bodySize);
        writer.WriteUShort(flags);

        if (properties.ContentType is not null)
        {
            writer.WriteShortstr(properties.ContentType);
        }

        if (properties.ContentEncoding is not null)
        {
            writer.WriteShortstr(properties.ContentEncoding);
        }

        if (properties.Headers is not null)
        {
            writer.WriteFieldTable(properties.Headers);
        }

        if (properties.DeliveryMode is not null)
        {
            writer.WriteOctet(properties.DeliveryMode.Value);
        }

        if (properties.Priority is not null)
        {
            writer.WriteOctet(properties.Priority.Value);
        }

        if (properties.CorrelationId is not null)
        {
            writer.WriteShortstr(properties.CorrelationId);
        }

        if (properties.ReplyTo is not null)
        {
            writer.WriteShortstr(properties.ReplyTo);
        }

        if (properties.Expiration is not null)
        {
            writer.WriteShortstr(properties.Expiration);
        }

        if (properties.MessageId is not null)
        {
            writer.WriteShortstr(properties.MessageId);
        }

        if (properties.Timestamp is not null)
        {
            writer.WriteTimestamp(properties.Timestamp.Value);
        }

        if (properties.Type is not null)
        {
            writer.WriteShortstr(properties.Type);
        }

        if (properties.UserId is not null)
        {
            writer.WriteShortstr(properties.UserId);
        }

        if (properties.AppId is not null)
        {
            writer.WriteShortstr(properties.AppId);
        }

        if (properties.ClusterId is not null)
        {
            writer.WriteShortstr(properties.ClusterId);
        }
    }

    private static bool FlagSet(ushort flags, ushort flag) => (flags & flag) != 0;

    private static ushort FlagsOf(ContentProperties properties) => (ushort)(
        (properties.ContentType is null ? 0 : FlagContentType)
        | (properties.ContentEncoding is null ? 0 : FlagContentEncoding)
        | (properties.Headers is null ? 0 : FlagHeaders)
        | (properties.DeliveryMode is null ? 0 : FlagDeliveryMode)
        | (properties.Priority is null ? 0 : FlagPriority)
        | (properties.CorrelationId is null ? 0 : FlagCorrelationId)
        | (properties.ReplyTo is null ? 0 : FlagReplyTo)
        | (properties.Expiration is null ? 0 : FlagExpiration)
        | (properties.MessageId is null ? 0 : FlagMessageId)
        | (properties.Timestamp is null ? 0 : FlagTimestamp)
        | (properties.Type is null ? 0 : FlagType)
        | (properties.UserId is null ? 0 : FlagUserId)
        | (properties.AppId is null ? 0 : FlagAppId)
        | (properties.ClusterId is null ? 0 : FlagClusterId));

    private static FieldTable? ReadHeaders(ref ByteReader reader, ushort flags)
    {
        if (!FlagSet(flags, FlagHeaders))
        {
            return null;
        }

        // The table was already framed (the whole content header arrived inside a
        // decoded frame), so a table that cannot be fully read is malformed content.
        if (!reader.TryReadFieldTable(out FieldTable table))
        {
            throw new AmqpDecodeException("Content header table value is truncated");
        }

        return table;
    }
}
