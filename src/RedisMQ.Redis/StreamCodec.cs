using System.Globalization;
using RedisMQ.Broker.Codec;

namespace RedisMQ.Redis;

/// <summary>
/// One durable queue entry: the envelope identity, routing origin, body, AMQP-wire
/// content properties, and the delivery metadata the mirror needs to replay the
/// message faithfully. Bodies and properties round-trip byte-exact; properties are
/// stored in their AMQP content-header wire form because a <see cref="ContentProperties"/>
/// carries a <see cref="FieldTable"/> that JSON cannot represent.
/// </summary>
public sealed record EnvelopeRecord(
    ulong EnvelopeId,
    string Exchange,
    string RoutingKey,
    byte[] Body,
    ContentProperties Properties,
    DateTimeOffset? ExpiresAt,
    int DeliveryCount,
    bool Redelivered);

public static class StreamCodec
{
    private const ushort BasicContentClassId = 60;

    /// <summary>Encodes an enqueue into stream field/value pairs.</summary>
    public static (string Field, string Value)[] EncodeEnqueue(EnvelopeRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        byte[] body = record.Body;
        ArgumentNullException.ThrowIfNull(body);
        ContentProperties properties = record.Properties ?? new ContentProperties();

        var headerWriter = new ByteWriter();
        ContentHeaderCodec.Write(headerWriter, BasicContentClassId, body.LongLength, properties);

        var fields = new List<(string Field, string Value)>
        {
            (RedisKeys.QueueField.EnvelopeId, record.EnvelopeId.ToString(CultureInfo.InvariantCulture)),
            (RedisKeys.QueueField.Exchange, record.Exchange ?? string.Empty),
            (RedisKeys.QueueField.RoutingKey, record.RoutingKey ?? string.Empty),
            (RedisKeys.QueueField.Body, Convert.ToBase64String(body)),
            (RedisKeys.QueueField.Properties, Convert.ToBase64String(headerWriter.ToArray())),
            (RedisKeys.QueueField.DeliveryCount, record.DeliveryCount.ToString(CultureInfo.InvariantCulture)),
            (RedisKeys.QueueField.Redelivered, record.Redelivered ? "1" : "0"),
        };

        if (record.ExpiresAt is DateTimeOffset expiresAt)
        {
            fields.Add((RedisKeys.QueueField.ExpiresAt, expiresAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)));
        }

        return [.. fields];
    }

    /// <summary>Decodes an enqueue from stream field/value pairs.</summary>
    public static EnvelopeRecord DecodeEnqueue(IReadOnlyList<(string Field, string Value)> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        var dict = fields.ToDictionary(kv => kv.Field, kv => kv.Value, StringComparer.Ordinal);

        ulong envelopeId = ulong.TryParse(
            dict.GetValueOrDefault(RedisKeys.QueueField.EnvelopeId),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var id)
            ? id
            : 0;

        string exchange = dict.GetValueOrDefault(RedisKeys.QueueField.Exchange) ?? string.Empty;
        string routingKey = dict.GetValueOrDefault(RedisKeys.QueueField.RoutingKey) ?? string.Empty;
        byte[] body = TryGetBase64(dict, RedisKeys.QueueField.Body, out var bytes)
            ? bytes ?? []
            : [];
        ContentProperties properties = TryDecodeProperties(dict, out var decoded) ? decoded : new ContentProperties();
        DateTimeOffset? expiresAt = TryDecodeExpiry(dict, out var expiry) ? expiry : null;
        int deliveryCount = int.TryParse(
            dict.GetValueOrDefault(RedisKeys.QueueField.DeliveryCount),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var count) && count > 0
            ? count
            : 0;
        bool redelivered = dict.GetValueOrDefault(RedisKeys.QueueField.Redelivered) == "1";

        return new EnvelopeRecord(envelopeId, exchange, routingKey, body, properties, expiresAt, deliveryCount, redelivered);
    }

    private static bool TryDecodeProperties(IReadOnlyDictionary<string, string> dict, out ContentProperties properties)
    {
        properties = new ContentProperties();
        if (!TryGetBase64(dict, RedisKeys.QueueField.Properties, out byte[]? encoded) || encoded is null || encoded.Length == 0)
        {
            return false;
        }

        try
        {
            var reader = new ByteReader(encoded);
            properties = ContentHeaderCodec.Read(ref reader).Properties;
            return true;
        }
        catch (Exception ex) when (ex is FormatException or AmqpDecodeException or ArgumentException)
        {
            // Replay must never crash on a corrupt mirror entry: absent or damaged
            // properties degrade to the empty set.
            properties = new ContentProperties();
            return false;
        }
    }

    private static bool TryDecodeExpiry(IReadOnlyDictionary<string, string> dict, out DateTimeOffset expiresAt)
    {
        expiresAt = default;
        if (!long.TryParse(
                dict.GetValueOrDefault(RedisKeys.QueueField.ExpiresAt),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out long unixMilliseconds))
        {
            return false;
        }

        try
        {
            expiresAt = DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    private static bool TryGetBase64(IReadOnlyDictionary<string, string> dict, string key, out byte[]? bytes)
    {
        bytes = null;
        if (!dict.TryGetValue(key, out var value) || string.IsNullOrEmpty(value))
        {
            return false;
        }

        try
        {
            bytes = Convert.FromBase64String(value);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }
}
