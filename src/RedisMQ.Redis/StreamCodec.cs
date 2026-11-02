using System.Text;

namespace RedisMQ.Redis;

/// <summary>
/// Encodes and decodes a single enqueue operation's stream fields. Bodies round-trip
/// byte-exact (stored as base64 so arbitrary binary survives Redis string handling).
/// Content properties are stored as their UTF-8 JSON form.
/// </summary>
public sealed record EnvelopeRecord(
    ulong EnvelopeId,
    string Exchange,
    string RoutingKey,
    byte[] Body,
    string PropertiesJson);

public static class StreamCodec
{
    /// <summary>Encodes an enqueue into stream field/value pairs.</summary>
    public static (string Field, string Value)[] EncodeEnqueue(EnvelopeRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        byte[] body = record.Body;
        ArgumentNullException.ThrowIfNull(body);
        return new[]
        {
            (RedisKeys.QueueField.EnvelopeId, record.EnvelopeId.ToString()),
            (RedisKeys.QueueField.Exchange, record.Exchange ?? string.Empty),
            (RedisKeys.QueueField.RoutingKey, record.RoutingKey ?? string.Empty),
            (RedisKeys.QueueField.Body, Convert.ToBase64String(body)),
            (RedisKeys.QueueField.Properties, record.PropertiesJson ?? string.Empty),
        };
    }

    /// <summary>Decodes an enqueue from stream field/value pairs.</summary>
    public static EnvelopeRecord DecodeEnqueue(IReadOnlyList<(string Field, string Value)> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        var dict = fields.ToDictionary(kv => kv.Field, kv => kv.Value, StringComparer.Ordinal);

        ulong envelopeId = ulong.TryParse(dict.GetValueOrDefault(RedisKeys.QueueField.EnvelopeId), out var id)
            ? id
            : 0;
        string exchange = dict.GetValueOrDefault(RedisKeys.QueueField.Exchange) ?? string.Empty;
        string routingKey = dict.GetValueOrDefault(RedisKeys.QueueField.RoutingKey) ?? string.Empty;
        byte[] body = TryGetBase64(dict, RedisKeys.QueueField.Body, out var bytes)
            ? bytes ?? []
            : [];
        string propertiesJson = dict.GetValueOrDefault(RedisKeys.QueueField.Properties) ?? string.Empty;

        return new EnvelopeRecord(envelopeId, exchange, routingKey, body, propertiesJson);
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
