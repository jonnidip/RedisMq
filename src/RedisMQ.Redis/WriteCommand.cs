using System.Text.Json;
using RedisMQ.Broker.Codec;

namespace RedisMQ.Redis;

/// <summary>
/// One mutating operation carried by the drainer channel. The protocol thread
/// enqueues these (synchronously, no Redis call); a single background drainer
/// writes them to Redis Streams. Each command carries enough payload to replay
/// against <see cref="Memory.InMemoryEngine"/> on startup and to append to Redis.
/// </summary>
public sealed record WriteCommand
{
    public WriteCommandKind Kind { get; init; }

    // Topology payload (DeclareExchange / DeclareQueue / Bind / Unbind).
    public string? Vhost { get; init; }
    public string? Name { get; init; }
    public string? Exchange { get; init; }
    public string? Queue { get; init; }
    public string? RoutingKey { get; init; }
    public bool Durable { get; init; }
    public bool Exclusive { get; init; }
    public bool AutoDelete { get; init; }
    public string? Type { get; init; }

    // Consumer payload (RegisterConsumer / UnregisterConsumer).
    public string? ConsumerTag { get; init; }

    // Enqueue / Requeue payload.
    public ulong EnvelopeId { get; init; }
    public byte[]? Body { get; init; }
    public ContentProperties? Properties { get; init; }
    public bool Redelivered { get; init; }

    // ---------- factory methods ----------

    public static WriteCommand DeclareExchange(string vhost, string name, string type) => new()
    {
        Kind = WriteCommandKind.DeclareExchange,
        Vhost = vhost,
        Name = name,
        Type = type,
    };

    public static WriteCommand DeclareQueue(string vhost, string name, bool durable, bool exclusive, bool autoDelete) => new()
    {
        Kind = WriteCommandKind.DeclareQueue,
        Vhost = vhost,
        Name = name,
        Durable = durable,
        Exclusive = exclusive,
        AutoDelete = autoDelete,
    };

    public static WriteCommand Bind(string vhost, string exchange, string queue, string routingKey) => new()
    {
        Kind = WriteCommandKind.Bind,
        Vhost = vhost,
        Exchange = exchange,
        Queue = queue,
        RoutingKey = routingKey,
    };

    public static WriteCommand Unbind(string vhost, string exchange, string queue, string routingKey) => new()
    {
        Kind = WriteCommandKind.Unbind,
        Vhost = vhost,
        Exchange = exchange,
        Queue = queue,
        RoutingKey = routingKey,
    };

    public static WriteCommand Enqueue(string vhost, string queue, ulong envelopeId, string exchange, string routingKey, byte[] body, ContentProperties? properties) => new()
    {
        Kind = WriteCommandKind.Enqueue,
        Vhost = vhost,
        Queue = queue,
        EnvelopeId = envelopeId,
        Exchange = exchange,
        RoutingKey = routingKey,
        Body = body,
        Properties = properties,
    };

    public static WriteCommand RegisterConsumer(string vhost, string queue, string consumerTag) => new()
    {
        Kind = WriteCommandKind.RegisterConsumer,
        Vhost = vhost,
        Queue = queue,
        ConsumerTag = consumerTag,
    };

    public static WriteCommand UnregisterConsumer(string vhost, string queue, string consumerTag) => new()
    {
        Kind = WriteCommandKind.UnregisterConsumer,
        Vhost = vhost,
        Queue = queue,
        ConsumerTag = consumerTag,
    };

    public static WriteCommand Requeue(string vhost, string queue, ulong envelopeId, string exchange, string routingKey, byte[] body, ContentProperties? properties, bool redelivered) => new()
    {
        Kind = WriteCommandKind.Requeue,
        Vhost = vhost,
        Queue = queue,
        EnvelopeId = envelopeId,
        Exchange = exchange,
        RoutingKey = routingKey,
        Body = body,
        Properties = properties,
        Redelivered = redelivered,
    };

    // ---------- stream field serialization ----------

    /// <summary>Serializes this command to stream field/value pairs.</summary>
    public (string Field, string Value)[] ToStreamFields()
    {
        var fields = new List<(string Field, string Value)>
        {
            (RedisKeys.TopologyField.Kind, Kind.ToString()),
        };

        if (Vhost is not null)
        {
            fields.Add((RedisKeys.TopologyField.Vhost, Vhost));
        }

        switch (Kind)
        {
            case WriteCommandKind.DeclareExchange:
                fields.Add(("exchange", Name ?? string.Empty));
                fields.Add(("type", Type ?? string.Empty));
                break;

            case WriteCommandKind.DeclareQueue:
                fields.Add(("queue", Name ?? string.Empty));
                fields.Add(("durable", Durable ? "1" : "0"));
                fields.Add(("exclusive", Exclusive ? "1" : "0"));
                fields.Add(("auto-delete", AutoDelete ? "1" : "0"));
                break;

            case WriteCommandKind.Bind or WriteCommandKind.Unbind:
                fields.Add(("exchange", Exchange ?? string.Empty));
                fields.Add(("queue", Queue ?? string.Empty));
                fields.Add(("routing-key", RoutingKey ?? string.Empty));
                break;

            case WriteCommandKind.RegisterConsumer or WriteCommandKind.UnregisterConsumer:
                fields.Add(("queue", Queue ?? string.Empty));
                fields.Add(("consumer-tag", ConsumerTag ?? string.Empty));
                break;

            case WriteCommandKind.Enqueue or WriteCommandKind.Requeue:
                fields.Add(("queue", Queue ?? string.Empty));
                fields.AddRange(StreamCodec.EncodeEnqueue(new EnvelopeRecord(
                    EnvelopeId, Exchange ?? string.Empty, RoutingKey ?? string.Empty,
                    Body ?? [], PropertiesJson: Properties is not null ? JsonSerializer.Serialize(Properties) : string.Empty)));
                fields.Add(("redelivered", Redelivered ? "1" : "0"));
                break;
        }

        return fields.ToArray();
    }

    /// <summary>Deserializes a command from stream field/value pairs.</summary>
    public static WriteCommand FromStreamFields(WriteCommandKind kind, IReadOnlyList<(string Field, string Value)> fields)
    {
        var dict = fields.ToDictionary(kv => kv.Field, kv => kv.Value, StringComparer.Ordinal);
        string vhost = dict.GetValueOrDefault(RedisKeys.TopologyField.Vhost) ?? string.Empty;

        // Enqueue and Requeue share the envelope-record encoding plus a queue field.
        if (kind is WriteCommandKind.Enqueue or WriteCommandKind.Requeue)
        {
            var record = StreamCodec.DecodeEnqueue(fields);
            string queue = dict.GetValueOrDefault("queue") ?? string.Empty;
            bool redelivered = bool.TryParse(dict.GetValueOrDefault("redelivered"), out var r) && r;
            ContentProperties? properties = null;
            if (!string.IsNullOrEmpty(record.PropertiesJson))
            {
                properties = JsonSerializer.Deserialize<ContentProperties>(record.PropertiesJson);
            }

            return kind == WriteCommandKind.Enqueue
                ? Enqueue(vhost, queue, record.EnvelopeId, record.Exchange, record.RoutingKey, record.Body, properties)
                : Requeue(vhost, queue, record.EnvelopeId, record.Exchange, record.RoutingKey, record.Body, properties, redelivered);
        }

        return kind switch
        {
            WriteCommandKind.DeclareExchange => DeclareExchange(
                vhost,
                dict.GetValueOrDefault("exchange") ?? string.Empty,
                dict.GetValueOrDefault("type") ?? string.Empty),

            WriteCommandKind.DeclareQueue => DeclareQueue(
                vhost,
                dict.GetValueOrDefault("queue") ?? string.Empty,
                bool.TryParse(dict.GetValueOrDefault("durable"), out var d) && d,
                bool.TryParse(dict.GetValueOrDefault("exclusive"), out var e) && e,
                bool.TryParse(dict.GetValueOrDefault("auto-delete"), out var a) && a),

            WriteCommandKind.Bind => Bind(
                vhost,
                dict.GetValueOrDefault("exchange") ?? string.Empty,
                dict.GetValueOrDefault("queue") ?? string.Empty,
                dict.GetValueOrDefault("routing-key") ?? string.Empty),

            WriteCommandKind.Unbind => Unbind(
                vhost,
                dict.GetValueOrDefault("exchange") ?? string.Empty,
                dict.GetValueOrDefault("queue") ?? string.Empty,
                dict.GetValueOrDefault("routing-key") ?? string.Empty),

            WriteCommandKind.RegisterConsumer => RegisterConsumer(
                vhost,
                dict.GetValueOrDefault("queue") ?? string.Empty,
                dict.GetValueOrDefault("consumer-tag") ?? string.Empty),

            WriteCommandKind.UnregisterConsumer => UnregisterConsumer(
                vhost,
                dict.GetValueOrDefault("queue") ?? string.Empty,
                dict.GetValueOrDefault("consumer-tag") ?? string.Empty),

            _ => throw new InvalidOperationException($"Unknown write command kind: {kind}"),
        };
    }
}

public enum WriteCommandKind
{
    DeclareExchange,
    DeclareQueue,
    Bind,
    Unbind,
    Enqueue,
    RegisterConsumer,
    UnregisterConsumer,
    Requeue,
}
