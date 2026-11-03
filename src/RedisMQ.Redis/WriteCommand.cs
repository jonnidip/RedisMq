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
    public int? MaxLength { get; init; }

    /// <summary>Binding arguments (headers exchanges match on them). Encoded as the
    /// AMQP field-table wire form so arbitrary <see cref="FieldValue"/> payloads
    /// round-trip byte-exactly.</summary>
    public FieldTable? Arguments { get; init; }

    // Consumer payload (RegisterConsumer / UnregisterConsumer).
    public string? ConsumerTag { get; init; }

    // Enqueue / Requeue payload.
    public ulong EnvelopeId { get; init; }
    public byte[]? Body { get; init; }
    public ContentProperties? Properties { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public int DeliveryCount { get; init; }
    public bool Redelivered { get; init; }

    /// <summary>Completes when the drainer has written (or failed to write) this
    /// command. Protocol-affine: never serialized and never replayed.</summary>
    public TaskCompletionSource<bool>? Completion { get; init; }

    /// <summary>True when the producer reserved one drainer slot for this command,
    /// so the drainer releases it when the command finishes. Never serialized.</summary>
    public bool ReservedDrainerSlot { get; init; }

    // ---------- factory methods ----------

    public static WriteCommand DeclareExchange(string vhost, string name, string type) => new()
    {
        Kind = WriteCommandKind.DeclareExchange,
        Vhost = vhost,
        Name = name,
        Type = type,
    };

    public static WriteCommand DeclareQueue(string vhost, string name, bool durable, bool exclusive, bool autoDelete, int? maxLength = null) => new()
    {
        Kind = WriteCommandKind.DeclareQueue,
        Vhost = vhost,
        Name = name,
        Durable = durable,
        Exclusive = exclusive,
        AutoDelete = autoDelete,
        MaxLength = maxLength,
    };

    public static WriteCommand Bind(string vhost, string exchange, string queue, string routingKey, FieldTable? arguments = null) => new()
    {
        Kind = WriteCommandKind.Bind,
        Vhost = vhost,
        Exchange = exchange,
        Queue = queue,
        RoutingKey = routingKey,
        Arguments = arguments,
    };

    public static WriteCommand Unbind(string vhost, string exchange, string queue, string routingKey) => new()
    {
        Kind = WriteCommandKind.Unbind,
        Vhost = vhost,
        Exchange = exchange,
        Queue = queue,
        RoutingKey = routingKey,
    };

    public static WriteCommand Enqueue(string vhost, string queue, ulong envelopeId, string exchange, string routingKey, byte[] body, ContentProperties? properties, DateTimeOffset? expiresAt = null, int deliveryCount = 0) => new()
    {
        Kind = WriteCommandKind.Enqueue,
        Vhost = vhost,
        Queue = queue,
        EnvelopeId = envelopeId,
        Exchange = exchange,
        RoutingKey = routingKey,
        Body = body,
        Properties = properties,
        ExpiresAt = expiresAt,
        DeliveryCount = deliveryCount,
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

    public static WriteCommand DeleteQueue(string vhost, string queue) => new()
    {
        Kind = WriteCommandKind.DeleteQueue,
        Vhost = vhost,
        Queue = queue,
    };

    public static WriteCommand Requeue(string vhost, string queue, ulong envelopeId, string exchange, string routingKey, byte[] body, ContentProperties? properties, bool redelivered, DateTimeOffset? expiresAt = null, int deliveryCount = 0) => new()
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
        ExpiresAt = expiresAt,
        DeliveryCount = deliveryCount,
    };

    // ---------- stream field serialization ----------

    /// <summary>Serializes this command to stream field/value pairs.</summary>
    public (string Field, string Value)[] ToStreamFields()    {
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
                if (MaxLength is int maxLength)
                {
                    fields.Add(("x-max-length", maxLength.ToString()));
                }
                break;

            case WriteCommandKind.Bind or WriteCommandKind.Unbind:
                fields.Add(("exchange", Exchange ?? string.Empty));
                fields.Add(("queue", Queue ?? string.Empty));
                fields.Add(("routing-key", RoutingKey ?? string.Empty));
                if (Arguments is { Count: > 0 })
                {
                    fields.Add(("arguments", EncodeArguments(Arguments)));
                }
                break;

            case WriteCommandKind.RegisterConsumer or WriteCommandKind.UnregisterConsumer:
                fields.Add(("queue", Queue ?? string.Empty));
                fields.Add(("consumer-tag", ConsumerTag ?? string.Empty));
                break;

            case WriteCommandKind.DeleteQueue:
                fields.Add(("queue", Queue ?? string.Empty));
                break;

            case WriteCommandKind.Enqueue or WriteCommandKind.Requeue:
                fields.Add(("queue", Queue ?? string.Empty));
                fields.AddRange(StreamCodec.EncodeEnqueue(new EnvelopeRecord(
                    EnvelopeId, Exchange ?? string.Empty, RoutingKey ?? string.Empty,
                    Body ?? [], Properties ?? new ContentProperties(), ExpiresAt, DeliveryCount, Redelivered)));
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

            return kind == WriteCommandKind.Enqueue
                ? Enqueue(vhost, queue, record.EnvelopeId, record.Exchange, record.RoutingKey, record.Body,
                    record.Properties, record.ExpiresAt, record.DeliveryCount)
                : Requeue(vhost, queue, record.EnvelopeId, record.Exchange, record.RoutingKey, record.Body,
                    record.Properties, record.Redelivered, record.ExpiresAt, record.DeliveryCount);
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
                bool.TryParse(dict.GetValueOrDefault("auto-delete"), out var a) && a,
                int.TryParse(dict.GetValueOrDefault("x-max-length"), out var ml) ? ml : null),

            WriteCommandKind.Bind => Bind(
                vhost,
                dict.GetValueOrDefault("exchange") ?? string.Empty,
                dict.GetValueOrDefault("queue") ?? string.Empty,
                dict.GetValueOrDefault("routing-key") ?? string.Empty,
                DecodeArguments(dict.GetValueOrDefault("arguments"))),

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

            WriteCommandKind.DeleteQueue => DeleteQueue(
                vhost,
                dict.GetValueOrDefault("queue") ?? string.Empty),

            _ => throw new InvalidOperationException($"Unknown write command kind: {kind}"),
        };
    }

    /// <summary>Encodes a field table in its AMQP 0-9-1 wire form (base64 for the
    /// stream value), so every FieldValue kind round-trips exactly.</summary>
    private static string EncodeArguments(FieldTable table)
    {
        var writer = new ByteWriter();
        writer.WriteFieldTable(table);
        return Convert.ToBase64String(writer.ToArray());
    }

    internal static FieldTable? DecodeArguments(string? encoded)
    {
        if (string.IsNullOrEmpty(encoded))
        {
            return null;
        }

        try
        {
            var reader = new ByteReader(Convert.FromBase64String(encoded));
            return reader.TryReadFieldTable(out FieldTable table) ? table : null;
        }
        catch (FormatException)
        {
            return null;
        }
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
    DeleteQueue,
}
