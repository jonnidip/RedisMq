using RedisMQ.Broker.Protocol;

namespace RedisMQ.Broker.Codec;

/// <summary>
/// One AMQP method decoded — or being built to encode — through its registry schema.
/// Field values are boxed against the schema's wire types: short fields carry short,
/// long fields int, longlong fields long, octet fields byte, bit fields bool,
/// shortstr fields string, longstr fields byte[], table fields FieldTable and timestamp
/// fields DateTimeOffset; field names are the fixture's hyphenated wire names.
/// Unset fields encode as the zero form (0, false, "", empty blob/table) so a partially
/// populated method always produces decode-safe wire.
/// </summary>
public sealed class AmqpMethod
{
    private readonly Dictionary<string, object?> _values;

    /// <summary>Builds a method with every schema field unset.</summary>
    public AmqpMethod(MethodSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        Schema = schema;
        _values = new Dictionary<string, object?>(schema.Fields.Count, StringComparer.Ordinal);
        foreach (var field in schema.Fields)
        {
            _values.Add(field.Name, null);
        }
    }

    /// <summary>The schema this method's shape follows.</summary>
    public MethodSchema Schema { get; }

    public ushort ClassId => Schema.ClassId;

    public ushort MethodId => Schema.MethodId;

    /// <summary>The field's boxed wire value (null when unset).</summary>
    /// <exception cref="KeyNotFoundException">The name is not part of the schema.</exception>
    public object? Get(string name) => _values[name];

    /// <summary>Sets a field's boxed wire value.</summary>
    /// <exception cref="KeyNotFoundException">The name is not part of the schema.</exception>
    public void Set(string name, object? value)
    {
        if (!_values.ContainsKey(name))
        {
            throw new KeyNotFoundException($"Method '{Schema.Name}' has no field '{name}'");
        }

        _values[name] = value;
    }

    public override string ToString() => $"{Schema.Name} ({Schema.Fields.Count} field(s))";
}

/// <summary>
/// Encodes/decodes the argument section of method frames through
/// <see cref="MethodRegistry"/> (the class-id/method-id pair precedes the arguments in
/// the frame payload itself). Reads must consume a fully-received method payload; every
/// truncation throws <see cref="AmqpDecodeException"/>. Bit groups are written with one
/// <see cref="ByteWriter.WriteBits(bool[])"/> call each — AMQP packs a method's run of
/// consecutive bit fields into whole octets, LSB-first, one group at a time.
/// </summary>
public static class MethodCodec
{
    /// <summary>
    /// Reads all schema fields in order into a new <see cref="AmqpMethod"/>. The reader
    /// must be positioned at the first argument (class-id/method-id already consumed).
    /// </summary>
    public static AmqpMethod ReadMethod(ref ByteReader reader, MethodSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var method = new AmqpMethod(schema);
        foreach (var field in schema.Fields)
        {
            method.Set(field.Name, ReadField(ref reader, schema, field));
        }

        return method;
    }

    /// <summary>
    /// Writes the full method frame payload: class-id, method-id, then every schema field
    /// in order. Consecutive bit fields are collected into one WriteBits group; values
    /// must match the wire field's CLR type (or be unset — the zero form) else
    /// <see cref="AmqpEncodeException"/>.
    /// </summary>
    public static void WriteMethod(ref ByteWriter writer, in AmqpMethod method)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(method);
        writer.WriteUShort(method.ClassId);
        writer.WriteUShort(method.MethodId);
        var bits = new List<bool>(method.Schema.Fields.Count);
        foreach (var field in method.Schema.Fields)
        {
            if (field.Type is WireType.Bit or WireType.PackedBit or WireType.DependingBit)
            {
                bits.Add(AsBool(field, method.Get(field.Name)));
                continue;
            }

            if (bits.Count > 0)
            {
                // A non-bit field ends the current bit run: flush it as ONE group, which
                // pads to the octet boundary, so the next field starts byte-aligned.
                writer.WriteBits(bits.ToArray());
                bits.Clear();
            }

            WriteField(ref writer, field, method.Get(field.Name));
        }

        if (bits.Count > 0)
        {
            writer.WriteBits(bits.ToArray());
        }
    }

    private static object? ReadField(ref ByteReader reader, MethodSchema schema, AmqpField field)
    {
        switch (field.Type)
        {
            case WireType.Octet:
                return reader.ReadOctet();
            case WireType.Short:
                return reader.ReadShort();
            case WireType.Long:
                return reader.ReadLong();
            case WireType.LongLong:
                return reader.ReadLongLong();
            case WireType.Shortstr:
                return reader.ReadShortstr();
            case WireType.Longstr:
                return reader.ReadLongstr();
            case WireType.Table:
                return reader.TryReadFieldTable(out var table)
                    ? table
                    : throw new AmqpDecodeException(
                        $"Field '{field.Name}' of {schema.Name} declares a field table longer than the remaining bytes");
            case WireType.Timestamp:
                return reader.ReadTimestamp();
            case WireType.Bit:
            case WireType.PackedBit:
            case WireType.DependingBit:
                // Lone bits still occupy one octet; runs chain octet to octet inside the reader.
                return reader.ReadBit();
            default:
                throw new AmqpDecodeException($"No reader for {field.Type} on field '{field.Name}' of {schema.Name}");
        }
    }

    private static void WriteField(ref ByteWriter writer, AmqpField field, object? value)
    {
        switch (field.Type)
        {
            case WireType.Octet:
                writer.WriteOctet(AsByte(field, value));
                break;
            case WireType.Short:
                writer.WriteShort(AsShort(field, value));
                break;
            case WireType.Long:
                writer.WriteLong(AsInt(field, value));
                break;
            case WireType.LongLong:
                writer.WriteLongLong(AsLong(field, value));
                break;
            case WireType.Shortstr:
                writer.WriteShortstr(AsString(field, value));
                break;
            case WireType.Longstr:
                writer.WriteLongstr(AsBlob(field, value));
                break;
            case WireType.Table:
                writer.WriteFieldTable(AsTable(field, value));
                break;
            case WireType.Timestamp:
                writer.WriteTimestamp(AsTimestamp(field, value));
                break;
            default:
                throw new AmqpEncodeException($"No writer for {field.Type} on field '{field.Name}'");
        }
    }

    // ---------- boxed-value casts: strict about the CLR type, permissive about null ----------

    private static byte AsByte(AmqpField field, object? value)
    {
        if (value is null)
        {
            return 0;
        }

        return value is byte v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a byte, got {Display(value)}");
    }

    private static short AsShort(AmqpField field, object? value)
    {
        if (value is null)
        {
            return 0;
        }

        return value is short v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a short, got {Display(value)}");
    }

    private static int AsInt(AmqpField field, object? value)
    {
        if (value is null)
        {
            return 0;
        }

        return value is int v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants an int, got {Display(value)}");
    }

    private static long AsLong(AmqpField field, object? value)
    {
        if (value is null)
        {
            return 0;
        }

        return value is long v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a long, got {Display(value)}");
    }

    private static bool AsBool(AmqpField field, object? value)
    {
        if (value is null)
        {
            return false;
        }

        return value is bool v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a bool, got {Display(value)}");
    }

    private static string AsString(AmqpField field, object? value)
    {
        if (value is null)
        {
            return string.Empty;
        }

        return value is string v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a string, got {Display(value)}");
    }

    private static byte[] AsBlob(AmqpField field, object? value)
    {
        if (value is null)
        {
            return Array.Empty<byte>();
        }

        return value is byte[] v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a byte[], got {Display(value)}");
    }

    private static FieldTable AsTable(AmqpField field, object? value)
    {
        if (value is null)
        {
            return new FieldTable();
        }

        return value is FieldTable v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a FieldTable, got {Display(value)}");
    }

    private static DateTimeOffset AsTimestamp(AmqpField field, object? value)
    {
        if (value is null)
        {
            return default;
        }

        return value is DateTimeOffset v ? v : throw new AmqpEncodeException($"Field '{field.Name}' wants a DateTimeOffset, got {Display(value)}");
    }

    private static string Display(object value) => value.GetType().Name;
}
