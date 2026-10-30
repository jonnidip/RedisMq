using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace RedisMQ.Broker.Codec;

/// <summary>
/// AMQP 0-9-1 field table: a mapping of shortstr keys to typed field values.
/// Entry order on the wire is insertion order, so encode/decode round trips are
/// byte-identical; adding a duplicate key replaces its value in place (the .NET
/// reference-client behavior). Lookup is dictionary-based.
/// </summary>
public sealed class FieldTable : IReadOnlyDictionary<string, FieldValue>
{
    private readonly Dictionary<string, FieldValue> _items;
    private readonly List<string> _order;

    public FieldTable()
    {
        _items = new Dictionary<string, FieldValue>(StringComparer.Ordinal);
        _order = new List<string>();
    }

    /// <summary>Builds a table from the given key/value pairs.</summary>
    public FieldTable(IEnumerable<KeyValuePair<string, FieldValue>> items)
        : this()
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach ((string key, FieldValue value) in items)
        {
            Add(key, value);
        }
    }

    /// <summary>Number of distinct keys.</summary>
    public int Count => _items.Count;

    public IEnumerable<string> Keys => _order;

    public IEnumerable<FieldValue> Values
    {
        get
        {
            foreach (string key in _order)
            {
                yield return _items[key];
            }
        }
    }

    /// <summary>
    /// Adds a key/value pair. Adding an existing key replaces its value while keeping
    /// its original position in the wire order (last value wins on the wire too, since
    /// peers that overwrite do so at the first position).
    /// </summary>
    public void Add(string key, FieldValue value)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_items.ContainsKey(key))
        {
            _items[key] = value;
            return;
        }

        _items[key] = value;
        _order.Add(key);
    }

    public bool ContainsKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _items.ContainsKey(key);
    }

    public bool TryGetValue(string key, out FieldValue value) => _items.TryGetValue(key, out value);

    /// <exception cref="KeyNotFoundException">The key is not present.</exception>
    public FieldValue this[string key] => _items[key];

    public IEnumerator<KeyValuePair<string, FieldValue>> GetEnumerator()
    {
        foreach (string key in _order)
        {
            yield return new KeyValuePair<string, FieldValue>(key, _items[key]);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// One typed field-table value: the canonical AMQP 0-9-1 wire letter
/// (<c>t b B s u I l L f d D S A F V T x</c>) plus its decoded payload.
/// Encode switches on the CLR type of <see cref="Raw"/>; decode switches on the wire
/// letter — so the letter is authoritative for re-encoding and round trips are
/// byte-identical. <c>L</c> (signed-64 wire letter) is produced by decoding a
/// <c>ulong</c>-carrying value; <see cref="List"/> yields <c>A</c>, nested
/// <see cref="FieldTable"/> yields <c>F</c>, and <see cref="Null"/> yields <c>V</c>.
/// </summary>
public readonly struct FieldValue : IEquatable<FieldValue>
{
    internal FieldValue(char typeLetter, object? raw)
    {
        TypeLetter = typeLetter;
        Raw = raw;
    }

    /// <summary>The canonical 0-9-1 wire letter carried by this value (or 'V' for null).</summary>
    public char TypeLetter { get; }

    /// <summary>The typed payload: a CLR value whose type matches the letter, or null for 'V'.</summary>
    public object? Raw { get; }

    public static FieldValue From(bool value) => new('t', value);
    public static FieldValue From(sbyte value) => new('b', value);
    public static FieldValue From(byte value) => new('B', value);
    public static FieldValue From(short value) => new('s', value);
    public static FieldValue From(ushort value) => new('u', value);
    public static FieldValue From(int value) => new('I', value);
    public static FieldValue From(long value) => new('l', value);
    public static FieldValue From(float value) => new('f', value);
    public static FieldValue From(double value) => new('d', value);
    public static FieldValue From(decimal value) => new('D', value);
    public static FieldValue From(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new FieldValue('S', value);
    }

    public static FieldValue From(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new FieldValue('x', value);
    }

    public static FieldValue From(DateTimeOffset value) => new('T', value);
    public static FieldValue From(FieldTable value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new FieldValue('F', value);
    }

    /// <summary>Builds an array ('A') value carrying the given items in order.</summary>
    public static FieldValue List(params FieldValue[] items)
    {
        ArgumentNullException.ThrowIfNull(items);
        return new FieldValue('A', items);
    }

    /// <summary>The ('V') absence-of-value entry.</summary>
    public static FieldValue Null { get; } = new('V', null);

    /// <summary>
    /// Letter-aware equality: the letters must match and the payloads must compare equal
    /// (byte arrays by content, nested tables as unordered maps, arrays element-wise in order).
    /// </summary>
    public bool Equals(FieldValue other) => TypeLetter == other.TypeLetter && ValuesEqual(Raw, other.Raw);

    public override bool Equals(object? obj) => obj is FieldValue other && Equals(other);

    public override int GetHashCode()
    {
        var hasher = new System.HashCode();
        hasher.Add(TypeLetter);
        switch (Raw)
        {
            case null:
                break;
            case bool v: hasher.Add(v); break;
            case sbyte v: hasher.Add(v); break;
            case byte v: hasher.Add(v); break;
            case short v: hasher.Add(v); break;
            case ushort v: hasher.Add(v); break;
            case int v: hasher.Add(v); break;
            case long v: hasher.Add(v); break;
            case ulong v: hasher.Add(v); break;
            case float v: hasher.Add(v); break;
            case double v: hasher.Add(v); break;
            case decimal v: hasher.Add(v); break;
            case string v: hasher.Add(v); break;
            case byte[] v: hasher.AddBytes(v); break;
            case DateTimeOffset v: hasher.Add(v.UtcTicks); break;
            case FieldTable v:
                // Tables are unordered maps: combine entry hashes order-insensitively.
                int sum = 0;
                foreach ((string key, FieldValue entry) in v)
                {
                    sum = unchecked(sum + key.GetHashCode() * 31 + entry.GetHashCode());
                }
                hasher.Add(sum);
                break;
            case FieldValue[] items:
                foreach (FieldValue item in items)
                {
                    hasher.Add(item);
                }
                break;
        }
        return hasher.ToHashCode();
    }

    public override string ToString() => Raw switch
    {
        null => $"('{TypeLetter}', null)",
        byte[] bytes => $"('{TypeLetter}', byte[{bytes.Length}])",
        FieldValue[] items => $"('A', FieldValue[{items.Length}])",
        _ => $"('{TypeLetter}', {Raw})",
    };

    private static bool ValuesEqual(object? left, object? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        if (left.GetType() != right.GetType())
        {
            return false;
        }

        switch (left)
        {
            case bool l: return l == (bool)right;
            case sbyte l: return l == (sbyte)right;
            case byte l: return l == (byte)right;
            case short l: return l == (short)right;
            case ushort l: return l == (ushort)right;
            case int l: return l == (int)right;
            case long l: return l == (long)right;
            case ulong l: return l == (ulong)right;
            case float l: return l == (float)right;
            case double l: return l == (double)right;
            case decimal l: return l == (decimal)right;
            case string l: return string.Equals(l, (string)right, StringComparison.Ordinal);
            case byte[] l: return l.AsSpan().SequenceEqual((byte[])right);
            case DateTimeOffset l: return l.UtcTicks == ((DateTimeOffset)right).UtcTicks;
            case FieldTable l: return TablesEqual(l, (FieldTable)right);
            case FieldValue[] items:
                FieldValue[] other = (FieldValue[])right;
                if (items.Length != other.Length)
                {
                    return false;
                }
                for (int i = 0; i < items.Length; i++)
                {
                    if (!items[i].Equals(other[i]))
                    {
                        return false;
                    }
                }
                return true;
            default:
                return false;
        }
    }

    private static bool TablesEqual(FieldTable left, FieldTable right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach ((string key, FieldValue value) in left)
        {
            if (!right.TryGetValue(key, out FieldValue other) || !value.Equals(other))
            {
                return false;
            }
        }
        return true;
    }
}

/// <summary>
/// The letter-level field value codec: this switch is the single home of the wire letter
/// mapping. Integer decode/encode keeps the wire letter authored on the stream, so
/// re-encoding a decoded table is byte-identical (including peers that do not choose the
/// shortest letter).
/// </summary>
internal static class FieldTableCodec
{
    /// <summary>Maximum table/array nesting accepted when parsing, matching the reference clients.</summary>
    internal const int MaxFieldValueNestingDepth = 32;

    internal static void ThrowIfDecoderDepthTooDeep(int depth)
    {
        if (depth > MaxFieldValueNestingDepth)
        {
            throw new AmqpDecodeException(
                $"Field tables and arrays nested more than {MaxFieldValueNestingDepth} levels deep");
        }
    }

    /// <summary>Reads table entries from a reader already positioned inside the declared
    /// region, stopping when the region is exhausted. The reader must be passed by
    /// <c>ref</c>: consumed offsets must propagate back to the caller's loop.</summary>
    internal static FieldTable ReadTableEntries(ref ByteReader reader, int depth)
    {
        var table = new FieldTable();
        while (reader.Remaining > 0)
        {
            string name = reader.ReadShortstr();
            FieldValue value = ReadValue(ref reader, depth + 1);
            table.Add(name, value);
        }
        return table;
    }

    /// <summary>Reads array items from a reader already positioned inside the declared
    /// region, stopping when the region is exhausted. Passed by <c>ref</c> like
    /// <see cref="ReadTableEntries"/> so consumed offsets propagate.</summary>
    internal static FieldValue[] ReadArrayEntries(ref ByteReader reader, int depth)
    {
        List<FieldValue> items = new();
        while (reader.Remaining > 0)
        {
            items.Add(ReadValue(ref reader, depth + 1));
        }
        return [.. items];
    }

    /// <summary>Reads one typed value starting at the type letter. The <paramref name="depth"/>
    /// argument is the nesting level at which values nested inside containers parse; the caller's
    /// reader advances in place (by <c>ref</c>).</summary>
    internal static FieldValue ReadValue(ref ByteReader reader, int depth)
    {
        byte letter = reader.ReadOctet();
        return letter switch
        {
            (byte)'t' => new FieldValue('t', reader.ReadOctet() != 0),
            (byte)'b' => new FieldValue('b', unchecked((sbyte)reader.ReadOctet())),
            (byte)'B' => new FieldValue('B', reader.ReadOctet()),
            (byte)'s' => new FieldValue('s', reader.ReadShort()),
            (byte)'u' => new FieldValue('u', reader.ReadUShort()),
            (byte)'I' => new FieldValue('I', reader.ReadLong()),
            (byte)'l' => new FieldValue('l', reader.ReadLongLong()),
            (byte)'L' => new FieldValue('L', unchecked((ulong)reader.ReadLongLong())),
            (byte)'f' => new FieldValue('f', reader.ReadSingle()),
            (byte)'d' => new FieldValue('d', reader.ReadDouble()),
            (byte)'D' => ReadDecimalValue(ref reader),
            (byte)'S' => new FieldValue('S', Encoding.UTF8.GetString(reader.ReadLongstr())),
            (byte)'x' => new FieldValue('x', reader.ReadLongstr()),
            (byte)'T' => new FieldValue('T', reader.ReadTimestamp()),
            (byte)'F' => ReadNestedTable(ref reader, depth),
            (byte)'A' => ReadNestedArray(ref reader, depth),
            (byte)'V' => FieldValue.Null,
            _ => throw new AmqpDecodeException($"Unrecognised field-table type letter '{(char)letter}' (0x{letter:X2})"),
        };
    }

    private static FieldValue ReadNestedTable(ref ByteReader reader, int depth)
    {
        if (!reader.TryReadFieldTable(depth, out FieldTable nested))
        {
            throw new AmqpDecodeException("Field table value truncated inside its declared region");
        }
        return new FieldValue('F', nested);
    }

    private static FieldValue ReadNestedArray(ref ByteReader reader, int depth)
    {
        if (!reader.TryReadFieldArray(depth, out FieldValue[] items))
        {
            throw new AmqpDecodeException("Field array value truncated inside its declared region");
        }
        return new FieldValue('A', items);
    }

    /// <summary>Dot-decimal value: scale octet then a 32-bit mantissa whose sign bit carries
    /// the sign (magnitude limited to 31 bits), mirroring the RabbitMQ clients.</summary>
    private static FieldValue ReadDecimalValue(ref ByteReader reader)
    {
        byte scale = reader.ReadOctet();
        if (scale > 28)
        {
            throw new AmqpDecodeException(
                $"Unrepresentable AMQP decimal scale {scale} (max 28)");
        }

        uint mantissa = unchecked((uint)reader.ReadLong());
        bool sign = (mantissa & 0x8000_0000u) != 0;
        int magnitude = (int)(mantissa & 0x7FFF_FFFFu);
        return new FieldValue('D', new decimal(magnitude, 0, 0, sign, scale));
    }
}
