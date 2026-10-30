using System.Buffers.Binary;
using System.Text;

namespace RedisMQ.Broker.Codec;

/// <summary>
/// Big-endian writer for the AMQP 0-9-1 wire primitives. Grows an internal buffer and
/// exposes the bytes written so far via <see cref="Length"/> plus <see cref="ToArray"/>.
/// <see cref="WriteBits"/> must be called once per AMQP bit-field group: a group occupies
/// whole octets (LSB-first, zero-padded to the octet boundary), so each call flushes its
/// trailing partial octet. Field tables/arrays are written directly into the buffer with
/// the 4-byte length backpatched, so nesting costs no scratch buffers. Pass instances by
/// <c>ref</c> between methods.
/// </summary>
public sealed class ByteWriter
{
    private const int DefaultCapacity = 256;

    private byte[] _buffer = new byte[DefaultCapacity];
    private int _length;
    private byte _currentBits;
    private int _bitPosition;

    /// <summary>Total bytes committed (including flushed bitfield octets).</summary>
    public int Length => _length;

    /// <summary>Returns a copy of the bytes written so far.</summary>
    public byte[] ToArray() => _buffer.AsSpan(0, _length).ToArray();

    public void WriteOctet(byte value)
    {
        Reserve(1);
        _buffer[_length] = value;
        _length++;
    }

    public void WriteUShort(ushort value)
    {
        Reserve(2);
        BinaryPrimitives.WriteUInt16BigEndian(_buffer.AsSpan(_length), value);
        _length += 2;
    }

    public void WriteShort(short value)
    {
        Reserve(2);
        BinaryPrimitives.WriteInt16BigEndian(_buffer.AsSpan(_length), value);
        _length += 2;
    }

    public void WriteLong(int value)
    {
        Reserve(4);
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(_length), value);
        _length += 4;
    }

    public void WriteLongLong(long value)
    {
        Reserve(8);
        BinaryPrimitives.WriteInt64BigEndian(_buffer.AsSpan(_length), value);
        _length += 8;
    }

    /// <summary>
    /// Writes a short string: one UTF-8 length octet then the content.
    /// Throws <see cref="AmqpEncodeException"/> when the UTF-8 encoding exceeds 255 bytes.
    /// </summary>
    public void WriteShortstr(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length == 0)
        {
            WriteOctet(0);
            return;
        }

        int byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount > byte.MaxValue)
        {
            throw new AmqpEncodeException(
                $"Short string too long: {byteCount} UTF-8 byte(s), max 255");
        }

        Reserve(1 + byteCount);
        _buffer[_length] = (byte)byteCount;
        int written = Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_length + 1, byteCount));
        if (written != byteCount)
        {
            throw new AmqpEncodeException("Short string encoding gave a different byte count than measured");
        }
        _length += 1 + byteCount;
    }

    /// <summary>Writes a long string/binary blob: 4-byte big-endian length then the content.</summary>
    public void WriteLongstr(ReadOnlySpan<byte> value)
    {
        Reserve(4 + value.Length);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(_length), (uint)value.Length);
        _length += 4;
        value.CopyTo(_buffer.AsSpan(_length));
        _length += value.Length;
    }

    /// <summary>Writes raw bytes with no length prefix: for pre-encoded payloads that the
    /// caller sizes explicitly (frame payloads are framed by their own size field).</summary>
    public void WriteRaw(ReadOnlySpan<byte> value)
    {
        if (value.Length == 0)
        {
            return;
        }

        Reserve(value.Length);
        value.CopyTo(_buffer.AsSpan(_length));
        _length += value.Length;
    }

    /// <summary>
    /// Writes an AMQP bit-field group, LSB-first into whole octets; a group shorter than
    /// 8 bits is padded with zero bits to the octet boundary.
    /// </summary>
    public void WriteBits(params bool[] bits)
    {
        ArgumentNullException.ThrowIfNull(bits);
        foreach (bool bit in bits)
        {
            if (bit)
            {
                _currentBits |= (byte)(1 << _bitPosition);
            }
            _bitPosition++;
            if (_bitPosition == 8)
            {
                FlushBits();
            }
        }

        if (_bitPosition > 0)
        {
            // A bit-field group always ends on an octet boundary; pad with zero bits.
            FlushBits();
        }
    }

    /// <summary>Writes a field table: 4-byte content length followed by the packed entries.</summary>
    public void WriteFieldTable(FieldTable table)
    {
        ArgumentNullException.ThrowIfNull(table);
        WriteFieldTableBody(table, depth: 0);
    }

    /// <summary>
    /// Writes an 8-byte POSIX timestamp (seconds since the Unix epoch, big-endian).
    /// Sub-second components are truncated; offsets are normalized to UTC.
    /// </summary>
    public void WriteTimestamp(DateTimeOffset value) => WriteLongLong(value.ToUnixTimeSeconds());

    private void WriteFieldTableBody(FieldTable table, int depth)
    {
        // depth is the level this table occupies (the top-level table is level 0);
        // going deeper than the accepted nesting level is refused before any bytes are written.
        ThrowIfEncoderDepthTooDeep(depth);
        int lengthPosition = _length;
        Reserve(sizeof(uint));
        _length += sizeof(uint);
        foreach ((string key, FieldValue value) in table)
        {
            WriteShortstr(key);
            WriteFieldValue(value, depth);
        }

        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(lengthPosition), (uint)(_length - lengthPosition - sizeof(uint)));
    }

    private void WriteFieldValue(FieldValue value, int depth)
    {
        object? raw = value.Raw;
        if (raw is null)
        {
            if (value.TypeLetter == 'V')
            {
                WriteOctet((byte)'V');
                return;
            }

            throw new AmqpEncodeException("Uninitialized field value (default): build values with the FieldValue factories");
        }

        switch (raw)
        {
            case bool bit:
                WriteOctet((byte)'t');
                WriteOctet(bit ? (byte)1 : (byte)0);
                break;
            case sbyte v:
                WriteOctet((byte)'b');
                WriteOctet((byte)v);
                break;
            case byte v:
                WriteOctet((byte)'B');
                WriteOctet(v);
                break;
            case short v:
                WriteOctet((byte)'s');
                WriteShort(v);
                break;
            case ushort v:
                WriteOctet((byte)'u');
                WriteUShort(v);
                break;
            case int v:
                WriteOctet((byte)'I');
                WriteLong(v);
                break;
            case long v:
                WriteOctet((byte)'l');
                WriteLongLong(v);
                break;
            case ulong v:
                WriteOctet((byte)'L');
                WriteLongLong(unchecked((long)v));
                break;
            case float v:
                WriteOctet((byte)'f');
                WriteSingle(v);
                break;
            case double v:
                WriteOctet((byte)'d');
                WriteDouble(v);
                break;
            case decimal v:
                WriteOctet((byte)'D');
                WriteDecimal(v);
                break;
            case string v:
                WriteOctet((byte)'S');
                WriteLongstr(Encoding.UTF8.GetBytes(v));
                break;
            case byte[] v:
                WriteOctet((byte)'x');
                WriteLongstr(v);
                break;
            case DateTimeOffset v:
                WriteOctet((byte)'T');
                WriteTimestamp(v);
                break;
            case FieldTable v:
                WriteOctet((byte)'F');
                WriteFieldTableBody(v, depth + 1);
                break;
            case FieldValue[] items:
                WriteOctet((byte)'A');
                WriteArrayBody(items, depth + 1);
                break;
            default:
                throw new AmqpEncodeException($"Unsupported field-table value type {value.Raw!.GetType().FullName}");
        }
    }

    private void WriteArrayBody(FieldValue[] items, int depth)
    {
        ThrowIfEncoderDepthTooDeep(depth);
        int lengthPosition = _length;
        Reserve(sizeof(uint));
        _length += sizeof(uint);
        foreach (FieldValue item in items)
        {
            WriteFieldValue(item, depth);
        }

        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(lengthPosition), (uint)(_length - lengthPosition - sizeof(uint)));
    }

    private void WriteSingle(float value)
    {
        Reserve(4);
        BinaryPrimitives.WriteSingleBigEndian(_buffer.AsSpan(_length), value);
        _length += 4;
    }

    private void WriteDouble(double value)
    {
        Reserve(8);
        BinaryPrimitives.WriteDoubleBigEndian(_buffer.AsSpan(_length), value);
        _length += 8;
    }

    /// <summary>
    /// Writes a decimal in dot-decimal form: one scale octet then a 4-byte mantissa whose
    /// sign lives in the high bit (31 magnitude bits), matching the RabbitMQ clients.
    /// </summary>
    private void WriteDecimal(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        uint lo = unchecked((uint)bits[0]);
        bool sign = (bits[3] & unchecked((int)0x8000_0000)) != 0;
        byte scale = (byte)((bits[3] >> 16) & 0xFF);
        if (bits[1] != 0 || bits[2] != 0 || (lo & 0x8000_0000u) != 0)
        {
            throw new AmqpEncodeException(
                $"Decimal value {value} does not fit the AMQP 32-bit dot-decimal form (mantissa wider than 31 bits)");
        }

        WriteOctet(scale);
        WriteLong(unchecked((int)(lo | (sign ? 0x8000_0000u : 0u))));
    }

    private void ThrowIfEncoderDepthTooDeep(int depth)
    {
        if (depth > FieldTableCodec.MaxFieldValueNestingDepth)
        {
            throw new AmqpEncodeException(
                $"Field tables and arrays nested deeper than {FieldTableCodec.MaxFieldValueNestingDepth} levels cannot be written");
        }
    }

    private void FlushBits()
    {
        Reserve(1);
        _buffer[_length] = _currentBits;
        _length++;
        _currentBits = 0;
        _bitPosition = 0;
    }

    private void Reserve(int additional)
    {
        int needed = _length + additional;
        if (needed <= _buffer.Length)
        {
            return;
        }

        int newSize = Math.Max(_buffer.Length * 2, needed);
        var grown = new byte[newSize];
        _buffer.AsSpan(0, _length).CopyTo(grown);
        _buffer = grown;
    }
}
