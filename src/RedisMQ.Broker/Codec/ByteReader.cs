using System.Buffers.Binary;
using System.Text;

namespace RedisMQ.Broker.Codec;

/// <summary>
/// Big-endian reader for the AMQP 0-9-1 wire primitives, advancing over a byte span.
/// All reads throw <see cref="AmqpDecodeException"/> when the stream is truncated.
/// Bit reads consume a single packed bitfield octet at a time, LSB-first, exactly as
/// AMQP packs consecutive bit fields; a non-bit read resynchronizes to the next octet.
/// Always pass instances by <c>ref</c>: this is a <c>ref struct</c> so that copies
/// silently losing the offset are impossible.
/// </summary>
public ref struct ByteReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _offset;
    private byte _currentBits;
    private int _bitPosition;

    public ByteReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _offset = 0;
        _currentBits = 0;
        _bitPosition = 0;
    }

    /// <summary>Number of bytes consumed so far.</summary>
    public int Offset => _offset;

    /// <summary>Number of bytes remaining before the end of the backing span.</summary>
    public int Remaining => _buffer.Length - _offset;

    public byte ReadOctet()
    {
        SyncBits();
        Require(1, "octet");
        byte value = _buffer[_offset];
        _offset++;
        return value;
    }

    public ushort ReadUShort()
    {
        SyncBits();
        Require(2, "short-uint");
        ushort value = BinaryPrimitives.ReadUInt16BigEndian(_buffer[_offset..]);
        _offset += 2;
        return value;
    }

    public short ReadShort()
    {
        SyncBits();
        Require(2, "short-int");
        short value = BinaryPrimitives.ReadInt16BigEndian(_buffer[_offset..]);
        _offset += 2;
        return value;
    }

    public int ReadLong()
    {
        SyncBits();
        Require(4, "long-int");
        int value = BinaryPrimitives.ReadInt32BigEndian(_buffer[_offset..]);
        _offset += 4;
        return value;
    }

    public long ReadLongLong()
    {
        SyncBits();
        Require(8, "long-long-int");
        long value = BinaryPrimitives.ReadInt64BigEndian(_buffer[_offset..]);
        _offset += 8;
        return value;
    }

    /// <summary>Reads a short string: one length octet followed by that many UTF-8 bytes.</summary>
    public string ReadShortstr()
    {
        SyncBits();
        Require(1, "shortstr length");
        int length = _buffer[_offset];
        _offset++;
        Require(length, "shortstr content");
        string value = Encoding.UTF8.GetString(_buffer.Slice(_offset, length));
        _offset += length;
        return value;
    }

    /// <summary>Reads a long string/binary blob: a 4-byte big-endian length then the content (copied).</summary>
    public byte[] ReadLongstr()
    {
        SyncBits();
        Require(4, "longstr length");
        uint length = BinaryPrimitives.ReadUInt32BigEndian(_buffer[_offset..]);
        _offset += 4;
        if (length > Remaining)
        {
            throw new AmqpDecodeException(
                $"Truncated longstr: declared {length} content byte(s) but only {Remaining} remain at offset {_offset}");
        }

        byte[] value = _buffer.Slice(_offset, (int)length).ToArray();
        _offset += (int)length;
        return value;
    }

    /// <summary>
    /// Reads the next bit of the current packed bitfield. Octets are fetched from the
    /// stream one at a time as the 8-bit boundary is crossed; every non-bit read resets
    /// the bit cursor so a later bit group starts on a fresh octet.
    /// </summary>
    public bool ReadBit()
    {
        if (_bitPosition == 0)
        {
            Require(1, "packed bitfield octet");
            _currentBits = _buffer[_offset];
            _offset++;
        }

        bool value = (_currentBits & (1 << _bitPosition)) != 0;
        _bitPosition = (_bitPosition + 1) % 8;
        return value;
    }

    /// <summary>Reads an 8-byte big-endian POSIX timestamp (seconds since the Unix epoch).</summary>
    public DateTimeOffset ReadTimestamp()
    {
        SyncBits();
        long seconds = ReadLongLong();
        try
        {
            return DateTimeOffset.FromUnixTimeSeconds(seconds);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new AmqpDecodeException($"Timestamp {seconds} is outside the representable date range", ex);
        }
    }

    /// <summary>
    /// Attempts to read a field table: a 4-byte content length followed by packed entries.
    /// Returns <c>false</c> — with the reader position left untouched and an empty table
    /// in <paramref name="table"/> out — when the remaining buffer cannot hold the declared
    /// table. Tables that are fully present but malformed (unknown letter, entry crossing
    /// the declared region) throw <see cref="AmqpDecodeException"/> instead: a complete
    /// stream yielding broken content is a protocol error, not an availability question.
    /// </summary>
    public bool TryReadFieldTable(out FieldTable table) => TryReadFieldTable(depth: 0, out table);

    internal bool TryReadFieldTable(int depth, out FieldTable table)
    {
        _bitPosition = 0;
        FieldTableCodec.ThrowIfDecoderDepthTooDeep(depth);
        if (Remaining < sizeof(uint))
        {
            table = new FieldTable();
            return false;
        }

        int start = _offset;
        uint length = BinaryPrimitives.ReadUInt32BigEndian(_buffer[_offset..]);
        _offset += sizeof(uint);
        if (length > Remaining)
        {
            _offset = start; // truncated: leave the reader position untouched for the caller
            table = new FieldTable();
            return false;
        }

        // Bound all entry parsing to the declared table region.
        var session = new ByteReader(_buffer.Slice(_offset, (int)length));
        _offset += (int)length;
        table = FieldTableCodec.ReadTableEntries(ref session, depth);
        return true;
    }

    internal bool TryReadFieldArray(int depth, out FieldValue[] items)
    {
        _bitPosition = 0;
        FieldTableCodec.ThrowIfDecoderDepthTooDeep(depth);
        if (Remaining < sizeof(uint))
        {
            items = Array.Empty<FieldValue>();
            return false;
        }

        int start = _offset;
        uint length = BinaryPrimitives.ReadUInt32BigEndian(_buffer[_offset..]);
        _offset += sizeof(uint);
        if (length > Remaining)
        {
            _offset = start;
            items = Array.Empty<FieldValue>();
            return false;
        }

        var session = new ByteReader(_buffer.Slice(_offset, (int)length));
        _offset += (int)length;
        items = FieldTableCodec.ReadArrayEntries(ref session, depth);
        return true;
    }

    internal float ReadSingle()
    {
        SyncBits();
        Require(4, "float");
        float value = BinaryPrimitives.ReadSingleBigEndian(_buffer[_offset..]);
        _offset += 4;
        return value;
    }

    internal double ReadDouble()
    {
        SyncBits();
        Require(8, "double");
        double value = BinaryPrimitives.ReadDoubleBigEndian(_buffer[_offset..]);
        _offset += 8;
        return value;
    }

    /// <summary>Marks the octet portion of the current bitfield as closed; the next ReadBit starts a fresh octet.</summary>
    private void SyncBits() => _bitPosition = 0;

    private void Require(int count, string what)
    {
        if (_offset + count > _buffer.Length)
        {
            throw new AmqpDecodeException(
                $"Truncated AMQP wire data: {count} byte(s) needed for a {what}, only {_buffer.Length - _offset} remain at offset {_offset}");
        }
    }
}
