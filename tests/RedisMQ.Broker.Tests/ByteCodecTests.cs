using System.Text;
using RedisMQ.Broker.Codec;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Byte-level codec primitives (Task 1): ByteReader / ByteWriter / FieldTable round-trips,
// big-endian wire layout, AMQP bit packing (LSB-first), field-table letters both directions
// (byte-identical re-encode), dot-decimal values, timestamps, and truncation behavior.
// Wire fixtures follow AMQP 0-9-1 as clarified by the RabbitMQ errata and reference clients.
public sealed class ByteCodecTests
{
    private const long TwoPow62 = 4611686018427387904L;

    // ---------- helpers ----------

    private static byte[] Encode(Action<ByteWriter> write)
    {
        var writer = new ByteWriter();
        write(writer);
        return writer.ToArray();
    }

    private static bool TryDecode(byte[] bytes, out FieldTable table)
    {
        var reader = new ByteReader(bytes);
        bool ok = reader.TryReadFieldTable(out table);
        Assert.Equal(bytes.Length, reader.Offset); // a successful table read consumes the whole buffer
        return ok;
    }

    private static (byte[] Wire, FieldValue Field) RoundTripValue(FieldValue value, string key = "k")
    {
        var built = new FieldTable();
        built.Add(key, value);

        var writer = new ByteWriter();
        writer.WriteFieldTable(built);
        byte[] original = writer.ToArray();

        Assert.True(TryDecode(original, out var decoded));
        Assert.True(decoded.ContainsKey(key));

        var rewriter = new ByteWriter();
        rewriter.WriteFieldTable(decoded);
        byte[] reencoded = rewriter.ToArray();

        // Byte-identical re-encode is the core field-table invariant.
        Assert.Equal(original, reencoded);
        return (original, decoded[key]);
    }

    /// <summary>Builds the full wire bytes of a field table whose chain of nested 'F' values
    /// is <paramref name="depth"/> levels deep: each level holds one key "k" whose value is
    /// the next inner table, and the innermost table is empty.</summary>
    private static byte[] NestedTableBytes(int depth)
    {
        byte[] payload = [0x00, 0x00, 0x00, 0x00]; // an empty table value: declared length 0
        for (int i = 0; i < depth; i++)
        {
            byte[] entry = [1, (byte)'k', (byte)'F', .. payload];
            payload = [.. UInt32Bytes((uint)entry.Length), .. entry];
        }
        return payload;
    }

    private static byte[] UInt32Bytes(uint value) =>
    [
        (byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value,
    ];

    /// <summary>Encodes one table entry (shortstr name + typed value) using only the public
    /// writer API, by routing the value through a single-row table and stripping its wrapper.</summary>
    private static byte[] EntryWire(string key, FieldValue value)
    {
        var writer = new ByteWriter();
        writer.WriteFieldTable(new FieldTable([new KeyValuePair<string, FieldValue>("_", value)]));
        byte[] wire = writer.ToArray();
        // one-row table wire = [len(4)] [name "_": (1) + 0x5F] [value...]; the entry value bytes start at index 6
        byte[] valueBytes = wire[6..];

        byte[] name =
        [
            (byte)Encoding.UTF8.GetByteCount(key),
            .. Encoding.UTF8.GetBytes(key),
        ];
        return [.. name, .. valueBytes];
    }

    // ---------- primitives: round trips + big-endian golden bytes ----------

    [Fact]
    public void ReadOctet_round_trips_all_sentinels()
    {
        byte[] wire = Encode(w =>
        {
            w.WriteOctet(0x00);
            w.WriteOctet(0xFF);
            w.WriteOctet(0x42);
        });
        Assert.Equal([0x00, 0xFF, 0x42], wire);

        var reader = new ByteReader(wire);
        Assert.Equal((byte)0x00, reader.ReadOctet());
        Assert.Equal((byte)0xFF, reader.ReadOctet());
        Assert.Equal((byte)0x42, reader.ReadOctet());
        Assert.Equal(3, reader.Offset);
        Assert.Equal(0, reader.Remaining);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(65535)]
    [InlineData(0x1234)]
    public void ReadUShort_round_trips_sentinels_big_endian(ushort value)
    {
        byte[] wire = Encode(w => w.WriteUShort(value));
        if (value == 0x1234)
        {
            Assert.Equal([0x12, 0x34], wire);
        }

        var reader = new ByteReader(wire);
        Assert.Equal(value, reader.ReadUShort());
    }

    [Theory]
    [InlineData(-32768)]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(32767)]
    public void ReadShort_round_trips_including_negatives_big_endian(short value)
    {
        byte[] wire = Encode(w => w.WriteShort(value));
        if (value == -1)
        {
            Assert.Equal([0xFF, 0xFF], wire);
        }

        var reader = new ByteReader(wire);
        Assert.Equal(value, reader.ReadShort());
    }

    [Theory]
    [InlineData(int.MinValue)]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(654321)]
    [InlineData(int.MaxValue)]
    public void ReadLong_round_trips_sentinels_big_endian(int value)
    {
        byte[] wire = Encode(w => w.WriteLong(value));
        if (value == 654321)
        {
            Assert.Equal([0x00, 0x09, 0xFB, 0xF1], wire); // 654321 = 0x0009FBF1
        }

        var reader = new ByteReader(wire);
        Assert.Equal(value, reader.ReadLong());
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(TwoPow62)]
    [InlineData(long.MaxValue)]
    public void ReadLongLong_round_trips_sentinels_big_endian(long value)
    {
        byte[] wire = Encode(w => w.WriteLongLong(value));
        if (value == TwoPow62)
        {
            Assert.Equal([0x40, 0, 0, 0, 0, 0, 0, 0], wire);
        }

        var reader = new ByteReader(wire);
        Assert.Equal(value, reader.ReadLongLong());
    }

    // ---------- shortstr / longstr ----------

    [Fact]
    public void ReadShortstr_round_trips_ascii_big_endian_length()
    {
        byte[] wire = Encode(w => w.WriteShortstr("hello"));
        Assert.Equal([0x05, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o'], wire);

        var reader = new ByteReader(wire);
        Assert.Equal("hello", reader.ReadShortstr());
        Assert.Equal(wire.Length, reader.Offset);
    }

    [Fact]
    public void ReadShortstr_round_trips_multi_byte_utf8()
    {
        string value = "nœud € café"; // multi-byte UTF-8, well under the 255-byte cap
        byte[] wire = Encode(w => w.WriteShortstr(value));
        var reader = new ByteReader(wire);
        Assert.Equal(value, reader.ReadShortstr());
    }

    [Fact]
    public void ReadShortstr_round_trips_empty_string()
    {
        byte[] wire = Encode(w => w.WriteShortstr(""));
        Assert.Equal([0x00], wire);

        var reader = new ByteReader(wire);
        Assert.Equal("", reader.ReadShortstr());
    }

    [Fact]
    public void ReadShortstr_accepts_exactly_255_bytes_but_writer_rejects_256()
    {
        string max = new string('a', 255);
        byte[] wire = Encode(w => w.WriteShortstr(max));
        Assert.Equal(255, wire[0]);
        Assert.Equal(1 + 255, wire.Length);

        var reader = new ByteReader(wire);
        Assert.Equal(max, reader.ReadShortstr());

        var tooLong = new ByteWriter();
        Assert.Throws<AmqpEncodeException>(() => tooLong.WriteShortstr(new string('a', 256)));
    }

    [Fact]
    public void ReadShortstr_rejects_truncated_declared_length()
    {
        byte[] wired = [0x05, (byte)'o', (byte)'n', (byte)'e']; // declares 5, only 3 present
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wired).ReadShortstr());
    }

    [Fact]
    public void ReadLongstr_round_trips_with_big_endian_length_prefix()
    {
        byte[] blob = [0xDE, 0xAD, 0xBE, 0xEF];
        byte[] wire = Encode(w =>
        {
            w.WriteLongstr(blob);
            w.WriteLongstr(ReadOnlySpan<byte>.Empty);
        });
        Assert.Equal([0x00, 0x00, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00], wire);

        var reader = new ByteReader(wire);
        Assert.Equal(blob, reader.ReadLongstr());
        Assert.Equal(Array.Empty<byte>(), reader.ReadLongstr());
    }

    [Fact]
    public void ReadLongstr_round_trips_large_blob()
    {
        byte[] blob = new byte[131073];
        for (int i = 0; i < blob.Length; i++)
        {
            blob[i] = (byte)(i % 251);
        }
        byte[] wire = Encode(w => w.WriteLongstr(blob));
        var reader = new ByteReader(wire);
        Assert.Equal(blob, reader.ReadLongstr());
    }

    [Fact]
    public void ReadLongstr_rejects_declared_length_beyond_buffer()
    {
        byte[] wired = [0x00, 0x00, 0x00, 0x08, 0x01, 0x02]; // claims 8 bytes, only 2 present
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wired).ReadLongstr());
    }

    // ---------- bits: LSB-first packing across octets ----------

    [Fact]
    public void WriteBits_packs_three_bits_into_one_octet_0x05()
    {
        byte[] wire = Encode(w => w.WriteBits(true, false, true));
        Assert.Equal([0x05], wire);
    }

    [Fact]
    public void WriteBits_packs_lsb_first_with_new_octet_after_eight_bits()
    {
        byte[] eight = Encode(w => w.WriteBits(true, true, true, true, true, true, true, true));
        Assert.Equal([0xFF], eight);

        byte[] nine = Encode(w =>
            w.WriteBits(true, true, true, true, true, true, true, true, true));
        Assert.Equal([0xFF, 0x01], nine); // the 9th bit starts a fresh octet at the LSB
    }

    [Fact]
    public void ReadBit_consumes_a_packed_bitfield_sequentially()
    {
        var reader = new ByteReader([0x05]); // bits [true,false,true,...] packed LSB-first
        Assert.True(reader.ReadBit());
        Assert.False(reader.ReadBit());
        Assert.True(reader.ReadBit());
        Assert.False(reader.ReadBit()); // 4th bit of the octet is unset
        Assert.Equal(1, reader.Offset); // exactly one octet consumed
    }

    [Fact]
    public void ReadBit_resets_between_separated_bit_groups()
    {
        byte[] wire = Encode(w =>
        {
            w.WriteBits(true, false, true); // group one -> 0x05
            w.WriteOctet(0xAA);             // a non-bit field in between
            w.WriteBits(true);              // group two -> own octet 0x01
        });
        Assert.Equal([0x05, 0xAA, 0x01], wire);

        var reader = new ByteReader(wire);
        Assert.True(reader.ReadBit());
        Assert.False(reader.ReadBit());
        Assert.True(reader.ReadBit());
        Assert.Equal(0xAA, reader.ReadOctet());
        Assert.True(reader.ReadBit());
        Assert.Equal(3, reader.Offset);
    }

    [Fact]
    public void Bits_nine_bits_across_a_group_boundary_round_trip()
    {
        bool[] bits = [true, false, true, true, false, false, true, false, true];
        byte[] wire = Encode(w => w.WriteBits(bits));

        var reader = new ByteReader(wire);
        foreach (bool expected in bits)
        {
            Assert.Equal(expected, reader.ReadBit());
        }
        Assert.Equal(2, reader.Offset);
    }

    [Fact]
    public void WriteBits_with_no_bits_writes_nothing()
    {
        byte[] wire = Encode(w => w.WriteBits());
        Assert.Equal(Array.Empty<byte>(), wire);
    }

    // ---------- timestamps ----------

    [Fact]
    public void ReadTimestamp_round_trips_utc_seconds()
    {
        var instant = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero); // 946684800
        byte[] wire = Encode(w => w.WriteTimestamp(instant));
        Assert.Equal([0, 0, 0, 0, 0x38, 0x6D, 0x43, 0x80], wire); // 946684800, big-endian

        var reader = new ByteReader(wire);
        Assert.Equal(instant, reader.ReadTimestamp());
    }

    [Fact]
    public void ReadTimestamp_normalizes_offsets_to_utc()
    {
        var withOffset = new DateTimeOffset(2026, 1, 2, 12, 0, 0, TimeSpan.FromHours(2));
        byte[] wire = Encode(w => w.WriteTimestamp(withOffset));

        DateTimeOffset decoded = new ByteReader(wire).ReadTimestamp();
        Assert.Equal(TimeSpan.Zero, decoded.Offset);
        Assert.Equal(withOffset.UtcDateTime, decoded.UtcDateTime);
    }

    [Fact]
    public void ReadTimestamp_round_trips_pre_epoch_value()
    {
        var beforeEpoch = new DateTimeOffset(1969, 7, 20, 20, 17, 40, TimeSpan.Zero);
        byte[] wire = Encode(w => w.WriteTimestamp(beforeEpoch));
        long seconds = BitConverter.ToInt64(wire);
        Assert.True(seconds < 0);

        DateTimeOffset decoded = new ByteReader(wire).ReadTimestamp();
        Assert.Equal(beforeEpoch, decoded);
    }

    [Fact]
    public void ReadTimestamp_epoch_is_all_zero()
    {
        byte[] wire = Encode(w => w.WriteTimestamp(DateTimeOffset.UnixEpoch));
        Assert.Equal(new byte[8], wire);
        Assert.Equal(DateTimeOffset.UnixEpoch, new ByteReader(wire).ReadTimestamp());
    }

    // ---------- truncation: one theory over every read type ----------

    public enum PrimKind { Octet, UShort, Short, Long, LongLong, Shortstr, Longstr, Bit, Timestamp }

    private static byte[] RepresentativeWire(PrimKind kind) => kind switch
    {
        PrimKind.Octet => Encode(w => w.WriteOctet(0x42)),
        PrimKind.UShort => Encode(w => w.WriteUShort(0xBEEF)),
        PrimKind.Short => Encode(w => w.WriteShort(-1234)),
        PrimKind.Long => Encode(w => w.WriteLong(654321)),
        PrimKind.LongLong => Encode(w => w.WriteLongLong(TwoPow62)),
        PrimKind.Shortstr => Encode(w => w.WriteShortstr("amqp")),
        PrimKind.Longstr => Encode(w => w.WriteLongstr([0xDE, 0xAD])),
        PrimKind.Bit => Encode(w => w.WriteBits(true, false, true)),
        PrimKind.Timestamp => Encode(w => w.WriteTimestamp(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private static object? ReadRepresentative(PrimKind kind, ByteReader reader) => kind switch
    {
        PrimKind.Octet => reader.ReadOctet(),
        PrimKind.UShort => reader.ReadUShort(),
        PrimKind.Short => reader.ReadShort(),
        PrimKind.Long => reader.ReadLong(),
        PrimKind.LongLong => reader.ReadLongLong(),
        PrimKind.Shortstr => reader.ReadShortstr(),
        PrimKind.Longstr => reader.ReadLongstr(),
        PrimKind.Bit => reader.ReadBit(),
        PrimKind.Timestamp => reader.ReadTimestamp(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    [Theory]
    [InlineData(PrimKind.Octet)]
    [InlineData(PrimKind.UShort)]
    [InlineData(PrimKind.Short)]
    [InlineData(PrimKind.Long)]
    [InlineData(PrimKind.LongLong)]
    [InlineData(PrimKind.Shortstr)]
    [InlineData(PrimKind.Longstr)]
    [InlineData(PrimKind.Bit)]
    [InlineData(PrimKind.Timestamp)]
    public void Truncation_mid_stream_throws_amqp_decode_exception(PrimKind kind)
    {
        byte[] wire = RepresentativeWire(kind);
        Assert.Equal(wire.Length, new ByteReader(wire).Remaining); // representative is the full stream

        byte[] truncated = wire[..^1]; // drop exactly one final byte
        Assert.Throws<AmqpDecodeException>(() => ReadRepresentative(kind, new ByteReader(truncated)));
    }

    // ---------- FieldTable Try semantics ----------

    [Fact]
    public void TryReadFieldTable_false_when_fewer_than_four_bytes_remain()
    {
        var oneByte = new ByteReader([0x01]);
        Assert.False(oneByte.TryReadFieldTable(out var table));
        Assert.Empty(table);
        Assert.Equal(0, oneByte.Offset); // position untouched on failure

        var empty = new ByteReader([]);
        Assert.False(empty.TryReadFieldTable(out table));
        Assert.Empty(table);
    }

    [Fact]
    public void TryReadFieldTable_false_when_declared_length_exceeds_remaining()
    {
        byte[] wire = [0x00, 0x00, 0x00, 0x0A, 0x01, 0x6B]; // claims 10 content bytes, only 2 follow
        var reader = new ByteReader(wire);
        Assert.False(reader.TryReadFieldTable(out var table));
        Assert.Empty(table);
        Assert.Equal(0, reader.Offset);
    }

    [Fact]
    public void TryReadFieldTable_true_for_empty_table()
    {
        byte[] wire = [0x00, 0x00, 0x00, 0x00];
        var reader = new ByteReader(wire);
        Assert.True(reader.TryReadFieldTable(out var table));
        Assert.Empty(table);
        Assert.Equal(wire.Length, reader.Offset);
    }

    // ---------- FieldValue letters: byte-identical re-encode in both directions ----------

    [Fact]
    public void FieldValue_bool_uses_letter_t()
    {
        (byte[] wire, FieldValue field) = RoundTripValue(FieldValue.From(true));
        Assert.Equal([0x00, 0x00, 0x00, 0x04, 0x01, (byte)'k', (byte)'t', 0x01], wire);
        Assert.Equal('t', field.TypeLetter);
        Assert.True((bool)field.Raw!);
    }

    [Fact]
    public void FieldValue_sbyte_uses_letter_b_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From((sbyte)-5));
        Assert.Equal('b', field.TypeLetter);
        Assert.Equal((sbyte)-5, field.Raw);
    }

    [Fact]
    public void FieldValue_byte_uses_letter_B_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From((byte)0xB7));
        Assert.Equal('B', field.TypeLetter);
        Assert.Equal((byte)0xB7, field.Raw);
    }

    [Fact]
    public void FieldValue_short_uses_letter_s_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From((short)-1024));
        Assert.Equal('s', field.TypeLetter);
        Assert.Equal((short)-1024, field.Raw);
    }

    [Fact]
    public void FieldValue_ushort_uses_letter_u_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From((ushort)1024));
        Assert.Equal('u', field.TypeLetter);
        Assert.Equal((ushort)1024, field.Raw);
    }

    [Fact]
    public void FieldValue_int_uses_letter_I_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From(70000));
        Assert.Equal('I', field.TypeLetter);
        Assert.Equal(70000, field.Raw);
    }

    [Fact]
    public void FieldValue_long_uses_letter_l_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From(5_000_000_000L));
        Assert.Equal('l', field.TypeLetter);
        Assert.Equal(5_000_000_000L, field.Raw);
    }

    [Fact]
    public void FieldValue_unsigned_long_letter_L_round_trips()
    {
        // 'L' has no CLR producer factory in From(); drive it from raw wire bytes.
        byte[] wire = Encode(w =>
        {
            w.WriteLong(2 + 1 + 8); // name + letter + 8-byte value
            w.WriteShortstr("k");
            w.WriteOctet((byte)'L');
            w.WriteLongLong(TwoPow62);
        });
        Assert.True(TryDecode(wire, out var table));

        FieldValue field = table["k"];
        Assert.Equal('L', field.TypeLetter);
        Assert.Equal((ulong)TwoPow62, Assert.IsType<ulong>(field.Raw));

        var rewriter = new ByteWriter();
        rewriter.WriteFieldTable(table);
        Assert.Equal(wire, rewriter.ToArray());
    }

    [Fact]
    public void FieldValue_float_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From(1.5f));
        Assert.Equal('f', field.TypeLetter);
        Assert.Equal(1.5f, field.Raw);
    }

    [Fact]
    public void FieldValue_double_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From(3.14d));
        Assert.Equal('d', field.TypeLetter);
        Assert.Equal(3.14d, field.Raw);
    }

    [Fact]
    public void FieldValue_decimal_uses_letter_D_with_scale_then_signed_mantissa()
    {
        (byte[] wire, FieldValue field) = RoundTripValue(FieldValue.From(123.45m));
        Assert.Equal([0x00, 0x00, 0x00, 0x08, 0x01, (byte)'k', (byte)'D', 0x02, 0x00, 0x00, 0x30, 0x39], wire);
        Assert.Equal('D', field.TypeLetter);
        Assert.Equal(123.45m, field.Raw);

        (_, FieldValue negative) = RoundTripValue(FieldValue.From(-123.45m));
        Assert.Equal(-123.45m, negative.Raw);
    }

    [Fact]
    public void Decimal_wire_keeps_sign_in_the_high_bit_of_the_mantissa()
    {
        byte[] wire = Encode(w => w.WriteFieldTable(
            new FieldTable([new KeyValuePair<string, FieldValue>("k", FieldValue.From(-123.45m))])));
        // scale octet 0x02, then mantissa: sign bit (0x80) | low 31 bits of magnitude (0x0000_3039)
        Assert.Equal([0x02, 0x80, 0x00, 0x30, 0x39], wire[7..]);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("1.2")]
    [InlineData("-123.45")]
    [InlineData("21474836.47")] // mantissa = int.MaxValue (largest encodable magnitude, 31 bits)
    [InlineData("-21474836.47")] // mantissa = -int.MaxValue
    [InlineData("0.0001")]
    [InlineData("-0.5")]
    public void Decimal_round_trips_exactly(string decimalText)
    {
        decimal value = ParseDecimal(decimalText);
        (_, FieldValue field) = RoundTripValue(FieldValue.From(value));
        Assert.Equal('D', field.TypeLetter);
        Assert.Equal(value, field.Raw);
    }

    [Fact]
    public void Decimal_write_rejects_mantissa_wider_than_32_bits()
    {
        Assert.Throws<AmqpEncodeException>(() => Encode(w => w.WriteFieldTable(
            new FieldTable([new KeyValuePair<string, FieldValue>("k", FieldValue.From(decimal.MaxValue))]))));

        // mantissa magnitude >= 2^31 does not fit either (the sign bit of the wire
        // mantissa carries the sign, so only 31 magnitude bits are available)
        Assert.Throws<AmqpEncodeException>(() => Encode(w => w.WriteFieldTable(
            new FieldTable([new KeyValuePair<string, FieldValue>("k", FieldValue.From(3000000000m))]))));
        Assert.Throws<AmqpEncodeException>(() => Encode(w => w.WriteFieldTable(
            new FieldTable([new KeyValuePair<string, FieldValue>("k", FieldValue.From(-21474836.48m))]))));
    }

    [Fact]
    public void Decimal_read_rejects_scale_over_28()
    {
        byte[] wire = Encode(w =>
        {
            w.WriteLong(2 + 1 + 1 + 4); // name + letter + scale + mantissa
            w.WriteShortstr("k");
            w.WriteOctet((byte)'D');
            w.WriteOctet(29); // CLR decimal supports scale 0..28 only
            w.WriteLong(0);
        });
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wire).TryReadFieldTable(out _));
    }

    [Fact]
    public void FieldValue_string_uses_letter_S_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(FieldValue.From("hëllo €"));
        Assert.Equal('S', field.TypeLetter);
        Assert.Equal("hëllo €", field.Raw);
    }

    [Fact]
    public void FieldValue_byte_array_uses_letter_x_and_round_trips()
    {
        byte[] bytes = [0x00, 0x01, 0x02, 0xFF];
        (_, FieldValue field) = RoundTripValue(FieldValue.From(bytes));
        Assert.Equal('x', field.TypeLetter);
        Assert.Equal(bytes, Assert.IsType<byte[]>(field.Raw));
    }

    [Fact]
    public void FieldValue_timestamp_uses_letter_T_and_round_trips()
    {
        var instant = new DateTimeOffset(2024, 5, 1, 10, 30, 0, TimeSpan.Zero);
        (_, FieldValue field) = RoundTripValue(FieldValue.From(instant));
        Assert.Equal('T', field.TypeLetter);
        DateTimeOffset decoded = Assert.IsType<DateTimeOffset>(field.Raw);
        Assert.Equal(instant.UtcDateTime, decoded.UtcDateTime);
    }

    [Fact]
    public void FieldValue_nested_table_uses_letter_F_and_round_trips()
    {
        var nested = new FieldTable();
        nested.Add("n", FieldValue.From(true));
        nested.Add("s", FieldValue.From("v"));

        (_, FieldValue field) = RoundTripValue(FieldValue.From(nested));
        Assert.Equal('F', field.TypeLetter);
        FieldTable decoded = Assert.IsType<FieldTable>(field.Raw);
        Assert.True((bool)decoded["n"].Raw!);
        Assert.Equal("v", (string)decoded["s"].Raw!);
    }

    [Fact]
    public void FieldValue_list_uses_letter_A_and_round_trips()
    {
        (_, FieldValue field) = RoundTripValue(
            FieldValue.List(FieldValue.From(1), FieldValue.From("two"), FieldValue.Null));
        Assert.Equal('A', field.TypeLetter);

        FieldValue[] items = Assert.IsType<FieldValue[]>(field.Raw);
        Assert.Equal(3, items.Length);
        Assert.Equal(1, (int)items[0].Raw!);
        Assert.Equal("two", (string)items[1].Raw!);
        Assert.Equal('V', items[2].TypeLetter);
        Assert.Null(items[2].Raw);
    }

    [Fact]
    public void FieldValue_null_uses_letter_V()
    {
        (byte[] wire, FieldValue field) = RoundTripValue(FieldValue.Null);
        Assert.Equal([0x00, 0x00, 0x00, 0x03, 0x01, (byte)'k', (byte)'V'], wire);
        Assert.Equal('V', field.TypeLetter);
        Assert.Null(field.Raw);
    }

    // ---------- FieldTable container behavior ----------

    [Fact]
    public void FieldTable_preserves_iteration_order_and_lookups()
    {
        var table = new FieldTable();
        table.Add("z", FieldValue.From(1));
        table.Add("a", FieldValue.From("str"));
        table.Add("m", FieldValue.Null);

        Assert.Equal(3, table.Count);
        Assert.Equal(["z", "a", "m"], table.Keys);
        Assert.Equal([FieldValue.From(1), FieldValue.From("str"), FieldValue.Null], table.Values);
        Assert.True(table.ContainsKey("a"));
        Assert.False(table.ContainsKey("nope"));
        Assert.True(table.TryGetValue("z", out FieldValue z));
        Assert.Equal(FieldValue.From(1), z);
        Assert.Equal(FieldValue.From(1), table["z"]);
        Assert.Throws<KeyNotFoundException>(() => table["nope"]);

        var prebuilt = new FieldTable([new KeyValuePair<string, FieldValue>("k", FieldValue.From(5))]);
        Assert.Single(prebuilt);
        Assert.Equal(FieldValue.From(5), prebuilt["k"]);
    }

    // ---------- composite table: values, letters, order, identity ----------

    [Fact]
    public void Composite_table_round_trips_byte_identically()
    {
        var nested = new FieldTable();
        nested.Add("flag", FieldValue.From(true));
        nested.Add("u", FieldValue.From((ushort)3));

        var table = new FieldTable();
        table.Add("tBool", FieldValue.From(false));
        table.Add("tSByte", FieldValue.From((sbyte)-3));
        table.Add("tByte", FieldValue.From((byte)200));
        table.Add("tShort", FieldValue.From((short)-300));
        table.Add("tUShort", FieldValue.From((ushort)60000));
        table.Add("tInt", FieldValue.From(-70000));
        table.Add("tLong", FieldValue.From(-5_000_000_000L));
        table.Add("tFloat", FieldValue.From(-0.5f));
        table.Add("tDouble", FieldValue.From(0.25d));
        table.Add("tDecimal", FieldValue.From(-123.45m));
        table.Add("tStr", FieldValue.From("grüß"));
        table.Add("tBin", FieldValue.From(new byte[] { 0x01, 0xFF }));
        table.Add("tStamp", FieldValue.From(new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.Zero)));
        table.Add("tNested", FieldValue.From(nested));
        table.Add("tList", FieldValue.List(FieldValue.From((byte)1), FieldValue.From("s"), FieldValue.From(0)));
        table.Add("tNull", FieldValue.Null);

        var writer = new ByteWriter();
        writer.WriteFieldTable(table);
        byte[] original = writer.ToArray();
        Assert.Equal(original.Length, writer.Length);

        var reader = new ByteReader(original);
        Assert.True(reader.TryReadFieldTable(out var decoded));
        Assert.Equal(16, decoded.Count);
        Assert.Equal(original.Length, reader.Offset);

        Assert.Equal(FieldValue.From(false), decoded["tBool"]);
        Assert.Equal(FieldValue.From((sbyte)-3), decoded["tSByte"]);
        Assert.Equal(FieldValue.From((byte)200), decoded["tByte"]);
        Assert.Equal(FieldValue.From((short)-300), decoded["tShort"]);
        Assert.Equal(FieldValue.From((ushort)60000), decoded["tUShort"]);
        Assert.Equal(FieldValue.From(-70000), decoded["tInt"]);
        Assert.Equal(FieldValue.From(-5_000_000_000L), decoded["tLong"]);
        Assert.Equal(FieldValue.From(-0.5f), decoded["tFloat"]);
        Assert.Equal(FieldValue.From(0.25d), decoded["tDouble"]);
        Assert.Equal(FieldValue.From(-123.45m), decoded["tDecimal"]);
        Assert.Equal(FieldValue.From("grüß"), decoded["tStr"]);
        Assert.Equal(FieldValue.From(new byte[] { 0x01, 0xFF }), decoded["tBin"]);
        Assert.Equal(FieldValue.From(new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.Zero)),
            decoded["tStamp"]);
        Assert.Equal(FieldValue.From(true), ((FieldTable)decoded["tNested"].Raw!)["flag"]);
        Assert.Equal('A', decoded["tList"].TypeLetter);
        Assert.Equal(FieldValue.Null, decoded["tNull"]);

        var rewriter = new ByteWriter();
        rewriter.WriteFieldTable(decoded);
        Assert.Equal(original, rewriter.ToArray());
    }

    [Fact]
    public void FieldValue_equality_is_letter_aware()
    {
        Assert.NotEqual(FieldValue.From(true), FieldValue.Null);
        Assert.NotEqual(FieldValue.From("x"), FieldValue.From(new byte[] { 0x78 }));
        Assert.NotEqual(FieldValue.From((byte)2), FieldValue.From((sbyte)2)); // 'B' vs 'b'
        Assert.NotEqual(FieldValue.From(7), FieldValue.From((long)7));        // 'I' vs 'l'
        Assert.Equal(FieldValue.From((long)7), FieldValue.From((long)7));
        Assert.Equal(FieldValue.From(-0.5f), FieldValue.From(-0.5f));
    }

    [Fact]
    public void Encoding_a_default_field_value_throws()
    {
        var table = new FieldTable();
        table.Add("k", default);

        Assert.Throws<AmqpEncodeException>(() => Encode(w => w.WriteFieldTable(table)));
    }

    [Fact]
    public void Field_table_key_over_255_utf8_bytes_rejected_on_write()
    {
        string longKey = new string('k', 256);
        var table = new FieldTable([new KeyValuePair<string, FieldValue>(longKey, FieldValue.Null)]);

        Assert.Throws<AmqpEncodeException>(() => Encode(w => w.WriteFieldTable(table)));
    }

    // ---------- field-table failure modes ----------

    [Fact]
    public void Unknown_type_letter_in_table_throws_amqp_decode_exception()
    {
        byte[] wire = Encode(w =>
        {
            w.WriteLong(1 + 1 + 1); // name "k"(2 bytes) + letter + one payload byte
            w.WriteShortstr("k");
            w.WriteOctet((byte)'Z');
            w.WriteOctet(0x01);
        });
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wire).TryReadFieldTable(out _));
    }

    [Fact]
    public void Entry_value_crossing_declared_table_length_throws()
    {
        byte[] wire = Encode(w =>
        {
            // name(2) + 'x'(1) + longstr length word(4) = the declared 7 content bytes,
            // but the longstr claims 10 more bytes that are not inside the table.
            w.WriteLong(7);
            w.WriteShortstr("k");
            w.WriteOctet((byte)'x');
            w.WriteLong(10);
        });
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wire).TryReadFieldTable(out _));
    }

    [Fact]
    public void Decimal_value_crossing_declared_table_length_throws()
    {
        byte[] wire = Encode(w =>
        {
            // name(2) + 'D'(1) = 3 real bytes inside the declared 4 content bytes,
            // but a decimal value needs a scale octet plus a 4-byte mantissa.
            w.WriteLong(4);
            w.WriteShortstr("k");
            w.WriteOctet((byte)'D');
            w.WriteOctet(0x02);
        });
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wire).TryReadFieldTable(out _));
    }

    [Fact]
    public void Nested_array_declared_length_shorter_than_its_items_throws()
    {
        byte[] wire = Encode(w =>
        {
            // name(2) + 'A'(1) + array length word(4) = 7 real bytes declared,
            // but the array content declares 16 bytes of items it does not contain.
            w.WriteLong(7);
            w.WriteShortstr("k");
            w.WriteOctet((byte)'A');
            w.WriteLong(16);
        });
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wire).TryReadFieldTable(out _));
    }

    [Fact]
    public void Trailing_bytes_beyond_declared_table_length_throw()
    {
        byte[] wire = Encode(w =>
        {
            // name(2) + 't'(1) + bool(1) = 4 real bytes, but declare 6 and pad with 2 junk bytes
            w.WriteLong(6);
            w.WriteShortstr("k");
            w.WriteOctet((byte)'t');
            w.WriteOctet(0x01);
            w.WriteLong(0x00001010); // only its first 2 bytes fall inside the declared length
        });
        Assert.Throws<AmqpDecodeException>(() => new ByteReader(wire).TryReadFieldTable(out _));
    }

    [Fact]
    public void Duplicate_keys_decode_last_value_wins()
    {
        byte[] first = EntryWire("a", FieldValue.From(1));
        byte[] second = EntryWire("a", FieldValue.From(2));
        byte[] content = [.. first, .. second];

        byte[] wire = [.. UInt32Bytes((uint)content.Length), .. content];
        Assert.True(TryDecode(wire, out var table));

        Assert.Single(table); // single key
        Assert.Equal(2, table["a"].Raw); // last value wins
    }

    // ---------- nesting depth guard ----------

    [Fact]
    public void Field_table_nesting_depth_is_bounded()
    {
        // Top table + at most 32 nested levels (33 tables total) parse fine;
        // one more nested level exceeds the cap and is rejected.
        var okReader = new ByteReader(NestedTableBytes(32));
        Assert.True(okReader.TryReadFieldTable(out var table));
        Assert.Single(table);

        Assert.Throws<AmqpDecodeException>(
            () => new ByteReader(NestedTableBytes(33)).TryReadFieldTable(out _));
    }

    // ---------- writer length bookkeeping ----------

    [Fact]
    public void Writer_length_tracks_written_bytes()
    {
        var writer = new ByteWriter();
        writer.WriteOctet(0x01);
        writer.WriteUShort(0x0203);
        writer.WriteShortstr("abc");
        writer.WriteBits(true, false, true);

        Assert.Equal(1 + 2 + 4 + 1, writer.Length);
        Assert.Equal(writer.Length, writer.ToArray().Length);
    }

    private static decimal ParseDecimal(string text) =>
        decimal.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
}
