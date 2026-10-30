using System.Buffers;
using System.Text.Json;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Protocol;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Task 3: the method registry is generated data from the canonical RabbitMQ 0-9-1 codegen
// fixture (tests/fixtures/rabbitmq-codegen.json, committed so CI stays offline). The
// verifier tests below re-derive each schema straight from that fixture — ids, field names,
// field order and field types, plus the bit-group classification — and assert the registry
// table equals the derivation method-for-method, so any hand-typing drift fails a run.
//
// Reading rules exercised here (documented in MethodRegistry.cs):
//  - bit fields that appear consecutively in a method's field list form one packed bitfield
//    group: a lone bit is WireType.Bit, the first bit of a run of 2+ is WireType.PackedBit
//    and the remaining bits are the WireType.DependingBit chain;
//  - Direction values are spec-side annotations (the fixture carries none); they are
//    spot-checked here, not fixture-verified;
//  - basic.publish carries 5 wire fields in the RabbitMQ dialect (ticket, exchange,
//    routing-key, mandatory, immediate), and basic.ack a single lone bit — the brief's
//    "6 fields" / "2 packed bits" phrasing is resolved against the fixture, and the
//    2-bit packed group is basic.nack, covered below.
public sealed class MethodRegistryTests
{
    private static readonly JsonDocument FixtureDoc = LoadFixtureDocument();

    private static JsonElement FixtureRoot => FixtureDoc.RootElement;

    private static JsonDocument LoadFixtureDocument()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "fixtures", "rabbitmq-codegen.json");
        return JsonDocument.Parse(File.ReadAllBytes(path));
    }

    // ---------- fixture side: independent derivation of what should be in the registry ----------

    private sealed record FixtureMethod(
        ushort ClassId,
        ushort MethodId,
        string FullName,
        IReadOnlyList<AmqpField> Fields);

    private static List<FixtureMethod> FixtureMethods()
    {
        var domains = new Dictionary<string, string>();
        foreach (var pair in FixtureRoot.GetProperty("domains").EnumerateArray())
        {
            domains[pair[0].GetString()!] = pair[1].GetString()!;
        }

        var methods = new List<FixtureMethod>();
        foreach (var cls in FixtureRoot.GetProperty("classes").EnumerateArray())
        {
            var classId = (ushort)cls.GetProperty("id").GetInt32();
            var className = cls.GetProperty("name").GetString()!;
            foreach (var method in cls.GetProperty("methods").EnumerateArray())
            {
                var baseTypes = new List<(string Name, string Type)>();
                if (method.TryGetProperty("arguments", out var arguments) && arguments.ValueKind == JsonValueKind.Array)
                {
                    foreach (var argument in arguments.EnumerateArray())
                    {
                        var baseType = argument.TryGetProperty("type", out var direct)
                            ? direct.GetString()!
                            : domains[argument.GetProperty("domain").GetString()!];
                        baseTypes.Add((argument.GetProperty("name").GetString()!, baseType));
                    }
                }

                // Consecutive 'bit' arguments pack into one octet group: a lone bit is a
                // full-octet single Bit, the first of a run of 2+ is PackedBit and the
                // rest are the DependingBit chain.
                var fields = new List<AmqpField>();
                var i = 0;
                while (i < baseTypes.Count)
                {
                    if (baseTypes[i].Type != "bit")
                    {
                        fields.Add(new AmqpField(baseTypes[i].Name, BaseTypeToWire(baseTypes[i].Type)));
                        i++;
                        continue;
                    }

                    var run = 0;
                    while (i + run < baseTypes.Count && baseTypes[i + run].Type == "bit")
                    {
                        run++;
                    }

                    for (var k = 0; k < run; k++)
                    {
                        var wireType = run == 1 ? WireType.Bit : k == 0 ? WireType.PackedBit : WireType.DependingBit;
                        fields.Add(new AmqpField(baseTypes[i + k].Name, wireType));
                    }

                    i += run;
                }

                var name = method.GetProperty("name").GetString()!;
                methods.Add(new FixtureMethod(
                    classId,
                    (ushort)method.GetProperty("id").GetInt32(),
                    className + "." + name,
                    fields));
            }
        }

        return methods;
    }

    private static WireType BaseTypeToWire(string baseType) => baseType switch
    {
        "octet" => WireType.Octet,
        "short" => WireType.Short,
        "long" => WireType.Long,
        "longlong" => WireType.LongLong,
        "shortstr" => WireType.Shortstr,
        "longstr" => WireType.Longstr,
        "table" => WireType.Table,
        "timestamp" => WireType.Timestamp,
        _ => throw new InvalidOperationException($"unresolved base type {baseType}"),
    };

    private static Dictionary<string, ushort> FixtureConstants()
    {
        var constants = new Dictionary<string, ushort>();
        foreach (var constant in FixtureRoot.GetProperty("constants").EnumerateArray())
        {
            constants[constant.GetProperty("name").GetString()!] = (ushort)constant.GetProperty("value").GetInt32();
        }

        return constants;
    }

    private static JsonElement FixtureBasicProperties()
    {
        foreach (var cls in FixtureRoot.GetProperty("classes").EnumerateArray())
        {
            if (cls.GetProperty("name").GetString() == "basic")
            {
                return cls.GetProperty("properties");
            }
        }

        throw new InvalidOperationException("fixture has no basic class properties");
    }

    // The M0 method set from the plan's Global Constraints — every entry must resolve.
    private static readonly string[] M0MethodNames =
    [
        "connection.start", "connection.start-ok", "connection.secure", "connection.secure-ok",
        "connection.tune", "connection.tune-ok", "connection.open", "connection.open-ok",
        "connection.close", "connection.close-ok", "connection.blocked", "connection.unblocked",
        "channel.open", "channel.open-ok", "channel.flow", "channel.flow-ok", "channel.close", "channel.close-ok",
        "exchange.declare", "exchange.declare-ok",
        "queue.declare", "queue.declare-ok", "queue.bind", "queue.bind-ok", "queue.unbind", "queue.unbind-ok",
        "basic.qos", "basic.qos-ok", "basic.consume", "basic.consume-ok", "basic.cancel", "basic.cancel-ok",
        "basic.publish", "basic.deliver", "basic.get", "basic.get-ok", "basic.get-empty",
        "basic.ack", "basic.nack", "basic.reject", "basic.recover", "basic.recover-ok",
    ];

    // ---------- registry content: fixture verbatim ----------

    [Fact]
    public void Registry_matches_the_fixture_for_every_method()
    {
        var expected = FixtureMethods();
        Assert.Equal(expected.Count, MethodRegistry.All.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            var schema = MethodRegistry.All[i];
            Assert.Equal(expected[i].ClassId, schema.ClassId);
            Assert.Equal(expected[i].MethodId, schema.MethodId);
            Assert.Equal(expected[i].FullName, schema.Name);
            Assert.Equal(expected[i].Fields.Count, schema.Fields.Count);
            for (var f = 0; f < expected[i].Fields.Count; f++)
            {
                Assert.Equal(expected[i].Fields[f].Name, schema.Fields[f].Name);
                Assert.Equal(expected[i].Fields[f].Type, schema.Fields[f].Type);
            }
        }
    }

    [Fact]
    public void Registry_tables_have_no_duplicate_methods()
    {
        var expected = FixtureMethods();
        Assert.Equal(expected.Select(m => m.FullName), expected.Select(m => m.FullName).Distinct(StringComparer.Ordinal));
        Assert.Equal(MethodRegistry.All.Select(m => m.Name), MethodRegistry.All.Select(m => m.Name).Distinct(StringComparer.Ordinal));
    }

    [Fact]
    public void Lookup_by_key_and_by_name_agree_with_the_fixture()
    {
        foreach (var expected in FixtureMethods())
        {
            var schema = MethodRegistry.Schema(expected.ClassId, expected.MethodId);
            Assert.NotNull(schema);
            Assert.Equal(expected.FullName, schema!.Name);
            Assert.True(MethodRegistry.TryGetByName(expected.FullName, out var byName));
            Assert.Same(schema, byName);
        }
    }

    [Fact]
    public void Unknown_ids_do_not_resolve_but_all_fixture_methods_do()
    {
        Assert.Null(MethodRegistry.Schema(99, 10)); // class absent from the fixture
        Assert.Null(MethodRegistry.Schema(60, 777)); // method absent from the fixture
        Assert.False(MethodRegistry.TryGetByName("connection.nope", out _));

        // Data-driven resolution beyond the M0 set: fixture-defined methods resolve even
        // though the server will answer not-implemented for them.
        Assert.Equal("tx.select", MethodRegistry.Schema(90, 10)!.Name);
        Assert.Equal("confirm.select", MethodRegistry.Schema(85, 10)!.Name);
        Assert.Equal("access.request", MethodRegistry.Schema(30, 10)!.Name);
        // The fixture's (quirky) id 51 for exchange.unbind-ok is preserved verbatim.
        Assert.Equal("exchange.unbind-ok", MethodRegistry.Schema(40, 51)!.Name);
    }

    // ---------- M0 coverage ----------

    [Fact]
    public void Every_M0_method_is_in_the_registry()
    {
        Assert.Equal(42, M0MethodNames.Length);
        foreach (var name in M0MethodNames)
        {
            Assert.True(MethodRegistry.TryGetByName(name, out var schema), name);
            Assert.NotNull(schema!.Fields);
            // Field lists are verified exhaustively against the fixture in the zip test;
            // empty replies (e.g. recover-ok) resolve with zero fields and that is correct.
        }
    }

    // ---------- error constants ----------

    [Fact]
    public void Error_codes_match_the_fixture_constants()
    {
        var constants = FixtureConstants();
        foreach (var (specName, actual) in new Dictionary<string, ushort>
                 {
                     ["invalid-path"] = AmqpConstants.InvalidPath,
                     ["frame-error"] = AmqpConstants.FrameError,
                     ["syntax-error"] = AmqpConstants.SyntaxError,
                     ["not-allowed"] = AmqpConstants.NotAllowed,
                     ["precondition-failed"] = AmqpConstants.PreconditionFailed,
                     ["not-found"] = AmqpConstants.NotFound,
                     ["access-refused"] = AmqpConstants.AccessRefused,
                     ["unexpected-frame"] = AmqpConstants.UnexpectedFrame,
                     ["internal-error"] = AmqpConstants.InternalError,
                     ["not-implemented"] = AmqpConstants.NotImplemented,
                     ["content-too-large"] = AmqpConstants.ContentTooLarge,
                     ["connection-forced"] = AmqpConstants.ConnectionForced,
                 })
        {
            var fixtureName = specName.ToUpperInvariant();
            Assert.True(constants.TryGetValue(fixtureName, out var fromFixture), fixtureName);
            Assert.Equal(fromFixture, actual);
        }
    }

    // ---------- content properties ----------

    [Fact]
    public void Content_properties_match_the_fixture_basic_class()
    {
        var basicProperties = FixtureBasicProperties();
        var properties = MethodRegistry.ContentProperties(60);
        Assert.NotNull(properties);
        var expected = basicProperties.EnumerateArray().ToArray();
        Assert.Equal(expected.Length, properties!.Count);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].GetProperty("name").GetString(), properties[i].Name);
            var baseType = expected[i].TryGetProperty("type", out var direct)
                ? direct.GetString()!
                : FixtureDomains()[expected[i].GetProperty("domain").GetString()!];
            Assert.Equal(BaseTypeToWire(baseType), properties[i].Type);
            // Flag ordinal follows the fixture property order; the encoder maps ordinal n
            // to the flag-word bit (15 - n), matching ContentHeaderCodec's flag masks.
            Assert.Equal(i, properties[i].FlagOrdinal);
        }

        Assert.Null(MethodRegistry.ContentProperties(10));
        Assert.Null(MethodRegistry.ContentProperties(99));
    }

    private static Dictionary<string, string> FixtureDomains()
    {
        var domains = new Dictionary<string, string>();
        foreach (var pair in FixtureRoot.GetProperty("domains").EnumerateArray())
        {
            domains[pair[0].GetString()!] = pair[1].GetString()!;
        }

        return domains;
    }

    // ---------- spot schemas ----------

    [Fact]
    public void Connection_start_lists_version_major_first()
    {
        var start = MethodRegistry.Schema(10, 10)!;
        Assert.Equal("connection.start", start.Name);
        Assert.Equal(Direction.ServerToClient, start.Direction);
        Assert.Equal(("version-major", WireType.Octet), (start.Fields[0].Name, start.Fields[0].Type));
        Assert.Equal(
            new[] { "version-major", "version-minor", "server-properties", "mechanisms", "locales" },
            start.Fields.Select(f => f.Name));
        Assert.Equal(
            new[] { WireType.Octet, WireType.Octet, WireType.Table, WireType.Longstr, WireType.Longstr },
            start.Fields.Select(f => f.Type));
    }

    [Fact]
    public void Queue_declare_ends_with_the_arguments_table()
    {
        var declare = MethodRegistry.Schema(50, 10)!;
        Assert.Equal(8, declare.Fields.Count); // ticket, queue, 5 bits, arguments
        Assert.Equal(new AmqpField("ticket", WireType.Short), declare.Fields[0]);
        Assert.Equal(WireType.PackedBit, declare.Fields[2].Type); // passive, first of a 5-bit run
        Assert.Equal(WireType.DependingBit, declare.Fields[6].Type);
        Assert.Equal(("arguments", WireType.Table), (declare.Fields[^1].Name, declare.Fields[^1].Type));
        Assert.Equal(new[] { "passive", "durable", "exclusive", "auto-delete", "nowait", "arguments" }, declare.Fields.Skip(2).Select(f => f.Name));
    }

    [Fact]
    public void Basic_publish_carries_the_fixture_dialect_s_five_fields()
    {
        var publish = MethodRegistry.Schema(60, 40)!;
        Assert.Equal(
            new[] { "ticket", "exchange", "routing-key", "mandatory", "immediate" },
            publish.Fields.Select(f => f.Name));
        Assert.Equal(
            new[] { WireType.Short, WireType.Shortstr, WireType.Shortstr, WireType.PackedBit, WireType.DependingBit },
            publish.Fields.Select(f => f.Type));
        Assert.True(publish.HasContent);
    }

    [Fact]
    public void Basic_ack_has_a_single_lone_bit_and_nack_the_two_bit_packed_group()
    {
        var ack = MethodRegistry.Schema(60, 80)!;
        Assert.Equal(
            new[] { new AmqpField("delivery-tag", WireType.LongLong), new AmqpField("multiple", WireType.Bit) },
            ack.Fields);

        var nack = MethodRegistry.Schema(60, 120)!;
        Assert.Equal(
            new[]
            {
                new AmqpField("delivery-tag", WireType.LongLong),
                new AmqpField("multiple", WireType.PackedBit),
                new AmqpField("requeue", WireType.DependingBit),
            },
            nack.Fields);
        Assert.False(ack.HasContent);
        Assert.False(nack.HasContent);
    }

    [Fact]
    public void Content_flows_are_marked_on_the_registry()
    {
        Assert.True(MethodRegistry.Schema(60, 60)!.HasContent); // basic.deliver
        Assert.True(MethodRegistry.Schema(60, 50)!.HasContent); // basic.return
        Assert.True(MethodRegistry.Schema(60, 71)!.HasContent); // basic.get-ok
        Assert.False(MethodRegistry.Schema(50, 10)!.HasContent); // queue.declare
        Assert.False(MethodRegistry.Schema(60, 10)!.HasContent); // basic.qos
        Assert.False(MethodRegistry.Schema(10, 10)!.HasContent); // connection.start
    }

    // ---------- direction spot checks (spec-side annotation, not fixture-derived) ----------

    [Fact]
    public void Direction_spot_checks()
    {
        Assert.Equal(Direction.ServerToClient, MethodRegistry.Schema(10, 10)!.Direction); // start
        Assert.Equal(Direction.ClientToServer, MethodRegistry.Schema(10, 11)!.Direction); // start-ok
        Assert.Equal(Direction.ServerToClient, MethodRegistry.Schema(10, 30)!.Direction); // tune
        Assert.Equal(Direction.ClientToServer, MethodRegistry.Schema(10, 31)!.Direction); // tune-ok
        // connection.close is client-sent in this registry's semantics; the server also sends
        // it for hard errors — that FSM behavior lives in the server, not the registry.
        Assert.Equal(Direction.ClientToServer, MethodRegistry.Schema(10, 50)!.Direction);
        Assert.Equal(Direction.ServerToClient, MethodRegistry.Schema(10, 51)!.Direction);
        Assert.Equal(Direction.ClientToServer, MethodRegistry.Schema(60, 40)!.Direction); // publish
        Assert.Equal(Direction.ServerToClient, MethodRegistry.Schema(60, 60)!.Direction); // deliver
        Assert.Equal(Direction.ServerToClient, MethodRegistry.Schema(50, 11)!.Direction); // queue.declare-ok
    }

    // ---------- method-arg codec round trips (real M0 frames) ----------

    // Golden method payload for connection.tune: class-id/method-id, channel-max (short 0),
    // frame-max (long 131072 = 0x00020000), heartbeat (short 60).
    private static readonly byte[] TunePayload =
    [
        0x00, 0x0A, 0x00, 0x1E, // class 10, method 30
        0x00, 0x00, // channel-max: short 0
        0x00, 0x02, 0x00, 0x00, // frame-max: long 131072
        0x00, 0x3C, // heartbeat: short 60
    ];

    /// <summary>Decodes a full method frame payload (the codec writes class-id, method-id
    /// and then the arguments): consumes and checks the id pair, runs the codec, and
    /// asserts the whole payload was consumed.</summary>
    private static AmqpMethod DecodeMethodPayload(byte[] wire, MethodSchema schema)
    {
        var reader = new ByteReader(wire);
        var classId = reader.ReadUShort();
        var methodId = reader.ReadUShort();
        Assert.Equal(schema.ClassId, classId);
        Assert.Equal(schema.MethodId, methodId);
        var decoded = MethodCodec.ReadMethod(ref reader, schema);
        Assert.Equal(wire.Length, reader.Offset);
        return decoded;
    }

    [Fact]
    public void Connection_tune_round_trips_at_golden_bytes()
    {
        var schema = MethodRegistry.Schema(10, 30)!;
        var method = NewMethod(schema);
        method.Set("channel-max", (short)0);
        method.Set("frame-max", 131072);
        method.Set("heartbeat", (short)60);

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        Assert.Equal(TunePayload, writer.ToArray());

        var decoded = DecodeMethodPayload(TunePayload, schema);
        Assert.Equal((short)0, (short)decoded.Get("channel-max")!);
        Assert.Equal(131072, (int)decoded.Get("frame-max")!);
        Assert.Equal((short)60, (short)decoded.Get("heartbeat")!);
        Assert.Equal(TunePayload, Reencoded(decoded));
    }

    private static AmqpMethod NewMethod(MethodSchema schema) => new(schema);

    private static byte[] Reencoded(AmqpMethod method)
    {
        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        return writer.ToArray();
    }

    [Fact]
    public void Connection_tune_round_trips_through_a_full_wire_frame()
    {
        // Frame the tune payload on channel 7 with the Task-2 frame writer, decode it with
        // the Task-2 frame decoder, then decode the method payload with the Task-3 codec.
        var frameWriter = new ByteWriter();
        FrameWriter.WriteMethod(ref frameWriter, 7, 10, 30, TunePayload.AsSpan(4));
        var wire = frameWriter.ToArray();

        var buffer = new ReadOnlySequence<byte>(wire);
        var frames = new List<AmqpFrame>();
        Assert.Equal(DecodeMode.NeedMore, FrameDecoder.ReadFrames(ref buffer, frames, AmqpFrame.FrameMinSize));
        var frame = Assert.Single(frames);
        Assert.Equal(AmqpFrame.FrameMethod, frame.Type);
        Assert.Equal((ushort)7, frame.Channel);

        var reader = new ByteReader(frame.Payload.Span);
        var classId = reader.ReadUShort();
        var methodId = reader.ReadUShort();
        Assert.Equal(10, classId);
        Assert.Equal(30, methodId);
        var decoded = MethodCodec.ReadMethod(ref reader, MethodRegistry.Schema(10, 30)!);
        Assert.Equal(frame.Payload.Length, reader.Offset);
        Assert.Equal(131072, (int)decoded.Get("frame-max")!);
    }

    [Fact]
    public void Queue_declare_round_trips_a_field_table_argument()
    {
        var schema = MethodRegistry.Schema(50, 10)!;
        var method = NewMethod(schema);
        method.Set("ticket", (short)0);
        method.Set("queue", "q1");
        method.Set("passive", false);
        method.Set("durable", true);
        method.Set("exclusive", true);
        method.Set("auto-delete", false);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable
        {
            { "x-int", FieldValue.From(7) },
            { "y-str", FieldValue.From("s") },
            { "z-bool", FieldValue.From(true) },
            { "nested", FieldValue.From(new FieldTable { { "inner", FieldValue.From(true) } }) },
        });

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        var payload = writer.ToArray();

        // Hypothesis check on the packed-5-bit octet: LSB-first, [passive=0, durable=1,
        // exclusive=1, auto-delete=0, nowait=0] → 0b00110 = 0x06, one octet, then the table.
        Assert.Equal(0x06, payload[9]);
        Assert.Equal(0x00, payload[10]); // table length starts high byte — table follows the bit group

        var decoded = DecodeMethodPayload(payload, schema);
        Assert.Equal("q1", (string)decoded.Get("queue")!);
        Assert.False((bool)decoded.Get("passive")!);
        Assert.True((bool)decoded.Get("durable")!);
        Assert.True((bool)decoded.Get("exclusive")!);
        var table = Assert.IsType<FieldTable>(decoded.Get("arguments"));
        Assert.Equal(4, table.Count);
        Assert.Equal(7, (int)table["x-int"].Raw!);
        Assert.Equal("s", (string)table["y-str"].Raw!);
        Assert.True((bool)table["z-bool"].Raw!);
        var nested = Assert.IsType<FieldTable>(table["nested"].Raw);
        Assert.Single(nested);
        Assert.True((bool)nested["inner"].Raw!);
        Assert.Equal(payload, Reencoded(decoded)); // byte-identical re-encode incl. wire key order
    }

    [Fact]
    public void Basic_publish_round_trips_all_five_fields()
    {
        var schema = MethodRegistry.Schema(60, 40)!;
        var method = NewMethod(schema);
        method.Set("ticket", (short)0);
        method.Set("exchange", ""); // default exchange
        method.Set("routing-key", "rk");
        method.Set("mandatory", true);
        method.Set("immediate", false);

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        // class/method | ticket short 0 | empty exchange shortstr | "rk" shortstr | bits octet.
        byte[] expected = [0x00, 0x3C, 0x00, 0x28, 0x00, 0x00, 0x00, 0x02, 0x72, 0x6B, 0x01];
        Assert.Equal(expected, writer.ToArray());

        var decoded = DecodeMethodPayload(expected, schema);
        Assert.Equal("", (string)decoded.Get("exchange")!);
        Assert.Equal("rk", (string)decoded.Get("routing-key")!);
        Assert.True((bool)decoded.Get("mandatory")!);
        Assert.False((bool)decoded.Get("immediate")!);
        Assert.Equal(expected, Reencoded(decoded));
    }

    [Fact]
    public void Basic_ack_round_trips_the_lone_bit()
    {
        var schema = MethodRegistry.Schema(60, 80)!;
        var method = NewMethod(schema);
        method.Set("delivery-tag", 0x0000_0000_0001_0203L);
        method.Set("multiple", true);

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        // class/method | delivery-tag longlong 0x0000000000010203 | bits octet 0x01.
        byte[] expected = [0x00, 0x3C, 0x00, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x01];
        Assert.Equal(expected, writer.ToArray());
        Assert.Equal(13, writer.Length); // 4 header + 8 longlong + 1 lone-bit octet

        var decoded = DecodeMethodPayload(expected, schema);
        Assert.Equal(0x0000_0000_0001_0203L, (long)decoded.Get("delivery-tag")!);
        Assert.True((bool)decoded.Get("multiple")!);
        Assert.Equal(expected, Reencoded(decoded));
    }

    [Fact]
    public void Basic_nack_round_trips_the_two_bit_packed_group()
    {
        var schema = MethodRegistry.Schema(60, 120)!;
        var method = NewMethod(schema);
        method.Set("delivery-tag", 7L);
        method.Set("multiple", true);
        method.Set("requeue", true);

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        // class/method | delivery-tag longlong 7 | ONE bit octet holding both bits, LSB-first.
        byte[] expected = [0x00, 0x3C, 0x00, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x03];
        Assert.Equal(expected, writer.ToArray());
        Assert.Equal(13, writer.Length);

        var decoded = DecodeMethodPayload(expected, schema);
        Assert.Equal(7L, (long)decoded.Get("delivery-tag")!);
        Assert.True((bool)decoded.Get("multiple")!);
        Assert.True((bool)decoded.Get("requeue")!);
        Assert.Equal(expected, Reencoded(decoded));
    }

    [Fact]
    public void Basic_consume_round_trips_bits_before_a_table()
    {
        var schema = MethodRegistry.Schema(60, 20)!;
        var method = NewMethod(schema);
        method.Set("ticket", (short)0);
        method.Set("queue", "q");
        method.Set("consumer-tag", "ct");
        method.Set("no-local", false);
        method.Set("no-ack", true);
        method.Set("exclusive", true);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable { { "k", FieldValue.From("v") } });

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        var payload = writer.ToArray();

        // 4-bit run [no-local=0, no-ack=1, exclusive=1, nowait=0] → 0b0110 = 0x06, then the
        // table. The bit octet sits at index 11: 4 header + ticket(2) + "q"(2) + "ct"(3).
        Assert.Equal(0x06, payload[11]);

        var decoded = DecodeMethodPayload(payload, schema);
        Assert.Equal("q", (string)decoded.Get("queue")!);
        Assert.Equal("ct", (string)decoded.Get("consumer-tag")!);
        Assert.Equal(WireType.PackedBit, schema.Fields[3].Type); // no-local, first of the 4-bit run
        Assert.Equal(WireType.DependingBit, schema.Fields[4].Type); // no-ack
        Assert.Single(Assert.IsType<FieldTable>(decoded.Get("arguments")));
        Assert.Equal(payload, Reencoded(decoded));
    }

    // ---------- zero-value safety ----------

    [Fact]
    public void Missing_values_encode_as_the_zero_form()
    {
        // A registry-built method with nothing set still produces decode-safe wire: null
        // values encode as the zero form (e.g. heartbeat 0 legitimately means "off") so a
        // partially populated method frame can never desynchronize both peers.
        var schema = MethodRegistry.Schema(10, 30)!;
        var method = NewMethod(schema);
        byte[] expected =
        [
            0x00, 0x0A, 0x00, 0x1E,
            0x00, 0x00, // channel-max short 0
            0x00, 0x00, 0x00, 0x00, // frame-max long 0
            0x00, 0x00, // heartbeat short 0
        ];

        var writer = new ByteWriter();
        MethodCodec.WriteMethod(ref writer, method);
        Assert.Equal(expected, writer.ToArray());

        var decoded = DecodeMethodPayload(expected, schema);
        Assert.Equal((short)0, (short)decoded.Get("channel-max")!);
        Assert.Equal(expected, Reencoded(decoded));
    }

    // ---------- error handling ----------

    [Fact]
    public void ReadMethod_throws_AmqpDecodeException_on_truncated_payloads()
    {
        var schema = MethodRegistry.Schema(10, 30)!;
        var full = TunePayload;
        for (var cut = 1; cut <= 8; cut++)
        {
            var truncated = full[0..^cut];
            Assert.Throws<AmqpDecodeException>(() =>
            {
                var reader = new ByteReader(truncated);
                reader.ReadUShort(); // class-id
                reader.ReadUShort(); // method-id
                _ = MethodCodec.ReadMethod(ref reader, schema);
            });
        }
    }

    [Fact]
    public void AmqpMethod_get_set_throws_for_unknown_fields()
    {
        var schema = MethodRegistry.Schema(10, 30)!;
        var method = NewMethod(schema);
        Assert.Throws<KeyNotFoundException>(() => method.Get("not-a-field"));
        Assert.Throws<KeyNotFoundException>(() => method.Set("not-a-field", 1));
        method.Set("heartbeat", (short)5);
        Assert.Equal((short)5, (short)method.Get("heartbeat")!);
        Assert.Equal(10, method.ClassId);
        Assert.Equal(30, method.MethodId);
    }
}
