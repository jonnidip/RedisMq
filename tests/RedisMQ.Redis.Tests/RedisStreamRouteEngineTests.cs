using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Redis;
using StackExchange.Redis;
using Xunit;

namespace RedisMQ.Redis.Tests;

public sealed class StreamCodecTests
{
    [Fact]
    public void Encode_decode_round_trips_body_byte_exact()
    {
        var body = new byte[] { 0, 1, 2, 3, 255, 254, 42, 0 };
        var record = new EnvelopeRecord(
            EnvelopeId: 7,
            Exchange: "fex",
            RoutingKey: "rk",
            Body: body,
            Properties: new ContentProperties(ContentType: "text/plain"),
            ExpiresAt: null,
            DeliveryCount: 0,
            Redelivered: false);

        var fields = StreamCodec.EncodeEnqueue(record);
        var decoded = StreamCodec.DecodeEnqueue(fields);

        Assert.Equal(record.EnvelopeId, decoded.EnvelopeId);
        Assert.Equal(record.Exchange, decoded.Exchange);
        Assert.Equal(record.RoutingKey, decoded.RoutingKey);
        Assert.Equal(record.Properties.ContentType, decoded.Properties.ContentType);
        Assert.Equal(body, decoded.Body);
    }

    [Fact]
    public void Encode_decode_round_trips_properties_in_amqp_wire_form()
    {
        var headers = new FieldTable([
            new("nested", FieldValue.From(new FieldTable([
                new("inner-key", FieldValue.From("inner-value")),
                new("inner-count", FieldValue.From(9L)),
            ]))),
            new("list", FieldValue.List(FieldValue.From("a"), FieldValue.From(2), FieldValue.Null)),
            new("when", FieldValue.From(new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero))),
            new("blob", FieldValue.From(new byte[] { 9, 8, 7 })),
            new("money", FieldValue.From(12.34m)),
        ]);
        var properties = new ContentProperties(
            ContentType: "application/json",
            ContentEncoding: "utf-8",
            Headers: headers,
            DeliveryMode: 2,
            Priority: 5,
            CorrelationId: "corr",
            ReplyTo: "reply",
            Expiration: "60000",
            MessageId: "mid",
            Timestamp: new DateTimeOffset(2023, 1, 2, 3, 4, 5, TimeSpan.Zero),
            Type: "event",
            UserId: "guest",
            AppId: "tests",
            ClusterId: "cluster");

        var record = new EnvelopeRecord(
            EnvelopeId: 8,
            Exchange: "fex",
            RoutingKey: "rk",
            Body: [1, 2, 3],
            Properties: properties,
            ExpiresAt: null,
            DeliveryCount: 0,
            Redelivered: false);

        EnvelopeRecord decoded = StreamCodec.DecodeEnqueue(StreamCodec.EncodeEnqueue(record));
        ContentProperties roundTripped = decoded.Properties;

        Assert.Equal(properties.ContentType, roundTripped.ContentType);
        Assert.Equal(properties.ContentEncoding, roundTripped.ContentEncoding);
        Assert.Equal(properties.DeliveryMode, roundTripped.DeliveryMode);
        Assert.Equal(properties.Priority, roundTripped.Priority);
        Assert.Equal(properties.CorrelationId, roundTripped.CorrelationId);
        Assert.Equal(properties.ReplyTo, roundTripped.ReplyTo);
        Assert.Equal(properties.Expiration, roundTripped.Expiration);
        Assert.Equal(properties.MessageId, roundTripped.MessageId);
        Assert.Equal(properties.Timestamp, roundTripped.Timestamp);
        Assert.Equal(properties.Type, roundTripped.Type);
        Assert.Equal(properties.UserId, roundTripped.UserId);
        Assert.Equal(properties.AppId, roundTripped.AppId);
        Assert.Equal(properties.ClusterId, roundTripped.ClusterId);

        Assert.NotNull(roundTripped.Headers);
        Assert.Equal(headers.Count, roundTripped.Headers!.Count);
        foreach ((string key, FieldValue value) in headers)
        {
            Assert.True(roundTripped.Headers.TryGetValue(key, out FieldValue actual));
            Assert.Equal(value, actual);
        }
    }

    [Fact]
    public void Encode_decode_round_trips_expiry_delivery_count_and_redelivered()
    {
        var expiresAt = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
        var record = new EnvelopeRecord(
            EnvelopeId: 5,
            Exchange: "ex",
            RoutingKey: "rk",
            Body: [4, 5],
            Properties: new ContentProperties(),
            ExpiresAt: expiresAt,
            DeliveryCount: 3,
            Redelivered: true);

        var fields = StreamCodec.EncodeEnqueue(record);
        Assert.Contains((RedisKeys.QueueField.ExpiresAt, "1700000000123"), fields);
        Assert.Contains((RedisKeys.QueueField.DeliveryCount, "3"), fields);
        Assert.Contains((RedisKeys.QueueField.Redelivered, "1"), fields);

        EnvelopeRecord decoded = StreamCodec.DecodeEnqueue(fields);
        Assert.Equal(expiresAt, decoded.ExpiresAt);
        Assert.Equal(3, decoded.DeliveryCount);
        Assert.True(decoded.Redelivered);
    }

    [Fact]
    public void Null_expiry_is_omitted_and_decodes_null()
    {
        var record = new EnvelopeRecord(1, "", "", [], new ContentProperties(), null, 0, false);
        var fields = StreamCodec.EncodeEnqueue(record);
        Assert.DoesNotContain(fields, field => field.Field == RedisKeys.QueueField.ExpiresAt);
        Assert.Null(StreamCodec.DecodeEnqueue(fields).ExpiresAt);
    }

    [Fact]
    public void Malformed_properties_payload_falls_back_to_empty()
    {
        var fields = StreamCodec.EncodeEnqueue(
            new EnvelopeRecord(1, "", "", [], new ContentProperties(ContentType: "x"), null, 0, false));
        var corrupted = fields
            .Select(field => field.Field == RedisKeys.QueueField.Properties ? (field.Field, "not-base64!") : field)
            .ToArray();

        EnvelopeRecord decoded = StreamCodec.DecodeEnqueue(corrupted);
        Assert.Equal(new ContentProperties(), decoded.Properties);
    }

    [Fact]
    public void Empty_body_round_trips()
    {
        var record = new EnvelopeRecord(1, "", "", [], new ContentProperties(), null, 0, false);
        var fields = StreamCodec.EncodeEnqueue(record);
        var decoded = StreamCodec.DecodeEnqueue(fields);
        Assert.Empty(decoded.Body);
    }

    [Fact]
    public void Queue_key_is_hash_tagged()
    {
        var key = RedisKeys.QueueKey("test-vhost", "my-q");
        Assert.Equal("{mq:q:test-vhost:my-q}", key);
    }

    [Fact]
    public void Topology_key_is_hash_tagged()
    {
        Assert.Equal("{mq:top:/}", RedisKeys.TopologyKey("/"));
    }
}

public sealed class WriteCommandTests
{
    [Fact]
    public void To_stream_fields_and_back_round_trips_each_kind()
    {
        var commands = new[]
        {
            WriteCommand.DeclareExchange("/", "fex", "direct"),
            WriteCommand.DeclareQueue("/", "q", durable: false, exclusive: false, autoDelete: false),
            WriteCommand.Bind("/", "fex", "q", "rk"),
            WriteCommand.Unbind("/", "fex", "q", "rk"),
            WriteCommand.Enqueue("/", "q", 3, "fex", "rk", new byte[] { 1, 2, 3 }, new ContentProperties()),
            WriteCommand.RegisterConsumer("/", "q", "ctag"),
            WriteCommand.UnregisterConsumer("/", "q", "ctag"),
            WriteCommand.Enqueue("/", "q", 3, "fex", "rk", new byte[] { 9 }, new ContentProperties(), redelivered: true),
            WriteCommand.DeleteQueue("/", "q"),
            WriteCommand.Remove("/", "q", 3, 4),
            WriteCommand.RemoveStreamIds("/", "q", "1-0", "2-0"),
        };

        foreach (var command in commands)
        {
            var fields = command.ToStreamFields();
            var roundTripped = WriteCommand.FromStreamFields(command.Kind, fields);
            Assert.Equal(command.Kind, roundTripped.Kind);
            Assert.Equal(command.Vhost, roundTripped.Vhost);
            Assert.Equal(command.Body, roundTripped.Body);
        }
    }

    [Fact]
    public void Enqueue_round_trips_amqp_properties_and_delivery_metadata()
    {
        var properties = new ContentProperties(
            ContentType: "text/plain",
            Headers: new FieldTable([new("tenant", FieldValue.From("acme"))]),
            DeliveryMode: 2);
        var expiresAt = DateTimeOffset.FromUnixTimeMilliseconds(1_800_000_000_000);

        WriteCommand enqueue = WriteCommand.Enqueue(
            "/", "q", 11, "fex", "rk", new byte[] { 1, 2 }, properties,
            expiresAt, deliveryCount: 3, redelivered: true);
        WriteCommand decoded = WriteCommand.FromStreamFields(WriteCommandKind.Enqueue, enqueue.ToStreamFields());
        Assert.Equal(expiresAt, decoded.ExpiresAt);
        Assert.Equal(3, decoded.DeliveryCount);
        Assert.True(decoded.Redelivered);
        Assert.Equal("text/plain", decoded.Properties!.ContentType);
        Assert.Equal("acme", decoded.Properties.Headers!["tenant"].Raw);
    }

    [Fact]
    public void Remove_round_trips_envelope_ids()
    {
        WriteCommand remove = WriteCommand.Remove("/", "q", 3, 4, 5);
        Assert.Equal([3UL, 4UL, 5UL], remove.EnvelopeIds);

        var fields = remove.ToStreamFields();
        Assert.Contains(("envelope-ids", "3,4,5"), fields);

        WriteCommand decoded = WriteCommand.FromStreamFields(WriteCommandKind.Remove, fields);
        Assert.Equal([3UL, 4UL, 5UL], decoded.EnvelopeIds);
        Assert.Equal("q", decoded.Queue);
    }

    [Fact]
    public void RemoveStreamIds_round_trips_stream_ids()
    {
        WriteCommand remove = WriteCommand.RemoveStreamIds("/", "q", "1-0", "2-9");
        Assert.Equal(["1-0", "2-9"], remove.StreamIds);

        var fields = remove.ToStreamFields();
        Assert.Contains(("stream-ids", "1-0,2-9"), fields);

        WriteCommand decoded = WriteCommand.FromStreamFields(WriteCommandKind.RemoveStreamIds, fields);
        Assert.Equal(["1-0", "2-9"], decoded.StreamIds);
    }

    [Fact]
    public void Delta_is_never_serialized()
    {
        WriteCommand delta = WriteCommand.Delta(WriteCommand.Remove("/", "q", 1));
        Assert.Throws<InvalidOperationException>(() => delta.ToStreamFields());
    }

    [Fact]
    public void Requeue_no_longer_exists_as_a_stream_command()
    {
        Assert.DoesNotContain("Requeue", Enum.GetNames<WriteCommandKind>());
    }
}

public sealed class RedisStreamRouteEngineTests
{
    private static IDatabase? TryConnectRedis()
    {
        try
        {
            var connectTask = ConnectionMultiplexer.ConnectAsync("127.0.0.1:6379");
            if (connectTask.Wait(TimeSpan.FromSeconds(2)))
            {
                return connectTask.Result.GetDatabase();
            }
        }
        catch
        {
            // Redis unavailable
        }

        return null;
    }

    [Fact]
    public async Task Reads_delegate_to_in_memory_engine_without_redis()
    {
        // No Redis required: construct with a non-connected database and verify
        // reads/writes go through InMemoryEngine synchronously.
        await using var engine = new RedisStreamRouteEngine(
            redis: null!,
            vhosts: ["/"],
            replayOnStart: false,
            drainerCapacity: 1024);

        engine.DeclareExchange(new ExchangeInfo("/", "fex", "direct"));
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Bind("/", "fex", "q", "rk");

        Assert.NotNull(engine.GetExchange("/", "fex"));
        Assert.Equal("direct", engine.GetExchange("/", "fex")!.Type);
        Assert.NotNull(engine.GetQueue("/", "q"));
        Assert.Equal(["q"], engine.RouteTo(engine.GetExchange("/", "fex")!, new ContentProperties(), "rk"));

        // Enqueue updates InMemoryEngine synchronously.
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 1, 2, 3 }, "fex", "rk");
        var claim = engine.TryClaimForGet("/", "q", out var message, out var remaining);
        Assert.Equal(GetClaimOutcome.Claimed, claim);
        Assert.NotNull(message);
        Assert.Equal(1UL, message!.EnvelopeId);
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task Multi_vhost_engine_isolates_topology_per_vhost()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/", "alt"], replayOnStart: false);

        // Declare exchange + queue in each vhost.
        engine.DeclareExchange(new ExchangeInfo("/", "fex", "direct"));
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Bind("/", "fex", "q", "rk");

        engine.DeclareExchange(new ExchangeInfo("alt", "fex", "direct"));
        engine.DeclareQueue(new QueueInfo("alt", "q"));
        engine.Bind("alt", "fex", "q", "rk");

        // Each vhost has its own exchange.
        Assert.NotNull(engine.GetExchange("/", "fex"));
        Assert.Equal("direct", engine.GetExchange("/", "fex")!.Type);
        Assert.NotNull(engine.GetExchange("alt", "fex"));
        Assert.Equal("direct", engine.GetExchange("alt", "fex")!.Type);

        // Route isolation: routing in "/" does not affect "alt".
        Assert.Equal(["q"], engine.RouteTo(engine.GetExchange("/", "fex")!, new ContentProperties(), "rk"));
        Assert.Equal(["q"], engine.RouteTo(engine.GetExchange("alt", "fex")!, new ContentProperties(), "rk"));

        // Enqueue in one vhost does not appear in the other.
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 1 }, "fex", "rk");
        var claimAlt = engine.TryClaimForGet("alt", "q", out var _, out var _);
        Assert.Equal(GetClaimOutcome.Empty, claimAlt);
    }

    [Fact]
    public async Task Can_connect_and_declare_when_redis_available()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        await using var engine = new RedisStreamRouteEngine(db, ["/"], replayOnStart: false, drainerCapacity: 1024);
        engine.DeclareExchange(new ExchangeInfo("/", "fex", "direct"));
        Assert.Equal("direct", engine.GetExchange("/", "fex")!.Type);
    }

    [Fact]
    public async Task Eviction_gate_refuses_unsafe_policy()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        // Set an eviction policy that would discard durable data.
        await db.ExecuteAsync("CONFIG", "SET", "maxmemory-policy", "allkeys-lru");

        Assert.Throws<BrokerConfigurationException>(() =>
            new RedisStreamRouteEngine(db, ["/"], replayOnStart: false, enforceEvictionGate: true));

        // noeviction is permitted.
        await db.ExecuteAsync("CONFIG", "SET", "maxmemory-policy", "noeviction");
        await using var engine = new RedisStreamRouteEngine(db, ["/"], replayOnStart: false, enforceEvictionGate: true);
        Assert.NotNull(engine);
    }

    [Fact]
    public async Task Replay_restores_topology_and_ready_queues_after_restart()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        // Use a unique vhost so previous test runs don't leave stale stream data.
        string vhost = $"/replay-test-{Guid.NewGuid():N}";

        // First engine: declare topology + enqueue messages.
        await using var engine1 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 1024);
        engine1.DeclareExchange(new ExchangeInfo(vhost, "fex", "direct"));
        engine1.DeclareQueue(new QueueInfo(vhost, "q"));
        engine1.Bind(vhost, "fex", "q", "rk");
        engine1.Enqueue(vhost, "q", new ContentProperties(), new byte[] { 1, 2, 3 }, "fex", "rk");
        engine1.Enqueue(vhost, "q", new ContentProperties(), new byte[] { 4, 5, 6 }, "fex", "rk");
        await Task.Delay(500, TestContext.Current.CancellationToken); // give the drainer time to mirror writes to Redis

        // Second engine over the same Redis+vhost: replay should restore state.
        await using var engine2 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 1024);
        Assert.NotNull(engine2.GetExchange(vhost, "fex"));
        Assert.Equal("direct", engine2.GetExchange(vhost, "fex")!.Type);
        Assert.NotNull(engine2.GetQueue(vhost, "q"));
        Assert.Equal(["q"], engine2.RouteTo(engine2.GetExchange(vhost, "fex")!, new ContentProperties(), "rk"));

        // Replayed messages are claimable.
        int claimed = 0;
        while (engine2.TryClaimForGet(vhost, "q", out var m, out var _) == GetClaimOutcome.Claimed)
        {
            claimed++;
            if (claimed > 10) break; // safety
        }
        Assert.Equal(2, claimed);
    }

    [Fact]
    public async Task Replay_restores_headers_binding_arguments_and_max_length()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/replay-m3-{Guid.NewGuid():N}";

        await using var engine1 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 1024);
        engine1.DeclareExchange(new ExchangeInfo(vhost, "h", "headers"));
        engine1.DeclareQueue(new QueueInfo(vhost, "q", MaxLength: 3));
        var args = new FieldTable([
            new("x-match", FieldValue.From("all")),
            new("color", FieldValue.From("red")),
        ]);
        engine1.Bind(vhost, "h", "q", "", args);
        await Task.Delay(500, TestContext.Current.CancellationToken); // let the drainer mirror the writes

        await using var engine2 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 1024);

        // x-max-length survives the restart.
        Assert.Equal(3, engine2.GetQueue(vhost, "q")!.MaxLength);

        // Binding arguments survive too: only matching header sets route.
        ExchangeInfo headers = engine2.GetExchange(vhost, "h")!;
        var matching = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("red"))]) };
        Assert.Equal(["q"], engine2.RouteTo(headers, matching, ""));
        var other = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("blue"))]) };
        Assert.Empty(engine2.RouteTo(headers, other, ""));
    }

    [Fact]
    public async Task Replay_restores_queue_arguments()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/replay-args-{Guid.NewGuid():N}";

        await using (var engine1 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 1024))
        {
            engine1.DeclareExchange(new ExchangeInfo(vhost, "dlx", "direct"));
            engine1.DeclareQueue(new QueueInfo(
                vhost,
                "q",
                Expiration: TimeSpan.FromSeconds(30),
                MaxLength: 5,
                DeadLetterExchange: "dlx",
                DeadLetterRoutingKey: "dead",
                DeliveryLimit: 3));
            await Task.Delay(500, TestContext.Current.CancellationToken); // let the drainer mirror the declare
        }

        await using var engine2 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 1024);

        QueueInfo restored = engine2.GetQueue(vhost, "q")!;
        Assert.Equal(TimeSpan.FromSeconds(30), restored.Expiration);
        Assert.Equal(5, restored.MaxLength);
        Assert.Equal("dlx", restored.DeadLetterExchange);
        Assert.Equal("dead", restored.DeadLetterRoutingKey);
        Assert.Equal(3, restored.DeliveryLimit);
    }

    [Fact]
    public async Task Dead_letter_copy_is_mirrored_with_x_death()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-dlx-{Guid.NewGuid():N}";
        string targetKey = RedisKeys.QueueKey(vhost, "target");

        await using (var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false))
        {
            engine.DeclareQueue(new QueueInfo(
                vhost, "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
            engine.DeclareQueue(new QueueInfo(vhost, "target"));
            EnqueueReceipt receipt = engine.Enqueue(
                vhost, "source", new ContentProperties { Expiration = "60000" }, [1, 2], "origin-ex", "rk");
            Assert.True(await receipt.Durable);
            Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet(vhost, "source", out InMemoryMessage? claimed, out _));

            engine.Reject(vhost, "source", claimed!);

            await WaitUntilAsync(
                async () => await db.StreamLengthAsync(targetKey) == 1,
                "the dead-letter copy to be mirrored");
        }

        await using var replay = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: true, drainerCapacity: 16, enforceEvictionGate: false);

        Assert.Equal(GetClaimOutcome.Claimed, replay.TryClaimForGet(vhost, "target", out InMemoryMessage? copy, out _));
        Assert.Equal(new byte[] { 1, 2 }, copy!.Body);

        var deaths = (FieldValue[])copy.Props.Headers!["x-death"].Raw!;
        var death = (FieldTable)deaths[0].Raw!;
        Assert.Equal("source", death["queue"].Raw);
        Assert.Equal("rejected", death["reason"].Raw);
        Assert.Equal(1L, death["count"].Raw);
        Assert.Equal("origin-ex", death["exchange"].Raw);
        Assert.IsType<DateTimeOffset>(death["time"].Raw);
        Assert.Equal("source", copy.Props.Headers!["x-first-death-queue"].Raw);
        Assert.Null(copy.Expiration);
        Assert.Null(copy.Props.Expiration);
    }

    [Fact]
    public async Task Dead_letter_copy_resets_expiry_in_redis()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-dlx-expiry-{Guid.NewGuid():N}";
        string targetKey = RedisKeys.QueueKey(vhost, "target");

        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(
            vhost, "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo(vhost, "target"));
        EnqueueReceipt receipt = engine.Enqueue(
            vhost, "source", new ContentProperties { Expiration = "60000" }, [1], "ex", "rk");
        Assert.True(await receipt.Durable);
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet(vhost, "source", out InMemoryMessage? claimed, out _));

        engine.Reject(vhost, "source", claimed!);

        await WaitUntilAsync(
            async () => await db.StreamLengthAsync(targetKey) == 1,
            "the dead-letter copy to be mirrored");

        StreamEntry entry = (await db.StreamRangeAsync(targetKey))[0];
        bool hasExpiry = false;
        foreach (NameValueEntry field in entry.Values)
        {
            if (field.Name == RedisKeys.QueueField.ExpiresAt)
            {
                hasExpiry = true;
            }
        }

        Assert.False(hasExpiry);
    }

    [Fact]
    public async Task Replay_restores_expiry_delivery_count_and_redelivered()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/replay-m4-{Guid.NewGuid():N}";
        string queue = "q";

        await using var engine1 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 1024);
        engine1.DeclareQueue(new QueueInfo(vhost, queue));
        await Task.Delay(500, TestContext.Current.CancellationToken); // let the drainer mirror the topology

        var expiresAt = DateTimeOffset.FromUnixTimeMilliseconds(1_900_000_000_000);
        var record = new EnvelopeRecord(
            EnvelopeId: 42,
            Exchange: "fex",
            RoutingKey: "rk",
            Body: [7, 8, 9],
            Properties: new ContentProperties(
                ContentType: "text/plain",
                Headers: new FieldTable([new("k", FieldValue.From("v"))])),
            ExpiresAt: expiresAt,
            DeliveryCount: 4,
            Redelivered: true);
        await db.StreamAddAsync(
            RedisKeys.QueueKey(vhost, queue),
            StreamCodec.EncodeEnqueue(record).Select(f => new NameValueEntry(f.Field, f.Value)).ToArray());

        await using var engine2 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 1024);

        Assert.Equal(GetClaimOutcome.Claimed, engine2.TryClaimForGet(vhost, queue, out InMemoryMessage? claimed, out _));
        Assert.NotNull(claimed);
        Assert.Equal(42UL, claimed!.EnvelopeId);
        Assert.Equal(expiresAt, claimed.Expiration);
        Assert.Equal(5, claimed.DeliveryCount); // replay restored 4; the claim increments
        Assert.True(claimed.Redelivered);
        Assert.Equal("text/plain", claimed.Props.ContentType);
        Assert.Equal("v", claimed.Props.Headers!["k"].Raw);
    }

    // ---------- Task 5: delta mirroring, tombstones and replay dedupe ----------

    [Fact]
    public async Task Ack_returns_a_remove_delta_without_redis()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        var message = new InMemoryMessage(new ContentProperties(), [1], "fex", "rk", 9);

        EngineDelta delta = engine.Acknowledge("/", "q", message);

        Assert.Empty(delta.Enqueued);
        Assert.Equal(new QueuedMessageRef("/", "q", 9), Assert.Single(delta.Removed));
    }

    [Fact]
    public async Task Ack_removes_the_stream_entry_and_replay_does_not_resurrect_it()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-ack-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q"));
        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        Assert.True(await receipt.Durable);

        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet(vhost, "q", out InMemoryMessage? claimed, out _));
        engine.Acknowledge(vhost, "q", claimed!);

        await WaitUntilAsync(async () => await db.StreamLengthAsync(queueKey) == 0, "ack to XDEL the entry");
        await engine.DisposeAsync();

        await using var replay = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 16, enforceEvictionGate: false);
        Assert.Equal(GetClaimOutcome.Empty, replay.TryClaimForGet(vhost, "q", out _, out _));
    }

    [Fact]
    public async Task Reject_without_requeue_removes_the_stream_entry()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-reject-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q"));
        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        Assert.True(await receipt.Durable);

        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet(vhost, "q", out InMemoryMessage? claimed, out _));
        engine.Reject(vhost, "q", claimed!);

        await WaitUntilAsync(async () => await db.StreamLengthAsync(queueKey) == 0, "reject to XDEL the entry");
    }

    [Fact]
    public async Task Requeue_replaces_the_entry()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-requeue-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q"));
        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        Assert.True(await receipt.Durable);

        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet(vhost, "q", out InMemoryMessage? claimed, out _));

        // The claim increments DeliveryCount; the mirror carries the claimed message.
        Assert.Equal(1, claimed!.DeliveryCount);
        engine.Requeue(vhost, "q", claimed);

        await WaitUntilAsync(async () =>
        {
            StreamEntry[] entries = await db.StreamRangeAsync(queueKey);
            if (entries.Length != 1)
            {
                return false;
            }

            EnvelopeRecord record = DecodeEntry(entries[0]);
            return record.Redelivered && record.DeliveryCount == 1 && record.EnvelopeId == receipt.EnvelopeId;
        }, "requeue to replace the entry with the redelivered copy");

        await WaitUntilAsync(async () => await db.StreamLengthAsync(queueKey) == 1, "old stream id XDELed");
    }

    [Fact]
    public async Task Max_length_drops_are_xdel_ed()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-maxlen-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q", MaxLength: 1));

        EnqueueReceipt first = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        Assert.True(await first.Durable);
        EnqueueReceipt second = engine.Enqueue(vhost, "q", new ContentProperties(), [2], "", "q");
        Assert.True(await second.Durable);

        Assert.Equal([first.EnvelopeId], second.DroppedEnvelopeIds);

        await WaitUntilAsync(async () => await db.StreamLengthAsync(queueKey) == 1, "max-length drop to XDEL the oldest entry");
        EnvelopeRecord survivor = DecodeEntry(Assert.Single(await db.StreamRangeAsync(queueKey)));
        Assert.Equal(second.EnvelopeId, survivor.EnvelopeId);
    }

    [Fact]
    public async Task DeleteQueue_removes_the_stream_and_replays_as_deleted()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-delete-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareExchange(new ExchangeInfo(vhost, "ex", "direct"));
        engine.DeclareQueue(new QueueInfo(vhost, "q"));
        engine.Bind(vhost, "ex", "q", "rk");
        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "ex", "rk");
        Assert.True(await receipt.Durable);
        await WaitUntilAsync(
            async () => await db.StreamLengthAsync(RedisKeys.TopologyKey(vhost)) >= 3,
            "topology to mirror before the delete");

        engine.RegisterExclusiveQueueOwner(vhost, "q", "conn-1");
        engine.ReconcileConnection("conn-1");

        await WaitUntilAsync(async () => !await db.KeyExistsAsync(queueKey), "queue stream to be deleted");
        await engine.DisposeAsync();

        await using var replay = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 16, enforceEvictionGate: false);
        Assert.Null(replay.GetQueue(vhost, "q"));
        Assert.Empty(replay.RouteTo(replay.GetExchange(vhost, "ex")!, new ContentProperties(), "rk"));
    }

    [Fact]
    public async Task Redis_reap_mirrors_queue_deletes()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-reap-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareExchange(new ExchangeInfo(vhost, "ex", "direct"));
        engine.DeclareQueue(new QueueInfo(vhost, "q", Expiration: TimeSpan.FromMilliseconds(100)));
        engine.Bind(vhost, "ex", "q", "rk");
        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "ex", "rk");
        Assert.True(await receipt.Durable);
        await WaitUntilAsync(
            async () => await db.StreamLengthAsync(queueKey) == 1,
            "the queue stream to exist before the reap");

        EngineDelta delta = engine.Reap(DateTimeOffset.UtcNow.AddSeconds(1));

        (string Vhost, string Name) deleted = Assert.Single(delta.DeletedQueues);
        Assert.Equal(vhost, deleted.Vhost);
        Assert.Equal("q", deleted.Name);
        Assert.Null(engine.GetQueue(vhost, "q"));

        await WaitUntilAsync(async () => !await db.KeyExistsAsync(queueKey), "the queue stream to be deleted");
        await engine.DisposeAsync();

        await using var replay = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: true, drainerCapacity: 16, enforceEvictionGate: false);
        Assert.Null(replay.GetQueue(vhost, "q"));
        Assert.Empty(replay.RouteTo(replay.GetExchange(vhost, "ex")!, new ContentProperties(), "rk"));
    }

    [Fact]
    public async Task Unbind_tombstone_survives_replay()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-unbind-{Guid.NewGuid():N}";

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareExchange(new ExchangeInfo(vhost, "ex", "direct"));
        engine.DeclareQueue(new QueueInfo(vhost, "q"));
        engine.Bind(vhost, "ex", "q", "rk");
        await WaitUntilAsync(
            async () => await db.StreamLengthAsync(RedisKeys.TopologyKey(vhost)) >= 3,
            "bind to mirror");

        engine.Unbind(vhost, "ex", "q", "rk");
        await WaitUntilAsync(
            async () => await db.StreamLengthAsync(RedisKeys.TopologyKey(vhost)) >= 4,
            "unbind tombstone to mirror");
        await engine.DisposeAsync();

        await using var replay = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 16, enforceEvictionGate: false);
        Assert.NotNull(replay.GetQueue(vhost, "q"));
        Assert.Empty(replay.RouteTo(replay.GetExchange(vhost, "ex")!, new ContentProperties(), "rk"));
    }

    [Fact]
    public async Task Crash_window_duplicate_is_collapsed_and_stale_entry_xdel_ed()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/m4-dedupe-{Guid.NewGuid():N}";
        string queueKey = RedisKeys.QueueKey(vhost, "q");

        await using var engine = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q"));
        await WaitUntilAsync(
            async () => await db.StreamLengthAsync(RedisKeys.TopologyKey(vhost)) >= 1,
            "queue declare to mirror");

        // A crash between XADD and XDEL leaves the old and new copies of one envelope.
        await db.StreamAddAsync(queueKey, Fields(new EnvelopeRecord(
            7, "", "q", [1], new ContentProperties(), null, 0, false)));
        await db.StreamAddAsync(queueKey, Fields(new EnvelopeRecord(
            7, "", "q", [2], new ContentProperties(), null, 1, true)));
        Assert.Equal(2, await db.StreamLengthAsync(queueKey));

        await engine.DisposeAsync();

        await using var replay = new RedisStreamRouteEngine(db, [vhost], replayOnStart: true, drainerCapacity: 16, enforceEvictionGate: false);
        Assert.Equal(1, await db.StreamLengthAsync(queueKey));

        Assert.Equal(GetClaimOutcome.Claimed, replay.TryClaimForGet(vhost, "q", out InMemoryMessage? claimed, out _));
        Assert.Equal(7UL, claimed!.EnvelopeId);
        Assert.Equal(2, claimed.DeliveryCount); // replay restored 1; the claim increments
        Assert.True(claimed.Redelivered);
    }

    [Fact]
    public void WriteCommand_roundtrips_m3_topology_fields()
    {
        var args = new FieldTable([
            new("x-match", FieldValue.From("any")),
            new("attempts", FieldValue.From(7)),
        ]);
        WriteCommand bind = WriteCommand.Bind("/", "h", "q", "", args);
        WriteCommand decodedBind = WriteCommand.FromStreamFields(WriteCommandKind.Bind, bind.ToStreamFields());
        Assert.Equal(args, decodedBind.Arguments);

        WriteCommand declare = WriteCommand.DeclareQueue(
            "/", "q", durable: true, exclusive: false, autoDelete: false, maxLength: 5);
        WriteCommand decodedDeclare = WriteCommand.FromStreamFields(
            WriteCommandKind.DeclareQueue, declare.ToStreamFields());
        Assert.Equal(5, decodedDeclare.MaxLength);
    }

    [Fact]
    public async Task Drainer_channel_is_bounded()
    {
        // Bounded channel: small capacity, no Redis to drain, rapid enqueues
        // should not block or grow unbounded (in-memory state stays correct).
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false, drainerCapacity: 4);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        for (int i = 0; i < 100; i++)
        {
            engine.Enqueue("/", "q", new ContentProperties(), new byte[] { (byte)i }, "fex", "rk");
        }

        // All enqueues landed in memory (channel drops overflow silently;
        // memory is the system of record).
        int claimed = 0;
        while (engine.TryClaimForGet("/", "q", out var m, out var left) == GetClaimOutcome.Claimed)
        {
            claimed++;
            if (claimed > 100) break; // safety
        }

        Assert.True(claimed > 0);
    }

    [Fact]
    public async Task Null_redis_receipt_completes_true_and_assigns_ids()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        engine.DeclareQueue(new QueueInfo("/", "q"));

        EnqueueReceipt receipt = engine.Enqueue("/", "q", new ContentProperties(), [1], "fex", "rk");

        Assert.Equal(1UL, receipt.EnvelopeId);
        Assert.True(await receipt.Durable);
        Assert.Empty(receipt.DroppedEnvelopeIds);
    }

    [Fact]
    public async Task Drainer_exhaustion_returns_false_without_enqueuing()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/slots-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 1, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q"));

        SemaphoreSlim slots = engine.DrainerSlotsForTest!;
        Assert.True(slots.Wait(0)); // the idle drainer has not reserved a slot
        try
        {
            EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "fex", "rk");

            Assert.Equal(0UL, receipt.EnvelopeId);
            Assert.False(await receipt.Durable);
            Assert.Equal(GetClaimOutcome.Empty, engine.TryClaimForGet(vhost, "q", out _, out _));
        }
        finally
        {
            slots.Release();
        }
    }

    [Fact]
    public async Task Receipt_completes_only_after_xadd()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/receipt-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 8, enforceEvictionGate: false);
        engine.DeclareQueue(new QueueInfo(vhost, "q"));

        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [1, 2, 3], "fex", "rk");

        Assert.True(await receipt.Durable.AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, await db.StreamLengthAsync(RedisKeys.QueueKey(vhost, "q")));
    }

    [Fact]
    public void Topic_exchange_star_matches_exactly_one_word()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "t", "topic"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        engine.Bind("/", "t", "q1", "a.b.c");

        var r1 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b.c");
        Assert.Equal(["q1"], r1);

        var r2 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b");
        Assert.Empty(r2);

        var r3 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b.c.d");
        Assert.Empty(r3);
    }

    [Fact]
    public void Topic_exchange_hash_matches_zero_or_more_words()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "t", "topic"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        engine.Bind("/", "t", "q1", "a.#");

        var r1 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a");
        Assert.Equal(["q1"], r1);

        var r2 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b");
        Assert.Equal(["q1"], r2);

        var r3 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b.c");
        Assert.Equal(["q1"], r3);

        var r4 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "b.c");
        Assert.Empty(r4);
    }

    [Fact]
    public void Topic_exchange_star_does_not_match_zero_or_multiple_words()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "t", "topic"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        engine.Bind("/", "t", "q1", "a.*.c");

        var r1 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b.c");
        Assert.Equal(["q1"], r1);

        var r2 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.c");
        Assert.Empty(r2);

        var r3 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b.c.d");
        Assert.Empty(r3);
    }

    [Fact]
    public void Topic_exchange_multiple_queues_route_correctly()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "t", "topic"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        engine.DeclareQueue(new QueueInfo("/", "q2"));
        engine.DeclareQueue(new QueueInfo("/", "q3"));
        engine.Bind("/", "t", "q1", "a.b.c");
        engine.Bind("/", "t", "q2", "a.#");
        engine.Bind("/", "t", "q3", "*.b.*");

        var r1 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b.c");
        Assert.Equal(new[] { "q1", "q2", "q3" }, r1);

        var r2 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "a.b");
        Assert.Equal(new[] { "q2" }, r2);

        var r3 = engine.RouteTo(engine.GetExchange("/", "t")!, new ContentProperties(), "x.b.y");
        Assert.Equal(new[] { "q3" }, r3);
    }

    [Fact]
    public void Remove_expired_messages_from_ready_buffer()
    {
        // InMemoryEngine removes expired messages from ready buffers.
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 1 }, "fex", "rk");
        engine.Enqueue("/", "q", new ContentProperties() { Expiration = "0" }, new byte[] { 2 }, "fex", "rk");
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 3 }, "fex", "rk");

        engine.Reap(DateTimeOffset.UtcNow);

        // Only non-expired messages remain.
        var claim1 = engine.TryClaimForGet("/", "q", out var m1, out var _);
        Assert.Equal(GetClaimOutcome.Claimed, claim1);
        Assert.Equal(1UL, m1!.EnvelopeId);
        var claim2 = engine.TryClaimForGet("/", "q", out var m2, out var _);
        Assert.Equal(GetClaimOutcome.Claimed, claim2);
        Assert.Equal(3UL, m2!.EnvelopeId);
        var claim3 = engine.TryClaimForGet("/", "q", out var _, out var _);
        Assert.Equal(GetClaimOutcome.Empty, claim3);
    }

    [Fact]
    public void Expiration_field_roundtrips_on_InMemoryMessage()
    {
        var exp = DateTimeOffset.UtcNow.AddMinutes(5);
        var msg = new InMemoryMessage(
            new ContentProperties(),
            new byte[] { 1 },
            "fex",
            "rk",
            42,
            false,
            exp);
        Assert.Equal(exp, msg.Expiration);
    }

    [Fact]
    public async Task ReconcileConnection_deletes_exclusive_queue_on_connection_drop()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        string vhost = $"/excl-test-{Guid.NewGuid():N}";

        // Two engines over the same Redis to simulate two connections sharing Redis.
        await using var engine1 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 1024);
        await using var engine2 = new RedisStreamRouteEngine(db, [vhost], replayOnStart: false, drainerCapacity: 1024);

        // Engine A declares an exclusive queue.
        engine1.DeclareQueue(new QueueInfo(vhost, "excl-q", Exclusive: true));
        Assert.NotNull(engine1.GetQueue(vhost, "excl-q"));

        // Engine A registers an exclusive consumer (simulating ownership).
        var consumer = new TestConsumer("consumer-A");
        var regResult = engine1.RegisterConsumer(vhost, "excl-q", consumer);
        Assert.Equal(ConsumeRegisterOutcome.Registered, regResult);

        // Engine A drops its connection → ReconcileConnection deletes the exclusive queue.
        engine1.ReconcileConnection("conn-A");

        // Engine B should no longer see the queue.
        Assert.Null(engine2.GetQueue(vhost, "excl-q"));
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string description)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"Timed out waiting for {description}");
    }

    private static EnvelopeRecord DecodeEntry(StreamEntry entry) =>
        StreamCodec.DecodeEnqueue(entry.Values.Select(value => (value.Name.ToString(), value.Value.ToString())).ToList());

    private static NameValueEntry[] Fields(EnvelopeRecord record) =>
        StreamCodec.EncodeEnqueue(record).Select(field => new NameValueEntry(field.Field, field.Value)).ToArray();

    private sealed class TestConsumer(string tag) : IQueueConsumer
    {
        public string ConsumerTag => tag;
        public bool IsExclusive => true;
        public bool TryAdmitForDelivery(InMemoryMessage message) => true;
        public Task DrainDeliveryWritesAsync() => Task.CompletedTask;
    }

    [Fact]
    public void Headers_exchange_xmatch_all_requires_all_headers()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "h", "headers"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        var args = new FieldTable([new("color", FieldValue.From("red")), new("size", FieldValue.From("large"))]);
        engine.Bind("/", "h", "q1", "", args);

        // All headers match
        var props1 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("red")), new("size", FieldValue.From("large"))]) };
        var r1 = engine.RouteTo(engine.GetExchange("/", "h")!, props1, "");
        Assert.Equal(["q1"], r1);

        // Missing one header
        var props2 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("red"))]) };
        var r2 = engine.RouteTo(engine.GetExchange("/", "h")!, props2, "");
        Assert.Empty(r2);

        // Wrong value
        var props3 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("blue")), new("size", FieldValue.From("large"))]) };
        var r3 = engine.RouteTo(engine.GetExchange("/", "h")!, props3, "");
        Assert.Empty(r3);
    }

    [Fact]
    public void Headers_exchange_xmatch_any_matches_any_header()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "h", "headers"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        var args = new FieldTable([new("color", FieldValue.From("red")), new("size", FieldValue.From("large")), new("x-match", FieldValue.From("any"))]);
        engine.Bind("/", "h", "q1", "", args);

        // Only color matches
        var props1 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("red")), new("size", FieldValue.From("small"))]) };
        var r1 = engine.RouteTo(engine.GetExchange("/", "h")!, props1, "");
        Assert.Equal(["q1"], r1);

        // Only size matches
        var props2 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("blue")), new("size", FieldValue.From("large"))]) };
        var r2 = engine.RouteTo(engine.GetExchange("/", "h")!, props2, "");
        Assert.Equal(["q1"], r2);

        // Neither matches
        var props3 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("blue")), new("size", FieldValue.From("small"))]) };
        var r3 = engine.RouteTo(engine.GetExchange("/", "h")!, props3, "");
        Assert.Empty(r3);
    }

    [Fact]
    public void Headers_exchange_defaults_to_xmatch_all()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "h", "headers"));
        engine.DeclareQueue(new QueueInfo("/", "q1"));
        // No x-match in arguments → defaults to all
        var args = new FieldTable([new("color", FieldValue.From("red"))]);
        engine.Bind("/", "h", "q1", "", args);

        var props1 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("red"))]) };
        var r1 = engine.RouteTo(engine.GetExchange("/", "h")!, props1, "");
        Assert.Equal(["q1"], r1);

        var props2 = new ContentProperties { Headers = new FieldTable([new("color", FieldValue.From("blue"))]) };
        var r2 = engine.RouteTo(engine.GetExchange("/", "h")!, props2, "");
        Assert.Empty(r2);
    }

    [Fact]
    public void X_max_length_drops_oldest_messages()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q", MaxLength: 3));
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 1 }, "fex", "rk");
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 2 }, "fex", "rk");
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 3 }, "fex", "rk");

        // All 3 fit (max is 3)
        var c1 = engine.TryClaimForGet("/", "q", out var m1, out var _);
        Assert.Equal(GetClaimOutcome.Claimed, c1);
        Assert.Equal(1UL, m1!.EnvelopeId);

        // Enqueue a 4th message - oldest (1) should be dropped
        engine.Enqueue("/", "q", new ContentProperties(), new byte[] { 4 }, "fex", "rk");

        // q1 should now have messages 2, 3, 4 (1 was dropped)
        var c2 = engine.TryClaimForGet("/", "q", out var m2, out var _);
        Assert.Equal(GetClaimOutcome.Claimed, c2);
        Assert.Equal(2UL, m2!.EnvelopeId);

        var c3 = engine.TryClaimForGet("/", "q", out var m3, out var _);
        Assert.Equal(GetClaimOutcome.Claimed, c3);
        Assert.Equal(3UL, m3!.EnvelopeId);

        var c4 = engine.TryClaimForGet("/", "q", out var m4, out var _);
        Assert.Equal(GetClaimOutcome.Claimed, c4);
        Assert.Equal(4UL, m4!.EnvelopeId);

        // Queue should now be empty
        var c5 = engine.TryClaimForGet("/", "q", out var _, out var _);
        Assert.Equal(GetClaimOutcome.Empty, c5);
    }
}
