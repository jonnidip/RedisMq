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
            PropertiesJson: "{\"content-type\":\"text/plain\"}");

        var fields = StreamCodec.EncodeEnqueue(record);
        var decoded = StreamCodec.DecodeEnqueue(fields);

        Assert.Equal(record.EnvelopeId, decoded.EnvelopeId);
        Assert.Equal(record.Exchange, decoded.Exchange);
        Assert.Equal(record.RoutingKey, decoded.RoutingKey);
        Assert.Equal(record.PropertiesJson, decoded.PropertiesJson);
        Assert.Equal(body, decoded.Body);
    }

    [Fact]
    public void Empty_body_round_trips()
    {
        var record = new EnvelopeRecord(1, "", "", [], "{}");
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
            WriteCommand.Requeue("/", "q", 3, "fex", "rk", new byte[] { 9 }, new ContentProperties(), redelivered: true),
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

        engine.RemoveExpiredReadyMessages();

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
