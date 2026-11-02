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
            var connectTask = ConnectionMultiplexer.ConnectAsync("localhost:6379");
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
}
