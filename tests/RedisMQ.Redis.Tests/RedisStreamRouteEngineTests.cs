using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
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
        Assert.Equal("{mq:top}", RedisKeys.TopologyKey);
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
            vhost: "/",
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
    public async Task Can_connect_and_declare_when_redis_available()
    {
        var db = TryConnectRedis();
        if (db is null)
        {
            return; // Redis unavailable: skip gracefully
        }

        await using var engine = new RedisStreamRouteEngine(db, "/", replayOnStart: false, drainerCapacity: 1024);
        engine.DeclareExchange(new ExchangeInfo("/", "fex", "direct"));
        Assert.Equal("direct", engine.GetExchange("/", "fex")!.Type);
    }
}
