using System.Text;
using RedisMQ.Redis;
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
            WriteCommand.Enqueue("/", "q", 3, "fex", "rk", new byte[] { 1, 2, 3}, "{\"ct\":\"text/plain\"}"),
            WriteCommand.RegisterConsumer("/", "q", "ctag"),
            WriteCommand.UnregisterConsumer("/", "q", "ctag"),
            WriteCommand.Requeue("/", "q", 3, "fex", "rk", new byte[] { 9 }, "{}", redelivered: true),
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
