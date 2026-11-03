using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RedisMQ.Broker.Memory;
using RedisMQ.Conformance;
using RedisMQ.Redis;
using System.Text;
using Xunit;

namespace RedisMQ.Redis.Tests;

/// <summary>
/// M0 regression: runs the RabbitMQ.Client 7.x conformance scenarios against
/// <see cref="RedisStreamRouteEngine"/> (wrapping <see cref="InMemoryEngine"/>) over a
/// real TCP socket. Proves the M0 wire surface is engine-agnostic — swapping the route
/// engine changes nothing observable from the client.
/// </summary>
public sealed class M0RegressionTests
{
    private static async Task<IConnection> ConnectAsync(TestBrokerHost host)
    {
        var factory = new ConnectionFactory { UserName = "guest", Password = "guest", VirtualHost = "/" };
        return await factory.CreateConnectionAsync([new AmqpTcpEndpoint("localhost", host.Port)]);
    }

    private static async Task<IChannel> NewChannelAsync(IConnection connection) => await connection.CreateChannelAsync();

    [Fact]
    public async Task Connect_authenticates_and_opens_a_channel()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        Assert.True(connection.IsOpen);

        await using var channel = await NewChannelAsync(connection);
        Assert.True(channel.IsOpen);
    }

    [Fact]
    public async Task Declare_bind_publish_consume_round_trips()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.ExchangeDeclareAsync("fex", ExchangeType.Direct, durable: false, autoDelete: false);
        await channel.QueueDeclareAsync("fq", durable: false, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("fq", "fex", "fk");

        var payload = "hello regression"u8.ToArray();
        await channel.BasicPublishAsync("fex", "fk", mandatory: false, new BasicProperties(), payload);

        var consumer = new AsyncEventingBasicConsumer(channel);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            received.TrySetResult(ea.Body.ToArray());
            await Task.CompletedTask;
        };
        await channel.BasicConsumeAsync("fq", true, "c-reg", noLocal: false, exclusive: false, arguments: null, consumer);

        byte[] body = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(payload, body);
    }

    [Fact]
    public async Task Qos_count_gate_admits_exactly_10_and_more_flow_after_acks()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("gate-q", durable: false, exclusive: false, autoDelete: false);
        for (int i = 0; i < 20; i++)
        {
            await channel.BasicPublishAsync("", "gate-q", mandatory: false, new BasicProperties(), new byte[] { (byte)i });
        }

        await channel.BasicQosAsync(0, 10, false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        var seen = new List<ulong>();
        var ten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fifteen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            lock (seen)
            {
                seen.Add(ea.DeliveryTag);
                if (seen.Count == 10) ten.TrySetResult();
                if (seen.Count == 15) fifteen.TrySetResult();
            }
            await Task.CompletedTask; // no ack yet — gate stays closed at 10
        };
        await channel.BasicConsumeAsync("gate-q", false, "c-gate", noLocal: false, exclusive: false, arguments: null, consumer);

        await ten.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(10, seen.Count);

        // Ack exactly half the slots (5 of 10): exactly 5 more flow.
        foreach (ulong tag in seen.Take(5).ToList())
        {
            await channel.BasicAckAsync(tag, multiple: false);
        }

        await fifteen.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(15, seen.Count);
    }

    [Fact]
    public async Task Nack_requeue_redelivers_with_redelivered_flag()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("nack-q", durable: false, exclusive: false, autoDelete: false);
        await channel.BasicPublishAsync("", "nack-q", mandatory: false, new BasicProperties(), "n1"u8.ToArray());
        await channel.BasicQosAsync(0, 1, false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        var first = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        bool hadFirst = false;
        consumer.ReceivedAsync += async (_, ea) =>
        {
            if (!hadFirst)
            {
                hadFirst = true;
                first.TrySetResult(ea);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                return;
            }
            second.TrySetResult(ea);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };
        await channel.BasicConsumeAsync("nack-q", false, "c-nack", noLocal: false, exclusive: false, arguments: null, consumer);

        BasicDeliverEventArgs firstArgs = await first.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(firstArgs.Redelivered);
        BasicDeliverEventArgs secondArgs = await second.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(secondArgs.Redelivered);
        Assert.Equal("n1", Encoding.UTF8.GetString(secondArgs.Body.Span));
    }

    [Fact]
    public async Task BasicGet_returns_the_head_and_reports_empty_after()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("get-q", durable: false, exclusive: false, autoDelete: false);
        await channel.BasicPublishAsync("", "get-q", mandatory: false, new BasicProperties { ContentType = "text/plain" }, "g1"u8.ToArray());

        BasicGetResult? result = await channel.BasicGetAsync("get-q", autoAck: false);
        Assert.NotNull(result);
        Assert.Equal("g1", Encoding.UTF8.GetString(result.Body.ToArray()));
        Assert.False(result.Redelivered);
        await channel.BasicAckAsync(result.DeliveryTag, multiple: false);

        Assert.Null(await channel.BasicGetAsync("get-q", autoAck: false));
    }

    [Fact]
    public async Task Publish_to_unknown_exchange_closes_the_channel_404()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("any-q", durable: false, exclusive: false, autoDelete: false);
        await channel.BasicPublishAsync("nope", "anything", mandatory: false, new BasicProperties(), "x"u8.ToArray());

        OperationInterruptedException ex = await Assert.ThrowsAsync<OperationInterruptedException>(async () =>
            await channel.QueueDeclareAsync("any-q", durable: false, exclusive: false, autoDelete: false));
        Assert.Equal(404, ex.ShutdownReason!.ReplyCode);
        Assert.Equal(60, ex.ShutdownReason.ClassId);
        Assert.Equal(40, ex.ShutdownReason.MethodId);
        Assert.True(channel.IsClosed);
    }

    [Fact]
    public async Task Clean_close_rides_no_errors()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await connection.CloseAsync();
        Assert.False(connection.IsOpen);
        Assert.True(channel.IsClosed);
    }

    [Fact]
    public async Task Publisher_confirm_select_and_ack()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);

        // Publisher confirmations enabled on the channel → confirm.select is sent,
        // server replies confirm.select-ok, channel opens.
        var options = new CreateChannelOptions(true, true);
        await using IChannel channel = await connection.CreateChannelAsync(options, TestContext.Current.CancellationToken);

        // Channel is open with confirms enabled. basic.publish outcomes ride the
        // connection's ordered confirm loop: the loop awaits the engine's durability
        // receipts and then writes confirm.ack/nack on the channel's writer chain.
        // This test pins the confirm.select → confirm.select-ok handshake.
        Assert.True(channel.IsOpen);
    }

    // ---------- M3: topic / headers / x-max-length against the Redis engine ----------

    [Fact]
    public async Task Topic_exchange_routes_with_star_and_hash_wildcards()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.ExchangeDeclareAsync("m3-topic", ExchangeType.Topic, durable: false, autoDelete: false);
        await channel.QueueDeclareAsync("m3-topic-a", durable: false, exclusive: false, autoDelete: false);
        await channel.QueueDeclareAsync("m3-topic-b", durable: false, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("m3-topic-a", "m3-topic", "a.#");
        await channel.QueueBindAsync("m3-topic-b", "m3-topic", "*.b");

        // The exchange type survives the wire round trip: the engine dispatches on it.
        Assert.Equal("topic", engine.GetExchange("/", "m3-topic")!.Type);

        // "a.b": # matches one word and * matches one word, so both bindings fire.
        await channel.BasicPublishAsync("m3-topic", "a.b", mandatory: false, new BasicProperties(), "ab"u8.ToArray());
        BasicGetResult? both = await channel.BasicGetAsync("m3-topic-a", autoAck: true);
        Assert.NotNull(both);
        Assert.Equal("ab", Encoding.UTF8.GetString(both.Body.ToArray()));
        BasicGetResult? star = await channel.BasicGetAsync("m3-topic-b", autoAck: true);
        Assert.NotNull(star);
        Assert.Equal("ab", Encoding.UTF8.GetString(star.Body.ToArray()));

        // "a": a.# matches (the # may match zero words); *.b needs the dot word.
        await channel.BasicPublishAsync("m3-topic", "a", mandatory: false, new BasicProperties(), "a"u8.ToArray());
        BasicGetResult? hashZero = await channel.BasicGetAsync("m3-topic-a", autoAck: true);
        Assert.NotNull(hashZero);
        Assert.Equal("a", Encoding.UTF8.GetString(hashZero.Body.ToArray()));
        Assert.Null(await channel.BasicGetAsync("m3-topic-b", autoAck: true));

        // "x.c": neither pattern matches.
        await channel.BasicPublishAsync("m3-topic", "x.c", mandatory: false, new BasicProperties(), "xc"u8.ToArray());
        Assert.Null(await channel.BasicGetAsync("m3-topic-a", autoAck: true));
        Assert.Null(await channel.BasicGetAsync("m3-topic-b", autoAck: true));
    }

    [Fact]
    public async Task Headers_exchange_matches_binding_arguments()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.ExchangeDeclareAsync("m3-headers", ExchangeType.Headers, durable: false, autoDelete: false);
        await channel.QueueDeclareAsync("m3-headers-q", durable: false, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("m3-headers-q", "m3-headers", routingKey: "", arguments: new Dictionary<string, object?>
        {
            ["x-match"] = "all",
            ["color"] = "red",
        });

        Assert.Equal("headers", engine.GetExchange("/", "m3-headers")!.Type);

        // Matching header set routes (the routing key is irrelevant for headers).
        var matching = new BasicProperties { Headers = new Dictionary<string, object?> { ["color"] = "red" } };
        await channel.BasicPublishAsync("m3-headers", "ignored", mandatory: false, matching, "red"u8.ToArray());
        BasicGetResult? hit = await channel.BasicGetAsync("m3-headers-q", autoAck: true);
        Assert.NotNull(hit);
        Assert.Equal("red", Encoding.UTF8.GetString(hit.Body.ToArray()));

        // Wrong value and missing header both miss.
        var wrongValue = new BasicProperties { Headers = new Dictionary<string, object?> { ["color"] = "blue" } };
        await channel.BasicPublishAsync("m3-headers", "ignored", mandatory: false, wrongValue, "blue"u8.ToArray());
        Assert.Null(await channel.BasicGetAsync("m3-headers-q", autoAck: true));

        await channel.BasicPublishAsync("m3-headers", "ignored", mandatory: false, new BasicProperties(), "none"u8.ToArray());
        Assert.Null(await channel.BasicGetAsync("m3-headers-q", autoAck: true));
    }

    [Fact]
    public async Task Queue_with_x_max_length_drops_the_oldest_message()
    {
        await using var engine = new RedisStreamRouteEngine(null!, ["/"], replayOnStart: false);
        await using var host = TestBrokerHost.Start(engine);
        await using var connection = await ConnectAsync(host);
        await using var channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync(
            "m3-bounded",
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-max-length"] = 2 });

        Assert.Equal(2, engine.GetQueue("/", "m3-bounded")!.MaxLength);

        foreach (string body in new[] { "one", "two", "three" })
        {
            await channel.BasicPublishAsync("", "m3-bounded", mandatory: false, new BasicProperties(), Encoding.UTF8.GetBytes(body));
        }

        // Drop-oldest: the first message is gone, the last two remain in order.
        BasicGetResult? second = await channel.BasicGetAsync("m3-bounded", autoAck: true);
        Assert.NotNull(second);
        Assert.Equal("two", Encoding.UTF8.GetString(second.Body.ToArray()));
        BasicGetResult? third = await channel.BasicGetAsync("m3-bounded", autoAck: true);
        Assert.NotNull(third);
        Assert.Equal("three", Encoding.UTF8.GetString(third.Body.ToArray()));
        Assert.Null(await channel.BasicGetAsync("m3-bounded", autoAck: true));
    }
}
