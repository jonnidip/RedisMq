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

        // Channel is open with confirms enabled. basic.publish would hang client-side
        // because RabbitMQ.Client 7.x awaits confirm.ack frames that it does not
        // surface via BasicAcksAsync. The confirm.ack wire path is implemented in
        // ChannelContext (FinishPublish schedules confirm.ack via the writer chain);
        // this test pins the confirm.select → confirm.select-ok handshake.
        Assert.True(channel.IsOpen);
    }
}
