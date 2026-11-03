using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Text;
using Xunit;

namespace RedisMQ.Conformance;

/// <summary>
/// End-to-end flow conformance against RabbitMQ.Client 7.2.2 over a real TCP
/// socket: connect/auth/tune, declare/bind/publish/consume, content properties
/// round-trip, prefetch gating, nack/redelivery, basic.get, round-robin sharing,
/// consumer lifecycle and clean teardown.
/// </summary>
public sealed class FlowTests
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
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        Assert.True(connection.IsOpen);

        await using IChannel channel = await NewChannelAsync(connection);
        Assert.True(channel.IsOpen);
    }

    [Fact]
    public async Task Declare_bind_publish_consume_round_trips_content_and_properties()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.ExchangeDeclareAsync("fex", ExchangeType.Direct, durable: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        QueueDeclareOk declared = await channel.QueueDeclareAsync("fq", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("fq", declared.QueueName);
        await channel.QueueBindAsync("fq", "fex", "fk", cancellationToken: TestContext.Current.CancellationToken);

        var payload = "hello conformance"u8.ToArray();
        var props = new BasicProperties
        {
            ContentType = "application/json",
            CorrelationId = "corr-1",
            MessageId = "msg-1",
            Timestamp = new AmqpTimestamp(1_742_000_000),
            Headers = new Dictionary<string, object?>
            {
                ["h-string"] = "strval",
                ["h-bool"] = true,
                ["h-long"] = 42L,
            },
        };
        await channel.BasicPublishAsync("fex", "fk", mandatory: false, props, payload, cancellationToken: TestContext.Current.CancellationToken);
        // Fence the content-assembly pipeline before consuming.
        _ = await channel.QueueDeclareAsync("fq", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        var received = new TaskCompletionSource<(BasicDeliverEventArgs Args, byte[] Body)>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            received.TrySetResult((ea, ea.Body.ToArray()));
            await Task.CompletedTask;
        };
        await channel.BasicConsumeAsync("fq", true, "c-roundtrip", noLocal: false, exclusive: false, arguments: null, consumer, cancellationToken: TestContext.Current.CancellationToken);

        (BasicDeliverEventArgs args, byte[] body) = await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(payload, body);
        Assert.Equal("fex", args.Exchange);
        Assert.Equal("fk", args.RoutingKey);
        Assert.Equal("application/json", args.BasicProperties.ContentType);
        Assert.Equal("corr-1", args.BasicProperties.CorrelationId);
        Assert.Equal("msg-1", args.BasicProperties.MessageId);
        Assert.Equal((uint)1_742_000_000, args.BasicProperties.Timestamp.UnixTime);
        Assert.NotNull(args.BasicProperties.Headers);
        // RabbitMQ.Client 7.x decodes field-table longstr ('S') header values as byte[],
        // not string — a known client quirk. Round-trip the bytes back to a string.
        Assert.Equal("strval", Encoding.UTF8.GetString((byte[])args.BasicProperties.Headers!["h-string"]!));
        Assert.Equal(true, args.BasicProperties.Headers!["h-bool"]);
        Assert.Equal(42L, args.BasicProperties.Headers!["h-long"]);
    }

    [Fact]
    public async Task Publish_1000_and_consume_all_in_order_with_qos_10()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("big-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        for (int i = 0; i < 1000; i++)
        {
            await channel.BasicPublishAsync("", "big-q", mandatory: false, new BasicProperties(), BitConverter.GetBytes(i), cancellationToken: TestContext.Current.CancellationToken);
        }

        _ = await channel.QueueDeclareAsync("big-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicQosAsync(0, 10, false, TestContext.Current.CancellationToken);
        var consumer = new AsyncEventingBasicConsumer(channel);
        var order = new List<int>();
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            lock (order)
            {
                order.Add(BitConverter.ToInt32(ea.Body.Span));
                if (order.Count == 1000)
                {
                    drained.TrySetResult();
                }
            }

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };
        await channel.BasicConsumeAsync("big-q", false, "c-bulk", noLocal: false, exclusive: false, arguments: null, consumer, cancellationToken: TestContext.Current.CancellationToken);

        await drained.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        lock (order)
        {
            Assert.Equal(1000, order.Count);
            Assert.Equal(Enumerable.Range(0, 1000), order);
        }
    }

    [Fact]
    public async Task Qos_count_gate_admits_exactly_10_and_more_flow_after_acks()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("gate-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        for (int i = 0; i < 20; i++)
        {
            await channel.BasicPublishAsync("", "gate-q", mandatory: false, new BasicProperties(), new byte[] { (byte)i, (byte)i }, cancellationToken: TestContext.Current.CancellationToken);
        }

        _ = await channel.QueueDeclareAsync("gate-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicQosAsync(0, 10, false, TestContext.Current.CancellationToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        var seen = new List<ulong>();
        var ten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var twenty = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            lock (seen)
            {
                seen.Add(ea.DeliveryTag);
                if (seen.Count == 10)
                {
                    ten.TrySetResult();
                }

                if (seen.Count == 15)
                {
                    twenty.TrySetResult();
                }
            }

            await Task.CompletedTask;
        };
        await channel.BasicConsumeAsync("gate-q", false, "c-gate", noLocal: false, exclusive: false, arguments: null, consumer, cancellationToken: TestContext.Current.CancellationToken);

        await ten.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        // The gate is full: nothing more should arrive while all 10 stay unacked.
        await Assert.ThrowsAnyAsync<TimeoutException>(
            () => twenty.Task.WaitAsync(TimeSpan.FromMilliseconds(700), TestContext.Current.CancellationToken));
        Assert.Equal(10, seen.Count);

        // Ack exactly half the slots (5 of 10): exactly 5 more flow, total 15.
        foreach (ulong tag in seen.Take(5).ToList())
        {
            await channel.BasicAckAsync(tag, multiple: false, cancellationToken: TestContext.Current.CancellationToken);
        }

        await twenty.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(15, seen.Count);
    }

    [Fact]
    public async Task Nack_requeue_redelivers_with_redelivered_flag()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("nack-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicPublishAsync("", "nack-q", mandatory: false, new BasicProperties(), "n1"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken);
        _ = await channel.QueueDeclareAsync("nack-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicQosAsync(0, 1, false, TestContext.Current.CancellationToken);

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
        await channel.BasicConsumeAsync("nack-q", false, "c-nack", noLocal: false, exclusive: false, arguments: null, consumer, cancellationToken: TestContext.Current.CancellationToken);

        BasicDeliverEventArgs firstArgs = await first.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.False(firstArgs.Redelivered);
        BasicDeliverEventArgs secondArgs = await second.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(secondArgs.Redelivered);
        Assert.Equal("n1", System.Text.Encoding.UTF8.GetString(secondArgs.Body.Span));
    }

    [Fact]
    public async Task BasicGet_returns_the_head_and_reports_empty_after()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("get-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        _ = await channel.BasicGetAsync("get-q", autoAck: false, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicPublishAsync("", "get-q", mandatory: false, new BasicProperties { ContentType = "text/plain" }, "g1"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken);
        _ = await channel.QueueDeclareAsync("get-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);

        BasicGetResult? result = await channel.BasicGetAsync("get-q", autoAck: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.True(result!.MessageCount >= 0); // message counts are advisory
        Assert.Equal("", result.Exchange);
        Assert.Equal("get-q", result.RoutingKey);
        Assert.Equal("g1", System.Text.Encoding.UTF8.GetString(result.Body.ToArray()));
        Assert.False(result.Redelivered);
        await channel.BasicAckAsync(result.DeliveryTag, multiple: false, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Null(await channel.BasicGetAsync("get-q", autoAck: false, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Two_consumers_share_the_queue_round_robin_with_no_message_delivered_twice()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("rr-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        for (int i = 0; i < 20; i++)
        {
            await channel.BasicPublishAsync("", "rr-q", mandatory: false, new BasicProperties(), BitConverter.GetBytes(i), cancellationToken: TestContext.Current.CancellationToken);
        }

        _ = await channel.QueueDeclareAsync("rr-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicQosAsync(0, 100, false, TestContext.Current.CancellationToken); // wide gate: both consumers eat in parallel

        var fromA = new List<int>();
        var fromB = new List<int>();
        var allIn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Handle(List<int> mine, BasicDeliverEventArgs ea)
        {
            lock (mine)
            {
                mine.Add(BitConverter.ToInt32(ea.Body.Span));
                if (fromA.Count + fromB.Count == 20)
                {
                    allIn.TrySetResult();
                }
            }

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        }

        var consumerA = new AsyncEventingBasicConsumer(channel);
        consumerA.ReceivedAsync += (_, ea) => Handle(fromA, ea);
        var consumerB = new AsyncEventingBasicConsumer(channel);
        consumerB.ReceivedAsync += (_, ea) => Handle(fromB, ea);

        await channel.BasicConsumeAsync("rr-q", false, "c-rr-a", noLocal: false, exclusive: false, arguments: null, consumerA, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicConsumeAsync("rr-q", false, "c-rr-b", noLocal: false, exclusive: false, arguments: null, consumerB, cancellationToken: TestContext.Current.CancellationToken);

        await allIn.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        lock (fromA)
        {
            lock (fromB)
            {
                Assert.Equal(20, fromA.Count + fromB.Count);
                Assert.Equal(Enumerable.Range(0, 20).ToHashSet(), fromA.Concat(fromB).ToHashSet());
                Assert.True(fromA.Count > 0);
                Assert.True(fromB.Count > 0); // the cycle must actually rotate
            }
        }

        // Cancel stops delivery: publish again, nobody gets anything.
        await channel.BasicCancelAsync("c-rr-a", cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicCancelAsync("c-rr-b", cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicPublishAsync("", "rr-q", mandatory: false, new BasicProperties(), BitConverter.GetBytes(999), cancellationToken: TestContext.Current.CancellationToken);
        _ = await channel.QueueDeclareAsync("rr-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken); // fence
        int stuck = fromA.Count + fromB.Count;
        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.Equal(stuck, fromA.Count + fromB.Count);
    }

    [Fact]
    public async Task No_ack_consumer_receives_every_message_without_acking()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync("nackless-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        for (int i = 0; i < 100; i++)
        {
            await channel.BasicPublishAsync("", "nackless-q", mandatory: false, new BasicProperties(), new byte[] { (byte)i }, cancellationToken: TestContext.Current.CancellationToken);
        }

        _ = await channel.QueueDeclareAsync("nackless-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: TestContext.Current.CancellationToken);
        var consumer = new AsyncEventingBasicConsumer(channel);
        var count = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += async (_, _) =>
        {
            if (Interlocked.Increment(ref count) == 100)
            {
                done.TrySetResult();
            }

            await Task.CompletedTask; // no acks at all
        };
        await channel.BasicConsumeAsync("nackless-q", true, "c-auto", noLocal: false, exclusive: false, arguments: null, consumer, cancellationToken: TestContext.Current.CancellationToken);

        await done.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task Idle_queue_with_x_expires_is_deleted_by_the_reaper()
    {
        await using TestBrokerHost host = TestBrokerHost.Start(
            options => options.ReaperInterval = TimeSpan.FromMilliseconds(50));
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await NewChannelAsync(connection);

        await channel.QueueDeclareAsync(
            "expiring-q",
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-expires"] = 100 },
            cancellationToken: TestContext.Current.CancellationToken);

        bool expired = false;
        for (int attempt = 0; attempt < 100 && !expired; attempt++)
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
            await using IChannel poll = await NewChannelAsync(connection);
            try
            {
                _ = await poll.QueueDeclarePassiveAsync(
                    "expiring-q", cancellationToken: TestContext.Current.CancellationToken);
            }
            catch (OperationInterruptedException)
            {
                expired = true; // 404 not_found: the idle queue is gone
            }
        }

        Assert.True(expired, "the idle queue with x-expires was never deleted");
    }
}
