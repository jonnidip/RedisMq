using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace RedisMQ.Conformance;

/// <summary>
/// Error-surface conformance: server-initiated channel/connection closes carrying the
/// RabbitMQ reply codes, wordings and the offending method's ids, surfaced by
/// RabbitMQ.Client as OperationInterruptedException with ShutdownEventArgs details.
/// (channel.flow has no client API in RabbitMQ.Client 7.x — its tests live in
/// <see cref="WireFlowTests"/> at the wire level.)
/// </summary>
public sealed class ErrorTests
{
    private static async Task<IConnection> ConnectAsync(TestBrokerHost host)
    {
        var factory = new ConnectionFactory { UserName = "guest", Password = "guest", VirtualHost = "/" };
        return await factory.CreateConnectionAsync([new AmqpTcpEndpoint("localhost", host.Port)]);
    }

    /// <summary>Runs the action expecting RabbitMQ.Client to surface the server's
    /// close as OperationInterruptedException; the test fails at once when the
    /// close never arrives.</summary>
    private static async Task<OperationInterruptedException> ExpectCloseAsync(Func<CancellationToken, Task> action)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        try
        {
            await action(ct);
            throw new InvalidOperationException("expected the operation to be interrupted by a close");
        }
        catch (OperationInterruptedException ex)
        {
            return ex;
        }
    }

    private static void AssertClose(OperationInterruptedException ex, ushort replyCode, ushort classId, ushort methodId, string textFragment)
    {
        ShutdownEventArgs reason = ex.ShutdownReason ?? throw new InvalidOperationException("ShutdownReason must not be null on OperationInterruptedException");
        Assert.Equal(replyCode, reason.ReplyCode);
        Assert.Equal(classId, reason.ClassId);
        Assert.Equal(methodId, reason.MethodId);
        Assert.Contains(textFragment, reason.ReplyText);
    }

    [Fact]
    public async Task Publish_to_unknown_exchange_closes_the_channel_404()
    {
        using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);

        await channel.QueueDeclareAsync("any-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicPublishAsync("nope", "anything", mandatory: false, new BasicProperties(), "x"u8.ToArray(), cancellationToken: TestContext.Current.CancellationToken);
        // The publish is one-way; the close lands at the next RPC on this channel.
        OperationInterruptedException ex = await ExpectCloseAsync(ct =>
            channel.QueueDeclareAsync("any-q", durable: false, exclusive: false, autoDelete: false, arguments: null, passive: true, cancellationToken: ct));
        AssertClose(ex, 404, classId: 60, methodId: 40, "no exchange 'nope' in vhost '/'");
        Assert.True(channel.IsClosed);
    }

    [Fact]
    public async Task Redeclare_queue_with_mismatched_flags_closes_406()
    {
        using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);

        CancellationToken ct = TestContext.Current.CancellationToken;
        await channel.QueueDeclareAsync("m-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: ct);
        OperationInterruptedException ex = await ExpectCloseAsync(token =>
            channel.QueueDeclareAsync("m-q", durable: true, exclusive: false, autoDelete: false, cancellationToken: token));
        AssertClose(ex, 406, classId: 50, methodId: 10, "equivalent");
        Assert.True(channel.IsClosed);
    }

    [Fact]
    public async Task Second_exclusive_consumer_is_refused_405()
    {
        using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);

        CancellationToken ct = TestContext.Current.CancellationToken;
        await channel.QueueDeclareAsync("x-q", durable: false, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.BasicConsumeAsync("x-q", true, "first-exclusive", noLocal: false, exclusive: true, arguments: null, new AsyncEventingBasicConsumer(channel), ct);

        OperationInterruptedException ex = await ExpectCloseAsync(token =>
            channel.BasicConsumeAsync("x-q", true, "second", noLocal: false, exclusive: true, arguments: null, new AsyncEventingBasicConsumer(channel), token));
        AssertClose(ex, 405, classId: 60, methodId: 20, "exclusive access to locked queue 'x-q'");
        Assert.True(channel.IsClosed);
    }

    [Fact]
    public async Task Consume_unknown_queue_closes_404()
    {
        using TestBrokerHost host = TestBrokerHost.Start();
        await using IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);

        OperationInterruptedException ex = await ExpectCloseAsync(ct =>
            channel.BasicConsumeAsync("missing", true, "c-missing", noLocal: false, exclusive: false, arguments: null, new AsyncEventingBasicConsumer(channel), ct));
        AssertClose(ex, 404, classId: 60, methodId: 20, "no queue 'missing' in vhost '/'");
        Assert.True(channel.IsClosed);
    }

    [Fact]
    public async Task Clean_close_rides_no_errors_and_leaves_the_connection_closed()
    {
        using TestBrokerHost host = TestBrokerHost.Start();
        IConnection connection = await ConnectAsync(host);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);

        await connection.CloseAsync(TestContext.Current.CancellationToken);

        Assert.False(connection.IsOpen);
        Assert.True(channel.IsClosed);
        await host.StopAsync();
    }
}
