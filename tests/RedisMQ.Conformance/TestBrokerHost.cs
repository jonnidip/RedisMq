using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Conformance;

/// <summary>
/// The conformance harness: binds a real TCP listener on an ephemeral port (the
/// SocketListener announced port), runs an <see cref="AmqpBrokerServer"/> over it with
/// guest/guest on vhost "/", and exposes the address for RabbitMQ.Client 7.x fixtures.
/// Stop completes the server and the listener (accept loop ends, connections drain).
/// </summary>
public sealed class TestBrokerHost : IAsyncDisposable, IDisposable
{
    private readonly AmqpBrokerServer _server;
    private readonly SocketListener _listener;

    private TestBrokerHost(AmqpBrokerServer server, SocketListener listener)
    {
        _server = server;
        _listener = listener;
    }

    /// <summary>The TCP port the listener bound (ephemeral when started unnamed).</summary>
    public int Port => _listener.AnnouncedPort;

    public static TestBrokerHost Start(Action<BrokerServerOptions>? configure = null)
    {
        var options = new BrokerServerOptions
        {
            Port = 0, // ephemeral: the announced port comes off the bound socket
            Vhosts = ["/"],
            Users = [BrokerUser.FullAccess("guest", "guest", "/")],
        };
        configure?.Invoke(options);
        options.Validate();

        var listener = new SocketListener(options.Port);
        // The listener de dicto port (0 → OS-assigned); the server negotiates through
        // its options (frame-max offer, heartbeat offer) only.
        var server = new AmqpBrokerServer(options, new InMemoryEngine(options.Vhosts), listener);
        server.StartAsync().GetAwaiter().GetResult();
        return new TestBrokerHost(server, listener);
    }

    /// <summary>Starts the host against a supplied <see cref="IRouteEngine"/> instead of
    /// the default <see cref="InMemoryEngine"/>. Used by M1 regression tests to verify
    /// that the M0 wire surface is engine-agnostic.</summary>
    public static TestBrokerHost Start(IRouteEngine engine, Action<BrokerServerOptions>? configure = null)
    {
        var options = new BrokerServerOptions
        {
            Port = 0,
            Vhosts = ["/"],
            Users = [BrokerUser.FullAccess("guest", "guest", "/")],
        };
        configure?.Invoke(options);
        options.Validate();

        var listener = new SocketListener(options.Port);
        var server = new AmqpBrokerServer(options, engine, listener);
        server.StartAsync().GetAwaiter().GetResult();
        return new TestBrokerHost(server, listener);
    }

    public async Task StopAsync()
    {
        // The server drains its connections first, then the listener dies.
        await _server.StopAsync();
        await _listener.DisposeAsync();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await StopAsync();
    }

    /// <summary>Sync convenience for using statements that avoid the awaitable form.</summary>
    public void Dispose()
    {
        StopAsync().GetAwaiter().GetResult();
    }
}
