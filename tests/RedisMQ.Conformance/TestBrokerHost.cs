using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Conformance;

/// <summary>
/// The conformance harness: binds the listeners the options describe (plaintext on an
/// ephemeral port and, when enabled, TLS on its configured port), runs an
/// <see cref="AmqpBrokerServer"/> over them with guest/guest on vhost "/", and exposes
/// both addresses for RabbitMQ.Client 7.x fixtures. Stop completes the server and then
/// disposes every listener (accept loops end, connections drain).
/// </summary>
public sealed class TestBrokerHost : IAsyncDisposable, IDisposable
{
    private readonly AmqpBrokerServer _server;
    private readonly IReadOnlyList<IListenerTransport> _listeners;
    private readonly int _port;
    private readonly int? _tlsPort;

    private TestBrokerHost(
        AmqpBrokerServer server,
        IReadOnlyList<IListenerTransport> listeners,
        int port,
        int? tlsPort)
    {
        _server = server;
        _listeners = listeners;
        _port = port;
        _tlsPort = tlsPort;
    }

    /// <summary>The plaintext TCP port (ephemeral when started unnamed); -1 when the
    /// options disabled plaintext.</summary>
    public int Port => _port;

    /// <summary>The TLS port when a TLS listener is running; null otherwise.</summary>
    public int? TlsPort => _tlsPort;

    public static TestBrokerHost Start(Action<BrokerServerOptions>? configure = null)
    {
        BrokerServerOptions options = NewOptions();
        configure?.Invoke(options);
        return StartCore(new InMemoryEngine(options.Vhosts), options);
    }

    /// <summary>Starts the host against a supplied <see cref="IRouteEngine"/> instead of
    /// the default <see cref="InMemoryEngine"/>. Used by M1 regression tests to verify
    /// that the M0 wire surface is engine-agnostic.</summary>
    public static TestBrokerHost Start(IRouteEngine engine, Action<BrokerServerOptions>? configure = null)
    {
        BrokerServerOptions options = NewOptions();
        configure?.Invoke(options);
        return StartCore(engine, options);
    }

    public async Task StopAsync()
    {
        // The server drains its connections first, then the listeners die.
        await _server.StopAsync();
        foreach (IListenerTransport listener in _listeners)
        {
            if (listener is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync();
            }
        }
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

    private static BrokerServerOptions NewOptions() => new()
    {
        Port = 0, // ephemeral: the announced port comes off the bound socket
        Vhosts = ["/"],
        Users = [BrokerUser.FullAccess("guest", "guest", "/")],
    };

    private static TestBrokerHost StartCore(IRouteEngine engine, BrokerServerOptions options)
    {
        options.Validate();

        IReadOnlyList<IListenerTransport> listeners = BrokerListeners.Create(options);
        var server = new AmqpBrokerServer(options, engine, listeners);
        // The listeners de dicto ports (0 → OS-assigned); the server negotiates through
        // its options (frame-max offer, heartbeat offer) only.
        server.StartAsync().GetAwaiter().GetResult();

        int port = -1;
        int? tlsPort = null;
        foreach (IListenerTransport listener in listeners)
        {
            if (listener is TlsSocketListener)
            {
                tlsPort = listener.AnnouncedPort;
            }
            else
            {
                port = listener.AnnouncedPort ?? -1;
            }
        }

        return new TestBrokerHost(server, listeners, port, tlsPort);
    }
}
