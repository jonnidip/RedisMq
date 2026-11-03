using RedisMQ.Broker.Server;

namespace RedisMQ.Broker.Transport;

/// <summary>
/// Builds the listeners a <see cref="BrokerServerOptions"/> describes: plaintext on
/// <see cref="BrokerServerOptions.Port"/> when it is not negative, and TLS on
/// <see cref="BrokerTlsOptions.Port"/> when TLS is enabled. The caller owns the returned
/// listeners and disposes them; <see cref="AmqpBrokerServer"/> only consumes them.
/// </summary>
public static class BrokerListeners
{
    public static IReadOnlyList<IListenerTransport> Create(BrokerServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        List<IListenerTransport> listeners = [];
        try
        {
            if (options.Port >= 0)
            {
                listeners.Add(new SocketListener(options.Port));
            }

            if (options.Tls is { Enabled: true } tls)
            {
                listeners.Add(new TlsSocketListener(tls));
            }
        }
        catch
        {
            // A half-created set would leak its bound sockets: release them before the
            // configuration failure propagates.
            DisposeAll(listeners);
            throw;
        }

        return listeners;
    }

    private static void DisposeAll(List<IListenerTransport> listeners)
    {
        foreach (IListenerTransport listener in listeners)
        {
            if (listener is IAsyncDisposable disposable)
            {
                try
                {
                    disposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
                catch
                {
                    // Best effort: the configuration error is the one worth surfacing.
                }
            }
        }
    }
}
