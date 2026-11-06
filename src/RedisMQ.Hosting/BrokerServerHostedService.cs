using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Hosting;

public sealed class BrokerServerHostedService : IHostedService, IDisposable, IAsyncDisposable
{
    private readonly RedisMqOptions _options;
    private readonly AmqpBrokerServer _server;
    private readonly IReadOnlyList<IListenerTransport> _listeners;
    private readonly ILogger? _logger;
    private int _started;
    private int _stopped;

    public BrokerServerHostedService(
        RedisMqOptions options,
        AmqpBrokerServer server,
        IReadOnlyList<IListenerTransport> listeners,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options;
        _server = server;
        _listeners = listeners;
        _logger = loggerFactory?.CreateLogger("RedisMQ.Hosting");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _started) != 0)
        {
            return;
        }

        if (_options.Broker.Port < 0 && _options.Broker.Tls is not { Enabled: true })
        {
            throw new BrokerConfigurationException(
                "RedisMQ has no enabled listener: set Broker:Port >= 0 or Broker:Tls:Enabled=true");
        }

        _options.Broker.Validate();
        if (_options.Broker.Users.Count == 0)
        {
            _logger?.LogWarning(
                "RedisMQ has no broker users; default-deny means every client will be refused");
        }

        await _server.StartAsync();
        Interlocked.Exchange(ref _started, 1);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        await _server.StopAsync();
        foreach (IListenerTransport listener in _listeners)
        {
            if (listener is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else if (listener is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    public async ValueTask DisposeAsync() => await StopAsync(CancellationToken.None);

    // Hosts that dispose their provider synchronously must not fail on this service.
    public void Dispose() => StopAsync(CancellationToken.None).GetAwaiter().GetResult();
}
