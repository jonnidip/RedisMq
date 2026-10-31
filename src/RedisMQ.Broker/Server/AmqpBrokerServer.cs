using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Broker.Server;

/// <summary>
/// The broker's connection lifecycle: an accept loop over an optional
/// <see cref="IListenerTransport"/>, handing every accepted transport side to a fresh
/// <see cref="WireConnection"/>. Task 8 supplies the TCP listener (and the
/// <see cref="BrokerServerOptions.Port"/> bind); M0 tests inject listeners over
/// <see cref="TransportPair"/>s, and with no listener StartAsync simply starts nothing.
/// </summary>
public sealed class AmqpBrokerServer
{
    private readonly BrokerServerOptions _options;
    private readonly IRouteEngine _engine;
    private readonly IListenerTransport? _listener;
    private readonly List<Task> _connectionTasks = [];
    private readonly object _gate = new();
    private CancellationTokenSource? _stoppingCts;
    private Task _acceptTask = Task.CompletedTask;
    private bool _started;

    public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine, IListenerTransport? listener = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _listener = listener;
    }

    /// <summary>
    /// Starts the accept loop. Returns as soon as accepting is armed (the loop itself
    /// runs in the background); no-op when the server was constructed without a
    /// listener. The stop call <see cref="StopAsync"/> ends the loop and drains
    /// connections.
    /// </summary>
    public Task StartAsync()
    {
        if (_started)
        {
            throw new InvalidOperationException("The server is already started");
        }

        _started = true;
        _stoppingCts = new CancellationTokenSource();
        _acceptTask = _listener is null
            ? Task.CompletedTask
            : RunAcceptLoopAsync(_stoppingCts.Token);
        return Task.CompletedTask;
    }

    /// <summary>Cancels accepting and waits for the accept loop plus every accepted
    /// connection task. Exceptions observed there are swallowed per-connection.</summary>
    public async Task StopAsync()
    {
        if (!_started)
        {
            return;
        }

        _started = false;
        _stoppingCts!.Cancel();
        try
        {
            await _acceptTask;
        }
        catch
        {
            // The accept loop follows the listener's own failure surface.
        }

        Task[] connections;
        lock (_gate)
        {
            connections = [.. _connectionTasks];
        }

        try
        {
            await Task.WhenAll(connections);
        }
        catch
        {
            // Connection tasks guard their own failures; this only observes stragglers.
        }
    }

    /// <summary>The accept loop: hands each transport side to a connection task.</summary>
    private async Task RunAcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TransportSide? side;
            try
            {
                side = await _listener!.AcceptAsync(ct);
            }
            catch (OperationCanceledException)
            {
                return; // stopping
            }

            if (side is null)
            {
                return; // listener closed: no more connections
            }

            TrackConnection(side, ct);
        }
    }

    /// <summary>Runs one connection and keeps its task shelved until StopAsync. The
    /// stopping token flows into the connection, so a server with live (even idle,
    /// heartbeat-0) connections can stop promptly: the run ends without a close frame
    /// and the transport completes (RabbitMQ-parity wire kill; the client sees EOF).</summary>
    private void TrackConnection(TransportSide side, CancellationToken ct)
    {
        Task task = RunConnectionAsync(side, ct);
        bool added;
        lock (_gate)
        {
            added = _started;
            if (added)
            {
                _connectionTasks.Add(task);
            }
        }

        if (!added)
        {
            // Stopped between the accept and the tracking: close the transport at once.
            _ = CloseQuietlyAsync(side);
            return;
        }

        _ = task.ContinueWith(t =>
        {
            lock (_gate)
            {
                _connectionTasks.Remove(t);
            }
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task RunConnectionAsync(TransportSide side, CancellationToken ct)
    {
        try
        {
            var connection = new WireConnection(_options, _engine, side);
            await connection.RunAsync(ct); // server shutdown cancels this token
        }
        catch
        {
            // WireConnection guards its own protocol failures; a transport-level break
            // here just ends that connection. M0 has no logging surface to record it.
        }
        finally
        {
            await CloseQuietlyAsync(side);
        }
    }

    private static async Task CloseQuietlyAsync(TransportSide side)
    {
        try
        {
            await side.CompleteAsync();
        }
        catch
        {
            // A completed transport is complete; nothing to observe.
        }
    }
}
