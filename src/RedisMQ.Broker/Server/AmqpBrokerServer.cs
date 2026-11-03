using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Broker.Server;

/// <summary>
/// The broker's connection lifecycle: one accept loop per
/// <see cref="IListenerTransport"/>, handing every accepted transport side to a fresh
/// <see cref="WireConnection"/>. Task 8 supplies the TCP listeners (and
/// <see cref="BrokerServerOptions.Port"/>); Task 13 adds the TLS listener and the
/// factory, while M0 tests inject listeners over <see cref="TransportPair"/>s. With no
/// listener StartAsync simply starts nothing.
/// </summary>
public sealed class AmqpBrokerServer
{
    private readonly BrokerServerOptions _options;
    private readonly IRouteEngine _engine;
    private readonly IReadOnlyList<IListenerTransport> _listeners;
    private readonly List<Task> _connectionTasks = [];
    private readonly object _gate = new();
    private CancellationTokenSource? _stoppingCts;
    private Task[] _acceptTasks = [];
    private Task _reaperTask = Task.CompletedTask;
    private bool _started;

    /// <summary>The single-listener constructor: M0/M1 test harnesses and callers that
    /// run one transport. A null listener starts no accept loop.</summary>
    public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine, IListenerTransport? listener = null)
        : this(
            options,
            engine,
            listener is null ? Array.Empty<IListenerTransport>() : new[] { listener })
    {
    }

    /// <summary>The multi-listener constructor: one accept loop per listener, all
    /// sharing the server's engine and reaper. The caller owns the listeners.</summary>
    public AmqpBrokerServer(
        BrokerServerOptions options,
        IRouteEngine engine,
        IReadOnlyList<IListenerTransport> listeners)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _listeners = listeners ?? throw new ArgumentNullException(nameof(listeners));
    }

    /// <summary>
    /// Starts every accept loop. Returns as soon as accepting is armed (the loops
    /// themselves run in the background); no-op when the server was constructed without
    /// a listener. The stop call <see cref="StopAsync"/> ends the loops and drains
    /// connections.
    /// </summary>
    public Task StartAsync()
    {
        if (_started)
        {
            throw new InvalidOperationException("The server is already started");
        }

        _options.Validate();
        _started = true;
        _stoppingCts = new CancellationTokenSource();
        CancellationToken ct = _stoppingCts.Token;
        _acceptTasks = [.. _listeners.Select(listener => RunAcceptLoopAsync(listener, ct))];
        _reaperTask = _options.ReaperInterval > TimeSpan.Zero
            ? RunReaperAsync(_options.ReaperInterval, ct)
            : Task.CompletedTask;
        return Task.CompletedTask;
    }

    /// <summary>Cancels accepting and waits for every accept loop plus every accepted
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
            await Task.WhenAll(_acceptTasks);
        }
        catch
        {
            // Each accept loop follows its listener's own failure surface.
        }

        try
        {
            await _reaperTask;
        }
        catch
        {
            // The reaper loop exits on cancellation; nothing else to observe.
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

    /// <summary>One accept loop per listener: hands each transport side to a connection
    /// task.</summary>
    private async Task RunAcceptLoopAsync(IListenerTransport listener, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TransportSide? side;
            try
            {
                side = await listener.AcceptAsync(ct);
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

    /// <summary>One shared reaper loop for the whole server (never one per listener):
    /// it sweeps expired messages and idle queues on the configured interval until
    /// <see cref="StopAsync"/> cancels it.</summary>
    private async Task RunReaperAsync(TimeSpan interval, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                _engine.Reap(DateTimeOffset.UtcNow);
            }
        }
        catch (OperationCanceledException)
        {
            // stopping
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
