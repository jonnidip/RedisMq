using System.Diagnostics;
using System.Diagnostics.Metrics;
using RedisMQ.Broker.Diagnostics;
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
    private readonly BrokerDiagnostics _diagnostics;
    private readonly BrokerRuntimeState _runtime = new();
    private readonly List<IDisposable> _gaugeHandles = [];
    private readonly List<Task> _connectionTasks = [];
    private readonly object _gate = new();
    private CancellationTokenSource? _stoppingCts;
    private Task[] _acceptTasks = [];
    private Task _reaperTask = Task.CompletedTask;
    private bool _started;
    private int _acceptFaulted;

    /// <summary>The single-listener constructor: M0/M1 test harnesses and callers that
    /// run one transport. A null listener starts no accept loop.</summary>
    public AmqpBrokerServer(
        BrokerServerOptions options,
        IRouteEngine engine,
        IListenerTransport? listener = null,
        BrokerDiagnostics? diagnostics = null)
        : this(
            options,
            engine,
            listener is null ? Array.Empty<IListenerTransport>() : new[] { listener },
            diagnostics)
    {
    }

    /// <summary>The multi-listener constructor: one accept loop per listener, all
    /// sharing the server's engine and reaper. The caller owns the listeners.</summary>
    public AmqpBrokerServer(
        BrokerServerOptions options,
        IRouteEngine engine,
        IReadOnlyList<IListenerTransport> listeners,
        BrokerDiagnostics? diagnostics = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _listeners = listeners ?? throw new ArgumentNullException(nameof(listeners));
        _diagnostics = diagnostics ?? new BrokerDiagnostics();
    }

    /// <summary>The connection/channel/unacked counters the health snapshot and the
    /// observable gauges read.</summary>
    internal BrokerRuntimeState RuntimeState => _runtime;

    /// <summary>The diagnostics instance this server emits to (never disposed here).</summary>
    public BrokerDiagnostics Diagnostics => _diagnostics;

    /// <summary>
    /// A synchronous health snapshot for a host's liveness/readiness endpoint. Cheap
    /// enough to call on a scrape interval: it reads counters and one engine snapshot,
    /// and never touches the wire or Redis.
    /// </summary>
    public BrokerHealth GetHealth()
    {
        IEngineDiagnostics? engineDiagnostics = _engine as IEngineDiagnostics;
        EngineHealth engine = engineDiagnostics?.GetEngineHealth()
            ?? new EngineHealth("custom", false, null, 0, 0, 0, null, null, null);

        IReadOnlyList<QueueSnapshot> snapshots = engineDiagnostics?.QueueSnapshots() ?? [];
        Dictionary<(string Vhost, string Queue), long> unacked = [];
        foreach (KeyValuePair<(string Vhost, string Queue), long> entry in _runtime.UnackedSnapshot())
        {
            unacked[entry.Key] = entry.Value;
        }

        HashSet<(string Vhost, string Queue)> liveQueues = [];
        List<QueueSnapshot> queues = new(snapshots.Count);
        foreach (QueueSnapshot snapshot in snapshots)
        {
            (string Vhost, string Queue) key = (snapshot.Vhost, snapshot.Name);
            liveQueues.Add(key);
            int count = unacked.TryGetValue(key, out long value)
                ? (int)Math.Min(value, int.MaxValue)
                : 0;
            queues.Add(snapshot with { Unacked = count });
        }

        // A deleted queue must never leave a stale unacked series in health.
        _runtime.PruneUnacked(liveQueues);

        bool live = _started && Volatile.Read(ref _acceptFaulted) == 0;
        bool ready = live;
        string? detail = null;
        if (!live)
        {
            detail = _started ? "listener failed" : "server is not started";
        }
        else if (engine.Kind == "redis" && engine.Durable && engine.RedisConnected == false)
        {
            // New publishes cannot honour the durability promise while Redis is down.
            ready = false;
            detail = engine.LastError ?? "redis disconnected";
        }

        return new BrokerHealth(
            live,
            ready,
            detail,
            DateTimeOffset.UtcNow,
            _runtime.ActiveConnections,
            _runtime.ActiveChannels,
            queues,
            engine);
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
        _acceptFaulted = 0;
        _stoppingCts = new CancellationTokenSource();
        CancellationToken ct = _stoppingCts.Token;

        _gaugeHandles.Add(_diagnostics.RegisterObservableGauge(
            "redismq.connections.active",
            "{connection}",
            "Connections currently serving.",
            ObserveActiveConnections));
        _gaugeHandles.Add(_diagnostics.RegisterObservableGauge(
            "redismq.channels.active",
            "{channel}",
            "Channels currently open.",
            ObserveActiveChannels));
        _gaugeHandles.Add(_diagnostics.RegisterObservableGauge(
            "redismq.messages.unacked",
            "{message}",
            "Unacknowledged deliveries per queue.",
            ObserveUnacked));

        _acceptTasks = [.. _listeners.Select(listener => RunAcceptLoopAsync(listener, ct))];
        _reaperTask = _options.ReaperInterval > TimeSpan.Zero
            ? RunReaperAsync(_options.ReaperInterval, ct)
            : Task.CompletedTask;
        _diagnostics.WriteLog(
            BrokerLogLevel.Information,
            BrokerLogEvent.ServerStarted,
            "broker server started");
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

        foreach (IDisposable handle in _gaugeHandles)
        {
            handle.Dispose();
        }

        _gaugeHandles.Clear();
        _diagnostics.WriteLog(
            BrokerLogLevel.Information,
            BrokerLogEvent.ServerStopped,
            "broker server stopped");
    }

    /// <summary>One accept loop per listener: hands each transport side to a connection
    /// task.</summary>
    private async Task RunAcceptLoopAsync(IListenerTransport listener, CancellationToken ct)
    {
        string listenerKind = listener is TlsSocketListener ? "tls" : "plaintext";
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
            catch (Exception ex)
            {
                Interlocked.Exchange(ref _acceptFaulted, 1);
                _diagnostics.WriteLog(
                    BrokerLogLevel.Error,
                    BrokerLogEvent.ListenerFailed,
                    "listener accept failed",
                    ex);
                return;
            }

            if (side is null)
            {
                return; // listener closed: no more connections
            }

            TrackConnection(side, listenerKind, ct);
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
                long started = Stopwatch.GetTimestamp();
                try
                {
                    _engine.Reap(DateTimeOffset.UtcNow);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _diagnostics.Count(_diagnostics.ReaperFailures, new TagList());
                    _diagnostics.WriteLog(
                        BrokerLogLevel.Error,
                        BrokerLogEvent.ReaperFailed,
                        "reaper tick failed",
                        ex);
                }
                finally
                {
                    _diagnostics.Count(_diagnostics.ReaperSweeps, new TagList());
                    _diagnostics.Measure(
                        _diagnostics.ReaperDuration,
                        Stopwatch.GetElapsedTime(started).TotalSeconds,
                        new TagList());
                }
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
    private void TrackConnection(TransportSide side, string listenerKind, CancellationToken ct)
    {
        _runtime.ConnectionStarted();
        _diagnostics.Count(
            _diagnostics.ConnectionsOpened,
            new TagList { { "listener", listenerKind } });

        Task task = RunConnectionAsync(side, listenerKind, ct);
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

    private async Task RunConnectionAsync(
        TransportSide side,
        string listenerKind,
        CancellationToken ct)
    {
        try
        {
            var connection = new WireConnection(
                _options,
                _engine,
                side,
                _diagnostics,
                _runtime,
                listenerKind);
            await connection.RunAsync(ct); // server shutdown cancels this token
        }
        catch
        {
            // WireConnection guards its own protocol failures; a transport-level break
            // here just ends that connection.
        }
        finally
        {
            await CloseQuietlyAsync(side);
        }
    }

    private IEnumerable<Measurement<long>> ObserveActiveConnections()
    {
        yield return new Measurement<long>(_runtime.ActiveConnections);
    }

    private IEnumerable<Measurement<long>> ObserveActiveChannels()
    {
        yield return new Measurement<long>(_runtime.ActiveChannels);
    }

    private IEnumerable<Measurement<long>> ObserveUnacked()
    {
        foreach (KeyValuePair<(string Vhost, string Queue), long> entry in _runtime.UnackedSnapshot())
        {
            yield return new Measurement<long>(
                entry.Value,
                new TagList { { "vhost", entry.Key.Vhost }, { "queue", entry.Key.Queue } });
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
