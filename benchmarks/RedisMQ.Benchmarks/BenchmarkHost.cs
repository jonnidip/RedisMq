using System.Diagnostics;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using RedisMQ.Redis;
using StackExchange.Redis;

namespace RedisMQ.Benchmarks;

/// <summary>
/// One benchmark run's broker: a real Redis multiplexer, a real
/// <see cref="RedisStreamRouteEngine"/>, a real AMQP listener on an ephemeral port, and
/// a unique vhost. It mirrors the conformance harness, but never flushes or scans the
/// store: cleanup deletes only the queue keys this run declared and its topology key.
/// </summary>
internal sealed class BenchmarkHost : IAsyncDisposable
{
    private static readonly TimeSpan DrainQuietWindow = TimeSpan.FromMilliseconds(500);

    private readonly IConnectionMultiplexer _redis;
    private readonly IDatabase _database;
    private readonly AmqpBrokerServer _server;
    private readonly IReadOnlyList<IListenerTransport> _listeners;
    private readonly List<string> _queues = [];
    private bool _stopped;
    private bool _engineDisposed;
    private bool _disposed;

    private BenchmarkHost(
        IConnectionMultiplexer redis,
        RedisStreamRouteEngine engine,
        AmqpBrokerServer server,
        IReadOnlyList<IListenerTransport> listeners,
        string vhost,
        int port)
    {
        _redis = redis;
        _database = redis.GetDatabase();
        Engine = engine;
        _server = server;
        _listeners = listeners;
        Vhost = vhost;
        Port = port;
    }

    public string Vhost { get; }

    public int Port { get; }

    public RedisStreamRouteEngine Engine { get; }

    public IDatabase Database => _database;

    public static async Task<BenchmarkHost> StartAsync(BenchmarkOptions options, CancellationToken ct)
    {
        var configuration = ConfigurationOptions.Parse(options.Redis);
        configuration.AbortOnConnectFail = true;

        IConnectionMultiplexer redis;
        try
        {
            redis = await ConnectionMultiplexer.ConnectAsync(configuration);
        }
        catch (Exception ex)
        {
            throw new BenchmarkRunException(
                $"Redis is unreachable at {options.Redis}: {ex.Message}", ex);
        }

        string vhost = "/bench-" + Guid.NewGuid().ToString("N")[..8];
        var engine = new RedisStreamRouteEngine(
            redis.GetDatabase(),
            [vhost],
            replayOnStart: false,
            drainerCapacity: options.DrainerCapacity,
            enforceEvictionGate: true,
            diagnostics: null,
            drainerBatchSize: options.DrainerBatchSize);

        var serverOptions = new BrokerServerOptions
        {
            Port = 0,
            Vhosts = [vhost],
            Users = [BrokerUser.FullAccess("guest", "guest", vhost)],
        };
        IReadOnlyList<IListenerTransport> listeners = [];
        AmqpBrokerServer? server = null;
        try
        {
            serverOptions.Validate();
            listeners = BrokerListeners.Create(serverOptions);
            server = new AmqpBrokerServer(serverOptions, engine, listeners);
            await server.StartAsync();
        }
        catch
        {
            // A failed start must not leak the multiplexer, the engine or its sockets.
            if (server is not null)
            {
                await StopAndDisposeAsync(server, listeners);
            }
            else
            {
                await DisposeListenersAsync(listeners);
            }

            await engine.DisposeAsync();
            redis.Dispose();
            throw;
        }

        AmqpBrokerServer started = server!;
        int port = -1;
        foreach (IListenerTransport listener in listeners)
        {
            if (listener is not TlsSocketListener)
            {
                port = listener.AnnouncedPort ?? -1;
            }
        }

        if (port <= 0)
        {
            await StopAndDisposeAsync(started, listeners);
            await engine.DisposeAsync();
            redis.Dispose();
            throw new BenchmarkRunException("The plaintext listener did not bind a port.");
        }

        return new BenchmarkHost(redis, engine, started, listeners, vhost, port);
    }

    /// <summary>Best-effort teardown after a failed start: the start failure is the
    /// error worth surfacing, not a follow-on dispose error.</summary>
    private static async Task StopAndDisposeAsync(
        AmqpBrokerServer server,
        IReadOnlyList<IListenerTransport> listeners)
    {
        try
        {
            await server.StopAsync();
        }
        catch
        {
            // The server never reached a running state.
        }

        await DisposeListenersAsync(listeners);
    }

    private static async Task DisposeListenersAsync(IReadOnlyList<IListenerTransport> listeners)
    {
        foreach (IListenerTransport listener in listeners)
        {
            if (listener is IAsyncDisposable disposable)
            {
                try
                {
                    await disposable.DisposeAsync();
                }
                catch
                {
                    // The start failure is already propagating.
                }
            }
        }
    }

    /// <summary>Records a queue this run declared so <see cref="CleanupAsync"/> can
    /// delete exactly its keys.</summary>
    public void TrackQueue(string name)
    {
        lock (_queues)
        {
            _queues.Add(name);
        }
    }

    /// <summary>Polls the engine's drainer depth every 50 ms and returns only after it
    /// has been zero continuously for 500 ms. One zero sample is not enough: the
    /// drainer removes a batch from the channel before writing it.</summary>
    public async Task WaitForDrainAsync(TimeSpan timeout, CancellationToken ct)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        DateTime? zeroSince = null;
        while (DateTime.UtcNow < deadline)
        {
            long depth = Engine.GetEngineHealth().DrainerDepth;
            if (depth == 0)
            {
                zeroSince ??= DateTime.UtcNow;
                if (DateTime.UtcNow - zeroSince >= DrainQuietWindow)
                {
                    return;
                }
            }
            else
            {
                zeroSince = null;
            }

            await Task.Delay(50, ct);
        }

        throw new BenchmarkRunException(
            $"Timed out waiting for the Redis drainer to reach zero depth after {timeout.TotalSeconds:0} s.");
    }

    /// <summary>Stops the mirror before key cleanup: the engine's disposal awaits the
    /// drainer, so an issued batch cannot land after <see cref="CleanupAsync"/> and
    /// recreate a deleted key. Idempotent with <see cref="DisposeAsync"/>.</summary>
    public async Task DisposeEngineAsync()
    {
        if (_engineDisposed)
        {
            return;
        }

        _engineDisposed = true;
        await Engine.DisposeAsync();
    }

    public async Task StopAsync()
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        await _server.StopAsync();
        foreach (IListenerTransport listener in _listeners)
        {
            if (listener is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync();
            }
        }
    }

    /// <summary>Deletes this run's queue keys and topology key in batches of 100. No
    /// <c>FLUSHDB</c>, no <c>SCAN</c>, no admin commands.</summary>
    public async Task CleanupAsync()
    {
        string[] queueKeys;
        lock (_queues)
        {
            queueKeys = [.. _queues.Distinct(StringComparer.Ordinal)
                .Select(queue => RedisKeys.QueueKey(Vhost, queue))];
        }

        List<string> keys = [.. queueKeys, RedisKeys.TopologyKey(Vhost)];
        foreach (string[] chunk in keys.Chunk(100))
        {
            await Task.WhenAll(chunk.Select(key => _database.KeyDeleteAsync(key)));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await StopAsync();
        await DisposeEngineAsync();
        _redis.Dispose();
    }
}
