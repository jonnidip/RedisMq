using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Redis;
using StackExchange.Redis;
using Xunit;

namespace RedisMQ.Redis.Tests;

// Observability Task 7: the Redis drainer's mirror health. A failed write is counted
// and logged once per episode; recovery logs once; a saturated or completed drainer
// degrades the mirror without flipping readiness; startup replay counts restored
// messages. Every test skips cleanly when Redis is unavailable.
public sealed class ObservabilityRedisTests
{
    [Fact]
    public async Task Mirror_failure_is_counted_logged_once_and_recovery_is_logged()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        var sink = new RedisRecordingLogSink();
        using var diag = new BrokerDiagnostics("redis1", sink);
        using var meter = new RedisMeterCollector();
        string vhost = $"/obs-mirror-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16,
            enforceEvictionGate: false, diagnostics: diag);
        engine.WriteOverrideForTest = _ => throw new InvalidOperationException("mirror down");

        EnqueueReceipt failed = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        Assert.False(await failed.Durable);
        await WaitUntilAsync(
            () => meter.For("redismq.redis.mirror.commands")
                .Any(r => r.Tags["broker.instance"] is "redis1" && (string?)r.Tags["outcome"] == "failure"),
            "the mirror failure was not counted");

        engine.WriteOverrideForTest = null;
        EnqueueReceipt recovered = engine.Enqueue(vhost, "q", new ContentProperties(), [2], "", "q");
        Assert.True(await recovered.Durable);

        Assert.Single(sink.Entries, e =>
            e.Event == BrokerLogEvent.RedisMirrorFailed && e.Level == BrokerLogLevel.Error);
        Assert.Contains(sink.Entries, e => e.Event == BrokerLogEvent.RedisMirrorRecovered);
        Assert.True(engine.GetEngineHealth().MirrorFailures >= 1);
        Assert.NotNull(engine.GetEngineHealth().LastMirrorFailureUtc);
    }

    [Fact]
    public async Task Saturated_drainer_refuses_the_publish_and_counts_it()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        using var diag = new BrokerDiagnostics("redis2");
        using var meter = new RedisMeterCollector();
        string vhost = $"/obs-sat-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 1,
            enforceEvictionGate: false, diagnostics: diag);
        Assert.True(engine.DrainerSlotsForTest!.Wait(0, TestContext.Current.CancellationToken));
        try
        {
            EnqueueReceipt refused = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
            Assert.Equal(0UL, refused.EnvelopeId);
            Assert.False(await refused.Durable);
        }
        finally
        {
            engine.DrainerSlotsForTest!.Release();
        }

        Assert.Equal(1, meter.For("redismq.redis.drainer.saturated")
            .Count(r => r.Tags["broker.instance"] is "redis2"));
    }

    [Fact]
    public async Task Drainer_channel_drop_is_counted_without_flipping_health()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        using var diag = new BrokerDiagnostics("redis3");
        using var meter = new RedisMeterCollector();
        string vhost = $"/obs-drop-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16,
            enforceEvictionGate: false, diagnostics: diag);
        Assert.True(engine.CompleteDrainerForTest());

        engine.DeclareExchange(new ExchangeInfo(vhost, "x", "direct"));

        MeasurementRecord dropped = Assert.Single(
            meter.For("redismq.redis.mirror.dropped"),
            r => r.Tags["broker.instance"] is "redis3");
        Assert.Equal("channel_full", dropped.Tags["reason"]);
        EngineHealth health = engine.GetEngineHealth();
        Assert.True(health.Durable);
        Assert.True(health.RedisConnected);
        Assert.Equal(1, health.MirrorDropped);
    }

    [Fact]
    public async Task Redis_health_reports_connected_and_depth()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        using var diag = new BrokerDiagnostics("redis4");
        string vhost = $"/obs-health-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16,
            enforceEvictionGate: false, diagnostics: diag);
        EngineHealth health = engine.GetEngineHealth();
        Assert.Equal("redis", health.Kind);
        Assert.True(health.Durable);
        Assert.True(health.RedisConnected);
        Assert.Equal(0, health.DrainerDepth);
    }

    [Fact]
    public async Task Replay_counts_restored_messages()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        using var diag = new BrokerDiagnostics("redis5");
        using var meter = new RedisMeterCollector();
        string vhost = $"/obs-replay-{Guid.NewGuid():N}";
        await using (var writer = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false))
        {
            writer.DeclareQueue(new QueueInfo(vhost, "q"));
            Assert.True(await writer.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q").Durable);
        }

        await using var replay = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: true, drainerCapacity: 16,
            enforceEvictionGate: false, diagnostics: diag);
        Assert.Equal(1, meter.For("redismq.redis.replay.messages")
            .Count(r => r.Tags["broker.instance"] is "redis5"));
    }

    [Fact]
    public async Task Dispose_counts_queued_writes_as_shutdown_drops()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        using var diag = new BrokerDiagnostics("redis6");
        using var meter = new RedisMeterCollector();
        string vhost = $"/obs-shutdown-{Guid.NewGuid():N}";
        // Batch size 1 keeps one command in flight so the two behind it stay queued
        // when disposal cancels the loop; with a larger batch they would all be
        // issued together and none would be abandoned.
        var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 16,
            enforceEvictionGate: false, diagnostics: diag, drainerBatchSize: 1);
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        engine.WriteOverrideForTest = _ => gate.Task;

        _ = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        EnqueueReceipt second = engine.Enqueue(vhost, "q", new ContentProperties(), [2], "", "q");
        EnqueueReceipt third = engine.Enqueue(vhost, "q", new ContentProperties(), [3], "", "q");

        // Wait until the drainer holds the first command, so the two behind it are
        // deterministically still queued when disposal cancels the loop.
        await WaitUntilAsync(
            () => engine.GetEngineHealth().DrainerDepth == 2,
            "the drainer did not take the first command");

        ValueTask disposal = engine.DisposeAsync();
        gate.SetResult(true);
        await disposal;

        Assert.False(await second.Durable);
        Assert.False(await third.Durable);
        Assert.Equal(2, engine.GetEngineHealth().MirrorDropped);
        Assert.Equal(
            2,
            meter.For("redismq.redis.mirror.dropped").Count(r =>
                r.Tags["broker.instance"] is "redis6"
                && (string?)r.Tags["reason"] == "shutdown"));
    }

    [Fact]
    public async Task Drainer_records_batch_size()
    {
        IDatabase? db = TryConnectRedis();
        if (db is null)
        {
            return;
        }

        using var diag = new BrokerDiagnostics("redis-batch");
        using var meter = new RedisMeterCollector();
        string vhost = $"/obs-batch-{Guid.NewGuid():N}";
        await using var engine = new RedisStreamRouteEngine(
            db, [vhost], replayOnStart: false, drainerCapacity: 64,
            enforceEvictionGate: false, diagnostics: diag, drainerBatchSize: 16);

        for (int i = 0; i < 32; i++)
        {
            EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [(byte)i], "", "q");
            Assert.True(await receipt.Durable);
        }

        await WaitUntilAsync(
            () => meter.For("redismq.redis.mirror.batch.size").Count > 0,
            "the drainer batch histogram");
        Assert.All(
            meter.For("redismq.redis.mirror.batch.size"),
            measurement => Assert.InRange((long)measurement.Value, 1, 16));
    }

    private static IDatabase? TryConnectRedis()
    {
        try
        {
            var connectTask = ConnectionMultiplexer.ConnectAsync("127.0.0.1:6379");
            if (connectTask.Wait(TimeSpan.FromSeconds(2)))
            {
                return connectTask.Result.GetDatabase();
            }
        }
        catch
        {
            // Redis unavailable
        }

        return null;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"Timed out waiting for {description}");
    }
}

/// <summary>One measurement the diagnostics meter emitted, flattened for assertions.</summary>
internal sealed record MeasurementRecord(
    string Instrument,
    double Value,
    IReadOnlyDictionary<string, object?> Tags);

/// <summary>Collects every measurement from the <c>RedisMQ.Broker</c> meter.</summary>
internal sealed class RedisMeterCollector : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly object _lock = new();
    private readonly List<MeasurementRecord> _measurements = [];

    public RedisMeterCollector()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (string.Equals(instrument.Meter.Name, "RedisMQ.Broker", StringComparison.Ordinal))
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.Start();
    }

    public IReadOnlyList<MeasurementRecord> For(string instrumentName)
    {
        lock (_lock)
        {
            return [.. _measurements.Where(measurement =>
                string.Equals(measurement.Instrument, instrumentName, StringComparison.Ordinal))];
        }
    }

    public void Dispose() => _listener.Dispose();

    private void Add(
        Instrument instrument,
        double value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        Dictionary<string, object?> copy = new(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            copy[tag.Key] = tag.Value;
        }

        lock (_lock)
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, value, copy));
        }
    }
}

/// <summary>A capturing log sink for the mirror failure/recovery assertions.</summary>
internal sealed class RedisRecordingLogSink : IBrokerLogSink
{
    private readonly object _lock = new();
    private readonly List<BrokerLogEntry> _entries = [];

    public IReadOnlyList<BrokerLogEntry> Entries
    {
        get
        {
            lock (_lock)
            {
                return [.. _entries];
            }
        }
    }

    public bool IsEnabled(BrokerLogLevel level) => true;

    public void Log(in BrokerLogEntry entry)
    {
        lock (_lock)
        {
            _entries.Add(entry);
        }
    }
}
