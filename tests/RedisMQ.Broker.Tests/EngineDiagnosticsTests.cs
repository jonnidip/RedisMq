using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 2: engine snapshots (queue depth/consumers), engine health, and the
// per-server runtime state that owns connection/channel/unacked counters.
public sealed class EngineDiagnosticsTests
{
    [Fact]
    public void Memory_engine_reports_queues_ready_and_consumers()
    {
        using var diag = new BrokerDiagnostics("engine1");
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");
        engine.RegisterConsumer("/", "q", new RefusingConsumer("ctag"));

        QueueSnapshot snapshot = Assert.Single(engine.QueueSnapshots());
        Assert.Equal("/", snapshot.Vhost);
        Assert.Equal("q", snapshot.Name);
        Assert.Equal(1, snapshot.Ready);
        Assert.Equal(1, snapshot.Consumers);
        Assert.Equal(0, snapshot.Unacked);
    }

    [Fact]
    public void Memory_engine_health_is_memory_and_not_durable()
    {
        using var engine = new InMemoryEngine(["/"], new BrokerDiagnostics("engine2"));

        EngineHealth health = engine.GetEngineHealth();

        Assert.Equal("memory", health.Kind);
        Assert.False(health.Durable);
        Assert.Null(health.RedisConnected);
        Assert.Null(health.LastError);
    }

    [Fact]
    public void Ready_and_consumer_gauges_are_per_queue()
    {
        using var diag = new BrokerDiagnostics("engine3");
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");

        MeasurementRecord ready = Assert.Single(
            meter.ObserveGauges("redismq.messages.ready")
                .Where(r => r.Tags["broker.instance"] is "engine3" && r.Tags["queue"] is "q"));

        Assert.Equal(1, ready.LongValue);
    }

    [Fact]
    public void Runtime_state_tracks_counts_and_unacked()
    {
        var state = new BrokerRuntimeState();
        state.ConnectionStarted();
        state.ConnectionStarted();
        state.ChannelOpened();
        state.DeliveryClaimed("/", "q");

        Assert.Equal(2, state.ActiveConnections);
        Assert.Equal(1, state.ActiveChannels);
        Assert.Equal(1, Assert.Single(state.UnackedSnapshot()).Value);

        state.DeliverySettled("/", "q");
        state.ConnectionClosed();

        Assert.Equal(1, state.ActiveConnections);
        Assert.Empty(state.UnackedSnapshot());
    }

    [Fact]
    public void Disposing_engine_unregisters_its_gauges()
    {
        using var diag = new BrokerDiagnostics("engine5");
        using var meter = new MeterCollector();
        var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo("/", "q"));

        engine.Dispose();

        Assert.Empty(meter.ObserveGauges("redismq.messages.ready")
            .Where(r => r.Tags["broker.instance"] is "engine5"));
    }

    private sealed class RefusingConsumer : IQueueConsumer
    {
        public RefusingConsumer(string tag) => ConsumerTag = tag;

        public string ConsumerTag { get; }

        public bool IsExclusive => false;

        public bool TryAdmitForDelivery(InMemoryMessage message) => false;

        public Task DrainDeliveryWritesAsync() => Task.CompletedTask;
    }
}
