using System;
using System.Linq;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 6: engine-side drop, expiry, dead-letter and queue-expiry
// metrics, with the existing x-death header reasons untouched.
public sealed class EngineObservabilityTests
{
    [Fact]
    public void Max_length_drop_is_counted()
    {
        using var diag = new BrokerDiagnostics("eng1");
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo("/", "q", MaxLength: 1));
        engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");
        engine.Enqueue("/", "q", new ContentProperties(), [2], "", "q");

        MeasurementRecord dropped = Assert.Single(
            meter.For("redismq.messages.dropped"),
            r => r.Tags["broker.instance"] is "eng1");
        Assert.Equal("max_length", dropped.Tags["reason"]);
        Assert.Equal("q", dropped.Tags["queue"]);
    }

    [Fact]
    public void Ttl_expiry_is_counted_and_dead_lettered_when_configured()
    {
        using var diag = new BrokerDiagnostics("eng2");
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareExchange(new ExchangeInfo("/", "dlx", "fanout"));
        engine.DeclareQueue(new QueueInfo("/", "dead"));
        engine.Bind("/", "dlx", "dead", "");
        engine.DeclareQueue(new QueueInfo("/", "q", DeadLetterExchange: "dlx"));
        engine.Enqueue("/", "q", new ContentProperties(Expiration: "0"), [1], "", "q");

        engine.Reap(DateTimeOffset.UtcNow.AddSeconds(1));

        Assert.Equal(1, meter.For("redismq.messages.expired")
            .Count(r => r.Tags["broker.instance"] is "eng2"));
        Assert.Equal("expired", Assert.Single(
            meter.For("redismq.messages.deadlettered"),
            r => r.Tags["broker.instance"] is "eng2").Tags["reason"]);
    }

    [Fact]
    public void Delivery_limit_requeue_uses_delivery_limit_metric_reason()
    {
        using var diag = new BrokerDiagnostics("eng3");
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareExchange(new ExchangeInfo("/", "dlx", "fanout"));
        engine.DeclareQueue(new QueueInfo("/", "dead"));
        engine.Bind("/", "dlx", "dead", "");
        engine.DeclareQueue(new QueueInfo("/", "q", DeadLetterExchange: "dlx", DeliveryLimit: 1));
        var message = new InMemoryMessage(new ContentProperties(), [1], "", "q", 7, DeliveryCount: 1);

        engine.Requeue("/", "q", message);

        MeasurementRecord dead = Assert.Single(
            meter.For("redismq.messages.deadlettered"),
            r => r.Tags["broker.instance"] is "eng3");
        Assert.Equal("delivery_limit", dead.Tags["reason"]);
    }

    [Fact]
    public void Configured_but_unusable_dlx_counts_as_deadletters_dropped()
    {
        using var diag = new BrokerDiagnostics("eng4");
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo("/", "missing", DeadLetterExchange: "nope"));
        engine.Enqueue("/", "missing", new ContentProperties(), [1], "", "missing");
        var msg = Assert.Single(engine.PeekQueue("missing"));
        engine.Reject("/", "missing", msg);

        Assert.Equal("missing_exchange", Assert.Single(
            meter.For("redismq.deadletters.dropped"),
            r => r.Tags["broker.instance"] is "eng4").Tags["reason"]);
    }

    [Fact]
    public void Queue_expiry_is_counted_and_logged()
    {
        var sink = new RecordingLogSink();
        using var diag = new BrokerDiagnostics("eng5", sink);
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo("/", "idle", Expiration: TimeSpan.Zero));

        engine.Reap(DateTimeOffset.UtcNow.AddSeconds(1));

        Assert.Equal(1, meter.For("redismq.queues.expired")
            .Count(r => r.Tags["broker.instance"] is "eng5"));
        Assert.Contains(
            sink.Entries,
            e => e.Event == BrokerLogEvent.QueueExpired && e.Level == BrokerLogLevel.Warning);
    }

    [Fact]
    public void Loop_guard_counts_deadletters_dropped()
    {
        using var diag = new BrokerDiagnostics("eng6");
        using var meter = new MeterCollector();
        using var engine = new InMemoryEngine(["/"], diag);
        engine.DeclareQueue(new QueueInfo(
            "/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        var headers = new FieldTable([
            new("x-death", FieldValue.List(
                FieldValue.From(Death("source", "rejected", 1, DateTimeOffset.UtcNow.AddSeconds(-1), "ex")))),
        ]);
        engine.Enqueue("/", "source", new ContentProperties(Headers: headers), [1], "ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));

        engine.Reject("/", "source", claimed!);

        Assert.Equal("loop_guard", Assert.Single(
            meter.For("redismq.deadletters.dropped"),
            r => r.Tags["broker.instance"] is "eng6").Tags["reason"]);
    }

    private static FieldTable Death(
        string queue, string reason, long count, DateTimeOffset time, string exchange) =>
        new([
            new("queue", FieldValue.From(queue)),
            new("reason", FieldValue.From(reason)),
            new("count", FieldValue.From(count)),
            new("time", FieldValue.From(time)),
            new("exchange", FieldValue.From(exchange)),
        ]);
}
