using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 4: publish outcomes, publisher confirms and the amqp.publish
// span. Every completed content dispatch counts one outcome and one body size, the
// span is owned by the channel (inline) or by the connection's confirm loop, and each
// span is stopped exactly once even when admission or retirement drops it.
public sealed class PublishDiagnosticsTests
{
    [Fact]
    public async Task Publish_outcomes_and_size_are_counted_and_spans_stop()
    {
        using var diag = new BrokerDiagnostics("pub1");
        using var meter = new MeterCollector();
        using var activities = new ActivityCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareExchangeAsync(1, "ex", "direct");
        await harness.DeclareQueueAsync(1, "q");
        await harness.BindAsync(1, "q", "ex", "rk");

        await harness.PublishAsync(1, "ex", "rk", body: "abc");
        await harness.PublishAsync(1, "ex", "missing", body: "de");
        await harness.PublishAsync(1, "ex", "missing", mandatory: true, body: "f");

        await WaitUntilAsync(
            () => meter.For("redismq.publishes").Count(r => r.Tags["broker.instance"] is "pub1") == 3,
            "the three publish outcomes were not observed");

        string[] outcomes = [.. meter.For("redismq.publishes")
            .Where(r => r.Tags["broker.instance"] is "pub1")
            .Select(r => (string)r.Tags["outcome"]!)];
        Assert.Equal(["routed", "unroutable", "mandatory_return"], outcomes);
        Assert.Equal(3, meter.For("redismq.publish.size")
            .Count(r => r.Tags["broker.instance"] is "pub1"));
        Assert.Equal(3, activities.Stopped.Count(a => a.OperationName == "amqp.publish"
            && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub1")));
    }

    [Fact]
    public async Task Confirm_ack_counts_duration_and_closes_the_span()
    {
        using var diag = new BrokerDiagnostics("pub2");
        using var meter = new MeterCollector();
        using var activities = new ActivityCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenConfirmChannelAsync();
        await harness.DeclareQueueAsync(1, "q");

        await harness.PublishAsync(1, "", "q");
        _ = await harness.ReadMethodOnChannelAsync(1, 85, 50); // confirm.ack

        await WaitUntilAsync(
            () => meter.For("redismq.confirms").Count(r => r.Tags["broker.instance"] is "pub2") == 1,
            "the confirm outcome was not observed");
        await WaitUntilAsync(
            () => activities.Stopped.Count(a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub2")) == 1,
            "the confirm span was not stopped");

        MeasurementRecord confirm = Assert.Single(
            meter.For("redismq.confirms"),
            r => r.Tags["broker.instance"] is "pub2");
        Assert.Equal("ack", confirm.Tags["outcome"]);
        Assert.Single(
            meter.For("redismq.confirm.duration"),
            r => r.Tags["broker.instance"] is "pub2");
        Activity span = Assert.Single(
            activities.Stopped,
            a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub2"));
        Assert.Equal("ack", span.GetTagItem("redismq.outcome"));
        Assert.Equal("routed", span.GetTagItem("redismq.publish.outcome"));
    }

    [Fact]
    public async Task Refused_publish_is_refused_and_confirm_nacks()
    {
        using var diag = new BrokerDiagnostics("pub3");
        using var meter = new MeterCollector();
        var engine = new RecordingEngine(["/"]);
        engine.EnqueueOverride = (_, _, _, _, _, _) =>
            new EnqueueReceipt(0, ValueTask.FromResult(false), []);
        await using var harness = DiagnosticsWireHarness.Start(engine: engine, diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenConfirmChannelAsync();
        await harness.DeclareQueueAsync(1, "q");

        await harness.PublishAsync(1, "", "q");
        _ = await harness.ReadMethodOnChannelAsync(1, 85, 51); // confirm.nack

        Assert.Equal("refused", Assert.Single(
            meter.For("redismq.publishes"),
            r => r.Tags["broker.instance"] is "pub3").Tags["outcome"]);
        Assert.Equal("nack", Assert.Single(
            meter.For("redismq.confirms"),
            r => r.Tags["broker.instance"] is "pub3").Tags["outcome"]);
    }

    [Fact]
    public async Task Confirm_timeout_is_counted_and_closes_the_span()
    {
        using var diag = new BrokerDiagnostics("pub4");
        using var meter = new MeterCollector();
        using var activities = new ActivityCollector();
        var engine = new RecordingEngine(["/"]);
        var never = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        engine.EnqueueOverride = (_, _, _, _, _, _) =>
            new EnqueueReceipt(1, new ValueTask<bool>(never.Task), []);
        BrokerServerOptions options = DiagnosticsWireHarness.NewOptions();
        options.ConfirmTimeout = TimeSpan.FromMilliseconds(50);
        await using var harness = DiagnosticsWireHarness.Start(
            options: options, engine: engine, diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenConfirmChannelAsync();
        await harness.DeclareQueueAsync(1, "q");

        await harness.PublishAsync(1, "", "q");
        _ = await harness.ReadMethodOnChannelAsync(1, 85, 51); // confirm.nack (timeout)

        await WaitUntilAsync(
            () => activities.Stopped.Count(a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub4")) == 1,
            "the timed-out publish span was not stopped");

        Assert.Equal("timeout", Assert.Single(
            meter.For("redismq.confirms"),
            r => r.Tags["broker.instance"] is "pub4").Tags["outcome"]);
        Activity span = Assert.Single(
            activities.Stopped,
            a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub4"));
        Assert.Equal(ActivityStatusCode.Error, span.Status);
    }

    [Fact]
    public async Task Confirm_span_stops_when_channel_retires_before_the_outcome()
    {
        using var diag = new BrokerDiagnostics("pub5");
        using var activities = new ActivityCollector();
        var engine = new RecordingEngine(["/"]);
        var receipt = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        engine.EnqueueOverride = (_, _, _, _, _, _) =>
            new EnqueueReceipt(1, new ValueTask<bool>(receipt.Task), []);
        await using var harness = DiagnosticsWireHarness.Start(engine: engine, diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenConfirmChannelAsync();
        await harness.DeclareQueueAsync(1, "q");

        await harness.PublishAsync(1, "", "q");
        await harness.CloseChannelAsync(1);
        // Reopening the number is the teardown fence: the fresh context proves the
        // original confirm entry is stale before its receipt resolves.
        await harness.OpenChannelAsync(1);
        receipt.SetResult(false);

        await WaitUntilAsync(
            () => activities.Stopped.Count(a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub5")) == 1,
            "the dropped publish span was not stopped");

        Activity span = Assert.Single(
            activities.Stopped,
            a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub5"));
        Assert.Equal("dropped", span.GetTagItem("redismq.outcome"));
    }

    [Fact]
    public async Task No_confirm_publish_span_ends_at_admission()
    {
        using var diag = new BrokerDiagnostics("pub6");
        using var activities = new ActivityCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareQueueAsync(1, "q");

        await harness.PublishAsync(1, "", "q");

        await WaitUntilAsync(
            () => activities.Stopped.Count(a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub6")) == 1,
            "the inline publish span was not stopped");

        Activity span = Assert.Single(
            activities.Stopped,
            a => a.OperationName == "amqp.publish"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub6"));
        Assert.Equal("routed", span.GetTagItem("redismq.outcome"));
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string message)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Fail(message);
    }
}
