using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using RedisMQ.Broker.Diagnostics;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 5: deliveries, settlements, deliver spans and the unacked gauge.
// A settlement reports the effective action (ack/reject/requeue) and the wire method,
// auto-ack deliveries say so, and the unacked counter falls exactly once on settle,
// recover and teardown.
public sealed class DeliveryDiagnosticsTests
{
    [Fact]
    public async Task Settlements_report_effective_kind_and_wire_method()
    {
        using var diag = new BrokerDiagnostics("del1");
        using var meter = new MeterCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareQueueAsync(1, "q");
        foreach (string body in new[] { "a", "b", "c", "d", "e" })
        {
            await harness.PublishAsync(1, "", "q", body: body);
        }

        foreach (string unused in new[] { "a", "b", "c", "d", "e" })
        {
            _ = await harness.BasicGetAsync(1, "q", noAck: false);
        }

        await harness.SendBasicAckAsync(1, 1);
        await harness.SendBasicNackAsync(1, 2, requeue: false);
        await harness.SendBasicNackAsync(1, 3, requeue: true);
        await harness.SendBasicRejectAsync(1, 4, requeue: false);
        await harness.SendBasicRejectAsync(1, 5, requeue: true);

        await WaitUntilAsync(
            () => Rows(meter).Count == 5,
            "the five settlements were not observed");

        (string Kind, string Method)[] rows = [.. Rows(meter)];
        Assert.Contains(("ack", "ack"), rows);
        Assert.Contains(("reject", "nack"), rows);
        Assert.Contains(("requeue", "nack"), rows);
        Assert.Contains(("reject", "reject"), rows);
        Assert.Contains(("requeue", "reject"), rows);
    }

    [Fact]
    public async Task Deliveries_mark_auto_ack_and_spans_stop_after_the_write()
    {
        using var diag = new BrokerDiagnostics("del2");
        using var meter = new MeterCollector();
        using var activities = new ActivityCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareQueueAsync(1, "q");
        await harness.PublishAsync(1, "", "q");
        await harness.BasicConsumeAsync(1, "q", noAck: true);
        _ = await harness.ReadDeliveryAsync(1);

        await WaitUntilAsync(
            () => activities.Stopped.Count(a => a.OperationName == "amqp.deliver"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "del2")) == 1,
            "the deliver span was not stopped");

        MeasurementRecord delivery = Assert.Single(
            meter.For("redismq.deliveries"),
            r => r.Tags["broker.instance"] is "del2");
        Assert.Equal("true", delivery.Tags["auto_ack"]);
        Assert.Single(
            activities.Stopped,
            a => a.OperationName == "amqp.deliver"
                && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "del2"));
        Assert.DoesNotContain(
            meter.For("redismq.settlements"),
            r => r.Tags["broker.instance"] is "del2");
    }

    [Fact]
    public async Task Unacked_gauge_rises_and_falls_with_manual_delivery()
    {
        using var diag = new BrokerDiagnostics("del3");
        using var meter = new MeterCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareQueueAsync(1, "q");
        await harness.PublishAsync(1, "", "q");
        _ = await harness.BasicGetAsync(1, "q", noAck: false);

        await WaitUntilAsync(
            () => Gauge(meter, "redismq.messages.unacked", "del3") == 1,
            "the unacked gauge did not rise");
        Assert.Equal(1, Gauge(meter, "redismq.messages.unacked", "del3"));

        await harness.SendBasicAckAsync(1, 1);

        await WaitUntilAsync(
            () => Gauge(meter, "redismq.messages.unacked", "del3") == 0,
            "the unacked gauge did not fall");
        Assert.Equal(0, Gauge(meter, "redismq.messages.unacked", "del3"));
    }

    [Fact]
    public async Task Channel_teardown_requeues_and_clears_unacked()
    {
        using var diag = new BrokerDiagnostics("del4");
        using var meter = new MeterCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareQueueAsync(1, "q");
        await harness.PublishAsync(1, "", "q");
        _ = await harness.BasicGetAsync(1, "q", noAck: false);
        await harness.CloseChannelAsync(1);

        await WaitUntilAsync(
            () => Gauge(meter, "redismq.messages.unacked", "del4") == 0,
            "teardown did not clear the unacked gauge");
        Assert.Equal(0, Gauge(meter, "redismq.messages.unacked", "del4"));
    }

    private static List<(string Kind, string Method)> Rows(MeterCollector meter) =>
    [
        .. meter.For("redismq.settlements")
            .Where(r => r.Tags["broker.instance"] is "del1")
            .Select(r => ((string)r.Tags["kind"]!, (string)r.Tags["method"]!)),
    ];

    private static long Gauge(MeterCollector meter, string instrument, string instance) =>
        meter.ObserveGauges(instrument)
            .Where(r => r.Tags["broker.instance"] is string value && value == instance)
            .Select(r => r.LongValue)
            .DefaultIfEmpty(0)
            .Sum();

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
