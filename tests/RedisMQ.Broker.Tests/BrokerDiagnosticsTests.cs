using System.Diagnostics;
using System.Diagnostics.Metrics;
using RedisMQ.Broker.Diagnostics;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 1: the dependency-free diagnostics core — instance attribution,
// shared gauge aggregation, and the log-sink contract that must never throw into the
// broker's hot path.
public sealed class BrokerDiagnosticsTests
{
    [Fact]
    public void Counter_carries_the_instance_tag()
    {
        using var diag = new BrokerDiagnostics("inst1");
        using var meter = new MeterCollector();
        diag.Count(diag.ReaperSweeps, new TagList());

        MeasurementRecord record = Assert.Single(meter.For("redismq.reaper.sweeps"));
        Assert.Equal(1, record.LongValue);
        Assert.Equal("inst1", record.Tags["broker.instance"]);
    }

    [Fact]
    public void Gauge_callbacks_with_identical_tags_sum_instead_of_replacing()
    {
        using var diag = new BrokerDiagnostics("inst-gauge");
        using var meter = new MeterCollector();
        using IDisposable first = diag.RegisterObservableGauge(
            "test.gauge", "{item}", "test", () => [new Measurement<long>(1,
                new TagList { { "vhost", "/" }, { "queue", "q" } })]);
        using IDisposable second = diag.RegisterObservableGauge(
            "test.gauge", "{item}", "test", () => [new Measurement<long>(2,
                new TagList { { "vhost", "/" }, { "queue", "q" } })]);

        MeasurementRecord record = Assert.Single(meter.ObserveGauges("test.gauge"));
        Assert.Equal(3, record.LongValue);
        Assert.Equal("inst-gauge", record.Tags["broker.instance"]);
    }

    [Fact]
    public void Disabled_sink_receives_no_entries()
    {
        var sink = new RecordingLogSink { Enabled = false };
        using var diag = new BrokerDiagnostics("inst-log", sink);
        diag.WriteLog(BrokerLogLevel.Warning, BrokerLogEvent.ReaperFailed, "ignored");

        Assert.Empty(sink.Entries);
    }

    [Fact]
    public void Sink_exception_is_swallowed()
    {
        var sink = new RecordingLogSink { ThrowOnLog = true };
        using var diag = new BrokerDiagnostics("inst-throw", sink);

        Exception? thrown = Record.Exception(() =>
            diag.WriteLog(BrokerLogLevel.Error, BrokerLogEvent.ReaperFailed, "boom"));

        Assert.Null(thrown);
    }

    [Fact]
    public void Two_instances_remain_separately_attributable()
    {
        using var first = new BrokerDiagnostics("a");
        using var second = new BrokerDiagnostics("b");
        using var meter = new MeterCollector();
        first.Count(first.ReaperSweeps, new TagList());
        second.Count(second.ReaperSweeps, new TagList());

        Assert.Equal(2, meter.For("redismq.reaper.sweeps").Count);
        Assert.Contains(meter.For("redismq.reaper.sweeps"), r => r.Tags["broker.instance"] is "a");
        Assert.Contains(meter.For("redismq.reaper.sweeps"), r => r.Tags["broker.instance"] is "b");
    }

    [Fact]
    public void Dispose_is_idempotent()
    {
        var diag = new BrokerDiagnostics("dispose");
        diag.Dispose();
        diag.Dispose();
    }
}
