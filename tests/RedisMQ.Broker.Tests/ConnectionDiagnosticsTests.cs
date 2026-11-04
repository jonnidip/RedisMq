using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Server;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 3: connection, channel, authentication, vhost and permission
// instrumentation — counted once per event, attributed to the instance, with no
// credentials in any log entry.
public sealed class ConnectionDiagnosticsTests
{
    [Fact]
    public async Task Connection_and_channel_lifecycle_are_counted_and_gauged()
    {
        using var diag = new BrokerDiagnostics("conn1");
        using var meter = new MeterCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();

        Assert.Equal(1, Single(meter, "redismq.connections.opened", "conn1").LongValue);
        Assert.Equal(1, Single(meter, "redismq.connections.authenticated", "conn1").LongValue);
        Assert.Equal(1, Single(meter, "redismq.channels.opened", "conn1").LongValue);
        Assert.Equal(1, Gauge(meter, "redismq.connections.active", "conn1"));
        Assert.Equal(1, Gauge(meter, "redismq.channels.active", "conn1"));

        await harness.CloseChannelAsync(1);
        await harness.CloseConnectionAsync();

        await WaitUntilAsync(
            () => meter.For("redismq.channels.closed").Any(r => r.Tags["broker.instance"] is "conn1"),
            "the channel close was not observed");
        await WaitUntilAsync(
            () => meter.For("redismq.connections.closed").Any(r => r.Tags["broker.instance"] is "conn1"),
            "the connection close was not observed");

        MeasurementRecord closed = Assert.Single(
            meter.For("redismq.channels.closed"),
            r => r.Tags["broker.instance"] is "conn1");
        Assert.Equal("client", closed.Tags["reason"]);
        Assert.Equal("client", Assert.Single(
            meter.For("redismq.connections.closed"),
            r => r.Tags["broker.instance"] is "conn1").Tags["reason"]);

        await WaitUntilAsync(
            () => Gauge(meter, "redismq.connections.active", "conn1") == 0,
            "the active connection gauge did not fall");
        Assert.Equal(0, Gauge(meter, "redismq.connections.active", "conn1"));
    }

    [Fact]
    public async Task Authentication_failure_classifies_reason_and_never_logs_the_password()
    {
        var sink = new RecordingLogSink();
        using var diag = new BrokerDiagnostics("auth1", sink);
        using var meter = new MeterCollector();
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await Assert.ThrowsAnyAsync<Exception>(() => harness.HandshakeAsync("guest", "wrong-secret"));

        MeasurementRecord failure = Assert.Single(
            meter.For("redismq.auth.failures"),
            r => r.Tags["broker.instance"] is "auth1");
        Assert.Equal("bad_credentials", failure.Tags["reason"]);
        BrokerLogEntry entry = Assert.Single(
            sink.Entries,
            e => e.Event == BrokerLogEvent.AuthenticationFailed);
        Assert.Equal(BrokerLogLevel.Warning, entry.Level);
        Assert.DoesNotContain("wrong-secret", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("guest", entry.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Vhost_refusal_counts_unknown_and_denied()
    {
        using var diag = new BrokerDiagnostics("vhost1");
        using var meter = new MeterCollector();
        await using var unknown = DiagnosticsWireHarness.Start(diagnostics: diag);
        await unknown.AuthenticateAsync();
        await Assert.ThrowsAnyAsync<Exception>(() => unknown.OpenVhostAsync("/missing"));

        await using var denied = DiagnosticsWireHarness.Start(
            options: Options(user: new BrokerUser("u", "p")),
            diagnostics: diag);
        await denied.AuthenticateAsync("u", "p");
        await Assert.ThrowsAnyAsync<Exception>(() => denied.OpenVhostAsync("/"));

        string[] reasons = [.. meter.For("redismq.vhost.refusals")
            .Where(r => r.Tags["broker.instance"] is "vhost1")
            .Select(r => (string)r.Tags["reason"]!)];
        Assert.Contains("unknown", reasons);
        Assert.Contains("denied", reasons);
    }

    [Fact]
    public async Task Permission_denials_are_tagged_by_operation()
    {
        using var diag = new BrokerDiagnostics("perm1");
        using var meter = new MeterCollector();
        await using var harness = DiagnosticsWireHarness.Start(
            options: Options(user: new BrokerUser("u", "p",
                Permissions: [new BrokerPermission("/", "allowed.*", "allowed.*", "allowed.*")])),
            diagnostics: diag);
        await harness.HandshakeAsync("u", "p");
        await harness.OpenChannelAsync();

        await harness.SendQueueDeclareAsync(1, "denied");
        _ = await harness.ReadMethodOnChannelAsync(1, 20, 40); // channel.close (configure)
        await harness.RetireMourningChannelAsync(1);
        await harness.OpenChannelAsync(1);
        await harness.PublishAsync(1, "", "denied");
        _ = await harness.ReadMethodOnChannelAsync(1, 20, 40); // channel.close (write)
        await harness.RetireMourningChannelAsync(1);
        await harness.OpenChannelAsync(1);
        await harness.SendBasicGetAsync(1, "denied");
        _ = await harness.ReadMethodOnChannelAsync(1, 20, 40); // channel.close (read)

        string[] operations = [.. meter.For("redismq.permission.denials")
            .Where(r => r.Tags["broker.instance"] is "perm1")
            .Select(r => (string)r.Tags["operation"]!)];
        Assert.Contains("configure", operations);
        Assert.Contains("write", operations);
        Assert.Contains("read", operations);
    }

    private static MeasurementRecord Single(MeterCollector meter, string instrument, string instance) =>
        Assert.Single(
            meter.For(instrument),
            r => r.Tags["broker.instance"] is string value && value == instance);

    private static long Gauge(MeterCollector meter, string instrument, string instance) =>
        meter.ObserveGauges(instrument)
            .Where(r => r.Tags["broker.instance"] is string value && value == instance)
            .Select(r => r.LongValue)
            .DefaultIfEmpty(0)
            .Sum();

    private static BrokerServerOptions Options(BrokerUser user) => new()
    {
        Users = [user],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

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
