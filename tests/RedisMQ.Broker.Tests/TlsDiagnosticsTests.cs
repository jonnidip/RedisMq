using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 8: TLS handshake counts and durations, with a failure logged only
// at Debug (hostile peers must not be able to flood a host log).
public sealed class TlsDiagnosticsTests
{
    [Fact]
    public async Task Successful_tls_handshake_is_counted_with_duration()
    {
        using var certs = new TestCertificates();
        using var diag = new BrokerDiagnostics("tls1");
        using var meter = new MeterCollector();
        await using var listener = new TlsSocketListener(ServerOptions(certs), diag);

        ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);
        (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(
            listener.AnnouncedPort, cancellationToken: TestContext.Current.CancellationToken);
        using (tcp)
        using (ssl)
        {
            _ = await accepted;
            Assert.Equal("success", Assert.Single(
                meter.For("redismq.tls.handshakes"),
                r => r.Tags["broker.instance"] is "tls1").Tags["result"]);
            Assert.Single(
                meter.For("redismq.tls.handshake.duration"),
                r => r.Tags["broker.instance"] is "tls1");
        }
    }

    [Fact]
    public async Task Failed_tls_handshake_is_counted_and_logged_at_debug()
    {
        using var certs = new TestCertificates();
        var sink = new RecordingLogSink();
        using var diag = new BrokerDiagnostics("tls2", sink);
        using var meter = new MeterCollector();
        await using var listener = new TlsSocketListener(ServerOptions(certs), diag);

        _ = listener.AcceptAsync(TestContext.Current.CancellationToken);
        using (var tcp = new TcpClient())
        {
            await tcp.ConnectAsync(
                "localhost", listener.AnnouncedPort, TestContext.Current.CancellationToken);
            tcp.Close(); // abort the handshake
        }

        await WaitUntilAsync(
            () => meter.For("redismq.tls.handshakes")
                .Any(r => r.Tags["broker.instance"] is "tls2" && (string?)r.Tags["result"] == "failure"),
            TimeSpan.FromSeconds(3));
        Assert.Contains(sink.Entries, e =>
            e.Event == BrokerLogEvent.TlsHandshakeFailed && e.Level == BrokerLogLevel.Debug);
    }

    private static BrokerTlsOptions ServerOptions(TestCertificates certs) => new()
    {
        Enabled = true,
        Port = 0,
        CertificatePath = certs.ServerPfxPath,
        CertificatePassword = TestCertificates.Password,
        HandshakeTimeout = TimeSpan.FromSeconds(5),
    };

    private static async Task<(TcpClient Tcp, SslStream Ssl)> ConnectTlsAsync(
        int port,
        X509Certificate2? clientCertificate = null,
        CancellationToken cancellationToken = default)
    {
        CancellationToken ct = cancellationToken == default
            ? TestContext.Current.CancellationToken
            : cancellationToken;

        var tcp = new TcpClient();
        try
        {
            await tcp.ConnectAsync(IPAddress.Loopback, port, ct);
            var network = new NetworkStream(tcp.Client, ownsSocket: true);
            var ssl = new SslStream(network, leaveInnerStreamOpen: false, (_, _, _, _) => true);
            var options = new SslClientAuthenticationOptions { TargetHost = "localhost" };
            if (clientCertificate is not null)
            {
                options.ClientCertificates = new X509CertificateCollection { clientCertificate };
            }

            await ssl.AuthenticateAsClientAsync(options, ct);
            return (tcp, ssl);
        }
        catch
        {
            tcp.Dispose();
            throw;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        Assert.Fail("the failed TLS handshake was not counted");
    }
}
