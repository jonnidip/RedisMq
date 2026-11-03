using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 12: certificate loading (PFX and PEM) and the SslStream listener, including
// hostile-handshake survival, mutual TLS and the per-connection handshake timeout.
public sealed class TlsTransportTests
{
    [Fact]
    public void Pfx_certificate_loads_and_reports_a_private_key()
    {
        using var certs = new TestCertificates();
        using BrokerCertificate certificate = BrokerCertificate.Load(new BrokerTlsOptions
        {
            Enabled = true,
            CertificatePath = certs.ServerPfxPath,
            CertificatePassword = TestCertificates.Password,
        });

        Assert.True(certificate.Certificate.HasPrivateKey);
        Assert.Contains("CN=localhost", certificate.Certificate.Subject, StringComparison.Ordinal);
    }

    [Fact]
    public void Pem_certificate_with_key_loads()
    {
        using var certs = new TestCertificates();
        using BrokerCertificate certificate = BrokerCertificate.Load(new BrokerTlsOptions
        {
            Enabled = true,
            CertificatePath = certs.ServerCertPemPath,
            CertificateKeyPath = certs.ServerKeyPemPath,
        });

        Assert.True(certificate.Certificate.HasPrivateKey);
        Assert.Contains("CN=localhost", certificate.Certificate.Subject, StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_certificate_file_throws_BrokerConfigurationException()
    {
        using var certs = new TestCertificates();
        string missing = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.pfx");

        Assert.Throws<BrokerConfigurationException>(() => BrokerCertificate.Load(new BrokerTlsOptions
        {
            Enabled = true,
            CertificatePath = missing,
        }));

        Assert.Throws<BrokerConfigurationException>(() => BrokerCertificate.Load(new BrokerTlsOptions
        {
            Enabled = true,
            CertificatePath = certs.ServerCertPemPath,
            CertificateKeyPath = missing,
        }));
    }

    [Fact]
    public async Task Tls_listener_accepts_an_ssl_client_and_relays_bytes()
    {
        using var certs = new TestCertificates();
        await using var listener = new TlsSocketListener(ServerOptions(certs));

        ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);
        (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(listener.AnnouncedPort);
        using (tcp)
        using (ssl)
        {
            TransportSide side = Assert.IsType<TransportSide>(await accepted);

            byte[] payload = [1, 2, 3, 4];
            await ssl.WriteAsync(payload, TestContext.Current.CancellationToken);
            Assert.Equal(payload, await ReadExactlyAsync(side.Reader, payload.Length));

            byte[] reply = [9, 8, 7];
            await side.Writer.WriteAsync(reply);
            await side.Writer.FlushAsync();
            byte[] received = new byte[reply.Length];
            await ssl.ReadExactlyAsync(received, TestContext.Current.CancellationToken);
            Assert.Equal(reply, received);
        }
    }

    [Fact]
    public async Task Bad_handshake_does_not_end_the_accept_stream()
    {
        using var certs = new TestCertificates();
        await using var listener = new TlsSocketListener(ServerOptions(certs));

        // A raw TCP peer that speaks anything but TLS must be discarded, not fatal.
        using (var garbage = new TcpClient())
        {
            await garbage.ConnectAsync(
                IPAddress.Loopback, listener.AnnouncedPort, TestContext.Current.CancellationToken);
            await garbage.GetStream().WriteAsync(
                "this is definitely not a TLS record"u8.ToArray(),
                TestContext.Current.CancellationToken);
        }

        ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);
        (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(listener.AnnouncedPort);
        using (tcp)
        using (ssl)
        {
            TransportSide side = Assert.IsType<TransportSide>(await accepted);
            byte[] payload = [4, 5, 6];
            await ssl.WriteAsync(payload, TestContext.Current.CancellationToken);
            Assert.Equal(payload, await ReadExactlyAsync(side.Reader, payload.Length));
        }
    }

    [Fact]
    public async Task Mutual_tls_rejects_a_client_without_a_certificate()
    {
        using var certs = new TestCertificates();
        await using var listener = new TlsSocketListener(MutualTlsOptions(certs));

        ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);

        // The certificate-less client must never reach a usable transport. In TLS 1.3
        // the rejection can surface on the first write/read rather than in the
        // handshake, so exercise both and swallow whichever refusal arrives.
        using (var refusal = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            try
            {
                (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(
                    listener.AnnouncedPort, cancellationToken: refusal.Token);
                using (tcp)
                using (ssl)
                {
                    await ssl.WriteAsync(new byte[] { 0 }, refusal.Token);
                    await ssl.ReadAsync(new byte[1], refusal.Token);
                }
            }
            catch (Exception exception) when (
                exception is AuthenticationException or IOException
                    or OperationCanceledException or SocketException)
            {
                // expected: mutual TLS refused the certificate-less peer
            }
        }

        (TcpClient goodTcp, SslStream goodSsl) = await ConnectTlsAsync(
            listener.AnnouncedPort, certs.ClientCertificate);
        using (goodTcp)
        using (goodSsl)
        {
            TransportSide side = Assert.IsType<TransportSide>(await accepted);
            byte[] payload = [7, 8, 9];
            await goodSsl.WriteAsync(payload, TestContext.Current.CancellationToken);
            Assert.Equal(payload, await ReadExactlyAsync(side.Reader, payload.Length));
        }
    }

    [Fact]
    public async Task Mutual_tls_accepts_a_ca_signed_client_certificate()
    {
        using var certs = new TestCertificates();
        await using var listener = new TlsSocketListener(MutualTlsOptions(certs));

        ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);
        (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(
            listener.AnnouncedPort, certs.ClientCertificate);
        using (tcp)
        using (ssl)
        {
            TransportSide side = Assert.IsType<TransportSide>(await accepted);
            byte[] payload = [10, 11, 12];
            await ssl.WriteAsync(payload, TestContext.Current.CancellationToken);
            Assert.Equal(payload, await ReadExactlyAsync(side.Reader, payload.Length));
        }
    }

    [Fact]
    public async Task Handshake_timeout_closes_that_socket_and_keeps_accepting()
    {
        using var certs = new TestCertificates();
        BrokerTlsOptions options = ServerOptions(certs);
        options.HandshakeTimeout = TimeSpan.FromMilliseconds(200);
        await using var listener = new TlsSocketListener(options);

        ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);

        // A peer that connects and then says nothing must not hold the accept loop.
        using var silent = new TcpClient();
        await silent.ConnectAsync(
            IPAddress.Loopback, listener.AnnouncedPort, TestContext.Current.CancellationToken);
        await Task.Delay(50, TestContext.Current.CancellationToken);

        (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(listener.AnnouncedPort);
        using (tcp)
        using (ssl)
        {
            TransportSide side = Assert.IsType<TransportSide>(await accepted);
            byte[] payload = [13, 14];
            await ssl.WriteAsync(payload, TestContext.Current.CancellationToken);
            Assert.Equal(payload, await ReadExactlyAsync(side.Reader, payload.Length));
        }
    }

    // ---------- harness ----------

    private static BrokerTlsOptions ServerOptions(TestCertificates certs) => new()
    {
        Enabled = true,
        Port = 0,
        CertificatePath = certs.ServerPfxPath,
        CertificatePassword = TestCertificates.Password,
        HandshakeTimeout = TimeSpan.FromSeconds(5),
    };

    private static BrokerTlsOptions MutualTlsOptions(TestCertificates certs)
    {
        BrokerTlsOptions options = ServerOptions(certs);
        options.ClientCertificateRequired = true;
        options.ClientCertificateAuthorityPath = certs.CaPemPath;
        return options;
    }

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

    private static async Task<byte[]> ReadExactlyAsync(PipeReader reader, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        byte[] buffer = new byte[count];
        int total = 0;
        while (total < count)
        {
            ReadResult result = await reader.ReadAsync(cts.Token);
            ReadOnlySequence<byte> sequence = result.Buffer;
            if (sequence.Length > 0)
            {
                int take = (int)Math.Min(sequence.Length, count - total);
                sequence.Slice(0, take).CopyTo(buffer.AsSpan(total));
                total += take;
                reader.AdvanceTo(sequence.GetPosition(take));
            }
            else
            {
                reader.AdvanceTo(sequence.Start, sequence.End);
                if (result.IsCompleted)
                {
                    throw new IOException("The TLS transport closed before all bytes arrived");
                }
            }
        }

        return buffer;
    }
}
