using System.Net.Security;
using RabbitMQ.Client;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Tests;
using Xunit;

namespace RedisMQ.Conformance;

/// <summary>
/// TLS conformance against RabbitMQ.Client 7.2.2 over a real socket: server-auth TLS
/// with a self-signed localhost certificate, mutual TLS with a CA-signed client
/// certificate, and the refusal of a client that presents none.
/// </summary>
public sealed class TlsFlowTests
{
    [Fact]
    public async Task Connect_over_tls_and_publish_consume()
    {
        using var certs = new TestCertificates();
        await using TestBrokerHost host = StartTls(certs);
        Assert.NotNull(host.TlsPort);

        await using IConnection connection = await ConnectAsync(host, clientCertificatePath: null);
        await using IChannel channel = await connection.CreateChannelAsync();
        await PublishAndGetAsync(channel, "tls-q", "over tls"u8.ToArray());
    }

    [Fact]
    public async Task Mutual_tls_accepts_a_client_certificate()
    {
        using var certs = new TestCertificates();
        await using TestBrokerHost host = StartTls(certs, requireClientCertificate: true);

        await using IConnection connection = await ConnectAsync(
            host, clientCertificatePath: certs.ClientPfxPath);
        await using IChannel channel = await connection.CreateChannelAsync();
        await PublishAndGetAsync(channel, "mtls-q", "mutual tls"u8.ToArray());
    }

    [Fact]
    public async Task Mutual_tls_refuses_a_client_without_a_certificate()
    {
        using var certs = new TestCertificates();
        await using TestBrokerHost host = StartTls(certs, requireClientCertificate: true);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using IConnection connection = await ConnectAsync(host, clientCertificatePath: null);
        });
    }

    // ---------- harness ----------

    private static TestBrokerHost StartTls(
        TestCertificates certs,
        bool requireClientCertificate = false) =>
        TestBrokerHost.Start(options =>
        {
            options.Tls = new BrokerTlsOptions
            {
                Enabled = true,
                Port = 0,
                CertificatePath = certs.ServerPfxPath,
                CertificatePassword = TestCertificates.Password,
                ClientCertificateRequired = requireClientCertificate,
                ClientCertificateAuthorityPath = requireClientCertificate ? certs.CaPemPath : null,
                HandshakeTimeout = TimeSpan.FromSeconds(5),
            };
        });

    private static async Task<IConnection> ConnectAsync(
        TestBrokerHost host,
        string? clientCertificatePath)
    {
        var ssl = new SslOption
        {
            Enabled = true,
            ServerName = "localhost",
            AcceptablePolicyErrors =
                SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors,
            CertPath = clientCertificatePath,
            CertPassphrase = clientCertificatePath is null ? null : TestCertificates.Password,
        };

        var factory = new ConnectionFactory
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            RequestedConnectionTimeout = TimeSpan.FromSeconds(10),
            Ssl = ssl,
        };

        // An explicit endpoint carries its own SslOption; ConnectionFactory.Ssl alone
        // does not upgrade endpoints supplied in the list.
        return await factory.CreateConnectionAsync(
            [new AmqpTcpEndpoint("localhost", host.TlsPort!.Value, ssl)]);
    }

    private static async Task PublishAndGetAsync(IChannel channel, string queue, byte[] body)
    {
        await channel.QueueDeclareAsync(
            queue,
            durable: false,
            exclusive: false,
            autoDelete: false,
            cancellationToken: TestContext.Current.CancellationToken);
        await channel.BasicPublishAsync(
            "", queue, mandatory: false, new BasicProperties(), body,
            cancellationToken: TestContext.Current.CancellationToken);

        // A passive declare fences the publish through the broker's ordered pipeline.
        _ = await channel.QueueDeclareAsync(
            queue, durable: false, exclusive: false, autoDelete: false, arguments: null,
            passive: true, cancellationToken: TestContext.Current.CancellationToken);

        BasicGetResult? result = await channel.BasicGetAsync(
            queue, autoAck: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal(body, result.Body.ToArray());
    }
}
