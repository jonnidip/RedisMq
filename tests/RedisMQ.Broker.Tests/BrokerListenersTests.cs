using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 13: the listener factory and the multi-listener server. The factory owns
// nothing after it returns; the server consumes the listeners without disposing them.
public sealed class BrokerListenersTests
{
    [Fact]
    public async Task Factory_returns_plain_only_when_tls_is_disabled()
    {
        IReadOnlyList<IListenerTransport> listeners = BrokerListeners.Create(PlainOptions());
        try
        {
            IListenerTransport listener = Assert.Single(listeners);
            Assert.IsType<SocketListener>(listener);
            Assert.True(listener.AnnouncedPort > 0);
        }
        finally
        {
            await DisposeAsync(listeners);
        }
    }

    [Fact]
    public async Task Factory_returns_both_when_tls_is_enabled()
    {
        using var certs = new TestCertificates();
        BrokerServerOptions options = PlainOptions();
        options.Tls = TlsOptions(certs);

        IReadOnlyList<IListenerTransport> listeners = BrokerListeners.Create(options);
        try
        {
            Assert.Equal(2, listeners.Count);
            Assert.IsType<SocketListener>(listeners[0]);
            Assert.IsType<TlsSocketListener>(listeners[1]);
            Assert.True(listeners[0].AnnouncedPort > 0);
            Assert.True(listeners[1].AnnouncedPort > 0);
            Assert.NotEqual(listeners[0].AnnouncedPort, listeners[1].AnnouncedPort);
        }
        finally
        {
            await DisposeAsync(listeners);
        }
    }

    [Fact]
    public async Task Factory_returns_no_plaintext_when_port_is_minus_one()
    {
        BrokerServerOptions options = PlainOptions();
        options.Port = -1;

        IReadOnlyList<IListenerTransport> listeners = BrokerListeners.Create(options);
        try
        {
            Assert.Empty(listeners);
        }
        finally
        {
            await DisposeAsync(listeners);
        }
    }

    [Fact]
    public async Task Multi_listener_server_accepts_on_both_ports()
    {
        using var certs = new TestCertificates();
        BrokerServerOptions options = PlainOptions();
        options.Tls = TlsOptions(certs);

        IReadOnlyList<IListenerTransport> listeners = BrokerListeners.Create(options);
        var server = new AmqpBrokerServer(options, new InMemoryEngine(options.Vhosts), listeners);
        await server.StartAsync();
        try
        {
            int plainPort = listeners.OfType<SocketListener>().Single().AnnouncedPort;
            int tlsPort = listeners.OfType<TlsSocketListener>().Single().AnnouncedPort;

            using (var plain = new TcpClient())
            {
                await plain.ConnectAsync(
                    IPAddress.Loopback, plainPort, TestContext.Current.CancellationToken);
                await SendProtocolHeaderAsync(plain.GetStream());
                (byte type, byte[] payload) = await ReadFrameAsync(plain.GetStream());
                AssertConnectionStart(type, payload);
            }

            using (var tcp = new TcpClient())
            {
                await tcp.ConnectAsync(
                    IPAddress.Loopback, tlsPort, TestContext.Current.CancellationToken);
                using var ssl = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false, (_, _, _, _) => true);
                await ssl.AuthenticateAsClientAsync(
                    new SslClientAuthenticationOptions { TargetHost = "localhost" },
                    TestContext.Current.CancellationToken);
                await SendProtocolHeaderAsync(ssl);
                (byte type, byte[] payload) = await ReadFrameAsync(ssl);
                AssertConnectionStart(type, payload);
            }
        }
        finally
        {
            await server.StopAsync();
            await DisposeAsync(listeners);
        }
    }

    [Fact]
    public async Task Single_listener_constructor_still_starts_one_loop()
    {
        BrokerServerOptions options = PlainOptions();
        var listener = new SocketListener(options.Port);
        var server = new AmqpBrokerServer(options, new InMemoryEngine(options.Vhosts), listener);
        await server.StartAsync();
        try
        {
            using var tcp = new TcpClient();
            await tcp.ConnectAsync(
                IPAddress.Loopback, listener.AnnouncedPort, TestContext.Current.CancellationToken);
            await SendProtocolHeaderAsync(tcp.GetStream());
            (byte type, byte[] payload) = await ReadFrameAsync(tcp.GetStream());
            AssertConnectionStart(type, payload);
        }
        finally
        {
            await server.StopAsync();
            await listener.DisposeAsync();
        }
    }

    // ---------- harness ----------

    private static BrokerServerOptions PlainOptions() => new()
    {
        Port = 0,
        Vhosts = ["/"],
        Users = [BrokerUser.FullAccess("guest", "guest", "/")],
    };

    private static BrokerTlsOptions TlsOptions(TestCertificates certs) => new()
    {
        Enabled = true,
        Port = 0,
        CertificatePath = certs.ServerPfxPath,
        CertificatePassword = TestCertificates.Password,
        HandshakeTimeout = TimeSpan.FromSeconds(5),
    };

    private static async Task DisposeAsync(IEnumerable<IListenerTransport> listeners)
    {
        foreach (IListenerTransport listener in listeners)
        {
            if (listener is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync();
            }
        }
    }

    private static async Task SendProtocolHeaderAsync(Stream stream) =>
        await stream.WriteAsync(
            "AMQP\x00\x00\x09\x01"u8.ToArray(), TestContext.Current.CancellationToken);

    private static async Task<(byte Type, byte[] Payload)> ReadFrameAsync(Stream stream)
    {
        byte[] header = new byte[7];
        await stream.ReadExactlyAsync(header, TestContext.Current.CancellationToken);
        int size = (header[3] << 24) | (header[4] << 16) | (header[5] << 8) | header[6];
        byte[] payload = new byte[size + 1]; // payload plus the frame-end octet
        await stream.ReadExactlyAsync(payload, TestContext.Current.CancellationToken);
        Assert.Equal(0xCE, payload[^1]);
        return (header[0], payload[..size]);
    }

    private static void AssertConnectionStart(byte type, byte[] payload)
    {
        Assert.Equal(1, type); // method frame
        Assert.True(payload.Length >= 4);
        Assert.Equal(10, (payload[0] << 8) | payload[1]); // connection
        Assert.Equal(10, (payload[2] << 8) | payload[3]); // start
    }
}
