using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using RedisMQ.Broker.Server;

namespace RedisMQ.Broker.Transport;

/// <summary>
/// The TLS <see cref="IListenerTransport"/>: binds a socket exactly like
/// <see cref="SocketListener"/>, then wraps each accepted socket in an
/// <see cref="SslStream"/>. A failed, timed-out or client-certificate-rejected
/// handshake closes only that socket — the accept stream continues — so a bad peer
/// can never stop the listener. A successful handshake becomes a
/// <see cref="TransportSide"/> over the encrypted stream.
/// </summary>
public sealed class TlsSocketListener : IListenerTransport, IAsyncDisposable
{
    private Socket? _socket;
    private readonly int _announcedPort;
    private readonly BrokerTlsOptions _options;
    private readonly BrokerCertificate _certificate;
    private readonly SslServerAuthenticationOptions _authentication;

    /// <summary>Loads the configured certificate and binds the TLS port.</summary>
    public TlsSocketListener(BrokerTlsOptions options)
        : this(options, BrokerCertificate.Load(options))
    {
    }

    /// <summary>A listener over an already-loaded certificate (used by tests and by
    /// callers that share one certificate between listeners).</summary>
    internal TlsSocketListener(BrokerTlsOptions options, BrokerCertificate certificate)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
        _authentication = certificate.CreateAuthenticationOptions();
        (_socket, _announcedPort) = SocketBinder.Bind(options.Port);
    }

    /// <summary>The port the bound TLS listener is reachable on (0 requested →
    /// OS-assigned).</summary>
    public int AnnouncedPort => _announcedPort;

    /// <inheritdoc/>
    int? IListenerTransport.AnnouncedPort => _announcedPort;

    /// <inheritdoc/>
    public async ValueTask<TransportSide?> AcceptAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            Socket? socket = _socket;
            if (socket is null)
            {
                return null;
            }

            Socket accepted;
            try
            {
                accepted = await socket.AcceptAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return null; // stopping
            }
            catch (ObjectDisposedException)
            {
                return null; // the listener was discarded: accept stream ends
            }
            catch (SocketException)
            {
                return null; // listener closed underneath: treated the same way
            }

            // The SslStream owns the NetworkStream, which owns the accepted socket, so
            // disposing the SslStream on failure closes the peer.
            var ssl = new SslStream(new NetworkStream(accepted, ownsSocket: true), leaveInnerStreamOpen: false);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(_options.HandshakeTimeout);
            try
            {
                await ssl.AuthenticateAsServerAsync(_authentication, timeout.Token);
                return new TransportSide(PipeReader.Create(ssl), PipeWriter.Create(ssl));
            }
            catch (Exception exception) when (
                exception is AuthenticationException or IOException or SocketException or OperationCanceledException)
            {
                ssl.Dispose();
                if (cancellationToken.IsCancellationRequested)
                {
                    return null;
                }

                // A bad, timed-out or mTLS-rejected handshake belongs to that one peer;
                // keep accepting so the listener survives hostile traffic.
                continue;
            }
        }
    }

    /// <summary>Ends the accept stream and releases the certificate. In-flight accepts
    /// return null (listener closed).</summary>
    public ValueTask DisposeAsync()
    {
        Socket? socket = Interlocked.Exchange(ref _socket, null);
        if (socket is not null)
        {
            try
            {
                socket.Dispose();
            }
            catch
            {
                // A disposed socket is disposed; concurrent accept holders unwind.
            }
        }

        _certificate.Dispose();
        return ValueTask.CompletedTask;
    }
}
