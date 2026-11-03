using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.IO.Pipelines;

namespace RedisMQ.Broker.Transport;

/// <summary>
/// The TCP <see cref="IListenerTransport"/>: binds a TCP port at construction
/// (port 0 → OS-assigned, reported through <see cref="AnnouncedPort"/>), and accepts
/// connections as <see cref="TransportSide"/>s over <see cref="NetworkStream"/>s —
/// exactly what the in-proc TransportPair sides fake for wire tests. Accept
/// cancellation or the listener being disposed ends the stream of sides with null
/// (the accept seam's "listener closed" contract).
/// </summary>
public sealed class SocketListener : IListenerTransport, IAsyncDisposable
{
    private Socket? _socket;
    private readonly int _announcedPort;

    /// <summary>Binds and listens. Throws for bind failures the way any socket user
    /// would observe them (in use → SocketException).</summary>
    public SocketListener(int AnnouncedPort) // named for the ctor-facing announced-port argument
    {
        (_socket, _announcedPort) = SocketBinder.Bind(AnnouncedPort);
    }

    /// <summary>The port the bound listener is reachable on (0 requested → OS-assigned).</summary>
    public int AnnouncedPort => _announcedPort;

    /// <inheritdoc/>
    int? IListenerTransport.AnnouncedPort => _announcedPort;

    /// <inheritdoc/>
    public async ValueTask<TransportSide?> AcceptAsync(CancellationToken cancellationToken = default)
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

        var stream = new NetworkStream(accepted, ownsSocket: true);
        return new TransportSide(
            reader: PipeReader.Create(stream),
            writer: PipeWriter.Create(stream));
    }

    /// <summary>Ends the accept stream. In-flight accepts return null (listener closed).</summary>
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

        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// The socket-binding step shared by the plaintext and TLS listeners: try a dual-mode
/// IPv6 socket on the wildcard address, fall back to IPv4 loopback where IPv6 is
/// unavailable, then listen and report the port the OS actually bound.
/// </summary>
internal static class SocketBinder
{
    internal static (Socket Socket, int Port) Bind(int port)
    {
        // Dual-mode IPv6 covers both ::1 and 127.0.0.1 on the loopback; a fallback
        // to plain IPv4 keeps it usable where v6 is unavailable.
        Socket socket = NewDualModeSocket();
        try
        {
            socket.Bind(new IPEndPoint(IPAddress.IPv6Any, port));
        }
        catch (SocketException ex) when (ex.SocketErrorCode is SocketError.AddressFamilyNotSupported or SocketError.ProtocolNotSupported or SocketError.AddressNotAvailable)
        {
            socket.Dispose();
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(new IPEndPoint(IPAddress.Loopback, port));
        }

        socket.Listen();
        return (socket, ((IPEndPoint)socket.LocalEndPoint!).Port);
    }

    private static Socket NewDualModeSocket() => new(AddressFamily.InterNetworkV6, SocketType.Stream, ProtocolType.Tcp)
    {
        DualMode = true,
    };
}
