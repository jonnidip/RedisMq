using System.IO.Pipelines;

namespace RedisMQ.Broker.Transport;

/// <summary>
/// One end of a byte transport: a reader and a writer, plus completion. In production
/// the sides wrap a socket's streams; in tests they are the halves of an in-proc
/// <see cref="TransportPair"/>.
/// </summary>
public sealed class TransportSide
{
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;

    public TransportSide(PipeReader reader, PipeWriter writer)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _writer = writer ?? throw new ArgumentNullException(nameof(writer));
    }

    /// <summary>Reads bytes arriving from the peer.</summary>
    public PipeReader Reader => _reader;

    /// <summary>Writes bytes to the peer.</summary>
    public PipeWriter Writer => _writer;

    /// <summary>Completes both ends: EOF flows to the peer's reader, and a pending read
    /// on this side is released. Idempotent through the Pipe contract.</summary>
    public async ValueTask CompleteAsync()
    {
        await _writer.CompleteAsync();
        await _reader.CompleteAsync();
    }
}

/// <summary>
/// A symmetric duplex in-proc transport pair: what one side writes arrives on the
/// other side's reader. <see cref="TransportClient"/> is handed to the client, and
/// <see cref="TransportServer"/> to the broker, letting Task-8-free tests drive the
/// wire server exactly as a socket would.
/// </summary>
public sealed class TransportPair
{
    public TransportPair(TransportSide transportClient, TransportSide transportServer)
    {
        TransportClient = transportClient ?? throw new ArgumentNullException(nameof(transportClient));
        TransportServer = transportServer ?? throw new ArgumentNullException(nameof(transportServer));
    }

    public TransportSide TransportClient { get; }

    public TransportSide TransportServer { get; }

    public static TransportPair Create()
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        return new TransportPair(
            transportClient: new TransportSide(serverToClient.Reader, clientToServer.Writer),
            transportServer: new TransportSide(clientToServer.Reader, serverToClient.Writer));
    }
}

/// <summary>
/// The accept seam of the broker: the connection loop pulls fresh
/// <see cref="TransportSide"/>s from here. Task 8 supplies the TCP listener
/// implementation; tests supply a listener that yields the halves of created
/// TransportPairs. Returning null means the listener is closed and the accept loop ends.
/// </summary>
public interface IListenerTransport
{
    /// <summary>The port the listener is reachable on, when it binds one; in-proc
    /// test listeners and other portless transports report null.</summary>
    int? AnnouncedPort => null;

    ValueTask<TransportSide?> AcceptAsync(CancellationToken cancellationToken = default);
}
