using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

/// <summary>
/// The shared raw-wire client the observability tests drive the broker with: options,
/// a transport-pair listener, the frame builders, and the convenience calls (handshake,
/// channel, declare, publish, settle) needed by the diagnostics suites. It is the
/// per-file ConfirmPipelineTests harness promoted to one reusable type.
/// </summary>
internal sealed class DiagnosticsWireHarness : IAsyncDisposable
{
    private const uint FrameMax = 131072;

    private DiagnosticsWireHarness(
        AmqpBrokerServer server,
        DiagnosticsWireClient client,
        BrokerDiagnostics diagnostics,
        BrokerServerOptions options)
    {
        Server = server;
        Client = client;
        Diagnostics = diagnostics;
        Options = options;
    }

    public AmqpBrokerServer Server { get; }

    public DiagnosticsWireClient Client { get; }

    public BrokerDiagnostics Diagnostics { get; }

    public BrokerServerOptions Options { get; }

    internal static BrokerServerOptions NewOptions() => new()
    {
        Users = [BrokerUser.FullAccess("guest", "guest", "/")],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

    internal static DiagnosticsWireHarness Start(
        BrokerServerOptions? options = null,
        IRouteEngine? engine = null,
        BrokerDiagnostics? diagnostics = null)
    {
        options ??= NewOptions();
        diagnostics ??= new BrokerDiagnostics();
        engine ??= new InMemoryEngine(options.Vhosts, diagnostics);

        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(
            options, engine, new TransportPairConnectionListener(pair), diagnostics);
        _ = server.StartAsync();
        return new DiagnosticsWireHarness(
            server, new DiagnosticsWireClient(pair.TransportClient), diagnostics, options);
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await Server.StopAsync();
    }

    // ---------- handshake ----------

    /// <summary>Header + start + start-ok + tune + tune-ok, stopping before
    /// connection.open so a test can open (or be refused on) a chosen vhost.</summary>
    public async Task AuthenticateAsync(string user = "guest", string password = "guest")
    {
        await Client.SendAsync("AMQP\x00\x00\x09\x01"u8.ToArray());
        _ = await Client.ReadMethodAsync(10, 10); // connection.start
        await Client.SendAsync(StartOkFrame(PlainResponse(user, password)));
        _ = await ReadExpectAsync(10, 30); // connection.tune (or a close refusal)
        await Client.SendAsync(TuneOkFrame(2047, 131072, 0));
    }

    /// <summary>The full handshake including connection.open on "/", consuming the
    /// connection.open-ok so channel reads start from a clean inbox.</summary>
    public async Task HandshakeAsync(string user = "guest", string password = "guest")
    {
        await AuthenticateAsync(user, password);
        await Client.SendAsync(OpenFrame("/"));
        _ = await ReadExpectAsync(10, 41); // connection.open-ok
    }

    /// <summary>Opens a vhost after <see cref="AuthenticateAsync"/> and consumes the
    /// open-ok, throwing on a connection.close refusal.</summary>
    public async Task OpenVhostAsync(string vhost)
    {
        await Client.SendAsync(OpenFrame(vhost));
        _ = await ReadExpectAsync(10, 41);
    }

    // ---------- channel lifecycle ----------

    public async Task OpenChannelAsync(ushort channel = 1)
    {
        await Client.SendAsync(MethodFrameOn(channel, ChannelOpenFrame()));
        _ = await Client.ReadMethodOnChannelAsync(channel, 20, 11);
    }

    public async Task OpenConfirmChannelAsync(ushort channel = 1)
    {
        await OpenChannelAsync(channel);
        await Client.SendAsync(MethodFrameOn(channel, ConfirmSelectFrame()));
        _ = await Client.ReadMethodOnChannelAsync(channel, 85, 11);
    }

    /// <summary>Client-initiated close: sends channel.close and consumes close-ok.</summary>
    public async Task CloseChannelAsync(ushort channel)
    {
        await Client.SendAsync(MethodFrameOn(channel, ClientChannelCloseFrame()));
        _ = await Client.ReadMethodOnChannelAsync(channel, 20, 41);
    }

    /// <summary>Completes a server-initiated close (the channel is mourning): sends
    /// channel.close-ok, which produces no reply and frees the channel number.</summary>
    public async Task RetireMourningChannelAsync(ushort channel) =>
        await Client.SendAsync(MethodFrameOn(channel, ChannelCloseOkFrame()));

    public async Task CloseConnectionAsync()
    {
        await Client.SendAsync(MethodFrameOn(0, ClientConnectionCloseFrame()));
        _ = await Client.ReadMethodAsync(10, 51); // connection.close-ok
    }

    // ---------- topology and publish ----------

    public async Task DeclareQueueAsync(ushort channel, string queue)
    {
        await Client.SendAsync(MethodFrameOn(channel, QueueDeclareFrame(channel, queue)));
        _ = await Client.ReadMethodOnChannelAsync(channel, 50, 11);
    }

    public async Task DeclareExchangeAsync(ushort channel, string exchange, string type)
    {
        await Client.SendAsync(MethodFrameOn(channel, ExchangeDeclareFrame(channel, exchange, type)));
        _ = await Client.ReadMethodOnChannelAsync(channel, 40, 11);
    }

    public async Task BindAsync(ushort channel, string queue, string exchange, string routingKey)
    {
        await Client.SendAsync(MethodFrameOn(channel, QueueBindFrame(channel, queue, exchange, routingKey)));
        _ = await Client.ReadMethodOnChannelAsync(channel, 50, 21);
    }

    public async Task PublishAsync(
        ushort channel,
        string exchange,
        string routingKey,
        bool mandatory = false,
        string body = "m")
    {
        await Client.SendAsync(MethodFrameOn(channel, PublishFrame(channel, exchange, routingKey, mandatory)));
        await Client.SendAsync(ContentHeaderFrameOn(
            channel, body: Encoding.UTF8.GetByteCount(body), properties: new ContentProperties(ContentType: "text/plain")));
        await Client.SendAsync(BodyFrameOn(channel, Encoding.UTF8.GetBytes(body)));
    }

    // ---------- raw sends (a soft error answers with channel.close) ----------

    public Task SendQueueDeclareAsync(ushort channel, string queue) =>
        Client.SendAsync(MethodFrameOn(channel, QueueDeclareFrame(channel, queue)));

    public Task SendBasicGetAsync(ushort channel, string queue, bool noAck = false) =>
        Client.SendAsync(MethodFrameOn(channel, BasicGetFrame(channel, queue, noAck)));

    public Task SendBasicAckAsync(ushort channel, ulong deliveryTag) =>
        Client.SendAsync(MethodFrameOn(channel, BasicAckFrame(deliveryTag)));

    public Task SendBasicNackAsync(ushort channel, ulong deliveryTag, bool requeue) =>
        Client.SendAsync(MethodFrameOn(channel, BasicNackFrame(deliveryTag, requeue)));

    public Task SendBasicRejectAsync(ushort channel, ulong deliveryTag, bool requeue) =>
        Client.SendAsync(MethodFrameOn(channel, BasicRejectFrame(deliveryTag, requeue)));

    // ---------- delivery and get ----------

    public async Task<(AmqpMethod GetOk, ContentHeader Header, byte[] Body)?> BasicGetAsync(
        ushort channel,
        string queue,
        bool noAck = false)
    {
        await SendBasicGetAsync(channel, queue, noAck);
        AmqpMethod answer = await Client.ReadMethodOnChannelAsync(channel, 60, 0);
        if (answer.MethodId == 72) // basic.get-empty
        {
            return null;
        }

        (ContentHeader header, byte[] body) = await Client.ReadContentOnChannelAsync(channel);
        return (answer, header, body);
    }

    public async Task BasicConsumeAsync(
        ushort channel,
        string queue,
        bool noAck = false,
        string consumerTag = "ct")
    {
        await Client.SendAsync(MethodFrameOn(
            channel, BasicConsumeFrame(channel, queue, consumerTag, noAck)));
        _ = await Client.ReadMethodOnChannelAsync(channel, 60, 21);
    }

    public async Task<(AmqpMethod Deliver, ContentHeader Header, byte[] Body)> ReadDeliveryAsync(
        ushort channel = 1)
    {
        AmqpMethod deliver = await Client.ReadMethodOnChannelAsync(channel, 60, 60);
        (ContentHeader header, byte[] body) = await Client.ReadContentOnChannelAsync(channel);
        return (deliver, header, body);
    }

    public Task<AmqpMethod> ReadMethodOnChannelAsync(ushort channel, ushort classId, ushort methodId) =>
        Client.ReadMethodOnChannelAsync(channel, classId, methodId);

    public Task<AmqpMethod> ReadMethodAsync(ushort classId, ushort methodId) =>
        Client.ReadMethodAsync(classId, methodId);

    private async Task<AmqpMethod> ReadExpectAsync(ushort classId, ushort methodId)
    {
        AmqpMethod method = await Client.ReadMethodAsync(0, 0);
        if (method.ClassId == 10 && method.MethodId == 50)
        {
            string text = method.Get("reply-text") as string ?? string.Empty;
            throw new InvalidOperationException($"Broker refused the connection: {text}");
        }

        if (method.ClassId != classId || method.MethodId != methodId)
        {
            throw new InvalidOperationException(
                $"Expected a method frame ({classId},{methodId}) but got ({method.ClassId},{method.MethodId})");
        }

        return method;
    }

    // ---------- frame builders ----------

    private static byte[] PlainResponse(string username, string password) =>
        Encoding.UTF8.GetBytes($"\0{username}\0{password}"); // PLAIN: authzid NUL authcid NUL password

    private static byte[] StartOkFrame(byte[] plainResponse)
    {
        AmqpMethod method = NewMethod("connection.start-ok");
        method.Set("client-properties", new FieldTable([new("product", FieldValue.From("diagnostics-client"))]));
        method.Set("mechanism", "PLAIN");
        method.Set("response", plainResponse);
        method.Set("locale", "en_US");
        return MethodFrameOn(0, method);
    }

    private static byte[] TuneOkFrame(ushort channelMax, uint frameMax, ushort heartbeat)
    {
        AmqpMethod method = NewMethod("connection.tune-ok");
        method.Set("channel-max", (short)channelMax);
        method.Set("frame-max", (int)frameMax);
        method.Set("heartbeat", (short)heartbeat);
        return MethodFrameOn(0, method);
    }

    private static byte[] OpenFrame(string vhost)
    {
        AmqpMethod method = NewMethod("connection.open");
        method.Set("virtual-host", vhost);
        method.Set("capabilities", "");
        method.Set("insist", false);
        return MethodFrameOn(0, method);
    }

    private static AmqpMethod ClientConnectionCloseFrame()
    {
        AmqpMethod method = NewMethod("connection.close");
        method.Set("reply-code", (short)200);
        method.Set("reply-text", "client-does-not-care");
        method.Set("class-id", (short)0);
        method.Set("method-id", (short)0);
        return method;
    }

    private static AmqpMethod ChannelOpenFrame()
    {
        AmqpMethod method = NewMethod("channel.open");
        method.Set("out-of-band", "");
        return method;
    }

    private static AmqpMethod ConfirmSelectFrame(bool nowait = false)
    {
        AmqpMethod method = NewMethod("confirm.select");
        method.Set("nowait", nowait);
        return method;
    }

    private static AmqpMethod ExchangeDeclareFrame(ushort channel, string exchange, string type)
    {
        AmqpMethod method = NewMethod("exchange.declare");
        method.Set("ticket", (short)0);
        method.Set("exchange", exchange);
        method.Set("type", type);
        method.Set("passive", false);
        method.Set("durable", false);
        method.Set("auto-delete", false);
        method.Set("internal", false);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod QueueDeclareFrame(ushort channel, string queue)
    {
        AmqpMethod method = NewMethod("queue.declare");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("passive", false);
        method.Set("durable", false);
        method.Set("exclusive", false);
        method.Set("auto-delete", false);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod QueueBindFrame(
        ushort channel,
        string queue,
        string exchange,
        string routingKey)
    {
        AmqpMethod method = NewMethod("queue.bind");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("exchange", exchange);
        method.Set("routing-key", routingKey);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod PublishFrame(
        ushort channel,
        string exchange,
        string routingKey,
        bool mandatory = false,
        bool immediate = false)
    {
        AmqpMethod method = NewMethod("basic.publish");
        method.Set("ticket", (short)0);
        method.Set("exchange", exchange);
        method.Set("routing-key", routingKey);
        method.Set("mandatory", mandatory);
        method.Set("immediate", immediate);
        return method;
    }

    private static AmqpMethod BasicGetFrame(ushort channel, string queue, bool noAck = false)
    {
        AmqpMethod method = NewMethod("basic.get");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("no-ack", noAck);
        return method;
    }

    private static AmqpMethod BasicConsumeFrame(
        ushort channel,
        string queue,
        string consumerTag,
        bool noAck)
    {
        AmqpMethod method = NewMethod("basic.consume");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("consumer-tag", consumerTag);
        method.Set("no-local", false);
        method.Set("no-ack", noAck);
        method.Set("exclusive", false);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod BasicAckFrame(ulong deliveryTag)
    {
        AmqpMethod method = NewMethod("basic.ack");
        method.Set("delivery-tag", (long)deliveryTag);
        method.Set("multiple", false);
        return method;
    }

    private static AmqpMethod BasicNackFrame(ulong deliveryTag, bool requeue)
    {
        AmqpMethod method = NewMethod("basic.nack");
        method.Set("delivery-tag", (long)deliveryTag);
        method.Set("multiple", false);
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod BasicRejectFrame(ulong deliveryTag, bool requeue)
    {
        AmqpMethod method = NewMethod("basic.reject");
        method.Set("delivery-tag", (long)deliveryTag);
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod ClientChannelCloseFrame()
    {
        AmqpMethod method = NewMethod("channel.close");
        method.Set("reply-code", (short)200);
        method.Set("reply-text", "client-does-not-care");
        method.Set("class-id", (short)0);
        method.Set("method-id", (short)0);
        return method;
    }

    private static AmqpMethod ChannelCloseOkFrame() => NewMethod("channel.close-ok");

    private static AmqpMethod NewMethod(string name) =>
        MethodRegistry.TryGetByName(name, out var schema)
            ? new AmqpMethod(schema!)
            : throw new InvalidOperationException($"Registry is missing {name}");

    private static byte[] MethodFrameOn(ushort channel, AmqpMethod method)
    {
        var payloadWriter = new ByteWriter();
        MethodCodec.WriteMethod(ref payloadWriter, method);
        byte[] payload = payloadWriter.ToArray();
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, channel, method.ClassId, method.MethodId, payload.AsSpan(4));
        return frame.ToArray();
    }

    private static byte[] ContentHeaderFrameOn(
        ushort channel, long body, ContentProperties properties)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteContentHeader(ref frame, channel, classId: 60, bodySize: body, properties);
        return frame.ToArray();
    }

    private static byte[] BodyFrameOn(ushort channel, byte[] payload)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteBody(ref frame, channel, payload, FrameMax);
        return frame.ToArray();
    }

    private sealed class TransportPairConnectionListener(params TransportPair[] pairs) : IListenerTransport
    {
        private int _index;

        public ValueTask<TransportSide?> AcceptAsync(CancellationToken cancellationToken = default)
        {
            if (_index < pairs.Length)
            {
                return ValueTask.FromResult<TransportSide?>(pairs[_index++].TransportServer);
            }

            return ValueTask.FromResult<TransportSide?>(null); // listener closed
        }
    }
}

/// <summary>
/// The client half of a <see cref="TransportPair"/>: sends raw byte frames and decodes
/// the real bytes the broker produces through the <see cref="FrameDecoder"/>.
/// </summary>
internal sealed class DiagnosticsWireClient(TransportSide side) : IAsyncDisposable
{
    private byte[] _buffer = new byte[4096];
    private int _length;
    private readonly Queue<AmqpFrame> _inbox = new();

    public async Task SendAsync(byte[] bytes)
    {
        var writer = side.Writer;
        var span = writer.GetSpan(bytes.Length);
        bytes.AsSpan().CopyTo(span);
        writer.Advance(bytes.Length);
        await writer.FlushAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync() => side.CompleteAsync();

    public Task<AmqpMethod> ReadMethodAsync(int timeoutMs = 5_000) => ReadMethodAsync(0, 0, timeoutMs);

    public async Task<AmqpMethod> ReadMethodAsync(ushort classId, ushort methodId, int timeoutMs = 5_000)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        while (true)
        {
            if (_inbox.Count == 0)
            {
                await PumpAsync(cts.Token);
                continue;
            }

            AmqpFrame frame = _inbox.Dequeue();
            if (frame.Type != AmqpFrame.FrameMethod)
            {
                continue;
            }

            var reader = new ByteReader(frame.Payload.Span);
            ushort actualClass = reader.ReadUShort();
            ushort actualMethod = reader.ReadUShort();
            if ((classId != 0 && actualClass != classId)
                || (methodId != 0 && actualMethod != methodId))
            {
                throw new InvalidOperationException(
                    $"Expected a method frame ({classId},{methodId}) but got ({actualClass},{actualMethod})");
            }

            return Decode(actualClass, actualMethod, ref reader);
        }
    }

    public async Task<AmqpMethod> ReadMethodOnChannelAsync(
        ushort channel, ushort classId = 0, ushort methodId = 0, int timeoutMs = 5_000)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        while (true)
        {
            if (_inbox.Count == 0)
            {
                await PumpAsync(cts.Token);
                continue;
            }

            AmqpFrame frame = _inbox.Dequeue();
            if (frame.Type != AmqpFrame.FrameMethod)
            {
                continue;
            }

            var reader = new ByteReader(frame.Payload.Span);
            ushort actualClass = reader.ReadUShort();
            ushort actualMethod = reader.ReadUShort();
            if (frame.Channel != channel)
            {
                throw new InvalidOperationException(
                    $"Cross-talk: expected ({classId},{methodId}) on channel {channel} " +
                    $"but the next method frame was ({actualClass},{actualMethod}) on channel {frame.Channel}");
            }

            if ((classId != 0 && actualClass != classId)
                || (methodId != 0 && actualMethod != methodId))
            {
                throw new InvalidOperationException(
                    $"Expected a method frame ({classId},{methodId}) on channel {channel} " +
                    $"but got ({actualClass},{actualMethod})");
            }

            return Decode(actualClass, actualMethod, ref reader);
        }
    }

    public async Task<(ContentHeader Header, byte[] Body)> ReadContentOnChannelAsync(
        ushort channel, int timeoutMs = 5_000)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        ContentHeader? header = null;
        var body = new List<byte>();
        while (true)
        {
            if (_inbox.Count == 0)
            {
                await PumpAsync(cts.Token);
                continue;
            }

            AmqpFrame frame = _inbox.Dequeue();
            if (frame.Type == AmqpFrame.FrameMethod)
            {
                throw new InvalidOperationException(
                    $"A method frame arrived on channel {frame.Channel} where content was expected");
            }

            if (frame.Channel != channel)
            {
                throw new InvalidOperationException(
                    $"Cross-talk: content was expected on channel {channel} but arrived on channel {frame.Channel}");
            }

            if (frame.Type == AmqpFrame.FrameHeader)
            {
                if (header is not null)
                {
                    throw new InvalidOperationException("A second content header frame arrived in one sequence");
                }

                var reader = new ByteReader(frame.Payload.Span);
                header = ContentHeaderCodec.Read(ref reader);
            }
            else
            {
                body.AddRange(frame.Payload.ToArray());
            }

            if (header is not null && body.Count >= header.BodySize)
            {
                return (header, body.ToArray());
            }
        }
    }

    private async Task PumpAsync(CancellationToken ct)
    {
        ReadResult read;
        try
        {
            read = await side.Reader.ReadAsync(ct);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException("No frame arrived from the broker in time");
        }

        if (read.Buffer.IsEmpty)
        {
            side.Reader.AdvanceTo(read.Buffer.Start, read.Buffer.End);
            if (read.IsCompleted)
            {
                throw new InvalidOperationException("The broker closed the transport before answering");
            }

            return;
        }

        CopyIn(read.Buffer);
        side.Reader.AdvanceTo(read.Buffer.End);
        DecodeAvailable();
    }

    private void CopyIn(ReadOnlySequence<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return;
        }

        long needed = _length + bytes.Length;
        if (needed > _buffer.Length)
        {
            int grown = (int)Math.Max(needed, _buffer.Length * 2L);
            Array.Resize(ref _buffer, grown);
        }

        bytes.CopyTo(_buffer.AsSpan(_length, (int)bytes.Length));
        _length += (int)bytes.Length;
    }

    private void DecodeAvailable()
    {
        if (_length == 0)
        {
            return;
        }

        var buffer = new ReadOnlySequence<byte>(_buffer, 0, _length);
        var frames = new List<AmqpFrame>();
        _ = FrameDecoder.ReadFrames(ref buffer, frames, uint.MaxValue);
        int remaining = (int)buffer.Length;
        if (remaining > 0)
        {
            _buffer.AsSpan(_length - remaining, remaining).CopyTo(_buffer.AsSpan(0, remaining));
        }

        Array.Clear(_buffer, remaining, _length - remaining);
        _length = remaining;
        foreach (var frame in frames)
        {
            _inbox.Enqueue(frame);
        }
    }

    private static AmqpMethod Decode(ushort classId, ushort methodId, ref ByteReader reader)
    {
        MethodSchema? schema = MethodRegistry.Schema(classId, methodId)
            ?? throw new InvalidOperationException($"Method ({classId},{methodId}) is not in the registry");
        return MethodCodec.ReadMethod(ref reader, schema);
    }
}
