using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 3: every settle path the channel owns must reach the engine, because the
// Redis mirror answers acks and requeues from those calls. These tests drive the
// real wire server against a RecordingEngine and assert the ordered settle trace.
public sealed class SettleMirroringTests
{
    [Fact]
    public async Task Ack_calls_Acknowledge()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            await FenceAsync(client, 1, "q");

            Assert.Equal(["Ack:q:1"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Reject_without_requeue_calls_Reject()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);

            await client.SendAsync(MethodFrameOn(1, RejectFrame(1, requeue: false)));
            await FenceAsync(client, 1, "q");

            Assert.Equal(["Reject:q:1"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Nack_with_requeue_calls_Requeue()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);

            await client.SendAsync(MethodFrameOn(1, NackFrame(1, requeue: true)));
            await FenceAsync(client, 1, "q");

            Assert.Equal(["Requeue:q:1"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task No_ack_consumer_calls_Acknowledge_on_delivery()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct", noAck: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);

            Assert.Equal(["Ack:q:1"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Get_with_no_ack_calls_Acknowledge()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q", noAck: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 71);
            _ = await client.ReadContentOnChannelAsync(1);

            Assert.Equal(["Ack:q:1"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Recover_calls_Requeue_for_every_unacked_entry()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);
            _ = await ReadDeliverAsync(client, 1);

            await client.SendAsync(MethodFrameOn(1, RecoverFrame(requeue: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 111);
            await FenceAsync(client, 1, "q");

            Assert.Equal(["Requeue:q:1", "Requeue:q:2"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Channel_close_requeues_unacked_entries()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);

            await client.SendAsync(MethodFrameOn(1, ClientChannelCloseFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 41);

            // close-ok is written before the consumer sweep retires the channel. A
            // fresh channel.open on the same number is a later frame the same pump
            // handles only after teardown has finished.
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            Assert.Equal(["Requeue:q:1"], engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Reject_without_requeue_delivers_x_death_on_the_dlx_target()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            var dlxArgs = new FieldTable([
                new("x-dead-letter-exchange", FieldValue.From("")),
                new("x-dead-letter-routing-key", FieldValue.From("target")),
            ]);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "source", arguments: dlxArgs), (50, 11));
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "target"), (50, 11));

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "source")));
            await client.SendAsync(ContentHeaderFrameOn(
                1, body: 1, new ContentProperties(ContentType: "text/plain")));
            await client.SendAsync(BodyFrameOn(1, [42]));
            await FenceAsync(client, 1, "source");

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "source", consumerTag: "ct-source")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1);

            await client.SendAsync(MethodFrameOn(1, RejectFrame(1, requeue: false)));
            await FenceAsync(client, 1, "source");

            // The dead-letter copy is a normal ready message in the target queue.
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "target", consumerTag: "ct-target")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod _, ContentHeader header, byte[] body) = await ReadDeliverAsync(client, 1);
            Assert.Equal(new byte[] { 42 }, body);

            var deaths = (FieldValue[])header.Properties.Headers!["x-death"].Raw!;
            var death = (FieldTable)deaths[0].Raw!;
            Assert.Equal("source", death["queue"].Raw);
            Assert.Equal("rejected", death["reason"].Raw);
            Assert.Equal(1L, death["count"].Raw);
            Assert.Equal(string.Empty, death["exchange"].Raw); // published on the default exchange
            Assert.IsType<DateTimeOffset>(death["time"].Raw);
            Assert.Equal("source", header.Properties.Headers!["x-first-death-queue"].Raw);

            Assert.Contains("Reject:source:1", engine.Settles);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- harness (the per-file M0 pattern mirrors ConsumeTests) ----------

    private static BrokerServerOptions NewOptions() => new()
    {
        Users = [new BrokerUser("guest", "guest")],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

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

    private static (AmqpBrokerServer Server, WireClient Client, RecordingEngine Engine) SpawnServer()
    {
        BrokerServerOptions options = NewOptions();
        var engine = new RecordingEngine(options.Vhosts);
        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(options, engine, new TransportPairConnectionListener(pair));
        server.StartAsync();
        return (server, new WireClient(pair.TransportClient), engine);
    }

    private static async Task LoadAsync(WireClient client, ushort channel, string queue, params string[] bodies)
    {
        await DeclareAsync(client, channel, QueueDeclareFrame(channel, queue), (50, 11));

        foreach (string body in bodies)
        {
            await client.SendAsync(MethodFrameOn(channel, PublishFrame(channel, "", queue)));
            await client.SendAsync(ContentHeaderFrameOn(channel, body: Encoding.UTF8.GetByteCount(body), properties: new ContentProperties(ContentType: "text/plain")));
            await client.SendAsync(BodyFrameOn(channel, Encoding.UTF8.GetBytes(body)));
        }

        await FenceAsync(client, channel, queue);
    }

    private static async Task FenceAsync(WireClient client, ushort channel, string queue) =>
        await DeclareAsync(client, channel, QueueDeclareFrame(channel, queue, passive: true), (50, 11));

    private static async Task DeclareAsync(WireClient client, ushort channel, AmqpMethod method, (ushort Class, ushort Method) okId)
    {
        await client.SendAsync(MethodFrameOn(channel, method));
        _ = await client.ReadMethodOnChannelAsync(channel, okId.Class, okId.Method);
    }

    private static async Task<(AmqpMethod Method, ContentHeader Header, byte[] Body)> ReadDeliverAsync(
        WireClient client, ushort channel)
    {
        if (client.TakeParkedDeliver(channel) is (AmqpMethod parked, ContentHeader parkedHeader, byte[] parkedBody))
        {
            return (parked, parkedHeader, parkedBody);
        }

        AmqpMethod method = await client.ReadMethodOnChannelAsync(channel, 60, 60);
        (ContentHeader header, byte[] body) = await client.ReadContentOnChannelAsync(channel);
        return (method, header, body);
    }

    // ---------- evil-client wire helpers ----------

    private static async Task HandshakeAsync(WireClient client)
    {
        await client.SendAsync("AMQP\x00\x00\x09\x01"u8.ToArray());
        _ = await client.ReadMethodAsync(10, 10); // connection.start
        await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
        _ = await client.ReadMethodAsync(10, 30); // connection.tune
        await client.SendAsync(TuneOkFrame(2047, 131072, 0));
        await client.SendAsync(OpenFrame("/"));
    }

    private static byte[] PlainResponse(string username, string password) =>
        Encoding.UTF8.GetBytes($"\0{username}\0{password}"); // PLAIN: authzid NUL authcid NUL password

    private static byte[] StartOkFrame(byte[] plainResponse)
    {
        AmqpMethod method = NewMethod("connection.start-ok");
        method.Set("client-properties", new FieldTable([new("product", FieldValue.From("evil-client"))]));
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

    private static AmqpMethod ChannelOpenFrame()
    {
        AmqpMethod method = NewMethod("channel.open");
        method.Set("out-of-band", "");
        return method;
    }

    private static AmqpMethod ConsumeFrame(
        ushort channel,
        string queue,
        string consumerTag,
        bool noLocal = false,
        bool noAck = false,
        bool exclusive = false,
        bool nowait = false)
    {
        AmqpMethod method = NewMethod("basic.consume");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("consumer-tag", consumerTag);
        method.Set("no-local", noLocal);
        method.Set("no-ack", noAck);
        method.Set("exclusive", exclusive);
        method.Set("nowait", nowait);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod AckFrame(long deliveryTag, bool multiple = false)
    {
        AmqpMethod method = NewMethod("basic.ack");
        method.Set("delivery-tag", deliveryTag);
        method.Set("multiple", multiple);
        return method;
    }

    private static AmqpMethod NackFrame(long deliveryTag, bool multiple = false, bool requeue = true)
    {
        AmqpMethod method = NewMethod("basic.nack");
        method.Set("delivery-tag", deliveryTag);
        method.Set("multiple", multiple);
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod RejectFrame(long deliveryTag, bool requeue)
    {
        AmqpMethod method = NewMethod("basic.reject");
        method.Set("delivery-tag", deliveryTag);
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod RecoverFrame(bool requeue)
    {
        AmqpMethod method = NewMethod("basic.recover");
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod GetFrame(ushort channel, string queue, bool noAck = false)
    {
        AmqpMethod method = NewMethod("basic.get");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("no-ack", noAck);
        return method;
    }

    private static AmqpMethod QueueDeclareFrame(
        ushort channel,
        string queue,
        bool durable = false,
        bool exclusive = false,
        bool autoDelete = false,
        bool passive = false,
        bool nowait = false,
        FieldTable? arguments = null)
    {
        AmqpMethod method = NewMethod("queue.declare");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("passive", passive);
        method.Set("durable", durable);
        method.Set("exclusive", exclusive);
        method.Set("auto-delete", autoDelete);
        method.Set("nowait", nowait);
        method.Set("arguments", arguments ?? new FieldTable());
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

    private static AmqpMethod ClientChannelCloseFrame()
    {
        AmqpMethod method = NewMethod("channel.close");
        method.Set("reply-code", (short)200);
        method.Set("reply-text", "client-does-not-care");
        method.Set("class-id", (short)0);
        method.Set("method-id", (short)0);
        return method;
    }

    private static AmqpMethod NewMethod(string name) => MethodRegistry.TryGetByName(name, out var schema)
        ? new AmqpMethod(schema!)
        : throw new InvalidOperationException($"Registry is missing {name} (Task 3 guarantee)");

    private static byte[] MethodFrameOn(ushort channel, AmqpMethod method)
    {
        var payloadWriter = new ByteWriter();
        MethodCodec.WriteMethod(ref payloadWriter, method);
        byte[] payload = payloadWriter.ToArray();
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, channel, method.ClassId, method.MethodId, payload.AsSpan(4));
        return frame.ToArray();
    }

    private static byte[] ContentHeaderFrameOn(ushort channel, long body, ContentProperties properties)
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

    private const uint FrameMax = 131072;

    private static async Task ShakeDownAsync(AmqpBrokerServer server, WireClient client)
    {
        await client.DisposeAsync();
        await server.StopAsync();
    }

    /// <summary>
    /// The client half of a TransportPair: sends raw byte frames and decodes the
    /// real bytes the broker produces through the FrameDecoder (ConsumeTests' shape).
    /// </summary>
    private sealed class WireClient(TransportSide side) : IAsyncDisposable
    {
        private byte[] _buffer = new byte[4096];
        private int _length;
        private readonly Queue<AmqpFrame> _inbox = new();

        private readonly List<(ushort Channel, AmqpMethod Deliver, ContentHeader Header, byte[] Body)> _parked = [];

        public async Task SendAsync(byte[] bytes)
        {
            var writer = side.Writer;
            var span = writer.GetSpan(bytes.Length);
            bytes.AsSpan().CopyTo(span);
            writer.Advance(bytes.Length);
            await writer.FlushAsync();
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
                    // RabbitMQ-legal interleave: a deliver claimed earlier by a
                    // consumer's pump can precede the reply this read is waiting for.
                    if (actualClass == 60 && actualMethod == 60 && frame.Channel == channel)
                    {
                        AmqpMethod deliver = Decode(60, 60, ref reader);
                        (ContentHeader parkedHeader, byte[] parkedBody) =
                            await ReadContentOnChannelAsync(channel, timeoutMs);
                        _parked.Add((channel, deliver, parkedHeader, parkedBody));
                        continue;
                    }

                    throw new InvalidOperationException(
                        $"Expected a method frame ({classId},{methodId}) on channel {channel} " +
                        $"but got ({actualClass},{actualMethod})");
                }

                return Decode(actualClass, actualMethod, ref reader);
            }
        }

        public (AmqpMethod, ContentHeader, byte[])? TakeParkedDeliver(ushort channel)
        {
            int index = _parked.FindIndex(parked => parked.Channel == channel);
            if (index < 0)
            {
                return null;
            }

            (ushort _, AmqpMethod deliver, ContentHeader header, byte[] body) = _parked[index];
            _parked.RemoveAt(index);
            return (deliver, header, body);
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
}
