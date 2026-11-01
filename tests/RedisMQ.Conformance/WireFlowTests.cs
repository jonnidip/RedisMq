using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Protocol;
using Xunit;

namespace RedisMQ.Conformance;

/// <summary>
/// Channel.flow at the wire level over the real TCP listener: raw TcpClient plus the
/// broker's own codec primitives (FrameWriter, MethodCodec, ByteWriter, MethodRegistry,
/// FrameDecoder). channel.flow has no client API in RabbitMQ.Client 7.x, so this is the
/// only way to pin its wire behavior against a real socket.
/// </summary>
public sealed class WireFlowTests
{
    [Fact]
    public async Task Channel_flow_true_answers_flow_ok_and_a_publish_still_delivers()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using SocketWireClient client = await ConnectAsync(host);

        await HandshakeAsync(client);

        // Declare a queue so there's somewhere to consume from.
        await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "flow-q")));
        _ = await client.ReadMethodOnChannelAsync(1, 50, 11); // queue.declare-ok

        // Publish one message through the default exchange.
        byte[] body = "flow-payload"u8.ToArray();
        await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "flow-q")));
        await client.SendAsync(ContentHeaderFrameOn(1, body: body.LongLength, properties: new ContentProperties()));
        await client.SendAsync(BodyFrameOn(1, body));

        // Start a consumer.
        await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "flow-q", consumerTag: "ct-flow")));
        _ = await client.ReadMethodOnChannelAsync(1, 60, 21); // basic.consume-ok

        // channel.flow(true) → channel.flow-ok(active=true), then delivery still flows.
        await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: true)));
        AmqpMethod flowOk = await client.ReadMethodOnChannelAsync(1, 20, 21);
        Assert.Equal(20, flowOk.ClassId);
        Assert.Equal(21, flowOk.MethodId);
        Assert.Equal(true, flowOk.Get("active"));

        (AmqpMethod deliver, ContentHeader _, byte[] receivedBody) = await ReadDeliverAsync(client, 1);
        Assert.Equal("flow-payload", Encoding.UTF8.GetString(receivedBody));
        Assert.Equal("ct-flow", deliver.Get("consumer-tag"));
        Assert.Equal("flow-q", deliver.Get("routing-key"));

        await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
    }

    [Fact]
    public async Task Channel_flow_false_hard_closes_the_connection_with_540()
    {
        await using TestBrokerHost host = TestBrokerHost.Start();
        await using SocketWireClient client = await ConnectAsync(host);

        await HandshakeAsync(client);

        // channel.flow(active=false) → hard connection.close(10, 50), code 540,
        // text NOT_IMPLEMENTED - active=false, class/method 20/20.
        await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: false)));

        AmqpMethod close = await client.ReadMethodAsync(10, 50);
        Assert.Equal(540, (ushort)(short)close.Get("reply-code")!);
        Assert.Equal(
            "NOT_IMPLEMENTED - active=false",
            (string)close.Get("reply-text")!);
        Assert.Equal(20, (ushort)(short)close.Get("class-id")!);
        Assert.Equal(20, (ushort)(short)close.Get("method-id")!);

        // Ack the server's close with close-ok; the server then completes the transport.
        await client.SendAsync(MethodFrameOn(0, ChannelCloseOkFrame()));

        // The TCP connection is now dead: any read hits EOF.
        await Assert.ThrowsAsync<IOException>(async () =>
        {
            byte[] probe = new byte[1];
            int read = await client._stream!.ReadAsync(probe, 0, 1, TestContext.Current.CancellationToken);
            if (read == 0)
                throw new IOException("EOF");
        });
    }

    // ---------- handshake and wire helpers ----------

    private static async Task<SocketWireClient> ConnectAsync(TestBrokerHost host)
    {
        var tcp = new TcpClient();
        await tcp.ConnectAsync("localhost", host.Port);
        return new SocketWireClient(tcp);
    }

    private static async Task HandshakeAsync(SocketWireClient client)
    {
        await client.SendAsync("AMQP\x00\x00\x09\x01"u8.ToArray());
        _ = await client.ReadMethodAsync(10, 10); // connection.start
        await client.SendAsync(StartOkFrame());
        _ = await client.ReadMethodAsync(10, 30); // connection.tune
        await client.SendAsync(TuneOkFrame(2047, 131072, 0)); // heartbeat 0 disables the watchdog
        await client.SendAsync(OpenFrame("/"));
        _ = await client.ReadMethodAsync(10, 41); // connection.open-ok

        await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
        _ = await client.ReadMethodOnChannelAsync(1, 20, 11); // channel.open-ok
    }

    private static byte[] StartOkFrame()
    {
        AmqpMethod method = NewMethod("connection.start-ok");
        method.Set("client-properties", new FieldTable([new("product", FieldValue.From("wire-flow-client"))]));
        method.Set("mechanism", "PLAIN");
        method.Set("response", Encoding.UTF8.GetBytes("\0guest\0guest"));
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

    private static AmqpMethod ChannelFlowFrame(ushort channel, bool active)
    {
        AmqpMethod method = NewMethod("channel.flow");
        method.Set("active", active);
        return method;
    }

    private static AmqpMethod ChannelCloseOkFrame()
    {
        AmqpMethod method = NewMethod("channel.close-ok");
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

    private static AmqpMethod PublishFrame(ushort channel, string exchange, string routingKey)
    {
        AmqpMethod method = NewMethod("basic.publish");
        method.Set("ticket", (short)0);
        method.Set("exchange", exchange);
        method.Set("routing-key", routingKey);
        method.Set("mandatory", false);
        method.Set("immediate", false);
        return method;
    }

    private static AmqpMethod NewMethod(string name) => MethodRegistry.TryGetByName(name, out var schema)
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

    private static byte[] ContentHeaderFrameOn(ushort channel, long body, ContentProperties properties)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteContentHeader(ref frame, channel, classId: 60, bodySize: body, properties);
        return frame.ToArray();
    }

    private static byte[] BodyFrameOn(ushort channel, byte[] payload)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteBody(ref frame, channel, payload, uint.MaxValue);
        return frame.ToArray();
    }

    /// <summary>Reads the delivery triple: the basic.deliver method plus its content
    /// header and body frames.</summary>
    private static async Task<(AmqpMethod Method, ContentHeader Header, byte[] Body)> ReadDeliverAsync(
        SocketWireClient client, ushort channel)
    {
        AmqpMethod method = await client.ReadMethodOnChannelAsync(channel, 60, 60);
        (ContentHeader header, byte[] body) = await client.ReadContentOnChannelAsync(channel);
        return (method, header, body);
    }

    // ---------- raw socket wire client ----------

    private sealed class SocketWireClient : IAsyncDisposable
    {
        private readonly TcpClient _tcp;
        private readonly PipeReader _reader;
        private readonly PipeWriter _writer;
        private byte[] _buffer = new byte[4096];
        private int _length;
        private readonly Queue<AmqpFrame> _inbox = new();

        // Deliveries that overtook a method the test was awaiting; parked in arrival
        // order, complete with content, and served to ReadDeliverAsync later.
        private readonly List<(ushort Channel, AmqpMethod Deliver, ContentHeader Header, byte[] Body)> _parked = [];

        public SocketWireClient(TcpClient tcp)
        {
            _tcp = tcp;
            var stream = tcp.GetStream();
            _reader = PipeReader.Create(stream);
            _writer = PipeWriter.Create(stream);
        }

        internal NetworkStream _stream => _tcp.GetStream();

        public async Task SendAsync(byte[] bytes)
        {
            var span = _writer.GetSpan(bytes.Length);
            bytes.AsSpan().CopyTo(span);
            _writer.Advance(bytes.Length);
            await _writer.FlushAsync();
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await _writer.CompleteAsync();
            }
            catch
            {
                // ignored on teardown
            }
            try
            {
                await _reader.CompleteAsync();
            }
            catch
            {
                // ignored on teardown
            }
            _tcp.Dispose();
        }

        public async Task<AmqpMethod> ReadMethodAsync(ushort classId = 0, ushort methodId = 0, int timeoutMs = 5_000)
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
            AmqpMethod? result = await TryReadMethodOnChannelCoreAsync(channel, classId, methodId, timeoutMs, required: true);
            return result!;
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

        private async Task<AmqpMethod?> TryReadMethodOnChannelCoreAsync(
            ushort channel, ushort classId, ushort methodId, int timeoutMs, bool required)
        {
            using var cts = new CancellationTokenSource(timeoutMs);
            while (true)
            {
                if (_inbox.Count == 0)
                {
                    try
                    {
                        await PumpAsync(cts.Token);
                    }
                    catch (TimeoutException)
                    {
                        if (required)
                        {
                            throw;
                        }

                        return null;
                    }

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
                    // RabbitMQ-legal interleave: a deliver claimed earlier can precede
                    // the reply this read is waiting for. Park the complete triple.
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
                read = await _reader.ReadAsync(ct);
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException("No frame arrived from the broker in time");
            }

            if (read.Buffer.IsEmpty)
            {
                _reader.AdvanceTo(read.Buffer.Start, read.Buffer.End);
                if (read.IsCompleted)
                {
                    throw new InvalidOperationException("The broker closed the transport before answering");
                }

                return;
            }

            CopyIn(read.Buffer);
            _reader.AdvanceTo(read.Buffer.End);
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

            var sequence = new ReadOnlySequence<byte>(_buffer, 0, _length);
            var frames = new List<AmqpFrame>();
            _ = FrameDecoder.ReadFrames(ref sequence, frames, uint.MaxValue);
            int remaining = (int)sequence.Length;
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
