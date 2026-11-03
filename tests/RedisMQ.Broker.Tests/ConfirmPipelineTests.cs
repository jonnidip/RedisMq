using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 6: publisher confirms become honest and ordered. In confirm mode the
// channel hands one PendingConfirm per publish (routed, unroutable, mandatory) to
// the connection's single confirm loop; the loop awaits every durability receipt
// before it emits confirm.ack/nack, and a full confirm queue blocks the frame pump
// instead of inventing an answer. The harness is the per-file raw-client copy the
// M0 tests use.
public sealed class ConfirmPipelineTests
{
    [Fact]
    public async Task Ack_waits_for_the_receipt()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await OpenConfirmChannelAsync(client);
            await DeclareQueueAsync(client, "q");

            var receipt = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            engine.EnqueueOverride = (_, _, _, _, _, _) =>
                new EnqueueReceipt(1, new ValueTask<bool>(receipt.Task), []);

            await PublishAsync(client, 1, "", "q");

            await Assert.ThrowsAsync<TimeoutException>(
                () => client.ReadMethodOnChannelAsync(1, 0, 0, timeoutMs: 200));

            receipt.SetResult(true);

            AmqpMethod ack = await client.ReadMethodOnChannelAsync(1, 85, 50);
            Assert.Equal(1L, ack.Get("delivery-tag"));
            Assert.False(ack.Get("multiple") is true);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Failed_receipt_nacks()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await OpenConfirmChannelAsync(client);
            await DeclareQueueAsync(client, "q");

            engine.EnqueueOverride = (_, _, _, _, _, _) =>
                new EnqueueReceipt(1, new ValueTask<bool>(Task.FromResult(false)), []);

            await PublishAsync(client, 1, "", "q");

            AmqpMethod nack = await client.ReadMethodOnChannelAsync(1, 85, 51);
            Assert.Equal(1L, nack.Get("delivery-tag"));
            Assert.False(nack.Get("multiple") is true);
            Assert.False(nack.Get("requeue") is true);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Receipts_complete_out_of_order_but_acks_stay_in_sequence()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await OpenConfirmChannelAsync(client);
            await DeclareQueueAsync(client, "q");

            ReceiptLog receipts = QueueReceipts(engine);

            await PublishAsync(client, 1, "", "q", body: "one");
            await PublishAsync(client, 1, "", "q", body: "two");
            await receipts.WaitForAsync(2);

            // The second receipt lands first; the first publish's confirm still owns
            // the head of the ordered stream.
            receipts[1].SetResult(true);
            await Assert.ThrowsAsync<TimeoutException>(
                () => client.ReadMethodOnChannelAsync(1, 0, 0, timeoutMs: 200));

            receipts[0].SetResult(true);

            AmqpMethod first = await client.ReadMethodOnChannelAsync(1, 85, 50);
            AmqpMethod second = await client.ReadMethodOnChannelAsync(1, 85, 50);
            Assert.Equal(1L, first.Get("delivery-tag"));
            Assert.Equal(2L, second.Get("delivery-tag"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Unroutable_publish_occupies_its_sequence_slot()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await OpenConfirmChannelAsync(client);
            await DeclareQueueAsync(client, "q");

            ReceiptLog receipts = QueueReceipts(engine);

            await PublishAsync(client, 1, "", "q", body: "routed");
            await PublishAsync(client, 1, "", "missing", body: "unroutable");

            // The unroutable publish has nothing to persist, but sequence 2 must still
            // wait behind sequence 1's outstanding receipt.
            await Assert.ThrowsAsync<TimeoutException>(
                () => client.ReadMethodOnChannelAsync(1, 0, 0, timeoutMs: 200));

            await receipts.WaitForAsync(1);
            receipts[0].SetResult(true);

            AmqpMethod first = await client.ReadMethodOnChannelAsync(1, 85, 50);
            AmqpMethod second = await client.ReadMethodOnChannelAsync(1, 85, 50);
            Assert.Equal(1L, first.Get("delivery-tag"));
            Assert.Equal(2L, second.Get("delivery-tag"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Mandatory_return_precedes_its_ack()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await OpenConfirmChannelAsync(client);

            await PublishAsync(client, 1, "", "missing", mandatory: true, body: "returned");

            AmqpMethod returned = await client.ReadMethodOnChannelAsync(1, 60, 50);
            Assert.Equal((short)AmqpConstants.NoRoute, returned.Get("reply-code"));
            Assert.Equal(string.Empty, returned.Get("exchange"));
            Assert.Equal("missing", returned.Get("routing-key"));

            (ContentHeader header, byte[] body) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal("returned", Encoding.UTF8.GetString(body));
            Assert.Equal(60, header.ClassId);

            AmqpMethod ack = await client.ReadMethodOnChannelAsync(1, 85, 50);
            Assert.Equal(1L, ack.Get("delivery-tag"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Retired_channel_discards_a_late_confirm()
    {
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer();
        try
        {
            await OpenConfirmChannelAsync(client);
            await DeclareQueueAsync(client, "q");

            ReceiptLog receipts = QueueReceipts(engine);
            await PublishAsync(client, 1, "", "q");
            await receipts.WaitForAsync(1);

            await client.SendAsync(MethodFrameOn(1, ClientChannelCloseFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 41);

            // Reopening the number is the teardown fence: close-ok is flushed before the
            // server finishes retiring the number, so the fresh context proves the
            // original confirm entry is stale before its receipt resolves.
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            receipts[0].SetResult(true);

            // Nothing may leave the wire for the retired channel number.
            await Assert.ThrowsAsync<TimeoutException>(
                () => client.ReadMethodOnChannelAsync(1, 0, 0, timeoutMs: 200));

            // ...while the connection keeps serving the reopened channel.
            await DeclareQueueAsync(client, "q");
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Full_confirm_queue_blocks_the_pump()
    {
        BrokerServerOptions options = NewOptions();
        options.ConfirmQueueCapacity = 1;
        (AmqpBrokerServer server, WireClient client, RecordingEngine engine) = SpawnServer(options);
        try
        {
            await OpenConfirmChannelAsync(client);
            await DeclareQueueAsync(client, "q");

            ReceiptLog receipts = QueueReceipts(engine);

            await PublishAsync(client, 1, "", "q", body: "one");
            await PublishAsync(client, 1, "", "q", body: "two");
            await receipts.WaitForAsync(2);

            // The second confirm cannot enter the capacity-1 queue until the first
            // publish's receipt resolves, so the pump never reaches queue.declare.
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", passive: true)));
            await Assert.ThrowsAsync<TimeoutException>(
                () => client.ReadMethodOnChannelAsync(1, 0, 0, timeoutMs: 200));

            receipts[1].SetResult(true);
            receipts[0].SetResult(true);

            AmqpMethod ackOne = await client.ReadMethodOnChannelAsync(1, 0, 0);
            Assert.Equal(85, ackOne.ClassId);
            Assert.Equal(50, ackOne.MethodId);
            Assert.Equal(1L, ackOne.Get("delivery-tag"));

            // The unblocked pump continues with publish 2's confirm and the queued
            // declare; their relative order is a scheduling race, their presence is not.
            AmqpMethod second = await client.ReadMethodOnChannelAsync(1, 0, 0);
            AmqpMethod third = await client.ReadMethodOnChannelAsync(1, 0, 0);
            AmqpMethod[] tail = [second, third];
            Assert.Contains(tail, method =>
                method.ClassId == 85 && method.MethodId == 50 && (long)method.Get("delivery-tag")! == 2L);
            Assert.Contains(tail, method => method.ClassId == 50 && method.MethodId == 11);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- harness (the per-file M0 pattern mirrors SettleMirroringTests) ----------

    private static BrokerServerOptions NewOptions() => new()
    {
        Users = [new BrokerUser("guest", "guest")],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

    private static ReceiptLog QueueReceipts(RecordingEngine engine)
    {
        var receipts = new ReceiptLog();
        ulong nextId = 0;
        engine.EnqueueOverride = (_, _, _, _, _, _) =>
        {
            var receipt = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            receipts.Add(receipt);
            return new EnqueueReceipt(++nextId, new ValueTask<bool>(receipt.Task), []);
        };

        return receipts;
    }

    /// <summary>The publish-side receipts in enqueue order; the server pump adds them
    /// from its own thread, so access is locked and awaited by count.</summary>
    private sealed class ReceiptLog
    {
        private readonly List<TaskCompletionSource<bool>> _receipts = [];
        private readonly object _gate = new();

        public void Add(TaskCompletionSource<bool> receipt)
        {
            lock (_gate)
            {
                _receipts.Add(receipt);
            }
        }

        public TaskCompletionSource<bool> this[int index]
        {
            get
            {
                lock (_gate)
                {
                    return _receipts[index];
                }
            }
        }

        public async Task WaitForAsync(int count)
        {
            DateTime deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                lock (_gate)
                {
                    if (_receipts.Count >= count)
                    {
                        return;
                    }
                }

                await Task.Delay(10);
            }

            throw new TimeoutException($"Timed out waiting for {count} enqueue receipt(s)");
        }
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

    private static (AmqpBrokerServer Server, WireClient Client, RecordingEngine Engine) SpawnServer(
        BrokerServerOptions? options = null)
    {
        options ??= NewOptions();
        var engine = new RecordingEngine(options.Vhosts);
        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(options, engine, new TransportPairConnectionListener(pair));
        server.StartAsync();
        return (server, new WireClient(pair.TransportClient), engine);
    }

    private static async Task OpenConfirmChannelAsync(WireClient client)
    {
        await HandshakeAsync(client);
        _ = await client.ReadMethodAsync(10, 41);
        await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
        _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

        await client.SendAsync(MethodFrameOn(1, ConfirmSelectFrame()));
        _ = await client.ReadMethodOnChannelAsync(1, 85, 11);
    }

    private static async Task DeclareQueueAsync(WireClient client, string queue)
    {
        await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, queue)));
        _ = await client.ReadMethodOnChannelAsync(1, 50, 11);
    }

    private static async Task PublishAsync(
        WireClient client,
        ushort channel,
        string exchange,
        string routingKey,
        bool mandatory = false,
        string body = "m")
    {
        await client.SendAsync(MethodFrameOn(channel, PublishFrame(channel, exchange, routingKey, mandatory)));
        await client.SendAsync(ContentHeaderFrameOn(
            channel, body: Encoding.UTF8.GetByteCount(body), properties: new ContentProperties(ContentType: "text/plain")));
        await client.SendAsync(BodyFrameOn(channel, Encoding.UTF8.GetBytes(body)));
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

    private static AmqpMethod ConfirmSelectFrame(bool nowait = false)
    {
        AmqpMethod method = NewMethod("confirm.select");
        method.Set("nowait", nowait);
        return method;
    }

    private static AmqpMethod QueueDeclareFrame(
        ushort channel,
        string queue,
        bool passive = false)
    {
        AmqpMethod method = NewMethod("queue.declare");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("passive", passive);
        method.Set("durable", false);
        method.Set("exclusive", false);
        method.Set("auto-delete", false);
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
    /// real bytes the broker produces through the FrameDecoder (SettleMirroringTests' shape).
    /// </summary>
    private sealed class WireClient(TransportSide side) : IAsyncDisposable
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
}
