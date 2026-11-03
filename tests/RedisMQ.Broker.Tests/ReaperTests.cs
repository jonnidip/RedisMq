using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 9: x-expires deletes an idle queue (no consumers, no use) and the broker
// server runs one shared reaper loop per process for message and queue expiry.
public sealed class ReaperTests
{
    [Fact]
    public void Idle_queue_with_x_expires_is_deleted()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "ex", "direct"));
        engine.DeclareQueue(new QueueInfo("/", "q", Expiration: TimeSpan.FromMilliseconds(100)));
        engine.Bind("/", "ex", "q", "rk");

        DateTimeOffset lastUsed = engine.GetQueueLastUsed("/", "q")!.Value;
        EngineDelta delta = engine.Reap(lastUsed.AddMilliseconds(200));

        Assert.Null(engine.GetQueue("/", "q"));
        Assert.Equal(GetClaimOutcome.QueueNotFound, engine.TryClaimForGet("/", "q", out _, out _));
        Assert.Empty(engine.RouteTo(engine.GetExchange("/", "ex")!, new ContentProperties(), "rk"));
        Assert.Single(delta.DeletedQueues);
        Assert.Equal(("/", "q"), delta.DeletedQueues[0]);
    }

    [Fact]
    public void Queue_with_a_consumer_is_not_expired()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q", Expiration: TimeSpan.FromMilliseconds(100)));
        Assert.Equal(
            ConsumeRegisterOutcome.Registered,
            engine.RegisterConsumer("/", "q", new TestConsumer("ct")));

        DateTimeOffset lastUsed = engine.GetQueueLastUsed("/", "q")!.Value;
        engine.Reap(lastUsed.AddSeconds(10));

        Assert.NotNull(engine.GetQueue("/", "q"));

        // The idle clock restarts when the last consumer leaves.
        engine.UnregisterConsumer("/", "q", "ct");
        DateTimeOffset idleSince = engine.GetQueueLastUsed("/", "q")!.Value;
        engine.Reap(idleSince.AddMilliseconds(200));

        Assert.Null(engine.GetQueue("/", "q"));
    }

    [Fact]
    public void Queue_expiry_deletes_messages_without_dead_lettering()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo(
            "/", "source", Expiration: TimeSpan.FromMilliseconds(100),
            DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Enqueue("/", "source", new ContentProperties(), [1], "ex", "rk");

        DateTimeOffset lastUsed = engine.GetQueueLastUsed("/", "source")!.Value;
        EngineDelta delta = engine.Reap(lastUsed.AddMilliseconds(200));

        Assert.Null(engine.GetQueue("/", "source"));
        Assert.Empty(engine.PeekQueue("source"));
        Assert.Empty(engine.PeekQueue("target"));
        Assert.Empty(delta.Enqueued); // queue expiry never dead-letters
        Assert.Single(delta.DeletedQueues);
    }

    [Fact]
    public async Task Last_used_is_touched_by_declare_bind_enqueue_get_and_consume()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "ex", "direct"));
        engine.DeclareQueue(new QueueInfo("/", "q"));
        DateTimeOffset previous = engine.GetQueueLastUsed("/", "q")!.Value;

        await Task.Delay(20, TestContext.Current.CancellationToken);
        engine.Bind("/", "ex", "q", "rk");
        DateTimeOffset afterBind = engine.GetQueueLastUsed("/", "q")!.Value;
        Assert.True(afterBind > previous);
        previous = afterBind;

        await Task.Delay(20, TestContext.Current.CancellationToken);
        engine.Enqueue("/", "q", new ContentProperties(), [1], "ex", "rk");
        DateTimeOffset afterEnqueue = engine.GetQueueLastUsed("/", "q")!.Value;
        Assert.True(afterEnqueue > previous);
        previous = afterEnqueue;

        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "q", out _, out _));
        DateTimeOffset afterGet = engine.GetQueueLastUsed("/", "q")!.Value;
        Assert.True(afterGet > previous);
        previous = afterGet;

        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.Equal(
            ConsumeRegisterOutcome.Registered,
            engine.RegisterConsumer("/", "q", new TestConsumer("ct")));
        DateTimeOffset afterConsume = engine.GetQueueLastUsed("/", "q")!.Value;
        Assert.True(afterConsume > previous);
    }

    [Fact]
    public async Task Server_reaper_loop_expires_a_message()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) =
            SpawnServer(TimeSpan.FromMilliseconds(50));
        try
        {
            await OpenChannelAsync(client);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "q"), (50, 11));

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "q")));
            await client.SendAsync(ContentHeaderFrameOn(1, body: 1, new ContentProperties(Expiration: "50")));
            await client.SendAsync(BodyFrameOn(1, [7]));
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "q", passive: true), (50, 11));

            Assert.Single(engine.PeekQueue("q"));

            await WaitUntilAsync(
                () => engine.PeekQueue("q").Count == 0,
                "the server reaper loop to expire the message");
            Assert.NotNull(engine.GetQueue("/", "q")); // no x-expires: the queue stays
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- harness ----------

    private static BrokerServerOptions NewOptions(TimeSpan reaperInterval) => new()
    {
        Users = [new BrokerUser("guest", "guest")],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
        ReaperInterval = reaperInterval,
    };

    private sealed class TestConsumer(string tag) : IQueueConsumer
    {
        public string ConsumerTag => tag;
        public bool IsExclusive => false;
        public bool TryAdmitForDelivery(InMemoryMessage message) => true;
        public Task DrainDeliveryWritesAsync() => Task.CompletedTask;
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

    private static (AmqpBrokerServer Server, WireClient Client, InMemoryEngine Engine) SpawnServer(
        TimeSpan reaperInterval)
    {
        BrokerServerOptions options = NewOptions(reaperInterval);
        var engine = new InMemoryEngine(options.Vhosts);
        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(options, engine, new TransportPairConnectionListener(pair));
        server.StartAsync();
        return (server, new WireClient(pair.TransportClient), engine);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            try
            {
                await Task.Delay(25, cts.Token);
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"Timed out waiting for {description}");
            }
        }
    }

    private static async Task OpenChannelAsync(WireClient client)
    {
        await HandshakeAsync(client);
        _ = await client.ReadMethodAsync(10, 41);
        await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
        _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
    }

    private static async Task DeclareAsync(
        WireClient client, ushort channel, AmqpMethod method, (ushort Class, ushort Method) okId)
    {
        await client.SendAsync(MethodFrameOn(channel, method));
        _ = await client.ReadMethodOnChannelAsync(channel, okId.Class, okId.Method);
    }

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

    private static AmqpMethod QueueDeclareFrame(
        ushort channel,
        string queue,
        bool durable = false,
        bool passive = false,
        FieldTable? arguments = null)
    {
        AmqpMethod method = NewMethod("queue.declare");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("passive", passive);
        method.Set("durable", durable);
        method.Set("exclusive", false);
        method.Set("auto-delete", false);
        method.Set("nowait", false);
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
        FrameWriter.WriteBody(ref frame, channel, payload, 131072);
        return frame.ToArray();
    }

    private static async Task ShakeDownAsync(AmqpBrokerServer server, WireClient client)
    {
        await client.DisposeAsync();
        await server.StopAsync();
    }

    /// <summary>The client half of a TransportPair: raw method frames out, decoded
    /// frames back.</summary>
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
