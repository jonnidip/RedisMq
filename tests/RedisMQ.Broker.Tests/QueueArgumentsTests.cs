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

// M4 Task 7: queue.declare grows the v1 argument set (x-max-length, x-expires,
// x-dead-letter-exchange, x-dead-letter-routing-key, x-delivery-limit), per-message
// expiration becomes relative to enqueue time, and claims increment DeliveryCount.
public sealed class QueueArgumentsTests
{
    [Fact]
    public async Task Declare_accepts_all_v1_queue_arguments()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await OpenChannelAsync(client);

            var arguments = new FieldTable([
                new("x-max-length", FieldValue.From(10L)),
                new("x-expires", FieldValue.From(30_000L)),
                new("x-dead-letter-exchange", FieldValue.From("dlx")),
                new("x-dead-letter-routing-key", FieldValue.From("dead")),
                new("x-delivery-limit", FieldValue.From(3L)),
            ]);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", arguments)));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);

            QueueInfo stored = engine.GetQueue("/", "q")!;
            Assert.Equal(10, stored.MaxLength);
            Assert.Equal(TimeSpan.FromSeconds(30), stored.Expiration);
            Assert.Equal("dlx", stored.DeadLetterExchange);
            Assert.Equal("dead", stored.DeadLetterRoutingKey);
            Assert.Equal(3, stored.DeliveryLimit);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Unknown_queue_argument_still_returns_not_implemented()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await OpenChannelAsync(client);

            var arguments = new FieldTable([new("x-message-ttl", FieldValue.From(1_000L))]);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", arguments)));

            (ushort code, string text, ushort classId, ushort methodId) = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, code);
            Assert.Contains("arguments are not supported", text);
            Assert.Equal((ushort)50, classId);
            Assert.Equal((ushort)10, methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Negative_or_non_integral_argument_returns_precondition_failed()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await OpenChannelAsync(client);

            (string Arg, FieldValue Value)[] cases =
            [
                ("x-max-length", FieldValue.From(-1L)),
                ("x-expires", FieldValue.From(-1L)),
                ("x-delivery-limit", FieldValue.From(-1L)),
                ("x-max-length", FieldValue.From("many")),
            ];

            foreach ((string arg, FieldValue value) in cases)
            {
                var arguments = new FieldTable([new(arg, value)]);
                await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", arguments)));

                (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
                Assert.Equal(AmqpConstants.PreconditionFailed, code);
                Assert.Contains($"invalid arg '{arg}'", text);

                // The soft close retires the channel number; reopen for the next case.
                await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
                await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
                _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            }
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Redeclare_with_a_different_dlx_arg_is_inequivalent()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await OpenChannelAsync(client);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(
                1, "q", new FieldTable([new("x-dead-letter-exchange", FieldValue.From("dlx-a"))]))));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(
                1, "q", new FieldTable([new("x-dead-letter-exchange", FieldValue.From("dlx-b"))]))));

            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.PreconditionFailed, code);
            Assert.Contains("inequivalent arg 'x-dead-letter-exchange'", text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Empty_dlx_name_is_the_default_exchange_not_no_dlx()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await OpenChannelAsync(client);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(
                1, "q", new FieldTable([new("x-dead-letter-exchange", FieldValue.From(""))]))));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);

            // Explicit "" means the default exchange; only an absent argument means no DLX.
            Assert.Equal(string.Empty, engine.GetQueue("/", "q")!.DeadLetterExchange);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public void Expiration_is_relative_to_enqueue_time()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));

        engine.Enqueue("/", "q", new ContentProperties { Expiration = "1000" }, [1], "", "q");

        InMemoryMessage message = Assert.Single(engine.PeekQueue("q"));
        Assert.NotNull(message.Expiration);
        double delta = (message.Expiration!.Value - DateTimeOffset.UtcNow.AddMilliseconds(1000)).TotalMilliseconds;
        Assert.InRange(delta, -100, 100);
    }

    [Fact]
    public void Zero_expiration_expires_immediately()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));

        engine.Enqueue("/", "q", new ContentProperties { Expiration = "0" }, [1], "", "q");
        engine.Reap(DateTimeOffset.UtcNow);

        Assert.Equal(GetClaimOutcome.Empty, engine.TryClaimForGet("/", "q", out _, out _));
    }

    [Fact]
    public void Negative_or_unparseable_expiration_leaves_no_deadline()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));

        engine.Enqueue("/", "q", new ContentProperties { Expiration = "-5" }, [1], "", "q");
        engine.Enqueue("/", "q", new ContentProperties { Expiration = "soon" }, [2], "", "q");

        IReadOnlyList<InMemoryMessage> messages = engine.PeekQueue("q");
        Assert.Equal(2, messages.Count);
        Assert.All(messages, message => Assert.Null(message.Expiration));
    }

    [Fact]
    public void Requeue_keeps_the_absolute_deadline()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Enqueue("/", "q", new ContentProperties { Expiration = "60000" }, [1], "", "q");

        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "q", out InMemoryMessage? claimed, out _));
        DateTimeOffset deadline = claimed!.Expiration!.Value;

        engine.Requeue("/", "q", claimed);

        InMemoryMessage requeued = Assert.Single(engine.PeekQueue("q"));
        Assert.Equal(deadline, requeued.Expiration);
        Assert.True(requeued.Redelivered);
    }

    [Fact]
    public void Delivery_count_increments_on_claim_and_requeue()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");

        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "q", out InMemoryMessage? first, out _));
        Assert.Equal(1, first!.DeliveryCount);

        engine.Requeue("/", "q", first);
        InMemoryMessage requeued = Assert.Single(engine.PeekQueue("q"));
        Assert.Equal(1, requeued.DeliveryCount);

        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "q", out InMemoryMessage? second, out _));
        Assert.Equal(2, second!.DeliveryCount);
    }

    // ---------- harness ----------

    private static BrokerServerOptions NewOptions() => new()
    {
        Users = [BrokerUser.FullAccess("guest", "guest", "/")],
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

    private static (AmqpBrokerServer Server, WireClient Client, InMemoryEngine Engine) SpawnServer()
    {
        BrokerServerOptions options = NewOptions();
        var engine = new InMemoryEngine(options.Vhosts);
        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(options, engine, new TransportPairConnectionListener(pair));
        server.StartAsync();
        return (server, new WireClient(pair.TransportClient), engine);
    }

    private static async Task OpenChannelAsync(WireClient client)
    {
        await HandshakeAsync(client);
        _ = await client.ReadMethodAsync(10, 41);
        await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
        _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
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

    private static AmqpMethod ChannelCloseOkFrame() => NewMethod("channel.close-ok");

    private static AmqpMethod QueueDeclareFrame(
        ushort channel,
        string queue,
        FieldTable? arguments = null,
        bool durable = false,
        bool passive = false)
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

    private static async Task<(ushort Code, string Text)> ReadChannelCloseAsync(WireClient client, ushort channel)
    {
        AmqpMethod close = await client.ReadMethodOnChannelAsync(channel, 20, 40);
        return ((ushort)(short)close.Get("reply-code")!, (string)close.Get("reply-text")!);
    }

    private static async Task<(ushort Code, string Text, ushort ClassId, ushort MethodId)> ReadConnectionCloseAsync(
        WireClient client)
    {
        AmqpMethod close = await client.ReadMethodAsync(10, 50);
        return (
            (ushort)(short)close.Get("reply-code")!,
            (string)close.Get("reply-text")!,
            (ushort)(short)close.Get("class-id")!,
            (ushort)(short)close.Get("method-id")!);
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
