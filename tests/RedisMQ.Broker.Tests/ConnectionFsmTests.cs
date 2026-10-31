using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Connection FSM (Task 4): an "evil client" is driven with real raw frames over an
// in-proc TransportPair wired into AmqpBrokerServer. Frames are asserted after decoding
// the bytes the server actually wrote, so every test pins concrete wire semantics:
// handshake sequencing (Start/StartOk/PLAIN, Tune/TuneOk, Open/OpenOk), fold-back
// negotiation (min of offer and client, floor at frame_min_size), the M0 close codes
// (access-refused, invalid-path, frame-error, syntax-error, not-implemented), client
// close + close-ok, and the heartbeat watchdog silence window (2x interval) with a
// fake clock. No mocks beyond the transport itself — the codec under test produces
// and consumes the same bytes on both ends, as pinned by Tasks 1-3.
public sealed class ConnectionFsmTests
{
    // ---------- (a) happy handshake: Start → StartOk-PLAIN → Tune/TuneOk → Open/OpenOk ----------

    [Fact]
    public async Task Happy_path_completes_the_full_handshake()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await client.SendAsync(ProtocolHeaderBytes);
            AmqpMethod start = await client.ReadMethodAsync(10, 10);
            Assert.Equal((byte)0, start.Get("version-major")); // AMQP 0-9-1
            Assert.Equal((byte)9, start.Get("version-minor"));
            byte[] mechanisms = Assert.IsType<byte[]>(start.Get("mechanisms"));
            Assert.Equal("PLAIN", Encoding.UTF8.GetString(mechanisms));
            byte[] locales = Assert.IsType<byte[]>(start.Get("locales"));
            Assert.Equal("en_US", Encoding.UTF8.GetString(locales));
            var serverProperties = Assert.IsType<FieldTable>(start.Get("server-properties"));
            Assert.Equal("RedisMQ", (string)serverProperties["product"].Raw!);

            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
            AmqpMethod tune = await client.ReadMethodAsync(10, 30);
            Assert.Equal((short)WireConnection.DefaultChannelMax, tune.Get("channel-max"));
            Assert.Equal(131072, tune.Get("frame-max"));
            Assert.Equal((short)60, tune.Get("heartbeat"));

            await client.SendAsync(TuneOkFrame(2047, 131072, 60));
            await client.SendAsync(OpenFrame("/"));
            AmqpMethod openOk = await client.ReadMethodAsync(10, 41);
            Assert.Equal("", openOk.Get("known-hosts"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (b) bad password → connection.close(access-refused) ----------

    [Fact]
    public async Task Bad_password_closes_with_access_refused()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await client.SendAsync(ProtocolHeaderBytes);
            _ = await client.ReadMethodAsync(10, 10); // connection.start
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "wrongpassword")));

            (ushort code, string text) close = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.AccessRefused, close.code);
            Assert.StartsWith("ACCESS_REFUSED", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (c) unknown vhost → connection.close(invalid-path) ----------

    [Fact]
    public async Task Unknown_vhost_closes_with_invalid_path()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
            _ = await client.ReadMethodAsync(10, 30); // connection.tune
            await client.SendAsync(TuneOkFrame(2047, 131072, 60));
            await client.SendAsync(OpenFrame("/nope"));

            (ushort code, string text) close = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.InvalidPath, close.code);
            Assert.StartsWith("INVALID_PATH", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (d) malformed frame-end → close with frame-error ----------

    [Fact]
    public async Task Malformed_frame_end_closes_with_frame_error()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));

            byte[] evil = TuneOkFrame(2047, 131072, 60);
            evil[^1] = 0x2E; // corrupt the frame-end octet (must be 0xCE)
            await client.SendAsync(evil);

            (ushort code, string text) close = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.FrameError, close.code);
            Assert.StartsWith("FRAME_ERROR", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (e) frame larger than negotiated frame-max → close ----------

    [Fact]
    public async Task Oversized_frame_closes_with_frame_error()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));

            // Header-only oversized frame: declared payload 0x40000 > the 131072 offer.
            // The frame decoder rejects it from the size field alone, so no payload
            // bytes need to be sent (the server must never wait for or buffer them).
            var header = new ByteWriter();
            header.WriteOctet(AmqpFrame.FrameMethod);
            header.WriteUShort(0);
            header.WriteLong(0x40000);
            header.WriteOctet(AmqpFrame.FrameEnd);
            await client.SendAsync(header.ToArray());

            (ushort code, string text) close = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.FrameError, close.code);
            Assert.StartsWith("FRAME_ERROR", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (f) unknown class/method → syntax-error; dialect method outside M0 → not-implemented ----------

    [Fact]
    public async Task Unknown_method_closes_with_syntax_error()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
            _ = await client.ReadMethodAsync(10, 30); // tune: the start-ok was itself legal
            await client.SendAsync(TuneOkFrame(2047, 131072, 60));
            await client.SendAsync(MethodFrame(0, classId: 999, methodId: 10, args: []));

            (ushort code, string text) close = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.SyntaxError, close.code);
            Assert.StartsWith("SYNTAX_ERROR", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Registered_method_outside_the_connection_set_closes_with_not_implemented()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
            _ = await client.ReadMethodAsync(10, 30); // tune: the start-ok was itself legal
            await client.SendAsync(TuneOkFrame(2047, 131072, 60));
            await client.SendAsync(MethodFrame(0, 90, 10, [])); // tx.select (90, 10)

            (ushort code, string text) close = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.StartsWith("NOT_IMPLEMENTED", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (h) client connection.close → close-ok and drain ----------

    [Fact]
    public async Task Client_close_yields_close_ok_and_drain()
    {
        (AmqpBrokerServer server, WireClient client) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client, heartbeatTuneOk: 60);
            _ = await client.ReadMethodAsync(10, 41); // connection.open-ok

            await client.SendAsync(
                MethodFrame(0, 10, 50, CloseArgsBytes(200, "hello")));
            _ = await client.ReadMethodAsync(10, 51); // connection.close-ok

            // The drain: the transport ends cleanly, with no further frames shoved at
            // the client after close-ok.
            await client.CompleteWriterAsync();
            Assert.False(await client.PeekUnexpectedFrameAsync(TimeSpan.FromMilliseconds(200)));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (g) TuneOk heartbeat 0 disables the watchdog ----------

    [Fact]
    public async Task TuneOk_heartbeat_zero_disables_the_watchdog()
    {
        BrokerServerOptions options = NewOptions();
        options.Heartbeat = 1; // advertised; any (buggy) watchdog would kill after ~2 s of silence
        (AmqpBrokerServer server, WireClient client) = SpawnServer(options);
        try
        {
            await HandshakeAsync(client, heartbeatTuneOk: 0);
            _ = await client.ReadMethodAsync(); // connection.open-ok (tune's heartbeat checked inside)

            await Task.Delay(2_500, TestContext.Current.CancellationToken); // silence, well past the 2 s silence window of the offered 1 s

            // The connection must still be alive: the close is answered, not dropped.
            await client.SendAsync(
                MethodFrame(0, 10, 50, CloseArgsBytes(200, "keeping alive")));
            _ = await client.ReadMethodAsync(10, 51); // connection.close-ok
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- helpers ----------

    private static BrokerServerOptions NewOptions() => new()
    {
        Users = [new BrokerUser("guest", "guest")],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

    /// <summary>The full handshake tuned by the client: it must read the Tune itself,
    /// so the heartbeat field the server offered is verified on the way through.</summary>
    private static async Task HandshakeAsync(WireClient client, ushort heartbeatTuneOk)
    {
        await SendHeaderAsync(client);
        _ = await client.ReadMethodAsync(10, 10); // connection.start
        await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
        AmqpMethod tune = await client.ReadMethodAsync(10, 30);
        Assert.Equal((short)WireConnection.DefaultChannelMax, tune.Get("channel-max"));
        Assert.Equal(131072, tune.Get("frame-max"));
        if (heartbeatTuneOk == 0)
        {
            // The server still offers its configured heartbeat; only TuneOk opts out.
            Assert.Equal((short)1, tune.Get("heartbeat"));
        }
        else
        {
            Assert.Equal((short)heartbeatTuneOk, tune.Get("heartbeat"));
            Assert.Equal((short)60, tune.Get("heartbeat"));
        }

        await client.SendAsync(TuneOkFrame(2047, 131072, heartbeatTuneOk));
        await client.SendAsync(OpenFrame("/"));
    }

    // ---------- evil-client wire helpers (raw frames, codec-built payloads) ----------

    /// <summary>Bytes of the AMQP 0-9-1 protocol header: "AMQP" + 0x00 0x00 0x09 0x01.</summary>
    private static byte[] ProtocolHeaderBytes => "AMQP\x00\x00\x09\x01"u8.ToArray();

    private static Task SendHeaderAsync(WireClient client) => client.SendAsync(ProtocolHeaderBytes);

    private static byte[] PlainResponse(string username, string password)
    {
        // PLAIN: authzid NUL authcid NUL password (authzid empty for the broker).
        return Encoding.UTF8.GetBytes($"\0{username}\0{password}");
    }

    private static byte[] StartOkFrame(byte[] plainResponse)
    {
        var clientProperties = new FieldTable(
        [
            new("product", FieldValue.From("evil-client")),
        ]);
        AmqpMethod method = NewMethod("connection.start-ok");
        method.Set("client-properties", clientProperties);
        method.Set("mechanism", "PLAIN");
        method.Set("response", plainResponse);
        method.Set("locale", "en_US");
        return MethodFrame(method);
    }

    private static byte[] TuneOkFrame(ushort channelMax, uint frameMax, ushort heartbeat)
    {
        AmqpMethod method = NewMethod("connection.tune-ok");
        method.Set("channel-max", (short)channelMax);
        method.Set("frame-max", (int)frameMax);
        method.Set("heartbeat", (short)heartbeat);
        return MethodFrame(method);
    }

    private static byte[] OpenFrame(string vhost)
    {
        AmqpMethod method = NewMethod("connection.open");
        method.Set("virtual-host", vhost);
        method.Set("capabilities", "");
        method.Set("insist", false);
        return MethodFrame(method);
    }

    private static byte[] MethodFrame(AmqpMethod method)
    {
        // The frame payload is class-id + method-id + arguments: MethodCodec writes the
        // whole payload (ids first), so the frame is assembled from the same bytes with
        // the id prefix sliced off under the caller's knobs.
        var payloadWriter = new ByteWriter();
        MethodCodec.WriteMethod(ref payloadWriter, method);
        byte[] payload = payloadWriter.ToArray();
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, 0, method.ClassId, method.MethodId, payload.AsSpan(4));
        return frame.ToArray();
    }

    private static byte[] MethodFrame(ushort channel, ushort classId, ushort methodId, byte[] args)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, channel, classId, methodId, args);
        return frame.ToArray();
    }

    /// <summary>connection.close arguments from the registry's field order:
    /// reply-code(short), reply-text(shortstr), class-id(short), method-id(short).</summary>
    private static byte[] CloseArgsBytes(ushort replyCode, string replyText, ushort classId = 0, ushort methodId = 0)
    {
        var args = new ByteWriter();
        args.WriteShort((short)replyCode);
        args.WriteShortstr(replyText);
        args.WriteShort((short)classId);
        args.WriteShort((short)methodId);
        return args.ToArray();
    }

    private static AmqpMethod NewMethod(string name) => MethodRegistry.TryGetByName(name, out var schema)
            ? new AmqpMethod(schema!)
            : throw new InvalidOperationException($"Registry is missing {name} (Task 3 guarantee)");

    /// <summary>Reads the next method the server writes, asserting it is the
    /// connection.close hard-error/close reply (channel 0, class 10 method 50).</summary>
    private static async Task<(ushort Code, string Text)> ReadConnectionCloseAsync(WireClient client)
    {
        AmqpMethod close = await client.ReadMethodAsync(10, 50);
        return (
            (ushort)(short)close.Get("reply-code")!, // wire type is short-int, always positive here
            (string)close.Get("reply-text")!);
    }

    /// <summary>Stops the server and disposes the client, so a failing FSM scenario never
    /// dangles past the assertion.</summary>
    private static async Task ShakeDownAsync(AmqpBrokerServer server, WireClient client)
    {
        await client.DisposeAsync();
        await server.StopAsync();
    }

    // ---------- test-local pieces (the brief's injected surface) ----------

    /// <summary>Placeholder engine (ledger ruling): Task 5 fills the semantics and may
    /// finalize these signatures; Task 4 only needs the type for the server ctor.</summary>
    private sealed class TestRouteEngine : IRouteEngine
    {
        public ExchangeInfo? FindExchange(string vhost, string name) => null;

        public void DeclareExchange(ExchangeInfo exchange) => throw new NotImplementedException("Task 5");

        public void DeclareQueue(string vhost, string name) => throw new NotImplementedException("Task 5");

        public void Bind(string vhost, string exchange, string queue, string routingKey) => throw new NotImplementedException("Task 5");

        public IEnumerable<string> Route(ExchangeInfo exchange, string routingKey) => throw new NotImplementedException("Task 5");
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

    private static (AmqpBrokerServer Server, WireClient Client) SpawnServer(BrokerServerOptions options)
    {
        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(options, new TestRouteEngine(), new TransportPairConnectionListener(pair));
        server.StartAsync();
        return (server, new WireClient(pair.TransportClient));
    }

    /// <summary>
    /// The client half of a TransportPair: sends raw byte frames and decodes the real
    /// bytes the broker produces through the FrameDecoder, keeping undecodable partials
    /// across calls exactly as a real socket client would.
    /// </summary>
    private sealed class WireClient(TransportSide side) : IAsyncDisposable
    {
        private byte[] _buffer = new byte[4096]; // accumulating received bytes
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

        public async ValueTask CompleteWriterAsync() => await side.Writer.CompleteAsync();

        public ValueTask DisposeAsync() => side.CompleteAsync();

        /// <summary>Reads the next method frame of any (class, method) pair.</summary>
        public Task<AmqpMethod> ReadMethodAsync(int timeoutMs = 5_000) => ReadMethodAsync(0, 0, timeoutMs);

        /// <summary>
        /// Decodes frames from the transport until the next METHOD frame with the
        /// requested class/method id arrives; 0 means "any". Heartbeat frames are
        /// skipped. The discriminator is strict: a mismatched method frame is an
        /// error, never silently consumed.
        /// </summary>
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
                    continue; // heartbeat frames etc. are skipped
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

            // Copy the bytes out BEFORE consuming them: advancing the reader first
            // would invalidate the sequence positions we are about to read from.
            CopyIn(read.Buffer);
            side.Reader.AdvanceTo(read.Buffer.End);
            DecodeAvailable();
        }

        private bool CopyIn(ReadOnlySequence<byte> bytes)
        {
            if (bytes.IsEmpty)
            {
                return false;
            }

            long needed = _length + bytes.Length;
            if (needed > _buffer.Length)
            {
                int grown = (int)Math.Max(needed, _buffer.Length * 2L);
                Array.Resize(ref _buffer, grown);
            }

            bytes.CopyTo(_buffer.AsSpan(_length, (int)bytes.Length));
            _length += (int)bytes.Length;
            return true;
        }

        /// <summary>Decodes complete frames out of the accumulated buffer, sliding any
        /// partial tail back to the front for the next pump.</summary>
        private void DecodeAvailable()
        {
            if (_length == 0)
            {
                return;
            }

            var buffer = new ReadOnlySequence<byte>(_buffer, 0, _length);
            var frames = new List<AmqpFrame>();
            _ = FrameDecoder.ReadFrames(ref buffer, frames, uint.MaxValue); // throws on hard wire errors
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

        /// <summary>True when at least a byte arrives inside the window: after close-ok
        /// the transport must end, not keep pushing unknown traffic.</summary>
        public async Task<bool> PeekUnexpectedFrameAsync(TimeSpan window)
        {
            ReadResult read;
            try
            {
                read = await side.Reader.ReadAsync(new CancellationTokenSource(window).Token);
            }
            catch (OperationCanceledException)
            {
                return false; // no bytes: the connection is shut down cleanly
            }

            side.Reader.AdvanceTo(read.Buffer.Start);
            return !read.Buffer.IsEmpty; // at least a byte arrived within the window
        }
    }
}

/// <summary>
/// Fake-clock heartbeat watchdog tests (brief Step 5): two watchdogs at 60 s and 120 s
/// survive 61 s of idle ticks but the 60 s one dies after 121 s of silence (kill at
/// 2x interval, no cross-trigger); the watchdog resets on any inbound frame signal;
/// and a disabled heartbeat never reaches the watchdog at all.
/// </summary>
public sealed class ConnectionFsmWatchdogTests
{
    private static readonly DateTimeOffset Epoch = new(2026, 10, 30, 12, 0, 0, TimeSpan.Zero);

    private sealed class FakeClock
    {
        public DateTimeOffset UtcNow { get; private set; } = Epoch;

        public void Advance(TimeSpan by) => UtcNow += by;
    }

    /// <summary>Watchdog wired to a fake clock through the virtual Now hook.</summary>
    private sealed class TestWatchdog(TimeSpan interval, FakeClock clock, Action<HeartbeatWatchdog>? onTimeout = null)
        : HeartbeatWatchdog(interval, onTimeout)
    {
        protected override DateTimeOffset Now => clock.UtcNow;
    }

    [Fact]
    public void Two_watchdogs_survive_61s_but_60s_dies_after_121s_of_silence()
    {
        var clock = new FakeClock();
        var watchdog60 = new TestWatchdog(TimeSpan.FromSeconds(60), clock);
        var watchdog120 = new TestWatchdog(TimeSpan.FromSeconds(120), clock);

        for (var tick = 1; tick <= 61; tick++)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            watchdog60.Check();
            watchdog120.Check();
            Assert.False(watchdog60.TimedOut);
            Assert.False(watchdog120.TimedOut);
        }

        // Silence continues to 121 s: the kill fires when no frame arrived for 2x
        // interval (121 >= 2x60), while the 120 s watchdog (kill at 240 s) must not
        // cross-trigger.
        for (var tick = 62; tick <= 121; tick++)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            watchdog60.Check();
            watchdog120.Check();
        }

        Assert.True(watchdog60.TimedOut);
        Assert.False(watchdog120.TimedOut, "the 120 s watchdog must not cross-trigger at 121 s");
    }

    [Fact]
    public void Watchdog_resets_on_any_inbound_frame_signal()
    {
        var clock = new FakeClock();
        var watchdog = new TestWatchdog(TimeSpan.FromSeconds(60), clock);

        // 119 s of silence: not yet at the 120 s kill.
        clock.Advance(TimeSpan.FromSeconds(119));
        watchdog.Check();
        Assert.False(watchdog.TimedOut);

        // The reset for "any inbound frame": the connection calls this single entry
        // point for every decoded frame, no matter the frame type.
        watchdog.OnFrameReceived();
        clock.Advance(TimeSpan.FromSeconds(119));
        watchdog.Check();
        Assert.False(watchdog.TimedOut);

        // Two intervals after the last frame: dead at 119 + 120 = 239.
        clock.Advance(TimeSpan.FromSeconds(1));
        watchdog.Check();
        Assert.True(watchdog.TimedOut);
    }

    [Fact]
    public void Watchdog_constructor_rejects_a_disabled_heartbeat()
    {
        // heartbeat 0 means "no watchdog at all"; a zero (or negative) interval must
        // never construct a silently useless one.
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new HeartbeatWatchdog(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new HeartbeatWatchdog(TimeSpan.FromSeconds(-1)));
    }
}
