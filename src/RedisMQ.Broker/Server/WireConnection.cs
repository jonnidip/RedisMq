using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Broker.Server;

/// <summary>
/// One AMQP 0-9-1 connection over a transport: the connection-level finite state
/// machine (protocol header → Start → StartOk/PLAIN → Tune → TuneOk → Open → OpenOk),
/// TuneOk negotiation, the M0 hard-error close paths, and the heartbeat watchdog.
///
/// Wire rules this class implements, for real clients and evil ones alike:
/// <list type="bullet">
/// <item>The connection starts by reading the 8-byte protocol header ("AMQP" + 0x00
/// 0x00 0x09 0x01); anything else gets the server's protocol header reply (so clients
/// detect a version mismatch) and a closed transport.</item>
/// <item>StartOk speaks SASL PLAIN only: the longstr response is "authzid NUL authcid
/// NUL password"; anything else closes with an access-refused reply carrying
/// RabbitMQ's wire wording.</item>
/// <item>TuneOk negotiation: frame-max is min(offer, client) with a TuneOk of 0
/// keeping the offer, but a TuneOk below <see cref="AmqpFrame.FrameMinSize"/> is
/// refused with not_allowed (RabbitMQ's rabbit_reader.erl semantics — never clamped
/// up, because a clamp could push the negotiated max above the client's own declared
/// value), channel-max is min(offer, client) but unenforced in M0, and the heartbeat
/// is adopted verbatim — 0 disables it entirely. Like the RabbitMQ server, no second
/// Tune is sent after TuneOk.</item>
/// <item>Open names the vhost; an unknown one closes with invalid-path.</item>
/// <item>Hard wire errors (bad frame-end, oversize frame, unknown frame type, decoder
/// truncation) close the connection with frame-error or syntax-error reply-text
/// carrying the codec's message.</item>
/// <item>An undispatchable method closes the connection: a pair outside the registry
/// with syntax-error, a registry-resolvable method outside the M0 connection set with
/// not-implemented, and a handshake method out of state with unexpected-frame.</item>
/// <item>Every inbound frame of any type resets the heartbeat watchdog, per the
/// plan's heartbeat stewardship rule.</item>
/// <item>Per-batch decode-abort rule: each <c>ReadAsync</c> batch is fully decoded
/// before any of its frames is dispatched, and a hard decode error anywhere in the
/// batch aborts the whole batch — even an otherwise-legal method sharing the batch
/// is never answered. Whether a legal method and an illegal one share a batch is wire
/// scheduling, not protocol, so both outcomes are legal: the close as the first
/// server method (same batch) or tune-then-close (separate batches).</item>
/// <item>The caller's cancellation (server shutdown) ends the run without a close
/// frame — RabbitMQ-parity wire kill: the transport completes and the client sees
/// EOF, no diagnostic bytes leave the socket.</item>
/// </list>
/// <item>Channel-level dispatch lives in <see cref="ChannelContext"/> (Task 5): each
/// non-zero channel carries its own FSM — channel.open promotes the fresh
/// Starting channel to Running, soft errors send channel.close on that channel and
/// the channel mourns (traffic discarded, close-ok repeating, number retired on
/// close-ok), and hard errors close the connection. Before the connection is serving,
/// any frame on a non-zero channel is unexpected-frame with the reader's wording,
/// as is a heartbeat on a non-zero channel at any stage.</item>
/// </summary>
public sealed class WireConnection : IAsyncDisposable
{
    /// <summary>The channel-max the broker offers at connection.tune in M0; channels
    /// are accepted but their count is not enforced (Task 5 territory).</summary>
    public const ushort DefaultChannelMax = 2047;

    private const string ProductVersion = "0.1.0-M0";

    /// <summary>Bytes of the AMQP 0-9-1 protocol header: "AMQP" + 0x00 0x00 0x09 0x01.</summary>
    private static readonly byte[] ProtocolHeaderBytes = "AMQP\x00\x00\x09\x01"u8.ToArray();

    private enum ConnectionState
    {
        AwaitStartOk,
        AwaitTuneOk,
        AwaitOpen,
        Serving,
        ClosingByServer, // this end sent connection.close and waits for close-ok
    }

    private readonly TransportSide _transport;
    private readonly BrokerServerOptions _options;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly CancellationTokenSource _heartbeatCts = new();
    private ITimer? _timer;
    private HeartbeatWatchdog? _watchdog;
    private ConnectionState _state = ConnectionState.AwaitStartOk;
    private uint _frameMax; // the negotiated frame-max; starts at the broker's offer
    private string _vhost = string.Empty; // captured at connection.open for channel dispatch
    private readonly Dictionary<ushort, ChannelContext> _channels = []; // Running and Mourning channels
    private bool _disposed;

    public WireConnection(BrokerServerOptions options, IRouteEngine routes, TransportSide transport)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(transport);

        _options = options;
        _transport = transport;
        _frameMax = options.FrameMax; // pre-TuneOk bound: the broker's own configured max
        // The topology engine backs Task 5's channel dispatch: every channel context
        // hands its declare/bind calls to it.
        Routes = routes;
    }

    /// <summary>The topology engine this connection was built against; channel
    /// declarations and bindings flow into it.</summary>
    public IRouteEngine Routes { get; }

    /// <summary>
    /// Runs the connection's whole lifecycle: protocol header, handshake, frame pump,
    /// the closing-wait, transport completion. Failures never escape: a hard wire
    /// error produces its close reply. The awaiter observes clean shutdown.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        // All pipe operations observe the caller's cancellation, our stop, and the
        // heartbeat silence kill, so any of the three ends the loop deterministically.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _stopCts.Token, _heartbeatCts.Token);
        var ct = linked.Token;
        try
        {
            if (await RunHandshakeAsync(ct))
            {
                await RunFrameLoopAsync(ct);
            }

            // A server-initiated close waits for the client's close-ok (RabbitMQ's
            // closing state); a client-initiated close already sent ours.
            if (_state == ConnectionState.ClosingByServer)
            {
                await WaitForCloseOkAsync(ct);
            }
        }
        catch (OperationCanceledException) when (_heartbeatCts.IsCancellationRequested && _watchdog?.TimedOut == true)
        {
            // Heartbeat silence: RabbitMQ closes the socket without a close frame.
        }
        catch (OperationCanceledException)
        {
            // Stopped externally (server shutdown, transport completion).
        }
        catch (AmqpProtocolException ex)
        {
            // Frame-layer hard error: frame-error close carrying the decoder's message.
            await SendCloseReplyThenCompleteAsync(AmqpConstants.FrameError, "FRAME_ERROR - " + ex.Message, ct);
        }
        catch (AmqpDecodeException ex)
        {
            await SendCloseReplyThenCompleteAsync(AmqpConstants.SyntaxError, "SYNTAX_ERROR - " + ex.Message, ct);
        }
        catch (AmqpEncodeException ex)
        {
            await SendCloseReplyThenCompleteAsync(AmqpConstants.InternalError, "INTERNAL_ERROR - " + ex.Message, ct);
        }
        catch (IOException)
        {
            // Transport faulted mid-wait/write; nothing left to say on it.
        }
        finally
        {
            await DisposeAsync();
        }
    }

    /// <summary>Completes the transport and stops the timers. Concurrent calls are safe.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _timer?.Dispose();
        _stopCts.Cancel();
        _heartbeatCts.Cancel();
        await _transport.CompleteAsync();
    }

    // ---------- handshake ----------

    /// <summary>Validates the protocol header and sends connection.start. Returns false
    /// when the transport closed or the header mismatched (after the version reply).</summary>
    private async Task<bool> RunHandshakeAsync(CancellationToken ct)
    {
        const int headerLength = 8;
        while (true)
        {
            ReadResult read = await _transport.Reader.ReadAsync(ct);
            var buffer = read.Buffer;
            if (buffer.Length < headerLength)
            {
                if (read.IsCompleted)
                {
                    return false; // peer vanished without a full header
                }

                _transport.Reader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            Span<byte> header = stackalloc byte[headerLength];
            buffer.Slice(0, headerLength).CopyTo(header);
            bool matches = header.SequenceEqual(ProtocolHeaderBytes);
            _transport.Reader.AdvanceTo(matches ? buffer.Slice(0, headerLength).End : buffer.End);
            if (!matches)
            {
                // RabbitMQ-style mismatch reply: the protocol header this server speaks.
                // The bytes stay readable on the pair before the transport completes.
                await WriteAsync(ProtocolHeaderBytes, ct);
                return false;
            }

            break;
        }

        await SendMethodAsync(Method(method =>
        {
            method.Set("version-major", (byte)0);
            method.Set("version-minor", (byte)9);
            method.Set("server-properties", ServerProperties());
            method.Set("mechanisms", Encoding.UTF8.GetBytes("PLAIN"));
            // The pinned codegen fixture declares locales longstr, and longstr is a
            // byte[] in the codec, so the "en_US" string goes out as its bytes.
            method.Set("locales", Encoding.UTF8.GetBytes("en_US"));
        }, "connection.start"), ct);
        return true;
    }

    private FieldTable ServerProperties() => new(
    [
        new("product", FieldValue.From("RedisMQ")),
        new("version", FieldValue.From(ProductVersion)),
        new("platform", FieldValue.From($".NET {Environment.Version}")),
        new("capabilities", FieldValue.From(new FieldTable(
        [
            // The truth about the M0 dialect surface.
            new("authentication_failure_close", FieldValue.From(true)),
            new("exchange_exchange_bindings", FieldValue.From(true)),
            new("basic.nack", FieldValue.From(true)),
            new("publisher_confirms", FieldValue.From(true)),
        ]))),
    ]);

    // ---------- frame pump ----------

    /// <summary>The read/pump loop: decode frames, dispatch, restart the watchdog.
    /// Returns when the transport ends, the watchdog kills, or the FSM asked to stop.</summary>
    private async Task RunFrameLoopAsync(CancellationToken ct)
    {
        while (true)
        {
            ReadResult read = await _transport.Reader.ReadAsync(ct);
            var buffer = read.Buffer;
            if (buffer.IsEmpty && read.IsCompleted)
            {
                return; // peer half-closed: drain done, clean end
            }

            var frames = new List<AmqpFrame>();
            FrameDecoder.ReadFrames(ref buffer, frames, _frameMax); // hard errors throw up to RunAsync
            _transport.Reader.AdvanceTo(buffer.Start, buffer.End);

            foreach (var frame in frames)
            {
                _watchdog?.OnFrameReceived(); // any inbound frame resets the watchdog
                if (!await HandleFrameAsync(frame, ct))
                {
                    return;
                }
            }

            if (read.IsCompleted)
            {
                return; // EOF after processing everything buffered
            }
        }
    }

    /// <summary>Dispatches one decoded frame. Returns false to end the connection run.</summary>
    private async Task<bool> HandleFrameAsync(AmqpFrame frame, CancellationToken ct)
    {
        switch (frame.Type)
        {
            case AmqpFrame.FrameHeartbeat:
                // Channel 0 heartbeats are bookkeeping everywhere. A heartbeat on a
                // non-zero channel is an unexpected_frame with the reader's own
                // wording, in any state (rabbit_reader has no clause that accepts one).
                return frame.Channel == 0
                    ? true
                    : await CloseConnectionAsync(
                        AmqpConstants.UnexpectedFrame,
                        UnexpectedFrameText(frame.Type, frame.Payload.Span),
                        0, 0, ct);
            case AmqpFrame.FrameMethod:
                return await HandleMethodFrameAsync(frame, ct);
            case AmqpFrame.FrameHeader:
            case AmqpFrame.FrameBody:
                return await HandleContentFrameAsync(frame, ct);
            default:
                // Unreachable: FrameDecoder rejects unknown frame types earlier.
                return false;
        }
    }

    /// <summary>The reader-wording unexpected-frame text for a pre-serving or
    /// stray frame: "type {N}, {all|first 16} octets = {~tp binary}".</summary>
    private static string UnexpectedFrameText(byte frameType, ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 0)
        {
            return $"UNEXPECTED_FRAME - type {frameType}, all octets = <<>>";
        }

        int shown = Math.Min(payload.Length, 16);
        var snippet = new StringBuilder(shown * 6 + 4);
        snippet.Append("<<");
        for (int index = 0; index < shown; index++)
        {
            if (index > 0)
            {
                snippet.Append(',');
            }

            snippet.Append(payload[index]);
        }

        snippet.Append(">>");
        string phrase = payload.Length <= 16 ? "all octets" : "first 16 octets";
        return $"UNEXPECTED_FRAME - type {frameType}, {phrase} = {snippet}";
    }

    /// <summary>Content frames: before the connection serves, any content frame is
    /// unexpected-frame with the reader's wording; content on the connection channel
    /// stays the accepted-and-ignored M0 ruling; every other non-zero channel runs
    /// its own assembler via <see cref="ChannelContext.OnContent"/> — a stray content
    /// frame or wrong-phase sequence dies with the assembler's wording, a soft
    /// unexpected-frame channel close (see the Task 6 report's D1 divergence note),
    /// and mourning channels discard their traffic.</summary>
    private async Task<bool> HandleContentFrameAsync(AmqpFrame frame, CancellationToken ct)
    {
        if (_state != ConnectionState.Serving)
        {
            return await CloseConnectionAsync(
                AmqpConstants.UnexpectedFrame,
                UnexpectedFrameText(frame.Type, frame.Payload.Span),
                0, 0, ct);
        }

        if (frame.Channel == 0)
        {
            // Content on the connection channel stays the accepted-and-ignored M0
            // soft path (a ruling from the earlier tasks' FSM review).
            return true;
        }

        ChannelContext? known = _channels.TryGetValue(frame.Channel, out ChannelContext? found) ? found : null;
        if (known is not null && known.IsMourning)
        {
            return true; // the mourning catch-all discards content with no reply
        }

        // The number exists on the connection (fresh Starting channels included):
        // the assembler is owned entirely by the context — this is Task 6's content
        // dispatch seam (publish assembly, cap violations, and the routing finishes).
        ChannelContext context = GetOrCreateChannel(frame.Channel);
        return await DispatchChannelOutcomeAsync(frame.Channel, context, context.OnContent(frame), ct);
    }

    // ---------- connection-class method dispatch ----------

    private async Task<bool> HandleMethodFrameAsync(AmqpFrame frame, CancellationToken ct)
    {
        var payload = new ByteReader(frame.Payload.Span);
        ushort classId = payload.ReadUShort();
        ushort methodId = payload.ReadUShort();

        if (frame.Channel != 0)
        {
            if (_state != ConnectionState.Serving)
            {
                // rabbit_reader's catch-all: before the connection serves, any frame
                // on a non-zero channel is unexpected_frame with the reader's wording.
                return await CloseConnectionAsync(
                    AmqpConstants.UnexpectedFrame,
                    UnexpectedFrameText(frame.Type, frame.Payload.Span),
                    0, 0, ct);
            }

            MethodSchema? channelSchema = MethodRegistry.Schema(classId, methodId);
            if (channelSchema is null)
            {
                return await CloseConnectionAsync(
                    AmqpConstants.SyntaxError,
                    $"SYNTAX_ERROR - ({classId},{methodId}) is not a method of the AMQP 0-9-1 dialect",
                    classId, methodId, ct);
            }

            AmqpMethod channelMethod;
            try
            {
                channelMethod = MethodCodec.ReadMethod(ref payload, channelSchema);
            }
            catch (AmqpDecodeException ex)
            {
                return await CloseConnectionAsync(
                    AmqpConstants.SyntaxError,
                    $"SYNTAX_ERROR - malformed method payload for {channelSchema.Name}: {ex.Message}",
                    classId, methodId, ct);
            }

            return await DispatchChannelMethodAsync(frame.Channel, channelMethod, ct);
        }

        MethodSchema? schema = MethodRegistry.Schema(classId, methodId);
        if (schema is null)
        {
            return await CloseConnectionAsync(
                AmqpConstants.SyntaxError,
                $"SYNTAX_ERROR - ({classId},{methodId}) is not a method of the AMQP 0-9-1 dialect",
                classId, methodId, ct);
        }

        AmqpMethod method;
        try
        {
            method = MethodCodec.ReadMethod(ref payload, schema);
        }
        catch (AmqpDecodeException ex)
        {
            return await CloseConnectionAsync(
                AmqpConstants.SyntaxError,
                $"SYNTAX_ERROR - malformed method payload for {schema.Name}: {ex.Message}",
                classId, methodId, ct);
        }

        // Full-dispatch classification first, so a dialect method outside the M0
        // connection set is refused regardless of the current FSM stage.
        if (classId != 10 || !IsConnectionClassMethod(methodId))
        {
            return await CloseConnectionAsync(
                AmqpConstants.NotImplemented,
                $"NOT_IMPLEMENTED - {schema.Name} ({classId},{methodId}) is not part of the M0 connection method set",
                classId, methodId, ct);
        }

        // classId is 10 from here on (the classifier above rejects the rest).
        switch (_state)
        {
            case ConnectionState.AwaitStartOk:
                return methodId switch
                {
                    11 => await OnStartOkAsync(method, ct), // connection.start-ok
                    50 => await OnClientCloseAsync(ct), // connection.close (10, 50)
                    _ => await UnexpectedMethodAsync(schema, ct),
                };

            case ConnectionState.AwaitTuneOk:
                return methodId switch
                {
                    31 => await OnTuneOkAsync(method, schema, ct), // connection.tune-ok
                    50 => await OnClientCloseAsync(ct),
                    _ => await UnexpectedMethodAsync(schema, ct),
                };

            case ConnectionState.AwaitOpen:
                return methodId switch
                {
                    40 => await OnOpenAsync(method, ct), // connection.open
                    50 => await OnClientCloseAsync(ct),
                    _ => await UnexpectedMethodAsync(schema, ct),
                };

            case ConnectionState.Serving:
                return methodId switch
                {
                    50 => await OnClientCloseAsync(ct), // connection.close
                    60 => true, // connection.blocked (10, 60): ledger-ruled no-op in M0
                    61 => true, // connection.unblocked (10, 61): no-op
                    _ => await UnexpectedMethodAsync(schema, ct),
                };

            case ConnectionState.ClosingByServer:
                // We already asked for the close: a close-ok is the clean exit, and
                // anything else ends the connection without further replies.
                return false;
            default:
                return false;
        }
    }

    private static bool IsConnectionClassMethod(ushort methodId) => methodId is
        11 or // start-ok (10, 11)
        31 or // tune-ok (10, 31)
        40 or // open (10, 40)
        50 or // close (10, 50)
        51 or // close-ok (10, 51)
        60 or // blocked (10, 60)
        61;   // unblocked (10, 61)

    private Task<bool> UnexpectedMethodAsync(MethodSchema schema, CancellationToken ct) =>
        CloseConnectionAsync(
            AmqpConstants.UnexpectedFrame,
            $"UNEXPECTED_FRAME - {schema.Name} is not expected in state {_state}",
            schema.ClassId, schema.MethodId, ct);

    // ---------- channel-class method dispatch (Task 5) ----------

    /// <summary>Runs one Serving-state method through the channel's FSM. The syntax /
    /// codec checks were already applied by the caller; the per-channel semantics live
    /// entirely in <see cref="ChannelContext"/>.</summary>
    private Task<bool> DispatchChannelMethodAsync(
        ushort channel,
        AmqpMethod method,
        CancellationToken ct)
    {
        ChannelContext context = GetOrCreateChannel(channel);
        return DispatchChannelOutcomeAsync(channel, context, context.OnMethod(method), ct);
    }

    /// <summary>Carries one channel decision (from a method or a content frame) to the
    /// wire: replies and silences keep the pump alive, soft errors close AND mourn the
    /// channel, hard errors drop the number and close the connection, and a mandatory
    /// unroutable leaves the channel alive after writing the basic.return with its
    /// content back out (rabbit_channel's basic_return/5, re-chunked per frame-max
    /// exactly like the writer sends any other content).</summary>
    private async Task<bool> DispatchChannelOutcomeAsync(
        ushort channel,
        ChannelContext context,
        ChannelOutcome outcome,
        CancellationToken ct)
    {
        switch (outcome)
        {
            case ChannelOutcomeReply reply:
                await SendMethodAsync(reply.Reply, channel, ct);
                if (reply.RetireAfter)
                {
                    _channels.Remove(channel); // the number is retired after its close-ok
                }

                return true;

            case ChannelOutcomeSilent silent:
                if (silent.RetireChannel)
                {
                    _channels.Remove(channel); // channel.close_ok performs channel_cleanup
                }

                return true;

            case ChannelOutcomeSoftError soft:
                context.Mourn(); // the channel.close went out on this channel
                return await SendChannelCloseAsync(channel, soft.Code, soft.Text, soft.ClassId, soft.MethodId, ct);

            case ChannelOutcomeHardError hard:
                _channels.Remove(channel);
                return await CloseConnectionAsync(hard.Code, hard.Text, hard.ClassId, hard.MethodId, ct);

            case ChannelOutcomeBasicReturn returned:
                await SendMethodAsync(returned.ReturnMethod, channel, ct);
                var returnHeader = new ByteWriter();
                FrameWriter.WriteContentHeader(
                    ref returnHeader, channel, classId: 60, returned.Body.LongLength, returned.Properties);
                await WriteAsync(returnHeader.ToArray(), ct);
                var returnBody = new ByteWriter();
                FrameWriter.WriteBody(ref returnBody, channel, returned.Body, _frameMax);
                await WriteAsync(returnBody.ToArray(), ct);
                return true;

            default:
                // Unreachable: every concrete ChannelOutcome shape is handled above.
                return false;
        }
    }

    /// <summary>Returns the channel's FSM, lazily creating the fresh Starting-state
    /// context a never-opened channel number implies (so a stray method on one meets
    /// the "expected 'channel.open'" guard).</summary>
    private ChannelContext GetOrCreateChannel(ushort channel)
    {
        if (_channels.TryGetValue(channel, out ChannelContext? existing))
        {
            return existing;
        }

        var fresh = new ChannelContext(_vhost, Routes);
        _channels[channel] = fresh;
        return fresh;
    }

    /// <summary>Sends the server's channel.close for a soft error; the channel stays
    /// mourning until close-ok arrives (discarding traffic in between).</summary>
    private async Task<bool> SendChannelCloseAsync(
        ushort channel,
        ushort replyCode,
        string replyText,
        ushort classId,
        ushort methodId,
        CancellationToken ct)
    {
        await SendMethodAsync(Method(close =>
        {
            close.Set("reply-code", (short)replyCode);
            close.Set("reply-text", replyText);
            close.Set("class-id", (short)classId);
            close.Set("method-id", (short)methodId);
        }, "channel.close"), channel, ct);
        return true; // the connection lives on; the channel mourns
    }

    // ---------- handshake steps ----------

    private async Task<bool> OnStartOkAsync(AmqpMethod method, CancellationToken ct)
    {
        string mechanism = (string)(method.Get("mechanism") ?? string.Empty);
        if (!string.Equals(mechanism, "PLAIN", StringComparison.Ordinal))
        {
            return await CloseConnectionAsync(
                AmqpConstants.AccessRefused,
                $"ACCESS_REFUSED - PLAIN is the only supported authentication mechanism (got '{mechanism}')",
                10, 11, ct);
        }

        byte[] response = Cast<byte[]>(method.Get("response"), "response", "connection.start-ok");
        (string username, string password)? credentials = ParsePlainResponse(response);
        BrokerUser? user = credentials is null
            ? null
            : FindUser(credentials.Value.username, credentials.Value.password);
        if (user is null)
        {
            // RabbitMQ's exact wire wording for a refused PLAIN login.
            return await CloseConnectionAsync(
                AmqpConstants.AccessRefused,
                "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN. For details see the broker logfile.",
                10, 11, ct);
        }

        await SendMethodAsync(Method(tune =>
        {
            tune.Set("channel-max", (short)DefaultChannelMax);
            tune.Set("frame-max", (int)_options.FrameMax);
            tune.Set("heartbeat", (short)_options.Heartbeat);
        }, "connection.tune"), ct);
        _state = ConnectionState.AwaitTuneOk;
        return true;
    }

    private static (string Username, string Password)? ParsePlainResponse(byte[] response)
    {
        // PLAIN: authzid NUL authcid NUL password; the authzid is unused but must be
        // present for a well-formed response (three NUL-separated parts).
        string[] parts = Encoding.UTF8.GetString(response).Split('\0');
        return parts.Length != 3 ? null : (parts[1], parts[2]);
    }

    private BrokerUser? FindUser(string username, string password)
    {
        foreach (var user in _options.Users)
        {
            if (string.Equals(user.Name, username, StringComparison.Ordinal)
                && string.Equals(user.Password, password, StringComparison.Ordinal))
            {
                return user;
            }
        }

        return null;
    }

    private async Task<bool> OnTuneOkAsync(AmqpMethod method, MethodSchema schema, CancellationToken ct)
    {
        short channelMax = Unbox<short>(method, schema, "channel-max");
        int frameMax = Unbox<int>(method, schema, "frame-max");
        short heartbeat = Unbox<short>(method, schema, "heartbeat");
        if (channelMax < 0 || frameMax < 0 || heartbeat < 0)
        {
            return await CloseConnectionAsync(
                AmqpConstants.SyntaxError,
                $"SYNTAX_ERROR - {schema.Name} values must be non-negative",
                schema.ClassId, schema.MethodId, ct);
        }

        // Frame-max: adopt min(offer, client), with 0 keeping the offer. A TuneOk
        // below the 4096 frame-min-size is refused outright — RabbitMQ's
        // rabbit_reader.erl semantics (protocol-error not_allowed). It is never
        // clamped UP: a clamp would push the negotiated max above the client's own
        // declared cap (a buffer-overflow hazard for a conforming client), the
        // post-review ruling that replaced the brief's floor-clamp (Task 4 report).
        if (frameMax != 0 && frameMax < AmqpFrame.FrameMinSize)
        {
            return await CloseConnectionAsync(
                AmqpConstants.NotAllowed,
                "NOT_ALLOWED - frame_max value specified in TuneOk < 4096",
                schema.ClassId, schema.MethodId, ct);
        }

        _frameMax = frameMax == 0
            ? _options.FrameMax
            : Math.Min((uint)_options.FrameMax, (uint)frameMax);

        // Heartbeat: the TuneOk value is the negotiated one, verbatim (the RabbitMQ
        // server adopts the client's TuneOk heartbeat without re-sending Tune). 0
        // disables heartbeats, and no watchdog is started at all.
        ushort heartbeatSeconds = (ushort)heartbeat;
        if (heartbeatSeconds > 0)
        {
            StartWatchdog(heartbeatSeconds);
        }

        // Channel-max: Task 5 accepts channels; M0 stores nothing and enforces none
        // (the brief rules channel-max out of enforcement for this milestone).
        _state = ConnectionState.AwaitOpen;
        return true;
    }

    private async Task<bool> OnOpenAsync(AmqpMethod method, CancellationToken ct)
    {
        string vhost = (string)(method.Get("virtual-host") ?? string.Empty);
        if (!Array.Exists(_options.Vhosts, v => string.Equals(v, vhost, StringComparison.Ordinal)))
        {
            return await CloseConnectionAsync(
                AmqpConstants.InvalidPath,
                $"INVALID_PATH - invalid virtual host '{vhost}'",
                10, 40, ct);
        }

        await SendMethodAsync(Method(openOk =>
        {
            openOk.Set("known-hosts", "");
        }, "connection.open-ok"), 0, ct);
        _vhost = vhost; // channel dispatch binds declarations to the connection's vhost
        _state = ConnectionState.Serving;
        return true;
    }

    /// <summary>Client-initiated connection.close: reply close-ok and finish.</summary>
    private async Task<bool> OnClientCloseAsync(CancellationToken ct)
    {
        await SendMethodAsync(Method(null, "connection.close-ok"), ct);
        return false;
    }

    // ---------- server-initiated close ----------

    /// <summary>Sends connection.close and ends the frame pump when the caller asked
    /// for a graceful path (the closing-wait then runs after the pump returns).</summary>
    private async Task<bool> CloseConnectionAsync(
        ushort replyCode,
        string replyText,
        ushort classId,
        ushort methodId,
        CancellationToken ct)
    {
        _state = ConnectionState.ClosingByServer;
        await SendMethodAsync(Method(close =>
        {
            close.Set("reply-code", (short)replyCode);
            close.Set("reply-text", replyText);
            close.Set("class-id", (short)classId);
            close.Set("method-id", (short)methodId);
        }, "connection.close"), ct);
        return false; // the frame pump ends; close-wait runs out of RunAsync
    }

    /// <summary>The closing tail used by hard-error paths (which blew out of the frame
    /// pump's exception surface, so the graceful pump path is unavailable): send
    /// connection.close, then wait for the client's close-ok or a dead transport.</summary>
    private async Task SendCloseReplyThenCompleteAsync(ushort replyCode, string replyText, CancellationToken ct)
    {
        try
        {
            await SendMethodAsync(Method(close =>
            {
                close.Set("reply-code", (short)replyCode);
                close.Set("reply-text", replyText);
                close.Set("class-id", (short)0);
                close.Set("method-id", (short)0);
            }, "connection.close"), ct);
            if (_state != ConnectionState.ClosingByServer)
            {
                _state = ConnectionState.ClosingByServer;
            }

            await WaitForCloseOkAsync(ct);
        }
        catch (OperationCanceledException)
        {
            // Stopped mid-reply: fall through to transport completion.
        }
        catch (Exception)
        {
            // The peer is going away mid-close; the transport completes anyway.
        }
    }

    /// <summary>Closing state: read the wire until connection.close-ok, EOF, or a
    /// broken frame ends it. Any other method completes the transport outright.</summary>
    private async Task WaitForCloseOkAsync(CancellationToken ct)
    {
        while (true)
        {
            ReadResult read = await _transport.Reader.ReadAsync(ct);
            var buffer = read.Buffer;
            if (buffer.IsEmpty && read.IsCompleted)
            {
                return; // peer gave up: fine, the close was delivered
            }

            var frames = new List<AmqpFrame>();
            try
            {
                FrameDecoder.ReadFrames(ref buffer, frames, _frameMax);
                _transport.Reader.AdvanceTo(buffer.Start, buffer.End);
            }
            catch (AmqpProtocolException)
            {
                return; // garbage while closing: complete the transport
            }

            foreach (var frame in frames)
            {
                _watchdog?.OnFrameReceived(); // any inbound frame resets the watchdog
                if (frame.Type == AmqpFrame.FrameHeartbeat)
                {
                    continue; // ignored like anywhere else
                }

                if (frame.Type != AmqpFrame.FrameMethod || frame.Channel != 0)
                {
                    return; // only a channel-0 close-ok is meaningful now
                }

                var payload = new ByteReader(frame.Payload.Span);
                ushort classId = payload.ReadUShort();
                ushort methodId = payload.ReadUShort();
                // close-ok acknowledges; anything else is already a lost cause here.
                return;
            }

            if (read.IsCompleted)
            {
                return;
            }
        }
    }

    // ---------- heartbeat watchdog ----------

    /// <summary>Arms the watchdog for the negotiated heartbeat: silence kill at twice
    /// the interval, checked by a timer at quarter-period granularity (bounded to sane
    /// min/max at the real-timer level, since the pure watchdog stays fake-clock-able).</summary>
    private void StartWatchdog(ushort negotiatedHeartbeatSeconds)
    {
        var interval = TimeSpan.FromSeconds(negotiatedHeartbeatSeconds);
        var watchdog = new HeartbeatWatchdog(interval, OnHeartbeatTimeout);
        _watchdog = watchdog;

        var period = TimeSpan.FromMilliseconds(Math.Clamp(interval.TotalMilliseconds / 4, 250, 5_000));
        _timer = new Timer(_ => watchdog.Check(), null, period, period);
    }

    private void OnHeartbeatTimeout(HeartbeatWatchdog watchdog)
    {
        if (_disposed)
        {
            return;
        }

        // The loop's exit strategy: cancels a pending ReadAsync without writing
        // anything (a peer stalling for 2x is a socket-level death in RabbitMQ too).
        _heartbeatCts.Cancel();
    }

    // ---------- helpers ----------

    private static T Cast<T>(object? value, string field, string method) => value is T typed
        ? typed
        : throw new AmqpDecodeException($"Field '{field}' of {method} did not arrive as {typeof(T).Name}");

    private static T Unbox<T>(AmqpMethod method, MethodSchema schema, string field) =>
        method.Get(field) is T value
            ? value
            : throw new AmqpDecodeException($"Field '{field}' of {schema.Name} did not arrive as {typeof(T).Name}");

    private static AmqpMethod Method(Action<AmqpMethod>? setFields, string name)
    {
        if (!MethodRegistry.TryGetByName(name, out var schema))
        {
            throw new AmqpEncodeException($"Registry is missing {name} (Task 3 guarantee)");
        }

        var method = new AmqpMethod(schema!);
        setFields?.Invoke(method);
        return method;
    }

    /// <summary>Encodes and writes one method frame on channel 0 and flushes.</summary>
    private ValueTask SendMethodAsync(AmqpMethod method, CancellationToken ct) =>
        SendMethodAsync(method, 0, ct);

    /// <summary>Encodes and writes one method frame on the given channel and flushes.</summary>
    private async ValueTask SendMethodAsync(AmqpMethod method, ushort channel, CancellationToken ct)
    {
        // MethodCodec writes class-id + method-id + arguments; FrameWriter rebuilds the
        // frame from those parts, so the id prefix is sliced off once here.
        var payloadWriter = new ByteWriter();
        MethodCodec.WriteMethod(ref payloadWriter, method);
        byte[] payload = payloadWriter.ToArray();

        var frameWriter = new ByteWriter();
        FrameWriter.WriteMethod(ref frameWriter, channel, method.ClassId, method.MethodId, payload.AsSpan(4));
        await WriteAsync(frameWriter.ToArray(), ct);
    }

    /// <summary>Writes bytes to the transport, absorbing the transport's own death so
    /// shutdown paths never crash on a vanished peer.</summary>
    private async ValueTask WriteAsync(byte[] bytes, CancellationToken ct)
    {
        try
        {
            var writer = _transport.Writer;
            var span = writer.GetSpan(bytes.Length);
            bytes.CopyTo(span);
            writer.Advance(bytes.Length);
            await writer.FlushAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // signaled shutdown: propagate to the loop's cancellation handlers
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            // The peer is gone or the pipe was completed underneath us; the connection
            // is dead either way, and no further bytes need to get out.
        }
    }
}
