namespace RedisMQ.Broker.Server;

using System;
using System.IO;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Protocol;

/// <summary>
/// What a channel <see cref="ChannelContext.OnMethod"/> / content dispatch decision
/// instructs <see cref="WireConnection"/> to do on the wire. The reply/silent shapes
/// keep the frame pump alive; the two error shapes fan out to map_exception's two
/// destinations (rabbit_binary_generator.erl): a soft code closes the channel
/// (channel.close on that channel, the channel starts mourning), a hard code closes
/// the connection (connection.close on channel 0 — every code M0 raises is hard.
/// </summary>
public abstract record ChannelOutcome;

/// <summary>Sends the method on the channel that produced it; <paramref name="retireAfter"/>
/// performs the channel-number cleanup (close-ok accounting) after the reply.</summary>
public sealed record ChannelOutcomeReply(AmqpMethod Reply, bool RetireAfter = false) : ChannelOutcome;

/// <summary>No bytes on the wire at all — the silent completions (a landed publish, a
/// dropped unroutable, the mourning catch-all's discard and the close-ok of a completed
/// close); <paramref name="retireChannel"/> frees the channel number for a fresh
/// channel.open.</summary>
public sealed record ChannelOutcomeSilent(bool RetireChannel = false) : ChannelOutcome;

/// <summary>Soft error: channel.close carrying the fields on the offending channel;
/// the channel lives on in the mourning state. The M0 soft codes are 403/404/405/406
/// plus the assembler's 505 sequence violations — the map_exception decision (soft ⇒
/// channel close) is RabbitMQ's rabbit_binary_generator.erl.</summary>
public sealed record ChannelOutcomeSoftError(ushort Code, string Text, ushort ClassId, ushort MethodId) : ChannelOutcome;

/// <summary>Hard error: connection.close on channel 0, reply class/method of the failed
/// method for method-bodied errors (0,0 for FSM/reader ones). The M0 hard codes are
/// 501/502/503/504/540, the assembler 505 pin (report deviation D1), and the oversize
/// body's 311 (report deviation D2 — the committed fixture actually classifies both
/// 505 and 311 differently, see the Task 6 report).</summary>
public sealed record ChannelOutcomeHardError(ushort Code, string Text, ushort ClassId, ushort MethodId) : ChannelOutcome;

/// <summary>A mandatory publish routed to nothing: basic.return (reply-code 312
/// NO_ROUTE, carrying the publishing exchange and routing key) followed by the full
/// returned content — the rabbit_channel basic_return/5 shape, re-chunked by the
/// writer per the negotiated frame-max (WireConnection writes it out).</summary>
public sealed record ChannelOutcomeBasicReturn(AmqpMethod ReturnMethod, ContentProperties Properties, byte[] Body) : ChannelOutcome;

/// <summary>The channel decision scheduled consumer-path writes on its serialized
/// writer chain (replies like consume-ok/qos-ok/cancel-ok/get-ok, recover's
/// post-flush requeue plan, etc.) and the caller must drain it — WireConnection runs
/// the chain right after dispatch, before reading more frames, so the chain's write
/// order is the wire order.</summary>
public sealed record ChannelOutcomeChain : ChannelOutcome
{
    /// <summary>The singleton shape: the drain target is the same channel's chain.</summary>
    public static readonly ChannelOutcomeChain Instance = new();

    private ChannelOutcomeChain()
    {
    }
}

/// <summary>
/// One AMQP channel's finite state machine, following rabbit_channel.erl: a fresh
/// channel number starts <c>Starting</c> where only channel.open is legal; open
/// promotes it to <c>Running</c>; a soft error sent channel.close turns it into
/// <c>Mourning</c>, where traffic is discarded, repeated channel.close re-answers
/// close-ok, and channel.close-ok retires the number; a hard error closes the whole
/// connection.
///
/// Wire rules M0 implements (with the RabbitMQ master sources as reference):
/// <list type="bullet">
/// <item>channel.open in Starting → open-ok with an empty channel-id; in Running →
/// hard channel_error "second 'channel.open' seen". Any other method in Starting →
/// hard channel_error "expected 'channel.open'" (reply class/method 0,0).</item>
/// <item>The M0 channel method set is channel.open/flow/close/close-ok, the exchange,
/// queue and bind/unbind trio; anything else returns the not-implemented close that
/// Task 4 established for dialect methods (RabbitMQ's own catch-all is command_invalid
/// 503 — recorded as an M0 deviation in the report).</item>
/// <item>channel.flow(true) replies flow-ok(true); flow(false) is the RabbitMQ
/// not_implemented "active=false" hard error.</item>
/// <item>exchange.declare: type check first ("unknown exchange type"), then the
/// default-exchange refusal, then equivalence in the order type → durable →
/// auto-delete → internal, then creation with the amq.-prefix refusal applied only on
/// creation (predeclared amq.direct can be redeclared). Passive declares skip the
/// type check, still refuse "", and demand existence.</item>
/// <item>queue.declare: the amq.-prefix refusal runs before existence (unlike the
/// exchange order); the empty name generates an <c>amq.gen-</c> server-name;
/// equivalence runs exclusive-access first (resource_locked) then durable then
/// auto-delete; every successful declare — passive and nowait included — updates the
/// most-recently-declared-queue hint.</item>
/// <item>queue.bind/queue.unbind run the "" shortcuts first (queue and, when both are
/// empty, the routing key expand to the most recently declared queue or the
/// "no previously declared queue" not-found), the default-exchange refusal before any
/// resource lookup, then exchange and queue existence in that order. queue.bind honors
/// nowait; queue.unbind has no nowait and always replies.</item>
/// <item>M0 refuses non-empty declare/bind arguments outright with not-implemented
/// (RabbitMQ validates and accepts arguments — deviation recorded in the report).</item>
/// <item>basic.publish opens the channel's content assembly (rabbit_command_assembler):
/// header → N body frames per channel; a method frame or wrong-kind content frame
/// mid-assembly closes the CHANNEL with unexpected-frame carrying the assembler's
/// wording (the pending basic.publish (60,40) ids, or (0,0) with none). THE PIN
/// DIVERGENCE: RabbitMQ's own frame-error classification makes UNEXPECTED-FRAME 505
/// hard (connection close); the M0 plan pins channel-level (report deviation D1) —
/// but the wording strings are the 4.x assembler's, verbatim.</item>
/// <item>Declared bodies over the 134,217,728-byte cap close the CONNECTION at header
/// arrival with content-too-large (the M0 brief's hard 311 pin; report deviation D2 —
/// the committed fixture classifies 311 as soft, and RabbitMQ 4.4 actually runs a
/// two-tier rule: a soft 406 at the configured cap with body discard, a hard 501
/// frame_error above its 536,870,912-byte absolute ceiling). Overshooting one frame's
/// remaining declared bytes, or an empty body frame with bytes still owed, are the
/// assembler's hard frame_error closes with its wordings.</item>
/// <item>Assembly completion dispatches the publish in rabbit_channel's order:
/// immediate=true → hard not_implemented "immediate=true"; then the exchange
/// lookup_or_die (not-found 404 when absent — the content must be complete before
/// this runs); the default exchange routes to the queue NAMED by the routing key
/// (rabbit_exchange's special clause — no binding table, absent queue = unroutable,
/// never a not-found); every routed queue is enqueued (`IRouteEngine.Enqueue`) under
/// one insertion order; unroutable + mandatory → basic.return 312 NO_ROUTE with the
/// full content back on the channel, else a silent drop. basic.publish itself never
/// replies on success.</item>
/// </list>
/// Activity such as consume, acks, confirms, and tx is Task 7+; publishing rides the
/// assembler above and the engine hand-off.
/// </summary>
public sealed class ChannelContext
{
    private const string DirectType = "direct";

    private enum ChannelState
    {
        Starting, // the number exists on the connection but saw no channel.open yet
        Running,
        Mourning, // the server sent channel.close; traffic is discarded until close-ok
    }

    /// <summary>The per-channel content assembly phase (rabbit_command_assembler):
    /// a content-bearing method parks the channel in ExpectHeader, body frames run
    /// ExpectBody, and completion — or a violation — returns everything to
    /// ExpectMethod.</summary>
    internal enum ContentPhase
    {
        ExpectMethod,
        ExpectHeader,
        ExpectBody,
    }

    /// <summary>
    /// The M0 hard body cap — RabbitMQ's configured per-message maximum (the server's
    /// default 134217728). A declared body above it dead-ends the publish at header
    /// arrival with the hard content-too-large close the brief pins (see the Task 6
    /// report deviation D2 for what RabbitMQ 4.4 really does).
    /// </summary>
    public const long MaxBodySize = 134_217_728;

    private readonly IRouteEngine _engine;
    private readonly IChannelDeliveryHost _host;
    private readonly ushort _channel;
    private readonly string _vhost;
    private ChannelState _state = ChannelState.Starting;

    /// <summary>RabbitMQ's most_recently_declared_queue "" shortcut target; empty
    /// string means "no successful declare happened on this channel yet".</summary>
    private string _mostRecentlyDeclaredQueue = string.Empty;
    private ContentPhase _pendingContentPhase = ContentPhase.ExpectMethod;

    // The open content sequence (rabbit_command_assembler state): the pending
    // content-bearing method, its declared header, and the body bytes arrived so far.
    private AmqpMethod? _pendingPublish;
    private ContentHeader? _pendingHeader;
    private MemoryStream? _pendingBody;

    // Task 7 — the channel-level consume accounting. All of it mutates under
    // _deliveryLock (the ladder: it is always INSIDE the engine's gate on the pump
    // claim path, and the handler paths release it before they touch the engine).
    private readonly object _deliveryLock = new();
    private readonly Dictionary<long, Unacked> _unacked = []; // delivery-tag → unsettled entry
    private long _nextDeliveryTag; // 0 = none assigned yet: tags ascend 1, 2, 3 per channel
    private long _outstandingBytes; // Σ unacked body lengths for the bytes gate
    private int _prefetchCount; // 0 = unlimited (basic.qos) — global=true is demoted per the ledger ruling
    private long _prefetchSize; // 0 = unlimited; the RedisMQ bytes-gate extension
    private readonly Dictionary<string, string> _consumerQueues = []; // tag → queue name
    private readonly Dictionary<string, ConsumerEntry> _consumerEntries = []; // tag → the engine-registered consumer
    private ulong _serverConsumerTags; // amq.ctag-N counter
    private bool _consumersTornDown;

    // Task 7 — the per-channel serialized writer chain: consumer-path replies and
    // deliveries are scheduled here (never written inline), so a queue-pump claim can
    // never put a deliver ahead of a reply the channel already promised. The single
    // drainer (_writerGate) runs jobs strictly in schedule order.
    private readonly SemaphoreSlim _writerGate = new(1, 1);
    private readonly object _writerLock = new();
    private readonly Queue<Func<ValueTask>> _writerJobs = [];

    internal ChannelContext(ushort channel, string vhost, IRouteEngine engine, IChannelDeliveryHost host)
    {
        _channel = channel;
        _vhost = vhost;
        _engine = engine;
        _host = host;
    }

    public bool IsMourning => _state == ChannelState.Mourning;

    /// <summary>The channel's content assembler phase; content dispatch consults it,
    /// and a method frame arriving out of phase dies with the assembler's wording.</summary>
    internal ContentPhase PendingContentPhase => _pendingContentPhase;

    /// <summary>Enters the mourning state after a soft error's channel.close is (or is
    /// being) sent: traffic is discarded, close re-answered, close-ok retires.</summary>
    internal void Mourn() => _state = ChannelState.Mourning;

    /// <summary>Dispatches one fully-decoded method by the channel's current state.</summary>
    public ChannelOutcome OnMethod(AmqpMethod method) => _state switch
    {
        ChannelState.Starting => OnStarting(method),
        ChannelState.Mourning => OnMourning(method),
        _ => OnRunning(method),
    };

    // ---------- Starting: only channel.open ----------

    private ChannelOutcome OnStarting(AmqpMethod method)
    {
        if (method.ClassId == 20 && method.MethodId == 10)
        {
            _state = ChannelState.Running;
            return new ChannelOutcomeReply(NewMethod("channel.open-ok", openOk =>
                openOk.Set("channel-id", Array.Empty<byte>()))); // RabbitMQ sends an empty binary
        }

        return new ChannelOutcomeHardError(
            AmqpConstants.ChannelError,
            "CHANNEL_ERROR - expected 'channel.open'",
            0, 0);
    }

    // ---------- Mourning: discard unless channel traffic ----------

    private ChannelOutcome OnMourning(AmqpMethod method)
    {
        if (method.ClassId != 20)
        {
            return new ChannelOutcomeSilent(); // the closing catch-all: no reply at all
        }

        return method.MethodId switch
        {
            40 => new ChannelOutcomeReply(NewMethod("channel.close-ok")), // close while closing: close-ok again, still mourning
            41 => new ChannelOutcomeSilent(RetireChannel: true),          // close-ok performs the cleanup
            _ => new ChannelOutcomeSilent(),
        };
    }

    // ---------- Running ----------

    private ChannelOutcome OnRunning(AmqpMethod method)
    {
        // rabbit_command_assembler: while content is owed (header or body), ANY method
        // frame is an unexpected_frame with the pending publish's ids. The intercept
        // runs before state dispatch so not-implemented or declare logic can hijack it.
        if (_pendingContentPhase != ContentPhase.ExpectMethod)
        {
            return _pendingContentPhase switch
            {
                ContentPhase.ExpectBody => UnexpectedFrame(
                    "UNEXPECTED_FRAME - expected content body, got non content body frame instead",
                    _pendingPublish!.ClassId, _pendingPublish.MethodId),
                _ => UnexpectedFrame(
                    $"UNEXPECTED_FRAME - expected content header for class {_pendingPublish!.ClassId}, " +
                    "got non content header frame instead",
                    _pendingPublish.ClassId, _pendingPublish.MethodId),
            };
        }

        ushort classId = method.ClassId;
        ushort methodId = method.MethodId;
        return (classId, methodId) switch
        {
            (20, 10) => new ChannelOutcomeHardError( // rabbit_channel's second channel.open
                AmqpConstants.ChannelError,
                "CHANNEL_ERROR - second 'channel.open' seen",
                0, 0),
            (20, 20) => OnChannelFlow(method),
            (20, 40) => new ChannelOutcomeReply( // client-initiated close: close-ok retires the number
                NewMethod("channel.close-ok"), RetireAfter: true),
            (40, 10) => OnExchangeDeclare(method),
            (50, 10) => OnQueueDeclare(method),
            (50, 20) => OnQueueBind(method),
            (50, 50) => OnQueueUnbind(method),
            (60, 10) => OnBasicQos(method),
            (60, 20) => OnBasicConsume(method),
            (60, 30) => OnBasicCancel(method),
            (60, 40) => OnBasicPublish(method),
            (60, 70) => OnBasicGet(method),
            (60, 80) => OnBasicAck(method), // basic.ack: rabbit_channel collect_acks
            (60, 90) => OnBasicReject(method),
            (60, 120) => OnBasicNack(method), // (60,100) is recover-async: outside the M0 set
            (60, 110) => OnBasicRecover(method),
            _ => new ChannelOutcomeHardError(
                AmqpConstants.NotImplemented,
                $"NOT_IMPLEMENTED - {method.Schema.Name} ({classId},{methodId}) is not part of the M0 channel method set",
                classId, methodId),
        };
    }

    // ---------- content assembly (basic.publish per rabbit_command_assembler) ----------

    /// <summary>Dispatches one content frame through the channel's open content
    /// sequence (or the idle assembler, where any content frame is the error).</summary>
    public ChannelOutcome OnContent(AmqpFrame frame) => _pendingContentPhase switch
    {
        ContentPhase.ExpectHeader => OnExpectedHeader(frame),
        ContentPhase.ExpectBody => OnExpectedBody(frame),
        _ => UnexpectedFrame(
            "UNEXPECTED_FRAME - expected method frame, got non method frame instead", 0, 0),
    };

    /// <summary>A content-bearing method opens the sequence: no bytes leave the wire;
    /// the header owes next, then the declared body bytes (rabbit's assembler parks
    /// the channel in the header phase — the method itself is still "in flight").</summary>
    private ChannelOutcome OnBasicPublish(AmqpMethod method)
    {
        _pendingPublish = method;
        _pendingContentPhase = ContentPhase.ExpectHeader;
        return new ChannelOutcomeSilent();
    }

    private ChannelOutcome OnExpectedHeader(AmqpFrame frame)
    {
        if (frame.Type != AmqpFrame.FrameHeader)
        {
            return UnexpectedFrame(
                $"UNEXPECTED_FRAME - expected content header for class {_pendingPublish!.ClassId}, " +
                "got non content header frame instead",
                _pendingPublish.ClassId, _pendingPublish.MethodId);
        }

        // The codec throws AmqpDecodeException for a non-basic class header — the
        // Task 2 codec ruling — which surfaces as the connection's syntax-error
        // close (conceptually the assembler's "expected content header for class 60,
        // got one for class N instead" path; M0 leaves it at the codec boundary).
        var reader = new ByteReader(frame.Payload.Span);
        ContentHeader header = ContentHeaderCodec.Read(ref reader);

        // The declared-size cap rides at header arrival — the assembler checks the
        if (header.BodySize > MaxBodySize)
        {
            // RabbitMQ 4.4 splits this into a soft 406 at its configured max plus a
            // hard 501 above the 512MiB ceiling; the M0 brief pins the single hard
            // content-too-large connection close instead (report deviation D2).
            return new ChannelOutcomeHardError(
                AmqpConstants.ContentTooLarge,
                $"CONTENT_TOO_LARGE - content body size {header.BodySize} " +
                $"exceeds maximum allowed size {MaxBodySize}",
                _pendingPublish!.ClassId, _pendingPublish.MethodId);
        }

        _pendingHeader = header;
        _pendingBody = new MemoryStream();
        if (header.BodySize == 0)
        {
            return FinishPublish(); // an empty declared body completes with the header
        }

        _pendingContentPhase = ContentPhase.ExpectBody;
        return new ChannelOutcomeSilent();
    }

    private ChannelOutcome OnExpectedBody(AmqpFrame frame)
    {
        if (frame.Type != AmqpFrame.FrameBody)
        {
            return UnexpectedFrame(
                "UNEXPECTED_FRAME - expected content body, got non content body frame instead",
                _pendingPublish!.ClassId, _pendingPublish.MethodId);
        }

        long declared = _pendingHeader!.BodySize;
        long accumulated = _pendingBody!.Length;

        // The assembler's own eagle-eye clauses — both hard frame_error closes, and
        // both with the source's wordings: an empty fragment while bytes are owed, or
        // a fragment that would overshoot the declared total.
        if (frame.Payload.Length == 0 && accumulated < declared)
        {
            return new ChannelOutcomeHardError(
                AmqpConstants.FrameError,
                "FRAME_ERROR - empty content body frame while content body is incomplete",
                _pendingPublish!.ClassId, _pendingPublish.MethodId);
        }

        if (accumulated + frame.Payload.Length > declared)
        {
            return new ChannelOutcomeHardError(
                AmqpConstants.FrameError,
                "FRAME_ERROR - content body frame exceeds remaining content size",
                _pendingPublish!.ClassId, _pendingPublish.MethodId);
        }

        _pendingBody!.Write(frame.Payload.Span);
        if (_pendingBody!.Length < declared)
        {
            return new ChannelOutcomeSilent();
        }

        return FinishPublish();
    }

    /// <summary>The content is whole: rabbit_channel's publish dispatch in order —
    /// immediate first, then lookup_or_die, then routing and the engine hand-off (or
    /// the mandatory/unroutable return). The pending sequence is already back in the
    /// expect-method state when any reply frame of this decision leaves the wire.</summary>
    private ChannelOutcome FinishPublish()
    {
        AmqpMethod method = _pendingPublish!;
        ContentHeader header = _pendingHeader!;
        byte[] body = _pendingBody!.ToArray();
        ResetPendingContent();

        // rabbit_channel: handle_method(#'basic.publish'{immediate = true}, ...)
        // exits protocol_error(not_implemented, "immediate=true") — hard, and it
        // receives the already-assembled content, before any exchange lookup.
        if (method.Get("immediate") is true)
        {
            return new ChannelOutcomeHardError(
                AmqpConstants.NotImplemented,
                "NOT_IMPLEMENTED - immediate=true",
                method.ClassId, method.MethodId);
        }

        string exchangeName = method.Get("exchange") as string ?? string.Empty;
        string routingKey = method.Get("routing-key") as string ?? string.Empty;

        // lookup_or_die happens after the full content assembly (it receives the
        // content), not-found being the ordinary soft 404.
        ExchangeInfo? exchange = _engine.GetExchange(_vhost, exchangeName);
        if (exchange is null)
        {
            return NotFound(kind: "exchange", exchangeName, method);
        }

        // rabbit_exchange's default-exchange clause: the queue resource NAMED by the
        // routing key, no binding table — an absent queue is unroutable, NOT a
        // not-found (verified in the source; see the Task 6 report).
        IReadOnlyList<string> targets = exchangeName.Length == 0
            ? (_engine.GetQueue(_vhost, routingKey) is null ? [] : [routingKey])
            : _engine.RouteTo(exchange, header.Properties, routingKey);

        if (targets.Count == 0)
        {
            // process_routing_mandatory: mandatory + nothing routed → basic_return
            // (no_route) carrying the publishing origin and the full content back;
            // non-mandatory → messages_unroutable_dropped, silence.
            if (method.Get("mandatory") is true)
            {
                return new ChannelOutcomeBasicReturn(
                    NewMethod("basic.return", basicReturn =>
                    {
                        basicReturn.Set("reply-code", (short)AmqpConstants.NoRoute); // NO_ROUTE
                        basicReturn.Set("reply-text", "NO_ROUTE");
                        basicReturn.Set("exchange", exchangeName);
                        basicReturn.Set("routing-key", routingKey);
                    }),
                    header.Properties,
                    body);
            }

            return new ChannelOutcomeSilent();
        }

        foreach (string queue in targets)
        {
            // One engine entry per routed queue, in binding-insertion order; the body
            // ownership may move because the engine stores its own defensive copy.
            _engine.Enqueue(_vhost, queue, header.Properties, body, exchangeName, routingKey);
        }

        return new ChannelOutcomeSilent(); // basic.publish never replies on success
    }

    private void ResetPendingContent()
    {
        _pendingPublish = null;
        _pendingHeader = null;
        _pendingBody = null;
        _pendingContentPhase = ContentPhase.ExpectMethod;
    }

    /// <summary>The assembler's unexpected_frame shape: soft 505 on this channel (the
    /// M0 pin — see the class doc's D1 note for RabbitMQ's hard reality).</summary>
    private static ChannelOutcome UnexpectedFrame(string text, ushort classId, ushort methodId) =>
        new ChannelOutcomeSoftError(AmqpConstants.UnexpectedFrame, text, classId, methodId);

    private ChannelOutcome OnChannelFlow(AmqpMethod method)
    {
        if (!Equals(method.Get("active"), true))
        {
            // rabbit_channel: flowing off exits not_implemented "active=false",
            // a hard error through map_exception.
            return new ChannelOutcomeHardError(
                AmqpConstants.NotImplemented,
                "NOT_IMPLEMENTED - active=false",
                method.ClassId, method.MethodId);
        }

        return new ChannelOutcomeReply(NewMethod("channel.flow-ok", flowOk =>
            flowOk.Set("active", true)));
    }

    // ---------- exchange.declare (rabbit_exchange order) ----------

    private ChannelOutcome OnExchangeDeclare(AmqpMethod method)
    {
        string name = method.Get("exchange") as string ?? string.Empty;
        string type = method.Get("type") as string ?? string.Empty;
        bool passive = method.Get("passive") is true;
        bool durable = method.Get("durable") is true;
        bool autoDelete = method.Get("auto-delete") is true;
        bool internalExchange = method.Get("internal") is true;
        bool nowait = method.Get("nowait") is true;
        FieldTable arguments = method.Get("arguments") as FieldTable ?? new FieldTable();

        if (!passive && arguments.Count > 0)
        {
            return NotSupportedArguments(method);
        }

        if (passive)
        {
            // rabbit_channel's passive arm: default-exchange check, then existence;
            // no type check, no equivalence.
            if (name.Length == 0)
            {
                return DefaultExchangeRefused(method);
            }

            ExchangeInfo? existing = _engine.GetExchange(_vhost, name);
            if (existing is null)
            {
                return NotFound(kind: "exchange", name, method);
            }

            return DeclareOk(nowait, "exchange.declare-ok", name);
        }

        // rabbit_exchange:check_type runs before everything else.
        if (!string.Equals(type, DirectType, StringComparison.Ordinal))
        {
            return Soft(method, AmqpConstants.PreconditionFailed, $"PRECONDITION_FAILED - unknown exchange type '{type}'");
        }

        if (name.Length == 0)
        {
            return DefaultExchangeRefused(method);
        }

        ExchangeInfo? declared = _engine.GetExchange(_vhost, name);
        if (declared is not null)
        {
            return DeclaredExchangeOutcome(method, name, declared, type, durable, autoDelete, internalExchange, nowait);
        }

        // amq.* is reserved, but only on actual creation (predeclared amq.* is exempt).
        if (name.StartsWith("amq.", StringComparison.Ordinal))
        {
            return Soft(
                method,
                AmqpConstants.AccessRefused,
                $"ACCESS_REFUSED - exchange name '{name}' contains reserved prefix 'amq.*'");
        }

        // Creation-only compare-and-set: on the miss path the insert happens under the
        // engine's single lock, so a racing creator with different flags cannot
        // overwrite it — the loser sees the winner's record and re-runs the equivalence
        // ladder against it (last-write-wins is unreachable).
        ExchangeInfo? prior = _engine.DeclareExchange(new ExchangeInfo(_vhost, name, type, durable, autoDelete, internalExchange));
        if (prior is not null)
        {
            return DeclaredExchangeOutcome(method, name, prior, type, durable, autoDelete, internalExchange, nowait);
        }

        return DeclareOk(nowait, "exchange.declare-ok", name);
    }

    /// <summary>The assert-equivalence ladder for a stored exchange, shared by the
    /// early-exists path and the declare race's loser: type → durable → auto-delete →
    /// internal (rabbit_exchange's assert_equivalence order), then declare-ok.</summary>
    private ChannelOutcome DeclaredExchangeOutcome(
        AmqpMethod method,
        string name,
        ExchangeInfo declared,
        string type,
        bool durable,
        bool autoDelete,
        bool internalExchange,
        bool nowait)
    {
        if (declared.Type != type)
        {
            return InequivalentExchange(method, name, "type", type, declared.Type);
        }

        if (declared.Durable != durable)
        {
            return InequivalentExchange(method, name, "durable", durable, declared.Durable);
        }

        if (declared.AutoDelete != autoDelete)
        {
            return InequivalentExchange(method, name, "auto-delete", autoDelete, declared.AutoDelete);
        }

        if (declared.Internal != internalExchange)
        {
            return InequivalentExchange(method, name, "internal", internalExchange, declared.Internal);
        }

        return DeclareOk(nowait, "exchange.declare-ok", name);
    }

    private ChannelOutcome InequivalentExchange(AmqpMethod method, string name, string key, object received, object current) =>
        Soft(
            method,
            AmqpConstants.PreconditionFailed,
            $"PRECONDITION_FAILED - inequivalent arg '{key}' for exchange '{name}' in vhost '{_vhost}': " +
            $"received '{ErlangAtom(received)}' but current is '{ErlangAtom(current)}'");

    // ---------- queue.declare ----------

    private ChannelOutcome OnQueueDeclare(AmqpMethod method)
    {
        string name = method.Get("queue") as string ?? string.Empty;
        bool passive = method.Get("passive") is true;
        bool durable = method.Get("durable") is true;
        bool exclusive = method.Get("exclusive") is true;
        bool autoDelete = method.Get("auto-delete") is true;
        bool nowait = method.Get("nowait") is true;
        FieldTable arguments = method.Get("arguments") as FieldTable ?? new FieldTable();

        if (!passive && arguments.Count > 0)
        {
            return NotSupportedArguments(method);
        }

        if (passive)
        {
            // No "" shortcut on declares: an empty name looks up the empty queue and
            // reports the ordinary not-found ("/" vhost wording with '' quoted).
            QueueInfo? existing = _engine.GetQueue(_vhost, name);
            if (existing is null)
            {
                return NotFound(kind: "queue", name, method);
            }

            return DeclareOk(nowait, "queue.declare-ok", name, updateMrdq: name);
        }

        // The reserved-prefix check runs BEFORE any lookup for queues (unlike
        // exchanges, whose check sits on the creation path only).
        if (name.Length > 0 && name.StartsWith("amq.", StringComparison.Ordinal))
        {
            return Soft(
                method,
                AmqpConstants.AccessRefused,
                $"ACCESS_REFUSED - queue name '{name}' contains reserved prefix 'amq.*'");
        }

        // Server-generated names bypass the reserved check the way RabbitMQ's
        // generation arm does (it is a sibling pendant of check_name).
        string actual = name;
        if (actual.Length == 0)
        {
            actual = $"amq.gen-{Guid.NewGuid().ToString("N")}"; // rabbit_guid's amq.gen + token
        }

        QueueInfo? declared = _engine.GetQueue(_vhost, actual);
        if (declared is not null)
        {
            return DeclaredQueueOutcome(method, actual, declared, durable, exclusive, autoDelete, nowait);
        }

        // Creation-only compare-and-set (see the exchange arm): a racing creator's
        // record is returned rather than overwritten, and the loser runs the same
        // resource_locked-first ladder against the winner's record.
        QueueInfo? prior = _engine.DeclareQueue(new QueueInfo(_vhost, actual, durable, exclusive, autoDelete));
        if (prior is not null)
        {
            return DeclaredQueueOutcome(method, actual, prior, durable, exclusive, autoDelete, nowait);
        }

        return DeclareOk(nowait, "queue.declare-ok", actual, updateMrdq: actual);
    }

    /// <summary>The assert-equivalence ladder for a stored queue, shared by the
    /// early-exists path and the declare race's loser: exclusive-access first
    /// (resource_locked in RabbitMQ), then durable, then auto-delete, then declare-ok.
    /// M0 compares flags verbatim — no exclusive-owner tracking — so a same-connection
    /// exclusive redeclare is refused the same way (deviation recorded in the report).</summary>
    private ChannelOutcome DeclaredQueueOutcome(
        AmqpMethod method,
        string name,
        QueueInfo declared,
        bool durable,
        bool exclusive,
        bool autoDelete,
        bool nowait)
    {
        if (declared.Exclusive != exclusive)
        {
            return Soft(
                method,
                AmqpConstants.ResourceLocked,
                $"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{name}' in vhost '{_vhost}'. " +
                "It could be originally declared on another connection or the exclusive property " +
                "value does not match that of the original declaration.");
        }

        if (declared.Durable != durable)
        {
            return InequivalentQueue(method, name, "durable", durable, declared.Durable);
        }

        if (declared.AutoDelete != autoDelete)
        {
            return InequivalentQueue(method, name, "auto-delete", autoDelete, declared.AutoDelete);
        }

        return DeclareOk(nowait, "queue.declare-ok", name, updateMrdq: name);
    }

    private ChannelOutcome InequivalentQueue(AmqpMethod method, string name, string key, bool received, bool current) =>
        Soft(
            method,
            AmqpConstants.PreconditionFailed,
            $"PRECONDITION_FAILED - inequivalent arg '{key}' for queue '{name}' in vhost '{_vhost}': " +
            $"received '{ErlangAtom(received)}' but current is '{ErlangAtom(current)}'");

    // ---------- queue.bind / queue.unbind ----------

    private ChannelOutcome OnQueueBind(AmqpMethod method) =>
        BindingAction(method, add: true);

    private ChannelOutcome OnQueueUnbind(AmqpMethod method) =>
        BindingAction(method, add: false);

    private ChannelOutcome BindingAction(AmqpMethod method, bool add)
    {
        string queue = method.Get("queue") as string ?? string.Empty;
        string exchange = method.Get("exchange") as string ?? string.Empty;
        string routingKey = method.Get("routing-key") as string ?? string.Empty;
        FieldTable arguments = method.Get("arguments") as FieldTable ?? new FieldTable();

        if (arguments.Count > 0)
        {
            return NotSupportedArguments(method);
        }

        // expand_shortcuts runs before any handle_method check in RabbitMQ: the ""
        // queue expands to the most recently declared queue or dies with
        // "no previously declared queue"; the routing key expands only when the
        // original queue name and key were both empty.
        string queueName = queue;
        if (queueName.Length == 0)
        {
            if (_mostRecentlyDeclaredQueue.Length == 0)
            {
                return Soft(method, AmqpConstants.NotFound, "NOT_FOUND - no previously declared queue");
            }

            queueName = _mostRecentlyDeclaredQueue;
        }

        string key = routingKey;
        if (queue.Length == 0 && key.Length == 0)
        {
            key = _mostRecentlyDeclaredQueue; // reached only when the queue expansion succeeded
        }

        // check_not_default_exchange: refused before any resource lookup — even when
        // the queue named does not exist either.
        if (exchange.Length == 0)
        {
            return DefaultExchangeRefused(method);
        }

        ExchangeInfo? source = _engine.GetExchange(_vhost, exchange);
        if (source is null)
        {
            return NotFound(kind: "exchange", exchange, method); // the exchange side resolves first
        }

        QueueInfo? destination = _engine.GetQueue(_vhost, queueName);
        if (destination is null)
        {
            return NotFound(kind: "queue", queueName, method);
        }

        if (add)
        {
            _engine.Bind(_vhost, exchange, queueName, key); // duplicates are a no-op
        }
        else
        {
            _engine.Unbind(_vhost, exchange, queueName, key); // removing an absent binding is ok
        }

        // queue.bind honors nowait; queue.unbind has no nowait field and always answers.
        string replyName = add ? "queue.bind-ok" : "queue.unbind-ok";
        if (add && method.Get("nowait") is true)
        {
            return new ChannelOutcomeSilent();
        }

        return new ChannelOutcomeReply(NewMethod(replyName));
    }

    // ---------- basic.qos / basic.consume / basic.cancel / basic.get (Task 7) ----------

    private ChannelOutcome OnBasicQos(AmqpMethod method)
    {
        // The schema fixes the boxed shapes: prefetch-size Long → int, prefetch-count
        // Short → short. global=true is DEMOTED to the per-channel limit (RabbitMQ 4.4
        // path with the global_qos flag off — ledger correction): the flag is read and
        // ignored, and the limit is what actually gates.
        int size = method.Get("prefetch-size") as int? ?? 0;
        short count = method.Get("prefetch-count") as short? ?? (short)0;
        lock (_deliveryLock)
        {
            _prefetchSize = size;
            _prefetchCount = count;
        }

        // A raised (or cleared) limit frees credit the pump may have idled out on.
        NudgeChannelQueues();
        return ChainReply("basic.qos-ok");
    }

    /// <summary>The consume path: queue lookup (not-found), consumer-tag reuse
    /// (not_allowed — per-channel registry, rabbit_channel 1482 wording), the
    /// exclusive arbitration inside the engine's atomic RegisterConsumer, then the
    /// consume-ok for BOTH tag flavors when nowait=false (rabbit 1817-1818; the
    /// ledger correction of the plan-text quirk). Server-assigned tags are
    /// amq.ctag-N (the counter form of rabbit_guid's token). no-local is accepted
    /// and ignored (4.4's channel handler also pattern-drops it).</summary>
    private ChannelOutcome OnBasicConsume(AmqpMethod method)
    {
        string queue = method.Get("queue") as string ?? string.Empty;
        string requestedTag = method.Get("consumer-tag") as string ?? string.Empty;
        bool noAck = method.Get("no-ack") is true;
        bool exclusive = method.Get("exclusive") is true;
        bool nowait = method.Get("nowait") is true;

        // rabbit_channel's lookup_or_die runs before any consumer mapping work.
        if (_engine.GetQueue(_vhost, queue) is null)
        {
            return NotFound(kind: "queue", queue, method);
        }

        // The per-channel tag-reuse refusal; the exclusive arbitration (a softer
        // resource_locked) happens before it inside the engine only when both apply
        // would RabbitMQ's order differ — an edge M0 records as implementation note.
        if (requestedTag.Length > 0 && _consumerQueues.ContainsKey(requestedTag))
        {
            return Soft(
                method,
                AmqpConstants.NotAllowed,
                $"NOT_ALLOWED - attempt to reuse consumer tag '{requestedTag}'");
        }

        string tag = requestedTag;
        if (tag.Length == 0)
        {
            tag = $"amq.ctag-{++_serverConsumerTags:x}";
        }

        var entry = new ConsumerEntry(this, tag, queue, noAck, exclusive);
        switch (_engine.RegisterConsumer(_vhost, queue, entry))
        {
            case ConsumeRegisterOutcome.QueueNotFound:
                return NotFound(kind: "queue", queue, method);

            case ConsumeRegisterOutcome.ExclusiveHeld:
                return Soft(
                    method,
                    AmqpConstants.ResourceLocked,
                    $"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{queue}' in vhost '{_vhost}'. " +
                    "It could be originally declared on another connection or the exclusive property " +
                    "value does not match that of the original declaration.");

            default:
                break;
        }

        _consumerQueues[tag] = queue;
        _consumerEntries[tag] = entry;

        if (nowait)
        {
            return new ChannelOutcomeSilent();
        }

        return ChainReply("basic.consume-ok", ok => ok.Set("consumer-tag", tag));
    }

    /// <summary>Cancel unregisters the consumer (unacked deliveries keep riding the
    /// ledger until acked/settled) and echoes cancel-ok for ANY tag — known or not
    /// (rabbit_channel 1541-1544 builds the OkMsg before the registry work). nowait
    /// gets silence.</summary>
    private ChannelOutcome OnBasicCancel(AmqpMethod method)
    {
        string tag = method.Get("consumer-tag") as string ?? string.Empty;
        bool nowait = method.Get("nowait") is true;
        if (_consumerQueues.TryGetValue(tag, out string? queue))
        {
            _consumerQueues.Remove(tag);
            _consumerEntries.Remove(tag);
            _engine.UnregisterConsumer(_vhost, queue, tag);
        }

        if (nowait)
        {
            return new ChannelOutcomeSilent();
        }

        return ChainReply("basic.cancel-ok", ok => ok.Set("consumer-tag", tag));
    }

    /// <summary>basic.get: the engine's atomic claim (existence → exclusive
    /// arbitration → head pop with the remaining count); the empty answer and the
    /// claimed get-ok + content both ride the writer chain. get is NEVER
    /// prefetch-refused (rabbit TODO 1594-1595), but its unacked entry counts
    /// toward the channel gate like any other delivery.</summary>
    private ChannelOutcome OnBasicGet(AmqpMethod method)
    {
        string queue = method.Get("queue") as string ?? string.Empty;
        bool noAck = method.Get("no-ack") is true;
        GetClaimOutcome outcome = _engine.TryClaimForGet(_vhost, queue, out InMemoryMessage? message, out int remaining);
        switch (outcome)
        {
            case GetClaimOutcome.QueueNotFound:
                return NotFound(kind: "queue", queue, method);

            case GetClaimOutcome.ExclusiveHeld:
                return Soft(
                    method,
                    AmqpConstants.ResourceLocked,
                    $"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{queue}' in vhost '{_vhost}'. " +
                    "It could be originally declared on another connection or the exclusive property " +
                    "value does not match that of the original declaration.");

            case GetClaimOutcome.Empty:
                // cluster-id "" — M0 is not a cluster (the field is the legacy remnant).
                return ChainReply("basic.get-empty", empty => empty.Set("cluster-id", string.Empty));

            default:
                break;
        }

        long tag;
        lock (_deliveryLock)
        {
            tag = ++_nextDeliveryTag;
            if (!noAck)
            {
                _unacked[tag] = new Unacked(tag, queue, message!);
                _outstandingBytes += message!.Body.LongLength;
            }
        }

        InMemoryMessage claimed = message!;
        ScheduleWriterJob(() => _host.WriteMethodToWireAsync(_channel, NewMethod("basic.get-ok", ok =>
        {
            ok.Set("delivery-tag", tag);
            ok.Set("redelivered", claimed.Redelivered);
            ok.Set("message-count", remaining);
            ok.Set("exchange", claimed.Exchange);
            ok.Set("routing-key", claimed.RoutingKey);
        }), default));
        ScheduleWriterJob(() => _host.WriteContentToWireAsync(_channel, claimed.Props, claimed.Body, default));
        return ChannelOutcomeChain.Instance;
    }

    /// <summary>The settlement triple (ack / reject / nack): collect the ledger
    /// entries under _deliveryLock — and release it BEFORE touching the engine, per
    /// the lock ladder (the pump takes _deliveryLock inside the engine gate) — then
    /// ack's drop settles the bytes free, reject/nack's requeue returns entries to
    /// their queues' ready tails. ack(0, multiple=true) acks everything outstanding
    /// (collect_acks 2112-2115); a missing single tag is the soft
    /// "PRECONDITION_FAILED - unknown delivery tag N" (collect_acks 2139, ids 60/80).
    /// The requeued subset requeues in delivery-tag order so a multi-requeue keeps
    /// the tail order stable. Credit freed → the channel's consumer queues are
    /// nudged (a pump idled out on the gate re-spawns).</summary>
    private ChannelOutcome OnBasicAck(AmqpMethod method) =>
        SettleLedger(method, method.Get("delivery-tag") as long? ?? 0L, method.Get("multiple") is true, requeue: false);

    private ChannelOutcome OnBasicReject(AmqpMethod method) =>
        SettleLedger(method, method.Get("delivery-tag") as long? ?? 0L, multiple: false, requeue: method.Get("requeue") is true);

    private ChannelOutcome OnBasicNack(AmqpMethod method) =>
        SettleLedger(method, method.Get("delivery-tag") as long? ?? 0L, method.Get("multiple") is true, requeue: method.Get("requeue") is true);

    private ChannelOutcome SettleLedger(AmqpMethod method, long tag, bool multiple, bool requeue)
    {
        List<Unacked> settled = [];
        lock (_deliveryLock)
        {
            if (multiple)
            {
                // collect_acks: tag 0 = the ledger's whole content; otherwise every
                // entry at or below the tag (existence of the tag itself is not owed).
                foreach ((long key, Unacked value) in _unacked)
                {
                    if (tag == 0 || key <= tag)
                    {
                        settled.Add(value);
                    }
                }

                foreach (Unacked entry in settled)
                {
                    _unacked.Remove(entry.Tag);
                }
            }
            else
            {
                if (!_unacked.TryGetValue(tag, out Unacked entry))
                {
                    return Soft(
                        method,
                        AmqpConstants.PreconditionFailed,
                        $"PRECONDITION_FAILED - unknown delivery tag {tag}");
                }

                _unacked.Remove(tag);
                settled.Add(entry);
            }

            foreach (Unacked entry in settled)
            {
                _outstandingBytes -= entry.Message.Body.LongLength;
            }
        }

        // Requeues land tail-first in tag order; and both paths free credit.
        settled.Sort((left, right) => left.Tag.CompareTo(right.Tag));
        if (requeue)
        {
            foreach (Unacked entry in settled)
            {
                _engine.Requeue(_vhost, entry.Queue, entry.Message);
            }
        }

        NudgeChannelQueues();
        return new ChannelOutcomeSilent();
    }

    /// <summary>recover(requeue=true) requeues THIS channel's whole unacked set,
    /// clearing the ledger at the handler (the client must not ack those tags
    /// anymore — recover settles them), scheduling recover-ok's write and then the
    /// requeue plan as a chain job so recover-ok leaves the wire first. requeue=false
    /// is RabbitMQ's hard not_implemented "requeue=false" (rabbit 1635-1636; it
    /// applies to recover-async too).</summary>
    private ChannelOutcome OnBasicRecover(AmqpMethod method)
    {
        if (method.Get("requeue") is not true)
        {
            return new ChannelOutcomeHardError(
                AmqpConstants.NotImplemented,
                "NOT_IMPLEMENTED - requeue=false",
                method.ClassId, method.MethodId);
        }

        List<Unacked> plan;
        lock (_deliveryLock)
        {
            plan = [.. _unacked.Values];
            plan.Sort(static (left, right) => left.Tag.CompareTo(right.Tag));
            _unacked.Clear();
            _outstandingBytes = 0;
        }

        // recover-ok's write goes first, the requeue plan second: the chain keeps the
        // order, so the reply is on the wire before any redelivery of the requeued set.
        ScheduleWriterJob(() => _host.WriteMethodToWireAsync(_channel, NewMethod("basic.recover-ok"), default));
        ScheduleWriterJob(async () =>
        {
            foreach (Unacked entry in plan)
            {
                _engine.Requeue(_vhost, entry.Queue, entry.Message);
            }

            await Task.CompletedTask;
        });

        return ChannelOutcomeChain.Instance;
    }

    /// <summary>The close/retire sweep: every consumer leaves the engine and every
    /// unacked delivery requeues with the redelivered flag (RabbitMQ's
    /// channel-cleanup). Before-recovery call-site (mourn/hard close/retire/
    /// connection end) — safe concurrently with the pump: the ledger snapshot is
    /// taken under _deliveryLock, the engine calls run after it is released.</summary>
    internal void TearDownActiveConsumers()
    {
        if (_consumersTornDown)
        {
            return;
        }

        _consumersTornDown = true;
        List<(string Queue, string Tag)> consumers;
        List<Unacked> plan;
        lock (_deliveryLock)
        {
            consumers = _consumerQueues.Select(pair => (pair.Value, pair.Key)).ToList();
            plan = _unacked.Values.ToList();
        }

        foreach ((string queue, string tag) in consumers)
        {
            _consumerQueues.Remove(tag);
            _consumerEntries.Remove(tag);
            _engine.UnregisterConsumer(_vhost, queue, tag);
        }

        plan.Sort((left, right) => left.Tag.CompareTo(right.Tag));
        foreach (Unacked entry in plan)
        {
            _engine.Requeue(_vhost, entry.Queue, entry.Message);
        }
    }

    // ---------- the serialized writer chain (Task 7) ----------

    /// <summary>Schedules one job on the channel's writer chain: pure synchronous
    /// enqueue, safe under the engine gate and _deliveryLock alike (the drains run
    /// elsewhere, so nothing here ever awaits).</summary>
    private void ScheduleWriterJob(Func<ValueTask> job)
    {
        lock (_writerLock)
        {
            _writerJobs.Enqueue(job);
        }
    }

    /// <summary>Drains the chain under the single-writer gate: every scheduled job
    /// runs, in schedule order, and late arrivals loop back in. Jobs are host writes
    /// or requeue plans — they surface cancellation and absorb transport death
    /// themselves, and the strict caller discipline (pump: gate released before the
    /// drain; frame pump: no engine lock held) keeps the writer-gate → engine-gate
    /// edge one-way.</summary>
    internal async Task RunWriterChainAsync(CancellationToken ct)
    {
        while (true)
        {
            await _writerGate.WaitAsync(ct);
            List<Func<ValueTask>> batch;
            lock (_writerLock)
            {
                batch = [.. _writerJobs];
                _writerJobs.Clear();
            }

            if (batch.Count == 0)
            {
                _writerGate.Release();
                return;
            }

            try
            {
                foreach (Func<ValueTask> job in batch)
                {
                    await job();
                }
            }
            finally
            {
                _writerGate.Release();
            }
        }
    }

    /// <summary>The consumer-family reply: scheduled on the chain (a pump claim can
    /// never jump a promise the channel already made) with the drain marker that
    /// dispatches it.</summary>
    private ChannelOutcome ChainReply(string name, Action<AmqpMethod>? setFields = null)
    {
        AmqpMethod reply = NewMethod(name, setFields);
        ScheduleWriterJob(() => _host.WriteMethodToWireAsync(_channel, reply, default));
        return ChannelOutcomeChain.Instance;
    }

    /// <summary>Credit frees on this channel (ack/nack/reject settle, qos raise): the
    /// queues this channel's consumers ride are nudged so an idled-out pump re-spawns.
    /// Ladder-safe: the snapshot is taken under _deliveryLock, the engine calls run
    /// after it is released.</summary>
    private void NudgeChannelQueues()
    {
        string[] queues;
        lock (_deliveryLock)
        {
            queues = [.. _consumerQueues.Values.Distinct()];
        }

        foreach (string queue in queues)
        {
            _engine.Nudge(_vhost, queue);
        }
    }

    /// <summary>The unacked-ledger entry: what a delivery-tag settles back to.</summary>
    private readonly record struct Unacked(long Tag, string Queue, InMemoryMessage Message);

    /// <summary>
    /// The channel-side consumer the engine's pump serves: the deliver admission
    /// (credit gate → ledger → writer job) under _deliveryLock, synchronous by
    /// contract because the pump holds the engine gate across the call.
    /// </summary>
    private sealed class ConsumerEntry(ChannelContext owner, string consumerTag, string queueName, bool noAck, bool exclusive) : IQueueConsumer
    {
        public string ConsumerTag => consumerTag;

        public bool IsExclusive => exclusive;

        public bool TryAdmitForDelivery(InMemoryMessage message)
        {
            long deliveryTag;
            lock (owner._deliveryLock)
            {
                if (noAck)
                {
                    // Auto-ack consumers ride past the gate: no ledger, no credit.
                    deliveryTag = ++owner._nextDeliveryTag;
                }
                else
                {
                    if (owner._prefetchCount > 0 && owner._unacked.Count >= owner._prefetchCount)
                    {
                        return false;
                    }

                    long bytes = message.Body.LongLength;
                    if (owner._prefetchSize > 0 && owner._outstandingBytes + bytes > owner._prefetchSize)
                    {
                        return false;
                    }

                    deliveryTag = ++owner._nextDeliveryTag;
                    owner._unacked[deliveryTag] = new Unacked(deliveryTag, queueName, message);
                    owner._outstandingBytes += bytes;
                }

                AmqpMethod deliver = ChannelContext.NewMethod("basic.deliver", deliverRecord =>
                {
                    deliverRecord.Set("consumer-tag", consumerTag);
                    deliverRecord.Set("delivery-tag", deliveryTag);
                    deliverRecord.Set("redelivered", message.Redelivered);
                    deliverRecord.Set("exchange", message.Exchange);
                    deliverRecord.Set("routing-key", message.RoutingKey);
                });

                owner.ScheduleWriterJob(() => owner._host.WriteDeliveryToWireAsync(
                    owner._channel, deliver, message.Props, message.Body, default));
            }

            return true;
        }

        /// <summary>The pump drain: outside the engine gate, tokenless (a cancelled
        /// transport surfaces through the write itself, and the pump catches).</summary>
        public Task DrainDeliveryWritesAsync() => owner.RunWriterChainAsync(CancellationToken.None);
    }




    private ChannelOutcome NotSupportedArguments(AmqpMethod method) =>
        new ChannelOutcomeHardError(
            AmqpConstants.NotImplemented,
            $"NOT_IMPLEMENTED - arguments are not supported in M0 for {method.Schema.Name}",
            method.ClassId, method.MethodId);

    private ChannelOutcome DefaultExchangeRefused(AmqpMethod method) =>
        Soft(method, AmqpConstants.AccessRefused, "ACCESS_REFUSED - operation not permitted on the default exchange");

    private ChannelOutcome NotFound(string kind, string name, AmqpMethod method) =>
        Soft(method, AmqpConstants.NotFound, $"NOT_FOUND - no {kind} '{name}' in vhost '{_vhost}'");

    /// <summary>A successful declare-ok; <paramref name="updateMrdq"/> records the
    /// most-recently-declared-queue hint for every successful declare, nowait
    /// included (RabbitMQ's return_queue_declare_ok).</summary>
    private ChannelOutcome DeclareOk(bool nowait, string replyName, string name, string? updateMrdq = null)
    {
        if (updateMrdq is not null)
        {
            _mostRecentlyDeclaredQueue = updateMrdq;
        }

        if (nowait)
        {
            return new ChannelOutcomeSilent();
        }

        return new ChannelOutcomeReply(replyName == "queue.declare-ok"
            ? NewMethod(replyName, ok =>
            {
                ok.Set("queue", name);
                ok.Set("message-count", 0);
                ok.Set("consumer-count", 0);
            })
            : NewMethod(replyName));
    }

    private ChannelOutcome Soft(AmqpMethod method, ushort code, string text) =>
        new ChannelOutcomeSoftError(code, text, method.ClassId, method.MethodId);

    /// <summary>Renders flag values as Erlang writes them into an inequivalent-arg
    /// explanation: the booleans 'true'/'false'.</summary>
    private static string ErlangAtom(object value) =>
        value is bool boolean ? (boolean ? "true" : "false") : value.ToString() ?? "";

    private static AmqpMethod NewMethod(string name, Action<AmqpMethod>? setFields = null)
    {
        if (!MethodRegistry.TryGetByName(name, out var schema))
        {
            throw new InvalidOperationException($"Registry is missing {name} (Task 3 guarantee)");
        }

        var method = new AmqpMethod(schema!);
        setFields?.Invoke(method);
        return method;
    }
}
