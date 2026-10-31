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

    internal ChannelContext(string vhost, IRouteEngine engine)
    {
        _vhost = vhost;
        _engine = engine;
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
            (60, 40) => OnBasicPublish(method),
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

    // ---------- shared shapes ----------

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
