namespace RedisMQ.Broker.Server;

using System;
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

/// <summary>No bytes on the wire at all — the mourning catch-all's discard and the
/// close-ok of a completed server-initiated close; <paramref name="retireChannel"/>
/// frees the channel number for a fresh channel.open.</summary>
public sealed record ChannelOutcomeSilent(bool RetireChannel = false) : ChannelOutcome;

/// <summary>Soft error (311/403/404/405/406): channel.close carrying the fields on the
/// offending channel; the channel lives on in the mourning state.</summary>
public sealed record ChannelOutcomeSoftError(ushort Code, string Text, ushort ClassId, ushort MethodId) : ChannelOutcome;

/// <summary>Hard error (501/502/503/504/505/540): connection.close on channel 0, reply
/// class/method of the failed method for method-bodied errors (0,0 for FSM/reader ones).</summary>
public sealed record ChannelOutcomeHardError(ushort Code, string Text, ushort ClassId, ushort MethodId) : ChannelOutcome;

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
/// </list>
/// Activity such as publish, consume, acks, and actual storage is Task 6+; M0
/// carries only the pending-content phase hook a content dispatch decides with.
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

    /// <summary>The per-channel content assembly phase (rabbit_command_assembler).
    /// M0 channels never open a content sequence, so it is always ExpectMethod; the
    /// enum exists so Task 6's assembler fits in without reshaping the dispatch.</summary>
    internal enum ContentPhase
    {
        ExpectMethod,
        ExpectHeader,
        ExpectBody,
    }

    private readonly IRouteEngine _engine;
    private readonly string _vhost;
    private ChannelState _state = ChannelState.Starting;

    /// <summary>RabbitMQ's most_recently_declared_queue "" shortcut target; empty
    /// string means "no successful declare happened on this channel yet".</summary>
    private string _mostRecentlyDeclaredQueue = string.Empty;
    private ContentPhase _pendingContentPhase = ContentPhase.ExpectMethod;

    internal ChannelContext(string vhost, IRouteEngine engine)
    {
        _vhost = vhost;
        _engine = engine;
    }

    public bool IsMourning => _state == ChannelState.Mourning;

    /// <summary>The channel's content assembler phase; M0 keeps it parked at
    /// ExpectMethod, which is what content dispatch actually consults.</summary>
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
            _ => new ChannelOutcomeHardError(
                AmqpConstants.NotImplemented,
                $"NOT_IMPLEMENTED - {method.Schema.Name} ({classId},{methodId}) is not part of the M0 channel method set",
                classId, methodId),
        };
    }

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
