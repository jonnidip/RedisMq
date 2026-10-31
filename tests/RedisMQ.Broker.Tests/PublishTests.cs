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

// Publish path (Task 6): content assembly (header → N body frames per channel), the
// direct fan-out onto InMemoryEngine, and the close semantics around them — same
// "evil client" harness shape as ChannelTopologyTests: raw frames over an in-proc
// TransportPair wired into an AmqpBrokerServer running the REAL InMemoryEngine, then
// PeekQueue introspection. Pinned by concrete wire semantics read out of the
// RabbitMQ server sources (master, 4.x):
//
//   rabbit_command_assembler.erl — the three unexpected_frame wordings (none /
//                             pending basic.publish method ids), the body-overshoot
//                             and empty-body-frame frame_error wordings, and the
//                             declared-size checks running at header arrival.
//   rabbit_channel.erl      — handle_method's protocol_error(not_implemented,
//                             "immediate=true") before exchange lookup (it receives
//                             the ALREADY assembled content), lookup_or_die after
//                             content, and process_routing_mandatory:
//                             mandatory + unroutable → basic_return(no_route)
//                             carrying the full content, mandatory=false → drop.
//   rabbit_exchange.erl     — the default-exchange route clause: the queue resource
//                             named by the routing key directly, no binding table;
//                             absent queue = unroutable, never not-found.
//   rabbit_binary_generator — soft codes channel.close on that channel, hard codes
//                             connection.close on channel 0.
//
// The M0 plan deliberately pins some shapes that differ from RabbitMQ 4.4 reality;
// those divergences are recorded in the Task 6 report (channel-level unexpected-frame
// for sequence violations, the hard content-too-large close). No mocks beyond the
// transport — the codec under test produces and consumes the same bytes on both ends.
public sealed class PublishTests
{
    private const string JsonContentType = "application/json";

    // ---------- direct publishing onto the engine ----------

    [Fact]
    public async Task Direct_publish_through_the_default_exchange_lands_in_the_queue()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41); // connection.open-ok
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await DeclareQueueAsync(client, 1, "orders");

            // basic.publish: exchange "", routing key "orders" → the queue NAMED
            // "orders" (the default exchange's route clause), then header + TWO
            // body frames on the same channel.
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "orders", mandatory: false)));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 11, new ContentProperties(
                ContentType: "text/plain",
                DeliveryMode: 2)));
            await client.SendAsync(BodyFrameOn(1, "hello"u8.ToArray()));
            await client.SendAsync(BodyFrameOn(1, " world"u8.ToArray()));

            // The passive redeclare answers only after every earlier channel-1 frame
            // was dispatched (single frame pump, in-order) — a deterministic fence.
            await DeclareQueueAsync(client, 1, "orders", passive: true);

            IReadOnlyList<InMemoryMessage> messages = engine.PeekQueue("orders");
            Assert.Single(messages);
            InMemoryMessage landed = messages[0];
            Assert.Equal(1UL, landed.EnvelopeId); // first enqueue ever on the engine == 1
            Assert.Equal("", landed.Exchange);
            Assert.Equal("orders", landed.RoutingKey);
            Assert.Equal("text/plain", landed.Props.ContentType);
            Assert.Equal((byte)2, landed.Props.DeliveryMode);
            Assert.Equal("hello world"u8.ToArray(), landed.Body);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Direct_publish_fans_out_to_bound_queues_in_insertion_order_with_distinct_envelope_ids()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await DeclareExchangeAsync(client, 1, "fan");
            await DeclareQueueAsync(client, 1, "fan-a");
            await DeclareQueueAsync(client, 1, "fan-b");
            await BindQueueAsync(client, 1, "fan-a", "fan", "rk");
            await BindQueueAsync(client, 1, "fan-b", "fan", "rk");

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "fan", "rk")));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 4, new ContentProperties(ContentType: JsonContentType)));
            await client.SendAsync(BodyFrameOn(1, "abcd"u8.ToArray()));

            await DeclareQueueAsync(client, 1, "fan-a", passive: true); // fence

            IReadOnlyList<InMemoryMessage> first = engine.PeekQueue("fan-a");
            IReadOnlyList<InMemoryMessage> second = engine.PeekQueue("fan-b");
            Assert.Single(first);
            Assert.Single(second);
            // Fan-out enqueues once per bound queue in binding-insertion order, and
            // every copy gets its own envelope id (1, 2, … per engine).
            Assert.Equal(1UL, first[0].EnvelopeId);
            Assert.Equal(2UL, second[0].EnvelopeId);
            Assert.Equal("fan", first[0].Exchange);
            Assert.Equal("rk", first[0].RoutingKey);
            Assert.Equal(JsonContentType, first[0].Props.ContentType);
            Assert.Equal("abcd"u8.ToArray(), first[0].Body);
            Assert.Equal(JsonContentType, second[0].Props.ContentType);
            Assert.Equal("abcd"u8.ToArray(), second[0].Body);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- basic.return for mandatory unroutables ----------

    [Fact]
    public async Task Mandatory_unroutable_publish_returns_the_message_on_the_channel()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // No queue named "nowhere": mandatory + unroutable → basic.return.
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "nowhere", mandatory: true)));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 3, new ContentProperties(ContentType: JsonContentType)));
            await client.SendAsync(BodyFrameOn(1, "abc"u8.ToArray()));

            AmqpMethod basicReturn = await client.ReadMethodOnChannelAsync(1, 60, 50);
            Assert.Equal((short)312, basicReturn.Get("reply-code")); // NO_ROUTE
            Assert.Equal("NO_ROUTE", basicReturn.Get("reply-text"));
            Assert.Equal("", basicReturn.Get("exchange"));
            Assert.Equal("nowhere", basicReturn.Get("routing-key"));

            // The return carries the content back: header (the declared properties)
            // plus the body — 3 bytes fit the negotiated frame-max, so exactly one
            // body frame, and exchange"" was returned (mandatory=false is silent).
            (ContentHeader header, byte[] body) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal(3, header.BodySize);
            Assert.Equal(JsonContentType, header.Properties.ContentType);
            Assert.Equal("abc"u8.ToArray(), body);
            Assert.Empty(engine.PeekQueue("probe-queue"));

            // The channel survived the return: a fresh declare answers, and nothing
            // leaked into storage.
            await DeclareQueueAsync(client, 1, "probe-queue");
            Assert.Empty(engine.PeekQueue("probe-queue"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Unroutable_mandatory_false_publish_is_silently_dropped()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await DeclareQueueAsync(client, 1, "probe-queue");

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "nowhere", mandatory: false)));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 3, new ContentProperties(ContentType: JsonContentType)));
            await client.SendAsync(BodyFrameOn(1, "abc"u8.ToArray()));

            // Fence: no basic.return, no close — the next channel-1 method answers.
            await DeclareQueueAsync(client, 1, "probe-queue", passive: true);

            Assert.Empty(engine.PeekQueue("probe-queue")); // inbox untouched by the drop
            Assert.Empty(engine.PeekQueue("nowhere"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- assembler violations while a publish is open (channel-level) ----------

    [Fact]
    public async Task Method_frame_while_a_publish_awaits_its_header_closes_the_channel_with_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "queue-untouched");

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "queue-untouched")));
            // A second publish method (or any method frame) intrudes while the
            // content header is still owed:
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "queue-untouched")));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal(
                "UNEXPECTED_FRAME - expected content header for class 60, got non content header frame instead",
                close.text);
            // The pending content-bearing method is basic.publish (60,40).
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);

            // The channel mourns; close-ok retires the number and nothing was stored.
            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
            Assert.Empty(engine.PeekQueue("queue-untouched"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Body_frame_while_a_publish_awaits_its_header_closes_the_channel_with_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "queue-untouched");

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "queue-untouched")));
            // Body where the header was owed:
            await client.SendAsync(BodyFrameOn(1, "stray"u8.ToArray()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal(
                "UNEXPECTED_FRAME - expected content header for class 60, got non content header frame instead",
                close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);

            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));

            // The number retires and a fresh channel works again; nothing was stored.
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "queue-untouched", passive: true);
            Assert.Empty(engine.PeekQueue("queue-untouched"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Method_frame_while_content_body_is_incomplete_closes_the_channel_with_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "queue-untouched");

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "queue-untouched")));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 4, new ContentProperties()));
            // A method frame where body bytes are owed:
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "other", durable: false)));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal(
                "UNEXPECTED_FRAME - expected content body, got non content body frame instead",
                close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);

            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
            Assert.Empty(engine.PeekQueue("queue-untouched"));
            Assert.Empty(engine.PeekQueue("other")); // the intruding declare never ran
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Extra_content_after_the_body_completed_closes_the_channel_with_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "landed");

            // This publish completes and lands:
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "landed")));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 2, new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, "ok"u8.ToArray()));
            await DeclareQueueAsync(client, 1, "landed", passive: true); // fence
            Assert.Single(engine.PeekQueue("landed"));

            // A late body frame is back in the expect-method state:
            await client.SendAsync(BodyFrameOn(1, "again"u8.ToArray()));
            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal(
                "UNEXPECTED_FRAME - expected method frame, got non method frame instead",
                close.text);
            // No pending method at that point — RabbitMQ's `none` ids.
            Assert.Equal(0, close.classId);
            Assert.Equal(0, close.methodId);

            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
            Assert.Single(engine.PeekQueue("landed")); // the completed publish is unaffected
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Stray_body_frame_on_an_idle_running_channel_closes_the_channel_with_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(BodyFrameOn(1, "idling"u8.ToArray()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal(
                "UNEXPECTED_FRAME - expected method frame, got non method frame instead",
                close.text);
            Assert.Equal(0, close.classId);
            Assert.Equal(0, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- the hard content cap ----------

    [Fact]
    public async Task Declared_body_over_the_hard_cap_closes_the_connection_with_content_too_large()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // The check sits at header arrival: declared size over the cap dies
            // before a single body byte need exist.
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "q")));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: ChannelContext.MaxBodySize + 1, new ContentProperties()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.ContentTooLarge, close.code);
            Assert.Equal(
                $"CONTENT_TOO_LARGE - content body size {ChannelContext.MaxBodySize + 1} " +
                $"exceeds maximum allowed size {ChannelContext.MaxBodySize}",
                close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Declared_body_at_the_hard_cap_is_accepted_without_a_close()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(2, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(2, 20, 11);

            // The cap itself must not trip content-too-large: the declared body is
            // simply incomplete (never finished), so nothing errors and nothing lands.
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "never", mandatory: false)));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: ChannelContext.MaxBodySize, new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, "tiny"u8.ToArray()));

            // Fence on channel 2: the pump demonstrably moved past every channel-1
            // frame without a connection close having gone out.
            await DeclareQueueAsync(client, 2, "fence");

            Assert.Empty(engine.PeekQueue("never")); // incomplete → nothing stored
            Assert.Empty(engine.PeekQueue("fence"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- exchange resolution timing ----------

    [Fact]
    public async Task Publishing_to_an_unknown_exchange_is_not_found_only_after_the_content_completes()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(2, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(2, 20, 11);

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "ghost-ex", "rk")));

            // Fence on channel 2 BEFORE the content: a not-found raised at method
            // arrival would arrive on channel 1 ahead of this reply and fail the
            // channel-discriminating read. RabbitMQ only resolves the exchange
            // lookup_or_die after the full content assembly.
            await DeclareQueueAsync(client, 2, "fence");

            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 3, new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, "abc"u8.ToArray()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotFound, close.code);
            Assert.Equal("NOT_FOUND - no exchange 'ghost-ex' in vhost '/'", close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- RabbitMQ-verified extras ----------

    [Fact]
    public async Task Immediate_publish_closes_the_connection_with_not_implemented()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // rabbit_channel: handle_method(#'basic.publish'{immediate = true},
            // _Content, ...) → protocol_error(not_implemented, "immediate=true");
            // it receives the already-assembled content, so the close comes after
            // the body — and it is a HARD error (540 exits on channelless close).
            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "q", immediate: true)));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 3, new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, "abc"u8.ToArray()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.Equal("NOT_IMPLEMENTED - immediate=true", close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Empty_body_frame_while_content_is_incomplete_closes_the_connection_with_frame_error()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "q")));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 9, new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, Array.Empty<byte>()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.FrameError, close.code);
            Assert.Equal(
                "FRAME_ERROR - empty content body frame while content body is incomplete",
                close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Body_frame_overshooting_the_declared_size_closes_the_connection_with_frame_error()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, PublishFrame(1, "", "q")));
            await client.SendAsync(ContentHeaderFrameOn(1, bodySize: 2, new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, "abc"u8.ToArray())); // 3 > 2 remaining

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.FrameError, close.code);
            Assert.Equal(
                "FRAME_ERROR - content body frame exceeds remaining content size",
                close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(40, close.methodId);
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

    /// <summary>The tuned handshake the publish tests start from; heartbeats are
    /// disabled via TuneOk so no watchdog ever fires mid-scenario.</summary>
    private static async Task HandshakeAsync(WireClient client)
    {
        await SendHeaderAsync(client);
        _ = await client.ReadMethodAsync(10, 10); // connection.start
        await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
        _ = await client.ReadMethodAsync(10, 30); // connection.tune
        await client.SendAsync(TuneOkFrame(2047, 131072, 0));
        await client.SendAsync(OpenFrame("/"));
    }

    // ---------- evil-client wire helpers ----------

    /// <summary>Bytes of the AMQP 0-9-1 protocol header: "AMQP" + 0x00 0x00 0x09 0x01.</summary>
    private static byte[] ProtocolHeaderBytes => "AMQP\x00\x00\x09\x01"u8.ToArray();

    private static Task SendHeaderAsync(WireClient client) => client.SendAsync(ProtocolHeaderBytes);

    private static byte[] PlainResponse(string username, string password) =>
        // PLAIN: authzid NUL authcid NUL password.
        Encoding.UTF8.GetBytes($"\0{username}\0{password}");

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

    private static AmqpMethod ChannelCloseOkFrame()
    {
        AmqpMethod method = NewMethod("channel.close-ok");
        return method;
    }

    private static AmqpMethod QueueDeclareFrame(ushort channel, string queue, bool durable = false, bool passive = false)
    {
        AmqpMethod method = NewMethod("queue.declare");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("passive", passive);
        method.Set("durable", durable);
        method.Set("exclusive", false);
        method.Set("auto-delete", false);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod ExchangeDeclareFrame(ushort channel, string exchange, bool durable)
    {
        AmqpMethod method = NewMethod("exchange.declare");
        method.Set("ticket", (short)0);
        method.Set("exchange", exchange);
        method.Set("type", "direct");
        method.Set("passive", false);
        method.Set("durable", durable);
        method.Set("auto-delete", false);
        method.Set("internal", false);
        method.Set("nowait", false);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod QueueBindFrame(ushort channel, string queue, string exchange, string routingKey)
    {
        AmqpMethod method = NewMethod("queue.bind");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("exchange", exchange);
        method.Set("routing-key", routingKey);
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

    private static AmqpMethod NewMethod(string name) => MethodRegistry.TryGetByName(name, out var schema)
        ? new AmqpMethod(schema!)
        : throw new InvalidOperationException($"Registry is missing {name} (Task 3 guarantee)");

    /// <summary>Frames a method object on the given channel: the payload is class-id +
    /// method-id + arguments, sliced from MethodCodec's encoding.</summary>
    private static byte[] MethodFrameOn(ushort channel, AmqpMethod method)
    {
        var payloadWriter = new ByteWriter();
        MethodCodec.WriteMethod(ref payloadWriter, method);
        byte[] payload = payloadWriter.ToArray();
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, channel, method.ClassId, method.MethodId, payload.AsSpan(4));
        return frame.ToArray();
    }

    private static byte[] ContentHeaderFrameOn(ushort channel, long bodySize, ContentProperties properties)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteContentHeader(ref frame, channel, classId: 60, bodySize, properties);
        return frame.ToArray();
    }

    /// <summary>Hand-rolls one body frame — needed precisely because
    /// <see cref="FrameWriter.WriteBody"/> emits no frames at all for an empty body,
    /// and an empty body frame is exactly what some of these scenarios must send.</summary>
    private static byte[] BodyFrameOn(ushort channel, byte[] payload)
    {
        var frame = new ByteWriter();
        frame.WriteOctet(AmqpFrame.FrameBody);
        frame.WriteUShort(channel);
        frame.WriteLong(payload.Length);
        frame.WriteRaw(payload);
        frame.WriteOctet(AmqpFrame.FrameEnd);
        return frame.ToArray();
    }

    private static Task DeclareQueueAsync(WireClient client, ushort channel, string queue, bool passive = false) =>
        DeclareAsync(client, channel, QueueDeclareFrame(channel, queue, passive: passive), (50, 11));

    private static Task DeclareExchangeAsync(WireClient client, ushort channel, string exchange) =>
        DeclareAsync(client, channel, ExchangeDeclareFrame(channel, exchange, durable: true), (40, 11));

    private static Task BindQueueAsync(WireClient client, ushort channel, string queue, string exchange, string routingKey) =>
        DeclareAsync(client, channel, QueueBindFrame(channel, queue, exchange, routingKey), (50, 21));

    private static async Task DeclareAsync(WireClient client, ushort channel, AmqpMethod method, (ushort Class, ushort Method) okId)
    {
        await client.SendAsync(MethodFrameOn(channel, method));
        _ = await client.ReadMethodOnChannelAsync(channel, okId.Class, okId.Method);
    }

    /// <summary>Reads the server's channel.close on the given channel (soft error
    /// close: class 20 method 40), returning all four fields.</summary>
    private static async Task<(ushort Code, string Text, ushort ClassId, ushort MethodId)> ReadChannelCloseAsync(
        WireClient client, ushort channel)
    {
        AmqpMethod close = await client.ReadMethodOnChannelAsync(channel, 20, 40);
        return (
            (ushort)(short)close.Get("reply-code")!, // wire type is short-int, always positive here
            (string)close.Get("reply-text")!,
            (ushort)(short)close.Get("class-id")!,
            (ushort)(short)close.Get("method-id")!);
    }

    /// <summary>Reads the server's hard-error connection.close on channel 0.</summary>
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

    /// <summary>Stops the server and disposes the client, so a failing scenario never
    /// dangles past the assertion.</summary>
    private static async Task ShakeDownAsync(AmqpBrokerServer server, WireClient client)
    {
        await client.DisposeAsync();
        await server.StopAsync();
    }

    // ---------- test-local pieces (the brief's injected surface) ----------

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

    /// <summary>
    /// The client half of a TransportPair: sends raw byte frames and decodes the
    /// real bytes the broker produces through the FrameDecoder, keeping undecodable
    /// partials across calls exactly as a real socket client would.
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

        public ValueTask DisposeAsync() => side.CompleteAsync();

        /// <summary>Reads the next method frame of any (class, method) pair on channel 0.</summary>
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

        /// <summary>Channel-discriminating read: only method frames on the given
        /// channel count; a matching (or wildcard) reply somewhere else is a
        /// cross-talk failure, never silently consumed.</summary>
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

        /// <summary>Reads a complete content sequence on the channel: one header frame
        /// then body frames until the declared size is reached. A method frame in
        /// between is a failure, not a skip.</summary>
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

            // Copy the bytes out BEFORE consuming them: advancing the reader first
            // would invalidate the sequence positions we are about to read from.
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
    }
}

// Engine-level unit tests for the publish-side storage seam (Task 6): Enqueue and
// PeekQueue — no wire, the InMemoryEngine under test directly, exactly like the
// Task 5 InMemoryRouteEngineTests class in ChannelTopologyTests.cs.
public sealed class PublishStoreTests
{
    private static readonly string[] Vhosts = ["/"];

    [Fact]
    public void Enqueue_assigns_monotonic_envelope_ids_and_records_the_origin()
    {
        var engine = new InMemoryEngine(Vhosts);
        var props = new ContentProperties(ContentType: "text/plain");

        engine.Enqueue("/", "q-first", props, [1, 2, 3], exchange: "x", routingKey: "rk-a");
        engine.Enqueue("/", "q-first", props, [4], exchange: "x", routingKey: "rk-b");
        engine.Enqueue("/", "q-second", props, [5, 6], exchange: "y", routingKey: "rk-c");

        IReadOnlyList<InMemoryMessage> first = engine.PeekQueue("q-first");
        IReadOnlyList<InMemoryMessage> second = engine.PeekQueue("q-second");
        Assert.Equal(2, first.Count);
        Assert.Single(second);

        // Ids are engine-global and assigned in enqueue order: 1, 2, 3.
        Assert.Equal(1UL, first[0].EnvelopeId);
        Assert.Equal(2UL, first[1].EnvelopeId);
        Assert.Equal(3UL, second[0].EnvelopeId);

        Assert.Equal("x", first[0].Exchange);
        Assert.Equal("rk-a", first[0].RoutingKey);
        Assert.Equal([4], first[1].Body);
        Assert.Equal("y", second[0].Exchange);
        Assert.Equal("rk-c", second[0].RoutingKey);
    }

    [Fact]
    public void PeekQueue_returns_a_snapshot_the_engine_does_not_share()
    {
        var engine = new InMemoryEngine(Vhosts);
        var props = new ContentProperties(ContentType: "text/plain");
        engine.Enqueue("/", "q", props, [9, 9, 9], exchange: "", routingKey: "q");

        IReadOnlyList<InMemoryMessage> snapshot = engine.PeekQueue("q");
        Assert.Single(snapshot);
        Assert.Equal([9, 9, 9], snapshot[0].Body);

        // Mutating the snapshot (its list and its body bytes) cannot reach the store:
        ((List<InMemoryMessage>)snapshot).Add(new InMemoryMessage(props, [1], "", "q", 999));
        snapshot[0].Body[0] = 0xFF;
        IReadOnlyList<InMemoryMessage> again = engine.PeekQueue("q");
        Assert.Single(again);
        Assert.Equal([9, 9, 9], again[0].Body);
    }

    [Fact]
    public void PeekQueue_of_an_unknown_queue_is_empty()
    {
        var engine = new InMemoryEngine(Vhosts);
        Assert.Empty(engine.PeekQueue("never-declared"));
    }
}
