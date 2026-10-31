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

// Channel FSM + in-memory topology (Task 5): the same "evil client" harness shape as
// ConnectionFsmTests — raw frames over an in-proc TransportPair wired into an
// AmqpBrokerServer running the REAL InMemoryEngine — pinned by concrete wire
// semantics read out of the RabbitMQ server sources (master, 4.x):
//
//   rabbit_channel.erl      — channel FSM clauses (starting/running/closing),
//                             "second 'channel.open' seen" and "expected
//                             'channel.open'" as channel_error (hard 504),
//                             channel.flow(true) → flow-ok(true) with
//                             flow(false) → not_implemented "active=false",
//                             a mourning (server channel.close) channel that
//                             discards traffic and repeats close-ok, declare/
//                             bind fan-out and return semantics,
//                             most_recently_declared_queue ("" queue shortcut).
//   rabbit_exchange.erl     — check_type ("unknown exchange type") and
//                             assert_equivalence order type→durable→
//                             auto-delete→internal.
//   rabbit_amqqueue.erl     — assert_equivalence order exclusive-access(durably,
//                             resource_locked wording first)→durable→auto-delete,
//                             not_found "no {kind} '{name}' in vhost '{vhost}'".
//   rabbit_reader.erl       — per-channel command assembly, heartbeat on a
//                             non-zero channel → unexpected_frame ("type N, all
//                             octets = ...") anywhere, channel.close_ok retiring
//                             the channel number so it can be re-opened.
//   rabbit_command_assembler.erl — "expected method frame, got non method
//                             frame instead" when content arrives with no
//                             pending content-bearing method.
//   rabbit_binary_generator.erl  — map_exception: soft codes close the channel
//                             (channel.close on that channel), hard codes close
//                             the connection (connection.close on channel 0).
//
// Soft codes (channel close): 311/403/404/405/406. Hard codes (connection close):
// 320/402/501/502/503/504/505/506/530/540/541. No mocks beyond the transport —
// the codec under test produces and consumes the same bytes on both ends.
public sealed class ChannelTopologyTests
{
    // ---------- (a) two channels on one connection ----------

    [Fact]
    public async Task Channel_open_ok_arrives_on_the_opening_channel_and_channels_coexist()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41); // connection.open-ok

            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            AmqpMethod openOk = await client.ReadMethodOnChannelAsync(1, 20, 11);
            byte[] channelId = Assert.IsType<byte[]>(openOk.Get("channel-id"));
            Assert.Empty(channelId); // RabbitMQ's open-ok carries an empty channel-id

            // A second channel coexists on the same connection, replies in its own channel.
            await client.SendAsync(MethodFrameOn(2, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(2, 20, 11);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (b) happy exchange.declare + queue.declare ----------

    [Fact]
    public async Task Exchange_and_queue_declare_happy_paths_reply_ok_on_the_sending_channel()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11); // exchange.declare-ok

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: true)));
            AmqpMethod declareOk = await client.ReadMethodOnChannelAsync(1, 50, 11);
            Assert.Equal("q", declareOk.Get("queue"));
            Assert.Equal(0, declareOk.Get("message-count"));
            Assert.Equal(0, declareOk.Get("consumer-count"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (c) redeclare mismatch → channel.close(precondition-failed) ----------

    [Fact]
    public async Task Redeclare_with_mismatched_durable_flag_closes_the_channel_with_precondition_failed()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: false)));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.PreconditionFailed, close.code);
            Assert.Equal(
                "PRECONDITION_FAILED - inequivalent arg 'durable' for exchange 'ex' in vhost '/': " +
                "received 'false' but current is 'true'",
                close.text);

            // The client's close-ok retires the number; both connection and number survive.
            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // The declared exchange outlived the closed channel: a passive re-declare succeeds.
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true, passive: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Queue_redeclare_with_mismatched_durable_flag_is_an_inequivalent_arg()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: false)));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.PreconditionFailed, close.code);
            Assert.Equal(
                "PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'q' in vhost '/': " +
                "received 'false' but current is 'true'",
                close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Queue_redeclare_with_mismatched_exclusive_flag_closes_with_resource_locked_first()
    {
        // RabbitMQ's queue equivalence runs exclusive-access first (resource_locked
        // 405 wording) before durable/auto-delete — pinned with BOTH flags changed:
        // the 405 must arrive, not the 406 the durable mismatch would produce.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: false, exclusive: true)));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.ResourceLocked, close.code);
            Assert.Equal(
                "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue 'q' in vhost '/'. " +
                "It could be originally declared on another connection or the exclusive property " +
                "value does not match that of the original declaration.",
                close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- passive declares and not-found routing ----------

    [Fact]
    public async Task Passive_declare_of_unknown_queue_is_not_found()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "missing", passive: true)));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotFound, close.code);
            Assert.Equal("NOT_FOUND - no queue 'missing' in vhost '/'", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Passive_declare_of_unknown_exchange_is_not_found()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "missing", durable: true, passive: true)));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotFound, close.code);
            Assert.Equal("NOT_FOUND - no exchange 'missing' in vhost '/'", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (d) bind/unbind ----------

    [Fact]
    public async Task Queue_bind_unbind_and_absent_unbind_replay_ok()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);

            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "q", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 21); // queue.bind-ok

            // A duplicate binding is a no-op, not an error.
            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "q", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 21);

            await client.SendAsync(MethodFrameOn(1, QueueUnbindFrame(1, "q", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 51); // queue.unbind-ok

            // Unbinding an absent binding is a success (RabbitMQ semantics).
            await client.SendAsync(MethodFrameOn(1, QueueUnbindFrame(1, "q", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 51);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Bind_to_a_missing_exchange_or_queue_is_not_found()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(2, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(2, 20, 11);
            await client.SendAsync(MethodFrameOn(2, QueueDeclareFrame(2, "q", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(2, 50, 11);

            // ch1: the exchange side is missing (checked first).
            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "q", "missing", "rk")));
            (ushort code1, string text1) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotFound, code1);
            Assert.Equal("NOT_FOUND - no exchange 'missing' in vhost '/'", text1);

            // ch2: the exchange exists, the queue does not.
            await client.SendAsync(MethodFrameOn(2, ExchangeDeclareFrame(2, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(2, 40, 11);
            await client.SendAsync(MethodFrameOn(2, QueueBindFrame(2, "missing", "ex", "rk")));
            (ushort code2, string text2) = await ReadChannelCloseAsync(client, 2);
            Assert.Equal(AmqpConstants.NotFound, code2);
            Assert.Equal("NOT_FOUND - no queue 'missing' in vhost '/'", text2);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (e) method on a closed channel: both wire-real readings ----------

    [Fact]
    public async Task Method_on_a_mourning_channel_is_discarded_and_the_connection_survives()
    {
        // Brief (e) reading A (RabbitMQ-true): after the server sent channel.close,
        // the mourning channel silently discards further traffic and repeats
        // close-ok for a repeated channel.close; the connection stays alive.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "amq.reserved", durable: true)));
            _ = await ReadChannelCloseAsync(client, 1); // mourning on channel 1

            // Discarded traffic on the mourning channel gets no reply at all: if the
            // server wrongly answered, the strict reads below would mismatch.
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "discarded", durable: true)));

            // channel.close during mourning → close-ok again, channel still closing.
            await client.SendAsync(MethodFrameOn(1, ClientChannelCloseFrame(1)));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 41);

            // The connection itself survives the channel-level error.
            await client.SendAsync(
                MethodFrameOn(0, 10, 50, CloseArgsBytes(200, "bye")));
            _ = await client.ReadMethodAsync(10, 51); // connection.close-ok
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Method_on_a_retired_channel_closes_the_connection_with_channel_error()
    {
        // Brief (e) reading B (RabbitMQ-true): once the client closed the channel
        // cleanly, the number is retired; a further method on it starts a fresh
        // starting-state channel that only accepts channel.open → hard 504.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ClientChannelCloseFrame(1)));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 41);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.ChannelError, close.code);
            Assert.Equal("CHANNEL_ERROR - expected 'channel.open'", close.text);
            Assert.Equal((ushort)0, close.classId); // reader-raised, no failed method
            Assert.Equal((ushort)0, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- channel FSM hard errors ----------

    [Fact]
    public async Task Second_channel_open_on_a_running_channel_is_a_hard_channel_error()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.ChannelError, close.code);
            Assert.Equal("CHANNEL_ERROR - second 'channel.open' seen", close.text);
            Assert.Equal((ushort)0, close.classId);
            Assert.Equal((ushort)0, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Channel_close_from_the_client_replies_close_ok_and_the_number_is_reusable()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ClientChannelCloseFrame(1)));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 41);

            // rabbit_reader frees the channel number once close-ok is seen, so the
            // client may reopen it (a fresh starting-state channel).
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // And the connection keeps serving other methods afterwards.
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (f) interleave: no cross-talk ----------

    [Fact]
    public async Task Interleaved_declares_between_two_channels_do_not_cross_talk()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(2, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(2, 20, 11);

            // Two declares in flight at once, one per channel. The M0 pump answers
            // serially, so the replies arrive in processing order — but each reply
            // must stay on its own channel: the strict channel discriminator fails
            // any cross-channel reply here, which is the no-cross-talk contract.
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex1", durable: true)));
            await client.SendAsync(MethodFrameOn(2, ExchangeDeclareFrame(2, "ex2", durable: true)));
            AmqpMethod reply1 = await client.ReadMethodOnChannelAsync(1, 40, 11);
            Assert.Equal((ushort)40, reply1.ClassId);
            AmqpMethod reply2 = await client.ReadMethodOnChannelAsync(2, 40, 11);
            Assert.Equal((ushort)40, reply2.ClassId);

            // A channel-level error on ch1 must not touch ch2.
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "amq.reserved", durable: true)));
            _ = await ReadChannelCloseAsync(client, 1);
            await client.SendAsync(MethodFrameOn(2, ExchangeDeclareFrame(2, "ex3", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(2, 40, 11);

            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (g) reserved amq.* names; predeclared amq.direct ----------

    [Fact]
    public async Task Reserved_amq_names_are_refused_on_create_but_amq_direct_can_be_redeclared()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "amq.reserved", durable: true)));
            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, close.code);
            Assert.Equal(
                "ACCESS_REFUSED - exchange name 'amq.reserved' contains reserved prefix 'amq.*'",
                close.text);
            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));

            // Reopen the number; the existing predeclared amq.direct is redeclared
            // with its original flags (no refusal: the reserved check only applies
            // on creation).
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "amq.direct", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);

            // A new queue creation with an amq.-prefixed name is refused the same way.
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "amq.notits")));
            close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, close.code);
            Assert.Equal(
                "ACCESS_REFUSED - queue name 'amq.notits' contains reserved prefix 'amq.*'",
                close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Declare_of_the_default_exchange_and_bind_to_it_are_refused()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(2, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(2, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "", durable: true)));
            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, close.code);
            Assert.Equal("ACCESS_REFUSED - operation not permitted on the default exchange", close.text);
            await client.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));

            // rabbit_channel checks the default exchange before the resource
            // lookups, so this refuses even though the queue does not exist yet.
            await client.SendAsync(MethodFrameOn(2, QueueBindFrame(2, "nonesuch", "", "rk")));
            close = await ReadChannelCloseAsync(client, 2);
            Assert.Equal(AmqpConstants.AccessRefused, close.code);
            Assert.Equal("ACCESS_REFUSED - operation not permitted on the default exchange", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Unknown_exchange_type_is_refused_with_a_precondition_failed()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "bogus", durable: true, type: "bananas")));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.PreconditionFailed, close.code);
            Assert.Equal("PRECONDITION_FAILED - unknown exchange type 'bananas'", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- (h) unsupported/unknown methods on a channel ----------

    [Fact]
    public async Task Dialect_method_outside_the_M0_channel_set_closes_the_connection_with_not_implemented()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // tx.select (90, 10) is a dialect method, so it resolves and is then
            // refused as not part of the M0 channel method set. RabbitMQ's own
            // method catch-all is command_invalid (503), but M0 keeps the Task-4
            // not-implemented wording convention (reported as a deviation).
            await client.SendAsync(MethodFrameOn(1, 90, 10, []));

            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.Equal(
                "NOT_IMPLEMENTED - tx.select (90,10) is not part of the M0 channel method set",
                close.text);
            Assert.Equal((ushort)90, close.classId);
            Assert.Equal((ushort)10, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Unresolvable_method_pair_on_a_channel_closes_the_connection_with_syntax_error()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, 999, 999, []));
            (ushort code, string text) close = await ReadConnectionCloseCodeTextAsync(client);
            Assert.Equal(AmqpConstants.SyntaxError, close.code);
            Assert.StartsWith("SYNTAX_ERROR", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Exchange_declare_on_channel_zero_stays_not_implemented()
    {
        // Channel 0 belongs to the connection class (Task 4 ruling), even once
        // channel-scoped declares work on channel >= 1.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);

            await client.SendAsync(MethodFrameOn(0, ExchangeDeclareFrame(0, "ex", durable: true)));
            (ushort code, string text) close = await ReadConnectionCloseCodeTextAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.StartsWith("NOT_IMPLEMENTED", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- channel.flow ----------

    [Fact]
    public async Task Channel_flow_active_true_replies_flow_ok()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: true)));
            AmqpMethod flowOk = await client.ReadMethodOnChannelAsync(1, 20, 21);
            Assert.Equal(true, flowOk.Get("active"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Channel_flow_active_false_closes_the_connection_with_not_implemented()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: false)));
            (ushort code, string text) close = await ReadConnectionCloseCodeTextAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.Equal("NOT_IMPLEMENTED - active=false", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- content and heartbeats on channels ----------

    [Fact]
    public async Task Stray_content_header_on_an_idle_running_channel_closes_with_unexpected_frame()
    {
        // With no content-bearing method pending, the per-channel command
        // assembler is in "expect method" state: any content frame is an
        // UNEXPECTED_FRAME close with the rabbit_command_assembler wording. Task 6
        // moved this to a CHANNEL-level soft close (the M0 pin; RabbitMQ 4.4's own
        // error classes actually make it a hard connection close — report note D1).
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(ContentHeaderFrameOn(1));

            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal(
                "UNEXPECTED_FRAME - expected method frame, got non method frame instead",
                close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Heartbeat_on_a_nonzero_channel_is_a_hard_unexpected_frame()
    {
        // rabbit_reader: a heartbeat on any non-zero channel is an
        // unexpected_frame with the reader's own wording, in any state.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(HeartbeatFrameOn(1));

            (ushort code, string text) close = await ReadConnectionCloseCodeTextAsync(client);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.Equal("UNEXPECTED_FRAME - type 8, all octets = <<>>", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Heartbeat_on_channel_zero_is_ignored()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(HeartbeatFrameOn(0));
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Method_frame_on_a_nonzero_channel_during_the_handshake_is_an_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
            _ = await client.ReadMethodAsync(10, 30);
            await client.SendAsync(TuneOkFrame(2047, 131072, 0));

            // Still pre-serving (no connection.open yet): channel frames are
            // unexpected_frame with the reader's wording, not channelity errors.
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            (ushort code, string text) close = await ReadConnectionCloseCodeTextAsync(client);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.StartsWith("UNEXPECTED_FRAME - type 1, ", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Content_frame_on_a_nonzero_channel_during_the_handshake_is_an_unexpected_frame()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await SendHeaderAsync(client);
            _ = await client.ReadMethodAsync(10, 10);
            await client.SendAsync(StartOkFrame(PlainResponse("guest", "guest")));
            _ = await client.ReadMethodAsync(10, 30);
            await client.SendAsync(TuneOkFrame(2047, 131072, 0));

            await client.SendAsync(ContentHeaderFrameOn(2));
            (ushort code, string text) close = await ReadConnectionCloseCodeTextAsync(client);
            Assert.Equal(AmqpConstants.UnexpectedFrame, close.code);
            Assert.StartsWith("UNEXPECTED_FRAME - type 2, ", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- server-generated queue names and the ""-queue shortcut ----------

    [Fact]
    public async Task Declare_with_an_empty_name_generates_amq_gen_queue_and_the_shortcut_binds_it()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer(NewOptions());
        try
        {
            const string vhost = "/";
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "", durable: true)));
            AmqpMethod declareOk = await client.ReadMethodOnChannelAsync(1, 50, 11);
            string generated = (string)declareOk.Get("queue")!;
            Assert.StartsWith("amq.gen-", generated);

            // The channel remembers the server-generated name: queue.bind with an
            // empty queue name binds the most recently declared queue (RabbitMQ's
            // expand_queue_name_shortcut), proving the shortcut really took effect
            // through the engine.
            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 21);
            ExchangeInfo ex = engine.GetExchange(vhost, "ex")!;
            Assert.Equal([generated], engine.RouteTo(ex, new ContentProperties(), "rk"));

            // queue.unbind expands the "" queue the same way; the routing key of
            // an entirely-empty bind/unbind expands to the generated name too.
            await client.SendAsync(MethodFrameOn(1, QueueUnbindFrame(1, "", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 51);
            Assert.Empty(engine.RouteTo(ex, new ContentProperties(), "rk"));

            // A bind with neither queue nor routing key expands both to the
            // recently declared queue name, binds, and serves the bind-ok.
            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "", "ex", "")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 21);
            Assert.Equal([generated], engine.RouteTo(ex, new ContentProperties(), generated));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Bind_with_empty_queue_name_and_no_prior_declare_is_not_found()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11);

            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "", "ex", "rk")));
            (ushort code, string text) close = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotFound, close.code);
            Assert.Equal("NOT_FOUND - no previously declared queue", close.text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Non_empty_declare_arguments_are_refused_with_not_implemented()
    {
        // M0 refuses arguments on non-passive declares/ binds outright (hard 540,
        // not_implemented per the fixture class) — a flagged RabbitMQ deviation:
        // real RabbitMQ accepts and validates declare arguments.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(NewOptions());
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            var args = new FieldTable([new("x-index", FieldValue.From(3))]);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: true, arguments: args)));
            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.Equal("NOT_IMPLEMENTED - arguments are not supported in M0 for queue.declare", close.text);
            Assert.Equal((ushort)50, close.classId);
            Assert.Equal((ushort)10, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- nowait semantics ----------

    [Fact]
    public async Task Nowait_declares_answer_nothing_but_still_update_channel_state()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer(NewOptions());
        try
        {
            const string vhost = "/";
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11); // still replies (nowait false implied)

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "", durable: true, nowait: true)));
            // No declare-ok reply arrives; the next wire turn is writable channel state.
            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", durable: true, passive: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 40, 11); // answers promptly, then no strays came
            var q = engine.GetQueue(vhost, FirstDeclared(engine, vhost));
            Assert.NotNull(q); // the nowait declare-created queue exists
            string generated = q!.Name;
            Assert.StartsWith("amq.gen-", generated);

            // The nowait declare still updated the most recently declared queue.
            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "", "ex", "rk")));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 21);
            ExchangeInfo ex = engine.GetExchange(vhost, "ex")!;
            Assert.Equal([generated], engine.RouteTo(ex, new ContentProperties(), "rk"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    private static string FirstDeclared(InMemoryEngine engine, string vhost)
    {
        // A scan hook for the runtime of a generated name: only one amq.gen-
        // queue exists in this isolated test's fresh engine.
        foreach (string name in engine.QueueNamesOfVhost(vhost))
        {
            if (name.StartsWith("amq.gen-", StringComparison.Ordinal))
            {
                return name;
            }
        }

        throw new InvalidOperationException("no generated queue was found in the engine");
    }

    // ---------- helpers ----------

    private static BrokerServerOptions NewOptions() => new()
    {
        Users = [new BrokerUser("guest", "guest")],
        Vhosts = ["/"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

    /// <summary>The tuned handshake the channel tests start from; heartbeats are
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
        return method; // channel set by the caller through MethodFrameOn
    }

    /// <summary>connection.close arguments from the registry's field order:
    /// reply-code(short), reply-text(shortstr), class-id(short), method-id(short).</summary>
    private static byte[] CloseArgsBytes(ushort replyCode, string replyText)
    {
        var args = new ByteWriter();
        args.WriteShort((short)replyCode);
        args.WriteShortstr(replyText);
        args.WriteShort(0);
        args.WriteShort(0);
        return args.ToArray();
    }

    /// <summary>Server-effective channel.close (20,40) as the client would raise it.</summary>
    private static AmqpMethod ClientChannelCloseFrame(ushort channel)
    {
        AmqpMethod method = NewMethod("channel.close");
        method.Set("reply-code", (short)200);
        method.Set("reply-text", "client-does-not-care");
        method.Set("class-id", (short)0);
        method.Set("method-id", (short)0);
        return method;
    }

    private static AmqpMethod ChannelCloseOkFrame()
    {
        AmqpMethod method = NewMethod("channel.close-ok");
        return method;
    }

    private static AmqpMethod ChannelFlowFrame(ushort channel, bool active)
    {
        AmqpMethod method = NewMethod("channel.flow");
        method.Set("active", active);
        return method;
    }

    private static AmqpMethod ExchangeDeclareFrame(
        ushort channel,
        string exchange,
        bool durable,
        bool autoDelete = false,
        bool exchangeInternal = false,
        string type = "direct",
        bool passive = false,
        FieldTable? arguments = null,
        bool nowait = false)
    {
        AmqpMethod method = NewMethod("exchange.declare");
        method.Set("ticket", (short)0);
        method.Set("exchange", exchange);
        method.Set("type", type);
        method.Set("passive", passive);
        method.Set("durable", durable);
        method.Set("auto-delete", autoDelete);
        method.Set("internal", exchangeInternal);
        method.Set("nowait", nowait);
        method.Set("arguments", arguments ?? new FieldTable());
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

    private static AmqpMethod QueueBindFrame(ushort channel, string queue, string exchange, string routingKey, FieldTable? arguments = null, bool nowait = false)
    {
        AmqpMethod method = NewMethod("queue.bind");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("exchange", exchange);
        method.Set("routing-key", routingKey);
        method.Set("nowait", nowait);
        method.Set("arguments", arguments ?? new FieldTable());
        return method;
    }

    private static AmqpMethod QueueUnbindFrame(ushort channel, string queue, string exchange, string routingKey, FieldTable? arguments = null)
    {
        AmqpMethod method = NewMethod("queue.unbind");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("exchange", exchange);
        method.Set("routing-key", routingKey);
        method.Set("arguments", arguments ?? new FieldTable());
        return method;
    }

    private static AmqpMethod NewMethod(string name) => MethodRegistry.TryGetByName(name, out var schema)
        ? new AmqpMethod(schema!)
        : throw new InvalidOperationException($"Registry is missing {name} (Task 3 guarantee)");

    /// <summary>Frames any AmqpMethod. The channel is supplied by a following helper
    /// so calls read as "the method frame of this method on channel N".</summary>
    private static byte[] MethodFrameOn(ushort channel, ushort classId, ushort methodId, byte[] args)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, channel, classId, methodId, args);
        return frame.ToArray();
    }

    /// <summary>Frames a method object on the given channel: the payload is class-id +
    /// method-id + arguments, sliced from MethodCodec's encoding.</summary>
    private static byte[] MethodFrameOnWire(AmqpMethod method, ushort channel)
    {
        var payloadWriter = new ByteWriter();
        MethodCodec.WriteMethod(ref payloadWriter, method);
        byte[] payload = payloadWriter.ToArray();
        var frame = new ByteWriter();
        FrameWriter.WriteMethod(ref frame, channel, method.ClassId, method.MethodId, payload.AsSpan(4));
        return frame.ToArray();
    }

    /// <summary>Frames a method object on the given channel. (Extension-friendly
    /// entry point so helper methods can read as `MethodFrameOn(channel, method)`.)</summary>
    private static byte[] MethodFrameOn(ushort channel, Func<AmqpMethod> builder) =>
        MethodFrameOnWire(builder(), channel);

    private static byte[] MethodFrameOn(ushort channel, AmqpMethod method) =>
        MethodFrameOnWire(method, channel);

    private static byte[] ContentHeaderFrameOn(ushort channel)
    {
        var frame = new ByteWriter();
        FrameWriter.WriteContentHeader(ref frame, channel, classId: 60, bodySize: 0, new ContentProperties());
        return frame.ToArray();
    }

    private static byte[] HeartbeatFrameOn(ushort channel)
    {
        var frame = new ByteWriter();
        frame.WriteOctet(AmqpFrame.FrameHeartbeat);
        frame.WriteUShort(channel);
        frame.WriteLong(0);
        frame.WriteOctet(AmqpFrame.FrameEnd);
        return frame.ToArray();
    }

    /// <summary>Reads the server's channel.close on the given channel
    /// (soft error close: class 20 method 40), returning all four fields.</summary>
    private static async Task<(ushort Code, string Text)> ReadChannelCloseAsync(WireClient client, ushort channel)
    {
        AmqpMethod close = await client.ReadMethodOnChannelAsync(channel, 20, 40);
        return (
            (ushort)(short)close.Get("reply-code")!, // wire type is short-int, always positive here
            (string)close.Get("reply-text")!);
    }

    /// <summary>Reads the server's hard-error connection.close on channel 0.</summary>
    private static async Task<(ushort Code, string Text, ushort ClassId, ushort MethodId)> ReadConnectionCloseAsync(WireClient client)
    {
        AmqpMethod close = await client.ReadMethodAsync(10, 50);
        return (
            (ushort)(short)close.Get("reply-code")!,
            (string)close.Get("reply-text")!,
            (ushort)(short)close.Get("class-id")!,
            (ushort)(short)close.Get("method-id")!);
    }

    /// <summary>The hard-error connection.close when only code and text are asserted.</summary>
    private static async Task<(ushort Code, string Text)> ReadConnectionCloseCodeTextAsync(WireClient client)
    {
        AmqpMethod close = await client.ReadMethodAsync(10, 50);
        return (
            (ushort)(short)close.Get("reply-code")!,
            (string)close.Get("reply-text")!);
    }

    /// <summary>Stops the server and disposes the client, so a failing FSM scenario
    /// never dangles past the assertion.</summary>
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

    private static (AmqpBrokerServer Server, WireClient Client, InMemoryEngine Engine) SpawnServer(BrokerServerOptions options)
    {
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

        /// <summary>Reads the next method frame of any (class, method) pair.</summary>
        public Task<AmqpMethod> ReadMethodAsync(int timeoutMs = 5_000) => ReadMethodAsync(0, 0, timeoutMs);

        /// <summary>
        /// Decodes frames from the transport until the next METHOD frame with the
        /// requested class/method id arrives (0 means "any"); heartbeats and
        /// content frames are skipped. The discriminator is strict: a mismatched
        /// method frame is an error, never silently consumed.
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
        public Task<AmqpMethod> ReadMethodOnChannelAsync(
            ushort channel, ushort classId = 0, ushort methodId = 0, int timeoutMs = 5_000)
        {
            return ReadMethodOnChannelAsyncCore(channel, classId, methodId, timeoutMs);
        }

        private async Task<AmqpMethod> ReadMethodOnChannelAsyncCore(
            ushort channel, ushort classId, ushort methodId, int timeoutMs)
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
    }
}

// In-memory route engine unit tests (Task 5): direct stores, bindings and the
// RouteTo seam that M1's topic engine replaces.
public sealed class InMemoryRouteEngineTests
{
    private static readonly string[] TwoVhosts = ["/", "test"];

    [Fact]
    public void Engine_predeclares_the_default_and_amq_direct_exchanges_per_vhost()
    {
        var engine = new InMemoryEngine(TwoVhosts);

        ExchangeInfo defaultExchange = engine.GetExchange("/", "")!;
        Assert.Equal("direct", defaultExchange.Type);
        Assert.True(defaultExchange.Durable);
        Assert.False(defaultExchange.AutoDelete);
        Assert.False(defaultExchange.Internal);

        ExchangeInfo amqDirect = engine.GetExchange("/", "amq.direct")!;
        Assert.Equal("direct", amqDirect.Type);
        Assert.True(amqDirect.Durable);

        Assert.NotNull(engine.GetExchange("test", "amq.direct"));
        Assert.Null(engine.GetExchange("nonexistent-vhost", "amq.direct"));
        Assert.Null(engine.GetQueue("/", "anything"));
    }

    [Fact]
    public void Declare_and_get_roundtrip_the_flag_records()
    {
        var engine = new InMemoryEngine(TwoVhosts);

        var exchange = new ExchangeInfo("/", "x", "direct", Durable: true, AutoDelete: false, Internal: false);
        engine.DeclareExchange(exchange);
        Assert.Equal(exchange, engine.GetExchange("/", "x"));

        var queue = new QueueInfo("/", "q", Durable: true, Exclusive: false, AutoDelete: false);
        engine.DeclareQueue(queue);
        Assert.Equal(queue, engine.GetQueue("/", "q"));

        // Re-declaring the same entity is idempotent (the channel owns the
        // equivalence checks; the engine just stores).
        engine.DeclareQueue(queue);
        Assert.Equal(queue, engine.GetQueue("/", "q"));

        Assert.Null(engine.GetQueue("test", "q"));
    }

    [Fact]
    public void Bindings_route_to_bound_queues_in_order_without_duplicates()
    {
        var engine = new InMemoryEngine(TwoVhosts);
        engine.DeclareExchange(new ExchangeInfo("/", "x", "direct", Durable: true, AutoDelete: false, Internal: false));
        const string vhost = "/";
        engine.DeclareQueue(new QueueInfo(vhost, "q1", Durable: true, Exclusive: false, AutoDelete: false));
        engine.DeclareQueue(new QueueInfo(vhost, "q2", Durable: true, Exclusive: false, AutoDelete: false));
        engine.DeclareQueue(new QueueInfo(vhost, "q3", Durable: true, Exclusive: false, AutoDelete: false));
        ExchangeInfo ex = engine.GetExchange(vhost, "x")!;
        var empty = new ContentProperties();

        engine.Bind(vhost, "x", "q1", "a");
        engine.Bind(vhost, "x", "q2", "a");
        engine.Bind(vhost, "x", "q3", "b");

        Assert.Equal(["q1", "q2"], engine.RouteTo(ex, empty, "a"));
        Assert.Equal(["q3"], engine.RouteTo(ex, empty, "b"));
        Assert.Empty(engine.RouteTo(ex, empty, "unbound"));
        Assert.Empty(engine.RouteTo(ex, empty, ""));

        // A duplicate binding is a no-op keeping the original position.
        engine.Bind(vhost, "x", "q1", "a");
        Assert.Equal(["q1", "q2"], engine.RouteTo(ex, empty, "a"));

        // An empty routing key is a legal direct key distinct from "unbound".
        engine.Bind(vhost, "x", "q3", "");
        Assert.Equal(["q3"], engine.RouteTo(ex, empty, ""));
    }

    [Fact]
    public void Unbind_removes_the_binding_and_absent_unbind_is_a_noop()
    {
        var engine = new InMemoryEngine(TwoVhosts);
        engine.DeclareExchange(new ExchangeInfo("/", "x", "direct", Durable: true, AutoDelete: false, Internal: false));
        const string vhost = "/";
        engine.DeclareQueue(new QueueInfo(vhost, "q1", Durable: true, Exclusive: false, AutoDelete: false));
        engine.DeclareQueue(new QueueInfo(vhost, "q2", Durable: true, Exclusive: false, AutoDelete: false));
        ExchangeInfo ex = engine.GetExchange(vhost, "x")!;
        var empty = new ContentProperties();

        engine.Bind(vhost, "x", "q1", "a");
        engine.Bind(vhost, "x", "q2", "a");
        engine.Unbind(vhost, "x", "q2", "a");
        Assert.Equal(["q1"], engine.RouteTo(ex, empty, "a"));

        engine.Unbind(vhost, "x", "q2", "a"); // now absent: no-op, no throw
        Assert.Equal(["q1"], engine.RouteTo(ex, empty, "a"));

        engine.Unbind(vhost, "x", "q1", "never-bound"); // a different key: no-op
        Assert.Equal(["q1"], engine.RouteTo(ex, empty, "a"));
    }

    // Cross-connection first-declare race: DeclareExchange/DeclareQueue are an
    // atomic creation-only compare-and-set under the engine's single lock, so of
    // two racing first-declares of one (vhost, name) exactly one call creates the
    // record (null return) and the other receives the winner's unchanged record
    // for its channel-side equivalence check — last-write-wins is unreachable.

    [Fact]
    public async Task Racing_creates_of_the_same_exchange_with_different_flags_keep_the_winner()
    {
        const int rounds = 25;
        for (int round = 0; round < rounds; round++)
        {
            var engine = new InMemoryEngine(TwoVhosts);
            var first = new ExchangeInfo("/", "race-x", "direct", Durable: true, AutoDelete: false, Internal: false);
            var second = new ExchangeInfo("/", "race-x", "direct", Durable: false, AutoDelete: false, Internal: false);

            // Release both racers together; RunContinuationsAsynchronously keeps the
            // release from running them inline one after the other.
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<ExchangeInfo?> a = Task.Run(async () => { await gate.Task; return engine.DeclareExchange(first); });
            Task<ExchangeInfo?> b = Task.Run(async () => { await gate.Task; return engine.DeclareExchange(second); });
            gate.SetResult();

            // A faulted racing task surfaces here (no exceptions allowed).
            ExchangeInfo?[] results = await Task.WhenAll(a, b);

            int creations = (results[0] is null ? 1 : 0) + (results[1] is null ? 1 : 0);
            Assert.Equal(1, creations);

            // The loser's non-null return IS the winner's record, and the store
            // still holds that same instance — the loser did not overwrite it.
            ExchangeInfo winner = (results[0] ?? results[1])!;
            Assert.Same(winner, engine.GetExchange("/", "race-x"));
        }
    }

    [Fact]
    public async Task Racing_creates_of_the_same_queue_with_different_flags_keep_the_winner()
    {
        const int rounds = 25;
        for (int round = 0; round < rounds; round++)
        {
            var engine = new InMemoryEngine(TwoVhosts);
            var first = new QueueInfo("/", "race-q", Durable: true, Exclusive: false, AutoDelete: false);
            var second = new QueueInfo("/", "race-q", Durable: false, Exclusive: false, AutoDelete: false);

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<QueueInfo?> a = Task.Run(async () => { await gate.Task; return engine.DeclareQueue(first); });
            Task<QueueInfo?> b = Task.Run(async () => { await gate.Task; return engine.DeclareQueue(second); });
            gate.SetResult();

            QueueInfo?[] results = await Task.WhenAll(a, b);

            int creations = (results[0] is null ? 1 : 0) + (results[1] is null ? 1 : 0);
            Assert.Equal(1, creations);

            QueueInfo winner = (results[0] ?? results[1])!;
            Assert.Same(winner, engine.GetQueue("/", "race-q"));
        }
    }

    [Fact]
    public async Task Racing_creates_with_identical_flags_create_exactly_once()
    {
        const int rounds = 25;

        for (int round = 0; round < rounds; round++)
        {
            var engine = new InMemoryEngine(TwoVhosts);
            var first = new ExchangeInfo("/", "race-x", "direct", Durable: true, AutoDelete: false, Internal: false);
            var second = new ExchangeInfo("/", "race-x", "direct", Durable: true, AutoDelete: false, Internal: false);

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<ExchangeInfo?> a = Task.Run(async () => { await gate.Task; return engine.DeclareExchange(first); });
            Task<ExchangeInfo?> b = Task.Run(async () => { await gate.Task; return engine.DeclareExchange(second); });
            gate.SetResult();

            ExchangeInfo?[] results = await Task.WhenAll(a, b);

            Assert.Equal(1, (results[0] is null ? 1 : 0) + (results[1] is null ? 1 : 0));

            // The single prior-return carries the created record, value-equal to
            // both requests.
            ExchangeInfo prior = (results[0] ?? results[1])!;
            Assert.Equal(first, prior);
            Assert.Equal(second, prior);
            Assert.Equal(first, engine.GetExchange("/", "race-x"));
        }

        for (int round = 0; round < rounds; round++)
        {
            var engine = new InMemoryEngine(TwoVhosts);
            var first = new QueueInfo("/", "race-q", Durable: true, Exclusive: false, AutoDelete: false);
            var second = new QueueInfo("/", "race-q", Durable: true, Exclusive: false, AutoDelete: false);

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<QueueInfo?> a = Task.Run(async () => { await gate.Task; return engine.DeclareQueue(first); });
            Task<QueueInfo?> b = Task.Run(async () => { await gate.Task; return engine.DeclareQueue(second); });
            gate.SetResult();

            QueueInfo?[] results = await Task.WhenAll(a, b);

            Assert.Equal(1, (results[0] is null ? 1 : 0) + (results[1] is null ? 1 : 0));

            QueueInfo prior = (results[0] ?? results[1])!;
            Assert.Equal(first, prior);
            Assert.Equal(second, prior);
            Assert.Equal(first, engine.GetQueue("/", "race-q"));
        }
    }
}
