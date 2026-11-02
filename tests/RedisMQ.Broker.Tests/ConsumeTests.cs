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

// Consume path (Task 7): queue consumers, the per-queue delivery pump, prefetch
// credits (count and bytes), ack/nack/reject settlement, basic.get, recover, and
// cancel — the same "real client over raw frames" harness shape as PublishTests and
// ChannelTopologyTests (per-file private copies are the M0 pattern; the harness
// consolidation is a recorded deferred minor). Every pinned string and field order
// below was verified against the RabbitMQ 4.4 channel sources:
//
//   rabbit_channel.erl — basic_consume builds ok_msg(NoWait, #'basic.consume_ok')
//                        UNCONDITIONALLY (lines 1817-1818): consume-ok answers
//                        whenever nowait=false, for client- and server-assigned
//                        tags alike (the brief's "only for server-assigned" quirk
//                        was a plan-text error, ruled so in the ledger); server
//                        tags are "amq.ctag" + a secure token (1446, 1528); tag
//                        reuse is not_allowed "attempt to reuse consumer tag '~ts'"
//                        (1482); qos prefetch_size!=0 is refused by RabbitMQ but
//                        ACCEPTED by RedisMQ as a documented bytes-gating extension
//                        (1552-1554; ledger product extension); global=true is
//                        demoted to per-channel when the global_qos flag is off,
//                        replying qos-ok (1570-1608); unknown delivery tags are
//                        precondition_failed "unknown delivery tag ~w"
//                        (collect_acks 2139); ack(0, multiple=true) acks everything
//                        outstanding (2112-2115); nack mirrors ack's multiple
//                        handling (1362-1371); recover(requeue=false) and
//                        recover_async(requeue=false) are hard not_implemented
//                        "requeue=false" (1635-1636); basic.get's unacked entries
//                        count toward the channel limiter per the source's own TODO
//                        (1594-1595); get itself is never prefetch-refused;
//                        message-count reports messages remaining after the
//                        delivered one (1404-1421); channel.flow(true) replies
//                        flow_ok (1786-1788) while active=false stays the Task 6
//                        committed hard not_implemented (1789-1790).
public sealed class ConsumeTests
{
    // ---------- consume and deliver ----------

    [Fact]
    public async Task Consume_delivers_ready_messages_in_order_with_the_full_deliver_fields_and_content()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // Two messages queued BEFORE the consumer exists: no pump nudges can race
            // the pre-publishes. A passive declare fences the publish batch.
            await LoadAsync(client, 1, "q", "m-one", "m-two");

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "")));
            AmqpMethod consumeOk = await client.ReadMethodOnChannelAsync(1, 60, 21);
            string consumerTag = Assert.IsType<string>(consumeOk.Get("consumer-tag"));

            (AmqpMethod method, ContentHeader header, byte[] body) one =
                await ReadDeliverAsync(client, 1);
            Assert.Equal(consumerTag, one.method.Get("consumer-tag"));
            Assert.Equal(1L, one.method.Get("delivery-tag"));
            Assert.Equal(false, one.method.Get("redelivered"));
            // The deliver's exchange/routing-key name the message's routing origin.
            Assert.Equal("", one.method.Get("exchange")); // published through the default exchange
            Assert.Equal("q", one.method.Get("routing-key"));
            Assert.Equal("text/plain", one.header.Properties.ContentType);
            Assert.Equal("m-one", Encoding.UTF8.GetString(one.body));

            (AmqpMethod method2, ContentHeader header2, byte[] body2) two =
                await ReadDeliverAsync(client, 1);
            Assert.Equal(2L, two.method2.Get("delivery-tag"));
            Assert.Equal("m-two", Encoding.UTF8.GetString(two.body2));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Qos_prefetch_count_gates_delivery_until_a_message_is_acked()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11); // qos-ok

            await LoadAsync(client, 1, "q", "m-one", "m-two");

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod oneMethod, ContentHeader _, byte[] bodyOne) = await ReadDeliverAsync(client, 1);
            Assert.Equal("ct", oneMethod.Get("consumer-tag"));
            Assert.Equal(1L, oneMethod.Get("delivery-tag"));
            Assert.Equal("m-one", Encoding.UTF8.GetString(bodyOne));

            // Prefetch 1 is fully outstanding: the second message may not deliver.
            Assert.Null(await TryReadMethodAsync(client, 1));

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            (AmqpMethod two, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal(2L, two.Get("delivery-tag"));
            Assert.Equal(false, two.Get("redelivered")); // freeing credit is not a redelivery
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Qos_prefetch_size_gates_by_the_sum_of_unacked_body_bytes()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // The bytes gate is the RedisMQ extension: RabbitMQ refuses size!=0, M0
            // accepts it and gates by the outstanding unacked body bytes (ledger).
            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchSize: 600)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);

            await LoadAsync(client, 1, "q", new string('a', 400), new string('b', 300));

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod oneMethod, ContentHeader _, byte[] body) = await ReadDeliverAsync(client, 1); // t1
            _ = oneMethod;
            Assert.Equal(400, body.Length); // 400 outstanding leaves 200 of the 600 budget

            // A second 300-byte delivery needs a 700-byte budget: gated.
            Assert.Null(await TryReadMethodAsync(client, 1));

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            (AmqpMethod twoMethod, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1); // t2
            _ = twoMethod;
            Assert.Equal(300, body2.Length);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- ack settlement ----------

    [Fact]
    public async Task Basic_ack_multiple_true_acks_every_outstanding_delivery_up_to_the_tag()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 2)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1); // t1
            _ = await ReadDeliverAsync(client, 1); // t2 — the gate is full

            // multiple=true acks everything at/under the tag: ack t2 (multiple:true)
            // settles t1 AND t2 (collect_acks, rabbit_channel 2112-2115), freeing both
            // credit slots, so both remaining publishes deliver.

            await client.SendAsync(MethodFrameOn(1, AckFrame(2, multiple: true)));
            await LoadUnfencedAsync(client, 1, "q", "m-three", "m-four");
            (AmqpMethod three, ContentHeader _, byte[] body3) = await ReadDeliverAsync(client, 1);
            Assert.Equal(3L, three.Get("delivery-tag"));
            Assert.Equal("m-three", Encoding.UTF8.GetString(body3));
            (AmqpMethod four, ContentHeader _, byte[] body4) = await ReadDeliverAsync(client, 1);
            Assert.Equal("m-four", Encoding.UTF8.GetString(body4));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Basic_ack_multiple_false_frees_one_slot_and_zero_multiple_acks_everything()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 2)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1); // t1
            _ = await ReadDeliverAsync(client, 1); // t2

            // A plain ack of t1 leaves t2 outstanding: one slot, not both.
            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            await LoadUnfencedAsync(client, 1, "q", "m-three", "m-four");
            (AmqpMethod three, ContentHeader _, byte[] body3) = await ReadDeliverAsync(client, 1);
            Assert.Equal(3L, three.Get("delivery-tag"));
            Assert.Equal("m-three", Encoding.UTF8.GetString(body3));

            // One slot is still held (t2, t3): m-four is gated.
            Assert.Null(await TryReadMethodAsync(client, 1));

            // The ack(0, multiple=true) special case acks EVERYTHING outstanding.
            await client.SendAsync(MethodFrameOn(1, AckFrame(0, multiple: true)));
            (AmqpMethod four, ContentHeader _, byte[] body4) = await ReadDeliverAsync(client, 1);
            Assert.Equal("m-four", Encoding.UTF8.GetString(body4));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Nack_requeue_brings_the_message_back_at_the_tail_and_redelivers_it()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod one, ContentHeader _, byte[] _) = await ReadDeliverAsync(client, 1);
            Assert.Equal(1L, one.Get("delivery-tag"));

            // nack(requeue=true) returns the message to the queue tail (the M0
            // stream-consonant ruling) with redelivered set for the next take, and
            // the single-message queue makes that redelivery immediate.
            await client.SendAsync(MethodFrameOn(1, NackFrame(1, requeue: true)));
            (AmqpMethod redelivered, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal(2L, redelivered.Get("delivery-tag")); // tags are per-channel monotonic
            Assert.Equal(true, redelivered.Get("redelivered"));
            Assert.Equal("m-one", Encoding.UTF8.GetString(body2)); // original body
            Assert.Equal("", redelivered.Get("exchange")); // and original properties
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Reject_without_requeue_drops_the_message()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod one, ContentHeader _, byte[] body1) = await ReadDeliverAsync(client, 1);
            Assert.Equal(1L, one.Get("delivery-tag"));
            Assert.Equal("m-one", Encoding.UTF8.GetString(body1));
            Assert.Equal("", one.Get("exchange"));

            // requeue=false rejects DROP (settle-dequeue): m-one never comes back.
            await client.SendAsync(MethodFrameOn(1, RejectFrame(1, requeue: false)));

            // The head is now m-two, delivered fresh (not redelivered).
            (AmqpMethod two, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal(2L, two.Get("delivery-tag"));
            Assert.Equal(false, two.Get("redelivered"));
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2));
            await client.SendAsync(MethodFrameOn(1, AckFrame(2)));

            await FenceAsync(client, 1, "q");
            Assert.Empty(engine.PeekQueue("q"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Ack_of_an_unknown_delivery_tag_soft_closes_the_channel()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "q");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);

            // collect_acks (rabbit_channel 2139): unsettled, unknown tags are
            // precondition_failed "unknown delivery tag ~w".
            await client.SendAsync(MethodFrameOn(1, AckFrame(47)));
            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.PreconditionFailed, close.code);
            Assert.Equal("PRECONDITION_FAILED - unknown delivery tag 47", close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(80, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- basic.get ----------

    [Fact]
    public async Task Basic_get_returns_get_ok_with_the_message_then_get_empty_when_drained()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two");

            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q")));
            AmqpMethod getOk = await client.ReadMethodOnChannelAsync(1, 60, 71);
            Assert.Equal(1L, getOk.Get("delivery-tag"));
            Assert.Equal(false, getOk.Get("redelivered"));
            Assert.Equal("", getOk.Get("exchange"));
            Assert.Equal("q", getOk.Get("routing-key"));
            // message-count = messages remaining AFTER the delivered one (rabbit dequeue).
            Assert.Equal(1, (int)getOk.Get("message-count")!);
            (ContentHeader header, byte[] body) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal("m-one", Encoding.UTF8.GetString(body));

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q")));
            AmqpMethod getOk2 = await client.ReadMethodOnChannelAsync(1, 60, 71);
            Assert.Equal(2L, getOk2.Get("delivery-tag"));
            Assert.Equal(0, (int)getOk2.Get("message-count")!);
            (ContentHeader _, byte[] body2) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2));
            await client.SendAsync(MethodFrameOn(1, AckFrame(2)));

            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q")));
            AmqpMethod empty = await client.ReadMethodOnChannelAsync(1, 60, 72);
            Assert.Equal("", empty.Get("cluster-id"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Basic_get_non_refused_by_prefetch_and_its_unacked_counts_toward_the_gate()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q-a", "m-one");
            await LoadAsync(client, 1, "q-b", "n-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q-a", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1); // t1: m-one, the gate is full

            // basic.get is never prefetch-refused (one-shot dequeue, rabbit TODO
            // 1594-1595): it takes n-one with its own tag.
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q-b")));
            AmqpMethod getOk = await client.ReadMethodOnChannelAsync(1, 60, 71);
            Assert.Equal(2L, getOk.Get("delivery-tag"));
            (ContentHeader _, byte[] getBody) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal("n-one", Encoding.UTF8.GetString(getBody));

            // Two outstanding deliveries against prefetch 1: m-two on q-a is gated.
            await LoadUnfencedAsync(client, 1, "q-a", "m-two");
            Assert.Null(await TryReadMethodAsync(client, 1));

            // Freeing the GET's tag alone is not enough: the consumer's t1 still holds.
            await client.SendAsync(MethodFrameOn(1, AckFrame(2)));
            Assert.Null(await TryReadMethodAsync(client, 1));

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            (AmqpMethod redelivered, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal(3L, redelivered.Get("delivery-tag"));
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- recover ----------

    [Fact]
    public async Task Basic_recover_requeue_requeues_the_whole_channel_ledger()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod one, ContentHeader _, byte[] bodyOne) = await ReadDeliverAsync(client, 1); // t1
            Assert.Equal("m-one", Encoding.UTF8.GetString(bodyOne));

            // m-two is ready behind the gate; recover requeues THIS channel's unacked
            // set (t1: m-one) to the tail ([m-two, m-one, m-three...]).
            await client.SendAsync(MethodFrameOn(1, RecoverFrame(requeue: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 111); // recover-ok precedes the redelivery

            (AmqpMethod two, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2)); // the ready head comes back first
            Assert.Equal(false, two.Get("redelivered"));
            await client.SendAsync(MethodFrameOn(1, AckFrame(2)));

            (AmqpMethod three, ContentHeader _, byte[] body3) = await ReadDeliverAsync(client, 1);
            Assert.Equal(true, three.Get("redelivered")); // the recovered t1 rides the tail
            Assert.Equal("m-one", Encoding.UTF8.GetString(body3));
            await client.SendAsync(MethodFrameOn(1, AckFrame(3)));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Basic_recover_requeue_false_closes_the_connection_with_not_implemented()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "q");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);

            // rabbit_channel 1635-1636: recover/recover_async with requeue=false is
            // the hard not_implemented "requeue=false".
            await client.SendAsync(MethodFrameOn(1, RecoverFrame(requeue: false)));
            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.NotImplemented, close.code);
            Assert.Equal("NOT_IMPLEMENTED - requeue=false", close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(110, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- cancel ----------

    [Fact]
    public async Task Basic_cancel_unregisters_the_consumer_and_stops_delivery()
    {
        (AmqpBrokerServer server, WireClient client, InMemoryEngine engine) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod one, ContentHeader _, byte[] _) = await ReadDeliverAsync(client, 1);
            Assert.Equal(1L, one.Get("delivery-tag"));

            await client.SendAsync(MethodFrameOn(1, CancelFrame("ct")));
            AmqpMethod cancelOk = await client.ReadMethodOnChannelAsync(1, 60, 31);
            Assert.Equal("ct", cancelOk.Get("consumer-tag"));

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            // The consumer is gone: m-two stays queued, and a NEW enqueue is not
            // delivered either. The fence before the assertions orders ack-then-check.
            await FenceAsync(client, 1, "q");
            IReadOnlyList<InMemoryMessage> stored = engine.PeekQueue("q");
            Assert.Single(stored);
            Assert.Equal("m-two", Encoding.UTF8.GetString(stored[0].Body));
            await LoadAsync(client, 1, "q", "m-three");
            Assert.Equal(2, engine.PeekQueue("q").Count);
            Assert.Null(await TryReadMethodAsync(client, 1));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Cancel_of_an_unknown_consumer_tag_still_replies_cancel_ok()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // rabbit_channel 1541-1544: cancel-ok echoes the tag unconditionally,
            // whether or not a consumer ever bore it.
            await client.SendAsync(MethodFrameOn(1, CancelFrame("ghost-tag")));
            AmqpMethod cancelOk = await client.ReadMethodOnChannelAsync(1, 60, 31);
            Assert.Equal("ghost-tag", cancelOk.Get("consumer-tag"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- consumer tags and registration edges ----------

    [Fact]
    public async Task Server_assigned_tags_use_amq_ctag_and_both_tag_flavors_reply_consume_ok()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "q-empty-a");
            await DeclareQueueAsync(client, 1, "q-empty-b");

            // Server-assigned: an amq.ctag tag with a token suffix (rabbit 1446/1528).
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q-empty-a", consumerTag: "")));
            AmqpMethod serverOk = await client.ReadMethodOnChannelAsync(1, 60, 21);
            string assigned = Assert.IsType<string>(serverOk.Get("consumer-tag"))!;
            Assert.StartsWith("amq.ctag", assigned);
            Assert.True(assigned.Length > "amq.ctag".Length, "the server tag carries a token suffix");

            // Client-assigned: RabbitMQ 4.4 replies consume-ok for BOTH flavors when
            // nowait=false (rabbit 1817-1818, ledger ruling) — the brief's quirk was
            // a plan-text error.
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q-empty-b", consumerTag: "my-tag")));
            AmqpMethod clientOk = await client.ReadMethodOnChannelAsync(1, 60, 21);
            Assert.Equal("my-tag", clientOk.Get("consumer-tag"));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Consume_with_nowait_answers_nothing_and_starts_plain_delivery()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await LoadAsync(client, 1, "q", "m-one");

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct", nowait: true)));
            // No consume-ok at all; the delivery itself proves the subscription took.
            (AmqpMethod deliver, ContentHeader _, byte[] body) = await ReadDeliverAsync(client, 1);
            Assert.Equal("ct", deliver.Get("consumer-tag"));
            Assert.Equal("m-one", Encoding.UTF8.GetString(body));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Reusing_a_consumer_tag_on_the_channel_is_not_allowed()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
            await DeclareQueueAsync(client, 1, "q");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);

            // rabbit_channel 1482: "attempt to reuse consumer tag '~ts'".
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotAllowed, close.code);
            Assert.Equal("NOT_ALLOWED - attempt to reuse consumer tag 'ct'", close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(20, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Consume_of_an_unknown_queue_is_not_found()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "missing", consumerTag: "ct")));
            (ushort code, string text, ushort classId, ushort methodId) close =
                await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.NotFound, close.code);
            Assert.Equal("NOT_FOUND - no queue 'missing' in vhost '/'", close.text);
            Assert.Equal(60, close.classId);
            Assert.Equal(20, close.methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Two_consumers_on_one_queue_are_served_round_robin_in_registration_order()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // Prefetch 1 pins the race: before c-two registers, the pump can claim at
            // most m-one for c-one (RabbitMQ-legal early claim); after the ack, the RR
            // cursor and the single free slot decide each next consumer.
            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one", "m-two", "m-three");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "c-one")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "c-two")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);

            // Registration order rules the RR cycle: c-one, c-two, c-one.
            (AmqpMethod first, ContentHeader _, byte[] b1) = await ReadDeliverAsync(client, 1);
            Assert.Equal("c-one", first.Get("consumer-tag"));
            Assert.Equal("m-one", Encoding.UTF8.GetString(b1));

            // Acker frees the one slot; the cursor hands m-two to c-two.
            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            (AmqpMethod second, ContentHeader _, byte[] b2) = await ReadDeliverAsync(client, 1);
            Assert.Equal("c-two", second.Get("consumer-tag"));
            Assert.Equal("m-two", Encoding.UTF8.GetString(b2));

            // The cycle wraps to c-one for m-three.
            await client.SendAsync(MethodFrameOn(1, AckFrame(2)));
            (AmqpMethod third, ContentHeader _, byte[] b3) = await ReadDeliverAsync(client, 1);
            Assert.Equal("c-one", third.Get("consumer-tag"));
            Assert.Equal("m-three", Encoding.UTF8.GetString(b3));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task A_competing_consumer_on_an_exclusive_consumer_queue_resource_locks()
    {
        (AmqpBrokerServer server, WireClient clientA, WireClient clientB, _) = SpawnTwoConnectionServer();
        try
        {
            await HandshakeAsync(clientA);
            _ = await clientA.ReadMethodAsync(10, 41);
            await clientA.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await clientA.ReadMethodOnChannelAsync(1, 20, 11);

            // Connection B joins for the competing consume attempt.
            await HandshakeAsync(clientB);
            _ = await clientB.ReadMethodAsync(10, 41);
            await clientB.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await clientB.ReadMethodOnChannelAsync(1, 20, 11);

            // The queue must exist: a consume of a missing queue is a NOT_FOUND soft
            // close, not this test's subject.
            await DeclareAsync(clientA, 1, QueueDeclareFrame(1, "q"), (50, 11));

            await clientA.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "first", exclusive: true)));
            _ = await clientA.ReadMethodOnChannelAsync(1, 60, 21);

            // The M0 wiring ruling pins the queue-declare exclusive wording style
            // (wording risk recorded in the ledger: RabbitMQ's consume-refusal
            // phrasing was not in the cached sources).
            await clientB.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "second")));
            (ushort code, string text) close = await ReadChannelCloseCodeTextAsync(clientB, 1);
            Assert.Equal(AmqpConstants.ResourceLocked, close.code);
            Assert.Equal(
                "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue 'q' in vhost '/'. " +
                "It could be originally declared on another connection or the exclusive property " +
                "value does not match that of the original declaration.",
                close.text);
            await clientB.SendAsync(MethodFrameOn(1, ChannelCloseOkFrame()));
        }
        finally
        {
            await ShakeDownAsync(server, clientA);
            await ShakeDownAsync(server, clientB);
        }
    }

    // ---------- qos variants and channel flow ----------

    [Fact]
    public async Task Qos_with_global_true_is_demoted_to_the_per_channel_limit()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // RabbitMQ 4.4 demotes global=true to per-channel when the global_qos
            // flag is off and answers qos-ok (rabbit 1583-1586, ledger correction) —
            // no hard error, and the per-channel limit is what actually gates.
            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1, global: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11); // qos-ok

            await LoadAsync(client, 1, "q", "m-one", "m-two");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            (AmqpMethod one, ContentHeader _, byte[] _) = await ReadDeliverAsync(client, 1);
            Assert.Equal(1L, one.Get("delivery-tag"));

            Assert.Null(await TryReadMethodAsync(client, 1)); // the gate holds
            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            (AmqpMethod two, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Channel_flow_true_answers_flow_ok_and_delivery_continues()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            _ = await client.ReadMethodAsync(10, 41);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            await client.SendAsync(MethodFrameOn(1, QosFrame(prefetchCount: 1)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 11);
            await LoadAsync(client, 1, "q", "m-one");
            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", consumerTag: "ct")));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 21);
            _ = await ReadDeliverAsync(client, 1); // t1

            // The Task 7 adapted ruling (the brief's off→on scenario is unreachable:
            // flow(false) is the Task 6 committed hard close). flow(true) answers
            // flow_ok with active=true (rabbit 1786-1788) and delivery continues.
            await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: true)));
            AmqpMethod flowOk = await client.ReadMethodOnChannelAsync(1, 20, 21);
            Assert.Equal(true, flowOk.Get("active"));

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
            await LoadUnfencedAsync(client, 1, "q", "m-two");
            (AmqpMethod two, ContentHeader _, byte[] body2) = await ReadDeliverAsync(client, 1);
            Assert.Equal("m-two", Encoding.UTF8.GetString(body2));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Channel_flow_false_pauses_delivery_true_resumes()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer();
        try
        {
            await HandshakeAsync(client);
            await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
            _ = await client.ReadMethodOnChannelAsync(1, 20, 11);

            // Declare queue through default exchange (routing key = queue name)
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "q", durable: false)));
            _ = await client.ReadMethodOnChannelAsync(1, 50, 11);

            // Publish a message through default exchange
            await client.SendAsync(MethodFrameOn(1, PublishFrame(channel: 1, exchange: "", routingKey: "q")));
            await client.SendAsync(ContentHeaderFrameOn(1, body: 1, properties: new ContentProperties()));
            await client.SendAsync(BodyFrameOn(1, new byte[] { 42 }));

            // channel.flow(active=false) pauses delivery
            await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: false)));
            AmqpMethod flowOk = await client.ReadMethodOnChannelAsync(1, 20, 21);
            Assert.False((bool)flowOk.Get("active")!);

            // basic.get should be paused - no message delivered
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q")));
            AmqpMethod getOk = await client.ReadMethodOnChannelAsync(1, 60, 71);
            Assert.Equal(0, (int)getOk.Get("message-count")!);
            (ContentHeader _, byte[] body) = await client.ReadContentOnChannelAsync(1);
            Assert.Empty(body);

            // Resume flow
            await client.SendAsync(MethodFrameOn(1, ChannelFlowFrame(1, active: true)));
            flowOk = await client.ReadMethodOnChannelAsync(1, 20, 21);
            Assert.True((bool)flowOk.Get("active")!);

            // Now basic.get should return the message
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q")));
            getOk = await client.ReadMethodOnChannelAsync(1, 60, 71);
            Assert.Equal(1L, getOk.Get("delivery-tag"));
            Assert.Equal(0, (int)getOk.Get("message-count")!);
            (ContentHeader _, byte[] body2) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal(1, body2.Length);
            Assert.Equal(42, body2[0]);

            await client.SendAsync(MethodFrameOn(1, AckFrame(1)));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- harness (the per-file M0 pattern mirrors PublishTests) ----------

    private static BrokerServerOptions NewOptions() => new()
    {
        Users = [new BrokerUser("guest", "guest")],
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

    private static (AmqpBrokerServer Server, WireClient ClientA, WireClient ClientB, InMemoryEngine Engine)
        SpawnTwoConnectionServer()
    {
        BrokerServerOptions options = NewOptions();
        var engine = new InMemoryEngine(options.Vhosts);
        var pairA = TransportPair.Create();
        var pairB = TransportPair.Create();
        var server = new AmqpBrokerServer(options, engine, new TransportPairConnectionListener(pairA, pairB));
        server.StartAsync();
        return (server, new WireClient(pairA.TransportClient), new WireClient(pairB.TransportClient), engine);
    }

    /// <summary>Declares the queue, publishes every body through the default exchange
    /// into it (default-exchange routing to the queue named by the routing key), then
    /// fences the batch with a same-channel passive declare so later steps know the
    /// publishes were processed. ONLY safe before consumers exist (or when nothing
    /// is deliverable): a live consumer's pump may write the deliver frames before
    /// the fence's declare-ok, which a method-discriminating read would reject.</summary>
    private static async Task LoadAsync(WireClient client, ushort channel, string queue, params string[] bodies)
    {
        // The queue must exist before the publishes route; the declare-ok is the
        // first sync point (and a passive fence of an undeclared queue would be a
        // NOT_FOUND soft close — that bug is left to the real tests).
        await DeclareAsync(client, channel, QueueDeclareFrame(channel, queue), (50, 11));

        foreach (string body in bodies)
        {
            await client.SendAsync(MethodFrameOn(channel, PublishFrame(channel, "", queue)));
            await client.SendAsync(ContentHeaderFrameOn(channel, body: Encoding.UTF8.GetByteCount(body), properties: new ContentProperties(ContentType: "text/plain")));
            await client.SendAsync(BodyFrameOn(channel, Encoding.UTF8.GetBytes(body)));
        }

        await FenceAsync(client, channel, queue);
    }

    /// <summary>The LoadAsync variant for publishing while a consumer is LIVE (and
    /// might immediately claim what is landing): no fence, the reads that follow
    /// observe the deliveries directly.</summary>
    private static async Task LoadUnfencedAsync(WireClient client, ushort channel, string queue, params string[] bodies)
    {
        foreach (string body in bodies)
        {
            await client.SendAsync(MethodFrameOn(channel, PublishFrame(channel, "", queue)));
            await client.SendAsync(ContentHeaderFrameOn(channel, body: Encoding.UTF8.GetByteCount(body), properties: new ContentProperties(ContentType: "text/plain")));
            await client.SendAsync(BodyFrameOn(channel, Encoding.UTF8.GetBytes(body)));
        }
    }

    /// <summary>Same-channel passive declare whose ok proves the frame pump reached
    /// that point in the channel's frame order.</summary>
    private static async Task FenceAsync(WireClient client, ushort channel, string queue) =>
        await DeclareQueueAsync(client, channel, queue, passive: true);

    private static Task DeclareQueueAsync(WireClient client, ushort channel, string queue, bool passive = false) =>
        DeclareAsync(client, channel, QueueDeclareFrame(channel, queue, passive: passive), (50, 11));

    private static async Task DeclareAsync(WireClient client, ushort channel, AmqpMethod method, (ushort Class, ushort Method) okId)
    {
        await client.SendAsync(MethodFrameOn(channel, method));
        _ = await client.ReadMethodOnChannelAsync(channel, okId.Class, okId.Method);
    }

    /// <summary>Reads the delivery triple: the basic.deliver method plus its content
    /// header and body frames (the method read must not run while deliver content
    /// is pending on the same channel — always pair them in that order). Deliveries
    /// parked by an earlier reply-wait are served first, in arrival order.</summary>
    private static async Task<(AmqpMethod Method, ContentHeader Header, byte[] Body)> ReadDeliverAsync(
        WireClient client, ushort channel)
    {
        if (client.TakeParkedDeliver(channel) is (AmqpMethod parked, ContentHeader parkedHeader, byte[] parkedBody))
        {
            return (parked, parkedHeader, parkedBody);
        }

        AmqpMethod method = await client.ReadMethodOnChannelAsync(channel, 60, 60);
        (ContentHeader header, byte[] body) = await client.ReadContentOnChannelAsync(channel);
        return (method, header, body);
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

    private static AmqpMethod QosFrame(int prefetchSize = 0, int prefetchCount = 0, bool global = false)
    {
        AmqpMethod method = NewMethod("basic.qos"); // prefetch-size long, prefetch-count short, global bit
        method.Set("prefetch-size", prefetchSize);
        method.Set("prefetch-count", (short)prefetchCount);
        method.Set("global", global);
        return method;
    }

    private static AmqpMethod ConsumeFrame(
        ushort channel,
        string queue,
        string consumerTag,
        bool noLocal = false,
        bool noAck = false,
        bool exclusive = false,
        bool nowait = false)
    {
        AmqpMethod method = NewMethod("basic.consume");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("consumer-tag", consumerTag);
        method.Set("no-local", noLocal);
        method.Set("no-ack", noAck);
        method.Set("exclusive", exclusive);
        method.Set("nowait", nowait);
        method.Set("arguments", new FieldTable());
        return method;
    }

    private static AmqpMethod CancelFrame(string consumerTag, bool nowait = false)
    {
        AmqpMethod method = NewMethod("basic.cancel");
        method.Set("consumer-tag", consumerTag);
        method.Set("nowait", nowait);
        return method;
    }

    private static AmqpMethod AckFrame(long deliveryTag, bool multiple = false)
    {
        AmqpMethod method = NewMethod("basic.ack");
        method.Set("delivery-tag", deliveryTag);
        method.Set("multiple", multiple);
        return method;
    }

    private static AmqpMethod NackFrame(long deliveryTag, bool multiple = false, bool requeue = true)
    {
        AmqpMethod method = NewMethod("basic.nack");
        method.Set("delivery-tag", deliveryTag);
        method.Set("multiple", multiple);
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod RejectFrame(long deliveryTag, bool requeue)
    {
        AmqpMethod method = NewMethod("basic.reject");
        method.Set("delivery-tag", deliveryTag);
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod RecoverFrame(bool requeue)
    {
        AmqpMethod method = NewMethod("basic.recover");
        method.Set("requeue", requeue);
        return method;
    }

    private static AmqpMethod GetFrame(ushort channel, string queue, bool noAck = false)
    {
        AmqpMethod method = NewMethod("basic.get");
        method.Set("ticket", (short)0);
        method.Set("queue", queue);
        method.Set("no-ack", noAck);
        return method;
    }

    private static AmqpMethod ChannelFlowFrame(ushort channel, bool active)
    {
        AmqpMethod method = NewMethod("channel.flow");
        method.Set("active", active);
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

    private static async Task<(ushort Code, string Text)> ReadChannelCloseCodeTextAsync(
        WireClient client, ushort channel)
    {
        (ushort code, string text, ushort _, ushort _) = await ReadChannelCloseAsync(client, channel);
        return (code, text);
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

    /// <summary>Harness-guarded absence check: pumps the client for a while and
    /// returns the method if ANY method frame arrived (a cross-talk or mismatch
    /// still throws), null when the window stays silent.</summary>
    private static Task<AmqpMethod?> TryReadMethodAsync(WireClient client, ushort channel, int timeoutMs = 120) =>
        client.TryReadMethodOnChannelAsync(channel, timeoutMs);

    private static AmqpMethod ChannelCloseOkFrame()
    {
        AmqpMethod method = NewMethod("channel.close-ok");
        return method;
    }

    /// <summary>
    /// The client half of a TransportPair: sends raw byte frames and decodes the
    /// real bytes the broker produces through the FrameDecoder (PublishTests' shape).
    /// </summary>
    private sealed class WireClient(TransportSide side) : IAsyncDisposable
    {
        private byte[] _buffer = new byte[4096];
        private int _length;
        private readonly Queue<AmqpFrame> _inbox = new();

        // Deliveries that overtook a method the test was awaiting (RabbitMQ-legal:
        // an already-claimed deliver may precede a later reply such as the second
        // consume-ok on the same channel). Parked in arrival order, complete with
        // content, and served to ReadDeliverAsync later.
        private readonly List<(ushort Channel, AmqpMethod Deliver, ContentHeader Header, byte[] Body)> _parked = [];

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
            AmqpMethod? result = await TryReadMethodOnChannelCoreAsync(channel, classId, methodId, timeoutMs, required: true);
            return result!;
        }

        /// <summary>The TryRead flavor: on a silent window it returns null instead of
        /// throwing (TimeoutException ⇔ broker produced no method frame in time). A
        /// method of the wrong kind still throws — absence, not failure, is optional.</summary>
        public async Task<AmqpMethod?> TryReadMethodOnChannelAsync(ushort channel, int timeoutMs = 120)
        {
            return await TryReadMethodOnChannelCoreAsync(channel, 0, 0, timeoutMs, required: false);
        }

        /// <summary>Pops the earliest parked delivery for the channel, or null when
        /// none is waiting. A zero match tuple is impossible on successful return:
        /// the delivery tag of a real deliver is always positive.</summary>
        public (AmqpMethod, ContentHeader, byte[])? TakeParkedDeliver(ushort channel)
        {
            int index = _parked.FindIndex(parked => parked.Channel == channel);
            if (index < 0)
            {
                return null;
            }

            (ushort _, AmqpMethod deliver, ContentHeader header, byte[] body) = _parked[index];
            _parked.RemoveAt(index);
            return (deliver, header, body);
        }

        private async Task<AmqpMethod?> TryReadMethodOnChannelCoreAsync(
            ushort channel, ushort classId, ushort methodId, int timeoutMs, bool required)
        {
            using var cts = new CancellationTokenSource(timeoutMs);
            while (true)
            {
                if (_inbox.Count == 0)
                {
                    try
                    {
                        await PumpAsync(cts.Token);
                    }
                    catch (TimeoutException)
                    {
                        if (required)
                        {
                            throw;
                        }

                        return null;
                    }

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
                    // RabbitMQ-legal interleave: a deliver claimed earlier by a
                    // consumer's pump can precede the reply this read is waiting for
                    // (e.g. the second consume-ok on the same channel). Park the
                    // complete triple and keep waiting rather than fail the test on
                    // a legal frame order.
                    if (actualClass == 60 && actualMethod == 60 && frame.Channel == channel)
                    {
                        AmqpMethod deliver = Decode(60, 60, ref reader);
                        (ContentHeader parkedHeader, byte[] parkedBody) =
                            await ReadContentOnChannelAsync(channel, timeoutMs);
                        _parked.Add((channel, deliver, parkedHeader, parkedBody));
                        continue;
                    }

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
