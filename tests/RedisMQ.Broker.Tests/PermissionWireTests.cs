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

// M4 Task 11: per-vhost permissions are enforced over the real wire — vhost-open,
// configure on declares/binds, write on publishes and read on consume/get — with
// RabbitMQ's exact 403 ACCESS_REFUSED wording.
public sealed class PermissionWireTests
{
    [Fact]
    public async Task Full_access_user_can_declare_bind_publish_consume_and_get()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(BrokerUser.FullAccess("guest", "guest", "/")));
        try
        {
            await OpenChannelAsync(client, "guest", "guest");

            await DeclareAsync(client, 1, ExchangeDeclareFrame(1, "ex", "direct"), (40, 11));
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "q"), (50, 11));
            await DeclareAsync(client, 1, QueueBindFrame(1, "q", "ex", "rk"), (50, 21));

            await PublishAsync(client, 1, "ex", "rk", [1, 2, 3]);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "q", passive: true), (50, 11));

            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q", noAck: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 71);
            (_, byte[] body) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal(new byte[] { 1, 2, 3 }, body);

            await DeclareAsync(client, 1, ConsumeFrame(1, "q", "ct"), (60, 21));
            await DeclareAsync(client, 1, CancelFrame(1, "ct"), (60, 31));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Restricted_user_is_denied_exchange_declare_without_configure()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", "^allowed$", ".*", ".*"))));
        try
        {
            await OpenChannelAsync(client);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "denied", "direct")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to exchange 'denied' in vhost '/' refused for user 'restricted'",
                text);

            // The same account may still declare a name its configure pattern allows.
            await ReopenChannelAsync(client, 1);
            await DeclareAsync(client, 1, ExchangeDeclareFrame(1, "allowed", "direct"), (40, 11));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Restricted_user_is_denied_queue_declare_without_configure()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", "^allowed$", ".*", ".*"))));
        try
        {
            await OpenChannelAsync(client);

            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "denied")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to queue 'denied' in vhost '/' refused for user 'restricted'",
                text);

            await ReopenChannelAsync(client, 1);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "allowed"), (50, 11));
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Restricted_user_is_denied_bind_without_write_on_the_exchange()
    {
        // (a) configure is fine, the exchange write is not.
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", ".*", "^allowed$", ".*"))));
        try
        {
            await OpenChannelAsync(client);
            await DeclareAsync(client, 1, ExchangeDeclareFrame(1, "ex", "direct"), (40, 11));
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "q"), (50, 11));

            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "q", "ex", "rk")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to exchange 'ex' in vhost '/' refused for user 'restricted'",
                text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }

        // (b) write is fine, the queue configure is not (checked before existence).
        (server, client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", "^ex$", ".*", ".*"))));
        try
        {
            await OpenChannelAsync(client);
            await DeclareAsync(client, 1, ExchangeDeclareFrame(1, "ex", "direct"), (40, 11));

            await client.SendAsync(MethodFrameOn(1, QueueBindFrame(1, "never-declared", "ex", "rk")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to queue 'never-declared' in vhost '/' refused for user 'restricted'",
                text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Restricted_user_is_denied_publish_without_write_on_the_exchange()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", ".*", "^allowed$", ".*"))));
        try
        {
            await OpenChannelAsync(client);
            await DeclareAsync(client, 1, ExchangeDeclareFrame(1, "ex", "direct"), (40, 11));

            await PublishAsync(client, 1, "ex", "rk", [1]);

            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to exchange 'ex' in vhost '/' refused for user 'restricted'",
                text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Default_exchange_publish_checks_the_routing_key()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", ".*", "^allowed$", ".*"))));
        try
        {
            await OpenChannelAsync(client);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "denied-q"), (50, 11));

            // On the default exchange the routing key names the resource the write
            // permission is checked against.
            await PublishAsync(client, 1, "", "denied-q", [1]);
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to exchange 'denied-q' in vhost '/' refused for user 'restricted'",
                text);

            // An allowed routing key publishes normally.
            await ReopenChannelAsync(client, 1);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "allowed"), (50, 11));
            await PublishAsync(client, 1, "", "allowed", [7]);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "allowed", passive: true), (50, 11));
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "allowed", noAck: true)));
            _ = await client.ReadMethodOnChannelAsync(1, 60, 71);
            (_, byte[] body) = await client.ReadContentOnChannelAsync(1);
            Assert.Equal(new byte[] { 7 }, body);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Restricted_user_is_denied_consume_and_get_without_read()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", ".*", ".*", "^allowed$"))));
        try
        {
            await OpenChannelAsync(client);
            await DeclareAsync(client, 1, QueueDeclareFrame(1, "q"), (50, 11));

            await client.SendAsync(MethodFrameOn(1, ConsumeFrame(1, "q", "ct")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to queue 'q' in vhost '/' refused for user 'restricted'",
                text);

            await ReopenChannelAsync(client, 1);
            await client.SendAsync(MethodFrameOn(1, GetFrame(1, "q", noAck: true)));
            (code, text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to queue 'q' in vhost '/' refused for user 'restricted'",
                text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Access_refused_wording_is_exact()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", "^none$", "^none$", "^none$"))));
        try
        {
            await OpenChannelAsync(client);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "an-exchange", "direct")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to exchange 'an-exchange' in vhost '/' refused for user 'restricted'",
                text);

            await ReopenChannelAsync(client, 1);
            await client.SendAsync(MethodFrameOn(1, QueueDeclareFrame(1, "a-queue")));
            (code, text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to queue 'a-queue' in vhost '/' refused for user 'restricted'",
                text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Connection_open_on_a_vhost_without_a_permission_entry_is_refused()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(new BrokerUser(
                "vhostless", "pw", Permissions: [new BrokerPermission("/other", ".*", ".*", ".*")])));
        try
        {
            // HandshakeAsync sends connection.open on "/" and waits no reply itself.
            await HandshakeAsync(client, "vhostless", "pw");

            (ushort code, string text, ushort classId, ushort methodId) = await ReadConnectionCloseAsync(client);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to vhost '/' refused for user 'vhostless'",
                text);
            Assert.Equal((ushort)10, classId);
            Assert.Equal((ushort)40, methodId);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    [Fact]
    public async Task Deny_all_permission_entry_allows_open_but_denies_operations()
    {
        (AmqpBrokerServer server, WireClient client, _) = SpawnServer(
            Options(Restricted(new BrokerPermission("/", "", "", ""))));
        try
        {
            // A listed vhost is openable even when every pattern denies.
            await OpenChannelAsync(client);

            await client.SendAsync(MethodFrameOn(1, ExchangeDeclareFrame(1, "ex", "direct")));
            (ushort code, string text) = await ReadChannelCloseAsync(client, 1);
            Assert.Equal(AmqpConstants.AccessRefused, code);
            Assert.Equal(
                "ACCESS_REFUSED - access to exchange 'ex' in vhost '/' refused for user 'restricted'",
                text);
        }
        finally
        {
            await ShakeDownAsync(server, client);
        }
    }

    // ---------- harness ----------

    private static BrokerServerOptions Options(params BrokerUser[] users) => new()
    {
        Users = users,
        Vhosts = ["/", "/other"],
        FrameMax = 131072,
        Heartbeat = 60,
    };

    private static BrokerUser Restricted(params BrokerPermission[] permissions) =>
        new("restricted", "pw", Permissions: permissions);

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
        BrokerServerOptions options)
    {
        var engine = new InMemoryEngine(options.Vhosts);
        var pair = TransportPair.Create();
        var server = new AmqpBrokerServer(options, engine, new TransportPairConnectionListener(pair));
        server.StartAsync();
        return (server, new WireClient(pair.TransportClient), engine);
    }

    private static async Task OpenChannelAsync(
        WireClient client, string username = "restricted", string password = "pw")
    {
        await HandshakeAsync(client, username, password);
        _ = await client.ReadMethodAsync(10, 41);
        await client.SendAsync(MethodFrameOn(1, ChannelOpenFrame()));
        _ = await client.ReadMethodOnChannelAsync(1, 20, 11);
    }

    private static async Task ReopenChannelAsync(WireClient client, ushort channel)
    {
        await client.SendAsync(MethodFrameOn(channel, ChannelCloseOkFrame()));
        await client.SendAsync(MethodFrameOn(channel, ChannelOpenFrame()));
        _ = await client.ReadMethodOnChannelAsync(channel, 20, 11);
    }

    private static async Task DeclareAsync(
        WireClient client, ushort channel, AmqpMethod method, (ushort Class, ushort Method) okId)
    {
        await client.SendAsync(MethodFrameOn(channel, method));
        _ = await client.ReadMethodOnChannelAsync(channel, okId.Class, okId.Method);
    }

    private static async Task PublishAsync(
        WireClient client, ushort channel, string exchange, string routingKey, byte[] body)
    {
        await client.SendAsync(MethodFrameOn(channel, PublishFrame(channel, exchange, routingKey)));
        await client.SendAsync(ContentHeaderFrameOn(channel, body.Length, new ContentProperties()));
        await client.SendAsync(BodyFrameOn(channel, body));
    }

    private static async Task HandshakeAsync(WireClient client, string username, string password)
    {
        await client.SendAsync("AMQP\x00\x00\x09\x01"u8.ToArray());
        _ = await client.ReadMethodAsync(10, 10); // connection.start
        await client.SendAsync(StartOkFrame(PlainResponse(username, password)));
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

    private static AmqpMethod ExchangeDeclareFrame(
        ushort channel,
        string exchange,
        string type,
        bool passive = false,
        bool durable = false,
        bool autoDelete = false,
        bool internalExchange = false,
        bool nowait = false,
        FieldTable? arguments = null)
    {
        AmqpMethod method = NewMethod("exchange.declare");
        method.Set("ticket", (short)0);
        method.Set("exchange", exchange);
        method.Set("type", type);
        method.Set("passive", passive);
        method.Set("durable", durable);
        method.Set("auto-delete", autoDelete);
        method.Set("internal", internalExchange);
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

    private static AmqpMethod QueueBindFrame(
        ushort channel,
        string queue,
        string exchange,
        string routingKey,
        bool nowait = false,
        FieldTable? arguments = null)
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

    private static AmqpMethod CancelFrame(ushort channel, string consumerTag, bool nowait = false)
    {
        AmqpMethod method = NewMethod("basic.cancel");
        method.Set("consumer-tag", consumerTag);
        method.Set("nowait", nowait);
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

    private static async Task<(ushort Code, string Text)> ReadChannelCloseAsync(WireClient client, ushort channel)
    {
        AmqpMethod close = await client.ReadMethodOnChannelAsync(channel, 20, 40);
        return ((ushort)(short)close.Get("reply-code")!, (string)close.Get("reply-text")!);
    }

    private static async Task<(ushort Code, string Text, ushort ClassId, ushort MethodId)>
        ReadConnectionCloseAsync(WireClient client)
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

    /// <summary>The client half of a TransportPair: raw frames out, decoded frames back.
    /// Deliveries claimed early are parked so a later reply read is not confused by
    /// their interleave; content reads assemble header + body.</summary>
    private sealed class WireClient(TransportSide side) : IAsyncDisposable
    {
        private byte[] _buffer = new byte[4096];
        private int _length;
        private readonly Queue<AmqpFrame> _inbox = new();
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
                    // RabbitMQ-legal interleave: a deliver claimed earlier by a
                    // consumer's pump can precede the reply this read is waiting for.
                    if (actualClass == 60 && actualMethod == 60)
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
