using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 8: the synchronous broker health snapshot. Live tracks the accept
// loops, ready tracks engine durability/connectivity, and per-queue unacked counts are
// overlaid from the server runtime.
public sealed class HealthTests
{
    [Fact]
    public void Health_before_start_is_not_live_or_ready()
    {
        using var diag = new BrokerDiagnostics("health1");
        using var engine = new InMemoryEngine(["/"], diag);
        var server = new AmqpBrokerServer(
            new BrokerServerOptions { Vhosts = ["/"] }, engine, diagnostics: diag);

        BrokerHealth health = server.GetHealth();

        Assert.False(health.Live);
        Assert.False(health.Ready);
        Assert.Equal("server is not started", health.Detail);
        Assert.Equal(0, health.ActiveConnections);
        Assert.Equal("memory", health.Engine.Kind);
    }

    [Fact]
    public async Task Health_reports_live_connections_channels_and_queue_depth()
    {
        using var diag = new BrokerDiagnostics("health2");
        await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
        await harness.HandshakeAsync();
        await harness.OpenChannelAsync();
        await harness.DeclareQueueAsync(1, "q");
        await harness.PublishAsync(1, "", "q");

        await WaitUntilAsync(
            () =>
            {
                BrokerHealth snapshot = harness.Server.GetHealth();
                return snapshot.Queues.Count == 1 && snapshot.Queues[0].Ready == 1;
            },
            "the published message was not visible to health");

        BrokerHealth health = harness.Server.GetHealth();

        Assert.True(health.Live);
        Assert.True(health.Ready);
        Assert.Null(health.Detail);
        Assert.Equal(1, health.ActiveConnections);
        Assert.Equal(1, health.ActiveChannels);
        Assert.Equal(1, Assert.Single(health.Queues).Ready);
    }

    [Fact]
    public async Task Redis_health_detail_makes_the_broker_not_ready()
    {
        using var diag = new BrokerDiagnostics("health3");
        var engine = new FakeRedisDiagnosticsEngine();
        var server = new AmqpBrokerServer(
            new BrokerServerOptions { Vhosts = ["/"] }, engine, diagnostics: diag);
        await server.StartAsync();
        try
        {
            BrokerHealth health = server.GetHealth();
            Assert.False(health.Ready);
            Assert.Equal("redis disconnected", health.Detail);
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Custom_engine_reports_custom_kind_and_stays_ready()
    {
        using var diag = new BrokerDiagnostics("health4");
        var engine = new RecordingEngine(["/"]);
        var server = new AmqpBrokerServer(
            new BrokerServerOptions { Vhosts = ["/"] }, engine, diagnostics: diag);
        await server.StartAsync();
        try
        {
            BrokerHealth health = server.GetHealth();
            Assert.True(health.Ready);
            Assert.Equal("custom", health.Engine.Kind);
            Assert.False(health.Engine.Durable);
            Assert.Empty(health.Queues);
        }
        finally
        {
            await server.StopAsync();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        Assert.Fail(description);
    }

    /// <summary>A Redis-shaped engine double: all routing delegates to memory, while
    /// health reports a configured but disconnected durable backend.</summary>
    private sealed class FakeRedisDiagnosticsEngine : IRouteEngine, IEngineDiagnostics
    {
        private readonly InMemoryEngine _memory = new(["/"]);

        public EngineHealth GetEngineHealth() =>
            new("redis", true, false, 0, 0, 0, null, null, "redis disconnected");

        public IReadOnlyList<QueueSnapshot> QueueSnapshots() => _memory.QueueSnapshots();

        public ExchangeInfo? GetExchange(string vhost, string name) => _memory.GetExchange(vhost, name);

        public ExchangeInfo? DeclareExchange(ExchangeInfo exchange) => _memory.DeclareExchange(exchange);

        public QueueInfo? GetQueue(string vhost, string name) => _memory.GetQueue(vhost, name);

        public QueueInfo? DeclareQueue(QueueInfo queue) => _memory.DeclareQueue(queue);

        public void Bind(string vhost, string exchange, string queue, string routingKey, FieldTable? arguments = null) =>
            _memory.Bind(vhost, exchange, queue, routingKey, arguments);

        public void Unbind(string vhost, string exchange, string queue, string routingKey) =>
            _memory.Unbind(vhost, exchange, queue, routingKey);

        public IReadOnlyList<string> RouteTo(
            ExchangeInfo exchange, in ContentProperties properties, string routingKey) =>
            _memory.RouteTo(exchange, properties, routingKey);

        public EnqueueReceipt Enqueue(
            string vhost,
            string queue,
            ContentProperties properties,
            byte[] body,
            string exchange,
            string routingKey) =>
            _memory.Enqueue(vhost, queue, properties, body, exchange, routingKey);

        public ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer) =>
            _memory.RegisterConsumer(vhost, queue, consumer);

        public void UnregisterConsumer(string vhost, string queue, string consumerTag) =>
            _memory.UnregisterConsumer(vhost, queue, consumerTag);

        public EngineDelta Requeue(string vhost, string queue, InMemoryMessage message) =>
            _memory.Requeue(vhost, queue, message);

        public EngineDelta Acknowledge(string vhost, string queue, InMemoryMessage message) =>
            _memory.Acknowledge(vhost, queue, message);

        public EngineDelta Reject(string vhost, string queue, InMemoryMessage message) =>
            _memory.Reject(vhost, queue, message);

        public EngineDelta Reap(DateTimeOffset now) => _memory.Reap(now);

        public GetClaimOutcome TryClaimForGet(
            string vhost, string queue, out InMemoryMessage? message, out int remaining) =>
            _memory.TryClaimForGet(vhost, queue, out message, out remaining);

        public void Nudge(string vhost, string queue) => _memory.Nudge(vhost, queue);

        public void ReconcileConnection(string connectionId) => _memory.ReconcileConnection(connectionId);

        public void RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId) =>
            _memory.RegisterExclusiveQueueOwner(vhost, queue, connectionId);
    }
}
