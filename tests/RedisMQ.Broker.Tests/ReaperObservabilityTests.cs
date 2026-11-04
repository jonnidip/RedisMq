using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Observability Task 6: one throwing reaper tick is counted, logged at Error, and the
// loop keeps running.
public sealed class ReaperObservabilityTests
{
    [Fact]
    public async Task A_throwing_reap_is_counted_logged_and_the_next_tick_runs()
    {
        var sink = new RecordingLogSink();
        using var diag = new BrokerDiagnostics("reap1", sink);
        using var meter = new MeterCollector();
        var engine = new FlakyReapEngine(["/"]);
        var options = new BrokerServerOptions
        {
            Vhosts = ["/"],
            ReaperInterval = TimeSpan.FromMilliseconds(20),
        };
        var server = new AmqpBrokerServer(options, engine, diagnostics: diag);
        await server.StartAsync();
        try
        {
            await WaitUntilAsync(
                () => engine.ReapCalls >= 2,
                TimeSpan.FromSeconds(3),
                "the reaper did not run a second tick after the failure");
        }
        finally
        {
            await server.StopAsync();
        }

        Assert.Equal(1, meter.For("redismq.reaper.failures")
            .Count(r => r.Tags["broker.instance"] is "reap1"));
        Assert.True(meter.For("redismq.reaper.sweeps")
            .Count(r => r.Tags["broker.instance"] is "reap1") >= 2);
        Assert.Contains(
            sink.Entries,
            e => e.Event == BrokerLogEvent.ReaperFailed && e.Level == BrokerLogLevel.Error);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string message)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Fail(message);
    }

    private sealed class FlakyReapEngine(IReadOnlyList<string> vhosts) : IRouteEngine
    {
        private readonly InMemoryEngine _memory = new(vhosts);
        private int _reapCalls;

        public int ReapCalls => Volatile.Read(ref _reapCalls);

        public EngineDelta Reap(DateTimeOffset now)
        {
            if (Interlocked.Increment(ref _reapCalls) == 1)
            {
                throw new InvalidOperationException("boom");
            }

            return _memory.Reap(now);
        }

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

        public GetClaimOutcome TryClaimForGet(
            string vhost, string queue, out InMemoryMessage? message, out int remaining) =>
            _memory.TryClaimForGet(vhost, queue, out message, out remaining);

        public void Nudge(string vhost, string queue) => _memory.Nudge(vhost, queue);

        public void ReconcileConnection(string connectionId) => _memory.ReconcileConnection(connectionId);

        public void RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId) =>
            _memory.RegisterExclusiveQueueOwner(vhost, queue, connectionId);
    }
}
