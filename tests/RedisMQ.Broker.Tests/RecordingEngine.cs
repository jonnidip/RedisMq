using System;
using System.Collections.Generic;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;

namespace RedisMQ.Broker.Tests;

/// <summary>
/// The engine double the M4 mirroring tests drive: every member delegates to the
/// real <see cref="InMemoryEngine"/>, and the three settle calls also append an
/// ordered <c>"Ack|Reject|Requeue:queue:envelope-id"</c> line the test reads.
/// <see cref="EnqueueOverride"/> lets the confirm-loop tests replace the publish
/// receipt without touching routing or storage.
/// </summary>
public sealed class RecordingEngine : IRouteEngine
{
    private readonly InMemoryEngine _memory;
    private readonly List<string> _settles = [];
    private readonly object _lock = new();

    public RecordingEngine(IReadOnlyList<string> vhosts) => _memory = new InMemoryEngine(vhosts);

    /// <summary>The ordered settle trace: one line per engine settle call.</summary>
    public IReadOnlyList<string> Settles
    {
        get
        {
            lock (_lock)
            {
                return [.. _settles];
            }
        }
    }

    /// <summary>When set, replaces the enqueue receipt (confirm-loop tests).</summary>
    public Func<string, string, ContentProperties, byte[], string, string, EnqueueReceipt>? EnqueueOverride { get; set; }

    private void Record(string action, string queue, InMemoryMessage message)
    {
        lock (_lock)
        {
            _settles.Add($"{action}:{queue}:{message.EnvelopeId}");
        }
    }

    public ExchangeInfo? GetExchange(string vhost, string name) => _memory.GetExchange(vhost, name);

    public ExchangeInfo? DeclareExchange(ExchangeInfo exchange) => _memory.DeclareExchange(exchange);

    public QueueInfo? GetQueue(string vhost, string name) => _memory.GetQueue(vhost, name);

    public QueueInfo? DeclareQueue(QueueInfo queue) => _memory.DeclareQueue(queue);

    public void Bind(string vhost, string exchange, string queue, string routingKey, FieldTable? arguments = null) =>
        _memory.Bind(vhost, exchange, queue, routingKey, arguments);

    public void Unbind(string vhost, string exchange, string queue, string routingKey) =>
        _memory.Unbind(vhost, exchange, queue, routingKey);

    public IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties, string routingKey) =>
        _memory.RouteTo(exchange, properties, routingKey);

    public EnqueueReceipt Enqueue(
        string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey) =>
        EnqueueOverride is { } enqueue
            ? enqueue(vhost, queue, properties, body, exchange, routingKey)
            : _memory.Enqueue(vhost, queue, properties, body, exchange, routingKey);

    public ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer) =>
        _memory.RegisterConsumer(vhost, queue, consumer);

    public void UnregisterConsumer(string vhost, string queue, string consumerTag) =>
        _memory.UnregisterConsumer(vhost, queue, consumerTag);

    public EngineDelta Requeue(string vhost, string queue, InMemoryMessage message)
    {
        Record("Requeue", queue, message);
        return _memory.Requeue(vhost, queue, message);
    }

    public EngineDelta Acknowledge(string vhost, string queue, InMemoryMessage message)
    {
        Record("Ack", queue, message);
        return _memory.Acknowledge(vhost, queue, message);
    }

    public EngineDelta Reject(string vhost, string queue, InMemoryMessage message)
    {
        Record("Reject", queue, message);
        return _memory.Reject(vhost, queue, message);
    }

    public GetClaimOutcome TryClaimForGet(string vhost, string queue, out InMemoryMessage? message, out int remaining) =>
        _memory.TryClaimForGet(vhost, queue, out message, out remaining);

    public void Nudge(string vhost, string queue) => _memory.Nudge(vhost, queue);

    public void ReconcileConnection(string connectionId) => _memory.ReconcileConnection(connectionId);

    public void RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId) =>
        _memory.RegisterExclusiveQueueOwner(vhost, queue, connectionId);
}
