namespace RedisMQ.Broker.Diagnostics;

/// <summary>One declared queue's delivery state at scrape time. <see cref="Unacked"/>
/// is supplied by the server runtime for a live connection when the engine itself does
/// not track unacked deliveries (the engine interface stays connection-agnostic).</summary>
public sealed record QueueSnapshot(string Vhost, string Name, int Ready, int Consumers, int Unacked = 0);

/// <summary>The engine half of <see cref="BrokerHealth"/>: what backend is configured,
/// whether it can honour a durability promise right now, and how its mirror is doing.
/// <see cref="LastError"/> is sanitized: no credentials, connection strings or message
/// contents.</summary>
public sealed record EngineHealth(
    string Kind,
    bool Durable,
    bool? RedisConnected,
    long DrainerDepth,
    long MirrorFailures,
    long MirrorDropped,
    DateTimeOffset? LastMirrorFailureUtc,
    DateTimeOffset? LastMirrorDropUtc,
    string? LastError);

/// <summary>A synchronous health snapshot for a host's liveness/readiness endpoint.
/// <see cref="Detail"/> explains a not-ready state and is null when ready.</summary>
public sealed record BrokerHealth(
    bool Live,
    bool Ready,
    string? Detail,
    DateTimeOffset Timestamp,
    int ActiveConnections,
    int ActiveChannels,
    IReadOnlyList<QueueSnapshot> Queues,
    EngineHealth Engine);

/// <summary>Implemented by engines that can describe themselves for health checks and
/// queue-depth snapshots. A third-party engine that does not implement this reports as
/// <c>kind=custom</c> with an empty queue list.</summary>
public interface IEngineDiagnostics
{
    EngineHealth GetEngineHealth();

    IReadOnlyList<QueueSnapshot> QueueSnapshots();
}
