namespace RedisMQ.Redis;

/// <summary>
/// Cluster-safe Redis key layout for the M1 write-ahead log. Every per-queue key is
/// hash-tagged so all of a queue's keys land on the same cluster slot; topology keys
/// share a single tag. No cross-slot commands are ever used.
/// </summary>
public static class RedisKeys
{
    /// <summary>Topology write-ahead log stream root pattern: every declare/bind/unbind operation.
    /// Per-vhost keys are built with <see cref="TopologyKey(string)"/>.</summary>
    public const string TopologyKeyRoot = "{mq:top}";

    /// <summary>Builds the per-vhost topology write-ahead log stream key.</summary>
    /// <param name="vhost">Vhost name (scope).</param>
    public static string TopologyKey(string vhost) => $"{{mq:top:{vhost}}}";

    /// <summary>Builds the per-queue write-ahead log stream key.</summary>
    /// <param name="vhost">Vhost name (scope).</param>
    /// <param name="queue">Queue name.</param>
    public static string QueueKey(string vhost, string queue) => $"{{mq:q:{vhost}:{queue}}}";

    /// <summary>Stream field names for topology entries.</summary>
    public static class TopologyField
    {
        public const string Op = "op";
        public const string Entity = "entity";
        public const string Kind = "kind";
        public const string Vhost = "vhost";
        public const string Name = "name";
    }

    /// <summary>Stream field names for queue (enqueue) entries.</summary>
    public static class QueueField
    {
        public const string EnvelopeId = "env";
        public const string Exchange = "ex";
        public const string RoutingKey = "rk";
        public const string Body = "body";
        public const string Properties = "props";
    }

    /// <summary>Topology operation kinds.</summary>
    public enum TopologyOp
    {
        DeclareExchange,
        DeclareQueue,
        Bind,
        Unbind,
    }
}
