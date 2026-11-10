namespace RedisMQ.Hosting;

/// <summary>Settings for the Redis-backed route engine. An empty connection string
/// selects the in-memory engine and starts no drainer.</summary>
public sealed class RedisEngineOptions
{
    public string? ConnectionString { get; set; }
    public bool ReplayOnStart { get; set; } = true;
    public int DrainerCapacity { get; set; } = 1024;
    public int DrainerBatchSize { get; set; } = 256;
    public bool EnforceEvictionGate { get; set; } = true;
}
