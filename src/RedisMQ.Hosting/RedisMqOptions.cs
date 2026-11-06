using RedisMQ.Broker.Server;

namespace RedisMQ.Hosting;

/// <summary>The root <c>RedisMQ</c> configuration section: broker settings, the
/// optional Redis engine, and the diagnostics identity.</summary>
public sealed class RedisMqOptions
{
    public string? InstanceId { get; set; }
    public string? ServiceVersion { get; set; }
    public BrokerServerOptions Broker { get; set; } = new();
    public RedisEngineOptions Redis { get; set; } = new();
}
