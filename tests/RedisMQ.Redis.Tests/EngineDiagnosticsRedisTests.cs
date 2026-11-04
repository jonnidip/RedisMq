using RedisMQ.Broker.Diagnostics;
using RedisMQ.Redis;
using Xunit;

namespace RedisMQ.Redis.Tests;

// Observability Task 2: the Redis engine's health snapshot in memory-only mode. This
// test lives here because the Broker test project does not reference RedisMQ.Redis.
public sealed class EngineDiagnosticsRedisTests
{
    [Fact]
    public async Task Redis_engine_without_database_reports_not_durable()
    {
        using var diag = new BrokerDiagnostics("engine4");
        await using var engine = new RedisStreamRouteEngine(
            null!, ["/"], replayOnStart: false, diagnostics: diag);

        EngineHealth health = engine.GetEngineHealth();

        Assert.Equal("redis", health.Kind);
        Assert.False(health.Durable);
        Assert.Null(health.RedisConnected);
        Assert.Empty(engine.QueueSnapshots());
    }
}
