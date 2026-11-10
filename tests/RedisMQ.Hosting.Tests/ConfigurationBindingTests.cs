using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RedisMQ.Broker.Server;
using RedisMQ.Hosting;
using Xunit;

namespace RedisMQ.Hosting.Tests;

public sealed class ConfigurationBindingTests
{
    private static IConfiguration Build() =>
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["RedisMQ:InstanceId"] = "bind-1",
            ["RedisMQ:ServiceVersion"] = "9.9.9",
            ["RedisMQ:Broker:Port"] = "5673",
            ["RedisMQ:Broker:Vhosts:0"] = "/",
            ["RedisMQ:Broker:FrameMax"] = "65536",
            ["RedisMQ:Broker:ConfirmTimeout"] = "00:00:07",
            ["RedisMQ:Broker:ReaperInterval"] = "00:00:09",
            ["RedisMQ:Broker:Users:0:Name"] = "guest",
            ["RedisMQ:Broker:Users:0:Password"] = "secret",
            ["RedisMQ:Broker:Users:0:Permissions:0:Vhost"] = "/",
            ["RedisMQ:Broker:Users:0:Permissions:0:Configure"] = ".*",
            ["RedisMQ:Broker:Users:0:Permissions:0:Write"] = ".*",
            ["RedisMQ:Broker:Users:0:Permissions:0:Read"] = ".*",
            ["RedisMQ:Broker:Tls:Enabled"] = "true",
            ["RedisMQ:Broker:Tls:Port"] = "5671",
            ["RedisMQ:Broker:Tls:CertificatePath"] = "/certs/server.pfx",
            ["RedisMQ:Redis:ConnectionString"] = "localhost:6379",
            ["RedisMQ:Redis:ReplayOnStart"] = "false",
            ["RedisMQ:Redis:DrainerCapacity"] = "64",
            ["RedisMQ:Redis:DrainerBatchSize"] = "32",
            ["RedisMQ:Redis:EnforceEvictionGate"] = "false",
        }).Build();

    [Fact]
    public void Configuration_binds_broker_users_and_tls()
    {
        var services = new ServiceCollection();
        services.AddRedisMQBroker(Build());
        using ServiceProvider provider = services.BuildServiceProvider();

        RedisMqOptions options = provider.GetRequiredService<RedisMqOptions>();
        Assert.Equal("bind-1", options.InstanceId);
        Assert.Equal("9.9.9", options.ServiceVersion);
        Assert.Equal(5673, options.Broker.Port);
        Assert.Equal(["/"], options.Broker.Vhosts);
        Assert.Equal(65536u, options.Broker.FrameMax);
        Assert.Equal(TimeSpan.FromSeconds(7), options.Broker.ConfirmTimeout);
        Assert.Equal(TimeSpan.FromSeconds(9), options.Broker.ReaperInterval);
        BrokerUser user = Assert.Single(options.Broker.Users);
        Assert.Equal("guest", user.Name);
        Assert.Equal("secret", user.Password);
        BrokerPermission permission = Assert.Single(user.Permissions!);
        Assert.Equal("/", permission.Vhost);
        Assert.Equal(".*", permission.Read);
        Assert.True(options.Broker.Tls!.Enabled);
        Assert.Equal("/certs/server.pfx", options.Broker.Tls.CertificatePath);
        Assert.Same(options.Broker, provider.GetRequiredService<BrokerServerOptions>());
    }

    [Fact]
    public void Configuration_binds_redis_engine_options()
    {
        var services = new ServiceCollection();
        services.AddRedisMQBroker(Build());
        using ServiceProvider provider = services.BuildServiceProvider();

        RedisEngineOptions redis = provider.GetRequiredService<RedisMqOptions>().Redis;
        Assert.Equal("localhost:6379", redis.ConnectionString);
        Assert.False(redis.ReplayOnStart);
        Assert.Equal(64, redis.DrainerCapacity);
        Assert.Equal(32, redis.DrainerBatchSize);
        Assert.False(redis.EnforceEvictionGate);
    }
}
