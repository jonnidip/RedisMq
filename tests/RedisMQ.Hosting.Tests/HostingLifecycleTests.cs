using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Hosting;
using RedisMQ.Redis;
using Xunit;

namespace RedisMQ.Hosting.Tests;

public sealed class HostingLifecycleTests
{
    private static IHost BuildHost(params (string Key, string Value)[] settings)
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        // Later entries win; callers override MemoryBroker() values by appending keys.
        Dictionary<string, string?> values = [];
        foreach ((string key, string value) in settings)
        {
            values[key] = value;
        }

        builder.Configuration.AddInMemoryCollection(values);
        builder.Services.AddRedisMQBroker(builder.Configuration);
        return builder.Build();
    }

    private static (string, string)[] MemoryBroker(int port = 0) =>
    [
        ("RedisMQ:Broker:Port", port.ToString()),
        ("RedisMQ:Broker:Vhosts:0", "/"),
        ("RedisMQ:Broker:Users:0:Name", "guest"),
        ("RedisMQ:Broker:Users:0:Password", "guest"),
        ("RedisMQ:Broker:Users:0:Permissions:0:Vhost", "/"),
        ("RedisMQ:Broker:Users:0:Permissions:0:Configure", ".*"),
        ("RedisMQ:Broker:Users:0:Permissions:0:Write", ".*"),
        ("RedisMQ:Broker:Users:0:Permissions:0:Read", ".*"),
        ("RedisMQ:Redis:ConnectionString", ""),
    ];

    [Fact]
    public async Task AddRedisMQBroker_registers_and_starts_a_memory_broker()
    {
        using IHost host = BuildHost(MemoryBroker());
        await host.StartAsync(TestContext.Current.CancellationToken);

        AmqpBrokerServer server = host.Services.GetRequiredService<AmqpBrokerServer>();
        Assert.True(server.GetHealth().Live);
        Assert.True(server.GetHealth().Ready);
        Assert.IsType<InMemoryEngine>(host.Services.GetRequiredService<IRouteEngine>());

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Hosted_service_stops_the_broker_and_releases_its_port()
    {
        int port = FreePort();
        using IHost host = BuildHost(MemoryBroker(port));
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(CanConnect(port));

        await host.StopAsync(TestContext.Current.CancellationToken);
        AmqpBrokerServer server = host.Services.GetRequiredService<AmqpBrokerServer>();
        Assert.False(server.GetHealth().Live);
        Assert.False(CanConnect(port));

        // A second stop/dispose must be harmless.
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Registering_twice_does_not_duplicate_services()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(
            MemoryBroker().ToDictionary(pair => pair.Item1, pair => (string?)pair.Item2));
        builder.Services.AddRedisMQBroker(builder.Configuration);
        builder.Services.AddRedisMQBroker(builder.Configuration);

        Assert.Single(builder.Services, descriptor => descriptor.ServiceType == typeof(AmqpBrokerServer));
        Assert.Single(builder.Services, descriptor => descriptor.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task No_enabled_listener_fails_startup()
    {
        using IHost host = BuildHost(
            [.. MemoryBroker(), ("RedisMQ:Broker:Port", "-1")]);
        await Assert.ThrowsAsync<BrokerConfigurationException>(
            () => host.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Invalid_broker_user_fails_startup()
    {
        using IHost host = BuildHost(
            [
                .. MemoryBroker(),
                ("RedisMQ:Broker:Users:1:Name", "guest"),
                ("RedisMQ:Broker:Users:1:Password", "other"),
            ]);
        await Assert.ThrowsAsync<BrokerConfigurationException>(
            () => host.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Empty_users_warns_and_still_starts()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["RedisMQ:Broker:Port"] = "0",
            ["RedisMQ:Broker:Vhosts:0"] = "/",
            ["RedisMQ:Redis:ConnectionString"] = "",
        });
        var logProvider = new CapturingLoggerProvider();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(logProvider);
        builder.Services.AddRedisMQBroker(builder.Configuration);

        using IHost host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(host.Services.GetRequiredService<AmqpBrokerServer>().GetHealth().Live);
        Assert.Contains(logProvider.Entries, entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("no broker users"));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Redis_configured_but_unreachable_reports_not_ready()
    {
        IHost host = BuildHost(
            [
                .. MemoryBroker(),
                ("RedisMQ:Redis:ConnectionString",
                    "127.0.0.1:1,connectTimeout=1000,connectRetry=1,abortConnect=false"),
                ("RedisMQ:Redis:ReplayOnStart", "false"),
                ("RedisMQ:Redis:EnforceEvictionGate", "false"),
            ]);
        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            AmqpBrokerServer server = host.Services.GetRequiredService<AmqpBrokerServer>();
            Assert.IsType<RedisStreamRouteEngine>(host.Services.GetRequiredService<IRouteEngine>());
            Assert.False(server.GetHealth().Ready);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            await DisposeHostAsync(host);
        }
    }

    // RedisStreamRouteEngine is IAsyncDisposable-only, so a synchronous provider
    // dispose throws; the host built by HostApplicationBuilder is IAsyncDisposable.
    private static async ValueTask DisposeHostAsync(IHost host)
    {
        if (host is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else
        {
            host.Dispose();
        }
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        int port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static bool CanConnect(int port)
    {
        try
        {
            using var client = new TcpClient();
            client.Connect("127.0.0.1", port);
            return client.Connected;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly CapturingLogger _logger = new();

        public List<(LogLevel Level, string Message)> Entries => _logger.Entries;

        public ILogger CreateLogger(string categoryName) => _logger;

        public void Dispose()
        {
        }

        private sealed class CapturingLogger : ILogger
        {
            public List<(LogLevel Level, string Message)> Entries { get; } = [];

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
                => Entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
