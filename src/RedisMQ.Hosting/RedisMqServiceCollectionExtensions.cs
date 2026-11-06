using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;
using RedisMQ.Redis;
using StackExchange.Redis;

namespace RedisMQ.Hosting;

public static class RedisMqServiceCollectionExtensions
{
    public static IServiceCollection AddRedisMQBroker(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "RedisMQ",
        Action<RedisMqOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        IConfigurationSection section = configuration.GetSection(sectionName);
        RedisMqOptions options = section.Get<RedisMqOptions>() ?? new RedisMqOptions();

        // The binder appends configured elements to the property's initialized
        // default, so a configured vhost list would otherwise come back as
        // ["/", ...configured]. A configured list replaces the default.
        IConfigurationSection vhosts = section.GetSection("Broker:Vhosts");
        if (vhosts.Exists())
        {
            options.Broker.Vhosts = vhosts.Get<string[]>() ?? [];
        }

        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton(options.Broker);

        if (!string.IsNullOrWhiteSpace(options.Redis.ConnectionString))
        {
            ConfigurationOptions redisConfiguration =
                ConfigurationOptions.Parse(options.Redis.ConnectionString);
            redisConfiguration.AbortOnConnectFail = false;
            services.TryAddSingleton<IConnectionMultiplexer>(
                _ => ConnectionMultiplexer.Connect(redisConfiguration));
        }

        services.TryAddSingleton(CreateDiagnostics);
        services.TryAddSingleton<IReadOnlyList<IListenerTransport>>(provider =>
            BrokerListeners.Create(
                provider.GetRequiredService<BrokerServerOptions>(),
                provider.GetRequiredService<BrokerDiagnostics>()));
        services.TryAddSingleton<IRouteEngine>(CreateEngine);
        services.TryAddSingleton(provider => new AmqpBrokerServer(
            provider.GetRequiredService<BrokerServerOptions>(),
            provider.GetRequiredService<IRouteEngine>(),
            provider.GetRequiredService<IReadOnlyList<IListenerTransport>>(),
            provider.GetRequiredService<BrokerDiagnostics>()));
        // The concrete hosted service is not separately registered: the container creates
        // exactly one instance to satisfy IHostedService, and that instance owns stop/dispose.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, BrokerServerHostedService>());
        return services;
    }

    private static BrokerDiagnostics CreateDiagnostics(IServiceProvider provider)
    {
        RedisMqOptions options = provider.GetRequiredService<RedisMqOptions>();
        return new BrokerDiagnostics(options.InstanceId, serviceVersion: options.ServiceVersion);
    }

    private static IRouteEngine CreateEngine(IServiceProvider provider)
    {
        RedisMqOptions options = provider.GetRequiredService<RedisMqOptions>();
        BrokerDiagnostics diagnostics = provider.GetRequiredService<BrokerDiagnostics>();

        if (string.IsNullOrWhiteSpace(options.Redis.ConnectionString))
        {
            return new InMemoryEngine(options.Broker.Vhosts, diagnostics);
        }

        IConnectionMultiplexer multiplexer =
            provider.GetRequiredService<IConnectionMultiplexer>();
        return new RedisStreamRouteEngine(
            multiplexer.GetDatabase(),
            options.Broker.Vhosts,
            options.Redis.ReplayOnStart,
            options.Redis.DrainerCapacity,
            options.Redis.EnforceEvictionGate,
            diagnostics);
    }
}
