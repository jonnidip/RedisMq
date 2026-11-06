using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RedisMQ.Broker.Server;

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
        return services;
    }
}
