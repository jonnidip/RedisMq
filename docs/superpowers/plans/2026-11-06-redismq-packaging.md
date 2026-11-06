# RedisMQ Packaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship RedisMQ as three NuGet packages plus a container-ready sample host, with configuration binding and DI lifecycle, without adding a package reference to the core.

**Architecture:** A new `RedisMQ.Hosting` package binds `RedisMQ` configuration into `RedisMqOptions`, registers `BrokerDiagnostics`/engine/listeners/`AmqpBrokerServer` as DI singletons, and owns start/stop through an `IHostedService`. A new `samples/RedisMQ.Host` Web SDK app exposes host-owned `/livez` and `/healthz`; a multi-stage Dockerfile and compose file wrap it. Metadata lives once in a root `Directory.Build.props`.

**Tech Stack:** .NET 10, `Microsoft.Extensions.*` 10.0.0, StackExchange.Redis (via `RedisMQ.Redis`), xunit.v3, Docker multi-stage build, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-05-redismq-packaging-design.md`

## Global Constraints

- `src/RedisMQ.Broker/RedisMQ.Broker.csproj` keeps **zero** `PackageReference`s; every new dependency goes in `RedisMQ.Hosting`, the sample, or tests.
- Everything targets `net10.0`; do not multi-target.
- Package version is exactly `0.1.0-preview.1`; licence is MIT; repository URL is `https://github.com/jonnidip/RedisMq`.
- The container runs as the image's non-root `app` user and starts memory-only when `Redis:ConnectionString` is empty.
- Do not publish to nuget.org, sign packages, or add a `/metrics` endpoint.
- Existing public constructors/APIs keep working; new parameters are optional and appended.
- Full build stays at **0 errors and exactly 86 analyzer warnings** (CS8600:2, CS8604:14, xUnit1031:1, xUnit1051:67, CA2022:1, CS8601:1). New test code uses `TestContext.Current.CancellationToken` and predicate `Assert.Single` overloads.
- Existing suites stay green: Broker 359/0, Conformance 19/0, Redis 67/0.
- Local test runner is `dotnet exec tests\<Proj>\bin\Debug\net10.0\<Proj>.dll -noColor`; `dotnet test` is broken on this machine. CI keeps using `dotnet test`.
- PowerShell uses `;`, not `&&`. Do not chain unrelated shell commands with `echo` separators.

## Review Focus

These are the inputs/conditions most likely to bite a user; each has a test in the owning task.

1. **A bare container with no Redis must start** (memory-only default) — Task 5 verification and Task 6 docker run.
2. **Redis configured but unreachable** must either fail fast (default) or report `ready=false` when the replay/eviction gates are disabled — Task 3 `Redis_configured_but_unreachable_reports_not_ready`.
3. **An empty user list** must warn and refuse clients, never invent an account — Task 3 `Empty_users_warns_and_still_starts`.
4. **Stopping and disposing twice** must not throw and must release the AMQP port - Task 3 `Hosted_service_stops_the_broker_and_releases_its_port`.
5. **Configuration values** (arrays, `TimeSpan`, booleans, environment overrides) must bind exactly as documented — Task 2 binding tests and Task 5 env-override check.

---

### Task 1: Package metadata, licence, and pack verification

**Files:**
- Create: `Directory.Build.props`
- Create: `LICENSE`
- Modify: `tests/RedisMQ.Broker.Tests/RedisMQ.Broker.Tests.csproj`
- Modify: `tests/RedisMQ.Conformance/RedisMQ.Conformance.csproj`
- Modify: `tests/RedisMQ.Redis.Tests/RedisMQ.Redis.Tests.csproj`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces: shared MSBuild metadata (`Version`, `Authors`, `PackageLicenseExpression`, `PackageTags`, `PackageReadmeFile`, `PackageProjectUrl`, `RepositoryType`, `RepositoryUrl`, `IncludeSymbols`, `SymbolPackageFormat`, `Deterministic`) applied to every project.

- [ ] **Step 1: Create `Directory.Build.props` at the repo root**

```xml
<Project>
  <PropertyGroup>
    <Version>0.1.0-preview.1</Version>
    <Authors>RedisMQ contributors</Authors>
    <Company>RedisMQ contributors</Company>
    <Description>AMQP 0-9-1 broker with a Redis Streams write-ahead log.</Description>
    <PackageLicenseExpression>MIT</PackageLicenseExpression>
    <PackageTags>amqp;rabbitmq;redis;broker;streams;messaging</PackageTags>
    <PackageReadmeFile>README.md</PackageReadmeFile>
    <PackageProjectUrl>https://github.com/jonnidip/RedisMq</PackageProjectUrl>
    <RepositoryType>git</RepositoryType>
    <RepositoryUrl>https://github.com/jonnidip/RedisMq</RepositoryUrl>
    <IncludeSymbols>true</IncludeSymbols>
    <SymbolPackageFormat>snupkg</SymbolPackageFormat>
    <Deterministic>true</Deterministic>
  </PropertyGroup>

  <ItemGroup>
    <None Include="$(MSBuildThisFileDirectory)README.md" Pack="true" PackagePath="\" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Create `LICENSE` with the MIT text**

Copyright line: `Copyright (c) 2026 RedisMQ contributors`. Use the standard SPDX MIT text.

- [ ] **Step 3: Mark the three existing test projects non-packable**

Add `<IsPackable>false</IsPackable>` inside each `<PropertyGroup>` in the three test csproj files.

- [ ] **Step 4: Ignore pack output**

Append to `.gitignore`:

```gitignore
# Pack output
artifacts/
```

- [ ] **Step 5: Verify the package contents**

Run:

```powershell
dotnet pack src\RedisMQ.Broker\RedisMQ.Broker.csproj -c Release -o artifacts
```

Expected: `Successfully created package '...\artifacts\RedisMQ.Broker.0.1.0-preview.1.nupkg'` and a `.snupkg`.

Then run:

```powershell
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [IO.Compression.ZipFile]::OpenRead((Resolve-Path artifacts\RedisMQ.Broker.0.1.0-preview.1.nupkg))
$zip.Entries.FullName | Sort-Object
$entry = $zip.GetEntry('RedisMQ.Broker.nuspec')
(New-Object IO.StreamReader($entry.Open())).ReadToEnd()
$zip.Dispose()
```

Expected entries include `README.md`, `lib/net10.0/RedisMQ.Broker.dll`, `RedisMQ.Broker.nuspec`. The nuspec contains `<license type="expression">MIT</license>`, `<readme>README.md</readme>`, and `<repository type="git" url="https://github.com/jonnidip/RedisMq" />`.

- [ ] **Step 6: Verify the core still has no package references**

Run:

```powershell
dotnet list src\RedisMQ.Broker\RedisMQ.Broker.csproj package --include-transitive
```

Expected: a "no packages" message for the project. If any package appears, stop and remove it.

- [ ] **Step 7: Commit**

```powershell
git add Directory.Build.props LICENSE .gitignore tests/RedisMQ.Broker.Tests/RedisMQ.Broker.Tests.csproj tests/RedisMQ.Conformance/RedisMQ.Conformance.csproj tests/RedisMQ.Redis.Tests/RedisMQ.Redis.Tests.csproj
git commit -m "build: add package metadata and licence"
```

---

### Task 2: `RedisMQ.Hosting` project and configuration binding

**Files:**
- Create: `src/RedisMQ.Hosting/RedisMQ.Hosting.csproj`
- Create: `src/RedisMQ.Hosting/RedisMqOptions.cs`
- Create: `src/RedisMQ.Hosting/RedisEngineOptions.cs`
- Create: `src/RedisMQ.Hosting/RedisMqServiceCollectionExtensions.cs`
- Create: `tests/RedisMQ.Hosting.Tests/RedisMQ.Hosting.Tests.csproj`
- Create: `tests/RedisMQ.Hosting.Tests/ConfigurationBindingTests.cs`
- Modify: `RedisMQ.sln` (add both projects)

**Interfaces:**
- Consumes: `BrokerServerOptions`/`BrokerUser`/`BrokerPermission`/`BrokerTlsOptions` from `RedisMQ.Broker.Server`.
- Produces:
  - `public sealed class RedisMqOptions { string? InstanceId; string? ServiceVersion; BrokerServerOptions Broker; RedisEngineOptions Redis; }`
  - `public sealed class RedisEngineOptions { string? ConnectionString; bool ReplayOnStart = true; int DrainerCapacity = 1024; bool EnforceEvictionGate = true; }`
  - `public static IServiceCollection AddRedisMQBroker(this IServiceCollection services, IConfiguration configuration, string sectionName = "RedisMQ", Action<RedisMqOptions>? configure = null)` — in this task it binds and registers `RedisMqOptions` and `BrokerServerOptions` only; later tasks extend it.

- [ ] **Step 1: Create the two projects and add them to the solution**

`src/RedisMQ.Hosting/RedisMQ.Hosting.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <LangVersion>latest</LangVersion>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.Configuration.Binder" Version="10.0.0" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="10.0.0" />
    <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.0.0" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.0" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\RedisMQ.Broker\RedisMQ.Broker.csproj" />
    <ProjectReference Include="..\RedisMQ.Redis\RedisMQ.Redis.csproj" />
  </ItemGroup>
</Project>
```

`tests/RedisMQ.Hosting.Tests/RedisMQ.Hosting.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <LangVersion>latest</LangVersion>
    <ImplicitUsings>enable</ImplicitUsings>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="18.11.0" />
    <PackageReference Include="xunit.v3" Version="4.0.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="4.0.1">
      <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
      <PrivateAssets>all</PrivateAssets>
    </PackageReference>
    <PackageReference Include="Microsoft.Extensions.Hosting" Version="10.0.0" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\..\src\RedisMQ.Hosting\RedisMQ.Hosting.csproj" />
    <ProjectReference Include="..\..\src\RedisMQ.Broker\RedisMQ.Broker.csproj" />
    <ProjectReference Include="..\..\src\RedisMQ.Redis\RedisMQ.Redis.csproj" />
  </ItemGroup>
</Project>
```

Run:

```powershell
dotnet sln RedisMQ.sln add src\RedisMQ.Hosting\RedisMQ.Hosting.csproj tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
```

- [ ] **Step 2: Write the failing binding tests**

`tests/RedisMQ.Hosting.Tests/ConfigurationBindingTests.cs`:

```csharp
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
        Assert.False(redis.EnforceEvictionGate);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

```powershell
dotnet build tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
```

Expected: compile errors for missing `RedisMqOptions`, `RedisEngineOptions` and `AddRedisMQBroker`.

- [ ] **Step 4: Implement the options and the binding extension**

`src/RedisMQ.Hosting/RedisMqOptions.cs`:

```csharp
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
```

`src/RedisMQ.Hosting/RedisEngineOptions.cs`:

```csharp
namespace RedisMQ.Hosting;

/// <summary>Settings for the Redis-backed route engine. An empty connection string
/// selects the in-memory engine and starts no drainer.</summary>
public sealed class RedisEngineOptions
{
    public string? ConnectionString { get; set; }
    public bool ReplayOnStart { get; set; } = true;
    public int DrainerCapacity { get; set; } = 1024;
    public bool EnforceEvictionGate { get; set; } = true;
}
```

`src/RedisMQ.Hosting/RedisMqServiceCollectionExtensions.cs` (binding only for this task):

```csharp
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

        RedisMqOptions options =
            configuration.GetSection(sectionName).Get<RedisMqOptions>() ?? new RedisMqOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton(options.Broker);
        return services;
    }
}
```

If `Get<T>()` cannot construct the positional `BrokerUser`/`BrokerPermission` records, add internal DTO classes `BrokerUserConfiguration`/`BrokerPermissionConfiguration` and map them in `AddRedisMQBroker`; keep the JSON shape above unchanged. Do not change the records' public shape.

- [ ] **Step 5: Run the binding tests to verify they pass**

```powershell
dotnet build tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
```

Expected: Total: 2, Failed: 0.

- [ ] **Step 6: Commit**

```powershell
git add src/RedisMQ.Hosting tests/RedisMQ.Hosting.Tests RedisMQ.sln
git commit -m "feat(hosting): bind broker options and redis settings"
```

---

### Task 3: DI registration, engine selection, and hosted lifecycle

**Files:**
- Create: `src/RedisMQ.Hosting/BrokerServerHostedService.cs`
- Modify: `src/RedisMQ.Hosting/RedisMqServiceCollectionExtensions.cs`
- Create: `tests/RedisMQ.Hosting.Tests/HostingLifecycleTests.cs`

**Interfaces:**
- Consumes: `BrokerListeners.Create(options, diagnostics)`, `InMemoryEngine(vhosts, diagnostics)`, `RedisStreamRouteEngine(IDatabase?, vhosts, replayOnStart, drainerCapacity, enforceEvictionGate, diagnostics)`, `AmqpBrokerServer(options, engine, listeners, diagnostics)`, `BrokerDiagnostics(instanceId, logSink, serviceVersion)`.
- Produces:
  - `public sealed class BrokerServerHostedService : IHostedService, IDisposable, IAsyncDisposable` with ctor `(RedisMqOptions options, AmqpBrokerServer server, IReadOnlyList<IListenerTransport> listeners, ILoggerFactory? loggerFactory = null)`.
  - Registered singletons: `RedisMqOptions`, `BrokerServerOptions`, `BrokerDiagnostics`, `IRouteEngine`, `IReadOnlyList<IListenerTransport>`, `IConnectionMultiplexer` (only when configured), `AmqpBrokerServer`, and exactly one `IHostedService` (the concrete `BrokerServerHostedService` is not separately registered).

- [ ] **Step 1: Write the failing lifecycle tests**

`tests/RedisMQ.Hosting.Tests/HostingLifecycleTests.cs`:

```csharp
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
```

Expected: the lifecycle tests fail at run time because `AmqpBrokerServer`, `IRouteEngine` and `IHostedService` are not registered; `No_enabled_listener_fails_startup` gets no exception.

- [ ] **Step 3: Implement `BrokerServerHostedService`**

`src/RedisMQ.Hosting/BrokerServerHostedService.cs`:

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;

namespace RedisMQ.Hosting;

public sealed class BrokerServerHostedService : IHostedService, IDisposable, IAsyncDisposable
{
    private readonly RedisMqOptions _options;
    private readonly AmqpBrokerServer _server;
    private readonly IReadOnlyList<IListenerTransport> _listeners;
    private readonly ILogger? _logger;
    private int _started;
    private int _stopped;

    public BrokerServerHostedService(
        RedisMqOptions options,
        AmqpBrokerServer server,
        IReadOnlyList<IListenerTransport> listeners,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options;
        _server = server;
        _listeners = listeners;
        _logger = loggerFactory?.CreateLogger("RedisMQ.Hosting");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _started) != 0)
        {
            return;
        }

        if (_options.Broker.Port < 0 && _options.Broker.Tls is not { Enabled: true })
        {
            throw new BrokerConfigurationException(
                "RedisMQ has no enabled listener: set Broker:Port >= 0 or Broker:Tls:Enabled=true");
        }

        _options.Broker.Validate();
        if (_options.Broker.Users.Count == 0)
        {
            _logger?.LogWarning(
                "RedisMQ has no broker users; default-deny means every client will be refused");
        }

        await _server.StartAsync();
        Interlocked.Exchange(ref _started, 1);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        await _server.StopAsync();
        foreach (IListenerTransport listener in _listeners)
        {
            if (listener is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync();
            }
            else if (listener is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    public async ValueTask DisposeAsync() => await StopAsync(CancellationToken.None);

    // Hosts that dispose their provider synchronously must not fail on this service.
    public void Dispose() => StopAsync(CancellationToken.None).GetAwaiter().GetResult();
}
```

Note: `StopAsync` must also be safe when `StartAsync` never ran (the `_server.StopAsync()` is a no-op when not started).

- [ ] **Step 4: Extend `AddRedisMQBroker` with diagnostics, engine, listeners, server and hosted service**

Add to the extension file:

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Transport;
using RedisMQ.Redis;
using StackExchange.Redis;

// inside AddRedisMQBroker, after the options registrations:
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
```

Add the two private factories in the same static class:

```csharp
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
```

Register the multiplexer before the engine, only when configured:

```csharp
if (!string.IsNullOrWhiteSpace(options.Redis.ConnectionString))
{
    ConfigurationOptions redisConfiguration =
        ConfigurationOptions.Parse(options.Redis.ConnectionString);
    redisConfiguration.AbortOnConnectFail = false;
    services.TryAddSingleton<IConnectionMultiplexer>(
        _ => ConnectionMultiplexer.Connect(redisConfiguration));
}
```

Do not add the `LoggerBrokerLogSink` yet; that is Task 4. `CreateDiagnostics` uses the null sink for now.

- [ ] **Step 5: Run the full hosting test suite**

```powershell
dotnet build tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
```

Expected: Total: 9, Failed: 0 (2 binding + 7 lifecycle). The unreachable-Redis test may take about a second.

- [ ] **Step 6: Run the existing suites to prove no regression**

```powershell
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -noColor
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll -noColor
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor
```

Expected: 359/0, 19/0, 67/0.

- [ ] **Step 7: Commit**

```powershell
git add src/RedisMQ.Hosting tests/RedisMQ.Hosting.Tests
git commit -m "feat(hosting): register and run the broker in a generic host"
```

---

### Task 4: Forward broker logs to `ILogger`

**Files:**
- Create: `src/RedisMQ.Hosting/LoggerBrokerLogSink.cs`
- Create: `tests/RedisMQ.Hosting.Tests/LogSinkTests.cs`
- Modify: `src/RedisMQ.Hosting/RedisMqServiceCollectionExtensions.cs` (diagnostics factory)

**Interfaces:**
- Consumes: `IBrokerLogSink`, `BrokerLogEntry`, `BrokerLogLevel`, `BrokerLogEvent`.
- Produces: `public sealed class LoggerBrokerLogSink : IBrokerLogSink` with ctor `(ILoggerFactory loggerFactory)`.

- [ ] **Step 1: Write the failing log-sink tests**

`tests/RedisMQ.Hosting.Tests/LogSinkTests.cs`:

```csharp
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Hosting;
using Xunit;

namespace RedisMQ.Hosting.Tests;

public sealed class LogSinkTests
{
    [Fact]
    public void Log_sink_maps_level_event_id_and_structured_data()
    {
        var factory = new CapturingLoggerFactory();
        var sink = new LoggerBrokerLogSink(factory);

        sink.Log(new BrokerLogEntry(
            BrokerLogLevel.Warning,
            BrokerLogEvent.ConfirmNack,
            "publish confirm refused",
            null,
            [new KeyValuePair<string, object?>("sequence", 7L)]));

        CapturedLog entry = Assert.Single(factory.Logger.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal((int)BrokerLogEvent.ConfirmNack, entry.EventId);
        Assert.Equal("publish confirm refused", entry.Message);
        Assert.Contains(entry.Data, pair => pair.Key == "sequence" && (long?)pair.Value == 7L);
    }

    [Fact]
    public void Log_sink_is_enabled_uses_the_mapped_level()
    {
        var factory = new CapturingLoggerFactory();
        var sink = new LoggerBrokerLogSink(factory);
        Assert.True(sink.IsEnabled(BrokerLogLevel.Error));
        Assert.False(sink.IsEnabled(BrokerLogLevel.Trace));
    }

    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public CapturingLogger Logger { get; } = new();
        public ILogger CreateLogger(string categoryName) => Logger;
        public void AddProvider(ILoggerProvider provider) { }
        public void Dispose() { }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<CapturedLog> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            IReadOnlyList<KeyValuePair<string, object?>> data =
                state as IReadOnlyList<KeyValuePair<string, object?>> ?? [];
            Entries.Add(new CapturedLog(logLevel, eventId.Id, formatter(state, exception), data));
        }
    }

    private sealed record CapturedLog(
        LogLevel Level,
        int EventId,
        string Message,
        IReadOnlyList<KeyValuePair<string, object?>> Data);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
```

Expected: compile error for missing `LoggerBrokerLogSink`.

- [ ] **Step 3: Implement the sink**

`src/RedisMQ.Hosting/LoggerBrokerLogSink.cs`:

```csharp
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Diagnostics;

namespace RedisMQ.Hosting;

public sealed class LoggerBrokerLogSink : IBrokerLogSink
{
    private readonly ILogger _logger;

    public LoggerBrokerLogSink(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _logger = loggerFactory.CreateLogger("RedisMQ.Broker");
    }

    public bool IsEnabled(BrokerLogLevel level) => _logger.IsEnabled(Map(level));

    public void Log(in BrokerLogEntry entry)
    {
        // Copy the in-parameter: a lambda may not capture an in parameter.
        BrokerLogEntry value = entry;
        IReadOnlyList<KeyValuePair<string, object?>> data =
            value.Data ?? Array.Empty<KeyValuePair<string, object?>>();
        _logger.Log(
            Map(value.Level),
            new EventId((int)value.Event, value.Event.ToString()),
            data,
            value.Exception,
            (_, _) => value.Message);
    }

    private static LogLevel Map(BrokerLogLevel level) => level switch
    {
        BrokerLogLevel.Trace => LogLevel.Trace,
        BrokerLogLevel.Debug => LogLevel.Debug,
        BrokerLogLevel.Information => LogLevel.Information,
        BrokerLogLevel.Warning => LogLevel.Warning,
        BrokerLogLevel.Error => LogLevel.Error,
        BrokerLogLevel.Critical => LogLevel.Critical,
        _ => LogLevel.None,
    };
}
```

- [ ] **Step 4: Wire the sink into the diagnostics factory**

Change `CreateDiagnostics` to use `ILoggerFactory` when present:

```csharp
private static BrokerDiagnostics CreateDiagnostics(IServiceProvider provider)
{
    RedisMqOptions options = provider.GetRequiredService<RedisMqOptions>();
    ILoggerFactory? loggerFactory = provider.GetService<ILoggerFactory>();
    IBrokerLogSink sink = loggerFactory is null
        ? NullBrokerLogSink.Instance
        : new LoggerBrokerLogSink(loggerFactory);
    return new BrokerDiagnostics(options.InstanceId, sink, options.ServiceVersion);
}
```

- [ ] **Step 5: Run the hosting suite**

```powershell
dotnet build tests\RedisMQ.Hosting.Tests\RedisMQ.Hosting.Tests.csproj
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
```

Expected: Total: 11, Failed: 0.

- [ ] **Step 6: Commit**

```powershell
git add src/RedisMQ.Hosting tests/RedisMQ.Hosting.Tests
git commit -m "feat(hosting): forward broker logs to ILogger"
```

---

### Task 5: Sample host with host-owned health endpoints

**Files:**
- Create: `samples/RedisMQ.Host/RedisMQ.Host.csproj`
- Create: `samples/RedisMQ.Host/Program.cs`
- Create: `samples/RedisMQ.Host/appsettings.json`
- Modify: `RedisMQ.sln` (add the sample)

**Interfaces:**
- Consumes: `AddRedisMQBroker`, `AmqpBrokerServer.GetHealth()`, `BrokerHealth`.
- Produces: a runnable host that binds AMQP on 5672 and HTTP on 8080, with `GET /`, `GET /livez`, `GET /healthz`.

- [ ] **Step 1: Create the sample project and add it to the solution**

`RedisMQ.Host.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <LangVersion>latest</LangVersion>
    <ImplicitUsings>enable</ImplicitUsings>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\..\src\RedisMQ.Hosting\RedisMQ.Hosting.csproj" />
  </ItemGroup>
</Project>
```

Run:

```powershell
dotnet sln RedisMQ.sln add samples\RedisMQ.Host\RedisMQ.Host.csproj
```

- [ ] **Step 2: Write the host**

`Program.cs`:

```csharp
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Server;
using RedisMQ.Hosting;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddRedisMQBroker(builder.Configuration);

WebApplication app = builder.Build();

app.MapGet("/", (IConfiguration configuration) =>
    Results.Text(
        $"RedisMQ broker — AMQP {configuration["RedisMQ:Broker:Port"] ?? "5672"}, " +
        $"HTTP {configuration["Urls"] ?? "http://+:8080"}"));

app.MapGet("/livez", (AmqpBrokerServer server) =>
{
    BrokerHealth health = server.GetHealth();
    return health.Live ? Results.Ok() : Results.StatusCode(503);
});

app.MapGet("/healthz", (AmqpBrokerServer server) =>
{
    BrokerHealth health = server.GetHealth();
    return Results.Json(health, statusCode: health.Ready ? 200 : 503);
});

await app.RunAsync();
```

`appsettings.json`:

```json
{
  "Urls": "http://+:8080",
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "RedisMQ": {
    "InstanceId": "redismq-host",
    "Broker": {
      "Port": 5672,
      "Vhosts": ["/"],
      "Users": [
        {
          "Name": "guest",
          "Password": "guest",
          "Permissions": [
            { "Vhost": "/", "Configure": ".*", "Write": ".*", "Read": ".*" }
          ]
        }
      ],
      "Tls": { "Enabled": false }
    },
    "Redis": { "ConnectionString": "" }
  }
}
```

- [ ] **Step 3: Verify the host starts, serves health, and honours an environment override**

```powershell
dotnet build samples\RedisMQ.Host\RedisMQ.Host.csproj
$p = Start-Process samples\RedisMQ.Host\bin\Debug\net10.0\RedisMQ.Host.exe -PassThru
Start-Sleep -Seconds 4
Invoke-RestMethod http://localhost:8080/healthz | ConvertTo-Json -Depth 6
[Net.Sockets.TcpClient]::new('127.0.0.1', 5672).Connected
Stop-Process -Id $p.Id
```

Expected: the health JSON has `live: true`, `ready: true`, and `engine.kind: "memory"`; the TCP check prints `True`. Then run again with an override:

```powershell
$env:RedisMQ__Broker__Port = '5673'
$p = Start-Process samples\RedisMQ.Host\bin\Debug\net10.0\RedisMQ.Host.exe -PassThru
Start-Sleep -Seconds 4
[Net.Sockets.TcpClient]::new('127.0.0.1', 5673).Connected
Stop-Process -Id $p.Id
Remove-Item Env:\RedisMQ__Broker__Port
```

Expected: `True` on 5673.

- [ ] **Step 4: Commit**

```powershell
git add samples/RedisMQ.Host RedisMQ.sln
git commit -m "feat(samples): add a container-ready broker host"
```

---

### Task 6: Container image and compose example

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: the sample host and its default configuration.
- Produces: `redismq:dev` image (AMQP 5672, HTTP 8080) and a compose stack with Redis.

- [ ] **Step 1: Write `.dockerignore`**

```text
**/bin
**/obj
.git
.gitignore
.idea
.superpowers
TestResults
artifacts
docs
tests
*.user
```

- [ ] **Step 2: Write the multi-stage `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props ./
COPY src/RedisMQ.Broker/RedisMQ.Broker.csproj src/RedisMQ.Broker/
COPY src/RedisMQ.Redis/RedisMQ.Redis.csproj src/RedisMQ.Redis/
COPY src/RedisMQ.Hosting/RedisMQ.Hosting.csproj src/RedisMQ.Hosting/
COPY samples/RedisMQ.Host/RedisMQ.Host.csproj samples/RedisMQ.Host/
RUN dotnet restore samples/RedisMQ.Host/RedisMQ.Host.csproj

COPY . .
RUN dotnet publish samples/RedisMQ.Host/RedisMQ.Host.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 5672 5671 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "RedisMQ.Host.dll"]
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 12

  broker:
    build: .
    depends_on:
      redis:
        condition: service_healthy
    environment:
      RedisMQ__Redis__ConnectionString: "redis:6379"
    ports:
      - "5672:5672"
      - "8080:8080"
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/5672'"]
      interval: 5s
      timeout: 3s
      retries: 12

volumes:
  redis-data:
```

- [ ] **Step 4: Verify the image starts memory-only without Redis**

```powershell
docker build -t redismq:dev .
docker run --rm -d --name redismq-dev -p 15672:8080 -p 15673:5672 redismq:dev
Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:15672/healthz | ConvertTo-Json -Depth 6
docker rm -f redismq-dev
```

Expected: `ready: true` and `engine.kind: "memory"`; no Redis container running.

- [ ] **Step 5: Verify the compose stack uses Redis and reports ready**

```powershell
docker compose up -d --build
Start-Sleep -Seconds 8
Invoke-RestMethod http://localhost:8080/healthz | ConvertTo-Json -Depth 6
docker compose down
```

Expected: `ready: true` and `engine.kind: "redis"`. If local ports 5672/8080 are occupied, temporarily change the compose host ports and note it; the container ports stay the same.

- [ ] **Step 6: Commit**

```powershell
git add Dockerfile .dockerignore docker-compose.yml
git commit -m "build: add container image and compose example"
```

---

### Task 7: CI pack and docker jobs

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 1 metadata and Task 6 Dockerfile.
- Produces: CI artifacts (`artifacts/*.nupkg`, `*.snupkg`) and a `docker build` gate.

- [ ] **Step 1: Add the `pack` and `docker` jobs**

Append after the existing `test` job:

```yaml
  pack:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup .NET SDK
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Build (Release)
        run: dotnet build RedisMQ.sln -c Release

      - name: Pack
        run: dotnet pack RedisMQ.sln -c Release --no-build -o artifacts

      - name: Core has no package references
        run: |
          dotnet list src/RedisMQ.Broker/RedisMQ.Broker.csproj package --include-transitive --format json > core-packages.json
          if jq -e '[.projects[].frameworks[] | (.topLevelPackages // []) + (.transitivePackages // [])] | flatten | length > 0' core-packages.json; then
            echo "RedisMQ.Broker gained a package reference"
            exit 1
          fi

      - name: Upload packages
        uses: actions/upload-artifact@v4
        with:
          name: nuget-packages
          path: |
            artifacts/*.nupkg
            artifacts/*.snupkg

  docker:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Build image
        run: docker build -t redismq:ci .
```

- [ ] **Step 2: Verify the pack and image commands locally**

```powershell
dotnet build RedisMQ.sln -c Release
dotnet pack RedisMQ.sln -c Release --no-build -o artifacts
docker build -t redismq:ci .
```

Expected: three `.nupkg` files (`RedisMQ.Broker`, `RedisMQ.Redis`, `RedisMQ.Hosting`) plus symbols, and a successful image build.

- [ ] **Step 3: Commit**

```powershell
git add .github/workflows/ci.yml
git commit -m "ci: pack packages and build the container"
```

---

### Task 8: Packaging documentation and README updates

**Files:**
- Create: `docs/PACKAGING.md`
- Modify: `README.md`
- Modify: `docs/DEVELOPMENT.md`

**Interfaces:**
- Consumes: the final API and commands from Tasks 1–7.
- Produces: user-facing install/config/run documentation.

- [ ] **Step 1: Write `docs/PACKAGING.md`**

Include, with the exact values from the spec:

- Package table (`RedisMQ.Broker`, `RedisMQ.Redis`, `RedisMQ.Hosting`) and install commands:
  ```bash
  dotnet add package RedisMQ.Hosting --prerelease
  ```
- A minimal `Program.cs` using `AddRedisMQBroker` and the health endpoints.
- The full configuration table: section, key, default, env-var form. Cover `InstanceId`,
  `ServiceVersion`, `Broker.Port`, `Vhosts`, `Users`, `FrameMax`, `Heartbeat`,
  `ConfirmQueueCapacity`, `ConfirmTimeout`, `ReaperInterval`,
  `Tls.Enabled`, `Tls.Port`, `Tls.CertificatePath`, `Tls.CertificateKeyPath`,
  `Tls.CertificatePassword`, `Tls.ClientCertificateRequired`,
  `Tls.ClientCertificateAuthorityPath`, `Tls.MinTlsVersion`, `Tls.HandshakeTimeout`,
  `Redis.ConnectionString`, `ReplayOnStart`, `DrainerCapacity`, `EnforceEvictionGate`.
- Redis notes: empty connection string = memory-only; configured but unreachable fails fast
  at startup unless `ReplayOnStart`/`EnforceEvictionGate` are disabled, in which case readiness
  is false; the eviction-policy gate.
- Lifecycle note: `RedisStreamRouteEngine` is `IAsyncDisposable` only. Hosts must await
  `host.RunAsync()`/`app.RunAsync()` (not `Run()`) so the provider disposes asynchronously.
- TLS notes: mount certificate files read-only, set `RedisMQ__Broker__Tls__*`, no hot reload.
- Container commands:
  ```bash
  docker build -t redismq:dev .
  docker run --rm -p 5672:5672 -p 8080:8080 redismq:dev
  docker compose up --build
  ```
- A security warning that `guest`/`guest` is a local demo only.
- "Not published": packages are built locally/in CI only; no nuget.org push.

- [ ] **Step 2: Add the Docker and package blocks to `README.md`**

- In the Quick Start, add a "Run in Docker" block with the two commands above and a link to
  `docs/PACKAGING.md`.
- Add a "Packages" subsection listing the three package names and the install line.
- Update the Project Layout tree with `src/RedisMQ.Hosting/`, `samples/RedisMQ.Host/`,
  `tests/RedisMQ.Hosting.Tests/`, `Dockerfile`, and `docker-compose.yml`.
- Add `RedisMQ.Hosting.Tests` to the expected-results table (11 tests) and the direct-runner
  commands.
- Correct the stale counts while touching that table: `RedisMQ.Broker.Tests` 357 → 359 and
  `RedisMQ.Redis.Tests` 66 → 67 (these match the current green suites), and the same numbers
  in the direct-runner comments.
- Leave the rest of the README structure intact.

- [ ] **Step 3: Update `docs/DEVELOPMENT.md`**

- Add the hosting suite to the test targets and the direct-runner list.
- Remove the "Packaging" bullet from "Remaining work toward production v1" and replace it
  with a link to `docs/PACKAGING.md` under a "Packaging" heading.

- [ ] **Step 4: Verify links and commands**

```powershell
Select-String -Path README.md,docs\DEVELOPMENT.md,docs\PACKAGING.md -Pattern "docs/PACKAGING.md"
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
```

Expected: every reference resolves to an existing file; hosting suite Total: 11, Failed: 0.

- [ ] **Step 5: Commit**

```powershell
git add docs/PACKAGING.md README.md docs/DEVELOPMENT.md
git commit -m "docs: add packaging and container guide"
```

---

## Final verification

- [ ] `dotnet build RedisMQ.sln --no-incremental` → 0 errors, 86 analyzer warnings.
- [ ] Broker 359/0, Conformance 19/0, Redis 67/0, Hosting 11/0.
- [ ] `dotnet pack RedisMQ.sln -c Release -o artifacts` produces three packages + symbols.
- [ ] `docker build` and both container smoke checks pass.
- [ ] `git status` is clean.
