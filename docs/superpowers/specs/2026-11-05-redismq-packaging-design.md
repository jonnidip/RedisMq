# RedisMQ Packaging Design (v1)

**Status:** proposed
**Date:** 2026-11-05
**Scope:** production-v1 packaging item: NuGet packages, container image, configuration binding.

---

## 1. Intent and why

RedisMQ is usable today by referencing the projects directly. A production v1 needs to be
consumable the way .NET services are consumed: `dotnet add package RedisMQ.Hosting`, set a
few configuration keys, run; or `docker run` a broker image. The goal is a paved road from
"empty project" to "AMQP broker accepting connections", without weakening the project's
existing guarantees.

The core promise that must survive: **`RedisMQ.Broker` has zero `PackageReference`s**, and
`RedisMQ.Redis` is the only project that carries StackExchange.Redis. Packaging adds
projects and metadata around that core; it does not move dependencies into it.

## 2. Chosen approach

Three delivery shapes were considered:

| Approach | What it is | Verdict |
|----------|------------|---------|
| **A — sample host only** | A console sample binds configuration; no new library package. | Too little: every consumer copies wiring, no DI. |
| **B — hosting package + sample host** | New `RedisMQ.Hosting` NuGet binds configuration, registers DI services, and owns server lifecycle; the sample host and container build on it. | **Chosen.** A package a real host can adopt, core stays zero-dependency. |
| C — binding inside core | Put `Microsoft.Extensions.*` into `RedisMQ.Broker`. | Rejected: breaks the zero-dependency rule. |

Approach B keeps the dependency direction one-way:

```
RedisMQ.Broker          (zero package references)
      ▲
      ├── RedisMQ.Redis (StackExchange.Redis)
      │         ▲
      └── RedisMQ.Hosting (Microsoft.Extensions.* + RedisMQ.Redis)
                ▲
        samples/RedisMQ.Host (Web SDK, shared framework only)
```

`RedisMQ.Hosting` referencing `RedisMQ.Redis` is deliberate: one hosting package is simpler
to version than a `RedisMQ.Broker.Hosting` + `RedisMQ.Redis.Hosting` split, and the zero-dep
promise belongs to `RedisMQ.Broker`, not to every optional package. A memory-only deployment
leaves `Redis:ConnectionString` empty and gets an `InMemoryEngine`; it still carries the
StackExchange.Redis assembly transitively. This tradeoff is documented.

## 3. Goals

- Three packable NuGet packages: `RedisMQ.Broker`, `RedisMQ.Redis`, `RedisMQ.Hosting`.
- A configuration schema that binds `BrokerServerOptions` plus Redis/engine settings.
- DI registration and hosted-service lifecycle: start on host start, stop and dispose on host stop.
- A runnable sample host with host-owned liveness/readiness HTTP endpoints.
- A multi-stage Linux container image, non-root, with a Redis-backed compose example.
- CI verifies `dotnet pack` and `docker build`; no publishing to nuget.org in this scope.
- Existing tests and the 86-warning analyzer baseline stay intact; new test projects add no
  new `xUnit1051`/`xUnit2031` warnings.

## 4. Non-goals

- Publishing packages to nuget.org, package signing, SBOM generation, multi-arch images.
- Kubernetes manifests, Helm charts, or a management UI.
- Certificate hot-reload, Redis reconnect policy, runtime user/vhost management (later items).
- Performance work (pipelining, mirror batching, benchmarks) and extra client compatibility.
- `/metrics`, Prometheus/OTLP exporters, or trace propagation. The host owns those; the sample
  shows only `GetHealth()`.

## 5. Anti-goals (would make this a failure even if it ships)

- Any `PackageReference` appearing in `src/RedisMQ.Broker/RedisMQ.Broker.csproj`.
- A container that runs as root, hard-codes a password with no warning, or exposes TLS on by
  default with an obviously invalid certificate.
- `RedisMQ.Hosting` refusing to work with `IHost`/Generic Host because it assumes ASP.NET Core.
- Packaging metadata duplicated per project and drifting; version must have one source.

## 6. Constraints

- `net10.0`; SDK on the CI image is `10.0.x` and locally `10.0.401`.
- MIT licence is assumed; a `LICENSE` file does not exist yet and must be added.
- The git remote exists: `origin https://github.com/jonnidip/RedisMq.git`, so package metadata
  can carry a real repository URL.
- The repo's README is large and shared; packages will embed the root `README.md` until
  package-specific readmes exist.
- Windows development machine (PowerShell), Linux container. All build/test commands must
  work on both where relevant.
- `dotnet test` is broken locally (SDK MTP bug); the direct xunit runner is the local path.
  CI keeps using `dotnet test`.

## 7. Package metadata (single source)

New root `Directory.Build.props` holds packable metadata for every project:

| Property | Value |
|----------|-------|
| `Version` | `0.1.0-preview.1` |
| `Authors` | `RedisMQ contributors` |
| `PackageLicenseExpression` | `MIT` |
| `PackageTags` | `amqp;rabbitmq;redis;broker;streams;messaging` |
| `PackageReadmeFile` | `README.md` |
| `IncludeSymbols` / `SymbolPackageFormat` | `true` / `snupkg` |
| `RepositoryType` | `git` |
| `RepositoryUrl` | `https://github.com/jonnidip/RedisMq` |
| `PackageProjectUrl` | `https://github.com/jonnidip/RedisMq` |
| `Deterministic` | `true` |

- Each packable project packs the root README:
  `<None Include="$(MSBuildThisFileDirectory)README.md" Pack="true" PackagePath="\" />`
- `RepositoryUrl`/`PackageProjectUrl` point at the real `origin` remote.
- `IsPackable=false` is set on the three test projects and the sample host.
- Version is `0.1.0-preview.1` because the production-v1 items remain open. **My call**, easy
  to change to `1.0.0-preview.1` if you prefer to signal API stability.
- Root `LICENSE` (MIT, "RedisMQ contributors", 2026). **My call**; say so if you want another
  licence before any package is published.

## 8. Configuration schema

Section name defaults to `RedisMQ`. Arrays replace, they do not merge.

```json
{
  "RedisMQ": {
    "InstanceId": "broker-1",
    "ServiceVersion": "0.1.0-preview.1",
    "Broker": {
      "Port": 5672,
      "Vhosts": ["/"],
      "FrameMax": 131072,
      "Heartbeat": 60,
      "ConfirmQueueCapacity": 4096,
      "ConfirmTimeout": "00:00:05",
      "ReaperInterval": "00:00:05",
      "Users": [
        {
          "Name": "guest",
          "Password": "guest",
          "Permissions": [
            { "Vhost": "/", "Configure": ".*", "Write": ".*", "Read": ".*" }
          ]
        }
      ],
      "Tls": {
        "Enabled": false,
        "Port": 5671,
        "CertificatePath": "/certs/server.pfx",
        "CertificatePassword": null,
        "ClientCertificateRequired": false
      }
    },
    "Redis": {
      "ConnectionString": "localhost:6379",
      "ReplayOnStart": true,
      "DrainerCapacity": 1024,
      "EnforceEvictionGate": true
    }
  }
}
```

- `Broker` binds directly to the existing `BrokerServerOptions`; `Tls` to `BrokerTlsOptions`.
- `Redis` binds to a new `RedisEngineOptions` record/class in `RedisMQ.Hosting`.
- Environment variables use the standard double-underscore form, e.g.
  `RedisMQ__Broker__Port=5672`, `RedisMQ__Redis__ConnectionString=redis:6379`.
- Empty/absent `Redis.ConnectionString` selects `InMemoryEngine` and starts no drainer.
- `InstanceId`/`ServiceVersion` flow into `BrokerDiagnostics`, so every metric keeps a stable
  `broker.instance`.
- Binding of `BrokerUser`/`BrokerPermission` positional records is verified by a test first.
  If the binder cannot construct them reliably, the hosting package maps a small DTO onto the
  records; the public configuration shape does not change. **My call**, chosen to keep the
  JSON above stable either way.

## 9. `RedisMQ.Hosting` API

```csharp
namespace RedisMQ.Hosting;

public sealed class RedisMqOptions
{
    public string? InstanceId { get; set; }
    public string? ServiceVersion { get; set; }
    public BrokerServerOptions Broker { get; set; } = new();
    public RedisEngineOptions Redis { get; set; } = new();
}

public sealed class RedisEngineOptions
{
    public string? ConnectionString { get; set; }
    public bool ReplayOnStart { get; set; } = true;
    public int DrainerCapacity { get; set; } = 1024;
    public bool EnforceEvictionGate { get; set; } = true;
}

public static class RedisMqServiceCollectionExtensions
{
    public static IServiceCollection AddRedisMQBroker(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "RedisMQ",
        Action<RedisMqOptions>? configure = null);
}

public sealed class BrokerServerHostedService : IHostedService, IDisposable, IAsyncDisposable;
public sealed class LoggerBrokerLogSink : IBrokerLogSink;
```

Registered services and lifetime:

| Service | Lifetime | Notes |
|---------|----------|-------|
| `RedisMqOptions` | singleton | bound once, validated at start |
| `BrokerDiagnostics` | singleton | one instance shared by engine and server |
| `BrokerServerOptions` | singleton | the bound `Broker` object |
| `IRouteEngine` | singleton | `InMemoryEngine` or `RedisStreamRouteEngine` |
| `ConnectionMultiplexer` | singleton | only when a Redis connection string is set |
| `AmqpBrokerServer` | singleton | injectable into health endpoints |
| `BrokerServerHostedService` | singleton (as `IHostedService`) | the only `IHostedService`; the concrete type is not separately registered |
| `IReadOnlyList<IListenerTransport>` | singleton | built by `BrokerListeners.Create`; disposed by the hosted service |

- `AddRedisMQBroker` uses `TryAdd` semantics so calling it twice is harmless.
- The log sink adapter maps `BrokerLogLevel` to `LogLevel`, uses `(int)BrokerLogEvent` as the
  log event id, and forwards `Data` as structured state. The adapter is installed only when an
  `ILoggerFactory` is available in the container; otherwise the null sink is used.
- Disposal follows DI ownership: the container owns the singleton engine, multiplexer,
  diagnostics and options and disposes them when the host/provider is disposed. The listeners
  are created as a singleton during host startup; the hosted service disposes them in
  `StopAsync`; `AmqpBrokerServer` is stopped with `StopAsync()` (it is not disposable).
  Host shutdown order is therefore: stop hosted service (server + listeners) → container
  disposes engine/multiplexer.
  `RedisStreamRouteEngine` is `IAsyncDisposable` only, so the provider must be disposed
  asynchronously: hosts use `await host.RunAsync()` (or `await app.RunAsync()`), never a
  synchronous `Dispose()` of a provider that holds a Redis engine. This is documented in the
  packaging guide, and the hosting tests dispose such hosts asynchronously.
- Startup validation: `BrokerServerOptions.Validate()` plus a hosting check that at least one
  listener is enabled (`Broker.Port >= 0` or `Broker.Tls.Enabled`). Port `0` stays legal
  (ephemeral, used by tests). An empty `Broker.Users` list is legal but logs a warning, because
  default-deny means the broker would accept no clients; `AddRedisMQBroker` never invents an
  account. **My call**, chosen to keep the sample/tests flexible while surfacing a useless
  configuration.
- Redis is connected synchronously during singleton construction with `AbortOnConnectFail=false`
  so a transient outage is visible through health rather than a hang. Startup replay still
  fails fast if Redis is configured but unreachable at start; that is the existing engine
  contract, documented in the packaging guide. **My call**, pending a future reconnect policy.

## 10. Sample host

New `samples/RedisMQ.Host` using `Microsoft.NET.Sdk.Web` (shared framework only, no packages):

- `Program.cs`: `WebApplication.CreateBuilder`, `AddRedisMQBroker`, then:
  - `GET /livez` → 200 when `BrokerHealth.Live`, else 503.
  - `GET /healthz` → 200 when `BrokerHealth.Ready`, else 503; body is the serialized snapshot
    (counters, queues, engine, detail).
  - `GET /` → one line naming the broker and the AMQP port.
- `appsettings.json` ships the local demo: `guest`/`guest` full access on `/`, port 5672,
  **`Redis.ConnectionString` empty** (memory-only, so a bare container starts), TLS disabled.
  A comment in the README warns that guest/guest is a local demo only. Redis is opted into by
  configuration/environment, never assumed by the image.
- No `/metrics` endpoint; that remains the host's responsibility and is already documented.

## 11. Container and compose

Root `Dockerfile`, multi-stage:

1. `mcr.microsoft.com/dotnet/sdk:10.0` restores and publishes the sample in Release.
2. `mcr.microsoft.com/dotnet/aspnet:10.0` runs it as the image's non-root `app` user.
3. `EXPOSE 5672 5671 8080`; `ASPNETCORE_URLS=http://+:8080`; entrypoint `dotnet RedisMQ.Host.dll`.

Root `.dockerignore` excludes `bin`, `obj`, `.git`, `.idea`, `.superpowers`, `TestResults`,
`artifacts`, and docs not needed by publish.

`docker-compose.yml` provides:

| Service | Image | Purpose |
|---------|-------|---------|
| `redis` | `redis:7-alpine` | AOF on, healthcheck via `redis-cli ping` |
| `broker` | built from the Dockerfile | depends on healthy redis; sets `RedisMQ__Redis__ConnectionString=redis:6379`; ports 5672 + 8080; TLS port omitted by default |

A bare `docker run -p 5672:5672 -p 8080:8080 redismq` starts memory-only and succeeds; adding
`-e RedisMQ__Redis__ConnectionString=host:6379` opts into durability. Compose shows the
Redis-backed shape. This keeps the headline container demo working with one command.

The broker healthcheck is a TCP check against 5672 using bash's `/dev/tcp` (the runtime image
has no curl). Compose is a local/demo artifact, explicitly not a production deployment recipe.

## 12. CI and build

- Add a `pack` job: `dotnet pack RedisMQ.sln -c Release -o artifacts`, fail if
  `src/RedisMQ.Broker` reports any package reference (`dotnet list ... package`), upload the
  `.nupkg`/`.snupkg` artifacts.
- Add a `docker` job: `docker build -t redismq:ci .` on `ubuntu-latest`. No push.
- Keep the existing test job unchanged.
- Local commands: `dotnet pack RedisMQ.sln -c Release -o artifacts`,
  `docker build -t redismq:dev .`, `docker compose up --build`.

## 13. Tests

New `tests/RedisMQ.Hosting.Tests` (xunit v3, same style as the other suites):

| Test | What it proves |
|------|----------------|
| `Configuration_binds_broker_users_and_tls` | nested binding, positional records, `TimeSpan`, arrays |
| `Configuration_binds_redis_engine_options` | Redis section and defaults |
| `AddRedisMQBroker_registers_and_starts_a_memory_broker` | port 0, health live/ready, injectable `AmqpBrokerServer` |
| `Hosted_service_stops_the_broker_on_host_stop` | `Live=false` after stop; listeners released; engine/multiplexer still disposable by the provider |
| `Registering_twice_does_not_duplicate_services` | `TryAdd` semantics |
| `Log_sink_forwards_broker_events_to_ILogger` | event id, level mapping, structured data |
| `Invalid_broker_user_fails_startup` | `BrokerConfigurationException` surfaces |
| `No_enabled_listener_fails_startup` | `Port = -1` with TLS disabled is rejected |

- Use `TestContext.Current.CancellationToken` in async calls and the predicate `Assert.Single`
  overloads, keeping the warning baseline at 86.
- Docker and `dotnet pack` are verified by CI and manually once; they are not unit tests.

## 14. Documentation

- New `docs/PACKAGING.md`: pack commands, install commands, full configuration table with
  defaults and env-var form, Redis/TLS setup, Docker/compose usage, secret-handling notes,
  "what is not published".
- `README.md`: a "Run with Docker" quick-start block and a "Packages" section linking the guide.
- `docs/DEVELOPMENT.md`: remove packaging from "Remaining work toward production v1"; point to
  the guide.
- Version and metadata changes noted in the release/CI section.

## 15. What is left to the builder

- Exact `Microsoft.Extensions.*` package versions (pin to the 10.0.x line matching the SDK).
- File layout inside `RedisMQ.Hosting` and test helper naming.
- Whether the sample uses minimal APIs or a tiny controller (minimal APIs expected).
- Compose volume/healthcheck details and image tags.
- Any package-specific README split later; this design deliberately ships the root README once.
