# Packaging and Hosting

How to consume RedisMQ as NuGet packages, configure a host from `IConfiguration`, and
run the container image. The runnable reference is
[`samples/RedisMQ.Host`](../samples/RedisMQ.Host).

## Packages

| Package | Contents | Dependencies |
|---------|----------|--------------|
| `RedisMQ.Broker` | Core broker: codec, protocol, transport, server, in-memory engine, diagnostics. | None. |
| `RedisMQ.Redis` | `RedisStreamRouteEngine`: the Redis Streams write-ahead log and startup replay. | `RedisMQ.Broker`, StackExchange.Redis. |
| `RedisMQ.Hosting` | `IConfiguration` binding, DI registration and the hosted-service lifecycle. | `RedisMQ.Broker`, `RedisMQ.Redis`, Microsoft.Extensions.*. |

```bash
dotnet add package RedisMQ.Hosting --prerelease
```

Add `RedisMQ.Broker` alone for an embedded in-memory broker, or `RedisMQ.Redis` when the
host constructs the engine directly. `RedisMQ.Hosting` is the batteries-included entry
point for a generic host or ASP.NET Core app.

## Minimal host

```csharp
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Server;
using RedisMQ.Hosting;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddRedisMQBroker(builder.Configuration);

WebApplication app = builder.Build();

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

// RunAsync, not Run: RedisStreamRouteEngine is IAsyncDisposable-only.
await app.RunAsync();
```

`AddRedisMQBroker(services, configuration, sectionName = "RedisMQ", configure = null)`
binds the section, applies the optional delegate, and registers:

- `RedisMqOptions` and `BrokerServerOptions`
- `BrokerDiagnostics`, forwarded to `ILoggerFactory` when the host provides one
- `IRouteEngine` — `InMemoryEngine` when `Redis:ConnectionString` is empty, otherwise
  `RedisStreamRouteEngine`
- `IReadOnlyList<IListenerTransport>` from `BrokerListeners.Create`
- `AmqpBrokerServer`
- exactly one `IHostedService`, which validates configuration, starts the server, and
  owns stop/dispose

Calling it twice is safe: every registration is `TryAdd`/`TryAddEnumerable`, so services
are not duplicated. The host owns the HTTP surface; RedisMQ ships no `/healthz`,
`/metrics` or exporter. Map `GetHealth()` yourself, as above.

### Lifecycle

`RedisStreamRouteEngine` implements `IAsyncDisposable` only. A host must dispose its
service provider asynchronously, which means `await host.RunAsync()` or
`await app.RunAsync()` rather than the synchronous `Run()`. Disposing the provider
synchronously throws because the engine has no synchronous dispose path.

The hosted service is idempotent: a second `StopAsync` (including the stop performed by
`DisposeAsync`) is harmless, and it also stops cleanly when startup never ran.

## Configuration

Settings live under the `RedisMQ` section. In JSON use `:`, in environment variables use
`__` (double underscore). Arrays use numeric indexes (`Users:0:Name`,
`RedisMQ__Broker__Users__0__Name`).

```json
{
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

| Key | Default | Environment form |
|-----|---------|------------------|
| `RedisMQ:InstanceId` | random 8-character id | `RedisMQ__InstanceId` |
| `RedisMQ:ServiceVersion` | assembly informational version | `RedisMQ__ServiceVersion` |
| `Broker:Port` | `0` (ephemeral); `-1` disables plaintext | `RedisMQ__Broker__Port` |
| `Broker:Vhosts` | `["/"]` | `RedisMQ__Broker__Vhosts__0`, `...__1`, ... |
| `Broker:Users` | `[]` (default-deny; startup logs a warning) | `RedisMQ__Broker__Users__0__Name`, `...__Password`, `...__PasswordHash`, `...__Permissions__0__Vhost`, ... |
| `Broker:FrameMax` | `131072` | `RedisMQ__Broker__FrameMax` |
| `Broker:Heartbeat` | `60` seconds; `0` offers no heartbeat | `RedisMQ__Broker__Heartbeat` |
| `Broker:ConfirmQueueCapacity` | `4096` | `RedisMQ__Broker__ConfirmQueueCapacity` |
| `Broker:ConfirmTimeout` | `00:00:05` | `RedisMQ__Broker__ConfirmTimeout` |
| `Broker:ReaperInterval` | `00:00:05`; `<= 0` disables the reaper | `RedisMQ__Broker__ReaperInterval` |
| `Broker:Tls:Enabled` | `false` | `RedisMQ__Broker__Tls__Enabled` |
| `Broker:Tls:Port` | `5671` | `RedisMQ__Broker__Tls__Port` |
| `Broker:Tls:CertificatePath` | none (PFX or PEM certificate) | `RedisMQ__Broker__Tls__CertificatePath` |
| `Broker:Tls:CertificateKeyPath` | none (PEM private key) | `RedisMQ__Broker__Tls__CertificateKeyPath` |
| `Broker:Tls:CertificatePassword` | none | `RedisMQ__Broker__Tls__CertificatePassword` |
| `Broker:Tls:ClientCertificateRequired` | `false` | `RedisMQ__Broker__Tls__ClientCertificateRequired` |
| `Broker:Tls:ClientCertificateAuthorityPath` | none | `RedisMQ__Broker__Tls__ClientCertificateAuthorityPath` |
| `Broker:Tls:MinTlsVersion` | `Tls12` | `RedisMQ__Broker__Tls__MinTlsVersion` |
| `Broker:Tls:HandshakeTimeout` | `00:00:10` | `RedisMQ__Broker__Tls__HandshakeTimeout` |
| `Redis:ConnectionString` | empty = in-memory engine | `RedisMQ__Redis__ConnectionString` |
| `Redis:ReplayOnStart` | `true` | `RedisMQ__Redis__ReplayOnStart` |
| `Redis:DrainerCapacity` | `1024` | `RedisMQ__Redis__DrainerCapacity` |
| `Redis:EnforceEvictionGate` | `true` | `RedisMQ__Redis__EnforceEvictionGate` |

`TimeSpan` values use the invariant `hh:mm:ss` form. `MinTlsVersion` accepts the
`SslProtocols` names (`Tls12`, `Tls13`, ...). A `Broker:Users` entry sets exactly one of
`Password` (development plaintext) or `PasswordHash` (PBKDF2-SHA256, produced by
`BrokerPassword.Hash`). Permissions are default-deny: an account with no permissions
cannot open any vhost, and a vhost absent from an account's permission list is refused.

Startup validation runs before the server starts:

- at least one listener must be enabled (`Broker:Port >= 0` or `Broker:Tls:Enabled=true`)
  or startup fails with `BrokerConfigurationException`
- duplicate usernames, malformed hashes, both/neither credential forms, and invalid
  permission regexes fail startup
- an empty user list is legal but logs a warning: default-deny means every client is
  refused

## Redis

An empty `Redis:ConnectionString` selects `InMemoryEngine`: no Redis connection is made,
no drainer starts, and confirms promise nothing durable. The bare container therefore
starts without Redis.

A configured connection string selects `RedisStreamRouteEngine` and registers an
`IConnectionMultiplexer` with `AbortOnConnectFail=false`:

- With the defaults (`ReplayOnStart=true`, `EnforceEvictionGate=true`), startup fails
  fast when Redis is unreachable or when `CONFIG GET maxmemory-policy` reports an
  eviction policy other than `noeviction`. Use `noeviction` or disable the gate
  deliberately.
- With `ReplayOnStart=false` and `EnforceEvictionGate=false`, an unreachable Redis does
  not abort startup; the broker serves memory-only and `/healthz` reports
  `ready: false` (the detail names the sanitized error).
- The write-ahead log is mirrored by one background drainer, so the protocol thread
  never blocks on Redis. A full drainer refuses the publish with `confirm.nack` rather
  than dropping it.

Use a fresh Redis store: there is no migration path from earlier milestone formats.

## TLS

Mount certificate material read-only and point the options at it:

```yaml
volumes:
  - ./certs:/certs:ro
environment:
  RedisMQ__Broker__Tls__Enabled: "true"
  RedisMQ__Broker__Tls__Port: "5671"
  RedisMQ__Broker__Tls__CertificatePath: "/certs/server.pfx"
  RedisMQ__Broker__Tls__CertificatePassword: "${TLS_PASSWORD}"
  RedisMQ__Broker__Tls__ClientCertificateRequired: "true"
  RedisMQ__Broker__Tls__ClientCertificateAuthorityPath: "/certs/client-ca.pem"
```

TLS runs beside plaintext on its own port. There is no hot reload: rotating a
certificate requires a restart.

## Container

```bash
docker build -t redismq:dev .
docker run --rm -p 5672:5672 -p 8080:8080 redismq:dev
docker compose up --build
```

The image is a multi-stage build (`sdk:10.0` → `aspnet:10.0`) running as the non-root
`APP_UID`. It exposes AMQP 5672 (plaintext), 5671 (TLS, when enabled) and HTTP 8080.
`docker run` uses the sample configuration: in-memory engine, `guest`/`guest` on `/`.

`docker-compose.yml` adds Redis 7 with AOF and a named `redis-data` volume, waits for its
health check, and points the broker at `redis:6379`. That stack reports
`engine.kind: "redis"` and durable confirms.

To override configuration in either form, use the environment form:

```bash
docker run --rm -e RedisMQ__Broker__Port=5672 -p 5672:5672 -p 8080:8080 redismq:dev
```

> **Security:** the bundled `guest`/`guest` account is a local demo only. Replace it with
> a hashed password and explicit per-vhost permissions before exposing a broker.

## Not published

The packages are built locally and by CI; nothing is pushed to nuget.org. `dotnet pack`
produces `RedisMQ.Broker`, `RedisMQ.Redis` and `RedisMQ.Hosting` `.nupkg` files plus
`.snupkg` symbol packages under `artifacts/`.
