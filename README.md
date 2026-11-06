# RedisMQ

RedisMQ is an AMQP 0-9-1 (RabbitMQ dialect) wire server whose message store is a
**Redis Streams write-ahead log** behind a synchronous, in-memory system of record.
Real RabbitMQ client libraries connect over TCP (optionally TLS), authenticate with
PLAIN, declare exchanges and queues, publish, consume with prefetch credits, settle
with ack/nack/reject, and receive **honest publisher confirms** that only acknowledge
durability once every fan-out `XADD` has been accepted by Redis.

The core library (`RedisMQ.Broker`) has **zero external dependencies**; Redis and
StackExchange.Redis live only in the optional `RedisMQ.Redis` project. With no Redis
connection the broker still runs, memory-only, but makes no durability promise.

This is the **M4 part-1 / v1-completeness** milestone: durable message identity,
honest confirms, ack/requeue mirroring, dead-lettering, TTL and queue expiry,
per-vhost permissions with PBKDF2 password hashes, and TLS with optional mutual TLS.
What remains for a production v1 is listed in [What's next](#whats-next).

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Durability Contract](#durability-contract)
3. [Observability](#observability)
4. [What RedisMQ Is (and Isn't)](#what-redismq-is-and-isnt)
5. [Architecture](#architecture)
6. [Project Layout](#project-layout)
7. [Running the Tests](#running-the-tests)
8. [Conformance Suite](#conformance-suite)
9. [API Reference](#api-reference)
10. [Protocol Implementation Notes](#protocol-implementation-notes)
11. [Accepted Limitations](#accepted-limitations)
12. [What's Next](#whats-next)
13. [CI](#ci)

---

## Quick Start

```bash
# Build everything
dotnet build RedisMQ.sln

# Run the full test suite (unit + conformance + Redis)
dotnet test RedisMQ.sln
```

> **Note:** On this development machine the .NET SDK's MTP test runner (`dotnet test`)
> is unusable due to an SDK bug (exit 5, "zero tests ran"). Use the direct runner instead:
>
> ```bash
> dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll
> dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
> dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll
> dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll
> ```

Expected results:

| Suite | Total | Failed | Warnings |
|-------|-------|--------|----------|
| `RedisMQ.Broker.Tests` | 359 | 0 | 0 |
| `RedisMQ.Conformance` | 19 | 0 | 0 |
| `RedisMQ.Redis.Tests` | 67 | 0 | 0 |
| `RedisMQ.Hosting.Tests` | 11 | 0 | 0 |
| `dotnet build RedisMQ.sln` | — | — | 0 |

### Run in Docker

```bash
# Build the image and run the memory-only broker
docker build -t redismq:dev .
docker run --rm -p 5672:5672 -p 8080:8080 redismq:dev

# Or run the broker plus Redis with durable confirms
docker compose up --build
```

See [docs/PACKAGING.md](docs/PACKAGING.md) for configuration, TLS and the container
details.

### Packages

RedisMQ ships as three packages:

- `RedisMQ.Broker` — the core broker with zero external dependencies
- `RedisMQ.Redis` — the Redis Streams write-ahead log
- `RedisMQ.Hosting` — `IConfiguration` binding, DI and hosted lifecycle

```bash
dotnet add package RedisMQ.Hosting --prerelease
```

### Minimal in-memory host

```csharp
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;
using RedisMQ.Broker.Transport;

var options = new BrokerServerOptions
{
    Port = 5672,                        // 0 = OS-assigned; -1 = no plaintext listener
    Vhosts = ["/"],
    // Permissions are default-deny. FullAccess is the explicit test/sample helper.
    Users = [BrokerUser.FullAccess("guest", "guest", "/")],
};

IReadOnlyList<IListenerTransport> listeners = BrokerListeners.Create(options);
var engine = new InMemoryEngine(options.Vhosts);
var server = new AmqpBrokerServer(options, engine, listeners);
await server.StartAsync();

// ... clients connect to localhost:5672 ...

await server.StopAsync();
foreach (var listener in listeners)
{
    if (listener is IAsyncDisposable disposable)
    {
        await disposable.DisposeAsync();
    }
}
```

Clients then connect exactly as they would to RabbitMQ:

```csharp
var factory = new ConnectionFactory { UserName = "guest", Password = "guest", VirtualHost = "/" };
await using var connection = await factory.CreateConnectionAsync(
    [new AmqpTcpEndpoint("localhost", 5672)]);
```

### Redis-backed host

`RedisStreamRouteEngine` wraps an `InMemoryEngine` and mirrors every mutation into
Redis Streams on a single background drainer, so the protocol thread never blocks on
Redis. Reads and credit gating stay in memory.

```csharp
using StackExchange.Redis;
using RedisMQ.Redis;

ConnectionMultiplexer redis = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
var engine = new RedisStreamRouteEngine(redis.GetDatabase(), options.Vhosts);

// Startup fails fast when Redis's maxmemory-policy could evict durable data
// (anything except noeviction). Pass enforceEvictionGate: false to opt out.
var server = new AmqpBrokerServer(options, engine, BrokerListeners.Create(options));
```

**Use a fresh Redis store.** There is no M1–M3 migration path: the current stream
and topology formats are the v1 format. See
[Resetting the Redis store](#resetting-the-redis-store).

### TLS host

TLS runs on its own listener beside plaintext, so a deployment can serve 5672 + 5671.
Certificate material is either a PFX or a PEM certificate plus a PEM key.

```csharp
options.Tls = new BrokerTlsOptions
{
    Enabled = true,
    Port = 5671,
    CertificatePath = "/etc/redismq/server.pfx",
    CertificatePassword = Environment.GetEnvironmentVariable("REDISMQ_TLS_PASSWORD"),
    MinTlsVersion = System.Security.Authentication.SslProtocols.Tls12,
    HandshakeTimeout = TimeSpan.FromSeconds(10),
    // Mutual TLS:
    // ClientCertificateRequired = true,
    // ClientCertificateAuthorityPath = "/etc/redismq/client-ca.pem",
};
```

Clients must give the endpoint its own `SslOption` — `ConnectionFactory.Ssl` alone
does not upgrade explicit endpoints:

```csharp
var ssl = new SslOption { Enabled = true, ServerName = "broker.example" };
await using var connection = await factory.CreateConnectionAsync(
    [new AmqpTcpEndpoint("broker.example", 5671, ssl)]);
```

---

## Durability Contract

RedisMQ is deliberately explicit about what a confirm means.

| Engine | `confirm.ack` means |
|--------|---------------------|
| `RedisStreamRouteEngine` (Redis reachable) | Every queue target of the publish accepted its `XADD`. For a fan-out to N queues, **all N** writes must succeed. |
| `RedisStreamRouteEngine` with a full drainer queue | The publish is refused **before it touches memory**; the confirm is `nack`. |
| `InMemoryEngine`, or a Redis engine constructed with a null `IDatabase` | Nothing durable happened. Confirms are immediate acks because the engine makes no durability promise. |
| Any engine, receipt slower than `ConfirmTimeout` | `confirm.nack`; the timed-out `XADD` may still land later. |

- Confirms are ordered per publish sequence and sent on the connection's single
  confirm loop. A mandatory-unroutable publish emits `basic.return` **before** its
  confirm.
- Confirmation mode is off by default. `confirm.select` enables it per channel;
  `confirm.select-ok` is the reply.
- Acknowledgments, rejects and requeues are mirrored to Redis. The mirror applies
  **enqueue/mutate first, remove second**, so a crash between the two can duplicate
  a message on replay but never lose a live one.
- Replay collapses duplicate envelope ids (crash-window duplicates): the copy with
  the higher delivery count wins, and the stale stream entry is `XDEL`ed.

What is **not** promised: exactly-once delivery. RedisMQ is at-least-once, and a
message replayed after a crash can arrive with `Redelivered=false` (see
[Accepted Limitations](#accepted-limitations)).

---

## Observability

The library publishes BCL diagnostics and leaves the HTTP surface and exporters to the
host. `BrokerDiagnostics` owns one `Meter` and one `ActivitySource`, both named
`RedisMQ.Broker`. Share one instance between the engine and the server so every
measurement carries the same `broker.instance` tag:

```csharp
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Memory;
using RedisMQ.Broker.Server;

var diagnostics = new BrokerDiagnostics();
var engine = new InMemoryEngine(["/"], diagnostics);
var server = new AmqpBrokerServer(options, engine, diagnostics: diagnostics);
await server.StartAsync();

BrokerHealth health = server.GetHealth();   // live, ready, detail, queues, engine
```

`GetHealth()` is synchronous and cheap enough for a scrape interval. It reports
connections/channels, per-queue ready/consumer/unacked counts, and (for Redis) mirror
depth, failures, drops and the last sanitized error. A Redis disconnect makes the
broker **not ready** because new publishes cannot honour the durability promise.

The host owns `/metrics`, OTLP and log-shipping endpoints: subscribe a `MeterListener`
or OpenTelemetry exporter to `RedisMQ.Broker`, attach an `IBrokerLogSink`, and map
`GetHealth()` onto liveness/readiness probes. Prometheus requires dot-to-underscore
name conversion and careful label-cardinality limits, so RedisMQ ships no exporter and
no HTTP endpoint. Metric names use the `redismq.` prefix; never add a tag whose value
is unbounded (message ids, routing keys or error text belong in logs, not metrics).

See the [API reference](docs/API.md#diagnostics-and-observability) and the
[architecture notes](docs/ARCHITECTURE.md#diagnostics-and-observability) for the full
metric catalog, event ids and tracing rules.

---

## What RedisMQ Is (and Isn't)

### Is

- An **AMQP 0-9-1 wire server** that real RabbitMQ client libraries can connect to.
- A **connection + channel state machine** following RabbitMQ's
  `rabbit_channel`/`rabbit_reader` conventions, including per-channel flow pause.
- **Four exchange types**: `direct`, `fanout`, `topic` (`*` and `#` wildcards) and
  `headers` (`x-match` all/any), plus the default exchange.
- **Publisher confirms** with a bounded, ordered pipeline and honest nack on
  refusal, fault or timeout.
- **Redis Streams write-ahead log** with startup replay and topology persistence.
- **Dead-lettering** via `x-dead-letter-exchange` / `x-dead-letter-routing-key`,
  `x-death` headers, and `x-delivery-limit` overflow.
- **TTL**: per-message `expiration` and queue idle expiry (`x-expires`) driven by a
  shared reaper; `x-max-length` drop-oldest.
- **Per-vhost permissions** (configure/write/read regexes) that are **default-deny**,
  with plaintext or PBKDF2-SHA256 credentials.
- **TLS** (`SslStream`) with PFX/PEM certificates and optional mutual TLS.
- Zero external runtime dependencies in the core library (`net10.0`).

### Isn't

- **Not a RabbitMQ replacement for clusters.** No quorum queues, no federation, no
  shovel, no management UI.
- **No exactly-once delivery** and no transactional publishes (`tx.*`).
- **No `basic.recover` with `requeue=false`** (hard `not_implemented`), no
  `channel.flow` hard-close behavior (flow pauses a channel in this version).
- **No vhost CRUD over the wire** and no runtime permission management: users,
  vhosts and permissions are process configuration.
- **No message persistence for `InMemoryEngine`** — durability requires
  `RedisStreamRouteEngine` and a Redis reachable at publish time.
- **No certificate hot-reload**: rotating a TLS certificate requires a restart.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  RabbitMQ.Client 7.x (real client)  ──TCP/TLS──►  BrokerListeners        │
│  (or in-proc TransportPair in unit tests)          (plain + TLS)         │
└──────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  AmqpBrokerServer: one accept loop per listener + one shared reaper      │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  WireConnection (per connection)                                   │  │
│  │  • connection FSM + PLAIN auth + vhost-open permission check       │  │
│  │  • HeartbeatWatchdog, connection-wide write gate                   │  │
│  │  • single ordered confirm loop (bounded queue = pump backpressure) │  │
│  │                                                                    │  │
│  │  ChannelContext (per channel)                                      │  │
│  │  • channel FSM, flow pause, content assembler                      │  │
│  │  • prefetch credits, unacked ledger, settlements                   │  │
│  │  • configure/write/read permission checks                          │  │
│  │  • serialized writer chain                                         │  │
│  └──────────────────────────────┬─────────────────────────────────────┘  │
│                                 │ IRouteEngine (synchronous)              │
│                    ┌────────────┴─────────────┐                          │
│                    ▼                          ▼                          │
│          InMemoryEngine               RedisStreamRouteEngine             │
│          (system of record)           (wraps InMemoryEngine)             │
│          • topology + routing         • one background drainer           │
│          • ready buffers + pumps      • XADD / XDEL / topology log       │
│          • DLX / TTL / LastUsed       • startup replay + dedupe          │
│          • EnqueueReceipt             • returns honest receipts          │
│          • EngineDelta                • mirrors enqueue-first/remove-2nd │
└──────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                               Redis Streams (optional)
                        {mq:top:<vhost>}  {mq:q:<vhost>:<queue>}
```

### Layers

| Layer | Namespace | Responsibility |
|-------|-----------|----------------|
| **Codec** | `RedisMQ.Broker.Codec` | BE primitives, field tables, frames, content headers, method codecs |
| **Protocol** | `RedisMQ.Broker.Protocol` | Method registry, content properties, wire types, error codes |
| **Transport** | `RedisMQ.Broker.Transport` | `TransportSide`/`TransportPair`, `SocketListener`, `TlsSocketListener`, `BrokerListeners`, `BrokerCertificate` |
| **Server** | `RedisMQ.Broker.Server` | `AmqpBrokerServer`, `WireConnection`, `ChannelContext`, `PendingConfirm`, heartbeat, options, permissions, TLS options |
| **Memory** | `RedisMQ.Broker.Memory` | `InMemoryEngine`, queues/messages, `IRouteEngine`, receipts and deltas |
| **Redis** | `RedisMQ.Redis` | `RedisStreamRouteEngine`, `RedisKeys`, `WriteCommand`, stream codec (StackExchange.Redis) |

---

## Project Layout

```
RedisMQ.sln
├── src/
│   ├── RedisMQ.Broker/                     # core engine, zero external deps
│   │   ├── Codec/                          # frames, field tables, properties, methods
│   │   ├── Memory/
│   │   │   ├── InMemoryEngine.cs           # topology, routing, DLX/TTL, reaper, receipts
│   │   │   ├── InMemoryMessage.cs          # stored message + Expiration + DeliveryCount
│   │   │   ├── InMemoryQueue.cs            # ready buffer + pump + round-robin
│   │   │   ├── IQueueConsumer.cs
│   │   │   └── IRouteEngine.cs             # seam + EnqueueReceipt + EngineDelta
│   │   ├── Protocol/                       # method registry + AMQP constants
│   │   ├── Server/
│   │   │   ├── AmqpBrokerServer.cs         # multi-listener lifecycle + shared reaper
│   │   │   ├── BrokerServerOptions.cs      # options + BrokerUser/BrokerPermission
│   │   │   ├── BrokerPassword.cs           # PBKDF2-SHA256 hashing/verification
│   │   │   ├── BrokerPrincipal.cs          # compiled permission regexes
│   │   │   ├── BrokerTlsOptions.cs
│   │   │   ├── BrokerConfigurationException.cs
│   │   │   ├── ChannelContext.cs           # channel FSM, consume, confirms, permissions
│   │   │   ├── PendingConfirm.cs
│   │   │   ├── HeartbeatWatchdog.cs
│   │   │   ├── IChannelDeliveryHost.cs
│   │   │   └── WireConnection.cs           # connection FSM, auth, confirm loop
│   │   └── Transport/
│   │       ├── SocketListener.cs           # TCP listener + SocketBinder
│   │       ├── TlsSocketListener.cs        # SslStream listener
│   │       ├── BrokerCertificate.cs        # PFX/PEM loading + mTLS chain validation
│   │       ├── BrokerListeners.cs          # plaintext + TLS factory
│   │       └── TransportPair.cs
│   ├── RedisMQ.Hosting/                    # configuration binding + DI lifecycle
│   │   ├── RedisMqOptions.cs               # root RedisMQ section
│   │   ├── RedisEngineOptions.cs           # Redis engine settings
│   │   ├── RedisMqServiceCollectionExtensions.cs  # AddRedisMQBroker
│   │   ├── BrokerServerHostedService.cs    # startup validation + stop/dispose
│   │   └── LoggerBrokerLogSink.cs          # broker logs -> ILogger
│   └── RedisMQ.Redis/                      # StackExchange.Redis only here
│       ├── RedisStreamRouteEngine.cs       # write-ahead mirror + replay
│       ├── RedisKeys.cs                    # cluster-safe key layout
│       ├── StreamCodec.cs                  # AMQP-wire properties in stream fields
│       └── WriteCommand.cs
├── samples/
│   └── RedisMQ.Host/                       # container-ready host + /livez + /healthz
├── tests/
│   ├── RedisMQ.Broker.Tests/               # codec/FSM/engine/server/permissions/TLS
│   ├── RedisMQ.Conformance/                # RabbitMQ.Client 7.x over real sockets
│   ├── RedisMQ.Hosting.Tests/              # binding, DI lifecycle, log sink
│   └── RedisMQ.Redis.Tests/                # Redis mirror, replay, eviction gate
├── docs/                                   # architecture, protocol, API, packaging
├── .github/workflows/ci.yml
├── Dockerfile
├── docker-compose.yml
├── global.json
└── README.md
```

---

## Running the Tests

### Preferred (CI path)

```bash
dotnet build RedisMQ.sln
dotnet test RedisMQ.sln
```

### Direct runner (this dev machine — `dotnet test` is broken by an SDK MTP bug)

```bash
# Unit tests (359 expected)
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll

# Conformance suite (19 expected)
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll

# Redis mirror/replay tests (67 expected; needs Redis on localhost:6379)
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll

# Hosting/configuration tests (11 expected)
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll
```

### Filtered runs

```bash
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConfirmPipelineTests"
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -method "Full.Qualified.Test.Name"
```

### Resetting the Redis store

Tests use unique vhosts per test, but local exploration benefits from a clean store.
Delete every RedisMQ key (`{mq:top:*}`, `{mq:q:*:*}`):

```bash
redis-cli --scan --pattern '{mq:*}' | xargs -r redis-cli DEL
```

Redis tests return early (skip) when Redis is unreachable; the broker and conformance
suites never need Redis.

---

## Conformance Suite

The conformance project (`tests/RedisMQ.Conformance`) uses **RabbitMQ.Client 7.2.2**
against a real socket to the in-process broker. It validates that RedisMQ speaks the
same dialect a real RabbitMQ broker does.

| File | What it covers |
|------|----------------|
| `TestBrokerHost.cs` | Harness: builds listeners through `BrokerListeners`, exposes `Port` and `TlsPort`, guest/guest with `BrokerUser.FullAccess` on `/` |
| `FlowTests.cs` | Connect/auth, declare/bind/publish/consume, content properties, QoS gating, nack redelivery, basic.get, round-robin, no-ack, queue expiry (9 tests) |
| `ErrorTests.cs` | Unknown exchange (404), redeclare mismatch (406), exclusive conflict (405), unknown queue (404), clean close (5 tests) |
| `WireFlowTests.cs` | `channel.flow` over raw `TcpClient`: pause/resume, no connection close (2 tests) |
| `TlsFlowTests.cs` | Server-auth TLS publish/consume, mutual TLS accept, mutual TLS refusal (3 tests) |

**Known client quirk:** RabbitMQ.Client 7.x decodes field-table longstr (`'S'`) header
values as `byte[]`, not `string`. RedisMQ emits `'S'` + longstr correctly; tests
round-trip the bytes back.

See [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for scenario-level detail and the
broker/Redis suite coverage that complements conformance.

---

## API Reference

Full signatures live in [docs/API.md](docs/API.md). The essentials:

### Server configuration

```csharp
public sealed class BrokerServerOptions
{
    public int Port { get; set; }                       // 0 = ephemeral, -1 = no plaintext
    public string[] Vhosts { get; set; } = ["/"];
    public IReadOnlyList<BrokerUser> Users { get; set; } = [];
    public uint FrameMax { get; set; } = 131072;
    public ushort Heartbeat { get; set; } = 60;
    public int ConfirmQueueCapacity { get; set; } = 4096;
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(5); // <= 0 disables
    public BrokerTlsOptions? Tls { get; set; }
    public void Validate();
}
```

### Accounts and permissions

```csharp
public sealed record BrokerPermission(string Vhost, string Configure, string Write, string Read);

public sealed record BrokerUser(
    string Name,
    string? Password = null,
    string? PasswordHash = null,
    IReadOnlyList<BrokerPermission>? Permissions = null)
{
    public static BrokerUser FullAccess(string name, string password, params string[] vhosts);
}

public static class BrokerPassword
{
    public static string Hash(string password);          // pbkdf2-sha256 encoding
    public static bool Verify(string password, BrokerUser user);
}

public sealed class BrokerPrincipal
{
    public bool IsKnownVhost(string vhost);
    public bool CanConfigure(string vhost, string name);
    public bool CanWrite(string vhost, string name);
    public bool CanRead(string vhost, string name);
}
```

Permissions are **default-deny**: an empty/missing user permission list denies every
vhost, and a vhost the list does not mention cannot be opened at all. Empty regex
patterns match only the empty name (effectively deny); `.*` grants everything.

### TLS

```csharp
public sealed class BrokerTlsOptions
{
    public bool Enabled { get; set; }
    public int Port { get; set; } = 5671;
    public string? CertificatePath { get; set; }         // PFX, or PEM certificate
    public string? CertificateKeyPath { get; set; }      // PEM private key
    public string? CertificatePassword { get; set; }     // PFX password
    public bool ClientCertificateRequired { get; set; }
    public string? ClientCertificateAuthorityPath { get; set; }
    public SslProtocols MinTlsVersion { get; set; } = SslProtocols.Tls12;
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
```

`BrokerListeners.Create(options)` returns the plaintext listener (when `Port >= 0`)
and the TLS listener (when `Tls.Enabled`); the caller disposes them.

### Engines

```csharp
public interface IRouteEngine
{
    ExchangeInfo? GetExchange(string vhost, string name);
    ExchangeInfo? DeclareExchange(ExchangeInfo exchange);
    QueueInfo?    GetQueue(string vhost, string name);
    QueueInfo?    DeclareQueue(QueueInfo queue);
    void          Bind(string vhost, string exchange, string queue, string routingKey,
                       FieldTable? arguments = null);
    void          Unbind(string vhost, string exchange, string queue, string routingKey);
    IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties,
                                  string routingKey);
    EnqueueReceipt Enqueue(string vhost, string queue, ContentProperties properties,
                           byte[] body, string exchange, string routingKey);
    ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer);
    void          UnregisterConsumer(string vhost, string queue, string consumerTag);
    EngineDelta   Requeue(string vhost, string queue, InMemoryMessage message);
    EngineDelta   Acknowledge(string vhost, string queue, InMemoryMessage message);
    EngineDelta   Reject(string vhost, string queue, InMemoryMessage message);
    EngineDelta   Reap(DateTimeOffset now);
    GetClaimOutcome TryClaimForGet(string vhost, string queue,
                                   out InMemoryMessage? message, out int remaining);
    void          Nudge(string vhost, string queue);
    void          ReconcileConnection(string connectionId);
    void          RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId);
}
```

```csharp
public readonly record struct EnqueueReceipt(
    ulong EnvelopeId,
    ValueTask<bool> Durable,
    IReadOnlyList<ulong> DroppedEnvelopeIds);

public sealed record EngineDelta(
    IReadOnlyList<QueuedMessage> Enqueued,
    IReadOnlyList<QueuedMessageRef> Removed,
    IReadOnlyList<(string Vhost, string Name)> DeletedQueues);
```

### Queue arguments

`queue.declare` accepts:

| Argument | Meaning |
|----------|---------|
| `x-expires` | Queue idle expiry in milliseconds (deleted without dead-lettering) |
| `x-max-length` | Ready-message cap; oldest messages are dropped on overflow |
| `x-delivery-limit` | After N deliveries, the next requeue dead-letters the message |
| `x-dead-letter-exchange` | Dead-letter target (explicit `""` = default exchange) |
| `x-dead-letter-routing-key` | Overrides the routing key for dead-letter copies |

Unknown arguments keep the `not_implemented` verdict; invalid values are a soft
`precondition-failed` naming the argument.

---

## Protocol Implementation Notes

Full detail lives in [docs/PROTOCOL.md](docs/PROTOCOL.md).

- Protocol header `"AMQP\0\0\9\1"`, frame-end `0xCE`, minimum frame-max 4096.
- PLAIN only. Authentication verifies a plaintext password or PBKDF2-SHA256 hash in
  fixed time.
- `connection.open` on a vhost with no permission entry → **403 access-refused**
  (default-deny). A listed but deny-all vhost still opens.
- `channel.flow(active=false)` **pauses** the channel and answers
  `channel.flow-ok(active=false)`; the connection stays open. Flow resumes with
  `active=true`.
- Exchange types: `direct`, `fanout`, `topic`, `headers`.
- `basic.publish` assembles content, routes, enqueues, and — on a confirm-mode
  channel — emits exactly one ordered confirm outcome per publish.
- A relative `expiration` property is converted to an absolute deadline at publish
  time and preserved across requeue; dead-letter copies clear it.
- Dead-letter copies reset `DeliveryCount` to 0 and carry `x-death`
  (`queue`, `reason`, `count`, `time`, `exchange`) plus `x-first-death-*`.

---

## Accepted Limitations

Documented and pinned by tests rather than hidden:

- **At-least-once, not exactly-once.** A crash between enqueue and remove may
  duplicate a message on replay; replay dedupes known crash-window duplicates but
  delivery can still repeat.
- **Unacked replay is `Redelivered=false`.** Messages restored from Redis never had
  a delivery stamped in the stream, so a crash-recovered delivery looks new.
- **Confirm timeout may still land.** A `confirm.nack` after `ConfirmTimeout` can be
  followed by the timed-out `XADD` reaching Redis, so a nacked publish may persist.
- **Settle/confirm mirroring can duplicate under pressure.** The mirror is
  best-effort by design; memory remains the system of record.
- **Partial fan-out on failure.** A publish to N queues can persist on some and
  fail on others; the confirm is `nack` when any receipt fails.
- **`x-death` copies that are unroutable are dropped silently.** There is no logging
  surface in v1.
- **The `x-death` loop guard is unconditional** for a matching (queue, reason): a
  message already dead-lettered from the same queue for the same reason is dropped
  rather than re-counted.
- **TLS certificates are loaded at startup**; rotation requires a restart.
- **No guest-loopback restriction.** RabbitMQ's implicit "guest may only connect
  from localhost" rule is not enforced; configure permissions instead.
- **Durable/auto-delete flags** are stored and replayed but the engine does not fsync
  queue metadata; Redis persistence settings govern survival.

---

## What's Next

M4 part 1 completes the functional v1 surface. Remaining for a production v1:

- **Connection/channel profiles** and tuning beyond RabbitMQ defaults.
- **Performance work**: pipelining, batching mirrors, throughput benchmarks.
- **Pika and other client compatibility** beyond RabbitMQ.Client 7.x.
- **Operational hardening**: certificate hot-reload, Redis failure recovery policy,
  management surface for users/vhosts/permissions.

---

## CI

`.github/workflows/ci.yml` runs on `ubuntu-latest` with the .NET 10.0.x SDK and the
standard `dotnet test` path. The Redis-dependent suite expects a Redis service; see
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for local setup and reset commands.
