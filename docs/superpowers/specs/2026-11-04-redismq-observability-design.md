# RedisMQ v1 observability — metrics, structured logs and health

Date: 2026-11-04 · Status: builder check resolved; awaiting your read/approval — nothing is built until approved.
Parent spec: `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md`
Predecessors: M0 shell, M1 write-through engine, M2 durability/topology, M3 full exchanges,
M4 part 1 (durable confirms, DLX/TTL, permissions, TLS).

---

## 1. Intent and why

M4 part 1 completed the functional v1 surface, but the broker is still opaque in
production. There is **no logging anywhere** in `src/`: connection failures, mirror
failures, unroutable dead-letters and listener faults are absorbed by bare `catch`
blocks. There are no metrics, and no way to ask "is this broker healthy, and is Redis
durability actually working?" without a debugger or wire capture.

This slice makes the broker observable without breaking the library's shape:

- **Metrics** through the BCL `System.Diagnostics.Metrics.Meter`, so any host can
  consume them with OpenTelemetry, `dotnet-counters`, or a `MeterListener`.
- **Traces** through `ActivitySource`, opt-in and in-process only.
- **Structured logs** through a small `IBrokerLogSink` seam with a no-op default; the
  host adapts it to `Microsoft.Extensions.Logging` (example in the docs).
- **Health** as a synchronous `BrokerHealth` snapshot on the server, covering liveness,
  readiness, connection/channel counts and per-queue depth/unacked.

The delivery model is approach A, agreed before this document: the library instruments
itself and exposes state; the host owns HTTP endpoints and exporters. No HTTP listener,
no Prometheus text writer, no new package reference.

## 2. Goals

- **G1 — Metrics without dependencies.** `RedisMQ.Broker` keeps zero
  `PackageReference`s. Instruments come from the shared framework
  (`System.Diagnostics.DiagnosticSource`), not a package.
- **G2 — Honest broker metrics.** Connection/channel lifecycle, authentication and
  permission refusals, publish and confirm outcomes (ack, nack, timeout, backpressure),
  deliveries, settlements, queue depth/consumers/unacked, dead-letters and drops,
  TTL/queue expiry, reaper runs, Redis drainer depth and mirror failures, TLS
  handshakes.
- **G3 — Structured logs where failures were silent.** Lifecycle at Information;
  refusals and faults at Warning/Error; no per-message logging above Debug. Event ids
  are stable enum values; messages are built only when the sink is enabled.
- **G4 — Health snapshot.** `AmqpBrokerServer.GetHealth()` reports live/ready, active
  connections/channels, per-queue ready/consumer/unacked counts, and engine health
  (memory vs Redis, Redis connectivity, drainer depth, mirror failures). Readiness
  reflects the configured durability promise.
- **G5 — Reaper resilience.** One failing `IRouteEngine.Reap` tick must not silently
  kill the shared reaper loop; it is logged, counted and the loop continues.
- **G6 — No wire or behaviour regression.** M0–M4 unit, Redis and conformance suites
  stay green. No new protocol bytes, no change to confirm/DLX/TTL/permission/TLS
  semantics.

## 3. Non-goals

- HTTP endpoints (`/metrics`, `/health`), Prometheus text exposition, dashboards,
  alert rules. The host owns these; a later packaging slice can ship a host.
- OpenTelemetry SDK, exporters or semantic-convention conformance packages.
- Cross-process trace propagation: no `traceparent` header injection or extraction.
  That changes message headers and is a separate protocol-visible decision.
- Distributed metrics aggregation, per-connection/per-channel tag cardinality,
  audit-log persistence, log shipping.
- Runtime log-level reconfiguration, per-event sampling or rate-limit knobs.
- Instrumenting the in-process `TransportPair` test transport as its own listener.

## 4. Constraints

- `RedisMQ.Broker` keeps zero `PackageReference`s; Redis client code stays in
  `RedisMQ.Redis`. Only `System.Diagnostics.Metrics`, `System.Diagnostics`
  (`ActivitySource`) and `System.Diagnostics.Tracing` types from the shared framework
  are used.
- `IRouteEngine` stays synchronous. Instrumentation must not add awaiting on the
  protocol path, must not block, and must not take the engine gate in a new order.
- Hot paths (publish, delivery, settle) do no work when no listener and no enabled sink
  is attached: guard with `Instrument.Enabled` and `IBrokerLogSink.IsEnabled`.
- Existing public constructors keep working; new parameters are optional and appended.
- Metric tags must never contain credentials, message bodies, connection ids or user
  names. Queue/exchange names are allowed and documented as user-controlled
  cardinality.

## 5. Approaches considered

The approach was chosen before this document:

| Approach | What it gives | Why not chosen |
|---|---|---|
| **A. Instrument the library; host owns HTTP (chosen)** | Meter + ActivitySource + log seam + health snapshot; zero dependencies; standard .NET consumption path | No built-in endpoints until a host exists |
| B. A + `RedisMQ.Diagnostics` HTTP project | Self-contained Prometheus/health endpoints | Hand-rolled HTTP surface that OpenTelemetry exporters already provide; extra security/timeout tests |
| C. Adopt `Microsoft.Extensions.Logging.Abstractions` in core | Idiomatic MEL logging with scopes | Breaks the documented zero-dependency invariant |

## 6. Current state and the exact gaps

- `AmqpBrokerServer` tracks accepted connection tasks in `_connectionTasks` but
  exposes no count, has no listener lifecycle logging, and `RunReaperAsync` only
  catches `OperationCanceledException` — an engine exception faults the reaper task
  and nothing observes it.
- `WireConnection` has a `_connectionId`, the connection FSM, PLAIN authentication and
  the vhost-open check, but no way to report accepted/closed/auth-failed/vhost-refused.
  `_channels` holds every open channel but the count is private.
- `ChannelContext` has the full publish/confirm/delivery/settle surface and the
  configure/write/read permission helpers (`CanConfigure`/`CanWrite`/`CanRead`), all
  silent on denial.
- `InMemoryEngine` already computes the facts metrics need: max-length
  `DroppedEnvelopeIds`, dead-letter reasons (`rejected`, `expired`,
  `delivery_limit`), `EngineDelta.DeletedQueues` for `x-expires`, and per-queue
  `ReadyCount`/consumer list. None are exposed.
- `RedisStreamRouteEngine` has a bounded drainer channel and a catch-all that swallows
  every mirror failure; there is no failure count, no depth, no last-error, and
  `RedisEngine.Reap` inherits the silent path.
- `TlsSocketListener` catches handshake failures and continues; nothing records them.
- `IChannelDeliveryHost` is the only surface a `ChannelContext` has to its connection;
  diagnostics must be threaded through it or its constructor.
- No logging, metrics, health or `Activity` code exists anywhere under `src/`.

## 7. Design

### 7.1 Dependency-free diagnostics core

New namespace `RedisMQ.Broker.Diagnostics` in the core project:

```csharp
public enum BrokerLogLevel { Trace, Debug, Information, Warning, Error, Critical }

public enum BrokerLogEvent
{
    ServerStarted, ServerStopped, ListenerFailed, ReaperFailed,
    ConnectionOpened, ConnectionClosed, AuthenticationFailed, VhostAccessDenied,
    ChannelOpened, ChannelClosed, PermissionDenied,
    PublishRefused, PublishUnroutable, ConfirmNack, ConfirmTimeout,
    DeliveryFailed, DeadLetterDropped, QueueExpired, MessageExpired,
    RedisMirrorFailed, RedisMirrorRecovered, DrainerSaturated, TlsHandshakeFailed
}

public readonly record struct BrokerLogEntry(
    BrokerLogLevel Level,
    BrokerLogEvent Event,
    string Message,
    Exception? Exception = null,
    IReadOnlyList<KeyValuePair<string, object?>>? Data = null);

public interface IBrokerLogSink
{
    bool IsEnabled(BrokerLogLevel level);
    void Log(in BrokerLogEntry entry);
}

public sealed class NullBrokerLogSink : IBrokerLogSink
{
    public static readonly NullBrokerLogSink Instance = new();
    public bool IsEnabled(BrokerLogLevel level) => false;
    public void Log(in BrokerLogEntry entry) { }
}
```

`BrokerDiagnostics` owns one `Meter` and one `ActivitySource`, both named
`RedisMQ.Broker` with the assembly version, plus the sink and the instrument handles:

```csharp
public sealed class BrokerDiagnostics : IDisposable
{
    public BrokerDiagnostics(
        string? instanceId = null,          // default: 8 random hex chars
        IBrokerLogSink? logSink = null,     // default: NullBrokerLogSink
        string? serviceVersion = null);     // default: assembly informational version

    public string InstanceId { get; }
    public Meter Meter { get; }
    public ActivitySource ActivitySource { get; }
    public IBrokerLogSink Log { get; }
    public void Dispose();                  // disposes Meter and ActivitySource
}
```

Every measurement carries `broker.instance = InstanceId` so a process running more
than one engine/server (tests, embedded scenarios) can attribute series. The supported
production shape is **one shared `BrokerDiagnostics` per broker process**; the docs
show passing the same instance to the engine and the server. If a component is given
`null`, it creates and owns its own instance for its lifetime (never disposed by the
component; `Meter`/`ActivitySource` are safe to leave to the GC, and `Dispose` exists
for hosts that want determinism).

Instruments are created lazily per instance but only touched when
`Instrument.Enabled` is true. Observable gauges are callbacks on that instance, so
multiple instances never fight over a shared registry.

### 7.2 Metric catalog

Names use a `redismq.` prefix (these are broker-side metrics; OpenTelemetry's
`messaging.*` conventions describe clients). Durations are seconds. Counts are `long`
counters. Every instrument also carries `broker.instance`.

| Instrument | Type | Unit | Tags | Emitted at |
|---|---|---|---|---|
| `redismq.connections.opened` | Counter | `{connection}` | `listener` (plaintext/tls) | accept handed to a connection |
| `redismq.connections.closed` | Counter | `{connection}` | `reason` (client/server/error/auth_refused/vhost_refused/heartbeat) | connection end |
| `redismq.connections.active` | ObservableGauge | `{connection}` | — | scrape |
| `redismq.connections.authenticated` | Counter | `{connection}` | — | `connection.open` accepted |
| `redismq.channels.opened` | Counter | `{channel}` | `vhost` | first `channel.open` accepted |
| `redismq.channels.closed` | Counter | `{channel}` | `vhost`, `reason` | channel retired |
| `redismq.channels.active` | ObservableGauge | `{channel}` | — | scrape |
| `redismq.auth.failures` | Counter | `{failure}` | `reason` (malformed/unknown_user/bad_credentials) | authentication refusal |
| `redismq.vhost.refusals` | Counter | `{refusal}` | `reason` (unknown/denied) | `connection.open` refusal |
| `redismq.permission.denials` | Counter | `{denial}` | `operation` (configure/write/read) | permission check fails |
| `redismq.tls.handshakes` | Counter | `{handshake}` | `result` (success/failure) | TLS handshake completes/fails |
| `redismq.tls.handshake.duration` | Histogram | `s` | `result` | handshake |
| `redismq.publishes` | Counter | `{message}` | `vhost`, `exchange`, `exchange_type`, `outcome` (routed/unroutable/mandatory_return/refused/error) | content dispatch completes |
| `redismq.publish.size` | Histogram | `By` | `vhost` | content dispatch |
| `redismq.confirms` | Counter | `{confirm}` | `outcome` (ack/nack/timeout) | confirm outcome written |
| `redismq.confirm.duration` | Histogram | `s` | `outcome` | publish admitted → outcome |
| `redismq.deliveries` | Counter | `{message}` | `vhost`, `queue`, `auto_ack` | delivery written (or no-ack get) |
| `redismq.settlements` | Counter | `{settlement}` | `vhost`, `queue`, `kind` (ack/requeue/reject), `method` (ack/nack/reject) | settle accepted |
| `redismq.messages.ready` | ObservableGauge | `{message}` | `vhost`, `queue` | scrape |
| `redismq.consumers` | ObservableGauge | `{consumer}` | `vhost`, `queue` | scrape |
| `redismq.messages.unacked` | ObservableGauge | `{message}` | `vhost`, `queue` | scrape |
| `redismq.messages.dropped` | Counter | `{message}` | `vhost`, `queue`, `reason` (max_length) | message evicted |
| `redismq.messages.expired` | Counter | `{message}` | `vhost`, `queue` | TTL expiry |
| `redismq.queues.expired` | Counter | `{queue}` | `vhost` | `x-expires` delete |
| `redismq.messages.deadlettered` | Counter | `{message}` | `vhost`, `queue`, `reason` (rejected/expired/delivery_limit) | dead-letter copy enqueued |
| `redismq.deadletters.dropped` | Counter | `{message}` | `vhost`, `queue`, `reason` (unroutable/missing_exchange/loop_guard) | configured DLX produced no copy |
| `redismq.reaper.sweeps` | Counter | `{sweep}` | — | reaper tick |
| `redismq.reaper.duration` | Histogram | `s` | — | reaper tick |
| `redismq.reaper.failures` | Counter | `{failure}` | — | reaper tick threw |
| `redismq.redis.mirror.commands` | Counter | `{command}` | `kind`, `outcome` (success/failure) | one drainer queue item finishes (a Delta batch counts once) |
| `redismq.redis.mirror.duration` | Histogram | `s` | `kind` | one drainer queue item |
| `redismq.redis.mirror.dropped` | Counter | `{command}` | `kind`, `reason` (channel_full/shutdown) | a command could not enter the drainer channel |
| `redismq.redis.drainer.depth` | ObservableGauge | `{command}` | — | scrape |
| `redismq.redis.drainer.saturated` | Counter | `{publish}` | — | enqueue refused: no drainer slot |
| `redismq.redis.replay.messages` | Counter | `{message}` | — | startup replay restores a message |
| `redismq.redis.replay.failures` | Counter | `{failure}` | — | startup replay fails |

Histogram boundaries (seconds): `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
2.5, 5, 10`. Publish-size boundaries (bytes): `64, 256, 1024, 4096, 16384, 65536,
262144, 1048576, 4194304`.

`exchange` and `queue` tags are only added when the value is non-empty. Queue-scoped
tags are user-controlled; this is documented rather than hidden (§13).

**Gauge registration and sharing.** `BrokerDiagnostics` exposes an internal
`RegisterObservableGauge(name, unit, description, Func<IEnumerable<Measurement<long>>>)`
that creates the gauge **once per name** and keeps a list of callbacks. Each
observation concatenates the callbacks and sums measurements with identical tag sets,
so one shared diagnostics instance can serve the server runtime, the memory engine and
the Redis engine without duplicate instrument registration or last-writer-wins series.
The server registers on `StartAsync` and disposes the handle on `StopAsync` (a restart
registers again); `InMemoryEngine` gains an additive `IDisposable` that unregisters, and
`RedisStreamRouteEngine` unregisters in `DisposeAsync`. With the supported one
engine/server per process the aggregation is a no-op.

**Outcome-tag contract** (pinned so tests and docs cannot drift):

- `publishes.outcome`: `routed` (at least one queue target), `unroutable` (no target,
  not mandatory), `mandatory_return` (no target, mandatory → `basic.return`),
  `refused` (engine refused before memory mutation, e.g. drainer slot exhaustion),
  `error` (the dispatch threw).
- `confirms.outcome`: `ack` (all durable receipts true, or no durability promise),
  `nack` (a receipt false or the publish was refused), `timeout` (the receipt did not
  complete within `ConfirmTimeout`). An unroutable publish still emits `ack`; the
  unroutable fact is carried by `publishes.outcome`, not by a confirm outcome.
- `settlements.kind` is the **effective action**: `ack`, `requeue`, `reject`.
  `basic.nack(requeue=true)` is `kind=requeue`, `basic.nack(requeue=false)` is
  `kind=reject`, and `basic.reject(requeue=...)` maps the same way. The wire verb is
  the separate `method` tag (`ack`/`nack`/`reject`). A requeue that overflows
  `x-delivery-limit` still counts as `kind=requeue` here and additionally increments
  `messages.deadlettered` with `reason=delivery_limit`.

### 7.3 Tracing

`BrokerDiagnostics.ActivitySource` is opt-in via `ActivitySource.HasListeners()`:

- **`amqp.publish`** — starts when content dispatch begins and ends when the publish's
  confirm outcome is written (or at enqueue completion when confirm mode is off). Tags:
  `messaging.system=rabbitmq`, `messaging.operation=publish`,
  `messaging.destination.name` (exchange), `redismq.vhost`,
  `messaging.rabbitmq.destination.routing_key`, `redismq.body.size`; on end
  `redismq.outcome` (routed/unroutable/mandatory_return/refused, or
  nack/timeout in confirm mode). Status is `Error` only for
  an engine refusal/timeout, not for a normal unroutable drop.
- **`amqp.deliver`** — one span per delivery, ending after the frame write completes.
  Tags: `messaging.system=rabbitmq`, `messaging.operation=deliver`,
  `messaging.destination.name` (queue), `redismq.vhost`, `redismq.delivery.tag`,
  `redismq.redelivered`, `redismq.body.size`.
- No `amqp.settle` span in this slice; settlement is covered by metrics. No parent
  context is injected into AMQP headers, so spans are in-process only.

`Activity` lifetime is owned by one place per path, and `Stop()` is called exactly once:

- **Confirm mode:** the publish `Activity` travels inside `PendingConfirm`; the
  connection's **confirm loop owns it** and stops it in a `finally` for every entry,
  including the early returns for a retired/replaced/not-open channel. When the loop
  exits it drains the entries still queued and stops their activities. A publish
  dropped by `EnqueueConfirm` because the connection is shutting down (or `TryWrite`
  fails) is stopped on that drop path. `ChannelContext.ScheduleConfirmResult` never
  stops it — it only writes the outcome.
- **No confirm mode:** `FinishPublish` stops the activity synchronously when it returns
  after memory admission (outcome `routed`/`unroutable`/`refused`). It deliberately
  does not await `EnqueueReceipt.Durable`; the span measures admission, not durability,
  because confirms are off and the protocol path must not block on Redis.
- **Teardown:** any activity still attached to a pending confirm is stopped when the
  confirm loop drains on shutdown; a channel close cannot leave an unstopped activity.

### 7.4 Structured logging

- Sink is attached through `BrokerDiagnostics` only; no new option property.
- Call sites use `if (log.IsEnabled(level))` and build the message/data only inside the
  guard. Event ids are the stable structure; `Data` is a small key/value list for
  values a MEL adapter can map to structured fields.
- Levels: `Information` for server start/stop and connection open/close;
  `Warning` for auth failure, vhost refusal, permission denial, publish refusal,
  confirm nack/timeout, dead-letter drop, queue expiry; `Error` for listener/reaper
  failure, mirror failure, TLS handshake failure; `Debug` for per-publish/delivery
  detail and Redis recovery.
- **No secrets**: never log passwords, password hashes, bodies or headers. User names
  are not logged at Information; an auth-failure entry may include the attempted
  username only at `Debug` (documented as PII).
- Mirror failure storm control: the Redis engine logs the **first** failure after a
  healthy period at `Error` and a `RedisMirrorRecovered` at `Information` on the next
  success. Every failed command still increments the counter and updates
  `LastMirrorFailureUtc`; no per-command log.
- TLS handshake failures are `Debug` (one line per failed attempt could be a hostile
  peer's lever) plus the counter. Reaper failures are `Error` per failed tick plus the
  counter.

### 7.5 Health model

```csharp
public sealed record QueueSnapshot(string Vhost, string Name, int Ready, int Consumers, int Unacked = 0);

public sealed record EngineHealth(
    string Kind,                          // "memory" or "redis"
    bool Durable,                         // a durable backend is configured
    bool? RedisConnected,                 // null for the memory engine
    long DrainerDepth,
    long MirrorFailures,
    long MirrorDropped,
    DateTimeOffset? LastMirrorFailureUtc,
    DateTimeOffset? LastMirrorDropUtc,
    string? LastError);                   // sanitized, no credentials

public sealed record BrokerHealth(
    bool Live,
    bool Ready,
    string? Detail,                       // why not ready, null when ready
    DateTimeOffset Timestamp,
    int ActiveConnections,
    int ActiveChannels,
    IReadOnlyList<QueueSnapshot> Queues,
    EngineHealth Engine);

public interface IEngineDiagnostics
{
    EngineHealth GetEngineHealth();
    IReadOnlyList<QueueSnapshot> QueueSnapshots();
}
```

`AmqpBrokerServer.GetHealth()` is synchronous and cheap enough for a scrape interval:

- **Live**: the server is started and its accept loops have not faulted. A server with
  no listeners is live (it may only run the reaper).
- **Ready**: live and the engine is ready. Memory engine: always ready with
  `Durable=false`. Redis engine with a `null` `IDatabase`: ready, `Durable=false`
  (memory-only mode). Redis engine with a database: ready only while
  `IDatabase.Multiplexer.IsConnected`; the detail names the last mirror error when one
  is known. A Redis disconnect makes the broker not ready because new publishes cannot
  honour the durability promise; confirms already answer `nack`.
- **Active connections/channels** come from a per-server `BrokerRuntimeState`:
  `Interlocked` counters updated by `WireConnection` (accepted/closed) and
  `ChannelContext` (opened/retired), plus per-queue unacked counters updated on
  manual delivery claim and on settle/teardown. Unacked entries are intersected with
  the engine's queue snapshot so deleted queues never leak into health.
- **Queues** come from `IEngineDiagnostics.QueueSnapshots()`. `InMemoryEngine` returns
  its queues with `ReadyCount` and consumer count under the engine gate and
  `Unacked = 0`; `GetHealth()` overlays the runtime state's unacked counter for each
  `(vhost, queue)` so the engine interface stays free of connection state.
  `RedisStreamRouteEngine` delegates to its in-memory engine. A third-party engine that
  does not implement `IEngineDiagnostics` reports `Kind="custom"`, `Durable=false`, and
  an empty queue list.

`BrokerRuntimeState` is internal. `AmqpBrokerServer` owns one and passes it to each
`WireConnection` through an internal constructor; the existing public constructor keeps
working by creating a private state. `ChannelContext` (already internal-constructed)
receives it from `WireConnection`.

### 7.6 Engine diagnostics

- `InMemoryEngine` implements `IEngineDiagnostics`: `Kind="memory"`, `Durable=false`,
  `RedisConnected=null`, queue snapshots from `_queues`/`_delivery`.
- `RedisStreamRouteEngine` implements it: `Kind="redis"`,
  `Durable = _redis is not null`, `RedisConnected = _redis?.Multiplexer.IsConnected`,
  `DrainerDepth = _channel?.Reader.Count ?? 0` (bounded channels are countable),
  `MirrorFailures`/`LastMirrorFailureUtc`/`MirrorDropped`/`LastMirrorDropUtc`/
  `LastError` from the drainer.
- `RedisStreamRouteEngine` constructs its inner store as
  `new InMemoryEngine(_vhosts, diagnostics)`, so the memory layer is the **sole**
  emitter of message-level counters (max-length, expiry, dead-letter, queue expiry) and
  the Redis wrapper emits only mirror/drainer/replay instruments. This avoids double
  counting and keeps the Redis deployment's queue metrics identical to memory's.
- A command rejected by the bounded drainer channel (`TryWrite` false, or a write
  during shutdown) increments `redis.mirror.dropped` and is recorded in
  `MirrorDropped`/`LastMirrorDropUtc`. It does **not** mark Redis as failed or flip
  readiness: best-effort settles are a documented durability limitation, not a
  connectivity failure. A `Delta` batch is one drainer queue item and one
  `mirror.commands` measurement; the individual XADD/XDEL calls inside it are not
  counted separately.
- Engines accept an optional `BrokerDiagnostics` in their constructors (appended
  parameter) and expose it as a `Diagnostics` property. Instrumentation itself lives in
  the engine methods that already have the facts:
  - enqueue: max-length drops (`DroppedEnvelopeIds`), publish-size histogram is
    channel-side;
  - requeue/reject/reap: dead-letter and drop counters, with the reason known at the
    call site; queue-expiry deletions from `DeletedQueues`;
  - `DeadLetterAlreadyLocked` gains an internal outcome enum (copy/none/no_dlx/
    missing_exchange/unroutable/loop_guard) so only configured-but-unusable DLX paths
    count as `deadletters.dropped`.
- The Redis engine counts every drainer command, its duration, its success/failure and
  the mirror-failure episode; it also exposes the depth gauge.
- Startup replay counts restored messages; a replay exception still throws (startup
  contract) but increments `replay.failures` and logs before propagating.

### 7.7 Wiring and lifetime

```csharp
public InMemoryEngine(IReadOnlyList<string> vhosts, BrokerDiagnostics? diagnostics = null);
public RedisStreamRouteEngine(IDatabase? redis, IReadOnlyList<string> vhosts,
    bool replayOnStart = true, int drainerCapacity = 1024,
    bool enforceEvictionGate = true, BrokerDiagnostics? diagnostics = null);
public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine,
    IReadOnlyList<IListenerTransport> listeners, BrokerDiagnostics? diagnostics = null);
public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine,
    IListenerTransport? listener = null, BrokerDiagnostics? diagnostics = null);
public WireConnection(BrokerServerOptions options, IRouteEngine routes,
    TransportSide transport, string? connectionId = null,
    BrokerDiagnostics? diagnostics = null);
public static IReadOnlyList<IListenerTransport> BrokerListeners.Create(
    BrokerServerOptions options, BrokerDiagnostics? diagnostics = null);
```

`BrokerListeners.Create` passes the diagnostics to `TlsSocketListener` (new optional
constructor parameter) so handshake metrics/logs have a home. `TlsSocketListener`
keeps its current constructors source-compatible.

Instrumentation points, by file:

| File | Hooks |
|---|---|
| `AmqpBrokerServer` | open/close counters, active gauges (via runtime state), reaper sweep/duration/failure, lifecycle logs, reaper try/catch |
| `WireConnection` | accepted/closed + reason, authenticated, auth failures, vhost refusal, active connections, publish span ownership |
| `ChannelContext` | channels opened/closed/active, publish outcome/size, confirm outcome/duration, deliveries, settlements, permission denials, deliver span |
| `InMemoryEngine` | max-length drops, expiry, dead-letter copies/drops, queue expiry, depth/consumer snapshots |
| `RedisStreamRouteEngine` | mirror commands/duration/failure episode, drainer depth/saturation, replay counts, engine health |
| `TlsSocketListener` | handshake result/duration, failure log |

### 7.8 Overhead and failure policy

- Counters/histograms: one `Enabled` check plus a `TagList` build on hot paths; `Add`
  is a no-op with no listener.
- Gauges: pull-based, allocate one snapshot per scrape; engine snapshots take the
  existing engine gate briefly (same order as today, no new locks).
- Logs: guarded; no message interpolation when disabled. Mirror/TLS storms controlled
  as in §7.4.
- Instrumentation never throws into the protocol path: counters are simple calls; log
  sinks are host code and an exception from a sink is caught and swallowed (a broken
  sink must not kill a connection). This catch itself uses no logging.
- The reaper's new try/catch changes availability (loop survives) without changing
  `Reap` semantics.

## 8. Public API summary

New namespace `RedisMQ.Broker.Diagnostics`: `BrokerDiagnostics`, `IBrokerLogSink`,
`BrokerLogLevel`, `BrokerLogEvent`, `BrokerLogEntry`, `NullBrokerLogSink`,
`BrokerHealth`, `EngineHealth`, `QueueSnapshot`, `IEngineDiagnostics`.

Changed (all additive, source-compatible):
`InMemoryEngine`, `RedisStreamRouteEngine`, `AmqpBrokerServer`, `WireConnection`
constructors; `BrokerListeners.Create`; `TlsSocketListener`; `PendingConfirm` gains an
optional `Activity` property (or equivalent internal carrier).

Unchanged: the wire protocol, `IRouteEngine`, `ChannelOutcome`, options semantics,
`BrokerUser`/permissions, TLS options.

## 9. Testing strategy

BCL-only test harnesses in `RedisMQ.Broker.Tests`:

- `MeterCollector`: a `MeterListener` subscribed to `RedisMQ.Broker`, recording
  `(instrument, tags, value)` for counters/histograms and invoking observable gauges on
  demand. Tests filter by `broker.instance` when a suite runs several servers.
- `ActivityCollector`: an `ActivityListener` with `AllDataAndRecorded` for the
  `RedisMQ.Broker` source.
- `RecordingLogSink`: captures entries and can return `false` from `IsEnabled` to prove
  gating (asserting no `Log` calls).

Broker tests:

1. Connection/channel lifecycle counters and gauges over `TransportPair`; close
   reasons.
2. Auth failure and vhost refusal counters/logs; permission denials for
   configure/write/read (including the default-exchange and bind ordering cases).
3. Publish outcomes: routed, unroutable, mandatory return; confirm ack, engine-refused
   nack, mirror nack, and timeout; publish/deliver spans start and stop on every path,
   including a confirm whose channel was retired before the outcome, a confirm dropped
   at connection shutdown, and a no-confirm publish (span ends at admission).
4. Deliveries and settlements: `kind`/`method` mapping for ack, nack(requeue=false),
   nack(requeue=true), reject(requeue=true/false); no-ack (no settle) and `basic.get`;
   unacked gauge rises and falls.
5. Engine: max-length drop, TTL expiry, delivery-limit dead-letter, configured-DLX
   unroutable drop, loop-guard drop, `x-expires` deletion.
6. Reaper: sweep/duration counters; a throwing engine is counted/logged and the next
   tick still runs (use a fake engine that throws once).
7. Health: memory engine live/ready and queue snapshot; zero active counts before
   start; counts after a client connects; unacked overlay; detail when not ready.
8. `BrokerDiagnostics` disposal; two instances in one process remaining separately
   attributable (instance tag); a shared instance with two gauge sources aggregating
   identical tag sets instead of dropping one.

Redis tests (skip without Redis):

9. Mirror command counters and failure episode: a failing `IDatabase`/closed
   multiplexer increments failures once, logs the first failure and the recovery; depth
   gauge reflects a blocked drainer (reuse the drainer-slot seam); a full/shut drainer
   channel increments `mirror.dropped` without flipping readiness.
10. Engine health: `Durable=true`, `RedisConnected` true/false, last error populated,
    `MirrorDropped`/`LastMirrorDropUtc`; drainer saturation counter when slots are
    exhausted.
11. Replay counters for restored messages.

Conformance: existing 19 tests stay unchanged and green. One optional conformance test
asserts a real RabbitMQ.Client publish/consume produces publish/deliver measurements and
a health snapshot with the expected queue; it must not depend on timing beyond the
existing flow tests.

## 10. Documentation

- `README.md`: new "Observability" section (Meter/ActivitySource names, instrument
  table link, `BrokerDiagnostics` wiring, `GetHealth()` example, MEL adapter example,
  and the note that endpoints/exporters are host-owned); update status and
  "What's next" (remove observability).
- `docs/API.md`: new types and constructor changes.
- `docs/ARCHITECTURE.md`: diagnostics layer in the diagram/table.
- `docs/DEVELOPMENT.md`: instrumentation conventions (Enabled/IsEnabled guards, event
  ids, no PII, no throwing sinks) and the remaining-work list.
- `docs/CONFORMANCE.md`: note that observability did not change the wire surface.
- Parent spec/M4 design: no rewrite; this document is the follow-on and the README
  links it.

## 11. Accepted limitations (to document, not hide)

- No built-in HTTP endpoints or exporters; a host must map the Meter, ActivitySource,
  log sink and health snapshot.
- Traces are in-process only; no `traceparent` propagation through AMQP headers.
- Log volume is not rate-limited beyond the mirror-failure episode rule; auth/TLS
  failures can be produced at connection-attempt rate, so hosts should filter.
- Health reflects the in-memory system of record, not the Redis stream contents.
  A Redis outage that has not been observed by the engine's connection state may lag
  readiness by one multiplexer probe.
- `queue`/`exchange` tags and `broker.instance` tags grow series cardinality in
  multi-tenant deployments; no per-instrument allowlist in v1.
- Unacked counts are best-effort under races: a delivery and its settle can cross a
  scrape; teardown subtracts the channel's ledger count to avoid permanent drift.
- Metrics are process-local; no aggregation across nodes.
- A log sink that blocks will block the caller; the design catches sink exceptions but
  does not add an asynchronous log queue.

## 12. Rollout order (the later plan will split this into tasks)

1. `Diagnostics` core types + `BrokerDiagnostics` + BCL test harnesses.
2. `IEngineDiagnostics` + engine snapshots and `BrokerRuntimeState`.
3. Connection/channel/auth/vhost/permission instrumentation.
4. Publish/confirm/delivery/settlement instrumentation + activities.
5. Engine drop/DLX/expiry instrumentation + reaper resilience.
6. Redis drainer metrics, failure episode, depth, replay metrics, engine health.
7. `GetHealth()` assembly, TLS handshake metrics, docs, full gate.

## 13. Calls I made (check these while reading)

- Per-component `BrokerDiagnostics` instances with a `broker.instance` tag; sharing one
  per process is recommended but not enforced. A shared instance aggregates gauge
  callbacks by name and sums identical tag sets (§7.2).
- No new `BrokerServerOptions` properties: diagnostics are constructor-injected.
- Unacked is tracked in a per-server runtime counter set, not by scanning channel
  ledgers; teardown reconciles against the ledger, and health overlays it onto engine
  snapshots.
- Readiness follows Redis `IsConnected` directly (no debounce/grace window).
- Missing DLX (queue has no dead-letter exchange) is a normal discard, not counted in
  `deadletters.dropped`; configured-but-unusable DLX paths are counted.
- The confirm loop owns and stops publish activities; `ScheduleConfirmResult` does not.
  A no-confirm publish span ends synchronously at memory admission and does not measure
  durability.
- `publishes` tags the publish outcome, `confirms` tags only ack/nack/timeout, and
  `settlements` splits the effective action (`kind`) from the wire verb (`method`).
- The Redis engine passes its diagnostics into the inner `InMemoryEngine`, which stays
  the sole emitter of message-level counters; the wrapper emits mirror/replay only.
- Commands that cannot enter the drainer channel are counted as `mirror.dropped` and
  surfaced in health, but do not make the broker not-ready.
- `amqp.deliver` spans cover the frame write only; no settlement span in this slice.
- TLS handshake failures log at Debug, not Warning, to avoid a hostile-peer log lever.
