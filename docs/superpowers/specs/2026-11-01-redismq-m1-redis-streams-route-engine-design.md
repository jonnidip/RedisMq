# RedisMQ M1 — Redis Streams route engine (write-through durability)

Date: 2026-11-01 · Status: **awaiting your read/approval** — nothing is built until you approve.

Parent plan: `docs/superpowers/plans/2026-10-30-redismq-m0-protocol-shell.md`
Parent spec: `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md`

---

## 1. Intent and why

M0 proved the AMQP 0-9-1 wire layer against real clients using a purely in-memory
`InMemoryEngine` behind the `IRouteEngine` seam. M1 layers Redis Streams underneath that same
seam as a **durable write-ahead log**: every topology change and every enqueued message is
appended to Redis so the broker can survive a restart and (later) scale read durability across
nodes, while the hot path — protocol dispatch, credit gating, delivery tags, the consume pump —
stays exactly as it is in M0 (synchronous, zero blocking calls, no changes to `WireConnection`,
`ChannelContext`, or `InMemoryQueue`).

This is **Approach D** from the M1 design conversation: an in-memory write-through cache backed
by Redis Streams. It deliberately does **not** make Redis Streams the live message store (that is
the spec's later multi-node active-active vision). M1 proves the seam works with a durable
backing store and gives crash recovery of topology + ready queues; in-flight unacked state is
recovered best-effort (M2 adds durable confirms/acks and full reconciliation).

## 2. Goals

- **G1 — Seam integrity:** `IRouteEngine` stays synchronous; `RedisMQ.Broker` keeps zero external
  dependencies. `RedisStreamRouteEngine : IRouteEngine` lives in a new project that references
  `RedisMQ.Broker` + `StackExchange.Redis` only.
- **G2 — Crash recovery of topology:** exchanges, queues, and bindings declared before a restart
  are reconstructed from Redis on startup.
- **G3 — Crash recovery of ready queues:** messages enqueued before a restart are rebuilt into
  each queue's ready buffer on startup (best-effort; in-flight unacked deliveries may be lost,
  documented as an M1 caveat).
- **G4 — No M0 regression:** every M0 test (233 unit + 15 conformance) passes unchanged against
  `RedisStreamRouteEngine` swapped in for `InMemoryEngine`.
- **G5 — Observable durability contract:** a per-profile note states what "durable" means at M1
  (topology + ready queues durable; unacked in-flight not guaranteed) so operators know what M2
  adds.

## 3. Non-goals (M1)

- Making Redis Streams the live message store (competing consumers via consumer groups, PEL,
  XREADGROUP/XAUTOCLAIM as the hot path). That is the spec's later multi-node design.
- Publisher confirms, DLX, TTL reaper, durable confirms/acks (spec M2/M3).
- TLS, vhost permissions, clustering (spec M2/M4).
- Any change to `WireConnection`, `ChannelContext`, `InMemoryQueue`, or the consume pump.

## 4. Constraints

- **C1:** `RedisMQ.Broker` (core) stays zero `PackageReference`. StackExchange.Redis lives only in
  the new M1 project.
- **C2:** `IRouteEngine` remains fully synchronous. Redis calls inside `RedisStreamRouteEngine`
  are fire-and-forget via a bounded channel + background drainer; no `.GetAwaiter().GetResult()`
  blocking on the protocol path.
- **C3:** Redis key layout is cluster-safe: all per-queue keys hash-tagged
  `{mq:q:<vhost>:<queue>}`; topology under `{mq:top}`. No cross-slot commands.
- **C4:** RESP2 default; StackExchange.Redis defaults acceptable.
- **C5:** M0 conformance suite (RabbitMQ.Client 7.2.2 over real TCP) must pass unchanged with the
  Redis engine.

## 5. Approach D — chosen (already selected by you)

`RedisStreamRouteEngine` wraps `InMemoryEngine` as the synchronous system of record and mirrors
every mutating call to Redis Streams asynchronously. Reads are served from `InMemoryEngine`
directly (fast, sync). Writes hit `InMemoryEngine` first (so the pump never blocks), then are
enqueued to a background drainer that appends to Redis. On startup, the drainer first **replays**
Redis into `InMemoryEngine` before accepting connections.

Trade-off accepted: a crash between the in-memory write and the Redis append loses that one
enqueue/topology change (durability is write-ahead-but-async). Reads are never stale because
`InMemoryEngine` is authoritative.

## 6. Design — parts decided

### 6.1 New project: `src/RedisMQ.Redis/RedisMQ.Redis.csproj`

```xml
<TargetFramework>net10.0</TargetFramework>
<PackageReference Include="StackExchange.Redis" Version="2.x" />
<ProjectReference Include="..\RedisMQ.Broker\RedisMQ.Broker.csproj" />
```

Contains one public type: `RedisStreamRouteEngine : IRouteEngine`. Internal helpers for Redis
key layout, stream entry encoding, and the async drainer.

### 6.2 `RedisStreamRouteEngine`

```csharp
public sealed class RedisStreamRouteEngine : IRouteEngine, IAsyncDisposable
{
    public RedisStreamRouteEngine(
        IDatabase redis,               // pre-configured StackExchange.Redis database
        string vhost,                  // key scope, e.g. "/"
        bool replayOnStart = true,     // replay Redis -> memory before serving
        int drainerCapacity = 1024);   // bounded write channel
}
```

- Holds a private `InMemoryEngine _memory` — the sync system of record.
- All `IRouteEngine` read methods (`GetExchange`, `GetQueue`, `RouteTo`, `TryClaimForGet`) delegate
  straight to `_memory`.
- All mutating methods (`DeclareExchange`, `DeclareQueue`, `Bind`, `Unbind`, `Enqueue`,
  `RegisterConsumer`, `UnregisterConsumer`, `Requeue`) call `_memory` synchronously, then enqueue
  a `WriteCommand` to the drainer channel.
- Constructor (when `replayOnStart`) replays Redis Streams into `_memory` before the engine is
  handed to `AmqpBrokerServer`, so the broker opens with durable topology + ready queues already
  present.

### 6.3 Redis write-ahead log model

Two streams (all hash-tagged):

| Stream | Key pattern | Purpose |
|--------|-------------|---------|
| Topology log | `{mq:top}` | Every declare/bind/unbind: `op`, entity JSON |
| Queue log | `{mq:q:<vhost>:<queue>}` | Every enqueue: envelope id, exchange, routing key, body, properties |

Stream entries use field/value pairs. Exact binary/JSON encoding of bodies and properties is
left to the builder (see "left to the builder"), but bodies must round-trip byte-exact.

### 6.4 Async drainer

A `System.Threading.Channels.Channel<WriteCommand>` (bounded, `Capacity = drainerCapacity`) plus
one background `Task` (`DrainAsync`) that reads commands and writes them to Redis with
`StreamAdd`. On channel full, `WriteCommand` blocks briefly (the channel writer's `Wait`) — this
is backpressure on the protocol thread, accepted as M1 honesty (durability can't outpace Redis).

Commands are idempotent by design where possible:
- Declares/binds are keyed by `(vhost, name)` — re-applying after replay is a no-op because
  `InMemoryEngine.DeclareExchange`/`DeclareQueue` are creation-only CAS.
- Enqueues carry the envelope id; replay reconstructs ready buffers in order.

### 6.5 Startup replay

1. Read all entries from `{mq:top}` → rebuild exchanges, queues, bindings into `_memory`
   via `DeclareExchange`/`DeclareQueue`/`Bind` (CAS semantics make re-application safe).
2. For each known queue, read `{mq:q:<vhost>:<queue>}` → rebuild ready buffer via internal
   append. (`InMemoryEngine.Enqueue` is public but goes through the drainer; replay uses an
   internal bulk-load path on `_memory` — see left-to-builder.)
3. Only after replay completes is `RedisStreamRouteEngine` passed to `AmqpBrokerServer`.

### 6.6 What does NOT change (zero-touch list)

`WireConnection`, `ChannelContext`, `InMemoryQueue`, `InMemoryEngine`, `InMemoryMessage`,
`ExchangeInfo`, `QueueInfo`, `IQueueConsumer`, `AmqpBrokerServer`, all codec/protocol/transport
files, all M0 tests. `IRouteEngine` itself does not change signature.

### 6.7 What M1 adds (new files)

- `src/RedisMQ.Redis/RedisMQ.Redis.csproj`
- `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- `src/RedisMQ.Redis/RedisKeys.cs` (key layout constants)
- `src/RedisMQ.Redis/StreamCodec.cs` (entry encode/decode)
- `src/RedisMQ.Redis/WriteCommand.cs` (command discriminated union / record)
- `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`
- `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngine.csproj` (references RedisMQ.Broker + RedisMQ.Redis + StackExchange.Redis)

## 7. Test plan

- **Unit:** `RedisStreamRouteEngine` against a real Redis (Testcontainers `redis:7-alpine`):
  - declare+redeclare round-trip survives engine rebuild
  - enqueue → durable → new engine instance replays and serves `TryClaimForGet`
  - bind → route survives restart
  - drainer backpressure (fill channel, engine still responds)
  - replay idempotency (double-replay is a no-op)
- **M0 regression:** run the existing M0 conformance suite (RabbitMQ.Client 7.2.2 over real TCP)
  and unit suite against `RedisStreamRouteEngine` swapped for `InMemoryEngine` in
  `TestBrokerHost` / `ConsumeTests` spawn helpers — 233 + 15 must stay green.
- No Testcontainers in CI yet (M4 adds that); M1 tests run locally against `localhost:6379` or
  are skipped gracefully when Redis is unavailable (builder decision).

## 8. Open items — left to the builder

- Exact stream field encoding for bodies/properties (JSON vs binary; base64 for bytes).
- Whether `RedisStreamRouteEngine` owns the `ConnectionMultiplexer` lifetime or receives an
  `IDatabase` (recommended: receives `IDatabase`, multiplexer owned by caller/Hosting).
- Internal bulk-load path on `InMemoryEngine` for replay (needs a small internal method;
  `InMemoryEngine` is in `RedisMQ.Broker` so this is an internal API addition).
- Default `drainerCapacity` and backpressure behavior when channel is full.
- StackExchange.Redis version pin.
- Testcontainers vs localhost Redis decision for CI.

## 9. What's left to you (approval gate)

You pick: approve this design as-is, or request changes. Once approved, the next step is
`superpowers:writing-plans` to produce the M1 task plan (commit-by-commit), then implementation
begins. Nothing in `RedisMQ.Broker` or any M0 test is touched.

Do you approve this design? If so, any changes to the open items in §8 before I write the plan?
