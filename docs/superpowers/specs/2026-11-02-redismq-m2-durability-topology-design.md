# RedisMQ M2 — Durability & Topology Design

Date: 2026-11-02 · Status: **awaiting your read/approval** — nothing is built until you approve.

Parent plan: `docs/superpowers/plans/2026-10-30-redismq-m0-protocol-shell.md`
Parent spec: `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md`
Parent design: `docs/superpowers/specs/2026-11-01-redismq-m1-redis-streams-route-engine-design.md`

---

## 1. Intent and why

M1 proved the `IRouteEngine` seam works with a Redis Streams write-ahead log, but it is single-vhost, has no publisher confirms, no durability guarantees, and no lifecycle cleanup. M2 makes RedisMQ a real broker: multiple virtual hosts, durable declarations that survive restarts, publisher confirms for at-least-once delivery, a background reaper for TTL/queue expiry, a startup eviction-policy gate, and exclusive/auto-delete queue reconciliation on disconnect.

## 2. Goals

- **G1 — Multi-vhost:** One `RedisStreamRouteEngine` instance serves many vhosts, each hash-tagged in Redis (`{mq:q:<vhost>:<queue>}`, `{mq:top:<vhost>}`). No cross-slot commands.
- **G2 — Durable declarations:** Exchanges, queues, and bindings declared `durable=true` survive engine restart via Redis replay. Non-durable declarations do not.
- **G3 — Publisher confirms:** `confirm.select`/`confirm.select-ok`/`confirm.ack`/`confirm.nack` channel methods. A published message is `confirm.ack`-ed once it lands in memory (system of record); `confirm.nack` on failure.
- **G4 — TTL & queue-expiry reaper:** Background reaper removes expired queues (`x-expires` on declare) and expired messages (per-message `expiration`). Runs as a background task.
- **G5 — Eviction-policy startup gate:** On startup, probe Redis `CONFIG GET maxmemory-policy`. Refuse (default) or warn (configurable) on `allkeys-*`/`volatile-*` eviction when durable data exists.
- **G6 — Exclusive/auto-delete reconciliation:** On connection end, delete exclusive queues owned by that connection; delete auto-delete queues when their last consumer disconnects.
- **G7 — No M0/M1 regression:** All 233 unit + 15 conformance + 16 M1 tests pass unchanged against `RedisStreamRouteEngine`.

## 3. Non-goals (M2)

- Making Redis Streams the live message store (consumer groups, XREADGROUP as hot path) — spec M3+.
- DLX dead-lettering — spec M2 mentions it but it's complex; deferred to M3. M2 only does expiry-based removal.
- Vhost permissions/authZ — spec M4.
- TLS — spec M2 mentions it but deferred to M4 per spec §5.2 (schema not yet designed).
- Exactly-once delivery — anti-goal per spec §3.

## 4. Constraints (inherited + M2-specific)

- **C1:** `RedisMQ.Broker` (core) keeps zero `PackageReference`s. StackExchange.Redis only in `src/RedisMQ.Redis`.
- **C2:** `IRouteEngine` remains fully synchronous. No blocking sync-over-async on the protocol path.
- **C3:** Redis key layout cluster-safe: all per-queue keys hash-tagged `{mq:q:<vhost>:<queue>}`; topology `{mq:top:<vhost>}`. No cross-slot commands.
- **C4:** `IRouteEngine` signature unchanged — M2 adds new methods only if unavoidable; prefers adding behavior to existing methods or new classes in `RedisMQ.Redis`.
- **C5:** M0 conformance suite (RabbitMQ.Client 7.2.2 over real TCP) must pass unchanged with `RedisStreamRouteEngine`.

## 5. Approach

Build on M1's Approach D (in-memory write-through cache backed by Redis Streams). `RedisStreamRouteEngine` becomes **multi-vhost**: it owns one `InMemoryEngine` (already multi-vhost internally) + one Redis connection. All writes go to memory first, then are mirrored to Redis via the bounded-channel drainer. M2 adds five layers on top:

| Layer | What it adds | Where |
|-------|--------------|-------|
| Multi-vhost engine | One engine serves N vhosts; Redis keys hash-tagged per vhost | `RedisStreamRouteEngine` ctor takes `IReadOnlyList<string> vhosts` |
| Eviction gate | Startup `CONFIG GET maxmemory-policy` check; refuse/warn on unsafe | `RedisStreamRouteEngine` constructor |
| Confirms | Per-channel confirm sequence + `confirm.ack`/`confirm.nack` channel methods | `ChannelContext` + `WireConnection` dispatch |
| Reaper | Background task: queue TTL + message expiry removal | `RedisMQ.Redis.Reaper` (IAsyncDisposable background loop) |
| Reconciliation | Track exclusive/auto-delete queue ownership per connection; clean up on disconnect | `IRouteEngine` + `WireConnection` teardown |

### 5.1 Multi-vhost engine

`RedisStreamRouteEngine` ctor changes from `(IDatabase?, string vhost, ...)` to `(IDatabase?, IReadOnlyList<string> vhosts, ...)`. Internally it creates one `InMemoryEngine(vhosts)` (already multi-vhost). Replay replays topology + ready queues for all vhosts. The drainer mirrors writes to vhost-specific streams.

Key layout:
- Topology: `{mq:top:<vhost>}`
- Queue: `{mq:q:<vhost>:<queue>}`

### 5.2 Eviction-policy gate

On constructor (when `redis is not null` and `enforceEvictionGate` true), run `CONFIG GET maxmemory-policy`. If the value is `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`, and any durable declaration exists → throw `BrokerConfigurationException` (refuse) or log warning (configurable). Default: refuse.

This is a startup check only; runtime eviction is Redis's problem.

### 5.3 Publisher confirms

Wire methods (added to `MethodRegistry`):
- `confirm.select` (85, 10) → `confirm.select-ok` (85, 11): enable confirms on channel. Per-channel `ConfirmMode` flag + sequence counter starting at 1.
- `confirm.ack` (85, 50): server→client. Fields: `delivery-tag` (longlong), `multiple` (bit).
- `confirm.nack` (85, 51): server→client. Fields: `delivery-tag` (longlong), `multiple` (bit), `requeue` (bit).

Channel-layer tracking (in `ChannelContext`):
- `_confirmMode` (bool), `_confirmSequence` (long)
- On `basic.publish` when confirm mode: assign `confirmSequence = ++_confirmSequence`; after `Enqueue` succeeds → send `confirm.ack(confirmSequence, multiple=false)`.
- `confirm.ack(deliveryTag, multiple=true)`: ack all confirms ≤ deliveryTag.
- `confirm.nack(deliveryTag, multiple, requeue)`: nack confirms; if requeue, requeue the messages.

Method registry needs `confirm.ack` (85, 50) and `confirm.nack` (85, 51) added. Verify exact IDs against `rabbitmq-codegen.json` fixture during implementation.

### 5.4 Reaper

`RedisMQ.Redis.Reaper` — background loop (IAsyncDisposable) that:
1. Every N seconds (configurable, default 30s), scans all known queues for `x-expires` (queue-level TTL set at declare). Delete expired queues from memory + Redis.
2. Scans ready + unacked messages for per-message `expiration` (timestamp in the future). Remove expired messages from ready buffer; optionally dead-letter (deferred to M3). For M2, just remove.

The reaper needs access to the engine's queues. `InMemoryEngine` exposes `QueueNamesOfVhost` (already public). For message TTL, `InMemoryMessage` needs an `Expiration` field. Add `Expiration` (DateTimeOffset?) to `InMemoryMessage` record.

Reaper runs inside `RedisStreamRouteEngine` or as a separate hosted service. For simplicity, make it a method on `RedisStreamRouteEngine` called `StartReaper(TimeSpan interval)` / `StopReaper()`.

### 5.5 Reconciliation (exclusive / auto-delete)

Track queue ownership:
- `IRouteEngine` gains no new public methods; instead `RedisStreamRouteEngine` tracks:
  - `_exclusiveQueues: Dictionary<(vhost, queue), connectionId>` — which connection owns an exclusive queue
  - `_autoDeleteQueues: Dictionary<(vhost, queue), HashSet<connectionId>>` — connections with consumers on auto-delete queues

`RegisterConsumer`/`UnregisterConsumer` update `_autoDeleteQueues`. `DeclareQueue` with `exclusive=true` records owner in `_exclusiveQueues`.

On `WireConnection.RunAsync` finally (connection end), call `engine.ReconcileConnection(connectionId)`:
- For each exclusive queue owned by this connection → delete queue from engine + Redis (mirror via drainer `Unbind`/delete).
- For each auto-delete queue where this connection was the last consumer → delete queue.

New `WriteCommandKind.DeleteQueue` for the drainer. New `IRouteEngine` method? To keep the interface stable, add an internal method on `RedisStreamRouteEngine` called by `WireConnection`. But `WireConnection` only knows `IRouteEngine`...

Options:
A. Add `ReconcileConnection(string connectionId)` to `IRouteEngine` — interface change, but clean.
B. `WireConnection` casts to `IRouteEngine` — ugly.
C. Pass a reconciliation callback into `ChannelContext`/`WireConnection` — complex.

Option A is cleanest. Add `void ReconcileConnection(string connectionId)` to `IRouteEngine`. `InMemoryEngine` implements it as no-op (M0 has no exclusive/auto-delete lifecycle). `RedisStreamRouteEngine` implements it.

Wait — but `InMemoryEngine` is in `RedisMQ.Broker` (core, zero deps) and `RedisStreamRouteEngine` is in `RedisMQ.Redis`. Adding `ReconcileConnection` to `IRouteEngine` means `InMemoryEngine` implements it as no-op. That's fine.

Actually, let me reconsider. The `WireConnection` finally block calls `channel.TearDownActiveConsumers()` which calls `_engine.UnregisterConsumer`. For auto-delete, the engine already knows when consumer count hits zero. The issue is: on *connection* end (not just consumer cancel), we need to clean up exclusive queues owned by that connection.

`TearDownActiveConsumers` already unregisters all consumers. If `UnregisterConsumer` triggers auto-delete cleanup (when last consumer gone), that covers auto-delete. For exclusive, we need connection ownership tracking.

Let me add `void ReconcileConnection(string connectionId)` to `IRouteEngine`. `WireConnection` gets a `connectionId` (Guid) at construction. In `RunAsync` finally, after `TearDownActiveConsumers`, call `_engine.ReconcileConnection(_connectionId)`.

### 5.6 Incoming connection ID

`WireConnection` constructor: `WireConnection(BrokerServerOptions options, IRouteEngine routes, TransportSide transport, string? connectionId = null)`. Generate `Guid.NewGuid().ToString("N")` if not provided.

## 6. Files changed

| File | Change |
|------|--------|
| `src/RedisMQ.Broker/Protocol/MethodRegistry.cs` | Add `confirm.ack` (85,50), `confirm.nack` (85,51) schemas |
| `src/RedisMQ.Broker/Server/ChannelContext.cs` | Confirm mode + sequence + ack/nack handling in `OnRunning` switch |
| `src/RedisMQ.Broker/Server/WireConnection.cs` | `connectionId` field; `ReconcileConnection` call in finally |
| `src/RedisMQ.Broker/Memory/IRouteEngine.cs` | Add `ReconcileConnection(string connectionId)` |
| `src/RedisMQ.Broker/Memory/InMemoryEngine.cs` | Implement `ReconcileConnection` (no-op); add `DeleteQueue` internal |
| `src/RedisMQ.Broker/Memory/InMemoryMessage.cs` | Add `Expiration` (DateTimeOffset?) field |
| `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` | Multi-vhost ctor; eviction gate; reconcile; delete-queue drainer command; reaper |
| `src/RedisMQ.Redis/WriteCommand.cs` | Add `DeleteQueue` kind |
| `src/RedisMQ.Redis/RedisKeys.cs` | Add topology key with vhost: `{mq:top:<vhost>}` |
| `src/RedisMQ.Redis/Reaper.cs` | New: background TTL/expiry reaper loop |
| `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs` | Multi-vhost + replay + eviction gate + reconcile tests |
| `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs` | Multi-vhost conformance against Redis engine |

## 7. Test plan

- **Unit (RedisStreamRouteEngineTests):** multi-vhost declare/route isolation; replay restores all vhosts; eviction gate refuses `allkeys-lru`; `ReconcileConnection` deletes exclusive queue + auto-delete queue; reaper removes expired queue.
- **M0 regression:** Run M0 conformance (15 tests) + unit (233 tests) against `RedisStreamRouteEngine` multi-vhost — all green.
- **Confirms:** Add confirm.ack/nack flow tests to M0 regression or Broker.Tests (publish with confirm.select → receive confirm.ack).

## 8. Open items (left to builder)

- Exact confirm.ack/confirm.nack method IDs from `rabbitmq-codegen.json` fixture (verify during Task 1).
- Default reaper interval (suggest 30s).
- Eviction gate: refuse vs warn default (suggest refuse).
- Confirm tracking persistence (M2 acks in-memory only; durable confirms await Redis drainer sync — noted as M3 improvement).
- `InMemoryMessage.Expiration` serialization in `StreamCodec` (add `expiration` field).

## 9. Approval gate

Nothing is built until you approve this design doc. Once approved, the next step is `superpowers:writing-plans` to produce the M2 task plan (commit-by-commit), then implementation begins.

Do you approve this design? If so, any changes to the open items in §8 before I write the plan?
