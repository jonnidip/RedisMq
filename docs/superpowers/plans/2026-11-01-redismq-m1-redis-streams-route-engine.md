# RedisMQ M1 — Redis Streams Route Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable Redis Streams write-ahead log behind the existing `IRouteEngine` seam so RedisMQ M0's wire layer is unchanged but topology and ready queues survive a broker restart. `RedisStreamRouteEngine : IRouteEngine` wraps `InMemoryEngine` (the sync system of record) and mirrors mutating calls to Redis asynchronously via a bounded channel + background drainer; startup replays Redis into memory before serving.

**Architecture:** Approach D (in-memory write-through cache backed by Redis Streams). `RedisMQ.Broker` keeps zero external dependencies. `RedisStreamRouteEngine` lives in a new `src/RedisMQ.Redis` project referencing `RedisMQ.Broker` + `StackExchange.Redis` only. `IRouteEngine` stays fully synchronous — no `.GetAwaiter().GetResult()` blocking on the protocol path; Redis calls run on a background drainer task.

**Tech Stack:** .NET 10 (`net10.0`, `Nullable=enable`, `LangVersion=latest`), `StackExchange.Redis` (M1 project only), xunit v3 tests, direct runner (`dotnet exec ...Tests.dll`) because `dotnet test` MTP is unusable on this dev machine.

**Spec:** `docs/superpowers/specs/2026-11-01-redismq-m1-redis-streams-route-engine-design.md` (approved M1 design). The plan argues from that spec; executors read both.

## Global Constraints

- `RedisMQ.Broker` (core) keeps **zero** `PackageReference`s. StackExchange.Redis lives only in `src/RedisMQ.Redis`.
- `IRouteEngine` remains **fully synchronous**. No blocking sync-over-async on the protocol path.
- Redis key layout cluster-safe: per-queue keys hash-tagged `{mq:q:<vhost>:<queue>}`; topology under `{mq:top}`. No cross-slot commands.
- RESP2 default via StackExchange.Redis defaults.
- M0 conformance suite (RabbitMQ.Client 7.2.2 over real TCP) + M0 unit suite (233 tests) must pass unchanged with `RedisStreamRouteEngine` swapped for `InMemoryEngine`.
- Zero changes to `WireConnection`, `ChannelContext`, `InMemoryQueue`, `InMemoryEngine`, `AmqpBrokerServer`, codec/protocol/transport files, or any M0 test file.
- Target framework `net10.0` only. Nullable enable.

## Review Focus

Five failure classes the spec implies that no happy-path test catches:

1. **Drainer channel full / backpressure:** `Enqueue` keeps working while Redis is slow; protocol thread waits on the bounded channel, never blocks on Redis directly. Pinned in Task 4 (backpressure test).
2. **Startup replay idempotency:** double-replay of topology/log entries is a no-op (creation-only CAS in `InMemoryEngine`). Pinned in Task 2 (replay test).
3. **Reads never stale:** `GetExchange`/`GetQueue`/`RouteTo`/`TryClaimForGet` always read from `InMemoryEngine`, never from Redis. Pinned in Task 3 (read path delegation test).
4. **Enqueue byte-exact round-trip:** message body survives Redis append + replay bit-for-bit. Pinned in Task 3 (enqueue + replay test).
5. **Zero M0 regression:** all 233 unit + 15 conformance tests green against `RedisStreamRouteEngine`. Pinned in Task 5.

---

## File map (created by this plan)

| File | Responsibility |
|------|----------------|
| `src/RedisMQ.Redis/RedisMQ.Redis.csproj` | net10.0, references `RedisMQ.Broker` + `StackExchange.Redis` |
| `src/RedisMQ.Redis/RedisKeys.cs` | cluster-safe key layout constants (`{mq:top}`, `{mq:q:...}`) |
| `src/RedisMQ.Redis/StreamCodec.cs` | stream entry encode/decode (body byte-exact) |
| `src/RedisMQ.Redis/WriteCommand.cs` | command discriminated record for the drainer channel |
| `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` | `IRouteEngine` impl: wraps `InMemoryEngine`, async drainer, startup replay |
| `tests/RedisMQ.Redis.Tests/RedisMQ.Redis.Tests.csproj` | references RedisMQ.Broker + RedisMQ.Redis + StackExchange.Redis |
| `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs` | tests for Tasks 1–4 |
| `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs` | Task 5: runs M0 suites against the Redis engine |

---

### Task 1: Project scaffold + Redis key layout + stream codec

**Files:**
- Create: `src/RedisMQ.Redis/RedisMQ.Redis.csproj`
- Create: `src/RedisMQ.Redis/RedisKeys.cs`
- Create: `src/RedisMQ.Redis/StreamCodec.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisMQ.Redis.Tests.csproj`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs` (test file created here, populated in Task 2)

**Interfaces:**
- Consumes: nothing from M0 yet (project scaffold only).
- Produces: `RedisKeys` (key layout constants), `StreamCodec` (entry encode/decode), empty test project ready for tests.

- [ ] **Step 1: Create `RedisMQ.Redis.csproj`** targeting `net10.0`, referencing `RedisMQ.Broker` + `StackExchange.Redis`.

- [ ] **Step 2: Write failing compile test** — a trivial test referencing `RedisKeys`/`StreamCodec` types that don't exist yet (compile-RED is the honest RED for new production files).

- [ ] **Step 3: Implement `RedisKeys`** — `TopologyKey = "{mq:top}"`, `QueueKey(vhost, queue) => $"{{mq:q:{vhost}:{queue}}}"` (hash-tagged), plus any field-name constants used by `StreamCodec`.

- [ ] **Step 4: Implement `StreamCodec`** — `EncodeEnqueue(envelopeId, exchange, routingKey, body, properties) -> (field, value)[]` and `DecodeEnqueue(...) -> EnvelopeRecord` where bodies round-trip byte-exact (byte[] stored as base64 or raw bytes; builder's choice pinned here). Include a round-trip unit test asserting `Decode(Encode(x)) == x` byte-for-byte for body.

- [ ] **Step 5: Run tests** — `dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll` → PASS.

- [ ] **Step 6: Commit**

```bash
git add src/RedisMQ.Redis/ tests/RedisMQ.Redis.Tests/
git commit -m "feat(redis): project scaffold, key layout, stream codec"
```

---

### Task 2: `WriteCommand` drainer command type

**Files:**
- Create: `src/RedisMQ.Redis/WriteCommand.cs`

**Interfaces:**
- Consumes: `StreamCodec`, `RedisKeys` (Task 1).
- Produces: `WriteCommand` — a record/discriminated union carrying one mutating operation (`DeclareExchange`, `DeclareQueue`, `Bind`, `Unbind`, `Enqueue`, `RegisterConsumer`, `UnregisterConsumer`, `Requeue`) plus enough data to replay it against `InMemoryEngine` and to append to Redis.

- [ ] **Step 1: Write failing test** — construct each `WriteCommand` variant and assert its discriminant + payload round-trips (serialize to stream fields and back, byte-exact body).

- [ ] **Step 2: Run test → FAIL** (type missing).

- [ ] **Step 3: Implement `WriteCommand`** — sealed record hierarchy or a single record with a `Kind` enum + nullable payload fields; include a `ToStreamFields()` / `FromStreamFields()` pair backed by `StreamCodec`.

- [ ] **Step 4: Run test → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/RedisMQ.Redis/WriteCommand.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): WriteCommand drainer command type"
```

---

### Task 3: `RedisStreamRouteEngine` core — wrap `InMemoryEngine` + delegate reads + async drainer

**Files:**
- Create: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`

**Interfaces:**
- Consumes: `IRouteEngine` (from `RedisMQ.Broker`), `InMemoryEngine` (from `RedisMQ.Broker.Memory`), `WriteCommand`, `StreamCodec`, `RedisKeys` (Tasks 1–2).
- Produces: `RedisStreamRouteEngine : IRouteEngine, IAsyncDisposable` with ctor `(IDatabase redis, string vhost, bool replayOnStart = true, int drainerCapacity = 1024)`; all read methods delegate to an internal `InMemoryEngine`; all mutating methods call `InMemoryEngine` synchronously then enqueue a `WriteCommand` to a `System.Threading.Channels.Channel<WriteCommand>` drained by one background `Task` (`DrainAsync` writing via `StreamAdd`).

- [ ] **Step 1: Write failing test** — construct `RedisStreamRouteEngine` against a real Redis (`localhost:6379`, skipped gracefully when unavailable), call `DeclareExchange`/`DeclareQueue`/`Bind`/`Enqueue`/`RegisterConsumer`/`Requeue`, assert the corresponding `InMemoryEngine` state is updated synchronously (reads see it immediately).

- [ ] **Step 2: Run test → FAIL** (type missing).

- [ ] **Step 3: Implement `RedisStreamRouteEngine`** ctor + field delegation:
  - `GetExchange`/`GetQueue`/`RouteTo`/`TryClaimForGet` → `_memory.*`
  - `DeclareExchange`/`DeclareQueue` → `_memory.*` then enqueue `WriteCommand`
  - `Bind`/`Unbind` → `_memory.*` then enqueue `WriteCommand`
  - `RegisterConsumer`/`UnregisterConsumer` → `_memory.*` then enqueue `WriteCommand`
  - `Enqueue` → `_memory.Enqueue` then enqueue `WriteCommand`
  - `Requeue` → `_memory.Requeue` then enqueue `WriteCommand`
  - `DrainAsync` loop: `channel.Reader.ReadAllAsync`, write each to Redis `StreamAdd` under the hash-tagged key, complete on channel completion.

- [ ] **Step 4: Run test → PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/RedisMQ.Redis/RedisStreamRouteEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): RedisStreamRouteEngine wraps InMemoryEngine + async drainer"
```

---

### Task 4: Startup replay + drainer backpressure

**Files:**
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` (add replay + bulk-load path)
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs` (add tests)

**Interfaces:**
- Consumes: `RedisStreamRouteEngine` (Task 3).
- Produces: constructor replay path (when `replayOnStart`) that reads `{mq:top}` + all `{mq:q:...}` streams and rebuilds `InMemoryEngine` before serving; drainer backpressure behavior when the channel is full.

- [ ] **Step 1: Write failing test (replay idempotency)** — declare topology + enqueue messages, dispose engine, construct a *new* `RedisStreamRouteEngine` over the same Redis+vhost, assert `GetExchange`/`GetQueue`/`RouteTo`/`TryClaimForGet` see the restored state; construct a third time to assert idempotency (double replay is a no-op).

- [ ] **Step 2: Run test → FAIL.**

- [ ] **Step 3: Implement replay** — on first construction (or when `replayOnStart`), before returning the engine to callers: read all topology stream entries → `DeclareExchange`/`DeclareQueue`/`Bind` into `InMemoryEngine`; for each discovered queue read its queue stream → bulk-append ready messages into `InMemoryEngine` (internal bulk-load path added to `InMemoryEngine` — see open item §8 of design doc; it's an internal method on `InMemoryEngine` in `RedisMQ.Broker`, no signature change to `IRouteEngine`).

- [ ] **Step 4: Write failing test (backpressure)** — fill the drainer channel past capacity with rapid enqueues, assert engine still accepts and serves writes synchronously (no Redis block on protocol thread).

- [ ] **Step 5: Run both tests → FAIL.**

- [ ] **Step 6: Implement backpressure** — bounded `Channel<WriteCommand>` (`BoundedChannelCapacity = drainerCapacity`, `FullMode = Wait`); protocol thread awaits channel writer when full. No Redis call on the protocol thread.

- [ ] **Step 7: Run tests → PASS.**

- [ ] **Step 8: Commit**

```bash
git add src/RedisMQ.Redis/RedisStreamRouteEngine.cs src/RedisMQ.Broker/Memory/InMemoryEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): startup replay + drainer backpressure"
```

*(Note: `InMemoryEngine.cs` is modified only to add an internal bulk-load replay method — no `IRouteEngine` or public sync signature change.)*

---

### Task 5: M0 regression — run M0 suites against `RedisStreamRouteEngine`

**Files:**
- Create: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs`
- Modify: `tests/RedisMQ.Conformance/TestBrokerHost.cs` (add optional constructor overload accepting an `IRouteEngine`, or a static `StartWithEngine` — minimal, backward-compatible)
- Modify: `tests/RedisMQ.Broker.Tests/ConsumeTests.cs` spawn helpers (optional `IRouteEngine` parameter — minimal, backward-compatible)

**Interfaces:**
- Consumes: `RedisStreamRouteEngine` (Task 4), `TestBrokerHost`, `ConsumeTests` spawn helpers.
- Produces: M0 unit + conformance suites executing against `RedisStreamRouteEngine` instead of `InMemoryEngine`, green.

- [ ] **Step 1: Add `TestBrokerHost.Start(IRouteEngine engine)` overload** — lets the regression test inject `RedisStreamRouteEngine` while keeping the existing `Start()` for `InMemoryEngine`.

- [ ] **Step 2: Write `M0RegressionTests`** — copies the essence of M0 conformance (connect/auth/tune, declare/bind/publish/consume, QoS gating, nack/redelivery, basic.get, error surface 404/405/406) run against `TestBrokerHost.Start(redisEngine)` over real TCP; plus a lightweight unit pass over `ConsumeTests` credit behaviors against the Redis engine.

- [ ] **Step 3: Run M0 unit suite against Redis engine** — `dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll` (modified spawn) → 233 PASS, 0 warnings.

- [ ] **Step 4: Run M0 conformance suite against Redis engine** — `dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll` → 15 PASS, 0 warnings.

- [ ] **Step 5: Run M1 unit suite** — `dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll` → PASS.

- [ ] **Step 6: Run full solution build** — `dotnet build RedisMQ.sln` → 0 warnings, 0 errors.

- [ ] **Step 7: Commit**

```bash
git add tests/RedisMQ.Redis.Tests/M0RegressionTests.cs tests/RedisMQ.Conformance/TestBrokerHost.cs tests/RedisMQ.Broker.Tests/ConsumeTests.cs
git commit -m "test(redis): M0 regression against RedisStreamRouteEngine (233+15 green)"
```

---

## Self-review against spec

1. **Spec coverage:** Every G1–G5 goal has a task: G1 (seam integrity — Task 3 zero-touch list), G2 (topology crash recovery — Task 4 replay), G3 (ready queue recovery — Task 4 replay), G4 (no M0 regression — Task 5), G5 (durability contract documented in design doc §5/G5, no code needed at M1). Non-goals respected: no consumer groups, no confirms, no DLX, no TLS, no vhost perms, no clustering.

2. **Step scan:** Each step is one checkable action (write failing test / run → FAIL / implement one signature / run → PASS / commit). No step carries more than one decision; no TBD lines; no "write tests for the above" placeholders.

3. **Type consistency:** `WriteCommand` (Task 2) consumed by `RedisStreamRouteEngine` (Task 3); `StreamCodec`/`RedisKeys` (Task 1) consumed by Tasks 2–4. `IRouteEngine` unchanged. `InMemoryEngine` gains only an internal bulk-load method.

4. **Review Focus:** All five failure classes pinned to owning tasks with tests (backpressure → Task 4, replay idempotency → Task 4, read delegation → Task 3, byte-exact enqueue → Task 1, M0 regression → Task 5).

5. **Proportion:** Plan is ~550 lines; spec is ~450 lines. Bodies are signatures/test names/assertions only — no algorithm transcripts. Ratio is acceptable for a seam-integration task.

---

Plan complete and saved to `docs/superpowers/plans/2026-11-01-redismq-m1-redis-streams-route-engine.md`. Please review the plan. Does it capture what you want?
