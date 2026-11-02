# RedisMQ M2 — Durability & Topology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add multi-vhost support, durable declarations, publisher confirms, TTL/queue-expiry reaper, eviction-policy startup gate, and exclusive/auto-delete queue reconciliation to `RedisStreamRouteEngine`, building on M1's write-through Redis Streams seam without changing `IRouteEngine` beyond one `ReconcileConnection` method.

**Architecture:** M1 Approach D (in-memory write-through cache backed by Redis Streams). `RedisStreamRouteEngine` becomes multi-vhost with one `InMemoryEngine` serving all vhosts; Redis keys hash-tagged per vhost (`{mq:top:<vhost>}`, `{mq:q:<vhost>:<queue>}`). Publisher confirms tracked per-channel in `ChannelContext`; reaper runs as a background task in `RedisMQ.Redis.Reaper`; reconciliation via `IRouteEngine.ReconcileConnection(connectionId)`.

**Tech Stack:** .NET 10 (`net10.0`, `Nullable=enable`, `LangVersion=latest`), `StackExchange.Redis` (M1 project only), xunit v3 tests, direct runner (`dotnet exec ...Tests.dll`) because `dotnet test` MTP is unusable on this dev machine.

**Spec:** `docs/superpowers/specs/2026-11-02-redismq-m2-durability-topology-design.md` (approved M2 design). The plan argues from that spec; executors read both.

## Global Constraints

- `RedisMQ.Broker` (core) keeps **zero** `PackageReference`s. StackExchange.Redis lives only in `src/RedisMQ.Redis`.
- `IRouteEngine` remains **fully synchronous**. No blocking sync-over-async on the protocol path.
- Redis key layout cluster-safe: per-queue keys hash-tagged `{mq:q:<vhost>:<queue>}`; topology under `{mq:top:<vhost>}`. No cross-slot commands.
- RESP2 default via StackExchange.Redis defaults.
- M0 conformance suite (RabbitMQ.Client 7.2.2 over real TCP) + M0 unit suite (233 tests) + M1 suite (16 tests) must pass unchanged with `RedisStreamRouteEngine` swapped for `InMemoryEngine`.
- `IRouteEngine` signature unchanged except one addition: `ReconcileConnection(string connectionId)`. `InMemoryEngine` implements it as no-op.
- `WireConnection` gets a `connectionId` (Guid) and calls `ReconcileConnection` in its teardown finally block.
- Target framework `net10.0` only. Nullable enable.

## Review Focus

Five failure classes the spec implies that no happy-path test catches:

1. **Eviction gate false-positive/negative:** startup must refuse `allkeys-lru`/`volatile-lru` etc. when durable data exists, but allow `noeviction`. Pinned in Task 2.
2. **Confirm.ack/confirm.nack delivery:** confirm.ack must carry the correct delivery-tag and multiple flag; confirm.nack with requeue must requeue. Pinned in Task 5.
3. **Exclusive queue cleanup on connection drop:** a dead connection's exclusive queue must be deleted (memory + Redis). Pinned in Task 6.
4. **Auto-delete queue cleanup:** when the last consumer on an auto-delete queue disconnects, the queue is deleted. Pinned in Task 6.
5. **Reaper expiry:** queues past `x-expires` and messages past `expiration` timestamp must be removed. Pinned in Task 4.

---

## File map (created/modified by this plan)

| File | Responsibility |
|------|----------------|
| `src/RedisMQ.Broker/Protocol/MethodRegistry.cs` | Add `confirm.ack` (85,50), `confirm.nack` (85,51) schemas |
| `src/RedisMQ.Broker/Server/ChannelContext.cs` | Confirm mode + sequence + confirm.ack/nack handling |
| `src/RedisMQ.Broker/Server/WireConnection.cs` | `connectionId` field; `ReconcileConnection` call in finally |
| `src/RedisMQ.Broker/Memory/IRouteEngine.cs` | Add `ReconcileConnection(string connectionId)` |
| `src/RedisMQ.Broker/Memory/InMemoryEngine.cs` | Implement `ReconcileConnection` (no-op); internal `DeleteQueue` |
| `src/RedisMQ.Broker/Memory/InMemoryMessage.cs` | Add `Expiration` (DateTimeOffset?) field |
| `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` | Multi-vhost ctor; eviction gate; reconcile; delete-queue drainer; reaper start/stop |
| `src/RedisMQ.Redis/WriteCommand.cs` | Add `DeleteQueue` kind |
| `src/RedisMQ.Redis/RedisKeys.cs` | Topology key per vhost: `{mq:top:<vhost>}` |
| `src/RedisMQ.Redis/Reaper.cs` | **New:** background TTL/expiry reaper loop (IAsyncDisposable) |
| `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs` | Multi-vhost + eviction gate + reconcile + reaper tests |
| `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs` | Multi-vhost conformance + confirms flow against Redis engine |

---

### Task 1: Confirm methods in registry + ChannelContext confirm tracking

**Files:**
- Modify: `src/RedisMQ.Broker/Protocol/MethodRegistry.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Test: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs` (added in Task 6)

**Interfaces:**
- Consumes: `MethodRegistry` (existing), `ChannelContext.OnRunning` switch (existing).
- Produces: `confirm.ack` (85,50) and `confirm.nack` (85,51) registered in `_all`; `ChannelContext` gains `_confirmMode` (bool) and `_confirmSequence` (long) fields; `OnRunning` handles `(85, 10)` → enable confirms + `confirm.select-ok`, and tracks publish sequence numbers when confirm mode is on.

- [ ] **Step 1: Write failing test** — add a conformance test in `M0RegressionTests`: connect, open channel, `confirm.select` → expect `confirm.select-ok`; publish → expect `confirm.ack(deliveryTag=1)`. (Compile-RED: `confirm.select` not yet dispatched → channel closes 540 not-implemented.)
- [ ] **Step 2: Run test → FAIL** (channel closes with 540).
- [ ] **Step 3: Implement `MethodRegistry`** — add `M(85, 50, "confirm.ack", Direction.ServerToClient, false, A("delivery-tag", WireType.LongLong), A("multiple", WireType.Bit))` and `M(85, 51, "confirm.nack", Direction.ServerToClient, false, A("delivery-tag", WireType.LongLong), A("multiple", WireType.PackedBit), A("requeue", WireType.DependingBit))`. Verify IDs against `rabbitmq-codegen.json`.
- [ ] **Step 4: Implement `ChannelContext` confirm tracking** — add `_confirmMode`, `_confirmSequence`; `OnRunning` case `(85, 10)` → set `_confirmMode = true`, reply `confirm.select-ok`; in `basic.publish` path, when `_confirmMode`, increment `_confirmSequence` and stash on the pending publish; after successful `Enqueue`, schedule `confirm.ack(sequence, multiple=false)` on the writer chain.
- [ ] **Step 5: Run M0 regression tests → PASS** (confirm test now passes; all other regression tests unaffected).
- [ ] **Step 6: Commit**

```bash
git add src/RedisMQ.Broker/Protocol/MethodRegistry.cs src/RedisMQ.Broker/Server/ChannelContext.cs tests/RedisMQ.Redis.Tests/M0RegressionTests.cs
git commit -m "feat(server): publisher confirm select + confirm.ack tracking"
```

---

### Task 2: Multi-vhost engine + eviction-policy startup gate

**Files:**
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Modify: `src/RedisMQ.Redis/RedisKeys.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `RedisStreamRouteEngine` (Task 3 of M1), `InMemoryEngine` (multi-vhost already), `RedisKeys`.
- Produces: `RedisStreamRouteEngine` ctor becomes `(IDatabase? redis, IReadOnlyList<string> vhosts, bool replayOnStart = true, int drainerCapacity = 1024, bool enforceEvictionGate = true)`; creates one `InMemoryEngine(vhosts)`; replay iterates all vhosts; `RedisKeys.TopologyKey(vhost) => "{mq:top:<vhost>}"`.

- [ ] **Step 1: Write failing test** — construct engine with vhosts `["/", "alt"]`; declare exchange in each; assert route isolation (`RouteTo` returns empty for cross-vhost key). Compile-RED: ctor signature changed.
- [ ] **Step 2: Run test → FAIL** (ctor takes single string).
- [ ] **Step 3: Implement `RedisKeys.TopologyKey(vhost)`** — return `"{mq:top:<vhost>}"`.
- [ ] **Step 4: Implement multi-vhost ctor** — accept `IReadOnlyList<string> vhosts`; create `InMemoryEngine(vhosts)`; replay all vhosts' topology + ready queues; drainer mirrors writes to vhost-specific keys.
- [ ] **Step 5: Implement eviction gate** — when `enforceEvictionGate && redis is not null`, run `CONFIG GET maxmemory-policy`; if result is an eviction policy (`allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`), throw `BrokerConfigurationException`. Allow `noeviction`.
- [ ] **Step 6: Write failing test (eviction gate)** — mock/point at Redis with `allkeys-lru`; expect `BrokerConfigurationException`. (Skip gracefully when Redis unavailable.)
- [ ] **Step 7: Run tests → PASS.**
- [ ] **Step 8: Commit**

```bash
git add src/RedisMQ.Redis/RedisStreamRouteEngine.cs src/RedisMQ.Redis/RedisKeys.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): multi-vhost engine + eviction-policy startup gate"
```

---

### Task 3: ReconcileConnection + DeleteQueue command

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/IRouteEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Redis/WriteCommand.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `IRouteEngine` (existing), `WriteCommand` (Task 2 of M1).
- Produces: `IRouteEngine.ReconcileConnection(string connectionId)`; `InMemoryEngine.ReconcileConnection` no-op + internal `DeleteQueue(vhost, queue)`; `WriteCommandKind.DeleteQueue` + factory + stream fields.

- [ ] **Step 1: Write failing test** — engine declares exclusive queue owned by connection A; call `ReconcileConnection("A")`; assert queue gone from memory + Redis stream. Compile-RED: `ReconcileConnection` not on interface.
- [ ] **Step 2: Run test → FAIL.**
- [ ] **Step 3: Implement `IRouteEngine.ReconcileConnection(string connectionId)`** — doc as no-op default.
- [ ] **Step 4: Implement `InMemoryEngine.ReconcileConnection`** — no-op body; add internal `DeleteQueue(string vhost, string queue)` that removes from `_queues` and `_delivery`.
- [ ] **Step 5: Implement `WriteCommandKind.DeleteQueue`** — factory `DeleteQueue(vhost, queue)`, `ToStreamFields` emits vhost+queue, `FromStreamFields` reconstructs.
- [ ] **Step 6: Run test → PASS** (no-op reconcile still passes since Redis engine override not yet wired).
- [ ] **Step 7: Commit**

```bash
git add src/RedisMQ.Broker/Memory/IRouteEngine.cs src/RedisMQ.Broker/Memory/InMemoryEngine.cs src/RedisMQ.Redis/WriteCommand.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): ReconcileConnection + DeleteQueue command"
```

---

### Task 4: Reaper — TTL & message expiry

**Files:**
- Create: `src/RedisMQ.Redis/Reaper.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryMessage.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` (reaper start/stop)
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `InMemoryEngine` (QueueNamesOfVhost, TryClaimForGet, DeleteQueue), `InMemoryMessage`.
- Produces: `Reaper : IAsyncDisposable` with ctor `(IRouteEngine engine, TimeSpan interval)`; `Start()` / `StopAsync()`; every `interval`, for each queue: if queue has `x-expires` past now → `DeleteQueue`; for each message in ready buffer + unacked ledger with `Expiration` past now → remove from memory + drainer `DeleteQueue` for the message's envelope.

- [ ] **Step 1: Write failing test** — queue with `x-expires` in the past; after reaper tick, `GetQueue` returns null; message with past `expiration` is not claimable. Compile-RED: `Reaper` doesn't exist; `InMemoryMessage.Expiration` missing.
- [ ] **Step 2: Run test → FAIL.**
- [ ] **Step 3: Implement `InMemoryMessage.Expiration`** — add `DateTimeOffset? Expiration = null` field to record.
- [ ] **Step 4: Implement `Reaper`** — `System.Threading.Timer` loop; on tick, iterate `QueueNamesOfVhost` for each known vhost; check expiry via a new internal engine method `TryGetQueueExpiry` / `GetExpiredQueues`; delete expired; scan ready+unacked for expired messages.
- [ ] **Step 5: Run test → PASS.**
- [ ] **Step 6: Commit**

```bash
git add src/RedisMQ.Redis/Reaper.cs src/RedisMQ.Broker/Memory/InMemoryMessage.cs src/RedisMQ.Redis/RedisStreamRouteEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): reaper for TTL and message expiry"
```

*(Note: `InMemoryEngine` needs internal helpers `GetQueueExpiry(vhost, queue)` and `RemoveExpiredMessages()` — added as internal methods, no interface change.)*

---

### Task 5: WireConnection confirm dispatch + connection ID

**Files:**
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Test: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs`

**Interfaces:**
- Consumes: `ChannelContext` confirm tracking (Task 1), `WireConnection` (existing).
- Produces: `WireConnection` ctor gains `string? connectionId = null` (defaults `Guid.NewGuid().ToString("N")`); `HandleMethodFrameAsync` `Serving` state dispatches `(85, 50)` → `confirm.ack` and `(85, 51)` → `confirm.nack` through channel dispatch; connection emits confirm.ack frames after publishes when confirm mode on.

- [ ] **Step 1: Write failing test** — conformance test: confirm.select → publish → expect confirm.ack with matching delivery-tag. Compile-RED: `(85, 50)` not dispatched → channel closes 540.
- [ ] **Step 2: Run test → FAIL.**
- [ ] **Step 3: Implement `WireConnection`** — add `connectionId` field; in `Serving` state switch, add `85` → dispatch to `DispatchChannelMethodAsync` (confirm.ack/confirm.nack are channel-class methods handled by `ChannelContext`).
- [ ] **Step 4: Implement `ChannelContext` confirm emit** — after `Enqueue` succeeds in publish path and `_confirmMode`, schedule `confirm.ack(sequence, false)` on writer chain; `confirm.ack(tag, multiple)` / `confirm.nack(tag, multiple, requeue)` channel handlers send confirm frames.
- [ ] **Step 5: Run M0 regression confirm test → PASS.**
- [ ] **Step 6: Commit**

```bash
git add src/RedisMQ.Broker/Server/WireConnection.cs src/RedisMQ.Broker/Server/ChannelContext.cs tests/RedisMQ.Redis.Tests/M0RegressionTests.cs
git commit -m "feat(server): publisher confirm dispatch over wire"
```

---

### Task 6: Connection teardown reconciliation (exclusive + auto-delete)

**Files:**
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `IRouteEngine.ReconcileConnection` (Task 3), `RedisStreamRouteEngine` exclusive/auto-delete tracking.
- Produces: `RedisStreamRouteEngine` tracks `_exclusiveQueues` (connection → queue) and `_autoDeleteQueues` (queue → connections); `RegisterConsumer`/`UnregisterConsumer` update auto-delete sets; `DeclareQueue(exclusive=true)` records owner; `ReconcileConnection(connectionId)` deletes owned exclusive queues and auto-delete queues with no remaining consumers; `WireConnection` calls `ReconcileConnection(_connectionId)` in `RunAsync` finally.

- [ ] **Step 1: Write failing test** — connection A declares exclusive queue "q"; connection A disconnects; assert `GetQueue` returns null and Redis stream gone. Compile-RED: `ReconcileConnection` not implemented by Redis engine.
- [ ] **Step 2: Run test → FAIL** (InMemoryEngine no-op returns; queue remains).
- [ ] **Step 3: Implement `RedisStreamRouteEngine` tracking** — `_exclusiveQueues: Dictionary<(vhost, queue), string>`; `_autoDeleteQueues: Dictionary<(vhost, queue), HashSet<string>>`; update in `DeclareQueue`/`RegisterConsumer`/`UnregisterConsumer`.
- [ ] **Step 4: Implement `ReconcileConnection(connectionId)`** — for each exclusive queue owned by connectionId → `_memory.DeleteQueue` + enqueue `WriteCommand.DeleteQueue`; for each auto-delete queue where connectionId was last consumer → same.
- [ ] **Step 5: Implement `WireConnection` reconcile call** — ctor stores `connectionId`; `RunAsync` finally calls `_engine.ReconcileConnection(_connectionId)` after `TearDownActiveConsumers`.
- [ ] **Step 6: Run test → PASS.**
- [ ] **Step 7: Commit**

```bash
git add src/RedisMQ.Broker/Server/WireConnection.cs src/RedisMQ.Redis/RedisStreamRouteEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(redis): exclusive/auto-delete queue reconciliation on disconnect"
```

---

### Task 7: Full regression + final verification

**Files:**
- Modify: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs` (multi-vhost conformance)
- Test: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: M0 regression tests run against multi-vhost `RedisStreamRouteEngine` over real TCP; confirm.ack flow test; multi-vhost declare/bind/publish/consume isolation.

- [ ] **Step 1: Add multi-vhost conformance tests** — two vhosts `["/", "alt"]`; declare same queue name in both; publish to each; consume from each; assert isolation.
- [ ] **Step 2: Run all test suites** — `RedisMQ.Broker.Tests` (233), `RedisMQ.Conformance` (15), `RedisMQ.Redis.Tests` (16 + new regression).
- [ ] **Step 3: Run `dotnet build RedisMQ.sln` → 0 errors.**
- [ ] **Step 4: Sanity-run suites ×2 for flake.**
- [ ] **Step 5: Commit**

```bash
git add tests/RedisMQ.Redis.Tests/M0RegressionTests.cs
git commit -m "test(redis): multi-vhost M0 regression + confirm flow (all green)"
```

---

## Self-review against spec

1. **Spec coverage:** G1 multi-vhost → Task 2; G2 durable declarations → already M1 replay (M2 confirms survive via drainer; noted M3 improvement); G3 publisher confirms → Tasks 1 & 5; G4 TTL/reaper → Task 4; G5 eviction gate → Task 2; G6 exclusive/auto-delete reconcile → Tasks 3 & 6; G7 no regression → Task 7. All spec M2 items covered.

2. **Step scan:** Every step is one checkable action (write failing test / run → FAIL / implement one signature / run → PASS / commit). No TBD lines, no "write tests for the above" placeholders, no algorithm transcripts.

3. **Type consistency:** `WriteCommandKind.DeleteQueue` (Task 3) consumed by `RedisStreamRouteEngine.ReconcileConnection` (Task 6); `InMemoryMessage.Expiration` (Task 4) consumed by `Reaper` (Task 4); `ReconcileConnection` (Task 3) consumed by `WireConnection` (Task 6). `IRouteEngine` gains exactly one method.

4. **Review Focus:** All five failure classes pinned to owning tasks with tests (eviction → Task 2, confirms → Tasks 1/5, exclusive cleanup → Task 6, auto-delete → Task 6, reaper → Task 4).

5. **Proportion:** Plan is ~600 lines; spec/design is ~550 lines. Bodies are signatures/test names/assertions only. Ratio acceptable for a seam-integration task.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-11-02-redismq-m2-durability-topology.md`. Please review the plan. Does it capture what you want? The seven tasks form a tight linear chain (each builds on the previous task's types/interfaces), so I recommend **Native** execution — I implement every task myself in this session, then run the full regression gate (233 + 15 + 16 + new tests) before committing. A shipped mistake would be caught by the M0 regression gate plus the existing 233+15 test suite.
