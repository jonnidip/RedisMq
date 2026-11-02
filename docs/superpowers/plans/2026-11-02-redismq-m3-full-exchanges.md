# RedisMQ M3 — Full Exchanges + Extras Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add topic exchange, headers exchange, `x-max-length` per-queue length limiting, content-type passthrough, per-channel bookkeeping hardening, and per-channel basic flow-control to RedisMQ, building on M2's durable multi-vhost engine.

**Architecture:** Approach A (engine-side `RouteTo` dispatch). Exchange-type dispatch stays in `ChannelContext.OnExchangeDeclare`; `IRouteEngine.RouteTo(exchange, properties, routingKey)` dispatches based on exchange type: direct (exact match, existing), topic (word-trie pattern match with `*`/`#` wildcards), headers (match message headers against binding arguments using `x-match: all|any`). No interface changes beyond storing exchange type on `ExchangeInfo`.

**Tech Stack:** .NET 10 (`net10.0`, `Nullable=enable`, `LangVersion=latest`), `StackExchange.Redis` (M1 project only), xunit v3 tests, direct runner (`dotnet exec ...Tests.dll`) because `dotnet test` MTP is unusable on this dev machine.

**Spec:** `docs/superpowers/specs/2026-11-02-redismq-m2-durability-topology-design.md` (M2 design) and `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md` (original M0 spec). The plan argues from these specs; executors read both.

## Global Constraints

- `RedisMQ.Broker` (core) keeps **zero** `PackageReference`s. StackExchange.Redis lives only in `src/RedisMQ.Redis`.
- `IRouteEngine` remains **fully synchronous**. No new public interface methods.
- Redis key layout cluster-safe; no cross-slot commands.
- M0/M1/M2 regression: all 233 + 15 + 22 tests pass unchanged with `RedisStreamRouteEngine`.
- Topic `#` matches zero or more words; `*` matches exactly one word (per your decision).
- Target framework `net10.0` only. Nullable enable.

## Review Focus

Five failure classes the spec implies that no happy-path test catches:

1. **Topic `#` matches zero words:** `a.#` should match `a` (zero words after `#`), not just `a.b`. Pinned in Task 1.
2. **Topic `*` matches exactly one word:** `a.*` matches `a.b` but not `a` or `a.b.c`. Pinned in Task 1.
3. **Headers `x-match: all` requires all headers present and matching:** a missing header key means no match. Pinned in Task 2.
4. **Headers `x-match: any` matches if any header matches:** a single matching header is sufficient. Pinned in Task 2.
5. **`x-max-length` drop-oldest:** when a queue exceeds max length, the oldest message is dropped to make room. Pinned in Task 3.

---

## File map (created/modified by this plan)

| File | Responsibility |
|------|----------------|
| `src/RedisMQ.Broker/Memory/InMemoryEngine.cs` | Topic trie + headers matching in `RouteTo`; `x-max-length` stored on `QueueInfo`; `Enqueue` respects max-length drop-oldest |
| `src/RedisMQ.Broker/Memory/InMemoryQueue.cs` | `MaxLength` field; `RemoveOldestMessages` internal method for `x-max-length` enforcement |
| `src/RedisMQ.Broker/Memory/QueueInfo.cs` | Add `MaxLength` (int?) field |
| `src/RedisMQ.Broker/Server/ChannelContext.cs` | Per-channel flow-control state; `channel.flow` toggle per channel (pause/resume delivery, not hard close) |
| `src/RedisMQ.Broker/Server/WireConnection.cs` | `channel.flow` dispatch per-channel instead of hard close |
| `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs` | Topic/headers/max-length/flow-control tests |
| `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs` | Topic/headers/max-length conformance against Redis engine |

---

### Task 1: Topic exchange routing (`*` and `#` wildcards)

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `IRouteEngine.RouteTo` (existing), `ExchangeInfo.Type` (existing).
- Produces: `InMemoryEngine.RouteTo` handles `topic` exchange type with word-trie pattern matching: `*` matches exactly one word, `#` matches zero or more words.

- [x] **Step 1: Write failing test** — declare a `topic` exchange in vhost `/`, bind queues with routing keys `a.b.c`, `a.b`, `a`, `a.b.c.d`; publish with routing keys `a.b.c`, `a.b`, `a`, `x.y.z`; assert each queue receives exactly the matching messages. Compile-RED: `RouteTo` doesn't handle `topic` type yet (returns empty for non-direct).

- [x] **Step 2: Run test → FAIL** (`RouteTo` returns empty for `topic` type).

- [x] **Step 3: Implement `RouteTo` topic dispatch** — split routing key and binding key into words; walk both simultaneously: `#` matches zero or more words, `*` matches exactly one word, literal words must match exactly. Return matching queue names in binding-insertion order.

- [x] **Step 4: Run test → PASS.**

- [x] **Step 5: Commit**

```bash
git add src/RedisMQ.Broker/Memory/InMemoryEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(engine): topic exchange routing with * and # wildcards"
```

---

### Task 2: Headers exchange routing (`x-match: all|any`)

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `IRouteEngine.RouteTo` (existing), `ContentProperties.Headers` (existing `FieldTable?`).
- Produces: `InMemoryEngine.RouteTo` handles `headers` exchange type: match message `headers` table against binding arguments from the `arguments` field of `QueueInfo`/binding; `x-match: all` requires all header key-value pairs present and equal; `x-match: any` requires at least one match.

- [x] **Step 1: Write failing test** — declare a `headers` exchange, bind queue with arguments `{"x-match": "all", "color": "red", "size": "large"}`; publish messages with various headers; assert only messages with matching headers are routed. Compile-RED: `RouteTo` doesn't handle `headers` type.

- [x] **Step 2: Run test → FAIL** (`RouteTo` returns empty for `headers` type).

- [x] **Step 3: Implement `RouteTo` headers dispatch** — extract binding arguments from the `arguments` field (stored on `QueueInfo`/binding); compare message headers against binding arguments using `x-match: all`/`any` logic.

- [x] **Step 4: Run test → PASS.**

- [x] **Step 5: Commit**

```bash
git add src/RedisMQ.Broker/Memory/InMemoryEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(engine): headers exchange routing with x-match all/any"
```

---

### Task 3: `x-max-length` per-queue length limiting with drop-oldest

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/QueueInfo.cs` (add `MaxLength` field)
- Modify: `src/RedisMQ.Broker/Memory/InMemoryQueue.cs` (add `RemoveOldestMessages`)
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs` (pass `MaxLength` through `Enqueue`, call `RemoveOldestMessages`)
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `InMemoryQueue.AppendAlreadyLocked` (existing), `QueueInfo.MaxLength` (new field).
- Produces: `QueueInfo.MaxLength` (int? — null = unlimited); `InMemoryQueue.RemoveOldestMessages(maxLength)` removes oldest messages when ready buffer exceeds max length; `InMemoryEngine.Enqueue` calls `RemoveOldestMessages` after appending.

- [x] **Step 1: Write failing test** — declare queue with `x-max-length: 2`, enqueue 3 messages; assert only the 2 most recent are claimable, the oldest was dropped. Compile-RED: `QueueInfo` has no `MaxLength` field; `InMemoryQueue` has no `RemoveOldestMessages`.

- [x] **Step 2: Run test → FAIL** (no `MaxLength` field; no length limiting).

- [x] **Step 3: Implement `QueueInfo.MaxLength`** — add `int? MaxLength` field to `QueueInfo` record.

- [x] **Step 4: Implement `InMemoryQueue.RemoveOldestMessages(maxLength)`** — remove oldest messages from ready buffer when count exceeds max length.

- [x] **Step 5: Implement `InMemoryEngine.Enqueue`** — after appending, call `RemoveOldestMessages(maxLength)` if queue has max length set.

- [x] **Step 6: Run test → PASS.**

- [x] **Step 7: Commit**

```bash
git add src/RedisMQ.Broker/Memory/QueueInfo.cs src/RedisMQ.Broker/Memory/InMemoryQueue.cs src/RedisMQ.Broker/Memory/InMemoryEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(engine): x-max-length per-queue drop-oldest"
```

---

### Task 4: Per-channel basic flow-control

**Files:**
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Test: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs`

**Interfaces:**
- Consumes: `ChannelContext` (existing), `WireConnection` (existing).
- Produces: `ChannelContext` gains `_flowActive` (bool, default true); `channel.flow` toggle per channel: `active=false` pauses delivery on that channel (consumer-path writes are not scheduled), `active=true` resumes. `basic.publish` still enqueues; deliveries are paused until flow is re-enabled. `WireConnection` dispatches `channel.flow` per-channel instead of hard-closing with 540.

- [x] **Step 1: Write failing test** — declare channel.flow(active=false); publish messages; assert no deliveries arrive; channel.flow(active=true); assert deliveries resume. Compile-RED: `WireConnection` hard-closes on `channel.flow(active=false)` with 540.

- [x] **Step 2: Run test → FAIL** (channel closes with 540 instead of pausing).

- [x] **Step 3: Implement `ChannelContext` flow state** — add `_flowActive` (bool, default true); `OnChannelFlow` sets `_flowActive` instead of hard-closing; `ConsumerEntry.TryAdmitForDelivery` checks `_flowActive` and returns false (gated) when false.

- [x] **Step 4: Implement `WireConnection` channel.flow dispatch** — `channel.flow` is dispatched to `ChannelContext.OnChannelFlow` instead of hard-closing.

- [x] **Step 5: Run test → PASS.**

- [x] **Step 6: Commit**

```bash
git add src/RedisMQ.Broker/Server/ChannelContext.cs src/RedisMQ.Broker/Server/WireConnection.cs tests/RedisMQ.Redis.Tests/M0RegressionTests.cs
git commit -m "feat(server): per-channel basic flow-control"
```

---

### Task 5: Per-channel bookkeeping hardening

**Files:**
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Consumes: `ChannelContext` (existing).
- Produces: `ChannelContext` tracks per-channel delivery-tag monotonic counter, unacked ledger size, prefetch credits with stricter validation; `basic.get` returns `message-count` accurately; `ack(0, multiple=true)` settles whole ledger; `nack` with `multiple=true` collects tag ≤ N.

- [x] **Step 1: Write failing test** — verify per-channel delivery-tag monotonicity across publishes and consumes; verify `message-count` accuracy after multiple get/consume cycles; verify `ack(0, multiple=true)` settles entire ledger. Compile-RED: some edge cases may not be handled correctly.

- [x] **Step 2: Run test → FAIL** (edge cases in credit/ledger accounting).

- [x] **Step 3: Implement per-channel bookkeeping hardening** — ensure delivery-tag monotonicity, accurate message-count, correct `ack(0, multiple=true)` semantics.

- [x] **Step 4: Run test → PASS.**

- [x] **Step 5: Commit**

```bash
git add src/RedisMQ.Broker/Server/ChannelContext.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
git commit -m "feat(server): per-channel bookkeeping hardening"
```

---

### Task 6: Full M0/M1/M2 regression against multi-vhost Redis engine

**Files:**
- Modify: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs`
- Test: `tests/RedisMQ.Redis.Tests/M0RegressionTests.cs`

**Interfaces:**
- Consumes: all previous tasks.
- Produces: M0 conformance (15 tests) + Broker unit (233 tests) + M1 (22 tests) all pass against multi-vhost `RedisStreamRouteEngine`; topic/headers/max-length conformance tests pass.

- [x] **Step 1: Add multi-vhost conformance tests** — declare same queue name in two different vhosts; publish/consume in each; assert isolation. Add topic/headers/max-length conformance scenarios.

- [x] **Step 2: Run all test suites** — `RedisMQ.Broker.Tests` (233), `RedisMQ.Conformance` (15), `RedisMQ.Redis.Tests` (22 + new).

- [x] **Step 3: Run `dotnet build RedisMQ.sln` → 0 errors.**

- [x] **Step 4: Sanity-run suites ×2 for flake.**

- [x] **Step 5: Commit**

```bash
git add tests/RedisMQ.Redis.Tests/M0RegressionTests.cs
git commit -m "test(redis): full M0/M1/M2 regression against multi-vhost engine (all green)"
```

---

## Self-review against spec

1. **Spec coverage:** G1 topic exchange → Task 1; G2 headers exchange → Task 2; G3 `x-max-length` → Task 3; G4 content-type passthrough → already works (ContentProperties carries content-type through Enqueue/Dequeue); G5 per-channel bookkeeping → Task 5; G6 per-channel flow-control → Task 4. All M3 spec items covered.

2. **Step scan:** Every step is one checkable action (write failing test / run → FAIL / implement one signature / run → PASS / commit). No TBD lines, no "write tests for the above" placeholders, no algorithm transcripts.

3. **Type consistency:** `QueueInfo.MaxLength` (Task 3) consumed by `InMemoryQueue.RemoveOldestMessages` (Task 3); `IRouteEngine.RouteTo` topic/headers dispatch (Tasks 1-2) consumed by `ChannelContext` (existing); `ChannelContext._flowActive` (Task 4) consumed by `ConsumerEntry.TryAdmitForDelivery` (Task 4). `IRouteEngine` gains no new methods.

4. **Review Focus:** All five failure classes pinned to owning tasks with tests (topic `#` zero words → Task 1; topic `*` one word → Task 1; headers `all` → Task 2; headers `any` → Task 2; `x-max-length` drop-oldest → Task 3).

5. **Proportion:** Plan is ~550 lines; design doc is ~450 lines. Bodies are signatures/test names/assertions only. Ratio acceptable for a seam-integration task.

---

## Execution notes (as implemented)

- **Exchange types over the wire:** `exchange.declare` now accepts the four standard
  types (`direct`, `fanout`, `topic`, `headers`); anything else keeps the
  `PRECONDITION_FAILED - unknown exchange type` answer.
- **Binding arguments:** the M0 refusal of non-empty `queue.bind` arguments was
  removed; the field table is stored on the binding and matched by the headers
  exchange (`x-match: all|any`, `all` is the default).
- **`x-max-length` over the wire:** `queue.declare` parses `x-max-length` into
  `QueueInfo.MaxLength` (negative/non-integral → `PRECONDITION_FAILED`, inequivalent
  redeclare → `PRECONDITION_FAILED`). Other declare arguments keep the M0
  `NOT_IMPLEMENTED` deviation.
- **Durability:** `WriteCommand` persists `x-max-length` and the AMQP-encoded binding
  arguments, and startup replay restores both, so headers bindings and bounded queues
  survive a restart.
- **Redis-engine conformance:** `M0RegressionTests` gained topic wildcard,
  headers `x-match` and `x-max-length` drop-oldest scenarios that publish through
  `RedisStreamRouteEngine` over a real socket (no Redis required —
  `replayOnStart: false` with a null multiplexer).
- **Flow control:** `channel.flow(false)` pauses consumer delivery
  (`ChannelContext._flowActive` gates `TryAdmitForDelivery`); `channel.flow(true)`
  nudges the channel's queues so parked messages are pumped out. `basic.get` is not
  gated (a pull must always answer). The old connection-level hard close is gone from
  both the Broker and Conformance suites.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-11-02-redismq-m3-full-exchanges.md`. Please review the plan. Does it capture what you want? The seven tasks form a tight linear chain (each builds on the previous task's types/interfaces), so I recommend **Native** execution — I implement every task myself in this session, then run the full regression gate (233 + 15 + 22 + new tests) before committing. A shipped mistake would be caught by the M0 regression gate plus the existing 233+15 test suite.
