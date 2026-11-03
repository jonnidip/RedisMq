# RedisMQ M4 (part 1) — v1 Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make RedisMQ honest enough to call itself v1: confirms mean "Redis accepted every fan-out write", acks/requeues survive restart, dead-lettering and TTL expiry work, accounts are constrained per vhost, and the wire can be encrypted with TLS.

**Architecture:** Keep `InMemoryEngine` as the live system of record and `IRouteEngine` fully synchronous. The engine gains a per-publish durability receipt (`EnqueueReceipt`), a per-mutation `EngineDelta` describing what Redis must do (enqueue/mutate first, remove second), and a `Reap` sweep. `ChannelContext` feeds every publish outcome — routed, unroutable, mandatory-unroutable — through one ordered per-connection confirm loop that awaits receipts. Redis mirroring stays in the single background drainer. DLX/TTL/permissions are engine/channel concerns; TLS is a listener concern.

**Tech Stack:** .NET 10 (`net10.0`, `Nullable=enable`, `LangVersion=latest`), `StackExchange.Redis` (only in `src/RedisMQ.Redis`), xunit v3, `System.Net.Security.SslStream`, direct test runners (`dotnet exec ...Tests.dll`) because `dotnet test` MTP is unusable on this machine.

**Spec:** `docs/superpowers/specs/2026-11-02-redismq-m4-v1-completeness-design.md` (approved 2026-11-02). Parent: `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md`. Executors read both.

## Global Constraints

- `src/RedisMQ.Broker` keeps **zero** `PackageReference`s. StackExchange.Redis stays only in `src/RedisMQ.Redis`; TLS uses BCL `SslStream`.
- `IRouteEngine` remains synchronous. The only intentional block is bounded confirm-queue backpressure on the frame pump.
- `InMemoryEngine` is the live store; Redis is the durable mirror. No Redis write on the frame pump.
- Fresh Redis store only. No migration from M1–M3 data; delete local `{mq:*}` keys when testing against a shared Redis.
- Default-deny permissions is a breaking change. Every test/sample `BrokerUser` construction site moves to `BrokerUser.FullAccess`.
- M0/M1/M2/M3 regression stays green: `RedisMQ.Broker.Tests`, `RedisMQ.Conformance`, `RedisMQ.Redis.Tests`, `dotnet build RedisMQ.sln` 0 errors.
- Test runner (build first; `dotnet test` is broken here):
  ```powershell
  dotnet build RedisMQ.sln
  dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll
  dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
  dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll
  ```
  Narrow a run with `-class RedisMQ.Broker.Tests.<Class>` or `-method <FullyQualifiedName>`.
- PowerShell: use `;`, not `&&`. If git fails with `.git/index.lock`, run `Remove-Item .git\index.lock -Force` and retry.
- Redis is optional locally: Redis-dependent tests keep the existing `TryConnectRedis()` skip pattern. CI supplies a real container.
- No logging surface exists in v1. The design's "log once per queue" for unroutable DLX copies becomes a documented silent drop (noted in Task 8).

## Review Focus

Nine failure classes the design implies that no happy-path test catches:

1. **A confirm is never a lie.** A failed/slot-refused/exception `XADD` produces `confirm.nack`, never `confirm.ack`. Pinned in Tasks 1 and 6.
2. **Confirm order is publish order for every outcome.** Routed, unroutable, and mandatory-unroutable publishes all consume one sequence slot; `basic.return` precedes that publish's ack. Pinned in Task 6.
3. **Ack/requeue mirroring.** Acked and rejected messages leave Redis; requeue replaces the entry instead of appending a second one. Pinned in Tasks 3 and 5.
4. **Requeue crash window.** An `XADD` that landed before its `XDEL` collapses on replay (higher delivery count wins; stale entry XDELed). Pinned in Task 5.
5. **`expiration` is relative.** `"1000"` means now + 1 s, not a 1970 timestamp; requeue keeps the deadline; DLX copies clear it. Pinned in Tasks 7–8.
6. **`x-death` is AMQP-typed and ordered newest-first.** `count` is `l`, `time` is `T`, matching entries increment, `x-first-death-*` is set once, `DeliveryCount` resets on the copy. Pinned in Task 8.
7. **Queue expiry deletes, never dead-letters.** `x-expires` is an idle duration; `LastUsed` is in-memory and resets on restart. Pinned in Task 9.
8. **Default deny and vhost-open.** A user with no permission entry for a vhost cannot open it; an empty permission list denies every channel operation. Pinned in Tasks 10–11.
9. **A bad TLS handshake does not kill the listener.** The accept loop closes that socket and keeps accepting. Pinned in Task 12.

---

## File map (created/modified by this plan)

| File | Responsibility |
|------|----------------|
| `src/RedisMQ.Broker/Memory/IRouteEngine.cs` | `EnqueueReceipt`, `EngineDelta`, `QueuedMessage`, `QueuedMessageRef`; `Enqueue`/`Requeue`/`Acknowledge`/`Reject` return values; `Reap`; `QueueInfo` DLX/TTL fields |
| `src/RedisMQ.Broker/Memory/InMemoryEngine.cs` | Envelope ids + drops, deltas, delivery count, DLX/`x-death`, relative TTL, `LastUsed`, queue expiry, replay id bump, binding cleanup |
| `src/RedisMQ.Broker/Memory/InMemoryMessage.cs` | `DeliveryCount` (absolute `Expiration` stays) |
| `src/RedisMQ.Broker/Memory/InMemoryQueue.cs` | Drop/expiry collection returns the removed messages |
| `src/RedisMQ.Broker/Server/ChannelContext.cs` | Settle action routing, auto-ack mirroring, confirm entries, permission checks, DLX-aware reject |
| `src/RedisMQ.Broker/Server/WireConnection.cs` | Principal capture/verification, vhost-open check, one ordered confirm loop |
| `src/RedisMQ.Broker/Server/IChannelDeliveryHost.cs` | `EnqueueConfirm`, `WriteReturnToWireAsync` |
| `src/RedisMQ.Broker/Server/PendingConfirm.cs` (new) | The ordered confirm entry |
| `src/RedisMQ.Broker/Server/BrokerServerOptions.cs` | `Tls`, `ConfirmTimeout`, `ConfirmQueueCapacity`, `ReaperInterval`; `BrokerUser`/`BrokerPermission` reshape |
| `src/RedisMQ.Broker/Server/BrokerPrincipal.cs` (new) | Compiled per-vhost permission checks |
| `src/RedisMQ.Broker/Server/BrokerPassword.cs` (new) | PBKDF2 hash/verify |
| `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs` | Multi-listener accept loops + reaper loop |
| `src/RedisMQ.Broker/Transport/TransportPair.cs` | `IListenerTransport.AnnouncedPort` default member |
| `src/RedisMQ.Broker/Transport/SocketListener.cs` | Explicit `AnnouncedPort`; shared socket binder |
| `src/RedisMQ.Broker/Transport/TlsSocketListener.cs` (new) | `SslStream` accept path |
| `src/RedisMQ.Broker/Transport/BrokerCertificate.cs` (new) | PFX/PEM loading + mTLS chain validation |
| `src/RedisMQ.Broker/Transport/BrokerListeners.cs` (new) | Plaintext/TLS listener factory |
| `src/RedisMQ.Broker/Server/BrokerTlsOptions.cs` (new) | TLS configuration shape |
| `src/RedisMQ.Redis/WriteCommand.cs` | Real ids, completion, deltas, `Remove`/`RemoveStreamIds`/`DeleteQueue`, queue args |
| `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` | Receipts/TCS/slots, stream-id map, delta mirroring, tombstones, replay dedupe, `Reap` |
| `src/RedisMQ.Redis/StreamCodec.cs` | AMQP-wire properties, `expires-at`, `delivery-count`, `redelivered` |
| `src/RedisMQ.Redis/RedisKeys.cs` | New stream field names |
| `src/RedisMQ.Redis/RedisMQ.Redis.csproj` | `InternalsVisibleTo RedisMQ.Redis.Tests` |
| `tests/RedisMQ.Broker.Tests/*` | Engine receipt/delta, settle mirroring, confirm pipeline, queue args, DLX, reaper, permissions, TLS ring tests |
| `tests/RedisMQ.Redis.Tests/*` | Codec, replay, mirroring, crash-window integration tests |
| `tests/RedisMQ.Conformance/*` | TLS host variant + full regression |
| `README.md`, `docs/*.md` | v1 behaviour and configuration docs |

---

# Phase 1 — Durable identity, deltas and honest confirms

## Task 1: Envelope receipts across the engine seam

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/IRouteEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryQueue.cs`
- Modify: `src/RedisMQ.Redis/WriteCommand.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Modify: `src/RedisMQ.Redis/RedisMQ.Redis.csproj`
- Test: `tests/RedisMQ.Broker.Tests/EngineReceiptTests.cs` (new)
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  public readonly record struct EnqueueReceipt(
      ulong EnvelopeId,
      ValueTask<bool> Durable,                 // await at most once
      IReadOnlyList<ulong> DroppedEnvelopeIds)
  {
      public static EnqueueReceipt Immediate(ulong envelopeId, IReadOnlyList<ulong>? dropped = null);
  }

  EnqueueReceipt Enqueue(string vhost, string queue, ContentProperties properties,
      byte[] body, string exchange, string routingKey);
  ```
- Produces in `WriteCommand`: `TaskCompletionSource<bool>? Completion { get; init; }` (never serialized) and `bool ReservedDrainerSlot { get; init; }`.
- Produces in `RedisStreamRouteEngine`: `internal SemaphoreSlim? DrainerSlotsForTest { get; }` (test seam, documented).

- [ ] **Step 1: Write failing engine tests** in `EngineReceiptTests.cs`:
  - `Enqueue_returns_monotonic_envelope_ids_and_a_completed_true_receipt`.
  - `Enqueue_reports_max_length_drops_in_DroppedEnvelopeIds` (declare `maxLength: 2`, enqueue three, assert the returned id is the third and `DroppedEnvelopeIds` is the first id).
  - `ReplayEnqueue_advances_the_id_counter` (replay id 100, then enqueue, assert the new id is 101).
  Compile-RED: `Enqueue` returns `void`.

- [ ] **Step 2: Run → FAIL** for the expected compile reason.

- [ ] **Step 3: Implement the memory side.** Add `EnqueueReceipt.Immediate`. Change `InMemoryQueue.RemoveOldestMessages` to return `List<InMemoryMessage>` of the removed ready messages. Change `InMemoryEngine.Enqueue` to assign `++_lastEnvelopeId`, append, run max-length, and return the receipt with dropped ids. In `ReplayEnqueue`, set `_lastEnvelopeId = Math.Max(_lastEnvelopeId, message.EnvelopeId)`.

- [ ] **Step 4: Implement the Redis side.**
  - Add `Completion`/`ReservedDrainerSlot` to `WriteCommand` (not serialized).
  - `RedisStreamRouteEngine` creates `_drainerSlots = redis is null ? null : new SemaphoreSlim(drainerCapacity)`.
  - `Enqueue`: if `_channel is null`, return `_memory.Enqueue(...)` unchanged (memory-only confirms true). Otherwise `Wait(0)` a slot; if unavailable return `new EnqueueReceipt(0, ValueTask.FromResult(false), [])` **without touching memory**; then `_memory.Enqueue`, create a `TaskCompletionSource<bool>(RunContinuationsAsynchronously)`, build `var command = WriteCommand.Enqueue(realId, ...) with { Completion = tcs, ReservedDrainerSlot = true };`, and `TryWrite` it; on `TryWrite` failure release the slot, complete the TCS `false`, and return the receipt.
  - Drainer `finally` releases the slot; on success completes `true`, on exception completes `false` before swallowing.
  - Add `InternalsVisibleTo RedisMQ.Redis.Tests` to `RedisMQ.Redis.csproj`.

- [ ] **Step 5: Write the Redis tests.** In `RedisStreamRouteEngineTests.cs`:
  - `Null_redis_receipt_completes_true_and_assigns_ids`.
  - `Drainer_exhaustion_returns_false_without_enqueuing` (consume every slot through `DrainerSlotsForTest`, enqueue, assert id 0 / false / `PeekQueue` empty, release slots).
  - `Receipt_completes_only_after_xadd` (skip without Redis; await `Durable`, assert true, `StreamLengthAsync` is 1).

- [ ] **Step 6: Run → PASS.**
  ```powershell
  dotnet build RedisMQ.sln
  dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class RedisMQ.Broker.Tests.EngineReceiptTests
  dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -class RedisMQ.Redis.Tests.RedisStreamRouteEngineTests
  ```

- [ ] **Step 7: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Memory src/RedisMQ.Redis tests/RedisMQ.Broker.Tests/EngineReceiptTests.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
  git commit -m "feat(engine): per-publish durability receipts and envelope identity"
  ```

---

## Task 2: Engine deltas and settle methods

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/IRouteEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Test: `tests/RedisMQ.Broker.Tests/EngineDeltaTests.cs` (new)

**Interfaces:**
- Produces:
  ```csharp
  public sealed record QueuedMessageRef(string Vhost, string Queue, ulong EnvelopeId);
  public sealed record QueuedMessage(string Vhost, string Queue, InMemoryMessage Message);
  public sealed record EngineDelta(
      IReadOnlyList<QueuedMessage> Enqueued,
      IReadOnlyList<QueuedMessageRef> Removed,
      IReadOnlyList<(string Vhost, string Name)> DeletedQueues)
  {
      public static EngineDelta Empty { get; }
  }

  EngineDelta Acknowledge(string vhost, string queue, InMemoryMessage message);
  EngineDelta Reject(string vhost, string queue, InMemoryMessage message);
  EngineDelta Requeue(string vhost, string queue, InMemoryMessage message);
  ```
- Consumes: Task 1's receipt.

- [ ] **Step 1: Write failing tests** in `EngineDeltaTests.cs`:
  - `Acknowledge_returns_the_message_as_removed`.
  - `Reject_returns_the_message_as_removed`.
  - `Requeue_enqueues_the_message_redelivered_and_removes_the_original_id`.
  - `Max_length_drop_ids_are_available_to_the_mirror` (three enqueues into `maxLength: 2`, assert the last receipt's dropped ids).
  Compile-RED: methods absent.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `InMemoryEngine`.**
  - `Acknowledge`/`Reject`: `new EngineDelta([], [new QueuedMessageRef(vhost, queue, message.EnvelopeId)], [])`. No memory mutation — the pump already popped the message; these methods only describe the Redis settle.
  - `Requeue`: append `message with { Redelivered = true }`, wake the pump, return `Enqueued: [new QueuedMessage(vhost, queue, ...)]`, `Removed: [original id]`.
  - `Enqueue` already returns drops through the receipt; no extra delta.

- [ ] **Step 4: Implement `RedisStreamRouteEngine` pass-throughs** (`Acknowledge`/`Reject`/`Requeue` delegate to `_memory`, return its delta). Mirroring lands in Task 5; this task only keeps the seam honest and compilable.

- [ ] **Step 5: Run → PASS** (`EngineDeltaTests`).

- [ ] **Step 6: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Memory src/RedisMQ.Redis tests/RedisMQ.Broker.Tests/EngineDeltaTests.cs
  git commit -m "feat(engine): engine deltas for settle and requeue mirroring"
  ```

---

## Task 3: Channel settle and auto-ack mirroring

**Files:**
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Test: `tests/RedisMQ.Broker.Tests/RecordingEngine.cs` (new shared test double)
- Test: `tests/RedisMQ.Broker.Tests/SettleMirroringTests.cs` (new)

**Interfaces:**
- Produces `RecordingEngine : IRouteEngine`: delegates to `InMemoryEngine`, records ordered settle calls (`Ack|Reject|Requeue:<queue>:<envelopeId>`), and exposes `Func<string, string, ContentProperties, byte[], string, string, EnqueueReceipt>? EnqueueOverride` for later confirm tests.
- Produces in `ChannelContext`:
  ```csharp
  private enum SettleAction { Ack, Reject, Requeue }
  private ChannelOutcome SettleLedger(AmqpMethod method, long tag, bool multiple, SettleAction action);
  ```
- Consumes: `Acknowledge`/`Reject`/`Requeue` (Task 2).

- [ ] **Step 1: Add `RecordingEngine`** by copying the private `TransportPairConnectionListener`/`WireClient`/handshake helper block pattern used by `PublishTests.cs` into the new test file (do not refactor the existing four copies).

- [ ] **Step 2: Write failing wire tests** in `SettleMirroringTests.cs`:
  - `Ack_calls_Acknowledge`.
  - `Reject_without_requeue_calls_Reject`.
  - `Nack_with_requeue_calls_Requeue`.
  - `No_ack_consumer_calls_Acknowledge_on_delivery`.
  - `Get_with_no_ack_calls_Acknowledge`.
  - `Recover_calls_Requeue_for_every_unacked_entry`.
  - `Channel_close_requeues_unacked_entries`.
  Compile-RED: ack path currently never calls the engine.

- [ ] **Step 3: Implement the settle routing.**
  - Replace `SettleLedger(..., bool requeue)` with `SettleLedger(..., SettleAction action)`.
  - `basic.ack` → `Ack`; `basic.reject`/`basic.nack` with `requeue=false` → `Reject`; with `requeue=true` → `Requeue`.
  - Ack path: `foreach entry in settled) _engine.Acknowledge(...)`.
  - Requeue path stays `_engine.Requeue(...)`; reject path `_engine.Reject(...)`.
  - `ConsumerEntry.TryAdmitForDelivery`: when `noAck`, call `owner._engine.Acknowledge(owner._vhost, queueName, message)` after the ledger decision (the call is synchronous and does no I/O).
  - `OnBasicGet`: when `noAck`, call `_engine.Acknowledge(...)` after the claim succeeds and before scheduling the reply.
  - `TearDownActiveConsumers`/`OnBasicRecover` already call `Requeue`; no change beyond the new return type.

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Server/ChannelContext.cs tests/RedisMQ.Broker.Tests/RecordingEngine.cs tests/RedisMQ.Broker.Tests/SettleMirroringTests.cs
  git commit -m "feat(server): mirror ack, reject, requeue and auto-ack settles through the engine"
  ```

---

## Task 4: AMQP-wire content properties in the stream codec

**Files:**
- Modify: `src/RedisMQ.Redis/StreamCodec.cs`
- Modify: `src/RedisMQ.Redis/RedisKeys.cs`
- Modify: `src/RedisMQ.Redis/WriteCommand.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryMessage.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  public sealed record EnvelopeRecord(
      ulong EnvelopeId,
      string Exchange,
      string RoutingKey,
      byte[] Body,
      ContentProperties Properties,
      DateTimeOffset? ExpiresAt,
      int DeliveryCount,
      bool Redelivered);
  ```
- Produces `InMemoryMessage(..., DateTimeOffset? Expiration = null, int DeliveryCount = 0)`.
- Stream fields: `env`, `ex`, `rk`, `props` (base64 `ContentHeaderCodec.Write`), `body`, `expires-at` (Unix ms, omitted when null), `delivery-count`, `redelivered` (`0`/`1`).
- Removes the `PropertiesJson` JSON path. `ContentProperties` is a `FieldTable` carrier and cannot round-trip JSON.

- [ ] **Step 1: Write failing codec tests** in `RedisStreamRouteEngineTests.cs` (`StreamCodecTests` class):
  - `Encode_decode_round_trips_properties_in_amqp_wire_form` — headers with a nested table, an array, a timestamp, a byte array and a decimal; assert every value/type survives.
  - `Encode_decode_round_trips_expiry_delivery_count_and_redelivered`.
  - `Replay_restores_expiry_delivery_count_and_redelivered` (skip without Redis).
  Compile-RED: `EnvelopeRecord` still has `PropertiesJson`.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `StreamCodec`.**
  - `EncodeEnqueue`: `ContentHeaderCodec.Write(writer, 60, body.Length, properties)`, base64 to `props`; add the three new fields; omit `expires-at` when null.
  - `DecodeEnqueue`: read `props` via `ContentHeaderCodec.Read(ref reader).Properties`; missing/empty → `new ContentProperties()`; a malformed `props` payload falls back to empty properties (replay must not crash on a corrupt entry); read the three fields with defaults.
  - Add `RedisKeys.QueueField.ExpiresAt`, `DeliveryCount`, `Redelivered`.

- [ ] **Step 4: Update `WriteCommand`** enqueue/requeue factories and serialization to carry `ExpiresAt`, `DeliveryCount`, `Redelivered` through `EnvelopeRecord`; delete the JSON serialize/deserialize path. Keep the M3 round-trip test compiling by updating its `EnvelopeRecord` construction.

- [ ] **Step 5: Update replay** to build `InMemoryMessage(..., Expiration: record.ExpiresAt, DeliveryCount: record.DeliveryCount, Redelivered: record.Redelivered)`.

- [ ] **Step 6: Run → PASS.**

- [ ] **Step 7: Commit**
  ```powershell
  git add src/RedisMQ.Redis/StreamCodec.cs src/RedisMQ.Redis/RedisKeys.cs src/RedisMQ.Redis/WriteCommand.cs src/RedisMQ.Broker/Memory/InMemoryMessage.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
  git commit -m "feat(redis): persist AMQP-wire properties, expiry and delivery metadata"
  ```

---

## Task 5: Redis delta mirroring, tombstones and replay dedupe

**Files:**
- Modify: `src/RedisMQ.Redis/WriteCommand.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Produces `WriteCommandKind`: `Remove`, `RemoveStreamIds`, `DeleteQueue`, `Delta` (drop `Requeue`; a requeue is an enqueue + remove delta).
  ```csharp
  public static WriteCommand Remove(string vhost, string queue, params ulong[] envelopeIds);
  public static WriteCommand RemoveStreamIds(string vhost, string queue, params string[] streamIds);
  public static WriteCommand Delta(params WriteCommand[] batch);
  public string[]? StreamIds { get; init; }
  public IReadOnlyList<WriteCommand>? Batch { get; init; }
  ```
  `Remove` serializes `envelope-ids` (comma-separated) and `RemoveStreamIds` serializes `stream-ids` (comma-separated) for round-trip tests; `Delta` is never serialized and `ToStreamFields` throws for it.
- Produces in `RedisStreamRouteEngine`: private `_streamIds` map `(vhost, queue, envelopeId) → RedisValue`, and `private void MirrorDelta(EngineDelta delta)`.
- Consumes: Task 2 deltas, Task 4 records.

- [ ] **Step 1: Write failing unit tests** (no Redis needed):
  - `Ack_returns_a_remove_delta_without_redis` — `new RedisStreamRouteEngine(null, ["/"], replayOnStart: false)`; `Acknowledge` returns one removed id; no throw.
  - `WriteCommand_Remove_round_trips` and `Delta_batch_round_trips` (or assert `ToStreamFields` throws for `Delta`, which is never replayed).
  - `Requeue_no_longer_exists_as_a_stream_command` (compile-level: update the M3 round-trip test to use `Enqueue` with `redelivered: true`).

- [ ] **Step 2: Write failing integration tests** (skip without Redis; unique vhost per test):
  - `Ack_removes_the_stream_entry_and_replay_does_not_resurrect_it` — enqueue, await receipt, `Acknowledge`, wait for `XLEN` 0, dispose, construct a new engine with replay, assert queue empty.
  - `Reject_without_requeue_removes_the_stream_entry`.
  - `Requeue_replaces_the_entry` — one entry remains, `delivery-count` incremented, `redelivered` true.
  - `Max_length_drops_are_xdel_ed`.
  - `DeleteQueue_removes_the_stream_and_replays_as_deleted` — bind, delete, dispose/replay, assert queue absent and binding gone.
  - `Unbind_tombstone_survives_replay`.
  - `Crash_window_duplicate_is_collapsed_and_stale_entry_xdel_ed` — manually `XADD` two entries with the same envelope id (one lower count), construct a replay engine, assert one message and `XLEN` 1.

- [ ] **Step 3: Implement the drainer map and command handling.**
  - `Enqueue` command: `StreamAddAsync`, store the returned id in `_streamIds`.
  - `Remove`: resolve each id from the map, `StreamDeleteAsync`, remove map entries.
  - `RemoveStreamIds`: `StreamDeleteAsync` the given ids and drop their map entries.
  - `DeleteQueue`: `KeyDeleteAsync(RedisKeys.QueueKey(...))` plus a topology `DeleteQueue` stream entry.
  - `Delta`: apply each child in order; `ToStreamFields` throws `InvalidOperationException("delta commands are not replayed")`.
  - `MirrorDelta`: stash removed stream ids first; build one `Delta` batch (enqueue copies, remove stashed ids, delete queues); `_channel.Writer.TryWrite` the batch; if it fails, skip the whole batch (documented duplicate-on-replay, never loss).
  - `Enqueue` dropped ids: after the enqueue command, `TryWrite` a `Remove(vhost, queue, droppedIds)` command (best effort).
  - `Acknowledge`/`Reject`/`Requeue` call `_memory` then `MirrorDelta`.

- [ ] **Step 4: Implement replay.**
  - Topology replay: apply `DeclareExchange`, `DeclareQueue` (all fields), `Bind`, `Unbind`, `DeleteQueue` in stream order; a deleted queue drops its bindings.
  - Queue replay: parse entries, group by envelope id, keep the higher `delivery-count` (tie: later stream id), `XDEL` the stale ids, `ReplayEnqueue` survivors with their metadata, and record their stream ids in `_streamIds`.

- [ ] **Step 5: Update `InMemoryEngine.DeleteQueue`** to also remove bindings and the queue's `LastUsed` entry (when Task 7 adds `LastUsed`; in this task remove bindings).

- [ ] **Step 6: Run → PASS** (unit always; integration with Redis when available).

- [ ] **Step 7: Commit**
  ```powershell
  git add src/RedisMQ.Redis src/RedisMQ.Broker/Memory/InMemoryEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
  git commit -m "feat(redis): mirror settles and topology tombstones, dedupe replay"
  ```

---

## Task 6: The ordered async confirm pipeline

**Files:**
- Modify: `src/RedisMQ.Broker/Server/IChannelDeliveryHost.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Modify: `src/RedisMQ.Broker/Server/BrokerServerOptions.cs`
- Create: `src/RedisMQ.Broker/Server/PendingConfirm.cs`
- Test: `tests/RedisMQ.Broker.Tests/ConfirmPipelineTests.cs` (new)
- Test: `tests/RedisMQ.Broker.Tests/RecordingEngine.cs` (extend)

**Interfaces:**
- Produces:
  ```csharp
  // Public because IChannelDeliveryHost is public; the design's "internal"
  // cannot appear in a public interface signature.
  public sealed record PendingConfirm(
      ChannelContext Channel,
      ushort ChannelNumber,
      long Sequence,
      IReadOnlyList<EnqueueReceipt> Receipts,
      ChannelOutcomeBasicReturn? ReturnFirst);

  // IChannelDeliveryHost
  void EnqueueConfirm(PendingConfirm confirm);
  ValueTask WriteReturnToWireAsync(ushort channel, ChannelOutcomeBasicReturn returned, CancellationToken ct);

  // ChannelContext
  internal bool IsOpen { get; }
  internal void ScheduleConfirmResult(long sequence, bool ack);
  internal void ScheduleConfirmReturn(ChannelOutcomeBasicReturn returned);
  internal Task FlushWriterChainAsync(CancellationToken ct);
  ```
- Produces `BrokerServerOptions.ConfirmQueueCapacity` (default 4096) and `ConfirmTimeout` (default 5 s).

- [ ] **Step 1: Write failing tests** in `ConfirmPipelineTests.cs` (copy the raw-client helper block; configure `ConfirmQueueCapacity` where needed):
  - `Ack_waits_for_the_receipt` — `RecordingEngine.EnqueueOverride` returns an uncompleted TCS; publish; assert no `confirm.ack` within 200 ms; complete true; assert ack.
  - `Failed_receipt_nacks` — complete false; assert `confirm.nack` with `requeue=false`.
  - `Receipts_complete_out_of_order_but_acks_stay_in_sequence` — two publishes, complete the second first, assert ack 1 then ack 2.
  - `Unroutable_publish_occupies_its_sequence_slot` — routed publish 1 (delayed), unroutable publish 2; complete 1; assert ack 1 then ack 2.
  - `Mandatory_return_precedes_its_ack` — mandatory unroutable; read `basic.return`, then `confirm.ack` for the same tag.
  - `Retired_channel_discards_a_late_confirm` — publish, close the channel, complete the receipt; assert no confirm on that channel and the connection still serves a new channel.
  - `Full_confirm_queue_blocks_the_pump` — capacity 1; publish 1 (uncompleted), publish 2, then `queue.declare`; assert no declare-ok until receipt 1 completes; then assert ack 1, ack 2 and declare-ok.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `PendingConfirm` and `IChannelDeliveryHost` additions.** `WriteReturnToWireAsync` writes the return method + header + body as one gated write (refactor the existing `ChannelOutcomeBasicReturn` case in `DispatchChannelOutcomeAsync` to call it).

- [ ] **Step 4: Rework `FinishPublish`.**
  - Routed: collect one `EnqueueReceipt` per target.
  - Unroutable non-mandatory: empty receipt list.
  - Mandatory unroutable: empty receipt list plus `ReturnFirst` with the same `basic.return` payload.
  - In confirm mode: `_host.EnqueueConfirm(new PendingConfirm(this, _channel, sequence, receipts, returnFirst))` and return `ChannelOutcomeSilent`.
  - In non-confirm mode: today's inline `basic.return`/silence.

- [ ] **Step 5: Implement the confirm loop in `WireConnection`.**
  - On entering `Serving` (after `connection.open-ok`), create a bounded `Channel<PendingConfirm>` with `ConfirmQueueCapacity`, start `RunConfirmLoopAsync` with a `CancellationTokenSource`.
  - `EnqueueConfirm` does a deliberate blocking `WriteAsync` on the bounded channel (pump backpressure); cancellation/closed channel drops the entry silently.
  - Loop: for each entry, `await Task.WhenAll(entry.Receipts.Select(r => r.Durable.AsTask())).WaitAsync(ConfirmTimeout, ct)`; all true or empty → ack; false/exception/timeout → nack.
  - Liveness: discard if `!_channels.TryGetValue(number, out current) || !ReferenceEquals(current, entry.Channel) || !current.IsOpen`.
  - Schedule `ReturnFirst` first, then the ack/nack, then `await channel.FlushWriterChainAsync(ct)`.
  - Dispose: cancel and await the loop before completing the transport.

- [ ] **Step 6: Implement `ChannelContext` helpers.** `ScheduleConfirmResult` builds `confirm.ack`/`confirm.nack` via `NewMethod` and schedules `_host.WriteMethodToWireAsync`; `ScheduleConfirmReturn` schedules `_host.WriteReturnToWireAsync`; `FlushWriterChainAsync` calls `RunWriterChainAsync`; `IsOpen => _state == ChannelState.Running`.

- [ ] **Step 7: Run → PASS.** Also run the existing `FlowTests` confirms (`Confirm_ack_round_trip`) to prove the in-memory path still acks fast.

- [ ] **Step 8: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Server tests/RedisMQ.Broker.Tests/ConfirmPipelineTests.cs tests/RedisMQ.Broker.Tests/RecordingEngine.cs
  git commit -m "feat(server): ordered async publisher confirms with receipt backpressure"
  ```

---

# Phase 2 — DLX, TTL and the reaper

## Task 7: Queue arguments, relative TTL and delivery count

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/IRouteEngine.cs` (`QueueInfo`)
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryMessage.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryQueue.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Modify: `src/RedisMQ.Redis/WriteCommand.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Test: `tests/RedisMQ.Broker.Tests/QueueArgumentsTests.cs` (new)
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  public sealed record QueueInfo(
      string Vhost, string Name, bool Durable = false, bool Exclusive = false,
      bool AutoDelete = false, TimeSpan? Expiration = null, int? MaxLength = null,
      string? DeadLetterExchange = null, string? DeadLetterRoutingKey = null,
      int? DeliveryLimit = null);
  ```
- `QueueInfo.Expiration` changes from `DateTimeOffset?` to `TimeSpan?` (idle duration).
- `InMemoryMessage` gains `int DeliveryCount = 0`.
- `ChannelContext` parses `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-delivery-limit`, `x-expires` alongside `x-max-length`.

- [ ] **Step 1: Write failing declare tests** in `QueueArgumentsTests.cs`:
  - `Declare_accepts_all_v1_queue_arguments` (assert the stored `QueueInfo` fields, including `Expiration == TimeSpan.FromSeconds(30)`).
  - `Unknown_queue_argument_still_returns_not_implemented`.
  - `Negative_or_non_integral_argument_returns_precondition_failed` (each of the four integral args).
  - `Redeclare_with_a_different_dlx_arg_is_inequivalent`.
  - `Empty_dlx_name_is_the_default_exchange_not_no_dlx`.

- [ ] **Step 2: Write failing TTL/count tests:**
  - `Expiration_is_relative_to_enqueue_time` — `expiration: "1000"`, assert `Expiration` is within 100 ms of now + 1 s.
  - `Zero_expiration_expires_immediately`.
  - `Negative_or_unparseable_expiration_leaves_no_deadline`.
  - `Requeue_keeps_the_absolute_deadline`.
  - `Delivery_count_increments_on_claim_and_requeue` — consume, nack-requeue, `PeekQueue` shows `DeliveryCount == 1`.

- [ ] **Step 3: Implement `QueueInfo` and declare parsing.** In `OnQueueDeclare`, parse the argument table into a small `QueueDeclareArguments` struct; unknown keys → `NotSupportedArguments`; invalid/negative → `PRECONDITION_FAILED - invalid arg '<name>' ...`; pass the fields into `new QueueInfo(...)`. Extend `DeclaredQueueOutcome` equivalence checks for each field. Server-generated names still work.

- [ ] **Step 4: Implement relative TTL and delivery count.**
  - `InMemoryEngine.Enqueue`: `Expiration = now + TimeSpan.FromMilliseconds(ms)`; guard overflow/negative (leave null).
  - `InMemoryQueue.TryClaimOneAlreadyLocked` and `InMemoryEngine.TryClaimForGet` hand out `message with { DeliveryCount = message.DeliveryCount + 1 }`.
  - `Requeue` preserves the deadline and the incremented count.
  - Add an in-memory `_lastUsed` map; touch it in `DeclareQueue`, `Bind`, `Enqueue`, `TryClaimForGet`, `RegisterConsumer`, and `UnregisterConsumer` (when the last consumer leaves). No persistence. Replace the unused `GetQueueExpiry` with `internal TimeSpan? GetQueueIdleExpiration(string vhost, string queue)` (or delete it; nothing else calls it).

- [ ] **Step 5: Persist the arguments.** Extend `WriteCommand.DeclareQueue(..., int? expiresMs = null, string? deadLetterExchange = null, string? deadLetterRoutingKey = null, int? deliveryLimit = null)`; serialize `x-expires`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-delivery-limit`; replay rebuilds `QueueInfo` with `Expiration: TimeSpan.FromMilliseconds(expiresMs)`.

- [ ] **Step 6: Run → PASS.** Update the existing `Redis_stream_engine_expires_messages` test to use `Reap` in Task 8; for now keep it compiling against the internal expiry helper.

- [ ] **Step 7: Commit**
  ```powershell
  git add src/RedisMQ.Broker src/RedisMQ.Redis tests/RedisMQ.Broker.Tests/QueueArgumentsTests.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
  git commit -m "feat(engine): queue TTL/DLX arguments, relative expiry and delivery counts"
  ```

---

## Task 8: Dead-letter construction and triggers

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryQueue.cs`
- Modify: `src/RedisMQ.Broker/Memory/IRouteEngine.cs` (`Reap` signature)
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` (`Reap` delegate + mirror)
- Modify: `tests/RedisMQ.Broker.Tests/RecordingEngine.cs` (`Reap` pass-through)
- Test: `tests/RedisMQ.Broker.Tests/DeadLetterTests.cs` (new)
- Test: `tests/RedisMQ.Broker.Tests/SettleMirroringTests.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`

**Interfaces:**
- Produces `EngineDelta Reap(DateTimeOffset now)` on `IRouteEngine` (message expiry only in this task; queue expiry is Task 9).
- Produces private `DeadLetterAlreadyLocked(...)` and `WithDeath(...)` in `InMemoryEngine`.
- `Reject` now dead-letters when the queue has a DLX; `Requeue` dead-letters on delivery-limit overflow.

- [ ] **Step 1: Write failing tests** in `DeadLetterTests.cs`:
  - `Reject_with_dlx_enqueues_a_copy_with_x_death` — declare source with DLX + target queue, bind, enqueue, consume/requeue, reject; assert the target has one message with `x-death` (`queue`, `reason = rejected`, `count = 1`, `time` is a timestamp, `exchange` is the original).
  - `Nack_without_requeue_dead_letters`.
  - `Requeue_over_limit_dead_letters` — `x-delivery-limit: 1`; deliver, requeue → copy in DLX.
  - `Expired_ready_message_dead_letters` — `expiration: "1"`, `Reap(now + 1 s)` → copy in DLX with `reason = expired`.
  - `Missing_dlx_exchange_drops_the_copy`.
  - `Empty_dlx_name_uses_the_default_exchange`.
  - `X_death_increments_matching_entry_and_prepends_a_new_one` (dead-letter through two queues).
  - `X_first_death_fields_are_set_once`.
  - `Dead_letter_copy_resets_delivery_count_and_clears_expiration`.
  - `Loop_guard_drops_a_second_death_for_the_same_queue_and_reason`.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `WithDeath`.** Clone the `Headers` table, find `x-death` (`FieldValue[]` of tables), increment the matching `(queue, reason)` table's `count` (a `long`/`l`) and refresh `time` (`T`), or prepend a new table `{ queue: S, reason: S, count: l, time: T, exchange: S }`; set `x-first-death-queue/reason/exchange` only when absent; return `properties with { Headers = headers, Expiration = null }`.

- [ ] **Step 4: Implement `DeadLetterAlreadyLocked`.**
  - Loop guard: if `x-death` already contains `(originQueue, reason)`, remove only.
  - Resolve `DeadLetterExchange`: null → remove only; `""` → default exchange; named → `GetExchange` (missing → remove only).
  - Routing key = `DeadLetterRoutingKey ?? message.RoutingKey`.
  - Targets: default exchange → destination queue named by the key if it exists; named → `RouteTo(exchange, props, key)`.
  - For each target: new envelope id (`++_lastEnvelopeId`), append `new InMemoryMessage(props, DefensiveCopy(body), dlxName, key, newId, DeliveryCount: 0, Expiration: null)`, apply max-length and add any evicted ids to `Removed`.
  - Add each copy to `Enqueued`; always add the original to `Removed`.

- [ ] **Step 5: Wire the triggers.**
  - `Reject`: if the queue has a DLX, dead-letter with `rejected`; else removed only.
  - `Requeue`: if `DeliveryLimit is int limit && message.DeliveryCount >= limit`, dead-letter with `rejected`; else requeue.
  - `Reap`: sweep each queue's ready messages with `Expiration <= now`; dead-letter with `expired` or remove; return one delta. Snapshot the queue keys before the sweep so a DLX copy into another queue does not mutate the iteration set.
  - `InMemoryQueue.RemoveExpiredMessages` becomes `CollectExpiredMessages(DateTimeOffset now, List<InMemoryMessage> into)`.
  - `ChannelContext`'s reject path already calls `_engine.Reject` (Task 3); no change needed beyond confirming it passes the message.
  - Update the existing M1 test `Redis_stream_engine_expires_messages` from `RemoveExpiredReadyMessages()` to `engine.Reap(DateTimeOffset.UtcNow)` and delete the old internal helper.

- [ ] **Step 6: Add a wire test** in `SettleMirroringTests.cs`: consume, `basic.reject(requeue:false)` with a DLX queue, consume from the DLX target, assert `x-death` headers on the delivered content.

- [ ] **Step 7: Add Redis integration tests** in `RedisStreamRouteEngineTests.cs` (skip without Redis; unique vhost):
  - `Dead_letter_copy_is_mirrored_with_x_death` — declare source with a DLX and target, enqueue, `Reject`, wait for the target's `XLEN` to be 1, dispose, replay, assert the target holds one message whose `x-death` survived.
  - `Dead_letter_copy_resets_expiry_in_redis` — assert the replayed copy's `expires-at` field is absent.
  Use a bounded polling helper (`WaitUntilAsync(Func<bool>, 2 s)`) because the drainer is asynchronous.

- [ ] **Step 8: Run → PASS.**

- [ ] **Step 9: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Memory src/RedisMQ.Redis tests/RedisMQ.Broker.Tests/DeadLetterTests.cs tests/RedisMQ.Broker.Tests/SettleMirroringTests.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs
  git commit -m "feat(engine): dead-letter routing with x-death and delivery-limit overflow"
  ```

---

## Task 9: Queue expiry and the reaper loop

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Modify: `src/RedisMQ.Broker/Server/BrokerServerOptions.cs`
- Modify: `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Test: `tests/RedisMQ.Broker.Tests/ReaperTests.cs` (new)
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`
- Test: `tests/RedisMQ.Conformance/FlowTests.cs` (add one expiry scenario)

**Interfaces:**
- `InMemoryEngine.Reap` extends to queue expiry; `RedisStreamRouteEngine.Reap` delegates + `MirrorDelta`.
- `BrokerServerOptions.ReaperInterval` (`TimeSpan`, default 5 s, `0` disables).
- `AmqpBrokerServer` owns one `PeriodicTimer` reaper loop for all listeners.

- [ ] **Step 1: Write failing tests** in `ReaperTests.cs`:
  - `Idle_queue_with_x_expires_is_deleted` — declare with `x-expires: 100`, `Reap(lastUsed + 200 ms)`, assert `GetQueue` null, bindings gone.
  - `Queue_with_a_consumer_is_not_expired`.
  - `Queue_expiry_deletes_messages_without_dead_lettering` — source with DLX; expire the queue; assert the DLX target is empty.
  - `Last_used_is_touched_by_declare_bind_enqueue_get_and_consume`.
  - `Server_reaper_loop_expires_a_message` — `ReaperInterval = 50 ms`, publish with `expiration: "50"`, wait, assert the queue is empty (no consumer).
  - `Redis_reap_mirrors_deletes` (integration; skip without Redis) — expire a queue, assert the stream key is gone and replay stays deleted.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement queue expiry in `Reap`.** Under `_lock`: for each queue with `Expiration is TimeSpan idle` and `now >= _lastUsed[(vhost,queue)] + idle` and no consumers, remove the queue, delivery state, bindings and `_lastUsed`, and add `(vhost, queue)` to `DeletedQueues`. Messages are dropped (no DLX path).

- [ ] **Step 4: Implement the server loop.** In `StartAsync`, start `RunReaperAsync` when `ReaperInterval > TimeSpan.Zero`:
  ```csharp
  using var timer = new PeriodicTimer(_options.ReaperInterval);
  while (await timer.WaitForNextTickAsync(ct)) { _engine.Reap(DateTimeOffset.UtcNow); }
  ```
  Start it alongside the accept loops; `StopAsync` cancels and awaits it (swallowing cancellation). Do not start a second loop per listener.

- [ ] **Step 5: Run → PASS** (including the new conformance expiry scenario).

- [ ] **Step 6: Commit**
  ```powershell
  git add src/RedisMQ.Broker src/RedisMQ.Redis tests/RedisMQ.Broker.Tests/ReaperTests.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs tests/RedisMQ.Conformance/FlowTests.cs
  git commit -m "feat(server): queue expiry and the shared reaper loop"
  ```

---

# Phase 3 — Per-vhost permissions

## Task 10: Permission model, PBKDF2 and startup validation

**Files:**
- Modify: `src/RedisMQ.Broker/Server/BrokerServerOptions.cs`
- Create: `src/RedisMQ.Broker/Server/BrokerPrincipal.cs`
- Create: `src/RedisMQ.Broker/Server/BrokerPassword.cs`
- Modify: `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Modify: `tests/RedisMQ.Conformance/TestBrokerHost.cs`
- Modify: `tests/RedisMQ.Broker.Tests/PublishTests.cs` (options helper)
- Modify: `tests/RedisMQ.Broker.Tests/ConsumeTests.cs`
- Modify: `tests/RedisMQ.Broker.Tests/ChannelTopologyTests.cs`
- Modify: `tests/RedisMQ.Broker.Tests/ConnectionFsmTests.cs`
- Test: `tests/RedisMQ.Broker.Tests/PermissionTests.cs` (new)

**Interfaces:**
- Produces:
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
      public static string Hash(string password);                // pbkdf2-sha256$210000$<salt>$<hash>
      public static bool Verify(string password, BrokerUser user); // fixed-time
  }

  public sealed class BrokerPrincipal
  {
      public BrokerPrincipal(string name, IReadOnlyList<BrokerPermission> permissions);
      public string Name { get; }
      public bool IsKnownVhost(string vhost);
      public bool CanConfigure(string vhost, string name);
      public bool CanWrite(string vhost, string name);
      public bool CanRead(string vhost, string name);
  }
  ```
  (Typo guard for the executor: the method is `BrokerPassword.Verify(string password, BrokerUser user)`.)
- Produces `BrokerServerOptions.Validate()` called from `AmqpBrokerServer.StartAsync` and `TestBrokerHost.Start`.

- [ ] **Step 1: Write failing tests** in `PermissionTests.cs`:
  - `Pbkdf2_hash_round_trips` (hash, verify true, wrong password false).
  - `Malformed_or_wrong_algorithm_hash_is_refused`.
  - `FullAccess_grants_configure_write_read_on_named_vhosts` and denies other vhosts.
  - `Empty_permission_list_denies_everything`.
  - `Principal_matches_regexes_with_culture_invariance` (`.*` allows, empty pattern denies non-empty names, `^a\..*$` etc.).
  - `Invalid_permission_regex_throws_BrokerConfigurationException`.
  - `Exactly_one_credential_form_is_required` (`Validate` throws for both/neither).
  - `Regex_timeout_denies_instead_of_hanging` (a pathological pattern with a 1 s timeout; assert the `RegexMatchTimeoutException` path returns false).

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `BrokerPermission`/`BrokerUser`.** Add the record parameters; `FullAccess` builds `new BrokerPermission(v, ".*", ".*", ".*")` for each vhost. Keep positional `new BrokerUser("guest", "guest")` compiling (password is the second parameter).

- [ ] **Step 4: Implement `BrokerPassword`.** `Rfc2898DeriveBytes.Pbkdf2(password, salt, 210_000, HashAlgorithmName.SHA256, 32)`; 16-byte random salt; encode `pbkdf2-sha256$210000$<base64 salt>$<base64 hash>`; parse exactly four `$` parts; `CryptographicOperations.FixedTimeEquals` for the hash and for plaintext passwords.

- [ ] **Step 5: Implement `BrokerPrincipal`.** Compile each permission triple once with `RegexOptions.CultureInvariant` and `TimeSpan.FromSeconds(1)`; `CanX(vhost, name)` finds the vhost entry (first match wins) and returns `regex.IsMatch(name)`, catching `RegexMatchTimeoutException` → false. `IsKnownVhost` is entry presence, even for a deny-all entry.

- [ ] **Step 6: Implement `BrokerServerOptions.Validate()`** and call it at server/host start. Validate: every user has exactly one of `Password`/`PasswordHash`; malformed hashes throw; every permission regex compiles; duplicate user names throw.

- [ ] **Step 7: Update the six construction sites** to `BrokerUser.FullAccess("guest", "guest", "/")`: `PublishTests.NewOptions`, `ConsumeTests` options helper, `ChannelTopologyTests` options helper, `ConnectionFsmTests.NewOptions`, and both `TestBrokerHost.Start` overloads.

- [ ] **Step 8: Run → PASS** plus the existing suites (default deny has not been enforced yet, so all wire tests should still pass).

- [ ] **Step 9: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Server tests/RedisMQ.Broker.Tests tests/RedisMQ.Conformance/TestBrokerHost.cs
  git commit -m "feat(server): per-vhost permissions, PBKDF2 accounts and FullAccess helper"
  ```

---

## Task 11: Principal propagation and wire enforcement

**Files:**
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Test: `tests/RedisMQ.Broker.Tests/PermissionWireTests.cs` (new)

**Interfaces:**
- Consumes: `BrokerPrincipal` (Task 10).
- `WireConnection` builds the principal at `StartOk` and passes it to `new ChannelContext(channel, _vhost, Routes, this, _connectionId, _principal)`.
- `ChannelContext` constructor gains `BrokerPrincipal? principal = null`; null means "internal test, full access" (only `WireConnection` creates channels in production).

- [ ] **Step 1: Write failing wire tests** in `PermissionWireTests.cs`:
  - `Full_access_user_can_declare_bind_publish_consume_and_get`.
  - `Restricted_user_is_denied_exchange_declare_without_configure`.
  - `Restricted_user_is_denied_queue_declare_without_configure`.
  - `Restricted_user_is_denied_bind_without_write_on_the_exchange` (and without configure on the queue).
  - `Restricted_user_is_denied_publish_without_write_on_the_exchange`.
  - `Default_exchange_publish_checks_the_routing_key`.
  - `Restricted_user_is_denied_consume_and_get_without_read`.
  - `Access_refused_wording_is_exact` — `403 ACCESS_REFUSED - access to <exchange|queue> '<name>' in vhost '<vhost>' refused for user '<user>'`.
  - `Connection_open_on_a_vhost_without_a_permission_entry_is_refused` — `403 ACCESS_REFUSED - access to vhost '<vhost>' refused for user '<user>'`, class/method 10/40.
  - `Deny_all_permission_entry_allows_open_but_denies_operations`.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement principal capture.** In `OnStartOkAsync`, find the user by name, verify with `BrokerPassword.Verify`, and set `_principal = new BrokerPrincipal(user.Name, user.Permissions ?? [])`; keep the existing `ACCESS_REFUSED` wording for bad credentials.

- [ ] **Step 4: Implement vhost-open.** In `OnOpenAsync`, after the `Vhosts` configured check, require `_principal is not null && _principal.IsKnownVhost(vhost)`; otherwise `CloseConnectionAsync(AccessRefused, $"ACCESS_REFUSED - access to vhost '{vhost}' refused for user '{_principal?.Name}'", 10, 40, ct)`.

- [ ] **Step 5: Implement channel checks** (all before existence/equivalence):
  - `OnExchangeDeclare`: `CanConfigure(name)`.
  - `OnQueueDeclare`: `CanConfigure(actual)` after server-name generation (passive included).
  - `BindingAction`: `CanConfigure(queueName) && CanWrite(exchange)`.
  - `FinishPublish`: `CanWrite(exchangeName.Length == 0 ? routingKey : exchangeName)`.
  - `OnBasicConsume` / `OnBasicGet`: `CanRead(queue)`.
  - Refusal helper returns `Soft(method, AccessRefused, "ACCESS_REFUSED - access to <kind> '<name>' in vhost '<vhost>' refused for user '<user>'")`.

- [ ] **Step 6: Run → PASS** and the full Broker suite (all FullAccess sites now exercise the new checks).

- [ ] **Step 7: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Server/WireConnection.cs src/RedisMQ.Broker/Server/ChannelContext.cs tests/RedisMQ.Broker.Tests/PermissionWireTests.cs
  git commit -m "feat(server): enforce per-vhost configure/write/read and vhost-open permissions"
  ```

---

# Phase 4 — TLS

## Task 12: Certificate loading and the TLS listener

**Files:**
- Create: `src/RedisMQ.Broker/Server/BrokerTlsOptions.cs`
- Create: `src/RedisMQ.Broker/Transport/BrokerCertificate.cs`
- Create: `src/RedisMQ.Broker/Transport/TlsSocketListener.cs`
- Modify: `src/RedisMQ.Broker/Server/BrokerServerOptions.cs`
- Modify: `src/RedisMQ.Broker/Transport/TransportPair.cs`
- Modify: `src/RedisMQ.Broker/Transport/SocketListener.cs`
- Test: `tests/RedisMQ.Broker.Tests/TlsTransportTests.cs` (new)
- Test: `tests/RedisMQ.Broker.Tests/TestCertificates.cs` (new)

**Interfaces:**
- Produces:
  ```csharp
  public sealed class BrokerTlsOptions
  {
      public bool Enabled { get; set; }
      public int Port { get; set; } = 5671;
      public string? CertificatePath { get; set; }
      public string? CertificateKeyPath { get; set; }
      public string? CertificatePassword { get; set; }
      public bool ClientCertificateRequired { get; set; }
      public string? ClientCertificateAuthorityPath { get; set; }
      public SslProtocols MinTlsVersion { get; set; } = SslProtocols.Tls12;
      public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
  }

  public sealed class BrokerCertificate : IDisposable
  {
      public static BrokerCertificate Load(BrokerTlsOptions options);
      public X509Certificate2 Certificate { get; }
      public SslServerAuthenticationOptions CreateAuthenticationOptions();
  }
  ```
- `IListenerTransport` gains `int? AnnouncedPort => null;`; `SocketListener` adds `int? IListenerTransport.AnnouncedPort => _announcedPort;`.
- `BrokerServerOptions.Tls` (`BrokerTlsOptions?`).

- [ ] **Step 1: Write `TestCertificates`.** Generate a self-signed `localhost` PFX and, for mTLS, a CA certificate + a CA-signed client certificate (RSA 2048, SHA-256, SAN DNS `localhost`). Write PFX/PEM to temp paths; implement `IDisposable` cleanup.

- [ ] **Step 2: Write failing tests** in `TlsTransportTests.cs`:
  - `Pfx_certificate_loads_and_reports_a_private_key`.
  - `Pem_certificate_with_key_loads`.
  - `Missing_certificate_file_throws_BrokerConfigurationException`.
  - `Tls_listener_accepts_an_ssl_client_and_relays_bytes` — start `TlsSocketListener` on port 0; connect `TcpClient` + `SslStream` with `RemoteCertificateValidationCallback = (_,_,_,_) => true`; write bytes; read from `TransportSide.Reader`; assert equality.
  - `Bad_handshake_does_not_end_the_accept_stream` — first open a raw TCP socket and send garbage; then connect a valid TLS client; assert the valid client is accepted.
  - `Mutual_tls_rejects_a_client_without_a_certificate`.
  - `Mutual_tls_accepts_a_ca_signed_client_certificate`.
  - `Handshake_timeout_closes_that_socket_and_keeps_accepting` (set `HandshakeTimeout = 200 ms`, connect and send nothing, then a valid client).

- [ ] **Step 3: Implement `BrokerCertificate`.**
  - PFX: `X509CertificateLoader.LoadPkcs12FromFile(path, password, X509KeyStorageFlags.EphemeralKeySet)`.
  - PEM: `X509Certificate2.CreateFromPemFile(certPath, keyPath)`, then re-export as PFX and reload with `EphemeralKeySet` on Windows (keeps `SslStream` happy).
  - mTLS: load CA bundle with `X509Certificate2Collection.ImportFromPemFile`; validation callback builds `X509Chain` with `TrustMode = CustomRootTrust`, `RevocationMode = NoCheck`, adds the CA certs, and returns `chain.Build(cert)`.
  - Protocol helper: `Tls12` → `Tls12 | Tls13`; `Tls13` → `Tls13`.

- [ ] **Step 4: Implement `TlsSocketListener`.** Reuse the socket binder (extract `internal static (Socket Socket, int Port) Bind(int port)` from `SocketListener` into a small `SocketBinder` type or a private static method shared in the same file). Accept loop: on `SocketException`/`ObjectDisposedException` return null; on accepted socket wrap `SslStream(new NetworkStream(socket, ownsSocket: true))`, `AuthenticateAsServerAsync(options, ct).AsTask().WaitAsync(HandshakeTimeout, ct)`; on failure dispose and `continue`; on success return `new TransportSide(PipeReader.Create(ssl), PipeWriter.Create(ssl))`. `DisposeAsync` disposes the socket and certificate.

- [ ] **Step 5: Run → PASS.**

- [ ] **Step 6: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Server src/RedisMQ.Broker/Transport tests/RedisMQ.Broker.Tests/TlsTransportTests.cs tests/RedisMQ.Broker.Tests/TestCertificates.cs
  git commit -m "feat(transport): SslStream listener with PFX/PEM and mutual TLS"
  ```

---

## Task 13: Listener factory, multi-listener server and TLS test host

**Files:**
- Create: `src/RedisMQ.Broker/Transport/BrokerListeners.cs`
- Modify: `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Modify: `tests/RedisMQ.Conformance/TestBrokerHost.cs`
- Test: `tests/RedisMQ.Broker.Tests/BrokerListenersTests.cs` (new)

**Interfaces:**
- Produces `public static IReadOnlyList<IListenerTransport> BrokerListeners.Create(BrokerServerOptions options)`:
  - plaintext `SocketListener(options.Port)` when `options.Port >= 0`;
  - `TlsSocketListener(options.Tls)` when `options.Tls is { Enabled: true }`.
- `AmqpBrokerServer` gains `AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine, IReadOnlyList<IListenerTransport> listeners)`; the existing single-listener ctor delegates. One accept loop per listener; stop cancels/awaits all.
- `TestBrokerHost` exposes `int Port` and `int? TlsPort`, creates listeners through the factory, and disposes every listener it created.

- [ ] **Step 1: Write failing tests** in `BrokerListenersTests.cs`:
  - `Factory_returns_plain_only_when_tls_is_disabled`.
  - `Factory_returns_both_when_tls_is_enabled` (TLS `Port = 0`).
  - `Factory_returns_no_plaintext_when_port_is_minus_one`.
  - `Multi_listener_server_accepts_on_both_ports` (start plain + TLS, connect a raw TLS client, complete the AMQP handshake far enough to read `connection.start`).
  - `Single_listener_constructor_still_starts_one_loop`.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement the factory and multi-listener server.** `StartAsync` starts every accept loop; `StopAsync` cancels and awaits all; the reaper loop (Task 9) is started once. The server does not dispose listeners.

- [ ] **Step 4: Update `TestBrokerHost`.** `Start` sets `Port = 0`; when `options.Tls?.Enabled == true` and `Tls.Port == 0`, keep the OS-assigned port. `Port` comes from the plain listener (or `-1`), `TlsPort` from the TLS listener. `StopAsync` stops the server, then disposes every listener. Both `Start` overloads share the code.

- [ ] **Step 5: Run → PASS** plus the existing conformance suite (plain path unchanged).

- [ ] **Step 6: Commit**
  ```powershell
  git add src/RedisMQ.Broker/Transport/BrokerListeners.cs src/RedisMQ.Broker/Server/AmqpBrokerServer.cs tests/RedisMQ.Conformance/TestBrokerHost.cs tests/RedisMQ.Broker.Tests/BrokerListenersTests.cs
  git commit -m "feat(server): plaintext+TLS listener factory and multi-listener accept loops"
  ```

---

## Task 14: TLS conformance, docs and the full v1 gate

**Files:**
- Create: `tests/RedisMQ.Conformance/TlsFlowTests.cs`
- Create: `tests/RedisMQ.Conformance/TestCertificates.cs` (or reuse via a shared link)
- Modify: `README.md`
- Modify: `docs/API.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/CONFORMANCE.md`
- Modify: `docs/DEVELOPMENT.md`
- Modify: `docs/PROTOCOL.md`
- Modify: `docs/index.md`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Add TLS conformance tests** in `TlsFlowTests.cs`:
  - `Connect_over_tls_and_publish_consume` — start a TLS host with a self-signed PFX; `ConnectionFactory.Ssl = new SslOption { Enabled = true, ServerName = "localhost", AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors }`; endpoint `new AmqpTcpEndpoint("localhost", host.TlsPort!.Value)`; declare, publish, consume.
  - `Mutual_tls_accepts_a_client_certificate` — configure `ClientCertificateRequired`, CA path; client `SslOption.CertPath`/`CertPassphrase`; publish/consume.
  - `Mutual_tls_refuses_a_client_without_a_certificate` — assert the connect attempt throws/fails.

- [ ] **Step 2: Run → PASS** (if the RabbitMQ.Client mTLS surface differs, pin the accept/refuse cases at the transport level in `TlsTransportTests` and keep conformance to server-auth TLS; note the deviation in the plan's execution notes).

- [ ] **Step 3: Update docs.**
  - `README.md`: quick start uses `BrokerUser.FullAccess`; TLS snippet; "fresh Redis store" note; durability contract (Redis confirm = every fan-out `XADD` accepted; memory-only = no durability promise).
  - `docs/API.md`: `BrokerServerOptions.Tls/ConfirmTimeout/ConfirmQueueCapacity/ReaperInterval`; `BrokerTlsOptions`; `BrokerUser`/`BrokerPermission`/`BrokerPassword`; `QueueInfo` arguments.
  - `docs/ARCHITECTURE.md`: receipts, `EngineDelta`, enqueue-first/remove-second, replay dedupe, DLX/TTL, `LastUsed`, permissions, TLS.
  - `docs/CONFORMANCE.md`: confirms, DLX/TTL, permissions, TLS coverage and the known limitations (at-least-once, Redelivered after crash, cert rotation).
  - `docs/DEVELOPMENT.md`: direct runner commands, fresh-store reset (`DEL` all `{mq:*}` keys), Redis-optional skip.
  - `docs/PROTOCOL.md`: queue arguments, relative `expiration`, `x-death` shape, confirm ordering, 403 wording.
  - `docs/index.md`: M4 part-1 status and what remains (profiles, observability, perf, Pika, packaging).

- [ ] **Step 4: Full regression gate.**
  ```powershell
  dotnet build RedisMQ.sln
  dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll
  dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
  dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll
  ```
  Requirements: build 0 errors; every pre-M4 test still green; new tests green; Redis-dependent tests green against a local container (or skipped with a recorded reason if Redis is unavailable).

- [ ] **Step 5: Run the suites a second time** to catch flakes (especially TLS handshake timing and confirm backpressure).

- [ ] **Step 6: Commit**
  ```powershell
  git add tests/RedisMQ.Conformance README.md docs
  git commit -m "test(m4): TLS conformance and v1 completeness regression gate"
  ```

---

## Self-review against the design

1. **Spec coverage.**
   - G1 honest confirms → Tasks 1, 2, 6.
   - G2 restart correctness (ack/reject/DLX/expiry/max-length removal, requeue replacement, id continuity) → Tasks 1, 3, 4, 5, 8.
   - G3 dead-lettering (`x-dead-letter-exchange`, routing key, delivery limit, `x-death`) → Tasks 5, 7, 8.
   - G4 TTL/queue expiry/reaper → Tasks 7, 8, 9.
   - G5 permissions + PBKDF2 → Tasks 10, 11.
   - G6 TLS → Tasks 12, 13, 14.
   - G7 no regression → every task's run step plus Task 14's full gate.

2. **Step granularity.** Every step is one checkable action (write a named test, run to fail, implement one seam, run to pass, commit). No "write tests for the above" placeholders and no algorithm transcripts.

3. **Type consistency.**
   - `EnqueueReceipt` (Task 1) is consumed by `PendingConfirm` (Task 6) and `WriteCommand.Completion` (Task 1).
   - `EngineDelta`/`QueuedMessage`/`QueuedMessageRef` (Task 2) are consumed by `MirrorDelta` (Task 5) and `Reap` (Tasks 8–9).
   - `QueueInfo.Expiration` becomes `TimeSpan?` in Task 7 and is consumed by `Reap` in Task 9; the replay `QueueInfo` construction moves in the same task.
   - `BrokerPrincipal` (Task 10) is consumed by `WireConnection`/`ChannelContext` (Task 11).
   - `IListenerTransport.AnnouncedPort` (Task 12) is consumed by `BrokerListeners`/`TestBrokerHost` (Task 13).
   - `PendingConfirm` lives in `Server/`, so `IChannelDeliveryHost` and `ChannelContext` stay in the same assembly.

4. **Review Focus mapping.** Honest nack → Tasks 1/6; confirm order → Task 6; ack/requeue mirroring → Tasks 3/5; crash window → Task 5; relative TTL → Tasks 7/8; `x-death` → Task 8; queue expiry → Task 9; default deny/vhost-open → Tasks 10/11; TLS accept-loop survival → Task 12.

5. **Proportion.** 14 tasks, four phases, each phase independently green. The design is ~669 lines; this plan is intentionally more granular because the executor has not seen the codebase. No task exceeds one seam plus its tests.

## Deliberate plan decisions (design refinements)

- **Task order inside Phase 1:** codec (Task 4) lands before Redis mirroring (Task 5) because replay dedupe needs `delivery-count`; channel settle wiring (Task 3) lands before mirroring so deltas are actually produced.
- **`WriteCommand.Remove` replaces the design's separate `Ack`/`Drop` kinds.** Both settle to the same Redis action (`XDEL`); the delta carries no reason. `RemoveStreamIds` exists because a requeue's `XADD` changes the map before the old id can be resolved.
- **`WriteCommand.Delta` batches a whole engine delta into one channel admission.** This makes "skip the whole delta if the channel is full" true; individual `TryWrite` calls could partially apply.
- **`x-death` copies are silently dropped when unroutable.** There is no logging surface in v1; the design's "log once per queue" cannot be honoured without inventing one. Documented in `docs/ARCHITECTURE.md`.
- **`DeliveryCount` increments at claim**, and `x-delivery-limit` dead-letters when the current count `>= limit` on requeue. This gives RabbitMQ's observable boundary (limit N allows N deliveries) and is pinned in Task 8.
- **`ConfirmQueueCapacity` is configurable** (default 4096) so the backpressure test can use capacity 1 without a 4096-publish fixture.
- **`BrokerPassword.Verify` takes a `BrokerUser`**, not a raw encoded string, so the plaintext/hash choice stays in one place.
- **The reaper runs once per server**, not per listener, so a plain+TLS deployment sweeps once.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-11-02-redismq-m4-v1-completeness.md`. The 14 tasks form a linear chain across four phases; each phase ends green. Native execution is the recommended path: implement each task in this session, run the per-task class, then run the full gate in Task 14. The highest-risk seams are the confirm loop (Task 6) and replay dedupe (Task 5); both have dedicated failing tests before implementation.
