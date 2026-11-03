# RedisMQ M4 (part 1) — v1 completeness: durable confirms, DLX/TTL, permissions, TLS

Date: 2026-11-02 · Status: builder check resolved; awaiting your read/approval — nothing is built until approved.
Parent spec: `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md`
Predecessors: M0 shell, M1 write-through engine, M2 durability/topology, M3 full exchanges.

---

## 1. Intent and why

M0–M3 made the broker speak the wire and route all four exchange types, but three v1
promises from the parent spec were deferred and never landed, and one of them drifted
into a correctness bug:

1. **Publisher confirms are not honest.** `confirm.ack` is emitted when a message
   lands in `InMemoryEngine`, before the Redis write-ahead mirror appends it.
2. **Consumer acks never reach Redis.** Replay re-adds every message ever published,
   so a restart resurrects acked messages; `Requeue` appends a second copy, so
   requeued messages duplicate on replay.
3. **Dead-lettering and TTL expiry are absent.** No `x-dead-letter-exchange`,
   `x-delivery-limit`, `x-expires`, or reaper loop exists; `RemoveExpiredReadyMessages`
   is only called by tests.
4. **Security surface is incomplete.** PLAIN accounts are plaintext strings with no
   per-vhost `configure`/`write`/`read` permissions, and there is no TLS.

This part of M4 makes the broker honest enough to call itself v1: confirms mean what
they say, restart does not resurrect settled work, dead-lettering and expiry behave,
accounts are constrained, and the wire can be encrypted. Capability profiles,
observability, the perf gate, Pika conformance and packaging remain the later M4
parts and are out of scope here.

## 2. Goals

- **G1 — Honest confirms.** In the Redis engine, `confirm.ack` is emitted only after
  every fan-out `XADD` for that publish has been accepted by Redis; failures emit
  `confirm.nack`. The in-memory engine keeps immediate confirms.
- **G2 — Restart correctness.** Acked, rejected-without-requeue, dead-lettered,
  expired and max-length-dropped messages are removed from Redis, so replay restores
  only live messages. Requeue replaces the original entry (no duplicate on replay).
  Envelope ids survive replay and never collide with new messages.
- **G3 — Dead-lettering.** `x-dead-letter-exchange`, `x-dead-letter-routing-key` and
  `x-delivery-limit` work for reject/nack-without-requeue, requeue-limit overflow and
  TTL expiry, with `x-death` headers.
- **G4 — TTL / queue expiry.** A reaper honours per-message `expiration` and queue
  `x-expires`, deleting/expiring state and mirroring it to Redis.
- **G5 — Permissions.** RabbitMQ-style per-vhost `configure`/`write`/`read` regexes
  enforced at declare/bind/publish/consume/get, default-deny, with PBKDF2 password
  hashes as the preferred configured form.
- **G6 — TLS.** Listeners can serve AMQP over TLS 1.2+ via `SslStream`, with optional
  mutual TLS, on a separate port; plaintext remains available for development.
- **G7 — No regression.** M0–M3 unit, Redis and conformance suites stay green.

## 3. Non-goals

- Capability profiles, durability-contract matrix per Redis deployment, OTel metrics,
  health checks, perf tuning, Pika conformance, packaging, containers and public
  NuGet publishing (later M4 parts).
- Multi-node active-active, consumer groups/PEL as the live store, `XAUTOCLAIM`
  reclaim, Redis-backed acks. Approach D remains: `InMemoryEngine` is the live system
  of record and Redis is the durable mirror.
- Management UI, wire-level user administration, vhost CRUD.
- `x-message-ttl`, `x-overflow`, priority queues, transactions.
- Certificate hot-reload, ACME, OCSP/CRL fetching, SASL EXTERNAL.
- Guest loopback restriction (documented as a deviation, not implemented).

## 4. Constraints

- `RedisMQ.Broker` keeps zero `PackageReference`s. Redis client stays only in
  `RedisMQ.Redis`. TLS uses `System.Net.Security.SslStream` from the BCL.
- `IRouteEngine` stays synchronous on the protocol path; expensive work stays behind
  the drainer/writer chains. No blocking sync-over-async on Redis I/O; the only
  intentional block is bounded in-memory confirm-queue backpressure (§7.4).
- Cluster-safe key layout is preserved (every per-queue key hash-tagged), but this
  design does not implement multi-node coordination.
- Existing wire wording, M0/M3 deviations and error codes are preserved unless this
  document explicitly changes them.
- Tests must run without Redis where they do today; Redis-dependent tests skip when no
  Redis is reachable, and get a real container in CI.

## 5. Approaches considered

The approach choices were agreed before this document:

| Area | Chosen | Rejected alternative and why |
|---|---|---|
| Confirms/durability | Add a per-publish durability receipt to the engine seam; the channel's confirm loop awaits receipts before writing `confirm.ack`. Mirror settles (`Ack`/`Reject`/`Drop`) and requeues to Redis. | Writing to Redis synchronously before acking would put Redis latency on the frame pump and break the "never block the pump" invariant. |
| Dead-lettering | Engine-owned: queue arguments + `DeliveryCount` on messages; the engine dead-letters on reject/requeue-limit/expiry, and the Redis engine mirrors the resulting enqueues/settles. | Channel-side dead-lettering cannot serve the TTL reaper. |
| Permissions | Static configuration on `BrokerUser` (per-vhost regex triple), enforced in `ChannelContext`; PBKDF2 hashes in config. | Topology-store permissions with versioned caches need a wire admin API that v1 does not have; defer. |
| TLS | Listener-level `SslStream`, separate TLS port, optional mTLS. | Proxy termination leaves the spec's `SslStream`/mTLS scope unmet. |
| Order | Confirms/message model → DLX/TTL → permissions → TLS. | DLX depends on correct settle/requeue semantics, so it follows the confirm model. |

## 6. Current state and the exact gaps

Grounding from the code:

- `IRouteEngine.Enqueue` returns `void`; `InMemoryEngine` assigns the envelope id
  internally. `RedisStreamRouteEngine.Enqueue` writes `EnvelopeId = 0` to the drainer.
- `WriteCommand` has no `Ack`, `Drop` or `DeleteQueue` handling in
  `WriteCommandToRedisAsync` (the switch has no `DeleteQueue` arm), and replay ignores
  `Unbind` and `DeleteQueue` topology entries.
- `ChannelContext.SettleLedger` handles ack and requeue entirely in memory; acks never
  call the engine. No-ack consumers and `basic.get(autoAck:true)` never mirror a
  settle.
- `queue.declare` accepts only `x-max-length`; `x-expires`, DLX args and delivery-limit
  are refused with the M0 `NOT_IMPLEMENTED` deviation.
- `QueueInfo.Expiration` exists but nothing parses `x-expires`; no reaper runs.
- `BrokerUser(Name, Password)` only; `WireConnection` compares plaintext passwords;
  `ChannelContext` has no principal.
- `SocketListener` accepts plain sockets only; `AmqpBrokerServer` takes one listener.

## 7. Design — durable message identity, settles and honest confirms

### 7.1 Envelope identity and the engine receipt

`Enqueue` gains a return value:

```csharp
public readonly record struct EnqueueReceipt(
    ulong EnvelopeId,
    ValueTask<bool> Durable,          // await at most once
    IReadOnlyList<ulong> DroppedEnvelopeIds); // max-length drops, for Redis mirroring
```

- `InMemoryEngine.Enqueue` returns the assigned id and a completed `true` receipt.
  `DroppedEnvelopeIds` lists ready messages removed by `x-max-length` in that queue.
- `RedisStreamRouteEngine.Enqueue`:
  1. reserves one of `drainerCapacity` slots (`SemaphoreSlim.Wait(0)`); if none is
     free, returns `EnvelopeId = 0` with an already-false `Durable` **without touching
     memory** — a failed target must not be left in a queue it cannot persist (a
     partially routed publish is the documented fan-out risk in §14);
  2. calls `_memory.Enqueue`, gets the id and drops;
  3. creates a `TaskCompletionSource<bool>(RunContinuationsAsynchronously)`;
  4. writes a `WriteCommand.Enqueue` with the real `EnvelopeId`, the durability
     completion, and the dropped ids;
  5. returns the receipt over the TCS.
- The drainer releases the reserved slot when it finishes (or fails) that command, so
  the channel can never be full while a slot is held; the semaphore makes the failure
  happen *before* memory mutation rather than after. A failed `TryWrite` during
  shutdown completes the receipt `false` (the publish was already in memory; the
  process is stopping).
- When the engine was constructed with `redis: null` (memory-only deployment or tests),
  there is no drainer and every receipt completes `true`. This is documented as
  memory-only semantics: the engine makes no durability promise without Redis.
- `InMemoryEngine.ReplayEnqueue` advances `_lastEnvelopeId` so replayed ids are never
  reused by new publishes. No separate persisted counter is needed: if all queue
  streams are empty there are no old ids to collide with, and otherwise the maximum
  replayed id is the starting point.

### 7.2 Engine deltas and settle mirroring

Every mutating engine operation that touches live messages returns an `EngineDelta`
describing what Redis must do. This keeps the ordering rule in one place instead of
scattering it across settle, requeue, DLX and the reaper:

```csharp
public sealed record QueuedMessageRef(string Vhost, string Queue, ulong EnvelopeId);

public sealed record QueuedMessage(string Vhost, string Queue, InMemoryMessage Message);

public sealed record EngineDelta(
    IReadOnlyList<QueuedMessage> Enqueued,   // XADD these first
    IReadOnlyList<QueuedMessageRef> Removed, // then XDEL these
    IReadOnlyList<(string Vhost, string Name)> DeletedQueues);
```

The mirroring rule is **enqueue/mutate first, remove second**:

- `XADD` every `Enqueued` entry, then `XDEL` every `Removed` entry.
- If the drainer dies or Redis fails between the two steps, replay sees a duplicate
  (old and new entry) but never loses a message. Replay collapses duplicates by
  envelope id (§7.5).
- Ack → `Removed` only. Reject without DLX → `Removed` only. Max-length drop →
  `Removed` only. Requeue → `Enqueued` (same id, incremented delivery count,
  `redelivered = true`) then `Removed` (old stream id). DLX → `Enqueued` (new id)
  then `Removed` (original).
- Because a requeue reuses the envelope id, the drainer resolves and stashes the
  existing stream id for every `Removed` entry **before** applying the `XADD`s, then
  records the new id(s) and XDELs the stashed ids afterwards. Otherwise the map update
  for the new entry would erase the old id and leak the old stream entry.

Drainer details:

- `WriteCommand` carries an optional `TaskCompletionSource<bool>? Completion` that is
  never serialized. The single drainer completes it `true` after the Redis call
  succeeds and `false` on any exception **before** its existing best-effort catch
  swallows the failure. Topology/settle commands leave it null but are still swallowed
  on failure (they are retried implicitly only by replay).
- The drainer keeps a per-queue map `(vhost, queue, envelopeId) → Redis stream id`,
  populated from each `XADD` result and from replay. It is bounded by live messages:
  entries are removed on settle, requeue and max-length drop.
- Topology `DeleteQueue` handling is added: `DEL {mq:q:<vhost>:<queue>}` plus a
  topology tombstone; replay applies `Unbind` and `DeleteQueue` entries in stream
  order, and a deleted queue's bindings are dropped with it.
- Settle/reap mirroring is best-effort: a settle command uses `TryWrite` on the drainer
  channel. If the channel is full the whole delta is skipped (Redis keeps its
  pre-settle state), which can duplicate on replay but never loses a message. This is
  the only place a *settle* is dropped; a refused enqueue (§7.1) is a failed publish,
  not a dropped settle.
- Max-length is mirrored by `Removed` deltas for `DroppedEnvelopeIds` (exact, no
  `XTRIM`), so the stream and memory agree on exactly which messages were dropped.
  Every engine enqueue that can evict (publish, requeue, DLX copy) reports the evicted
  ids in the same `EngineDelta.Removed` list.

### 7.3 Auto-ack paths

No-ack consumers and `basic.get(autoAck:true)` must mirror a settle when the message is
handed out:

- `ConsumerEntry.TryAdmitForDelivery` calls `_engine.Acknowledge(...)` for no-ack
  admissions (inside the pump, after the ledger decision).
- `OnBasicGet` calls `_engine.Acknowledge(...)` when `no-ack` is true.
- Acked ledger entries call `_engine.Acknowledge(...)`; requeued entries call
  `_engine.Requeue(...)` (which mirrors the replace); rejected entries call
  `_engine.Reject(...)`. Each call returns an `EngineDelta`; `RedisStreamRouteEngine`
  mirrors it with the enqueue-first/remove-second rule, while `ChannelContext` only
  ever sees the synchronous memory result.
- These extra engine calls happen on the protocol path but do no I/O: they mutate
  memory and `TryWrite` a bounded command. A full drainer skips the mirror as described
  in §7.2; it never blocks the frame pump.

### 7.4 The async confirm pipeline

Confirms must not block the frame pump on Redis, and every publish outcome on a
confirm-mode channel must be emitted in publish order.

**One ordered path for every outcome.** `ChannelContext` never writes a confirm inline.
In confirm mode, `FinishPublish` builds exactly one `PendingConfirm` for *every*
publish, including unroutable and mandatory-unroutable publishes:

```csharp
internal sealed record PendingConfirm(
    ChannelContext Channel,          // identity, not just the channel number
    ushort ChannelNumber,
    long Sequence,
    IReadOnlyList<EnqueueReceipt> Receipts,
    ChannelOutcomeBasicReturn? ReturnFirst); // mandatory-unroutable return, if any
```

- Routed publish → one receipt per target queue.
- Unroutable non-mandatory → empty receipt list (nothing to persist, acks
  immediately once its turn arrives).
- Mandatory-unroutable → `ReturnFirst` carries the basic.return; the loop writes the
  return, then the ack for the same sequence. This preserves RabbitMQ's
  return-before-confirm order and keeps it inside the single ordered stream.
- Non-confirm mode keeps today's inline `basic.return` and no confirms.

`IChannelDeliveryHost` gains
`void EnqueueConfirm(PendingConfirm confirm)`. `ChannelContext` holds a reference to
its host (already does) and hands the entry over before returning
`ChannelOutcomeSilent`.

**The loop.** `WireConnection` owns exactly one confirm loop, started when the
connection enters `Serving` and cancelled on teardown:

- a bounded `Channel<PendingConfirm>` (default 4096). If full, `EnqueueConfirm`
  deliberately blocks the frame pump on `WriteAsync` until the loop drains. This is
  backpressure, not a drop and not a nack: the client stops being read, Redis receives
  no new publishes, and no confirm is invented. It never blocks on Redis itself.
- the loop takes entries in arrival order, awaits every receipt in the entry (overall
  timeout `BrokerServerOptions.ConfirmTimeout`, default 5 s), then schedules the
  outcome through a new internal `ChannelContext.ScheduleConfirm` built on the existing
  `ScheduleWriterJob`/`RunWriterChainAsync` writer chain, so confirms interleave
  correctly with deliveries and replies.
- all receipts `true` (or the list is empty) → `confirm.ack(sequence, multiple=false)`;
  any receipt `false`, exception, or timeout → `confirm.nack(sequence, multiple=false,
  requeue=false)`.
- **Liveness:** `PendingConfirm` carries the `ChannelContext` object. Before writing,
  the loop checks the channel is still the connection's current, open context for that
  number (channels are retired and re-created under the same number). A retired or
  replaced channel's entry is discarded silently; its sequence is never reused.
- **Timeout is not cancellation.** A timed-out `XADD` may still land, so a nack can be
  followed by a durable message. That is at-least-once, and is documented, not fixed.

Because `InMemoryEngine` receipts already complete, in-memory tests and conformance
keep fast confirms through the same code path. A publish whose receipt failed
(drainer full or Redis error) is nacked rather than acked — never the reverse.

The durability contract is stated in docs: **Redis engine confirm = every fan-out
`XADD` accepted by Redis** (persistence beyond Redis memory follows deployment
config, documented in the later profiles part); **in-memory engine confirm = landed
in memory**.

### 7.5 Stream encoding and replay

**Content properties are stored in AMQP wire form, not JSON.** `ContentProperties`
contains a `FieldTable` for `Headers`, and `System.Text.Json` cannot round-trip
`FieldValue` (nested tables, arrays, byte arrays, timestamps). The content header is
therefore encoded with the existing `ContentHeaderCodec.Write` (class 60, body size,
properties) and the resulting bytes stored base64 in a `props` stream field; replay
decodes them with `ContentHeaderCodec.Read`. This preserves every property and every
header type exactly, and is what makes `x-death` replay possible.

`StreamCodec` fields become:

| Field | Encoding |
|---|---|
| `envelope-id` | `ulong` decimal |
| `exchange`, `routing-key` | UTF-8 |
| `props` | base64 of `ContentHeaderCodec.Write(60, body.Length, properties)` |
| `body` | base64 |
| `expires-at` | Unix ms decimal, omitted when absent (absolute deadline, see §8.2) |
| `delivery-count` | `int` decimal |
| `redelivered` | `0`/`1` |

Replay rules:

- Replay reads queue streams as today, but settled entries are gone (XDEL) and
  requeues write the new entry before deleting the old one, so a clean shutdown has no
  duplicates.
- If a crash landed between a requeue's `XADD` and `XDEL`, two entries carry the same
  envelope id. Replay collapses them, keeping the entry with the higher
  `delivery-count` (then the later stream id), and XDELs the stale entries during
  replay. This both prevents duplicates and stops the stale copy resurrecting after the
  surviving message is later settled.
- Replay restores `DeliveryCount` and `Redelivered` from the stream.
- Replay records each entry's stream id into the drainer map so a later ack can XDEL
  it.
- Known limitation: a message delivered but unacked when the process crashed replays
  as ready, but `Redelivered` may be `false` because delivery state is not persisted.
  At-least-once still holds. Documented, not fixed.

## 8. Design — expiry, dead-lettering and the reaper

### 8.1 Queue arguments

`queue.declare` accepts (alongside `x-max-length`):

| Argument | Type | Meaning |
|---|---|---|
| `x-dead-letter-exchange` | shortstr | Exchange in the same vhost used for dead-lettered copies. |
| `x-dead-letter-routing-key` | shortstr | Overrides the original routing key for the copy. |
| `x-delivery-limit` | integral ≥ 0 | Requeue attempts before dead-lettering. Absent = no limit. |
| `x-expires` | integral ≥ 0 (ms) | Delete the queue after this long without consumers/use. |

- Unknown arguments keep the M0 `NOT_IMPLEMENTED - arguments are not supported ...`
  answer.
- Non-integral/negative values → `PRECONDITION_FAILED` with the arg name.
- Redeclare equivalence compares each new field; mismatches use the existing
  inequivalent-arg wording.
- `x-dead-letter-exchange` absent = no dead-lettering. An explicitly empty string is
  the **default exchange**, not "no DLX" (RabbitMQ semantics). `x-dead-letter-routing-key`
  absent = keep the original routing key; an explicit empty string is the empty key.
- `x-expires` is an **idle duration**, not a deadline: `QueueInfo` replaces its unused
  `DateTimeOffset? Expiration` with `TimeSpan? Expiration`, set from the argument.
  `LastUsed` is in-memory state in `InMemoryEngine` (a `(vhost, queue) → DateTimeOffset`
  map), touched on declare/bind/enqueue/claim/consume/get; it resets on restart, so a
  queue restarts its idle clock rather than expiring immediately.
- `QueueInfo` also gains `DeadLetterExchange`, `DeadLetterRoutingKey`, `DeliveryLimit`.

### 8.2 Delivery count, TTL and dead-letter triggers

`InMemoryMessage` gains `int DeliveryCount = 0`. Its existing `Expiration` field stays
as the **absolute** deadline.

**Per-message TTL is relative.** The AMQP `expiration` property is a string of
milliseconds *from enqueue*. `InMemoryEngine.Enqueue` parses it and computes
`Expiration = now + ms` (the current code wrongly treats the number as an absolute Unix
timestamp). Rules:

- Requeue keeps the original absolute deadline; a message that expired while in flight
  is expired as soon as it becomes ready again.
- A dead-letter copy clears `Expiration` and the `expiration` property (§8.3).
- Unacked messages never expire in the reaper (RabbitMQ semantics); only ready messages
  are swept, and a message redelivered after expiry is handled by the ready sweep.
- An unparseable or negative `expiration` leaves the message without a deadline
  (current M1 behavior, pinned by tests).

Delivery and dead-letter triggers:

- **Reject/nack(requeue:false):** if the queue has a DLX, dead-letter with reason
  `rejected`; otherwise drop. Both mirror a settle to Redis (§7.2).
- **Requeue/recover/teardown:** increment `DeliveryCount`. If
  `DeliveryLimit is int limit && next > limit`, dead-letter with reason `rejected`
  (delivery-limit overflow); otherwise requeue with the incremented count. The boundary
  (`DeliveryCount` counts deliveries; the limit is the last allowed delivery) is pinned
  by tests.
- **TTL expiry (ready messages):** the reaper dead-letters with reason `expired` when
  a DLX exists, else drops.
- **Queue expiry:** the reaper deletes an idle queue (no consumers and `now >=
  LastUsed + x-expires`). Deleting the queue removes its queue record, delivery state,
  bindings and Redis stream, and appends a topology tombstone. Messages still in the
  queue are **deleted, not dead-lettered** (RabbitMQ queue expiry has no DLX path).
- **Max-length:** still drop-oldest; the `x-overflow: reject-publish*` forms remain
  v1.1 non-goals.

### 8.3 Dead-letter construction

- Resolve `DeadLetterExchange` in the same vhost; an explicit `""` resolves to the
  default exchange. A named exchange that does not exist, or a DLX with no matching
  binding, means the copy is unroutable and is dropped (no publisher error), matching
  RabbitMQ; log once per queue.
- Routing key = `DeadLetterRoutingKey ?? originalRoutingKey`; for the default exchange,
  that key addresses the destination queue directly.
- Build the copy's properties from the original and add `x-death`:
  - `x-death` is an AMQP array (`A`) of tables; each entry carries `queue` (`S`),
    `reason` (`S`), `count` (`l`, signed 64-bit), `time` (`T`, timestamp). Entries are
    ordered newest first, as RabbitMQ does.
  - Increment the matching `(queue, reason)` entry if present; otherwise prepend one.
  - Set `x-first-death-queue`, `x-first-death-reason` and `x-first-death-exchange`
    once, from the first death.
- Clear `expiration` on the copy (both `ContentProperties.Expiration` and
  `InMemoryMessage.Expiration`) so it is not immediately expired in the DLX.
- **Reset `DeliveryCount` to 0 on the copy**, so the dead-letter target gets its own
  delivery budget.
- Loop guard: if the origin queue is also a DLX target and the message already has an
  `x-death` entry for `(origin, reason)`, drop instead of re-dead-lettering.
- Each copy is enqueued through the engine's internal enqueue path, assigning a new
  envelope id, and is returned in the `EngineDelta.Enqueued` list; the Redis engine
  applies it before removing the original (§7.2). Because properties are persisted in
  AMQP wire form (§7.5), `x-death` survives restart and further dead-letters.

### 8.4 The reaper

- `IRouteEngine` gains `EngineDelta Reap(DateTimeOffset now)`.
- `InMemoryEngine.Reap` performs the whole sweep under its lock: expire ready messages
  (dead-letter or drop), delete idle queues, and return one `EngineDelta` with the
  enqueued copies, removed ids and deleted queues. `RedisStreamRouteEngine.Reap`
  delegates to memory and mirrors that delta with the enqueue-first/remove-second rule.
- `AmqpBrokerServer` owns one loop (`PeriodicTimer`,
  `BrokerServerOptions.ReaperInterval`, default 5 s, `0` disables) started with the
  server and cancelled on stop, so both engines get expiry in tests and conformance.
  The loop is shared by all listeners.
- Unacked messages do not expire until requeued (RabbitMQ semantics); only ready
  messages are swept.

## 9. Design — per-vhost permissions

### 9.1 Configuration model

```csharp
public sealed record BrokerPermission(string Vhost, string Configure, string Write, string Read);

public sealed record BrokerUser(
    string Name,
    string? Password = null,      // plaintext, dev only
    string? PasswordHash = null,  // pbkdf2-sha256$iterations$salt$hash
    IReadOnlyList<BrokerPermission>? Permissions = null); // null/empty = deny
```

- Regexes are compiled once at startup with `CultureInvariant` and a 1 s match timeout;
  invalid patterns throw `BrokerConfigurationException`.
- `.*` grants everything; an empty pattern matches only the empty name (effectively
  deny). Documented.
- Exactly one of `Password`/`PasswordHash` must be set; validate at startup.
- `BrokerUser.FullAccess(name, password, params vhosts)` is the explicit test/sample
  helper. Default-deny is a breaking change: the six test/sample construction sites are
  updated.
- PBKDF2: SHA-256, 210,000 iterations, 16-byte salt, 32-byte hash, encoded as
  `pbkdf2-sha256$210000$<base64 salt>$<base64 hash>`. `BrokerPassword.Hash` and
  `Verify` (fixed-time compare) live in core.

### 9.2 Principal propagation

- `WireConnection` resolves the `BrokerUser` at StartOk, verifies the password
  (plaintext or PBKDF2), and keeps a `BrokerPrincipal` (user name + compiled
  permissions).
- `connection.open` refuses a vhost the principal has **no permission entry** for, with
  `403 ACCESS_REFUSED - access to vhost '<vhost>' refused for user '<user>'`. A vhost
  listed in `BrokerServerOptions.Vhosts` is not automatically openable; the user needs
  at least one entry (even a deny-all one counts as "known"). This closes the current
  hole where any authenticated user can open any configured vhost.
- `ChannelContext` receives the same `BrokerPrincipal` at creation.

### 9.3 Enforcement

Permissions are checked before existence/equivalence checks so a restricted user cannot
probe names.

| Method | Permission |
|---|---|
| `exchange.declare` (active or passive) | `configure` on the exchange name |
| `queue.declare` (active or passive) | `configure` on the actual queue name (after server generation) |
| `queue.bind` / `queue.unbind` | `configure` on the queue **and** `write` on the exchange |
| `basic.publish` | `write` on the exchange name; for the default exchange, `write` on the routing key |
| `basic.consume` / `basic.get` | `read` on the queue |
| ack/nack/reject/recover/qos/cancel/channel methods | no check |

`connection.open` is checked in `WireConnection` before the vhost is accepted (see
§9.2); the table covers channel methods only.

- Refusal: soft channel close `403 ACCESS_REFUSED` with
  `ACCESS_REFUSED - access to <exchange|queue> '<name>' in vhost '<vhost>' refused for user '<user>'`,
  class/method of the offending frame.
- Internal dead-letter enqueues are **not** re-checked against `write` permission: the
  user who declared the queue already held `configure` on it, and RabbitMQ does not
  re-authorize broker-internal routing.
- The default-exchange declare refusal and reserved `amq.*` refusals keep their
  existing wording and ordering where they do not conflict with the table above.

## 10. Design — TLS

### 10.1 Configuration

```csharp
public sealed class BrokerTlsOptions
{
    public bool Enabled { get; set; }
    public int Port { get; set; } = 5671;
    public string? CertificatePath { get; set; }     // PFX, or PEM certificate
    public string? CertificateKeyPath { get; set; }  // PEM private key (when CertificatePath is PEM)
    public string? CertificatePassword { get; set; } // PFX password
    public bool ClientCertificateRequired { get; set; }
    public string? ClientCertificateAuthorityPath { get; set; } // PEM CA bundle for mTLS
    public SslProtocols MinTlsVersion { get; set; } = SslProtocols.Tls12;
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
```

- `BrokerServerOptions.Tls` holds this; plaintext is still served on `Port` when
  enabled, so a deployment can run 5672 + 5671.
- Startup validation: certificate path exists; PEM needs a key path; PFX needs a
  password when encrypted; CA path exists when mTLS is required.

### 10.2 Listener and server

- New `TlsSocketListener : IListenerTransport`:
  - binds a socket exactly like `SocketListener`;
  - on accept, wraps the socket in `SslStream` and awaits
    `AuthenticateAsServerAsync(SslServerAuthenticationOptions)`;
  - on handshake failure closes that socket and keeps accepting (it does not end the
    accept stream);
  - on success returns `new TransportSide(PipeReader.Create(ssl), PipeWriter.Create(ssl))`.
- `BrokerCertificate` loads PFX or PEM (`X509Certificate2.CreateFromPemFile` +
  `CopyWithPrivateKey` where needed) once at startup.
- mTLS validation uses an `X509Chain` with `CustomRootTrust` populated from the CA
  bundle, `RevocationMode = NoCheck`.
- `IListenerTransport` gains `int? AnnouncedPort { get; }` (default interface member
  returning null). `SocketListener` and `TlsSocketListener` report their bound port;
  in-proc test listeners return null. This is how callers discover ephemeral TLS
  ports.
- A small `BrokerListeners.Create(BrokerServerOptions)` factory returns the listener
  set the options describe: plaintext on `Port` when `Port >= 0`, TLS on `Tls.Port`
  when `Tls.Enabled` (`Port = -1` disables plaintext). **The caller owns the returned
  listeners and disposes them**; `AmqpBrokerServer` still only consumes them (its
  `StopAsync` drains connections, it does not dispose listeners), matching today's
  `TestBrokerHost` contract.
- `AmqpBrokerServer` gains a multi-listener constructor (`IReadOnlyList<IListenerTransport>`)
  and runs one accept loop per listener; the existing single-listener ctor wraps it.
  Stop cancels all loops and drains all connections.
- `TestBrokerHost` uses the factory, exposes both `Port` and `TlsPort`, and disposes
  every listener it created.
- The AMQP handshake and PLAIN flow are unchanged; TLS protects the credentials in
  transit. Docs recommend TLS whenever hashed accounts are configured.

## 11. Public API and file impact

| File / project | Change |
|---|---|
| `Memory/IRouteEngine.cs` | `Enqueue` returns `EnqueueReceipt`; add `EngineDelta`, `QueuedMessage`, `QueuedMessageRef`; `Acknowledge`/`Reject`/`Requeue` return `EngineDelta`; add `Reap`; `QueueInfo.Expiration` becomes `TimeSpan?`, gains DLX fields. |
| `Memory/InMemoryEngine.cs` | Envelope receipt, dropped ids, delivery count, DLX routing/`x-death`, relative TTL, queue expiry/`LastUsed` map (and `GetQueueExpiry` returns `TimeSpan?`), replay dedupe + id tracking, binding cleanup on delete. |
| `Memory/InMemoryMessage.cs` | `DeliveryCount` (absolute `Expiration` stays). |
| `Memory/InMemoryQueue.cs` | Return dropped messages; expired-message collection returns the messages, not just a count. |
| `Server/ChannelContext.cs` | Confirms via `PendingConfirm`; ack/auto-ack/reject mirroring; permission checks; DLX-aware settle. |
| `Server/WireConnection.cs` | Principal capture, vhost-open check, confirm loop (`PendingConfirm` queue + liveness), starts/stops it with the connection. |
| `Server/BrokerServerOptions.cs` | `Tls`, `ConfirmTimeout`, `ReaperInterval`; `BrokerUser`/`BrokerPermission` reshape. |
| `Server/BrokerPrincipal.cs` (new) | User + compiled permissions + PBKDF2 verification. |
| `Server/AmqpBrokerServer.cs` | Multi-listener accept loops + reaper loop. |
| `Transport/TransportPair.cs` | `IListenerTransport.AnnouncedPort`. |
| `Transport/BrokerListeners.cs` (new) | Plaintext/TLS listener factory. |
| `Transport/TlsSocketListener.cs` (new) | `SslStream` accept path. |
| `Transport/BrokerCertificate.cs` (new) | PFX/PEM loading and chain validation. |
| `RedisMQ.Redis/WriteCommand.cs` | Real envelope id, delivery count, `Ack`/`Drop`/`Requeue`, `DeleteQueue` serialization, non-serialized `Completion`. |
| `RedisMQ.Redis/RedisStreamRouteEngine.cs` | Receipts/TCS, stream-id map, `EngineDelta` mirroring (enqueue first, remove second), tombstone replay, `Reap`, replay dedupe. |
| `RedisMQ.Redis/StreamCodec.cs` | AMQP-wire properties, `expires-at`, `delivery-count`, `redelivered`. |
| `RedisMQ.Redis/RedisKeys.cs` | Only if `DeleteQueue` needs a new key; per-queue stream key reused. |
| tests | New engine/wire/permission/TLS tests; Redis integration tests; M0–M3 regression. |

## 12. Test plan

**Unit (Broker.Tests, no Redis)**
- Envelope receipt ids monotonic and returned; replay ids cannot collide.
- `StreamCodec` round-trips properties through AMQP wire form, including a `Headers`
  table with nested table, array, timestamp, byte-array and decimal values (the cases
  JSON loses); `expires-at`, `delivery-count` and `redelivered` round-trip.
- Relative TTL: `expiration: "1000"` means now + 1 s; requeue keeps the deadline; DLX
  copy clears it; unparseable/negative values leave no deadline.
- DLX: reject/nack without requeue routes with `x-death`; requeue-limit overflow
  dead-letters; expired ready messages dead-letter; missing exchange drops; explicit
  `""` uses the default exchange; `x-death` increments/prepends and `x-first-death-*`
  is set once; `DeliveryCount` resets on the copy; loop guard.
- `x-expires`: idle queue deleted, active/consuming queue kept, bindings removed,
  messages deleted (not dead-lettered), `LastUsed` touches.
- Permissions: allow/deny matrix for configure/write/read; default deny; 403 wording;
  default-exchange publish checks the routing key; vhost-open denied without an entry;
  PBKDF2 round-trip and malformed hashes.
- TLS: cert loader PFX/PEM, invalid config throws; chain validation unit tests.
- Confirm ordering: receipts complete out of order → acks still emitted in sequence;
  unroutable publishes still occupy their sequence slot; mandatory return precedes its
  ack; false/timeout → nack; a full confirm queue blocks (backpressure) instead of
  nacking; a retired channel's pending confirms are discarded.

**Wire (Broker.Tests raw client)**
- confirms: Redis-independent fake receipt engine proves ack delayed until receipt;
  a failed receipt nacks; an unroutable publish between two routed publishes acks in
  order.
- permissions over the wire: declare/bind/publish/consume/get refused with 403;
  `connection.open` on a permission-less vhost refused.
- DLX and x-expires over the wire with `InMemoryEngine`.

**Redis integration (Redis.Tests, skip without Redis; real container in CI)**
- ack/reject/requeue mirroring: restart replays only live messages; no duplicates.
- real envelope ids, delivery counts and `Redelivered` survive replay.
- a requeue crash-window simulation (new entry present and old entry not yet deleted)
  is collapsed on replay and the stale stream entry is XDELed.
- max-length drops are removed from Redis.
- delete/unbind tombstones survive replay; a deleted queue's bindings do not come back.
- `x-dead-letter-exchange` survives replay and dead-lettered copies (with `x-death`)
  land in Redis.
- queue expiry removes the stream and replays as deleted.
- confirm receipt completes only after `XADD`; a failed `XADD` nacks.

**Conformance (RabbitMQ.Client)**
- all existing 15 tests stay green; confirm pipeline still emits ordered acks.
- TLS host variant: connect with `SslOption`, publish/consume, mTLS accept/refuse.

**Regression**
- M0–M3 Broker.Tests, Conformance, Redis.Tests; `dotnet build RedisMQ.sln` 0 errors.

## 13. Acceptance gates

1. Build 0 errors; all existing suites green.
2. A restart test proves acked messages do not come back and requeued messages appear
   once.
3. A Redis-down or drainer-full test proves `confirm.nack` is emitted, not a false
   `confirm.ack`.
4. DLX/TTL/permissions/TLS scenarios pass over the wire.
5. Redis integration suite green against a local container; conformance green.

## 14. Risks and known limitations

- **At-least-once everywhere on failure.** A crash between memory settle and Redis
  `XDEL`, a full drainer dropping a settle, or a confirm timeout whose `XADD` lands
  later can all produce a duplicate. None can lose a message.
- **Settle mirroring is best-effort under drainer pressure:** a skipped delta leaves
  Redis in its pre-settle state (duplicate after restart), never loss. Enqueue
  durability, by contrast, is confirmed.
- **Partial fan-out on failure:** a publish is acked only when *every* target `XADD`
  succeeds. If one target fails, the publish is nacked but targets that succeeded
  already hold the message, so a client retry duplicates them. At-least-once, and
  unavoidable without a two-phase commit.
- **Confirm-queue backpressure stalls publishing** when Redis cannot keep up: the
  frame pump blocks on the bounded confirm queue. This is deliberate and visible
  (throughput drops, no false acks); it is not a deadlock because the confirm loop
  never waits on the frame pump.
- **`Redelivered` after restart** may be false for messages that were in flight.
- **Replay dedupe** collapses duplicate envelope ids within one queue only; envelope
  ids are already unique per fan-out target, so this is the only duplicate source.
- **Queue expiry deletes messages** with no dead-letter path, and `LastUsed` resets on
  restart (a restart gives every idle queue a fresh window).
- **Dead-letter loop guard** is per `(queue, reason)`; exotic cycles across multiple
  queues could still be constructed and are bounded by `x-death` inspection.
- **TLS certificate rotation** needs a restart in v1.
- **Permissions default-deny** is a breaking config change; all samples/tests move to
  `FullAccess`.
- **Delivery-limit boundary** follows RabbitMQ as observed in tests; the exact
  off-by-one is pinned early in implementation.
- **No guest loopback restriction** remains a documented deviation.

## 15. What is left to the builder

- Exact task split and commit sequence for the four phases (next: the plan doc).
- Redis stream-id map eviction details under extreme max-length churn (the map is
  bounded by live messages; a dropped `Removed` leaves stale ids that a later replay
  XDELs, but the exact opportunistic sweep is a plan decision).
- Whether to expose a small PBKDF2 hashing helper in the future standalone tool (not
  in this part).

Defaults pinned by this design: confirm-queue capacity 4096, confirm timeout 5 s,
reaper interval 5 s, regex match timeout 1 s, PBKDF2-SHA256 210,000 iterations.
