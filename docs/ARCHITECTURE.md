# RedisMQ Architecture

This document describes the internal architecture of the RedisMQ broker, layer by
layer, with the data-flow and synchronization rules that keep it correct.

## Overview

RedisMQ is an AMQP 0-9-1 wire server. A real client (RabbitMQ.Client 7.x) connects
over TCP or TLS, performs the SASL PLAIN handshake, negotiates tune parameters, opens
channels, and exchanges methods and content frames. Each connection owns one
`WireConnection`; each channel on that connection owns one `ChannelContext`. The
`ChannelContext` delegates topology and storage to a synchronous `IRouteEngine`.

Two engines implement that seam:

- **`InMemoryEngine`** is the system of record for reads, credit gating and routing.
  It owns exchanges, queues, bindings, ready buffers, delivery pumps, dead-lettering,
  TTL and the `x-expires` idle clock.
- **`RedisStreamRouteEngine`** wraps an `InMemoryEngine` and mirrors every mutation to
  Redis Streams on one background drainer. It returns honest durability receipts and
  replays the streams into memory at startup. With a null `IDatabase`, it degrades to
  memory-only and makes no durability promise.

```
Client ──TCP/TLS──► BrokerListeners ──TransportSide──► AmqpBrokerServer
   (SocketListener / TlsSocketListener)                  │
                                                         ▼
                                              one accept loop per listener
                                                         │
                                                         ▼
                                              WireConnection (per connection)
                                                         │
                          ┌──────────────────────────────┼──────────────────────────┐
                          │                                                         │
                          ▼                                                         ▼
                    Connection FSM                                        Channel dispatch (per number)
                    header → start →                                    ┌────────────────────────┐
                    tune → open → serve                                 │ ChannelContext         │
                    + PLAIN auth + vhost check                          │ • channel FSM          │
                    + ordered confirm loop                              │ • flow pause           │
                                                                        │ • content assembler    │
                                                                        │ • consume handlers     │
                                                                        │ • ack/nack/settle      │
                                                                        │ • permission checks    │
                                                                        │ • writer chain         │
                                                                        └──────────┬─────────────┘
                                                                                   │ IRouteEngine
                                                                    ┌──────────────┴──────────────┐
                                                                    ▼                             ▼
                                                            InMemoryEngine             RedisStreamRouteEngine
                                                            (system of record)         (wraps InMemoryEngine)
                                                                                          │
                                                                                          ▼
                                                                                   Redis Streams
```

## Layers

### 1. Codec (`RedisMQ.Broker.Codec`)

The lowest layer. Turns raw bytes into AMQP frames and back.

| File | Responsibility |
|------|----------------|
| `ByteReader` | `ref struct` big-endian reader. Octets, shorts, ints, longs, shortstr, longstr, bits, timestamps, field tables. Throws `AmqpDecodeException` on truncation. |
| `ByteWriter` | Big-endian writer with bit-group support. Field tables with depth-limited nesting. |
| `FieldTable` / `FieldValue` | Ordered map of shortstr keys to typed values. Wire letters `t b B s u I l L f d D S A F V T x`. Round-trips byte-identical. |
| `AmqpFrame` | Decoded frame record plus frame constants. |
| `FrameDecoder` | Pulls complete frames off a `ReadOnlySequence<byte>`; partial frames wait for the next fill. Hard errors throw `AmqpProtocolException`. |
| `FrameWriter` | Assembles method, content-header and body frames. Body splitting caps each frame at `frameMax - FrameOverhead`. |
| `ContentHeaderCodec` / `ContentProperties` | Encodes/decodes the basic-class content header (class-id, weight, body size, 14 property flags). |
| `MethodCodec` / `AmqpMethod` | Encodes/decodes method arguments through `MethodSchema`. |
| `Exceptions` | `AmqpDecodeException`, `AmqpEncodeException`, `AmqpProtocolException`. |

### 2. Protocol (`RedisMQ.Broker.Protocol`)

| File | Responsibility |
|------|----------------|
| `MethodRegistry` | Data-driven method schema table (class/method ids, field names+types, direction, content flag), derived from the canonical `rabbitmq-codegen.json` fixture. |
| `AmqpConstants` | Reply/error codes (311, 312, 320, 402–406, 501–505, 530, 540, 541). |
| `WireType` | Octet, Short, Long, LongLong, Shortstr, Longstr, Table, Timestamp, Bit, PackedBit, DependingBit. |
| `Direction` | `ClientToServer` / `ServerToClient`. |
| `ContentProperty` | Name, wire type, flag ordinal for basic properties. |

### 3. Transport (`RedisMQ.Broker.Transport`)

| File | Responsibility |
|------|----------------|
| `TransportSide` | One end of a byte transport: `PipeReader` + `PipeWriter` + `CompleteAsync()`. |
| `TransportPair` | Symmetric in-proc duplex pair used by unit tests. |
| `IListenerTransport` | Accept seam: `AcceptAsync` → `TransportSide?` (null = closed), plus `AnnouncedPort`. |
| `SocketListener` / `SocketBinder` | Real TCP listener. Dual-mode IPv6 bind with IPv4 fallback; port 0 → OS-assigned. |
| `TlsSocketListener` | Binds a socket like `SocketListener`, then wraps each accepted socket in `SslStream`. Failed handshakes close only that peer. |
| `BrokerCertificate` | Loads PFX/PEM server material, validates the private key, imports the mTLS CA bundle. |
| `BrokerListeners` | Factory building the plaintext and TLS listeners from `BrokerServerOptions`. |

### 4. Server (`RedisMQ.Broker.Server`)

| File | Responsibility |
|------|----------------|
| `AmqpBrokerServer` | Lifecycle: one accept loop per listener, one shared reaper, connection tracking. |
| `WireConnection` | One connection's lifecycle: protocol header → handshake → frame loop → close-wait. Owns the connection-wide write gate, the heartbeat watchdog, PLAIN authentication, vhost-open permission check, and the single ordered confirm loop. |
| `ChannelContext` | One channel's FSM, content assembly, consume handlers, ack/nack/settle ledger, prefetch credits, confirm state, flow state, permission checks and the serialized writer chain. |
| `PendingConfirm` | One publish waiting on the confirm loop. |
| `BrokerServerOptions` | Port, vhosts, users, tune values, confirm limits, reaper interval, TLS. Also `BrokerUser` / `BrokerPermission`. |
| `BrokerPassword` | PBKDF2-SHA256 hashing and fixed-time verification. |
| `BrokerPrincipal` | Compiled configure/write/read regex triples per vhost. |
| `BrokerTlsOptions` | TLS certificate, mTLS, protocol floor and handshake budget. |
| `BrokerConfigurationException` | Invalid configuration at startup. |
| `HeartbeatWatchdog` | Silence policing: kill at 2× negotiated heartbeat. |
| `IChannelDeliveryHost` | Write surface for pumps: method/content/delivery writes, `EnqueueConfirm`, `WriteReturnToWireAsync`. |

### 5. Memory (`RedisMQ.Broker.Memory`)

| File | Responsibility |
|------|----------------|
| `InMemoryEngine` | `IRouteEngine` implementation: topology, binding-insertion routing (direct/fanout/topic/headers), per-queue delivery, consumer registration, get claim, requeue, reject, dead-lettering, TTL, queue expiry, reaping, receipts and deltas. |
| `InMemoryQueue` | Ready buffer (FIFO), consumer registry (registration order), round-robin cursor, wake-driven pump, expired-message collection. |
| `InMemoryMessage` | `Props`, `Body`, `Exchange`, `RoutingKey`, `EnvelopeId`, `Redelivered`, `Expiration`, `DeliveryCount`. |
| `IRouteEngine` | The synchronous seam, plus `EnqueueReceipt`, `EngineDelta`, `QueuedMessage`, `QueuedMessageRef`. |
| `IQueueConsumer` | Pump-facing contract. |
| `ExchangeInfo` / `QueueInfo` | Declared manifests, including queue arguments. |
| `ConsumeRegisterOutcome` / `GetClaimOutcome` | Decision enums. |

### 6. Redis (`RedisMQ.Redis`)

| File | Responsibility |
|------|----------------|
| `RedisStreamRouteEngine` | Write-ahead mirror + startup replay + crash-window dedupe. |
| `RedisKeys` | Cluster-safe key layout and stream field names. |
| `WriteCommand` | Drainer commands: declare, bind, enqueue, remove, delete queue, batch delta. |
| `StreamCodec` | Encodes AMQP wire-form properties and message metadata into stream fields. |

---

## Diagnostics and Observability

A cross-cutting layer (`RedisMQ.Broker.Diagnostics`) instruments the protocol path
without adding a wire surface or a package dependency:

- `BrokerDiagnostics` owns the process-shared BCL `Meter` and `ActivitySource` (both
  named `RedisMQ.Broker`), a host-attached `IBrokerLogSink`, and one gauge registry so
  the server and engine can contribute to the same observable instrument. Every
  measurement carries `broker.instance`.
- The server records connection/channel/auth/vhost/permission counts, active gauges,
  publish/confirm/delivery/settlement telemetry, reaper ticks, TLS handshakes and
  `amqp.publish` / `amqp.deliver` spans.
- `AmqpBrokerServer.GetHealth()` assembles a synchronous `BrokerHealth` from the
  runtime counters, the engine's `IEngineDiagnostics` snapshot and an overlay of live
  unacked counts. Readiness follows Redis connectivity for a durable engine.
- `RedisStreamRouteEngine` emits **mirror and replay metrics only**; the inner
  `InMemoryEngine` remains the sole emitter of message-level counters (enqueue,
  expiry, dead-lettering, queue depth/consumers). This split keeps one series per
  event when the Redis wrapper delegates to memory.
- The host owns HTTP endpoints, OTLP/Prometheus exporters and log shipping. RedisMQ
  deliberately ships none of them, so a scrape interval, exporter choice and
  label-cardinality policy stay deployment concerns.

---

## Connection Lifecycle

`WireConnection.RunAsync(ct)`:

1. **Handshake** — read the 8-byte protocol header, reply on mismatch; send
   `connection.start`; expect `connection.start-ok` (PLAIN only); resolve the user and
   verify the credential; send `connection.tune`; expect `connection.tune-ok`; expect
   `connection.open`; check the vhost against the user's permissions; send
   `connection.open-ok`. State: `AwaitStartOk` → `AwaitTuneOk` → `AwaitOpen` →
   `Serving`.
2. **Frame loop** — `ReadAsync` → `FrameDecoder.ReadFrames` → dispatch each frame →
   reset watchdog on every frame → repeat until transport completes, watchdog kills,
   or the FSM stops.
3. **Close-wait** — if the server initiated a close (`ClosingByServer`), wait for
   `connection.close-ok` or EOF.
4. **Teardown** — `finally`: tear down every live channel's consumers (requeue
   unacked, unregister), call `IRouteEngine.ReconcileConnection(connectionId)`,
   complete the transport.

### Handshake details

- `connection.start` advertises version 0.9, server-properties (product "RedisMQ",
  capabilities), mechanisms "PLAIN", locales "en_US".
- PLAIN response format: `authzid\0authcid\0password` (authzid unused).
  `BrokerPassword.Verify` compares plaintext or PBKDF2 in fixed time.
- TuneOk negotiation: frame-max = min(offer, client), 0 keeps the offer; below 4096 →
  `not_allowed` (530) hard close. Heartbeat adopted verbatim; 0 disables the
  watchdog. No second `connection.tune` is ever sent.
- `connection.open` first checks `BrokerServerOptions.Vhosts` (unknown → hard 402),
  then the account's permissions. It is **default-deny**: a vhost with no permission
  entry for the account, or no account permissions at all, gets a hard 403
  access-refused close. A listed vhost with deny-all patterns still opens, and each
  operation is then refused individually.
- On entering `Serving`, the connection starts its single confirm loop.

### Hard vs soft errors

- **Hard errors** (connection close, channel 0): frame-error (501), syntax-error
  (502), channel-error (504), not-allowed (530), not-implemented (540),
  internal-error (541), content-too-large (311), invalid-path (402), access-refused
  for authentication/vhost-open (403).
- **Soft errors** (channel close, channel mourns): access-refused for resource
  permissions and reserved names (403), not-found (404), resource-locked (405),
  precondition-failed (406), unexpected-frame (505 — M0 pin).

---

## Channel FSM

States: `Starting` → `Running` → `Mourning`.

- **Starting**: only `channel.open` is legal. Anything else → hard `channel-error`
  (504). `channel.open` → `channel.open-ok`, state = `Running`.
- **Running**: full method set dispatched by `(classId, methodId)`. Content-bearing
  methods open the content assembler. Soft errors → `channel.close`, state =
  `Mourning`.
- **Mourning**: traffic discarded; `channel.close` re-answered with
  `channel.close-ok`; `channel.close-ok` retires the number.

`channel.flow(active=false)` sets the channel's flow state to paused and replies
`channel.flow-ok(active=false)`. No new deliveries are admitted while paused; the
connection, other channels, and the paused channel's own method handling stay alive.
`active=true` resumes and nudges the channel's queues.

### Content assembler (`rabbit_command_assembler`)

`basic.publish` parks the channel in `ExpectHeader`:

1. Header frame → validate class 60, check body-size cap (134,217,728), enter
   `ExpectBody`.
2. Body frames → accumulate until declared size; empty fragment while owed or
   overshooting fragment → hard `frame-error` (501).
3. Complete → `FinishPublish()`: check `immediate=true` (hard 540), apply permission
   checks, lookup exchange (soft 404 if absent), default-exchange route or `RouteTo`,
   enqueue or `basic.return`(312) or silent drop.
4. Reset to `ExpectMethod`. Any method frame while `ExpectHeader`/`ExpectBody` → soft
   `unexpected-frame` (505) with the pending publish's ids.

---

## Consume Path

The consumable path is the most synchronized part of the system. The lock ladder is:

```
engine gate (_lock in InMemoryEngine)
    └─► ChannelContext._deliveryLock
            └─► (released before any engine call from handler paths)
```

The reverse (engine gate → `_deliveryLock`) is taken by the pump claim path. The
writer-gate → engine-gate edge is one-way.

### Credit model

Per channel, under `_deliveryLock`:

- `_prefetchCount` (short): max outstanding unacked deliveries. 0 = unlimited.
- `_prefetchSize` (long): RedisMQ bytes-gating extension. Max outstanding unacked
  body bytes.
- `_outstandingBytes`: Σ body lengths of unacked entries.
- `_unacked`: `Dictionary<long, Unacked>` (delivery-tag → queue + message).
- `_nextDeliveryTag`: monotonic counter, first tag is 1.

Admission (`ConsumerEntry.TryAdmitForDelivery`, under engine gate + `_deliveryLock`):

1. If `noAck`: assign a tag, skip ledger/credit, schedule deliver write, return true.
2. If `prefetchCount > 0` and `_unacked.Count >= prefetchCount`: return false.
3. If `prefetchSize > 0` and `_outstandingBytes + body > prefetchSize`: return false.
4. Otherwise: assign tag, add ledger entry, add bytes, schedule deliver write, return
   true.
5. If flow is paused for the channel: return false (message stays queued).

Delivery **count** increments at claim time for the delivery pump and `basic.get`
alike. Requeue preserves the count; the queue's `x-delivery-limit` dead-letters on a
requeue when the count has already reached the limit.

### Round-robin

`InMemoryQueue.TryClaimOneAlreadyLocked` iterates from `_rrIndex` over consumers in
registration order. The first consumer that admits the message wins; `_rrIndex` is
positioned after the claimer, modulo the current registry. This preserves the cycle
edge when a consumer joins after a one-consumer-registry claim.

### Settlements and the durable mirror

- `ack(tag, multiple=false)`: remove the single ledger entry; unknown tag → soft 406
  `unknown delivery tag N`.
- `ack(tag, multiple=true)`: remove all entries with key ≤ tag; tag 0 = whole ledger.
- `nack`/`reject` with `requeue=true`: remove settled entries, then `Requeue` each
  (tail, `Redelivered=true`, delivery count preserved).
- `reject`/`nack` with `requeue=false`: `Reject`, which dead-letters when the queue
  has a DLX.
- Every settle is mirrored through the engine's `EngineDelta`; see
  [Durability Mirroring](#durability-mirroring).
- After any settlement: `_outstandingBytes` reduced, then `NudgeChannelQueues()`
  re-arms pumps.

### Writer chain

Each `ChannelContext` has a serialized writer chain (`_writerGate` + job queue).
Consumer-path writes (deliveries, consume-ok, qos-ok, cancel-ok, get-ok, recover-ok)
and confirm outcomes are scheduled as jobs and drained by `RunWriterChainAsync` right
after dispatch. This guarantees chain order = wire order and prevents a queue-pump
claim from jumping ahead of a promise the channel already made.

---

## Publisher Confirms

`confirm.select` enables confirm mode on a channel and replies `confirm.select-ok`.
Every subsequent publish — routed, unroutable, and mandatory-unroutable alike —
receives exactly one ordered outcome.

1. `ChannelContext.FinishPublish` assigns the next confirm sequence and collects one
   `EnqueueReceipt` per routed queue (none for an unroutable publish).
2. It hands a `PendingConfirm` to the connection's `EnqueueConfirm`.
3. The connection's single confirm loop processes entries in sequence order:
   - Await every receipt, bounded by `BrokerServerOptions.ConfirmTimeout`.
   - All durable or no receipts → `basic.ack(tag, multiple=false)`.
   - Any refusal, fault or timeout → `basic.nack(tag, multiple=false,
     requeue=false)`.
   - A mandatory-unroutable publish writes its `basic.return` **before** the confirm.
4. Outcomes are written through the channel's writer chain so they interleave
   correctly with deliveries and replies.

**Backpressure, not drops.** `ConfirmQueueCapacity` bounds admitted-but-unflushed
confirms per connection. When full, `EnqueueConfirm` blocks the frame pump until a
slot drains. A slow Redis therefore slows publishes instead of inventing acks.

**Retired channels discard.** A confirm whose channel number has been closed and
re-used resolves against a different `ChannelContext`; the loop detects the mismatch
and discards the outcome rather than writing it onto the new channel.

**Timeout is not cancellation.** A timed-out `XADD` may still reach Redis later; that
is documented at-least-once behavior.

---

## Durability Mirroring

`RedisStreamRouteEngine` keeps `InMemoryEngine` as the system of record and mirrors
mutations asynchronously.

### Receipts

`Enqueue` reserves a drainer slot **before** touching memory. No slot → the publish is
refused with `(EnvelopeId 0, Durable=false)` and never reaches the ready buffer. With
a slot, memory stores the message and the drainer command carries a
`TaskCompletionSource` completed after the `XADD` returns. The confirm loop awaits
that receipt.

### Deltas

`Requeue`, `Acknowledge`, `Reject` and `Reap` return an `EngineDelta`:

- `Enqueued`: messages the durable mirror must append (requeued copies, dead-letter
  copies, x-max-length survivors).
- `Removed`: envelope ids the mirror must `XDEL`.
- `DeletedQueues`: queues whose streams and topology entries must be deleted.

`MirrorDelta` resolves removed envelope ids to **stream ids before** queuing the
batch. This matters because a requeue reuses the envelope id: the new `XADD` would
otherwise overwrite the map entry and leak the old stream entry.

**Ordering contract:** enqueue/mutate first, remove second. A crash between the two
can duplicate a message on replay, but never loses a live one.

### Replay and dedupe

At startup, `RedisStreamRouteEngine`:

1. Replays `{mq:top:<vhost>}` declares/binds/unbinds/deletes into memory.
2. For every known queue, reads `{mq:q:<vhost>:<queue>}` and rebuilds the ready
   buffer.
3. Collapses crash-window duplicates: entries sharing an envelope id keep the higher
   delivery count (tie → later stream id), and the stale stream entries are `XDEL`ed.
   Survivor stream ids are recorded so later acks can remove them.

Replay restores ready messages, not unacked deliveries: a message delivered but not
settled before a crash comes back as ready with `Redelivered=false`.

### Eviction gate

When `enforceEvictionGate` is true, the engine reads `CONFIG GET maxmemory-policy`
and refuses to start unless the policy is `noeviction`. An eviction policy could
silently discard durable data, which would make confirms dishonest.

---

## Dead-Lettering and TTL

### Message expiry

A publish's AMQP `expiration` property is a relative millisecond string. The engine
converts it to an absolute `DateTimeOffset` deadline at publish time and stores it on
the message. Requeue preserves the deadline; a dead-letter copy clears it. An invalid
or negative value means "no expiry".

The shared reaper calls `IRouteEngine.Reap(now)` every `ReaperInterval`:

1. **Queue expiry first.** A queue with `x-expires` whose `LastUsed` stamp is older
   than its idle duration (and which has no active consumer) is deleted with all its
   messages, delivery state, bindings and stamp. Its messages are **dropped, not
   dead-lettered**, and the queue is reported in `DeletedQueues`.
2. **Ready-message expiry second.** Every ready message whose deadline is at or
   before `now` is removed and, when its queue has a DLX, dead-lettered with reason
   `expired`. Unacked messages are untouched (RabbitMQ semantics). Keys are
   snapshotted before iterating so a dead-letter copy into another queue cannot
   mutate the enumeration.

`LastUsed` is in-memory and resets on restart. It is touched by declare, bind,
enqueue, get and consume; `UnregisterConsumer` touches it only when the last consumer
leaves; `DeleteQueue` removes the stamp.

### Dead-lettering

A queue with `x-dead-letter-exchange` dead-letters on:

- `basic.reject(requeue=false)` / `basic.nack(requeue=false)` — reason `rejected`.
- Requeue when `x-delivery-limit` has been reached — reason `rejected`.
- Ready-message expiry — reason `expired`.

The target exchange is the configured DLX; an explicit empty string means the default
exchange, which addresses a queue by routing key. The routing key is
`x-dead-letter-routing-key` when set, otherwise the message's original key. A copy:

- Gets a new envelope id and a defensive body copy.
- Resets `DeliveryCount` to 0.
- Clears the expiry deadline.
- Carries `x-death` (newest first) with `queue`, `reason`, `count`, `time`,
  `exchange`, plus `x-first-death-queue`, `x-first-death-reason` and
  `x-first-death-exchange` set once.
- Is dropped silently when the DLX is missing or the copy is unroutable.
- Is dropped when the headers already carry an `x-death` entry for the same origin
  queue and reason (unconditional loop guard).

`x-max-length` applies to dead-letter targets too: a copy that overflows a target
queue drops that target's oldest message, and the removal is mirrored.

---

## Permissions

Accounts are configured, not managed over the wire. `BrokerServerOptions.Validate`
rejects duplicate names, missing/ambiguous credential forms, malformed PBKDF2 hashes
and invalid regexes at startup.

After PLAIN authentication, `WireConnection` builds one `BrokerPrincipal` per
connection:

- One compiled configure/write/read regex triple per vhost.
- Regex options: `CultureInvariant`, one-second match timeout.
- Empty pattern → `\A\z` (matches only the empty name → deny).
- First matching vhost entry wins.

Enforcement points, in RabbitMQ order:

| Operation | Check |
|-----------|-------|
| `connection.open` | vhost must have a permission entry (default-deny). |
| `exchange.declare`, `queue.declare` | configure on the resource name. |
| `queue.bind` / `queue.unbind` | configure on the queue, then write on the exchange. |
| `basic.publish` | write on the exchange name; on the default exchange, write on the routing key (the queue it addresses). |
| `basic.consume`, `basic.get`, `basic.cancel` | read on the queue. |
| Passive declares | configure on the resource name. |

Checks run **before** existence lookups, so a forbidden resource reports 403 rather
than leaking whether it exists. A refused operation is a soft channel close with
RabbitMQ's `ACCESS_REFUSED - ...` wording.

A null `BrokerPrincipal` on `ChannelContext` means full access and is reserved for
internal test channels; `WireConnection` always passes the authenticated principal.

---

## TLS

TLS is a listener-level concern; the wire layer above it is unchanged.

- `BrokerCertificate.Load` validates all material once at startup: PFX or PEM
  certificate, optional PEM key, private key present, and (for mTLS) a non-empty CA
  bundle.
- Windows loads keys with `UserKeySet | Exportable` because Schannel refuses server
  credentials with ephemeral keys; Unix uses `EphemeralKeySet`. PEM material is
  re-imported through PKCS#12 so its key lives in the platform-compatible key set.
- `TlsSocketListener.AcceptAsync` accepts a socket, performs
  `AuthenticateAsServerAsync` under `HandshakeTimeout`, and returns a
  `TransportSide` over the `SslStream`. On failure it disposes that stream (which owns
  the socket) and continues accepting; only cancellation or listener disposal ends
  the accept stream.
- Mutual TLS installs a validation callback that chains the client certificate to the
  configured CA bundle with custom root trust and revocation disabled.
- `BrokerListeners.Create` composes plaintext and TLS listeners; `AmqpBrokerServer`
  runs one accept loop per listener and one shared reaper.

Certificate rotation requires a restart: the material is loaded once per listener.

---

## Error Surface Summary

| Scenario | Code | Class/Method | Channel or Connection |
|----------|------|--------------|----------------------|
| Bad PLAIN mechanism or credentials | 403 | 10/11 | Connection (hard) |
| Unlisted vhost (default-deny) | 403 | 10/40 | Connection (hard) |
| Unknown vhost (not in options) | 402 | 10/40 | Connection (hard) |
| TuneOk frame-max < 4096 | 530 | 10/31 | Connection (hard) |
| Unknown method (class,method) | 502 / 540 | varies | Connection (hard) |
| Second channel.open | 504 | 0/0 | Channel (soft) |
| Expected channel.open | 504 | 0/0 | Channel (soft) |
| Unknown exchange (publish) | 404 | 60/40 | Channel (soft) |
| Unknown queue (consume/get) | 404 | 60/20 / 60/70 | Channel (soft) |
| Redeclare mismatch | 406 | 50/10 / 40/10 | Channel (soft) |
| Reserved `amq.*` name | 403 | varies | Channel (soft) |
| Permission denied | 403 | varies | Channel (soft) |
| Exclusive arbitration | 405 | 60/20 / 50/10 | Channel (soft) |
| Unknown delivery tag (ack) | 406 | 60/80 | Channel (soft) |
| Content out of phase | 505 | pending ids | Channel (soft, M0 pin) |
| `channel.flow(active=false)` | — | 20/20 | Pauses the channel; flow-ok |
| `recover(requeue=false)` | 540 | 60/110 | Connection (hard) |
| Body > 134,217,728 | 311 | 60/40 | Connection (hard) |
| Bad body fragment | 501 | 60/40 | Connection (hard) |
| Out-of-set method | 540 | varies | Connection (hard) |

---

## Synchronization Rules (Invariants)

1. **Engine gate is never held across a frame write.** The pump claims under the
   gate, releases, then drains the writer chain outside it.
2. **`_deliveryLock` is taken inside the engine gate on the pump claim path.** Handler
   paths release `_deliveryLock` before calling engine methods.
3. **Writer-gate → engine-gate is one-way.** No code takes the engine gate while
   holding the writer gate.
4. **Every inbound frame resets the heartbeat watchdog** (any type, any channel).
5. **A decode error anywhere in a `ReadAsync` batch aborts the whole batch.**
6. **Channel numbers are retired on close-ok**, not on close. Mourning channels
   discard traffic until close-ok.
7. **Connection end tears down all channels** (requeue unacked, unregister consumers)
   before completing the transport, then reconciles exclusive/auto-delete queues.
8. **The durable mirror is enqueue-first, remove-second.** A crash can duplicate,
   never lose.
9. **The confirm loop is one per connection and strictly ordered by publish
   sequence.** A full pipeline blocks the frame pump instead of dropping.
10. **A confirm only becomes an ack when every receipt completed durable**, bounded
    by `ConfirmTimeout`; anything else is a nack.
11. **Permission checks run before existence lookups** and before any state change.
12. **The reaper snapshots queue/delivery keys before mutating**, so dead-letter
    copies cannot invalidate its iteration.
