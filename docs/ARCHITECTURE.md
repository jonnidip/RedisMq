# RedisMQ Architecture

This document describes the internal architecture of the RedisMQ broker, layer by layer,
with the data-flow and synchronization rules that keep it correct.

## Overview

RedisMQ is an AMQP 0-9-1 wire server. A real client (RabbitMQ.Client 7.x) connects over TCP,
performs the SASL PLAIN handshake, negotiates tune parameters, opens channels, and then
exchanges methods and content frames. Each connection owns one `WireConnection`; each
channel on that connection owns one `ChannelContext`. The `ChannelContext` delegates
topology and storage to an `IRouteEngine` (M0: `InMemoryEngine`).

```
Client ──TCP──► SocketListener ──TransportSide──► AmqpBrokerServer
                                               │
                                               ▼
                                         accept loop
                                               │
                                               ▼
                                         WireConnection (per connection)
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    │                                                        │
                    ▼                                                        ▼
              Connection FSM                                      Channel dispatch (per channel number)
              (header → start →                                  ┌────────────────────────┐
               tune → open → serve)                              │ ChannelContext         │
                                               │                  │ (channel FSM)          │
                                               │                  │ • content assembler  │
                                               │                  │ • consume handlers   │
                                               │                  │ • ack/nack/settle    │
                                               │                  │ • writer chain       │
                                               │                  └──────────┬───────────┘
                                               │                             │
                                               │                             ▼
                                               │                    IRouteEngine (InMemoryEngine)
                                               │                    • exchanges / queues
                                               │                    • bindings
                                               │                    • RouteTo (direct)
                                               │                    • Enqueue / Requeue
                                               │                    • RegisterConsumer
                                               │                    • TryClaimForGet
                                               │                    • Nudge
                                               └───────────────────────────────────────────┘
```

## Layers

### 1. Codec (`RedisMQ.Broker.Codec`)

The lowest layer. Turns raw bytes into AMQP frames and back.

| File | Responsibility |
|------|----------------|
| `ByteReader` | `ref struct` big-endian reader over `ReadOnlySpan<byte>`. Octets, shorts, ints, longs, shortstr, longstr, bits, timestamps, field tables. Throws `AmqpDecodeException` on truncation. |
| `ByteWriter` | Big-endian writer with bit-group support (`WriteBits` flushes one octet per group). Field tables with depth-limited nesting. |
| `FieldTable` / `FieldValue` | Ordered map of shortstr keys to typed values. Wire letters `t b B s u I l L f d D S A F V T x`. Round-trips byte-identical. |
| `AmqpFrame` | Decoded frame record: type, channel, payload. Constants: `FrameMethod=1`, `FrameHeader=2`, `FrameBody=3`, `FrameHeartbeat=8`, `FrameEnd=0xCE`, `FrameOverhead=8`, `FrameMinSize=4096`. |
| `FrameDecoder` | Pulls complete frames off a `ReadOnlySequence<byte>`. Partial frames are left for the next fill. Hard errors (unknown type, bad 0xCE, oversize) throw `AmqpProtocolException`. |
| `FrameWriter` | Assembles method, content-header, and body frames onto a `ByteWriter`. Body splitting caps each frame at `frameMax - FrameOverhead`. |
| `ContentHeaderCodec` / `ContentProperties` | Encodes/decodes the basic-class content-header payload (class-id, weight, body size, 14 property flag bits). |
| `MethodCodec` / `AmqpMethod` | Encodes/decodes method arguments through `MethodSchema`. Bit fields are packed into octet groups; consecutive bits share one `WriteBits` call. |
| `Exceptions` | `AmqpDecodeException`, `AmqpEncodeException`, `AmqpProtocolException`. |

### 2. Protocol (`RedisMQ.Broker.Protocol`)

| File | Responsibility |
|------|----------------|
| `MethodRegistry` | Data-driven method schema table (class/method ids, field names+types, direction, content flag). Derived from the canonical `rabbitmq-codegen.json` fixture. Provides `Schema(classId, methodId)` and `TryGetByName(name)`. |
| `AmqpConstants` | M0 reply/error codes (311, 312, 320, 402–406, 501–505, 530, 540, 541). |
| `WireType` | Octet, Short, Long, LongLong, Shortstr, Longstr, Table, Timestamp, Bit, PackedBit, DependingBit. |
| `Direction` | `ClientToServer` / `ServerToClient` (spec-side annotation). |
| `ContentProperty` | Name, wire type, flag ordinal for basic properties. |

### 3. Transport (`RedisMQ.Broker.Transport`)

| File | Responsibility |
|------|----------------|
| `TransportSide` | One end of a byte transport: `PipeReader` + `PipeWriter` + `CompleteAsync()`. |
| `TransportPair` | Symmetric in-proc duplex pair (`TransportClient` / `TransportServer`). Used by unit tests so they don't need a real socket. |
| `IListenerTransport` | Accept seam: `AcceptAsync(CancellationToken)` → `TransportSide?` (null = listener closed). |
| `SocketListener` | Real TCP `IListenerTransport`. Dual-mode IPv6 bind with IPv4 fallback. Port 0 → OS-assigned ephemeral, reported via `AnnouncedPort`. Wraps `NetworkStream` in `PipeReader`/`PipeWriter`. |

### 4. Server (`RedisMQ.Broker.Server`)

| File | Responsibility |
|------|----------------|
| `AmqpBrokerServer` | Lifecycle: options, engine, optional listener. `StartAsync()` arms the accept loop in the background; `StopAsync()` cancels and drains. |
| `WireConnection` | One connection's entire lifecycle: protocol header → handshake → frame loop → close-wait. Implements `IChannelDeliveryHost`. Owns the connection-wide `_writeGate` (`SemaphoreSlim(1,1)`) serializing all wire writes. Runs the `HeartbeatWatchdog`. |
| `ChannelContext` | One channel's FSM: `Starting` → `Running` → `Mourning`. Owns content assembly, consume-family handlers, ack/nack/settle ledger, prefetch credits, and the serialized writer chain. |
| `HeartbeatWatchdog` | Silence policing: kill at 2× negotiated heartbeat. Virtual `Now` for fake-clock tests. Reset on every inbound frame of any type. |
| `BrokerServerOptions` / `BrokerUser` | Configuration: port, vhosts, PLAIN users, frame-max, heartbeat. |
| `IChannelDeliveryHost` | Write surface for guest callers (queue pumps): `WriteMethodToWireAsync`, `WriteContentToWireAsync`, `WriteDeliveryToWireAsync`. Implemented by `WireConnection`. |

### 5. Memory (`RedisMQ.Broker.Memory`)

| File | Responsibility |
|------|----------------|
| `InMemoryEngine` | `IRouteEngine` implementation: exchanges, queues, bindings per vhost; per-queue delivery state; consumer registration; basic.get claim; requeue; nudge. |
| `InMemoryQueue` | Per-queue state: ready buffer (FIFO), consumer registry (registration order), round-robin cursor, wake-driven pump task. Pump runs outside the engine lock during awaits. |
| `InMemoryMessage` | Stored message: `Props`, `Body`, `Exchange`, `RoutingKey`, `EnvelopeId`, `Redelivered`. |
| `IRouteEngine` | The seam between wire server and storage. M0-swappable for M1. |
| `IQueueConsumer` | Pump-facing contract: `TryAdmitForDelivery` (synchronous credit check + ledger + writer-job schedule under engine gate), `DrainDeliveryWritesAsync` (outside gate). |
| `ExchangeInfo` / `QueueInfo` | Declared manifests. |
| `ConsumeRegisterOutcome` / `GetClaimOutcome` | Decision enums for consume and basic.get. |

---

## Connection Lifecycle

`WireConnection.RunAsync(ct)`:

1. **Handshake** — read the 8-byte protocol header, reply on mismatch; send `connection.start`; expect
   `connection.start-ok` (PLAIN only); send `connection.tune`; expect `connection.tune-ok`; expect
   `connection.open`; send `connection.open-ok`. State: `AwaitStartOk` → `AwaitTuneOk` → `AwaitOpen` →
   `Serving`.
2. **Frame loop** — `ReadAsync` → `FrameDecoder.ReadFrames` → dispatch each frame → reset watchdog on
   every frame → repeat until transport completes, watchdog kills, or the FSM stops.
3. **Close-wait** — if the server initiated a close (`ClosingByServer`), wait for `connection.close-ok`
   or EOF.
4. **Teardown** — `finally`: tear down every live channel's consumers (requeue unacked, unregister),
   complete the transport.

### Handshake details

- `connection.start` advertises: version 0.9, server-properties (product "RedisMQ", version,
  platform, capabilities), mechanisms "PLAIN", locales "en_US".
- PLAIN response format: `authzid\0authcid\0password` (3 NUL-separated parts; authzid unused).
- TuneOk negotiation: frame-max = min(offer, client), 0 keeps the offer; below 4096 → `not_allowed`
  (530) hard close. Heartbeat adopted verbatim; 0 disables the watchdog. No second `connection.tune`
  is ever sent.

### Hard vs soft errors

- **Hard errors** (connection close, channel 0): frame-error (501), syntax-error (502),
  channel-error (504), not-allowed (530), not-implemented (540), internal-error (541),
  content-too-large (311), invalid-path (402).
- **Soft errors** (channel close, channel stays mourning): access-refused (403), not-found (404),
  resource-locked (405), precondition-failed (406), unexpected-frame (505 — M0 pin; RabbitMQ treats
  505 as hard).

---

## Channel FSM

States: `Starting` → `Running` → `Mourning`.

- **Starting**: only `channel.open` is legal. Anything else → hard `channel-error` (504)
  "expected 'channel.open'". `channel.open` → `channel.open-ok`, state = `Running`.
- **Running**: full method set dispatched by `(classId, methodId)`. Content-bearing methods
  (`basic.publish`) open the content assembler. Soft errors → `channel.close`, state = `Mourning`.
- **Mourning**: traffic discarded; `channel.close` re-answered with `channel.close-ok`;
  `channel.close-ok` retires the number.

### Content assembler (`rabbit_command_assembler`)

`basic.publish` parks the channel in `ExpectHeader`:
1. Header frame → validate class 60, check body-size cap (134,217,728), enter `ExpectBody`.
2. Body frames → accumulate until declared size; empty fragment while owed or overshooting fragment → hard `frame-error` (501).
3. Complete → `FinishPublish()`: check `immediate=true` (hard 540), lookup exchange (soft 404 if absent),
   default-exchange route or `RouteTo`, enqueue or `basic.return`(312) or silent drop.
4. Reset to `ExpectMethod`. Any method frame while `ExpectHeader`/`ExpectBody` → soft `unexpected-frame`
   (505) with the pending publish's ids.

---

## Consume Path (Task 7)

The consume path is the most synchronized part of the system. The lock ladder is:

```
engine gate (_lock in InMemoryEngine)
    └─► ChannelContext._deliveryLock
            └─► (released before any engine call from handler paths)
```

The reverse (engine gate → `_deliveryLock`) is taken by the pump claim path. The writer-gate → engine-gate
edge is one-way.

### Credit model

Per channel, under `_deliveryLock`:

- `_prefetchCount` (short): max outstanding unacked deliveries. 0 = unlimited.
- `_prefetchSize` (long): RedisMQ bytes-gating extension. Max outstanding unacked body bytes.
- `_outstandingBytes`: Σ body lengths of unacked entries.
- `_unacked`: `Dictionary<long, Unacked>` (delivery-tag → queue + message).
- `_nextDeliveryTag`: monotonic counter, starts at 0, first tag is 1.

Admission (`ConsumerEntry.TryAdmitForDelivery`, called under engine gate + `_deliveryLock`):

1. If `noAck`: assign a tag, skip ledger/credit, schedule deliver write, return true.
2. If `prefetchCount > 0` and `_unacked.Count >= prefetchCount`: return false (message stays queued).
3. If `prefetchSize > 0` and `_outstandingBytes + body > prefetchSize`: return false.
4. Otherwise: assign tag, add ledger entry, add bytes, schedule deliver write, return true.

### Round-robin

`InMemoryQueue.TryClaimOneAlreadyLocked` iterates from `_rrIndex` over consumers in registration order.
The first consumer that admits the message wins; `_rrIndex = index + 1` (position **after** the claimer;
modulo applied at use against the current registry). This preserves the cycle edge when a consumer joins
after a one-consumer-registry claim.

### Settlements

- `ack(tag, multiple=false)`: remove the single ledger entry; unknown tag → soft 406 `unknown delivery tag N` (ids 60/80).
- `ack(tag, multiple=true)`: remove all entries with key ≤ tag; tag 0 = whole ledger.
- `nack`/`reject` with `requeue=true`: remove settled entries, `_engine.Requeue` each (tail, `Redelivered=true`).
- `reject`/`nack` with `requeue=false`: drop.
- After any settlement: `_outstandingBytes` reduced, then `NudgeChannelQueues()` re-arms pumps.

### Writer chain

Each `ChannelContext` has a serialized writer chain (`_writerGate` + job queue). Consumer-path writes
(deliveries, consume-ok, qos-ok, cancel-ok, get-ok, recover-ok) are scheduled as jobs and drained by
`RunWriterChainAsync` right after dispatch. This guarantees chain order = wire order and prevents a
queue-pump claim from jumping ahead of a promise the channel already made.

---

## Error Surface Summary

| Scenario | Code | Class/Method | Channel or Connection |
|----------|------|--------------|----------------------|
| Bad PLAIN mechanism | 403 | 10/11 | Connection (hard) |
| Bad PLAIN credentials | 403 | 10/11 | Connection (hard) |
| Unknown vhost | 402 | 10/40 | Connection (hard) |
| TuneOk frame-max < 4096 | 530 | 10/31 | Connection (hard) |
| Unknown method (class,method) | 502 / 540 | varies | Connection (hard) |
| Second channel.open | 504 | 0/0 | Channel (soft) |
| Expected channel.open | 504 | 0/0 | Channel (soft) |
| Unknown exchange (publish) | 404 | 60/40 | Channel (soft) |
| Unknown queue (consume/get) | 404 | 60/20 / 60/70 | Channel (soft) |
| Redeclare mismatch | 406 | 50/10 / 40/10 | Channel (soft) |
| Reserved `amq.*` name | 403 | varies | Channel (soft) |
| Exclusive arbitration | 405 | 60/20 / 50/10 | Channel (soft) |
| Unknown delivery tag (ack) | 406 | 60/80 | Channel (soft) |
| Content out of phase | 505 | pending ids | Channel (soft, M0 pin) |
| channel.flow(active=false) | 540 | 20/20 | Connection (hard) |
| recover(requeue=false) | 540 | 60/110 | Connection (hard) |
| Body > 134,217,728 | 311 | 60/40 | Connection (hard) |
| Bad body fragment | 501 | 60/40 | Connection (hard) |
| Out-of-M0-set method | 540 | varies | Connection (hard) |

---

## Synchronization Rules (Invariants)

1. **Engine gate is never held across a frame write.** The pump claims under the gate, releases, then
   drains the writer chain outside it.
2. **`_deliveryLock` is taken inside the engine gate on the pump claim path.** Handler paths release
   `_deliveryLock` before calling engine methods (enqueue/requeue/unregister).
3. **Writer-gate → engine-gate is one-way.** No code takes the engine gate while holding the writer gate.
4. **Every inbound frame resets the heartbeat watchdog** (any type, any channel).
5. **A decode error anywhere in a `ReadAsync` batch aborts the whole batch** (per-batch decode-abort rule).
6. **Channel numbers are retired on close-ok**, not on close. Mourning channels discard traffic until close-ok.
7. **Connection end tears down all channels** (requeue unacked, unregister consumers) before completing the transport.
