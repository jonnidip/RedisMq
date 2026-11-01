# RedisMQ

RedisMQ is an AMQP 0-9-1 (RabbitMQ dialect) wire-server shell over **in-memory queues**.
This is the **M0 milestone**: it proves the wire layer — codec, connection/channel state
machines, handshake, declare/bind, publish, consume with prefetch credits, ack/nack/reject,
basic.get, recover — against a real RabbitMQ.Client 7.x client over TCP.

**No Redis anywhere in M0.** The in-memory engine implements `IRouteEngine` so that M1 can
swap in `RedisStreamRouteEngine` without touching codec or wire code.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [What M0 Is (and Isn't)](#what-m0-is-and-isnt)
3. [Architecture](#architecture)
4. [Project Layout](#project-layout)
5. [Running the Tests](#running-the-tests)
6. [Conformance Suite](#conformance-suite)
7. [API Reference](#api-reference)
8. [Protocol Implementation Notes](#protocol-implementation-notes)
9. [Extending to M1 (Redis Streams)](#extending-to-m1-redis-streams)
10. [Rulings and Deviations](#rulings-and-deviations)
11. [CI](#ci)

---

## Quick Start

```bash
# Build everything
dotnet build RedisMQ.sln

# Run the full test suite (unit + conformance)
dotnet test RedisMQ.sln
```

> **Note:** On this development machine the .NET SDK's MTP test runner (`dotnet test`) is
> unusable due to an SDK bug (exit 5, "zero tests ran"). Use the direct runner instead:
>
> ```bash
> dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll
> dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
> ```

Expected results:

| Suite | Total | Failed | Warnings |
|-------|-------|--------|----------|
| `RedisMQ.Broker.Tests` | 233 | 0 | 0 |
| `RedisMQ.Conformance` | 15 | 0 | 0 |
| `dotnet build RedisMQ.sln` | — | — | 0 |

---

## What M0 Is (and Isn't)

### Is

- An **AMQP 0-9-1 wire server** that real RabbitMQ client libraries can connect to.
- A **connection + channel state machine** following RabbitMQ's `rabbit_channel`/`rabbit_reader` conventions.
- An **in-memory route engine** (exchanges, queues, direct bindings) behind `IRouteEngine`.
- A **publish path** with full content assembly (header + body frames, re-chunked per negotiated frame-max).
- A **consume path** with per-channel prefetch credits (count and bytes), ack/nack/reject settlement,
  basic.get, basic.recover, basic.cancel.
- A **TCP listener** (`SocketListener`) for real-client conformance over actual sockets.
- Zero external runtime dependencies in the core library (`net10.0`, no NuGet packages).

### Isn't

- **No Redis.** M0 is purely in-memory. Redis Streams lands in M1.
- **No topic/headers exchanges** — only `direct` (and the default exchange).
- **No confirms, no TTL, no persistence, no publisher confirms.**
- **No TLS, no vhost permissions, no clustering.**
- Durable/auto-delete flags are accepted and stored but behave as non-durable (documented in code).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  RabbitMQ.Client 7.x (real client)  ──TCP──►  SocketListener       │
│  (or in-proc TransportPair in unit tests)                           │
└─────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AmqpBrokerServer                                                   │
│  ┌─────────────┐  accept loop  ┌──────────────────────────────┐   │
│  │  Listener   │ ─────────────►│  WireConnection (per conn)   │   │
│  └─────────────┘               │  ┌────────────────────────┐  │   │
│                                │  │  Connection FSM        │  │   │
│                                │  │  (header → start →     │  │   │
│                                │  │   tune → open → serve) │  │   │
│                                │  │  HeartbeatWatchdog     │  │   │
│                                │  │  Write gate (Semaphore)│  │   │
│                                │  └────────────────────────┘  │   │
│                                │  Channel dispatch            │   │
│                                │  ┌────────────────────────┐  │   │
│                                │  │ ChannelContext (per ch)│  │   │
│                                │  │  • channel FSM         │  │   │
│                                │  │  • content assembler   │  │   │
│                                │  │  • consume handlers    │  │   │
│                                │  │  • ack/nack/settle     │  │   │
│                                │  │  • writer chain        │  │   │
│                                │  └───────────┬─────────────┘  │   │
│                                └──────────────┼────────────────┘   │
│                                               │                      │
│                                ┌──────────────┴────────────────┐   │
│                                │  IRouteEngine (InMemoryEngine)│   │
│                                │  • exchanges / queues         │   │
│                                │  • bindings                   │   │
│                                │  • RouteTo (direct)           │   │
│                                │  • Enqueue / Requeue          │   │
│                                │  • RegisterConsumer           │   │
│                                │  • TryClaimForGet (basic.get) │   │
│                                └───────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Layers

| Layer | Namespace | Responsibility |
|-------|-----------|----------------|
| **Codec** | `RedisMQ.Broker.Codec` | BE byte primitives (`ByteReader`/`ByteWriter`), field tables (`FieldTable`/`FieldValue`), AMQP frames (`AmqpFrame`/`FrameDecoder`/`FrameWriter`), content headers (`ContentHeaderCodec`/`ContentProperties`), method encode/decode (`MethodCodec`/`AmqpMethod`) |
| **Protocol** | `RedisMQ.Broker.Protocol` | Method registry (`MethodRegistry`), content properties (`ContentProperty`), wire types (`WireType`), direction (`Direction`), error codes (`AmqpConstants`) |
| **Transport** | `RedisMQ.Broker.Transport` | `TransportSide`, `TransportPair` (in-proc), `IListenerTransport`, `SocketListener` (real TCP) |
| **Server** | `RedisMQ.Broker.Server` | `AmqpBrokerServer` (lifecycle), `WireConnection` (connection FSM + handshake + watchdog + write gate), `ChannelContext` (channel FSM + content assembly + consume), `HeartbeatWatchdog`, `BrokerServerOptions`/`BrokerUser`, `IChannelDeliveryHost` |
| **Memory** | `RedisMQ.Broker.Memory` | `InMemoryEngine` (IRouteEngine impl), `InMemoryQueue` (per-queue pump), `InMemoryMessage`, `ExchangeInfo`/`QueueInfo`, `IRouteEngine`, `IQueueConsumer` |

---

## Project Layout

```
RedisMQ.sln
├── src/
│   └── RedisMQ.Broker/                     # core engine, zero external deps
│       ├── RedisMQ.Broker.csproj
│       ├── Codec/
│       │   ├── AmqpFrame.cs                # frame record + type constants (0xCE end)
│       │   ├── ByteReader.cs               # BE primitives over ReadOnlySpan<byte>
│       │   ├── ByteWriter.cs               # BE primitives + shortstr/longstr/field-table/bits
│       │   ├── ContentHeaderCodec.cs       # content header + property-flag codec
│       │   ├── Exceptions.cs               # AmqpDecode/Encode/ProtocolException
│       │   ├── FieldTable.cs               # FieldTable + FieldValue union model
│       │   ├── FrameDecoder.cs             # byte sequence → frames (partial-safe)
│       │   ├── FrameWriter.cs              # frame assembly helpers
│       │   └── MethodCodec.cs              # method encode/decode via registry + AmqpMethod
│       ├── Memory/
│       │   ├── InMemoryEngine.cs           # IRouteEngine impl (exchanges/queues/bindings)
│       │   ├── InMemoryMessage.cs          # stored message (props, body, routing, redelivered)
│       │   ├── InMemoryQueue.cs            # per-queue ready buffer + pump + RR cursor
│       │   ├── IQueueConsumer.cs           # pump-facing consumer contract
│       │   └── IRouteEngine.cs             # topology/routing seam (M1-swappable)
│       ├── Protocol/
│       │   ├── AmqpConstants.cs            # M0 reply/error codes (from codegen fixture)
│       │   └── MethodRegistry.cs           # method schema table (from codegen fixture)
│       ├── Server/
│       │   ├── AmqpBrokerServer.cs         # lifecycle + accept loop
│       │   ├── BrokerServerOptions.cs      # port, vhosts, users, frame-max, heartbeat
│       │   ├── BrokerUser.cs               # PLAIN credentials
│       │   ├── ChannelContext.cs           # channel FSM, consume, content assembly
│       │   ├── HeartbeatWatchdog.cs        # fake-clock-able silence kill
│       │   ├── IChannelDeliveryHost.cs     # connection-owned write surface for pumps
│       │   └── WireConnection.cs           # per-connection read loop + connection FSM
│       └── Transport/
│           ├── SocketListener.cs           # TCP IListenerTransport (port 0 → ephemeral)
│           └── TransportPair.cs            # in-proc duplex pair + IListenerTransport
├── tests/
│   ├── RedisMQ.Broker.Tests/               # codec/FSM unit tests (xunit v3)
│   │   ├── RedisMQ.Broker.Tests.csproj
│   │   ├── fixtures/
│   │   │   └── rabbitmq-codegen.json       # canonical AMQP 0-9-1 codegen (offline)
│   │   ├── ByteCodecTests.cs
│   │   ├── ChannelTopologyTests.cs
│   │   ├── ConnectionFsmTests.cs
│   │   ├── ConsumeTests.cs                 # consume path (Task 7)
│   │   ├── FrameCodecTests.cs
│   │   ├── MethodRegistryTests.cs
│   │   ├── PlaceholderTests.cs
│   │   └── PublishTests.cs
│   └── RedisMQ.Conformance/                # real-client tests (RabbitMQ.Client 7.x)
│       ├── RedisMQ.Conformance.csproj
│       ├── ErrorTests.cs                   # error-surface (404/405/406 closes)
│       ├── FlowTests.cs                    # end-to-end flow (publish/consume/qos/get)
│       ├── PlaceholderTests.cs  (deleted)
│       ├── TestBrokerHost.cs               # ephemeral TCP host harness
│       └── WireFlowTests.cs                # channel.flow at raw wire level
├── .github/
│   └── workflows/
│       └── ci.yml                          # ubuntu-latest, dotnet 10.0.x
├── .gitignore
├── global.json
├── handoff.md
└── README.md
```

---

## Running the Tests

### Preferred (CI path)

```bash
dotnet build RedisMQ.sln
dotnet test RedisMQ.sln
```

### Direct runner (this dev machine — `dotnet test` is broken by an SDK MTP bug)

```bash
# Unit tests (233 expected)
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll

# Conformance suite (15 expected)
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
```

### Filtered runs

The direct runner supports xunit v3 filters:

```bash
# Run only one test class
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConsumeTests"

# Run only one test method
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -method "RedisMQ.Broker.Tests.ConsumeTests.Qos_prefetch_count_gates_delivery_until_a_message_is_acked"
```

---

## Conformance Suite

The conformance project (`tests/RedisMQ.Conformance`) uses **RabbitMQ.Client 7.2.2** against a real
TCP socket to the in-process broker. It validates that RedisMQ speaks the same dialect a real
RabbitMQ broker does.

### Test files

| File | What it covers |
|------|----------------|
| `TestBrokerHost.cs` | Harness: binds ephemeral TCP port, runs `AmqpBrokerServer` over `SocketListener`, guest/guest on vhost `/` |
| `FlowTests.cs` | End-to-end flow: connect/auth, declare/bind/publish/consume, content properties round-trip, QoS count gating, nack/redelivery, basic.get, round-robin, no-ack, clean close |
| `ErrorTests.cs` | Error surface: publish to unknown exchange (404), redeclare mismatch (406), second exclusive consumer (405), consume unknown queue (404), clean close |
| `WireFlowTests.cs` | Wire-level `channel.flow` over raw `TcpClient` + broker codec primitives (no client API exists in RabbitMQ.Client 7.x) |

### Known client quirk (documented in tests)

RabbitMQ.Client 7.x decodes field-table longstr (`'S'`) header values as `byte[]`, not `string`.
The conformance tests round-trip these back with `Encoding.UTF8.GetString((byte[])value)`. This is
a client-side behavior, not a broker bug — RedisMQ correctly emits `'S'` + longstr on the wire.

---

## API Reference

### Core library (`RedisMQ.Broker`)

#### `BrokerServerOptions`

Broker-wide settings.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Port` | `int` | 0 | TCP port (0 = OS-assigned ephemeral) |
| `Vhosts` | `string[]` | `["/"]` | Openable virtual hosts |
| `Users` | `IReadOnlyList<BrokerUser>` | `[]` | PLAIN credentials |
| `FrameMax` | `uint` | 131072 | Frame-max offered at connection.tune |
| `Heartbeat` | `ushort` | 60 | Heartbeat offered at connection.tune (seconds) |

#### `BrokerUser(name, password)`

A single PLAIN SASL account.

#### `AmqpBrokerServer(options, engine, listener?)`

The broker lifecycle. Construct with options, an `IRouteEngine`, and an optional
`IListenerTransport` (tests inject `TransportPair`s; production uses `SocketListener`).

- `StartAsync()` — arms the accept loop
- `StopAsync()` — cancels accepting and drains connections

#### `IRouteEngine`

The topology/routing seam. M0's `InMemoryEngine` implements it; M1 will provide
`RedisStreamRouteEngine`.

| Method | Purpose |
|--------|---------|
| `GetExchange(vhost, name)` | Lookup exchange |
| `DeclareExchange(exchange)` | Atomic create-only declare (returns prior if exists) |
| `GetQueue(vhost, name)` | Lookup queue |
| `DeclareQueue(queue)` | Atomic create-only declare (returns prior if exists) |
| `Bind(vhost, exchange, queue, routingKey)` | Record a binding |
| `Unbind(...)` | Drop a binding |
| `RouteTo(exchange, properties, routingKey)` | Queue names for a direct route (binding-insertion order) |
| `Enqueue(vhost, queue, properties, body, exchange, routingKey)` | Land a publish in a queue |
| `RegisterConsumer(vhost, queue, consumer)` | Register a consumer (exclusive arbitration) |
| `UnregisterConsumer(vhost, queue, tag)` | Drop a consumer |
| `Requeue(vhost, queue, message)` | Return unacked message to ready tail with `Redelivered=true` |
| `TryClaimForGet(vhost, queue, out msg, out remaining)` | Atomic basic.get head pop |
| `Nudge(vhost, queue)` | Arm/re-spawn a queue's delivery pump |

#### `ExchangeInfo(vhost, name, type, durable, autoDelete, internal)`

Declared exchange manifest.

#### `QueueInfo(vhost, name, durable, exclusive, autoDelete)`

Declared queue manifest.

#### `InMemoryMessage(props, body, exchange, routingKey, envelopeId, redelivered)`

A stored message. `EnvelopeId` is engine-global, monotonic, 1-based. `Redelivered` flips to `true`
on requeue.

---

## Protocol Implementation Notes

### Wire dialect

AMQP 0-9-1 as spoken by RabbitMQ:
- Protocol header: `"AMQP\0\0\9\1"` (8 bytes)
- Frame-end octet: `0xCE`
- Frame overhead: 8 bytes (type 1 + channel 2 + size 4 + end 1)
- Minimum negotiated frame-max: 4096 (`frame_min_size`)

### Connection handshake

1. Client sends protocol header; server validates or replies with its own header on mismatch.
2. `connection.start` (server) → `connection.start-ok` (client, PLAIN only).
3. `connection.tune` (server) → `connection.tune-ok` (client). Frame-max = min(offer, client);
   a TuneOk below 4096 is refused with `not_allowed` (never clamped up). Heartbeat adopted
   verbatim; 0 disables.
4. `connection.open` (client, vhost) → `connection.open-ok` (server). Unknown vhost → `invalid-path`
   (402).
5. `channel.open` → `channel.open-ok`; channel numbers then run independent FSMs.

### Channel method set (M0)

| Class | Method | Ids | Server behavior |
|-------|--------|-----|-----------------|
| 20 | channel.open | (20,10) | Starting → Running, open-ok |
| 20 | channel.flow | (20,20) | `active=true` → flow-ok; `active=false` → hard **540 NOT_IMPLEMENTED** close |
| 20 | channel.close | (20,40) | close-ok, retire channel number |
| 20 | channel.close-ok | (20,41) | retire channel number |
| 40 | exchange.declare | (40,10) | type check → default-exchange refusal → equivalence → create |
| 50 | queue.declare | (50,10) | reserved-prefix check → existence → equivalence → create |
| 50 | queue.bind | (50,20) | shortcuts → default-exchange refusal → existence → bind |
| 50 | queue.unbind | (50,50) | always replies |
| 60 | basic.qos | (60,10) | stores count+size; `global=true` demoted to per-channel |
| 60 | basic.consume | (60,20) | queue lookup → tag reuse check → register → consume-ok (both tag flavors) |
| 60 | basic.cancel | (60,30) | unregister; cancel-ok echoes any tag |
| 60 | basic.publish | (60,40) | content assembly → routing → enqueue |
| 60 | basic.get | (60,70) | atomic claim → get-ok + content, or get-empty |
| 60 | basic.ack | (60,80) | settle ledger (tag 0, multiple = whole ledger) |
| 60 | basic.reject | (60,90) | settle single, optional requeue |
| 60 | basic.nack | (60,120) | settle single/multiple, optional requeue |
| 60 | basic.recover | (60,110) | `requeue=true` → recover-ok + requeue channel ledger; `false` → hard 540 |

### Error codes (`AmqpConstants`)

| Code | Name | Use |
|------|------|-----|
| 311 | `ContentTooLarge` | body over 134,217,728 bytes → hard connection close |
| 312 | `NoRoute` | basic.return for mandatory unroutables |
| 320 | `ConnectionForced` | — |
| 402 | `InvalidPath` | unknown vhost → hard connection close |
| 403 | `AccessRefused` | bad PLAIN / reserved `amq.*` name → soft channel close |
| 404 | `NotFound` | unknown exchange/queue → soft channel close |
| 405 | `ResourceLocked` | exclusive arbitration → soft channel close |
| 406 | `PreconditionFailed` | equivalence mismatch / unknown delivery tag → soft channel close |
| 501 | `FrameError` | oversize / bad body fragment → hard connection close |
| 502 | `SyntaxError` | malformed method payload → hard connection close |
| 504 | `ChannelError` | second channel.open / expected channel.open → hard |
| 505 | `UnexpectedFrame` | content/method out of phase → soft channel close (M0 pin) |
| 530 | `NotAllowed` | TuneOk frame-max < 4096 → hard connection close |
| 540 | `NotImplemented` | out-of-M0-set methods / `channel.flow(active=false)` → hard |
| 541 | `InternalError` | encode failures → hard connection close |

### Prefetch gating

- `prefetch-count` (short): max outstanding unacked deliveries per channel. 0 = unlimited.
- `prefetch-size` (long): RedisMQ extension — max outstanding unacked **body bytes** per channel.
  RabbitMQ refuses `prefetch-size != 0`; M0 accepts it as a documented bytes-gating extension.
- `global=true` is demoted to per-channel (RabbitMQ 4.4 behavior with `global_qos` flag off).
- `basic.get` is **never** prefetch-refused (one-shot dequeue), but its unacked entry counts toward
  the channel gate.

### Content assembly

`basic.publish` opens the channel's content assembler: header frame → N body frames → dispatch.
- Body over 134,217,728 bytes → hard `content-too-large` (311) connection close at header arrival.
- Empty body frame while bytes owed, or fragment overshooting declared size → hard `frame-error` (501).
- `immediate=true` → hard `not_implemented` (540).
- Unknown exchange → soft `not-found` (404) channel close.
- Default exchange routes to the queue named by the routing key (no binding table; absent queue = unroutable, not not-found).
- Unroutable + `mandatory=true` → `basic.return` (312 `NO_ROUTE`) with full content back on channel.
- Unroutable + `mandatory=false` → silent drop.
- Success → no reply (basic.publish is one-way).

### Delivery and credits

- Delivery tags ascend monotonically per channel (1, 2, 3 …).
- `ack(0, multiple=true)` settles **every** outstanding entry (`collect_acks` semantics).
- `ack(N, multiple=true)` settles all entries with tag ≤ N (prefix semantics).
- Unknown single tag → soft `precondition-failed` (406) `unknown delivery tag N` (ids 60/80).
- `nack(requeue=true)` returns to queue **tail** with `redelivered=true` (M0 stream-consonant placement).
- `reject(requeue=false)` drops (settle-dequeue).
- `recover(requeue=true)` requeues the channel's whole unacked ledger, reply precedes redelivery.
- `recover(requeue=false)` → hard 540 `not_implemented`.
- Channel close/teardown requeues all unacked deliveries and unregisters consumers.

---

## Extending to M1 (Redis Streams)

M0's `IRouteEngine` is the seam. To build M1:

1. Implement `RedisStreamRouteEngine : IRouteEngine` backed by Redis Streams (`XADD` for enqueue,
   consumer groups for deliveries/acks, `XCLAIM` for recover/requeue).
2. Store durable topology metadata (exchange/queue/bindings) in Redis.
3. Construct `AmqpBrokerServer(options, redisEngine, socketListener)` — **no codec or wire changes**.

The `IRouteEngine` contract is intentionally minimal and lock-based so M1 can add async Redis calls
behind the same interface without changing `ChannelContext` or `WireConnection`.

---

## Rulings and Deviations

Documented in `.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/progress.md`. Key ones:

- **channel.flow(active=false)** → hard connection close 540 `NOT_IMPLEMENTED - active=false`
  (rabbit_channel 1789-1790); `active=true` → flow-ok + delivery continues.
- **qos global=true** → demoted to per-channel, not a hard error (rabbit 1583-1586).
- **no-local=true** → accepted and ignored (rabbit channel handler pattern-drops it).
- **basic.get** → never prefetch-refused, but unacked entries count toward the gate (rabbit TODO 1594-1595).
- **ack(0, multiple=true)** → whole ledger; prefix semantics for `ack(N, multiple=true)`.
- **channel.flow** has no client API in RabbitMQ.Client 7.x → wire-level tests only.
- **Field-table `'S'` longstr** → RabbitMQ.Client 7.x decodes as `byte[]`; tests round-trip back to string.
- **Frame-max TuneOk < 4096** → hard `not_allowed` close (never clamped up).
- **Durable/auto-delete** → accepted and stored, behave as non-durable.
- **`amq.*` prefix** → refused on creation only (predeclared `amq.direct` exempt).

---

## CI

`.github/workflows/ci.yml` runs on `ubuntu-latest` with the .NET 10.0.x SDK:

```yaml
jobs:
  test:
    steps:
      - checkout
      - setup-dotnet (10.0.x)
      - dotnet build RedisMQ.sln
      - dotnet test RedisMQ.sln
```

The workflow carries the standard `dotnet test` even though this dev machine uses direct runners;
CI is the intended green path.
