# RedisMQ M0 — Protocol Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A .NET 10 AMQP 0-9-1 (RabbitMQ dialect) wire server that real RabbitMQ client libraries can connect to, declare a direct exchange + queues, publish, consume, ack/nack/reject with prefetch credits — all against an **in-memory** queue engine (no Redis yet). This proves the wire layer; M1 swaps the memory engine for Redis Streams.

**Architecture:** `System.Net.Sockets` listener + `System.IO.Pipelines` frame pipeline; span-based AMQP codec (bytes → frames → connection/channel state machines); method/property schemas loaded from the canonical AMQP 0-9-1 codegen JSON (fixture); all routing for M0 is an in-memory direct-exchange engine behind an interface the M1 Redis engine will implement.

**Tech Stack:** .NET 10 (`net10.0`), no runtime dependencies in the core; xunit v3 tests; `RabbitMQ.Client` (7.x) only in the conformance test project.

**Spec:** `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md` (this plan implements its M0 milestone; read both).

## Global Constraints

These apply to every task, copied from the spec:

- Target framework: `net10.0` only. Nullable: enable. Core package has **zero external dependencies** for M0.
- Wire dialect: AMQP 0-9-1 as spoken by RabbitMQ (protocol header `"AMQP\0\0\9\1"`, frame-end `0xCE`).
- Defaults: frame-max negotiation floor **131072**; heartbeat floor **60 s**; watchdog kills at **2× negotiated heartbeat**; default vhost **`/`**.
- SASL: mechanism **PLAIN** only in M0; PLAIN response is `authzid\0authcid\0password` (NUL-separated, authzid usually empty).
- M0 method set (only these are implemented; everything else gets `NOT_IMPLEMENTED` close): connection (start, start-ok, secure?, tune, tune-ok, open, open-ok, close, close-ok, blocked, unblocked), channel (open, open-ok, flow, flow-ok, close, close-ok), exchange (declare, declare-ok), queue (declare, declare-ok, bind, bind-ok, unbind, unbind-ok), basic (qos, qos-ok, consume, consume-ok?, cancel, cancel-ok, publish, deliver, get, get-ok, get-empty, ack, nack, reject, recover, recover-ok).
- Error numbers/bit numbers/property bits are **not hand-typed**: they come from the canonical codegen JSON fetched in Task 3 (source of truth; fixture saved in-repo so CI stays offline).
- M0 has no confirms, no topic exchange, no TTL, no persistence; durable/auto-delete flags are accepted and stored but behave as non-durable (documented in code).
- Commit style: Conventional Commits; every task commits green.
- Repo: this task list starts by running `git init` in `C:\source\RedisMq`; `.superpowers/` and `docs/` stay in the repo; add `.superpowers/` **and** `bin/`, `obj/`, `.idea/` to `.gitignore`.

## Review Focus

The five failure classes the spec implies that no happy-path test catches (each owns its pinning test below):

1. **Malformed wire — bad frame-end ≠ `0xCE`, frame larger than negotiated frame-max, or truncated frames:** the connection must send `connection.close` (hard error) and never corrupt the pipeline. Pinned in Task 4 (Step 9, malformed-frame tests).
2. **Illegal method sequencing per channel — e.g. content body frames arriving with no content-header, or a method arrived while channel just closed:** must close the *channel* with an unexpected-frame error or reply cleanly — never kill the connection for a soft error. Pinned in Task 5 (Step 9).
3. **Heartbeat stewardship with slow input:** watchdog must reset on *any* inbound frame (not just heartbeats), and must never fire while a read is actively progressing; two idle connections at different heartbeats must not cross-trigger. Pinned in Task 4 (Step 10, fake-clock watchdog tests).
4. **Interleaved channel multiplexing:** frames for different channels alternating in one connection must each take their own state machine without cross-talk. Pinned in Task 5 (Step 10).
5. **Field table and property-flag round-trip fidelity** — stop-bit flag encoding, all field-table value types (incl. RabbitMQ byte-array extension), signed/unsigned int variants: decode(encode(x)) must be byte-identical. Pinned in Task 1 (Step 9) and Task 3 (Step 7).

---

## File Structure

```
RedisMQ.sln
src/RedisMQ.Broker/                       # core engine (zero deps)
  RedisMQ.Broker.csproj
  Codec/ByteReader.cs                     # BE primitives over ReadOnlySpan<byte>
  Codec/ByteWriter.cs                     # BE primitives + shortstr/longstr/field-table
  Codec/FieldTable.cs                     # FieldValue union model + encode/decode
  Codec/AmqpFrame.cs                      # frame record + type constants
  Codec/FrameDecoder.cs                   # sequence → frame (partial reads safe)
  Codec/MethodCodec.cs                    # method args encode/decode via MethodRegistry
  Codec/ContentHeaderCodec.cs             # content header + property bits
  Protocol/AmqpConstants.cs               # header bytes, frame-end, error codes
  Protocol/MethodRegistry.cs              # class/method ids + arg schema (from fixture)
  Server/BrokerServerOptions.cs           # port, users, vhosts, frame-max, heartbeat
  Server/AmqpBrokerServer.cs              # lifecycle + accept loop
  Server/WireConnection.cs                # per-connection read loop + connection FSM
  Server/HeartbeatWatchdog.cs             # fake-clock-able idle kill
  Server/ChannelContext.cs                # per-channel state, unacked map, credits
  Memory/InMemoryEngine.cs                # exchanges/queues/registry (M0 router)
  Memory/InMemoryQueue.cs                 # reader-side queue with credit gating
tests/RedisMQ.Broker.Tests/               # codec/FSM unit tests (xunit v3)
  RedisMQ.Broker.Tests.csproj
  fixtures/rabbitmq-codegen.json          # saved in Task 3, referenced by registry tests
tests/RedisMQ.Conformance/                # real-client tests (RabbitMQ.Client 7.x)
  RedisMQ.Conformance.csproj
  TestBrokerHost.cs                       # spawns AmqpBrokerServer on port 0, returns port
  FlowTests.cs, ErrorTests.cs
```

---

### Task 0: Repository + solution scaffold

**Files:**
- Create: `RedisMQ.sln`, `src/RedisMQ.Broker/RedisMQ.Broker.csproj`, `tests/RedisMQ.Broker.Tests/RedisMQ.Broker.Tests.csproj`, `tests/RedisMQ.Conformance/RedisMQ.Conformance.csproj`, `.gitignore`

- [ ] **Step 1: git init + .gitignore**

```bash
git init
```
`.gitignore` contains at least: `bin/`, `obj/`, `.superpowers/`, `.idea/`, `*.user`, `TestResults/`. (`.superpowers/` is the brainstorm session dir — do not commit it.)

- [ ] **Step 2: create solution + projects**

`dotnet new sln -n RedisMQ`; library `src/RedisMQ.Broker` (`net10.0`, `Nullable=enable`, `LangVersion=latest`, **no PackageReferences**); test projects `tests/RedisMQ.Broker.Tests` (xunit v3, `xunit.v3`, `xunit.runner.visualstudio` adapted, `Microsoft.NET.Test.Sdk`) and `tests/RedisMQ.Conformance` (same test stack + `RabbitMQ.Client` **7.x**); reference core from both tests. Add all to the solution.

- [ ] **Step 3: run tests to verify scaffold green**

Run: `dotnet test`
Expected: 0 failures (placeholder test files with one trivial pass test each).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: scaffold RedisMQ solution (broker core + unit/conformance tests)"
```

### Task 1: Byte-level codec primitives

**Files:**
- Create: `src/RedisMQ.Broker/Codec/ByteReader.cs`, `src/RedisMQ.Broker/Codec/ByteWriter.cs`, `src/RedisMQ.Broker/Codec/FieldTable.cs`
- Test: `tests/RedisMQ.Broker.Tests/ByteCodecTests.cs`

**Interfaces (produced — later tasks consume these exactly):**
- `ByteReader(ReadOnlySpan<byte>)` with: `byte ReadOctet(); ushort ReadUShort(); short ReadShort(); int ReadLong(); long ReadLongLong(); string ReadShortstr(); byte[] ReadLongstr(); bool TryReadFieldTable(out FieldTable); bool ReadBit(); DateTimeOffset ReadTimestamp();` — all big-endian; `ReadBit()` consumes bit-pairs sequentially from a packed bitfield; throws `AmqpDecodeException` on truncation.
- `ByteWriter` with same `Write*` octet→longlong pair + `WriteShortstr(string)`, `WriteLongstr(ReadOnlySpan<byte>)`, `WriteFieldTable(FieldTable)`, `WriteBits(params bool[])`, instance property `Length`, method `byte[] ToArray()`.
- `FieldTable : IReadOnlyDictionary<string, FieldValue>`; `FieldValue` = struct with `char TypeLetter` + `object Raw` and static factories: `FieldValue.From(bool|sbyte|byte|short|ushort|int|long|float|double|decimal|string|byte[]|DateTimeOffset|FieldTable)`, `FieldValue.List(...)` and `FieldValue.Null`. Type letters map from the codegen fixture (`t`,`b`,`B`,`s`,`u`,`I`,`l`,`L`,`f`,`d`,`D`,`S`,`A`,`F`,`V`,`x` — RabbitMQ extension `x` = byte array). Encode writes the shortest RabbitMQ-accepted letter for each CLR type.

- [ ] **Step 1: Write the failing tests (ByteCodecTests.cs)**

Cover: round-trip of every `Write*`/`Read*` pair at sentinel values incl. `short` negatives, `ushort` 65535, long 2^62; shortstr ASCII + 255-byte-length cap; bit-packing (e.g. bits `[true,false,true]` → two octets `0x05` pattern per AMQP bit packing); all FieldValue letters both directions asserting **byte-identical re-encode** (`Assert.Equal(original, reencoded)`); truncation of every type mid-stream throws `AmqpDecodeException` (theory over each type); decimal round-trip (dot-decimal encoding per AMQP: octets + digits + exponent).

- [ ] **Step 2: Run tests to verify red**

Run: `dotnet test tests/RedisMQ.Broker.Tests --filter "FullyQualifiedName~ByteCodec"`
Expected: FAIL (missing types).

- [ ] **Step 3: Implement the three files**

`ByteReader` advances a span + offset counter; `FieldTable.cs` owns the letter→codec switch. No type inference tricks: encode switches on CLR type, decode switches on the wire letter.

- [ ] **Step 4: Run tests to verify green**

Run: `dotnet test tests/RedisMQ.Broker.Tests --filter "FullyQualifiedName~ByteCodec"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(codec): byte primitives and field tables"
```

### Task 2: Frame codec (frames off the stream)

**Files:**
- Create: `src/RedisMQ.Broker/Codec/AmqpFrame.cs`, `src/RedisMQ.Broker/Codec/FrameDecoder.cs`, `src/RedisMQ.Broker/Codec/ContentHeaderCodec.cs`
- Test: `tests/RedisMQ.Broker.Tests/FrameCodecTests.cs`

**Interfaces (produced):**
- `AmqpFrame` record: `byte Type; ushort Channel; ReadOnlyMemory<byte> Payload;` constants: `FrameMethod=1, FrameHeader=2, FrameBody=3, FrameHeartbeat=8, FrameEnd=0xCE, FrameMinSize=4096`.
- `FrameDecoder` supports partial/segmented buffers: ` DecodeMode ReadFrames(ref ReadOnlySequence<byte>, List<AmqpFrame>, ushort negotiatedFrameMax)` — produces 0..n frames, removes consumed bytes; returns protocol state (NeedMore / Error). On frame-end mismatch or size > frameMax throws `AmqpProtocolException` (hard-error class).
- `ContentHeaderCodec`: `ContentHeader Read(ref ByteReader)` → `(ushort ClassId, ushort Weight, long BodySize, ContentProperties Props)` where `ContentProperties` is a record with nullable fields in 0-9-1 basic property order (contentType, contentEncoding, headers: FieldTable, deliveryMode: byte, priority: byte, correlationId, replyTo, expiration, messageId, timestamp, type, userId, appId, clusterId) parsed by flag bits with the trailing stop-bit rule; `Write(ByteWriter, ClassId, BodySize, ContentProperties)`.
- `FrameWriter` (static): `WriteMethod(ref ByteWriter, ushort channel, ushort classId, ushort methodId)` producing frame bytes for a prepared args payload; `WriteContentHeader(...)`, `WriteBody(...)` (splits >frameMax into N body frames).

- [ ] **Step 1: Write failing tests**

Golden-byte tests: method frame for `connection.tune` (handwrite expected bytes); heartbeat frame (7 bytes, all zeros payload); body splitting of a 200,000-byte message at frameMax=131072 → 2 body frames, exact byte counts; out-of-order channel prefix preserved (`frame.Channel` per frame); property-flags encode cases: ({contentType:null with headers set → bits pattern}), stop-bit boundary at bit 16 (flags=0x0001 first property → encoded flag seq with continuation bit); partial feeds: feed a frame in 3-byte chunks → one frame at the end; frame-end wrong byte → `AmqpProtocolException`; size > frameMax → exception.

- [ ] **Step 2: Run red:** `dotnet test tests/RedisMQ.Broker.Tests --filter "FullyQualifiedName~FrameCodec"` → FAIL.
- [ ] **Step 3: Implement** the three files (guard: decoder never reads past frame-end mid-sequence — it is the only place that decides hard errors).
- [ ] **Step 4: Run green** (same filter) → PASS.
- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(codec): frame layer with property flags and body framing"
```

### Task 3: Method registry from canonical codegen JSON

**Files:**
- Create: `src/RedisMQ.Broker/Protocol/AmqpConstants.cs`, `src/RedisMQ.Broker/Protocol/MethodRegistry.cs`, `tests/RedisMQ.Broker.Tests/fixtures/rabbitmq-codegen.json`
- Test: `tests/RedisMQ.Broker.Tests/MethodRegistryTests.cs`

**Interfaces (produced):**
- `AmqpConstants` error-code table for M0 close/error codes (values read from the fixture's `errors` array), each as `const` with spec names (`invalid-path`, `frame-error`, `syntax-error`, `not-allowed`, `precondition-failed`, `not-found`, `access-refused`, `unexpected-frame`, `internal-error`, `not-implemented`, `content-too-large`, `connection-forced`).
- `MethodRegistry`: static descriptor `MethodSchema schema(int classId, ushort methodId)`; `MethodSchema` = `(ushort ClassId, ushort MethodId, string Name, Direction Direction, IReadOnlyList<AmqpField> Fields)`; `AmqpField` = `(string Name, WireType Type /* Octet|Short|Long|LongLong|Shortstr|Longstr|Table|Timestamp|Bit|PackedBit|DependingBit */)` (AMQP "bit fields in sequence" are represented as `DependingBit` chain). Registry covers the M0 method set (Global Constraints list) **and** their content properties.
- `MethodCodec`: `AmqpMethod ReadMethod(ref ByteReader, MethodSchema)`; `void WriteMethod(ref ByteWriter, in AmqpMethod)`; `AmqpMethod` typed accessor (`Get(string name)/Set(string name, object?)` boxed model is acceptable for M0).

**Source of truth:** scroll https://raw.githubusercontent.com/rabbitmq/rabbitmq-codegen/main/amqp-rabbitmq-0.9.1.json — fetch once into `tests/RedisMQ.Broker.Tests/fixtures/` and commit. The registry tables (class/method ids, field names/orders/types, error ids) MUST be generated/verified from this file, not typed by hand. If the URL 404s, fall back to the same file under the `rabbitmq/rabbitmq-codegen` repo tags (e.g. `v4.0.7`).

- [ ] **Step 1: Fetch fixture + write failing tests**

Save the JSON. Tests: for each M0 method the registry's ids/field list/order match the fixture verbatim (parse fixture in test, assert equality for the listed methods); every referenced error code matches the fixture's errors array; spot assert `connection.start` has fields listing `version-major` as first field and `queue.declare`'s fields end with `arguments` table.

- [ ] **Step 2: Run red:** `dotnet test tests/RedisMQ.Broker.Tests --filter "FullyQualifiedName~MethodRegistry"` → FAIL.
- [ ] **Step 3: Implement registry + codec** (a small source-generation-by-hand step: read the fixture now and produce the table; add a test-side parse-loop that re-verifies every M0 schema each CI run so drift fails marks).
- [ ] **Step 4: Run green** → PASS.
- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(protocol): method registry generated from canonical 0-9-1 fixture"
```

### Task 4: Connection FSM, transport, handshake, heartbeats

**Files:**
- Create: `src/RedisMQ.Broker/Server/BrokerServerOptions.cs`, `src/RedisMQ.Broker/Transport/TransportPair.cs`, `src/RedisMQ.Broker/Server/WireConnection.cs`, `src/RedisMQ.Broker/Server/HeartbeatWatchdog.cs`, `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Test: `tests/RedisMQ.Broker.Tests/ConnectionFsmTests.cs`

**Interfaces:**
- Consumes: `FrameDecoder`, `MethodCodec` (Tasks 2–3).
- Produces: `BrokerServerOptions { int Port; string[] Vhosts = { "/" }; IReadOnlyList<BrokerUser> Users; ushort FrameMax=131072; ushort Heartbeat=60; }` with `BrokerUser(string Name, string Password)`; `TransportPair.Create()` → symmetric duplex in-proc pair returning `{TransportClient, TransportServer}` where each side is `{ PipeReader Reader; PipeWriter Writer; ValueTask CompleteAsync() }`; `AmqpBrokerServer` ctor `AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine)`; `Task StartAsync()` binds and accepts (TCP bind deferred to Task 8 — ctor accepts an optional `IListenerTransport` so tests inject `TransportPair`s); `WireConnection` handles: protocol header, Start/StartOk/PLAIN, Tune/TuneOk (min of offer and client), Open/OpenOk (vhost exists else `connection.close` w/ `invalid-path`), thread channels; `HeartbeatWatchdog(interval, onTimeout)` with `protected virtual DateTimeOffset Now` (fake clock in tests).

- [ ] **Step 1: Failing FSM tests (scripted client over TransportPair)**

Drive raw bytes as an "evil client" through `TransportClient`: (a) correct header + StartOk PLAIN with valid creds → expect Tune then TuneOk then Open → `OpenOk`; (b) bad password → `connection.close`(`access-refused` code, wording matches spec strings); (c) unknown-vhost Open → `connection.close`(`invalid-path`); (d) malformed frame-end → close with frame-error; (e) frame bigger than frameMax → close; (f) method with unknown class id → close with `syntax-error`/`not-implemented` (`not-implemented` for valid class outside M0 set); (g) TuneOk heartbeat `0` disables watchdog (no timeout); (h) client `connection.close` → reply `close-ok` and drain.

- [ ] **Step 2: Run red** (`--filter "FullyQualifiedName~ConnectionFsm"`), **Step 3: implement**, **Step 4: green**.
- [ ] **Step 5: Fake-clock watchdog tests:** two watchdogs at 60 s and 120 s survive 61 s of “frame-token” ticks but the 60 s at 121 s of silence dies; watchdog resets on any inbound frame type. Green → **Step 6: commit**

```bash
git add -A
git commit -m "feat(server): connection fsm, plain sasl, tune negotiation, heartbeat watchdog"
```

### Task 5: Channel FSM + in-memory topology (declare/bind)

**Files:**
- Create: `src/RedisMQ.Broker/Server/ChannelContext.cs`, `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs` (route channel frames)
- Test: `tests/RedisMQ.Broker.Tests/ChannelTopologyTests.cs`

**Interfaces:**
- Produces: `IRouteEngine` (interface M1 will reimplement): `ExchangeInfo? GetExchange(string vhost, string name); void DeclareExchange(...); QueueInfo? GetQueue(...); void DeclareQueue(...); void Bind(...); void Unbind(...); IEnumerable<string> RouteTo(ExchangeInfo, in ContentProperties props, string routingKey)`. M0 only direct type; `amq.direct` predeclared; names starting `amq.` rejected (403 `access-refused`).
- `ChannelContext`: open/close FSM, tracks pending content state (awaiting header/body), unacked map, per-consumer credit, and soft-vs-hard error classification: unknown entity on action → `not-found` **channel** close; redeclare mismatch → `precondition-failed` (406) channel close; unsupported queue argument → `not-implemented` channel close.

- [ ] **Step 1: Write the failing tests** — via the Task 4 scripted-client harness: (a) open two channels on one connection; (b) `exchange.declare` + `queue.declare` happy paths; (c) redeclare with different durable flag → channel close `precondition-failed`; (d) bind/unbind direct; (e) send a method on a *closed* channel → channel-close error, connection must survive; (f) interleaved frames alternating between the two channels (declare on ch1, bind on ch2, out of order) with no cross-talk; (g) reserved `amq.*` names → `access-refused`; (h) unknown method on a channel → `not-implemented` channel close.
- [ ] **Step 2: Run test to verify it fails** — `dotnet test tests/RedisMQ.Broker.Tests --filter "FullyQualifiedName~ChannelTopology"` → FAIL.
- [ ] **Step 3: Implement** the two files.
- [ ] **Step 4: Run test to verify it passes** → PASS.
- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(server): channel fsm and in-memory topology engine"
```

### Task 6: Publish path (direct fan-out on memory engine)

**Files:**
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs` (content assembly), `src/RedisMQ.Broker/Memory/InMemoryEngine.cs` (enqueue)
- Test: `tests/RedisMQ.Broker.Tests/PublishTests.cs`

**Interfaces:**
- Consumes: `IRouteEngine.RouteTo`, `ContentHeaderCodec`.
- Produces: content-frame pipeline state (header → N body frames per channel, max body 134217728 = RabbitMQ's hard cap; reject oversize with `content-too-large` close), `InMemoryMessage { ContentProperties Props; byte[] Body; string Exchange; string RoutingKey; ulong EnvelopeId; }`.

- [ ] **Step 1: Write the failing tests** — direct-publish with header + 2 body frames lands in queue; publish with `mandatory=true` and no bound queue → `basic.return` delivered carrying properties + first 1 body frame, and no other effect; `mandatory=false` unroutable → silently dropped, queue untouched; publish without header → channel close `unexpected-frame`; body total exceeding 134,217,728 → hard `content-too-large` connection close; publish to unknown exchange → channel `not-found`; enqueue visible via `[internal]` test introspection `IReadOnlyList<InMemoryMessage> PeekQueue(string name)`.
- [ ] **Step 2: Run test to verify it fails** — `dotnet test tests/RedisMQ.Broker.Tests --filter "FullyQualifiedName~PublishTests"` → FAIL.
- [ ] **Step 3: Implement** content assembly + routing on `ChannelContext`/`InMemoryEngine`.
- [ ] **Step 4: Run test to verify it passes** → PASS.
- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(server): basic.publish content assembly and direct routing"
```

### Task 7: Consume path — prefetch credits, ack/reject/nack, get, recover

**Files:**
- Create: `src/RedisMQ.Broker/Memory/InMemoryQueue.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs` (deliver loop, ack bookkeeping)
- Test: `tests/RedisMQ.Broker.Tests/ConsumeTests.cs`

**Interfaces:**
- Produces: `basic.deliver` with correct redelivered flag; prefetch gating: consumer blocked when outstanding (count or bytes) ≥ qos; `basic.ack(delivery-tag)` frees credit; `nack(requeue:true)` → fast-path immediate redelivery (M0: same consumer suffices — M1 adds cross-node claims); `reject(requeue:false)` → drop; `basic.get`/`get-ok`/`get-empty` under the same credit accounting; `basic.recover(requeue)` requeues *this channel's* unacked set; `basic.cancel`/`cancel-ok`; when the client omits its consumer-tag the server assigns `ctag-N` and replies with a `consume-ok` carrying it (0-9-1 quirk: consume-ok only for server-assigned tags). Delivery tags increase monotonically per channel and requeue preserves original properties.

- [ ] **Step 1: Failing tests:** consume-and-deliver single; qos=1 gating needs ack; qos bytes gating (1-byte small, 512-byte gated); ack multiple=false vs `multiple:true` batch; nack requeue immediate redelivery flag true; reject drop removes from queue; basic.get-ok/get-empty; recover; cancel stops delivery; server ctag generation; unacked map survives channel flow off->on? (cover flow: `channel.flow` toggle pausing delivery then resume). **Step 2 red → Step 3 implement → Step 4 green → Step 5 commit**

```bash
git add -A
git commit -m "feat(server): consume loop with prefetch credits, acks, nack, get, recover"
```

### Task 8: TCP listener + real-client conformance (RabbitMQ.Client)

**Files:**
- Create: `src/RedisMQ.Broker/Transport/SocketListener.cs`, `tests/RedisMQ.Conformance/TestBrokerHost.cs`, `tests/RedisMQ.Conformance/FlowTests.cs`, `tests/RedisMQ.Conformance/ErrorTests.cs`
- Test project fixtures only.

**Interfaces:**
- Consumes: `AmqpBrokerServer` (accepts `IListenerTransport`; `SocketListener` provides the TCP one; port 0 → announced port).
- Produces: `TestBrokerHost.Start(Action<BrokerServerOptions>? configure = null)` → object with `int Port; Task StopAsync();` (ephemeral port, user `guest`/`guest`, vhost `/`).

- [ ] **Step 1: Failing conformance tests (RabbitMQ.Client 7.x, async API)**

Scenarios: connect (Tune/Open, PLAIN) → `CreateModel`; declare direct exchange + queue, bind, `BasicPublish` → `BasicConsume` listener receives payload byte-identical and all content properties (contentType, headers dict incl. long/bool/string values, correlationId, message id, timestamp second precision); publish N=1000 in-loop then receive all in order cross single consumer/QoS(10); QoS: publish 20/QoS 10, assert exactly 10 delivered pre-ack, ack 5 → 5 more; nack requeue redelivery & redelivered flag; `BasicGet` ok/empty; CRUD channel err comportamiento: publish unknown exchange → channel close 404 + client `IModel` closed; redeclare mismatch → 406; exclusive-consumer-like contention (two consumers single queue round-robin get all messages exactly once); cancel stops; `basic.flow` honored; consumer id 100 msgs with `no-ack=true` delivers all; connection.close clean; multiple concurrent connections independent; TLS skipped in M0.

- [ ] **Step 2:** red as needed (most M0 behavior exists; expect mostly wire fixes), **Step 3: fix until green**, **Step 4: full green**

Run: `dotnet test` (all projects)
Expected: PASS (zero failures across codec, FSM, conformance).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(conformance): rabbitmq.client end-to-end flow and error surface (m0 green)"
```

### Task 9: CI + README

**Files:**
- Create: `.github/workflows/ci.yml`, `README.md`
- Modify: docs non-code only.

- [ ] **Step 1: CI workflow:** ubuntu-latest, .NET 10 SDK (`actions/setup-dotnet`, `dotnet-version: '10.0.x'`), run `dotnet test` across all test projects. **Step 2: verify locally** — run `dotnet test` green and sanity-check the YAML (no remote/Actions run is expected yet; the workflow rides along until a remote is added). **Step 3: README** — one-paragraph "what this is (M0): an AMQP 0-9-1 wire-server shell over in-memory queues; Redis Streams lands in M1 (spec linked)"; a runnable snippet: `dotnet test tests/RedisMQ.Conformance`. **Step 4: commit**

```bash
git add -A
git commit -m "chore(ci,docs): ci workflow and m0 readme"
```

---

## Definition of done for this plan

- `dotnet test`: all green (3 projects), conformance suite exercises RabbitMQ.Client 7.x against the in-process broker over TCP.
- `RabbitMQ.Client` can: connect/auth, tune-negotiate, declare+bind 1 direct exchange, publish (any size < hard cap), consume with QoS count+bytes credits, ack/reack/nack-with-requeue/reject, basic.get, cancel, clean udp close; every error surface in Review Focus has a test that passes.
- No Redis anywhere in M0 — the memory engine implements `IRouteEngine` such that the M1 plan can add `RedisStreamRouteEngine : IRouteEngine` + durable-topology metadata without touching codec/wire code.

## Next plans (not part of this plan)

- M1 plan (written after M0 lands): Redis Streams route engine (`XADD`/consumer groups/acks/claims), durable topology store, publisher confirms, DLX, `RedisMQ.Hosting` package skeleton.
- M2 plan: vhosts/TLS, TTL reaper, eviction/persistence gates, multi-node leases (WATCH topology CAS), authZ permissions (schema from spec §5.2).
- M3 plan: topic trie + headers exchange, `x-max-length`, channel flow-control hardening.
- M4 plan: capability profiles (cluster/serverless/managed), OTel/health, perf gate vs user-confirmed envelope, packaging (nuget.org after trademark check), Pika + Java/Linux conformance.
