# RedisMQ — an AMQP 0-9-1 message broker backed by Redis Streams

Date: 2026-10-30 · Status: builder-check resolved; awaiting your read/approval
Working name: **RedisMQ** (trademark check required before any public release — see Open items)

---

## 1. Intent and why

Build a **message broker for general application-to-application messaging** that speaks the
**AMQP 0-9-1 wire protocol** (the RabbitMQ dialect) and stores all messages and topology in
**Redis Streams**. Existing RabbitMQ-format client libraries — `RabbitMQ.Client` (.NET), Pika
(Python), rabbitmq-client (Java), amqp091-go, amqplib (Node), Bunny (Ruby) — connect to it as to
RabbitMQ, but the persistence and fan-out machinery is Redis, which users already deploy in
cloud or self-managed form.

Why a NuGet: the broker ships as **one core library** with **two consumption modes**:

1. **Self-hosted** — an app embeds the broker and accepts AMQP connections (Generic Host /
   `IHostedService` wiring in a `RedisMQ.Hosting` package).
2. **Standalone** — a `dotnet tool` / executable / OCI container running the same core,
   pointed at any Redis-compatible service.

Both modes expose identical wire behavior.

## 2. Goals

- **G1 — Wire compatibility.** Mainstream AMQP 0-9-1 client libraries can connect, declare
  topology, publish, and consume. Enforced by a CI conformance suite using *real* client
  libraries (at minimum RabbitMQ.Client and Pika).
- **G2 — Reliable delivery.** At-least-once; ack / requeue / reject/nack with redelivery flag;
  publisher confirms (ordered per publisher), faithful per-queue FIFO on the normal path.
- **G3 — Exchange routing.** Direct, fanout, topic (pattern binding keys), and headers
  exchanges; durable topology (exchanges, bindings, queues) stored in Redis.
- **G4 — Dead-lettering.** `x-dead-letter-exchange` with a delivery-limit failure trigger, plus
  TTL-expiry dead-lettering (see G4/TTL reaper below).
- **G5 — Generic Redis.** Works against Redis 7.x / 8.x, Valkey, Redis Enterprise, and the
  managed equivalents (ElastiCache incl. cluster mode, Memorystore, Azure Cache, Redis Cloud,
  Upstash-class serverless). Cluster-safe key layout from day one; **capability profiles**
  decide fallback paths (module 5).
- **G6 — Operational honesty.** A **durability contract per profile**: what "publisher
  confirmed" and "durable" mean given the backing Redis persistence. Startup validation of
  eviction policy and persistence settings; refuse-or-warn (configurable) on unsafe configs.
- **G7 — Distributions.** Core engine + Hosting package + standalone dotnet-tool/container, one
  code path feeding all three.
- **G8 — Package hygiene.** `net10.0` target, nullable-enable, SourceLink, deterministic
  build, symbols on NuGet. Only hard dependency of the core: StackExchange.Redis (MIT).
- **G9 — Observability.** OpenTelemetry metrics (connections, channels, publish rate,
  confirmed, unacked, per-queue backlog/PEL size, reclaimed), health checks registered with
  `IHealthChecksBuilder` in Hosting mode, server lifecycle logging.

## 3. Non-goals (v1)

- **AMQP 1.0** — different protocol; not supported (maybe a later separate project).
- **Priority queues** (`x-priority`) — streams are strictly FIFO; faking priorities distorts
  ordering/deadlettering. Out indefinitely unless a strong need appears.
- **Transactions** (`tx.select/commit/rollback`) — **deferred to v1.1**; confirms cover the use
  case per RabbitMQ's own guidance.
- Management UI / RabbitMQ management HTTP API; federation; shovel; plugin system;
  `rabbitmqctl`-style TLS-encrypted management over wire — no.
- Exactly-once delivery (consumer idempotency stays the application's job).
- Overflow strategy `reject-publish` on bounded queues — deferred to v1.1 (`x-max-length` with
  drop-oldest via `XTRIM MAXLEN ~` ships in v1).
- Archive/compression tiers, multi-region replication.

**Anti-goals** (what would make this a failure even if it "works"):

- Wire compatibility without the semantics it implies — e.g., confirms that don't mean
  "persisted-as-agreed", silent message loss, or dead-lettering that drops messages.
- A broker that only interops with one client dialect — no.
- Reimplementing RabbitMQ the product (plugins, management plane, clustering protocols).

## 4. Constraints

- **Protocol:** AMQP 0-9-1 as actually spoken by mainstream clients (RabbitMQ dialect),
  0-9-1 spec + RabbitMQ clarifications as reference; field tables must round-trip byte-exact.
- **RESP baseline:** RESP2 everywhere; RESP3 used only when negotiated AND profile allows.
  Avoid `EVAL`/Lua for core correctness — use `XADD` pipelines and `MULTI/EXEC`; Lua only as
  later micro-optimization where profiles allow.
- **The cluster rule:** every per-queue (and per-queue heartbeat/PEL bookkeeping) key set is
  hash-tagged `{mq:q:<name>}`; topology namespace `{mq:top}`. No cross-slot commands.
- **Scale envelope (v1 perf gate — user-confirmed):** ≤500 concurrent connections,
  ≤1,000 consumers, ≤1,000 queues per vhost, sustained ≈5k msgs/s publishes, p99
  first-delivery < 20 ms against Redis on the local network; typical message ≤ 1 MB. The M4
  perf pass gates on these numbers against a local containerized Redis — not on cloud
  backends.
- **TFM:** core/broker targets `net10.0` only — clients are language-agnostic over the wire,
  so no lower-TFM burden is needed. (If a surprise need arises, `net8.0` can be added without
  API changes; builder may trim polyfills.)
- **Server stack:** plain `System.Net.Sockets` + `System.IO.Pipelines` + `SslStream` for the
  wire; **no ASP.NET/Kestrel in the core**. Per-connection async state machines; one connection,
  many channels; AMQP frames parsed with `Span<byte>`-based zero-copy-ish codec on the read path.
- **Redis client:** StackExchange.Redis only.
- **Licensing posture:** broker is a *client* of Redis; no Redis/Valkey binary distribution.
  Core package license: **Apache-2.0** (builder decides final license text file/format).
- **Naming:** "RedisMQ" internal working name; possible published names must pass a Redis Ltd
  trademark-policy check before public release.

## 5. Architecture

### 5.1 Routing engine (decided: Approach A — publish-time fan-out)

Chosen over (B) exchange-ingress-stream + dispatcher workers and (C) one shared
stream + client-side filtering. Reasons recorded in the session: B is a broker-in-broker
(leader election, idempotent bus→queue step), C breaks ack/DLX/backlog semantics; A's only cost
is one `XADD` per bound queue at publish, which is acceptable at task-scale fan-out.

- Exchange publish:
  1. Resolve active bindings for exchange × routing-key/headers.
  2. For each bound queue: `XADD {mq:q:<name>} ...` (pipelined per exchange in one round trip
     per batch; all slot-local, cluster-safe).
  3. Confirms = ordered acks back as each `XADD` is accepted (per-channel publisher-confirm
     sequence like RabbitMQ).
- Each AMQP queue = Redis Stream **+ one Redis consumer group** named after the broker's
  consumer pool. Competing consumers on a queue are group members → natural at-least-once with
  per-message state in the PEL (Pending Entries List).
- Message body and attributes stored as stream fields: routing key, headers
  (byte-exact AMQP field-table bytes), content-type/content-encoding, priority field (stored,
  not enforced), delivery-mode, timestamp, correlation/message ids, expiration, plus broker
  bookkeeping (`delivery-count`). Frame properties never re-encoded — stored raw where
  possible.
- Per-queue limits: `x-max-length` ⇒ `XTRIM MAXLEN ~ n` (drop-oldest). `x-expires` in TTL
  reaper. `reject-publish` — v1.1.
- Dead-lettering:
  - **Delivery-limit path:** reclaim (idle threshold exceeded, still unacked) bumps
    `delivery-count`; when ≥ `x-delivery-limit` (default 3? make default revisable), broker
    publishes the message body+headers to the queue's DLX with death headers
    (`x-death: queue/reason/count/time`), then `XACK`+`XDEL` the original.
  - **TTL path:** reaper moves expired entries to the DLX the same way.
- Redelivery, fast path (protocol requirement): nack / reject-with-requeue / requeue, and
  channel or connection close for a known consumption handle, must redeliver **immediately**
  in-process — the broker walks its in-memory delivery-tag map first. The idle-claim path
  (`XAUTOCLAIM`, any node, Redis-side PEL) exists only for crashed consumers/connections and
  hard faults.
- Requeue ordering (my call): AMQP permits approximate requeue-to-original-position; Streams
  can only append. Requeues go to the **stream tail** (`XACK` + `XADD` carrying
  `delivery-count`), so order guarantees are best-effort for requeued messages — an explicit
  documented caveat.
- Redelivery of crashed consumers: any broker node may reclaim/claim; PEL ownership is
  Redis-side, so redelivery works after a consumer or whole broker node crashes. Per-connection
  channel maps delivery-tags ⇄ stream entry IDs (in-memory), reconciled with `XACK` over the
  wire.
- Publisher confirm on fan-out (my call): the unit of confirmation is the whole fan-out
  pipeline — confirm when **every** bound queue's `XADD` in that publish was accepted; any copy
  failure (queue deleted mid-publish, failover, timeout) ⇒ `basic.nack` for that confirm tag.
  Unroutable mandatory ⇒ `basic.return` and no confirm; unroutable non-mandatory behavior is
  verified against RabbitMQ's rules in the conformance suite (M1 builder task).

### 5.2 Wire layer

- Connection FSM: protocol header 65–77 ("AMQP\0\0\9\1"), Start/StartOk (server/prod info,
  mechanisms), Secure handled iff PLAIN challenge needed (normally NO), Tune/TuneOk, Open/
  OpenOk (vhost), then channel multiplexing. Heartbeat frames + watchdog (timeout = 2×
  negotiated interval); `XREADGROUP BLOCK` never exceeds heartbeat margin so the loop
  interleaves heartbeats.
- Channels: per-channel delivery-tag allocator, unacked map, prefetch credits (count + bytes),
  publisher-confirm sequence; QoS enforced via per-consumer credit queue in the delivery loop.
- Exchange types: direct (exact map), fanout (set), topic (word-trie over `*`/`#`), headers
  (`x-match: all|any` binding args). Declarations follow AMQP rules: redeclare with same
  properties is idempotent, mismatched redeclare = `PRECONDITION_FAILED` channel error.
- vhost = namespace prefix in all Redis keys (plus separate topology sets). Default vhost `/`.
- SASL: `PLAIN` in v1 (stored salted password hashes in topology config; scheme finalized at
  plan stage), TLS 1.2+ via `SslStream` (optional mutual-TLS). ANONYMOUS only if explicitly
  enabled per profile.
- **Authorization (user-confirmed v1 scope):** RabbitMQ-style per-user, per-vhost permissions —
  `configure`/`read`/`write` regex patterns matched against resource names — enforced at
  declare/bind/delete (configure), consume (read), and publish (write). User/vhost/permission
  data lives in the topology store; caches invalidate on topology version change. Users are
  managed via configuration in v1 (no wire-level admin API, no management UI).
- Graceful stop (my call): on shutdown, stop accepting connections, finish in-flight confirm
  responses (short deadline, suggest 5 s), then close connections cleanly with
  `connection.close`; unacked messages remain in PELs for recovery — same redelivery contract
  as a crash. Standalone mode maps SIGTERM to this; hosting mode maps to `StopAsync`.

### 5.3 Topology & expiry (in Redis)

- Topology namespace (hash-tagged), keys: exchanges, bindings (with matcher descriptors),
  queues (arguments incl. DLX, TTLs, max-length, delivery-limit, durable/auto-delete flags).
- Durable declarations persist; on broker restart in **all modes**: connection-scoped state
  (exclusive queues, auto-delete when last consumer disconnects, non-durable queues) is
  reconciled — non-durable queue streams are deleted, auto-delete queues without consumers are
  removed, exclusive queues rebuilt if their connection is gone.
- **TTL/queue reaper:** background hosted sub-service (single leader per vhost via a short Redis
  lease in the cluster-safe namespace — reuse the same tag slotting): claims due entries in
  sorted set `{expired index}`, processes expiration, `x-expires`, and idle consumer group
  cleanup; works both in-process and standalone schedules.

### 5.4 Multi-node active-active (v1 requirement — user-confirmed)

Two or more broker processes sharing one Redis is a supported topology in v1, not an
afterthought:

- **Data plane is concurrent by construction:** competing consumers across nodes are just
  consumer-group members; publish fan-out from any node is plain `XADD`s; redelivery of
  crashed consumers happens via `XCLAIM/XAUTOCLAIM` from any node. No cross-node chatter.
- **Control plane races get optimistic locking (Lua-free):** topology
  declare/delete/binding mutations run as `WATCH`-based transactions on hash-tagged keys
  (single slot — valid on clusters and managed Redis alike) with retry-on-conflict.
- **Single-writer services** — reaper, reconciliation (auto-delete/exclusive cleanup) — run
  under short leases in the cluster-safe namespace, so at most one actor sweeps per vhost and
  tasks rezoned on lease loss.
- **Topology propagation:** nodes watch a topology version counter (debounced poll; pub/sub
  notify as fast path) and refresh route caches. A node serving a publish from a stale cache
  may hit a queue that was just deleted — covered by the confirm-nack semantics.
- **Exclusive/auto-delete queues:** remain connection-owned (RabbitMQ semantics) at the
  owning node; lease-loss means the owning node died and the reaper deletes the exclusive
  queue after its idle grace.
- No split-brain risk in the broker itself: Redis is the single point of truth; Redis HA
  (replica failover) is the deployment's concern, and PEL state follows the failover.

### 5.5 Capability profiles (G5)

Probed at connect (`HELLO`/`INFO`/`COMMAND`): version, cluster mode, max memory setting,
persistence capability, blocking-command support.

| Profile | Triggers | Fallbacks |
|---|---|---|
| **Standard** | Redis/Valkey ≥7, no cluster or cluster OK, blocking OK | — |
| **Managed** | ElastiCache/Memorystore/Azure/Redis Cloud | value on `CONFIG` restrictions; assume AOF governed by service |
| **Serverless** | Upstash-class metrics | non-blocking poll loop, no Lua anywhere, `XAUTOCLAIM` ≥6.2 confirmed (else `XCLAIM`) |
| **Cluster** | `CLUSTER INFO` connected_state ok | hashtags enforced; multi-slot pipelines split per slot |

Durability contract table per profile is part of the docs; startup refuses (by default)
or warns on: `allkeys-*`/`volatile-*` eviction when queues durable, TTL-limited cluster
passive non-AOF in standard profile, missing AOF ack window vs publisher-confirm cadence.

## 6. Distributions (decided)

- **`RedisMQ.Broker`** — core engine: codec, connection FSM, router, stream adapter, profiles,
  reaper, confirmations. References StackExchange.Redis only.
- **`RedisMQ.Hosting`** — `IHostedService`, DI extensions (`AddRedisMq(...)`), health checks,
  OTel wiring, config binding (appsettings/env).
- **`RedisMQ.Tool`** (standalone) — console host over core: CLI flags or config file; ships as
  dotnet tool, self-contained binary, and Linux/HCI container (`mcr.microsoft.com/dotnet/runtime`
  base, OCI Registry port-style tags) plus docker-compose sample against a local Redis.
- Client conformance tests and internal harness never ship in these packages.

## 7. Testing strategy

- **Unit:** codec round-trip (field tables, property tables, all frame types byte-exact),
  topic/header matchers, topology declarations, credit/QoS arithmetic, confirm sequencing.
- **Integration (Testcontainers):** stock Redis image, Redis 8, Valkey, 3-master cluster-mode
  compose, AOF on/off combos. Redis Enterprise Cloud and Upstash covered by a manual probe
  checklist (no free vendor images in CI).
- **Conformance suite (the gate for M-features):** fresh client connections in CI:
  1. `RabbitMQ.Client` (.NET, real tutorials-style scenarios + edge cases: redeclare mismatch,
     requeue/redelivery, confirm ordering, nack with requeue, basic.get,
     exclusive/auto-delete/idle timeouts, heartbeats under load, TLS).
  2. `Pika` (Python) — same script set.
  3. Phase 2: amqp091-go, node amqplib (add when stable), Java rabbitmq-client.
- **Perf smoke:** throughput/latency bench vs the same Redis via direct stream use, and
  optionally RabbitMQ (permissive comparison, not a GA target).

## 8. Milestones (mapping to plan doc later, not yet implementation)

- **M0 — protocol shell (spike allowed):** socket+pipelines server, handshake, Start/Tune/Open,
  heartbeat, method dispatch skeleton; direct exchange in-memory only (no Redis), one queue,
  publish/consume with real clients. *Goal: prove the wire layer to real clients.*
- **M1 — stream-backed queues:** queue stream + consumer group per queue, publish fan-out,
  ack/requeue/reject, prefetch credit, redelivery via idle-claim, `basic.get`, DLX by
  delivery-limit.
- **M2 — durability & topology:** durable declarations, vhosts, confirms, TTL/queue-expiry/tool
  reaper, eviction-policy startup gate, exclusion/auto-delete reconciliation.
- **M3 — full exchanges + extras:** topic trie + headers exchange, `x-max-length`,
  content-type passthrough, per-channel bookkeeping hardening, basic flow-control per channel.
- **M4 — hardening:** profiles (cluster/serverless/managed), OTel metrics/health, perf pass,
  conformance suite green on RabbitMQ.Client+Pika, packaging/pipeline (NuGet+tool+container,
  docs).

## 9. What is left to the builder (explicitly not pre-decided)

- Exact class/namespace naming, internal API shapes, buffer pooling details, cert loader.
- Rebalance sizes: default heartbeat (suggest 60s), default delivery-limit (suggest 3),
  claim idle threshold (suggest heartbeat × 2).
- Dockerfile/image details, CI runner matrix layout.
- Internal test mock of Redis if needed (StackExchange.Redis has mocking helpers).
- Doc aesthetics, README structure.

## 10. Open items / pre-release checks

- **Trademark/name check** on any public package ID: "RedisMQ" is *not* confirmed OK for
  public pub; alternatives list must exist by M4 (decide before NuGet publishing).
- Password storage format for SASL users (final scheme: salted PBKDF2 vs SHA-256 — builder to
  propose at plan stage).
- Confirm `XAUTOCLAIM` presence in Valkey releases used (≥7.2.4 fork includes it; verify in CI
  on the Valkey image).
- No probe accounts available before M4 (user-confirmed): **serverless and managed cloud
  profiles ship labeled *experimental***; completing the manual probe checklist (Redis
  Enterprise Cloud, Upstash-class) is the gate for removing the label. CI covers Redis, Redis
  8, Valkey, and 3-master cluster-mode containers.

## 11. Build & ship infrastructure (user-confirmed)

- Repository initialized as git in this folder when implementation is approved; worktree/branch
  per feature.
- CI: GitHub Actions; .NET 10 SDK required on build agents (builder verifies runner
  availability at plan stage).
- Publishing: dotnet tool + NuGet packages to nuget.org **only after** the trademark/name
  decision; internal feed mapping decided at plan stage.
- Designer note: the multi-node, permissions, and requeue-ordering choices raise M1/M2 unit
  and conformance test volume — the plan must budget for it.
