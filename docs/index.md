# RedisMQ Documentation

| Document | Description |
|----------|-------------|
| [`README.md`](../README.md) | Project overview, quick start, durability contract, architecture summary |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Internal architecture, layers, lifecycle, confirms, durability mirroring, DLX/TTL, permissions, TLS |
| [`PROTOCOL.md`](PROTOCOL.md) | AMQP 0-9-1 wire dialect, method set, queue arguments, confirms, error codes, deviations |
| [`CONFORMANCE.md`](CONFORMANCE.md) | C# and multi-client (Pika, amqplib, Java, Go) conformance guide and complementary coverage |
| [`API.md`](API.md) | API reference for `RedisMQ.Broker` and `RedisMQ.Redis` |
| [`DEVELOPMENT.md`](DEVELOPMENT.md) | Build/test workflow, Redis reset, SDD task flow, conventions, remaining work |

## Repository layout

```
RedisMQ.sln
src/RedisMQ.Broker/          # core engine (zero external deps, net10.0)
src/RedisMQ.Redis/           # Redis Streams write-ahead engine (StackExchange.Redis)
tests/RedisMQ.Broker.Tests/  # codec/FSM/engine/server/permission/TLS tests (319)
tests/RedisMQ.Conformance/   # RabbitMQ.Client 7.x conformance over sockets (19)
tests/RedisMQ.Redis.Tests/   # Redis mirror/replay/dedupe tests (60)
.github/workflows/ci.yml     # ubuntu-latest, dotnet 10.0.x
```

## Test status (committed state)

| Suite | Total | Failed | Warnings | Runner |
|-------|-------|--------|----------|--------|
| `RedisMQ.Broker.Tests` | 319 | 0 | 0 | direct exec |
| `RedisMQ.Conformance` | 19 | 0 | 0 | direct exec |
| `RedisMQ.Redis.Tests` | 60 | 0 | 0 | direct exec (Redis required) |
| `dotnet build RedisMQ.sln` | — | — | 0 | — |

> `dotnet test` (SDK MTP) is unusable on this dev machine (exit-5 bug). Use the direct
> runner; CI uses `dotnet test` on ubuntu-latest.

## Milestone status

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 | AMQP wire shell, in-memory engine, connection/channel FSMs, publish/consume/get/recover | Complete |
| M1 | Redis Streams write-ahead engine, startup replay | Complete |
| M2 | Durable topology, publisher confirms, ordered pipeline | Complete |
| M3 | Direct/fanout/topic/headers exchanges, flow pause, `x-max-length`, connection reconcile | Complete |
| M4 | Durable identity/receipts, ack/requeue mirroring, DLX/TTL/queue expiry, permissions + PBKDF2, TLS | **Part 1 complete (v1 functional surface)** |

### What M4 part 1 delivered

- `EnqueueReceipt` per publish target: confirms only ack after every fan-out `XADD`
  is accepted.
- One ordered confirm loop per connection with bounded backpressure.
- `EngineDelta` mirroring for ack/reject/requeue/DLX/expiry, enqueue-first remove-second,
  crash-window dedupe on replay.
- `x-expires`, `x-max-length`, `x-delivery-limit`, `x-dead-letter-exchange`,
  `x-dead-letter-routing-key`, `x-death` headers.
- Relative `expiration` converted to absolute deadlines; shared reaper.
- Default-deny configure/write/read permissions, PBKDF2-SHA256 accounts,
  `BrokerUser.FullAccess` for tests/samples.
- TLS listener (PFX/PEM) with optional mutual TLS, multi-listener server, listener
  factory.

## What remains for production v1

- Connection/channel profiles and tuning beyond RabbitMQ defaults.
- Performance: pipelining, mirror batching, benchmarks.
- Operational hardening: certificate hot-reload, Redis failure recovery policy,
  runtime user/vhost/permission management.

## SDD artifacts

The Superpowers SDD process artifacts live under `.superpowers/sdd/` (gitignored;
written for reviewer packs). The current milestone is
`.superpowers/sdd/2026-11-02-redismq-m4-v1-completeness/`:

- `progress.md` — rulings + task log (source of truth for decisions)
- `task-{N}-brief.md` — per-task requirements
- `task-{N}-report.md` — per-task completion report
- `task-{N}-tests.log` — test execution logs

Design and plan documents are committed under `docs/superpowers/specs/` and
`docs/superpowers/plans/`:

- `2026-11-02-redismq-m4-v1-completeness-design.md`
- `2026-11-02-redismq-m4-v1-completeness.md`
- plus the M0–M3 design/plan pairs.
