# RedisMQ Documentation

| Document | Description |
|----------|-------------|
| [`README.md`](../README.md) | Project overview, quick start, architecture summary |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Internal architecture, layers, connection/channel lifecycle, synchronization rules |
| [`PROTOCOL.md`](PROTOCOL.md) | AMQP 0-9-1 wire dialect, method set, error codes, deviations from RabbitMQ |
| [`CONFORMANCE.md`](CONFORMANCE.md) | RabbitMQ.Client 7.x conformance suite guide |
| [`API.md`](API.md) | API reference for `RedisMQ.Broker` (server, memory, codec, protocol, transport) |
| [`DEVELOPMENT.md`](DEVELOPMENT.md) | Build/test workflow, SDD task flow, adding methods/properties, M1 roadmap |

## Repository layout

```
RedisMQ.sln
src/RedisMQ.Broker/          # core engine (zero external deps, net10.0)
tests/RedisMQ.Broker.Tests/  # codec/FSM unit tests (xunit v3, 233 tests)
tests/RedisMQ.Conformance/   # RabbitMQ.Client 7.x conformance (15 tests)
.github/workflows/ci.yml     # ubuntu-latest, dotnet 10.0.x
```

## Test status (committed state)

| Suite | Total | Failed | Warnings | Runner |
|-------|-------|--------|----------|--------|
| `RedisMQ.Broker.Tests` | 233 | 0 | 0 | direct exec |
| `RedisMQ.Conformance` | 15 | 0 | 0 | direct exec |
| `dotnet build RedisMQ.sln` | — | — | 0 | — |

> `dotnet test` (SDK MTP) is unusable on this dev machine (exit-5 bug). Use the direct runner;
> CI uses `dotnet test` on ubuntu-latest.

## SDD artifacts

The Superpowers SDD process artifacts live in `.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/`
(gitignored; written for reviewer packs):

- `progress.md` — rulings + task log (source of truth for decisions)
- `task-{N}-brief.md` — per-task requirements
- `task-{N}-report.md` — per-task completion report
- `task-{N}-tests.log` — test execution logs
