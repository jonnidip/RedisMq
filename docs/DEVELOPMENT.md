# RedisMQ Development Guide

How to build, test, and extend the RedisMQ broker.

## Environment

- **OS:** Windows (WSL bash available but has no `dotnet` — use PowerShell/CMD).
- **SDK:** .NET 10 (`global.json` pins Microsoft.Testing.Platform runner).
- **IDE:** Visual Studio / Rider / VS Code with C# Dev Kit.

## Tooling quirk (read first)

On this development machine, `dotnet test` (SDK MTP) is **unusable** due to an SDK bug
(exit 5, "zero tests ran", dotnet/sdk#56104 shape). All test execution on this machine goes
through the **direct xunit v3 runner**:

```bash
# Unit tests
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll

# Conformance suite
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
```

CI (`.github/workflows/ci.yml`) uses the standard `dotnet test` on ubuntu-latest — that is the
intended green path.

Filtered runs (direct runner):

```bash
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConsumeTests"
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -method "Full.Qualified.Name"
```

## Build

```bash
dotnet build RedisMQ.sln          # all three projects, 0 warnings expected
dotnet test RedisMQ.sln           # CI path (ubuntu-latest)
```

## Test targets

| Suite | Project | Expected |
|-------|---------|----------|
| Codec/FSM unit tests | `RedisMQ.Broker.Tests` | 233 passed |
| Real-client conformance | `RedisMQ.Conformance` | 15 passed |

## Adding a feature (task flow)

The repo follows the Superpowers SDD (Specification-Driven Development) process:

1. **Read the plan** — `docs/superpowers/plans/2026-10-30-redismq-m0-protocol-shell.md`
2. **Read the brief** — `.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/task-N-brief.md`
3. **Read the ledger** — `.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/progress.md` (rulings + task log)
4. **Write tests first (RED)** — xunit v3, direct runner, watch them fail
5. **Implement (GREEN)** — server-side fixes only; no design changes to pass tests
6. **Ledger the result** — record RED failures, fixes, and any new `Ruling:` lines
7. **Commit** — conventional commit, green suite, 0 warnings

### Commit style

Conventional Commits:

```
feat(server): consume loop with prefetch credits, acks, nack, get, recover
test(conformance): rabbitmq.client end-to-end flow and error surface (m0 green)
chore(ci,docs): ci workflow and m0 readme
```

Types: `feat`, `fix`, `test`, `chore`, `refactor`, `docs`.

## Adding a protocol method

1. Add the schema to `MethodRegistry.cs` (class id, method id, name, direction, fields).
2. Verify against `tests/RedisMQ.Broker.Tests/fixtures/rabbitmq-codegen.json` (source of truth).
3. `MethodRegistryTests` re-derives the whole table from the fixture each run — hand-editing drift fails the build.
4. Add the dispatch case to `ChannelContext.OnRunning` (channel methods) or `WireConnection.HandleMethodFrameAsync` (connection methods).
5. Add tests to the appropriate test project.

## Adding a content property

1. Add to `ContentProperties` record (wire order matters).
2. Add flag mask to `ContentHeaderCodec`.
3. Add `ContentProperty` to `MethodRegistry._basicContentProperties`.
4. Update `ContentHeaderCodec.Write` and `Read` accordingly.

## Project structure conventions

- One public type per file (mostly).
- XML doc comments on all public API (`/// <summary>`).
- `ArgumentNullException.ThrowIfNull` for null arguments.
- `ConfigureAwait` not used (no SynchronizationContext).
- Cancellation tokens flow through all async APIs; `TestContext.Current.CancellationToken` in tests.
- Lock ladder: engine gate → `_deliveryLock` (pump claim path); handler paths release `_deliveryLock` before engine calls. Writer-gate → engine-gate is one-way.

## Where the SDD artifacts live

- Plan: `docs/superpowers/plans/2026-10-30-redismq-m0-protocol-shell.md`
- Spec: `docs/superpowers/specs/2026-10-30-redismq-amqp-broker-design.md`
- Ledger (rulings + task log): `.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/progress.md`
- Task briefs/reports: `.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/task-{N}-{brief,report}.md`
  (`.superpowers/` is gitignored — written for reviewer packs, not committed)

## Deferred minors (review queue)

These are explicitly parked for the final whole-branch review:

- ConsumeTests: nack-multiple wire test; direct noAck-ledger-skip assertion.
- Harness consolidation (per-file copies of ConsumeTests/Publish/Conformance harnesses).
- Transport claims untested: basic.return re-chunk split at `frameMax−overhead`; D3 codec-throw wire path.
- `ChannelOutcome` base-doc stale sentence; mourning `_pending*` defensive reset.

## M1 roadmap (not yet started)

- `RedisStreamRouteEngine : IRouteEngine` backed by Redis Streams (`XADD`/`XREADGROUP`/`XACK`/`XCLAIM`).
- Durable topology store in Redis.
- Publisher confirms, DLX, TTL reaper.
- No changes to codec or wire code expected — `IRouteEngine` is the seam.
