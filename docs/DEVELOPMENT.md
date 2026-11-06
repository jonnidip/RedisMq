# RedisMQ Development Guide

How to build, test, and extend the RedisMQ broker.

## Environment

- **OS:** Windows (WSL bash available but has no `dotnet` — use PowerShell/CMD).
- **SDK:** .NET 10 (`global.json` pins Microsoft.Testing.Platform runner).
- **Redis:** a local Redis container on `localhost:6379` for `RedisMQ.Redis.Tests`
  (`maxmemory-policy noeviction`; the engine's startup gate rejects eviction
  policies).
- **IDE:** Visual Studio / Rider / VS Code with C# Dev Kit.

## Tooling quirk (read first)

On this development machine, `dotnet test` (SDK MTP) is **unusable** due to an SDK bug
(exit 5, "zero tests ran", dotnet/sdk#56104 shape). All test execution on this machine
goes through the **direct xunit v3 runner**:

```bash
# Unit tests (broker)
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll

# Conformance suite (real RabbitMQ.Client over sockets)
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll

# Redis mirror/replay suite
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll

# Hosting/configuration suite
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll
```

CI (`.github/workflows/ci.yml`) uses the standard `dotnet test` on ubuntu-latest — that
is the intended green path.

Filtered runs (direct runner):

```bash
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConfirmPipelineTests"
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -method "Full.Qualified.Name"
```

## Build

```bash
dotnet build RedisMQ.sln          # all projects, 0 errors
dotnet test RedisMQ.sln           # CI path (ubuntu-latest)
```

The analyzer baseline is **86 warnings** (mostly the pre-existing xUnit1051 set
inherited with the test project). A change must not add a new warning; compare
`dotnet build RedisMQ.sln --no-incremental` warning counts rather than expecting zero.
(`--no-incremental` also prints `MSB3061` file-lock warnings for the test executables;
those are not analyzer warnings and do not count.)

## Test targets

| Suite | Project | Expected |
|-------|---------|----------|
| Codec/FSM/engine/server unit tests | `RedisMQ.Broker.Tests` | 359 passed |
| Real-client conformance | `RedisMQ.Conformance` | 19 passed |
| Redis mirror/replay | `RedisMQ.Redis.Tests` | 67 passed (or skipped without Redis) |
| Hosting/configuration | `RedisMQ.Hosting.Tests` | 11 passed |

Run the broker and conformance suites twice after touching TLS or confirm timing to
catch handshake and backpressure flakes.

## Redis for tests

The Redis suite connects to IPv4 `localhost:6379` and uses a unique vhost per test.
When Redis is unreachable, Redis-dependent tests **return early (skip)** with a comment
recording the reason; the broker and conformance suites never require it.

### Resetting the store

There is no migration path between milestone formats; start from a fresh store. Every
RedisMQ key is under the `{mq:` prefix:

```bash
redis-cli --scan --pattern '{mq:*}' | xargs -r redis-cli DEL
```

Key layout:

| Pattern | Contents |
|---------|----------|
| `{mq:top:<vhost>}` | Topology write-ahead log (declare/bind/unbind/delete/consumer) |
| `{mq:q:<vhost>:<queue>}` | One ready-message stream per queue |

If a Redis test leaves state behind, clear the store and re-run — replay is expected to
rebuild topology and ready queues from the streams alone.

## Adding a feature (task flow)

The repo follows the Superpowers SDD (Specification-Driven Development) process. The
current milestone is M4 (v1 completeness):

1. **Read the design** —
   `docs/superpowers/specs/2026-11-02-redismq-m4-v1-completeness-design.md`
2. **Read the plan** —
   `docs/superpowers/plans/2026-11-02-redismq-m4-v1-completeness.md`
3. **Read the brief** —
   `.superpowers/sdd/2026-11-02-redismq-m4-v1-completeness/task-N-brief.md`
4. **Read the ledger** —
   `.superpowers/sdd/2026-11-02-redismq-m4-v1-completeness/progress.md`
   (rulings + task log)
5. **Write tests first (RED)** — xunit v3, direct runner, watch them fail
6. **Implement (GREEN)** — server-side fixes only; no design changes to pass tests
7. **Ledger the result** — record RED failures, fixes, and any new `Ruling:` lines
8. **Commit** — conventional commit, green suite, 0 warnings

Earlier milestones follow the same shape under
`.superpowers/sdd/2026-10-30-redismq-m0-protocol-shell/`,
`.superpowers/sdd/2026-11-01-redismq-m1-*`, `2026-11-02-redismq-m2-*` and
`2026-11-02-redismq-m3-*`.

### Commit style

Conventional Commits:

```
feat(server): ordered async publisher confirms with receipt backpressure
feat(engine): dead-letter routing with x-death and delivery-limit overflow
test(m4): TLS conformance and v1 completeness regression gate
chore(ci,docs): ci workflow and m0 readme
```

Types: `feat`, `fix`, `test`, `chore`, `refactor`, `docs`.

## Adding a protocol method

1. Add the schema to `MethodRegistry.cs` (class id, method id, name, direction,
   fields).
2. Verify against `tests/RedisMQ.Broker.Tests/fixtures/rabbitmq-codegen.json` (source
   of truth).
3. `MethodRegistryTests` re-derives the whole table from the fixture each run —
   hand-editing drift fails the build.
4. Add the dispatch case to `ChannelContext.OnRunning` (channel methods) or
   `WireConnection.HandleMethodFrameAsync` (connection methods).
5. If it mutates state, decide whether it needs a permission check and an
   `EngineDelta` for the durable mirror.
6. Add tests to the appropriate test project.

## Adding a content property

1. Add to `ContentProperties` record (wire order matters).
2. Add the flag mask to `ContentHeaderCodec`.
3. Add `ContentProperty` to `MethodRegistry._basicContentProperties`.
4. Update `ContentHeaderCodec.Write` and `Read` accordingly.
5. If the property affects durability (like `expiration`), update
   `StreamCodec`/`WriteCommand` so the Redis mirror preserves it.

## Adding a queue argument

1. Parse and validate it in `ChannelContext.TryReadQueueArguments` (unknown keys keep
   the `not_implemented` verdict; invalid values are a soft `precondition-failed`).
2. Add it to `QueueInfo` and to the equivalence ladder in
   `DeclaredQueueOutcome`.
3. Implement its engine behavior in `InMemoryEngine` (enqueue, reject, requeue or
   reap), expressing any storage change as an `EngineDelta`.
4. Persist it in `WriteCommand.DeclareQueue` and restore it in
   `RedisStreamRouteEngine.ReplayTopologyEntries`.
5. Add broker tests and, where observable through a real client, a conformance test.

## Project structure conventions

- One public type per file (mostly).
- XML doc comments on all public API (`/// <summary>`).
- `ArgumentNullException.ThrowIfNull` for null arguments.
- `ConfigureAwait` not used (no SynchronizationContext).
- Cancellation tokens flow through all async APIs; `TestContext.Current.CancellationToken`
  in tests.
- Lock ladder: engine gate → `_deliveryLock` (pump claim path); handler paths release
  `_deliveryLock` before engine calls. Writer-gate → engine-gate is one-way.
- **Permissions default-deny.** Any new wire operation that touches a named resource
  must check the appropriate `BrokerPrincipal` capability before lookup or mutation.
  Tests and samples use `BrokerUser.FullAccess` explicitly.
- **Durability is expressed through the engine.** New mutations return or extend
  `EngineDelta`; the Redis engine mirrors deltas, it never reaches into server code.
- **The confirm loop is the only place a confirm is written.** Publish paths hand it a
  `PendingConfirm`; they never answer confirms inline.

## Instrumentation conventions

Diagnostics are part of the public contract (`RedisMQ.Broker.Diagnostics`), so changes
there follow the same review bar as protocol changes.

- **Share one `BrokerDiagnostics` per process.** Pass the same instance to the engine
  and the server; a private per-component instance gives every measurement a different
  `broker.instance` and splits your series. The Redis engine forwards it to its inner
  `InMemoryEngine` — never create a second one inside the wrapper.
- **Guard log work before building it.** Test `diagnostics.Log.IsEnabled(level)` at the
  call site so a disabled host pays nothing, and prefer `BrokerLogEvent` plus small
  structured `Data` fields over formatted strings.
- **Event ids are the contract; message text is not.** Never renumber or repurpose a
  `BrokerLogEvent` value, and never make a host depend on the wording of
  `BrokerLogEntry.Message`.
- **No PII, secrets or message contents.** No routing keys, message ids, bodies,
  connection strings, passwords or certificate subjects in metrics, logs or health.
  Health exposes a sanitized single-line error only.
- **Metrics are cardinality-bounded.** Tag values must come from a fixed set (outcomes,
  reasons, kinds, vhost/queue names). Never tag with an unbounded value such as a
  message id or an exception message.
- **Every measurement carries `broker.instance`.** Use the `BrokerDiagnostics`
  helpers (`Count`, `Measure`, `RegisterObservableGauge`) rather than calling
  `Counter.Add` directly, so the tag is never forgotten.
- **Gauges are registered once per name and aggregated.** Symptom of a bug: one
  callback per server registration. The shared registry sums identical tag sets; a
  component that registers a gauge disposes the handle when it stops.
- **Sinks never throw into the protocol path.** `BrokerDiagnostics` already contains a
  throwing sink; do not add a second `try`/`catch` around every `WriteLog` call, and do
  not let instrumentation failures change an AMQP reply.
- **Instrumentation must not add a wire surface.** No new frames, methods or fields:
  observability observes the existing protocol, it does not extend it.

## Packaging

NuGet packages, `IConfiguration` binding, the sample host and the container image are
documented in [PACKAGING.md](PACKAGING.md).

## Remaining work toward production v1

- Connection/channel profiles and tuning beyond RabbitMQ defaults.
- Throughput work: pipelining, mirror batching, benchmarks.
- Client compatibility beyond RabbitMQ.Client 7.x (Pika and others).
- Operational hardening: certificate hot-reload, Redis failure recovery policy,
  runtime user/vhost/permission management.
