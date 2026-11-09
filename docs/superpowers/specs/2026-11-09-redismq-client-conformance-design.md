# RedisMQ multi-client conformance — design

Date: 2026-11-09 · Status: awaiting approval

## Intent and why

RedisMQ claims mainstream AMQP 0-9-1 client libraries can connect and use it as they
would RabbitMQ. Today only `RabbitMQ.Client` 7.x is proven, by 19 in-process conformance
tests. Production v1 needs evidence from independent client implementations: **Pika
(Python), amqplib (Node), the RabbitMQ Java client, and amqp091-go**, with the existing
C# suite retained as the semantic reference.

This slice builds that evidence and fixes the broker where a real client exposes a
dialect gap. It is a black-box interoperability gate, not a performance or durability
slice.

## Goals

- G1 — Four independent client suites run the same named core scenarios over real TCP
  against a freshly started broker.
- G2 — One shared orchestrator owns broker lifecycle, port selection, credentials and
  scenario-parity checking; language suites contain only client code.
- G3 — Every suite reports `PASS <scenario-id>` for every required scenario; the runner
  fails when a scenario is missing, not just when a process exits non-zero.
- G4 — CI runs a per-client matrix, and the same suites run locally from one PowerShell
  command.
- G5 — Any broker fix required by a non-.NET client is pinned by a C# regression test so
  the gap cannot silently return.
- G6 — The existing C# suite remains green and remains the reference for TLS, Redis
  engine behavior and protocol edge cases.

## Non-goals

- TLS conformance for the four non-.NET clients. TLS is already pinned end-to-end by the
  C# `TlsFlowTests`; per-language trust-store setup is deployment configuration, not a
  different wire dialect. It can be a later slice.
- Redis-engine runs for the non-.NET suites. The matrix uses `InMemoryEngine`; engine
  durability semantics stay covered by `RedisMQ.Redis.Tests` and the C# suite.
- Load, throughput or latency measurement. That is the performance slice.
- Transactions, `basic.recover`, management APIs, clustering or any feature RedisMQ does
  not implement. Suites assert only the supported core.
- Publishing packages or changing the wire protocol beyond fixes a client proves
  necessary.

## Constraints

- Broker behaviour and defaults stay as shipped by the packaging slice: PLAIN auth,
  default-deny permissions, vhost `/`, guest/guest supplied by the harness.
- The core `RedisMQ.Broker` keeps zero `PackageReference`s.
- Each suite must use its client's public API with ordinary defaults, not hand-rolled
  frames. That is what makes the evidence meaningful.
- Local Windows is the development environment; CI is `ubuntu-latest`. Toolchains that
  are absent locally (Maven, Go) are installed locally during the slice and provisioned by
  the standard setup actions in CI.
- Tests must not depend on wall-clock sleeps for correctness; wait with deadlines.

## Approaches considered

1. **Per-language self-contained suites.** Each suite starts and stops its own broker.
   Simple to run in isolation, but four copies of port selection, startup waiting,
   credentials, log capture and cleanup — four places to drift.
2. **Shared broker runner with thin language suites. Chosen.** One PowerShell
   orchestrator starts the published sample host on a free port, exports connection
   settings, invokes the selected suite, verifies the scenario manifest, captures the
   broker log and stops the process. Each suite is a small idiomatic client program.
3. **.NET xUnit orchestrator.** xUnit would spawn Python/Node/Java/Go processes. Reuses
   the in-process fixture and port-0 binding, but couples `dotnet test` to four external
   toolchains, slows every unit-test run and makes cross-language failures hard to
   diagnose.

Approach 2 was selected. PowerShell 7 (`pwsh`) is preinstalled on GitHub runners, so the
same orchestrator runs locally and in CI; no second bash implementation to keep in sync.

## Architecture

```
tests/clients/
  README.md                 # prerequisites, versions, commands
  scenarios.json            # required scenario ids + C# test mapping
  run.ps1                   # orchestrator: build/publish, start broker, run suites, check manifest
  pika/
    requirements.txt        # pinned pika
    conformance.py
  node/
    package.json
    package-lock.json       # pinned amqplib
    conformance.js
  java/
    pom.xml                 # pinned amqp-client, shade plugin
    src/main/java/dev/redismq/conformance/Conformance.java
  go/
    go.mod
    go.sum                  # pinned amqp091-go
    main.go
```

The in-process C# suite stays at `tests/RedisMQ.Conformance` and is not spawned by the
orchestrator.

### Orchestrator contract

`tests/clients/run.ps1`:

- accepts `-Client pika|node|java|go|all` (default `all`), `-Port` (default: free port),
  `-Configuration Release`, `-SkipPrepare` and `-KeepBroker` for diagnosis
- starts **one fresh broker per selected client**; `all` runs Pika, Node, Java then Go in
  sequence, each against its own process and port. This matches the CI matrix's
  one-client-per-job isolation and prevents one suite's unacked messages or leftover
  queues from affecting the next.
- prepares the selected clients unless `-SkipPrepare`: `pip install -r requirements.txt`,
  `npm ci`, `mvn -q -DskipTests package`, `go mod download`, so a clean checkout works
  with one command. CI may let the runner prepare; it only installs the toolchain.
- preflights the selected clients' executables and fails with a list of what is missing.
  `-Client all` does **not** silently skip an absent toolchain (a partial run must not
  look green); `-SkipMissing` is the explicit opt-out.
- runs `dotnet publish samples/RedisMQ.Host -c Release -o <temp>/host`
- starts the published `RedisMQ.Host.dll` with content root in that directory and
  environment overrides:
  - `RedisMQ__Broker__Port=<port>`
  - `RedisMQ__Broker__Vhosts__0=/`
  - `RedisMQ__Broker__Users__0__Name=guest`, `...__Password=guest`, permissions `.*`
  - `RedisMQ__Redis__ConnectionString=` (memory-only)
  - `ASPNETCORE_URLS=http://127.0.0.1:0` (no fixed HTTP port collision)
- waits until a TCP connection to the AMQP port succeeds, with a bounded deadline, and
  fails with the broker log if the process exits first
- sets `REDISMQ_HOST`, `REDISMQ_PORT`, `REDISMQ_USER`, `REDISMQ_PASSWORD`,
  `REDISMQ_VHOST` for the child process
- invokes only the selected client command:
  - Pika: `python conformance.py`
  - Node: `node conformance.js`
  - Java: `java -jar target/redismq-client-conformance.jar`
  - Go: `go run .`
- tees client output, requires every id in `scenarios.json` to appear as `PASS <id>`,
  rejects any `FAIL <id>` line, and exits non-zero on the first suite that fails
- stops the broker and deletes the publish directory in `finally`; with `-KeepBroker`,
  prints the port, leaves that broker running, and requires exactly one `-Client`

The suites read the environment variables and otherwise take no broker-lifecycle
responsibility.

### Scenario contract

`tests/clients/scenarios.json` lists the required ids once:

| Id | What it proves |
|----|----------------|
| `connect` | PLAIN auth, vhost open, channel open, clean close |
| `roundtrip` | Declare direct exchange + queue + binding, publish, `basic.get`, byte-identical body, ack |
| `properties` | Content type, correlation id, message id, timestamp and string/bool/long headers survive publish → get |
| `qos` | Prefetch count admits exactly N unacked deliveries, then more after acks |
| `nack-requeue` | `basic.nack(requeue=true)` redelivers the same body with the redelivered flag |
| `ack` | Acked messages leave the queue; a later `basic.get` is empty |
| `basic-get-empty` | `basic.get` on an empty queue returns get-empty without closing the channel |
| `confirms` | Confirm mode receives one `basic.ack` for a successful publish |
| `round-robin` | Two consumers on one queue split the messages, collectively receiving each exactly once |

All nine ids are required for every suite. The file also records the strongest C# test
that covers each id — in `RedisMQ.Conformance` where a real-client test exists, otherwise
in `RedisMQ.Broker.Tests` (for example `confirms` maps to `ConfirmPipelineTests`). The C#
conformance suite is not modified to print ids and stays at 19 tests.

Suite conventions:

- Resource names include the scenario id (`conformance.roundtrip.exchange`,
  `conformance.qos.queue`, ...). Every scenario deletes what it declared and closes its
  channel before reporting, so scenarios are independent even within one broker process.
- Property assertions are value-level round-trips using each client's idiomatic encoding,
  not wire-type identity: content type, correlation id, message id, timestamp (compared
  to the second) and at least one string, one boolean and one numeric header. Exact wire
  types stay pinned by the C# suite; a client that encodes `42` as int8 must still get
  `42` back.
- Explicit manual acks except where a scenario is about no-ack behavior.
- Bounded waits for deliveries (for example 10 s) that report the scenario id on timeout.
- Print `PASS <id>` only after the assertions for that id pass; print `FAIL <id>: reason`
  and exit non-zero on the first failure.
- Human-readable progress goes to stderr so stdout remains a clean pass/fail stream.

### Broker fixes

A failing scenario is the RED step. Where the failure is a broker dialect bug, the fix
lands in `RedisMQ.Broker`/`RedisMQ.Broker.Server` with a C# regression test that
reproduces the protocol detail. The external suite then reruns to prove interoperability.
Where the failure is a client/API expectation outside RedisMQ's contract, the scenario is
adjusted and the reason recorded in `tests/clients/README.md`; the scenario contract is
never silently weakened.

## CI

Add a `conformance` job beside the existing `test`, `pack` and `docker` jobs:

- `strategy.fail-fast: false`, matrix `client: [pika, node, java, go]`
- steps: checkout; setup .NET 10; conditional setup of Python 3.12 / Node 22 / Java 21
  (with Maven cache) / Go 1.23
- one run step: `pwsh tests/clients/run.ps1 -Client ${{ matrix.client }}` — the runner
  restores that client's dependencies and builds its artifacts
- on failure, upload the broker log and client output; job timeout 10 minutes

The existing `test` job still runs the C# suite. The matrix is memory-only and needs no
Redis service.

## Local workflow

`tests/clients/README.md` records the required toolchains and pinned versions. One command
runs everything available:

```powershell
pwsh tests/clients/run.ps1 -Client all
```

or one suite while iterating:

```powershell
pwsh tests/clients/run.ps1 -Client pika
```

The runner reports missing executables clearly and fails rather than skipping them by
default; `-SkipMissing` is the explicit opt-out. Maven and Go are installed locally as
part of this slice so `all` is expected to pass on this machine; CI remains authoritative
for a clean environment.

## Documentation

- `docs/CONFORMANCE.md`: new multi-client section, scenario table, C# mapping, how to run
  locally and in CI, and the TLS/Redis non-goals.
- `tests/clients/README.md`: toolchains, pinned versions, commands, troubleshooting.
- `docs/DEVELOPMENT.md`: add the client suites to the commands and describe the extra
  prerequisites.
- `README.md`: one short line in the conformance section pointing at the multi-client
  gate; no structural rewrite.
- `docs/index.md`: remove client compatibility from the remaining list when the slice is
  complete.

## Acceptance

- `pwsh tests/clients/run.ps1 -Client all` reports all nine ids for all four clients and
  exits 0 on this machine.
- CI `conformance` matrix is green for all four clients.
- C# suite remains 19/0; Broker 359/0, Redis 67/0, Hosting 11/0 unless new C# regression
  tests from G5 raise Broker counts (the plan will state the new expected total).
- Build remains 0 errors with no new analyzer warnings beyond the 86 baseline.
- The core has no package references; no new wire surface beyond fixes justified by a
  failing client.
- Documentation links resolve and the remaining-work lists no longer claim client
  compatibility is missing.

## What is left to the builder

- Exact pinned versions after checking the registries at implementation time.
- Whether Java is built with Maven Shade or an exec plugin; the design requires a single
  runnable jar command.
- Internal helper naming and output formatting inside each suite.
- Whether Go and Maven are installed with `winget` or a direct download on this machine.
- The precise C# regression test names for any broker fix.
