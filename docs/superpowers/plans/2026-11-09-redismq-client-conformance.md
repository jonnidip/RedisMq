# Multi-Client Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove RedisMQ's AMQP 0-9-1 dialect against Pika, amqplib, the RabbitMQ Java client and amqp091-go, and fix any broker gap they expose.

**Architecture:** One PowerShell orchestrator publishes the sample host, starts a fresh memory-only broker per client on a free port, runs a thin client-only suite, and verifies that every required scenario id was reported as `PASS`. The existing in-process C# suite remains the semantic reference.

**Tech Stack:** .NET 10, PowerShell 7, Python 3 + Pika, Node 22 + amqplib, Java 21 + RabbitMQ Java client + Maven, Go + amqp091-go, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-09-redismq-client-conformance-design.md`

## Global Constraints

- `RedisMQ.Broker` keeps **zero** `PackageReference`s.
- Broker defaults stay as shipped: PLAIN auth, default-deny permissions, vhost `/`.
- The external matrix uses `InMemoryEngine` only (`RedisMQ__Redis__ConnectionString=`); no Redis service.
- No TLS in the external suites; C# `TlsFlowTests` remains the TLS gate.
- Nine required scenario ids: `connect`, `roundtrip`, `properties`, `qos`, `nack-requeue`, `ack`, `basic-get-empty`, `confirms`, `round-robin`.
- Each suite prints `PASS <id>` on success and `FAIL <id>: <reason>` on failure; progress goes to stderr.
- Wait for deliveries with bounded deadlines (10 s), never fixed correctness sleeps.
- Pin client versions: Pika `1.4.4`, amqplib `2.3.1`, `com.rabbitmq:amqp-client:5.25.0`, `github.com/rabbitmq/amqp091-go v1.16.0`.
- No fixed broker port; the orchestrator picks a free one.
- Existing build baseline: 0 errors, 86 analyzer warnings; do not add warnings.

## Review Focus

1. **Non-.NET property encodings** (amqplib may send int8 where C# sent int64; Java sends a `Date` timestamp). The broker must round-trip values without closing the channel — covered by `properties` in Tasks 1–4.
2. **Clean client close sequencing** differs between libraries. The broker must answer `connection.close-ok`, not leave a half-open socket — covered by `connect` in Tasks 1–4.
3. **QoS accounting across clients** (Pika callbacks, amqplib promises, Java `DefaultConsumer`, Go channels). Prefetch must admit exactly N unacked messages and resume after acks — covered by `qos`.
4. **`basic.get` on an empty queue** must return get-empty on the same channel, not close it — covered by `basic-get-empty`.
5. **Redelivery flag on `basic.nack(requeue=true)`** must survive each client's settlement path — covered by `nack-requeue`.

---

### Task 1: Shared runner, manifest, and the Pika suite

**Files:**
- Create: `tests/clients/scenarios.json`
- Create: `tests/clients/run.ps1`
- Create: `tests/clients/README.md`
- Create: `tests/clients/pika/requirements.txt`
- Create: `tests/clients/pika/conformance.py`
- Modify: `.gitignore`
- Test: `tests/clients/pika/conformance.py` (the suite is the test)

**Interfaces:**
- Consumes: `samples/RedisMQ.Host` (AMQP + HTTP host), configuration keys from the packaging slice.
- Produces: `scenarios.json` with a `required` array; `run.ps1` accepting `-Client`, `-Port`, `-Configuration`, `-SkipPrepare`, `-SkipMissing`, `-KeepBroker`; child environment `REDISMQ_HOST`, `REDISMQ_PORT`, `REDISMQ_USER`, `REDISMQ_PASSWORD`, `REDISMQ_VHOST`. Later tasks add sibling directories only.

- [ ] **Step 1: Write `tests/clients/scenarios.json`**

```json
{
  "required": [
    "connect",
    "roundtrip",
    "properties",
    "qos",
    "nack-requeue",
    "ack",
    "basic-get-empty",
    "confirms",
    "round-robin"
  ],
  "reference": {
    "connect": "tests/RedisMQ.Conformance/FlowTests.cs: Connect_authenticates_and_opens_a_channel",
    "roundtrip": "tests/RedisMQ.Conformance/FlowTests.cs: Declare_bind_publish_consume_round_trips_content_and_properties",
    "properties": "tests/RedisMQ.Conformance/FlowTests.cs: Declare_bind_publish_consume_round_trips_content_and_properties",
    "qos": "tests/RedisMQ.Conformance/FlowTests.cs: Qos_count_gate_admits_exactly_10_and_more_flow_after_acks",
    "nack-requeue": "tests/RedisMQ.Conformance/FlowTests.cs: Nack_requeue_redelivers_with_redelivered_flag",
    "ack": "tests/RedisMQ.Conformance/FlowTests.cs: Declare_bind_publish_consume_round_trips_content_and_properties",
    "basic-get-empty": "tests/RedisMQ.Conformance/FlowTests.cs: BasicGet_returns_the_head_and_reports_empty_after",
    "confirms": "tests/RedisMQ.Broker.Tests/ConfirmPipelineTests.cs: Ack_waits_for_the_receipt",
    "round-robin": "tests/RedisMQ.Conformance/FlowTests.cs: Two_consumers_share_the_queue_round_robin_with_no_message_delivered_twice"
  }
}
```

- [ ] **Step 2: Write `tests/clients/run.ps1`**

Parameter block:

```powershell
param(
    [ValidateSet('pika', 'node', 'java', 'go', 'all')]
    [string[]]$Client = @('all'),
    [int]$Port = 0,
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',
    [switch]$SkipPrepare,
    [switch]$SkipMissing,
    [switch]$KeepBroker
)
```

Behaviour, in order:

1. Expand `all` to `pika`, `node`, `java`, `go`. `-KeepBroker` with more than one client is an error.
2. Resolve the repo root from `$PSScriptRoot\..\..`.
3. Read `scenarios.json` and keep its `required` array.
4. Preflight each selected client:
   - `pika`: `python` (or `py`) and `tests/clients/pika/conformance.py`
   - `node`: `node`, `npm`, `tests/clients/node/conformance.js`
   - `java`: `java`, `mvn`, `tests/clients/java/pom.xml`
   - `go`: `go`, `tests/clients/go/go.mod`
   Collect missing items; fail with the full list unless `-SkipMissing`, which warns and drops those clients. A missing item is never silently ignored otherwise.
5. Prepare unless `-SkipPrepare`, from the client directory:
   - `python -m pip install -r requirements.txt`
   - `npm ci`
   - `mvn -q -DskipTests package`
   - `go mod download`
   A failed prepare stops the run with that client named.
6. Publish once: `dotnet publish samples/RedisMQ.Host/RedisMQ.Host.csproj -c $Configuration -o <temp>/host` (let it restore).
7. For each remaining client sequentially:
   - pick a free TCP port on `127.0.0.1` unless `-Port` was given
   - start `dotnet <temp>/host/RedisMQ.Host.dll` with working directory `<temp>/host` and environment:
     `RedisMQ__Broker__Port`, `RedisMQ__Broker__Vhosts__0=/`,
     `RedisMQ__Broker__Users__0__Name=guest`, `...__Password=guest`,
     `...__Permissions__0__Vhost=/`, `...__Configure=.*`, `...__Write=.*`, `...__Read=.*`,
     `RedisMQ__Redis__ConnectionString=`, `ASPNETCORE_URLS=http://127.0.0.1:0`
   - redirect stdout/stderr to `tests/clients/.logs/<client>.log` (create the directory)
   - wait up to 20 s for a TCP connection to the AMQP port; if the process exits, print the log tail and fail
   - set `REDISMQ_HOST=127.0.0.1`, `REDISMQ_PORT`, `REDISMQ_USER=guest`, `REDISMQ_PASSWORD=guest`, `REDISMQ_VHOST=/`
   - run the client command from its directory, tee output, and capture the exit code:
     `python conformance.py`, `node conformance.js`,
     `java -jar target/redismq-client-conformance.jar`, `go run .`
   - parse captured stdout: every required id must appear as a whole-line `PASS <id>`; any `FAIL <id>` is a failure; a non-zero exit is a failure
   - on failure print the broker log tail and stop the run with exit 1
   - always stop the broker process in `finally`
8. With `-KeepBroker`, print the port and connection settings, leave the broker running, and skip cleanup of the publish directory.
9. Exit 0 only when every selected client passed every required id.

- [ ] **Step 3: Write `tests/clients/README.md`**

Document: prerequisites and pinned versions, the one-command local run, per-client runs, the environment contract, `-SkipPrepare`/`-SkipMissing`/`-KeepBroker`, and that TLS/Redis-engine runs are out of scope.

- [ ] **Step 4: Write `tests/clients/pika/requirements.txt` and update `.gitignore`**

```text
pika==1.4.4
```

Append these entries to the repo `.gitignore` (later tasks rely on them):

```text
# Client conformance artifacts
tests/clients/.logs/
tests/clients/pika/__pycache__/
tests/clients/node/node_modules/
tests/clients/java/target/
```

- [ ] **Step 5: Write the Pika suite**

`tests/clients/pika/conformance.py`: a small runner with `scenario(id, fn)` that prints `PASS id` after `fn` returns and `FAIL id: <message>` then exits 1. Read the five environment variables with defaults (`127.0.0.1`, `5672`, `guest`, `guest`, `/`). Use a fresh `BlockingConnection` per scenario and close it in `finally`.

Implement each scenario with these assertions:

- `connect`: connect, `channel()`, assert `is_open`, close.
- `roundtrip`: declare direct exchange `conformance.roundtrip.exchange`, queue `conformance.roundtrip.queue`, bind `rk`; publish `b"roundtrip"`; poll `basic_get` up to 10 s; assert body, ack; delete queue/exchange.
- `properties`: publish with `BasicProperties(content_type="text/plain", correlation_id="corr-1", message_id="msg-1", timestamp=int(time.time()), headers={"s": "text", "b": True, "n": 42})`; assert every value on the returned properties (`timestamp` to the second); ack.
- `qos`: queue `conformance.qos.queue`, `basic_qos(prefetch_count=2)`, publish 4, consume with a callback; after 10 s or two deliveries, assert exactly 2 arrived and a third does not arrive within 500 ms; ack one and assert a third arrives; ack all.
- `nack-requeue`: publish one, `basic_get`, `basic_nack(delivery_tag, requeue=True)`, poll `basic_get` until `redelivered` is true; assert same body; ack.
- `ack`: publish two, get/ack both, assert a later `basic_get` returns `(None, None, None)`.
- `basic-get-empty`: declare an empty queue, assert `basic_get` returns `(None, None, None)`, then publish and get on the same channel to prove it stayed open.
- `confirms`: `channel.confirm_delivery()`, publish to a declared queue, assert the publish returns `True` and no exception; clean up.
- `round-robin`: queue `conformance.roundrobin.queue`, two channels with `basic_qos(prefetch_count=1)`, one consumer each, publish 10 bodies; collect deliveries with `process_data_events` until 10 or 10 s; assert every body exactly once and ack each.

- [ ] **Step 6: Run the Pika suite against a real broker**

Run:

```powershell
pwsh tests/clients/run.ps1 -Client pika
```

Expected: the runner publishes the host, starts it, installs Pika, and the suite reports all nine ids. If a scenario fails, continue to Step 7.

- [ ] **Step 7: Fix any broker gap, test-first**

For each failing scenario:

1. Add or adjust a C# test in `tests/RedisMQ.Broker.Tests` (or `tests/RedisMQ.Conformance` when a real-client test is the better pin) that reproduces the protocol detail, and watch it fail with the existing direct runner.
2. Fix the broker in `RedisMQ.Broker`/`RedisMQ.Broker.Server` only; do not weaken the scenario.
3. Re-run the new C# test and then `pwsh tests/clients/run.ps1 -Client pika`.

If all nine scenarios pass on the first run, skip this step and record that no broker change was needed.

- [ ] **Step 8: Run the existing C# suites**

```powershell
dotnet build RedisMQ.sln --no-incremental
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -noColor
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll -noColor
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
```

Expected: 0 errors, 86 warnings (or the stated new count if Step 7 added tests), Broker 359/0 plus any new tests, Conformance 19/0, Redis 67/0, Hosting 11/0.

- [ ] **Step 9: Commit**

```powershell
git add tests/clients .gitignore
git commit -m "test(conformance): add shared client runner and pika suite"
```

If Step 7 changed the broker, include those files and use `fix(server): ...` in a separate commit before this one.

---

### Task 2: Node amqplib suite

**Files:**
- Create: `tests/clients/node/package.json`
- Create: `tests/clients/node/package-lock.json` (generated)
- Create: `tests/clients/node/conformance.js`
- Modify: `.gitignore` (`tests/clients/node/node_modules/`)

**Interfaces:**
- Consumes: the environment contract and PASS/FAIL protocol from Task 1; `amqplib@2.3.1`.
- Produces: a Node suite the Task 1 runner can invoke unchanged.

- [ ] **Step 1: Write `package.json`**

```json
{
  "name": "redismq-node-conformance",
  "private": true,
  "version": "1.0.0",
  "description": "amqplib conformance suite for RedisMQ",
  "main": "conformance.js",
  "scripts": {
    "test": "node conformance.js"
  },
  "dependencies": {
    "amqplib": "2.3.1"
  }
}
```

- [ ] **Step 2: Write the suite**

`tests/clients/node/conformance.js`: read the same five environment variables; one async `scenario(id, fn)` that prints `PASS id` or `FAIL id: message` and exits 1; connect with `amqp.connect({ hostname, port, username, password, vhost })`; close connections and channels in `finally`.

Mirror the nine Task 1 scenarios with amqplib's public API. Required assertions:

- `connect`: connect and `createChannel`; close both.
- `roundtrip`: `assertExchange` direct, `assertQueue`, `bindQueue`, `publish`, `get` (not `false`), body equals `Buffer.from('roundtrip')`, `ack`; `deleteQueue`/`deleteExchange`.
- `properties`: publish with `contentType`, `correlationId`, `messageId`, `timestamp` in seconds and headers `{ s: 'text', b: true, n: 42 }`; assert the same values on the delivery (`fields.timestamp` compared as seconds).
- `qos`: `prefetch(2)`, publish 4, `consume`; assert exactly 2 deliveries arrive, a third does not arrive within 500 ms, ack one and assert a third arrives, then ack all.
- `nack-requeue`: get one, `nack(message, false, true)`, get again until `fields.redelivered === true`; assert body; ack.
- `ack`: publish two, get/ack both, assert a later `get` returns `false`.
- `basic-get-empty`: `get` on an empty queue returns `false`, then publish and get on the same channel.
- `confirms`: `createConfirmChannel`, publish with the callback wrapped in a Promise, assert the callback error is null; clean up.
- `round-robin`: two consumers with `prefetch(1)`, publish 10, collect until 10 or 10 s, assert each body exactly once, ack each.

Use a `withTimeout(promise, ms, id)` helper for bounded waits; do not use fixed waits for correctness.

- [ ] **Step 3: Generate the lockfile**

Run from `tests/clients/node`:

```powershell
npm install
```

Expected: `package-lock.json` exists and `node_modules/` is ignored.

- [ ] **Step 4: Run the Node suite**

```powershell
pwsh tests/clients/run.ps1 -Client node
```

Expected: Task 1's runner prepares with `npm ci`, starts a fresh broker, and all nine ids pass. If a scenario fails, follow Task 1 Step 7 (C# test first, broker fix, rerun).

- [ ] **Step 5: Run the existing C# suites and commit**

Run the four suites from Task 1 Step 8; expected counts unchanged unless Step 4 added tests.

```powershell
git add tests/clients/node .gitignore
git commit -m "test(conformance): add node amqplib suite"
```

---

### Task 3: Java RabbitMQ client suite

**Files:**
- Create: `tests/clients/java/pom.xml`
- Create: `tests/clients/java/src/main/java/dev/redismq/conformance/Conformance.java`
- Modify: `.gitignore` (`tests/clients/java/target/`)

**Interfaces:**
- Consumes: the environment contract and PASS/FAIL protocol from Task 1; `com.rabbitmq:amqp-client:5.25.0`.
- Produces: `target/redismq-client-conformance.jar`, which the Task 1 runner invokes with `java -jar`.

- [ ] **Step 1: Install Maven locally**

```powershell
winget install --id Apache.Maven --accept-source-agreements --accept-package-agreements
```

Expected: `mvn -version` works in a new shell. If the winget id differs, use `winget search maven` and record the exact id in `tests/clients/README.md`. CI already has Maven on `ubuntu-latest` and uses `setup-java` for the JDK.

- [ ] **Step 2: Write `pom.xml`**

- `groupId dev.redismq`, `artifactId redismq-client-conformance`, `version 1.0.0`
- `maven.compiler.release` 21
- dependency `com.rabbitmq:amqp-client:5.25.0`
- `maven-shade-plugin` with `Main-Class: dev.redismq.conformance.Conformance`
- `<finalName>redismq-client-conformance</finalName>` so the jar path is fixed

- [ ] **Step 3: Write `Conformance.java`**

Read the same five environment variables. One `scenario(String id, ThrowingRunnable)` helper prints `PASS id` or `FAIL id: message` and exits 1. Build a `ConnectionFactory` once per scenario; use `newConnection()` and a fresh channel; clean up in `finally`.

Mirror the nine scenarios with these Java APIs:

- `connect`: `newConnection()`, `createChannel()`, `isOpen()`, close.
- `roundtrip`: `exchangeDeclare`, `queueDeclare`, `queueBind`, `basicPublish` with `BasicProperties`, `basicGet`, body compare, `basicAck`, cleanup.
- `properties`: `BasicProperties.Builder` with content type, correlation id, message id, `timestamp(new Date())`, headers `Map.of("s","text","b",true,"n",42L)`; assert on the `GetResponse` properties; timestamp compared to the second.
- `qos`: `basicQos(2)`, publish 4, `basicConsume` + `CountDownLatch`; assert 2 before ack (no third within 500 ms), ack one, assert a third, ack all.
- `nack-requeue`: publish, get, `basicNack(tag, false, true)`, get until `envelope.isRedeliver()`, assert body, ack.
- `ack`: publish two, get/ack both, assert `basicGet` returns null.
- `basic-get-empty`: null on an empty queue, then publish/get on the same channel.
- `confirms`: `confirmSelect()`, publish, `waitForConfirmsOrDie(10_000)`.
- `round-robin`: two channels with `basicQos(1)` and `basicConsume`, publish 10, collect via `BlockingQueue` until 10 or 10 s, assert each body exactly once, ack each.

- [ ] **Step 4: Build and run**

```powershell
cd tests/clients/java
mvn -q -DskipTests package
cd ..\..
pwsh tests/clients/run.ps1 -Client java
```

Expected: the jar builds and all nine ids pass. If a scenario fails, follow Task 1 Step 7.

- [ ] **Step 5: Run the existing C# suites and commit**

Run the four suites from Task 1 Step 8; expected counts unchanged unless Step 4 added tests.

```powershell
git add tests/clients/java .gitignore tests/clients/README.md
git commit -m "test(conformance): add java rabbitmq client suite"
```

---

### Task 4: Go amqp091 suite

**Files:**
- Create: `tests/clients/go/go.mod`
- Create: `tests/clients/go/go.sum` (generated)
- Create: `tests/clients/go/main.go`

**Interfaces:**
- Consumes: the environment contract and PASS/FAIL protocol from Task 1; `github.com/rabbitmq/amqp091-go v1.16.0`.
- Produces: a module the Task 1 runner invokes with `go run .`.

- [ ] **Step 1: Install Go locally**

```powershell
winget install --id GoLang.Go --accept-source-agreements --accept-package-agreements
```

Expected: `go version` works in a new shell. If the winget id differs, record the exact id in `tests/clients/README.md`.

- [ ] **Step 2: Write `go.mod` and fetch the dependency**

```text
module redismq/conformance

go 1.23

require github.com/rabbitmq/amqp091-go v1.16.0
```

Run `go mod tidy` from `tests/clients/go`; expected: `go.sum` is created.

- [ ] **Step 3: Write `main.go`**

Read the same five environment variables. A `run(id string, fn func() error)` helper prints `PASS id` or `FAIL id: message` and exits 1. Dial with `amqp.DialConfig` or an `amqp://` URL built from the environment; open a fresh channel per scenario; close in `defer`.

Mirror the nine scenarios with these APIs:

- `connect`: dial, `Channel()`, verify not closed, close.
- `roundtrip`: `ExchangeDeclare`, `QueueDeclare`, `QueueBind`, `PublishWithContext`, `Get`, body compare, `Ack`, `QueueDelete`/`ExchangeDelete`.
- `properties`: `Publishing` with content type, correlation id, message id, `Timestamp`, headers `amqp.Table{"s":"text","b":true,"n":int64(42)}`; assert on `Delivery`.
- `qos`: `Qos(2, 0, false)`, publish 4, `Consume`; assert 2 before ack, no third within 500 ms, ack one, assert a third, ack all.
- `nack-requeue`: publish, get, `Nack(tag, false, true)`, get until `Redelivered`, assert body, ack.
- `ack`: publish two, get/ack both, assert the next `Get` returns `ok == false`.
- `basic-get-empty`: `ok == false` on an empty queue, then publish/get on the same channel.
- `confirms`: `Confirm(false)`, `NotifyPublish`, publish, await one confirmation with `Ack == true` within 10 s.
- `round-robin`: two consumers on one channel with `Qos(1...)`, publish 10, collect until 10 or 10 s, assert each body exactly once, ack each.

Use `context.WithTimeout` and channel receives in `select` for bounded waits.

- [ ] **Step 4: Run the Go suite**

```powershell
pwsh tests/clients/run.ps1 -Client go
```

Expected: all nine ids pass. If a scenario fails, follow Task 1 Step 7.

- [ ] **Step 5: Run the existing C# suites and commit**

Run the four suites from Task 1 Step 8; expected counts unchanged unless Step 4 added tests.

```powershell
git add tests/clients/go tests/clients/README.md
git commit -m "test(conformance): add go amqp091 suite"
```

---

### Task 5: CI matrix, documentation, and full local gate

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/CONFORMANCE.md`
- Modify: `docs/DEVELOPMENT.md`
- Modify: `docs/index.md`
- Modify: `README.md`
- Modify: `tests/clients/README.md`

**Interfaces:**
- Consumes: `run.ps1` and all four suites from Tasks 1–4.
- Produces: a `conformance` CI job matrix and user-facing instructions.

- [ ] **Step 1: Add the CI `conformance` job**

Append after `test` (before `pack`), with:

```yaml
  conformance:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    strategy:
      fail-fast: false
      matrix:
        client: [pika, node, java, go]
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup .NET SDK
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Setup Python
        if: matrix.client == 'pika'
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Setup Node
        if: matrix.client == 'node'
        uses: actions/setup-node@v4
        with:
          node-version: '22'

      - name: Setup Java
        if: matrix.client == 'java'
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '21'
          cache: maven

      - name: Setup Go
        if: matrix.client == 'go'
        uses: actions/setup-go@v5
        with:
          go-version: 'stable'

      - name: Run client conformance
        run: pwsh tests/clients/run.ps1 -Client ${{ matrix.client }}

      - name: Upload broker log
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: conformance-${{ matrix.client }}-logs
          path: tests/clients/.logs/**
          if-no-files-found: ignore
```

Change `run.ps1` to write broker logs to `tests/clients/.logs/<client>.log` (create the directory; add `.logs/` to `.gitignore`) so the upload path exists. Keep the temp publish directory outside the repo.

- [ ] **Step 2: Update `docs/CONFORMANCE.md`**

Add a "Multi-client conformance" section: the nine scenario ids and what each proves, the four clients and pinned versions, local command, CI matrix, the C# reference mapping, and the TLS/Redis non-goals. State that the C# suite remains 19 tests.

- [ ] **Step 3: Update `docs/DEVELOPMENT.md`**

Add a "Client conformance" subsection with prerequisites (Python 3, Node 22, Java 21 + Maven, Go) and `pwsh tests/clients/run.ps1 -Client all`; remove client compatibility from the remaining-work list.

- [ ] **Step 4: Update `docs/index.md`**

Remove "Broader client compatibility (Pika and others)" from "What remains for production v1". Correct any other stale entries only if this slice changes them (packaging and observability are already done; remove them too if still listed).

- [ ] **Step 5: Add one README line**

In the Conformance Suite section, add: "A second gate runs Pika, amqplib, the RabbitMQ Java client and amqp091-go over real sockets; see [docs/CONFORMANCE.md](docs/CONFORMANCE.md#multi-client-conformance)."

- [ ] **Step 6: Final local verification**

```powershell
pwsh tests/clients/run.ps1 -Client all
dotnet build RedisMQ.sln --no-incremental
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -noColor
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll -noColor
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
```

Expected: all four client suites report all nine ids; build 0 errors and the recorded warning count; Broker 359/0 plus any new regression tests, Conformance 19/0, Redis 67/0, Hosting 11/0. Check every documentation link and `git status` (only intended files).

- [ ] **Step 7: Commit**

```powershell
git add .github/workflows/ci.yml docs/CONFORMANCE.md docs/DEVELOPMENT.md docs/index.md README.md tests/clients/README.md
git commit -m "ci(conformance): gate mainstream amqp clients"
```

---

## Final verification

- `pwsh tests/clients/run.ps1 -Client all` → Pika/Node/Java/Go each report all nine ids, exit 0.
- CI `conformance` matrix green for all four clients.
- C# suites: Broker 359/0 (plus any new regression tests), Conformance 19/0, Redis 67/0, Hosting 11/0.
- `dotnet build RedisMQ.sln --no-incremental` → 0 errors, recorded analyzer warning count.
- `RedisMQ.Broker` still has no package references.
- Documentation links resolve; remaining-work lists no longer claim client compatibility is missing.
- `git status` clean.
