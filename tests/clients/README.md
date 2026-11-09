# Client conformance suites

These suites prove that mainstream AMQP 0-9-1 client libraries talk to RedisMQ the way
they talk to RabbitMQ. Each suite is a small client-only program; one shared PowerShell
runner owns the broker lifecycle and checks that every suite reported every required
scenario.

The nine required scenario ids live in [`scenarios.json`](scenarios.json):

| Id | What it proves |
|----|----------------|
| `connect` | PLAIN auth, vhost and channel open, clean close |
| `roundtrip` | Declare, bind, publish, `basic.get`, byte-identical body, ack |
| `properties` | Content type, correlation id, message id, timestamp and string/bool/number headers survive a round trip |
| `qos` | Prefetch admits exactly N unacked deliveries and resumes after acks |
| `nack-requeue` | `basic.nack(requeue=true)` redelivers the same body with the redelivered flag |
| `ack` | Acked messages leave the queue |
| `basic-get-empty` | `basic.get` on an empty queue returns get-empty without closing the channel |
| `confirms` | Publisher confirms receive one ack for a successful publish |
| `round-robin` | Two consumers on one queue each receive every message exactly once |

## Prerequisites

| Client | Toolchain | Pinned library |
|--------|-----------|----------------|
| Pika | Python 3.9+ | `pika==1.4.4` |
| Node | Node 22 + npm | `amqplib@2.3.1` |
| Java | Java 21+ and Maven | `com.rabbitmq:amqp-client:5.25.0` |
| Go | Go 1.23+ | `github.com/rabbitmq/amqp091-go v1.16.0` |

The runner publishes `samples/RedisMQ.Host`, so the .NET 10 SDK is required too. The
broker runs memory-only (`RedisMQ__Redis__ConnectionString=`); no Redis server is needed.
PowerShell 7 (`pwsh`) is used locally and in CI; Windows PowerShell 5.1 also runs the
script.

Maven is not available through winget in every environment; use the Apache Maven 3.9.x
binary zip (or `choco install maven` from an elevated shell) and put its `bin` on `PATH`.
Go installs from the official zip, or `winget install GoLang.Go` where winget has it.

## Run

From the repository root:

```powershell
pwsh tests/clients/run.ps1 -Client all
```

One client while iterating:

```powershell
pwsh tests/clients/run.ps1 -Client pika
```

The runner installs dependencies (`pip install -r`, `npm ci`, `mvn package`,
`go mod download`) unless `-SkipPrepare` is passed, and starts a fresh broker on a free
port for each client. It fails if a selected client's toolchain is missing; pass
`-SkipMissing` to run only the clients that are installed.

Diagnostic flags:

| Flag | Effect |
|------|--------|
| `-Port <n>` | Use a fixed AMQP port instead of a free one |
| `-Configuration Debug` | Publish a Debug host |
| `-SkipPrepare` | Do not install dependencies or build client artifacts |
| `-SkipMissing` | Warn and skip clients whose toolchain is absent |
| `-KeepBroker` | Leave the broker running; requires exactly one `-Client` |

Broker and client logs are written to `tests/clients/.logs/` (git-ignored) and are
uploaded by CI on failure.

## Environment contract

The runner exports these variables to each client process:

| Variable | Default in the suites | Meaning |
|----------|-----------------------|---------|
| `REDISMQ_HOST` | `127.0.0.1` | AMQP host |
| `REDISMQ_PORT` | `5672` | AMQP port |
| `REDISMQ_USER` | `guest` | PLAIN user |
| `REDISMQ_PASSWORD` | `guest` | PLAIN password |
| `REDISMQ_VHOST` | `/` | Virtual host |

Suites print `PASS <id>` on stdout after a scenario's assertions pass, and
`FAIL <id>: <reason>` on the first failure. Progress goes to stderr, so stdout stays a
clean pass/fail stream. The runner rejects a run that exits non-zero, prints any
`FAIL`, or omits a required id.

## Out of scope

- TLS: `tests/RedisMQ.Conformance` (`TlsFlowTests`) is the TLS gate; per-language trust
  stores are deployment configuration.
- Redis-engine behavior: these suites run `InMemoryEngine`; durability semantics stay
  covered by `RedisMQ.Redis.Tests` and the C# conformance suite.
- Load or latency measurement: that is a separate performance slice.

The C# suite (`tests/RedisMQ.Conformance`) remains the semantic reference and is not
spawned by this runner.
