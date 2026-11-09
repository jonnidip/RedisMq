# RedisMQ Conformance Suite

The conformance project (`tests/RedisMQ.Conformance`) validates RedisMQ against a real
**RabbitMQ.Client 7.2.2** client over a **real socket**. It proves that RedisMQ speaks
the same AMQP 0-9-1 dialect a real RabbitMQ broker does.

## Test files

| File | Purpose |
|------|---------|
| `TestBrokerHost.cs` | Harness: builds listeners through `BrokerListeners`, exposes `Port` and `TlsPort`, runs guest/guest with `BrokerUser.FullAccess` on vhost `/`. |
| `FlowTests.cs` | End-to-end flow scenarios, including queue expiry (9 tests). |
| `ErrorTests.cs` | Error-surface scenarios (5 tests). |
| `WireFlowTests.cs` | Wire-level `channel.flow` over raw `TcpClient` + broker codec primitives (2 tests). |
| `TlsFlowTests.cs` | TLS and mutual TLS over a real socket (3 tests). |
| `TestCertificates.cs` | Shared test material (linked from `Broker.Tests`): self-signed localhost PFX/PEM, test CA, CA-signed client certificate. |

Total: **19 tests**.

## TestBrokerHost

```csharp
await using TestBrokerHost host = TestBrokerHost.Start();

// Plaintext:
await using var connection = await factory.CreateConnectionAsync(
    [new AmqpTcpEndpoint("localhost", host.Port)]);

// TLS:
var ssl = new SslOption { Enabled = true, ServerName = "localhost" };
await using var tlsConnection = await factory.CreateConnectionAsync(
    [new AmqpTcpEndpoint("localhost", host.TlsPort!.Value, ssl)]);
```

- `Start(Action<BrokerServerOptions>? configure = null)` creates the default
  `InMemoryEngine`; `Start(IRouteEngine engine, ...)` runs the suite against a supplied
  engine (used for Redis-engine conformance).
- Options default to `Port = 0`, `Vhosts = ["/"]`, and
  `Users = [BrokerUser.FullAccess("guest", "guest", "/")]`.
- `Port` is `-1` when plaintext is disabled; `TlsPort` is null when TLS is off.
- `StopAsync()` stops the server, then disposes every listener it created.

## FlowTests (end-to-end)

| Test | What it verifies |
|------|------------------|
| `Connect_authenticates_and_opens_a_channel` | PLAIN auth, tune/open handshake, `IsOpen` on connection and channel |
| `Declare_bind_publish_consume_round_trips_content_and_properties` | Direct exchange + queue + bind → publish → consume. Payload byte-identical. Properties round-trip: contentType, correlationId, messageId, timestamp, headers (string/bool/long). |
| `Publish_1000_and_consume_all_in_order_with_qos_10` | 1000 publishes, single consumer, QoS 10, all consumed in order with per-delivery acks |
| `Qos_count_gate_admits_exactly_10_and_more_flow_after_acks` | Publish 20 / QoS 10 → exactly 10 pre-ack → ack 5 → exactly 5 more flow (15 total) |
| `Nack_requeue_redelivers_with_redelivered_flag` | nack(requeue=true) → redelivery with `Redelivered=true`, original body |
| `BasicGet_returns_the_head_and_reports_empty_after` | basic.get → get-ok with head message → get-empty after drain (`cluster-id ""`) |
| `Two_consumers_share_the_queue_round_robin_with_no_message_delivered_twice` | Two consumers, round-robin, all 20 messages exactly once; cancel both → no further delivery |
| `No_ack_consumer_receives_every_message_without_acking` | No-ack consumer receives all 100 messages without acks |
| `Idle_queue_with_x_expires_is_deleted_by_the_reaper` | Declare with `x-expires`, wait for the reaper, passive declare → 404. Polls on a fresh channel because RabbitMQ.Client closes the channel on 404. |

## ErrorTests (error surface)

| Test | Reply code | Class/Method | Text fragment |
|------|-----------|--------------|---------------|
| `Publish_to_unknown_exchange_closes_the_channel_404` | 404 | 60/40 | `no exchange 'nope' in vhost '/'` |
| `Redeclare_queue_with_mismatched_flags_closes_406` | 406 | 50/10 | `equivalent` |
| `Second_exclusive_consumer_is_refused_405` | 405 | 60/20 | `exclusive access to locked queue 'x-q'` |
| `Consume_unknown_queue_closes_404` | 404 | 60/20 | `no queue 'missing' in vhost '/'` |
| `Clean_close_rides_no_errors_and_leaves_the_connection_closed` | — | — | — (no exception) |

Each asserts `OperationInterruptedException.ShutdownReason.ReplyCode/ClassId/MethodId/ReplyText`.

## WireFlowTests (channel.flow at raw wire level)

RabbitMQ.Client 7.x has **no client API for `channel.flow`**, so these tests use a raw
`TcpClient` plus the broker's own codec primitives (`FrameWriter`, `MethodCodec`,
`ByteWriter`, `MethodRegistry`, `FrameDecoder`) to speak AMQP directly over the real
`SocketListener`.

| Test | What it verifies |
|------|------------------|
| `Channel_flow_true_answers_flow_ok_and_a_publish_still_delivers` | `channel.flow(active=true)` → `channel.flow-ok(active=true)` (20/21); a publish→deliver still flows afterward |
| `Channel_flow_false_pauses_the_channel_instead_of_closing_the_connection` | `channel.flow(active=false)` → `channel.flow-ok(active=false)`; the channel and connection stay open, a second channel opens normally, and flow can resume |

## TlsFlowTests (TLS and mutual TLS)

| Test | What it verifies |
|------|------------------|
| `Connect_over_tls_and_publish_consume` | Server-auth TLS using a self-signed localhost PFX; declare, publish, get |
| `Mutual_tls_accepts_a_client_certificate` | `ClientCertificateRequired` + CA path; client presents a CA-signed PFX via `SslOption.CertPath`/`CertPassphrase`; publish/get succeeds |
| `Mutual_tls_refuses_a_client_without_a_certificate` | A certificate-less client cannot establish a usable connection |

TLS clients must attach the `SslOption` to the explicit endpoint:
`new AmqpTcpEndpoint("localhost", host.TlsPort!.Value, ssl)`. Setting
`ConnectionFactory.Ssl` alone does **not** upgrade endpoints passed in a list.

The self-signed test certificate requires
`AcceptablePolicyErrors = RemoteCertificateNameMismatch | RemoteCertificateChainErrors`.

## Multi-client conformance

A second gate drives the published sample host over real sockets with four independent
AMQP 0-9-1 client libraries: Python **Pika 1.4.4**, Node **amqplib 2.3.1**, the
**RabbitMQ Java client 5.25.0**, and Go **amqp091-go v1.16.0**. The suites are
client-only; one PowerShell runner publishes the host, starts a fresh memory-only broker
on a free port per client, and fails unless every required scenario prints
`PASS <id>`.

| Scenario id | What it proves |
|-------------|----------------|
| `connect` | PLAIN auth, vhost and channel open, clean close |
| `roundtrip` | Declare, bind, publish, `basic.get`, byte-identical body, ack |
| `properties` | Content type, correlation id, message id, timestamp, and string/bool/number headers survive value-level round trips |
| `qos` | Prefetch admits exactly N unacked deliveries and resumes after acks |
| `nack-requeue` | `basic.nack(requeue=true)` redelivers the body with the redelivered flag |
| `ack` | Acked messages leave the queue |
| `basic-get-empty` | `basic.get` on an empty queue returns get-empty without closing the channel |
| `confirms` | A successful publish receives `basic.ack` in confirm mode |
| `round-robin` | Two consumers on one queue each receive every message exactly once |

[`tests/clients/scenarios.json`](../tests/clients/scenarios.json) maps every id to the
C# test that pins the same semantics; `confirms` maps to
`RedisMQ.Broker.Tests/ConfirmPipelineTests.cs`. The C# conformance suite remains **19
tests** and is still the semantic reference; this gate adds real third-party client
behavior on top.

Run it locally:

```powershell
pwsh tests/clients/run.ps1 -Client all
```

Prerequisites and per-client details are in
[`tests/clients/README.md`](../tests/clients/README.md). CI has a `conformance` matrix
job with one leg per client.

Out of scope for this gate: TLS (covered by `TlsFlowTests` and per-language trust
stores) and Redis-engine behavior (covered by `RedisMQ.Redis.Tests`). These suites run
memory-only (`InMemoryEngine`).

## Beyond the conformance suite

Observability added **no wire surface**: no new frames, methods or content-property
fields, so the 19 conformance tests above are unchanged and still describe the whole
protocol contract. Metrics, structured logs and the health snapshot are in-process
library seams only; see [API.md](API.md#diagnostics-and-observability).

Some M4 behavior is pinned by the broker and Redis suites rather than by a real client,
because it needs fault injection or engine introspection:

| Area | Suite | Examples |
|------|-------|----------|
| Honest confirms / backpressure / ordering / retired channels | `RedisMQ.Broker.Tests` (`ConfirmPipelineTests`) | Receipt failure → nack; full pipeline blocks; mandatory return before confirm; late confirm discarded |
| DLX, `x-death`, delivery limit, TTL | `RedisMQ.Broker.Tests` (engine + wire tests) | Reject/expiry dead-letter; loop guard; count reset; relative expiry preserved on requeue |
| Queue expiry / `LastUsed` | `RedisMQ.Broker.Tests` (`ReaperTests`) + conformance | Idle queue deleted; active consumer keeps it alive |
| Permissions / PBKDF2 / TLS transport faults | `RedisMQ.Broker.Tests` (`PermissionTests`, `PermissionWireTests`, `TlsTransportTests`, `BrokerListenersTests`) | Default deny, 403 wording, regex timeout, bad handshake survival, mTLS rejection |
| Redis mirror / replay / dedupe / eviction gate | `RedisMQ.Redis.Tests` | Receipt completion, settle removal, requeue replacement, crash-window dedupe, topology replay |

## Known limitations to document alongside conformance

- **At-least-once delivery.** Confirms acknowledge durability, not exactly-once
  consumption.
- **`Redelivered=false` after crash replay.** Unacked messages are not persisted as
  unacked; they return as ready.
- **Confirm timeout may still land.** A nacked publish can persist if the timed-out
  `XADD` completes later.
- **Partial fan-out.** A publish can persist on some targets and fail on others; the
  confirm is a nack.
- **TLS certificate rotation requires a restart.**
- **No guest-loopback restriction.** Configure permissions instead.

## Known client quirk (documented in tests)

RabbitMQ.Client 7.x decodes field-table longstr (`'S'`) header values as `byte[]`, not
`string`. The conformance tests round-trip:

```csharp
Assert.Equal("strval", Encoding.UTF8.GetString((byte[])args.BasicProperties.Headers!["h-string"]!));
```

This is client-side behavior, not a broker bug — RedisMQ correctly emits `'S'` +
longstr on the wire.

## Running

### Preferred (CI path)

```bash
dotnet test RedisMQ.sln
```

### Direct runner (this dev machine — `dotnet test` is broken by an SDK MTP bug)

```bash
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
```

Expected: `Total: 19, Errors: 0, Failed: 0, Skipped: 0`.

Run it twice when touching TLS or confirm timing; the second pass catches handshake
and backpressure flakes.

## Adding a conformance test

1. Add a `[Fact]` to `FlowTests.cs`, `ErrorTests.cs`, `WireFlowTests.cs`, or
   `TlsFlowTests.cs`.
2. Use `TestBrokerHost.Start()` for a fresh broker per test (each test gets its own
   ephemeral port). Use `Start(options => options.Tls = ...)` for TLS.
3. Connect with `ConnectionFactory { UserName = "guest", Password = "guest", VirtualHost = "/" }`.
4. Pass `TestContext.Current.CancellationToken` to client calls (xUnit1051 rule).
5. Build with `dotnet build tests/RedisMQ.Conformance` — must be **0 warnings, 0
   errors**.
6. Run the suite — must be green.
