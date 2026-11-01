# RedisMQ Conformance Suite

The conformance project (`tests/RedisMQ.Conformance`) validates RedisMQ against a real
**RabbitMQ.Client 7.2.2** client over a **real TCP socket**. It proves that RedisMQ speaks
the same AMQP 0-9-1 dialect a real RabbitMQ broker does.

## Test files

| File | Purpose |
|------|---------|
| `TestBrokerHost.cs` | Harness: binds an ephemeral TCP port, runs `AmqpBrokerServer` over `SocketListener` with guest/guest on vhost `/`, exposes `Port`. |
| `FlowTests.cs` | End-to-end flow scenarios (10 tests). |
| `ErrorTests.cs` | Error-surface scenarios (5 tests). |
| `WireFlowTests.cs` | Wire-level `channel.flow` over raw `TcpClient` + broker codec primitives (2 tests). |

## TestBrokerHost

```csharp
var host = TestBrokerHost.Start();
// host.Port is the ephemeral TCP port
await using var connection = factory.CreateConnectionAsync([new AmqpTcpEndpoint("localhost", host.Port)]);
// ...
await host.StopAsync(); // or dispose
```

`Start(Action<BrokerServerOptions>? configure = null)` lets callers override options (frame-max,
heartbeat, users, vhosts).

## FlowTests (end-to-end)

| Test | What it verifies |
|------|------------------|
| `Connect_authenticates_and_opens_a_channel` | PLAIN auth, tune/open handshake, `IsOpen` on connection and channel |
| `Declare_bind_publish_consume_round_trips_content_and_properties` | Declare direct exchange + queue + bind → publish → consume. Payload byte-identical. Content properties round-trip: contentType, correlationId, messageId, timestamp, headers (string/bool/long). |
| `Publish_1000_and_consume_all_in_order_with_qos_10` | 1000 publishes, single consumer, QoS 10, all consumed in order with per-delivery acks |
| `Qos_count_gate_admits_exactly_10_and_more_flow_after_acks` | Publish 20 / QoS 10 → exactly 10 pre-ack → ack 5 → exactly 5 more flow (15 total) |
| `Nack_requeue_redelivers_with_redelivered_flag` | nack(requeue=true) → redelivery with `Redelivered=true`, original body |
| `BasicGet_returns_the_head_and_reports_empty_after` | basic.get → get-ok with head message → get-empty after drain (`cluster-id ""`) |
| `Two_consumers_share_the_queue_round_robin_with_no_message_delivered_twice` | Two consumers, round-robin, all 20 messages exactly once, both consumers get ≥ 1; cancel both → no further delivery |
| `No_ack_consumer_receives_every_message_without_acking` | No-ack consumer receives all 100 messages without acks |
| *(inherited setup/teardown)* | Clean `connection.close` → connection closed, channel closed |

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

RabbitMQ.Client 7.x has **no client API for `channel.flow`**, so these tests use a raw `TcpClient`
plus the broker's own codec primitives (`FrameWriter`, `MethodCodec`, `ByteWriter`, `MethodRegistry`,
`FrameDecoder`) to speak AMQP directly over the real `SocketListener`.

| Test | What it verifies |
|------|------------------|
| `Channel_flow_true_answers_flow_ok_and_a_publish_still_delivers` | `channel.flow(active=true)` → `channel.flow-ok(active=true)` (20/21); a publish→deliver still flows afterward |
| `Channel_flow_false_hard_closes_the_connection_with_540` | `channel.flow(active=false)` → hard `connection.close(10,50)`, code 540, text `NOT_IMPLEMENTED - active=false`, class/method 20/20; TCP peer then sees EOF |

## Known client quirk (documented in tests)

RabbitMQ.Client 7.x decodes field-table longstr (`'S'`) header values as `byte[]`, not `string`.
The conformance tests round-trip:

```csharp
Assert.Equal("strval", Encoding.UTF8.GetString((byte[])args.BasicProperties.Headers!["h-string"]!));
```

This is client-side behavior, not a broker bug — RedisMQ correctly emits `'S'` + longstr on the wire.

## Running

### Preferred (CI path)

```bash
dotnet test RedisMQ.sln
```

### Direct runner (this dev machine — `dotnet test` is broken by an SDK MTP bug)

```bash
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll
```

Expected: `Total: 15, Errors: 0, Failed: 0, Skipped: 0`

## Adding a conformance test

1. Add a `[Fact]` to `FlowTests.cs`, `ErrorTests.cs`, or `WireFlowTests.cs`.
2. Use `TestBrokerHost.Start()` for a fresh broker per test (each test gets its own ephemeral port).
3. Connect with `ConnectionFactory { UserName = "guest", Password = "guest", VirtualHost = "/" }` and
   `factory.CreateConnectionAsync([new AmqpTcpEndpoint("localhost", host.Port)])`.
4. Pass `TestContext.Current.CancellationToken` to client calls (xUnit1051 rule).
5. Build with `dotnet build tests/RedisMQ.Conformance` — must be **0 warnings, 0 errors**.
6. Run the suite — must be green.
