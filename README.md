# RedisMQ

RedisMQ is an AMQP 0-9-1 (RabbitMQ dialect) wire-server shell over **in-memory queues**. This is the **M0 milestone**: it proves the wire layer — codec, connection/channel state machines, handshake, declare/bind, publish, consume with prefetch credits, ack/nack/reject, basic.get, recover — against a real RabbitMQ.Client 7.x client over TCP. M1 swaps the in-memory engine for Redis Streams (`XADD`/consumer groups).

## Quick start

```bash
dotnet test tests/RedisMQ.Conformance
```

Runs the RabbitMQ.Client 7.2.2 conformance suite (connect/auth/tune, declare/bind/publish/consume, QoS gating, ack/nack, basic.get, error surface) against the in-process broker over a real TCP socket.

## Structure

- `src/RedisMQ.Broker/` — core engine (zero external dependencies, `net10.0`): codec, frame pipeline, connection/channel FSMs, in-memory route engine.
- `tests/RedisMQ.Broker.Tests/` — codec/FSM unit tests against the canonical AMQP 0-9-1 codegen fixture.
- `tests/RedisMQ.Conformance/` — end-to-end conformance with RabbitMQ.Client 7.x.

No Redis anywhere in M0 — the memory engine implements `IRouteEngine` such that M1 can add `RedisStreamRouteEngine` without touching codec/wire code.
