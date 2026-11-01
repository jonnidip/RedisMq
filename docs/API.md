# RedisMQ API Reference

Reference for the core library `RedisMQ.Broker` (`net10.0`, zero external dependencies).

## Namespaces

| Namespace | Contents |
|-----------|----------|
| `RedisMQ.Broker.Server` | `AmqpBrokerServer`, `WireConnection`, `ChannelContext`, `BrokerServerOptions`, `BrokerUser`, `HeartbeatWatchdog`, `IChannelDeliveryHost`, `ChannelOutcome*` |
| `RedisMQ.Broker.Memory` | `InMemoryEngine`, `InMemoryQueue`, `InMemoryMessage`, `ExchangeInfo`, `QueueInfo`, `IRouteEngine`, `IQueueConsumer`, `ConsumeRegisterOutcome`, `GetClaimOutcome` |
| `RedisMQ.Broker.Codec` | `ByteReader`, `ByteWriter`, `FieldTable`, `FieldValue`, `AmqpFrame`, `FrameDecoder`, `FrameWriter`, `ContentHeaderCodec`, `ContentProperties`, `ContentHeader`, `MethodCodec`, `AmqpMethod`, `AmqpDecodeException`, `AmqpEncodeException`, `AmqpProtocolException` |
| `RedisMQ.Broker.Protocol` | `MethodRegistry`, `MethodSchema`, `AmqpField`, `ContentProperty`, `AmqpConstants`, `WireType`, `Direction` |
| `RedisMQ.Broker.Transport` | `TransportSide`, `TransportPair`, `IListenerTransport`, `SocketListener` |

---

## Server layer

### `AmqpBrokerServer`

The broker's connection lifecycle.

```csharp
public sealed class AmqpBrokerServer
{
    public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine, IListenerTransport? listener = null);
    public Task StartAsync();
    public Task StopAsync();
}
```

- `StartAsync()` arms the accept loop in the background. No-op (returns completed task) when constructed
  without a listener. Throws `InvalidOperationException` if already started.
- `StopAsync()` cancels accepting, waits for the accept loop, then drains all connection tasks.
- Connection tasks are tracked and removed as they complete. Server shutdown cancels the per-connection
  token, ending the run without a close frame (RabbitMQ-parity wire kill; client sees EOF).

### `BrokerServerOptions`

```csharp
public sealed class BrokerServerOptions
{
    public int Port { get; set; }                        // 0 = OS-assigned ephemeral
    public string[] Vhosts { get; set; } = ["/"];
    public IReadOnlyList<BrokerUser> Users { get; set; } = [];
    public uint FrameMax { get; set; } = 131072;
    public ushort Heartbeat { get; set; } = 60;
}
```

### `BrokerUser`

```csharp
public sealed record BrokerUser(string Name, string Password);
```

A single PLAIN SASL account.

### `WireConnection`

One AMQP connection over a transport. Implements `IChannelDeliveryHost`. Not constructed directly by
users — created by `AmqpBrokerServer` for each accepted transport side.

Key internals (documented for maintainers):
- Connection FSM: `AwaitStartOk` → `AwaitTuneOk` → `AwaitOpen` → `Serving` → `ClosingByServer`.
- `_writeGate` (`SemaphoreSlim(1,1)`) serializes every wire write across the frame pump and queue pumps.
- `HeartbeatWatchdog` kills at 2× negotiated heartbeat; reset on every inbound frame.

### `ChannelContext`

One AMQP channel's finite state machine. States: `Starting` → `Running` → `Mourning`.

```csharp
public sealed class ChannelContext
{
    public bool IsMourning { get; }
    public ChannelOutcome OnMethod(AmqpMethod method);
    public ChannelOutcome OnContent(AmqpFrame frame);
    internal void TearDownActiveConsumers();
    internal ContentPhase PendingContentPhase { get; }
    internal ChannelContext(ushort channel, string vhost, IRouteEngine engine, IChannelDeliveryHost host);
}
```

Created lazily by `WireConnection.GetOrCreateChannel(channel)`. Owns:
- Prefetch credits (`_prefetchCount`, `_prefetchSize`, `_outstandingBytes`).
- Unacked ledger (`_unacked`: tag → `(queue, message)`).
- Consumer registries (`_consumerQueues`, `_consumerEntries`).
- Serialized writer chain (`_writerGate` + job queue, drained by `RunWriterChainAsync`).
- Content assembler phase (`_pendingContentPhase`).

### `IChannelDeliveryHost`

Write surface for guest callers (queue pumps). Implemented by `WireConnection`.

```csharp
public interface IChannelDeliveryHost
{
    ValueTask WriteMethodToWireAsync(ushort channel, AmqpMethod method, CancellationToken ct);
    ValueTask WriteContentToWireAsync(ushort channel, ContentProperties properties, byte[] body, CancellationToken ct);
    ValueTask WriteDeliveryToWireAsync(ushort channel, AmqpMethod deliver, ContentProperties properties, byte[] body, CancellationToken ct);
}
```

- `WriteMethodToWireAsync` — single method frame, gated.
- `WriteContentToWireAsync` — header + body re-chunked per frame-max, one atomic write.
- `WriteDeliveryToWireAsync` — deliver method + header + body re-chunked, one atomic write (delivery frames never interleave).

### `HeartbeatWatchdog`

Silence policing. Not a timer itself — checked by the connection's timer.

```csharp
public class HeartbeatWatchdog
{
    public HeartbeatWatchdog(TimeSpan heartbeatInterval, Action<HeartbeatWatchdog>? onTimeout = null);
    public bool TimedOut { get; private set; }
    protected virtual DateTimeOffset Now => DateTimeOffset.UtcNow;
    public void OnFrameReceived();   // reset silence window
    public void Check();             // evaluate; raise onTimeout once at 2x interval
}
```

- `heartbeatInterval` must be positive; 0 means no watchdog is started.
- Kill fires at `2 × heartbeatInterval` of silence.
- `Now` is virtual for fake-clock tests.

### `ChannelOutcome` family

Abstract record returned by `ChannelContext.OnMethod` / `OnContent`, instructing `WireConnection`
what to do on the wire:

| Record | Meaning |
|--------|---------|
| `ChannelOutcomeReply(Reply, RetireAfter)` | Send the method reply on the channel; optionally retire after close-ok. |
| `ChannelOutcomeSilent(RetireChannel)` | No wire bytes; optionally free the channel number. |
| `ChannelOutcomeSoftError(Code, Text, ClassId, MethodId)` | Soft channel close (403/404/405/406/505); channel starts mourning. |
| `ChannelOutcomeHardError(Code, Text, ClassId, MethodId)` | Hard connection close on channel 0. |
| `ChannelOutcomeBasicReturn(ReturnMethod, Properties, Body)` | basic.return (312) + full content. |
| `ChannelOutcomeChain` | Consumer-path writes scheduled on the channel's writer chain; connection drains inline. |

---

## Memory layer

### `IRouteEngine`

The topology/routing seam. M0's `InMemoryEngine` implements it; M1 will provide `RedisStreamRouteEngine`.

```csharp
public interface IRouteEngine
{
    ExchangeInfo?   GetExchange(string vhost, string name);
    ExchangeInfo?   DeclareExchange(ExchangeInfo exchange);
    QueueInfo?      GetQueue(string vhost, string name);
    QueueInfo?      DeclareQueue(QueueInfo queue);
    void            Bind(string vhost, string exchange, string queue, string routingKey);
    void            Unbind(string vhost, string exchange, string queue, string routingKey);
    IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties, string routingKey);
    void            Enqueue(string vhost, string queue, ContentProperties properties, byte[] body, string exchange, string routingKey);
    ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer);
    void            UnregisterConsumer(string vhost, string queue, string consumerTag);
    void            Requeue(string vhost, string queue, InMemoryMessage message);
    GetClaimOutcome TryClaimForGet(string vhost, string queue, out InMemoryMessage? message, out int remaining);
    void            Nudge(string vhost, string queue);
}
```

**Declare contract** (`DeclareExchange` / `DeclareQueue`): creation-only compare-and-set. On miss,
store and return `null`. On hit, return the existing record unchanged — never replaced. This makes
concurrent first-declares deterministic: exactly one caller observes `null`, the other observes the
winner's record.

### `InMemoryEngine`

```csharp
public sealed class InMemoryEngine : IRouteEngine
{
    public InMemoryEngine(IReadOnlyList<string> vhosts);
    // predeclares "" (default) and "amq.direct" exchanges per vhost
    public IEnumerable<string> QueueNamesOfVhost(string vhost); // test introspection
    public IReadOnlyList<InMemoryMessage> PeekQueue(string queue); // test introspection, ready-only
}
```

Also implements all `IRouteEngine` members. Thread-safe via a single `_lock`.

### `ExchangeInfo` / `QueueInfo`

```csharp
public sealed record ExchangeInfo(string Vhost, string Name, string Type, bool Durable = false, bool AutoDelete = false, bool Internal = false);
public sealed record QueueInfo(string Vhost, string Name, bool Durable = false, bool Exclusive = false, bool AutoDelete = false);
```

### `InMemoryMessage`

```csharp
public sealed record InMemoryMessage(
    ContentProperties Props,
    byte[] Body,
    string Exchange,
    string RoutingKey,
    ulong EnvelopeId,
    bool Redelivered = false);
```

- `EnvelopeId`: engine-global, monotonic, 1-based. Fan-out to N queues creates N messages with N ids.
- `Redelivered`: false on first delivery; set to `true` by `Requeue`.
- Body is a defensive copy — no aliasing.

### `InMemoryQueue`

Per-queue delivery state (internal to `InMemoryEngine`). Ready buffer (FIFO), consumer registry
(registration order), round-robin cursor, wake-driven pump task.

### `IQueueConsumer`

Pump-facing contract. Created by the channel, registered into the engine.

```csharp
public interface IQueueConsumer
{
    string ConsumerTag { get; }
    bool IsExclusive { get; }
    bool TryAdmitForDelivery(InMemoryMessage message); // synchronous under engine gate
    Task DrainDeliveryWritesAsync();                   // outside engine gate
}
```

`TryAdmitForDelivery` performs the credit check, ledger registration, and writer-job scheduling
synchronously. Returns `true` when admitted (message leaves the ready buffer); `false` when the
consumer's channel has no credit (message stays queued).

### `ConsumeRegisterOutcome`

| Value | Meaning |
|-------|---------|
| `Registered` | Consumer joined; pump armed. |
| `QueueNotFound` | Queue does not exist in vhost. |
| `ExclusiveHeld` | Exclusive consumer holds queue, or new consumer demands exclusivity on occupied queue. |

### `GetClaimOutcome`

| Value | Meaning |
|-------|---------|
| `Claimed` | Head message popped; `message` and `remaining` are meaningful. |
| `QueueNotFound` | Queue does not exist. |
| `ExclusiveHeld` | Exclusive consumer holds queue; basic.get refused. |
| `Empty` | No ready message → get-empty path. |

---

## Codec layer

### `ByteReader`

`ref struct` big-endian reader over `ReadOnlySpan<byte>`.

```csharp
public readonly struct ByteReader
{
    public ByteReader(ReadOnlySpan<byte> buffer);
    public int Offset { get; }
    public int Remaining { get; }
    public byte ReadOctet();
    public ushort ReadUShort();
    public short ReadShort();
    public int ReadLong();
    public long ReadLongLong();
    public string ReadShortstr();
    public byte[] ReadLongstr();
    public bool ReadBit();               // packed bitfield, LSB-first
    public DateTimeOffset ReadTimestamp();
    public bool TryReadFieldTable(out FieldTable table);
    internal bool TryReadFieldArray(int depth, out FieldValue[] items);
}
```

All reads throw `AmqpDecodeException` on truncation. Non-bit reads resynchronize the bit cursor.

### `ByteWriter`

Big-endian writer with bit-group support.

```csharp
public sealed class ByteWriter
{
    public ByteWriter();
    public int Length { get; }
    public byte[] ToArray();
    public void WriteOctet(byte value);
    public void WriteUShort(ushort value);
    public void WriteShort(short value);
    public void WriteLong(int value);
    public void WriteLongLong(long value);
    public void WriteShortstr(string value);      // 1-byte length + UTF-8 (max 255 bytes)
    public void WriteLongstr(ReadOnlySpan<byte> value); // 4-byte length + content
    public void WriteRaw(ReadOnlySpan<byte> value);
    public void WriteBits(params bool[] bits);     // one octet per group, LSB-first, zero-padded
    public void WriteFieldTable(FieldTable table);
    public void WriteTimestamp(DateTimeOffset value);
}
```

### `FieldTable` / `FieldValue`

Ordered map of shortstr keys to typed field values. Wire letters:

| Letter | Type | CLR type | Factory |
|--------|------|----------|---------|
| `t` | boolean | `bool` | `FieldValue.From(bool)` |
| `b` | signed-8 | `sbyte` | `FieldValue.From(sbyte)` |
| `B` | unsigned-8 | `byte` | `FieldValue.From(byte)` |
| `s` | signed-16 | `short` | `FieldValue.From(short)` |
| `u` | unsigned-16 | `ushort` | `FieldValue.From(ushort)` |
| `I` | signed-32 | `int` | `FieldValue.From(int)` |
| `l` | signed-64 | `long` | `FieldValue.From(long)` |
| `L` | unsigned-64 | `ulong` | `FieldValue.From(ulong)` |
| `f` | float | `float` | `FieldValue.From(float)` |
| `d` | double | `double` | `FieldValue.From(double)` |
| `D` | decimal | `decimal` | `FieldValue.From(decimal)` |
| `S` | long string | `string` | `FieldValue.From(string)` |
| `x` | long binary | `byte[]` | `FieldValue.From(byte[])` |
| `T` | timestamp | `DateTimeOffset` | `FieldValue.From(DateTimeOffset)` |
| `F` | nested table | `FieldTable` | `FieldValue.From(FieldTable)` |
| `A` | array | `FieldValue[]` | `FieldValue.List(...)` |
| `V` | null | `null` | `FieldValue.Null` |

`FieldTable : IReadOnlyDictionary<string, FieldValue>`. Insertion order preserved on the wire;
duplicate key replaces value in place.

### `AmqpFrame` / `FrameDecoder` / `FrameWriter`

```csharp
public sealed record AmqpFrame(byte Type, ushort Channel, ReadOnlyMemory<byte> Payload);
// FrameMethod=1, FrameHeader=2, FrameBody=3, FrameHeartbeat=8, FrameEnd=0xCE
// FrameOverhead=8, FrameMinSize=4096

public static class FrameDecoder
{
    public static DecodeMode ReadFrames(ref ReadOnlySequence<byte> buffer, List<AmqpFrame> frames, uint negotiatedFrameMax);
}

public static class FrameWriter
{
    public static void WriteMethod(ref ByteWriter writer, ushort channel, ushort classId, ushort methodId, ReadOnlySpan<byte> args);
    public static void WriteContentHeader(ref ByteWriter writer, ushort channel, ushort classId, long bodySize, ContentProperties properties);
    public static void WriteBody(ref ByteWriter writer, ushort channel, ReadOnlySpan<byte> body, uint frameMax);
}
```

### `ContentHeaderCodec` / `ContentProperties`

```csharp
public sealed record ContentProperties(
    string? ContentType = null, string? ContentEncoding = null, FieldTable? Headers = null,
    byte? DeliveryMode = null, byte? Priority = null, string? CorrelationId = null,
    string? ReplyTo = null, string? Expiration = null, string? MessageId = null,
    DateTimeOffset? Timestamp = null, string? Type = null, string? UserId = null,
    string? AppId = null, string? ClusterId = null);

public static class ContentHeaderCodec
{
    public static ContentHeader Read(ref ByteReader reader);
    public static void Write(ByteWriter writer, ushort classId, long bodySize, ContentProperties properties);
}

public sealed record ContentHeader(ushort ClassId, ushort Weight, long BodySize, ContentProperties Properties);
```

### `MethodCodec` / `AmqpMethod` / `MethodRegistry`

```csharp
public sealed class AmqpMethod
{
    public AmqpMethod(MethodSchema schema);
    public MethodSchema Schema { get; }
    public ushort ClassId => Schema.ClassId;
    public ushort MethodId => Schema.MethodId;
    public object? Get(string name);
    public void Set(string name, object? value);
}

public static class MethodCodec
{
    public static AmqpMethod ReadMethod(ref ByteReader reader, MethodSchema schema);
    public static void WriteMethod(ref ByteWriter writer, in AmqpMethod method);
}

public static class MethodRegistry
{
    public static IReadOnlyList<MethodSchema> All { get; }
    public static MethodSchema? Schema(int classId, ushort methodId);
    public static bool TryGetByName(string fullName, out MethodSchema? schema);
    public static IReadOnlyList<ContentProperty>? ContentProperties(int classId);
}
```

`MethodSchema(classId, methodId, name, direction, fields)` with `HasContent` init-only property.
`AmqpField(name, wireType)`.

### `AmqpConstants`

M0 reply/error codes:

```csharp
public static class AmqpConstants
{
    public const ushort ContentTooLarge = 311;
    public const ushort NoRoute = 312;
    public const ushort AccessRefused = 403;
    public const ushort NotFound = 404;
    public const ushort ResourceLocked = 405;
    public const ushort PreconditionFailed = 406;
    public const ushort ConnectionForced = 320;
    public const ushort InvalidPath = 402;
    public const ushort FrameError = 501;
    public const ushort SyntaxError = 502;
    public const ushort ChannelError = 504;
    public const ushort UnexpectedFrame = 505;
    public const ushort NotAllowed = 530;
    public const ushort NotImplemented = 540;
    public const ushort InternalError = 541;
}
```

### Exceptions

| Exception | When |
|-----------|------|
| `AmqpDecodeException` | Wire data truncated or unparseable |
| `AmqpEncodeException` | CLR value not expressible in AMQP wire form |
| `AmqpProtocolException` | Hard protocol error (bad 0xCE, oversize frame, unknown type) |

---

## Transport layer

### `IListenerTransport`

```csharp
public interface IListenerTransport
{
    ValueTask<TransportSide?> AcceptAsync(CancellationToken cancellationToken = default);
}
```

Null return = listener closed (accept stream ends).

### `TransportSide`

```csharp
public sealed class TransportSide
{
    public PipeReader Reader { get; }
    public PipeWriter Writer { get; }
    public async ValueTask CompleteAsync(); // idempotent
}
```

### `TransportPair`

In-proc duplex pair for unit tests:

```csharp
public static class TransportPair
{
    public static TransportPair Create(); // { TransportClient, TransportServer }
}
```

### `SocketListener`

Real TCP `IListenerTransport`:

```csharp
public sealed class SocketListener : IListenerTransport, IAsyncDisposable
{
    public SocketListener(int announcedPort); // 0 → OS-assigned
    public int AnnouncedPort { get; }
    public ValueTask<TransportSide?> AcceptAsync(CancellationToken ct = default);
    public ValueTask DisposeAsync();
}
```

- Dual-mode IPv6 bind (`::1` + `127.0.0.1`) with IPv4 fallback.
- Wraps accepted `Socket` in `NetworkStream` → `PipeReader`/`PipeWriter`.

---

## Conformance test dependencies

`tests/RedisMQ.Conformance` uses:
- `RabbitMQ.Client` 7.2.2 (real client over TCP)
- `xunit.v3` 4.0.2, `Microsoft.NET.Test.Sdk` 18.11.0

`tests/RedisMQ.Broker.Tests` uses:
- `xunit.v3` 4.0.2, `Microsoft.NET.Test.Sdk` 18.11.0
- `fixtures/rabbitmq-codegen.json` (canonical AMQP 0-9-1 codegen, committed for offline CI)

## Build requirements

- .NET 10 SDK (`net10.0` target)
- `Nullable=enable`, `LangVersion=latest`, `ImplicitUsings=enable`
- Core project: zero `PackageReference`s
- Test projects: xunit v3 + Test SDK
