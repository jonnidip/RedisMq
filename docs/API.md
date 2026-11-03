# RedisMQ API Reference

Reference for the core library `RedisMQ.Broker` (`net10.0`, zero external
dependencies) and the optional `RedisMQ.Redis` engine (StackExchange.Redis only
there).

## Namespaces

| Namespace | Contents |
|-----------|----------|
| `RedisMQ.Broker.Server` | `AmqpBrokerServer`, `WireConnection`, `ChannelContext`, `PendingConfirm`, `BrokerServerOptions`, `BrokerUser`, `BrokerPermission`, `BrokerPassword`, `BrokerPrincipal`, `BrokerTlsOptions`, `BrokerConfigurationException`, `HeartbeatWatchdog`, `IChannelDeliveryHost`, `ChannelOutcome*` |
| `RedisMQ.Broker.Memory` | `InMemoryEngine`, `InMemoryQueue`, `InMemoryMessage`, `ExchangeInfo`, `QueueInfo`, `IRouteEngine`, `IQueueConsumer`, `ConsumeRegisterOutcome`, `GetClaimOutcome`, `EnqueueReceipt`, `EngineDelta`, `QueuedMessage`, `QueuedMessageRef` |
| `RedisMQ.Broker.Codec` | `ByteReader`, `ByteWriter`, `FieldTable`, `FieldValue`, `AmqpFrame`, `FrameDecoder`, `FrameWriter`, `ContentHeaderCodec`, `ContentProperties`, `ContentHeader`, `MethodCodec`, `AmqpMethod`, `AmqpDecodeException`, `AmqpEncodeException`, `AmqpProtocolException` |
| `RedisMQ.Broker.Protocol` | `MethodRegistry`, `MethodSchema`, `AmqpField`, `ContentProperty`, `AmqpConstants`, `WireType`, `Direction` |
| `RedisMQ.Broker.Transport` | `TransportSide`, `TransportPair`, `IListenerTransport`, `SocketListener`, `TlsSocketListener`, `BrokerCertificate`, `BrokerListeners` |
| `RedisMQ.Redis` | `RedisStreamRouteEngine`, `RedisKeys`, `WriteCommand`, `WriteCommandKind`, `StreamCodec` |

---

## Server layer

### `AmqpBrokerServer`

The broker's connection lifecycle. One accept loop runs per listener; one reaper loop
is shared by all listeners.

```csharp
public sealed class AmqpBrokerServer
{
    // Single-listener form (tests, simple hosts):
    public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine,
                            IListenerTransport? listener = null);

    // Multi-listener form (plaintext + TLS):
    public AmqpBrokerServer(BrokerServerOptions options, IRouteEngine engine,
                            IReadOnlyList<IListenerTransport> listeners);

    public Task StartAsync();
    public Task StopAsync();
}
```

- `StartAsync()` calls `options.Validate()`, arms every accept loop in the background,
  and starts the reaper when `ReaperInterval > 0`. Returns a completed task once
  accepting is armed. Throws `InvalidOperationException` if already started.
- `StopAsync()` cancels and awaits every accept loop, the reaper, then all tracked
  connection tasks. Server shutdown cancels the per-connection token, ending the run
  without a close frame (client sees EOF).
- The server **does not dispose the listeners**; the caller owns them.

### `BrokerServerOptions`

```csharp
public sealed class BrokerServerOptions
{
    public int Port { get; set; }                        // 0 = OS-assigned; -1 = no plaintext
    public string[] Vhosts { get; set; } = ["/"];
    public IReadOnlyList<BrokerUser> Users { get; set; } = [];
    public uint FrameMax { get; set; } = 131072;
    public ushort Heartbeat { get; set; } = 60;
    public int ConfirmQueueCapacity { get; set; } = 4096;
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(5);
    public BrokerTlsOptions? Tls { get; set; }

    public void Validate();
}
```

| Property | Meaning |
|----------|---------|
| `Port` | Plaintext listener port. `0` asks the OS for an ephemeral port; a negative value disables the plaintext listener (TLS-only deployments). |
| `Vhosts` | Virtual hosts the broker can open. Opening is still permission-gated per user. |
| `Users` | PLAIN accounts. Empty by default: nobody can authenticate until a user is configured. |
| `FrameMax` / `Heartbeat` | Values offered at `connection.tune`. |
| `ConfirmQueueCapacity` | Admitted-but-unflushed confirms per connection. A full pipeline blocks the frame pump (backpressure), never drops or invents a confirm. |
| `ConfirmTimeout` | How long the confirm loop waits for a publish's receipts before `confirm.nack`. |
| `ReaperInterval` | Sweep interval for message TTL, `x-expires` and `x-delivery-limit` work. `<= 0` disables the reaper. |
| `Tls` | TLS listener settings; `null` or disabled means no TLS listener. |

`Validate()` throws `BrokerConfigurationException` for: two users with the same name;
a user with both or neither of `Password`/`PasswordHash`; a malformed PBKDF2 hash; or
an invalid permission regex. `StartAsync` calls it automatically.

### `BrokerUser` / `BrokerPermission`

```csharp
public sealed record BrokerPermission(string Vhost, string Configure, string Write, string Read);

public sealed record BrokerUser(
    string Name,
    string? Password = null,
    string? PasswordHash = null,
    IReadOnlyList<BrokerPermission>? Permissions = null)
{
    public static BrokerUser FullAccess(string name, string password, params string[] vhosts);
}
```

- A user sets **exactly one** credential form: a plaintext `Password` (development
  only) or a `PasswordHash` produced by `BrokerPassword.Hash`.
- Permissions are **default-deny**. `Permissions` null/empty denies every vhost; a
  vhost absent from the list cannot be opened at all. A listed vhost with deny-all
  patterns opens but permits nothing.
- Each permission field is a .NET regex matched against the resource name:
  - `Configure`: queue/exchange declare and bind/unbind.
  - `Write`: publish (the exchange name, or the routing key on the default exchange).
  - `Read`: consume and basic.get.
- `FullAccess` is the explicit test/sample helper: `.*` for all three fields on each
  named vhost. Production deployments should use narrower patterns.

### `BrokerPassword`

```csharp
public static class BrokerPassword
{
    public static string Hash(string password);
    public static bool Verify(string password, BrokerUser user);
}
```

Hash encoding: `pbkdf2-sha256$<iterations>$<base64 salt>$<base64 hash>` with 210,000
iterations, a 16-byte salt and a 32-byte hash. Verification is fixed-time and refuses
malformed or unsupported hashes. `IsWellFormed` is internal and used by
`BrokerServerOptions.Validate`.

### `BrokerPrincipal`

An authenticated account's resolved permission set. Created by `WireConnection` after
a successful PLAIN handshake; passed to every `ChannelContext`.

```csharp
public sealed class BrokerPrincipal
{
    public BrokerPrincipal(string name, IReadOnlyList<BrokerPermission> permissions);

    public string Name { get; }
    public bool IsKnownVhost(string vhost);
    public bool CanConfigure(string vhost, string name);
    public bool CanWrite(string vhost, string name);
    public bool CanRead(string vhost, string name);
}
```

- Regexes compile once at construction with `RegexOptions.CultureInvariant` and a
  one-second match timeout. An invalid pattern throws `BrokerConfigurationException`;
  a match timeout denies instead of hanging the channel.
- An empty pattern compiles as `\A\z`, so it matches only the empty name
  (effectively deny), not every name.
- The first matching vhost entry wins.

### `BrokerTlsOptions`

```csharp
public sealed class BrokerTlsOptions
{
    public bool Enabled { get; set; }
    public int Port { get; set; } = 5671;
    public string? CertificatePath { get; set; }         // PFX, or PEM certificate
    public string? CertificateKeyPath { get; set; }      // PEM private key
    public string? CertificatePassword { get; set; }     // PFX password
    public bool ClientCertificateRequired { get; set; }
    public string? ClientCertificateAuthorityPath { get; set; }
    public SslProtocols MinTlsVersion { get; set; } = SslProtocols.Tls12;
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
```

- A PFX is loaded with `X509CertificateLoader.LoadPkcs12FromFile`; a PEM certificate
  requires `CertificateKeyPath` and is re-imported through PKCS#12 so the private key
  lives in a platform-usable key set. Windows uses `UserKeySet | Exportable` because
  Schannel rejects ephemeral keys for server authentication; Unix uses
  `EphemeralKeySet`.
- `MinTlsVersion = Tls12` enables TLS 1.2 and 1.3; `Tls13` enables only 1.3.
- With `ClientCertificateRequired`, client certificates must chain to the PEM bundle
  at `ClientCertificateAuthorityPath` (custom root trust, revocation disabled).
- TLS runs **beside** plaintext: the plaintext listener is unaffected when TLS is
  enabled.

### `BrokerConfigurationException`

Thrown when the broker cannot safely start: invalid accounts, malformed hashes,
invalid permission regexes, unreadable/missing certificate material, a PEM without a
key, or (from `RedisStreamRouteEngine`) a Redis eviction policy that could discard
durable data.

### `WireConnection`

One AMQP connection over a transport. Implements `IChannelDeliveryHost`. Not
constructed directly by users — created by `AmqpBrokerServer` for each accepted
transport side.

Key internals (documented for maintainers):

- Connection FSM: `AwaitStartOk` → `AwaitTuneOk` → `AwaitOpen` → `Serving` →
  `ClosingByServer`.
- PLAIN authentication resolves a `BrokerUser` with `BrokerPassword.Verify`, then
  builds a `BrokerPrincipal`.
- `connection.open` checks `BrokerServerOptions.Vhosts` (unknown → 402), then
  `BrokerPrincipal.IsKnownVhost`; an unlisted vhost is a **hard 403 access-refused**
  close (default-deny).
- `_writeGate` (`SemaphoreSlim(1,1)`) serializes every wire write across the frame
  pump and queue pumps.
- One ordered confirm loop per connection. `EnqueueConfirm` blocks the frame pump
  when the bounded pipeline is full; the loop waits on every receipt, then emits
  `basic.return` (mandatory unroutable) before `confirm.ack`/`confirm.nack`.
- `HeartbeatWatchdog` kills at 2× negotiated heartbeat; reset on every inbound frame.
- Connection teardown calls `IRouteEngine.ReconcileConnection(connectionId)`.

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
    internal ChannelContext(ushort channel, string vhost, IRouteEngine engine,
                            IChannelDeliveryHost host, string? connectionId = null,
                            BrokerPrincipal? principal = null);
}
```

Created lazily by `WireConnection.GetOrCreateChannel(channel)`. Owns:

- Permission enforcement (`BrokerPrincipal`; a null principal means full access for
  internal test channels only).
- Prefetch credits (`_prefetchCount`, `_prefetchSize`, `_outstandingBytes`).
- Unacked ledger (`_unacked`: tag → queue + message).
- Consumer registries (`_consumerQueues`, `_consumerEntries`).
- Publisher-confirm state (`_confirmMode`, `_confirmSequence`).
- Flow state (`_flowActive`); `channel.flow(false)` pauses admissions.
- Serialized writer chain (`_writerGate` + job queue, drained by
  `RunWriterChainAsync`).
- Content assembler phase (`_pendingContentPhase`).

### `PendingConfirm`

```csharp
public sealed record PendingConfirm(
    ChannelContext Channel,
    ushort ChannelNumber,
    long Sequence,
    IReadOnlyList<EnqueueReceipt> Receipts,
    ChannelOutcomeBasicReturn? ReturnFirst);
```

One publish outcome waiting on the connection's confirm loop. `Receipts` is empty for
an unroutable publish; `ReturnFirst` carries the mandatory `basic.return` that must
precede the confirm.

### `IChannelDeliveryHost`

Write surface for guest callers (queue pumps). Implemented by `WireConnection`.

```csharp
public interface IChannelDeliveryHost
{
    ValueTask WriteMethodToWireAsync(ushort channel, AmqpMethod method, CancellationToken ct);
    ValueTask WriteContentToWireAsync(ushort channel, ContentProperties properties, byte[] body, CancellationToken ct);
    ValueTask WriteDeliveryToWireAsync(ushort channel, AmqpMethod deliver, ContentProperties properties, byte[] body, CancellationToken ct);
    void EnqueueConfirm(PendingConfirm confirm);
    ValueTask WriteReturnToWireAsync(ushort channel, ChannelOutcomeBasicReturn returned, CancellationToken ct);
}
```

- `WriteMethodToWireAsync` — single gated method frame.
- `WriteContentToWireAsync` — header + body re-chunked per frame-max, one atomic write.
- `WriteDeliveryToWireAsync` — deliver + header + body, one atomic write.
- `EnqueueConfirm` — hands a publish outcome to the ordered confirm loop; blocks
  under deliberate backpressure when full.
- `WriteReturnToWireAsync` — mandatory-unroutable `basic.return`, one atomic write.

### `HeartbeatWatchdog`

Silence policing. Not a timer itself — checked by the connection's timer.

```csharp
public class HeartbeatWatchdog
{
    public HeartbeatWatchdog(TimeSpan heartbeatInterval, Action<HeartbeatWatchdog>? onTimeout = null);
    public bool TimedOut { get; private set; }
    protected virtual DateTimeOffset Now => DateTimeOffset.UtcNow;
    public void OnFrameReceived();
    public void Check();
}
```

- `heartbeatInterval` must be positive; 0 means no watchdog is started.
- Kill fires at `2 × heartbeatInterval` of silence.
- `Now` is virtual for fake-clock tests.

### `ChannelOutcome` family

Abstract record returned by `ChannelContext.OnMethod` / `OnContent`:

| Record | Meaning |
|--------|---------|
| `ChannelOutcomeReply(Reply, RetireAfter)` | Send the method reply; optionally retire after close-ok. |
| `ChannelOutcomeSilent(RetireChannel)` | No wire bytes; optionally free the channel number. |
| `ChannelOutcomeSoftError(Code, Text, ClassId, MethodId)` | Soft channel close (403/404/405/406/505); channel mourns. |
| `ChannelOutcomeHardError(Code, Text, ClassId, MethodId)` | Hard connection close on channel 0. |
| `ChannelOutcomeBasicReturn(ReturnMethod, Properties, Body)` | `basic.return` (312) + full content. |
| `ChannelOutcomeChain` | Consumer-path writes scheduled on the channel's writer chain. |

---

## Memory layer

### `IRouteEngine`

The synchronous seam between wire server and storage. `InMemoryEngine` is the system
of record; `RedisStreamRouteEngine` wraps it and mirrors mutations.

```csharp
public interface IRouteEngine
{
    ExchangeInfo?   GetExchange(string vhost, string name);
    ExchangeInfo?   DeclareExchange(ExchangeInfo exchange);
    QueueInfo?      GetQueue(string vhost, string name);
    QueueInfo?      DeclareQueue(QueueInfo queue);
    void            Bind(string vhost, string exchange, string queue, string routingKey,
                         FieldTable? arguments = null);
    void            Unbind(string vhost, string exchange, string queue, string routingKey);
    IReadOnlyList<string> RouteTo(ExchangeInfo exchange, in ContentProperties properties,
                                  string routingKey);
    EnqueueReceipt  Enqueue(string vhost, string queue, ContentProperties properties,
                            byte[] body, string exchange, string routingKey);
    ConsumeRegisterOutcome RegisterConsumer(string vhost, string queue, IQueueConsumer consumer);
    void            UnregisterConsumer(string vhost, string queue, string consumerTag);
    EngineDelta     Requeue(string vhost, string queue, InMemoryMessage message);
    EngineDelta     Acknowledge(string vhost, string queue, InMemoryMessage message);
    EngineDelta     Reject(string vhost, string queue, InMemoryMessage message);
    EngineDelta     Reap(DateTimeOffset now);
    GetClaimOutcome TryClaimForGet(string vhost, string queue,
                                   out InMemoryMessage? message, out int remaining);
    void            Nudge(string vhost, string queue);
    void            ReconcileConnection(string connectionId);
    void            RegisterExclusiveQueueOwner(string vhost, string queue, string connectionId);
}
```

**Declare contract** (`DeclareExchange` / `DeclareQueue`): creation-only
compare-and-set. On miss, store and return `null`. On hit, return the existing record
unchanged — never replaced. Concurrent first-declares are deterministic: exactly one
caller observes `null`.

**Bind contract**: `arguments` carries the binding arguments for a headers exchange
(`x-match` and header key/value pairs). Duplicate binds are a no-op preserving
insertion order. Routing is binding-insertion order, with `direct` exact match,
`fanout` all bindings, `topic` `*`/`#` wildcards, and `headers` `x-match=all|any`.

**Enqueue contract**: returns an `EnqueueReceipt`; the engine assigns the 1-based
monotonic envelope id and stores a defensive body copy per target. `Durable` completes
only after the durable store accepted the message.

**Requeue/Acknowledge/Reject/Reap** return an `EngineDelta` describing what the mirror
must do. Deltas order enqueues/mutations before removals.

### `EnqueueReceipt`

```csharp
public readonly record struct EnqueueReceipt(
    ulong EnvelopeId,
    ValueTask<bool> Durable,
    IReadOnlyList<ulong> DroppedEnvelopeIds)
{
    public static EnqueueReceipt Immediate(ulong envelopeId,
                                           IReadOnlyList<ulong>? dropped = null);
}
```

- `Durable` completes `true` only after the durable store accepted the message
  (Redis `XADD` returned). A memory-only engine returns an already-completed `true`
  because it makes no durability promise. It completes `false` when the store refused
  or could not write it. Await it at most once.
- `DroppedEnvelopeIds` lists messages evicted by `x-max-length` while landing this
  one.

### `EngineDelta` / `QueuedMessage` / `QueuedMessageRef`

```csharp
public sealed record QueuedMessage(string Vhost, string Queue, InMemoryMessage Message);
public sealed record QueuedMessageRef(string Vhost, string Queue, ulong EnvelopeId);

public sealed record EngineDelta(
    IReadOnlyList<QueuedMessage> Enqueued,
    IReadOnlyList<QueuedMessageRef> Removed,
    IReadOnlyList<(string Vhost, string Name)> DeletedQueues)
{
    public static EngineDelta Empty { get; }
}
```

One mutation expressed as durable-mirror operations. **Ordering contract:** enqueue /
mutate first, remove second — a crash between the two can duplicate a message but never
lose one.

### `InMemoryEngine`

```csharp
public sealed class InMemoryEngine : IRouteEngine
{
    public InMemoryEngine(IReadOnlyList<string> vhosts);
    // predeclares "" (default) and "amq.direct" exchanges per vhost
    public IEnumerable<string> QueueNamesOfVhost(string vhost);     // test introspection
    public IReadOnlyList<InMemoryMessage> PeekQueue(string queue);  // ready-only introspection
}
```

Also implements all `IRouteEngine` members. Thread-safe via a single `_lock`. Owns:

- Ready buffers, consumer registries and round-robin cursors (`InMemoryQueue`).
- Dead-lettering (`Reject` / `Requeue` overflow / expired messages) with `x-death`.
- The `LastUsed` map for `x-expires` idle accounting (in-memory only; restarts reset
  the clock).
- The `Reap(now)` sweep: idle queues first (deleted without dead-lettering), then
  ready-message expiry.
- `x-max-length` drop-oldest on enqueue and dead-letter copies.

### `ExchangeInfo` / `QueueInfo`

```csharp
public sealed record ExchangeInfo(
    string Vhost, string Name, string Type,
    bool Durable = false, bool AutoDelete = false, bool Internal = false);

public sealed record QueueInfo(
    string Vhost, string Name,
    bool Durable = false, bool Exclusive = false, bool AutoDelete = false,
    TimeSpan? Expiration = null,
    int? MaxLength = null,
    string? DeadLetterExchange = null,
    string? DeadLetterRoutingKey = null,
    int? DeliveryLimit = null);
```

- `Expiration` is the `x-expires` **idle duration**, not a deadline.
- `DeadLetterExchange` preserves the absent (`null`, no dead-lettering) versus
  explicitly empty (`""`, the default exchange) distinction.
- `DeliveryLimit` is `x-delivery-limit`.

### `InMemoryMessage`

```csharp
public sealed record InMemoryMessage(
    ContentProperties Props,
    byte[] Body,
    string Exchange,
    string RoutingKey,
    ulong EnvelopeId,
    bool Redelivered = false,
    DateTimeOffset? Expiration = null,
    int DeliveryCount = 0);
```

- `EnvelopeId`: engine-global, monotonic, 1-based. Fan-out to N queues creates N
  messages with N ids.
- `Redelivered`: false on first delivery; set to `true` by `Requeue`.
- `Expiration`: absolute deadline derived from the AMQP relative `expiration`
  property at publish time. Preserved across requeue; cleared on dead-letter copies.
- `DeliveryCount`: incremented at claim time (delivery pump **and** basic.get), so a
  `basic.get` consumes one delivery-limit attempt. Requeue preserves the count and
  dead-letters when the count has reached the queue's limit.
- Body is a defensive copy — no aliasing.

### `InMemoryQueue`

Per-queue delivery state (internal). Ready buffer (FIFO), consumer registry
(registration order), round-robin cursor, wake-driven pump task, and expired-message
collection.

### `IQueueConsumer`

```csharp
public interface IQueueConsumer
{
    string ConsumerTag { get; }
    bool IsExclusive { get; }
    bool TryAdmitForDelivery(InMemoryMessage message);
    Task DrainDeliveryWritesAsync();
}
```

`TryAdmitForDelivery` performs the credit check, ledger registration and writer-job
scheduling synchronously under the engine gate. Returns `true` when admitted; `false`
when the channel has no credit (the message stays queued).

### `ConsumeRegisterOutcome`

| Value | Meaning |
|-------|---------|
| `Registered` | Consumer joined; pump armed. |
| `QueueNotFound` | Queue does not exist in vhost. |
| `ExclusiveHeld` | Exclusive consumer holds queue, or new consumer demands exclusivity on an occupied queue. |

### `GetClaimOutcome`

| Value | Meaning |
|-------|---------|
| `Claimed` | Head message popped; `message` and `remaining` are meaningful. |
| `QueueNotFound` | Queue does not exist. |
| `ExclusiveHeld` | Exclusive consumer holds the queue; basic.get refused. |
| `Empty` | No ready message → get-empty path. |

---

## Redis layer

### `RedisStreamRouteEngine`

```csharp
public sealed class RedisStreamRouteEngine : IRouteEngine, IAsyncDisposable
{
    public RedisStreamRouteEngine(
        IDatabase? redis,
        IReadOnlyList<string> vhosts,
        bool replayOnStart = true,
        int drainerCapacity = 1024,
        bool enforceEvictionGate = true);

    public ValueTask DisposeAsync();
}
```

- `redis == null` → memory-only; no drainer starts and every write stays local.
- `replayOnStart` rebuilds topology and ready queues from Redis before serving.
- `drainerCapacity` bounds the write-behind queue; a full queue means
  `Enqueue` refuses the publish **before** touching memory.
- `enforceEvictionGate` reads `CONFIG GET maxmemory-policy` and throws
  `BrokerConfigurationException` unless it is `noeviction`.
- One background drainer serializes every Redis write. `Enqueue` returns a receipt
  whose `Durable` completes when its `XADD` returns.
- `MirrorDelta` converts `EngineDelta`s into one drainer batch, resolving removed
  envelope ids to stream ids **before** enqueuing so a requeue cannot overwrite the
  old id's map entry.
- Replay collapses crash-window duplicates by envelope id (higher delivery count wins;
  tie → later stream id) and `XDEL`s the stale copies.

### `RedisKeys`

Cluster-safe key layout. Every queue key is hash-tagged so a queue's keys share a
cluster slot; no cross-slot commands are used.

| Member | Key / value |
|--------|-------------|
| `RedisKeys.TopologyKey(vhost)` | `{mq:top:<vhost>}` — declare/bind/unbind/delete log |
| `RedisKeys.QueueKey(vhost, queue)` | `{mq:q:<vhost>:<queue>}` — ready-message stream |
| `RedisKeys.QueueField.*` | `env`, `ex`, `rk`, `body`, `props`, `expires-at`, `delivery-count`, `redelivered` |
| `RedisKeys.TopologyField.*` | `op`, `entity`, `kind`, `vhost`, `name` |

`props` stores the AMQP **wire-form** content properties, not JSON.

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
    public bool ReadBit();
    public DateTimeOffset ReadTimestamp();
    public bool TryReadFieldTable(out FieldTable table);
    internal bool TryReadFieldArray(int depth, out FieldValue[] items);
}
```

All reads throw `AmqpDecodeException` on truncation. Non-bit reads resynchronize the
bit cursor.

### `ByteWriter`

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
    public void WriteShortstr(string value);
    public void WriteLongstr(ReadOnlySpan<byte> value);
    public void WriteRaw(ReadOnlySpan<byte> value);
    public void WriteBits(params bool[] bits);
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

`FieldTable : IReadOnlyDictionary<string, FieldValue>`. Insertion order preserved on
the wire; duplicate key replaces value in place.

### `AmqpFrame` / `FrameDecoder` / `FrameWriter`

```csharp
public sealed record AmqpFrame(byte Type, ushort Channel, ReadOnlyMemory<byte> Payload);

public static class FrameDecoder
{
    public static DecodeMode ReadFrames(ref ReadOnlySequence<byte> buffer,
                                        List<AmqpFrame> frames, uint negotiatedFrameMax);
}

public static class FrameWriter
{
    public static void WriteMethod(ref ByteWriter writer, ushort channel,
                                   ushort classId, ushort methodId, ReadOnlySpan<byte> args);
    public static void WriteContentHeader(ref ByteWriter writer, ushort channel,
                                          ushort classId, long bodySize, ContentProperties properties);
    public static void WriteBody(ref ByteWriter writer, ushort channel,
                                 ReadOnlySpan<byte> body, uint frameMax);
}
```

Constants: `FrameMethod=1`, `FrameHeader=2`, `FrameBody=3`, `FrameHeartbeat=8`,
`FrameEnd=0xCE`, `FrameOverhead=8`, `FrameMinSize=4096`.

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
    public static void Write(ByteWriter writer, ushort classId, long bodySize,
                             ContentProperties properties);
}

public sealed record ContentHeader(ushort ClassId, ushort Weight, long BodySize,
                                  ContentProperties Properties);
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

`MethodSchema(classId, methodId, name, direction, fields)` with `HasContent` init-only
property. `AmqpField(name, wireType)`.

### `AmqpConstants`

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
| `BrokerConfigurationException` | Invalid broker/engine configuration at startup |

---

## Transport layer

### `IListenerTransport`

```csharp
public interface IListenerTransport
{
    int? AnnouncedPort => null;
    ValueTask<TransportSide?> AcceptAsync(CancellationToken cancellationToken = default);
}
```

Null return = listener closed (accept stream ends). `AnnouncedPort` is the concrete
bound port (`null` for portless in-proc transports).

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

- Dual-mode IPv6 bind (`::1` + `127.0.0.1`) with IPv4 fallback (`SocketBinder`).
- Wraps accepted `Socket` in `NetworkStream` → `PipeReader`/`PipeWriter`.

### `TlsSocketListener`

```csharp
public sealed class TlsSocketListener : IListenerTransport, IAsyncDisposable
{
    public TlsSocketListener(BrokerTlsOptions options);
    public int AnnouncedPort { get; }
    public ValueTask<TransportSide?> AcceptAsync(CancellationToken ct = default);
    public ValueTask DisposeAsync();
}
```

- Loads and owns its `BrokerCertificate`; `DisposeAsync` releases both socket and
  certificate.
- A failed, timed-out or mTLS-rejected handshake disposes only that socket; the
  accept stream continues. Only cancellation/listener disposal ends the stream.

### `BrokerCertificate`

```csharp
public sealed class BrokerCertificate : IDisposable
{
    public X509Certificate2 Certificate { get; }
    public static BrokerCertificate Load(BrokerTlsOptions options);
    public SslServerAuthenticationOptions CreateAuthenticationOptions();
    public void Dispose();
}
```

Loads PFX or PEM material, validates that a private key is present, and (for mutual
TLS) imports the CA bundle used to chain client certificates.

### `BrokerListeners`

```csharp
public static class BrokerListeners
{
    public static IReadOnlyList<IListenerTransport> Create(BrokerServerOptions options);
}
```

- Adds `SocketListener(options.Port)` when `options.Port >= 0`.
- Adds `TlsSocketListener(options.Tls)` when `options.Tls is { Enabled: true }`.
- If a later listener fails to bind (for example a bad certificate), already-bound
  listeners are disposed before the exception propagates. The caller owns the
  returned listeners.

---

## Conformance test dependencies

`tests/RedisMQ.Conformance` uses:
- `RabbitMQ.Client` 7.2.2 (real client over TCP/TLS)
- `xunit.v3` 4.0.2, `Microsoft.NET.Test.Sdk` 18.11.0

`tests/RedisMQ.Broker.Tests` uses:
- `xunit.v3` 4.0.2, `Microsoft.NET.Test.Sdk` 18.11.0
- `fixtures/rabbitmq-codegen.json` (canonical AMQP 0-9-1 codegen, committed for offline CI)

`tests/RedisMQ.Redis.Tests` uses:
- `StackExchange.Redis` (via the `RedisMQ.Redis` project)
- A Redis server on `localhost:6379`; tests skip when it is unreachable

## Build requirements

- .NET 10 SDK (`net10.0` target)
- `Nullable=enable`, `LangVersion=latest`, `ImplicitUsings=enable`
- Core project: zero `PackageReference`s
- Test projects: xunit v3 + Test SDK
- Redis project: StackExchange.Redis only
