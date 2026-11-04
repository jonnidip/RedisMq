# RedisMQ v1 Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dependency-free metrics, opt-in traces, structured logs, engine health and a broker health snapshot to RedisMQ v1 without changing the AMQP wire surface.

**Architecture:** A per-instance `BrokerDiagnostics` owns one BCL `Meter`, one `ActivitySource`, a log sink and a shared observable-gauge registry. The server, channel and both engines are instrumented at the points that already own the facts; the Redis engine delegates message-level counters to its inner `InMemoryEngine` and emits only mirror/replay metrics. Hosts own HTTP endpoints and exporters.

**Tech Stack:** .NET 10, C# latest, BCL `System.Diagnostics.Metrics` and `System.Diagnostics.ActivitySource`, xUnit v3 in-process runner, StackExchange.Redis, RabbitMQ.Client conformance fixtures.

**Spec:** `docs/superpowers/specs/2026-11-04-redismq-observability-design.md`

## Global Constraints

- `RedisMQ.Broker` keeps zero `PackageReference`s. Only shared-framework diagnostics types are used.
- `IRouteEngine` stays synchronous. Instrumentation must not await, block, or take the engine gate in a new order.
- Every metric and activity carries `broker.instance = BrokerDiagnostics.InstanceId`; Meter and ActivitySource are both named `RedisMQ.Broker`.
- Hot paths do no work when no listener/sink is attached: guard logs with `IBrokerLogSink.IsEnabled` and metrics with the instrument's `Enabled` property.
- Existing public constructors keep working; new parameters are optional and appended.
- Metric tags never contain credentials, password hashes, message bodies, headers, connection ids or user names. Queue and exchange names are allowed and documented as user-controlled cardinality.
- Publish outcomes are exactly `routed|unroutable|mandatory_return|refused`; confirm outcomes are exactly `ack|nack|timeout`; settlement `kind` is `ack|requeue|reject` and `method` is `ack|nack|reject`.
- Settlement `kind` is the effective action: `nack(requeue=true)` and `reject(requeue=true)` are `kind=requeue`; `nack(requeue=false)` and `reject(requeue=false)` are `kind=reject`.
- A missing DLX is normal discard and is not counted in `redismq.deadletters.dropped`; only configured-but-unusable DLX paths are counted.
- No HTTP endpoints, Prometheus text writer, exporter dependency, `traceparent` propagation or protocol-visible change.
- No new package references in any `src` project.
- All new async test code passes `TestContext.Current.CancellationToken` to token-aware awaits, so the build adds no new `xUnit1051` warnings.

## Review Focus

1. **Activity lifetime under abnormal confirm paths.** A publish span must stop exactly once when the channel is retired before the outcome, when the connection shuts down with entries queued, and when confirm mode is off; a leaked or double-stopped span is the most likely production defect this slice can introduce. Tested in Task 4.
2. **Shared diagnostics with more than one gauge source.** A server plus an engine sharing one `BrokerDiagnostics` must aggregate gauges with identical tags, not silently replace one callback. Tested in Task 1.
3. **Drainer-channel rejection versus Redis failure.** A command that cannot enter the bounded drainer channel must be counted and surfaced in health, but must not flip readiness or masquerade as a Redis outage. Tested in Task 7.
4. **Settlement kind/method mapping.** `basic.nack` and `basic.reject` both have a requeue flag; the metric must report the effective action and the wire verb separately. Tested in Task 5.
5. **Disabled instrumentation.** A disabled sink and no meter listener must not change protocol behavior or emit log work; this is checked in Task 1 and again by the full M0–M4 gate in Task 8.

---

## File Structure

New diagnostics core in `src/RedisMQ.Broker/Diagnostics/`:

| File | Responsibility |
|---|---|
| `BrokerLog.cs` | `BrokerLogLevel`, `BrokerLogEvent`, `BrokerLogEntry`, `IBrokerLogSink`, `NullBrokerLogSink` |
| `BrokerHealth.cs` | `QueueSnapshot`, `EngineHealth`, `BrokerHealth`, `IEngineDiagnostics` |
| `BrokerDiagnostics.cs` | `Meter`, `ActivitySource`, instrument handles, tag helpers, gauge registry, safe log writer, activity starters |

New server/runtime files:

| File | Responsibility |
|---|---|
| `src/RedisMQ.Broker/Server/BrokerRuntimeState.cs` | Internal active connection/channel counters and per-queue unacked counters |
| `tests/RedisMQ.Broker.Tests/Diagnostics/MeterCollector.cs` | `MeterListener` test collector |
| `tests/RedisMQ.Broker.Tests/Diagnostics/ActivityCollector.cs` | `ActivityListener` test collector |
| `tests/RedisMQ.Broker.Tests/Diagnostics/RecordingLogSink.cs` | Capturing/gating test sink |
| `tests/RedisMQ.Broker.Tests/Diagnostics/DiagnosticsWireHarness.cs` | Shared raw-wire harness for diagnostics tests |

Modified production files: `AmqpBrokerServer.cs`, `WireConnection.cs`, `ChannelContext.cs`, `PendingConfirm.cs`, `IChannelDeliveryHost.cs` (no signature change unless required), `InMemoryEngine.cs`, `InMemoryQueue.cs`, `TlsSocketListener.cs`, `BrokerListeners.cs`, `RedisStreamRouteEngine.cs`.

New test files: `BrokerDiagnosticsTests.cs`, `EngineDiagnosticsTests.cs`, `ConnectionDiagnosticsTests.cs`, `PublishDiagnosticsTests.cs`, `DeliveryDiagnosticsTests.cs`, `EngineObservabilityTests.cs`, `ReaperObservabilityTests.cs`, `HealthTests.cs`, `ObservabilityRedisTests.cs`.

---

### Task 1: Diagnostics Core and Test Harnesses

**Files:**
- Create: `src/RedisMQ.Broker/Diagnostics/BrokerLog.cs`
- Create: `src/RedisMQ.Broker/Diagnostics/BrokerHealth.cs`
- Create: `src/RedisMQ.Broker/Diagnostics/BrokerDiagnostics.cs`
- Create: `tests/RedisMQ.Broker.Tests/Diagnostics/MeterCollector.cs`
- Create: `tests/RedisMQ.Broker.Tests/Diagnostics/ActivityCollector.cs`
- Create: `tests/RedisMQ.Broker.Tests/Diagnostics/RecordingLogSink.cs`
- Test: `tests/RedisMQ.Broker.Tests/BrokerDiagnosticsTests.cs`

**Interfaces:**
- Produces: `BrokerDiagnostics(string? instanceId = null, IBrokerLogSink? logSink = null, string? serviceVersion = null)`, `string InstanceId`, `Meter Meter`, `ActivitySource ActivitySource`, `IBrokerLogSink Log`, `void Dispose()`.
- Produces: internal `void Count(Counter<long> counter, TagList tags)`, `void Measure(Histogram<double> histogram, double value, TagList tags)`, `void Measure(Histogram<long> histogram, long value, TagList tags)`, `IDisposable RegisterObservableGauge(string name, string unit, string description, Func<IEnumerable<Measurement<long>>> observe)`, `void WriteLog(BrokerLogLevel level, BrokerLogEvent evt, string message, Exception? exception = null, params KeyValuePair<string, object?>[] data)`, `Activity? StartPublishActivity(string vhost, string exchange, string routingKey, int bodySize)`, `Activity? StartDeliverActivity(string vhost, string queue, long deliveryTag, bool redelivered, int bodySize)`, `static void StopActivity(Activity? activity, string outcome, bool error = false)`.
- Produces: internal instrument handles listed in Step 3.
- Produces: `MeterCollector` with `IReadOnlyList<MeasurementRecord> Measurements`, `IReadOnlyList<MeasurementRecord> For(string instrumentName)`, `IReadOnlyList<MeasurementRecord> ObserveGauges(string instrumentName)`, `void Dispose()`.
- Produces: `ActivityCollector` with `IReadOnlyList<Activity> Stopped`, `void Dispose()`.
- Produces: `RecordingLogSink` with `bool Enabled { get; set; } = true`, `bool ThrowOnLog { get; set; }`, `IReadOnlyList<BrokerLogEntry> Entries`.

- [ ] **Step 1: Write the failing diagnostics tests**

Create `BrokerDiagnosticsTests.cs` with these cases. Each test creates a `MeterCollector`/`ActivityCollector` before measuring and filters by `diagnostics.InstanceId`.

```csharp
[Fact]
public void Counter_carries_the_instance_tag()
{
    using var diag = new BrokerDiagnostics("inst1");
    using var meter = new MeterCollector();
    diag.Count(diag.ReaperSweeps, new TagList());
    MeasurementRecord record = Assert.Single(meter.For("redismq.reaper.sweeps"));
    Assert.Equal(1, record.LongValue);
    Assert.Equal("inst1", record.Tags["broker.instance"]);
}

[Fact]
public void Gauge_callbacks_with_identical_tags_sum_instead_of_replacing()
{
    using var diag = new BrokerDiagnostics("inst-gauge");
    using var meter = new MeterCollector();
    using IDisposable first = diag.RegisterObservableGauge(
        "test.gauge", "{item}", "test", () => [new Measurement<long>(1,
            new TagList { { "vhost", "/" }, { "queue", "q" } })]);
    using IDisposable second = diag.RegisterObservableGauge(
        "test.gauge", "{item}", "test", () => [new Measurement<long>(2,
            new TagList { { "vhost", "/" }, { "queue", "q" } })]);

    MeasurementRecord record = Assert.Single(meter.ObserveGauges("test.gauge"));
    Assert.Equal(3, record.LongValue);
    Assert.Equal("inst-gauge", record.Tags["broker.instance"]);
}

[Fact]
public void Disabled_sink_receives_no_entries()
{
    var sink = new RecordingLogSink { Enabled = false };
    using var diag = new BrokerDiagnostics("inst-log", sink);
    diag.WriteLog(BrokerLogLevel.Warning, BrokerLogEvent.ReaperFailed, "ignored");
    Assert.Empty(sink.Entries);
}

[Fact]
public void Sink_exception_is_swallowed()
{
    var sink = new RecordingLogSink { ThrowOnLog = true };
    using var diag = new BrokerDiagnostics("inst-throw", sink);
    Exception? thrown = Record.Exception(() =>
        diag.WriteLog(BrokerLogLevel.Error, BrokerLogEvent.ReaperFailed, "boom"));
    Assert.Null(thrown);
}

[Fact]
public void Two_instances_remain_separately_attributable()
{
    using var first = new BrokerDiagnostics("a");
    using var second = new BrokerDiagnostics("b");
    using var meter = new MeterCollector();
    first.Count(first.ReaperSweeps, new TagList());
    second.Count(second.ReaperSweeps, new TagList());

    Assert.Equal(2, meter.For("redismq.reaper.sweeps").Count);
    Assert.Contains(meter.For("redismq.reaper.sweeps"), r => r.Tags["broker.instance"] is "a");
    Assert.Contains(meter.For("redismq.reaper.sweeps"), r => r.Tags["broker.instance"] is "b");
}

[Fact]
public void Dispose_is_idempotent()
{
    var diag = new BrokerDiagnostics("dispose");
    diag.Dispose();
    diag.Dispose();
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.BrokerDiagnosticsTests" -noColor
```

Expected: build fails because `RedisMQ.Broker.Diagnostics` types do not exist.

- [ ] **Step 3: Implement the diagnostics core**

Create `BrokerLog.cs` with exactly the public shapes from spec §7.1: `BrokerLogLevel`, `BrokerLogEvent`, `BrokerLogEntry`, `IBrokerLogSink.IsEnabled(BrokerLogLevel)` / `Log(in BrokerLogEntry)`, `NullBrokerLogSink.Instance`.

Create `BrokerHealth.cs` with exactly:

```csharp
public sealed record QueueSnapshot(string Vhost, string Name, int Ready, int Consumers, int Unacked = 0);
public sealed record EngineHealth(
    string Kind, bool Durable, bool? RedisConnected, long DrainerDepth,
    long MirrorFailures, long MirrorDropped, DateTimeOffset? LastMirrorFailureUtc,
    DateTimeOffset? LastMirrorDropUtc, string? LastError);
public sealed record BrokerHealth(
    bool Live, bool Ready, string? Detail, DateTimeOffset Timestamp,
    int ActiveConnections, int ActiveChannels,
    IReadOnlyList<QueueSnapshot> Queues, EngineHealth Engine);
public interface IEngineDiagnostics
{
    EngineHealth GetEngineHealth();
    IReadOnlyList<QueueSnapshot> QueueSnapshots();
}
```

Create `BrokerDiagnostics.cs`:

- Constructor defaults: `instanceId = Guid.NewGuid().ToString("N")[..8]`, `logSink = NullBrokerLogSink.Instance`, `serviceVersion = typeof(BrokerDiagnostics).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0"`.
- `Meter = new Meter("RedisMQ.Broker", serviceVersion)`, `ActivitySource = new ActivitySource("RedisMQ.Broker", serviceVersion)`.
- Create every instrument named in spec §7.2 with the exact unit and description. Use these handle names:

```csharp
internal Counter<long> ConnectionsOpened, ConnectionsClosed, ConnectionsAuthenticated;
internal Counter<long> ChannelsOpened, ChannelsClosed;
internal Counter<long> AuthFailures, VhostRefusals, PermissionDenials;
internal Counter<long> TlsHandshakes;
internal Counter<long> Publishes, Confirms, Deliveries, Settlements;
internal Counter<long> MessagesDropped, MessagesExpired, QueuesExpired;
internal Counter<long> MessagesDeadlettered, DeadLettersDropped;
internal Counter<long> ReaperSweeps, ReaperFailures;
internal Counter<long> RedisMirrorCommands, RedisMirrorDropped, RedisDrainerSaturated;
internal Counter<long> RedisReplayMessages, RedisReplayFailures;
internal Histogram<double> TlsHandshakeDuration, ConfirmDuration, ReaperDuration, RedisMirrorDuration;
internal Histogram<long> PublishSize;
```

- Set histogram advice: seconds boundaries `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`; publish-size boundaries `64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304`.
- `Count` and both `Measure` overloads append `broker.instance` to the passed `TagList`, then call `Add`/`Record`. They are no-ops when the instrument's `Enabled` property is false.
- `WriteLog` checks `Log.IsEnabled(level)` again, constructs the entry and calls `Log.Log(in entry)` inside `try/catch` that swallows sink exceptions.
- `RegisterObservableGauge` creates one `ObservableGauge<long>` per name under a lock, stores `(ObservableGauge<long>, List<Func<...>>)`, and returns a handle that removes its callback and disposes the gauge when the list is empty. The observation callback snapshots the callback list, calls each outside the lock, appends `broker.instance` to each measurement's tags, and sums measurements whose sorted `key=value` tag strings are identical.
- `StartPublishActivity` returns null when `!ActivitySource.HasListeners()`. Otherwise it starts `amqp.publish` (`ActivityKind.Producer`) with tags `messaging.system=rabbitmq`, `messaging.operation=publish`, `messaging.destination.name`, `redismq.vhost`, `messaging.rabbitmq.destination.routing_key`, `redismq.body.size`, `broker.instance`.
- `StartDeliverActivity` returns null when `!ActivitySource.HasListeners()`. Otherwise it starts `amqp.deliver` (`ActivityKind.Consumer`) with tags `messaging.system=rabbitmq`, `messaging.operation=deliver`, `messaging.destination.name`, `redismq.vhost`, `redismq.delivery.tag`, `redismq.redelivered`, `redismq.body.size`, `broker.instance`.
- `StopActivity` sets `redismq.outcome`, sets `ActivityStatusCode.Error` when `error`, and calls `Stop()`; null is a no-op.
- `Dispose` disposes every gauge, then `Meter` and `ActivitySource`, and is idempotent.

Create the three test collectors using only BCL diagnostics types. `MeterCollector` implements `MeterListener` callbacks for `long` and `double`, stores `MeasurementRecord(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags)` with `LongValue => (long)Value`, and `ObserveGauges` pattern-matches `ObservableGauge<long>`/`ObservableCounter<long>`/`ObservableUpDownCounter<long>` (and the double variants) on instruments from `InstrumentPublished`. `ActivityCollector` uses `ActivityListener` with `Sample = AllDataAndRecorded` and records `ActivityStopped`. `RecordingLogSink` implements the interface exactly with `in BrokerLogEntry`.

- [ ] **Step 4: Run the tests to verify they pass**

Run the command from Step 2. Expected: `Total: 6, Failed: 0`.

- [ ] **Step 5: Commit**

```powershell
git add src\RedisMQ.Broker\Diagnostics tests\RedisMQ.Broker.Tests\Diagnostics tests\RedisMQ.Broker.Tests\BrokerDiagnosticsTests.cs
git commit -m "feat(diagnostics): add dependency-free metrics logs and health types"
```

---

### Task 2: Engine Snapshots and Runtime State

**Files:**
- Create: `src/RedisMQ.Broker/Server/BrokerRuntimeState.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryQueue.cs`
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs` (constructor + diagnostics delegation only)
- Test: `tests/RedisMQ.Broker.Tests/EngineDiagnosticsTests.cs`

**Interfaces:**
- Consumes: `BrokerDiagnostics`, `IEngineDiagnostics`, `QueueSnapshot`, `EngineHealth`, `RegisterObservableGauge`.
- Produces: `InMemoryEngine(IReadOnlyList<string> vhosts, BrokerDiagnostics? diagnostics = null)`, `BrokerDiagnostics Diagnostics`, `IDisposable`.
- Produces: `RedisStreamRouteEngine(..., BrokerDiagnostics? diagnostics = null)`, `BrokerDiagnostics Diagnostics`.
- Produces: internal `InMemoryQueue.ConsumerCount`.
- Produces: internal `BrokerRuntimeState` with `int ActiveConnections`, `int ActiveChannels`, `void ConnectionStarted()`, `void ConnectionClosed()`, `void ChannelOpened()`, `void ChannelClosed()`, `void DeliveryClaimed(string vhost, string queue)`, `void DeliverySettled(string vhost, string queue)`, `IReadOnlyList<KeyValuePair<(string Vhost, string Queue), long>> UnackedSnapshot()`, `void PruneUnacked(IReadOnlySet<(string Vhost, string Queue)> liveQueues)`.

- [ ] **Step 1: Write the failing engine diagnostics tests**

Create `EngineDiagnosticsTests.cs`:

```csharp
[Fact]
public void Memory_engine_reports_queues_ready_and_consumers()
{
    using var diag = new BrokerDiagnostics("engine1");
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareQueue(new QueueInfo("/", "q"));
    engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");
    engine.RegisterConsumer("/", "q", new RefusingConsumer("ctag"));

    QueueSnapshot snapshot = Assert.Single(engine.QueueSnapshots());
    Assert.Equal("/", snapshot.Vhost);
    Assert.Equal("q", snapshot.Name);
    Assert.Equal(1, snapshot.Ready);
    Assert.Equal(1, snapshot.Consumers);
    Assert.Equal(0, snapshot.Unacked);
}

[Fact]
public void Memory_engine_health_is_memory_and_not_durable()
{
    using var engine = new InMemoryEngine(["/"], new BrokerDiagnostics("engine2"));
    EngineHealth health = engine.GetEngineHealth();
    Assert.Equal("memory", health.Kind);
    Assert.False(health.Durable);
    Assert.Null(health.RedisConnected);
    Assert.Null(health.LastError);
}

[Fact]
public void Ready_and_consumer_gauges_are_per_queue()
{
    using var diag = new BrokerDiagnostics("engine3");
    using var meter = new MeterCollector();
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareQueue(new QueueInfo("/", "q"));
    engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");

    MeasurementRecord ready = Assert.Single(
        meter.ObserveGauges("redismq.messages.ready").Where(r => r.Tags["broker.instance"] is "engine3" && r.Tags["queue"] is "q"));
    Assert.Equal(1, ready.LongValue);
}

[Fact]
public async Task Redis_engine_without_database_reports_not_durable()
{
    using var diag = new BrokerDiagnostics("engine4");
    await using var engine = new RedisStreamRouteEngine(
        null!, ["/"], replayOnStart: false, diagnostics: diag);
    EngineHealth health = engine.GetEngineHealth();
    Assert.Equal("redis", health.Kind);
    Assert.False(health.Durable);
    Assert.Null(health.RedisConnected);
    Assert.Empty(engine.QueueSnapshots());
}

[Fact]
public void Runtime_state_tracks_counts_and_unacked()
{
    var state = new BrokerRuntimeState();
    state.ConnectionStarted();
    state.ConnectionStarted();
    state.ChannelOpened();
    state.DeliveryClaimed("/", "q");
    Assert.Equal(2, state.ActiveConnections);
    Assert.Equal(1, state.ActiveChannels);
    Assert.Equal(1, Assert.Single(state.UnackedSnapshot()).Value);
    state.DeliverySettled("/", "q");
    state.ConnectionClosed();
    Assert.Equal(1, state.ActiveConnections);
    Assert.Empty(state.UnackedSnapshot());
}

[Fact]
public void Disposing_engine_unregisters_its_gauges()
{
    using var diag = new BrokerDiagnostics("engine5");
    using var meter = new MeterCollector();
    var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareQueue(new QueueInfo("/", "q"));
    engine.Dispose();
    Assert.Empty(meter.ObserveGauges("redismq.messages.ready")
        .Where(r => r.Tags["broker.instance"] is "engine5"));
}
```

Add a private `RefusingConsumer : IQueueConsumer` in the file whose `ConsumerTag` returns the constructor tag, `IsExclusive` is false, `TryAdmitForDelivery` returns false, and `DrainDeliveryWritesAsync` returns `Task.CompletedTask`.

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.EngineDiagnosticsTests" -noColor
```

Expected: build fails because `InMemoryEngine` has no diagnostics constructor and does not implement `IEngineDiagnostics`.

- [ ] **Step 3: Implement engine diagnostics and runtime state**

- Add `ConsumerCount => _consumers.Count` to `InMemoryQueue`.
- `InMemoryEngine` gets `BrokerDiagnostics _diagnostics`, `public BrokerDiagnostics Diagnostics => _diagnostics`, and registers `redismq.messages.ready` and `redismq.consumers` gauges in the constructor. Each callback takes `_lock`, iterates `_queues`, and yields `ReadyCount` / `ConsumerCount` with `vhost` and `queue` tags.
- Implement `GetEngineHealth()` returning `new EngineHealth("memory", false, null, 0, 0, 0, null, null, null)` and `QueueSnapshots()` returning every declared queue with its ready and consumer counts under `_lock`, `Unacked = 0`.
- Implement `IDisposable.Dispose()` by disposing the two gauge handles. It is idempotent and does not dispose the shared diagnostics.
- Add `BrokerRuntimeState` exactly as defined in Interfaces. Connection/channel counters use `Interlocked`; the unacked dictionary uses one private lock and removes entries at zero.
- `RedisStreamRouteEngine` appends `BrokerDiagnostics? diagnostics = null`, creates `_diagnostics` (or a new instance when null), constructs `_memory` as `new InMemoryEngine(_vhosts, _diagnostics)`, exposes `Diagnostics`, implements `IEngineDiagnostics.QueueSnapshots()` by delegation and `GetEngineHealth()` with `Kind="redis"`, `Durable = _redis is not null`, `RedisConnected = _redis?.Multiplexer.IsConnected`, zero counters and null last-error until Task 7 fills them. Call `_memory.Dispose()` in `DisposeAsync`.

- [ ] **Step 4: Run the tests to verify they pass**

Run the command from Step 2. Expected: `Total: 6, Failed: 0`.

- [ ] **Step 5: Run the existing engine suites to prove no regression**

```powershell
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.EngineDeltaTests" -noColor; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.DeadLetterTests" -noColor
```

Expected: both classes with 0 failed.

- [ ] **Step 6: Commit**

```powershell
git add src\RedisMQ.Broker\Server\BrokerRuntimeState.cs src\RedisMQ.Broker\Memory\InMemoryQueue.cs src\RedisMQ.Broker\Memory\InMemoryEngine.cs src\RedisMQ.Redis\RedisStreamRouteEngine.cs tests\RedisMQ.Broker.Tests\EngineDiagnosticsTests.cs
git commit -m "feat(diagnostics): expose engine snapshots and runtime state"
```

---

### Task 3: Connection, Channel, Auth, Vhost and Permission Instrumentation

**Files:**
- Modify: `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Create: `tests/RedisMQ.Broker.Tests/Diagnostics/DiagnosticsWireHarness.cs`
- Test: `tests/RedisMQ.Broker.Tests/ConnectionDiagnosticsTests.cs`

**Interfaces:**
- Consumes: `BrokerDiagnostics`, `BrokerRuntimeState`, instrument handles.
- Produces: `AmqpBrokerServer(..., BrokerDiagnostics? diagnostics = null)` on both constructors; `AmqpBrokerServer.RuntimeState` is internal.
- Produces: public `WireConnection(BrokerServerOptions, IRouteEngine, TransportSide, string? connectionId = null, BrokerDiagnostics? diagnostics = null)` and internal constructor with `BrokerRuntimeState runtime, string listenerKind`.
- Produces: internal `ChannelContext(..., BrokerDiagnostics diagnostics, BrokerRuntimeState runtime)` appended to the existing internal constructor.
- Produces: `ChannelContext.MarkClosed(string reason)` idempotent.
- Produces test harness: `DiagnosticsWireHarness.Start(BrokerServerOptions? options = null, IRouteEngine? engine = null, BrokerDiagnostics? diagnostics = null)` returning `Server`, `Client`, `Diagnostics`, `Options`; `ValueTask DisposeAsync()`.

- [ ] **Step 1: Create the shared wire harness**

Copy the raw-wire harness from `tests/RedisMQ.Broker.Tests/ConfirmPipelineTests.cs`: `NewOptions`, `TransportPairConnectionListener`, `WireClient`, frame builders, `HandshakeAsync`, `OpenConfirmChannelAsync`, `DeclareQueueAsync`, `PublishAsync`, and `ShakeDownAsync`. Put it in `DiagnosticsWireHarness.cs` as internal types. Change `SpawnServer` into `DiagnosticsWireHarness.Start` that accepts an optional engine and diagnostics and passes both to `AmqpBrokerServer`. Add convenience methods on the harness:

```csharp
Task HandshakeAsync(string user = "guest", string password = "guest");
Task OpenChannelAsync(ushort channel = 1);
Task OpenConfirmChannelAsync(ushort channel = 1);
Task DeclareQueueAsync(ushort channel, string queue);
Task DeclareExchangeAsync(ushort channel, string exchange, string type);
Task BindAsync(ushort channel, string queue, string exchange, string routingKey);
Task PublishAsync(ushort channel, string exchange, string routingKey, bool mandatory = false, string body = "m");
Task<AmqpMethod> ReadMethodOnChannelAsync(ushort channel, ushort classId, ushort methodId);
Task<AmqpMethod> ReadMethodAsync(ushort classId, ushort methodId);
Task CloseChannelAsync(ushort channel);
Task CloseConnectionAsync();
```

The existing raw helpers return `Task`/`Task<AmqpMethod>`; keep them internal so every diagnostics test file can use them.

- [ ] **Step 2: Write the failing connection diagnostics tests**

Create `ConnectionDiagnosticsTests.cs`:

```csharp
[Fact]
public async Task Connection_and_channel_lifecycle_are_counted_and_gauged()
{
    using var diag = new BrokerDiagnostics("conn1");
    using var meter = new MeterCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.ReadMethodOnChannelAsync(1, 20, 11);

    Assert.Equal(1, Single(meter, "redismq.connections.opened", "conn1").LongValue);
    Assert.Equal(1, Single(meter, "redismq.connections.authenticated", "conn1").LongValue);
    Assert.Equal(1, Single(meter, "redismq.channels.opened", "conn1").LongValue);
    Assert.Equal(1, Gauge(meter, "redismq.connections.active", "conn1"));
    Assert.Equal(1, Gauge(meter, "redismq.channels.active", "conn1"));

    await harness.CloseChannelAsync(1);
    await harness.CloseConnectionAsync();

    MeasurementRecord closed = Assert.Single(meter.For("redismq.channels.closed")
        .Where(r => r.Tags["broker.instance"] is "conn1"));
    Assert.Equal("client", closed.Tags["reason"]);
    Assert.Equal("client", Assert.Single(meter.For("redismq.connections.closed")
        .Where(r => r.Tags["broker.instance"] is "conn1")).Tags["reason"]);
    Assert.Equal(0, Gauge(meter, "redismq.connections.active", "conn1"));
}

[Fact]
public async Task Authentication_failure_classifies_reason_and_never_logs_the_password()
{
    var sink = new RecordingLogSink();
    using var diag = new BrokerDiagnostics("auth1", sink);
    using var meter = new MeterCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await Assert.ThrowsAnyAsync<Exception>(() => harness.HandshakeAsync("guest", "wrong-secret"));

    MeasurementRecord failure = Assert.Single(meter.For("redismq.auth.failures")
        .Where(r => r.Tags["broker.instance"] is "auth1"));
    Assert.Equal("bad_credentials", failure.Tags["reason"]);
    BrokerLogEntry entry = Assert.Single(sink.Entries.Where(e => e.Event == BrokerLogEvent.AuthenticationFailed));
    Assert.Equal(BrokerLogLevel.Warning, entry.Level);
    Assert.DoesNotContain("wrong-secret", entry.Message, StringComparison.Ordinal);
    Assert.DoesNotContain("guest", entry.Message, StringComparison.Ordinal);
}

[Fact]
public async Task Vhost_refusal_counts_unknown_and_denied()
{
    using var diag = new BrokerDiagnostics("vhost1");
    using var meter = new MeterCollector();
    await using var unknown = DiagnosticsWireHarness.Start(diagnostics: diag);
    await unknown.HandshakeAsync();
    await Assert.ThrowsAnyAsync<Exception>(() => unknown.OpenVhostAsync("/missing"));

    await using var denied = DiagnosticsWireHarness.Start(
        options: Options(user: new BrokerUser("u", "p")),
        diagnostics: diag);
    await denied.HandshakeAsync("u", "p");
    await Assert.ThrowsAnyAsync<Exception>(() => denied.OpenVhostAsync("/"));

    string[] reasons = [.. meter.For("redismq.vhost.refusals")
        .Where(r => r.Tags["broker.instance"] is "vhost1")
        .Select(r => (string)r.Tags["reason"]!)];
    Assert.Contains("unknown", reasons);
    Assert.Contains("denied", reasons);
}

[Fact]
public async Task Permission_denials_are_tagged_by_operation()
{
    using var diag = new BrokerDiagnostics("perm1");
    using var meter = new MeterCollector();
    await using var harness = DiagnosticsWireHarness.Start(
        options: Options(user: new BrokerUser("u", "p",
            Permissions: [new BrokerPermission("/", "allowed.*", "allowed.*", "allowed.*")])),
        diagnostics: diag);
    await harness.HandshakeAsync("u", "p");
    await harness.OpenChannelAsync();

    await harness.SendQueueDeclareAsync(1, "denied");
    await harness.ReadMethodOnChannelAsync(1, 20, 40); // channel.close (configure)
    await harness.CloseChannelAsync(1);
    await harness.OpenChannelAsync(1);
    await harness.PublishAsync(1, "", "denied");
    await harness.ReadMethodOnChannelAsync(1, 20, 40); // channel.close (write)
    await harness.CloseChannelAsync(1);
    await harness.OpenChannelAsync(1);
    await harness.SendBasicGetAsync(1, "denied");
    await harness.ReadMethodOnChannelAsync(1, 20, 40); // channel.close (read)

    string[] operations = [.. meter.For("redismq.permission.denials")
        .Where(r => r.Tags["broker.instance"] is "perm1")
        .Select(r => (string)r.Tags["operation"]!)];
    Assert.Contains("configure", operations);
    Assert.Contains("write", operations);
    Assert.Contains("read", operations);
}
```

Add local helpers `Single`, `Gauge`, `Options` and harness helpers `OpenVhostAsync`, `SendQueueDeclareAsync`, `SendBasicGetAsync` in the test/harness files. `HandshakeAsync` stops after sending `connection.open`; `OpenVhostAsync` sends `connection.open` and reads `connection.open-ok` or the `connection.close` refusal.

- [ ] **Step 3: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConnectionDiagnosticsTests" -noColor
```

Expected: build fails because the diagnostics constructors and `GetHealth`-adjacent state do not exist.

- [ ] **Step 4: Implement connection and channel instrumentation**

- `AmqpBrokerServer`: add `_diagnostics` (own instance when null) and `_runtime = new BrokerRuntimeState()`. Both public constructors append `BrokerDiagnostics? diagnostics = null`; the single-listener overload forwards it.
- Register `redismq.connections.active`, `redismq.channels.active` and `redismq.messages.unacked` gauges in `StartAsync`, store handles in `_gaugeHandles`, and dispose them in `StopAsync`. Unacked callbacks read `_runtime.UnackedSnapshot()`.
- `RunAcceptLoopAsync`: wrap `AcceptAsync` in a general `catch (Exception ex)` that writes an Error `ListenerFailed` log and sets `_acceptFaulted = 1`, then returns.
- `TrackConnection`: take the listener kind (`tls` for `TlsSocketListener`, otherwise `plaintext`), call `_runtime.ConnectionStarted()`, and `Count(ConnectionsOpened, tags { listener })`.
- `RunConnectionAsync`: construct `WireConnection` with the internal constructor carrying `_runtime`, `_listenerKind` and `_diagnostics`.
- `WireConnection`: public constructor creates a private `BrokerRuntimeState` and uses `"plaintext"`, then delegates to the internal constructor. In `RunAsync`'s `finally`, after connection cleanup and before `DisposeAsync`, call `_runtime.ConnectionClosed()` and `Count(ConnectionsClosed, tags { reason = _closeReason })`, and write `ConnectionClosed` at Information unless `_closeReason` is `auth_refused`/`vhost_refused`/`heartbeat`/`error` (those already logged a more specific event).
- Track `_closeReason` with values `client`, `server`, `error`, `auth_refused`, `vhost_refused`, `heartbeat`. Set it in: `OnClientCloseAsync` (`client`); `CloseConnectionAsync` (`server`); auth refusals (`auth_refused`); vhost refusals (`vhost_refused`); heartbeat catch (`heartbeat`); `AmqpProtocolException`/`AmqpDecodeException`/`AmqpEncodeException`/`IOException`/generic cancellation not caused by server shutdown (`error`).
- Authentication: classify `malformed`, `unknown_user` and `bad_credentials`. `malformed` covers a non-PLAIN mechanism or a non-three-part PLAIN response; `unknown_user` means no configured user has the name; `bad_credentials` means the name exists but verification failed. Count `AuthFailures` with `reason`; write `AuthenticationFailed` at Warning with no username or password in the message (reason in `Data` only).
- Vhost: on the unknown-vhost branch count `VhostRefusals` `reason=unknown`; on the permission branch count `reason=denied`; write `VhostAccessDenied` at Warning with the vhost in `Data`.
- Channels: add `BrokerDiagnostics _diagnostics`, `BrokerRuntimeState _runtime`, and `bool _closed` to `ChannelContext`. On the first successful `channel.open`, call `_runtime.ChannelOpened()`, count `ChannelsOpened` with `vhost`, write `ChannelOpened` at Debug, and set a flag so a repeated open cannot double-count. Implement `MarkClosed(string reason)`: if already closed return; otherwise count `ChannelsClosed` with `vhost` and `reason`, call `_runtime.ChannelClosed()`, and write `ChannelClosed` at Debug. `WireConnection.RetireChannelAsync` calls `MarkClosed("client")` when the retire came from a client close-ok, `"server"` from a server close-ok, and the hard-error branch calls `MarkClosed("server")`. In `RunAsync`'s finally, call `MarkClosed("connection")` on every remaining context.
- Permissions: change `PermissionRefused` to `PermissionRefused(string operation, string kind, string name, AmqpMethod method)` and update every call site to pass `configure`, `write` or `read`. Count `PermissionDenials` with `operation` and write `PermissionDenied` at Warning with `operation`, `kind` and `name` in `Data`. Do not log the user name.
- `WireConnection.GetOrCreateChannel` passes both `_diagnostics` and `_runtime` to the internal `ChannelContext` constructor.

- [ ] **Step 5: Run the tests to verify they pass**

Run the command from Step 3. Expected: `Total: 4, Failed: 0`.

- [ ] **Step 6: Run the existing permission and connection suites**

```powershell
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.PermissionWireTests" -noColor; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConnectionFsmTests" -noColor
```

Expected: both classes with 0 failed.

- [ ] **Step 7: Commit**

```powershell
git add src\RedisMQ.Broker\Server tests\RedisMQ.Broker.Tests\Diagnostics\DiagnosticsWireHarness.cs tests\RedisMQ.Broker.Tests\ConnectionDiagnosticsTests.cs
git commit -m "feat(diagnostics): instrument connections channels auth and permissions"
```

---

### Task 4: Publish Outcomes, Confirms and Publish Tracing

**Files:**
- Modify: `src/RedisMQ.Broker/Server/PendingConfirm.cs`
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Test: `tests/RedisMQ.Broker.Tests/PublishDiagnosticsTests.cs`

**Interfaces:**
- Consumes: `BrokerDiagnostics.StartPublishActivity`, `StopActivity`, `Publishes`, `PublishSize`, `Confirms`, `ConfirmDuration`.
- Produces: `PendingConfirm(..., ChannelOutcomeBasicReturn? ReturnFirst, long StartedTimestamp = 0, Activity? Activity = null)`.
- Produces: confirm-loop ownership: every admitted entry's activity is stopped exactly once in the loop's `finally`; `EnqueueConfirm` stops an entry it cannot admit.

- [ ] **Step 1: Write the failing publish/confirm tests**

Create `PublishDiagnosticsTests.cs`:

```csharp
[Fact]
public async Task Publish_outcomes_and_size_are_counted_and_spans_stop()
{
    using var diag = new BrokerDiagnostics("pub1");
    using var meter = new MeterCollector();
    using var activities = new ActivityCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareExchangeAsync(1, "ex", "direct");
    await harness.DeclareQueueAsync(1, "q");
    await harness.BindAsync(1, "q", "ex", "rk");

    await harness.PublishAsync(1, "ex", "rk", body: "abc");
    await harness.PublishAsync(1, "ex", "missing", body: "de");
    await harness.PublishAsync(1, "ex", "missing", mandatory: true, body: "f");

    string[] outcomes = [.. meter.For("redismq.publishes")
        .Where(r => r.Tags["broker.instance"] is "pub1")
        .Select(r => (string)r.Tags["outcome"]!)];
    Assert.Equal(["routed", "unroutable", "mandatory_return"], outcomes);
    Assert.Equal(3, meter.For("redismq.publish.size")
        .Count(r => r.Tags["broker.instance"] is "pub1"));
    Assert.Equal(3, activities.Stopped.Count(a => a.OperationName == "amqp.publish" && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub1")));
}

[Fact]
public async Task Confirm_ack_counts_duration_and_closes_the_span()
{
    using var diag = new BrokerDiagnostics("pub2");
    using var meter = new MeterCollector();
    using var activities = new ActivityCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenConfirmChannelAsync();
    await harness.DeclareQueueAsync(1, "q");

    await harness.PublishAsync(1, "", "q");
    await harness.ReadMethodOnChannelAsync(1, 60, 80); // confirm.ack

    MeasurementRecord confirm = Assert.Single(meter.For("redismq.confirms")
        .Where(r => r.Tags["broker.instance"] is "pub2"));
    Assert.Equal("ack", confirm.Tags["outcome"]);
    Assert.Single(meter.For("redismq.confirm.duration")
        .Where(r => r.Tags["broker.instance"] is "pub2"));
    Activity span = Assert.Single(activities.Stopped.Where(a => a.OperationName == "amqp.publish"
        && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub2")));
    Assert.Equal("ack", span.GetTagItem("redismq.outcome"));
    Assert.Equal("routed", span.GetTagItem("redismq.publish.outcome"));
}

[Fact]
public async Task Refused_publish_is_refused_and_confirm_nacks()
{
    using var diag = new BrokerDiagnostics("pub3");
    using var meter = new MeterCollector();
    var engine = new RecordingEngine(["/"]) { EnqueueOverride = (_, _, _, _, _, _) =>
        new EnqueueReceipt(0, ValueTask.FromResult(false), []) };
    await using var harness = DiagnosticsWireHarness.Start(engine: engine, diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenConfirmChannelAsync();
    await harness.DeclareQueueAsync(1, "q");

    await harness.PublishAsync(1, "", "q");
    await harness.ReadMethodOnChannelAsync(1, 60, 120); // confirm.nack

    Assert.Equal("refused", Assert.Single(meter.For("redismq.publishes")
        .Where(r => r.Tags["broker.instance"] is "pub3")).Tags["outcome"]);
    Assert.Equal("nack", Assert.Single(meter.For("redismq.confirms")
        .Where(r => r.Tags["broker.instance"] is "pub3")).Tags["outcome"]);
}

[Fact]
public async Task Confirm_timeout_is_counted_and_closes_the_span()
{
    using var diag = new BrokerDiagnostics("pub4");
    using var meter = new MeterCollector();
    using var activities = new ActivityCollector();
    var engine = new RecordingEngine(["/"]);
    var never = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    engine.EnqueueOverride = (_, _, _, _, _, _) => new EnqueueReceipt(1, new ValueTask<bool>(never.Task), []);
    BrokerServerOptions options = DiagnosticsWireHarness.NewOptions();
    options.ConfirmTimeout = TimeSpan.FromMilliseconds(50);
    await using var harness = DiagnosticsWireHarness.Start(options: options, engine: engine, diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenConfirmChannelAsync();
    await harness.DeclareQueueAsync(1, "q");

    await harness.PublishAsync(1, "", "q");
    await harness.ReadMethodOnChannelAsync(1, 60, 120);

    Assert.Equal("timeout", Assert.Single(meter.For("redismq.confirms")
        .Where(r => r.Tags["broker.instance"] is "pub4")).Tags["outcome"]);
    Activity span = Assert.Single(activities.Stopped.Where(a => a.OperationName == "amqp.publish"
        && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub4")));
    Assert.Equal(ActivityStatusCode.Error, span.Status);
}

[Fact]
public async Task Confirm_span_stops_when_channel_retires_before_the_outcome()
{
    using var diag = new BrokerDiagnostics("pub5");
    using var activities = new ActivityCollector();
    var engine = new RecordingEngine(["/"]);
    var receipt = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    engine.EnqueueOverride = (_, _, _, _, _, _) => new EnqueueReceipt(1, new ValueTask<bool>(receipt.Task), []);
    await using var harness = DiagnosticsWireHarness.Start(engine: engine, diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenConfirmChannelAsync();
    await harness.DeclareQueueAsync(1, "q");

    await harness.PublishAsync(1, "", "q");
    await harness.CloseChannelAsync(1);
    receipt.SetResult(false);

    Activity span = Assert.Single(activities.Stopped.Where(a => a.OperationName == "amqp.publish"
        && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub5")));
    Assert.Equal("dropped", span.GetTagItem("redismq.outcome"));
}

[Fact]
public async Task No_confirm_publish_span_ends_at_admission()
{
    using var diag = new BrokerDiagnostics("pub6");
    using var activities = new ActivityCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareQueueAsync(1, "q");

    await harness.PublishAsync(1, "", "q");

    Activity span = Assert.Single(activities.Stopped.Where(a => a.OperationName == "amqp.publish"
        && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "pub6")));
    Assert.Equal("routed", span.GetTagItem("redismq.outcome"));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.PublishDiagnosticsTests" -noColor
```

Expected: build fails because `PendingConfirm` has no activity/timestamp fields and no publish metrics are emitted.

- [ ] **Step 3: Implement publish and confirm instrumentation**

- Add `using System.Diagnostics;` and the two optional properties to `PendingConfirm`.
- `ChannelContext.FinishPublish`: start one publish activity at entry; set `redismq.publish.outcome` on it for `refused` (immediate, permission, exchange-not-found), `unroutable`, `mandatory_return`, or `routed`. Count `Publishes` once with `vhost`, `exchange` (when non-empty), `exchange_type` (when the exchange was found) and `outcome`; record `PublishSize` in bytes. Treat any `EnqueueReceipt.EnvelopeId == 0` as `refused`.
- No confirm mode: call `StopActivity(activity, outcome, error: outcome == "refused")` before returning.
- Confirm mode: create `PendingConfirm(... StartedTimestamp: Stopwatch.GetTimestamp(), Activity: activity)`. If `EnqueueConfirm` cannot admit the entry, `WireConnection` stops the activity with `dropped`; `ChannelContext` does not stop it.
- `WireConnection.EnqueueConfirm`: stop the activity with `dropped` when slots/queue are null, when the slot wait is cancelled, or when `TryWrite` returns false.
- Refactor `ProcessConfirmAsync` to return a `ConfirmOutcome` (`Ack`, `Nack`, `Timeout`, `Dropped`) rather than writing metrics inside. After the liveness check it schedules the return/confirm writes, counts `Confirms` with `ack|nack|timeout`, records `ConfirmDuration` in seconds, sets the activity's `redismq.outcome` to the same value, and flushes the chain. A stale channel returns `Dropped` before emitting confirm metrics.
- `RunConfirmLoopAsync`: call `StopActivity(confirm.Activity, outcome, error: outcome is "nack" or "timeout")` in a `finally` for every entry. In an outer `finally`, drain any entries left in the reader after cancellation and stop their activities with `dropped`. Preserve the existing slot release and cancellation behavior.
- Write `ConfirmNack`/`ConfirmTimeout` at Warning with the sequence in `Data`; do not log message bodies.
- `WireConnection.DisposeAsync` already cancels and completes the queue; no new shutdown path is needed beyond the loop drain.

- [ ] **Step 4: Run the tests to verify they pass**

Run the command from Step 2. Expected: `Total: 6, Failed: 0`.

- [ ] **Step 5: Run the existing confirm suite**

```powershell
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConfirmPipelineTests" -noColor
```

Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```powershell
git add src\RedisMQ.Broker\Server\PendingConfirm.cs src\RedisMQ.Broker\Server\ChannelContext.cs src\RedisMQ.Broker\Server\WireConnection.cs tests\RedisMQ.Broker.Tests\PublishDiagnosticsTests.cs
git commit -m "feat(diagnostics): instrument publishes confirms and publish spans"
```

---

### Task 5: Deliveries, Settlements, Deliver Tracing and Unacked Gauges

**Files:**
- Modify: `src/RedisMQ.Broker/Server/ChannelContext.cs`
- Modify: `src/RedisMQ.Broker/Server/WireConnection.cs`
- Test: `tests/RedisMQ.Broker.Tests/DeliveryDiagnosticsTests.cs`

**Interfaces:**
- Consumes: `Deliveries`, `Settlements`, `StartDeliverActivity`, `StopActivity`, `BrokerRuntimeState`.
- Produces: deliveries with `auto_ack`, settlements with `kind`/`method`, unacked gauge updates on claim/settle/teardown/recover, `amqp.deliver` spans ending after the frame write.

- [ ] **Step 1: Write the failing delivery tests**

Create `DeliveryDiagnosticsTests.cs`:

```csharp
[Fact]
public async Task Settlements_report_effective_kind_and_wire_method()
{
    using var diag = new BrokerDiagnostics("del1");
    using var meter = new MeterCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareQueueAsync(1, "q");
    foreach (string body in new[] { "a", "b", "c", "d", "e" })
    {
        await harness.PublishAsync(1, "", "q", body: body);
    }

    foreach (string body in new[] { "a", "b", "c", "d", "e" })
    {
        await harness.BasicGetAsync(1, "q", noAck: false);
    }

    await harness.SendBasicAckAsync(1, 1);
    await harness.SendBasicNackAsync(1, 2, requeue: false);
    await harness.SendBasicNackAsync(1, 3, requeue: true);
    await harness.SendBasicRejectAsync(1, 4, requeue: false);
    await harness.SendBasicRejectAsync(1, 5, requeue: true);

    (string Kind, string Method)[] rows = [.. meter.For("redismq.settlements")
        .Where(r => r.Tags["broker.instance"] is "del1")
        .Select(r => ((string)r.Tags["kind"]!, (string)r.Tags["method"]!))];
    Assert.Contains(("ack", "ack"), rows);
    Assert.Contains(("reject", "nack"), rows);
    Assert.Contains(("requeue", "nack"), rows);
    Assert.Contains(("reject", "reject"), rows);
    Assert.Contains(("requeue", "reject"), rows);
}

[Fact]
public async Task Deliveries_mark_auto_ack_and_spans_stop_after_the_write()
{
    using var diag = new BrokerDiagnostics("del2");
    using var meter = new MeterCollector();
    using var activities = new ActivityCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareQueueAsync(1, "q");
    await harness.PublishAsync(1, "", "q");
    await harness.BasicConsumeAsync(1, "q", noAck: true);
    await harness.ReadDeliveryAsync(1);

    MeasurementRecord delivery = Assert.Single(meter.For("redismq.deliveries")
        .Where(r => r.Tags["broker.instance"] is "del2"));
    Assert.Equal("true", delivery.Tags["auto_ack"]);
    Assert.Single(activities.Stopped.Where(a => a.OperationName == "amqp.deliver"
        && a.TagObjects.Any(t => t.Key == "broker.instance" && (string?)t.Value == "del2")));
    Assert.Empty(meter.For("redismq.settlements").Where(r => r.Tags["broker.instance"] is "del2"));
}

[Fact]
public async Task Unacked_gauge_rises_and_falls_with_manual_delivery()
{
    using var diag = new BrokerDiagnostics("del3");
    using var meter = new MeterCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareQueueAsync(1, "q");
    await harness.PublishAsync(1, "", "q");
    await harness.BasicGetAsync(1, "q", noAck: false);

    Assert.Equal(1, Gauge(meter, "redismq.messages.unacked", "del3"));
    await harness.SendBasicAckAsync(1, 1);
    Assert.Equal(0, Gauge(meter, "redismq.messages.unacked", "del3"));
}

[Fact]
public async Task Channel_teardown_requeues_and_clears_unacked()
{
    using var diag = new BrokerDiagnostics("del4");
    using var meter = new MeterCollector();
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareQueueAsync(1, "q");
    await harness.PublishAsync(1, "", "q");
    await harness.BasicGetAsync(1, "q", noAck: false);
    await harness.CloseChannelAsync(1);

    Assert.Equal(0, Gauge(meter, "redismq.messages.unacked", "del4"));
}
```

Add harness helpers `BasicGetAsync` (sends `basic.get`, reads get-ok and consumes the content frames), `BasicConsumeAsync`, `ReadDeliveryAsync`, `SendBasicAckAsync`, `SendBasicNackAsync`, `SendBasicRejectAsync`.

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.DeliveryDiagnosticsTests" -noColor
```

Expected: build fails because the delivery/settlement metrics and runtime updates do not exist.

- [ ] **Step 3: Implement delivery and settlement instrumentation**

- `ConsumerEntry.TryAdmitForDelivery`: after scheduling the delivery write, count `Deliveries` with `vhost`, `queue`, `auto_ack` (`"true"`/`"false"`). Start a deliver activity and wrap the write job in `try/catch/finally`: on exception write `DeliveryFailed` at Warning and rethrow; in `finally` stop the activity with `delivered` (or `error` on exception).
- Manual consumers: after adding the ledger entry (outside `_deliveryLock`), call `_runtime.DeliveryClaimed(_vhost, queueName)`.
- `OnBasicGet`: count `Deliveries` with `auto_ack`; start a deliver activity and combine the get-ok and content writes into one writer job that stops the activity after the content write; manual gets call `_runtime.DeliveryClaimed`; no-ack gets do not.
- `SettleLedger`: add a `string method` parameter (`"ack"`, `"nack"`, `"reject"` from the three callers). After the ledger/engine work, for each settled entry count `Settlements` with `vhost`, `queue`, `kind` (`action.ToString().ToLowerInvariant()`), `method`, and call `_runtime.DeliverySettled(entry.Queue)`.
- `OnBasicRecover`: after requeueing the plan, call `_runtime.DeliverySettled` for every plan entry. Do not emit `Settlements` (recover is not one of the three settle verbs in the catalog).
- `TearDownActiveConsumers`: under `_deliveryLock`, snapshot `_unacked`, clear the dictionary and `_outstandingBytes` so a later settle cannot double-decrement; after releasing the lock, call `_runtime.DeliverySettled` for every entry.
- `WireConnection.GetOrCreateChannel` already passes the runtime from Task 3.

- [ ] **Step 4: Run the tests to verify they pass**

Run the command from Step 2. Expected: `Total: 4, Failed: 0`.

- [ ] **Step 5: Run the existing consume and settle suites**

```powershell
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ConsumeTests" -noColor; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.SettleMirroringTests" -noColor
```

Expected: both classes with 0 failed.

- [ ] **Step 6: Commit**

```powershell
git add src\RedisMQ.Broker\Server\ChannelContext.cs src\RedisMQ.Broker\Server\WireConnection.cs tests\RedisMQ.Broker.Tests\Diagnostics\DiagnosticsWireHarness.cs tests\RedisMQ.Broker.Tests\DeliveryDiagnosticsTests.cs
git commit -m "feat(diagnostics): instrument deliveries settlements and unacked state"
```

---

### Task 6: Engine Drops, Expiry, Dead-Lettering and Reaper Resilience

**Files:**
- Modify: `src/RedisMQ.Broker/Memory/InMemoryEngine.cs`
- Modify: `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Test: `tests/RedisMQ.Broker.Tests/EngineObservabilityTests.cs`
- Test: `tests/RedisMQ.Broker.Tests/ReaperObservabilityTests.cs`

**Interfaces:**
- Consumes: `MessagesDropped`, `MessagesExpired`, `QueuesExpired`, `MessagesDeadlettered`, `DeadLettersDropped`, `ReaperSweeps`, `ReaperDuration`, `ReaperFailures`.
- Produces: internal `DeadLetterDisposition` enum with values `NoDeadLetterExchange`, `Copied`, `MissingExchange`, `Unroutable`, `LoopGuard`.
- Produces: reaper loop that catches every `Reap` exception, counts it, logs `ReaperFailed` at Error, and continues.

- [ ] **Step 1: Write the failing engine metric tests**

Create `EngineObservabilityTests.cs`:

```csharp
[Fact]
public void Max_length_drop_is_counted()
{
    using var diag = new BrokerDiagnostics("eng1");
    using var meter = new MeterCollector();
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareQueue(new QueueInfo("/", "q", MaxLength: 1));
    engine.Enqueue("/", "q", new ContentProperties(), [1], "", "q");
    engine.Enqueue("/", "q", new ContentProperties(), [2], "", "q");

    MeasurementRecord dropped = Assert.Single(meter.For("redismq.messages.dropped")
        .Where(r => r.Tags["broker.instance"] is "eng1"));
    Assert.Equal("max_length", dropped.Tags["reason"]);
    Assert.Equal("q", dropped.Tags["queue"]);
}

[Fact]
public void Ttl_expiry_is_counted_and_dead_lettered_when_configured()
{
    using var diag = new BrokerDiagnostics("eng2");
    using var meter = new MeterCollector();
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareExchange(new ExchangeInfo("/", "dlx", "fanout"));
    engine.DeclareQueue(new QueueInfo("/", "dead"));
    engine.Bind("/", "dlx", "dead", "");
    engine.DeclareQueue(new QueueInfo("/", "q", DeadLetterExchange: "dlx"));
    engine.Enqueue("/", "q", new ContentProperties(Expiration: "0"), [1], "", "q");

    engine.Reap(DateTimeOffset.UtcNow.AddSeconds(1));

    Assert.Equal(1, meter.For("redismq.messages.expired")
        .Count(r => r.Tags["broker.instance"] is "eng2"));
    Assert.Equal("expired", Assert.Single(meter.For("redismq.messages.deadlettered")
        .Where(r => r.Tags["broker.instance"] is "eng2")).Tags["reason"]);
}

[Fact]
public void Delivery_limit_requeue_uses_delivery_limit_metric_reason()
{
    using var diag = new BrokerDiagnostics("eng3");
    using var meter = new MeterCollector();
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareExchange(new ExchangeInfo("/", "dlx", "fanout"));
    engine.DeclareQueue(new QueueInfo("/", "dead"));
    engine.Bind("/", "dlx", "dead", "");
    engine.DeclareQueue(new QueueInfo("/", "q", DeadLetterExchange: "dlx", DeliveryLimit: 1));
    var message = new InMemoryMessage(new ContentProperties(), [1], "", "q", 7, DeliveryCount: 1);

    engine.Requeue("/", "q", message);

    MeasurementRecord dead = Assert.Single(meter.For("redismq.messages.deadlettered")
        .Where(r => r.Tags["broker.instance"] is "eng3"));
    Assert.Equal("delivery_limit", dead.Tags["reason"]);
}

[Fact]
public void Configured_but_unusable_dlx_counts_as_deadletters_dropped()
{
    using var diag = new BrokerDiagnostics("eng4");
    using var meter = new MeterCollector();
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareQueue(new QueueInfo("/", "missing", DeadLetterExchange: "nope"));
    engine.Enqueue("/", "missing", new ContentProperties(), [1], "", "missing");
    var msg = Assert.Single(engine.PeekQueue("missing"));
    engine.Reject("/", "missing", msg);

    Assert.Equal("missing_exchange", Assert.Single(meter.For("redismq.deadletters.dropped")
        .Where(r => r.Tags["broker.instance"] is "eng4")).Tags["reason"]);
}

[Fact]
public void Queue_expiry_is_counted_and_logged()
{
    var sink = new RecordingLogSink();
    using var diag = new BrokerDiagnostics("eng5", sink);
    using var meter = new MeterCollector();
    using var engine = new InMemoryEngine(["/"], diag);
    engine.DeclareQueue(new QueueInfo("/", "idle", Expiration: TimeSpan.Zero));

    engine.Reap(DateTimeOffset.UtcNow.AddSeconds(1));

    Assert.Equal(1, meter.For("redismq.queues.expired")
        .Count(r => r.Tags["broker.instance"] is "eng5"));
    Assert.Contains(sink.Entries, e => e.Event == BrokerLogEvent.QueueExpired && e.Level == BrokerLogLevel.Warning);
}
```

Add a loop-guard case to the same class: enqueue into a queue whose message already carries an `x-death` entry for `(queue, rejected)` and whose DLX is configured; reject it and assert `deadletters.dropped` reason `loop_guard`.

Create `ReaperObservabilityTests.cs` with a `FlakyReapEngine` that delegates every member to `InMemoryEngine` but throws on the first `Reap` and increments `ReapCalls` on every call:

```csharp
[Fact]
public async Task A_throwing_reap_is_counted_logged_and_the_next_tick_runs()
{
    var sink = new RecordingLogSink();
    using var diag = new BrokerDiagnostics("reap1", sink);
    using var meter = new MeterCollector();
    var engine = new FlakyReapEngine(["/"]);
    var options = new BrokerServerOptions { Vhosts = ["/"], ReaperInterval = TimeSpan.FromMilliseconds(20) };
    await using var server = new AmqpBrokerServer(options, engine, diagnostics: diag);
    await server.StartAsync();
    try
    {
        await WaitUntilAsync(() => engine.ReapCalls >= 2, TimeSpan.FromSeconds(3));
    }
    finally
    {
        await server.StopAsync();
    }

    Assert.Equal(1, meter.For("redismq.reaper.failures")
        .Count(r => r.Tags["broker.instance"] is "reap1"));
    Assert.True(meter.For("redismq.reaper.sweeps")
        .Count(r => r.Tags["broker.instance"] is "reap1") >= 2);
    Assert.Contains(sink.Entries, e => e.Event == BrokerLogEvent.ReaperFailed && e.Level == BrokerLogLevel.Error);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.EngineObservabilityTests" -noColor; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ReaperObservabilityTests" -noColor
```

Expected: build fails or the metric assertions fail because the counters and reaper catch do not exist.

- [ ] **Step 3: Implement engine metrics and reaper resilience**

- `InMemoryEngine.Enqueue`: after the lock, count `MessagesDropped` with `vhost`, `queue`, `reason=max_length` for each id in the dropped list.
- `DeadLetterAlreadyLocked`: add a `string metricReason` parameter and return `DeadLetterDisposition`. Reorder the checks to: (1) no DLX -> `NoDeadLetterExchange` and no metric, (2) loop guard -> `LoopGuard` and count `DeadLettersDropped` reason `loop_guard`, (3) named DLX absent -> `MissingExchange` and count reason `missing_exchange`, (4) no targets -> `Unroutable` and count reason `unroutable`, (5) copies -> `Copied`, count `MessagesDeadlettered` once per copy with the source queue and `metricReason`. Keep the x-death header reason exactly as today (`rejected` or `expired`) so existing tests stay green.
- `Requeue` delivery-limit path passes `metricReason: "delivery_limit"`; `Reject` passes `metricReason: "rejected"`; `Reap` expiration passes `metricReason: "expired"`.
- `Reap`: for every expired message count `MessagesExpired` with `vhost`, `queue`; write `MessageExpired` at Debug. For every deleted queue count `QueuesExpired` with `vhost` and write `QueueExpired` at Warning after the lock.
- `AmqpBrokerServer.RunReaperAsync`: wrap each `_engine.Reap(DateTimeOffset.UtcNow)` in `Stopwatch`-timed `try/catch/finally`. `catch (OperationCanceledException) when (ct.IsCancellationRequested)` returns; any other exception counts `ReaperFailures`, writes `ReaperFailed` at Error with the exception, and continues. The `finally` counts `ReaperSweeps` and records `ReaperDuration` for every attempted tick.

- [ ] **Step 4: Run the tests to verify they pass**

Run both commands from Step 2. Expected: both classes with 0 failed.

- [ ] **Step 5: Run the existing dead-letter and reaper suites**

```powershell
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.DeadLetterTests" -noColor; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.ReaperTests" -noColor
```

Expected: both classes with 0 failed.

- [ ] **Step 6: Commit**

```powershell
git add src\RedisMQ.Broker\Memory\InMemoryEngine.cs src\RedisMQ.Broker\Server\AmqpBrokerServer.cs tests\RedisMQ.Broker.Tests\EngineObservabilityTests.cs tests\RedisMQ.Broker.Tests\ReaperObservabilityTests.cs
git commit -m "feat(diagnostics): instrument drops expiry dead letters and reaper health"
```

---

### Task 7: Redis Drainer, Mirror Failure Episode, Replay and Engine Health

**Files:**
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Test: `tests/RedisMQ.Redis.Tests/ObservabilityRedisTests.cs`

**Interfaces:**
- Consumes: `RedisMirrorCommands`, `RedisMirrorDuration`, `RedisMirrorDropped`, `RedisDrainerSaturated`, `RedisReplayMessages`, `RedisReplayFailures`, `RegisterObservableGauge`, `EngineHealth`.
- Produces: internal `Func<WriteCommand, Task>? WriteOverrideForTest { get; set; }`, internal `bool CompleteDrainerForTest()`.
- Produces: `GetEngineHealth()` with real mirror counters, depth, timestamps and last error; `QueueSnapshots()` delegation.

- [ ] **Step 1: Write the failing Redis tests**

Create `ObservabilityRedisTests.cs` and reuse the `TryConnectRedis`, `WaitUntilAsync` and `DecodeEntry` patterns from `RedisStreamRouteEngineTests.cs`. Every test returns early when Redis is unavailable.

```csharp
[Fact]
public async Task Mirror_failure_is_counted_logged_once_and_recovery_is_logged()
{
    IDatabase? db = TryConnectRedis();
    if (db is null) { return; }
    var sink = new RecordingLogSink();
    using var diag = new BrokerDiagnostics("redis1", sink);
    using var meter = new MeterCollector();
    string vhost = $"/obs-mirror-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 16,
        enforceEvictionGate: false, diagnostics: diag);
    engine.WriteOverrideForTest = _ => throw new InvalidOperationException("mirror down");

    EnqueueReceipt failed = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
    Assert.False(await failed.Durable);
    await WaitUntilAsync(() => meter.For("redismq.redis.mirror.commands")
        .Any(r => r.Tags["broker.instance"] is "redis1" && (string?)r.Tags["outcome"] == "failure"), "failure metric");

    engine.WriteOverrideForTest = null;
    EnqueueReceipt recovered = engine.Enqueue(vhost, "q", new ContentProperties(), [2], "", "q");
    Assert.True(await recovered.Durable);

    Assert.Single(sink.Entries.Where(e => e.Event == BrokerLogEvent.RedisMirrorFailed && e.Level == BrokerLogLevel.Error));
    Assert.Contains(sink.Entries, e => e.Event == BrokerLogEvent.RedisMirrorRecovered);
    Assert.True(engine.GetEngineHealth().MirrorFailures >= 1);
    Assert.NotNull(engine.GetEngineHealth().LastMirrorFailureUtc);
}

[Fact]
public async Task Saturated_drainer_refuses_the_publish_and_counts_it()
{
    IDatabase? db = TryConnectRedis();
    if (db is null) { return; }
    using var diag = new BrokerDiagnostics("redis2");
    using var meter = new MeterCollector();
    string vhost = $"/obs-sat-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 1,
        enforceEvictionGate: false, diagnostics: diag);
    Assert.True(engine.DrainerSlotsForTest!.Wait(0));
    try
    {
        EnqueueReceipt refused = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
        Assert.Equal(0UL, refused.EnvelopeId);
        Assert.False(await refused.Durable);
    }
    finally
    {
        engine.DrainerSlotsForTest!.Release();
    }

    Assert.Equal(1, meter.For("redismq.redis.drainer.saturated")
        .Count(r => r.Tags["broker.instance"] is "redis2"));
}

[Fact]
public async Task Drainer_channel_drop_is_counted_without_flipping_health()
{
    IDatabase? db = TryConnectRedis();
    if (db is null) { return; }
    using var diag = new BrokerDiagnostics("redis3");
    using var meter = new MeterCollector();
    string vhost = $"/obs-drop-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 16,
        enforceEvictionGate: false, diagnostics: diag);
    Assert.True(engine.CompleteDrainerForTest());

    engine.DeclareExchange(new ExchangeInfo(vhost, "x", "direct"));

    MeasurementRecord dropped = Assert.Single(meter.For("redismq.redis.mirror.dropped")
        .Where(r => r.Tags["broker.instance"] is "redis3"));
    Assert.Equal("channel_full", dropped.Tags["reason"]);
    EngineHealth health = engine.GetEngineHealth();
    Assert.True(health.Durable);
    Assert.True(health.RedisConnected);
    Assert.Equal(1, health.MirrorDropped);
}

[Fact]
public async Task Redis_health_reports_connected_and_depth()
{
    IDatabase? db = TryConnectRedis();
    if (db is null) { return; }
    using var diag = new BrokerDiagnostics("redis4");
    string vhost = $"/obs-health-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 16,
        enforceEvictionGate: false, diagnostics: diag);
    EngineHealth health = engine.GetEngineHealth();
    Assert.Equal("redis", health.Kind);
    Assert.True(health.Durable);
    Assert.True(health.RedisConnected);
    Assert.Equal(0, health.DrainerDepth);
}

[Fact]
public async Task Replay_counts_restored_messages()
{
    IDatabase? db = TryConnectRedis();
    if (db is null) { return; }
    using var diag = new BrokerDiagnostics("redis5");
    using var meter = new MeterCollector();
    string vhost = $"/obs-replay-{Guid.NewGuid():N}";
    await using (var writer = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 16, enforceEvictionGate: false))
    {
        writer.DeclareQueue(new QueueInfo(vhost, "q"));
        Assert.True(await writer.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q").Durable);
    }

    await using var replay = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: true, drainerCapacity: 16,
        enforceEvictionGate: false, diagnostics: diag);
    Assert.Equal(1, meter.For("redismq.redis.replay.messages")
        .Count(r => r.Tags["broker.instance"] is "redis5"));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -class "RedisMQ.Redis.Tests.ObservabilityRedisTests" -noColor
```

Expected: build fails because the new constructor parameter/seams and metrics do not exist. If Redis is unreachable the tests skip, but the build must still fail on the missing API.

- [ ] **Step 3: Implement Redis instrumentation**

- Constructor: append `BrokerDiagnostics? diagnostics = null`; create `_diagnostics`; construct `_memory` with it; register the `redismq.redis.drainer.depth` gauge when `_channel` is not null; dispose it in `DisposeAsync`; also call `_memory.Dispose()` there.
- Add private counters `long _mirrorFailures`, `_mirrorDropped`, `DateTimeOffset? _lastMirrorFailureUtc`, `_lastMirrorDropUtc`, `string? _lastError`, `bool _failureEpisode`.
- Add `WriteOverrideForTest` checked at the top of `WriteCommandToRedisAsync`; if set, await it and return.
- Add `CompleteDrainerForTest()` returning `_channel?.Writer.TryComplete() ?? false`.
- Every `TryWrite` failure on the drainer channel (`Enqueue` initial command, max-length remove, `EnqueueCommand`, `MirrorDelta`) increments `RedisMirrorDropped` with `kind = command.Kind.ToString()` and `reason = "channel_full"`, and records `_mirrorDropped`/`_lastMirrorDropUtc`.
- `Enqueue` with no slot increments `RedisDrainerSaturated`, writes `DrainerSaturated` at Warning, and returns the existing refused receipt.
- `DrainAsync`: for each command, measure elapsed seconds; `try` write, `catch (Exception ex)` set `_lastError` to a sanitized first line (max 200 chars), increment `_mirrorFailures` and `_lastMirrorFailureUtc`; if `!_failureEpisode`, set it true and write `RedisMirrorFailed` at Error. On success, if `_failureEpisode` was true, clear it and write `RedisMirrorRecovered` at Information. `finally` counts `RedisMirrorCommands` with `kind` and `outcome` and records `RedisMirrorDuration` with `kind`. A `Delta` command counts once; child writes inside it are not separately counted.
- Startup replay: wrap the constructor's `ReplayAsync().GetAwaiter().GetResult()` in `try/catch`, count `RedisReplayFailures`, write `RedisMirrorFailed` at Error with a "replay failed" message, and rethrow. Count `RedisReplayMessages` for every `_memory.ReplayEnqueue` winner.
- `GetEngineHealth`: return `new EngineHealth("redis", _redis is not null, _redis?.Multiplexer.IsConnected, _channel?.Reader.Count ?? 0, Interlocked.Read(ref _mirrorFailures), Interlocked.Read(ref _mirrorDropped), _lastMirrorFailureUtc, _lastMirrorDropUtc, _lastError)`.
- `QueueSnapshots()` delegates to `_memory.QueueSnapshots()`.

- [ ] **Step 4: Run the tests to verify they pass**

Run the command from Step 2. Expected with Redis: `Total: 5, Failed: 0`. Without Redis: all five skip cleanly.

- [ ] **Step 5: Run the existing Redis suite**

```powershell
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor
```

Expected: all existing Redis tests pass, 0 failed.

- [ ] **Step 6: Commit**

```powershell
git add src\RedisMQ.Redis\RedisStreamRouteEngine.cs tests\RedisMQ.Redis.Tests\ObservabilityRedisTests.cs
git commit -m "feat(diagnostics): instrument redis drainer mirror failures and replay"
```

---

### Task 8: Broker Health, TLS Metrics, Documentation and Full Gate

**Files:**
- Modify: `src/RedisMQ.Broker/Server/AmqpBrokerServer.cs`
- Modify: `src/RedisMQ.Broker/Transport/TlsSocketListener.cs`
- Modify: `src/RedisMQ.Broker/Transport/BrokerListeners.cs`
- Test: `tests/RedisMQ.Broker.Tests/HealthTests.cs`
- Test: `tests/RedisMQ.Broker.Tests/TlsDiagnosticsTests.cs`
- Modify: `README.md`, `docs/API.md`, `docs/ARCHITECTURE.md`, `docs/DEVELOPMENT.md`, `docs/CONFORMANCE.md`

**Interfaces:**
- Consumes: `IEngineDiagnostics`, `BrokerRuntimeState`, `TlsHandshakes`, `TlsHandshakeDuration`, all prior instrumentation.
- Produces: `public BrokerHealth GetHealth()`.
- Produces: `BrokerListeners.Create(BrokerServerOptions options, BrokerDiagnostics? diagnostics = null)`.
- Produces: `TlsSocketListener(BrokerTlsOptions options, BrokerDiagnostics? diagnostics = null)` and internal overload appending diagnostics.

- [ ] **Step 1: Write the failing health and TLS tests**

Create `HealthTests.cs`:

```csharp
[Fact]
public async Task Health_before_start_is_not_live_or_ready()
{
    using var diag = new BrokerDiagnostics("health1");
    var engine = new InMemoryEngine(["/"], diag);
    await using var server = new AmqpBrokerServer(
        new BrokerServerOptions { Vhosts = ["/"] }, engine, diagnostics: diag);
    BrokerHealth health = server.GetHealth();
    Assert.False(health.Live);
    Assert.False(health.Ready);
    Assert.Equal("server is not started", health.Detail);
    Assert.Equal(0, health.ActiveConnections);
    Assert.Equal("memory", health.Engine.Kind);
}

[Fact]
public async Task Health_reports_live_connections_channels_and_queue_depth()
{
    using var diag = new BrokerDiagnostics("health2");
    await using var harness = DiagnosticsWireHarness.Start(diagnostics: diag);
    await harness.HandshakeAsync();
    await harness.OpenChannelAsync();
    await harness.DeclareQueueAsync(1, "q");
    await harness.PublishAsync(1, "", "q");

    BrokerHealth health = harness.Server.GetHealth();
    Assert.True(health.Live);
    Assert.True(health.Ready);
    Assert.Null(health.Detail);
    Assert.Equal(1, health.ActiveConnections);
    Assert.Equal(1, health.ActiveChannels);
    Assert.Equal(1, Assert.Single(health.Queues).Ready);
}

[Fact]
public async Task Redis_health_detail_makes_the_broker_not_ready()
{
    using var diag = new BrokerDiagnostics("health3");
    var engine = new FakeRedisDiagnosticsEngine();
    await using var server = new AmqpBrokerServer(
        new BrokerServerOptions { Vhosts = ["/"] }, engine, diagnostics: diag);
    await server.StartAsync();
    try
    {
        BrokerHealth health = server.GetHealth();
        Assert.False(health.Ready);
        Assert.Equal("redis disconnected", health.Detail);
    }
    finally { await server.StopAsync(); }
}

[Fact]
public async Task Custom_engine_reports_custom_kind_and_stays_ready()
{
    using var diag = new BrokerDiagnostics("health4");
    var engine = new RecordingEngine(["/"]);
    await using var server = new AmqpBrokerServer(
        new BrokerServerOptions { Vhosts = ["/"] }, engine, diagnostics: diag);
    await server.StartAsync();
    try
    {
        BrokerHealth health = server.GetHealth();
        Assert.True(health.Ready);
        Assert.Equal("custom", health.Engine.Kind);
        Assert.False(health.Engine.Durable);
        Assert.Empty(health.Queues);
    }
    finally { await server.StopAsync(); }
}
```

`FakeRedisDiagnosticsEngine` delegates all `IRouteEngine` members to `InMemoryEngine` and implements `IEngineDiagnostics` with `Kind="redis"`, `Durable=true`, `RedisConnected=false`, `LastError="redis disconnected"`.

Create `TlsDiagnosticsTests.cs` using `TestCertificates` and the `TlsSocketListener` pattern from `TlsTransportTests`:

```csharp
[Fact]
public async Task Successful_tls_handshake_is_counted_with_duration()
{
    using var certs = new TestCertificates();
    using var diag = new BrokerDiagnostics("tls1");
    using var meter = new MeterCollector();
    await using var listener = new TlsSocketListener(ServerOptions(certs), diag);
    ValueTask<TransportSide?> accepted = listener.AcceptAsync(TestContext.Current.CancellationToken);
    (TcpClient tcp, SslStream ssl) = await ConnectTlsAsync(listener.AnnouncedPort);
    using (tcp) using (ssl)
    {
        _ = await accepted;
        Assert.Equal("success", Assert.Single(meter.For("redismq.tls.handshakes")
            .Where(r => r.Tags["broker.instance"] is "tls1")).Tags["result"]);
        Assert.Single(meter.For("redismq.tls.handshake.duration")
            .Where(r => r.Tags["broker.instance"] is "tls1"));
    }
}

[Fact]
public async Task Failed_tls_handshake_is_counted_and_logged_at_debug()
{
    using var certs = new TestCertificates();
    var sink = new RecordingLogSink();
    using var diag = new BrokerDiagnostics("tls2", sink);
    using var meter = new MeterCollector();
    await using var listener = new TlsSocketListener(ServerOptions(certs), diag);
    _ = listener.AcceptAsync(TestContext.Current.CancellationToken);
    using (var tcp = new TcpClient())
    {
        await tcp.ConnectAsync("localhost", listener.AnnouncedPort, TestContext.Current.CancellationToken);
        tcp.Close(); // abort the handshake
    }

    await WaitUntilAsync(() => meter.For("redismq.tls.handshakes")
        .Any(r => r.Tags["broker.instance"] is "tls2" && (string?)r.Tags["result"] == "failure"), TimeSpan.FromSeconds(3));
    Assert.Contains(sink.Entries, e => e.Event == BrokerLogEvent.TlsHandshakeFailed && e.Level == BrokerLogLevel.Debug);
}
```

Copy `ServerOptions` and `ConnectTlsAsync` from `TlsTransportTests`, and add a `WaitUntilAsync` local helper.

- [ ] **Step 2: Run the tests to verify they fail**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.HealthTests" -noColor; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -class "RedisMQ.Broker.Tests.TlsDiagnosticsTests" -noColor
```

Expected: build fails because `GetHealth`, the TLS diagnostics constructor and the metrics do not exist.

- [ ] **Step 3: Implement health and TLS instrumentation**

- `AmqpBrokerServer.GetHealth()`: synchronous. `Live = _started && Volatile.Read(ref _acceptFaulted) == 0`. Build `EngineHealth` from `_engine as IEngineDiagnostics`; when absent use `new EngineHealth("custom", false, null, 0, 0, 0, null, null, null)`. `Ready` is false when not live with detail `"server is not started"`; false when `Kind=="redis" && Durable && RedisConnected == false` with detail `LastError ?? "redis disconnected"`; otherwise true with null detail. Queue snapshots come from the engine; overlay `_runtime` unacked per `(vhost, queue)`, then call `_runtime.PruneUnacked` with the live queue keys. Fill `Timestamp = DateTimeOffset.UtcNow`.
- Ensure `TrackConnection`/`WireConnection` update the same `_runtime` instance the health method reads.
- `StartAsync` writes `ServerStarted` at Information; `StopAsync` writes `ServerStopped` at Information.
- `TlsSocketListener`: add optional diagnostics to both constructors; store `_diagnostics`. Around `AuthenticateAsServerAsync`, take a `Stopwatch` timestamp. On success count `TlsHandshakes` `result=success` and record duration. On the caught handshake exception count `result=failure`, record duration, and write `TlsHandshakeFailed` at Debug with `exception.GetType().Name` only (no certificate or peer data). Do not log when the cancellation token is the server stopping.
- `BrokerListeners.Create` appends `BrokerDiagnostics? diagnostics = null` and passes it to `TlsSocketListener`.
- Keep the existing constructors source-compatible: the new parameter is appended and optional.

- [ ] **Step 4: Run the tests to verify they pass**

Run both commands from Step 2. Expected: both classes with 0 failed.

- [ ] **Step 5: Update documentation**

- `README.md`: add an `## Observability` section after `## Durability Contract` covering: the `RedisMQ.Broker` Meter and ActivitySource names; a short shared-diagnostics snippet (`var diag = new BrokerDiagnostics();` passed to both engine and server); `GetHealth()` returning `BrokerHealth`; the host-owned endpoints/exporters note; and the metric-prefix/tag-cardinality caveat. Add observability to the API/engineering docs links. Remove "Observability" from `## What's Next`. Update the three test counts in `## Running the Tests` to the final verified totals.
- `docs/API.md`: document `BrokerDiagnostics`, `IBrokerLogSink`, `BrokerLogLevel`, `BrokerLogEvent`, `BrokerLogEntry`, `NullBrokerLogSink`, `BrokerHealth`, `EngineHealth`, `QueueSnapshot`, `IEngineDiagnostics`, `AmqpBrokerServer.GetHealth()`, and every appended constructor parameter.
- `docs/ARCHITECTURE.md`: add a diagnostics layer row/section describing the Meter/ActivitySource/log sink/health snapshot and the rule that the Redis wrapper emits mirror/replay only while the memory engine emits message-level counters.
- `docs/DEVELOPMENT.md`: add instrumentation conventions (Enabled/IsEnabled guards, stable event ids, no PII, no throwing sinks, `broker.instance` on every measurement, shared diagnostics per process) and remove observability from the remaining-work list if present.
- `docs/CONFORMANCE.md`: add one sentence that observability added no wire surface and the existing 19 conformance tests are unchanged.

- [ ] **Step 6: Run the full gate**

```powershell
dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -noColor; dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll -noColor; dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor
```

Expected: build 0 errors and no new warnings beyond the pre-existing `xUnit1051` set; all three assemblies report 0 failed. Baseline before this plan was Broker 319, Conformance 19, Redis 60; the final counts are those plus the new tests.

- [ ] **Step 7: Commit**

```powershell
git add src\RedisMQ.Broker\Server\AmqpBrokerServer.cs src\RedisMQ.Broker\Transport\TlsSocketListener.cs src\RedisMQ.Broker\Transport\BrokerListeners.cs tests\RedisMQ.Broker.Tests\HealthTests.cs tests\RedisMQ.Broker.Tests\TlsDiagnosticsTests.cs README.md docs\API.md docs\ARCHITECTURE.md docs\DEVELOPMENT.md docs\CONFORMANCE.md
git commit -m "feat(diagnostics): add broker health tls metrics and docs"
```

---

## Self-Review Notes

- **Spec coverage:** §7.1 Task 1; §7.2 Tasks 1, 3–7; §7.3 Tasks 4–5; §7.4 Tasks 3, 4, 6, 7, 8; §7.5–7.6 Tasks 2, 7, 8; §7.7 Tasks 2–8; §9 broker tests Tasks 3–6 and 8, Redis tests Task 7; §10 Task 8.
- **Deliberate spec readings:** publish activities get both `redismq.publish.outcome` (dispatch fact) and `redismq.outcome` (final confirm outcome) so the two facts cannot be confused; an unroutable publish still confirms `ack`. `QueueSnapshots()` returns `Unacked = 0` and `GetHealth()` overlays runtime state, keeping the engine interface free of connection state. `RedisStreamRouteEngine` owns an `InMemoryEngine` instrumented with the same diagnostics, so message-level counters are never emitted twice.
- **No new package references** are added; every test helper uses BCL diagnostics types.
- **Out of scope by spec:** HTTP endpoints, exporters, trace propagation and a settlement span.
