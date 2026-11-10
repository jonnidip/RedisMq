# RedisMQ performance slice implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pipeline the Redis mirror and add a benchmark gate that proves the v1 envelope with 500 connections, 1,000 consumers and 1,000 queues live at ≥5,000 confirmed publishes/s and p99 first delivery <20 ms.

**Architecture:** Keep the single memory-first drainer. Read a bounded batch of ready `WriteCommand`s, issue independent Redis writes without awaiting each one, then await their tasks in order and resolve confirms, health and metrics. `Delta`, `Remove` and `DeleteQueue` are barriers. A new console benchmark project drives real AMQP clients against a broker backed by a real Redis and emits a JSON gate report.

**Tech Stack:** .NET 10, C#, StackExchange.Redis 2.8.16, RabbitMQ.Client 7.2.2, xUnit v3, GitHub Actions with a `redis:7` service.

**Spec:** `docs/superpowers/specs/2026-11-10-redismq-performance-design.md`

## Global Constraints

- `src/RedisMQ.Broker` keeps zero `PackageReference`s.
- `src/RedisMQ.Redis` keeps StackExchange.Redis as its only package reference.
- Redis key layout and hash tags are unchanged.
- The drainer stays a single task; no extra Redis connections or `MULTI/EXEC`.
- Gate values: 500 connections, 1,000 consumers, 1,000 queues, ≥5,000 confirmed publishes/s at 1 KB, p99 first delivery ≤20 ms, zero publish failures and zero mirror failure/drop deltas during measurement.
- Benchmarks use a unique vhost and never `FLUSHDB` or issue admin commands.
- Local Windows test invocation is `dotnet exec tests\<Proj>\bin\Debug\net10.0\<Proj>.dll -noColor`; `dotnet test` is the CI path.
- Redis-dependent tests skip only when `127.0.0.1:6379` is unreachable.
- Do not rewrite the README wholesale; only the targeted sections this plan names.

## Review Focus

- **Max-length eviction during a pipelined burst.** A `Remove` issued before its `XADD` continuation populates `_streamIds` silently leaks and resurrects the evicted entry. Pinned by `Max_length_eviction_under_pipelined_burst_is_mirrored`.
- **Shutdown while the mirror is blocked.** Publisher confirm tasks must resolve true or false, never hang, when the drainer is disposed mid-batch. Pinned by `Shutdown_completes_issued_batch_and_abandons_the_rest`.
- **One failed write inside a batch.** Neighbouring commands must still complete and only the failed publisher gets `false`. Pinned by `Pipelined_batch_failure_does_not_fail_neighbours`.
- **Benchmark against a shared Redis.** Two consecutive runs must use distinct vhosts, leave no RedisMQ keys behind, and never flush the store. Verified by running `throughput` twice and scanning `{mq:*}` keys for the bench vhosts.
- **A nack, drop or undelivered measured message.** The gate must fail, not report success. Pinned by the `--selftest` gate-evaluator assertions in Task 3.

---

### Task 1: Pipelined drainer and batch metric

**Files:**
- Modify: `src/RedisMQ.Broker/Diagnostics/BrokerDiagnostics.cs`
- Modify: `src/RedisMQ.Redis/RedisStreamRouteEngine.cs`
- Test: `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`
- Test: `tests/RedisMQ.Redis.Tests/ObservabilityRedisTests.cs`

**Interfaces:**
- Consumes: existing `WriteCommand`, `WriteCommandKind.Delta`, `WriteCommandKind.Remove`, `WriteCommandKind.DeleteQueue`, `BrokerDiagnostics.Count`/`Measure`.
- Produces:
  - `RedisStreamRouteEngine(IDatabase? redis, IReadOnlyList<string> vhosts, bool replayOnStart = true, int drainerCapacity = 1024, bool enforceEvictionGate = true, BrokerDiagnostics? diagnostics = null, int drainerBatchSize = 256)`
  - `internal int RedisStreamRouteEngine.DrainerBatchSizeForTest { get; }`
  - `internal Histogram<long> BrokerDiagnostics.RedisMirrorBatchSize { get; }`
  - `private static bool RedisStreamRouteEngine.IsBarrier(WriteCommand command)`
  - `private async Task RedisStreamRouteEngine.ProcessSingleAsync(WriteCommand command)`
  - `private async Task RedisStreamRouteEngine.CompleteAsync(WriteCommand command, Task write, long started)`

- [ ] **Step 1: Write the failing pipelining test**

Add to `tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs`:

```csharp
[Fact]
public async Task Drainer_issues_more_than_one_write_before_completion()
{
    IDatabase? db = TryConnectRedis();
    if (db is null)
    {
        return;
    }

    string vhost = $"/batch-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 64,
        enforceEvictionGate: false, drainerBatchSize: 16);

    var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    int issued = 0;
    engine.WriteOverrideForTest = async _ =>
    {
        Interlocked.Increment(ref issued);
        await gate.Task;
    };

    for (int i = 0; i < 64; i++)
    {
        engine.Enqueue(vhost, "q", new ContentProperties(), [(byte)i], "", "q");
    }

    await WaitUntilAsync(
        () => Task.FromResult(Volatile.Read(ref issued) > 1),
        "the drainer to issue more than one write");
    int observed = Volatile.Read(ref issued);
    gate.TrySetResult(true);

    Assert.InRange(observed, 2, 16);
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:
`dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -method "RedisMQ.Redis.Tests.RedisStreamRouteEngineTests.Drainer_issues_more_than_one_write_before_completion" -noColor`

Expected: FAIL. The current drainer awaits each write, so `issued` stays 1 and `WaitUntilAsync` times out after five seconds.

- [ ] **Step 3: Add the batch-size instrument**

In `src/RedisMQ.Broker/Diagnostics/BrokerDiagnostics.cs`, next to `RedisMirrorDuration`:

```csharp
RedisMirrorBatchSize = Meter.CreateHistogram<long>(
    "redismq.redis.mirror.batch.size",
    "{command}",
    "Commands issued per drainer batch.");
```

Declare `internal Histogram<long> RedisMirrorBatchSize { get; }` beside the other Redis instruments. Do not add tags.

- [ ] **Step 4: Add the constructor parameter and accessor**

Append `int drainerBatchSize = 256` to the `RedisStreamRouteEngine` constructor and store:

```csharp
_drainerBatchSize = Math.Clamp(drainerBatchSize, 1, Math.Max(1, drainerCapacity));
```

Add `internal int DrainerBatchSizeForTest => _drainerBatchSize;` beside `DrainerSlotsForTest`.

- [ ] **Step 5: Rewrite `DrainAsync` into batch, barrier and completion phases**

Replace the body of `DrainAsync` with this shape. Move the existing try/catch/finally verbatim out of the loop into `CompleteAsync`; do not duplicate the failure/episode/counter logic.

```csharp
private async Task DrainAsync(CancellationToken ct)
{
    List<WriteCommand> batch = new(_drainerBatchSize);
    await foreach (WriteCommand first in _channel!.Reader.ReadAllAsync(ct))
    {
        batch.Clear();
        batch.Add(first);
        while (batch.Count < _drainerBatchSize
               && _channel.Reader.TryRead(out WriteCommand? next)
               && next is not null)
        {
            batch.Add(next);
        }

        _diagnostics.Measure(
            _diagnostics.RedisMirrorBatchSize,
            batch.Count,
            new TagList());

        int index = 0;
        while (index < batch.Count)
        {
            if (ct.IsCancellationRequested)
            {
                for (; index < batch.Count; index++)
                {
                    AbandonWrite(batch[index]);
                }

                break;
            }

            if (IsBarrier(batch[index]))
            {
                await ProcessSingleAsync(batch[index]);
                index++;
                continue;
            }

            List<(WriteCommand Command, Task Write, long Started)> pending = [];
            while (index < batch.Count && !IsBarrier(batch[index]))
            {
                WriteCommand command = batch[index++];
                pending.Add((command, WriteCommandToRedisAsync(command), Stopwatch.GetTimestamp()));
            }

            foreach ((WriteCommand command, Task write, long started) in pending)
            {
                await CompleteAsync(command, write, started);
            }
        }
    }
}

private static bool IsBarrier(WriteCommand command) =>
    command.Kind is WriteCommandKind.Delta
        or WriteCommandKind.Remove
        or WriteCommandKind.DeleteQueue;

private async Task ProcessSingleAsync(WriteCommand command)
{
    long started = Stopwatch.GetTimestamp();
    await CompleteAsync(command, WriteCommandToRedisAsync(command), started);
}

private async Task CompleteAsync(WriteCommand command, Task write, long started)
{
    bool succeeded = true;
    try
    {
        await write;
        // existing recovery-episode logging verbatim
    }
    catch (Exception ex)
    {
        // existing failure counters, sanitization and episode logging verbatim
        succeeded = false;
    }
    finally
    {
        // existing slot release, command counter, duration and Completion.TrySetResult verbatim
    }
}
```

`WriteCommandToRedisAsync` keeps its existing body, including `Delta` recursion and the `_streamIds` update after `StreamAddAsync`.

- [ ] **Step 6: Run the pipelining test and the Redis suite**

Run:
`dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor`

Expected: the pipelining test passes and all existing Redis tests pass (Redis running). If an existing test assumed one command per drainer iteration, adjust only the assumption, not its semantics.

- [ ] **Step 7: Add the ordering and partial-failure regression tests**

Add to `RedisStreamRouteEngineTests`:

```csharp
[Fact]
public async Task Pipelined_batch_preserves_per_queue_stream_order()
{
    IDatabase? db = TryConnectRedis();
    if (db is null)
    {
        return;
    }

    string vhost = $"/order-{Guid.NewGuid():N}";
    string queueKey = RedisKeys.QueueKey(vhost, "q");
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 4096,
        enforceEvictionGate: false, drainerBatchSize: 256);

    List<EnqueueReceipt> receipts = [];
    for (int i = 0; i < 2000; i++)
    {
        receipts.Add(engine.Enqueue(vhost, "q", new ContentProperties(), BitConverter.GetBytes(i), "", "q"));
    }

    foreach (EnqueueReceipt receipt in receipts)
    {
        Assert.True(await receipt.Durable);
    }

    StreamEntry[] entries = await db.StreamRangeAsync(queueKey);
    Assert.Equal(2000, entries.Length);
    for (int i = 0; i < entries.Length; i++)
    {
        Assert.Equal((ulong)(i + 1), DecodeEntry(entries[i]).EnvelopeId);
    }
}

[Fact]
public async Task Pipelined_batch_failure_does_not_fail_neighbours()
{
    IDatabase? db = TryConnectRedis();
    if (db is null)
    {
        return;
    }

    string vhost = $"/partial-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 64,
        enforceEvictionGate: false, drainerBatchSize: 16);
    engine.WriteOverrideForTest = command =>
        command.Kind == WriteCommandKind.Enqueue && command.EnvelopeId == 2
            ? Task.FromException(new InvalidOperationException("boom"))
            : Task.CompletedTask;

    EnqueueReceipt first = engine.Enqueue(vhost, "q", new ContentProperties(), [1], "", "q");
    EnqueueReceipt second = engine.Enqueue(vhost, "q", new ContentProperties(), [2], "", "q");
    EnqueueReceipt third = engine.Enqueue(vhost, "q", new ContentProperties(), [3], "", "q");

    Assert.True(await first.Durable);
    Assert.False(await second.Durable);
    Assert.True(await third.Durable);
    Assert.Equal(1, engine.GetEngineHealth().MirrorFailures);
}
```

- [ ] **Step 8: Add the max-length barrier test**

```csharp
[Fact]
public async Task Max_length_eviction_under_pipelined_burst_is_mirrored()
{
    IDatabase? db = TryConnectRedis();
    if (db is null)
    {
        return;
    }

    string vhost = $"/maxlen-batch-{Guid.NewGuid():N}";
    string queueKey = RedisKeys.QueueKey(vhost, "q");
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 128,
        enforceEvictionGate: false, drainerBatchSize: 64);
    engine.DeclareQueue(new QueueInfo(vhost, "q", MaxLength: 2));

    List<EnqueueReceipt> receipts = [];
    for (int i = 0; i < 50; i++)
    {
        receipts.Add(engine.Enqueue(vhost, "q", new ContentProperties(), [(byte)i], "", "q"));
    }

    foreach (EnqueueReceipt receipt in receipts)
    {
        Assert.True(await receipt.Durable);
    }

    await WaitUntilAsync(
        async () => await db.StreamLengthAsync(queueKey) == 2,
        "the evicted entries to be XDELed");
    StreamEntry[] entries = await db.StreamRangeAsync(queueKey);
    Assert.Equal([49UL, 50UL], entries.Select(entry => DecodeEntry(entry).EnvelopeId));
}
```

- [ ] **Step 9: Add the shutdown test**

```csharp
[Fact]
public async Task Shutdown_completes_issued_batch_and_abandons_the_rest()
{
    IDatabase? db = TryConnectRedis();
    if (db is null)
    {
        return;
    }

    string vhost = $"/shutdown-{Guid.NewGuid():N}";
    var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    int issued = 0;
    var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 64,
        enforceEvictionGate: false, drainerBatchSize: 8);
    engine.WriteOverrideForTest = async _ =>
    {
        Interlocked.Increment(ref issued);
        await gate.Task;
    };

    List<EnqueueReceipt> receipts = [];
    for (int i = 0; i < 64; i++)
    {
        receipts.Add(engine.Enqueue(vhost, "q", new ContentProperties(), [(byte)i], "", "q"));
    }

    await WaitUntilAsync(
        () => Task.FromResult(Volatile.Read(ref issued) >= 1),
        "the drainer to issue a write");
    Task dispose = engine.DisposeAsync().AsTask();
    gate.TrySetResult(true);
    await dispose.WaitAsync(TimeSpan.FromSeconds(10));

    int durable = 0;
    foreach (EnqueueReceipt receipt in receipts)
    {
        if (await receipt.Durable.AsTask().WaitAsync(TimeSpan.FromSeconds(5)))
        {
            durable++;
        }
    }

    Assert.InRange(durable, 1, 8);
}
```

- [ ] **Step 10: Add the batch-size metric test**

Add to `tests/RedisMQ.Redis.Tests/ObservabilityRedisTests.cs`:

```csharp
[Fact]
public async Task Drainer_records_batch_size()
{
    IDatabase? db = TryConnectRedis();
    if (db is null)
    {
        return;
    }

    using var diag = new BrokerDiagnostics("redis-batch");
    using var meter = new RedisMeterCollector();
    string vhost = $"/obs-batch-{Guid.NewGuid():N}";
    await using var engine = new RedisStreamRouteEngine(
        db, [vhost], replayOnStart: false, drainerCapacity: 64,
        enforceEvictionGate: false, diagnostics: diag, drainerBatchSize: 16);

    for (int i = 0; i < 32; i++)
    {
        EnqueueReceipt receipt = engine.Enqueue(vhost, "q", new ContentProperties(), [(byte)i], "", "q");
        Assert.True(await receipt.Durable);
    }

    await WaitUntilAsync(
        () => meter.For("redismq.redis.mirror.batch.size").Count > 0,
        "the drainer batch histogram");
    Assert.All(
        meter.For("redismq.redis.mirror.batch.size"),
        measurement => Assert.InRange((long)measurement.Value, 1, 16));
}
```

- [ ] **Step 11: Add the clamp test**

```csharp
[Fact]
public async Task Batch_size_is_clamped_to_drainer_capacity()
{
    await using var engine = new RedisStreamRouteEngine(
        null!, ["/"], replayOnStart: false, drainerCapacity: 8, drainerBatchSize: 32);

    Assert.Equal(8, engine.DrainerBatchSizeForTest);
}
```

- [ ] **Step 12: Run the full Redis suite**

Run:
`dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor`

Expected: all Redis tests pass; the new tests add seven passes.

- [ ] **Step 13: Commit**

```bash
git add src/RedisMQ.Broker/Diagnostics/BrokerDiagnostics.cs src/RedisMQ.Redis/RedisStreamRouteEngine.cs tests/RedisMQ.Redis.Tests/RedisStreamRouteEngineTests.cs tests/RedisMQ.Redis.Tests/ObservabilityRedisTests.cs
git commit -m "perf(redis): pipeline the drainer"
```

---

### Task 2: Hosting option for the drainer batch size

**Files:**
- Modify: `src/RedisMQ.Hosting/RedisEngineOptions.cs`
- Modify: `src/RedisMQ.Hosting/RedisMqServiceCollectionExtensions.cs`
- Test: `tests/RedisMQ.Hosting.Tests/ConfigurationBindingTests.cs`

**Interfaces:**
- Consumes: `RedisStreamRouteEngine` constructor parameter `drainerBatchSize` from Task 1.
- Produces: `RedisEngineOptions.DrainerBatchSize` (`int`, default `256`), bound from `RedisMQ:Redis:DrainerBatchSize`, and passed as the engine constructor's final argument.

- [ ] **Step 1: Write the failing binding assertion**

In `ConfigurationBindingTests.Build()`, add:

```csharp
["RedisMQ:Redis:DrainerBatchSize"] = "32",
```

In `Configuration_binds_redis_engine_options`, add:

```csharp
Assert.Equal(32, redis.DrainerBatchSize);
```

- [ ] **Step 2: Run the hosting suite and verify it fails**

Run:
`dotnet build RedisMQ.sln`

Expected: build fails with `CS0117: 'RedisEngineOptions' does not contain a definition for 'DrainerBatchSize'`.

- [ ] **Step 3: Implement the option and pass-through**

Add to `RedisEngineOptions`:

```csharp
public int DrainerBatchSize { get; set; } = 256;
```

In `CreateEngine`, pass `options.Redis.DrainerBatchSize` as the final argument to the `RedisStreamRouteEngine` constructor.

- [ ] **Step 4: Run the hosting suite**

Run:
`dotnet build RedisMQ.sln; dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor`

Expected: all hosting tests pass. Redis is not required for this test.

- [ ] **Step 5: Commit**

```bash
git add src/RedisMQ.Hosting/RedisEngineOptions.cs src/RedisMQ.Hosting/RedisMqServiceCollectionExtensions.cs tests/RedisMQ.Hosting.Tests/ConfigurationBindingTests.cs
git commit -m "perf(hosting): expose drainer batch size"
```

---

### Task 3: Benchmark host, report and throughput canary

**Files:**
- Create: `benchmarks/RedisMQ.Benchmarks/RedisMQ.Benchmarks.csproj`
- Create: `benchmarks/RedisMQ.Benchmarks/BenchmarkOptions.cs`
- Create: `benchmarks/RedisMQ.Benchmarks/BenchmarkHost.cs`
- Create: `benchmarks/RedisMQ.Benchmarks/Stats.cs`
- Create: `benchmarks/RedisMQ.Benchmarks/PerfReport.cs`
- Create: `benchmarks/RedisMQ.Benchmarks/Scenarios.cs`
- Create: `benchmarks/RedisMQ.Benchmarks/Program.cs`
- Modify: `RedisMQ.sln`

**Interfaces:**
- Consumes: `RedisMQ.Broker`, `RedisMQ.Redis`, `AmqpBrokerServer`, `BrokerListeners.Create`, `RedisStreamRouteEngine`, `EngineHealth`, RabbitMQ.Client 7.2.2.
- Produces:
  - `internal sealed class BenchmarkOptions` with `Scenario`, `Redis`, `DurationSeconds`, `WarmupSeconds`, `PublishBytes`, `Inflight`, `Connections`, `Consumers`, `Queues`, `WarmupRounds`, `LargeBytes`, `LargeCount`, `OutPath`, `NoGate`, `DrainerCapacity`, `DrainerBatchSize`.
  - `internal sealed class BenchmarkHost : IAsyncDisposable` with `Vhost`, `Port`, `Engine`, `Task WaitForDrainAsync(TimeSpan timeout)`, `Task CleanupAsync()`.
  - `internal sealed record ScenarioResult(string Name, bool Passed, IReadOnlyDictionary<string, double> Metrics, IReadOnlyDictionary<string, double> Thresholds, IReadOnlyList<string> Failures)`.
  - `internal static class Stats` with `double Percentile(IReadOnlyList<double> values, double percent)`.
  - `internal static class Gate` with `ScenarioResult Evaluate(string name, IReadOnlyDictionary<string, double> metrics, IReadOnlyDictionary<string, double> thresholds)`.
  - `internal static Task<ScenarioResult> RunThroughputAsync(BenchmarkHost host, BenchmarkOptions options, CancellationToken ct)`.

- [ ] **Step 1: Create the project and add it to the solution**

Create `benchmarks/RedisMQ.Benchmarks/RedisMQ.Benchmarks.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <LangVersion>latest</LangVersion>
    <ImplicitUsings>enable</ImplicitUsings>
    <IsPackable>false</IsPackable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="RabbitMQ.Client" Version="7.2.2" />
    <PackageReference Include="StackExchange.Redis" Version="2.8.16" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\RedisMQ.Broker\RedisMQ.Broker.csproj" />
    <ProjectReference Include="..\..\src\RedisMQ.Redis\RedisMQ.Redis.csproj" />
  </ItemGroup>
</Project>
```

Add the project to `RedisMQ.sln` under a new `benchmarks` solution folder with `dotnet sln RedisMQ.sln add benchmarks\RedisMQ.Benchmarks\RedisMQ.Benchmarks.csproj --solution-folder benchmarks`. Build:

`dotnet build RedisMQ.sln`

Expected: 0 errors.

- [ ] **Step 2: Implement argument parsing**

`BenchmarkOptions.Parse(string[] args)` accepts the flags in the spec's CLI, using `--name value` pairs and the boolean `--no-gate`. Unknown flags and non-integer values return an error and exit code 2 from `Program`. Defaults match the spec (`all`, `127.0.0.1:6379`, 30/5, 1024/128, 500/1000/1000/10, 1 MB/1000, `artifacts/perf.json`, capacity 1024, batch 256). `--help` prints the usage line and exits 0.

- [ ] **Step 3: Implement `Stats` and the gate evaluator**

`Stats.Percentile` sorts a copy and returns the nearest-rank value:

```csharp
int rank = Math.Clamp((int)Math.Ceiling(percent / 100.0 * values.Count) - 1, 0, values.Count - 1);
```

`Gate.Evaluate` builds `Failures` and sets `Passed` from: rate below the threshold, p99 above the threshold, `publishFailures != 0`, `mirrorDropped != 0`, `mirrorFailures != 0`, `unconsumed != 0`, or any connection/consumer/queue count below its threshold.

Add `--selftest` to `Program`: with no Redis, assert `Percentile([1..100], 50) == 50`, `Percentile([1..100], 99) == 99`, and that metrics with `publishFailures = 1`, `mirrorDropped = 1`, `mirrorFailures = 1`, a missed rate, a missed p99, or a missed count all fail `Gate.Evaluate`. Exit 0 on success.

- [ ] **Step 4: Implement `BenchmarkHost`**

- Connect with `ConfigurationOptions.Parse(options.Redis)` and `AbortOnConnectFail = true`; fail with a clear message if Redis is unreachable.
- Vhost: `/bench-<8 hex>`.
- `BrokerServerOptions`: `Port = 0`, `Vhosts = [vhost]`, `Users = [BrokerUser.FullAccess("guest", "guest", vhost)]`.
- Create listeners with `BrokerListeners.Create(options)`, `AmqpBrokerServer` over the `RedisStreamRouteEngine`, call `StartAsync`.
- Port from the plaintext listener's `AnnouncedPort`.
- `WaitForDrainAsync` polls `Engine.GetEngineHealth().DrainerDepth` every 50 ms and returns after it has been zero continuously for 500 ms (one zero sample is not enough: the drainer removes a batch from the channel before writing it), or throws at the deadline.
- Dispose order: `server.StopAsync`, dispose listeners, dispose the engine. `CleanupAsync` deletes `KeyDeleteAsync(RedisKeys.QueueKey(vhost, queue))` for every queue and `KeyDeleteAsync(RedisKeys.TopologyKey(vhost))`, in batches of 100 with `Task.WhenAll`. Never call `FLUSHDB` or `SCAN`.

- [ ] **Step 5: Implement `PerfReport`**

Serialize the schema in the spec with `System.Text.Json` (`camelCase`, indented): `schemaVersion`, `generatedAt`, `scenario`, `environment` (OS, framework, processor count, Redis endpoint, `redis_version` parsed from `INFO server`), `parameters`, `results`, `passed`. Create the output directory if needed, write the file, print one line per scenario plus a summary, and set the process exit code from `Passed` unless `--no-gate`. A scenario result carries its own failures list; the report's `passed` is the conjunction.

- [ ] **Step 6: Implement the throughput canary**

`RunThroughputAsync` follows the spec:

- `ConnectionFactory { UserName = "guest", Password = "guest", VirtualHost = host.Vhost }`.
- Publisher channel: `new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true)`.
- Declare one durable queue named `throughput`.
- Body: 8-byte sequence + 8-byte `Stopwatch.GetTimestamp()`, padded to `PublishBytes`.
- Keep `Inflight` `BasicPublishAsync` calls outstanding; count only confirmed completions and publish failures.
- Consumer channel: `new CreateChannelOptions(consumerDispatchConcurrency: 4)`, `BasicQosAsync(0, 512, false)`, `AsyncEventingBasicConsumer`, ack every delivery, record `Stopwatch.GetElapsedTime(timestamp)` in a `ConcurrentQueue<long>`.
- Warm up for `WarmupSeconds` (discarded), measure for `DurationSeconds`, stop publishing, wait up to 10 s for measured deliveries.
- Metrics: `publishesPerSecond`, `deliveriesPerSecond`, `publishConfirmP50Ms`, `publishConfirmP99Ms`, `deliveryP50Ms`, `deliveryP99Ms`, `publishFailures`, `mirrorDropped`, `mirrorFailures`, `unconsumed`.
- Health deltas: sample `GetEngineHealth()` before measurement (after warm-up and `WaitForDrainAsync`) and after the delivery wait; gate on the delta. If the baseline `MirrorFailures` or `MirrorDropped` is non-zero, fail with `setup was not clean` rather than hiding it in the delta.
- Thresholds: 5,000/s, p99 20 ms, zero failures/drops, zero unconsumed.

- [ ] **Step 7: Wire the CLI and run the canary**

`Program` dispatches `--selftest`, `throughput`, and rejects `combined`/`large`/`all` with "not implemented in this task" until Task 4. Ensure Redis is running on `127.0.0.1:6379`.

Run:
`dotnet run -c Release --project benchmarks\RedisMQ.Benchmarks -- --selftest`
`dotnet run -c Release --project benchmarks\RedisMQ.Benchmarks -- --scenario throughput --no-gate --duration 10 --warmup 2 --out artifacts\perf-throughput.json`

Expected: selftest exits 0; the throughput run prints metrics, writes JSON, and exits 0.

- [ ] **Step 8: Verify no Redis keys leak**

Run the throughput command twice. Then, with `redis-cli`, list `{mq:*}` keys and assert neither bench vhost's queue key nor topology key remains. If `redis-cli` is unavailable locally, use a short `dotnet` snippet with `db.KeyExistsAsync` or skip this check locally and rely on the cleanup code plus CI.

- [ ] **Step 9: Commit**

```bash
git add benchmarks RedisMQ.sln
git commit -m "perf(bench): add benchmark host and throughput canary"
```

---

### Task 4: Combined envelope, large messages and the gate

**Files:**
- Modify: `benchmarks/RedisMQ.Benchmarks/Scenarios.cs`
- Modify: `benchmarks/RedisMQ.Benchmarks/PerfReport.cs`
- Modify: `benchmarks/RedisMQ.Benchmarks/Program.cs`
- Modify: `benchmarks/RedisMQ.Benchmarks/BenchmarkHost.cs`

**Interfaces:**
- Consumes: Task 3's `BenchmarkHost`, `BenchmarkOptions`, `Stats`, `ScenarioResult`, `PerfReport`.
- Produces:
  - `internal static Task<ScenarioResult> RunCombinedAsync(BenchmarkHost host, BenchmarkOptions options, CancellationToken ct)`
  - `internal static Task<ScenarioResult> RunLargeAsync(BenchmarkHost host, BenchmarkOptions options, CancellationToken ct)`
  - `all` dispatch running `combined`, `throughput`, `large` in that order and writing one report.

- [ ] **Step 1: Implement the combined scenario**

- Build the topology: `Connections` connections total. Connection 0 carries the publisher channel and two consumer channels; every other connection carries two consumer channels. Declare `Queues` durable queues, two per connection. Each consumer channel consumes one queue with prefetch 16. Create queue names `q-0000`…`q-0999` and pass them to cleanup.
- `WarmupRounds` rounds: publish one timestamped message to every queue, confirm, wait for all deliveries.
- Measure for `DurationSeconds`: one publisher channel with confirm tracking and `Inflight` outstanding publishes, round-robin across all queues. No artificial rate cap.
- Consumers ack and record first-delivery latency into a shared `ConcurrentQueue<long>`.
- After the deadline, wait up to 20 s for all measured messages.
- Metrics: `connections`, `consumers`, `queues`, `publishesPerSecond`, `deliveriesPerSecond`, confirm p50/p99, delivery p50/p99, `publishFailures`, `mirrorDropped`, `mirrorFailures`, `unconsumed`, `wallTimeSeconds`.
- Thresholds: exact 500/1,000/1,000 (or the overridden values), 5,000/s, p99 20 ms, zero failures/drops/unconsumed.
- Use `WaitForDrainAsync` after setup and again after warm-up. If the baseline health has non-zero `MirrorFailures` or `MirrorDropped`, fail with `setup was not clean`.

- [ ] **Step 2: Implement the large-message scenario**

One durable queue, one publisher/consumer pair, `LargeBytes`, `LargeCount`, bounded in-flight publishing (`Inflight`), same confirm/latency/ack loop. Gate on all delivered, p99 ≤20 ms, zero failures/drops. Rate is reported but not thresholded.

- [ ] **Step 3: Wire `all`**

`--scenario all` runs `combined`, then `throughput`, then `large`, each with a fresh host (unique vhost) and its own cleanup. One JSON report contains all three results. The process exits 1 if any scenario fails, unless `--no-gate`.

- [ ] **Step 4: Run the large scenario**

Run:
`dotnet run -c Release --project benchmarks\RedisMQ.Benchmarks -- --scenario large --no-gate --large-count 200 --out artifacts\perf-large.json`

Expected: 200 messages delivered, p99 reported, exit 0.

- [ ] **Step 5: Run the full envelope, ungated**

Run:
`dotnet run -c Release --project benchmarks\RedisMQ.Benchmarks -- --scenario all --no-gate --out artifacts\perf-all.json`

Expected: all three results, scale topology reaches 500/1,000/1,000, no failures. On Windows, if connections are limited, first run with lower `--connections`/`--consumers` to validate the loop, then the defaults.

- [ ] **Step 6: Run the gated envelope and record the baseline**

Run:
`dotnet run -c Release --project benchmarks\RedisMQ.Benchmarks -- --scenario all --out artifacts\perf-all.json`

Expected: exit 0, `"passed": true`. Record the measured rate, p99 confirm, p99 delivery, and the machine/Redis version from the JSON for `docs/PERFORMANCE.md` in Task 6. If the gate fails, do not lower the threshold; investigate the engine or the harness and fix the cause.

- [ ] **Step 7: Commit**

```bash
git add benchmarks
git commit -m "perf(bench): gate the combined v1 envelope"
```

---

### Task 5: CI Redis service and performance job

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 4's `--scenario all` gate and `artifacts/perf.json`.
- Produces: a `test` job with Redis and a `perf` job that fails on a missed threshold.

- [ ] **Step 1: Add manual dispatch**

Add `workflow_dispatch:` under `on:`.

- [ ] **Step 2: Give the test job Redis**

Add to the `test` job:

```yaml
    services:
      redis:
        image: redis:7
        ports:
          - 6379:6379
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 20
```

- [ ] **Step 3: Add the perf job**

```yaml
  perf:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    services:
      redis:
        image: redis:7
        ports:
          - 6379:6379
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 20
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'
      - name: Build (Release)
        run: dotnet build RedisMQ.sln -c Release
      - name: Run performance gate
        run: |
          ulimit -n 65535
          dotnet run -c Release --no-build --project benchmarks/RedisMQ.Benchmarks -- --scenario all --out artifacts/perf.json
      - name: Upload performance report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: perf-report
          path: artifacts/perf.json
          if-no-files-found: warn
```

- [ ] **Step 4: Validate the workflow file**

Run a YAML parse (for example `python -c "import yaml,pathlib; yaml.safe_load(pathlib.Path('.github/workflows/ci.yml').read_text())"`) if PyYAML is available. Otherwise inspect the indentation and rely on the push. A `perf` job timeout of 20 minutes must accommodate 500 connections and 1,000 channels.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(perf): run the envelope gate against redis"
```

---

### Task 6: Documentation, baseline and final verification

**Files:**
- Create: `docs/PERFORMANCE.md`
- Modify: `README.md`
- Modify: `docs/index.md`
- Modify: `docs/DEVELOPMENT.md`
- Modify: `docs/API.md`

**Interfaces:**
- Consumes: the gate command and baseline numbers from Task 4; the metric name from Task 1.
- Produces: user-facing instructions and a recorded baseline.

- [ ] **Step 1: Add the metric to the API catalog**

In `docs/API.md`, after `redismq.redis.mirror.duration`:

```markdown
| `redismq.redis.mirror.batch.size` | Histogram | `{command}` | — |
```

- [ ] **Step 2: Write `docs/PERFORMANCE.md`**

Cover: the envelope table; what each scenario measures; how to start Redis (`docker run --rm -p 6379:6379 redis:7`); the full command; `--no-gate`; how to read the JSON; the CI `perf` job; the measured baseline from Task 4 (machine, OS, Redis version, rate, p99 confirm, p99 delivery); and the caveats (same-host Redis, shared CI runner variance, 5,000/s defined at 1 KB, 1 MB gated on delivery/p99 only).

- [ ] **Step 3: Update the README**

- Add a `## Performance` section before `## What's Next` with the envelope and a link to `docs/PERFORMANCE.md`.
- Add its table-of-contents entry and renumber the following two entries.
- Remove the "Performance work" bullet from `## What's Next`.
- Add one sentence to `## CI` naming the perf job.
- Update the test-count table (line ~65) and the inline Redis count (~line 426) to the actual final totals from the runs below.

- [ ] **Step 4: Update `docs/index.md` and `docs/DEVELOPMENT.md`**

- Replace the performance bullet in each "remaining work" list with a sentence that the pipelined mirror and gate have landed, linking `PERFORMANCE.md`.
- Add a short "Performance gate" subsection to `docs/DEVELOPMENT.md` near the Redis section, with the local command and a pointer to the doc.
- Update the test-count table in `docs/DEVELOPMENT.md` to the actual totals.

- [ ] **Step 5: Run the whole verification set**

```powershell
dotnet build RedisMQ.sln
dotnet exec tests\RedisMQ.Broker.Tests\bin\Debug\net10.0\RedisMQ.Broker.Tests.dll -noColor
dotnet exec tests\RedisMQ.Conformance\bin\Debug\net10.0\RedisMQ.Conformance.dll -noColor
dotnet exec tests\RedisMQ.Redis.Tests\bin\Debug\net10.0\RedisMQ.Redis.Tests.dll -noColor
dotnet exec tests\RedisMQ.Hosting.Tests\bin\Debug\net10.0\RedisMQ.Hosting.Tests.dll -noColor
dotnet run -c Release --project benchmarks\RedisMQ.Benchmarks -- --scenario all --out artifacts\perf.json
dotnet list src\RedisMQ.Broker\RedisMQ.Broker.csproj package --include-transitive
```

Expected: build 0 errors; all suites pass; the gate passes; `RedisMQ.Broker` lists no packages.

- [ ] **Step 6: Commit**

```bash
git add docs/PERFORMANCE.md README.md docs/index.md docs/DEVELOPMENT.md docs/API.md
git commit -m "docs(perf): document the envelope gate and baseline"
```

---

## Acceptance

- [ ] `dotnet build RedisMQ.sln` succeeds and `RedisMQ.Broker` has zero package references.
- [ ] `RedisMQ.Broker.Tests`, `RedisMQ.Conformance`, `RedisMQ.Redis.Tests` and `RedisMQ.Hosting.Tests` all pass.
- [ ] `dotnet run -c Release --project benchmarks/RedisMQ.Benchmarks -- --scenario all` passes against local `redis:7` and writes JSON.
- [ ] The gate proves 500 connections, 1,000 consumers and 1,000 queues live at ≥5,000 confirmed publishes/s with p99 <20 ms and zero failure/drop deltas.
- [ ] README, `docs/index.md`, `docs/DEVELOPMENT.md`, `docs/API.md` and `docs/PERFORMANCE.md` reflect the landed gate.
- [ ] The branch contains one commit per task and no unrelated changes.
