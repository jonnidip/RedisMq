# RedisMQ performance slice — design

Date: 2026-11-10 · Status: awaiting approval

## Intent and why

The broker is functionally complete for v1, but the Redis mirror writes one command per
round trip: `RedisStreamRouteEngine.DrainAsync` awaits one `XADD` (or `XDEL`) at a time,
and a publisher's confirm resolves only after that write. Throughput therefore tracks
Redis round-trip latency rather than CPU, and no harness proves the scale envelope the
v1 design accepts.

This slice makes the mirror pipelined, proves the user-confirmed envelope against a local
containerized Redis, and adds a repeatable gate so later work cannot silently regress it.
It does not add protocol features or change durability semantics.

## Goals

- G1 — The drainer issues independent Redis writes without awaiting each one, then
  resolves their outcomes in order. Publisher confirms keep their current meaning: `true`
  only after the message's `XADD` succeeded.
- G2 — Per-queue ordering, delta ordering, failure isolation, backpressure, shutdown
  accounting and observability behave exactly as they do today.
- G3 — A console benchmark project drives real AMQP clients against a broker backed by a
  real Redis and reports publish throughput, publisher-confirm latency, end-to-end
  first-delivery latency, and scale results as machine-readable JSON.
- G4 — One command runs the combined v1 envelope gate: 500 concurrent connections,
  1,000 consumers and 1,000 queues live simultaneously while ≥5,000 confirmed publishes/s
  are sustained at 1 KB, with p99 first delivery <20 ms and zero mirror failures or
  drops during the measurement.
- G5 — CI runs the gate against a Redis service container on every push and pull request,
  uploads the JSON report, and fails on a missed threshold.
- G6 — Existing unit, conformance and hosting tests stay green. New tests pin batching
  order, pipelined failure isolation and configuration binding.

## Non-goals

- Changing the wire protocol, confirm semantics, or the AMQP surface.
- Consumer-side pipelining, socket batching or new delivery scheduling. This slice
  changes the Redis mirror, not the AMQP write path.
- Multiple drainers, separate Redis connections, Lua, `MULTI/EXEC`, or a WAL redesign.
- Redis Cloud or cross-host latency budgets. The gate is local-container Redis, as the
  v1 design states.
- TLS in the benchmark. TLS is pinned by conformance tests; the gate measures the
  engine, not the TLS stack.
- Tuning every queue/consumer combination. The envelope numbers are the gate; other
  shapes are reported by ad-hoc runs.
- Operational hardening, certificate hot-reload, profiles, or a README rewrite.

## Anti-goals

- A benchmark that measures the client library's own queueing instead of the broker:
  load must keep a bounded, configurable number of publishes in flight and must count only
  confirmed publishes.
- A gate that passes by dropping, nacking or silently skipping messages. Any publish
  failure, mirror drop, mirror failure or undelivered measured message fails the gate.
- "Pipelining" that reorders one queue's stream, or that reports a confirm before the
  corresponding write completed.
- A benchmark that needs a dedicated Redis instance or `FLUSHDB`. It must be safe to run
  against a shared development Redis.

## Constraints

- `RedisMQ.Broker` keeps zero `PackageReference`s.
- `RedisMQ.Redis` keeps StackExchange.Redis as its only package reference.
- Redis key layout, hash tags and the `IRouteEngine` synchronous seam stay unchanged.
- The drainer remains a single task. Parallel Redis connections are not introduced.
- The gate runs on `ubuntu-latest` with a `redis:7` service, and locally on Windows
  against a container or native Redis on `127.0.0.1:6379`.
- No wall-clock sleeps for correctness in tests. Benchmarks use deadlines and counters;
  their measurement windows are deliberate timing, not correctness dependencies.
- Local development has Windows PowerShell 5.1 and the known SDK test-runner issue; the
  repository's existing `dotnet exec` test invocation remains the way to run xUnit
  locally. The benchmark itself is a normal `dotnet run` console app.

## Approaches considered

1. **Pipelined drainer plus a benchmark harness. Chosen.** Keep the single drainer and
   memory-first engine; read a bounded window of ready commands, issue their Redis writes
   back-to-back, then await the returned tasks in order and resolve each command's
   outcome. StackExchange.Redis already pipelines commands issued on its multiplexer, so
   no new connection or batch API is needed. This removes the per-command round-trip stall
   while preserving every existing guarantee, and the harness measures the result.

2. **Partitioned drainer lanes.** Several drainer tasks, each owning a subset of queues,
   with topology pinned to lane zero. It adds concurrency but not network parallelism:
   one multiplexer already serializes writes, and partition assignment plus cross-lane
   delta ordering is new complexity for little gain. Rejected.

3. **Weaken confirm semantics.** Confirm transient publishes on memory append and confirm
   durable publishes after a periodic mirror flush. This is cheap throughput, but it
   changes the durability contract the README and design promise. Rejected.

The user chose approach 1 in conversation on 2026-11-10.

## Design

### 1. Pipelined drainer

#### Configuration

`RedisStreamRouteEngine` gains a trailing constructor parameter:

```csharp
public RedisStreamRouteEngine(
    IDatabase? redis,
    IReadOnlyList<string> vhosts,
    bool replayOnStart = true,
    int drainerCapacity = 1024,
    bool enforceEvictionGate = true,
    BrokerDiagnostics? diagnostics = null,
    int drainerBatchSize = 256)
```

Adding it last keeps every existing positional call valid. The value is clamped to
`[1, drainerCapacity]`. `RedisEngineOptions` gains
`public int DrainerBatchSize { get; set; } = 256;`, bound from
`RedisMQ:Redis:DrainerBatchSize`, and `RedisMqServiceCollectionExtensions.CreateEngine`
passes it through.

The batch size is a maximum, never a target. The drainer takes the first command with
`ReadAllAsync`, then greedily `TryRead`s up to the limit. There is no timer and no delay:
under light load a batch is one command, so latency is unchanged; under load the batch
grows to the limit.

#### Issue and completion phases

Extract today's per-command body from `DrainAsync` into two helpers:

- `IssueAsync(WriteCommand)` returns the `Task` from `WriteCommandToRedisAsync` without
  awaiting it. Calling an `async` method runs it synchronously to its first await, which
  issues the Redis operation and returns.
- `CompleteAsync(WriteCommand, Task, long started)` awaits the task inside try/catch,
  records success/failure and mirror health exactly as today, releases the reserved slot,
  emits the command counter and duration, and completes the publisher's
  `TaskCompletionSource`.

For each batch:

1. If cancellation was requested before issue, abandon every buffered command.
2. Walk the batch in order. Consecutive pipelineable commands are issued back-to-back into
   a pending list, then awaited one by one with `CompleteAsync`.
3. Barriers are issued and awaited before the next command is issued:
   - `Delta` — its children are order-dependent (`XADD` before the corresponding `XDEL`,
     both on the same queue key) and today a failed child aborts the remaining children.
     Keeping the existing recursive, sequential `WriteCommandToRedisAsync` path preserves
     both properties. Deltas are rare on the hot path; acks and requeues normally mirror
     as a single `RemoveStreamIds` command.
   - `Remove` — it resolves envelope ids to stream ids from `_streamIds` at issue time.
     Those entries are written only after the corresponding `XADD` completes. Issuing
     `[XADD E1, XADD E2(evicts E1), Remove E1]` without awaiting would resolve `Remove`
     before E1's map entry exists, silently leak E1 in Redis, and resurrect it on replay.
     Flushing the pending list before a `Remove` closes that race. `Remove` appears only
     for `x-max-length` evictions, not on the ordinary publish path.
   - `DeleteQueue` — it writes the topology log and then deletes the queue key. When the
     topology key and queue key map to different cluster slots, issuing both without
     awaiting lets the delete race the deletion record. The barrier costs nothing outside
     queue deletion.
4. A failure in one pipelineable command never prevents its neighbours from completing.
   That matches independent commands today. Failure handling inside a delta keeps the
   existing abort-the-rest behavior.

`WriteCommandToRedisAsync` keeps its shape: an `Enqueue` calls `StreamAddAsync` and,
after the await, records the returned stream id in `_streamIds`. The stream-id map is
still written only by the drainer. `Remove`, `RemoveStreamIds`, topology writes, consumer
registration and `Delta` recursion are unchanged.

#### Ordering

All keys for a queue share the hash tag `{mq:q:<vhost>:<queue>}`, so they map to one
cluster slot and one Redis server; topology keys share `{mq:top:<vhost>}`. StackExchange.Redis
writes commands to a server's connection in the order they are issued, and the drainer
issues in channel order. Therefore the `XADD`s for a queue execute in publish order, and
no cross-key ordering is relied on. This is the same assumption the current sequential
drainer already makes about the multiplexer; the slice documents it and pins it with a
test.

#### Shutdown

`DisposeAsync` still cancels, awaits the drainer, then abandons whatever remains in the
channel. A batch already read finishes issuing and completes; commands not yet issued
when cancellation is observed are abandoned through the existing `AbandonWrite` path
(slot released, publisher told `false`, counted as `reason=shutdown`). This is strictly
fewer dropped commands than today.

#### Observability

Add one instrument:

- `redismq.redis.mirror.batch.size`, histogram, unit `{command}`, recorded once per
  batch in `DrainAsync`.

Existing `redismq.redis.mirror.commands`, `.duration`, `.dropped`,
`redismq.redis.drainer.saturated`, and the depth gauge keep their names and tags.
`_diagnostics.Measure` is used the same way as the existing histograms.

### 2. Benchmark harness

New project `benchmarks/RedisMQ.Benchmarks`, an executable console app (`net10.0`,
`IsPackable=false`), added to `RedisMQ.sln`. References: `RedisMQ.Broker`,
`RedisMQ.Redis`, `RabbitMQ.Client` 7.2.2, `StackExchange.Redis` 2.8.16. It does not
reference the test projects.

Layout:

- `Program.cs` — argument parsing, scenario dispatch, exit code.
- `BenchmarkHost.cs` — Redis multiplexer, `RedisStreamRouteEngine` on a unique vhost,
  listeners from `BrokerListeners.Create`, `AmqpBrokerServer`, start/stop and key cleanup.
- `Scenarios.cs` — throughput, scale and large-message scenarios.
- `Stats.cs` — percentile and rate helpers.
- `PerfReport.cs` — JSON DTOs, console table, threshold evaluation.

The host mirrors `tests/RedisMQ.Conformance/TestBrokerHost`: bind port 0, guest/guest,
vhost `/bench-<hex>`, `replayOnStart: false`, eviction gate on. The engine is
`IAsyncDisposable`-only; the host stops the server, disposes listeners, then disposes the
engine. Cleanup deletes each declared queue key and the vhost's topology key. No
`FLUSHDB` and no admin commands.

#### CLI

```
dotnet run -c Release --project benchmarks/RedisMQ.Benchmarks -- \
  [--scenario all|throughput|combined|large] \
  [--redis 127.0.0.1:6379] \
  [--duration 30] [--warmup 5] \
  [--publish-bytes 1024] [--inflight 128] \
  [--connections 500] [--consumers 1000] [--queues 1000] [--warmup-rounds 10] \
  [--large-bytes 1048576] [--large-count 1000] \
  [--out artifacts/perf.json] [--no-gate]
```

Defaults match the envelope. `--no-gate` reports without failing on thresholds; the exit
code is then zero unless the run itself failed. Missing Redis, topology errors, publish
nacks, dropped messages and undelivered messages always fail the run. Two further
options expose the shipped engine knobs for diagnosis: `--drainer-capacity` (default
1024) and `--drainer-batch-size` (default 256).

#### Setup, settling and health sampling

The drainer channel drops rather than blocks when full, and teardown abandons queued
commands. A gate on absolute `mirrorDropped` would therefore be flaky and would count
shutdown drops. Instead:

- After topology declaration and again after consumer registration, the host polls
  `GetEngineHealth().DrainerDepth` every 50 ms until it has been zero continuously for
  500 ms, or a 30-second deadline passes. A single zero sample is not enough: the drainer
  removes a batch from the channel before writing it. This is a deadline wait, not a
  correctness sleep.
- Before the measurement window, capture `GetEngineHealth()`: mirror failures, mirror
  drops, and the command counters.
- After publishing and delivery complete, poll the depth to zero again and capture
  health once more, **before** stopping the server or disposing the engine.
- The gate evaluates the deltas between those two samples, so teardown and unregistration
  never contribute. A drop or failure during setup is reported and fails the run with a
  clear "setup was not clean" message, because it means topology or consumer state reached
  Redis incompletely.
- The harness uses the shipped drainer capacity and batch size by default. Raising them
  is a diagnosis option, not the gated configuration.

#### Throughput and latency scenario

This single-queue scenario is a canary for the mirror path, not the envelope gate. `all`
runs it alongside the combined scenario so a single-queue regression is isolated from a
topology-scale one.

- One durable queue, direct exchange, publisher connection and channel created with
  `CreateChannelOptions(publisherConfirmationsEnabled: true,
  publisherConfirmationTrackingEnabled: true)`; `--inflight` publishes outstanding at a
  time.
- Body: 8-byte sequence + 8-byte `Stopwatch.GetTimestamp()` timestamp, padded to
  `--publish-bytes`.
- One consumer channel, `BasicQosAsync(prefetch: 512)`, dispatch concurrency 4. On each
  delivery: compute age from the embedded timestamp, ack immediately, store the latency.
- Warm-up for `--warmup` seconds; reset counters; measure for `--duration` seconds.
  Publishing stops at the deadline; wait up to 10 seconds for measured messages to be
  consumed.
- Metrics: confirmed publishes/s, deliveries/s, publish failure count, publisher-confirm
  p50/p99, first-delivery p50/p99, and the health deltas described above.
- Gate at 1 KB: `publishesPerSecond >= 5000`, `deliveryP99Ms <= 20`, zero failures, zero
  drops, and every measured message consumed.

5,000/s is defined at 1 KB bodies. The 1 MB scenario gates correct delivery and p99, not
rate: 5,000 × 1 MB/s is not a meaningful local budget. This is a builder call; if the
intent is a rate gate at a larger typical size, the threshold moves.

#### Combined envelope scenario

This is the gated scenario. It proves the dimensions together, not piecewise: the full
topology stays live while the rate is sustained.

- 500 AMQP connections total. Connection 0 carries the publisher channel and two consumer
  channels; each remaining connection carries two consumer channels. That yields exactly
  1,000 consumers with 500 connections.
- Declare 1,000 durable queues, two per connection. Each consumer channel consumes one
  queue with prefetch 16.
- Warm-up: `--warmup-rounds` rounds (default 10), publishing one timestamped message to
  every queue per round with confirms, then waiting for all deliveries with a deadline.
  Warm-up samples are discarded.
- Measurement: for `--duration` seconds, the publisher channel keeps `--inflight`
  confirmed publishes outstanding and round-robins across all 1,000 queues. There is no
  artificial rate cap; it publishes as fast as confirms allow. Consumers ack every
  delivery and record first-delivery latency. At the deadline publishing stops and the
  harness waits for every measured message to be consumed.
- Metrics: confirmed publishes/s, deliveries/s, established connections, active consumer
  count, declared queue count, measured delivered count, publisher-confirm p50/p99,
  first-delivery p50/p99, failures, health deltas, wall time.
- Gate: 500 connections, 1,000 consumers, 1,000 queues,
  `publishesPerSecond >= 5000`, `deliveryP99Ms <= 20`, every measured message delivered,
  and zero failure/drop deltas.
- On Linux, raise the file-descriptor limit in the same shell (`ulimit -n 65535`). CI does
  this. Windows development may need a raised ephemeral-port range; the documented
  workaround is to lower `--connections` and the rate expectation with `--no-gate` for
  local smoke runs.

#### Large-message scenario

- One publisher/consumer pair, `--large-bytes` (1 MB), `--large-count` messages,
  bounded in-flight publishing and the same delivery/ack loop.
- Gate: every message delivered, p99 ≤20 ms, zero failures/drops; rate is reported but
  not asserted.

#### Report

JSON schema version 1:

```json
{
  "schemaVersion": 1,
  "generatedAt": "2026-11-10T12:00:00Z",
  "scenario": "all",
  "environment": {
    "os": "...", "framework": "...", "processorCount": 8,
    "redisEndpoint": "127.0.0.1:6379", "redisVersion": "7.x"
  },
  "parameters": { "publishBytes": 1024, "durationSeconds": 30, "connections": 500 },
  "results": [
    {
      "name": "combined",
      "passed": true,
      "metrics": {
        "connections": 500, "consumers": 1000, "queues": 1000,
        "publishesPerSecond": 0, "deliveriesPerSecond": 0,
        "publishConfirmP50Ms": 0, "publishConfirmP99Ms": 0,
        "deliveryP50Ms": 0, "deliveryP99Ms": 0,
        "publishFailures": 0, "mirrorDropped": 0, "mirrorFailures": 0
      },
      "thresholds": {
        "connections": 500, "consumers": 1000, "queues": 1000,
        "minPublishesPerSecond": 5000, "maxDeliveryP99Ms": 20
      }
    },
    {
      "name": "throughput",
      "passed": true,
      "metrics": {
        "publishesPerSecond": 0, "deliveriesPerSecond": 0,
        "publishConfirmP50Ms": 0, "publishConfirmP99Ms": 0,
        "deliveryP50Ms": 0, "deliveryP99Ms": 0,
        "publishFailures": 0, "mirrorDropped": 0, "mirrorFailures": 0
      },
      "thresholds": { "minPublishesPerSecond": 5000, "maxDeliveryP99Ms": 20 }
    }
  ],
  "passed": true
}
```

Console output is a short table per scenario plus explicit failure reasons. Percentiles
are computed by sorting all samples; 30 seconds at 5,000/s is 150,000 samples, about
1.2 MB, acceptable for a benchmark process.

### 3. Tests

Redis-dependent tests skip when Redis is unreachable, following
`RedisStreamRouteEngineTests.TryConnectRedis`. Add to `tests/RedisMQ.Redis.Tests`:

1. `Pipelined_batch_preserves_per_queue_stream_order` — publish 2,000 sequenced messages,
   await every receipt, `XRANGE` the queue stream and assert the `env` field order matches
   publish order and every receipt is `true`.
2. `Pipelined_batch_failure_does_not_fail_neighbours` — install a
   `WriteOverrideForTest` that throws only for one envelope id, publish a burst, assert
   that one receipt is `false`, its neighbours are `true`, and the mirror failure counter
   is one.
3. `Max_length_eviction_under_pipelined_burst_is_mirrored` — declare a queue with
   `x-max-length`, publish a burst larger than the limit without awaiting each receipt,
   await them all, then settle and assert the stream holds only live envelopes and no
   evicted entry leaked. This pins the `Remove` barrier.
4. `Drainer_issues_more_than_one_write_before_completion` — an override blocks each write
   on a gate and records the number issued before the gate opens; with batch size 16 and a
   burst of 64, assert more than one write was issued while the first was blocked and no
   more than 16. This pins the pipelining behavior without timing thresholds.
5. `Shutdown_completes_issued_batch_and_abandons_the_rest` — block the drainer on a gate,
   queue more than one batch, dispose, and assert every publisher's durability task
   completes (`true` for issued commands, `false` for abandoned ones) rather than hanging.
6. Existing backpressure, replay, delta and observability tests are updated only where
   the batch window changes their assumptions; their assertions stay semantically
   identical.

Add to `tests/RedisMQ.Hosting.Tests`:

7. `Drainer_batch_size_binds_from_configuration` — `RedisMQ:Redis:DrainerBatchSize: 32`
   produces a `RedisStreamRouteEngine` whose configuration path is exercised. If the
   engine's batch size is not externally readable, assert through
   `RedisMqOptions.Redis.DrainerBatchSize` binding and a constructed engine, following the
   existing configuration-binding test style.

No test asserts a wall-clock throughput number; the gate does that.

### 4. CI and docs

`.github/workflows/ci.yml` gains a `perf` job on `ubuntu-latest`,
`timeout-minutes: 20`, with a `redis:7` service on port 6379 and a ping health check:

```yaml
- name: Run performance gate
  run: |
    ulimit -n 65535
    dotnet run -c Release --no-build --project benchmarks/RedisMQ.Benchmarks -- \
      --scenario all --out artifacts/perf.json
```

The job builds Release first, always uploads `artifacts/perf.json`, and fails when the
gate fails. Triggers stay as today (push and pull request), plus `workflow_dispatch` for
manual runs.

The existing `test` job also gains a `redis:7` service on port 6379. Today every
Redis-dependent test returns early because no service is present, so the ordering,
failure-isolation and shutdown guarantees this slice introduces would be unexercised in
CI. With the service, `tests/RedisMQ.Redis.Tests` and the Redis-backed hosting tests run
on every push and pull request. The local skip behavior stays for developer machines
without Redis.

Docs:

- New `docs/PERFORMANCE.md`: the envelope, what each scenario measures, how to run it
  locally (`docker run --rm -p 6379:6379 redis:7`), how to read the JSON, the CI job, and
  the recorded baseline from the implementation run.
- `README.md`: add a short `## Performance` section and table-of-contents entry linking
  to `docs/PERFORMANCE.md`; replace the "Performance work" What's Next bullet with a
  statement that the gate landed; mention the perf job in `## CI`. No broader rewrite.
- `docs/index.md` and `docs/DEVELOPMENT.md`: replace the performance bullet with the
  landed gate and a link; add local run instructions to DEVELOPMENT.
- `docs/API.md`: add `redismq.redis.mirror.batch.size` to the metric catalog, since
  diagnostics are documented as part of the public contract.
- `docs/superpowers/specs/2026-11-05-redismq-packaging-design.md` and other dated specs
  keep their original wording as records.

## What's left to the builder

- Exact argument-parser shape and JSON property order.
- File split inside the benchmark project, provided the responsibilities above are
  separate.
- Percentile implementation (sort-and-index) and whether to keep samples in a `long[]`
  or a reusable buffer.
- Whether the combined scenario reuses a single publishing channel for all queues
  (expected) and how connections are distributed when parameters are overridden.
- The precise place of the `## Performance` section in the README, subject to the
  existing table of contents.
- Test names, subject to the existing naming style.
- Whether `DrainerBatchSize` should be exposed for assertions or only through options
  binding.

## Calls made during review

- `Remove` is a barrier because it resolves envelope ids at issue time; without it,
  max-length evictions can leak under batching.
- The gate is combined: rate and p99 are measured with the full 500/1,000/1,000 topology
  live, not as separate scenarios.
- 5,000/s is defined at 1 KB bodies; the 1 MB scenario gates delivery and p99 only.
- Mirror health is evaluated as a delta over the measured window, after draining to zero
  and before disposal, so teardown drops do not count.
- CI's `test` job gains Redis so the new Redis tests are not silent skips.
- The new batch-size histogram is added to `docs/API.md`, not only to the performance doc.

## Acceptance

- `dotnet build RedisMQ.sln` succeeds; `RedisMQ.Broker` still has zero package
  references.
- All existing tests pass, plus the new Redis and hosting tests when Redis is available.
- `dotnet run -c Release --project benchmarks/RedisMQ.Benchmarks -- --scenario all`
  passes the envelope against a local `redis:7` container and writes the JSON report.
- The CI `perf` job passes on the same commit and uploads its report.
- README, `docs/index.md` and `docs/DEVELOPMENT.md` point at the gate and no longer
  list performance as future work.
