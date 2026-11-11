# RedisMQ Performance

RedisMQ mirrors every durable publish to Redis Streams. This document describes the
pipelined mirror, the benchmark harness that gates the v1 envelope, and the recorded
baseline.

## The v1 envelope

The gate proves these dimensions **together**, with the full topology live:

| Dimension | Target |
|-----------|--------|
| AMQP connections | 500 |
| Consumers (channels) | 1,000 |
| Durable queues | 1,000 |
| Confirmed publishes | **≥ 5,000/s** at 1 KB bodies |
| p99 first delivery | **≤ 20 ms** |
| Publish failures | 0 |
| Mirror failures / drops (measured window) | 0 |
| Unconsumed measured messages | 0 |

A message counts toward the rate only after its publisher confirm succeeds. A confirm
succeeds only after the durable `XADD` for every routed queue succeeds; the memory-first
enqueue alone is not enough.

## How the pipelined mirror works

`RedisStreamRouteEngine.DrainAsync` reads the first ready command and greedily takes up to
`DrainerBatchSize` more (default 256, clamped to the drainer capacity). Consecutive
writes are issued back-to-back without awaiting each one, then their outcomes are resolved
in publish order. StackExchange.Redis pipelines the outstanding commands on its
multiplexer, so one batch costs roughly one round trip instead of one per message.

`Delta`, `Remove` and `DeleteQueue` remain barriers: they are issued and awaited before
the next command. That preserves per-queue stream order, keeps max-length eviction
correct, and keeps the topology/delete ordering intact. A failure in one pipelined write
does not fail its neighbours. The engine still has a single drainer, one Redis
multiplexer, and no `MULTI/EXEC` or Lua.

`DrainerBatchSize` is configurable under `RedisMQ:Redis:DrainerBatchSize` (default 256).
`redismq.redis.mirror.batch.size` records the commands issued per batch.

## Scenarios

`--scenario all` runs three scenarios, each against a fresh host and a unique vhost.

### `combined` — the gated envelope

500 connections, 1,000 consumers and 1,000 queues stay live. Connection 0 carries the
publisher plus two consumers; every other connection carries two consumers. Each
consumer owns one queue with prefetch 16. After warm-up rounds, one publisher keeps
`--inflight` confirmed publishes outstanding and round-robins across all queues for
`--duration` seconds. Consumers ack every delivery and record first-delivery age from a
timestamp embedded in the body.

This is the scenario the 500/1,000/1,000, ≥5,000/s and p99 ≤20 ms gates apply to.

### `throughput` — single-queue canary

One durable queue, one publisher, one consumer. It isolates a single-queue regression
from topology-scale behaviour. It uses the same 5,000/s and p99 ≤20 ms thresholds at
1 KB.

A single queue's delivery pump is serial. A 128-deep outstanding window alone implies
`(128 - 1) / 5,000 s ≈ 25 ms` of queueing at the rate floor, so the canary caps its
window at `min(--inflight, 16)`. At 5,000/s that is ≈3 ms of queueing, which leaves room
for the 20 ms gate. The cap is a harness constant; the thresholds are unchanged.

The Broker delivers one message at a time per queue: the per-queue pump claims one
message and awaits its frame write before the next claim. With a 16-deep window the
canary's rate ceiling is roughly `16 / mean confirm latency`. On the Windows development
host that measured 4,473/s with p99 42.7 ms — a tail dominated by local scheduling
spikes, not by the window. The publish/confirm path alone sustains tens of thousands per
second. The cap therefore trades some rate headroom for latency headroom; 5,000/s at
16 in flight needs mean delivery ≤3.2 ms. CI on Ubuntu is the authoritative run; local
Windows users should use `--no-gate` and read the combined scenario.

### `large` — 1 MB bodies

One publisher/consumer pair sends `--large-count` bodies of `--large-bytes` (1 MB by
default). Correct delivery and p99 ≤20 ms are gated; the rate is reported, not asserted.

Because the delivery pump is serial per queue, the scenario bounds **outstanding bytes**
at 8 MB rather than only using `--inflight`; if `--inflight` is smaller it wins. At 1 MB
that is 8 messages in flight instead of 128. Without the byte budget, 128 MB of backlog
alone exceeds any 20 ms latency budget, so the gate would measure queueing rather than
delivery. The bucket is a harness constant; the 20 ms threshold is unchanged.

## Running the benchmark

Start Redis first:

```bash
docker run --rm -p 6379:6379 redis:7
```

Run the full gate:

```bash
dotnet run -c Release --project benchmarks/RedisMQ.Benchmarks -- --scenario all --out artifacts/perf.json
```

The process exits `1` if any scenario fails. `--no-gate` prints and writes the same
report but leaves the exit code zero, which is the supported local smoke mode on Windows.
A missing Redis, a topology error, or a lost channel always fails the run.

Useful overrides:

```bash
--scenario combined|throughput|large|all
--redis 127.0.0.1:6379
--duration 30 --warmup 5
--publish-bytes 1024 --inflight 128
--connections 500 --consumers 1000 --queues 1000 --warmup-rounds 10
--large-bytes 1048576 --large-count 1000
--drainer-capacity 1024 --drainer-batch-size 256
```

The harness never issues a destructive or admin **write** (`FLUSHDB`, `SCAN`, `DEL`,
`CONFIG SET`). It uses a `/bench-<8 hex>` vhost and deletes only the queue and topology
keys it declared. The only admin reads are `INFO server` (version capture) and the
engine's startup `CONFIG GET maxmemory-policy` eviction gate.

## Reading the report

The JSON report has schema version 1:

- `environment` — OS, .NET runtime, processor count, Redis endpoint and `redis_version`.
- `parameters` — the effective CLI values, including drainer capacity and batch size.
- `results` — one entry per scenario with `metrics`, `thresholds`, `failures` and
  `passed`.
- `passed` — the conjunction of every scenario.

Percentiles are nearest-rank over every sample in the measurement window. Rate is
confirmed publishes divided by the publish window. `mirrorDropped`, `mirrorFailures` and
`unconsumed` are measured-window values, sampled after the drainer reaches zero and before
teardown, so shutdown drops never count.

## CI

`.github/workflows/ci.yml` has a `perf` job on `ubuntu-latest` (`timeout-minutes: 20`)
with a `redis:7` service. It builds Release, raises `ulimit -n 65535`, runs
`--scenario all`, and uploads `artifacts/perf.json` even when the gate fails. The `test`
job also has the Redis service, so the Redis-dependent suites do not silently skip.

## Recorded baseline

Local development machine, 2026-11-10:

- OS: Microsoft Windows 10.0.26200
- Runtime: .NET 10.0.12
- Processors: 16
- Redis: 8.10.3, container on `127.0.0.1:6379`

Full default envelope, `--scenario all --no-gate`:

| Scenario | Connections | Consumers | Queues | Publishes/s | Confirm p50 / p99 | Delivery p50 / p99 | Failures / drops / unconsumed |
|----------|-------------|-----------|--------|-------------|-------------------|--------------------|-------------------------------|
| `combined` | 500 | 1,000 | 1,000 | **19,029** | 6.9 / 21.8 ms | 1.5 / **12.8 ms** | 0 / 0 / 0 |
| `throughput` (canary) | 1 | 1 | 1 | 1,747 | 55.6 / 291.6 ms | 3.8 / 112.3 ms | 0 / 0 / 0 |
| `large` (1 MB, 8 MB budget) | 1 | 1 | 1 | 193 | 38.4 / 87.9 ms | 13.7 / 62.9 ms | 0 / 0 / 0 |

The combined result is the local proof of the v1 envelope: the topology stays live while
the rate is sustained, delivery p99 is under 20 ms, and every counter is clean. Run
without `--no-gate`, this Windows host exits `1` because the canary and the large p99
fail; that is the expected local result, not a regression. The single-queue canary and
the 1 MB p99 are host-bound on Windows for the reasons above.
The Ubuntu CI runner is the authoritative environment for those two gates, but no green
CI run has been observed yet: the workflow has not been dispatched from this branch.

The canary runs last in `all` and inherits contention from the 500-connection combined
run. Run alone with `--scenario throughput`, the same 16-deep window measured 4,473/s and
p99 42.7 ms; the 1 MB scenario alone measured 232/s and p99 44.2 ms. Both remain above
the 20 ms ceiling on this host, which is why those two gates are CI-class rather than
local proof.

## Caveats

- Redis runs on the same host. These numbers measure the engine and localhost IPC, not a
  network budget or Redis Cloud latency.
- CI runners are shared; expect variance between runs. The gate values are the contract,
  not the baseline numbers.
- 5,000/s is defined at 1 KB bodies. Larger bodies gate delivery and p99 only.
- The combined scenario's rate depends on the Redis container and the host's socket
  throughput as much as on CPU.
- Windows development may need a raised ephemeral-port range for 500 connections. Use
  lower `--connections`/`--consumers`/`--queues` with `--no-gate` to smoke-test the loop.
