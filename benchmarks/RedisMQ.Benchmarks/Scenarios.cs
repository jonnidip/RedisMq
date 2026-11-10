using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RedisMQ.Broker.Diagnostics;

namespace RedisMQ.Benchmarks;

/// <summary>One scenario's measurement: real AMQP clients against the host's broker and
/// real Redis. Every published message is confirmed; every delivery is acked and its
/// first-delivery age recorded.</summary>
internal static class Scenarios
{
    private sealed record PublishOutcome(long Confirmed, long Failed, double ElapsedSeconds);

    /// <summary>The single-queue mirror canary: one durable queue, one publisher and
    /// one consumer, bounded confirmed publishes in flight.</summary>
    public static async Task<ScenarioResult> RunThroughputAsync(
        BenchmarkHost host,
        BenchmarkOptions options,
        CancellationToken ct)
    {
        const string queue = "throughput";
        host.TrackQueue(queue);

        var factory = new ConnectionFactory
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = host.Vhost,
        };
        await using IConnection connection = await factory.CreateConnectionAsync(
            [new AmqpTcpEndpoint("127.0.0.1", host.Port)], ct);

        await using IChannel publisher = await connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: null,
                consumerDispatchConcurrency: null),
            ct);
        await publisher.QueueDeclareAsync(
            queue, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken: ct);

        await using IChannel consumerChannel = await connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                outstandingPublisherConfirmationsRateLimiter: null,
                consumerDispatchConcurrency: 4),
            ct);
        await consumerChannel.BasicQosAsync(0, 512, false, ct);

        var state = new DeliveryState();
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            // Count every delivery so warm-up completion can be awaited; only the
            // measurement window contributes latency samples.
            state.RecordDelivery(DeliveryAgeMs(delivery));
            await consumerChannel.BasicAckAsync(
                delivery.DeliveryTag, multiple: false, delivery.CancellationToken);
        };
        await consumerChannel.BasicConsumeAsync(
            queue, autoAck: false, consumerTag: "bench-throughput",
            noLocal: false, exclusive: false, arguments: null, consumer, ct);

        PublishOutcome warmup = await PublishForAsync(
            publisher, queue, options, TimeSpan.FromSeconds(options.WarmupSeconds), null, 0, ct);
        await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), ct);
        bool warmupDelivered = await state.WaitForTotalAsync(
            warmup.Confirmed, TimeSpan.FromSeconds(30), ct);

        EngineHealth baseline = host.Engine.GetEngineHealth();
        bool cleanSetup = baseline.MirrorFailures == 0
            && baseline.MirrorDropped == 0
            && warmup.Failed == 0
            && warmupDelivered;

        state.ResetMeasured();
        state.Measuring = true;
        PublishOutcome measured = await PublishForAsync(
            publisher, queue, options, TimeSpan.FromSeconds(options.DurationSeconds),
            state.ConfirmLatencies, 0, ct);
        state.Measuring = false;

        await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), ct);
        _ = await state.WaitForTotalAsync(
            warmup.Confirmed + measured.Confirmed, TimeSpan.FromSeconds(10), ct);
        EngineHealth after = host.Engine.GetEngineHealth();

        double elapsed = Math.Max(measured.ElapsedSeconds, 0.001);
        IReadOnlyList<double> confirmations = [.. state.ConfirmLatencies];
        IReadOnlyList<double> deliveries = state.DeliveryLatencies;
        var metrics = new Dictionary<string, double>
        {
            ["publishesPerSecond"] = measured.Confirmed / elapsed,
            ["deliveriesPerSecond"] = state.MeasuredDelivered / elapsed,
            ["publishConfirmP50Ms"] = Stats.Percentile(confirmations, 50),
            ["publishConfirmP99Ms"] = Stats.Percentile(confirmations, 99),
            ["deliveryP50Ms"] = Stats.Percentile(deliveries, 50),
            ["deliveryP99Ms"] = Stats.Percentile(deliveries, 99),
            ["publishFailures"] = warmup.Failed + measured.Failed,
            ["mirrorDropped"] = after.MirrorDropped - baseline.MirrorDropped,
            ["mirrorFailures"] = after.MirrorFailures - baseline.MirrorFailures,
            ["unconsumed"] = Math.Max(0, measured.Confirmed - state.MeasuredDelivered),
        };
        var thresholds = new Dictionary<string, double>
        {
            ["minPublishesPerSecond"] = 5000,
            ["maxDeliveryP99Ms"] = 20,
        };

        ScenarioResult result = Gate.Evaluate("throughput", metrics, thresholds);
        if (!cleanSetup)
        {
            result = Gate.Fail(
                result,
                "setup was not clean: " +
                $"mirrorFailures={baseline.MirrorFailures}, " +
                $"mirrorDropped={baseline.MirrorDropped}, " +
                $"warmupFailures={warmup.Failed}, " +
                $"warmupDelivered={warmupDelivered}");
        }

        return result;
    }

    /// <summary>Publishes as fast as confirms allow with at most
    /// <see cref="BenchmarkOptions.Inflight"/> outstanding, for <paramref name="duration"/>.
    /// Only confirmed publishes count toward the rate.</summary>
    private static async Task<PublishOutcome> PublishForAsync(
        IChannel channel,
        string queue,
        BenchmarkOptions options,
        TimeSpan duration,
        ConcurrentQueue<double>? confirmLatencies,
        long startSequence,
        CancellationToken ct)
    {
        var outstanding = new List<Task<bool>>(options.Inflight);
        long confirmed = 0;
        long failed = 0;
        long sequence = startSequence;
        var clock = Stopwatch.StartNew();

        while (clock.Elapsed < duration)
        {
            while (outstanding.Count < options.Inflight && clock.Elapsed < duration)
            {
                byte[] body = CreateBody(sequence++, options.PublishBytes);
                outstanding.Add(PublishOneAsync(channel, queue, body, confirmLatencies));
            }

            Task<bool> completed = await Task.WhenAny(outstanding);
            outstanding.Remove(completed);
            if (await completed)
            {
                confirmed++;
            }
            else
            {
                failed++;
            }
        }

        foreach (Task<bool> task in outstanding)
        {
            if (await task)
            {
                confirmed++;
            }
            else
            {
                failed++;
            }
        }

        return new PublishOutcome(confirmed, failed, clock.Elapsed.TotalSeconds);
    }

    private static async Task<bool> PublishOneAsync(
        IChannel channel,
        string queue,
        byte[] body,
        ConcurrentQueue<double>? confirmLatencies)
    {
        long started = Stopwatch.GetTimestamp();
        try
        {
            await channel.BasicPublishAsync("", queue, mandatory: false, new BasicProperties(), body);
            confirmLatencies?.Enqueue(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            return true;
        }
        catch (Exception)
        {
            // A nack, return or lost channel. The gate checks the failure count.
            return false;
        }
    }

    /// <summary>8-byte sequence then the publisher's 8-byte timestamp, zero-padded to
    /// the configured body size. The same host consumes it, so Stopwatch timestamps are
    /// comparable.</summary>
    private static byte[] CreateBody(long sequence, int size)
    {
        byte[] body = new byte[Math.Max(16, size)];
        BinaryPrimitives.WriteInt64LittleEndian(body, sequence);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), Stopwatch.GetTimestamp());
        return body;
    }

    private static double DeliveryAgeMs(BasicDeliverEventArgs delivery)
    {
        if (delivery.Body.Length < 16)
        {
            return 0;
        }

        long timestamp = BinaryPrimitives.ReadInt64LittleEndian(delivery.Body.Span[8..16]);
        return Stopwatch.GetElapsedTime(timestamp).TotalMilliseconds;
    }

    /// <summary>Counters shared by the consumers and the measurement loop.</summary>
    private sealed class DeliveryState
    {
        private readonly ConcurrentQueue<double> _deliveryLatencies = new();
        private long _totalDelivered;
        private long _measuredDelivered;
        private volatile bool _measuring;

        public ConcurrentQueue<double> ConfirmLatencies { get; } = new();

        public bool Measuring
        {
            get => _measuring;
            set => _measuring = value;
        }

        public long MeasuredDelivered => Interlocked.Read(ref _measuredDelivered);

        public IReadOnlyList<double> DeliveryLatencies => [.. _deliveryLatencies];

        public void RecordDelivery(double latencyMs)
        {
            Interlocked.Increment(ref _totalDelivered);
            if (_measuring)
            {
                Interlocked.Increment(ref _measuredDelivered);
                _deliveryLatencies.Enqueue(latencyMs);
            }
        }

        public void ResetMeasured()
        {
            Interlocked.Exchange(ref _measuredDelivered, 0);
            while (_deliveryLatencies.TryDequeue(out _))
            {
            }
        }

        public async Task<bool> WaitForTotalAsync(long count, TimeSpan timeout, CancellationToken ct)
        {
            DateTime deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (Interlocked.Read(ref _totalDelivered) >= count)
                {
                    return true;
                }

                await Task.Delay(25, ct);
            }

            return Interlocked.Read(ref _totalDelivered) >= count;
        }
    }
}
