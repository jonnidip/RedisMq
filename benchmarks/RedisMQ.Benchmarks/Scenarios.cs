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

        ConnectionFactory factory = CreateFactory(host);
        await using IConnection connection = await ConnectAsync(factory, host, ct);

        await using IChannel publisher = await CreatePublisherAsync(connection, ct);
        await publisher.QueueDeclareAsync(
            queue, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken: ct);

        await using IChannel consumerChannel = await CreateConsumerChannelAsync(connection, 4, ct);
        await consumerChannel.BasicQosAsync(0, 512, false, ct);

        var state = new DeliveryState();
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            // Count every delivery so warm-up completion can be awaited; only the
            // measurement window contributes latency samples.
            state.RecordDelivery(DeliveryAgeMs(delivery));
            await AckAsync(consumerChannel, delivery);
        };
        await consumerChannel.BasicConsumeAsync(
            queue, autoAck: false, consumerTag: "bench-throughput",
            noLocal: false, exclusive: false, arguments: null, consumer, ct);

        PublishOutcome warmup = await PublishForAsync(
            publisher, queue, options, TimeSpan.FromSeconds(options.WarmupSeconds), null, ct);
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
            state.ConfirmLatencies, ct);

        await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), ct);
        _ = await state.WaitForTotalAsync(
            warmup.Confirmed + measured.Confirmed, TimeSpan.FromSeconds(10), ct);
        state.Measuring = false;
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

    /// <summary>The gated v1 envelope: <see cref="BenchmarkOptions.Connections"/>
    /// connections, <see cref="BenchmarkOptions.Consumers"/> consumers and
    /// <see cref="BenchmarkOptions.Queues"/> queues stay live while one publisher keeps
    /// publishes in flight across every queue.</summary>
    public static async Task<ScenarioResult> RunCombinedAsync(
        BenchmarkHost host,
        BenchmarkOptions options,
        CancellationToken ct)
    {
        var wall = Stopwatch.StartNew();
        List<string> queues = [.. Enumerable.Range(0, options.Queues).Select(index => $"q-{index:0000}")];
        foreach (string queue in queues)
        {
            host.TrackQueue(queue);
        }

        ConnectionFactory factory = CreateFactory(host);
        List<IConnection> connections = new(options.Connections);
        List<IChannel> channels = [];
        var state = new DeliveryState();
        try
        {
            for (int index = 0; index < options.Connections; index++)
            {
                connections.Add(await ConnectAsync(factory, host, ct));
            }

            // Declare every queue, spread across the connections so no single channel
            // carries the whole topology. With the defaults this is two per connection.
            for (int connectionIndex = 0; connectionIndex < connections.Count; connectionIndex++)
            {
                IChannel declareChannel = await connections[connectionIndex].CreateChannelAsync(cancellationToken: ct);
                channels.Add(declareChannel);
                for (int queueIndex = connectionIndex; queueIndex < queues.Count; queueIndex += connections.Count)
                {
                    await declareChannel.QueueDeclareAsync(
                        queues[queueIndex], durable: true, exclusive: false, autoDelete: false,
                        arguments: null, cancellationToken: ct);
                }
            }

            IChannel publisher = await CreatePublisherAsync(connections[0], ct);
            channels.Add(publisher);

            // Two consumers per connection at the defaults: consumer i rides connection
            // i % connections and consumes queue i % queues (1:1 with the defaults, so
            // connection 0 carries the publisher and its two consumers).
            for (int index = 0; index < options.Consumers; index++)
            {
                IConnection connection = connections[index % connections.Count];
                string queue = queues[index % queues.Count];
                IChannel consumerChannel = await CreateConsumerChannelAsync(connection, 4, ct);
                channels.Add(consumerChannel);
                await consumerChannel.BasicQosAsync(0, (ushort)16, false, ct);

                var consumer = new AsyncEventingBasicConsumer(consumerChannel);
                consumer.ReceivedAsync += async (_, delivery) =>
                {
                    state.RecordDelivery(DeliveryAgeMs(delivery));
                    await AckAsync(consumerChannel, delivery);
                };
                await consumerChannel.BasicConsumeAsync(
                    queue, autoAck: false, consumerTag: $"bench-{index:0000}",
                    noLocal: false, exclusive: false, arguments: null, consumer, ct);
            }

            // Warm-up: one timestamped message to every queue per round, then drain and
            // wait for every delivery before taking the baseline health sample.
            long warmupTarget = (long)options.WarmupRounds * queues.Count;
            PublishOutcome warmup = new(0, 0, 0);
            bool warmupDelivered = true;
            if (warmupTarget > 0)
            {
                warmup = await PublishCountAsync(
                    publisher,
                    sequence => queues[(int)(sequence % queues.Count)],
                    options.PublishBytes,
                    warmupTarget,
                    options.Inflight,
                    null,
                    ct);
                await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), ct);
                warmupDelivered = await state.WaitForTotalAsync(
                    warmup.Confirmed, TimeSpan.FromSeconds(30), ct);
            }

            EngineHealth baseline = host.Engine.GetEngineHealth();
            bool cleanSetup = baseline.MirrorFailures == 0
                && baseline.MirrorDropped == 0
                && warmup.Failed == 0
                && warmupDelivered;

            state.ResetMeasured();
            state.Measuring = true;
            PublishOutcome measured = await PublishTimedAsync(
                publisher,
                sequence => queues[(int)(sequence % queues.Count)],
                options.PublishBytes,
                TimeSpan.FromSeconds(options.DurationSeconds),
                options.Inflight,
                state.ConfirmLatencies,
                ct);

            await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), ct);
            _ = await state.WaitForTotalAsync(
                warmup.Confirmed + measured.Confirmed, TimeSpan.FromSeconds(20), ct);
            state.Measuring = false;
            EngineHealth after = host.Engine.GetEngineHealth();
            wall.Stop();

            double elapsed = Math.Max(measured.ElapsedSeconds, 0.001);
            IReadOnlyList<double> confirmations = [.. state.ConfirmLatencies];
            IReadOnlyList<double> deliveries = state.DeliveryLatencies;
            var metrics = new Dictionary<string, double>
            {
                ["connections"] = connections.Count,
                ["consumers"] = options.Consumers,
                ["queues"] = queues.Count,
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
                ["wallTimeSeconds"] = wall.Elapsed.TotalSeconds,
            };
            var thresholds = new Dictionary<string, double>
            {
                ["connections"] = options.Connections,
                ["consumers"] = options.Consumers,
                ["queues"] = options.Queues,
                ["minPublishesPerSecond"] = 5000,
                ["maxDeliveryP99Ms"] = 20,
            };

            ScenarioResult result = Gate.Evaluate("combined", metrics, thresholds);
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
        finally
        {
            foreach (IChannel channel in channels)
            {
                try
                {
                    await channel.DisposeAsync();
                }
                catch
                {
                    // Teardown races are not a scenario failure.
                }
            }

            foreach (IConnection connection in connections)
            {
                try
                {
                    await connection.DisposeAsync();
                }
                catch
                {
                    // Teardown races are not a scenario failure.
                }
            }
        }
    }

    /// <summary>The large-message scenario: one publisher/consumer pair sends
    /// <see cref="BenchmarkOptions.LargeCount"/> bodies of
    /// <see cref="BenchmarkOptions.LargeBytes"/> with bounded in-flight publishing.
    /// Correct delivery and p99 are gated; the rate is reported, not thresholded.</summary>
    public static async Task<ScenarioResult> RunLargeAsync(
        BenchmarkHost host,
        BenchmarkOptions options,
        CancellationToken ct)
    {
        const string queue = "large";
        host.TrackQueue(queue);

        ConnectionFactory factory = CreateFactory(host);
        await using IConnection connection = await ConnectAsync(factory, host, ct);

        await using IChannel publisher = await CreatePublisherAsync(connection, ct);
        await publisher.QueueDeclareAsync(
            queue, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken: ct);

        await using IChannel consumerChannel = await CreateConsumerChannelAsync(connection, 4, ct);
        await consumerChannel.BasicQosAsync(0, (ushort)Math.Min(options.Inflight, ushort.MaxValue), false, ct);

        var state = new DeliveryState();
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            state.RecordDelivery(DeliveryAgeMs(delivery));
            await AckAsync(consumerChannel, delivery);
        };
        await consumerChannel.BasicConsumeAsync(
            queue, autoAck: false, consumerTag: "bench-large",
            noLocal: false, exclusive: false, arguments: null, consumer, ct);

        EngineHealth baseline = host.Engine.GetEngineHealth();
        bool cleanSetup = baseline.MirrorFailures == 0 && baseline.MirrorDropped == 0;

        state.ResetMeasured();
        state.Measuring = true;
        PublishOutcome measured = await PublishCountAsync(
            publisher,
            _ => queue,
            options.LargeBytes,
            options.LargeCount,
            options.Inflight,
            state.ConfirmLatencies,
            ct);

        await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), ct);
        _ = await state.WaitForTotalAsync(measured.Confirmed, TimeSpan.FromSeconds(30), ct);
        state.Measuring = false;
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
            ["publishFailures"] = measured.Failed,
            ["mirrorDropped"] = after.MirrorDropped - baseline.MirrorDropped,
            ["mirrorFailures"] = after.MirrorFailures - baseline.MirrorFailures,
            ["unconsumed"] = Math.Max(0, measured.Confirmed - state.MeasuredDelivered),
        };
        var thresholds = new Dictionary<string, double>
        {
            ["maxDeliveryP99Ms"] = 20,
        };

        ScenarioResult result = Gate.Evaluate("large", metrics, thresholds);
        if (!cleanSetup)
        {
            result = Gate.Fail(
                result,
                "setup was not clean: " +
                $"mirrorFailures={baseline.MirrorFailures}, " +
                $"mirrorDropped={baseline.MirrorDropped}");
        }

        return result;
    }

    /// <summary>Publishes as fast as confirms allow with at most
    /// <see cref="BenchmarkOptions.Inflight"/> outstanding, for <paramref name="duration"/>.
    /// Only confirmed publishes count toward the rate.</summary>
    private static Task<PublishOutcome> PublishForAsync(
        IChannel channel,
        string queue,
        BenchmarkOptions options,
        TimeSpan duration,
        ConcurrentQueue<double>? confirmLatencies,
        CancellationToken ct) =>
        PublishTimedAsync(
            channel, _ => queue, options.PublishBytes, duration, options.Inflight, confirmLatencies, ct);

    private static async Task<PublishOutcome> PublishTimedAsync(
        IChannel channel,
        Func<long, string> routingKey,
        int bodyBytes,
        TimeSpan duration,
        int inflight,
        ConcurrentQueue<double>? confirmLatencies,
        CancellationToken ct)
    {
        var outstanding = new List<Task<bool>>(inflight);
        long confirmed = 0;
        long failed = 0;
        long issued = 0;
        var clock = Stopwatch.StartNew();

        while (clock.Elapsed < duration)
        {
            while (outstanding.Count < inflight && clock.Elapsed < duration)
            {
                byte[] body = CreateBody(issued, bodyBytes);
                outstanding.Add(PublishOneAsync(channel, routingKey(issued), body, confirmLatencies));
                issued++;
            }

            if (outstanding.Count == 0)
            {
                break;
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

    /// <summary>Publishes exactly <paramref name="count"/> messages with at most
    /// <paramref name="inflight"/> outstanding, waiting for their confirms.</summary>
    private static async Task<PublishOutcome> PublishCountAsync(
        IChannel channel,
        Func<long, string> routingKey,
        int bodyBytes,
        long count,
        int inflight,
        ConcurrentQueue<double>? confirmLatencies,
        CancellationToken ct)
    {
        var outstanding = new List<Task<bool>>(inflight);
        long confirmed = 0;
        long failed = 0;
        long issued = 0;
        var clock = Stopwatch.StartNew();

        while (issued < count)
        {
            while (outstanding.Count < inflight && issued < count)
            {
                byte[] body = CreateBody(issued, bodyBytes);
                outstanding.Add(PublishOneAsync(channel, routingKey(issued), body, confirmLatencies));
                issued++;
            }

            if (outstanding.Count == 0)
            {
                break;
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
        string routingKey,
        byte[] body,
        ConcurrentQueue<double>? confirmLatencies)
    {
        long started = Stopwatch.GetTimestamp();
        try
        {
            await channel.BasicPublishAsync("", routingKey, mandatory: false, new BasicProperties(), body);
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

    private static ConnectionFactory CreateFactory(BenchmarkHost host) => new()
    {
        UserName = "guest",
        Password = "guest",
        VirtualHost = host.Vhost,
    };

    private static Task<IConnection> ConnectAsync(
        ConnectionFactory factory,
        BenchmarkHost host,
        CancellationToken ct) =>
        factory.CreateConnectionAsync([new AmqpTcpEndpoint("127.0.0.1", host.Port)], ct);

    private static Task<IChannel> CreatePublisherAsync(IConnection connection, CancellationToken ct) =>
        connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: null,
                consumerDispatchConcurrency: null),
            ct);

    private static Task<IChannel> CreateConsumerChannelAsync(
        IConnection connection,
        int dispatchConcurrency,
        CancellationToken ct) =>
        connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                outstandingPublisherConfirmationsRateLimiter: null,
                consumerDispatchConcurrency: (ushort)dispatchConcurrency),
            ct);

    /// <summary>Acknowledges one delivery; a channel lost during teardown is not a
    /// scenario failure (the unconsumed gate reports genuinely undelivered messages).</summary>
    private static async Task AckAsync(IChannel channel, BasicDeliverEventArgs delivery)
    {
        try
        {
            await channel.BasicAckAsync(
                delivery.DeliveryTag, multiple: false, delivery.CancellationToken);
        }
        catch
        {
            // The connection closed underneath the consumer.
        }
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
