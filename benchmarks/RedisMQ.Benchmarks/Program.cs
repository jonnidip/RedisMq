using System.Runtime.InteropServices;
using StackExchange.Redis;

namespace RedisMQ.Benchmarks;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Contains("--help", StringComparer.Ordinal))
        {
            Console.WriteLine(BenchmarkOptions.Usage);
            return 0;
        }

        if (args.Contains("--selftest", StringComparer.Ordinal))
        {
            return RunSelfTest();
        }

        BenchmarkOptions options;
        try
        {
            options = BenchmarkOptions.Parse(args);
        }
        catch (BenchmarkUsageException ex)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine();
            Console.Error.WriteLine(BenchmarkOptions.Usage);
            return 2;
        }

        using var stop = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            stop.Cancel();
        };

        try
        {
            string redisVersion = await ReadRedisVersionAsync(options.Redis, stop.Token);

            List<ScenarioResult> results = [];
            switch (options.Scenario)
            {
                case "throughput":
                    results.Add(await RunHostedAsync(
                        options,
                        (host, ct) => Scenarios.RunThroughputAsync(host, options, ct),
                        stop.Token));
                    break;

                case "combined":
                    results.Add(await RunHostedAsync(
                        options,
                        (host, ct) => Scenarios.RunCombinedAsync(host, options, ct),
                        stop.Token));
                    break;

                case "large":
                    results.Add(await RunHostedAsync(
                        options,
                        (host, ct) => Scenarios.RunLargeAsync(host, options, ct),
                        stop.Token));
                    break;

                case "all":
                    results.Add(await RunHostedAsync(
                        options,
                        (host, ct) => Scenarios.RunCombinedAsync(host, options, ct),
                        stop.Token));
                    results.Add(await RunHostedAsync(
                        options,
                        (host, ct) => Scenarios.RunThroughputAsync(host, options, ct),
                        stop.Token));
                    results.Add(await RunHostedAsync(
                        options,
                        (host, ct) => Scenarios.RunLargeAsync(host, options, ct),
                        stop.Token));
                    break;

                default:
                    Console.Error.WriteLine($"Unknown scenario '{options.Scenario}'.");
                    return 2;
            }

            var report = new PerfReportDocument
            {
                GeneratedAt = DateTimeOffset.UtcNow,
                Scenario = options.Scenario,
                Environment = new EnvironmentInfo(
                    RuntimeInformation.OSDescription,
                    RuntimeInformation.FrameworkDescription,
                    Environment.ProcessorCount,
                    options.Redis,
                    redisVersion),
                Parameters = BuildParameters(options),
                Results = results,
            };

            await PerfReport.WriteAsync(options.OutPath, report, stop.Token);
            PerfReport.Print(report, options.OutPath);
            return report.Passed || options.NoGate ? 0 : 1;
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("benchmark cancelled");
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"benchmark failed: {ex.Message}");
            return 1;
        }
    }

    private static async Task<ScenarioResult> RunHostedAsync(
        BenchmarkOptions options,
        Func<BenchmarkHost, CancellationToken, Task<ScenarioResult>> run,
        CancellationToken ct)
    {
        await using BenchmarkHost host = await BenchmarkHost.StartAsync(options, ct);
        try
        {
            return await run(host, ct);
        }
        finally
        {
            await host.StopAsync();
            try
            {
                await host.WaitForDrainAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
            }
            catch (BenchmarkRunException)
            {
                // Cleanup still runs; a stuck drainer is reported by the scenario's
                // mirror health, not by hanging teardown.
            }

            await host.CleanupAsync();
        }
    }

    private static IReadOnlyDictionary<string, object?> BuildParameters(BenchmarkOptions options) =>
        new Dictionary<string, object?>
        {
            ["publishBytes"] = options.PublishBytes,
            ["durationSeconds"] = options.DurationSeconds,
            ["warmupSeconds"] = options.WarmupSeconds,
            ["inflight"] = options.Inflight,
            ["connections"] = options.Connections,
            ["consumers"] = options.Consumers,
            ["queues"] = options.Queues,
            ["warmupRounds"] = options.WarmupRounds,
            ["largeBytes"] = options.LargeBytes,
            ["largeCount"] = options.LargeCount,
            ["drainerCapacity"] = options.DrainerCapacity,
            ["drainerBatchSize"] = options.DrainerBatchSize,
        };

    private static async Task<string> ReadRedisVersionAsync(string endpoint, CancellationToken ct)
    {
        var configuration = ConfigurationOptions.Parse(endpoint);
        configuration.AbortOnConnectFail = true;
        using IConnectionMultiplexer redis = await ConnectionMultiplexer.ConnectAsync(configuration);
        RedisResult info = await redis.GetDatabase().ExecuteAsync("INFO", "server");
        foreach (string line in info.ToString().Split('\n'))
        {
            const string prefix = "redis_version:";
            if (line.StartsWith(prefix, StringComparison.Ordinal))
            {
                return line[prefix.Length..].Trim();
            }
        }

        return "unknown";
    }

    private static int RunSelfTest()
    {
        try
        {
            List<double> hundred = [.. Enumerable.Range(1, 100).Select(value => (double)value)];
            Expect(Stats.Percentile(hundred, 50) == 50, "Percentile([1..100], 50) should be 50");
            Expect(Stats.Percentile(hundred, 99) == 99, "Percentile([1..100], 99) should be 99");

            Dictionary<string, double> metrics = new()
            {
                ["connections"] = 500,
                ["consumers"] = 1000,
                ["queues"] = 1000,
                ["publishesPerSecond"] = 6000,
                ["deliveryP99Ms"] = 10,
                ["publishFailures"] = 0,
                ["mirrorDropped"] = 0,
                ["mirrorFailures"] = 0,
                ["unconsumed"] = 0,
            };
            Dictionary<string, double> thresholds = new()
            {
                ["connections"] = 500,
                ["consumers"] = 1000,
                ["queues"] = 1000,
                ["minPublishesPerSecond"] = 5000,
                ["maxDeliveryP99Ms"] = 20,
            };

            Expect(Gate.Evaluate("selftest", metrics, thresholds).Passed, "clean metrics should pass");

            // Boundaries are inclusive: exactly the gate values pass.
            Expect(
                Gate.Evaluate("selftest", With("publishesPerSecond", 5000), thresholds).Passed,
                "a rate exactly at the threshold should pass");
            Expect(
                Gate.Evaluate("selftest", With("deliveryP99Ms", 20), thresholds).Passed,
                "a p99 exactly at the threshold should pass");

            Expect(!Gate.Evaluate("selftest", With("publishFailures", 1), thresholds).Passed,
                "a publish failure should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("mirrorDropped", 1), thresholds).Passed,
                "a mirror drop should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("mirrorFailures", 1), thresholds).Passed,
                "a mirror failure should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("unconsumed", 1), thresholds).Passed,
                "an unconsumed message should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("publishesPerSecond", 4999), thresholds).Passed,
                "a missed rate should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("deliveryP99Ms", 21), thresholds).Passed,
                "a missed p99 should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("connections", 499), thresholds).Passed,
                "a missed connection count should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("consumers", 999), thresholds).Passed,
                "a missed consumer count should fail the gate");
            Expect(!Gate.Evaluate("selftest", With("queues", 999), thresholds).Passed,
                "a missed queue count should fail the gate");

            Console.WriteLine("selftest passed");
            return 0;

            Dictionary<string, double> With(string key, double value)
            {
                Dictionary<string, double> copy = new(metrics) { [key] = value };
                return copy;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"selftest failed: {ex.Message}");
            return 1;
        }
    }

    private static void Expect(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
