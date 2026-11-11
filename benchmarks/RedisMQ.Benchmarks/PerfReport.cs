using System.Text.Json;

namespace RedisMQ.Benchmarks;

/// <summary>One scenario's measured metrics, the thresholds they were judged
/// against, and every reason the scenario failed. <see cref="Passed"/> is the gate.</summary>
internal sealed record ScenarioResult(
    string Name,
    bool Passed,
    IReadOnlyDictionary<string, double> Metrics,
    IReadOnlyDictionary<string, double> Thresholds,
    IReadOnlyList<string> Failures);

/// <summary>Turns metrics and thresholds into a pass/fail result. A threshold that is
/// absent is not evaluated; the zero-tolerance counters are checked whenever the
/// metric is present.</summary>
internal static class Gate
{
    public static ScenarioResult Evaluate(
        string name,
        IReadOnlyDictionary<string, double> metrics,
        IReadOnlyDictionary<string, double> thresholds)
    {
        List<string> failures = [];

        CheckMinimum("connections", "connections");
        CheckMinimum("consumers", "consumers");
        CheckMinimum("queues", "queues");
        CheckMinimum("minPublishesPerSecond", "publishesPerSecond");
        CheckMaximum("maxDeliveryP99Ms", "deliveryP99Ms");
        CheckZero("publishFailures");
        CheckZero("mirrorDropped");
        CheckZero("mirrorFailures");
        CheckZero("unconsumed");

        return new ScenarioResult(name, failures.Count == 0, metrics, thresholds, failures);

        void CheckMinimum(string thresholdKey, string metricKey)
        {
            if (!thresholds.TryGetValue(thresholdKey, out double minimum))
            {
                return;
            }

            if (!metrics.TryGetValue(metricKey, out double actual))
            {
                failures.Add($"{metricKey} is missing from the metrics");
                return;
            }

            if (actual < minimum)
            {
                failures.Add($"{metricKey} {Format(actual)} is below the required {Format(minimum)}");
            }
        }

        void CheckMaximum(string thresholdKey, string metricKey)
        {
            if (!thresholds.TryGetValue(thresholdKey, out double maximum))
            {
                return;
            }

            if (!metrics.TryGetValue(metricKey, out double actual))
            {
                failures.Add($"{metricKey} is missing from the metrics");
                return;
            }

            if (actual > maximum)
            {
                failures.Add($"{metricKey} {Format(actual)} exceeds the allowed {Format(maximum)}");
            }
        }

        void CheckZero(string metricKey)
        {
            if (!metrics.TryGetValue(metricKey, out double actual))
            {
                failures.Add($"{metricKey} is missing from the metrics");
                return;
            }

            if (actual != 0)
            {
                failures.Add($"{metricKey} is {Format(actual)}, expected zero");
            }
        }
    }

    /// <summary>A result that passed every threshold but must still fail because the
    /// setup or the run itself was not clean.</summary>
    public static ScenarioResult Fail(ScenarioResult result, string reason) =>
        result with
        {
            Passed = false,
            Failures = [.. result.Failures, reason],
        };

    private static string Format(double value) =>
        Math.Abs(value - Math.Round(value)) < 0.0001
            ? ((long)Math.Round(value)).ToString()
            : value.ToString("0.###");
}

internal sealed record EnvironmentInfo(
    string Os,
    string Framework,
    int ProcessorCount,
    string RedisEndpoint,
    string RedisVersion);

internal sealed class PerfReportDocument
{
    public int SchemaVersion { get; set; } = 1;

    public DateTimeOffset GeneratedAt { get; set; }

    public string Scenario { get; set; } = string.Empty;

    public EnvironmentInfo Environment { get; set; } = new(string.Empty, string.Empty, 0, string.Empty, string.Empty);

    public IReadOnlyDictionary<string, object?> Parameters { get; set; } =
        new Dictionary<string, object?>();

    public IReadOnlyList<ScenarioResult> Results { get; set; } = [];

    public bool Passed => Results.Count > 0 && Results.All(result => result.Passed);
}

internal static class PerfReport
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static async Task WriteAsync(string path, PerfReportDocument report, CancellationToken ct)
    {
        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(fullPath, JsonSerializer.Serialize(report, JsonOptions), ct);
    }

    public static void Print(PerfReportDocument report, string reportPath)
    {
        foreach (ScenarioResult result in report.Results)
        {
            Console.WriteLine();
            Console.WriteLine($"[{result.Name}] {(result.Passed ? "PASS" : "FAIL")}");
            foreach (KeyValuePair<string, double> metric in result.Metrics.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                Console.WriteLine($"  {metric.Key,-24} {metric.Value,14:0.###}");
            }

            if (result.Thresholds.Count > 0)
            {
                Console.WriteLine(
                    "  thresholds               " +
                    string.Join(
                        ", ",
                        result.Thresholds.Select(pair => $"{pair.Key}={pair.Value:0.###}")));
            }

            foreach (string failure in result.Failures)
            {
                Console.WriteLine($"  FAILURE: {failure}");
            }
        }

        Console.WriteLine();
        Console.WriteLine($"Report: {Path.GetFullPath(reportPath)}");
        Console.WriteLine($"Overall: {(report.Passed ? "PASS" : "FAIL")}");
    }
}
