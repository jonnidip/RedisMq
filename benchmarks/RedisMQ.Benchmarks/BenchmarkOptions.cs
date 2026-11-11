using System.Globalization;

namespace RedisMQ.Benchmarks;

/// <summary>A command-line mistake: printed with the usage line and exit code 2.</summary>
internal sealed class BenchmarkUsageException(string message) : Exception(message);

/// <summary>A run that could not start or could not finish: Redis missing, topology
/// failure, a channel lost. Always fails the process, even with <c>--no-gate</c>.</summary>
internal sealed class BenchmarkRunException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>The benchmark's command-line surface. Defaults match the approved v1
/// envelope; <see cref="Parse"/> rejects unknown flags and malformed values.</summary>
internal sealed class BenchmarkOptions
{
    public const string Usage =
        """
        RedisMQ benchmark

        Usage:
          dotnet run -c Release --project benchmarks/RedisMQ.Benchmarks -- [options]

        Options:
          --scenario all|throughput|combined|large   Scenario to run (default: all)
          --redis <host:port>                        Redis endpoint (default: 127.0.0.1:6379)
          --duration <seconds>                       Measurement window (default: 30)
          --warmup <seconds>                         Throughput warm-up (default: 5)
          --publish-bytes <bytes>                    Throughput/combined body size (default: 1024)
          --inflight <count>                         Confirmed publishes outstanding (default: 128)
          --connections <count>                      Combined topology connections (default: 500)
          --consumers <count>                        Combined topology consumers (default: 1000)
          --queues <count>                           Combined topology queues (default: 1000)
          --warmup-rounds <count>                    Combined warm-up rounds (default: 10)
          --large-bytes <bytes>                      Large-message body size (default: 1048576)
          --large-count <count>                      Large messages to send (default: 1000)
          --out <path>                               JSON report path (default: artifacts/perf.json)
          --drainer-capacity <count>                 Engine drainer capacity (default: 1024)
          --drainer-batch-size <count>               Engine drainer batch size (default: 256)
          --no-gate                                  Report thresholds without failing the exit code
          --inflight is capped per scenario: the canary uses min(--inflight, 16); the
          1 MB scenario uses min(--inflight, 8 MB / --large-bytes).
          --selftest                                 Run percentile/gate checks without Redis
          --help                                     Print this help
        """;

    public string Scenario { get; set; } = "all";

    public string Redis { get; set; } = "127.0.0.1:6379";

    public int DurationSeconds { get; set; } = 30;

    public int WarmupSeconds { get; set; } = 5;

    public int PublishBytes { get; set; } = 1024;

    public int Inflight { get; set; } = 128;

    public int Connections { get; set; } = 500;

    public int Consumers { get; set; } = 1000;

    public int Queues { get; set; } = 1000;

    public int WarmupRounds { get; set; } = 10;

    public int LargeBytes { get; set; } = 1048576;

    public int LargeCount { get; set; } = 1000;

    public string OutPath { get; set; } = "artifacts/perf.json";

    public bool NoGate { get; set; }

    public int DrainerCapacity { get; set; } = 1024;

    public int DrainerBatchSize { get; set; } = 256;

    public static BenchmarkOptions Parse(string[] args)
    {
        var options = new BenchmarkOptions();
        for (int index = 0; index < args.Length; index++)
        {
            string argument = args[index];
            if (argument == "--no-gate")
            {
                options.NoGate = true;
                continue;
            }

            if (index + 1 >= args.Length)
            {
                throw new BenchmarkUsageException($"Option '{argument}' requires a value.");
            }

            string value = args[++index];
            switch (argument)
            {
                case "--scenario":
                    options.Scenario = value;
                    break;
                case "--redis":
                    options.Redis = value;
                    break;
                case "--duration":
                    options.DurationSeconds = ParseInt(argument, value);
                    break;
                case "--warmup":
                    options.WarmupSeconds = ParseInt(argument, value);
                    break;
                case "--publish-bytes":
                    options.PublishBytes = ParseInt(argument, value);
                    break;
                case "--inflight":
                    options.Inflight = ParseInt(argument, value);
                    break;
                case "--connections":
                    options.Connections = ParseInt(argument, value);
                    break;
                case "--consumers":
                    options.Consumers = ParseInt(argument, value);
                    break;
                case "--queues":
                    options.Queues = ParseInt(argument, value);
                    break;
                case "--warmup-rounds":
                    options.WarmupRounds = ParseInt(argument, value);
                    break;
                case "--large-bytes":
                    options.LargeBytes = ParseInt(argument, value);
                    break;
                case "--large-count":
                    options.LargeCount = ParseInt(argument, value);
                    break;
                case "--out":
                    options.OutPath = value;
                    break;
                case "--drainer-capacity":
                    options.DrainerCapacity = ParseInt(argument, value);
                    break;
                case "--drainer-batch-size":
                    options.DrainerBatchSize = ParseInt(argument, value);
                    break;
                default:
                    throw new BenchmarkUsageException($"Unknown option '{argument}'.");
            }
        }

        options.Validate();
        return options;
    }

    private static int ParseInt(string option, string value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
        {
            throw new BenchmarkUsageException($"Option '{option}' expects an integer, got '{value}'.");
        }

        return parsed;
    }

    private void Validate()
    {
        if (Scenario is not ("all" or "throughput" or "combined" or "large"))
        {
            throw new BenchmarkUsageException(
                $"Unknown scenario '{Scenario}'. Expected all, throughput, combined or large.");
        }

        Require(DurationSeconds > 0, "--duration must be greater than zero.");
        Require(WarmupSeconds >= 0, "--warmup cannot be negative.");
        Require(PublishBytes >= 16, "--publish-bytes must be at least 16 (sequence and timestamp).");
        Require(Inflight > 0, "--inflight must be greater than zero.");
        Require(Connections > 0, "--connections must be greater than zero.");
        Require(Consumers > 0, "--consumers must be greater than zero.");
        Require(Queues > 0, "--queues must be greater than zero.");
        Require(WarmupRounds >= 0, "--warmup-rounds cannot be negative.");
        Require(LargeBytes >= 16, "--large-bytes must be at least 16 (sequence and timestamp).");
        Require(LargeCount > 0, "--large-count must be greater than zero.");
        Require(!string.IsNullOrWhiteSpace(OutPath), "--out cannot be empty.");
        Require(DrainerCapacity > 0, "--drainer-capacity must be greater than zero.");
        Require(DrainerBatchSize > 0, "--drainer-batch-size must be greater than zero.");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new BenchmarkUsageException(message);
        }
    }
}
