using RedisMQ.Broker.Diagnostics;

namespace RedisMQ.Broker.Tests;

/// <summary>A capturing log sink with switches for the two host-failure cases: disabled
/// sinks must receive nothing, and throwing sinks must not reach the broker.</summary>
internal sealed class RecordingLogSink : IBrokerLogSink
{
    private readonly object _lock = new();
    private readonly List<BrokerLogEntry> _entries = [];

    public bool Enabled { get; set; } = true;

    public bool ThrowOnLog { get; set; }

    public IReadOnlyList<BrokerLogEntry> Entries
    {
        get
        {
            lock (_lock)
            {
                return [.. _entries];
            }
        }
    }

    public bool IsEnabled(BrokerLogLevel level) => Enabled;

    public void Log(in BrokerLogEntry entry)
    {
        if (ThrowOnLog)
        {
            throw new InvalidOperationException("recording sink configured to throw");
        }

        lock (_lock)
        {
            _entries.Add(entry);
        }
    }
}
