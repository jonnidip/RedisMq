using System.Diagnostics.Metrics;

namespace RedisMQ.Broker.Tests;

/// <summary>One measurement the broker emitted, flattened for assertions.</summary>
internal sealed record MeasurementRecord(
    string Instrument,
    double Value,
    IReadOnlyDictionary<string, object?> Tags)
{
    public long LongValue => (long)Value;
}

/// <summary>
/// A <see cref="MeterListener"/> that records every measurement from the
/// <c>RedisMQ.Broker</c> meter. Observable gauges are pull-based, so
/// <see cref="ObserveGauges"/> drives their callbacks and returns the measurements they
/// produced. Disposing the collector detaches the listener.
/// </summary>
internal sealed class MeterCollector : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly object _lock = new();
    private readonly List<MeasurementRecord> _measurements = [];

    public MeterCollector()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (!string.Equals(instrument.Meter.Name, "RedisMQ.Broker", StringComparison.Ordinal))
            {
                return;
            }

            listener.EnableMeasurementEvents(instrument);
        };

        _listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.Start();
    }

    public IReadOnlyList<MeasurementRecord> Measurements => For(string.Empty);

    public IReadOnlyList<MeasurementRecord> For(string instrumentName)
    {
        lock (_lock)
        {
            return [.. _measurements.Where(measurement =>
                instrumentName.Length == 0
                || string.Equals(measurement.Instrument, instrumentName, StringComparison.Ordinal))];
        }
    }

    /// <summary>Pulls every observable instrument, then returns the named gauge's
    /// measurements (the listener delivers them synchronously).</summary>
    public IReadOnlyList<MeasurementRecord> ObserveGauges(string instrumentName)
    {
        _listener.RecordObservableInstruments();
        return For(instrumentName);
    }

    public void Dispose() => _listener.Dispose();

    private void Add(
        Instrument instrument,
        double value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        Dictionary<string, object?> copy = new(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            copy[tag.Key] = tag.Value;
        }

        lock (_lock)
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, value, copy));
        }
    }
}
