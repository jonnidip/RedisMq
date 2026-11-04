using System.Diagnostics;

namespace RedisMQ.Broker.Tests;

/// <summary>Collects every <see cref="Activity"/> this diagnostics source stops.</summary>
internal sealed class ActivityCollector : IDisposable
{
    private readonly ActivityListener _listener = new();
    private readonly object _lock = new();
    private readonly List<Activity> _stopped = [];

    public ActivityCollector()
    {
        _listener.ShouldListenTo = static source =>
            string.Equals(source.Name, "RedisMQ.Broker", StringComparison.Ordinal);
        _listener.Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
            ActivitySamplingResult.AllDataAndRecorded;
        _listener.SampleUsingParentId = static (ref ActivityCreationOptions<string> _) =>
            ActivitySamplingResult.AllDataAndRecorded;
        _listener.ActivityStopped = activity =>
        {
            lock (_lock)
            {
                _stopped.Add(activity);
            }
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public IReadOnlyList<Activity> Stopped
    {
        get
        {
            lock (_lock)
            {
                return [.. _stopped];
            }
        }
    }

    public void Dispose() => _listener.Dispose();
}
