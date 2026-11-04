using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Reflection;

namespace RedisMQ.Broker.Diagnostics;

/// <summary>
/// One broker instance's diagnostics home: the <see cref="Meter"/> and
/// <see cref="ActivitySource"/> (both named <c>RedisMQ.Broker</c>), the host-attached
/// log sink, and the shared observable-gauge registry. Every measurement carries
/// <c>broker.instance</c> so a process with several brokers stays attributable.
/// The supported production shape is one shared instance per process, passed to both
/// the engine and the server; a component given null owns a private instance.
/// </summary>
public sealed class BrokerDiagnostics : IDisposable
{
    /// <summary>The meter and activity-source name hosts subscribe to.</summary>
    public const string SourceName = "RedisMQ.Broker";

    private const string InstanceTag = "broker.instance";

    private static readonly double[] DurationBoundaries =
        [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

    private static readonly long[] SizeBoundaries =
        [64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304];

    private readonly object _gaugeLock = new();
    private readonly Dictionary<string, GaugeRegistration> _gauges = new(StringComparer.Ordinal);
    private bool _disposed;

    public BrokerDiagnostics(
        string? instanceId = null,
        IBrokerLogSink? logSink = null,
        string? serviceVersion = null)
    {
        InstanceId = string.IsNullOrWhiteSpace(instanceId) ? Guid.NewGuid().ToString("N")[..8] : instanceId;
        Log = logSink ?? NullBrokerLogSink.Instance;
        string version = serviceVersion
            ?? typeof(BrokerDiagnostics).Assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? "0.0.0";

        Meter = new Meter(SourceName, version);
        ActivitySource = new ActivitySource(SourceName, version);

        ConnectionsOpened = CreateCounter(
            "redismq.connections.opened", "{connection}", "Connections accepted by a listener.");
        ConnectionsClosed = CreateCounter(
            "redismq.connections.closed", "{connection}", "Connections that ended, by reason.");
        ConnectionsAuthenticated = CreateCounter(
            "redismq.connections.authenticated", "{connection}", "Connections that passed authentication and opened a vhost.");
        ChannelsOpened = CreateCounter(
            "redismq.channels.opened", "{channel}", "Channels opened on a connection.");
        ChannelsClosed = CreateCounter(
            "redismq.channels.closed", "{channel}", "Channels retired, by reason.");
        AuthFailures = CreateCounter(
            "redismq.auth.failures", "{failure}", "Authentication refusals, by reason.");
        VhostRefusals = CreateCounter(
            "redismq.vhost.refusals", "{refusal}", "Virtual-host refusals, by reason.");
        PermissionDenials = CreateCounter(
            "redismq.permission.denials", "{denial}", "Permission checks that denied an operation.");
        TlsHandshakes = CreateCounter(
            "redismq.tls.handshakes", "{handshake}", "TLS handshakes, by result.");
        Publishes = CreateCounter(
            "redismq.publishes", "{message}", "Content dispatches completed, by outcome.");
        Confirms = CreateCounter(
            "redismq.confirms", "{confirm}", "Publisher confirms written, by outcome.");
        Deliveries = CreateCounter(
            "redismq.deliveries", "{message}", "Messages delivered or returned by a no-ack get.");
        Settlements = CreateCounter(
            "redismq.settlements", "{settlement}", "Settlements accepted, by effective action and wire method.");
        MessagesDropped = CreateCounter(
            "redismq.messages.dropped", "{message}", "Messages discarded by an engine policy.");
        MessagesExpired = CreateCounter(
            "redismq.messages.expired", "{message}", "Messages removed by TTL expiry.");
        QueuesExpired = CreateCounter(
            "redismq.queues.expired", "{queue}", "Queues deleted by x-expires.");
        MessagesDeadlettered = CreateCounter(
            "redismq.messages.deadlettered", "{message}", "Dead-letter copies enqueued.");
        DeadLettersDropped = CreateCounter(
            "redismq.deadletters.dropped", "{message}", "Messages lost because a configured dead-letter path produced no copy.");
        ReaperSweeps = CreateCounter(
            "redismq.reaper.sweeps", "{sweep}", "Reaper ticks attempted.");
        ReaperFailures = CreateCounter(
            "redismq.reaper.failures", "{failure}", "Reaper ticks that threw.");
        RedisMirrorCommands = CreateCounter(
            "redismq.redis.mirror.commands", "{command}", "Drainer queue items finished, by kind and outcome.");
        RedisMirrorDropped = CreateCounter(
            "redismq.redis.mirror.dropped", "{command}", "Commands that could not enter the drainer channel.");
        RedisDrainerSaturated = CreateCounter(
            "redismq.redis.drainer.saturated", "{publish}", "Publishes refused because no drainer slot was free.");
        RedisReplayMessages = CreateCounter(
            "redismq.redis.replay.messages", "{message}", "Messages restored by startup replay.");
        RedisReplayFailures = CreateCounter(
            "redismq.redis.replay.failures", "{failure}", "Startup replay attempts that failed.");

        TlsHandshakeDuration = CreateSeconds(
            "redismq.tls.handshake.duration", "TLS handshake time.");
        ConfirmDuration = CreateSeconds(
            "redismq.confirm.duration", "Publish admission to confirm outcome.");
        ReaperDuration = CreateSeconds(
            "redismq.reaper.duration", "Reaper tick time.");
        RedisMirrorDuration = CreateSeconds(
            "redismq.redis.mirror.duration", "Drainer queue item time.");

        PublishSize = Meter.CreateHistogram<long>(
            "redismq.publish.size",
            "By",
            "Published body size in bytes.",
            tags: null,
            new InstrumentAdvice<long> { HistogramBucketBoundaries = SizeBoundaries });
    }

    /// <summary>This instance's random attribution tag, stamped on every measurement.</summary>
    public string InstanceId { get; }

    /// <summary>The meter hosts subscribe to (name <see cref="SourceName"/>).</summary>
    public Meter Meter { get; }

    /// <summary>The activity source hosts subscribe to (name <see cref="SourceName"/>).</summary>
    public ActivitySource ActivitySource { get; }

    /// <summary>The host-attached log sink; <see cref="NullBrokerLogSink.Instance"/> when none.</summary>
    public IBrokerLogSink Log { get; }

    internal Counter<long> ConnectionsOpened { get; }

    internal Counter<long> ConnectionsClosed { get; }

    internal Counter<long> ConnectionsAuthenticated { get; }

    internal Counter<long> ChannelsOpened { get; }

    internal Counter<long> ChannelsClosed { get; }

    internal Counter<long> AuthFailures { get; }

    internal Counter<long> VhostRefusals { get; }

    internal Counter<long> PermissionDenials { get; }

    internal Counter<long> TlsHandshakes { get; }

    internal Counter<long> Publishes { get; }

    internal Counter<long> Confirms { get; }

    internal Counter<long> Deliveries { get; }

    internal Counter<long> Settlements { get; }

    internal Counter<long> MessagesDropped { get; }

    internal Counter<long> MessagesExpired { get; }

    internal Counter<long> QueuesExpired { get; }

    internal Counter<long> MessagesDeadlettered { get; }

    internal Counter<long> DeadLettersDropped { get; }

    internal Counter<long> ReaperSweeps { get; }

    internal Counter<long> ReaperFailures { get; }

    internal Counter<long> RedisMirrorCommands { get; }

    internal Counter<long> RedisMirrorDropped { get; }

    internal Counter<long> RedisDrainerSaturated { get; }

    internal Counter<long> RedisReplayMessages { get; }

    internal Counter<long> RedisReplayFailures { get; }

    internal Histogram<double> TlsHandshakeDuration { get; }

    internal Histogram<double> ConfirmDuration { get; }

    internal Histogram<double> ReaperDuration { get; }

    internal Histogram<double> RedisMirrorDuration { get; }

    internal Histogram<long> PublishSize { get; }

    /// <summary>Adds one to a counter, stamping <c>broker.instance</c>, and does no work
    /// when no listener has enabled the instrument.</summary>
    internal void Count(Counter<long> counter, TagList tags)
    {
        if (!counter.Enabled)
        {
            return;
        }

        tags.Add(InstanceTag, InstanceId);
        counter.Add(1, in tags);
    }

    /// <summary>Records one duration (seconds), stamping <c>broker.instance</c>.</summary>
    internal void Measure(Histogram<double> histogram, double value, TagList tags)
    {
        if (!histogram.Enabled)
        {
            return;
        }

        tags.Add(InstanceTag, InstanceId);
        histogram.Record(value, in tags);
    }

    /// <summary>Records one byte-size measurement, stamping <c>broker.instance</c>.</summary>
    internal void Measure(Histogram<long> histogram, long value, TagList tags)
    {
        if (!histogram.Enabled)
        {
            return;
        }

        tags.Add(InstanceTag, InstanceId);
        histogram.Record(value, in tags);
    }

    /// <summary>
    /// Registers one observable-gauge callback. Instruments are created once per name so
    /// a diagnostics instance shared by a server and an engine observes every source;
    /// measurements with identical tag sets are summed. Disposing the handle removes
    /// that callback and disposes the instrument when the last source leaves.
    /// </summary>
    internal IDisposable RegisterObservableGauge(
        string name,
        string unit,
        string description,
        Func<IEnumerable<Measurement<long>>> observe)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(observe);

        lock (_gaugeLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_gauges.TryGetValue(name, out GaugeRegistration? registration))
            {
                registration = new GaugeRegistration(this, name);
                registration.Instrument = Meter.CreateObservableGauge(
                    name, registration.Observe, unit, description);
                _gauges[name] = registration;
            }

            registration.Callbacks.Add(observe);
            return new GaugeHandle(this, registration, observe);
        }
    }

    /// <summary>Writes one structured entry when the sink enables its level; a sink
    /// exception is swallowed so host logging can never kill a connection.</summary>
    internal void WriteLog(
        BrokerLogLevel level,
        BrokerLogEvent evt,
        string message,
        Exception? exception = null,
        params KeyValuePair<string, object?>[] data)
    {
        if (!Log.IsEnabled(level))
        {
            return;
        }

        var entry = new BrokerLogEntry(
            level,
            evt,
            message,
            exception,
            data.Length == 0 ? null : data);

        try
        {
            Log.Log(in entry);
        }
        catch
        {
            // A broken host sink must never break the broker.
        }
    }

    /// <summary>Starts the <c>amqp.publish</c> span, or returns null when no listener
    /// is sampling this source.</summary>
    internal Activity? StartPublishActivity(string vhost, string exchange, string routingKey, int bodySize)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        Activity? activity = ActivitySource.StartActivity("amqp.publish", ActivityKind.Producer);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag("messaging.system", "rabbitmq");
        activity.SetTag("messaging.operation", "publish");
        if (exchange.Length > 0)
        {
            activity.SetTag("messaging.destination.name", exchange);
        }

        activity.SetTag("redismq.vhost", vhost);
        activity.SetTag("messaging.rabbitmq.destination.routing_key", routingKey);
        activity.SetTag("redismq.body.size", bodySize);
        activity.SetTag(InstanceTag, InstanceId);
        return activity;
    }

    /// <summary>Starts the <c>amqp.deliver</c> span, or returns null when no listener
    /// is sampling this source.</summary>
    internal Activity? StartDeliverActivity(
        string vhost,
        string queue,
        long deliveryTag,
        bool redelivered,
        int bodySize)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        Activity? activity = ActivitySource.StartActivity("amqp.deliver", ActivityKind.Consumer);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag("messaging.system", "rabbitmq");
        activity.SetTag("messaging.operation", "deliver");
        activity.SetTag("messaging.destination.name", queue);
        activity.SetTag("redismq.vhost", vhost);
        activity.SetTag("redismq.delivery.tag", deliveryTag);
        activity.SetTag("redismq.redelivered", redelivered);
        activity.SetTag("redismq.body.size", bodySize);
        activity.SetTag(InstanceTag, InstanceId);
        return activity;
    }

    /// <summary>Ends one span exactly once with its final outcome; null is a no-op.</summary>
    internal static void StopActivity(Activity? activity, string outcome, bool error = false)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetTag("redismq.outcome", outcome);
        if (error)
        {
            activity.SetStatus(ActivityStatusCode.Error);
        }

        activity.Stop();
    }

    /// <summary>Disposes the registry, then the meter and activity source. Idempotent.</summary>
    public void Dispose()
    {
        lock (_gaugeLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            // Individual instruments have no Dispose in this BCL; the meter owns them.
            _gauges.Clear();
        }

        Meter.Dispose();
        ActivitySource.Dispose();
    }

    private Counter<long> CreateCounter(string name, string unit, string description) =>
        Meter.CreateCounter<long>(name, unit, description);

    private Histogram<double> CreateSeconds(string name, string description) =>
        Meter.CreateHistogram<double>(
            name,
            "s",
            description,
            tags: null,
            new InstrumentAdvice<double> { HistogramBucketBoundaries = DurationBoundaries });

    private sealed class GaugeRegistration
    {
        private readonly BrokerDiagnostics _owner;

        public GaugeRegistration(BrokerDiagnostics owner, string name)
        {
            _owner = owner;
            Name = name;
        }

        public string Name { get; }

        public ObservableGauge<long>? Instrument { get; set; }

        public List<Func<IEnumerable<Measurement<long>>>> Callbacks { get; } = [];

        public IEnumerable<Measurement<long>> Observe()
        {
            Func<IEnumerable<Measurement<long>>>[] callbacks;
            lock (_owner._gaugeLock)
            {
                callbacks = [.. Callbacks];
            }

            Dictionary<string, (long Value, KeyValuePair<string, object?>[] Tags)> sums =
                new(StringComparer.Ordinal);

            foreach (Func<IEnumerable<Measurement<long>>> callback in callbacks)
            {
                IEnumerable<Measurement<long>> measurements;
                try
                {
                    measurements = callback();
                }
                catch
                {
                    continue; // one bad source must not blank the whole gauge
                }

                foreach (Measurement<long> measurement in measurements)
                {
                    List<KeyValuePair<string, object?>> tags = [];
                    foreach (KeyValuePair<string, object?> tag in measurement.Tags)
                    {
                        if (tag.Key == InstanceTag)
                        {
                            continue;
                        }

                        tags.Add(tag);
                    }

                    tags.Add(new KeyValuePair<string, object?>(InstanceTag, _owner.InstanceId));
                    tags.Sort(static (left, right) => string.CompareOrdinal(left.Key, right.Key));
                    string key = string.Join(
                        '|',
                        tags.Select(tag =>
                            $"{tag.Key}={Convert.ToString(tag.Value, CultureInfo.InvariantCulture)}"));

                    if (sums.TryGetValue(key, out var existing))
                    {
                        sums[key] = (existing.Value + measurement.Value, existing.Tags);
                    }
                    else
                    {
                        sums[key] = (measurement.Value, [.. tags]);
                    }
                }
            }

            foreach (var sum in sums.Values)
            {
                yield return new Measurement<long>(sum.Value, sum.Tags);
            }
        }
    }

    private sealed class GaugeHandle : IDisposable
    {
        private readonly BrokerDiagnostics _owner;
        private readonly Func<IEnumerable<Measurement<long>>> _callback;
        private GaugeRegistration? _registration;

        public GaugeHandle(
            BrokerDiagnostics owner,
            GaugeRegistration registration,
            Func<IEnumerable<Measurement<long>>> callback)
        {
            _owner = owner;
            _registration = registration;
            _callback = callback;
        }

        public void Dispose()
        {
            if (_registration is null)
            {
                return;
            }

            lock (_owner._gaugeLock)
            {
                // The instrument is kept for the meter's lifetime so a later register
                // with the same name reuses it; with zero callbacks it observes nothing.
                _registration.Callbacks.Remove(_callback);
                _registration = null;
            }
        }
    }
}
