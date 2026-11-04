namespace RedisMQ.Broker.Diagnostics;

/// <summary>Severity of a structured broker log entry, ordered from most verbose to
/// most severe.</summary>
public enum BrokerLogLevel
{
    Trace,
    Debug,
    Information,
    Warning,
    Error,
    Critical,
}

/// <summary>
/// The stable event ids the broker emits. A host adapter maps these to its logging
/// framework's event ids; the enum value is the contract, the message text is not.
/// </summary>
public enum BrokerLogEvent
{
    ServerStarted,
    ServerStopped,
    ListenerFailed,
    ReaperFailed,
    ConnectionOpened,
    ConnectionClosed,
    AuthenticationFailed,
    VhostAccessDenied,
    ChannelOpened,
    ChannelClosed,
    PermissionDenied,
    PublishRefused,
    PublishUnroutable,
    ConfirmNack,
    ConfirmTimeout,
    DeliveryFailed,
    DeadLetterDropped,
    QueueExpired,
    MessageExpired,
    RedisMirrorFailed,
    RedisMirrorRecovered,
    DrainerSaturated,
    TlsHandshakeFailed,
}

/// <summary>One structured log record: the event id is stable, the message is
/// human-readable, and <see cref="Data"/> carries small structured fields. Secrets and
/// message contents are never placed in any of these.</summary>
public readonly record struct BrokerLogEntry(
    BrokerLogLevel Level,
    BrokerLogEvent Event,
    string Message,
    Exception? Exception = null,
    IReadOnlyList<KeyValuePair<string, object?>>? Data = null);

/// <summary>The host-owned logging seam. Implementations forward entries to the
/// application's logger; a throwing sink is contained by <see cref="BrokerDiagnostics"/>
/// and never reaches the protocol path.</summary>
public interface IBrokerLogSink
{
    bool IsEnabled(BrokerLogLevel level);

    void Log(in BrokerLogEntry entry);
}

/// <summary>The default sink: disabled, so the broker does no log work unless a host
/// attaches a real sink.</summary>
public sealed class NullBrokerLogSink : IBrokerLogSink
{
    public static readonly NullBrokerLogSink Instance = new();

    private NullBrokerLogSink()
    {
    }

    public bool IsEnabled(BrokerLogLevel level) => false;

    public void Log(in BrokerLogEntry entry)
    {
    }
}
