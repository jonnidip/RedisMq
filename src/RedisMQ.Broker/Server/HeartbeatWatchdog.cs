namespace RedisMQ.Broker.Server;

/// <summary>
/// Silence policing for one AMQP connection (receive-side heartbeat stewardship):
/// the kill fires when no inbound frame has arrived for twice the negotiated
/// heartbeat interval — RabbitMQ's rule that two missed heartbeats make the peer
/// unreachable. The watchdog is deliberately not a timer: it keeps a silence window
/// and is checked by its owner (WireConnection's timer / read loop), which keeps the
/// time source injectable for fake-clock tests via the virtual <see cref="Now"/>.
/// It never fires while activity is being reported — WireConnection resets the window
/// on EVERY inbound frame, of any type, per the plan's heartbeat rule.
/// </summary>
public class HeartbeatWatchdog
{
    private readonly TimeSpan _silenceLimit; // 2x the negotiated heartbeat
    private readonly Action<HeartbeatWatchdog>? _onTimeout;
    private DateTimeOffset _lastActivity;

    /// <param name="heartbeatInterval">The negotiated heartbeat in seconds-scale.
    /// Zero is a disabled heartbeat and must never reach this constructor — the
    /// connection simply starts no watchdog.</param>
    public HeartbeatWatchdog(TimeSpan heartbeatInterval, Action<HeartbeatWatchdog>? onTimeout = null)
    {
        if (heartbeatInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(heartbeatInterval),
                heartbeatInterval,
                "A heartbeat watchdog needs a positive interval; heartbeat 0 means no watchdog is started");
        }

        _silenceLimit = heartbeatInterval + heartbeatInterval; // two missed heartbeats
        _onTimeout = onTimeout;
        _lastActivity = Now;
    }

    /// <summary>The clock, virtual so tests can drive it with a fake.</summary>
    protected virtual DateTimeOffset Now => DateTimeOffset.UtcNow;

    /// <summary>True once the silence window expired and onTimeout was raised.</summary>
    public bool TimedOut { get; private set; }

    /// <summary>Records sign of life: WireConnection calls this for every inbound
    /// frame decoded — heartbeat frames, methods, content, anything (any type).</summary>
    public void OnFrameReceived() => _lastActivity = Now;

    /// <summary>Evaluates the silence window and raises onTimeout exactly once when
    /// the silence limit was reached with no inbound frame.</summary>
    public void Check()
    {
        if (TimedOut)
        {
            return;
        }

        if (Now - _lastActivity >= _silenceLimit)
        {
            TimedOut = true;
            _onTimeout?.Invoke(this);
        }
    }
}
