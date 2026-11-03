namespace RedisMQ.Broker.Server;

/// <summary>One broker-side SASL account: the PLAIN username/password pair.</summary>
public sealed record BrokerUser(string Name, string Password);

/// <summary>
/// Broker-wide settings: the TCP port for the Task-8 listener, the virtual hosts the
/// server makes openable, the PLAIN users, and the Tune-slots offered at handshake
/// (RabbitMQ defaults: 131072 frame-max, 60 s heartbeat). Heartbeat 0 in the options
/// means "no heartbeats offered"; a client TuneOk of 0 always disables them.
/// </summary>
public sealed class BrokerServerOptions
{
    public int Port { get; set; }

    public string[] Vhosts { get; set; } = ["/"];

    public IReadOnlyList<BrokerUser> Users { get; set; } = [];

    /// <summary>The frame-max the broker offers at connection.tune. The negotiated
    /// frame-max is a u32 quantity on the wire (long-uint; RabbitMQ offers 131072), so
    /// this is a uint rather than the brief's "ushort" placeholder — a ushort cannot
    /// even hold 131072 (pinned by the Task-2 frame codec tests).</summary>
    public uint FrameMax { get; set; } = 131072;

    /// <summary>The heartbeat the broker offers at connection.tune, in seconds.</summary>
    public ushort Heartbeat { get; set; } = 60;

    /// <summary>The number of publisher confirmations that may be outstanding on a
    /// connection before the frame pump blocks (deliberate backpressure, never a
    /// drop or an invented nack).</summary>
    public int ConfirmQueueCapacity { get; set; } = 4096;

    /// <summary>How long the confirm loop waits for a publish's durability receipts
    /// before answering confirm.nack.</summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How often the server sweeps expired messages and idle queues. The
    /// reaper is one shared loop for every listener; zero disables it.</summary>
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(5);
}
