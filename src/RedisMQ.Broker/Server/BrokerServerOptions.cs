namespace RedisMQ.Broker.Server;

/// <summary>One per-vhost permission triple: regexes matching the configure, write
/// and read names the account may touch. An empty pattern matches only the empty
/// name (effectively deny); <c>.*</c> grants everything.</summary>
public sealed record BrokerPermission(string Vhost, string Configure, string Write, string Read);

/// <summary>
/// One broker-side SASL account: the PLAIN username plus exactly one credential form
/// (a development-only plaintext password or a PBKDF2 hash) and the per-vhost
/// permissions. Null or empty permissions deny every vhost; a vhost the list does not
/// mention is not openable at all.
/// </summary>
public sealed record BrokerUser(
    string Name,
    string? Password = null,
    string? PasswordHash = null,
    IReadOnlyList<BrokerPermission>? Permissions = null)
{
    /// <summary>The explicit full-access helper for tests and samples: configure,
    /// write and read everything on each named vhost.</summary>
    public static BrokerUser FullAccess(string name, string password, params string[] vhosts)
    {
        ArgumentNullException.ThrowIfNull(vhosts);
        return new BrokerUser(
            name,
            password,
            Permissions: [.. vhosts.Select(vhost => new BrokerPermission(vhost, ".*", ".*", ".*"))]);
    }
}

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
    /// before answering basic.nack.</summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How often the server sweeps expired messages and idle queues. The
    /// reaper is one shared loop for every listener; zero disables it.</summary>
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The TLS listener settings; null or disabled means no TLS listener.</summary>
    public BrokerTlsOptions? Tls { get; set; }

    /// <summary>
    /// Validates the account list at startup: every user names exactly one credential
    /// form, every PBKDF2 hash is well formed, every permission regex compiles, and no
    /// two users share a name. Throws <see cref="BrokerConfigurationException"/> when
    /// the broker cannot safely start.
    /// </summary>
    public void Validate()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (BrokerUser user in Users)
        {
            ArgumentNullException.ThrowIfNull(user);
            if (!names.Add(user.Name))
            {
                throw new BrokerConfigurationException($"Duplicate broker user '{user.Name}'");
            }

            bool hasPassword = user.Password is not null;
            bool hasHash = user.PasswordHash is not null;
            if (hasPassword == hasHash)
            {
                throw new BrokerConfigurationException(
                    $"Broker user '{user.Name}' must set exactly one of Password or PasswordHash");
            }

            if (hasHash && !BrokerPassword.IsWellFormed(user.PasswordHash!))
            {
                throw new BrokerConfigurationException(
                    $"Broker user '{user.Name}' has a malformed PasswordHash " +
                    "(expected pbkdf2-sha256$<iterations>$<base64 salt>$<base64 hash>)");
            }

            // Compiles every permission triple; it throws BrokerConfigurationException
            // for an invalid pattern, which is exactly the startup contract.
            _ = new BrokerPrincipal(user.Name, user.Permissions ?? []);
        }
    }
}
