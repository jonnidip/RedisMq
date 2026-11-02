namespace RedisMQ.Broker.Server;

/// <summary>
/// Thrown when the broker's configuration is invalid and the broker cannot safely start.
/// Examples: Redis eviction policy that would discard durable data, missing required
/// configuration, incompatible runtime settings.
/// </summary>
public sealed class BrokerConfigurationException : Exception
{
    public BrokerConfigurationException(string message)
        : base(message)
    {
    }

    public BrokerConfigurationException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
