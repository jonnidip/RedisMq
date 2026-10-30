namespace RedisMQ.Broker.Codec;

/// <summary>
/// Thrown when AMQP wire data is truncated mid-value or cannot be parsed
/// (unknown type letter, length inconsistent with the declared region,
/// unrepresentable value). Decode failures with no partial-buffer workaround
/// surface through <see cref="ByteReader.TryReadFieldTable"/>'s bool return instead.
/// </summary>
public sealed class AmqpDecodeException : Exception
{
    public AmqpDecodeException(string message)
        : base(message)
    {
    }

    public AmqpDecodeException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Thrown when a CLR value cannot be expressed in AMQP 0-9-1 wire form
/// (shortstr over 255 UTF-8 bytes, decimal mantissa wider than the 32-bit
/// wire form, unsupported field-table value type).
/// </summary>
public sealed class AmqpEncodeException : Exception
{
    public AmqpEncodeException(string message)
        : base(message)
    {
    }

    public AmqpEncodeException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
