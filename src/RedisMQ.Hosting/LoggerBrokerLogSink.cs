using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Diagnostics;

namespace RedisMQ.Hosting;

public sealed class LoggerBrokerLogSink : IBrokerLogSink
{
    private readonly ILogger _logger;

    public LoggerBrokerLogSink(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _logger = loggerFactory.CreateLogger("RedisMQ.Broker");
    }

    public bool IsEnabled(BrokerLogLevel level) => _logger.IsEnabled(Map(level));

    public void Log(in BrokerLogEntry entry)
    {
        // Copy the in-parameter: a lambda may not capture an in parameter.
        BrokerLogEntry value = entry;
        IReadOnlyList<KeyValuePair<string, object?>> data =
            value.Data ?? Array.Empty<KeyValuePair<string, object?>>();
        _logger.Log(
            Map(value.Level),
            new EventId((int)value.Event, value.Event.ToString()),
            data,
            value.Exception,
            (_, _) => value.Message);
    }

    private static LogLevel Map(BrokerLogLevel level) => level switch
    {
        BrokerLogLevel.Trace => LogLevel.Trace,
        BrokerLogLevel.Debug => LogLevel.Debug,
        BrokerLogLevel.Information => LogLevel.Information,
        BrokerLogLevel.Warning => LogLevel.Warning,
        BrokerLogLevel.Error => LogLevel.Error,
        BrokerLogLevel.Critical => LogLevel.Critical,
        _ => LogLevel.None,
    };
}
