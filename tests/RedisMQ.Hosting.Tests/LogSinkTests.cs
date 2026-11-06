using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using RedisMQ.Broker.Diagnostics;
using RedisMQ.Hosting;
using Xunit;

namespace RedisMQ.Hosting.Tests;

public sealed class LogSinkTests
{
    [Fact]
    public void Log_sink_maps_level_event_id_and_structured_data()
    {
        var factory = new CapturingLoggerFactory();
        var sink = new LoggerBrokerLogSink(factory);

        sink.Log(new BrokerLogEntry(
            BrokerLogLevel.Warning,
            BrokerLogEvent.ConfirmNack,
            "publish confirm refused",
            null,
            [new KeyValuePair<string, object?>("sequence", 7L)]));

        CapturedLog entry = Assert.Single(factory.Logger.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal((int)BrokerLogEvent.ConfirmNack, entry.EventId);
        Assert.Equal("publish confirm refused", entry.Message);
        Assert.Contains(entry.Data, pair => pair.Key == "sequence" && (long?)pair.Value == 7L);
    }

    [Fact]
    public void Log_sink_is_enabled_uses_the_mapped_level()
    {
        var factory = new CapturingLoggerFactory();
        var sink = new LoggerBrokerLogSink(factory);
        Assert.True(sink.IsEnabled(BrokerLogLevel.Error));
        Assert.False(sink.IsEnabled(BrokerLogLevel.Trace));
    }

    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public CapturingLogger Logger { get; } = new();
        public ILogger CreateLogger(string categoryName) => Logger;
        public void AddProvider(ILoggerProvider provider) { }
        public void Dispose() { }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<CapturedLog> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            IReadOnlyList<KeyValuePair<string, object?>> data =
                state as IReadOnlyList<KeyValuePair<string, object?>> ?? [];
            Entries.Add(new CapturedLog(logLevel, eventId.Id, formatter(state, exception), data));
        }
    }

    private sealed record CapturedLog(
        LogLevel Level,
        int EventId,
        string Message,
        IReadOnlyList<KeyValuePair<string, object?>> Data);
}
