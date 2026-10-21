using System.Collections.Concurrent;

namespace Mqtt.Tests.TestSupport;

internal sealed class TestLogger : IMqttLogger
{
    public ConcurrentQueue<(MqttLogLevel Level, string ClientId, string Message)> Entries { get; } = new();

    public void Log(MqttLogLevel level, string clientId, string message, Exception? exception) =>
        Entries.Enqueue((level, clientId, message));

    public bool Contains(MqttLogLevel level, string text) => Entries.Any(e => e.Level == level && e.Message.Contains(text));
}
