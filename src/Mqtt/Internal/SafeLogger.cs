namespace Mqtt.Internal;

/// <summary>
/// Wraps an optional <see cref="IMqttLogger"/> so a missing or throwing logger never affects the client.
/// </summary>
internal readonly struct SafeLogger(IMqttLogger? logger, string clientId)
{
    public bool IsEnabled => logger is not null;

    public void Log(MqttLogLevel level, string message, Exception? exception = null)
    {
        if (logger is null)
        {
            return;
        }

        try
        {
            logger.Log(level, clientId, message, exception);
        }
        catch
        {
            // App logging must never take down a connection.
        }
    }
}
