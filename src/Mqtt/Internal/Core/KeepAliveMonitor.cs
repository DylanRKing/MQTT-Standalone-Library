using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Core;

/// <summary>
/// One timer per connection, ticking every second: sends PINGREQ when nothing has been sent for
/// the keep-alive interval, and reports the connection dead when no packet answers a PINGREQ
/// within half the interval, or when a PUBACK/SUBACK is overdue.
/// </summary>
internal sealed class KeepAliveMonitor(
    Connection connection,
    TimeSpan keepAlive,
    TimeSpan ackTimeout,
    TimeProvider time,
    Func<long?> oldestPendingAckTimestamp,
    Action<Connection, Exception> onDead,
    SafeLogger log)
{
    private readonly TimeSpan _tickInterval = TimeSpan.FromSeconds(1);

    public async Task RunAsync()
    {
        using var timer = new PeriodicTimer(_tickInterval, time);
        try
        {
            while (await timer.WaitForNextTickAsync(connection.Lifetime.Token).ConfigureAwait(false))
            {
                if (!Tick())
                {
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Connection torn down.
        }
    }

    /// <summary>Returns false once the connection has been reported dead.</summary>
    private bool Tick()
    {
        long pingSent = connection.PingSentTimestamp;
        if (pingSent != 0)
        {
            if (time.GetElapsedTime(pingSent) >= keepAlive / 2)
            {
                onDead(connection, new MqttConnectionLostException($"No response to PINGREQ within {(keepAlive / 2).TotalSeconds:0} s."));
                return false;
            }
        }
        else if (time.GetElapsedTime(connection.LastSentTimestamp) >= keepAlive)
        {
            connection.PingSentTimestamp = time.GetTimestamp();
            _ = SendPingAsync();
        }

        if (oldestPendingAckTimestamp() is long oldest && time.GetElapsedTime(oldest) >= ackTimeout)
        {
            onDead(connection, new MqttConnectionLostException($"No PUBACK or SUBACK within {ackTimeout.TotalSeconds:0} s."));
            return false;
        }

        return true;
    }

    private async Task SendPingAsync()
    {
        try
        {
            await connection.SendAsync(new PingReqPacket()).ConfigureAwait(false);
            log.Log(MqttLogLevel.Trace, "Sent PINGREQ.");
        }
        catch (Exception ex)
        {
            // The transport reports the lost connection itself.
            log.Log(MqttLogLevel.Debug, "PINGREQ not sent.", ex);
        }
    }
}
