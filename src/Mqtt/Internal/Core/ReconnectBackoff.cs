namespace Mqtt.Internal.Core;

/// <summary>
/// Jittered exponential backoff: delay is random between the floor and
/// min(MaxDelay, InitialDelay × 2^attempt). The attempt counter resets only after a connection
/// stays up for <see cref="StableAfter"/>, so a device that connects and is dropped at once keeps
/// backing off. Used only under the client's lifecycle lock.
/// </summary>
internal sealed class ReconnectBackoff(ReconnectOptions options, TimeSpan floor, Random random)
{
    public static readonly TimeSpan StableAfter = TimeSpan.FromSeconds(30);

    private int _attempt;

    public int Attempt => _attempt;

    public void OnConnectionLost(TimeSpan uptime)
    {
        if (uptime >= StableAfter)
        {
            _attempt = 0;
        }
    }

    public TimeSpan NextDelay()
    {
        TimeSpan lower = floor;
        TimeSpan initial = options.InitialDelay > floor ? options.InitialDelay : floor;
        TimeSpan max = options.MaxDelay > lower ? options.MaxDelay : lower;

        // Cap the exponent so the multiplication cannot overflow.
        double upperTicks = Math.Min(max.Ticks, initial.Ticks * Math.Pow(2, Math.Min(_attempt, 30)));
        _attempt++;
        double ticks = lower.Ticks + (random.NextDouble() * (upperTicks - lower.Ticks));
        return TimeSpan.FromTicks((long)Math.Max(lower.Ticks, ticks));
    }
}
