namespace Mqtt.Internal.Core;

/// <summary>
/// Token bucket for outbound PUBLISHes: up to one second's worth in a burst, then a steady
/// <c>perSecond</c>. Callers past the rate wait their turn, in order, instead of AWS silently
/// discarding publishes over its per-connection limit. Thread-safe.
/// </summary>
internal sealed class PublishRateLimiter(int perSecond, TimeProvider time)
{
    private readonly Lock _lock = new();
    private double _tokens = perSecond;
    private long _lastRefill = time.GetTimestamp();

    public ValueTask WaitAsync(CancellationToken ct)
    {
        TimeSpan wait;
        lock (_lock)
        {
            long now = time.GetTimestamp();
            _tokens = Math.Min(perSecond, _tokens + (time.GetElapsedTime(_lastRefill, now).TotalSeconds * perSecond));
            _lastRefill = now;

            // Reserve a token even if it takes the balance negative; the debt sets the wait,
            // so later callers queue up behind earlier ones.
            _tokens -= 1;
            if (_tokens >= 0)
            {
                return ValueTask.CompletedTask;
            }

            wait = TimeSpan.FromSeconds(-_tokens / perSecond);
        }

        return new ValueTask(DelayAsync(wait, ct));
    }

    private async Task DelayAsync(TimeSpan wait, CancellationToken ct)
    {
        try
        {
            await Task.Delay(wait, time, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            lock (_lock)
            {
                _tokens += 1;
            }

            throw;
        }
    }
}
