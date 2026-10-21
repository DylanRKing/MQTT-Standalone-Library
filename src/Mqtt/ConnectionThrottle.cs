namespace Mqtt;

/// <summary>
/// Limits how many clients connect at once. Share one instance between clients through
/// <see cref="MqttClientOptions.ConnectionThrottle"/>: TLS handshakes are CPU-heavy, so starting
/// 100 devices, or recovering them all after a network drop, goes smoother in waves.
/// </summary>
/// <remarks>
/// A slot is held from the TCP connect until CONNACK (or failure). Time spent waiting for a slot
/// does not count towards <see cref="MqttClientOptions.ConnectTimeout"/>.
/// </remarks>
public sealed class ConnectionThrottle
{
    private readonly SemaphoreSlim _slots;

    /// <summary>Creates a throttle allowing <paramref name="maxConcurrentConnects"/> connects at a time.</summary>
    public ConnectionThrottle(int maxConcurrentConnects)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrentConnects, 1);
        MaxConcurrentConnects = maxConcurrentConnects;
        _slots = new SemaphoreSlim(maxConcurrentConnects, maxConcurrentConnects);
    }

    /// <summary>Connects allowed at the same time.</summary>
    public int MaxConcurrentConnects { get; }

    internal Task WaitAsync(CancellationToken ct) => _slots.WaitAsync(ct);

    internal void Release() => _slots.Release();
}
