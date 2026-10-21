namespace Mqtt;

/// <summary>
/// Automatic reconnect after an established connection is lost. Delays are jittered: each
/// attempt waits a random time between 1 s and min(<see cref="MaxDelay"/>,
/// <see cref="InitialDelay"/> × 2^attempt). The 1 s floor respects AWS's limit of one CONNECT per
/// second per client ID, and the jitter spreads many devices out after a network drop.
/// </summary>
/// <remarks>
/// Not retried: CONNACK refusals other than "server unavailable", and TLS authentication
/// failures. The client then goes to <see cref="MqttConnectionState.Disconnected"/> with the reason.
/// </remarks>
public sealed record ReconnectOptions
{
    /// <summary>Reconnect turned off: a lost connection goes straight to Disconnected.</summary>
    public static ReconnectOptions Disabled { get; } = new() { Enabled = false };

    /// <summary>Whether to reconnect automatically.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Base delay that doubles with each failed attempt.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Upper bound on the delay between attempts.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Failed attempts per outage before giving up; null for unlimited.</summary>
    public int? MaxAttempts { get; init; }
}

/// <summary>What happens to a QoS 1 publish when the offline queue is full.</summary>
public enum OfflineQueueFullMode
{
    /// <summary>The new publish returns <see cref="PublishResult.QueueFull"/> at once.</summary>
    RejectNew,

    /// <summary>
    /// The oldest queued publish is dropped and its task returns <see cref="PublishResult.QueueFull"/>;
    /// the new one is queued.
    /// </summary>
    DropOldest,
}
