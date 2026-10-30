using System.Net.Security;
using System.Text;

namespace Mqtt;

/// <summary>
/// Immutable settings for one <see cref="MqttClient"/>. Validated when the client is constructed.
/// </summary>
public sealed record MqttClientOptions
{
    /// <summary>AWS IoT Core client ID limit, in UTF-8 bytes.</summary>
    public int MaxClientIdBytes => 128;

    /// <summary>Lowest keep-alive AWS honours; shorter values are raised to this.</summary>
    public TimeSpan MinKeepAlive => TimeSpan.FromSeconds(30);

    /// <summary>Highest keep-alive AWS honours; longer values are lowered to this.</summary>
    public TimeSpan MaxKeepAlive => TimeSpan.FromSeconds(1200);

    /// <summary>AWS limit on unacknowledged QoS 1 messages per connection.</summary>
    public int MaxInFlightLimit => 100;

    /// <summary>Broker host name, normally the account's AWS IoT ATS data endpoint.</summary>
    public required string Host { get; init; }

    /// <summary>Broker port; 8883 for MQTT over TLS.</summary>
    public int Port { get; init; } = 8883;

    /// <summary>MQTT client ID; required, at most 128 UTF-8 bytes. One connection per ID at a time.</summary>
    public required string ClientId { get; init; }

    /// <summary>True for a clean session on every connect; false for a persistent session.</summary>
    public bool CleanSession { get; init; } = true;

    /// <summary>Keep-alive interval; clamped to 30–1,200 s to match AWS.</summary>
    public TimeSpan KeepAlive { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Supplies the client certificate on every connect. Required.</summary>
    public required ICertificateProvider CertificateProvider { get; init; }

    /// <summary>Time allowed for TCP connect, TLS handshake and CONNACK together.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Time to wait for a PUBACK or SUBACK before the connection is treated as unhealthy and closed.</summary>
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Unacknowledged outbound QoS 1 publishes allowed at once; callers past this wait. At most 100.</summary>
    public int MaxInFlight { get; init; } = 20;

    /// <summary>QoS 1 publishes held in memory while connecting or reconnecting.</summary>
    public int OfflineQueueCapacity { get; init; } = 1_000;

    /// <summary>What a QoS 1 publish does when the offline queue is full.</summary>
    public OfflineQueueFullMode OfflineQueueFullMode { get; init; } = OfflineQueueFullMode.RejectNew;

    /// <summary>Automatic reconnect settings; enabled by default.</summary>
    public ReconnectOptions Reconnect { get; init; } = new();

    /// <summary>
    /// Shared limit on concurrent connects across clients; null to connect immediately.
    /// Pass the same instance to every client's options.
    /// </summary>
    public ConnectionThrottle? ConnectionThrottle { get; init; }

    /// <summary>
    /// Outbound publishes per second allowed on this connection, null for no limit. Publishes over
    /// the rate wait instead of being sent. AWS discards publishes beyond 100 per second per
    /// connection, inbound and outbound combined, so leave room for inbound traffic.
    /// </summary>
    public int? MaxPublishRate { get; init; }

    /// <summary>Receives log output; null for none.</summary>
    public IMqttLogger? Logger { get; init; }

    /// <summary>Keep-alive actually sent in CONNECT, after clamping.</summary>
    public TimeSpan EffectiveKeepAlive =>
        KeepAlive < MinKeepAlive ? MinKeepAlive : KeepAlive > MaxKeepAlive ? MaxKeepAlive : KeepAlive;

    /// <summary>Overrides server certificate validation (tests; a pinned-root option later).</summary>
    internal RemoteCertificateValidationCallback? ServerCertificateValidator { get; init; }

    /// <summary>Clock for keep-alive, ack and reconnect timing; replaced in tests.</summary>
    internal TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Shortest reconnect delay; AWS allows one CONNECT per second per client ID. Lowered in tests.</summary>
    internal TimeSpan ReconnectDelayFloor { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Jitter source for reconnect delays; seeded in tests.</summary>
    internal Random Random { get; init; } = Random.Shared;

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
        {
            throw new ArgumentException("Host is required.", nameof(Host));
        }

        if (Port is < 1 or > 65_535)
        {
            throw new ArgumentOutOfRangeException(nameof(Port), Port, "Port must be 1–65,535.");
        }

        if (string.IsNullOrEmpty(ClientId))
        {
            throw new ArgumentException("ClientId is required.", nameof(ClientId));
        }

        if (Encoding.UTF8.GetByteCount(ClientId) > MaxClientIdBytes || ClientId.Contains('\0'))
        {
            throw new ArgumentException($"ClientId must be at most {MaxClientIdBytes} UTF-8 bytes with no U+0000.", nameof(ClientId));
        }

        if (CertificateProvider is null)
        {
            throw new ArgumentException("CertificateProvider is required.", nameof(CertificateProvider));
        }

        if (ConnectTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ConnectTimeout), ConnectTimeout, "ConnectTimeout must be positive.");
        }

        if (AckTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(AckTimeout), AckTimeout, "AckTimeout must be positive.");
        }

        if (MaxInFlight < 1 || MaxInFlight > MaxInFlightLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxInFlight), MaxInFlight, $"MaxInFlight must be 1–{MaxInFlightLimit}.");
        }

        if (MaxPublishRate is < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxPublishRate), MaxPublishRate, "MaxPublishRate must be at least 1, or null for no limit.");
        }

        if (OfflineQueueCapacity < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(OfflineQueueCapacity), OfflineQueueCapacity, "OfflineQueueCapacity cannot be negative.");
        }

        if (!Enum.IsDefined(OfflineQueueFullMode))
        {
            throw new ArgumentOutOfRangeException(nameof(OfflineQueueFullMode), OfflineQueueFullMode, "Unknown OfflineQueueFullMode.");
        }

        if (Reconnect is null)
        {
            throw new ArgumentException("Reconnect is required; set Enabled = false to turn it off.", nameof(Reconnect));
        }

        if (Reconnect.InitialDelay <= TimeSpan.Zero || Reconnect.MaxDelay < Reconnect.InitialDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(Reconnect), "Reconnect delays must be positive, with MaxDelay at least InitialDelay.");
        }

        if (Reconnect.MaxAttempts is < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(Reconnect), Reconnect.MaxAttempts, "Reconnect.MaxAttempts must be at least 1, or null for unlimited.");
        }
    }
}
