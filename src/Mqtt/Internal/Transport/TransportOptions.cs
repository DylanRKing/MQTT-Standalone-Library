using System.Net.Security;

namespace Mqtt.Internal.Transport;

/// <summary>Settings for one <see cref="MqttTransport"/> connection, built by the client core.</summary>
internal sealed record TransportOptions
{
    /// <summary>AWS's 128 KB payload limit plus room for the fixed header, topic and packet ID.</summary>
    public const int DefaultMaxFrameSize = (128 * 1024) + (4 * 1024);

    public required string Host { get; init; }

    public int Port { get; init; } = 8883;

    public required ICertificateProvider CertificateProvider { get; init; }

    /// <summary>Client ID used only to label log entries; the transport does not speak MQTT.</summary>
    public string ClientId { get; init; } = string.Empty;

    public IMqttLogger? Logger { get; init; }

    /// <summary>Largest inbound frame accepted; a larger one closes the connection.</summary>
    public int MaxFrameSize { get; init; } = DefaultMaxFrameSize;

    /// <summary>
    /// Overrides server certificate validation. Null uses the OS trust store, which covers the
    /// Amazon Root CA behind AWS ATS endpoints. Used for a pinned root, and by tests.
    /// </summary>
    public RemoteCertificateValidationCallback? ServerCertificateValidator { get; init; }

    /// <summary>Whether to check server certificate revocation online (an open design question; off for now).</summary>
    public bool CheckCertificateRevocation { get; init; }

    /// <summary>Frames that can wait for the write loop before senders are made to wait.</summary>
    public int SendQueueCapacity { get; init; } = 256;
}
