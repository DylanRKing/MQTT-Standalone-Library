using System.Security.Cryptography.X509Certificates;

namespace Mqtt.Simple;

/// <summary>Where and how <see cref="SimpleMqttClient"/> connects to AWS IoT Core.</summary>
public sealed class SimpleMqttClientOptions
{
    /// <summary>AWS IoT endpoint, e.g. <c>abc123-ats.iot.eu-west-1.amazonaws.com</c>.</summary>
    public required string Host { get; init; }

    /// <summary>Client ID; for AWS normally the thing name.</summary>
    public required string ClientId { get; init; }

    /// <summary>Device certificate with its private key. See <see cref="AwsCertificateLoader"/>.</summary>
    public required X509Certificate2 Certificate { get; init; }

    /// <summary>
    /// Amazon's root CA (<c>AmazonRootCA1.pem</c>). Optional when the machine already trusts it,
    /// which most do; set it on machines that don't.
    /// </summary>
    public X509Certificate2? RootCa { get; init; }

    /// <summary>8883 for MQTT over TLS.</summary>
    public int Port { get; init; } = 8883;

    /// <summary>Sent to AWS in CONNECT; a ping goes out every half of this. AWS accepts 30 s to 1200 s.</summary>
    public TimeSpan KeepAlive { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>How long to wait for a PUBACK or SUBACK before failing the call.</summary>
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);
}
