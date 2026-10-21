namespace Mqtt;

/// <summary>
/// <see cref="MqttClient.ConnectAsync"/> failed: the broker refused the CONNECT, the TLS handshake
/// failed, the broker could not be reached, or no CONNACK arrived within the connect timeout.
/// </summary>
public class MqttConnectException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public MqttConnectException()
        : base("Connecting to the broker failed.")
    {
    }

    /// <summary>Creates the exception with a message.</summary>
    public MqttConnectException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and the underlying cause.</summary>
    public MqttConnectException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates the exception for a CONNACK refusal.</summary>
    public MqttConnectException(string message, ReasonCode reasonCode)
        : base(message)
    {
        ReasonCode = reasonCode;
    }

    /// <summary>The CONNACK refusal code, or null when no CONNACK was received.</summary>
    public ReasonCode? ReasonCode { get; }
}
