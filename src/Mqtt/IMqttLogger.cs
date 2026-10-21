namespace Mqtt;

/// <summary>Severity of a library log entry.</summary>
public enum MqttLogLevel
{
    /// <summary>Per-packet detail.</summary>
    Trace,

    /// <summary>Diagnostic detail.</summary>
    Debug,

    /// <summary>Normal lifecycle events such as connect and disconnect.</summary>
    Information,

    /// <summary>Recoverable problems such as a lost connection.</summary>
    Warning,

    /// <summary>Failures the client cannot recover from on its own.</summary>
    Error,
}

/// <summary>
/// Receives the library's log output. Every entry carries the client ID so logs from many
/// devices can be filtered per device. Payloads, private keys and certificate contents are
/// never logged; certificates are identified by thumbprint only.
/// </summary>
public interface IMqttLogger
{
    /// <summary>Writes one log entry. Exceptions thrown here are swallowed by the library.</summary>
    void Log(MqttLogLevel level, string clientId, string message, Exception? exception);
}
