namespace Mqtt;

/// <summary>Connection state of an <see cref="MqttClient"/>.</summary>
public enum MqttConnectionState
{
    /// <summary>Not connected, and not trying to connect.</summary>
    Disconnected,

    /// <summary><see cref="MqttClient.ConnectAsync"/> is in progress.</summary>
    Connecting,

    /// <summary>CONNACK accepted; the client can publish and subscribe.</summary>
    Connected,

    /// <summary>The connection was lost and the client is reconnecting automatically.</summary>
    Reconnecting,

    /// <summary>The client has been disposed.</summary>
    Disposed,
}

/// <summary>Raised through <see cref="MqttClient.ConnectionStateChanged"/>.</summary>
/// <param name="Previous">State before the change.</param>
/// <param name="Current">State after the change.</param>
/// <param name="Reason">Why the connection ended, when it ended unexpectedly; otherwise null.</param>
public sealed record ConnectionStateChange(MqttConnectionState Previous, MqttConnectionState Current, Exception? Reason);
