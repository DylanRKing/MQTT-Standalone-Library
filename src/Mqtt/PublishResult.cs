namespace Mqtt;

/// <summary>Outcome of <see cref="MqttClient.PublishAsync"/>. Expected outcomes are values, not exceptions.</summary>
public enum PublishResult
{
    /// <summary>QoS 0: written to the TLS stream.</summary>
    Sent,

    /// <summary>QoS 1: PUBACK received from the broker.</summary>
    Acknowledged,

    /// <summary>QoS 0 while not connected: the message was dropped.</summary>
    DroppedOffline,

    /// <summary>QoS 1 while not connected and the offline queue is full: the message was not queued.</summary>
    QueueFull,
}
