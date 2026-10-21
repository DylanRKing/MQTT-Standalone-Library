namespace Mqtt;

/// <summary>An application message received from the broker.</summary>
/// <param name="Topic">Topic the message was published to.</param>
/// <param name="Payload">Message body; a private copy that stays valid after the handler returns.</param>
/// <param name="Qos">QoS the broker delivered with.</param>
/// <param name="Retain">
/// True for a retained message replayed on subscribe; false for live traffic.
/// </param>
public sealed record MqttMessage(string Topic, ReadOnlyMemory<byte> Payload, MqttQos Qos, bool Retain);
