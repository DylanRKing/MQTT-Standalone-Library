namespace Mqtt.Internal.Codec.Packets;

/// <summary>
/// PUBLISH. <see cref="PacketId"/> is 0 for QoS 0 and 1–65,535 for QoS 1.
/// Decoded payloads are copies, never views of the transport's read buffer.
/// </summary>
internal sealed record PublishPacket(
    string Topic,
    ReadOnlyMemory<byte> Payload,
    MqttQos Qos = MqttQos.AtMostOnce,
    bool Retain = false,
    bool Dup = false,
    ushort PacketId = 0) : IMqttPacket
{
    public PacketType Type => PacketType.Publish;
}
