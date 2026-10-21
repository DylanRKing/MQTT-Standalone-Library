namespace Mqtt.Internal.Codec.Packets;

/// <summary>An immutable, decoded MQTT control packet.</summary>
internal interface IMqttPacket
{
    PacketType Type { get; }
}
