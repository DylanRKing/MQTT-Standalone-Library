namespace Mqtt.Internal.Codec.Packets;

/// <summary>CONNACK; the 3.1.1 return code is mapped to <see cref="Mqtt.ReasonCode"/>.</summary>
internal sealed record ConnAckPacket(bool SessionPresent, ReasonCode ReasonCode) : IMqttPacket
{
    public PacketType Type => PacketType.ConnAck;
}
