namespace Mqtt.Internal.Codec.Packets;

internal sealed record PubAckPacket(ushort PacketId) : IMqttPacket
{
    public PacketType Type => PacketType.PubAck;
}
