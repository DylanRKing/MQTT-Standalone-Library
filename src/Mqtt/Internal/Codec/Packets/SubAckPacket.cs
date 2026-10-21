namespace Mqtt.Internal.Codec.Packets;

/// <summary>SUBACK; one reason code per filter, in request order.</summary>
internal sealed record SubAckPacket(ushort PacketId, IReadOnlyList<ReasonCode> ReasonCodes) : IMqttPacket
{
    public PacketType Type => PacketType.SubAck;
}
