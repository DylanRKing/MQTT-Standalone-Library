namespace Mqtt.Internal.Codec.Packets;

/// <summary>PINGREQ; has no body.</summary>
internal sealed class PingReqPacket : IMqttPacket
{
    public PacketType Type => PacketType.PingReq;
}

/// <summary>PINGRESP; has no body.</summary>
internal sealed class PingRespPacket : IMqttPacket
{
    public PacketType Type => PacketType.PingResp;
}

/// <summary>DISCONNECT; has no body in 3.1.1.</summary>
internal sealed class DisconnectPacket : IMqttPacket
{
    public PacketType Type => PacketType.Disconnect;
}
