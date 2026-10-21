namespace Mqtt.Internal.Codec.Packets;

/// <summary>PINGREQ; has no body, so a single instance is shared.</summary>
internal sealed class PingReqPacket : IMqttPacket
{
    public static readonly PingReqPacket Instance = new();

    private PingReqPacket()
    {
    }

    public PacketType Type => PacketType.PingReq;
}

/// <summary>PINGRESP; has no body, so a single instance is shared.</summary>
internal sealed class PingRespPacket : IMqttPacket
{
    public static readonly PingRespPacket Instance = new();

    private PingRespPacket()
    {
    }

    public PacketType Type => PacketType.PingResp;
}

/// <summary>DISCONNECT; has no body in 3.1.1, so a single instance is shared.</summary>
internal sealed class DisconnectPacket : IMqttPacket
{
    public static readonly DisconnectPacket Instance = new();

    private DisconnectPacket()
    {
    }

    public PacketType Type => PacketType.Disconnect;
}
