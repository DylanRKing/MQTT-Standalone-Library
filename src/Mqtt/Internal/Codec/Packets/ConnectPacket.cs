namespace Mqtt.Internal.Codec.Packets;

/// <summary>
/// CONNECT. User name, password and will are not supported yet; the flags for them are always clear.
/// </summary>
internal sealed record ConnectPacket(string ClientId, bool CleanSession, ushort KeepAliveSeconds) : IMqttPacket
{
    public PacketType Type => PacketType.Connect;
}
