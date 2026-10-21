namespace Mqtt.Internal.Codec.Packets;

/// <summary>
/// SUBSCRIBE. The codec accepts any number of filters; splitting into AWS's batches of 8 is the core's job.
/// </summary>
internal sealed record SubscribePacket(ushort PacketId, IReadOnlyList<TopicFilter> Filters) : IMqttPacket
{
    public PacketType Type => PacketType.Subscribe;
}
