using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;
using static Mqtt.Tests.Codec.CodecHelpers;

namespace Mqtt.Tests.Codec;

public class EncoderValidationTests
{
    // object, not IMqttPacket: public test methods cannot expose internal types.
    public static TheoryData<object> InvalidPackets() =>
    [
        new PublishPacket("a/+", ReadOnlyMemory<byte>.Empty),
        new PublishPacket("a/#", ReadOnlyMemory<byte>.Empty),
        new PublishPacket("", ReadOnlyMemory<byte>.Empty),
        new PublishPacket("a\0b", ReadOnlyMemory<byte>.Empty),
        new PublishPacket(new string('a', 65_536), ReadOnlyMemory<byte>.Empty),
        new PublishPacket("a", ReadOnlyMemory<byte>.Empty, MqttQos.AtLeastOnce, PacketId: 0),
        new PublishPacket("a", ReadOnlyMemory<byte>.Empty, MqttQos.AtMostOnce, PacketId: 5),
        new PublishPacket("a", ReadOnlyMemory<byte>.Empty, MqttQos.AtMostOnce, Dup: true),
        new PublishPacket("a", ReadOnlyMemory<byte>.Empty, (MqttQos)2, PacketId: 1),
        new PubAckPacket(0),
        new SubscribePacket(0, [new("a")]),
        new SubscribePacket(1, []),
        new SubscribePacket(1, [new("a/#/b")]),
        new SubscribePacket(1, [new("a", (MqttQos)2)]),
        new SubAckPacket(1, []),
        new SubAckPacket(1, [(ReasonCode)0x03]),
        new ConnAckPacket(true, ReasonCode.NotAuthorized),
        new ConnAckPacket(false, ReasonCode.UnspecifiedError),
        new ConnectPacket("", CleanSession: false, 60),
        new ConnectPacket("\uD800", CleanSession: true, 60),
    ];

    [Theory]
    [MemberData(nameof(InvalidPackets))]
    public void InvalidPacket_Throws(object packet)
    {
        Assert.ThrowsAny<ArgumentException>(() => new MqttEncoder().GetSize((IMqttPacket)packet, ProtocolVersion.V311));
    }

    [Fact]
    public void LongestAllowedString_Encodes()
    {
        var packet = new PublishPacket(new string('a', 65_535), ReadOnlyMemory<byte>.Empty);

        Assert.Equal(1 + 3 + 2 + 65_535, Encode(packet).Length);
    }

    [Fact]
    public void Write_DestinationTooSmall_Throws()
    {
        var packet = new PubAckPacket(1);
        var buffer = new byte[3];

        Assert.Throws<ArgumentException>(() => new MqttEncoder().Write(buffer, packet, ProtocolVersion.V311));
    }

    [Fact]
    public void Write_LargerDestination_ReturnsExactSize()
    {
        var buffer = new byte[64];

        int written = new MqttEncoder().Write(buffer, new PubAckPacket(1), ProtocolVersion.V311);

        Assert.Equal(4, written);
    }
}
