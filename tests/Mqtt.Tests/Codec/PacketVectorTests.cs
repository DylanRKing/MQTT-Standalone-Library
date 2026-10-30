using System.Text;
using Mqtt.Internal.Codec.Packets;
using static Mqtt.Tests.Codec.CodecHelpers;

namespace Mqtt.Tests.Codec;

/// <summary>Golden byte vectors: each packet must encode to exactly these bytes and decode back.</summary>
public class PacketVectorTests
{
    [Fact]
    public void Connect_CleanSession()
    {
        var packet = new ConnectPacket("dev1", CleanSession: true, KeepAliveSeconds: 60);
        byte[] expected = Hex("10 10 0004 4D515454 04 02 003C 0004 64657631");

        Assert.Equal(expected, Encode(packet));
        Assert.Equal(packet, Decode<ConnectPacket>(expected));
    }

    [Fact]
    public void Connect_PersistentSession()
    {
        var packet = new ConnectPacket("dev1", CleanSession: false, KeepAliveSeconds: 1200);
        byte[] expected = Hex("10 10 0004 4D515454 04 00 04B0 0004 64657631");

        Assert.Equal(expected, Encode(packet));
        Assert.Equal(packet, Decode<ConnectPacket>(expected));
    }

    [Theory]
    [InlineData("20 02 00 00", false, ReasonCode.Success)]
    [InlineData("20 02 01 00", true, ReasonCode.Success)]
    [InlineData("20 02 00 01", false, ReasonCode.UnsupportedProtocolVersion)]
    [InlineData("20 02 00 02", false, ReasonCode.ClientIdentifierNotValid)]
    [InlineData("20 02 00 03", false, ReasonCode.ServerUnavailable)]
    [InlineData("20 02 00 04", false, ReasonCode.BadUserNameOrPassword)]
    [InlineData("20 02 00 05", false, ReasonCode.NotAuthorized)]
    public void ConnAck(string hex, bool sessionPresent, ReasonCode code)
    {
        var packet = new ConnAckPacket(sessionPresent, code);

        Assert.Equal(Hex(hex), Encode(packet));
        Assert.Equal(packet, Decode<ConnAckPacket>(Hex(hex)));
    }

    [Fact]
    public void Publish_Qos0()
    {
        var packet = new PublishPacket("a/b", "hi"u8.ToArray());
        byte[] expected = Hex("30 07 0003 612F62 6869");

        Assert.Equal(expected, Encode(packet));
        AssertPublishEqual(packet, Decode<PublishPacket>(expected));
    }

    [Fact]
    public void Publish_Qos1_DupRetain()
    {
        var packet = new PublishPacket("a/b", "hi"u8.ToArray(), MqttQos.AtLeastOnce, Retain: true, Dup: true, PacketId: 10);
        byte[] expected = Hex("3B 09 0003 612F62 000A 6869");

        Assert.Equal(expected, Encode(packet));
        AssertPublishEqual(packet, Decode<PublishPacket>(expected));
    }

    [Fact]
    public void Publish_EmptyRetainedPayload_ClearsRetained()
    {
        var packet = new PublishPacket("a", ReadOnlyMemory<byte>.Empty, Retain: true);
        byte[] expected = Hex("31 03 0001 61");

        Assert.Equal(expected, Encode(packet));
        AssertPublishEqual(packet, Decode<PublishPacket>(expected));
    }

    [Fact]
    public void Publish_LargePayload_UsesMultiByteRemainingLength()
    {
        byte[] payload = new byte[128 * 1024];
        Random.Shared.NextBytes(payload);
        var packet = new PublishPacket("t", payload, MqttQos.AtLeastOnce, PacketId: 1);

        byte[] frame = Encode(packet);

        // 3 (topic) + 2 (id) + 131,072 = 131,077 = 0x85 0x80 0x08.
        Assert.Equal(Hex("32 85 80 08"), frame[..4]);
        AssertPublishEqual(packet, Decode<PublishPacket>(frame));
    }

    [Fact]
    public void Publish_Utf8Topic()
    {
        var packet = new PublishPacket("sensör/€", "x"u8.ToArray());
        byte[] frame = Encode(packet);

        Assert.Equal(Encoding.UTF8.GetByteCount(packet.Topic), (frame[2] << 8) | frame[3]);
        AssertPublishEqual(packet, Decode<PublishPacket>(frame));
    }

    [Fact]
    public void Publish_DecodedPayloadIsCopiedOutOfFrame()
    {
        byte[] frame = Hex("30 07 0003 612F62 6869");
        var decoded = Decode<PublishPacket>(frame);

        frame[^1] = 0xFF;

        Assert.Equal("hi"u8.ToArray(), decoded.Payload.ToArray());
    }

    [Fact]
    public void PubAck()
    {
        var packet = new PubAckPacket(42);
        byte[] expected = Hex("40 02 002A");

        Assert.Equal(expected, Encode(packet));
        Assert.Equal(packet, Decode<PubAckPacket>(expected));
    }

    [Fact]
    public void Subscribe()
    {
        var packet = new SubscribePacket(1, [new("a/+", MqttQos.AtLeastOnce), new("#", MqttQos.AtMostOnce)]);
        byte[] expected = Hex("82 0C 0001 0003 612F2B 01 0001 23 00");

        Assert.Equal(expected, Encode(packet));
        var decoded = Decode<SubscribePacket>(expected);
        Assert.Equal(packet.PacketId, decoded.PacketId);
        Assert.Equal(packet.Filters, decoded.Filters);
    }

    [Fact]
    public void SubAck()
    {
        var packet = new SubAckPacket(1, [ReasonCode.GrantedQos1, ReasonCode.UnspecifiedError, ReasonCode.Success]);
        byte[] expected = Hex("90 05 0001 01 80 00");

        Assert.Equal(expected, Encode(packet));
        var decoded = Decode<SubAckPacket>(expected);
        Assert.Equal(packet.PacketId, decoded.PacketId);
        Assert.Equal(packet.ReasonCodes, decoded.ReasonCodes);
    }

    [Fact]
    public void EmptyPackets()
    {
        Assert.Equal(Hex("C0 00"), Encode(new PingReqPacket()));
        Assert.Equal(Hex("D0 00"), Encode(new PingRespPacket()));
        Assert.Equal(Hex("E0 00"), Encode(new DisconnectPacket()));

        Decode<PingReqPacket>(Hex("C0 00"));
        Decode<PingRespPacket>(Hex("D0 00"));
        Decode<DisconnectPacket>(Hex("E0 00"));
    }

    private static void AssertPublishEqual(PublishPacket expected, PublishPacket actual)
    {
        Assert.Equal(expected.Topic, actual.Topic);
        Assert.Equal(expected.Qos, actual.Qos);
        Assert.Equal(expected.Retain, actual.Retain);
        Assert.Equal(expected.Dup, actual.Dup);
        Assert.Equal(expected.PacketId, actual.PacketId);
        Assert.True(expected.Payload.Span.SequenceEqual(actual.Payload.Span));
    }
}
