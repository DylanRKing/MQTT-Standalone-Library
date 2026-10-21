using Mqtt.Internal.Codec;
using static Mqtt.Tests.Codec.CodecHelpers;

namespace Mqtt.Tests.Codec;

public class MalformedInputTests
{
    [Theory]
    [InlineData("", "empty frame")]
    [InlineData("40", "header with no length")]
    [InlineData("40 03 0001", "frame shorter than remaining length")]
    [InlineData("40 02 0001 FF", "frame longer than remaining length")]
    [InlineData("30 FF FF FF FF 7F", "five-byte remaining length")]
    [InlineData("00 00", "reserved type 0")]
    [InlineData("F0 00", "reserved type 15")]
    [InlineData("41 02 0001", "PUBACK with flags set")]
    [InlineData("80 0C 0001 0003 612F2B 01 0001 23 00", "SUBSCRIBE without 0010 flags")]
    [InlineData("C1 00", "PINGREQ with flags set")]
    [InlineData("D0 01 00", "PINGRESP with a body")]
    [InlineData("E0 01 00", "DISCONNECT with a body")]
    [InlineData("40 02 0000", "PUBACK packet ID 0")]
    [InlineData("40 01 00", "PUBACK truncated")]
    [InlineData("20 02 02 00", "CONNACK reserved flag bits")]
    [InlineData("20 02 00 06", "CONNACK return code 6")]
    [InlineData("20 02 01 05", "CONNACK session present on refusal")]
    [InlineData("20 03 00 00 00", "CONNACK too long")]
    [InlineData("36 05 0003 612F62", "PUBLISH QoS 3")]
    [InlineData("38 05 0003 612F62", "PUBLISH QoS 0 with DUP")]
    [InlineData("32 07 0003 612F62 0000", "PUBLISH QoS 1 packet ID 0")]
    [InlineData("32 06 0003 612F62 00", "PUBLISH QoS 1 truncated packet ID")]
    [InlineData("30 05 0003 612F2B", "PUBLISH topic with +")]
    [InlineData("30 05 0003 612F23", "PUBLISH topic with #")]
    [InlineData("30 02 0000", "PUBLISH empty topic")]
    [InlineData("30 04 0002 C328", "PUBLISH topic invalid UTF-8")]
    [InlineData("30 04 0002 6100", "PUBLISH topic with U+0000")]
    [InlineData("30 04 0005 6162", "PUBLISH topic length beyond frame")]
    [InlineData("82 02 0001", "SUBSCRIBE with no filters")]
    [InlineData("82 06 0001 0001 61", "SUBSCRIBE missing QoS byte")]
    [InlineData("82 06 0001 0001 61 03", "SUBSCRIBE QoS 3")]
    [InlineData("82 06 0001 0001 61 05", "SUBSCRIBE reserved option bits")]
    [InlineData("82 07 0001 0002 612B 00", "SUBSCRIBE invalid filter")]
    [InlineData("90 02 0001", "SUBACK with no codes")]
    [InlineData("90 03 0001 03", "SUBACK invalid code")]
    [InlineData("10 10 0004 4D515455 04 02 003C 0004 64657631", "CONNECT wrong protocol name")]
    [InlineData("10 10 0004 4D515454 04 03 003C 0004 64657631", "CONNECT reserved flag")]
    [InlineData("10 0C 0004 4D515454 04 00 003C 0000", "CONNECT empty client ID without clean session")]
    public void Malformed(string hex, string because)
    {
        Assert.True(DecodeErrorOf(hex) == DecodeError.MalformedPacket, because);
    }

    [Theory]
    [InlineData("34 07 0003 612F62 0001", "PUBLISH QoS 2")]
    [InlineData("50 02 0001", "PUBREC")]
    [InlineData("62 02 0001", "PUBREL")]
    [InlineData("70 02 0001", "PUBCOMP")]
    [InlineData("A2 05 0001 0001 61", "UNSUBSCRIBE")]
    [InlineData("B0 02 0001", "UNSUBACK")]
    [InlineData("82 06 0001 0001 61 02", "SUBSCRIBE QoS 2")]
    [InlineData("10 10 0004 4D515454 05 02 003C 0004 64657631", "CONNECT protocol level 5")]
    [InlineData("10 10 0004 4D515454 04 C2 003C 0004 64657631", "CONNECT with user name and password")]
    [InlineData("10 10 0004 4D515454 04 06 003C 0004 64657631", "CONNECT with will")]
    public void Unsupported(string hex, string because)
    {
        Assert.True(DecodeErrorOf(hex) == DecodeError.UnsupportedPacket, because);
    }

    [Fact]
    public void FailedDecode_LeavesPacketNull()
    {
        MqttDecoder.TryDecode(Hex("40 02 0000"), ProtocolVersion.V311, out var packet);

        Assert.Null(packet);
    }
}
