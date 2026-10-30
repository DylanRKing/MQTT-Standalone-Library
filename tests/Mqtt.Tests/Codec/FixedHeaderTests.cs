using Mqtt.Internal.Codec;
using static Mqtt.Tests.Codec.CodecHelpers;

namespace Mqtt.Tests.Codec;

public class FixedHeaderTests
{
    [Theory]
    [InlineData(0, "00")]
    [InlineData(127, "7F")]
    [InlineData(128, "8001")]
    [InlineData(16_383, "FF7F")]
    [InlineData(16_384, "808001")]
    [InlineData(2_097_151, "FFFF7F")]
    [InlineData(2_097_152, "80808001")]
    [InlineData(268_435_455, "FFFFFF7F")]
    public void RemainingLength_RoundTrips(int value, string hex)
    {
        byte[] encoded = Hex(hex);
        var buffer = new byte[new MqttEncoder().GetVariableByteIntegerSize(value)];
        var writer = new MqttWriter(buffer);
        writer.WriteVariableByteInteger(value);

        Assert.Equal(encoded, buffer);

        byte[] header = [0x30, .. encoded];
        Assert.Equal(FrameStatus.Complete, new FixedHeader().TryRead(header, out byte typeAndFlags, out int remaining, out int headerLength));
        Assert.Equal(0x30, typeAndFlags);
        Assert.Equal(value, remaining);
        Assert.Equal(header.Length, headerLength);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(268_435_456)]
    public void RemainingLength_OutOfRange_Throws(int value)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MqttEncoder().GetVariableByteIntegerSize(value));
    }

    [Theory]
    [InlineData("")]
    [InlineData("30")]
    [InlineData("30 80")]
    [InlineData("30 FF FF")]
    [InlineData("30 FF FF FF")]
    public void PartialHeader_IsIncomplete(string hex)
    {
        Assert.Equal(FrameStatus.Incomplete, new FixedHeader().TryRead(Hex(hex), out _, out _, out _));
    }

    [Fact]
    public void FiveLengthBytes_IsMalformed()
    {
        Assert.Equal(FrameStatus.Malformed, new FixedHeader().TryRead(Hex("30 FF FF FF FF 01"), out _, out _, out _));
    }

    [Fact]
    public void HeaderIsReadWithoutTheBody()
    {
        // Transport case: only the first bytes of a large frame have arrived.
        Assert.Equal(FrameStatus.Complete, new FixedHeader().TryRead(Hex("32 85 80 08 00"), out _, out int remaining, out int headerLength));
        Assert.Equal(131_077, remaining);
        Assert.Equal(4, headerLength);
    }
}
