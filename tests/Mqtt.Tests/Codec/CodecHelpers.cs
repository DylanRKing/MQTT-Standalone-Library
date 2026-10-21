using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Tests.Codec;

internal static class CodecHelpers
{
    public static byte[] Encode(IMqttPacket packet)
    {
        var buffer = new byte[MqttEncoder.GetSize(packet, ProtocolVersion.V311)];
        int written = MqttEncoder.Write(buffer, packet, ProtocolVersion.V311);
        Assert.Equal(buffer.Length, written);
        return buffer;
    }

    public static T Decode<T>(byte[] frame)
        where T : IMqttPacket
    {
        DecodeError error = MqttDecoder.TryDecode(frame, ProtocolVersion.V311, out IMqttPacket? packet);
        Assert.Equal(DecodeError.None, error);
        return Assert.IsType<T>(packet);
    }

    public static DecodeError DecodeErrorOf(string hex) =>
        MqttDecoder.TryDecode(Convert.FromHexString(hex.Replace(" ", "")), ProtocolVersion.V311, out _);

    public static byte[] Hex(string hex) => Convert.FromHexString(hex.Replace(" ", ""));
}
