using System.Buffers.Binary;
using System.Text;

namespace Mqtt.Internal.Codec;

/// <summary>
/// Writes MQTT primitive types into a span. The caller sizes the span exactly using the
/// size helpers on <see cref="MqttEncoder"/>, so writes never bounds-check beyond the span itself.
/// </summary>
internal ref struct MqttWriter
{
    private readonly Span<byte> _buffer;
    private int _position;

    public MqttWriter(Span<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public readonly int Position => _position;

    public void WriteByte(byte value) => _buffer[_position++] = value;

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(_buffer[_position..], value);
        _position += 2;
    }

    /// <summary>Writes a UInt16 length prefix followed by the UTF-8 bytes of <paramref name="value"/>.</summary>
    public void WriteString(string value)
    {
        int length = Encoding.UTF8.GetBytes(value, _buffer[(_position + 2)..]);
        WriteUInt16((ushort)length);
        _position += length;
    }

    /// <summary>Writes raw bytes with no length prefix (PUBLISH payload).</summary>
    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        value.CopyTo(_buffer[_position..]);
        _position += value.Length;
    }

    public void WriteVariableByteInteger(int value)
    {
        do
        {
            byte encoded = (byte)(value & 0x7F);
            value >>= 7;
            if (value > 0)
            {
                encoded |= 0x80;
            }

            WriteByte(encoded);
        }
        while (value > 0);
    }
}
