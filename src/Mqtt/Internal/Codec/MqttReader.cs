using System.Buffers.Binary;
using System.Text;
using System.Text.Unicode;

namespace Mqtt.Internal.Codec;

/// <summary>
/// Reads MQTT primitive types from a span. Every method is a <c>Try</c> method so malformed
/// input is reported without exceptions on the receive path.
/// </summary>
internal ref struct MqttReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _position;

    public MqttReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public readonly int Remaining => _buffer.Length - _position;

    public readonly bool IsEmpty => _position == _buffer.Length;

    public bool TryReadByte(out byte value)
    {
        if (Remaining < 1)
        {
            value = 0;
            return false;
        }

        value = _buffer[_position++];
        return true;
    }

    public bool TryReadUInt16(out ushort value)
    {
        if (!BinaryPrimitives.TryReadUInt16BigEndian(_buffer[_position..], out value))
        {
            return false;
        }

        _position += 2;
        return true;
    }

    /// <summary>
    /// Reads a length-prefixed UTF-8 string. Fails on truncation, invalid UTF-8 or U+0000.
    /// </summary>
    public bool TryReadString(out string value)
    {
        value = string.Empty;
        if (!TryReadUInt16(out ushort length) || Remaining < length)
        {
            return false;
        }

        ReadOnlySpan<byte> bytes = _buffer.Slice(_position, length);
        if (bytes.Contains((byte)0) || !Utf8.IsValid(bytes))
        {
            return false;
        }

        value = Encoding.UTF8.GetString(bytes);
        _position += length;
        return true;
    }

    /// <summary>Returns everything not yet read and advances to the end.</summary>
    public ReadOnlySpan<byte> ReadRemaining()
    {
        ReadOnlySpan<byte> rest = _buffer[_position..];
        _position = _buffer.Length;
        return rest;
    }
}
