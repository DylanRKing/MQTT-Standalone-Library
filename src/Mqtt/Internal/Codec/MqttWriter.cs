using System.Buffers.Binary;
using System.Text;

namespace Mqtt.Internal.Codec;

/// <summary>
/// Writes MQTT primitive types into a span. The caller sizes the span exactly using the
/// matching <c>Get*Size</c> helpers, so writes never bounds-check beyond the span itself.
/// </summary>
internal ref struct MqttWriter
{
    /// <summary>Largest value the variable-length remaining-length field can carry.</summary>
    public const int MaxVariableByteInteger = 268_435_455;

    /// <summary>Largest UTF-8 string or binary field, limited by its UInt16 length prefix.</summary>
    public const int MaxStringBytes = ushort.MaxValue;

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

    public static int GetVariableByteIntegerSize(int value) => value switch
    {
        < 0 or > MaxVariableByteInteger => throw new ArgumentOutOfRangeException(nameof(value), value, "Remaining length exceeds the MQTT maximum of 268,435,455 bytes."),
        < 128 => 1,
        < 16_384 => 2,
        < 2_097_152 => 3,
        _ => 4,
    };

    /// <summary>
    /// Returns the encoded size of a length-prefixed UTF-8 string, throwing if it cannot be encoded
    /// (over 65,535 bytes or containing U+0000).
    /// </summary>
    public static int GetStringSize(string value, string paramName)
    {
        ArgumentNullException.ThrowIfNull(value, paramName);
        if (value.Contains('\0'))
        {
            throw new ArgumentException("MQTT strings may not contain U+0000.", paramName);
        }

        int length;
        try
        {
            length = Utf8Strict.GetByteCount(value);
        }
        catch (EncoderFallbackException ex)
        {
            throw new ArgumentException("MQTT strings must be valid UTF-16 (no unpaired surrogates).", paramName, ex);
        }

        if (length > MaxStringBytes)
        {
            throw new ArgumentException($"MQTT strings are limited to {MaxStringBytes} UTF-8 bytes; got {length}.", paramName);
        }

        return 2 + length;
    }

    private static readonly UTF8Encoding Utf8Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
}
