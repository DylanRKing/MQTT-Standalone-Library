namespace Mqtt.Internal.Codec;

internal enum FrameStatus
{
    /// <summary>A complete fixed header was parsed; the frame may still need more bytes.</summary>
    Complete,

    /// <summary>More bytes are needed to finish the fixed header.</summary>
    Incomplete,

    /// <summary>The remaining-length field is longer than four bytes.</summary>
    Malformed,
}

/// <summary>
/// Parses the fixed header (type/flags byte plus variable-length remaining length).
/// Used by the decoder and by the transport to find frame boundaries in a byte stream.
/// </summary>
internal static class FixedHeader
{
    /// <summary>Largest fixed header: one type byte plus four remaining-length bytes.</summary>
    public const int MaxSize = 5;

    /// <summary>
    /// Tries to parse a fixed header at the start of <paramref name="buffer"/>.
    /// On <see cref="FrameStatus.Complete"/> the whole frame is
    /// <paramref name="headerLength"/> + <paramref name="remainingLength"/> bytes.
    /// </summary>
    public static FrameStatus TryRead(ReadOnlySpan<byte> buffer, out byte typeAndFlags, out int remainingLength, out int headerLength)
    {
        typeAndFlags = 0;
        remainingLength = 0;
        headerLength = 0;
        if (buffer.IsEmpty)
        {
            return FrameStatus.Incomplete;
        }

        int value = 0;
        int multiplier = 1;
        for (int i = 1; i < MaxSize; i++)
        {
            if (i >= buffer.Length)
            {
                return FrameStatus.Incomplete;
            }

            byte encoded = buffer[i];
            value += (encoded & 0x7F) * multiplier;
            if ((encoded & 0x80) == 0)
            {
                typeAndFlags = buffer[0];
                remainingLength = value;
                headerLength = i + 1;
                return FrameStatus.Complete;
            }

            multiplier <<= 7;
        }

        return FrameStatus.Malformed;
    }
}
