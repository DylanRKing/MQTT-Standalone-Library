using Mqtt.Internal.Transport;
using static Mqtt.Tests.Codec.CodecHelpers;

namespace Mqtt.Tests.Transport;

public class FrameReaderTests
{
    [Fact]
    public void SingleFrame()
    {
        using var reader = new FrameReader(maxFrameSize: 1024);
        Feed(reader, Hex("40 02 002A"));

        Assert.Equal(FrameReadStatus.Frame, reader.TryRead(out var frame));
        Assert.Equal(Hex("40 02 002A"), frame.ToArray());
        Assert.Equal(FrameReadStatus.NeedMoreData, reader.TryRead(out _));
    }

    [Fact]
    public void ByteAtATime()
    {
        using var reader = new FrameReader(maxFrameSize: 1024);
        byte[] bytes = Hex("30 07 0003 612F62 6869");
        var frames = new List<byte[]>();

        foreach (byte b in bytes)
        {
            Feed(reader, [b]);
            while (reader.TryRead(out var frame) == FrameReadStatus.Frame)
            {
                frames.Add(frame.ToArray());
            }
        }

        Assert.Equal([bytes], frames);
    }

    [Fact]
    public void SeveralFramesInOneRead_WithTrailingPartial()
    {
        using var reader = new FrameReader(maxFrameSize: 1024);
        Feed(reader, Hex("C0 00 D0 00 40 02 00"));

        Assert.Equal(FrameReadStatus.Frame, reader.TryRead(out var first));
        Assert.Equal(Hex("C0 00"), first.ToArray());
        Assert.Equal(FrameReadStatus.Frame, reader.TryRead(out var second));
        Assert.Equal(Hex("D0 00"), second.ToArray());
        Assert.Equal(FrameReadStatus.NeedMoreData, reader.TryRead(out _));

        Feed(reader, Hex("2A"));
        Assert.Equal(FrameReadStatus.Frame, reader.TryRead(out var third));
        Assert.Equal(Hex("40 02 002A"), third.ToArray());
    }

    [Fact]
    public void LargeFrame_GrowsBuffer_ThenShrinksBack()
    {
        using var reader = new FrameReader(maxFrameSize: 200_000, initialSize: 64);
        int initialCapacity = reader.Capacity;
        byte[] frame = Encode(new Internal.Codec.Packets.PublishPacket("t", new byte[100_000]));

        Feed(reader, frame);
        Assert.Equal(FrameReadStatus.Frame, reader.TryRead(out var read));
        Assert.Equal(frame, read.ToArray());
        Assert.True(reader.Capacity >= frame.Length);

        reader.GetMemory();
        Assert.Equal(initialCapacity, reader.Capacity);
    }

    [Fact]
    public void FrameOverMaximum_IsTooLarge()
    {
        using var reader = new FrameReader(maxFrameSize: 100, initialSize: 64);
        Feed(reader, Hex("30 80 01"));

        Assert.Equal(FrameReadStatus.TooLarge, reader.TryRead(out _));
    }

    [Fact]
    public void FiveByteRemainingLength_IsMalformed()
    {
        using var reader = new FrameReader(maxFrameSize: 1024);
        Feed(reader, Hex("30 FF FF FF FF 01"));

        Assert.Equal(FrameReadStatus.Malformed, reader.TryRead(out _));
    }

    /// <summary>Copies bytes in through GetMemory/Advance, as the read loop does.</summary>
    private static void Feed(FrameReader reader, ReadOnlySpan<byte> bytes)
    {
        while (!bytes.IsEmpty)
        {
            Memory<byte> memory = reader.GetMemory();
            int count = Math.Min(memory.Length, bytes.Length);
            Assert.True(count > 0, "GetMemory returned no space.");
            bytes[..count].CopyTo(memory.Span);
            reader.Advance(count);
            bytes = bytes[count..];
        }
    }
}
