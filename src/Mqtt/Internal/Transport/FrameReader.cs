using System.Buffers;
using Mqtt.Internal.Codec;

namespace Mqtt.Internal.Transport;

internal enum FrameReadStatus
{
    /// <summary>A complete frame was returned.</summary>
    Frame,

    /// <summary>No complete frame is buffered yet; read more bytes.</summary>
    NeedMoreData,

    /// <summary>The fixed header is invalid; the stream cannot be resynchronised.</summary>
    Malformed,

    /// <summary>The frame is larger than the configured maximum.</summary>
    TooLarge,
}

/// <summary>
/// Splits a byte stream into whole MQTT frames using a pooled buffer. Partial frames stay
/// buffered until the rest arrives; the buffer grows only as far as the maximum frame size
/// and shrinks back once a large frame has been consumed.
/// </summary>
/// <remarks>Not thread-safe: owned by the transport's single read loop.</remarks>
internal sealed class FrameReader : IDisposable
{
    public const int DefaultInitialSize = 4 * 1024;

    private readonly int _initialSize;
    private readonly int _maxFrameSize;
    private byte[] _buffer;
    private int _start;
    private int _end;

    // Total size of the partial frame at _start, once its fixed header has been parsed.
    private int _pendingFrameSize;

    public FrameReader(int maxFrameSize, int initialSize = DefaultInitialSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(initialSize, FixedHeader.MaxSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxFrameSize, FixedHeader.MaxSize);
        _initialSize = Math.Min(initialSize, maxFrameSize);
        _maxFrameSize = maxFrameSize;
        _buffer = ArrayPool<byte>.Shared.Rent(_initialSize);
    }

    /// <summary>Current buffer capacity; exposed for tests.</summary>
    public int Capacity => _buffer.Length;

    /// <summary>
    /// Returns free space to read into. Invalidates frames previously returned by <see cref="TryRead"/>.
    /// </summary>
    public Memory<byte> GetMemory()
    {
        ObjectDisposedException.ThrowIf(_buffer.Length == 0, this);
        int buffered = _end - _start;

        // The buffer may have filled before TryRead saw a complete header; size the frame now.
        if (_pendingFrameSize == 0
            && FixedHeader.TryRead(_buffer.AsSpan(_start, buffered), out _, out int remainingLength, out int headerLength) == FrameStatus.Complete)
        {
            _pendingFrameSize = (int)Math.Min((long)headerLength + remainingLength, _maxFrameSize);
        }

        if (buffered == 0 && _buffer.Length > _initialSize)
        {
            // A large frame has been consumed: go back to the small buffer.
            Replace(_initialSize, buffered);
        }
        else if (_pendingFrameSize > _buffer.Length)
        {
            Replace(_pendingFrameSize, buffered);
        }
        else if (_start > 0)
        {
            // Move the partial frame to the front so it can complete in place.
            _buffer.AsSpan(_start, buffered).CopyTo(_buffer);
            _start = 0;
            _end = buffered;
        }

        if (_end == _buffer.Length)
        {
            // An empty read buffer would look like end of stream to the caller.
            throw new InvalidOperationException("No buffer space left; call TryRead until it returns NeedMoreData first.");
        }

        return _buffer.AsMemory(_end);
    }

    /// <summary>Records that <paramref name="count"/> bytes were read into the memory from <see cref="GetMemory"/>.</summary>
    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, _buffer.Length - _end);
        _end += count;
    }

    /// <summary>
    /// Returns the next complete frame, fixed header included. The memory is valid until the next
    /// <see cref="GetMemory"/> call.
    /// </summary>
    public FrameReadStatus TryRead(out ReadOnlyMemory<byte> frame)
    {
        frame = default;
        ReadOnlySpan<byte> buffered = _buffer.AsSpan(_start, _end - _start);
        switch (FixedHeader.TryRead(buffered, out _, out int remainingLength, out int headerLength))
        {
            case FrameStatus.Incomplete:
                return FrameReadStatus.NeedMoreData;
            case FrameStatus.Malformed:
                return FrameReadStatus.Malformed;
        }

        long frameSize = (long)headerLength + remainingLength;
        if (frameSize > _maxFrameSize)
        {
            return FrameReadStatus.TooLarge;
        }

        if (buffered.Length < frameSize)
        {
            _pendingFrameSize = (int)frameSize;
            return FrameReadStatus.NeedMoreData;
        }

        frame = _buffer.AsMemory(_start, (int)frameSize);
        _start += (int)frameSize;
        _pendingFrameSize = 0;
        return FrameReadStatus.Frame;
    }

    public void Dispose()
    {
        if (_buffer.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = [];
        }
    }

    private void Replace(int size, int buffered)
    {
        byte[] next = ArrayPool<byte>.Shared.Rent(size);
        _buffer.AsSpan(_start, buffered).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
        _start = 0;
        _end = buffered;
    }
}
