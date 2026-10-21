using System.Buffers;

namespace Mqtt.Internal.Transport;

/// <summary>
/// An encoded frame waiting for the write loop. Owns a buffer rented from
/// <see cref="ArrayPool{T}.Shared"/> and returns it exactly once, on success or failure.
/// </summary>
internal sealed class OutboundFrame
{
    private readonly TaskCompletionSource _written = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private byte[]? _buffer;

    public OutboundFrame(byte[] buffer, int length)
    {
        _buffer = buffer;
        Length = length;
    }

    public int Length { get; }

    /// <summary>Completes once the bytes have been written to the TLS stream.</summary>
    public Task Written => _written.Task;

    public ReadOnlyMemory<byte> Memory => (_buffer ?? throw new InvalidOperationException("Frame already completed.")).AsMemory(0, Length);

    public void Complete()
    {
        ReturnBuffer();
        _written.TrySetResult();
    }

    public void Fail(Exception exception)
    {
        ReturnBuffer();
        _written.TrySetException(exception);
    }

    private void ReturnBuffer()
    {
        byte[]? buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
