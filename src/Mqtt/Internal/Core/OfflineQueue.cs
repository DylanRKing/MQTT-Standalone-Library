namespace Mqtt.Internal.Core;

/// <summary>A QoS 1 publish accepted from the caller but not yet given a packet ID.</summary>
internal sealed class QueuedPublish(string topic, ReadOnlyMemory<byte> payload, bool retain)
{
    public string Topic { get; } = topic;

    public ReadOnlyMemory<byte> Payload { get; } = payload;

    public bool Retain { get; } = retain;

    /// <summary>Completes on PUBACK; carried over to the in-flight entry when the message is sent.</summary>
    public TaskCompletionSource<PublishResult> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

internal enum EnqueueOutcome
{
    /// <summary>The queue is open: connected with nothing waiting, so the caller sends directly.</summary>
    Bypass,

    /// <summary>Queued; it will be sent when the queue is flushed after (re)connecting.</summary>
    Queued,

    /// <summary>Full in <see cref="OfflineQueueFullMode.RejectNew"/> mode; not queued.</summary>
    Full,
}

/// <summary>
/// Bounded in-memory queue for QoS 1 publishes made while connecting or reconnecting. It stays
/// closed until the post-connect flush has emptied it, so publishes made during the flush queue
/// behind older ones and order is kept. In memory only: a process restart loses it. Thread-safe.
/// </summary>
internal sealed class OfflineQueue(int capacity, OfflineQueueFullMode fullMode)
{
    private readonly LinkedList<QueuedPublish> _items = new();
    private readonly Lock _lock = new();
    private bool _open;

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _items.Count;
            }
        }
    }

    public EnqueueOutcome Enqueue(QueuedPublish item, out QueuedPublish? dropped)
    {
        dropped = null;
        lock (_lock)
        {
            if (_open)
            {
                return EnqueueOutcome.Bypass;
            }

            if (_items.Count >= capacity)
            {
                if (fullMode == OfflineQueueFullMode.RejectNew || _items.Count == 0)
                {
                    return EnqueueOutcome.Full;
                }

                dropped = _items.First!.Value;
                _items.RemoveFirst();
            }

            _items.AddLast(item);
            return EnqueueOutcome.Queued;
        }
    }

    /// <summary>
    /// Returns the oldest queued publish without removing it. When the queue is empty, opens it
    /// (direct sending resumes) if <paramref name="canOpen"/> still holds, and returns null.
    /// </summary>
    public QueuedPublish? PeekOrOpen(Func<bool> canOpen)
    {
        lock (_lock)
        {
            if (_items.First is { } first)
            {
                return first.Value;
            }

            if (canOpen())
            {
                _open = true;
            }

            return null;
        }
    }

    public bool TryRemove(QueuedPublish item)
    {
        lock (_lock)
        {
            return _items.Remove(item);
        }
    }

    /// <summary>Closes the queue so publishes are queued again; called when the connection is lost.</summary>
    public void Close()
    {
        lock (_lock)
        {
            _open = false;
        }
    }

    public List<QueuedPublish> RemoveAll()
    {
        lock (_lock)
        {
            List<QueuedPublish> all = [.. _items];
            _items.Clear();
            return all;
        }
    }
}
