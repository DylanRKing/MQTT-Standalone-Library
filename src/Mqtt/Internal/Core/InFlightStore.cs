using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Core;

/// <summary>An outbound QoS 1 PUBLISH waiting for its PUBACK.</summary>
internal sealed class InFlightPublish(PublishPacket packet, long sentTimestamp)
{
    public PublishPacket Packet { get; } = packet;

    public long SentTimestamp { get; } = sentTimestamp;

    public TaskCompletionSource<PublishResult> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>
/// Insertion-ordered map of packet ID to unacknowledged QoS 1 publish, so the oldest entry
/// (the first to hit the ack timeout, and the first to resend) is always at the front. Thread-safe.
/// </summary>
internal sealed class InFlightStore
{
    private readonly OrderedDictionary<ushort, InFlightPublish> _entries = [];
    private readonly Lock _lock = new();

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count;
            }
        }
    }

    public void Add(InFlightPublish entry)
    {
        lock (_lock)
        {
            _entries.Add(entry.Packet.PacketId, entry);
        }
    }

    public bool TryRemove(ushort packetId, out InFlightPublish? entry)
    {
        lock (_lock)
        {
            return _entries.Remove(packetId, out entry);
        }
    }

    /// <summary>Send time of the oldest unacknowledged publish, or null if none.</summary>
    public long? OldestSentTimestamp
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count == 0 ? null : _entries.GetAt(0).Value.SentTimestamp;
            }
        }
    }

    /// <summary>Removes and returns every entry, oldest first.</summary>
    public List<InFlightPublish> RemoveAll()
    {
        lock (_lock)
        {
            List<InFlightPublish> all = [.. _entries.Values];
            _entries.Clear();
            return all;
        }
    }
}
