using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Core;

/// <summary>
/// Best-effort filter for inbound QoS 1 redeliveries. Remembers the last 1,000 delivered QoS 1
/// messages for up to an hour, keyed by packet ID plus a hash of topic and payload: brokers reuse
/// packet IDs after PUBACK, so an ID match alone would drop new messages. Only PUBLISHes with
/// DUP = 1 are checked. Payloads that must be counted exactly should carry their own message ID.
/// Thread-safe.
/// </summary>
internal sealed class DuplicateCache(TimeProvider time, int capacity = DuplicateCache.DefaultCapacity)
{
    public const int DefaultCapacity = 1_000;

    public static readonly TimeSpan Lifetime = TimeSpan.FromHours(1);

    // Oldest first; the dictionary points into the list so a repeat moves to the back.
    private readonly LinkedList<(Key Key, long Timestamp)> _order = new();
    private readonly Dictionary<Key, LinkedListNode<(Key Key, long Timestamp)>> _entries = [];
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

    /// <summary>True when <paramref name="packet"/> is a DUP redelivery of a message already delivered.</summary>
    public bool IsDuplicate(PublishPacket packet)
    {
        if (!packet.Dup || packet.Qos != MqttQos.AtLeastOnce)
        {
            return false;
        }

        lock (_lock)
        {
            return _entries.TryGetValue(Key.For(packet), out var node) && time.GetElapsedTime(node.Value.Timestamp) < Lifetime;
        }
    }

    public void Record(PublishPacket packet)
    {
        var key = Key.For(packet);
        lock (_lock)
        {
            if (_entries.Remove(key, out var existing))
            {
                _order.Remove(existing);
            }

            _entries[key] = _order.AddLast((key, time.GetTimestamp()));
            while (_order.First is { } oldest
                && (_entries.Count > capacity || time.GetElapsedTime(oldest.Value.Timestamp) >= Lifetime))
            {
                _order.RemoveFirst();
                _entries.Remove(oldest.Value.Key);
            }
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            _entries.Clear();
            _order.Clear();
        }
    }

    private readonly record struct Key(ushort PacketId, string Topic, int PayloadLength, int PayloadHash)
    {
        public static Key For(PublishPacket packet)
        {
            var hash = new HashCode();
            hash.AddBytes(packet.Payload.Span);
            return new Key(packet.PacketId, packet.Topic, packet.Payload.Length, hash.ToHashCode());
        }
    }
}
