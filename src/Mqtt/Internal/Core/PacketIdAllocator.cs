namespace Mqtt.Internal.Core;

/// <summary>
/// Hands out packet IDs 1–65,535 in rotation, skipping any still held by an unacknowledged
/// PUBLISH or a pending SUBSCRIBE. Thread-safe.
/// </summary>
internal sealed class PacketIdAllocator
{
    private readonly HashSet<ushort> _inUse = [];
    private readonly Lock _lock = new();
    private ushort _last;

    public int InUseCount
    {
        get
        {
            lock (_lock)
            {
                return _inUse.Count;
            }
        }
    }

    public ushort Allocate()
    {
        lock (_lock)
        {
            if (_inUse.Count == ushort.MaxValue)
            {
                throw new InvalidOperationException("All 65,535 packet IDs are in use.");
            }

            do
            {
                _last = _last == ushort.MaxValue ? (ushort)1 : (ushort)(_last + 1);
            }
            while (!_inUse.Add(_last));

            return _last;
        }
    }

    public void Release(ushort packetId)
    {
        lock (_lock)
        {
            _inUse.Remove(packetId);
        }
    }

    /// <summary>Releases every ID, for when a clean session discards the old one.</summary>
    public void Clear()
    {
        lock (_lock)
        {
            _inUse.Clear();
        }
    }
}
