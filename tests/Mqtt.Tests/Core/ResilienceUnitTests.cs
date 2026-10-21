using Microsoft.Extensions.Time.Testing;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Internal.Core;

namespace Mqtt.Tests.Core;

public class OfflineQueueTests
{
    [Fact]
    public void ClosedQueue_Queues_UntilFull()
    {
        var queue = new OfflineQueue(2, OfflineQueueFullMode.RejectNew);

        Assert.Equal(EnqueueOutcome.Queued, queue.Enqueue(Item("a"), out _));
        Assert.Equal(EnqueueOutcome.Queued, queue.Enqueue(Item("b"), out _));
        Assert.Equal(EnqueueOutcome.Full, queue.Enqueue(Item("c"), out var dropped));
        Assert.Null(dropped);
        Assert.Equal(2, queue.Count);
    }

    [Fact]
    public void DropOldest_ReturnsTheDroppedItem()
    {
        var queue = new OfflineQueue(2, OfflineQueueFullMode.DropOldest);
        QueuedPublish a = Item("a");
        queue.Enqueue(a, out _);
        queue.Enqueue(Item("b"), out _);

        Assert.Equal(EnqueueOutcome.Queued, queue.Enqueue(Item("c"), out var dropped));
        Assert.Same(a, dropped);
        Assert.Equal(["b", "c"], queue.RemoveAll().Select(i => i.Topic));
    }

    [Fact]
    public void ZeroCapacity_RejectsEvenInDropOldestMode()
    {
        var queue = new OfflineQueue(0, OfflineQueueFullMode.DropOldest);

        Assert.Equal(EnqueueOutcome.Full, queue.Enqueue(Item("a"), out _));
    }

    [Fact]
    public void PeekOrOpen_OpensOnlyWhenEmptyAndAllowed()
    {
        var queue = new OfflineQueue(10, OfflineQueueFullMode.RejectNew);
        QueuedPublish a = Item("a");
        queue.Enqueue(a, out _);

        Assert.Same(a, queue.PeekOrOpen(() => true));
        Assert.True(queue.TryRemove(a));
        Assert.Null(queue.PeekOrOpen(() => false));
        Assert.Equal(EnqueueOutcome.Queued, queue.Enqueue(Item("b"), out _));

        queue.RemoveAll();
        Assert.Null(queue.PeekOrOpen(() => true));
        Assert.Equal(EnqueueOutcome.Bypass, queue.Enqueue(Item("c"), out _));

        queue.Close();
        Assert.Equal(EnqueueOutcome.Queued, queue.Enqueue(Item("d"), out _));
    }

    private static QueuedPublish Item(string topic) => new(topic, default, retain: false);
}

public class DuplicateCacheTests
{
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void OnlyDupFlaggedQos1_MatchingIdTopicAndPayload_IsDuplicate()
    {
        var cache = new DuplicateCache(_time);
        cache.Record(Publish(7, "t", "x"));

        Assert.True(cache.IsDuplicate(Publish(7, "t", "x", dup: true)));
        Assert.False(cache.IsDuplicate(Publish(7, "t", "x", dup: false)));
        Assert.False(cache.IsDuplicate(Publish(7, "t", "y", dup: true)));
        Assert.False(cache.IsDuplicate(Publish(7, "u", "x", dup: true)));
        Assert.False(cache.IsDuplicate(Publish(8, "t", "x", dup: true)));
    }

    [Fact]
    public void EntriesExpireAfterAnHour()
    {
        var cache = new DuplicateCache(_time);
        cache.Record(Publish(1, "t", "x"));

        _time.Advance(TimeSpan.FromMinutes(59));
        Assert.True(cache.IsDuplicate(Publish(1, "t", "x", dup: true)));

        _time.Advance(TimeSpan.FromMinutes(1));
        Assert.False(cache.IsDuplicate(Publish(1, "t", "x", dup: true)));
    }

    [Fact]
    public void OldestEntriesEvictedPastCapacity_AndRepeatsMoveToBack()
    {
        var cache = new DuplicateCache(_time, capacity: 3);
        cache.Record(Publish(1, "t", "a"));
        cache.Record(Publish(2, "t", "b"));
        cache.Record(Publish(3, "t", "c"));
        cache.Record(Publish(1, "t", "a"));
        cache.Record(Publish(4, "t", "d"));

        Assert.Equal(3, cache.Count);
        Assert.True(cache.IsDuplicate(Publish(1, "t", "a", dup: true)));
        Assert.False(cache.IsDuplicate(Publish(2, "t", "b", dup: true)));

        cache.Clear();
        Assert.Equal(0, cache.Count);
    }

    private static PublishPacket Publish(ushort id, string topic, string payload, bool dup = false) =>
        new(topic, System.Text.Encoding.UTF8.GetBytes(payload), MqttQos.AtLeastOnce, Dup: dup, PacketId: id);
}

public class ReconnectBackoffTests
{
    private static readonly ReconnectOptions Defaults = new();

    [Fact]
    public void Delays_StayBetweenFloorAndDoublingCap()
    {
        var backoff = new ReconnectBackoff(Defaults, TimeSpan.FromSeconds(1), new Random(42));

        for (int attempt = 0; attempt < 12; attempt++)
        {
            TimeSpan delay = backoff.NextDelay();
            double cap = Math.Min(60, Math.Pow(2, attempt));
            Assert.InRange(delay.TotalSeconds, 1, cap);
        }
    }

    [Fact]
    public void FirstDelay_IsTheFloor_AndLaterOnesReachMaxDelay()
    {
        var backoff = new ReconnectBackoff(Defaults, TimeSpan.FromSeconds(1), new FixedRandom(1.0));

        Assert.Equal(TimeSpan.FromSeconds(1), backoff.NextDelay());
        Assert.Equal(TimeSpan.FromSeconds(2), backoff.NextDelay());
        Assert.Equal(TimeSpan.FromSeconds(4), backoff.NextDelay());
        for (int i = 0; i < 50; i++)
        {
            backoff.NextDelay();
        }

        Assert.Equal(TimeSpan.FromSeconds(60), backoff.NextDelay());
    }

    [Fact]
    public void AttemptCounter_ResetsOnlyAfterStableConnection()
    {
        var backoff = new ReconnectBackoff(Defaults, TimeSpan.FromSeconds(1), new FixedRandom(1.0));
        backoff.NextDelay();
        backoff.NextDelay();

        backoff.OnConnectionLost(TimeSpan.FromSeconds(29));
        Assert.Equal(TimeSpan.FromSeconds(4), backoff.NextDelay());

        backoff.OnConnectionLost(TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.FromSeconds(1), backoff.NextDelay());
    }

    [Fact]
    public void InitialDelayBelowFloor_IsRaisedToFloor()
    {
        var options = new ReconnectOptions { InitialDelay = TimeSpan.FromMilliseconds(100) };
        var backoff = new ReconnectBackoff(options, TimeSpan.FromSeconds(1), new FixedRandom(0.0));

        Assert.Equal(TimeSpan.FromSeconds(1), backoff.NextDelay());
    }

    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }
}
