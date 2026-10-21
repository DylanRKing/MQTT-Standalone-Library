using Microsoft.Extensions.Time.Testing;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Internal.Core;

namespace Mqtt.Tests.Core;

public class PublishRateLimiterTests
{
    private readonly FakeTimeProvider _time = new();

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task AllowsOneSecondBurst_ThenPacesAtTheRate()
    {
        var limiter = new PublishRateLimiter(10, _time);
        for (int i = 0; i < 10; i++)
        {
            Assert.True(limiter.WaitAsync(default).IsCompletedSuccessfully);
        }

        Task eleventh = limiter.WaitAsync(default).AsTask();
        Task twelfth = limiter.WaitAsync(default).AsTask();
        Assert.False(eleventh.IsCompleted);

        // Fake-clock delays complete asynchronously, so await rather than check at once.
        _time.Advance(TimeSpan.FromMilliseconds(100));
        await eleventh.WaitAsync(Timeout);
        Assert.False(twelfth.IsCompleted);

        _time.Advance(TimeSpan.FromMilliseconds(100));
        await twelfth.WaitAsync(Timeout);
    }

    [Fact]
    public void Refills_UpToOneSecondOnly()
    {
        var limiter = new PublishRateLimiter(5, _time);
        _time.Advance(TimeSpan.FromMinutes(1));

        for (int i = 0; i < 5; i++)
        {
            Assert.True(limiter.WaitAsync(default).IsCompletedSuccessfully);
        }

        Assert.False(limiter.WaitAsync(default).IsCompleted);
    }

    [Fact]
    public async Task CancelledWait_ReturnsItsToken()
    {
        var limiter = new PublishRateLimiter(1, _time);
        await limiter.WaitAsync(default);
        using var cts = new CancellationTokenSource();

        ValueTask waiting = limiter.WaitAsync(cts.Token);
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await waiting);

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(limiter.WaitAsync(default).IsCompletedSuccessfully);
    }
}

public class PublishRateLimitTests : ClientTestBase
{
    [Fact]
    public async Task PublishesOverTheRate_WaitInsteadOfBeingSent()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var (client, broker) = await ConnectAsync(Broker.Options() with { MaxPublishRate = 2, TimeProvider = time });

        Assert.Equal(PublishResult.Sent, await client.PublishAsync("t/1", default));
        Assert.Equal(PublishResult.Sent, await client.PublishAsync("t/2", default));
        Task<PublishResult> third = client.PublishAsync("t/3", default);
        await Task.Delay(200);
        Assert.False(third.IsCompleted);

        time.Advance(TimeSpan.FromMilliseconds(500));
        Assert.Equal(PublishResult.Sent, await third.WaitAsync(Timeout));
        for (int i = 1; i <= 3; i++)
        {
            Assert.Equal($"t/{i}", (await broker.ReceiveAsync<PublishPacket>()).Topic);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void InvalidRate_Throws(int rate)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MqttClient(Broker.Options() with { MaxPublishRate = rate }));
    }

    [Fact]
    public void InvalidThrottle_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ConnectionThrottle(0));
    }
}
