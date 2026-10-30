using Microsoft.Extensions.Time.Testing;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Core;

/// <summary>Keep-alive and ack deadlines, driven by a fake clock.</summary>
public class KeepAliveTests : ClientTestBase
{
    private readonly FakeTimeProvider _time = new(DateTimeOffset.UtcNow);

    [Fact]
    public async Task Idle_SendsPingReq_AndStaysConnectedWhenAnswered()
    {
        var (client, broker) = await ConnectAsync(Broker.Options() with { TimeProvider = _time });

        _time.Advance(TimeSpan.FromSeconds(59));
        Task<IMqttPacket?> next = broker.ReceiveAnyAsync();
        await Task.Delay(200);
        Assert.False(next.IsCompleted);

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.IsType<PingReqPacket>(await next);
        await broker.SendAsync(new PingRespPacket());

        // Frames are handled in order, so once this PUBACK arrives the PINGRESP has been processed.
        await broker.SendAsync(new PublishPacket("sync", default, MqttQos.AtLeastOnce, PacketId: 1));
        await broker.ReceiveAsync<PubAckPacket>();

        _time.Advance(TimeSpan.FromSeconds(31));
        await Task.Delay(200);
        Assert.Equal(MqttConnectionState.Connected, client.State);
    }

    [Fact]
    public async Task PingReqUnanswered_DropsConnectionAfterHalfKeepAlive()
    {
        var (client, broker) = await ConnectAsync(Broker.Options() with { TimeProvider = _time });
        var states = new StateRecorder(client);

        _time.Advance(TimeSpan.FromSeconds(60));
        await broker.ReceiveAsync<PingReqPacket>();
        _time.Advance(TimeSpan.FromSeconds(30));

        ConnectionStateChange change = await states.WaitForAsync(MqttConnectionState.Reconnecting);
        Assert.Contains("PINGREQ", change.Reason?.Message);
        await broker.AssertClosedAsync();
    }

    [Fact]
    public async Task RecentSend_PostponesPingReq()
    {
        var (client, broker) = await ConnectAsync(Broker.Options() with { TimeProvider = _time });

        _time.Advance(TimeSpan.FromSeconds(40));
        await client.PublishAsync("t", default);
        await broker.ReceiveAsync<PublishPacket>();

        _time.Advance(TimeSpan.FromSeconds(40));
        Task<IMqttPacket?> next = broker.ReceiveAnyAsync();
        await Task.Delay(200);
        Assert.False(next.IsCompleted);

        _time.Advance(TimeSpan.FromSeconds(20));
        Assert.IsType<PingReqPacket>(await next);
    }

    [Fact]
    public async Task PubAckOverdue_WithoutReconnect_ClosesConnection_AndFailsPublish()
    {
        var (client, broker) = await ConnectAsync(Broker.Options() with
        {
            TimeProvider = _time,
            AckTimeout = TimeSpan.FromSeconds(30),
            Reconnect = new ReconnectOptions { Enabled = false },
        });

        Task<PublishResult> publishing = client.PublishAsync("t", default, MqttQos.AtLeastOnce);
        await broker.ReceiveAsync<PublishPacket>();
        _time.Advance(TimeSpan.FromSeconds(30));

        var ex = await Assert.ThrowsAsync<MqttConnectionLostException>(() => publishing.WaitAsync(Timeout));
        Assert.Contains("PUBACK", ex.InnerException?.Message);
        await broker.AssertClosedAsync();
    }

    [Fact]
    public async Task SubAckOverdue_ClosesConnection()
    {
        var (client, broker) = await ConnectAsync(Broker.Options() with { TimeProvider = _time });

        Task<SubscribeResult> subscribing = client.SubscribeAsync([new("a")]);
        await broker.ReceiveAsync<SubscribePacket>();
        _time.Advance(TimeSpan.FromSeconds(30));

        await Assert.ThrowsAsync<MqttConnectionLostException>(() => subscribing.WaitAsync(Timeout));
    }
}
