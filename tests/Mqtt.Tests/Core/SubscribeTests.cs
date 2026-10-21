using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Tests.Core;

public class SubscribeTests : ClientTestBase
{
    [Fact]
    public async Task Subscribe_ReturnsGrantedQos()
    {
        var (client, broker) = await ConnectAsync();

        Task<SubscribeResult> subscribing = client.SubscribeAsync([new("sim/device-001/cmd", MqttQos.AtLeastOnce)]);
        var subscribe = await broker.ReceiveAsync<SubscribePacket>();
        Assert.Equal([new TopicFilter("sim/device-001/cmd", MqttQos.AtLeastOnce)], subscribe.Filters);
        await broker.SendAsync(new SubAckPacket(subscribe.PacketId, [ReasonCode.GrantedQos1]));

        SubscribeResult result = await subscribing.WaitAsync(Timeout);
        Assert.True(result.AllSucceeded);
        Assert.Equal(MqttQos.AtLeastOnce, Assert.Single(result.Items).GrantedQos);
    }

    [Fact]
    public async Task MoreThanEightFilters_AreSentInBatches_AndFailuresReported()
    {
        var (client, broker) = await ConnectAsync();
        TopicFilter[] filters = [.. Enumerable.Range(0, 10).Select(i => new TopicFilter($"f/{i}", MqttQos.AtLeastOnce))];

        Task<SubscribeResult> subscribing = client.SubscribeAsync(filters);
        var first = await broker.ReceiveAsync<SubscribePacket>();
        var second = await broker.ReceiveAsync<SubscribePacket>();

        Assert.Equal(8, first.Filters.Count);
        Assert.Equal(2, second.Filters.Count);
        Assert.NotEqual(first.PacketId, second.PacketId);

        // Answer out of order; refuse the last filter, as AWS does past 50 subscriptions.
        await broker.SendAsync(new SubAckPacket(second.PacketId, [ReasonCode.GrantedQos1, ReasonCode.UnspecifiedError]));
        await broker.SendAsync(new SubAckPacket(first.PacketId, [.. Enumerable.Repeat(ReasonCode.Success, 8)]));

        SubscribeResult result = await subscribing.WaitAsync(Timeout);
        Assert.Equal(filters, result.Items.Select(i => i.Filter));
        Assert.False(result.AllSucceeded);
        Assert.Equal(MqttQos.AtMostOnce, result.Items[0].GrantedQos);
        Assert.False(result.Items[9].Succeeded);
        Assert.Null(result.Items[9].GrantedQos);
    }

    [Fact]
    public async Task SubAckWithWrongCodeCount_ClosesConnection()
    {
        var (client, broker) = await ConnectAsync();

        Task<SubscribeResult> subscribing = client.SubscribeAsync([new("a"), new("b")]);
        var subscribe = await broker.ReceiveAsync<SubscribePacket>();
        await broker.SendAsync(new SubAckPacket(subscribe.PacketId, [ReasonCode.Success]));

        await Assert.ThrowsAsync<InvalidDataException>(() => subscribing.WaitAsync(Timeout));
        await broker.AssertClosedAsync();
    }

    [Fact]
    public async Task ConnectionLost_FailsPendingSubscribe()
    {
        var (client, broker) = await ConnectAsync();

        Task<SubscribeResult> subscribing = client.SubscribeAsync([new("a")]);
        await broker.ReceiveAsync<SubscribePacket>();
        await broker.DisposeAsync();

        await Assert.ThrowsAsync<MqttConnectionLostException>(() => subscribing.WaitAsync(Timeout));
    }

    [Fact]
    public async Task NotConnected_Throws()
    {
        MqttClient client = CreateClient();

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.SubscribeAsync([new("a")]));
    }

    [Theory]
    [InlineData("a/#/b")]
    [InlineData("a+")]
    [InlineData("")]
    [InlineData("1/2/3/4/5/6/7/8/#")]
    public async Task InvalidFilter_Throws(string filter)
    {
        MqttClient client = CreateClient();

        await Assert.ThrowsAnyAsync<ArgumentException>(() => client.SubscribeAsync([new(filter)]));
    }

    [Fact]
    public async Task NoFilters_OrQos2_Throws()
    {
        MqttClient client = CreateClient();

        await Assert.ThrowsAnyAsync<ArgumentException>(() => client.SubscribeAsync([]));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => client.SubscribeAsync([new("a", (MqttQos)2)]));
    }
}
