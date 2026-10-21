using Mqtt.Internal.Codec.Packets;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Core;

public class PublishTests : ClientTestBase
{
    [Fact]
    public async Task Qos0_IsSent()
    {
        var (client, broker) = await ConnectAsync();

        PublishResult result = await client.PublishAsync("sim/device-001/telemetry", "{}"u8.ToArray(), retain: true);

        Assert.Equal(PublishResult.Sent, result);
        var publish = await broker.ReceiveAsync<PublishPacket>();
        Assert.Equal("sim/device-001/telemetry", publish.Topic);
        Assert.Equal(MqttQos.AtMostOnce, publish.Qos);
        Assert.True(publish.Retain);
        Assert.Equal("{}"u8.ToArray(), publish.Payload.ToArray());
    }

    [Fact]
    public async Task Qos1_CompletesOnPubAck()
    {
        var (client, broker) = await ConnectAsync();

        Task<PublishResult> publishing = client.PublishAsync("t", "x"u8.ToArray(), MqttQos.AtLeastOnce);
        var publish = await broker.ReceiveAsync<PublishPacket>();

        Assert.Equal(MqttQos.AtLeastOnce, publish.Qos);
        Assert.NotEqual(0, publish.PacketId);
        Assert.False(publish.Dup);
        Assert.False(publishing.IsCompleted);

        await broker.SendAsync(new PubAckPacket(publish.PacketId));
        Assert.Equal(PublishResult.Acknowledged, await publishing.WaitAsync(Timeout));
    }

    [Fact]
    public async Task Qos1_ConcurrentPublishes_GetDistinctIds_AndAcksInAnyOrder()
    {
        var (client, broker) = await ConnectAsync();

        Task<PublishResult>[] publishing = Enumerable.Range(0, 20)
            .Select(i => client.PublishAsync($"t/{i}", default, MqttQos.AtLeastOnce))
            .ToArray();
        var ids = new List<ushort>();
        for (int i = 0; i < 20; i++)
        {
            ids.Add((await broker.ReceiveAsync<PublishPacket>()).PacketId);
        }

        Assert.Equal(20, ids.Distinct().Count());
        foreach (ushort id in Enumerable.Reverse(ids))
        {
            await broker.SendAsync(new PubAckPacket(id));
        }

        Assert.All(await Task.WhenAll(publishing).WaitAsync(Timeout), r => Assert.Equal(PublishResult.Acknowledged, r));
    }

    [Fact]
    public async Task Qos1_PastMaxInFlight_CallerWaitsForAck()
    {
        var (client, broker) = await ConnectAsync(Broker.Options() with { MaxInFlight = 2 });

        Task<PublishResult>[] publishing = [.. Enumerable.Range(0, 3).Select(i => client.PublishAsync($"t/{i}", default, MqttQos.AtLeastOnce))];
        var first = await broker.ReceiveAsync<PublishPacket>();
        await broker.ReceiveAsync<PublishPacket>();

        Task<PublishPacket> third = broker.ReceiveAsync<PublishPacket>();
        Assert.NotSame(third, await Task.WhenAny(third, Task.Delay(300)));

        await broker.SendAsync(new PubAckPacket(first.PacketId));
        Assert.Equal(PublishResult.Acknowledged, await publishing[0].WaitAsync(Timeout));
        await third.WaitAsync(Timeout);
    }

    [Fact]
    public async Task Qos1_ConnectionLost_FailsPendingPublish()
    {
        var (client, broker) = await ConnectAsync();

        Task<PublishResult> publishing = client.PublishAsync("t", default, MqttQos.AtLeastOnce);
        await broker.ReceiveAsync<PublishPacket>();
        await broker.DisposeAsync();

        await Assert.ThrowsAsync<MqttConnectionLostException>(() => publishing.WaitAsync(Timeout));
    }

    [Fact]
    public async Task NotConnected_Qos0IsDropped_Qos1Throws()
    {
        MqttClient client = CreateClient();

        Assert.Equal(PublishResult.DroppedOffline, await client.PublishAsync("t", default));
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.PublishAsync("t", default, MqttQos.AtLeastOnce));
    }

    [Fact]
    public async Task Cancelling_StopsTheWait_NotThePublish()
    {
        var (client, broker) = await ConnectAsync();
        using var cts = new CancellationTokenSource();

        Task<PublishResult> publishing = client.PublishAsync("t", default, MqttQos.AtLeastOnce, ct: cts.Token);
        var publish = await broker.ReceiveAsync<PublishPacket>();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publishing);
        await broker.SendAsync(new PubAckPacket(publish.PacketId));
        Assert.Equal(PublishResult.Sent, await client.PublishAsync("after", default));
        await broker.ReceiveAsync<PublishPacket>();
    }

    public static TheoryData<string, int, MqttQos> InvalidArguments() => new()
    {
        { "a/+", 0, MqttQos.AtMostOnce },
        { "a/#", 0, MqttQos.AtMostOnce },
        { "", 0, MqttQos.AtMostOnce },
        { new string('a', 257), 0, MqttQos.AtMostOnce },
        { "1/2/3/4/5/6/7/8/9", 0, MqttQos.AtMostOnce },
        { "t", (128 * 1024) + 1, MqttQos.AtMostOnce },
        { "t", 0, (MqttQos)2 },
    };

    [Theory]
    [MemberData(nameof(InvalidArguments))]
    public async Task InvalidArguments_Throw(string topic, int payloadSize, MqttQos qos)
    {
        MqttClient client = CreateClient();

        await Assert.ThrowsAnyAsync<ArgumentException>(() => client.PublishAsync(topic, new byte[payloadSize], qos));
    }

    [Fact]
    public async Task AwsLimits_ExactlyAtLimit_AreAllowed()
    {
        var (client, broker) = await ConnectAsync();

        await client.PublishAsync("1/2/3/4/5/6/7/" + new string('a', 242), new byte[128 * 1024]);

        Assert.Equal(128 * 1024, (await broker.ReceiveAsync<PublishPacket>()).Payload.Length);
    }
}
