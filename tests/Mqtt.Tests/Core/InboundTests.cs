using System.Collections.Concurrent;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Tests.Core;

public class InboundTests : ClientTestBase
{
    [Fact]
    public async Task Messages_AreDeliveredInOrder_WithRetainFlag()
    {
        var (client, broker) = await ConnectAsync();
        var received = new ConcurrentQueue<MqttMessage>();
        var allReceived = new TaskCompletionSource();
        client.MessageReceived += async message =>
        {
            // Yield so out-of-order dispatch would show up.
            await Task.Delay(Random.Shared.Next(5));
            received.Enqueue(message);
            if (received.Count == 5)
            {
                allReceived.SetResult();
            }
        };

        await broker.SendAsync(new PublishPacket("state", "old"u8.ToArray(), Retain: true));
        for (int i = 1; i < 5; i++)
        {
            await broker.SendAsync(new PublishPacket($"live/{i}", new[] { (byte)i }));
        }

        await allReceived.Task.WaitAsync(Timeout);
        Assert.Equal(["state", "live/1", "live/2", "live/3", "live/4"], received.Select(m => m.Topic));
        Assert.True(received.First().Retain);
        Assert.All(received.Skip(1), m => Assert.False(m.Retain));
        Assert.Equal("old"u8.ToArray(), received.First().Payload.ToArray());
    }

    [Fact]
    public async Task Qos1_PubAckIsSentAfterHandlerReturns()
    {
        var (client, broker) = await ConnectAsync();
        var handlerEntered = new TaskCompletionSource();
        var releaseHandler = new TaskCompletionSource();
        client.MessageReceived += async message =>
        {
            handlerEntered.SetResult();
            await releaseHandler.Task;
        };

        await broker.SendAsync(new PublishPacket("cmd", "go"u8.ToArray(), MqttQos.AtLeastOnce, PacketId: 5));
        await handlerEntered.Task.WaitAsync(Timeout);
        Task<PubAckPacket> pubAck = broker.ReceiveAsync<PubAckPacket>();
        Assert.NotSame(pubAck, await Task.WhenAny(pubAck, Task.Delay(300)));

        releaseHandler.SetResult();
        Assert.Equal(5, (await pubAck.WaitAsync(Timeout)).PacketId);
    }

    [Fact]
    public async Task HandlerThrows_PubAckStillSent_AndLaterMessagesDelivered()
    {
        var (client, broker) = await ConnectAsync();
        var second = new TaskCompletionSource<MqttMessage>();
        client.MessageReceived += message => message.Topic == "bad"
            ? throw new InvalidOperationException("handler bug")
            : Task.Run(() => second.SetResult(message));

        await broker.SendAsync(new PublishPacket("bad", default, MqttQos.AtLeastOnce, PacketId: 1));
        await broker.SendAsync(new PublishPacket("good", default, MqttQos.AtLeastOnce, PacketId: 2));

        Assert.Equal(1, (await broker.ReceiveAsync<PubAckPacket>()).PacketId);
        Assert.Equal(2, (await broker.ReceiveAsync<PubAckPacket>()).PacketId);
        Assert.Equal("good", (await second.Task.WaitAsync(Timeout)).Topic);
    }

    [Fact]
    public async Task NoHandler_Qos1IsStillAcknowledged()
    {
        var (_, broker) = await ConnectAsync();

        await broker.SendAsync(new PublishPacket("cmd", default, MqttQos.AtLeastOnce, PacketId: 9));

        Assert.Equal(9, (await broker.ReceiveAsync<PubAckPacket>()).PacketId);
    }

    [Fact]
    public async Task UnexpectedPacketFromBroker_ClosesConnection()
    {
        var (client, broker) = await ConnectAsync();

        await broker.SendAsync(new ConnAckPacket(false, ReasonCode.Success));

        await broker.AssertClosedAsync();
    }

    [Fact]
    public async Task ClientsAreIsolated_SlowHandlerDoesNotBlockAnotherClient()
    {
        var (slow, slowBroker) = await ConnectAsync(Broker.Options("slow"));
        var (fast, fastBroker) = await ConnectAsync(Broker.Options("fast"));
        var neverRelease = new TaskCompletionSource();
        var fastReceived = new TaskCompletionSource();
        slow.MessageReceived += _ => neverRelease.Task;
        fast.MessageReceived += _ => Task.Run(fastReceived.SetResult);

        await slowBroker.SendAsync(new PublishPacket("t", default));
        await fastBroker.SendAsync(new PublishPacket("t", default));

        await fastReceived.Task.WaitAsync(Timeout);
        neverRelease.SetResult();
    }
}
