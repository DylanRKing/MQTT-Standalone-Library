using System.Collections.Concurrent;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Core;

/// <summary>Reconnect, session modes, offline queue and duplicate filtering, end to end.</summary>
public class ResilienceTests : ClientTestBase
{
    [Fact]
    public async Task CleanSession_Reconnect_RestoresSubscriptions_ResendsWithNewIds_ThenFlushesQueue()
    {
        var (client, first) = await ConnectAsync();
        var states = new StateRecorder(client);
        await SubscribeAsync(client, first, [new("cmd/a", MqttQos.AtLeastOnce), new("cmd/b")]);
        Task<PublishResult> m1 = client.PublishAsync("m/1", "1"u8.ToArray(), MqttQos.AtLeastOnce);
        var sent1 = await first.ReceiveAsync<PublishPacket>();

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);
        Task<PublishResult> m2 = client.PublishAsync("m/2", "2"u8.ToArray(), MqttQos.AtLeastOnce);
        Task<PublishResult> m3 = client.PublishAsync("m/3", "3"u8.ToArray(), MqttQos.AtLeastOnce);
        Assert.False(m2.IsCompleted);

        BrokerSession second = await AcceptAsync();
        Assert.True((await second.AcceptConnectAsync()).CleanSession);

        // 1. Subscriptions restored.
        var resubscribe = await second.ReceiveAsync<SubscribePacket>();
        Assert.Equal(
            new HashSet<TopicFilter> { new("cmd/a", MqttQos.AtLeastOnce), new("cmd/b") },
            resubscribe.Filters.ToHashSet());
        await second.SendAsync(new SubAckPacket(resubscribe.PacketId, [ReasonCode.GrantedQos1, ReasonCode.Success]));

        // 2. Unacknowledged publish resent as new; 3. queue flushed in order.
        var resent1 = await second.ReceiveAsync<PublishPacket>();
        Assert.Equal("m/1", resent1.Topic);
        Assert.False(resent1.Dup);
        Assert.NotEqual(sent1.PacketId, resent1.PacketId);
        var sent2 = await second.ReceiveAsync<PublishPacket>();
        var sent3 = await second.ReceiveAsync<PublishPacket>();
        Assert.Equal(["m/2", "m/3"], [sent2.Topic, sent3.Topic]);

        foreach (var publish in new[] { resent1, sent2, sent3 })
        {
            await second.SendAsync(new PubAckPacket(publish.PacketId));
        }

        Assert.All(await Task.WhenAll(m1, m2, m3).WaitAsync(Timeout), r => Assert.Equal(PublishResult.Acknowledged, r));
        await states.WaitForAsync(MqttConnectionState.Connected);
        Assert.Equal(
            [MqttConnectionState.Reconnecting, MqttConnectionState.Connected],
            states.Changes.Select(c => c.Current));
        Assert.IsType<MqttConnectionLostException>(states.Changes[0].Reason);
    }

    [Fact]
    public async Task PersistentSession_SessionPresent_ResendsSameIdWithDup_AndDoesNotResubscribe()
    {
        var (client, first) = await ConnectAsync(Broker.Options() with { CleanSession = false });
        await SubscribeAsync(client, first, [new("cmd", MqttQos.AtLeastOnce)]);
        Task<PublishResult> m1 = client.PublishAsync("m/1", default, MqttQos.AtLeastOnce);
        var sent = await first.ReceiveAsync<PublishPacket>();

        await first.DisposeAsync();
        BrokerSession second = await AcceptAsync();
        Assert.False((await second.AcceptConnectAsync(sessionPresent: true)).CleanSession);

        var resent = await second.ReceiveAsync<PublishPacket>();
        Assert.Equal(sent.PacketId, resent.PacketId);
        Assert.True(resent.Dup);
        await second.SendAsync(new PubAckPacket(resent.PacketId));
        Assert.Equal(PublishResult.Acknowledged, await m1.WaitAsync(Timeout));
    }

    [Fact]
    public async Task PersistentSession_SessionExpired_Resubscribes()
    {
        var (client, first) = await ConnectAsync(Broker.Options() with { CleanSession = false });
        await SubscribeAsync(client, first, [new("cmd", MqttQos.AtLeastOnce)]);

        await first.DisposeAsync();
        BrokerSession second = await AcceptAsync();
        await second.AcceptConnectAsync(sessionPresent: false);

        var resubscribe = await second.ReceiveAsync<SubscribePacket>();
        Assert.Equal([new TopicFilter("cmd", MqttQos.AtLeastOnce)], resubscribe.Filters);
    }

    [Fact]
    public async Task OfflineQueueFull_RejectNew_ReturnsQueueFull_AndQos0IsDropped()
    {
        var (client, first) = await ConnectAsync(Broker.Options() with { OfflineQueueCapacity = 2 });
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);
        Task<PublishResult>[] queued = [client.PublishAsync("q/1", default, MqttQos.AtLeastOnce), client.PublishAsync("q/2", default, MqttQos.AtLeastOnce)];

        Assert.Equal(PublishResult.QueueFull, await client.PublishAsync("q/3", default, MqttQos.AtLeastOnce));
        Assert.Equal(PublishResult.DroppedOffline, await client.PublishAsync("q0", default));

        BrokerSession second = await AcceptAsync();
        await second.AcceptConnectAsync();
        await AckNextAsync(second, "q/1");
        await AckNextAsync(second, "q/2");
        await Task.WhenAll(queued).WaitAsync(Timeout);
    }

    [Fact]
    public async Task OfflineQueueFull_DropOldest_DropsTheOldest()
    {
        var (client, first) = await ConnectAsync(Broker.Options() with { OfflineQueueCapacity = 2, OfflineQueueFullMode = OfflineQueueFullMode.DropOldest });
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);
        Task<PublishResult> oldest = client.PublishAsync("q/1", default, MqttQos.AtLeastOnce);
        Task<PublishResult> kept2 = client.PublishAsync("q/2", default, MqttQos.AtLeastOnce);
        Task<PublishResult> kept3 = client.PublishAsync("q/3", default, MqttQos.AtLeastOnce);

        Assert.Equal(PublishResult.QueueFull, await oldest.WaitAsync(Timeout));
        BrokerSession second = await AcceptAsync();
        await second.AcceptConnectAsync();
        await AckNextAsync(second, "q/2");
        await AckNextAsync(second, "q/3");
        Assert.Equal(PublishResult.Acknowledged, await kept3.WaitAsync(Timeout));
    }

    [Fact]
    public async Task PublishDuringFlush_QueuesBehindOlderMessages()
    {
        var (client, first) = await ConnectAsync(Broker.Options() with { MaxInFlight = 1 });
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);
        _ = client.PublishAsync("q/1", default, MqttQos.AtLeastOnce);
        _ = client.PublishAsync("q/2", default, MqttQos.AtLeastOnce);
        BrokerSession second = await AcceptAsync();
        await second.AcceptConnectAsync();
        var q1 = await second.ReceiveAsync<PublishPacket>();

        // Connected, but the flush is waiting for q/1's PUBACK before sending q/2.
        await states.WaitForAsync(MqttConnectionState.Connected);
        Task<PublishResult> late = client.PublishAsync("late", default, MqttQos.AtLeastOnce);

        await second.SendAsync(new PubAckPacket(q1.PacketId));
        await AckNextAsync(second, "q/2");
        await AckNextAsync(second, "late");
        Assert.Equal(PublishResult.Acknowledged, await late.WaitAsync(Timeout));
    }

    [Fact]
    public async Task Reconnect_ServerUnavailable_IsRetried()
    {
        var (client, first) = await ConnectAsync();
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        BrokerSession refused = await AcceptAsync();
        await refused.AcceptConnectAsync(ReasonCode.ServerUnavailable);
        BrokerSession accepted = await AcceptAsync();
        await accepted.AcceptConnectAsync();

        await states.WaitForAsync(MqttConnectionState.Connected);
        Assert.Equal(MqttConnectionState.Connected, client.State);
    }

    [Theory]
    [InlineData(ReasonCode.NotAuthorized)]
    [InlineData(ReasonCode.ClientIdentifierNotValid)]
    [InlineData(ReasonCode.BadUserNameOrPassword)]
    [InlineData(ReasonCode.UnsupportedProtocolVersion)]
    public async Task Reconnect_PermanentRefusal_GivesUp_AndFailsQueuedPublishes(ReasonCode code)
    {
        var (client, first) = await ConnectAsync();
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);
        Task<PublishResult> queued = client.PublishAsync("q", default, MqttQos.AtLeastOnce);
        BrokerSession refused = await AcceptAsync();
        await refused.AcceptConnectAsync(code);

        ConnectionStateChange change = await states.WaitForAsync(MqttConnectionState.Disconnected);
        Assert.Equal(code, Assert.IsType<MqttConnectException>(change.Reason).ReasonCode);
        await Assert.ThrowsAsync<MqttConnectionLostException>(() => queued.WaitAsync(Timeout));
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.PublishAsync("q", default, MqttQos.AtLeastOnce));
    }

    [Fact]
    public async Task Reconnect_MaxAttempts_GivesUp()
    {
        var options = Broker.Options() with
        {
            Reconnect = new ReconnectOptions { InitialDelay = TimeSpan.FromMilliseconds(10), MaxDelay = TimeSpan.FromMilliseconds(20), MaxAttempts = 2 },
        };
        var (client, first) = await ConnectAsync(options);
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        for (int i = 0; i < 2; i++)
        {
            await (await AcceptAsync()).AcceptConnectAsync(ReasonCode.ServerUnavailable);
        }

        ConnectionStateChange change = await states.WaitForAsync(MqttConnectionState.Disconnected);
        Assert.Equal(ReasonCode.ServerUnavailable, Assert.IsType<MqttConnectException>(change.Reason).ReasonCode);
    }

    [Fact]
    public async Task DisconnectWhileReconnecting_StopsReconnect_AndFailsQueuedPublishes()
    {
        var options = Broker.Options() with
        {
            ReconnectDelayFloor = TimeSpan.FromSeconds(5),
            Reconnect = new ReconnectOptions { InitialDelay = TimeSpan.FromSeconds(5), MaxDelay = TimeSpan.FromSeconds(5) },
        };
        var (client, first) = await ConnectAsync(options);
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);
        Task<PublishResult> queued = client.PublishAsync("q", default, MqttQos.AtLeastOnce);

        await client.DisconnectAsync().WaitAsync(Timeout);

        Assert.Equal(MqttConnectionState.Disconnected, client.State);
        await Assert.ThrowsAsync<MqttConnectionLostException>(() => queued.WaitAsync(Timeout));
        Task<BrokerSession> attempt = AcceptAsync();
        Assert.NotSame(attempt, await Task.WhenAny(attempt, Task.Delay(500)));
    }

    [Fact]
    public async Task WhileReconnecting_SubscribeThrowsConnectionLost_AndConnectIsRejected()
    {
        var options = Broker.Options() with
        {
            ReconnectDelayFloor = TimeSpan.FromSeconds(5),
            Reconnect = new ReconnectOptions { InitialDelay = TimeSpan.FromSeconds(5), MaxDelay = TimeSpan.FromSeconds(5) },
        };
        var (client, first) = await ConnectAsync(options);
        var states = new StateRecorder(client);

        await first.DisposeAsync();
        await states.WaitForAsync(MqttConnectionState.Reconnecting);

        await Assert.ThrowsAsync<MqttConnectionLostException>(() => client.SubscribeAsync([new("a")]));
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.ConnectAsync());
    }

    [Fact]
    public async Task InboundDuplicates_WithDupFlag_AreAcknowledgedButNotDelivered()
    {
        var (client, broker) = await ConnectAsync();
        var delivered = new MessageLog(client);

        await broker.SendAsync(new PublishPacket("t", "x"u8.ToArray(), MqttQos.AtLeastOnce, PacketId: 7));
        await broker.SendAsync(new PublishPacket("t", "x"u8.ToArray(), MqttQos.AtLeastOnce, Dup: true, PacketId: 7));
        await broker.SendAsync(new PublishPacket("t", "y"u8.ToArray(), MqttQos.AtLeastOnce, Dup: true, PacketId: 7));
        await broker.SendAsync(new PublishPacket("t", "x"u8.ToArray(), MqttQos.AtLeastOnce, PacketId: 7));

        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(7, (await broker.ReceiveAsync<PubAckPacket>()).PacketId);
        }

        Assert.Equal(["x", "y", "x"], await delivered.WaitForAsync(3));
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public async Task DuplicateCache_ClearedOnCleanReconnect_KeptForPersistentSession(bool cleanSession, bool redeliveredAfterReconnect)
    {
        var (client, first) = await ConnectAsync(Broker.Options() with { CleanSession = cleanSession });
        var delivered = new MessageLog(client);
        await first.SendAsync(new PublishPacket("t", "x"u8.ToArray(), MqttQos.AtLeastOnce, PacketId: 7));
        await first.ReceiveAsync<PubAckPacket>();

        await first.DisposeAsync();
        BrokerSession second = await AcceptAsync();
        await second.AcceptConnectAsync(sessionPresent: !cleanSession);
        await second.SendAsync(new PublishPacket("t", "x"u8.ToArray(), MqttQos.AtLeastOnce, Dup: true, PacketId: 7));
        await second.SendAsync(new PublishPacket("t", "sync"u8.ToArray(), MqttQos.AtLeastOnce, PacketId: 8));
        await second.ReceiveAsync<PubAckPacket>();
        await second.ReceiveAsync<PubAckPacket>();

        string[] expected = redeliveredAfterReconnect ? ["x", "x", "sync"] : ["x", "sync"];
        Assert.Equal(expected, await delivered.WaitForAsync(expected.Length));
    }

    [Fact]
    public async Task RetainedPublish_WithEmptyPayload_ClearsRetained()
    {
        var (client, broker) = await ConnectAsync();

        Task<PublishResult> publishing = client.PublishAsync("state", ReadOnlyMemory<byte>.Empty, MqttQos.AtLeastOnce, retain: true);
        var publish = await broker.ReceiveAsync<PublishPacket>();

        Assert.True(publish.Retain);
        Assert.Equal(0, publish.Payload.Length);
        await broker.SendAsync(new PubAckPacket(publish.PacketId));
        Assert.Equal(PublishResult.Acknowledged, await publishing.WaitAsync(Timeout));
    }

    [Fact]
    public async Task RepeatedImmediateDrops_WarnAboutDuplicateClientId()
    {
        var logger = new TestLogger();
        var (_, first) = await ConnectAsync(Broker.Options() with { Logger = logger });

        await first.DisposeAsync();
        BrokerSession second = await AcceptAsync();
        await second.AcceptConnectAsync();
        await second.DisposeAsync();
        await (await AcceptAsync()).AcceptConnectAsync();

        Assert.True(logger.Contains(MqttLogLevel.Warning, "another client may be using client ID"));
        Assert.All(logger.Entries, e => Assert.Equal("device-001", e.ClientId));
    }

    private static async Task SubscribeAsync(MqttClient client, BrokerSession broker, TopicFilter[] filters)
    {
        Task<SubscribeResult> subscribing = client.SubscribeAsync(filters);
        var subscribe = await broker.ReceiveAsync<SubscribePacket>();
        await broker.SendAsync(new SubAckPacket(subscribe.PacketId, [.. filters.Select(f => (ReasonCode)f.Qos)]));
        Assert.True((await subscribing.WaitAsync(Timeout)).AllSucceeded);
    }

    private static async Task AckNextAsync(BrokerSession broker, string expectedTopic)
    {
        var publish = await broker.ReceiveAsync<PublishPacket>();
        Assert.Equal(expectedTopic, publish.Topic);
        await broker.SendAsync(new PubAckPacket(publish.PacketId));
    }

    /// <summary>Collects delivered payloads as strings.</summary>
    private sealed class MessageLog
    {
        private readonly ConcurrentQueue<string> _payloads = new();

        public MessageLog(MqttClient client) => client.MessageReceived += message =>
        {
            _payloads.Enqueue(System.Text.Encoding.UTF8.GetString(message.Payload.Span));
            return Task.CompletedTask;
        };

        public async Task<string[]> WaitForAsync(int count)
        {
            using var cts = new CancellationTokenSource(Timeout);
            while (_payloads.Count < count)
            {
                await Task.Delay(10, cts.Token);
            }

            // Give a stray extra delivery a moment to show up.
            await Task.Delay(50);
            return [.. _payloads];
        }
    }
}
