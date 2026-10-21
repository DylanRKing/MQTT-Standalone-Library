using System.Security.Authentication;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Core;

public class ConnectTests : ClientTestBase
{
    [Fact]
    public async Task Connect_SendsConnect_AndReportsStates()
    {
        MqttClient client = CreateClient(Broker.Options("device-042") with { CleanSession = false });
        var states = new StateRecorder(client);
        Task<ConnectPacket> broker = Task.Run(async () => await (await AcceptAsync()).AcceptConnectAsync());

        await client.ConnectAsync().WaitAsync(Timeout);
        ConnectPacket connect = await broker;

        Assert.Equal(new ConnectPacket("device-042", CleanSession: false, KeepAliveSeconds: 60), connect);
        Assert.Equal(MqttConnectionState.Connected, client.State);
        await states.WaitForAsync(MqttConnectionState.Connected);
        Assert.Equal(
            [MqttConnectionState.Connecting, MqttConnectionState.Connected],
            states.Changes.Select(c => c.Current));
    }

    [Theory]
    [InlineData(5, 30)]
    [InlineData(45, 45)]
    [InlineData(5_000, 1_200)]
    public async Task KeepAlive_IsClampedToAwsRange(int requestedSeconds, int sentSeconds)
    {
        MqttClient client = CreateClient(Broker.Options() with { KeepAlive = TimeSpan.FromSeconds(requestedSeconds) });
        Task<ConnectPacket> broker = Task.Run(async () => await (await AcceptAsync()).AcceptConnectAsync());

        await client.ConnectAsync().WaitAsync(Timeout);

        Assert.Equal(sentSeconds, (await broker).KeepAliveSeconds);
    }

    [Theory]
    [InlineData(ReasonCode.NotAuthorized)]
    [InlineData(ReasonCode.ClientIdentifierNotValid)]
    [InlineData(ReasonCode.ServerUnavailable)]
    public async Task Connect_Refused_ThrowsWithReasonCode(ReasonCode code)
    {
        MqttClient client = CreateClient();
        var states = new StateRecorder(client);
        Task<BrokerSession> broker = Task.Run(async () =>
        {
            BrokerSession session = await AcceptAsync();
            await session.AcceptConnectAsync(code);
            return session;
        });

        var ex = await Assert.ThrowsAsync<MqttConnectException>(() => client.ConnectAsync());

        Assert.Equal(code, ex.ReasonCode);
        Assert.Equal(MqttConnectionState.Disconnected, client.State);
        await (await broker).AssertClosedAsync();
        await states.WaitForAsync(MqttConnectionState.Disconnected);
    }

    [Fact]
    public async Task Connect_NoConnAck_TimesOut()
    {
        MqttClient client = CreateClient(Broker.Options() with { ConnectTimeout = TimeSpan.FromMilliseconds(500) });
        Task<BrokerSession> broker = Task.Run(async () =>
        {
            BrokerSession session = await AcceptAsync();
            await session.ReceiveAsync<ConnectPacket>();
            return session;
        });

        var ex = await Assert.ThrowsAsync<MqttConnectException>(() => client.ConnectAsync());

        Assert.Contains("timed out", ex.Message);
        Assert.Null(ex.ReasonCode);
        await (await broker).AssertClosedAsync();
    }

    [Fact]
    public async Task Connect_FirstPacketNotConnAck_Fails()
    {
        MqttClient client = CreateClient();
        _ = Task.Run(async () =>
        {
            BrokerSession session = await AcceptAsync();
            await session.ReceiveAsync<ConnectPacket>();
            await session.SendAsync(PingRespPacket.Instance);
        });

        var ex = await Assert.ThrowsAsync<MqttConnectException>(() => client.ConnectAsync());

        Assert.Contains("CONNACK", ex.InnerException?.ToString());
        Assert.Equal(MqttConnectionState.Disconnected, client.State);
    }

    [Fact]
    public async Task Connect_TlsFailure_WrapsAuthenticationException()
    {
        MqttClient client = CreateClient(Broker.Options() with { ServerCertificateValidator = (_, _, _, _) => false });
        _ = Task.Run(async () => await Record.ExceptionAsync(AcceptAsync));

        var ex = await Assert.ThrowsAsync<MqttConnectException>(() => client.ConnectAsync());

        Assert.IsType<AuthenticationException>(ex.InnerException);
    }

    [Fact]
    public async Task Connect_WhenConnected_DoesNothing()
    {
        var (client, broker) = await ConnectAsync();

        await client.ConnectAsync();

        // The next packet the broker sees is the publish, not a second CONNECT.
        await client.PublishAsync("t", "x"u8.ToArray());
        await broker.ReceiveAsync<PublishPacket>();
    }

    [Fact]
    public async Task Disconnect_SendsDisconnect_AndCloses()
    {
        var (client, broker) = await ConnectAsync();
        var states = new StateRecorder(client);

        await client.DisconnectAsync();
        await client.DisconnectAsync();

        await broker.ReceiveAsync<DisconnectPacket>();
        await broker.AssertClosedAsync();
        Assert.Equal(MqttConnectionState.Disconnected, client.State);
        Assert.Null((await states.WaitForAsync(MqttConnectionState.Disconnected)).Reason);
    }

    [Fact]
    public async Task BrokerDropsConnection_GoesDisconnectedWithReason()
    {
        var (client, broker) = await ConnectAsync();
        var states = new StateRecorder(client);

        await broker.DisposeAsync();

        ConnectionStateChange change = await states.WaitForAsync(MqttConnectionState.Disconnected);
        Assert.IsType<MqttConnectionLostException>(change.Reason);
        Assert.Equal(MqttConnectionState.Disconnected, client.State);
    }

    [Fact]
    public async Task Dispose_DisconnectsAndRejectsFurtherCalls()
    {
        var (client, broker) = await ConnectAsync();

        await client.DisposeAsync();
        await client.DisposeAsync();

        await broker.ReceiveAsync<DisconnectPacket>();
        Assert.Equal(MqttConnectionState.Disposed, client.State);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.ConnectAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.PublishAsync("t", default));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.SubscribeAsync([new("t")]));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.DisconnectAsync());
    }
}
