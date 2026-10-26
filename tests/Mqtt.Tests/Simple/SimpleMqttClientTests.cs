using System.Text;
using Mqtt.Simple;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Simple;

/// <summary>Round trips through <see cref="SimpleMqttClient"/> against the in-process broker.</summary>
public class SimpleMqttClientTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Connect_Subscribe_Publish_Receive_Disconnect()
    {
        await using var broker = new AutoBroker();
        await using SimpleMqttClient client = await ConnectAsync(broker);

        var received = new TaskCompletionSource<(string Topic, string Payload)>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.MessageReceived += (topic, payload) => received.TrySetResult((topic, Encoding.UTF8.GetString(payload)));

        await client.SubscribeAsync("devices/+/cmd", qos: 1).WaitAsync(Timeout);
        Assert.Equal(1, await broker.PublishAsync("devices/1/cmd", "on"u8.ToArray(), MqttQos.AtLeastOnce));
        Assert.Equal(("devices/1/cmd", "on"), await received.Task.WaitAsync(Timeout));

        await client.PublishAsync("devices/1/status", "ok", qos: 1).WaitAsync(Timeout);
        await client.PublishAsync("devices/1/status", "fine", qos: 0).WaitAsync(Timeout);
        await client.DisconnectAsync().WaitAsync(Timeout);

        Assert.Equal(1, broker.Received["devices/1/status|ok"]);
        await WaitUntilAsync(() => broker.Received.ContainsKey("devices/1/status|fine"));
        await WaitUntilAsync(() => broker.OpenSessions == 0);
    }

    [Fact]
    public async Task BrokerDrop_RaisesDisconnected()
    {
        await using var broker = new AutoBroker();
        await using SimpleMqttClient client = await ConnectAsync(broker);
        var lost = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Disconnected += ex => lost.TrySetResult(ex);

        await WaitUntilAsync(() => broker.OpenSessions == 1);
        await broker.DropAllAsync();

        await lost.Task.WaitAsync(Timeout);
        await Assert.ThrowsAsync<IOException>(() => client.PublishAsync("t", "x", qos: 1));
    }

    private static Task<SimpleMqttClient> ConnectAsync(AutoBroker broker)
    {
        MqttClientOptions options = broker.Options("simple-1");
        return SimpleMqttClient.ConnectAsync(
            options.Host, options.ClientId, TestCertificates.Client, options.Port, options.ServerCertificateValidator)
            .WaitAsync(Timeout);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(Timeout);
        while (!condition())
        {
            await Task.Delay(10, cts.Token);
        }
    }
}
