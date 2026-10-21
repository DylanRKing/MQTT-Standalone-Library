using System.Collections.Concurrent;
using System.Text;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Scale;

/// <summary>Many clients in one process against one broker: throttling, isolation, mass reconnect.</summary>
public sealed class ScaleTests : IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private readonly AutoBroker _broker = new();
    private readonly List<MqttClient> _clients = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await Task.WhenAll(_clients.Select(c => c.DisposeAsync().AsTask()));
        await _broker.DisposeAsync();
    }

    [Fact]
    public async Task Throttle_LimitsConcurrentConnects()
    {
        _broker.ConnAckDelay = TimeSpan.FromMilliseconds(50);
        var throttle = new ConnectionThrottle(3);

        MqttClient[] clients = Create(30, id => _broker.Options(id) with { ConnectionThrottle = throttle });
        await Task.WhenAll(clients.Select(c => c.ConnectAsync())).WaitAsync(Timeout);

        Assert.All(clients, c => Assert.Equal(MqttConnectionState.Connected, c.State));
        Assert.InRange(_broker.MaxPendingConnects, 1, 3);
    }

    [Fact]
    public async Task WithoutThrottle_ConnectsRunConcurrently()
    {
        // Control for the test above: shows the broker's measurement does see concurrency.
        _broker.ConnAckDelay = TimeSpan.FromMilliseconds(200);

        MqttClient[] clients = Create(30, _broker.Options);
        await Task.WhenAll(clients.Select(c => c.ConnectAsync())).WaitAsync(Timeout);

        Assert.True(_broker.MaxPendingConnects > 3, $"Max pending connects was {_broker.MaxPendingConnects}.");
    }

    [Fact]
    public async Task HundredDevices_SurviveMassDrop_WithoutLosingQos1()
    {
        const int devices = 100;
        const int messagesPerDevice = 20;
        var throttle = new ConnectionThrottle(10);
        MqttClient[] clients = Create(devices, id => _broker.Options(id) with { ConnectionThrottle = throttle });
        var commands = new ConcurrentDictionary<string, string>();
        foreach (MqttClient client in clients)
        {
            client.MessageReceived += message =>
            {
                commands[client.ClientId] = message.Topic;
                return Task.CompletedTask;
            };
        }

        await Task.WhenAll(clients.Select(c => c.ConnectAsync())).WaitAsync(Timeout);
        Assert.InRange(_broker.MaxPendingConnects, 1, 10);
        await Task.WhenAll(clients.Select(c => c.SubscribeAsync([new($"sim/{c.ClientId}/cmd", MqttQos.AtLeastOnce)]))).WaitAsync(Timeout);

        // Every device publishes concurrently; halfway through, the network drops for everyone.
        var halfway = new CountdownEvent(devices);
        var dropped = new TaskCompletionSource();
        Task<PublishResult[]>[] publishing = [.. clients.Select(client => Task.Run(async () =>
        {
            var results = new List<Task<PublishResult>>();
            for (int n = 0; n < messagesPerDevice; n++)
            {
                if (n == messagesPerDevice / 2)
                {
                    halfway.Signal();
                    await dropped.Task;
                }

                results.Add(client.PublishAsync($"sim/{client.ClientId}/telemetry", Encoding.UTF8.GetBytes($"{n}"), MqttQos.AtLeastOnce));
            }

            return await Task.WhenAll(results);
        }))];

        Assert.True(halfway.Wait(Timeout));
        await _broker.DropAllAsync();
        dropped.SetResult();

        PublishResult[][] results = await Task.WhenAll(publishing).WaitAsync(Timeout);
        Assert.All(results.SelectMany(r => r), r => Assert.Equal(PublishResult.Acknowledged, r));

        // At least once: every message reached the broker, some possibly twice after the drop.
        foreach (MqttClient client in clients)
        {
            for (int n = 0; n < messagesPerDevice; n++)
            {
                Assert.True(_broker.Received.ContainsKey($"sim/{client.ClientId}/telemetry|{n}"), $"{client.ClientId} message {n} lost.");
            }
        }

        Assert.True(_broker.Connects >= devices * 2, $"Expected every device to reconnect; saw {_broker.Connects} connects.");
        await WaitUntilAsync(() => clients.All(c => c.State == MqttConnectionState.Connected));

        // Subscriptions were restored on the new connections: each device gets only its own command.
        await WaitUntilAsync(() => _broker.OpenSessions == devices);
        foreach (MqttClient client in clients)
        {
            Assert.Equal(1, await _broker.PublishAsync($"sim/{client.ClientId}/cmd", "go"u8.ToArray(), MqttQos.AtLeastOnce));
        }

        await WaitUntilAsync(() => commands.Count == devices);
        Assert.All(commands, kv => Assert.Equal($"sim/{kv.Key}/cmd", kv.Value));
    }

    private MqttClient[] Create(int count, Func<string, MqttClientOptions> options)
    {
        MqttClient[] clients = [.. Enumerable.Range(0, count).Select(i => new MqttClient(options($"device-{i:000}")))];
        _clients.AddRange(clients);
        return clients;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(Timeout);
        while (!condition())
        {
            await Task.Delay(20, cts.Token);
        }
    }
}
