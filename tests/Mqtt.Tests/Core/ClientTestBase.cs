using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Core;

public abstract class ClientTestBase : IAsyncLifetime
{
    private readonly List<MqttClient> _clients = [];
    private readonly List<BrokerSession> _sessions = [];

    internal FakeBroker Broker { get; } = new();

    internal static TimeSpan Timeout => BrokerSession.Timeout;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (MqttClient client in _clients)
        {
            await client.DisposeAsync();
        }

        foreach (BrokerSession session in _sessions)
        {
            await session.DisposeAsync();
        }

        Broker.Dispose();
    }

    internal MqttClient CreateClient(MqttClientOptions? options = null)
    {
        var client = new MqttClient(options ?? Broker.Options());
        _clients.Add(client);
        return client;
    }

    internal async Task<BrokerSession> AcceptAsync()
    {
        BrokerSession session = await Broker.AcceptAsync();
        _sessions.Add(session);
        return session;
    }

    /// <summary>Connects a new client; the broker accepts with CONNACK 0.</summary>
    internal async Task<(MqttClient Client, BrokerSession Broker)> ConnectAsync(MqttClientOptions? options = null)
    {
        MqttClient client = CreateClient(options);
        Task<BrokerSession> accept = Task.Run(async () =>
        {
            BrokerSession session = await AcceptAsync();
            await session.AcceptConnectAsync();
            return session;
        });
        await client.ConnectAsync().WaitAsync(Timeout);
        return (client, await accept.WaitAsync(Timeout));
    }
}
