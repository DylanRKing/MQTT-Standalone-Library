using System.Collections.Concurrent;

namespace Mqtt.Tests.TestSupport;

/// <summary>Records <see cref="MqttClient.ConnectionStateChanged"/> events and waits for a given state.</summary>
internal sealed class StateRecorder
{
    private readonly ConcurrentQueue<ConnectionStateChange> _changes = new();
    private readonly SemaphoreSlim _signal = new(0);

    public StateRecorder(MqttClient client) => client.ConnectionStateChanged += change =>
    {
        _changes.Enqueue(change);
        _signal.Release();
        return Task.CompletedTask;
    };

    public IReadOnlyList<ConnectionStateChange> Changes => [.. _changes];

    public async Task<ConnectionStateChange> WaitForAsync(MqttConnectionState state)
    {
        using var cts = new CancellationTokenSource(BrokerSession.Timeout);
        while (true)
        {
            ConnectionStateChange? match = _changes.LastOrDefault(c => c.Current == state);
            if (match is not null)
            {
                return match;
            }

            await _signal.WaitAsync(cts.Token);
        }
    }
}
