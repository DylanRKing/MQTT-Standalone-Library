using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Mqtt.Tests.TestSupport;
using Xunit.Abstractions;

namespace Mqtt.Tests.Scale;

/// <summary>Runs only when MQTT_SOAK_MINUTES is set, e.g. <c>MQTT_SOAK_MINUTES=240 dotnet test --filter SoakTests</c>.</summary>
public sealed class SoakFactAttribute : FactAttribute
{
    public SoakFactAttribute()
    {
        if (SoakTests.Duration is null)
        {
            Skip = "Set MQTT_SOAK_MINUTES to run the soak test.";
        }
    }
}

/// <summary>
/// 100 devices publishing for a long time with periodic network drops. Checks that no QoS 1
/// message is lost and that memory stays flat.
/// </summary>
public sealed class SoakTests(ITestOutputHelper output)
{
    private const int Devices = 100;

    public static TimeSpan? Duration =>
        double.TryParse(Environment.GetEnvironmentVariable("MQTT_SOAK_MINUTES"), out double minutes) && minutes > 0
            ? TimeSpan.FromMinutes(minutes)
            : null;

    private static TimeSpan DropInterval =>
        double.TryParse(Environment.GetEnvironmentVariable("MQTT_SOAK_DROP_SECONDS"), out double seconds) && seconds > 0
            ? TimeSpan.FromSeconds(seconds)
            : TimeSpan.FromSeconds(60);

    [SoakFact]
    public async Task HundredDevices_PeriodicDrops_NoQos1Lost_MemoryFlat()
    {
        TimeSpan duration = Duration!.Value;
        // The broker shares this process, so it must not grow either for the memory check to mean anything.
        await using var broker = new AutoBroker { RecordReceived = false };
        var throttle = new ConnectionThrottle(10);
        MqttClient[] clients = [.. Enumerable.Range(0, Devices).Select(i => new MqttClient(broker.Options($"soak-{i:000}") with
        {
            ConnectionThrottle = throttle,
            MaxPublishRate = 50,
        }))];

        try
        {
            await Task.WhenAll(clients.Select(c => c.ConnectAsync()));
            await Task.WhenAll(clients.Select(c => c.SubscribeAsync([new($"sim/{c.ClientId}/cmd", MqttQos.AtLeastOnce)])));

            using var stop = new CancellationTokenSource(duration);
            var outcomes = new ConcurrentDictionary<PublishResult, long>();
            long failures = 0;
            Task[] publishers = [.. clients.Select(client => Task.Run(async () =>
            {
                long n = 0;
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        PublishResult result = await client.PublishAsync($"sim/{client.ClientId}/telemetry", Encoding.UTF8.GetBytes($"{n++}"), MqttQos.AtLeastOnce);
                        outcomes.AddOrUpdate(result, 1, (_, c) => c + 1);
                        await client.PublishAsync($"sim/{client.ClientId}/heartbeat", "x"u8.ToArray());
                        await Task.Delay(Random.Shared.Next(200, 800));
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref failures);
                        output.WriteLine($"{client.ClientId}: {ex.GetType().Name}: {ex.Message}");
                    }
                }

                return n;
            }))];

            long? baseline = null;
            var clock = Stopwatch.StartNew();
            while (!stop.IsCancellationRequested)
            {
                await Task.Delay(DropInterval, stop.Token).ContinueWith(_ => { });
                if (stop.IsCancellationRequested)
                {
                    break;
                }

                await broker.DropAllAsync();
                long memory = GC.GetTotalMemory(forceFullCollection: true);

                // Measured from the second drop, once pools and caches have warmed up.
                if (clock.Elapsed > DropInterval * 1.5)
                {
                    baseline ??= memory;
                }

                output.WriteLine($"{clock.Elapsed:hh\\:mm\\:ss} dropped all; connects {broker.Connects}, broker received {broker.ReceivedCount}, managed memory {memory / 1024 / 1024} MB, acked {outcomes.GetValueOrDefault(PublishResult.Acknowledged)}");
            }

            await Task.WhenAll(publishers);
            long finalMemory = GC.GetTotalMemory(forceFullCollection: true);
            output.WriteLine($"Done: {outcomes.GetValueOrDefault(PublishResult.Acknowledged)} acknowledged, {failures} failures, memory {finalMemory / 1024 / 1024} MB.");

            Assert.Equal(0, failures);
            Assert.Equal(0, outcomes.GetValueOrDefault(PublishResult.QueueFull));
            if (baseline is long first)
            {
                Assert.True(finalMemory < first + (64 * 1024 * 1024), $"Managed memory grew from {first / 1024 / 1024} MB to {finalMemory / 1024 / 1024} MB.");
            }
        }
        finally
        {
            await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));
        }
    }
}
