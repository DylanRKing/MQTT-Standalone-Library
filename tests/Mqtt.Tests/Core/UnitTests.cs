using Mqtt.Internal.Core;
using Mqtt.Tests.TestSupport;

namespace Mqtt.Tests.Core;

public class PacketIdAllocatorTests
{
    [Fact]
    public void StartsAtOne_AndWrapsAfter65535_SkippingIdsInUse()
    {
        var allocator = new PacketIdAllocator();
        Assert.Equal(1, allocator.Allocate());
        Assert.Equal(2, allocator.Allocate());

        for (int i = 3; i <= ushort.MaxValue; i++)
        {
            allocator.Release(allocator.Allocate());
        }

        // 1 and 2 are still held, so the wrap skips to 3.
        Assert.Equal(3, allocator.Allocate());
        allocator.Release(1);
        allocator.Release(3);
        Assert.Equal(4, allocator.Allocate());
    }

    [Fact]
    public void AllInUse_Throws()
    {
        var allocator = new PacketIdAllocator();
        for (int i = 0; i < ushort.MaxValue; i++)
        {
            allocator.Allocate();
        }

        Assert.Throws<InvalidOperationException>(() => allocator.Allocate());
        allocator.Clear();
        Assert.Equal(0, allocator.InUseCount);
    }
}

public class OptionsValidationTests
{
    private static MqttClientOptions Valid => new()
    {
        Host = "example.iot.eu-west-2.amazonaws.com",
        ClientId = "device-001",
        CertificateProvider = new TestCertificateProvider(TestCertificates.Client),
    };

    public static TheoryData<string> InvalidOptionNames() =>
    [
        "Host", "Port", "ClientId empty", "ClientId too long", "CertificateProvider", "ConnectTimeout", "AckTimeout", "MaxInFlight 0", "MaxInFlight 101",
    ];

    [Theory]
    [MemberData(nameof(InvalidOptionNames))]
    public void Invalid_Throws(string name)
    {
        MqttClientOptions options = name switch
        {
            "Host" => Valid with { Host = " " },
            "Port" => Valid with { Port = 0 },
            "ClientId empty" => Valid with { ClientId = "" },
            "ClientId too long" => Valid with { ClientId = new string('é', 65) },
            "CertificateProvider" => Valid with { CertificateProvider = null! },
            "ConnectTimeout" => Valid with { ConnectTimeout = TimeSpan.Zero },
            "AckTimeout" => Valid with { AckTimeout = TimeSpan.FromSeconds(-1) },
            "MaxInFlight 0" => Valid with { MaxInFlight = 0 },
            "MaxInFlight 101" => Valid with { MaxInFlight = 101 },
            _ => throw new ArgumentOutOfRangeException(nameof(name)),
        };

        Assert.ThrowsAny<ArgumentException>(() => new MqttClient(options));
    }

    [Fact]
    public async Task Valid_Constructs_WithDefaults()
    {
        await using var client = new MqttClient(Valid with { ClientId = new string('a', 128) });

        Assert.Equal(MqttConnectionState.Disconnected, client.State);
        Assert.Equal(8883, Valid.Port);
        Assert.Equal(TimeSpan.FromSeconds(60), Valid.EffectiveKeepAlive);
        Assert.Equal(20, Valid.MaxInFlight);
    }
}
