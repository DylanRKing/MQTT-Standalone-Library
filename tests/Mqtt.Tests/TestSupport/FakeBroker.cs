using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Tests.TestSupport;

/// <summary>
/// Scripted in-process broker over loopback TLS. Tests drive each accepted connection packet by
/// packet, so they can withhold acks, refuse CONNECTs or drop the socket at any point.
/// </summary>
internal sealed class FakeBroker : IDisposable
{
    private readonly TlsTestServer _server = new();

    public int Port => _server.Port;

    public MqttClientOptions Options(string clientId = "device-001") => new()
    {
        Host = "localhost",
        Port = _server.Port,
        ClientId = clientId,
        CertificateProvider = new TestCertificateProvider(TestCertificates.Client),
        ServerCertificateValidator = (_, certificate, _, _) =>
            certificate?.GetCertHashString() == TestCertificates.Server.Thumbprint,
        ConnectTimeout = TimeSpan.FromSeconds(5),

        // Fast reconnects; real clients keep AWS's 1 s floor.
        ReconnectDelayFloor = TimeSpan.FromMilliseconds(10),
        Reconnect = new ReconnectOptions { InitialDelay = TimeSpan.FromMilliseconds(10), MaxDelay = TimeSpan.FromMilliseconds(50) },
    };

    public async Task<BrokerSession> AcceptAsync() => new(await _server.AcceptAsync().WaitAsync(BrokerSession.Timeout));

    public void Dispose() => _server.Dispose();
}

internal sealed class BrokerSession(ServerConnection connection) : IAsyncDisposable
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>Reads the next packet; null when the client closed the connection.</summary>
    public async Task<IMqttPacket?> ReceiveAnyAsync()
    {
        byte[]? frame = await connection.ReadFrameAsync().WaitAsync(Timeout);
        if (frame is null)
        {
            return null;
        }

        DecodeError error = MqttDecoder.TryDecode(frame, ProtocolVersion.V311, out IMqttPacket? packet);
        Assert.Equal(DecodeError.None, error);
        return packet;
    }

    public async Task<T> ReceiveAsync<T>()
        where T : IMqttPacket
    {
        IMqttPacket? packet = await ReceiveAnyAsync();
        return Assert.IsType<T>(packet);
    }

    /// <summary>Waits for the client to close the connection, failing if another packet arrives.</summary>
    public async Task AssertClosedAsync() => Assert.Null(await ReceiveAnyAsync());

    public Task SendAsync(IMqttPacket packet)
    {
        var buffer = new byte[MqttEncoder.GetSize(packet, ProtocolVersion.V311)];
        MqttEncoder.Write(buffer, packet, ProtocolVersion.V311);
        return connection.WriteAsync(buffer);
    }

    /// <summary>Receives CONNECT and answers with CONNACK.</summary>
    public async Task<ConnectPacket> AcceptConnectAsync(ReasonCode code = ReasonCode.Success, bool sessionPresent = false)
    {
        var connect = await ReceiveAsync<ConnectPacket>();
        await SendAsync(new ConnAckPacket(sessionPresent, code));
        return connect;
    }

    public ValueTask DisposeAsync() => connection.DisposeAsync();
}
