using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading.Channels;
using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Internal.Transport;
using Mqtt.Tests.TestSupport;
using static Mqtt.Tests.Codec.CodecHelpers;

namespace Mqtt.Tests.Transport;

public sealed class MqttTransportTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly TlsTestServer _server = new();
    private readonly Channel<byte[]> _received = Channel.CreateUnbounded<byte[]>();
    private readonly TaskCompletionSource<Exception> _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _closedCount;

    public void Dispose() => _server.Dispose();

    [Fact]
    public async Task Connect_UsesMutualTls12OrLater()
    {
        var provider = new TestCertificateProvider(TestCertificates.Client);
        var (transport, connection) = await ConnectAsync(Options() with { CertificateProvider = provider });
        await using var _ = transport;
        await using var __ = connection;

        Assert.Equal(TestCertificates.Client.Thumbprint, connection.ClientCertificate?.GetCertHashString());
        Assert.True(transport.SslProtocol is SslProtocols.Tls12 or SslProtocols.Tls13);
        Assert.Equal(1, provider.Calls);
    }

    [Fact]
    public async Task Send_ConcurrentFrames_ArriveWholeAndNotInterleaved()
    {
        var (transport, connection) = await ConnectAsync(Options() with { SendQueueCapacity = 8 });
        await using var _ = transport;
        await using var __ = connection;
        const int count = 200;

        Task[] sends = Enumerable.Range(0, count)
            .Select(i => Task.Run(() => SendAsync(transport, new PublishPacket($"t/{i}", new byte[i * 37]))))
            .ToArray();

        var topics = new HashSet<string>();
        for (int i = 0; i < count; i++)
        {
            byte[] frame = (await connection.ReadFrameAsync().WaitAsync(Timeout))!;
            var publish = Decode<PublishPacket>(frame);
            Assert.Equal(int.Parse(publish.Topic[2..]) * 37, publish.Payload.Length);
            topics.Add(publish.Topic);
        }

        await Task.WhenAll(sends).WaitAsync(Timeout);
        Assert.Equal(count, topics.Count);
    }

    [Fact]
    public async Task Receive_FramesSplitAcrossAndCoalescedWithinReads()
    {
        var (transport, connection) = await ConnectAsync(Options());
        await using var _ = transport;
        await using var __ = connection;
        byte[][] frames =
        [
            Encode(new ConnAckPacket(false, ReasonCode.Success)),
            Encode(new PublishPacket("a/b", "hello"u8.ToArray(), MqttQos.AtLeastOnce, PacketId: 7)),
            Encode(new SubAckPacket(1, [ReasonCode.GrantedQos1])),
            Encode(PingRespPacket.Instance),
        ];

        // First two frames one byte per TLS record, the last two in a single write.
        foreach (byte b in frames[0].Concat(frames[1]))
        {
            await connection.WriteAsync([b]);
        }

        await connection.WriteAsync([.. frames[2], .. frames[3]]);

        foreach (byte[] expected in frames)
        {
            Assert.Equal(expected, await _received.Reader.ReadAsync().AsTask().WaitAsync(Timeout));
        }
    }

    [Fact]
    public async Task Receive_MaximumSizePayload()
    {
        var (transport, connection) = await ConnectAsync(Options());
        await using var _ = transport;
        await using var __ = connection;
        byte[] payload = new byte[128 * 1024];
        Random.Shared.NextBytes(payload);
        byte[] frame = Encode(new PublishPacket("sim/device-042/cmd", payload, MqttQos.AtLeastOnce, PacketId: 1));

        await connection.WriteAsync(frame);

        Assert.Equal(frame, await _received.Reader.ReadAsync().AsTask().WaitAsync(Timeout));
    }

    [Fact]
    public async Task Receive_FrameOverMaximum_ClosesConnection()
    {
        var (transport, connection) = await ConnectAsync(Options() with { MaxFrameSize = 8 * 1024 });
        await using var _ = transport;
        await using var __ = connection;

        await connection.WriteAsync(Encode(new PublishPacket("t", new byte[10_000])));

        Exception reason = await _closed.Task.WaitAsync(Timeout);
        Assert.IsType<MqttConnectionLostException>(reason);
        Assert.Contains("maximum frame size", reason.Message);
        Assert.True(transport.IsClosed);
    }

    [Fact]
    public async Task Receive_MalformedHeader_ClosesConnection()
    {
        var (transport, connection) = await ConnectAsync(Options());
        await using var _ = transport;
        await using var __ = connection;

        await connection.WriteAsync(Hex("30 FF FF FF FF 01"));

        Exception reason = await _closed.Task.WaitAsync(Timeout);
        Assert.Contains("malformed", reason.Message);
    }

    [Fact]
    public async Task HandlerThrows_ClosesConnection()
    {
        var (transport, connection) = await ConnectAsync(
            Options(),
            onFrame: _ => throw new InvalidDataException("bad packet"));
        await using var _ = transport;
        await using var __ = connection;

        await connection.WriteAsync(Encode(PingRespPacket.Instance));

        Exception reason = await _closed.Task.WaitAsync(Timeout);
        Assert.IsType<InvalidDataException>(reason.InnerException);
    }

    [Fact]
    public async Task BrokerCloses_RaisesClosedOnce_AndLaterSendsFail()
    {
        var (transport, connection) = await ConnectAsync(Options());
        await using var _ = transport;

        await connection.DisposeAsync();

        Exception reason = await _closed.Task.WaitAsync(Timeout);
        Assert.IsType<MqttConnectionLostException>(reason);
        await Assert.ThrowsAsync<MqttConnectionLostException>(() => SendAsync(transport, PingReqPacket.Instance));
        await transport.CloseAsync();
        Assert.Equal(1, _closedCount);
    }

    [Fact]
    public async Task CloseAsync_DoesNotRaiseClosed_AndIsIdempotent()
    {
        var (transport, connection) = await ConnectAsync(Options());
        await using var __ = connection;

        await SendAsync(transport, DisconnectPacket.Instance);
        await transport.CloseAsync();
        await transport.CloseAsync();

        Assert.Equal(Hex("E0 00"), await connection.ReadFrameAsync().WaitAsync(Timeout));
        Assert.Null(await connection.ReadFrameAsync().WaitAsync(Timeout));
        await Assert.ThrowsAsync<MqttConnectionLostException>(() => SendAsync(transport, PingReqPacket.Instance));
        Assert.Equal(0, _closedCount);
    }

    [Fact]
    public async Task ServerCertificateRejected_ThrowsAuthenticationException()
    {
        Task accept = _server.AcceptAsync();

        await Assert.ThrowsAsync<AuthenticationException>(() => MqttTransport.ConnectAsync(
            Options() with { ServerCertificateValidator = (_, _, _, _) => false },
            OnFrame,
            OnClosed,
            CancellationToken.None));

        // With TLS 1.3 the server may finish its side before the client rejects; either outcome is fine.
        await Record.ExceptionAsync(() => accept);
    }

    [Fact]
    public async Task Cancelled_WhileBrokerNeverHandshakes_Throws()
    {
        Task<Socket> accept = _server.AcceptRawAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            MqttTransport.ConnectAsync(Options(), OnFrame, OnClosed, cts.Token));

        (await accept).Dispose();
    }

    [Fact]
    public async Task NothingListening_ThrowsSocketException()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();

        await Assert.ThrowsAsync<SocketException>(() =>
            MqttTransport.ConnectAsync(Options() with { Port = port }, OnFrame, OnClosed, CancellationToken.None));
    }

    [Fact]
    public async Task CertificateWithoutPrivateKey_IsRejectedBeforeConnecting()
    {
        var options = Options() with { CertificateProvider = new TestCertificateProvider(TestCertificates.ClientWithoutKey) };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            MqttTransport.ConnectAsync(options, OnFrame, OnClosed, CancellationToken.None));
    }

    [Fact]
    public async Task CertificateProvider_IsCalledOnEveryConnect()
    {
        var provider = new TestCertificateProvider(TestCertificates.Client);

        for (int i = 0; i < 2; i++)
        {
            var (transport, connection) = await ConnectAsync(Options() with { CertificateProvider = provider });
            await transport.CloseAsync();
            await connection.DisposeAsync();
        }

        Assert.Equal(2, provider.Calls);
    }

    private TransportOptions Options() => new()
    {
        Host = "localhost",
        Port = _server.Port,
        ClientId = "test-device",
        CertificateProvider = new TestCertificateProvider(TestCertificates.Client),
        ServerCertificateValidator = (_, certificate, _, _) =>
            certificate?.GetCertHashString() == TestCertificates.Server.Thumbprint,
    };

    private async Task<(MqttTransport Transport, ServerConnection Connection)> ConnectAsync(
        TransportOptions options,
        FrameHandler? onFrame = null)
    {
        Task<ServerConnection> accept = _server.AcceptAsync();
        MqttTransport transport = await MqttTransport.ConnectAsync(options, onFrame ?? OnFrame, OnClosed, CancellationToken.None)
            .WaitAsync(Timeout);
        return (transport, await accept.WaitAsync(Timeout));
    }

    private ValueTask OnFrame(ReadOnlyMemory<byte> frame) => _received.Writer.WriteAsync(frame.ToArray());

    private void OnClosed(Exception reason)
    {
        Interlocked.Increment(ref _closedCount);
        _closed.TrySetResult(reason);
    }

    private static Task SendAsync(MqttTransport transport, IMqttPacket packet)
    {
        int size = MqttEncoder.GetSize(packet, ProtocolVersion.V311);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        MqttEncoder.Write(buffer, packet, ProtocolVersion.V311);
        return transport.SendAsync(buffer, size);
    }
}
