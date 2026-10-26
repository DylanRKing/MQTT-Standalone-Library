using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Mqtt.Simple;

/// <summary>
/// A minimal MQTT 3.1.1 client for AWS IoT Core: connect over TLS with a client certificate,
/// publish, subscribe and disconnect. QoS 0 and 1 only. No reconnect, no offline queue and
/// almost no validation; if the connection drops, create a new client.
/// </summary>
public sealed class SimpleMqttClient : IAsyncDisposable
{
    private const int KeepAliveSeconds = 60;

    private readonly SslStream _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<ushort, TaskCompletionSource<byte[]>> _pendingAcks = new();
    private readonly CancellationTokenSource _stop = new();
    private Task _readLoop = Task.CompletedTask;
    private Task _pingLoop = Task.CompletedTask;
    private int _nextPacketId;

    private SimpleMqttClient(SslStream stream) => _stream = stream;

    /// <summary>Raised from the read loop for each message received: topic, then payload.</summary>
    public event Action<string, byte[]>? MessageReceived;

    /// <summary>Raised once if the connection drops, with the reason.</summary>
    public event Action<Exception>? Disconnected;

    /// <summary>
    /// Opens a TLS connection and sends CONNECT. Completes when the broker accepts it.
    /// </summary>
    /// <param name="host">AWS IoT endpoint, e.g. <c>abc123-ats.iot.eu-west-1.amazonaws.com</c>.</param>
    /// <param name="clientId">Client ID; for AWS normally the thing name.</param>
    /// <param name="certificate">Device certificate with its private key. See <see cref="LoadCertificate"/>.</param>
    /// <param name="port">8883 for MQTT over TLS.</param>
    /// <param name="serverCertificateValidation">Leave null for AWS; set it to trust a local test broker.</param>
    public static async Task<SimpleMqttClient> ConnectAsync(
        string host,
        string clientId,
        X509Certificate2 certificate,
        int port = 8883,
        RemoteCertificateValidationCallback? serverCertificateValidation = null,
        CancellationToken ct = default)
    {
        var tcp = new TcpClient { NoDelay = true };
        SslStream? stream = null;
        try
        {
            await tcp.ConnectAsync(host, port, ct);
            stream = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false);
            await stream.AuthenticateAsClientAsync(
                new SslClientAuthenticationOptions
                {
                    TargetHost = host,
                    ClientCertificates = [certificate],
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    RemoteCertificateValidationCallback = serverCertificateValidation,
                },
                ct);

            var client = new SimpleMqttClient(stream);
            await client.WriteAsync(Connect(clientId), ct);

            (byte header, byte[] body) = await client.ReadPacketAsync(ct);
            if (header >> 4 != 2 || body.Length < 2)
            {
                throw new IOException("Expected CONNACK.");
            }

            if (body[1] != 0)
            {
                throw new IOException($"Broker refused the connection, CONNACK code {body[1]}.");
            }

            client._readLoop = Task.Run(client.ReadLoopAsync);
            client._pingLoop = Task.Run(client.PingLoopAsync);
            return client;
        }
        catch
        {
            if (stream is not null)
            {
                await stream.DisposeAsync();
            }

            tcp.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Loads the PEM certificate and private key AWS gives you when you create a thing.
    /// Re-imports through PKCS#12 because Windows TLS cannot use a key loaded straight from PEM.
    /// </summary>
    public static X509Certificate2 LoadCertificate(string certificatePemPath, string privateKeyPemPath)
    {
        using X509Certificate2 pem = X509Certificate2.CreateFromPemFile(certificatePemPath, privateKeyPemPath);
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), password: null);
    }

    /// <summary>Publishes a UTF-8 string.</summary>
    public Task PublishAsync(string topic, string payload, int qos = 0, CancellationToken ct = default) =>
        PublishAsync(topic, Encoding.UTF8.GetBytes(payload), qos, ct);

    /// <summary>
    /// Publishes a message. QoS 0 completes once written; QoS 1 completes when the broker's PUBACK arrives.
    /// </summary>
    public async Task PublishAsync(string topic, byte[] payload, int qos = 0, CancellationToken ct = default)
    {
        var body = new List<byte>();
        AddString(body, topic);
        ushort packetId = 0;
        if (qos > 0)
        {
            packetId = NextPacketId();
            AddUInt16(body, packetId);
        }

        body.AddRange(payload);
        byte header = (byte)(0x30 | (qos > 0 ? 0x02 : 0x00));

        if (qos == 0)
        {
            await WriteAsync(Frame(header, body), ct);
            return;
        }

        await SendAndWaitForAckAsync(packetId, Frame(header, body), ct);
    }

    /// <summary>Subscribes to one topic filter. Completes when the broker's SUBACK arrives.</summary>
    /// <exception cref="IOException">The broker refused the subscription.</exception>
    public async Task SubscribeAsync(string topicFilter, int qos = 0, CancellationToken ct = default)
    {
        ushort packetId = NextPacketId();
        var body = new List<byte>();
        AddUInt16(body, packetId);
        AddString(body, topicFilter);
        body.Add((byte)qos);

        byte[] ack = await SendAndWaitForAckAsync(packetId, Frame(0x82, body), ct);
        if (ack.Length < 3 || ack[2] == 0x80)
        {
            throw new IOException($"Broker refused the subscription to '{topicFilter}'.");
        }
    }

    /// <summary>Sends DISCONNECT and closes the connection.</summary>
    public async Task DisconnectAsync()
    {
        if (_stop.IsCancellationRequested)
        {
            return;
        }

        try
        {
            await WriteAsync([0xE0, 0x00], CancellationToken.None);
        }
        catch (IOException)
        {
            // Already gone.
        }

        await CloseAsync();
    }

    public ValueTask DisposeAsync() => new(DisconnectAsync());

    private async Task<byte[]> SendAndWaitForAckAsync(ushort packetId, byte[] frame, CancellationToken ct)
    {
        var ack = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingAcks[packetId] = ack;
        try
        {
            await WriteAsync(frame, ct);
            return await ack.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
        }
        finally
        {
            _pendingAcks.TryRemove(packetId, out _);
        }
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (true)
            {
                (byte header, byte[] body) = await ReadPacketAsync(_stop.Token);
                switch (header >> 4)
                {
                    case 3: // PUBLISH
                        await HandlePublishAsync(header, body);
                        break;
                    case 4: // PUBACK
                    case 9: // SUBACK
                        if (_pendingAcks.TryGetValue((ushort)((body[0] << 8) | body[1]), out var ack))
                        {
                            ack.TrySetResult(body);
                        }

                        break;
                }
            }
        }
        catch (Exception ex) when (!_stop.IsCancellationRequested)
        {
            await CloseAsync();
            Disconnected?.Invoke(ex);
        }
        catch
        {
            // Closed locally.
        }
    }

    private async Task HandlePublishAsync(byte header, byte[] body)
    {
        int qos = (header >> 1) & 0x03;
        int topicLength = (body[0] << 8) | body[1];
        string topic = Encoding.UTF8.GetString(body, 2, topicLength);
        int offset = 2 + topicLength;
        if (qos > 0)
        {
            byte[] pubAck = [0x40, 0x02, body[offset], body[offset + 1]];
            offset += 2;
            await WriteAsync(pubAck, _stop.Token);
        }

        MessageReceived?.Invoke(topic, body[offset..]);
    }

    private async Task PingLoopAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(KeepAliveSeconds / 2));
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token))
            {
                await WriteAsync([0xC0, 0x00], _stop.Token);
            }
        }
        catch
        {
            // Stopped, or the read loop will report the failure.
        }
    }

    private async Task<(byte Header, byte[] Body)> ReadPacketAsync(CancellationToken ct)
    {
        var one = new byte[1];
        await _stream.ReadExactlyAsync(one, ct);
        byte header = one[0];

        int length = 0;
        for (int shift = 0; ; shift += 7)
        {
            await _stream.ReadExactlyAsync(one, ct);
            length |= (one[0] & 0x7F) << shift;
            if ((one[0] & 0x80) == 0)
            {
                break;
            }
        }

        var body = new byte[length];
        await _stream.ReadExactlyAsync(body, ct);
        return (header, body);
    }

    private async Task WriteAsync(byte[] frame, CancellationToken ct)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            await _stream.WriteAsync(frame, ct);
        }
        catch (ObjectDisposedException ex)
        {
            throw new IOException("The connection is closed.", ex);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task CloseAsync()
    {
        if (_stop.IsCancellationRequested)
        {
            return;
        }

        await _stop.CancelAsync();
        await _stream.DisposeAsync();
        foreach (var ack in _pendingAcks.Values)
        {
            ack.TrySetException(new IOException("The connection closed."));
        }
    }

    private ushort NextPacketId()
    {
        // Packet IDs run 1..65535; 0 is not allowed.
        return (ushort)((Interlocked.Increment(ref _nextPacketId) % 65535) + 1);
    }

    private static byte[] Connect(string clientId)
    {
        var body = new List<byte>();
        AddString(body, "MQTT");
        body.Add(4);    // Protocol level 3.1.1
        body.Add(0x02); // Clean session
        AddUInt16(body, KeepAliveSeconds);
        AddString(body, clientId);
        return Frame(0x10, body);
    }

    private static byte[] Frame(byte header, List<byte> body)
    {
        var frame = new List<byte>(body.Count + 5) { header };
        int length = body.Count;
        do
        {
            byte digit = (byte)(length % 128);
            length /= 128;
            frame.Add(length > 0 ? (byte)(digit | 0x80) : digit);
        }
        while (length > 0);

        frame.AddRange(body);
        return [.. frame];
    }

    private static void AddString(List<byte> body, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        AddUInt16(body, bytes.Length);
        body.AddRange(bytes);
    }

    private static void AddUInt16(List<byte> body, int value)
    {
        body.Add((byte)(value >> 8));
        body.Add((byte)value);
    }
}
