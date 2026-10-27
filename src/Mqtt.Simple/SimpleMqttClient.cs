using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Mqtt.Simple.Codec;
using Mqtt.Simple.Transport;

namespace Mqtt.Simple;

/// <summary>
/// A minimal MQTT 3.1.1 client for AWS IoT Core: connect over TLS with a client certificate,
/// publish, subscribe and disconnect. QoS 0 and 1 only. No reconnect, no offline queue and
/// almost no validation; if the connection drops, create a new client.
/// </summary>
public sealed class SimpleMqttClient : IAsyncDisposable
{
    private const int KeepAliveSeconds = 60;
    private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(30);

    private readonly MqttTransport _transport;
    private readonly ConcurrentDictionary<ushort, TaskCompletionSource<Packet>> _pendingAcks = new();
    private readonly CancellationTokenSource _stop = new();
    private Task _readLoop = Task.CompletedTask;
    private Task _pingLoop = Task.CompletedTask;
    private int _nextPacketId;

    private SimpleMqttClient(MqttTransport transport) => _transport = transport;

    /// <summary>Raised from the read loop for each message received: topic, then payload.</summary>
    public event Action<string, byte[]>? MessageReceived;

    /// <summary>Raised once if the connection drops, with the reason.</summary>
    public event Action<Exception>? Disconnected;

    /// <summary>
    /// Opens a TLS connection and sends CONNECT. Completes when AWS accepts it.
    /// </summary>
    /// <param name="host">AWS IoT endpoint, e.g. <c>abc123-ats.iot.eu-west-1.amazonaws.com</c>.</param>
    /// <param name="clientId">Client ID; for AWS normally the thing name.</param>
    /// <param name="certificate">Device certificate with its private key. See <see cref="LoadCertificate"/>.</param>
    /// <param name="rootCa">
    /// Amazon's root CA (<c>AmazonRootCA1.pem</c>, see <see cref="LoadRootCa"/>). Optional when the
    /// machine already trusts it, which most do; pass it on machines that don't.
    /// </param>
    /// <param name="port">8883 for MQTT over TLS.</param>
    public static async Task<SimpleMqttClient> ConnectAsync(
        string host,
        string clientId,
        X509Certificate2 certificate,
        X509Certificate2? rootCa = null,
        int port = 8883,
        CancellationToken ct = default)
    {
        MqttTransport transport = await MqttTransport.ConnectAsync(host, port, certificate, rootCa, ct);
        try
        {
            await transport.WriteAsync(MqttCodec.Connect(clientId, KeepAliveSeconds), ct);
            Packet connAck;
            try
            {
                connAck = await transport.ReadAsync(ct);
            }
            catch (EndOfStreamException ex)
            {
                // AWS closes the connection instead of sending a CONNACK error.
                throw new IOException(
                    "AWS closed the connection after CONNECT. Usually the certificate is not active, has no policy " +
                    $"attached, or the policy does not allow iot:Connect for client ID '{clientId}'.",
                    ex);
            }

            int code = MqttCodec.ConnAckReturnCode(connAck);
            if (code != 0)
            {
                throw new IOException($"AWS refused the connection, CONNACK code {code}.");
            }
        }
        catch
        {
            await transport.DisposeAsync();
            throw;
        }

        var client = new SimpleMqttClient(transport);
        client._readLoop = Task.Run(client.ReadLoopAsync);
        client._pingLoop = Task.Run(client.PingLoopAsync);
        return client;
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

    /// <summary>Loads a CA certificate from a PEM file, such as <c>AmazonRootCA1.pem</c>.</summary>
    public static X509Certificate2 LoadRootCa(string pemPath) =>
        X509Certificate2.CreateFromPem(File.ReadAllText(pemPath));

    /// <summary>Publishes a UTF-8 string.</summary>
    public Task PublishAsync(string topic, string payload, int qos = 0, CancellationToken ct = default) =>
        PublishAsync(topic, Encoding.UTF8.GetBytes(payload), qos, ct);

    /// <summary>
    /// Publishes a message. QoS 0 completes once written; QoS 1 completes when the PUBACK arrives.
    /// </summary>
    public async Task PublishAsync(string topic, byte[] payload, int qos = 0, CancellationToken ct = default)
    {
        if (qos == 0)
        {
            await _transport.WriteAsync(MqttCodec.Publish(topic, payload, 0, 0), ct);
            return;
        }

        ushort packetId = NextPacketId();
        await SendAndWaitForAckAsync(packetId, MqttCodec.Publish(topic, payload, qos, packetId), ct);
    }

    /// <summary>Subscribes to one topic filter. Completes when the SUBACK arrives.</summary>
    /// <exception cref="IOException">AWS refused the subscription.</exception>
    public async Task SubscribeAsync(string topicFilter, int qos = 0, CancellationToken ct = default)
    {
        ushort packetId = NextPacketId();
        Packet subAck = await SendAndWaitForAckAsync(packetId, MqttCodec.Subscribe(topicFilter, qos, packetId), ct);
        if (MqttCodec.SubAckReturnCode(subAck) == 0x80)
        {
            throw new IOException($"AWS refused the subscription to '{topicFilter}'.");
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
            await _transport.WriteAsync(MqttCodec.Disconnect(), CancellationToken.None);
        }
        catch (IOException)
        {
            // Already gone.
        }

        await CloseAsync();
    }

    public ValueTask DisposeAsync() => new(DisconnectAsync());

    private async Task<Packet> SendAndWaitForAckAsync(ushort packetId, byte[] frame, CancellationToken ct)
    {
        var ack = new TaskCompletionSource<Packet>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingAcks[packetId] = ack;
        try
        {
            await _transport.WriteAsync(frame, ct);
            return await ack.Task.WaitAsync(AckTimeout, ct);
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
                Packet packet = await _transport.ReadAsync(_stop.Token);
                switch (packet.Type)
                {
                    case PacketType.Publish:
                        IncomingPublish publish = MqttCodec.ReadPublish(packet);
                        if (publish.Qos > 0)
                        {
                            await _transport.WriteAsync(MqttCodec.PubAck(publish.PacketId), _stop.Token);
                        }

                        MessageReceived?.Invoke(publish.Topic, publish.Payload);
                        break;
                    case PacketType.PubAck:
                    case PacketType.SubAck:
                        if (_pendingAcks.TryGetValue(MqttCodec.PacketId(packet), out var ack))
                        {
                            ack.TrySetResult(packet);
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

    private async Task PingLoopAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(KeepAliveSeconds / 2));
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token))
            {
                await _transport.WriteAsync(MqttCodec.PingReq(), _stop.Token);
            }
        }
        catch
        {
            // Stopped, or the read loop will report the failure.
        }
    }

    private async Task CloseAsync()
    {
        if (_stop.IsCancellationRequested)
        {
            return;
        }

        await _stop.CancelAsync();
        await _transport.DisposeAsync();
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
}
