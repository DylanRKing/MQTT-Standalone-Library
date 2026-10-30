using System.Collections.Concurrent;
using System.Text;
using Mqtt.Simple.Codec;
using Mqtt.Simple.Transport;

namespace Mqtt.Simple;

/// <summary>
/// A minimal MQTT 3.1.1 client for AWS IoT Core: connect over TLS with a client certificate,
/// publish, subscribe and disconnect. QoS 0 and 1 only. No automatic reconnect, no offline
/// queue and almost no validation. After a disconnect, call <see cref="ConnectAsync"/> again
/// and re-subscribe.
/// </summary>
public sealed class SimpleMqttClient(SimpleMqttClientOptions options) : IAsyncDisposable
{
    private readonly MqttCodec _codec = new();
    private readonly ConcurrentDictionary<ushort, TaskCompletionSource<Packet>> _pendingAcks = new();
    private readonly Lock _gate = new();
    private Connection? _connection;
    private int _nextPacketId;

    /// <summary>Raised from the read loop for each message received: topic, then payload.</summary>
    public event Action<string, byte[]>? MessageReceived;

    /// <summary>Raised once per connection if it drops, with the reason. Not raised by <see cref="DisconnectAsync"/>.</summary>
    public event Action<Exception>? Disconnected;

    /// <summary>True between a successful <see cref="ConnectAsync"/> and the connection closing.</summary>
    public bool IsConnected => Volatile.Read(ref _connection) is { IsOpen: true };

    /// <summary>
    /// Opens a TLS connection and sends CONNECT. Completes when AWS accepts it.
    /// </summary>
    /// <exception cref="InvalidOperationException">Already connected or connecting.</exception>
    /// <exception cref="IOException">AWS refused or closed the connection.</exception>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        var transport = new MqttTransport(options.Host, options.Port, options.Certificate, options.RootCa);
        var connection = new Connection(transport);
        lock (_gate)
        {
            if (_connection is { Stop.IsCancellationRequested: false })
            {
                throw new InvalidOperationException("The client is already connected or connecting.");
            }

            _connection = connection;
        }

        try
        {
            await transport.ConnectAsync(ct);
            await transport.WriteAsync(_codec.Connect(options.ClientId, (int)options.KeepAlive.TotalSeconds), ct);
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
                    $"attached, or the policy does not allow iot:Connect for client ID '{options.ClientId}'.",
                    ex);
            }

            int code = _codec.ConnAckReturnCode(connAck);
            if (code != 0)
            {
                throw new IOException($"AWS refused the connection, CONNACK code {code}.");
            }

            connection.Accepted = true;
        }
        catch
        {
            await CloseAsync(connection);
            throw;
        }

        _ = Task.Run(() => ReadLoopAsync(connection));
        _ = Task.Run(() => PingLoopAsync(connection));
    }

    /// <summary>Publishes a UTF-8 string.</summary>
    public Task PublishAsync(string topic, string payload, int qos = 0, CancellationToken ct = default) =>
        PublishAsync(topic, Encoding.UTF8.GetBytes(payload), qos, ct);

    /// <summary>
    /// Publishes a message. QoS 0 completes once written; QoS 1 completes when the PUBACK arrives.
    /// </summary>
    /// <exception cref="IOException">Not connected, or the connection closed before the PUBACK.</exception>
    public async Task PublishAsync(string topic, byte[] payload, int qos = 0, CancellationToken ct = default)
    {
        if (qos == 0)
        {
            await CurrentTransport().WriteAsync(_codec.Publish(topic, payload, 0, 0), ct);
            return;
        }

        ushort packetId = NextPacketId();
        await SendAndWaitForAckAsync(packetId, _codec.Publish(topic, payload, qos, packetId), ct);
    }

    /// <summary>Subscribes to one topic filter. Completes when the SUBACK arrives.</summary>
    /// <exception cref="IOException">Not connected, or AWS refused the subscription.</exception>
    public async Task SubscribeAsync(string topicFilter, int qos = 0, CancellationToken ct = default)
    {
        ushort packetId = NextPacketId();
        Packet subAck = await SendAndWaitForAckAsync(packetId, _codec.Subscribe(topicFilter, qos, packetId), ct);
        if (_codec.SubAckReturnCode(subAck) == 0x80)
        {
            throw new IOException($"AWS refused the subscription to '{topicFilter}'.");
        }
    }

    /// <summary>Sends DISCONNECT and closes the connection. Does nothing if not connected.</summary>
    public async Task DisconnectAsync()
    {
        Connection? connection = Volatile.Read(ref _connection);
        if (connection is null || connection.Stop.IsCancellationRequested)
        {
            return;
        }

        try
        {
            await connection.Transport.WriteAsync(_codec.Disconnect(), CancellationToken.None);
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException)
        {
            // Already gone, or still mid-handshake.
        }

        await CloseAsync(connection);
    }

    public ValueTask DisposeAsync() => new(DisconnectAsync());

    private MqttTransport CurrentTransport() =>
        Volatile.Read(ref _connection) is { IsOpen: true } connection
            ? connection.Transport
            : throw new IOException("The client is not connected.");

    private async Task<Packet> SendAndWaitForAckAsync(ushort packetId, byte[] frame, CancellationToken ct)
    {
        MqttTransport transport = CurrentTransport();
        var ack = new TaskCompletionSource<Packet>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingAcks[packetId] = ack;
        try
        {
            await transport.WriteAsync(frame, ct);
            return await ack.Task.WaitAsync(options.AckTimeout, ct);
        }
        finally
        {
            _pendingAcks.TryRemove(packetId, out _);
        }
    }

    private async Task ReadLoopAsync(Connection connection)
    {
        CancellationToken ct = connection.Stop.Token;
        try
        {
            while (true)
            {
                Packet packet = await connection.Transport.ReadAsync(ct);
                switch (packet.Type)
                {
                    case PacketType.Publish:
                        IncomingPublish publish = _codec.ReadPublish(packet);
                        if (publish.Qos > 0)
                        {
                            await connection.Transport.WriteAsync(_codec.PubAck(publish.PacketId), ct);
                        }

                        MessageReceived?.Invoke(publish.Topic, publish.Payload);
                        break;
                    case PacketType.PubAck:
                    case PacketType.SubAck:
                        if (_pendingAcks.TryGetValue(_codec.PacketId(packet), out var ack))
                        {
                            ack.TrySetResult(packet);
                        }

                        break;
                }
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            await CloseAsync(connection);
            Disconnected?.Invoke(ex);
        }
        catch
        {
            // Closed locally.
        }
    }

    private async Task PingLoopAsync(Connection connection)
    {
        using var timer = new PeriodicTimer(options.KeepAlive / 2);
        try
        {
            while (await timer.WaitForNextTickAsync(connection.Stop.Token))
            {
                await connection.Transport.WriteAsync(_codec.PingReq(), connection.Stop.Token);
            }
        }
        catch
        {
            // Stopped, or the read loop will report the failure.
        }
    }

    private async Task CloseAsync(Connection connection)
    {
        lock (_gate)
        {
            if (connection.Stop.IsCancellationRequested)
            {
                return;
            }

            connection.Stop.Cancel();
        }

        await connection.Transport.DisposeAsync();
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

    /// <summary>One connection's transport and the token that stops its loops.</summary>
    private sealed class Connection(MqttTransport transport)
    {
        public MqttTransport Transport { get; } = transport;

        public CancellationTokenSource Stop { get; } = new();

        /// <summary>Set once AWS has sent a successful CONNACK.</summary>
        public volatile bool Accepted;

        public bool IsOpen => Accepted && !Stop.IsCancellationRequested;
    }
}
