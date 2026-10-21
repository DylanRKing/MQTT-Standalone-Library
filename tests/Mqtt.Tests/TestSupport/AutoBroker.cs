using System.Collections.Concurrent;
using System.Net.Sockets;
using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Tests.TestSupport;

/// <summary>
/// Self-running fake broker for many devices: accepts every CONNECT, grants every SUBSCRIBE,
/// acknowledges every QoS 1 PUBLISH, answers PINGREQ and routes publishes to subscribers.
/// Records what it receives and can drop every connection at once to simulate a network outage.
/// </summary>
internal sealed class AutoBroker : IAsyncDisposable
{
    private readonly TlsTestServer _server = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<Session, byte> _sessions = new();
    private readonly Task _acceptLoop;
    private int _pendingConnects;
    private int _maxPendingConnects;
    private int _connects;
    private long _receivedCount;

    public AutoBroker() => _acceptLoop = Task.Run(AcceptLoopAsync);

    /// <summary>Delay before each CONNACK, to make connect concurrency observable.</summary>
    public TimeSpan ConnAckDelay { get; set; }

    /// <summary>Most connections seen between TCP accept and CONNACK at the same time.</summary>
    public int MaxPendingConnects => Volatile.Read(ref _maxPendingConnects);

    /// <summary>Successful CONNECTs so far.</summary>
    public int Connects => Volatile.Read(ref _connects);

    public int OpenSessions => _sessions.Count;

    /// <summary>Whether to fill <see cref="Received"/>; off for long soaks, where it would grow without bound.</summary>
    public bool RecordReceived { get; set; } = true;

    /// <summary>Publishes received, in total.</summary>
    public long ReceivedCount => Interlocked.Read(ref _receivedCount);

    /// <summary>Count of each received publish, keyed by "topic|payload".</summary>
    public ConcurrentDictionary<string, int> Received { get; } = new();

    public MqttClientOptions Options(string clientId) => new()
    {
        Host = "localhost",
        Port = _server.Port,
        ClientId = clientId,
        CertificateProvider = new TestCertificateProvider(TestCertificates.Client),
        ServerCertificateValidator = (_, certificate, _, _) =>
            certificate?.GetCertHashString() == TestCertificates.Server.Thumbprint,
        ConnectTimeout = TimeSpan.FromSeconds(15),
        ReconnectDelayFloor = TimeSpan.FromMilliseconds(10),
        Reconnect = new ReconnectOptions { InitialDelay = TimeSpan.FromMilliseconds(10), MaxDelay = TimeSpan.FromMilliseconds(200) },
    };

    /// <summary>Closes every open connection, as a network drop would.</summary>
    public async Task DropAllAsync()
    {
        foreach (Session session in _sessions.Keys)
        {
            await session.DisposeAsync();
        }
    }

    /// <summary>Publishes to every session whose subscriptions match <paramref name="topic"/>.</summary>
    public async Task<int> PublishAsync(string topic, byte[] payload, MqttQos qos)
    {
        int delivered = 0;
        foreach (Session session in _sessions.Keys)
        {
            if (session.Matches(topic))
            {
                await session.SendPublishAsync(topic, payload, qos);
                delivered++;
            }
        }

        return delivered;
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _server.Dispose();
        await DropAllAsync();
        await Record.ExceptionAsync(() => _acceptLoop);
    }

    private async Task AcceptLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            Socket socket;
            try
            {
                socket = await AcceptSocketAsync();
            }
            catch
            {
                return;
            }

            _ = Task.Run(() => RunSessionAsync(socket));
        }
    }

    private Task<Socket> AcceptSocketAsync() => _server.AcceptRawAsync(_stop.Token);

    private async Task RunSessionAsync(Socket socket)
    {
        int pending = Interlocked.Increment(ref _pendingConnects);
        int max;
        while (pending > (max = Volatile.Read(ref _maxPendingConnects)) && Interlocked.CompareExchange(ref _maxPendingConnects, pending, max) != max)
        {
        }

        bool connected = false;
        Session? session = null;
        try
        {
            ServerConnection connection = await TlsTestServer.AuthenticateAsync(socket, _stop.Token);
            session = new Session(connection);
            while (await connection.ReadFrameAsync(_stop.Token) is { } frame)
            {
                if (MqttDecoder.TryDecode(frame, ProtocolVersion.V311, out IMqttPacket? packet) != DecodeError.None)
                {
                    break;
                }

                switch (packet)
                {
                    case ConnectPacket:
                        if (ConnAckDelay > TimeSpan.Zero)
                        {
                            await Task.Delay(ConnAckDelay);
                        }

                        // Counted as done before CONNACK goes out, so a throttled client let in by
                        // this CONNACK is never counted alongside it.
                        Interlocked.Decrement(ref _pendingConnects);
                        connected = true;
                        await session.SendAsync(new ConnAckPacket(false, ReasonCode.Success));
                        Interlocked.Increment(ref _connects);
                        _sessions[session] = 0;
                        break;
                    case SubscribePacket subscribe:
                        session.Subscribe(subscribe.Filters);
                        await session.SendAsync(new SubAckPacket(subscribe.PacketId, [.. subscribe.Filters.Select(f => (ReasonCode)f.Qos)]));
                        break;
                    case PublishPacket publish:
                        Interlocked.Increment(ref _receivedCount);
                        if (RecordReceived)
                        {
                            Received.AddOrUpdate($"{publish.Topic}|{System.Text.Encoding.UTF8.GetString(publish.Payload.Span)}", 1, (_, n) => n + 1);
                        }

                        if (publish.Qos == MqttQos.AtLeastOnce)
                        {
                            await session.SendAsync(new PubAckPacket(publish.PacketId));
                        }

                        break;
                    case PingReqPacket:
                        await session.SendAsync(PingRespPacket.Instance);
                        break;
                    case DisconnectPacket:
                        return;
                }
            }
        }
        catch
        {
            // Dropped, or the client went away mid-handshake.
        }
        finally
        {
            if (!connected)
            {
                Interlocked.Decrement(ref _pendingConnects);
            }

            if (session is not null)
            {
                _sessions.TryRemove(session, out _);
                await session.DisposeAsync();
            }
            else
            {
                socket.Dispose();
            }
        }
    }

    private sealed class Session(ServerConnection connection) : IAsyncDisposable
    {
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly ConcurrentDictionary<string, MqttQos> _filters = new();
        private int _nextPacketId;

        public void Subscribe(IEnumerable<TopicFilter> filters)
        {
            foreach (TopicFilter filter in filters)
            {
                _filters[filter.Filter] = filter.Qos;
            }
        }

        public bool Matches(string topic) => _filters.Keys.Any(filter => TopicMatches(filter, topic));

        public Task SendPublishAsync(string topic, byte[] payload, MqttQos qos)
        {
            ushort id = qos == MqttQos.AtMostOnce ? (ushort)0 : (ushort)((Interlocked.Increment(ref _nextPacketId) % 65_535) + 1);
            return SendAsync(new PublishPacket(topic, payload, qos, PacketId: id));
        }

        public async Task SendAsync(IMqttPacket packet)
        {
            var buffer = new byte[MqttEncoder.GetSize(packet, ProtocolVersion.V311)];
            MqttEncoder.Write(buffer, packet, ProtocolVersion.V311);
            await _writeLock.WaitAsync();
            try
            {
                await connection.WriteAsync(buffer);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async ValueTask DisposeAsync() => await Record.ExceptionAsync(async () => await connection.DisposeAsync());

        private static bool TopicMatches(string filter, string topic)
        {
            string[] f = filter.Split('/');
            string[] t = topic.Split('/');
            for (int i = 0; i < f.Length; i++)
            {
                if (f[i] == "#")
                {
                    return true;
                }

                if (i >= t.Length || (f[i] != "+" && f[i] != t[i]))
                {
                    return false;
                }
            }

            return f.Length == t.Length;
        }
    }
}
