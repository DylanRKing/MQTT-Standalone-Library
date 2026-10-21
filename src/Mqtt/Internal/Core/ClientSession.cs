using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Internal.Transport;

namespace Mqtt.Internal.Core;

/// <summary>A SUBSCRIBE batch waiting for its SUBACK.</summary>
internal sealed class PendingSubscribe(SubscribePacket packet, long sentTimestamp)
{
    public SubscribePacket Packet { get; } = packet;

    public long SentTimestamp { get; } = sentTimestamp;

    public TaskCompletionSource<SubAckPacket> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>
/// The per-device protocol engine behind <see cref="MqttClient"/>. Owns connection state, packet
/// IDs, in-flight QoS 1 publishes, pending SUBSCRIBEs, keep-alive and inbound dispatch, and drives
/// the transport and codec. All protocol rules live here.
/// </summary>
internal sealed class ClientSession : IAsyncDisposable
{
    private static readonly TimeSpan DisconnectSendTimeout = TimeSpan.FromSeconds(5);

    private readonly MqttClientOptions _options;
    private readonly TransportOptions _transportOptions;
    private readonly SafeLogger _log;
    private readonly TimeProvider _time;

    // Connect, disconnect and connection-loss handling never run at the same time.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private readonly SemaphoreSlim _inFlightSlots;
    private readonly PacketIdAllocator _packetIds = new();
    private readonly InFlightStore _inFlight = new();
    private readonly Dictionary<ushort, PendingSubscribe> _pendingSubscribes = [];
    private readonly Dictionary<string, MqttQos> _grantedSubscriptions = [];
    private readonly Lock _subscribeLock = new();
    private readonly InboundDispatcher _dispatcher;

    private volatile Connection? _current;
    private int _state = (int)MqttConnectionState.Disconnected;
    private int _disposed;

    public ClientSession(MqttClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _time = options.TimeProvider;
        _log = new SafeLogger(options.Logger, options.ClientId);
        _inFlightSlots = new SemaphoreSlim(options.MaxInFlight, options.MaxInFlight);
        _transportOptions = new TransportOptions
        {
            Host = options.Host,
            Port = options.Port,
            CertificateProvider = options.CertificateProvider,
            ClientId = options.ClientId,
            Logger = options.Logger,
            ServerCertificateValidator = options.ServerCertificateValidator,
        };
        _dispatcher = new InboundDispatcher(HandleInboundAsync, HandleStateChangeAsync, _log);
    }

    public event Func<MqttMessage, Task>? MessageReceived;

    public event Func<ConnectionStateChange, Task>? ConnectionStateChanged;

    public string ClientId => _options.ClientId;

    public MqttConnectionState State => (MqttConnectionState)Volatile.Read(ref _state);

    /// <summary>Filters the broker has granted, kept to restore subscriptions after a clean-session reconnect.</summary>
    internal IReadOnlyDictionary<string, MqttQos> GrantedSubscriptions
    {
        get
        {
            lock (_subscribeLock)
            {
                return new Dictionary<string, MqttQos>(_grantedSubscriptions);
            }
        }
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        ThrowIfDisposed();
        await _lifecycle.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (_current is { IsOpen: true })
            {
                return;
            }

            SetState(MqttConnectionState.Connecting, null);
            var connection = new Connection(_time);
            try
            {
                await OpenAsync(connection, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                connection.Lifetime.Cancel();
                if (connection.Transport is { } transport)
                {
                    await transport.CloseAsync().ConfigureAwait(false);
                }

                SetState(MqttConnectionState.Disconnected, ex is OperationCanceledException ? null : ex);
                throw;
            }

            _current = connection;
            var monitor = new KeepAliveMonitor(
                connection,
                _options.EffectiveKeepAlive,
                _options.AckTimeout,
                _time,
                OldestPendingAckTimestamp,
                (c, reason) => _ = HandleConnectionLostAsync(c, reason),
                _log);
            _ = monitor.RunAsync();
            SetState(MqttConnectionState.Connected, null);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async Task<PublishResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, MqttQos qos, bool retain, CancellationToken ct)
    {
        AwsLimits.ValidateTopic(topic, nameof(topic));
        AwsLimits.ValidatePayload(payload, nameof(payload));
        AwsLimits.ValidateQos(qos, nameof(qos));
        ThrowIfDisposed();

        Connection? connection = _current;
        if (connection is not { IsOpen: true })
        {
            if (qos == MqttQos.AtMostOnce)
            {
                return PublishResult.DroppedOffline;
            }

            // The offline queue (build step 4) will hold these while reconnecting.
            throw new InvalidOperationException("The client is not connected. Call ConnectAsync first.");
        }

        if (qos == MqttQos.AtMostOnce)
        {
            try
            {
                await connection.SendAsync(new PublishPacket(topic, payload, MqttQos.AtMostOnce, retain), ct).ConfigureAwait(false);
                return PublishResult.Sent;
            }
            catch (MqttConnectionLostException)
            {
                return PublishResult.DroppedOffline;
            }
        }

        // Past MaxInFlight, callers wait here: natural backpressure.
        await _inFlightSlots.WaitAsync(ct).ConfigureAwait(false);
        InFlightPublish entry;
        try
        {
            ushort packetId = _packetIds.Allocate();
            entry = new InFlightPublish(new PublishPacket(topic, payload, MqttQos.AtLeastOnce, retain, Dup: false, packetId), _time.GetTimestamp());
            _inFlight.Add(entry);
        }
        catch
        {
            _inFlightSlots.Release();
            throw;
        }

        // Added before sending so a fast PUBACK finds it. If the connection was torn down in the
        // meantime, teardown may already have swept the store, so fail the entry here.
        if (_current != connection || !connection.IsOpen)
        {
            FailInFlight(entry.Packet.PacketId, new MqttConnectionLostException("The connection closed before the message was sent."));
        }
        else
        {
            try
            {
                // Not cancellable: once queued the frame goes out, and only the wait below honours ct.
                await connection.SendAsync(entry.Packet, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                FailInFlight(entry.Packet.PacketId, ex);
            }
        }

        return await entry.Completion.Task.WaitAsync(ct).ConfigureAwait(false);
    }

    public async Task<SubscribeResult> SubscribeAsync(IReadOnlyList<TopicFilter> filters, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(filters);
        if (filters.Count == 0)
        {
            throw new ArgumentException("At least one topic filter is required.", nameof(filters));
        }

        foreach (TopicFilter filter in filters)
        {
            AwsLimits.ValidateFilter(filter, nameof(filters));
        }

        ThrowIfDisposed();
        Connection connection = _current is { IsOpen: true } current
            ? current
            : throw new InvalidOperationException("The client is not connected. Call ConnectAsync first.");

        // AWS accepts at most 8 filters per SUBSCRIBE.
        var pending = new List<PendingSubscribe>();
        foreach (TopicFilter[] batch in filters.Chunk(AwsLimits.MaxFiltersPerSubscribe))
        {
            var entry = new PendingSubscribe(new SubscribePacket(_packetIds.Allocate(), batch), _time.GetTimestamp());
            lock (_subscribeLock)
            {
                _pendingSubscribes.Add(entry.Packet.PacketId, entry);
            }

            pending.Add(entry);
            if (_current != connection || !connection.IsOpen)
            {
                FailSubscribe(entry.Packet.PacketId, new MqttConnectionLostException("The connection closed before SUBSCRIBE was sent."));
                continue;
            }

            try
            {
                await connection.SendAsync(entry.Packet, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                FailSubscribe(entry.Packet.PacketId, ex);
            }
        }

        SubAckPacket[] acks = await Task.WhenAll(pending.Select(p => p.Completion.Task)).WaitAsync(ct).ConfigureAwait(false);

        var results = new List<SubscriptionResult>(filters.Count);
        lock (_subscribeLock)
        {
            for (int b = 0; b < pending.Count; b++)
            {
                IReadOnlyList<TopicFilter> requested = pending[b].Packet.Filters;
                for (int i = 0; i < requested.Count; i++)
                {
                    var result = new SubscriptionResult(requested[i], acks[b].ReasonCodes[i]);
                    results.Add(result);
                    if (result.Succeeded)
                    {
                        _grantedSubscriptions[requested[i].Filter] = requested[i].Qos;
                    }
                    else
                    {
                        _log.Log(MqttLogLevel.Warning, $"Subscription to '{requested[i].Filter}' refused ({result.ReasonCode}).");
                    }
                }
            }
        }

        return new SubscribeResult(results);
    }

    public async Task DisconnectAsync(CancellationToken ct)
    {
        ThrowIfDisposed();
        await _lifecycle.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await DisconnectCoreAsync().ConfigureAwait(false);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            await DisconnectCoreAsync().ConfigureAwait(false);
            SetState(MqttConnectionState.Disposed, null);
        }
        finally
        {
            _lifecycle.Release();
        }

        // Not awaited: DisposeAsync may be called from inside a handler.
        _dispatcher.Complete();
    }

    private async Task OpenAsync(Connection connection, CancellationToken ct)
    {
        using var timeout = new CancellationTokenSource(_options.ConnectTimeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
        ConnAckPacket connAck;
        try
        {
            connection.Transport = await MqttTransport.ConnectAsync(
                _transportOptions,
                frame => OnFrameAsync(connection, frame),
                reason => _ = HandleConnectionLostAsync(connection, reason),
                linked.Token).ConfigureAwait(false);

            var connect = new ConnectPacket(_options.ClientId, _options.CleanSession, (ushort)_options.EffectiveKeepAlive.TotalSeconds);
            await connection.SendAsync(connect, linked.Token).ConfigureAwait(false);
            connAck = await connection.ConnAck.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new MqttConnectException($"Connecting to {_options.Host}:{_options.Port} timed out after {_options.ConnectTimeout.TotalSeconds:0.#} s.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new MqttConnectException($"Connecting to {_options.Host}:{_options.Port} failed: {ex.Message}", ex);
        }

        if (connAck.ReasonCode != ReasonCode.Success)
        {
            _log.Log(MqttLogLevel.Error, $"Broker refused the connection: {connAck.ReasonCode}.");
            throw new MqttConnectException($"The broker refused the connection: {connAck.ReasonCode}.", connAck.ReasonCode);
        }

        _log.Log(MqttLogLevel.Information, $"MQTT session established (clean session: {_options.CleanSession}, session present: {connAck.SessionPresent}).");
    }

    /// <summary>Called by the read loop for every frame. Never awaits application code.</summary>
    private ValueTask OnFrameAsync(Connection connection, ReadOnlyMemory<byte> frame)
    {
        // Any packet from the broker answers an outstanding PINGREQ.
        connection.PingSentTimestamp = 0;

        DecodeError error = MqttDecoder.TryDecode(frame.Span, ProtocolVersion.V311, out IMqttPacket? packet);
        if (error != DecodeError.None)
        {
            throw new InvalidDataException($"Received a packet the client cannot accept ({error}).");
        }

        if (!connection.ConnAck.Task.IsCompleted)
        {
            if (packet is ConnAckPacket connAck)
            {
                connection.ConnAck.TrySetResult(connAck);
                return default;
            }

            throw new InvalidDataException($"Expected CONNACK as the first packet, received {packet!.Type}.");
        }

        switch (packet)
        {
            case PublishPacket publish:
                return _dispatcher.EnqueueAsync(new InboundPublish(publish, connection), connection.Lifetime.Token);
            case PubAckPacket pubAck:
                if (!_inFlight.TryRemove(pubAck.PacketId, out InFlightPublish? entry))
                {
                    _log.Log(MqttLogLevel.Debug, $"PUBACK for unknown packet ID {pubAck.PacketId} ignored.");
                    break;
                }

                _packetIds.Release(pubAck.PacketId);
                _inFlightSlots.Release();
                entry!.Completion.TrySetResult(PublishResult.Acknowledged);
                break;
            case SubAckPacket subAck:
                OnSubAck(subAck);
                break;
            case PingRespPacket:
                break;
            default:
                throw new InvalidDataException($"Unexpected {packet!.Type} from the broker.");
        }

        return default;
    }

    private void OnSubAck(SubAckPacket subAck)
    {
        PendingSubscribe? entry;
        lock (_subscribeLock)
        {
            _pendingSubscribes.Remove(subAck.PacketId, out entry);
        }

        if (entry is null)
        {
            _log.Log(MqttLogLevel.Debug, $"SUBACK for unknown packet ID {subAck.PacketId} ignored.");
            return;
        }

        _packetIds.Release(subAck.PacketId);
        if (subAck.ReasonCodes.Count != entry.Packet.Filters.Count)
        {
            var error = new InvalidDataException($"SUBACK has {subAck.ReasonCodes.Count} codes for {entry.Packet.Filters.Count} filters.");
            entry.Completion.TrySetException(error);
            throw error;
        }

        entry.Completion.TrySetResult(subAck);
    }

    private async Task HandleInboundAsync(InboundPublish item)
    {
        PublishPacket packet = item.Packet;
        if (Volatile.Read(ref _disposed) == 0)
        {
            var message = new MqttMessage(packet.Topic, packet.Payload, packet.Qos, packet.Retain);
            await InboundDispatcher.InvokeAsync(MessageReceived, message, _log, nameof(MessageReceived)).ConfigureAwait(false);
        }

        // PUBACK only after the handler: a crash before handling means AWS redelivers. Sent even
        // if the handler threw, so one bad message cannot loop forever.
        if (packet.Qos == MqttQos.AtLeastOnce && item.Connection.IsOpen)
        {
            try
            {
                await item.Connection.SendAsync(new PubAckPacket(packet.PacketId)).ConfigureAwait(false);
            }
            catch (MqttConnectionLostException)
            {
                // AWS redelivers on the next connection.
            }
        }
    }

    private Task HandleStateChangeAsync(ConnectionStateChange change) =>
        InboundDispatcher.InvokeAsync(ConnectionStateChanged, change, _log, nameof(ConnectionStateChanged));

    /// <summary>Transport closed, keep-alive expired or an ack is overdue.</summary>
    private async Task HandleConnectionLostAsync(Connection connection, Exception reason)
    {
        // Wakes a ConnectAsync still waiting for CONNACK, which then cleans up itself.
        connection.ConnAck.TrySetException(reason);

        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_current != connection)
            {
                return;
            }

            _log.Log(MqttLogLevel.Warning, $"Connection lost: {reason.Message}");

            // Reconnect (build step 4) replaces this.
            await TearDownAsync(connection, reason).ConfigureAwait(false);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private async Task DisconnectCoreAsync()
    {
        Connection? connection = _current;
        if (connection is null)
        {
            return;
        }

        if (connection.IsOpen)
        {
            try
            {
                await connection.SendAsync(DisconnectPacket.Instance).WaitAsync(DisconnectSendTimeout, _time).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.Log(MqttLogLevel.Debug, "DISCONNECT not sent; closing anyway.", ex);
            }
        }

        await TearDownAsync(connection, reason: null).ConfigureAwait(false);
        _log.Log(MqttLogLevel.Information, "Disconnected.");
    }

    /// <summary>Closes a connection and fails everything waiting on it. Caller holds the lifecycle lock.</summary>
    private async Task TearDownAsync(Connection connection, Exception? reason)
    {
        _current = null;
        connection.Lifetime.Cancel();
        if (connection.Transport is { } transport)
        {
            await transport.CloseAsync().ConfigureAwait(false);
        }

        var lost = new MqttConnectionLostException(
            reason is null ? "Disconnected before the broker acknowledged." : "The connection was lost before the broker acknowledged.",
            reason);
        foreach (InFlightPublish entry in _inFlight.RemoveAll())
        {
            ReleaseInFlight(entry, lost);
        }

        List<PendingSubscribe> subscribes;
        lock (_subscribeLock)
        {
            subscribes = [.. _pendingSubscribes.Values];
            _pendingSubscribes.Clear();
        }

        foreach (PendingSubscribe entry in subscribes)
        {
            _packetIds.Release(entry.Packet.PacketId);
            entry.Completion.TrySetException(lost);
        }

        SetState(MqttConnectionState.Disconnected, reason);
    }

    private void FailInFlight(ushort packetId, Exception exception)
    {
        if (_inFlight.TryRemove(packetId, out InFlightPublish? entry))
        {
            ReleaseInFlight(entry!, exception);
        }
    }

    private void ReleaseInFlight(InFlightPublish entry, Exception exception)
    {
        _packetIds.Release(entry.Packet.PacketId);
        _inFlightSlots.Release();
        entry.Completion.TrySetException(exception);
    }

    private void FailSubscribe(ushort packetId, Exception exception)
    {
        PendingSubscribe? entry;
        lock (_subscribeLock)
        {
            _pendingSubscribes.Remove(packetId, out entry);
        }

        if (entry is not null)
        {
            _packetIds.Release(packetId);
            entry.Completion.TrySetException(exception);
        }
    }

    private long? OldestPendingAckTimestamp()
    {
        long? oldest = _inFlight.OldestSentTimestamp;
        lock (_subscribeLock)
        {
            foreach (PendingSubscribe entry in _pendingSubscribes.Values)
            {
                if (oldest is null || entry.SentTimestamp < oldest)
                {
                    oldest = entry.SentTimestamp;
                }
            }
        }

        return oldest;
    }

    private void SetState(MqttConnectionState state, Exception? reason)
    {
        var previous = (MqttConnectionState)Interlocked.Exchange(ref _state, (int)state);
        if (previous != state)
        {
            _dispatcher.PostStateChange(new ConnectionStateChange(previous, state, reason));
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
