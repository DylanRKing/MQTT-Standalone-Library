using System.Security.Authentication;
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
/// IDs, in-flight QoS 1 publishes, the offline queue, pending SUBSCRIBEs, keep-alive, inbound
/// dispatch and reconnect, and drives the transport and codec. All protocol rules live here.
/// </summary>
/// <remarks>
/// A lost connection is recovered with jittered backoff. On recovery the client restores
/// subscriptions, resends unacknowledged QoS 1 publishes, then drains the offline queue, in that
/// order. The client only gives up (goes to Disconnected) on DisconnectAsync, a failure retrying
/// cannot fix, or when reconnect attempts run out; everything pending then fails.
/// </remarks>
internal sealed class ClientSession : IAsyncDisposable
{
    private static readonly TimeSpan DisconnectSendTimeout = TimeSpan.FromSeconds(5);

    // Connections dropped sooner than this after CONNACK count towards the duplicate client ID warning.
    private static readonly TimeSpan ShortLivedConnection = TimeSpan.FromSeconds(5);

    private readonly MqttClientOptions _options;
    private readonly TransportOptions _transportOptions;
    private readonly SafeLogger _log;
    private readonly TimeProvider _time;

    // Connect, disconnect, connection-loss handling and reconnect attempts never run at the same time.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private readonly SemaphoreSlim _inFlightSlots;

    // One recovery at a time: a new connection's recovery waits for the previous one to give up.
    private readonly SemaphoreSlim _recovery = new(1, 1);
    private readonly PacketIdAllocator _packetIds = new();
    private readonly InFlightStore _inFlight = new();
    private readonly OfflineQueue _offlineQueue;
    private readonly DuplicateCache _duplicates;
    private readonly ReconnectBackoff _backoff;
    private readonly PublishRateLimiter? _rateLimiter;
    private readonly Dictionary<ushort, PendingSubscribe> _pendingSubscribes = [];
    private readonly Dictionary<string, MqttQos> _grantedSubscriptions = [];
    private readonly Lock _subscribeLock = new();
    private readonly InboundDispatcher _dispatcher;

    private volatile Connection? _current;
    private CancellationTokenSource? _reconnectCts;
    private int _state = (int)MqttConnectionState.Disconnected;
    private int _disposed;
    private int _shortLivedConnections;

    // True from ConnectAsync until the client gives up: QoS 1 publishes are accepted (queued if need be).
    private volatile bool _acceptingQos1;

    public ClientSession(MqttClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _time = options.TimeProvider;
        _log = new SafeLogger(options.Logger, options.ClientId);
        _inFlightSlots = new SemaphoreSlim(options.MaxInFlight, options.MaxInFlight);
        _offlineQueue = new OfflineQueue(options.OfflineQueueCapacity, options.OfflineQueueFullMode);
        _duplicates = new DuplicateCache(_time);
        _backoff = new ReconnectBackoff(options.Reconnect, options.ReconnectDelayFloor, options.Random);
        _rateLimiter = options.MaxPublishRate is int rate ? new PublishRateLimiter(rate, _time) : null;
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

    /// <summary>Filters the broker has granted, restored after a reconnect that lost the session.</summary>
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

            if (State == MqttConnectionState.Reconnecting)
            {
                throw new InvalidOperationException("The client is reconnecting automatically. Call DisconnectAsync first to stop it.");
            }

            _acceptingQos1 = true;
            SetState(MqttConnectionState.Connecting, null);
            var connection = new Connection(_time);
            ConnAckPacket connAck;
            try
            {
                connAck = await OpenAsync(connection, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await CloseConnectionAsync(connection, ex).ConfigureAwait(false);
                EnterDisconnected(ex is OperationCanceledException ? null : ex, new MqttConnectionLostException("ConnectAsync failed before the message was sent.", ex));
                throw;
            }

            Activate(connection, connAck, isReconnect: false);
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

        if (qos == MqttQos.AtMostOnce)
        {
            if (_current is not { IsOpen: true } connection)
            {
                return PublishResult.DroppedOffline;
            }

            try
            {
                await WaitForPublishRateAsync(ct).ConfigureAwait(false);
                await connection.SendAsync(new PublishPacket(topic, payload, MqttQos.AtMostOnce, retain), ct).ConfigureAwait(false);
                return PublishResult.Sent;
            }
            catch (MqttConnectionLostException)
            {
                return PublishResult.DroppedOffline;
            }
        }

        if (!_acceptingQos1)
        {
            throw new InvalidOperationException("The client is not connected. Call ConnectAsync first.");
        }

        var item = new QueuedPublish(topic, payload, retain);
        switch (_offlineQueue.Enqueue(item, out QueuedPublish? dropped))
        {
            case EnqueueOutcome.Full:
                _log.Log(MqttLogLevel.Warning, $"Offline queue full ({_options.OfflineQueueCapacity}); QoS 1 publish to '{topic}' rejected.");
                return PublishResult.QueueFull;

            case EnqueueOutcome.Queued:
                if (dropped is not null)
                {
                    _log.Log(MqttLogLevel.Warning, $"Offline queue full ({_options.OfflineQueueCapacity}); dropped oldest publish to '{dropped.Topic}'.");
                    dropped.Completion.TrySetResult(PublishResult.QueueFull);
                }

                // The client may have given up between the check above and queueing.
                if (!_acceptingQos1 && _offlineQueue.TryRemove(item))
                {
                    item.Completion.TrySetException(new MqttConnectionLostException("The client disconnected before the message was sent."));
                }

                break;

            case EnqueueOutcome.Bypass:
                // Past MaxInFlight, callers wait here: natural backpressure.
                await _inFlightSlots.WaitAsync(ct).ConfigureAwait(false);
                await SendNewQos1Async(item).ConfigureAwait(false);
                break;
        }

        // Cancelling stops the wait, not the publish.
        return await item.Completion.Task.WaitAsync(ct).ConfigureAwait(false);
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
        if (_current is { IsOpen: true } connection)
        {
            return await SubscribeCoreAsync(connection, filters, ct).ConfigureAwait(false);
        }

        if (State is MqttConnectionState.Connecting or MqttConnectionState.Reconnecting)
        {
            throw new MqttConnectionLostException("The client is reconnecting; SUBSCRIBE cannot be queued. Retry once connected.");
        }

        throw new InvalidOperationException("The client is not connected. Call ConnectAsync first.");
    }

    public async Task DisconnectAsync(CancellationToken ct)
    {
        ThrowIfDisposed();

        // Stop reconnecting first, so a reconnect attempt holding the lock gives it up promptly.
        StopReconnect();
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

        StopReconnect();
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

    /// <summary>
    /// TCP, TLS, CONNECT and CONNACK within the connect timeout, holding a connection throttle slot
    /// if one is configured. Throws on refusal.
    /// </summary>
    private async Task<ConnAckPacket> OpenAsync(Connection connection, CancellationToken ct)
    {
        ConnectionThrottle? throttle = _options.ConnectionThrottle;
        if (throttle is null)
        {
            return await OpenCoreAsync(connection, ct).ConfigureAwait(false);
        }

        // Waiting for a slot is not part of the connect timeout.
        await throttle.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await OpenCoreAsync(connection, ct).ConfigureAwait(false);
        }
        finally
        {
            throttle.Release();
        }
    }

    private async Task<ConnAckPacket> OpenCoreAsync(Connection connection, CancellationToken ct)
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
        return connAck;
    }

    /// <summary>Makes an accepted connection current and starts recovery. Caller holds the lifecycle lock.</summary>
    private void Activate(Connection connection, ConnAckPacket connAck, bool isReconnect)
    {
        long now = _time.GetTimestamp();

        // Ack deadlines restart now; resending refreshes them again.
        _inFlight.TouchAll(now);
        connection.ConnectedTimestamp = now;
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
        _ = RecoverAsync(connection, connAck.SessionPresent, isReconnect);
    }

    /// <summary>Restores subscriptions, resends unacknowledged QoS 1, then drains the offline queue.</summary>
    private async Task RecoverAsync(Connection connection, bool sessionPresent, bool isReconnect)
    {
        await _recovery.WaitAsync().ConfigureAwait(false);
        try
        {
            ThrowIfNotCurrent(connection);
            if (isReconnect && (_options.CleanSession || !sessionPresent))
            {
                TopicFilter[] filters = [.. GrantedSubscriptions.Select(s => new TopicFilter(s.Key, s.Value))];
                if (filters.Length > 0)
                {
                    _log.Log(MqttLogLevel.Information, $"Restoring {filters.Length} subscription(s).");
                    await SubscribeCoreAsync(connection, filters, CancellationToken.None).ConfigureAwait(false);
                }
            }

            await ResendInFlightAsync(connection).ConfigureAwait(false);
            await FlushOfflineQueueAsync(connection).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is MqttConnectionLostException or OperationCanceledException or InvalidDataException)
        {
            // The connection dropped again; the next reconnect picks up where this left off.
            _log.Log(MqttLogLevel.Debug, "Recovery interrupted by connection loss.", ex);
        }
        catch (Exception ex)
        {
            _log.Log(MqttLogLevel.Error, "Recovery after connect failed.", ex);
        }
        finally
        {
            _recovery.Release();
        }
    }

    private async Task ResendInFlightAsync(Connection connection)
    {
        foreach (InFlightPublish entry in _inFlight.Snapshot())
        {
            ThrowIfNotCurrent(connection);
            PublishPacket packet;
            if (_options.CleanSession)
            {
                // The broker discarded the old session, so its packet IDs mean nothing: send as new.
                if (!_inFlight.TryRemove(entry.Packet.PacketId, out _))
                {
                    continue;
                }

                _packetIds.Release(entry.Packet.PacketId);
                packet = entry.Packet with { PacketId = _packetIds.Allocate(), Dup = false };
                entry.Packet = packet;
                _inFlight.Add(entry);
                if (!_acceptingQos1)
                {
                    FailInFlight(packet.PacketId, new MqttConnectionLostException("The client disconnected before the broker acknowledged."));
                    continue;
                }
            }
            else
            {
                packet = entry.Packet with { Dup = true };
                entry.Packet = packet;
            }

            await WaitForPublishRateAsync(CancellationToken.None).ConfigureAwait(false);
            entry.SentTimestamp = _time.GetTimestamp();
            await connection.SendAsync(packet, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task FlushOfflineQueueAsync(Connection connection)
    {
        while (_offlineQueue.PeekOrOpen(() => _current == connection && connection.IsOpen) is { } item)
        {
            await _inFlightSlots.WaitAsync(connection.Lifetime.Token).ConfigureAwait(false);
            if (!_offlineQueue.TryRemove(item))
            {
                // Dropped (DropOldest) or failed while waiting for a slot.
                _inFlightSlots.Release();
                continue;
            }

            await SendNewQos1Async(item).ConfigureAwait(false);
            ThrowIfNotCurrent(connection);
        }
    }

    /// <summary>
    /// Gives a QoS 1 publish a packet ID and sends it. The caller holds an in-flight slot, which the
    /// entry now owns. If the connection is down, the entry waits in the in-flight store for resend.
    /// </summary>
    private async Task SendNewQos1Async(QueuedPublish item)
    {
        // Before the entry is created, so time spent waiting does not count towards the ack timeout.
        await WaitForPublishRateAsync(CancellationToken.None).ConfigureAwait(false);
        var entry = new InFlightPublish(
            new PublishPacket(item.Topic, item.Payload, MqttQos.AtLeastOnce, item.Retain, Dup: false, _packetIds.Allocate()),
            _time.GetTimestamp(),
            item.Completion);

        // Added before sending so a fast PUBACK finds it.
        _inFlight.Add(entry);

        // If the client gave up meanwhile, the sweep may already have run.
        if (!_acceptingQos1)
        {
            FailInFlight(entry.Packet.PacketId, new MqttConnectionLostException("The client disconnected before the message was sent."));
            return;
        }

        if (_current is { IsOpen: true } connection)
        {
            try
            {
                // Not cancellable: once queued the frame goes out, and only the caller's wait honours ct.
                await connection.SendAsync(entry.Packet, CancellationToken.None).ConfigureAwait(false);
            }
            catch (MqttConnectionLostException)
            {
                // Resent after reconnect, or failed if the client gives up.
            }
        }
    }

    private async Task<SubscribeResult> SubscribeCoreAsync(Connection connection, IReadOnlyList<TopicFilter> filters, CancellationToken ct)
    {
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
                        _grantedSubscriptions.Remove(requested[i].Filter);
                        _log.Log(MqttLogLevel.Warning, $"Subscription to '{requested[i].Filter}' refused ({result.ReasonCode}).");
                    }
                }
            }
        }

        return new SubscribeResult(results);
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
                // A clean session starts with an empty duplicate cache. Cleared here, on the read
                // loop, so no PUBLISH that follows the CONNACK is checked against the old session.
                if (_options.CleanSession && connAck.ReasonCode == ReasonCode.Success)
                {
                    _duplicates.Clear();
                }

                connection.ConnAck.TrySetResult(connAck);
                return default;
            }

            throw new InvalidDataException($"Expected CONNACK as the first packet, received {packet!.Type}.");
        }

        switch (packet)
        {
            case PublishPacket publish:
                bool deliver = true;
                if (publish.Qos == MqttQos.AtLeastOnce)
                {
                    if (_duplicates.IsDuplicate(publish))
                    {
                        _log.Log(MqttLogLevel.Debug, $"Duplicate QoS 1 delivery on '{publish.Topic}' (packet ID {publish.PacketId}) acknowledged, not delivered.");
                        deliver = false;
                    }
                    else
                    {
                        _duplicates.Record(publish);
                    }
                }

                return _dispatcher.EnqueueAsync(new InboundPublish(publish, connection, deliver), connection.Lifetime.Token);
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
        if (item.Deliver && Volatile.Read(ref _disposed) == 0)
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
        // Wakes a connect attempt still waiting for CONNACK, which then cleans up itself.
        connection.ConnAck.TrySetException(reason);

        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_current != connection)
            {
                return;
            }

            TimeSpan uptime = _time.GetElapsedTime(connection.ConnectedTimestamp);
            _log.Log(MqttLogLevel.Warning, $"Connection lost after {uptime.TotalSeconds:0.#} s: {reason.Message}");
            await CloseConnectionAsync(connection, reason).ConfigureAwait(false);
            WarnIfDuplicateClientId(uptime);

            if (!_options.Reconnect.Enabled || Volatile.Read(ref _disposed) != 0)
            {
                EnterDisconnected(reason, new MqttConnectionLostException("The connection was lost before the broker acknowledged.", reason));
                return;
            }

            _backoff.OnConnectionLost(uptime);
            SetState(MqttConnectionState.Reconnecting, reason);
            var cts = new CancellationTokenSource();
            _reconnectCts = cts;
            _ = Task.Run(() => ReconnectLoopAsync(cts.Token));
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private async Task ReconnectLoopAsync(CancellationToken ct)
    {
        int failures = 0;
        try
        {
            while (true)
            {
                TimeSpan delay = _backoff.NextDelay();
                _log.Log(MqttLogLevel.Information, $"Reconnecting in {delay.TotalSeconds:0.##} s (attempt {failures + 1}).");
                await Task.Delay(delay, _time, ct).ConfigureAwait(false);

                await _lifecycle.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    ct.ThrowIfCancellationRequested();
                    var connection = new Connection(_time);
                    ConnAckPacket connAck;
                    try
                    {
                        connAck = await OpenAsync(connection, ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        await CloseConnectionAsync(connection, ex).ConfigureAwait(false);
                        ct.ThrowIfCancellationRequested();
                        failures++;
                        if (!IsRetryable(ex))
                        {
                            _log.Log(MqttLogLevel.Error, $"Reconnect failed and will not be retried: {ex.Message}", ex);
                            EnterDisconnected(ex, new MqttConnectionLostException("Reconnecting failed before the broker acknowledged.", ex));
                            return;
                        }

                        if (_options.Reconnect.MaxAttempts is int max && failures >= max)
                        {
                            _log.Log(MqttLogLevel.Error, $"Giving up after {failures} reconnect attempts: {ex.Message}", ex);
                            EnterDisconnected(ex, new MqttConnectionLostException("Reconnecting failed before the broker acknowledged.", ex));
                            return;
                        }

                        _log.Log(MqttLogLevel.Warning, $"Reconnect attempt {failures} failed: {ex.Message}");
                        continue;
                    }

                    Activate(connection, connAck, isReconnect: true);
                    return;
                }
                finally
                {
                    _lifecycle.Release();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // DisconnectAsync or DisposeAsync stopped reconnecting.
        }
        catch (Exception ex)
        {
            _log.Log(MqttLogLevel.Error, "Reconnect loop failed.", ex);
        }
    }

    private async Task DisconnectCoreAsync()
    {
        StopReconnect();
        Connection? connection = _current;
        if (connection is not null)
        {
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

            await CloseConnectionAsync(connection, reason: null).ConfigureAwait(false);
            _log.Log(MqttLogLevel.Information, "Disconnected.");
        }

        EnterDisconnected(null, new MqttConnectionLostException("Disconnected before the broker acknowledged."));
    }

    /// <summary>Closes one connection and fails SUBSCRIBEs waiting on it. Publishes are kept for resend.</summary>
    private async Task CloseConnectionAsync(Connection connection, Exception? reason)
    {
        if (_current == connection)
        {
            _current = null;
        }

        // Cancel before closing the queue: the flush only reopens it while the connection is open.
        connection.Lifetime.Cancel();
        _offlineQueue.Close();
        if (connection.Transport is { } transport)
        {
            await transport.CloseAsync().ConfigureAwait(false);
        }

        List<PendingSubscribe> subscribes;
        lock (_subscribeLock)
        {
            subscribes = [.. _pendingSubscribes.Values];
            _pendingSubscribes.Clear();
        }

        var lost = new MqttConnectionLostException("The connection closed before SUBACK arrived.", reason);
        foreach (PendingSubscribe entry in subscribes)
        {
            _packetIds.Release(entry.Packet.PacketId);
            entry.Completion.TrySetException(lost);
        }
    }

    /// <summary>The client gives up: everything pending fails. Caller holds the lifecycle lock.</summary>
    private void EnterDisconnected(Exception? reason, MqttConnectionLostException failure)
    {
        _acceptingQos1 = false;
        StopReconnect();
        _offlineQueue.Close();
        foreach (QueuedPublish item in _offlineQueue.RemoveAll())
        {
            item.Completion.TrySetException(failure);
        }

        foreach (InFlightPublish entry in _inFlight.RemoveAll())
        {
            ReleaseInFlight(entry, failure);
        }

        // Restoring subscriptions applies to automatic reconnects only.
        lock (_subscribeLock)
        {
            _grantedSubscriptions.Clear();
        }

        SetState(MqttConnectionState.Disconnected, reason);
    }

    private ValueTask WaitForPublishRateAsync(CancellationToken ct) =>
        _rateLimiter?.WaitAsync(ct) ?? ValueTask.CompletedTask;

    private void StopReconnect() => Interlocked.Exchange(ref _reconnectCts, null)?.Cancel();

    /// <summary>
    /// CONNACK refusals other than "server unavailable" and TLS authentication failures cannot be
    /// fixed by retrying.
    /// </summary>
    private static bool IsRetryable(Exception ex) => ex switch
    {
        MqttConnectException { ReasonCode: { } code } => code == ReasonCode.ServerUnavailable,
        MqttConnectException { InnerException: AuthenticationException } => false,
        _ => true,
    };

    /// <summary>AWS drops the older connection when a client ID connects twice; two processes sharing an ID flap.</summary>
    private void WarnIfDuplicateClientId(TimeSpan uptime)
    {
        if (uptime >= ShortLivedConnection)
        {
            _shortLivedConnections = 0;
            return;
        }

        if (++_shortLivedConnections >= 2)
        {
            _log.Log(
                MqttLogLevel.Warning,
                $"Connection dropped within seconds of connecting {_shortLivedConnections} times in a row; another client may be using client ID '{_options.ClientId}'.");
        }
    }

    private void ThrowIfNotCurrent(Connection connection)
    {
        if (_current != connection || !connection.IsOpen)
        {
            throw new MqttConnectionLostException("The connection closed during recovery.");
        }
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
