using Mqtt.Internal.Core;

namespace Mqtt;

/// <summary>
/// One MQTT 3.1.1 client: one device with its own TLS connection to AWS IoT Core.
/// All members are safe to call from multiple threads at once.
/// </summary>
/// <example>
/// <code>
/// await using var device = new MqttClient(options);
/// device.MessageReceived += msg => HandleAsync(msg);
/// await device.ConnectAsync();
/// await device.SubscribeAsync([new("sim/device-042/cmd", MqttQos.AtLeastOnce)]);
/// await device.PublishAsync("sim/device-042/telemetry", payload, MqttQos.AtLeastOnce);
/// await device.DisconnectAsync();
/// </code>
/// </example>
public sealed class MqttClient : IAsyncDisposable
{
    private readonly ClientSession _session;

    /// <summary>Creates a client. Validates <paramref name="options"/>; does not connect.</summary>
    /// <exception cref="ArgumentException">An option is missing or out of range.</exception>
    public MqttClient(MqttClientOptions options) => _session = new ClientSession(options);

    /// <summary>
    /// Raised for each application message received. Handlers for one client run one at a time in
    /// arrival order. For QoS 1 the PUBACK is sent after the handler returns; a handler exception is
    /// logged and the PUBACK still sent.
    /// </summary>
    public event Func<MqttMessage, Task>? MessageReceived
    {
        add => _session.MessageReceived += value;
        remove => _session.MessageReceived -= value;
    }

    /// <summary>Raised after each state change, in order, off the network thread.</summary>
    public event Func<ConnectionStateChange, Task>? ConnectionStateChanged
    {
        add => _session.ConnectionStateChanged += value;
        remove => _session.ConnectionStateChanged -= value;
    }

    /// <summary>The MQTT client ID from the options.</summary>
    public string ClientId => _session.ClientId;

    /// <summary>Current connection state.</summary>
    public MqttConnectionState State => _session.State;

    /// <summary>
    /// Connects and completes when the broker accepts the CONNECT. Does nothing if already connected.
    /// </summary>
    /// <exception cref="MqttConnectException">
    /// Timeout, TLS failure, unreachable broker, or CONNACK refusal (see <see cref="MqttConnectException.ReasonCode"/>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
    public Task ConnectAsync(CancellationToken ct = default) => _session.ConnectAsync(ct);

    /// <summary>
    /// Publishes a message. QoS 0 completes when written (<see cref="PublishResult.Sent"/>), or returns
    /// <see cref="PublishResult.DroppedOffline"/> when not connected. QoS 1 completes on PUBACK
    /// (<see cref="PublishResult.Acknowledged"/>); callers past <see cref="MqttClientOptions.MaxInFlight"/> wait.
    /// An empty payload with <paramref name="retain"/> clears the topic's retained message.
    /// </summary>
    /// <exception cref="ArgumentException">Invalid topic, payload over 128 KB, or QoS 2.</exception>
    /// <exception cref="MqttConnectionLostException">QoS 1: the connection dropped before PUBACK.</exception>
    /// <exception cref="InvalidOperationException">QoS 1 while not connected.</exception>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
    public Task<PublishResult> PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        MqttQos qos = MqttQos.AtMostOnce,
        bool retain = false,
        CancellationToken ct = default) => _session.PublishAsync(topic, payload, qos, retain, ct);

    /// <summary>
    /// Subscribes to one or more filters, sent in batches of 8, and completes when every batch's SUBACK
    /// arrives. Per-filter refusals are reported in the result, not thrown.
    /// </summary>
    /// <exception cref="ArgumentException">No filters, an invalid filter, or QoS 2.</exception>
    /// <exception cref="MqttConnectionLostException">The connection dropped before every SUBACK arrived.</exception>
    /// <exception cref="InvalidOperationException">Not connected.</exception>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
    public Task<SubscribeResult> SubscribeAsync(IReadOnlyList<TopicFilter> filters, CancellationToken ct = default) =>
        _session.SubscribeAsync(filters, ct);

    /// <summary>
    /// Sends DISCONNECT and closes the connection. Does not throw if already disconnected.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The client has been disposed.</exception>
    public Task DisconnectAsync(CancellationToken ct = default) => _session.DisconnectAsync(ct);

    /// <summary>Disconnects if connected and releases the client. Later calls throw <see cref="ObjectDisposedException"/>.</summary>
    public ValueTask DisposeAsync() => _session.DisposeAsync();
}
