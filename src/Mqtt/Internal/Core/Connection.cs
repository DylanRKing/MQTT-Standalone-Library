using System.Buffers;
using Mqtt.Internal.Codec;
using Mqtt.Internal.Codec.Packets;
using Mqtt.Internal.Transport;

namespace Mqtt.Internal.Core;

/// <summary>
/// State for one network connection of a client: its transport, CONNACK wait and keep-alive
/// timestamps. A reconnect creates a new instance, so callbacks from an old connection can be
/// recognised and ignored.
/// </summary>
internal sealed class Connection(TimeProvider time)
{
    private long _lastSentTimestamp = time.GetTimestamp();
    private long _pingSentTimestamp;

    public MqttTransport? Transport { get; set; }

    public TaskCompletionSource<ConnAckPacket> ConnAck { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Cancelled when the connection is torn down; stops its keep-alive monitor.</summary>
    public CancellationTokenSource Lifetime { get; } = new();

    public bool IsOpen => !Lifetime.IsCancellationRequested && Transport is { IsClosed: false };

    public long LastSentTimestamp => Volatile.Read(ref _lastSentTimestamp);

    /// <summary>When CONNACK was accepted; used to spot connections dropped right after connecting.</summary>
    public long ConnectedTimestamp { get; set; }

    /// <summary>When the outstanding PINGREQ was sent, or 0 when none is outstanding.</summary>
    public long PingSentTimestamp
    {
        get => Volatile.Read(ref _pingSentTimestamp);
        set => Volatile.Write(ref _pingSentTimestamp, value);
    }

    /// <summary>Encodes <paramref name="packet"/> into a pooled buffer and sends it.</summary>
    /// <exception cref="MqttConnectionLostException">The connection closed first.</exception>
    public async Task SendAsync(IMqttPacket packet, CancellationToken ct = default)
    {
        MqttTransport transport = Transport ?? throw new MqttConnectionLostException("Not connected.");
        int size = MqttEncoder.GetSize(packet, ProtocolVersion.V311);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            MqttEncoder.Write(buffer, packet, ProtocolVersion.V311);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }

        await transport.SendAsync(buffer, size, ct).ConfigureAwait(false);
        Volatile.Write(ref _lastSentTimestamp, time.GetTimestamp());
    }
}
