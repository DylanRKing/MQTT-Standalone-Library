using System.Diagnostics;
using System.Text;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Codec;

/// <summary>
/// Stateless packet encoder. Call <see cref="GetSize"/> to size a buffer, then <see cref="Write"/>.
/// Invalid packets throw <see cref="ArgumentException"/>: they are built by the library itself, so
/// a bad one is a bug or an unvalidated caller argument, not a network condition.
/// </summary>
internal sealed class MqttEncoder
{
    private readonly TopicValidator _topics = new();
    private readonly ReasonCodeMapper _reasonCodes = new();
    private readonly UTF8Encoding _utf8Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private string ProtocolName => "MQTT";

    /// <summary>Returns the total frame size of <paramref name="packet"/>, fixed header included.</summary>
    public int GetSize(IMqttPacket packet, ProtocolVersion version)
    {
        int remainingLength = GetRemainingLength(packet, version);
        return 1 + GetVariableByteIntegerSize(remainingLength) + remainingLength;
    }

    /// <summary>Writes the complete frame to <paramref name="destination"/> and returns the bytes written.</summary>
    public int Write(Span<byte> destination, IMqttPacket packet, ProtocolVersion version)
    {
        int remainingLength = GetRemainingLength(packet, version);
        int size = 1 + GetVariableByteIntegerSize(remainingLength) + remainingLength;
        if (destination.Length < size)
        {
            throw new ArgumentException($"Destination is {destination.Length} bytes; packet needs {size}.", nameof(destination));
        }

        var writer = new MqttWriter(destination);
        writer.WriteByte(GetTypeAndFlags(packet));
        writer.WriteVariableByteInteger(remainingLength);
        WriteBody(ref writer, packet, version);
        Debug.Assert(writer.Position == size, "Encoded size does not match GetSize.");
        return size;
    }

    private byte GetTypeAndFlags(IMqttPacket packet)
    {
        byte type = (byte)((byte)packet.Type << 4);
        return packet switch
        {
            PublishPacket p => (byte)(type | (p.Dup ? 0x08 : 0) | ((byte)p.Qos << 1) | (p.Retain ? 0x01 : 0)),
            SubscribePacket => (byte)(type | 0x02),
            _ => type,
        };
    }

    private int GetRemainingLength(IMqttPacket packet, ProtocolVersion version)
    {
        ArgumentNullException.ThrowIfNull(packet);
        return packet switch
        {
            ConnectPacket p => GetConnectSize(p, version),
            ConnAckPacket p => GetConnAckSize(p, version),
            PublishPacket p => GetPublishSize(p, version),
            PubAckPacket p => GetPubAckSize(p, version),
            SubscribePacket p => GetSubscribeSize(p, version),
            SubAckPacket p => GetSubAckSize(p, version),
            PingReqPacket or PingRespPacket or DisconnectPacket => 0,
            _ => throw new ArgumentException($"Cannot encode packet type {packet.Type}.", nameof(packet)),
        };
    }

    private void WriteBody(ref MqttWriter writer, IMqttPacket packet, ProtocolVersion version)
    {
        switch (packet)
        {
            case ConnectPacket p:
                WriteConnect(ref writer, p, version);
                break;
            case ConnAckPacket p:
                WriteConnAck(ref writer, p, version);
                break;
            case PublishPacket p:
                WritePublish(ref writer, p, version);
                break;
            case PubAckPacket p:
                writer.WriteUInt16(p.PacketId);
                break;
            case SubscribePacket p:
                WriteSubscribe(ref writer, p, version);
                break;
            case SubAckPacket p:
                WriteSubAck(ref writer, p, version);
                break;
        }
    }

    // CONNECT

    private int GetConnectSize(ConnectPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                if (p.ClientId.Length == 0 && !p.CleanSession)
                {
                    throw new ArgumentException("An empty client ID requires a clean session.", nameof(p));
                }

                // Protocol name, level, connect flags, keep-alive, client ID.
                return (2 + ProtocolName.Length) + 1 + 1 + 2 + GetStringSize(p.ClientId, nameof(p.ClientId));
            default:
                throw Unsupported(version);
        }
    }

    private void WriteConnect(ref MqttWriter writer, ConnectPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                writer.WriteString(ProtocolName);
                writer.WriteByte((byte)version);
                writer.WriteByte(p.CleanSession ? (byte)0x02 : (byte)0x00);
                writer.WriteUInt16(p.KeepAliveSeconds);
                writer.WriteString(p.ClientId);
                break;
            default:
                throw Unsupported(version);
        }
    }

    // CONNACK

    private int GetConnAckSize(ConnAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!_reasonCodes.TryToConnAckV311(p.ReasonCode, out _))
                {
                    throw new ArgumentException($"{p.ReasonCode} has no MQTT 3.1.1 CONNACK return code.", nameof(p));
                }

                if (p.SessionPresent && p.ReasonCode != ReasonCode.Success)
                {
                    throw new ArgumentException("Session present must be clear when the connection is refused.", nameof(p));
                }

                return 2;
            default:
                throw Unsupported(version);
        }
    }

    private void WriteConnAck(ref MqttWriter writer, ConnAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                _reasonCodes.TryToConnAckV311(p.ReasonCode, out byte returnCode);
                writer.WriteByte(p.SessionPresent ? (byte)0x01 : (byte)0x00);
                writer.WriteByte(returnCode);
                break;
            default:
                throw Unsupported(version);
        }
    }

    // PUBLISH

    private int GetPublishSize(PublishPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!_topics.IsValidTopicName(p.Topic))
                {
                    throw new ArgumentException("PUBLISH topic must be non-empty and contain no wildcards.", nameof(p));
                }

                switch (p.Qos)
                {
                    case MqttQos.AtMostOnce when p.PacketId != 0 || p.Dup:
                        throw new ArgumentException("QoS 0 PUBLISH must have packet ID 0 and DUP clear.", nameof(p));
                    case MqttQos.AtLeastOnce when p.PacketId == 0:
                        throw new ArgumentException("QoS 1 PUBLISH needs a non-zero packet ID.", nameof(p));
                    case MqttQos.AtMostOnce or MqttQos.AtLeastOnce:
                        break;
                    default:
                        throw new ArgumentException($"QoS {(byte)p.Qos} is not supported.", nameof(p));
                }

                return GetStringSize(p.Topic, nameof(p.Topic))
                    + (p.Qos == MqttQos.AtMostOnce ? 0 : 2)
                    + p.Payload.Length;
            default:
                throw Unsupported(version);
        }
    }

    private void WritePublish(ref MqttWriter writer, PublishPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                writer.WriteString(p.Topic);
                if (p.Qos != MqttQos.AtMostOnce)
                {
                    writer.WriteUInt16(p.PacketId);
                }

                writer.WriteBytes(p.Payload.Span);
                break;
            default:
                throw Unsupported(version);
        }
    }

    // PUBACK

    private int GetPubAckSize(PubAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                RequirePacketId(p.PacketId, nameof(p));
                return 2;
            default:
                throw Unsupported(version);
        }
    }

    // SUBSCRIBE

    private int GetSubscribeSize(SubscribePacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                RequirePacketId(p.PacketId, nameof(p));
                if (p.Filters.Count == 0)
                {
                    throw new ArgumentException("SUBSCRIBE needs at least one filter.", nameof(p));
                }

                int size = 2;
                foreach (TopicFilter filter in p.Filters)
                {
                    if (!_topics.IsValidTopicFilter(filter.Filter))
                    {
                        throw new ArgumentException($"Invalid topic filter '{filter.Filter}'.", nameof(p));
                    }

                    if (filter.Qos is not (MqttQos.AtMostOnce or MqttQos.AtLeastOnce))
                    {
                        throw new ArgumentException($"QoS {(byte)filter.Qos} is not supported.", nameof(p));
                    }

                    size += GetStringSize(filter.Filter, nameof(filter.Filter)) + 1;
                }

                return size;
            default:
                throw Unsupported(version);
        }
    }

    private void WriteSubscribe(ref MqttWriter writer, SubscribePacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                writer.WriteUInt16(p.PacketId);
                foreach (TopicFilter filter in p.Filters)
                {
                    writer.WriteString(filter.Filter);
                    writer.WriteByte((byte)filter.Qos);
                }

                break;
            default:
                throw Unsupported(version);
        }
    }

    // SUBACK

    private int GetSubAckSize(SubAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                RequirePacketId(p.PacketId, nameof(p));
                if (p.ReasonCodes.Count == 0)
                {
                    throw new ArgumentException("SUBACK needs at least one return code.", nameof(p));
                }

                foreach (ReasonCode code in p.ReasonCodes)
                {
                    if (!_reasonCodes.IsValidSubAckV311(code))
                    {
                        throw new ArgumentException($"{code} is not a valid MQTT 3.1.1 SUBACK code.", nameof(p));
                    }
                }

                return 2 + p.ReasonCodes.Count;
            default:
                throw Unsupported(version);
        }
    }

    private void WriteSubAck(ref MqttWriter writer, SubAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                writer.WriteUInt16(p.PacketId);
                foreach (ReasonCode code in p.ReasonCodes)
                {
                    writer.WriteByte((byte)code);
                }

                break;
            default:
                throw Unsupported(version);
        }
    }

    private void RequirePacketId(ushort packetId, string paramName)
    {
        if (packetId == 0)
        {
            throw new ArgumentException("Packet ID 0 is not allowed.", paramName);
        }
    }

    private NotSupportedException Unsupported(ProtocolVersion version) =>
        new($"Protocol version {version} is not implemented.");

    /// <summary>Encoded size of the variable-length remaining-length field.</summary>
    internal int GetVariableByteIntegerSize(int value) => value switch
    {
        // 268,435,455 is the largest value four 7-bit groups can carry.
        < 0 or > 268_435_455 => throw new ArgumentOutOfRangeException(nameof(value), value, "Remaining length exceeds the MQTT maximum of 268,435,455 bytes."),
        < 128 => 1,
        < 16_384 => 2,
        < 2_097_152 => 3,
        _ => 4,
    };

    /// <summary>
    /// Returns the encoded size of a length-prefixed UTF-8 string, throwing if it cannot be encoded
    /// (over 65,535 bytes or containing U+0000).
    /// </summary>
    internal int GetStringSize(string value, string paramName)
    {
        ArgumentNullException.ThrowIfNull(value, paramName);
        if (value.Contains('\0'))
        {
            throw new ArgumentException("MQTT strings may not contain U+0000.", paramName);
        }

        int length;
        try
        {
            length = _utf8Strict.GetByteCount(value);
        }
        catch (EncoderFallbackException ex)
        {
            throw new ArgumentException("MQTT strings must be valid UTF-16 (no unpaired surrogates).", paramName, ex);
        }

        // The length prefix is a UInt16.
        if (length > ushort.MaxValue)
        {
            throw new ArgumentException($"MQTT strings are limited to {ushort.MaxValue} UTF-8 bytes; got {length}.", paramName);
        }

        return 2 + length;
    }
}
