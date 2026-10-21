using System.Diagnostics;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Codec;

/// <summary>
/// Stateless packet encoder. Call <see cref="GetSize"/> to size a buffer, then <see cref="Write"/>.
/// Invalid packets throw <see cref="ArgumentException"/>: they are built by the library itself, so
/// a bad one is a bug or an unvalidated caller argument, not a network condition.
/// </summary>
internal static class MqttEncoder
{
    private const string ProtocolName = "MQTT";

    /// <summary>Returns the total frame size of <paramref name="packet"/>, fixed header included.</summary>
    public static int GetSize(IMqttPacket packet, ProtocolVersion version)
    {
        int remainingLength = GetRemainingLength(packet, version);
        return 1 + MqttWriter.GetVariableByteIntegerSize(remainingLength) + remainingLength;
    }

    /// <summary>Writes the complete frame to <paramref name="destination"/> and returns the bytes written.</summary>
    public static int Write(Span<byte> destination, IMqttPacket packet, ProtocolVersion version)
    {
        int remainingLength = GetRemainingLength(packet, version);
        int size = 1 + MqttWriter.GetVariableByteIntegerSize(remainingLength) + remainingLength;
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

    private static byte GetTypeAndFlags(IMqttPacket packet)
    {
        byte type = (byte)((byte)packet.Type << 4);
        return packet switch
        {
            PublishPacket p => (byte)(type | (p.Dup ? 0x08 : 0) | ((byte)p.Qos << 1) | (p.Retain ? 0x01 : 0)),
            SubscribePacket => (byte)(type | 0x02),
            _ => type,
        };
    }

    private static int GetRemainingLength(IMqttPacket packet, ProtocolVersion version)
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

    private static void WriteBody(ref MqttWriter writer, IMqttPacket packet, ProtocolVersion version)
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

    private static int GetConnectSize(ConnectPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                if (p.ClientId.Length == 0 && !p.CleanSession)
                {
                    throw new ArgumentException("An empty client ID requires a clean session.", nameof(p));
                }

                // Protocol name, level, connect flags, keep-alive, client ID.
                return (2 + ProtocolName.Length) + 1 + 1 + 2 + MqttWriter.GetStringSize(p.ClientId, nameof(p.ClientId));
            default:
                throw Unsupported(version);
        }
    }

    private static void WriteConnect(ref MqttWriter writer, ConnectPacket p, ProtocolVersion version)
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

    private static int GetConnAckSize(ConnAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!ReasonCodeMapper.TryToConnAckV311(p.ReasonCode, out _))
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

    private static void WriteConnAck(ref MqttWriter writer, ConnAckPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                ReasonCodeMapper.TryToConnAckV311(p.ReasonCode, out byte returnCode);
                writer.WriteByte(p.SessionPresent ? (byte)0x01 : (byte)0x00);
                writer.WriteByte(returnCode);
                break;
            default:
                throw Unsupported(version);
        }
    }

    // PUBLISH

    private static int GetPublishSize(PublishPacket p, ProtocolVersion version)
    {
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!TopicValidator.IsValidTopicName(p.Topic))
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

                return MqttWriter.GetStringSize(p.Topic, nameof(p.Topic))
                    + (p.Qos == MqttQos.AtMostOnce ? 0 : 2)
                    + p.Payload.Length;
            default:
                throw Unsupported(version);
        }
    }

    private static void WritePublish(ref MqttWriter writer, PublishPacket p, ProtocolVersion version)
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

    private static int GetPubAckSize(PubAckPacket p, ProtocolVersion version)
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

    private static int GetSubscribeSize(SubscribePacket p, ProtocolVersion version)
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
                    if (!TopicValidator.IsValidTopicFilter(filter.Filter))
                    {
                        throw new ArgumentException($"Invalid topic filter '{filter.Filter}'.", nameof(p));
                    }

                    if (filter.Qos is not (MqttQos.AtMostOnce or MqttQos.AtLeastOnce))
                    {
                        throw new ArgumentException($"QoS {(byte)filter.Qos} is not supported.", nameof(p));
                    }

                    size += MqttWriter.GetStringSize(filter.Filter, nameof(filter.Filter)) + 1;
                }

                return size;
            default:
                throw Unsupported(version);
        }
    }

    private static void WriteSubscribe(ref MqttWriter writer, SubscribePacket p, ProtocolVersion version)
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

    private static int GetSubAckSize(SubAckPacket p, ProtocolVersion version)
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
                    if (!ReasonCodeMapper.IsValidSubAckV311(code))
                    {
                        throw new ArgumentException($"{code} is not a valid MQTT 3.1.1 SUBACK code.", nameof(p));
                    }
                }

                return 2 + p.ReasonCodes.Count;
            default:
                throw Unsupported(version);
        }
    }

    private static void WriteSubAck(ref MqttWriter writer, SubAckPacket p, ProtocolVersion version)
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

    private static void RequirePacketId(ushort packetId, string paramName)
    {
        if (packetId == 0)
        {
            throw new ArgumentException("Packet ID 0 is not allowed.", paramName);
        }
    }

    private static NotSupportedException Unsupported(ProtocolVersion version) =>
        new($"Protocol version {version} is not implemented.");
}
