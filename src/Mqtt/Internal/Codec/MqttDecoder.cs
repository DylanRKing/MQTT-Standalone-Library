using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Codec;

/// <summary>
/// Stateless packet decoder. Takes exactly one complete frame (fixed header included) and never
/// throws on malformed input. Decodes both directions so the test broker can share it.
/// </summary>
internal static class MqttDecoder
{
    private const string ProtocolName = "MQTT";

    public static DecodeError TryDecode(ReadOnlySpan<byte> frame, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        if (FixedHeader.TryRead(frame, out byte typeAndFlags, out int remainingLength, out int headerLength) != FrameStatus.Complete
            || headerLength + remainingLength != frame.Length)
        {
            return DecodeError.MalformedPacket;
        }

        var type = (PacketType)(typeAndFlags >> 4);
        int flags = typeAndFlags & 0x0F;
        var reader = new MqttReader(frame[headerLength..]);

        // Every type except PUBLISH has fixed flags: 0010 for SUBSCRIBE, 0000 for the rest.
        if (type != PacketType.Publish && flags != (type is PacketType.Subscribe or PacketType.Unsubscribe or PacketType.PubRel ? 0x02 : 0x00))
        {
            return DecodeError.MalformedPacket;
        }

        DecodeError error = type switch
        {
            PacketType.Connect => DecodeConnect(ref reader, version, out packet),
            PacketType.ConnAck => DecodeConnAck(ref reader, version, out packet),
            PacketType.Publish => DecodePublish(ref reader, flags, version, out packet),
            PacketType.PubAck => DecodePubAck(ref reader, version, out packet),
            PacketType.Subscribe => DecodeSubscribe(ref reader, version, out packet),
            PacketType.SubAck => DecodeSubAck(ref reader, version, out packet),
            PacketType.PingReq => DecodeEmpty(PingReqPacket.Instance, out packet),
            PacketType.PingResp => DecodeEmpty(PingRespPacket.Instance, out packet),
            PacketType.Disconnect => DecodeEmpty(DisconnectPacket.Instance, out packet),

            // QoS 2 flow and UNSUBSCRIBE are valid MQTT but not implemented by this library.
            PacketType.PubRec or PacketType.PubRel or PacketType.PubComp
                or PacketType.Unsubscribe or PacketType.UnsubAck => DecodeError.UnsupportedPacket,

            // 0 and 15 are reserved in 3.1.1.
            _ => DecodeError.MalformedPacket,
        };

        if (error == DecodeError.None && !reader.IsEmpty)
        {
            error = DecodeError.MalformedPacket;
        }

        if (error != DecodeError.None)
        {
            packet = null;
        }

        return error;
    }

    private static DecodeError DecodeConnect(ref MqttReader reader, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        if (!reader.TryReadString(out string protocolName) || protocolName != ProtocolName || !reader.TryReadByte(out byte level))
        {
            return DecodeError.MalformedPacket;
        }

        if (level != (byte)version)
        {
            return DecodeError.UnsupportedPacket;
        }

        switch (version)
        {
            case ProtocolVersion.V311:
                if (!reader.TryReadByte(out byte connectFlags)
                    || !reader.TryReadUInt16(out ushort keepAlive)
                    || !reader.TryReadString(out string clientId))
                {
                    return DecodeError.MalformedPacket;
                }

                // Bit 0 is reserved. Will (bits 2-5), password (6) and user name (7) are not supported.
                if ((connectFlags & 0x01) != 0)
                {
                    return DecodeError.MalformedPacket;
                }

                if ((connectFlags & 0xFC) != 0)
                {
                    return DecodeError.UnsupportedPacket;
                }

                bool cleanSession = (connectFlags & 0x02) != 0;
                if (clientId.Length == 0 && !cleanSession)
                {
                    return DecodeError.MalformedPacket;
                }

                packet = new ConnectPacket(clientId, cleanSession, keepAlive);
                return DecodeError.None;
            default:
                return DecodeError.UnsupportedPacket;
        }
    }

    private static DecodeError DecodeConnAck(ref MqttReader reader, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!reader.TryReadByte(out byte ackFlags) || !reader.TryReadByte(out byte returnCode))
                {
                    return DecodeError.MalformedPacket;
                }

                bool sessionPresent = (ackFlags & 0x01) != 0;
                if ((ackFlags & 0xFE) != 0
                    || !ReasonCodeMapper.TryFromConnAckV311(returnCode, out ReasonCode reasonCode)
                    || (sessionPresent && reasonCode != ReasonCode.Success))
                {
                    return DecodeError.MalformedPacket;
                }

                packet = new ConnAckPacket(sessionPresent, reasonCode);
                return DecodeError.None;
            default:
                return DecodeError.UnsupportedPacket;
        }
    }

    private static DecodeError DecodePublish(ref MqttReader reader, int flags, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        bool dup = (flags & 0x08) != 0;
        int qos = (flags >> 1) & 0x03;
        bool retain = (flags & 0x01) != 0;
        switch (qos)
        {
            case 3:
                return DecodeError.MalformedPacket;
            case 2:
                return DecodeError.UnsupportedPacket;
            case 0 when dup:
                return DecodeError.MalformedPacket;
        }

        switch (version)
        {
            case ProtocolVersion.V311:
                if (!reader.TryReadString(out string topic) || !TopicValidator.IsValidTopicName(topic))
                {
                    return DecodeError.MalformedPacket;
                }

                ushort packetId = 0;
                if (qos > 0 && (!reader.TryReadUInt16(out packetId) || packetId == 0))
                {
                    return DecodeError.MalformedPacket;
                }

                // Copy: the frame usually lives in the transport's pooled read buffer.
                byte[] payload = reader.ReadRemaining().ToArray();
                packet = new PublishPacket(topic, payload, (MqttQos)qos, retain, dup, packetId);
                return DecodeError.None;
            default:
                return DecodeError.UnsupportedPacket;
        }
    }

    private static DecodeError DecodePubAck(ref MqttReader reader, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!reader.TryReadUInt16(out ushort packetId) || packetId == 0)
                {
                    return DecodeError.MalformedPacket;
                }

                packet = new PubAckPacket(packetId);
                return DecodeError.None;
            default:
                return DecodeError.UnsupportedPacket;
        }
    }

    private static DecodeError DecodeSubscribe(ref MqttReader reader, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!reader.TryReadUInt16(out ushort packetId) || packetId == 0 || reader.IsEmpty)
                {
                    return DecodeError.MalformedPacket;
                }

                var filters = new List<TopicFilter>();
                while (!reader.IsEmpty)
                {
                    if (!reader.TryReadString(out string filter)
                        || !TopicValidator.IsValidTopicFilter(filter)
                        || !reader.TryReadByte(out byte options)
                        || (options & 0xFC) != 0
                        || options == 3)
                    {
                        return DecodeError.MalformedPacket;
                    }

                    if (options == 2)
                    {
                        return DecodeError.UnsupportedPacket;
                    }

                    filters.Add(new TopicFilter(filter, (MqttQos)options));
                }

                packet = new SubscribePacket(packetId, filters);
                return DecodeError.None;
            default:
                return DecodeError.UnsupportedPacket;
        }
    }

    private static DecodeError DecodeSubAck(ref MqttReader reader, ProtocolVersion version, out IMqttPacket? packet)
    {
        packet = null;
        switch (version)
        {
            case ProtocolVersion.V311:
                if (!reader.TryReadUInt16(out ushort packetId) || packetId == 0 || reader.IsEmpty)
                {
                    return DecodeError.MalformedPacket;
                }

                ReadOnlySpan<byte> codes = reader.ReadRemaining();
                var reasonCodes = new ReasonCode[codes.Length];
                for (int i = 0; i < codes.Length; i++)
                {
                    reasonCodes[i] = (ReasonCode)codes[i];
                    if (!ReasonCodeMapper.IsValidSubAckV311(reasonCodes[i]))
                    {
                        return DecodeError.MalformedPacket;
                    }
                }

                packet = new SubAckPacket(packetId, reasonCodes);
                return DecodeError.None;
            default:
                return DecodeError.UnsupportedPacket;
        }
    }

    // A non-empty body is rejected by the caller's IsEmpty check.
    private static DecodeError DecodeEmpty(IMqttPacket instance, out IMqttPacket? packet)
    {
        packet = instance;
        return DecodeError.None;
    }
}
