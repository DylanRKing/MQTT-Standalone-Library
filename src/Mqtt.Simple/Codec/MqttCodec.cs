using System.Text;

namespace Mqtt.Simple.Codec;

/// <summary>MQTT 3.1.1 control packet types, from the top four bits of the first byte.</summary>
internal enum PacketType : byte
{
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    Subscribe = 8,
    SubAck = 9,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
}

/// <summary>One packet as read off the wire: the first header byte and the bytes after the length.</summary>
internal readonly record struct Packet(byte Header, byte[] Body)
{
    public PacketType Type => (PacketType)(Header >> 4);
}

/// <summary>An incoming PUBLISH. <see cref="PacketId"/> is 0 for QoS 0.</summary>
internal readonly record struct IncomingPublish(string Topic, int Qos, ushort PacketId, byte[] Payload);

/// <summary>
/// Builds outgoing packets as complete frames and reads the fields the client needs from incoming
/// ones. No validation beyond what's needed to avoid reading past the end of a packet.
/// </summary>
internal sealed class MqttCodec
{
    public byte[] Connect(string clientId, int keepAliveSeconds)
    {
        var body = new List<byte>();
        AddString(body, "MQTT");
        body.Add(4);    // Protocol level 3.1.1
        body.Add(0x02); // Clean session
        AddUInt16(body, keepAliveSeconds);
        AddString(body, clientId);
        return Frame(PacketType.Connect, 0, body);
    }

    public byte[] Publish(string topic, byte[] payload, int qos, ushort packetId)
    {
        var body = new List<byte>();
        AddString(body, topic);
        if (qos > 0)
        {
            AddUInt16(body, packetId);
        }

        body.AddRange(payload);
        return Frame(PacketType.Publish, (byte)(qos << 1), body);
    }

    public byte[] Subscribe(string topicFilter, int qos, ushort packetId)
    {
        var body = new List<byte>();
        AddUInt16(body, packetId);
        AddString(body, topicFilter);
        body.Add((byte)qos);

        // SUBSCRIBE must carry flags 0010.
        return Frame(PacketType.Subscribe, 0x02, body);
    }

    public byte[] PubAck(ushort packetId) => [0x40, 0x02, (byte)(packetId >> 8), (byte)packetId];

    public byte[] PingReq() => [0xC0, 0x00];

    public byte[] Disconnect() => [0xE0, 0x00];

    /// <summary>The CONNACK return code: 0 means accepted.</summary>
    public int ConnAckReturnCode(Packet connAck) =>
        connAck.Type == PacketType.ConnAck && connAck.Body.Length >= 2
            ? connAck.Body[1]
            : throw new IOException($"Expected CONNACK, got {connAck.Type}.");

    /// <summary>The SUBACK return code for a single-filter SUBSCRIBE: 0 or 1 is the granted QoS, 0x80 is refused.</summary>
    public int SubAckReturnCode(Packet subAck) =>
        subAck.Body.Length >= 3 ? subAck.Body[2] : 0x80;

    /// <summary>The packet ID at the start of a PUBACK or SUBACK.</summary>
    public ushort PacketId(Packet packet) =>
        packet.Body.Length >= 2
            ? (ushort)((packet.Body[0] << 8) | packet.Body[1])
            : throw new IOException($"{packet.Type} is too short.");

    public IncomingPublish ReadPublish(Packet publish)
    {
        byte[] body = publish.Body;
        int qos = (publish.Header >> 1) & 0x03;
        int topicLength = (body[0] << 8) | body[1];
        string topic = Encoding.UTF8.GetString(body, 2, topicLength);
        int offset = 2 + topicLength;

        ushort packetId = 0;
        if (qos > 0)
        {
            packetId = (ushort)((body[offset] << 8) | body[offset + 1]);
            offset += 2;
        }

        return new IncomingPublish(topic, qos, packetId, body[offset..]);
    }

    private byte[] Frame(PacketType type, byte flags, List<byte> body)
    {
        var frame = new List<byte>(body.Count + 5) { (byte)(((byte)type << 4) | flags) };

        // Remaining length: 7 bits per byte, high bit set when another byte follows.
        int length = body.Count;
        do
        {
            byte digit = (byte)(length % 128);
            length /= 128;
            frame.Add(length > 0 ? (byte)(digit | 0x80) : digit);
        }
        while (length > 0);

        frame.AddRange(body);
        return [.. frame];
    }

    private void AddString(List<byte> body, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        AddUInt16(body, bytes.Length);
        body.AddRange(bytes);
    }

    private void AddUInt16(List<byte> body, int value)
    {
        body.Add((byte)(value >> 8));
        body.Add((byte)value);
    }
}
