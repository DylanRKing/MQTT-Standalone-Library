namespace Mqtt.Internal.Codec;

/// <summary>Result of <see cref="MqttDecoder.TryDecode"/>; decoding never throws on bad input.</summary>
internal enum DecodeError
{
    None,

    /// <summary>The bytes violate the MQTT specification. The connection must be closed.</summary>
    MalformedPacket,

    /// <summary>A valid packet this library does not implement (QoS 2 flow, UNSUBSCRIBE, will, ...).</summary>
    UnsupportedPacket,
}
