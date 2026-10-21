namespace Mqtt.Internal.Codec;

/// <summary>
/// Protocol version passed to the encoder and decoder. Values are the CONNECT protocol level byte.
/// Only 3.1.1 is implemented; 5.0 adds a branch per packet rather than new packet types.
/// </summary>
internal enum ProtocolVersion : byte
{
    V311 = 4,
}
