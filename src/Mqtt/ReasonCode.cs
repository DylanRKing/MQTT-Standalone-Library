namespace Mqtt;

/// <summary>
/// Outcome codes reported by the broker in CONNACK and SUBACK.
/// </summary>
/// <remarks>
/// Values follow the MQTT 5.0 reason code numbering so that 5.0 support can add codes
/// without changing existing ones. MQTT 3.1.1 return codes are mapped onto this set.
/// </remarks>
public enum ReasonCode : byte
{
    /// <summary>CONNACK accepted, or SUBACK granted at QoS 0.</summary>
    Success = 0x00,

    /// <summary>SUBACK granted at QoS 1.</summary>
    GrantedQos1 = 0x01,

    /// <summary>SUBACK granted at QoS 2 (never requested by this library).</summary>
    GrantedQos2 = 0x02,

    /// <summary>SUBACK failure (3.1.1 code 0x80).</summary>
    UnspecifiedError = 0x80,

    /// <summary>CONNACK 3.1.1 code 1: unacceptable protocol version.</summary>
    UnsupportedProtocolVersion = 0x84,

    /// <summary>CONNACK 3.1.1 code 2: identifier rejected.</summary>
    ClientIdentifierNotValid = 0x85,

    /// <summary>CONNACK 3.1.1 code 4: bad user name or password.</summary>
    BadUserNameOrPassword = 0x86,

    /// <summary>CONNACK 3.1.1 code 5: not authorized.</summary>
    NotAuthorized = 0x87,

    /// <summary>CONNACK 3.1.1 code 3: server unavailable.</summary>
    ServerUnavailable = 0x88,
}
