namespace Mqtt.Internal.Codec;

/// <summary>Maps 3.1.1 CONNACK return codes and SUBACK return codes to and from <see cref="ReasonCode"/>.</summary>
internal sealed class ReasonCodeMapper
{
    public bool TryFromConnAckV311(byte returnCode, out ReasonCode reasonCode)
    {
        reasonCode = returnCode switch
        {
            0 => ReasonCode.Success,
            1 => ReasonCode.UnsupportedProtocolVersion,
            2 => ReasonCode.ClientIdentifierNotValid,
            3 => ReasonCode.ServerUnavailable,
            4 => ReasonCode.BadUserNameOrPassword,
            5 => ReasonCode.NotAuthorized,
            _ => ReasonCode.UnspecifiedError,
        };
        return returnCode <= 5;
    }

    public bool TryToConnAckV311(ReasonCode reasonCode, out byte returnCode)
    {
        returnCode = reasonCode switch
        {
            ReasonCode.Success => 0,
            ReasonCode.UnsupportedProtocolVersion => 1,
            ReasonCode.ClientIdentifierNotValid => 2,
            ReasonCode.ServerUnavailable => 3,
            ReasonCode.BadUserNameOrPassword => 4,
            ReasonCode.NotAuthorized => 5,
            _ => byte.MaxValue,
        };
        return returnCode != byte.MaxValue;
    }

    /// <summary>3.1.1 SUBACK codes share their numeric value with the reason code.</summary>
    public bool IsValidSubAckV311(ReasonCode reasonCode) => reasonCode is
        ReasonCode.Success or ReasonCode.GrantedQos1 or ReasonCode.GrantedQos2 or ReasonCode.UnspecifiedError;
}
