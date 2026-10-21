namespace Mqtt;

/// <summary>
/// MQTT quality of service. QoS 2 is not offered because AWS IoT Core does not support it.
/// </summary>
public enum MqttQos : byte
{
    /// <summary>QoS 0: fire and forget.</summary>
    AtMostOnce = 0,

    /// <summary>QoS 1: acknowledged with PUBACK, may be delivered more than once.</summary>
    AtLeastOnce = 1,
}
