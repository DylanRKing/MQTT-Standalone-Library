namespace Mqtt;

/// <summary>
/// A topic filter and the maximum QoS requested for it in a SUBSCRIBE.
/// </summary>
/// <param name="Filter">The filter; may contain <c>+</c> and <c>#</c> wildcards in valid positions.</param>
/// <param name="Qos">The maximum QoS the broker should deliver with.</param>
public sealed record TopicFilter(string Filter, MqttQos Qos = MqttQos.AtMostOnce);
