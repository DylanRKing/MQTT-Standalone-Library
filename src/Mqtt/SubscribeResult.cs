namespace Mqtt;

/// <summary>The broker's answer for one filter in a SUBSCRIBE.</summary>
/// <param name="Filter">The filter as requested.</param>
/// <param name="ReasonCode">
/// <see cref="ReasonCode.Success"/> or <see cref="ReasonCode.GrantedQos1"/> when granted;
/// <see cref="ReasonCode.UnspecifiedError"/> when refused (for example over the 50-subscription limit,
/// or not allowed by the device's IoT policy).
/// </param>
public sealed record SubscriptionResult(TopicFilter Filter, ReasonCode ReasonCode)
{
    /// <summary>True when the broker accepted the subscription.</summary>
    public bool Succeeded => ReasonCode is ReasonCode.Success or ReasonCode.GrantedQos1 or ReasonCode.GrantedQos2;

    /// <summary>QoS the broker granted, which may be lower than requested; null when refused.</summary>
    public MqttQos? GrantedQos => ReasonCode switch
    {
        ReasonCode.Success => MqttQos.AtMostOnce,
        ReasonCode.GrantedQos1 or ReasonCode.GrantedQos2 => MqttQos.AtLeastOnce,
        _ => null,
    };
}

/// <summary>
/// Outcome of <see cref="MqttClient.SubscribeAsync"/>. Per-filter refusals are reported here, not thrown.
/// </summary>
public sealed class SubscribeResult
{
    internal SubscribeResult(IReadOnlyList<SubscriptionResult> items) => Items = items;

    /// <summary>One result per requested filter, in request order.</summary>
    public IReadOnlyList<SubscriptionResult> Items { get; }

    /// <summary>True when every filter was granted.</summary>
    public bool AllSucceeded => Items.All(i => i.Succeeded);
}
