using System.Text;
using Mqtt.Internal.Codec;

namespace Mqtt.Internal.Core;

/// <summary>
/// AWS IoT Core quotas checked before anything is sent, so violations fail fast with
/// <see cref="ArgumentException"/> instead of AWS silently dropping the packet or the connection.
/// </summary>
internal static class AwsLimits
{
    public const int MaxPayloadBytes = 128 * 1024;
    public const int MaxTopicBytes = 256;
    public const int MaxTopicSlashes = 7;
    public const int MaxFiltersPerSubscribe = 8;

    public static void ValidateTopic(string topic, string paramName)
    {
        ArgumentNullException.ThrowIfNull(topic, paramName);
        if (!TopicValidator.IsValidTopicName(topic))
        {
            throw new ArgumentException("Topic must be non-empty and contain no + or # wildcards.", paramName);
        }

        ValidateSize(topic, "Topic", paramName);
    }

    public static void ValidateFilter(TopicFilter filter, string paramName)
    {
        ArgumentNullException.ThrowIfNull(filter, paramName);
        ArgumentNullException.ThrowIfNull(filter.Filter, paramName);
        if (!TopicValidator.IsValidTopicFilter(filter.Filter))
        {
            throw new ArgumentException($"Invalid topic filter '{filter.Filter}': + must fill a whole level and # must be the whole last level.", paramName);
        }

        ValidateSize(filter.Filter, "Topic filter", paramName);
        ValidateQos(filter.Qos, paramName);
    }

    public static void ValidatePayload(ReadOnlyMemory<byte> payload, string paramName)
    {
        if (payload.Length > MaxPayloadBytes)
        {
            throw new ArgumentException($"Payload is {payload.Length} bytes; AWS IoT Core allows at most {MaxPayloadBytes}.", paramName);
        }
    }

    public static void ValidateQos(MqttQos qos, string paramName)
    {
        if (qos is not (MqttQos.AtMostOnce or MqttQos.AtLeastOnce))
        {
            throw new ArgumentException($"QoS {(byte)qos} is not supported; AWS IoT Core supports QoS 0 and 1 only.", paramName);
        }
    }

    private static void ValidateSize(string value, string what, string paramName)
    {
        if (value.Contains('\0'))
        {
            throw new ArgumentException($"{what} may not contain U+0000.", paramName);
        }

        if (Encoding.UTF8.GetByteCount(value) > MaxTopicBytes)
        {
            throw new ArgumentException($"{what} exceeds AWS IoT Core's {MaxTopicBytes}-byte limit.", paramName);
        }

        if (value.AsSpan().Count('/') > MaxTopicSlashes)
        {
            throw new ArgumentException($"{what} has more than AWS IoT Core's {MaxTopicSlashes} forward slashes.", paramName);
        }
    }
}
