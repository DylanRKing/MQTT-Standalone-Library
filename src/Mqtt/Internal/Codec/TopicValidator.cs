namespace Mqtt.Internal.Codec;

/// <summary>
/// MQTT 3.1.1 topic name and filter syntax rules (section 4.7). Broker-specific limits such as
/// AWS's 256-byte, 7-slash topics are checked by the client core, not here.
/// </summary>
internal static class TopicValidator
{
    /// <summary>A topic name to publish to: non-empty and free of wildcards.</summary>
    public static bool IsValidTopicName(string topic) =>
        !string.IsNullOrEmpty(topic) && topic.AsSpan().IndexOfAny('+', '#') < 0;

    /// <summary>
    /// A subscription filter: non-empty, <c>+</c> only as a whole level, <c>#</c> only as the whole last level.
    /// </summary>
    public static bool IsValidTopicFilter(string filter)
    {
        if (string.IsNullOrEmpty(filter))
        {
            return false;
        }

        ReadOnlySpan<char> span = filter;
        for (int i = 0; i < span.Length; i++)
        {
            char c = span[i];
            if (c != '+' && c != '#')
            {
                continue;
            }

            bool startsLevel = i == 0 || span[i - 1] == '/';
            bool endsLevel = i == span.Length - 1 || span[i + 1] == '/';
            if (!startsLevel || !endsLevel)
            {
                return false;
            }

            if (c == '#' && i != span.Length - 1)
            {
                return false;
            }
        }

        return true;
    }
}
