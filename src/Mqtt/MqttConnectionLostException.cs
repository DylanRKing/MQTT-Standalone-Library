namespace Mqtt;

/// <summary>
/// The connection to the broker dropped before a pending operation completed, and the
/// operation could not be queued for retry.
/// </summary>
public class MqttConnectionLostException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public MqttConnectionLostException()
        : base("The connection to the broker was lost.")
    {
    }

    /// <summary>Creates the exception with a message.</summary>
    public MqttConnectionLostException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and the underlying cause.</summary>
    public MqttConnectionLostException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
