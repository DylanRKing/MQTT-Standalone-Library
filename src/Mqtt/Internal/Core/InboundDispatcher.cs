using System.Threading.Channels;
using Mqtt.Internal.Codec.Packets;

namespace Mqtt.Internal.Core;

/// <summary>
/// An inbound PUBLISH and the connection it arrived on (where its PUBACK must go).
/// <paramref name="Deliver"/> is false for a filtered duplicate, which is only acknowledged.
/// </summary>
internal sealed record InboundPublish(PublishPacket Packet, Connection Connection, bool Deliver = true);

/// <summary>
/// Runs application handlers off the read loop. Messages go through a bounded channel and are
/// handled one at a time in arrival order; when it fills, the read loop waits, so a slow handler
/// slows only its own client. State changes have their own unbounded queue so they never block.
/// </summary>
internal sealed class InboundDispatcher
{
    private readonly Channel<InboundPublish> _messages;
    private readonly Channel<ConnectionStateChange> _stateChanges = Channel.CreateUnbounded<ConnectionStateChange>(new() { SingleReader = true });
    private readonly Func<InboundPublish, Task> _handleMessage;
    private readonly Func<ConnectionStateChange, Task> _handleStateChange;
    private readonly SafeLogger _log;
    private readonly int _warnThreshold;
    private bool _warned;

    public InboundDispatcher(
        Func<InboundPublish, Task> handleMessage,
        Func<ConnectionStateChange, Task> handleStateChange,
        SafeLogger log,
        int capacity = 1_000)
    {
        _handleMessage = handleMessage;
        _handleStateChange = handleStateChange;
        _log = log;
        _warnThreshold = capacity * 8 / 10;
        _messages = Channel.CreateBounded<InboundPublish>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        Completion = Task.WhenAll(
            Task.Run(() => RunAsync(_messages.Reader, _handleMessage)),
            Task.Run(() => RunAsync(_stateChanges.Reader, _handleStateChange)));
    }

    /// <summary>Completes when both queues have been drained after <see cref="Complete"/>.</summary>
    public Task Completion { get; }

    /// <summary>Queues a message, waiting while the queue is full. Called from the read loop.</summary>
    public ValueTask EnqueueAsync(InboundPublish message, CancellationToken ct)
    {
        int count = _messages.Reader.Count;
        if (count >= _warnThreshold && !_warned)
        {
            _warned = true;
            _log.Log(MqttLogLevel.Warning, $"Inbound queue is {count} messages deep; MessageReceived handlers are not keeping up.");
        }
        else if (count < _warnThreshold / 2)
        {
            _warned = false;
        }

        return _messages.Writer.WriteAsync(message, ct);
    }

    public void PostStateChange(ConnectionStateChange change) => _stateChanges.Writer.TryWrite(change);

    public void Complete()
    {
        _messages.Writer.TryComplete();
        _stateChanges.Writer.TryComplete();
    }

    /// <summary>Awaits each subscriber in turn; an exception is logged and does not stop the others.</summary>
    public async Task InvokeAsync<T>(Func<T, Task>? handlers, T argument, SafeLogger log, string eventName)
    {
        if (handlers is null)
        {
            return;
        }

        foreach (Func<T, Task> handler in handlers.GetInvocationList().Cast<Func<T, Task>>())
        {
            try
            {
                await handler(argument).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log.Log(MqttLogLevel.Error, $"{eventName} handler threw.", ex);
            }
        }
    }

    private async Task RunAsync<T>(ChannelReader<T> reader, Func<T, Task> handle)
    {
        await foreach (T item in reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await handle(item).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.Log(MqttLogLevel.Error, "Inbound dispatch failed.", ex);
            }
        }
    }
}
