using System.Buffers;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;

namespace Mqtt.Internal.Transport;

/// <summary>
/// Receives one complete inbound frame. The memory is only valid until the returned task
/// completes. Throwing closes the connection, which is how the core rejects a bad packet.
/// </summary>
internal delegate ValueTask FrameHandler(ReadOnlyMemory<byte> frame);

/// <summary>
/// One TLS connection to the broker: a TCP socket wrapped in <see cref="SslStream"/>, one read
/// loop and one write loop. Moves whole MQTT frames and knows nothing about what they mean.
/// Never reconnects; when the connection fails it reports once through the closed callback
/// and the client core decides what to do.
/// </summary>
internal sealed class MqttTransport : IAsyncDisposable
{
    private readonly TransportOptions _options;
    private readonly FrameHandler _onFrame;
    private readonly Action<Exception> _onClosed;
    private readonly SafeLogger _log;
    private readonly FrameReader _frameReader;
    private readonly Channel<OutboundFrame> _outbound;
    private readonly CancellationTokenSource _shutdown = new();
    private SslStream? _stream;
    private Task _run = Task.CompletedTask;

    // Set once by the first Shutdown call; later calls are no-ops.
    private Exception? _closeReason;
    private bool _closedLocally;

    /// <summary>Creates an unconnected transport; call <see cref="ConnectAsync"/> once to open it.</summary>
    /// <param name="options">Endpoint, certificate provider and limits.</param>
    /// <param name="onFrame">Called from the read loop for each inbound frame, one at a time.</param>
    /// <param name="onClosed">
    /// Called once, after both loops have stopped, if the connection fails. Not called after
    /// <see cref="CloseAsync"/> or when <see cref="ConnectAsync"/> itself fails.
    /// </param>
    public MqttTransport(TransportOptions options, FrameHandler onFrame, Action<Exception> onClosed)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(onFrame);
        ArgumentNullException.ThrowIfNull(onClosed);
        _options = options;
        _onFrame = onFrame;
        _onClosed = onClosed;
        _log = new SafeLogger(options.Logger, options.ClientId);
        _frameReader = new FrameReader(options.MaxFrameSize);
        _outbound = Channel.CreateBounded<OutboundFrame>(new BoundedChannelOptions(options.SendQueueCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    // Set by ConnectAsync before the loops start.
    private SslStream Stream => _stream ?? throw new InvalidOperationException("The transport is not connected.");

    /// <summary>TLS version negotiated with the broker.</summary>
    public SslProtocols SslProtocol => _stream?.SslProtocol ?? SslProtocols.None;

    /// <summary>True once the connection has failed or been closed.</summary>
    public bool IsClosed => Volatile.Read(ref _closeReason) is not null;

    /// <summary>
    /// Opens TCP, completes the TLS handshake with the client certificate, and starts the read and
    /// write loops. The caller bounds the time taken with <paramref name="ct"/>. Call once; after a
    /// failure the transport is closed and a new one is needed.
    /// </summary>
    /// <param name="ct">Cancels the TCP connect and TLS handshake.</param>
    /// <exception cref="AuthenticationException">The TLS handshake failed; retrying will not help.</exception>
    /// <exception cref="SocketException">The broker could not be reached.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was cancelled.</exception>
    /// <exception cref="InvalidOperationException">Called more than once.</exception>
    public async Task ConnectAsync(CancellationToken ct)
    {
        if (_stream is not null || IsClosed)
        {
            throw new InvalidOperationException("ConnectAsync can only be called once per transport.");
        }

        try
        {
            _stream = await OpenStreamAsync(_options, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Nothing started: mark closed (without the closed callback) and release the buffers.
            Shutdown(ex, locally: true);
            _frameReader.Dispose();
            _shutdown.Dispose();
            throw;
        }

        _run = RunAsync();
    }

    private async Task<SslStream> OpenStreamAsync(TransportOptions options, CancellationToken ct)
    {
        X509Certificate2 certificate = await options.CertificateProvider.GetClientCertificateAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("The certificate provider returned null.");
        if (!certificate.HasPrivateKey)
        {
            throw new InvalidOperationException($"Client certificate {certificate.Thumbprint} has no private key.");
        }

        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        SslStream? stream = null;
        try
        {
            await socket.ConnectAsync(options.Host, options.Port, ct).ConfigureAwait(false);
            stream = new SslStream(new NetworkStream(socket, ownsSocket: true), leaveInnerStreamOpen: false);
            await stream.AuthenticateAsClientAsync(
                new SslClientAuthenticationOptions
                {
                    TargetHost = options.Host,
                    ClientCertificateContext = SslStreamCertificateContext.Create(certificate, additionalCertificates: null, offline: true),
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = options.CheckCertificateRevocation ? X509RevocationMode.Online : X509RevocationMode.NoCheck,
                    RemoteCertificateValidationCallback = options.ServerCertificateValidator,
                },
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                socket.Dispose();
            }

            _log.Log(MqttLogLevel.Warning, $"Connection to {options.Host}:{options.Port} failed: {ex.Message}", ex);
            throw;
        }

        _log.Log(
            MqttLogLevel.Information,
            $"Connected to {options.Host}:{options.Port} with {stream.SslProtocol}, client certificate {certificate.Thumbprint}.");
        return stream;
    }

    /// <summary>
    /// Queues a frame and completes once its bytes are written to the TLS stream. Takes ownership
    /// of <paramref name="pooledBuffer"/>, which must come from <see cref="ArrayPool{T}.Shared"/>,
    /// and returns it to the pool whatever the outcome.
    /// </summary>
    /// <exception cref="MqttConnectionLostException">The connection closed before the frame was written.</exception>
    public async Task SendAsync(byte[] pooledBuffer, int length, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pooledBuffer);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, pooledBuffer.Length);

        var frame = new OutboundFrame(pooledBuffer, length);
        try
        {
            await _outbound.Writer.WriteAsync(frame, ct).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            frame.Fail(ConnectionLost());
        }
        catch (Exception ex)
        {
            frame.Fail(ex);
        }

        // Cancelling stops the wait, not the write: once queued the frame is still sent.
        await frame.Written.WaitAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Closes the connection without raising the closed callback. Frames not yet written fail with
    /// <see cref="MqttConnectionLostException"/>. Safe to call more than once and after a failure.
    /// </summary>
    public async Task CloseAsync()
    {
        if (Shutdown(new MqttConnectionLostException("The connection was closed locally."), locally: true))
        {
            _log.Log(MqttLogLevel.Debug, "Transport closed locally.");
        }

        await _run.ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => new(CloseAsync());

    private async Task RunAsync()
    {
        await Task.WhenAll(ReadLoopAsync(), WriteLoopAsync()).ConfigureAwait(false);

        // Catch frames queued between the shutdown drain and the channel completing.
        DrainOutbound();
        await Stream.DisposeAsync().ConfigureAwait(false);
        _frameReader.Dispose();
        _shutdown.Dispose();

        if (!_closedLocally)
        {
            _log.Log(MqttLogLevel.Warning, $"Connection lost: {_closeReason!.Message}", _closeReason.InnerException);
            try
            {
                _onClosed(_closeReason);
            }
            catch (Exception ex)
            {
                _log.Log(MqttLogLevel.Error, "Closed callback threw.", ex);
            }
        }
    }

    private async Task ReadLoopAsync()
    {
        CancellationToken ct = _shutdown.Token;
        try
        {
            while (true)
            {
                int read = await Stream.ReadAsync(_frameReader.GetMemory(), ct).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new MqttConnectionLostException("The broker closed the connection.");
                }

                _frameReader.Advance(read);
                while (true)
                {
                    FrameReadStatus status = _frameReader.TryRead(out ReadOnlyMemory<byte> frame);
                    if (status == FrameReadStatus.NeedMoreData)
                    {
                        break;
                    }

                    if (status != FrameReadStatus.Frame)
                    {
                        throw new MqttConnectionLostException(status == FrameReadStatus.TooLarge
                            ? "Received a frame larger than the maximum frame size."
                            : "Received a malformed fixed header.");
                    }

                    await _onFrame(frame).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            Shutdown(ex);
        }
    }

    private async Task WriteLoopAsync()
    {
        ChannelReader<OutboundFrame> reader = _outbound.Reader;
        CancellationToken ct = _shutdown.Token;
        try
        {
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (reader.TryRead(out OutboundFrame? frame))
                {
                    try
                    {
                        // SslStream does not buffer writes, so a completed write has reached the socket.
                        await Stream.WriteAsync(frame.Memory, ct).ConfigureAwait(false);
                        frame.Complete();
                    }
                    catch (Exception ex)
                    {
                        Shutdown(ex);
                        frame.Fail(ConnectionLost());
                        throw;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Shutdown(ex);
        }
    }

    /// <summary>Starts shutdown once; returns false if it had already started.</summary>
    private bool Shutdown(Exception reason, bool locally = false)
    {
        if (reason is not MqttConnectionLostException)
        {
            reason = new MqttConnectionLostException($"The connection was lost: {reason.Message}", reason);
        }

        if (Interlocked.CompareExchange(ref _closeReason, reason, null) is not null)
        {
            return false;
        }

        _closedLocally = locally;
        _outbound.Writer.TryComplete();
        _shutdown.Cancel();
        DrainOutbound();
        return true;
    }

    private void DrainOutbound()
    {
        while (_outbound.Reader.TryRead(out OutboundFrame? frame))
        {
            frame.Fail(ConnectionLost());
        }
    }

    private MqttConnectionLostException ConnectionLost() =>
        new("The connection closed before the frame was sent.", Volatile.Read(ref _closeReason));
}
