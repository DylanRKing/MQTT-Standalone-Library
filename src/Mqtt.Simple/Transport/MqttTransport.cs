using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Mqtt.Simple.Codec;

namespace Mqtt.Simple.Transport;

/// <summary>
/// A TLS connection to AWS IoT Core that reads and writes whole MQTT packets. Writes are
/// serialised so packets never interleave; reads must come from one caller at a time.
/// One instance is one connection: create a new one to connect again.
/// </summary>
internal sealed class MqttTransport(string host, int port, X509Certificate2 certificate, X509Certificate2? rootCa) : IAsyncDisposable
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly byte[] _oneByte = new byte[1];
    private SslStream? _stream;

    private SslStream Stream => _stream ?? throw new InvalidOperationException("The transport is not connected.");

    /// <summary>
    /// Opens TCP and completes the TLS handshake with the device certificate. With a root CA
    /// the server is trusted only if its chain ends at that CA; without one the operating
    /// system's trust store is used.
    /// </summary>
    public async Task ConnectAsync(CancellationToken ct)
    {
        var tcp = new TcpClient { NoDelay = true };
        SslStream? stream = null;
        try
        {
            await tcp.ConnectAsync(host, port, ct);
            stream = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false);
            await stream.AuthenticateAsClientAsync(
                new SslClientAuthenticationOptions
                {
                    TargetHost = host,
                    ClientCertificates = [certificate],
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateChainPolicy = rootCa is null ? null : new X509ChainPolicy
                    {
                        TrustMode = X509ChainTrustMode.CustomRootTrust,
                        CustomTrustStore = { rootCa },
                        RevocationMode = X509RevocationMode.NoCheck,
                    },
                },
                ct);
            _stream = stream;
        }
        catch
        {
            if (stream is not null)
            {
                await stream.DisposeAsync();
            }

            tcp.Dispose();
            throw;
        }
    }

    /// <summary>Writes one complete frame.</summary>
    /// <exception cref="IOException">The connection is closed or failed.</exception>
    public async Task WriteAsync(byte[] frame, CancellationToken ct)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            await Stream.WriteAsync(frame, ct);
        }
        catch (ObjectDisposedException ex)
        {
            throw new IOException("The connection is closed.", ex);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>Reads the next packet. Throws when the connection closes.</summary>
    public async Task<Packet> ReadAsync(CancellationToken ct)
    {
        byte header = await ReadByteAsync(ct);

        // Remaining length: 7 bits per byte, high bit set when another byte follows.
        int length = 0;
        for (int shift = 0; ; shift += 7)
        {
            byte digit = await ReadByteAsync(ct);
            length |= (digit & 0x7F) << shift;
            if ((digit & 0x80) == 0)
            {
                break;
            }
        }

        var body = new byte[length];
        await Stream.ReadExactlyAsync(body, ct);
        return new Packet(header, body);
    }

    public ValueTask DisposeAsync() => _stream?.DisposeAsync() ?? ValueTask.CompletedTask;

    private async Task<byte> ReadByteAsync(CancellationToken ct)
    {
        await Stream.ReadExactlyAsync(_oneByte, ct);
        return _oneByte[0];
    }
}
