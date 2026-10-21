using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Mqtt.Internal.Codec;

namespace Mqtt.Tests.TestSupport;

/// <summary>
/// Loopback TLS server that requires a client certificate and lets a test read and write raw
/// MQTT frames. The scripted fake broker for integration tests will build on this.
/// </summary>
internal sealed class TlsTestServer : IDisposable
{
    private readonly TcpListener _listener;

    public TlsTestServer()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public async Task<ServerConnection> AcceptAsync(CancellationToken ct = default)
    {
        Socket socket = await _listener.AcceptSocketAsync(ct);
        var stream = new SslStream(new NetworkStream(socket, ownsSocket: true));
        X509Certificate? clientCertificate = null;
        await stream.AuthenticateAsServerAsync(
            new SslServerAuthenticationOptions
            {
                ServerCertificate = TestCertificates.Server,
                ClientCertificateRequired = true,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    clientCertificate = certificate;
                    return certificate?.GetCertHashString() == TestCertificates.Client.Thumbprint;
                },
            },
            ct);
        return new ServerConnection(stream, clientCertificate);
    }

    /// <summary>Accepts TCP but never starts TLS, to test handshake timeouts.</summary>
    public async Task<Socket> AcceptRawAsync(CancellationToken ct = default) => await _listener.AcceptSocketAsync(ct);

    public void Dispose() => _listener.Dispose();
}

internal sealed class ServerConnection(SslStream stream, X509Certificate? clientCertificate) : IAsyncDisposable
{
    public SslStream Stream => stream;

    public X509Certificate? ClientCertificate => clientCertificate;

    public async Task WriteAsync(byte[] bytes)
    {
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    /// <summary>Reads exactly one frame, or returns null at end of stream.</summary>
    public async Task<byte[]?> ReadFrameAsync(CancellationToken ct = default)
    {
        var header = new List<byte>(FixedHeader.MaxSize);
        var one = new byte[1];
        while (true)
        {
            if (await stream.ReadAsync(one, ct) == 0)
            {
                return null;
            }

            header.Add(one[0]);
            FrameStatus status = FixedHeader.TryRead(header.ToArray(), out _, out int remainingLength, out _);
            if (status == FrameStatus.Malformed)
            {
                throw new InvalidDataException("Malformed fixed header from client.");
            }

            if (status == FrameStatus.Complete)
            {
                var frame = new byte[header.Count + remainingLength];
                header.CopyTo(frame);
                await stream.ReadExactlyAsync(frame.AsMemory(header.Count), ct);
                return frame;
            }
        }
    }

    public ValueTask DisposeAsync() => stream.DisposeAsync();
}
