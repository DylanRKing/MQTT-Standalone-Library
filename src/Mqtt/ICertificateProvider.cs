using System.Security.Cryptography.X509Certificates;

namespace Mqtt;

/// <summary>
/// Supplies the X.509 client certificate used for TLS mutual authentication with the broker.
/// </summary>
/// <remarks>
/// <para>Called on every connect and reconnect, so an implementation can rotate certificates
/// without restarting the client.</para>
/// <para>The returned certificate must have its private key attached. The provider owns the
/// certificate's lifetime; the library never disposes it.</para>
/// <para>On Windows, SChannel needs a persisted key: a key loaded from PEM must be persisted
/// (certificate store or PFX) before TLS can use it.</para>
/// </remarks>
public interface ICertificateProvider
{
    /// <summary>Returns the client certificate, with private key, for the next connection.</summary>
    ValueTask<X509Certificate2> GetClientCertificateAsync(CancellationToken ct);
}
