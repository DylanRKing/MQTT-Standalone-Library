using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Mqtt.Tests.TestSupport;

/// <summary>Self-signed certificates for loopback TLS tests, created once per test run.</summary>
internal static class TestCertificates
{
    private static readonly Lazy<X509Certificate2> LazyServer = new(() => Create("CN=localhost", serverAuth: true));
    private static readonly Lazy<X509Certificate2> LazyClient = new(() => Create("CN=test-device", serverAuth: false));

    public static X509Certificate2 Server => LazyServer.Value;

    public static X509Certificate2 Client => LazyClient.Value;

    /// <summary>A certificate with no private key, as a misconfigured provider might return.</summary>
    public static X509Certificate2 ClientWithoutKey => X509CertificateLoader.LoadCertificate(Client.RawData);

    private static X509Certificate2 Create(string subject, bool serverAuth)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [serverAuth ? new Oid("1.3.6.1.5.5.7.3.1") : new Oid("1.3.6.1.5.5.7.3.2")],
            critical: false));
        if (serverAuth)
        {
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName("localhost");
            request.CertificateExtensions.Add(san.Build());
        }

        using X509Certificate2 ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        // Round-trip through PFX so the key is usable by SChannel on Windows as well as OpenSSL.
        return X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), password: null);
    }
}

/// <summary>Temporary certificate provider for tests; counts how often it is asked.</summary>
internal sealed class TestCertificateProvider(X509Certificate2 certificate) : ICertificateProvider
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    public ValueTask<X509Certificate2> GetClientCertificateAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref _calls);
        return ValueTask.FromResult(certificate);
    }
}
