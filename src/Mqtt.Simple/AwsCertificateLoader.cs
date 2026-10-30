using System.Security.Cryptography.X509Certificates;

namespace Mqtt.Simple;

/// <summary>Loads the PEM files AWS gives you when you create a thing.</summary>
public sealed class AwsCertificateLoader
{
    /// <summary>
    /// Loads the device certificate and its private key. Re-imports through PKCS#12 because
    /// Windows TLS cannot use a key loaded straight from PEM.
    /// </summary>
    public X509Certificate2 LoadDeviceCertificate(string certificatePemPath, string privateKeyPemPath)
    {
        using X509Certificate2 pem = X509Certificate2.CreateFromPemFile(certificatePemPath, privateKeyPemPath);
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), password: null);
    }

    /// <summary>Loads a CA certificate from a PEM file, such as <c>AmazonRootCA1.pem</c>.</summary>
    public X509Certificate2 LoadRootCa(string pemPath) =>
        X509Certificate2.CreateFromPem(File.ReadAllText(pemPath));
}
