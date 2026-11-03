using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace RedisMQ.Broker.Tests;

/// <summary>
/// Generates the TLS material the transport tests need under a throwaway temp
/// directory: a self-signed <c>localhost</c> server certificate (PFX and PEM+key), plus
/// a test CA and a CA-signed client certificate for mutual TLS. Every file is removed
/// on dispose.
/// </summary>
public sealed class TestCertificates : IDisposable
{
    /// <summary>The password protecting the generated PFX containers.</summary>
    public const string Password = "redismq-test";

    private static readonly DateTimeOffset NotBefore = DateTimeOffset.UtcNow.AddDays(-1);
    private static readonly DateTimeOffset NotAfter = DateTimeOffset.UtcNow.AddDays(7);

    private readonly string _directory;

    public TestCertificates()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"redismq-tls-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);

        using (RSA serverKey = RSA.Create(2048))
        {
            CertificateRequest request = ServerRequest(serverKey);
            using X509Certificate2 certificate = request.CreateSelfSigned(NotBefore, NotAfter);
            ServerPfxPath = Write("server.pfx", certificate.Export(X509ContentType.Pkcs12, Password));
            ServerCertPemPath = WriteText("server.cert.pem", certificate.ExportCertificatePem());
            ServerKeyPemPath = WriteText("server.key.pem", serverKey.ExportPkcs8PrivateKeyPem());
        }

        using (RSA caKey = RSA.Create(2048))
        {
            var caRequest = new CertificateRequest(
                "CN=RedisMQ Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, true, 0, critical: true));
            caRequest.CertificateExtensions.Add(
                new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, critical: true));
            using X509Certificate2 ca = caRequest.CreateSelfSigned(NotBefore, NotAfter);
            CaPemPath = WriteText("ca.pem", ca.ExportCertificatePem());

            using RSA clientKey = RSA.Create(2048);
            CertificateRequest clientRequest = ClientRequest(clientKey);
            using X509Certificate2 issued = clientRequest.Create(
                ca, NotBefore, NotAfter, RandomNumberGenerator.GetBytes(16));
            using X509Certificate2 client = issued.CopyWithPrivateKey(clientKey);
            ClientPfxPath = Write("client.pfx", client.Export(X509ContentType.Pkcs12, Password));
            ClientCertPemPath = WriteText("client.cert.pem", client.ExportCertificatePem());
            ClientKeyPemPath = WriteText("client.key.pem", clientKey.ExportPkcs8PrivateKeyPem());
            ClientCertificate = X509CertificateLoader.LoadPkcs12FromFile(ClientPfxPath, Password);
        }
    }

    public string ServerPfxPath { get; }

    public string ServerCertPemPath { get; }

    public string ServerKeyPemPath { get; }

    public string CaPemPath { get; }

    public string ClientPfxPath { get; }

    public string ClientCertPemPath { get; }

    public string ClientKeyPemPath { get; }

    /// <summary>The CA-signed client certificate with its private key, ready for an
    /// mTLS client handshake.</summary>
    public X509Certificate2 ClientCertificate { get; }

    public void Dispose()
    {
        ClientCertificate.Dispose();
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch
        {
            // Best effort: a locked file on Windows must not fail a test teardown.
        }
    }

    private static CertificateRequest ServerRequest(RSA key)
    {
        var request = new CertificateRequest(
            "CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        AddLocalhostSubjectAlternativeName(request);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.1") }, critical: false)); // serverAuth
        return request;
    }

    private static CertificateRequest ClientRequest(RSA key)
    {
        var request = new CertificateRequest(
            "CN=localhost-client", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        AddLocalhostSubjectAlternativeName(request);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature, critical: false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.2") }, critical: false)); // clientAuth
        return request;
    }

    private static void AddLocalhostSubjectAlternativeName(CertificateRequest request)
    {
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        request.CertificateExtensions.Add(san.Build());
    }

    private string Write(string name, byte[] contents)
    {
        string path = Path.Combine(_directory, name);
        File.WriteAllBytes(path, contents);
        return path;
    }

    private string WriteText(string name, string contents) =>
        Write(name, System.Text.Encoding.UTF8.GetBytes(contents));
}
