using System.Security.Authentication;

namespace RedisMQ.Broker.Server;

/// <summary>
/// The TLS listener's configuration: certificate material (PFX, or PEM certificate
/// plus key), optional mutual-TLS client verification against a CA bundle, the
/// minimum protocol and the per-connection handshake budget. Plaintext on
/// <see cref="BrokerServerOptions.Port"/> is unaffected when this is enabled, so a
/// deployment can serve 5672 and 5671 side by side.
/// </summary>
public sealed class BrokerTlsOptions
{
    /// <summary>Whether the broker should bind the TLS listener at all.</summary>
    public bool Enabled { get; set; }

    /// <summary>The TLS port; 0 asks the OS for an ephemeral port.</summary>
    public int Port { get; set; } = 5671;

    /// <summary>A PKCS#12 (PFX) file, or a PEM certificate when
    /// <see cref="CertificateKeyPath"/> is also set.</summary>
    public string? CertificatePath { get; set; }

    /// <summary>The PEM private key that pairs with a PEM <see cref="CertificatePath"/>.</summary>
    public string? CertificateKeyPath { get; set; }

    /// <summary>The PFX password, when the PKCS#12 container is encrypted.</summary>
    public string? CertificatePassword { get; set; }

    /// <summary>Require and verify a client certificate (mutual TLS).</summary>
    public bool ClientCertificateRequired { get; set; }

    /// <summary>The PEM CA bundle client certificates must chain to.</summary>
    public string? ClientCertificateAuthorityPath { get; set; }

    /// <summary>The minimum accepted protocol; TLS 1.2 also enables TLS 1.3.</summary>
    public SslProtocols MinTlsVersion { get; set; } = SslProtocols.Tls12;

    /// <summary>How long one TLS handshake may take before the socket is dropped.</summary>
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
