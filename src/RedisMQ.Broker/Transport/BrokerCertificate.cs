using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using RedisMQ.Broker.Server;

namespace RedisMQ.Broker.Transport;

/// <summary>
/// The broker's loaded server certificate plus, for mutual TLS, the CA bundle client
/// certificates must chain to. Loading happens once at startup: a PFX is read with
/// <see cref="X509CertificateLoader"/>, a PEM pair is read with
/// <see cref="X509Certificate2.CreateFromPemFile"/> and re-exported into an ephemeral
/// PKCS#12 context (SslStream requires a cert whose key is usable on the platform).
/// Every malformed or missing input throws <see cref="BrokerConfigurationException"/>.
/// </summary>
public sealed class BrokerCertificate : IDisposable
{
    private readonly BrokerTlsOptions _options;
    private readonly X509Certificate2Collection? _clientAuthorities;

    private BrokerCertificate(
        X509Certificate2 certificate,
        BrokerTlsOptions options,
        X509Certificate2Collection? clientAuthorities)
    {
        Certificate = certificate;
        _options = options;
        _clientAuthorities = clientAuthorities;
    }

    /// <summary>The loaded server certificate (always with a private key).</summary>
    public X509Certificate2 Certificate { get; }

    /// <summary>Loads the configured certificate material, validating every path and
    /// secret up front. Throws <see cref="BrokerConfigurationException"/> when the
    /// broker cannot serve TLS with this configuration.</summary>
    public static BrokerCertificate Load(BrokerTlsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        string path = RequireFile(options.CertificatePath, "TLS certificate");
        X509Certificate2 certificate;
        if (options.CertificateKeyPath is { Length: > 0 } keyPath)
        {
            RequireFile(keyPath, "TLS certificate key");
            try
            {
                using X509Certificate2 fromPem = X509Certificate2.CreateFromPemFile(path, keyPath);
                certificate = ReloadPortable(fromPem);
            }
            catch (CryptographicException exception)
            {
                throw new BrokerConfigurationException(
                    $"Could not load the TLS PEM certificate '{path}' with key '{keyPath}': {exception.Message}",
                    exception);
            }
        }
        else
        {
            try
            {
                certificate = X509CertificateLoader.LoadPkcs12FromFile(
                    path, options.CertificatePassword, KeyStorageFlags);
            }
            catch (CryptographicException exception)
            {
                throw new BrokerConfigurationException(
                    $"Could not load the TLS PFX certificate '{path}': {exception.Message}",
                    exception);
            }
        }

        if (!certificate.HasPrivateKey)
        {
            certificate.Dispose();
            throw new BrokerConfigurationException(
                $"The TLS certificate '{path}' has no private key; supply the key (PEM) or a PFX with its key");
        }

        X509Certificate2Collection? authorities = null;
        if (options.ClientCertificateRequired)
        {
            string caPath = RequireFile(
                options.ClientCertificateAuthorityPath, "TLS client certificate authority");
            authorities = new X509Certificate2Collection();
            try
            {
                authorities.ImportFromPemFile(caPath);
            }
            catch (CryptographicException exception)
            {
                certificate.Dispose();
                throw new BrokerConfigurationException(
                    $"Could not load the TLS client CA bundle '{caPath}': {exception.Message}",
                    exception);
            }

            if (authorities.Count == 0)
            {
                certificate.Dispose();
                throw new BrokerConfigurationException(
                    $"The TLS client CA bundle '{caPath}' contains no certificates");
            }
        }

        return new BrokerCertificate(certificate, options, authorities);
    }

    /// <summary>Builds the per-connection server authentication options. The client
    /// validation callback is only installed for mutual TLS; it chains the presented
    /// certificate to the configured CA bundle with revocation checks disabled.</summary>
    public SslServerAuthenticationOptions CreateAuthenticationOptions()
    {
        var options = new SslServerAuthenticationOptions
        {
            ServerCertificate = Certificate,
            ClientCertificateRequired = _options.ClientCertificateRequired,
            EnabledSslProtocols = SupportedProtocols(_options.MinTlsVersion),
        };

        if (_options.ClientCertificateRequired)
        {
            options.RemoteCertificateValidationCallback = ValidateClientCertificate;
        }

        return options;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Certificate.Dispose();
        if (_clientAuthorities is not null)
        {
            foreach (X509Certificate2 authority in _clientAuthorities)
            {
                authority.Dispose();
            }
        }
    }

    private bool ValidateClientCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors errors)
    {
        if (certificate is null || _clientAuthorities is null)
        {
            return false;
        }

        using X509Certificate2 client = certificate as X509Certificate2
            ?? X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
        using var custom = new X509Chain();
        custom.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        custom.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        custom.ChainPolicy.CustomTrustStore.AddRange(_clientAuthorities);
        return custom.Build(client);
    }

    /// <summary>TLS 1.2 as the floor also admits TLS 1.3; asking for 1.3 admits only
    /// 1.3.</summary>
    private static SslProtocols SupportedProtocols(SslProtocols minimum) =>
        minimum.HasFlag(SslProtocols.Tls13)
            ? SslProtocols.Tls13
            : SslProtocols.Tls12 | SslProtocols.Tls13;

    /// <summary>Windows Schannel cannot serve a certificate whose key lives only in
    /// memory ("the platform does not support ephemeral keys"), so Windows uses a
    /// persisted user key container; Unix keeps the ephemeral set.</summary>
    private static X509KeyStorageFlags KeyStorageFlags =>
        OperatingSystem.IsWindows()
            ? X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable
            : X509KeyStorageFlags.EphemeralKeySet;

    /// <summary>Re-imports a PEM-loaded certificate through PKCS#12 so its private key
    /// lives in the platform's SslStream-compatible key set.</summary>
    private static X509Certificate2 ReloadPortable(X509Certificate2 certificate)
    {
        byte[] pfx = certificate.Export(X509ContentType.Pkcs12);
        return X509CertificateLoader.LoadPkcs12(pfx, password: null, KeyStorageFlags);
    }

    private static string RequireFile(string? path, string description)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new BrokerConfigurationException(
                $"TLS is enabled but no {description.ToLowerInvariant()} path is configured");
        }

        if (!File.Exists(path))
        {
            throw new BrokerConfigurationException($"The {description.ToLowerInvariant()} '{path}' does not exist");
        }

        return path;
    }
}
