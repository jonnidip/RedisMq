namespace RedisMQ.Broker.Server;

using System.Security.Cryptography;
using System.Text;

/// <summary>
/// The PBKDF2 password helper for broker accounts. Hashes are encoded as
/// <c>pbkdf2-sha256$&lt;iterations&gt;$&lt;base64 salt&gt;$&lt;base64 hash&gt;</c> with
/// SHA-256, 210,000 iterations, a 16-byte random salt and a 32-byte hash. Verification
/// compares fixed-time; a malformed or unsupported hash simply refuses the login.
/// </summary>
public static class BrokerPassword
{
    private const string Algorithm = "pbkdf2-sha256";
    private const int Iterations = 210_000;
    private const int SaltBytes = 16;
    private const int HashBytes = 32;

    /// <summary>Hashes a plaintext password into the configuration encoding.</summary>
    public static string Hash(string password)
    {
        ArgumentNullException.ThrowIfNull(password);
        byte[] salt = RandomNumberGenerator.GetBytes(SaltBytes);
        byte[] hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA256, HashBytes);
        return $"{Algorithm}${Iterations}${Convert.ToBase64String(salt)}${Convert.ToBase64String(hash)}";
    }

    /// <summary>
    /// Verifies a plaintext password against the user's configured credential form,
    /// comparing fixed-time. A user with no credential (or a malformed hash) never
    /// verifies.
    /// </summary>
    public static bool Verify(string password, BrokerUser user)
    {
        ArgumentNullException.ThrowIfNull(password);
        ArgumentNullException.ThrowIfNull(user);

        if (user.PasswordHash is string encoded)
        {
            return VerifyHash(password, encoded);
        }

        return user.Password is string plaintext && FixedTimeEquals(plaintext, password);
    }

    /// <summary>True when the encoded hash has the expected shape and decodable payload.
    /// Startup validation uses this to reject a malformed account list.</summary>
    internal static bool IsWellFormed(string encoded)
    {
        if (!TryParse(encoded, out _, out byte[] salt, out byte[] expected))
        {
            return false;
        }

        return salt.Length == SaltBytes && expected.Length == HashBytes;
    }

    private static bool VerifyHash(string password, string encoded)
    {
        if (!TryParse(encoded, out int iterations, out byte[] salt, out byte[] expected))
        {
            return false;
        }

        if (salt.Length != SaltBytes || expected.Length != HashBytes)
        {
            return false;
        }

        byte[] actual = Rfc2898DeriveBytes.Pbkdf2(
            password, salt, iterations, HashAlgorithmName.SHA256, expected.Length);
        return CryptographicOperations.FixedTimeEquals(actual, expected);
    }

    private static bool TryParse(string? encoded, out int iterations, out byte[] salt, out byte[] expected)
    {
        iterations = 0;
        salt = [];
        expected = [];

        string[] parts = (encoded ?? string.Empty).Split('$');
        if (parts.Length != 4
            || !string.Equals(parts[0], Algorithm, StringComparison.Ordinal)
            || !int.TryParse(parts[1], out iterations)
            || iterations <= 0)
        {
            return false;
        }

        try
        {
            salt = Convert.FromBase64String(parts[2]);
            expected = Convert.FromBase64String(parts[3]);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <summary>Compares two strings without an early exit on the first differing byte
    /// (both sides are UTF-8 encoded; unequal lengths still refuse).</summary>
    private static bool FixedTimeEquals(string left, string right)
    {
        byte[] leftBytes = Encoding.UTF8.GetBytes(left);
        byte[] rightBytes = Encoding.UTF8.GetBytes(right);
        return leftBytes.Length == rightBytes.Length
            && CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
    }
}
