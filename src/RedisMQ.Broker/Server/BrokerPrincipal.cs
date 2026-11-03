namespace RedisMQ.Broker.Server;

using System.Text.RegularExpressions;

/// <summary>
/// An authenticated account's resolved permission set: the user name plus one compiled
/// configure/write/read regex triple per vhost. The triples are compiled once at
/// construction with <see cref="RegexOptions.CultureInvariant"/> and a one-second match
/// timeout; an invalid pattern throws <see cref="BrokerConfigurationException"/>, and a
/// match that times out denies instead of hanging the channel.
/// </summary>
public sealed class BrokerPrincipal
{
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    private readonly Entry[] _entries;

    public BrokerPrincipal(string name, IReadOnlyList<BrokerPermission> permissions)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(permissions);
        Name = name;
        _entries = [.. permissions.Select(permission => new Entry(
            permission.Vhost,
            Compile(permission.Vhost, "configure", permission.Configure),
            Compile(permission.Vhost, "write", permission.Write),
            Compile(permission.Vhost, "read", permission.Read)))];
    }

    public string Name { get; }

    /// <summary>True when the account has any permission entry for this vhost, even a
    /// deny-all one: a listed vhost is openable, merely restricted.</summary>
    public bool IsKnownVhost(string vhost) => Find(vhost) is not null;

    public bool CanConfigure(string vhost, string name) => Matches(vhost, name, static entry => entry.Configure);

    public bool CanWrite(string vhost, string name) => Matches(vhost, name, static entry => entry.Write);

    public bool CanRead(string vhost, string name) => Matches(vhost, name, static entry => entry.Read);

    /// <summary>First matching vhost entry wins; no fallback to a later duplicate.</summary>
    private bool Matches(string vhost, string name, Func<Entry, Regex> select)
    {
        Entry? entry = Find(vhost);
        if (entry is null)
        {
            return false;
        }

        try
        {
            return select(entry).IsMatch(name);
        }
        catch (RegexMatchTimeoutException)
        {
            return false; // never hang a channel on a pathological name/pattern
        }
    }

    private Entry? Find(string vhost)
    {
        foreach (Entry entry in _entries)
        {
            if (string.Equals(entry.Vhost, vhost, StringComparison.Ordinal))
            {
                return entry;
            }
        }

        return null;
    }

    private static Regex Compile(string vhost, string field, string pattern)
    {
        // An empty pattern is an explicit deny, not "match everything": .NET's empty
        // regex matches at position 0 of any string, so anchor it to the empty name.
        string effective = pattern.Length == 0 ? "\\A\\z" : pattern;
        try
        {
            return new Regex(effective, RegexOptions.CultureInvariant, MatchTimeout);
        }
        catch (ArgumentException exception)
        {
            throw new BrokerConfigurationException(
                $"Invalid {field} permission regex '{pattern}' for vhost '{vhost}': {exception.Message}",
                exception);
        }
    }

    private sealed record Entry(string Vhost, Regex Configure, Regex Write, Regex Read);
}
