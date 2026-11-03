using System;
using System.Collections.Generic;
using RedisMQ.Broker.Server;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 10: the permission model (per-vhost configure/write/read regexes),
// PBKDF2 password hashing and startup validation of the account list.
public sealed class PermissionTests
{
    [Fact]
    public void Pbkdf2_hash_round_trips()
    {
        string hash = BrokerPassword.Hash("secret");

        Assert.StartsWith("pbkdf2-sha256$210000$", hash, StringComparison.Ordinal);
        Assert.Equal(4, hash.Split('$').Length);

        var user = new BrokerUser("u", PasswordHash: hash);
        Assert.True(BrokerPassword.Verify("secret", user));
        Assert.False(BrokerPassword.Verify("Secret", user));
        Assert.False(BrokerPassword.Verify("", user));
    }

    [Fact]
    public void Plaintext_password_verifies_in_fixed_time()
    {
        var user = new BrokerUser("u", "letmein");

        Assert.True(BrokerPassword.Verify("letmein", user));
        Assert.False(BrokerPassword.Verify("letmeout", user));
        Assert.False(BrokerPassword.Verify("", user));
    }

    [Fact]
    public void Malformed_or_wrong_algorithm_hash_is_refused()
    {
        string valid = BrokerPassword.Hash("secret");
        string[] malformed =
        [
            "not-a-hash",
            "bcrypt$210000$AAAA$BBBB",
            "pbkdf2-sha256$210000$only-three-parts",
            "pbkdf2-sha256$not-a-number$AAAA$BBBB",
            "pbkdf2-sha256$0$AAAA$BBBB",
            "pbkdf2-sha256$210000$not-base64$BBBB",
            "pbkdf2-sha256$210000$AAAA$BBBB$extra",
            "pbkdf2-sha256$210000$$BBBB",
            "pbkdf2-sha256$210000$AAAA$",
            valid[..^4], // truncated hash
        ];

        foreach (string hash in malformed)
        {
            var user = new BrokerUser("u", PasswordHash: hash);
            Assert.False(BrokerPassword.Verify("secret", user), $"hash should be refused: {hash}");
        }
    }

    [Fact]
    public void FullAccess_grants_configure_write_read_on_named_vhosts()
    {
        BrokerUser user = BrokerUser.FullAccess("guest", "guest", "/", "/other");

        Assert.Equal("guest", user.Name);
        Assert.Equal("guest", user.Password);
        Assert.Null(user.PasswordHash);
        Assert.Equal(2, user.Permissions!.Count);

        var principal = new BrokerPrincipal(user.Name, user.Permissions);
        Assert.True(principal.IsKnownVhost("/"));
        Assert.True(principal.IsKnownVhost("/other"));
        Assert.True(principal.CanConfigure("/", "anything"));
        Assert.True(principal.CanWrite("/", "anything"));
        Assert.True(principal.CanRead("/", "anything"));

        Assert.False(principal.IsKnownVhost("/unlisted"));
        Assert.False(principal.CanConfigure("/unlisted", "anything"));
        Assert.False(principal.CanWrite("/unlisted", "anything"));
        Assert.False(principal.CanRead("/unlisted", "anything"));
    }

    [Fact]
    public void Empty_permission_list_denies_everything()
    {
        var principal = new BrokerPrincipal("u", []);

        Assert.False(principal.IsKnownVhost("/"));
        Assert.False(principal.CanConfigure("/", "q"));
        Assert.False(principal.CanWrite("/", "q"));
        Assert.False(principal.CanRead("/", "q"));
    }

    [Fact]
    public void Principal_matches_regexes_with_culture_invariance()
    {
        var principal = new BrokerPrincipal("u",
        [
            new BrokerPermission("/", ".*", "^a\\..*$", ""),
        ]);

        Assert.True(principal.CanConfigure("/", "queue"));
        Assert.True(principal.CanWrite("/", "a.b"));
        Assert.False(principal.CanWrite("/", "b.b"));
        Assert.False(principal.CanRead("/", "q")); // the empty pattern denies non-empty names
        Assert.True(principal.CanRead("/", "")); // ... but matches the empty name itself
    }

    [Fact]
    public void First_matching_vhost_entry_wins()
    {
        var principal = new BrokerPrincipal("u",
        [
            new BrokerPermission("/", "^allowed$", ".*", ".*"),
            new BrokerPermission("/", ".*", ".*", ".*"),
        ]);

        Assert.True(principal.CanConfigure("/", "allowed"));
        Assert.False(principal.CanConfigure("/", "denied")); // never reaches the second entry
        Assert.True(principal.CanWrite("/", "anything"));
    }

    [Fact]
    public void Invalid_permission_regex_throws_BrokerConfigurationException()
    {
        Assert.Throws<BrokerConfigurationException>(() =>
            new BrokerPrincipal("u", [new BrokerPermission("/", "(", ".*", ".*")]));
        Assert.Throws<BrokerConfigurationException>(() =>
            new BrokerPrincipal("u", [new BrokerPermission("/", ".*", "[", ".*")]));
        Assert.Throws<BrokerConfigurationException>(() =>
            new BrokerPrincipal("u", [new BrokerPermission("/", ".*", ".*", "\\")]));
    }

    [Fact]
    public void Exactly_one_credential_form_is_required()
    {
        var both = new BrokerServerOptions
        {
            Users = [new BrokerUser("u", "pw", BrokerPassword.Hash("pw"))],
        };
        Assert.Throws<BrokerConfigurationException>(() => both.Validate());

        var neither = new BrokerServerOptions { Users = [new BrokerUser("u")] };
        Assert.Throws<BrokerConfigurationException>(() => neither.Validate());

        var malformedHash = new BrokerServerOptions
        {
            Users = [new BrokerUser("u", PasswordHash: "not-a-hash")],
        };
        Assert.Throws<BrokerConfigurationException>(() => malformedHash.Validate());

        var duplicateNames = new BrokerServerOptions
        {
            Users =
            [
                BrokerUser.FullAccess("u", "pw", "/"),
                BrokerUser.FullAccess("u", "pw", "/"),
            ],
        };
        Assert.Throws<BrokerConfigurationException>(() => duplicateNames.Validate());

        var invalidRegex = new BrokerServerOptions
        {
            Users =
            [
                new BrokerUser("u", "pw", Permissions: [new BrokerPermission("/", "(", ".*", ".*")]),
            ],
        };
        Assert.Throws<BrokerConfigurationException>(() => invalidRegex.Validate());

        // A valid account list passes.
        new BrokerServerOptions
        {
            Users =
            [
                BrokerUser.FullAccess("guest", "guest", "/"),
                new BrokerUser("hashed", PasswordHash: BrokerPassword.Hash("secret"), Permissions: []),
            ],
        }.Validate();
    }

    [Fact]
    public void Regex_timeout_denies_instead_of_hanging()
    {
        var principal = new BrokerPrincipal("u",
        [
            new BrokerPermission("/", "^(a+)+$", ".*", ".*"),
        ]);

        string pathological = new string('a', 100_000) + "!";
        Assert.False(principal.CanConfigure("/", pathological));
    }
}
