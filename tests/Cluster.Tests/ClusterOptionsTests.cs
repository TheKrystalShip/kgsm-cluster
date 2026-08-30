using Microsoft.Extensions.Configuration;
using TheKrystalShip.KGSM.Cluster;

namespace TheKrystalShip.KGSM.Cluster.Tests;

public class ClusterOptionsTests
{
    private static ClusterOptions Base(int retryTtlDays, int retentionDays) => new()
    {
        MemberId = "a",
        Secret = "s",
        StorePath = "ignored.db",
        RetryTtlDays = retryTtlDays,
        RetentionDays = retentionDays,
    };

    [Fact]
    public void RetentionIsRaisedAboveTheRetryTtl()
    {
        // A retention window shorter than the retry TTL would let the ledger forget a message the
        // outbox is still retrying, and the redelivery would apply a second time.
        ClusterOptions options = Base(retryTtlDays: 7, retentionDays: 3).Validate();
        Assert.True(options.RetentionDays > options.RetryTtlDays);
    }

    [Fact]
    public void ARetentionWindowAlreadyPastTheTtlIsLeftAlone()
    {
        ClusterOptions options = Base(retryTtlDays: 7, retentionDays: 30).Validate();
        Assert.Equal(30, options.RetentionDays);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void TheRetryTtlHasAFloorOfOneDay(int configured)
    {
        Assert.Equal(1, Base(configured, 30).Validate().RetryTtlDays);
    }

    [Fact]
    public void ABlankSecretMeansUnclustered()
    {
        var options = new ClusterOptions { MemberId = "a", Secret = "  ", StorePath = "x.db" };
        Assert.False(options.Enabled);
    }

    [Fact]
    public void ASecretMeansClustered()
    {
        Assert.True(new ClusterOptions { MemberId = "a", Secret = "s", StorePath = "x.db" }.Enabled);
    }
}

public class ClusterConfigurationTests
{
    private static IConfiguration From(params (string Key, string Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.ToDictionary(p => p.Key, p => (string?)p.Value))
            .Build();

    [Fact]
    public void TheSecretIsReadFromTheSectionThePackageOwns()
    {
        IConfiguration configuration = From(("Cluster:Secret", "shared"));
        Assert.Equal("shared", ClusterConfiguration.Secret(configuration));
    }

    [Fact]
    public void AMemberHoldingNoSecretReadsAnEmptyOneRatherThanNull()
    {
        // No secret is "not part of a cluster", which everything downstream treats as a state rather
        // than as something missing.
        Assert.Equal("", ClusterConfiguration.Secret(From()));
        Assert.Equal("", ClusterConfiguration.SecretPrevious(From()));
    }

    [Fact]
    public void SurroundingWhitespaceIsNotPartOfTheSecret()
    {
        // A shared env file is edited by hand, and a trailing space would otherwise derive a different
        // signing key on one member than on every other — which presents as a cluster that authenticates
        // nobody, with nothing in any log naming the reason.
        IConfiguration configuration = From(("Cluster:Secret", "  shared  "));
        Assert.Equal("shared", ClusterConfiguration.Secret(configuration));
    }

    [Fact]
    public void ABlankSecretIsTheSameAsNone()
    {
        Assert.Equal("", ClusterConfiguration.Secret(From(("Cluster:Secret", "   "))));
    }

    [Fact]
    public void ThePreviousSecretIsReadSeparately()
    {
        IConfiguration configuration = From(("Cluster:Secret", "new"), ("Cluster:SecretPrevious", "old"));
        Assert.Equal("new", ClusterConfiguration.Secret(configuration));
        Assert.Equal("old", ClusterConfiguration.SecretPrevious(configuration));
    }

    [Fact]
    public void TheEnvironmentSpellingReachesTheKey()
    {
        // Cluster__Secret in a unit's EnvironmentFile has to land on Cluster:Secret here. If it does not,
        // the shared file is read by nobody and every member decides separately, and silently, that it is
        // not part of a cluster.
        string name = $"Cluster__Secret";
        try
        {
            Environment.SetEnvironmentVariable(name, "from-the-shared-file");
            IConfiguration configuration = new ConfigurationBuilder().AddEnvironmentVariables().Build();
            Assert.Equal("from-the-shared-file", ClusterConfiguration.Secret(configuration));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }
}
