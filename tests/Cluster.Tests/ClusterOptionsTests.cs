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
