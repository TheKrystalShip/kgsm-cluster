using Microsoft.Extensions.Logging.Abstractions;
using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Membership;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// A member's cluster state belongs to the cluster whose secret it was learned under, and a machine
/// founded a cluster only while it still holds that cluster's secret.
/// </summary>
public class SecretBindingTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"kgsm-cluster-secret-{Guid.NewGuid():N}");

    public SecretBindingTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private string StorePath => Path.Combine(_dir, "cluster.db");

    private ClusterOptions Options(string secret, string previous = "") => new()
    {
        MemberId = "node-a", Secret = secret, SecretPrevious = previous, StorePath = StorePath,
        FoundedPath = Path.Combine(_dir, "cluster-founded"),
    };

    /// <summary>Opens the file as a member holding <paramref name="options"/> would at startup.</summary>
    private static async Task<(MembersStore Members, ClusterStateStore State)> OpenAsync(ClusterOptions options)
    {
        var store = new ClusterStore(options, NullLogger<ClusterStore>.Instance);
        await store.StartAsync(default);
        return (new MembersStore(store), new ClusterStateStore(store, options));
    }

    private static MemberRow Row(string memberId) =>
        MemberRow.New(memberId, MemberKind.Anchor) with { Url = $"http://{memberId}:8098" };

    private async Task LearnAClusterAsync(ClusterOptions options)
    {
        (MembersStore members, ClusterStateStore state) = await OpenAsync(options);
        await members.UpsertAsync(Row("old-auth"), default);
        Assert.True(await state.TryClaimAsync(ClusterCapability.Auth, "old-auth", default));
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
    }

    [Fact]
    public async Task ADifferentSecretOpensAFileEmptiedOfTheOldClusterState()
    {
        // The hazard: the old cluster's assignment of the accounts, gossiped into the new cluster as if
        // current, where the tie-break can hand it the new cluster's accounts.
        await LearnAClusterAsync(Options("first-cluster"));

        (MembersStore members, ClusterStateStore state) = await OpenAsync(Options("second-cluster"));

        Assert.Empty(await members.ListAsync(default));
        Assert.Null(await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task TheSameSecretKeepsEverything()
    {
        await LearnAClusterAsync(Options("first-cluster"));

        (MembersStore members, ClusterStateStore state) = await OpenAsync(Options("first-cluster"));

        Assert.Single(await members.ListAsync(default));
        Assert.Equal("old-auth", await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task ARotationIsTheSameClusterAndKeepsEverything()
    {
        await LearnAClusterAsync(Options("first-cluster"));

        (MembersStore members, ClusterStateStore state) =
            await OpenAsync(Options("rotated-secret", previous: "first-cluster"));

        Assert.Single(await members.ListAsync(default));
        Assert.Equal("old-auth", await state.HolderAsync(ClusterCapability.Auth, default));

        // And the rotated secret is now the one the file answers to, once the overlap is cleared.
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        (members, _) = await OpenAsync(Options("rotated-secret"));
        Assert.Single(await members.ListAsync(default));
    }

    [Fact]
    public void TheFingerprintIsWhatTheInstallWritesWithSha256sum()
    {
        // printf '%s\n' "founding-secret" | sha256sum
        Assert.Equal("bbfdc5f663d9b9a550629d2f8fd483595a81d00d60e4058d000bca8055e69d67",
            ClusterFounding.Fingerprint("founding-secret"));
    }

    [Fact]
    public void AMachineFoundedTheClusterOnlyWhileItHoldsTheSecretItFoundedWith()
    {
        ClusterOptions founding = Options("founding-secret");
        Assert.False(ClusterFounding.IsFoundedHere(founding));

        File.WriteAllText(founding.FoundedPath, ClusterFounding.Fingerprint("founding-secret") + "\n");
        Assert.True(ClusterFounding.IsFoundedHere(founding));

        // The same machine after taking another cluster's secret: the record stays, and names a secret
        // it no longer holds.
        Assert.False(ClusterFounding.IsFoundedHere(Options("joined-cluster")));
        Assert.False(ClusterFounding.IsFoundedHere(Options("")));
    }
}
