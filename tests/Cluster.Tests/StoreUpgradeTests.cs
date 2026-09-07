using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Membership;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// Opening a store written by an earlier build. Every member that has ever been in a cluster has one, so
/// this is the path an upgrade actually takes.
/// </summary>
public class StoreUpgradeTests : IDisposable
{
    private readonly string _path =
        Path.Combine(Path.GetTempPath(), $"kgsm-cluster-upgrade-{Guid.NewGuid():N}.db");

    private ClusterOptions Options => new()
    {
        MemberId = "node-b", Secret = "upgrade-secret", StorePath = _path,
    };

    private async Task WriteAsync(string sql)
    {
        await using var connection = new SqliteConnection(
            new SqliteConnectionStringBuilder { DataSource = _path }.ToString());
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>The members table as it shipped before per-member published facts existed.</summary>
    private const string MembersBeforePublishedFacts = """
        CREATE TABLE members (
            id TEXT PRIMARY KEY, member_id TEXT NOT NULL, kind TEXT NOT NULL, url TEXT NOT NULL,
            candidates TEXT NOT NULL, address_verified INTEGER NOT NULL, nickname TEXT NULL,
            incarnation INTEGER NOT NULL, status TEXT NOT NULL, membership_state TEXT NOT NULL,
            state_changed_at TEXT NULL, latency_ms INTEGER NULL, last_seen TEXT NULL,
            api_version TEXT NOT NULL, enabled INTEGER NOT NULL
        );
        INSERT INTO members VALUES
            ('m1','hotrod','node','http://hotrod:8080','',0,NULL,3,'reachable','alive',NULL,2,NULL,'v1',1);
        """;

    [Fact]
    public async Task AStoreFromBeforeAColumnExistedGainsItRatherThanFailingEveryQuery()
    {
        // The failure this replaces: the table exists, so CREATE TABLE IF NOT EXISTS leaves it alone, and
        // every query naming the new column throws once per tick — in a member that starts, serves and
        // answers health while being silently absent from the mesh.
        await WriteAsync(MembersBeforePublishedFacts);

        var store = new ClusterStore(Options, NullLogger<ClusterStore>.Instance);
        var members = new MembersStore(store);

        IReadOnlyList<MemberRow> rows = await members.ListAsync(default);

        Assert.Single(rows);
        Assert.Equal("hotrod", rows[0].MemberId);
        // The row it already held survives the addition intact.
        Assert.Equal(3, rows[0].Incarnation);
        Assert.Equal(2, rows[0].LatencyMs);
        Assert.Equal("", rows[0].Published);
    }

    [Fact]
    public async Task AnUpgradedStoreThenWorksNormally()
    {
        await WriteAsync(MembersBeforePublishedFacts);
        var store = new ClusterStore(Options, NullLogger<ClusterStore>.Instance);
        var members = new MembersStore(store);

        MemberRow row = (await members.GetByMemberIdAsync("hotrod", default))!;
        await members.UpsertAsync(row with { Published = PublishedFacts.Encode(
            new Dictionary<string, string> { ["auth.publickey"] = "k" }) }, default);

        Assert.Equal("k", (await members.GetByMemberIdAsync("hotrod", default))!.Read("auth.publickey"));
    }

    [Fact]
    public async Task ReopeningAnAlreadyUpgradedStoreChangesNothing()
    {
        await WriteAsync(MembersBeforePublishedFacts);
        _ = await new MembersStore(new ClusterStore(Options, NullLogger<ClusterStore>.Instance))
            .ListAsync(default);

        // A second build opening the same file must not try the addition again.
        IReadOnlyList<MemberRow> rows =
            await new MembersStore(new ClusterStore(Options, NullLogger<ClusterStore>.Instance))
                .ListAsync(default);
        Assert.Single(rows);
    }

    [Fact]
    public async Task AStoreThatCannotBeBroughtForwardStopsTheMemberInsteadOfLimping()
    {
        // A table of the right name and the wrong shape is not repairable by adding a column, and the
        // alternative to refusing is the failure this whole check exists to remove: a member that starts,
        // serves, and is quietly not in the mesh.
        await WriteAsync("CREATE TABLE members (id TEXT PRIMARY KEY, something_else TEXT NOT NULL);");

        var store = new ClusterStore(Options, NullLogger<ClusterStore>.Instance);

        InvalidOperationException thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new MembersStore(store).ListAsync(default));
        Assert.Contains("members.member_id", thrown.Message, StringComparison.Ordinal);
        // And it says what to do, because the store holds only state that re-converges.
        Assert.Contains("re-join", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFreshStoreNeedsNoUpgrade()
    {
        var store = new ClusterStore(Options, NullLogger<ClusterStore>.Instance);
        Assert.Empty(await new MembersStore(store).ListAsync(default));
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (string path in new[] { _path, _path + "-wal", _path + "-shm" })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
    }
}
