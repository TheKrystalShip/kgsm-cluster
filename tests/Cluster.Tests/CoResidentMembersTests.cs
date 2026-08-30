using Microsoft.Data.Sqlite;
using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Membership;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// Two members on one machine. The deployment shape this covers is a node and an anchor sharing a host,
/// each running as its own unit with its own <c>StateDirectory=</c> — so the thing under test is that
/// neither's membership depends on the other's process, its file, or its identity.
/// </summary>
public class CoResidentMembersTests
{
    private const string Secret = "co-resident-secret";

    private static string StatePath(string unit) =>
        // A directory each, because that is what StateDirectory= gives two units.
        Path.Combine(Path.GetTempPath(), $"kgsm-{unit}-{Guid.NewGuid():N}", "cluster.db");

    private static void Prepare(string path) => Directory.CreateDirectory(Path.GetDirectoryName(path)!);

    [Fact]
    public async Task ANodeAndAnAnchorOnOneMachineHoldSeparateRosters()
    {
        string nodeState = StatePath("kgsm-api");
        string anchorState = StatePath("kgsm-auth");
        Prepare(nodeState);
        Prepare(anchorState);

        await using MemberHost far = await MemberHost.StartAsync("hotbox", Secret);
        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret, dbPath: nodeState);
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, dbPath: anchorState, kind: MemberKind.Anchor);

        // Only the node joins the far member. The anchor sits on the same machine and learns nothing
        // from that, because it has its own roster and nobody told it anything.
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(far.Url, null, default);

        Assert.NotNull(await node.Resolve<MembersStore>().GetByMemberIdAsync("hotbox", default));
        Assert.Empty(await anchor.Resolve<MembersStore>().ListAsync(default));

        // And the reverse: what the anchor joins is the anchor's, not the machine's.
        await anchor.Resolve<MemberHandshakeService>().AddMemberAsync(far.Url, null, default);
        Assert.Single(await anchor.Resolve<MembersStore>().ListAsync(default));
        Assert.Single(await node.Resolve<MembersStore>().ListAsync(default));
    }

    [Fact]
    public async Task OneMemberStoppingLeavesTheOtherFullyWorking()
    {
        // The acceptance in one sentence: neither member's membership depends on the other's process.
        string nodeState = StatePath("kgsm-api");
        string anchorState = StatePath("kgsm-auth");
        Prepare(nodeState);
        Prepare(anchorState);

        await using MemberHost far = await MemberHost.StartAsync("hotbox", Secret);
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, dbPath: anchorState, kind: MemberKind.Anchor);

        await using (MemberHost node = await MemberHost.StartAsync("hotrod", Secret, dbPath: nodeState))
        {
            await node.Resolve<MemberHandshakeService>().AddMemberAsync(far.Url, null, default);
            await anchor.Resolve<MemberHandshakeService>().AddMemberAsync(far.Url, null, default);
        }

        // The co-resident node is gone. The anchor still holds its roster, can still probe, and can
        // still reach the far member.
        MembersStore members = anchor.Resolve<MembersStore>();
        Assert.Single(await members.ListAsync(default));

        await anchor.Resolve<MemberLatencyPoller>().RunTickAsync(default);

        MemberRow row = (await members.GetByMemberIdAsync("hotbox", default))!;
        Assert.Equal(MemberStatus.Reachable, row.Status);
        Assert.Equal(GossipState.Alive, row.MembershipState);
    }

    [Fact]
    public async Task DisablingOneMemberLeavesACoResidentOneEnabled()
    {
        // The gate keys on member id, never on a machine. Keying it by host would make disabling a node
        // silently disable the anchor beside it, and that only surfaces once somebody tries to sign in.
        string nodeState = StatePath("kgsm-api");
        Prepare(nodeState);

        await using MemberHost far = await MemberHost.StartAsync("hotbox", Secret, dbPath: nodeState);
        MembersStore members = far.Resolve<MembersStore>();

        await members.UpsertAsync(MemberRow.New("hotrod", MemberKind.Node) with { Url = "http://hotrod:8080" }, default);
        await members.UpsertAsync(
            MemberRow.New("auth-anchor", MemberKind.Anchor) with { Url = "http://hotrod:9090" }, default);

        MemberRow node = (await members.GetByMemberIdAsync("hotrod", default))!;
        await members.SetEnabledAsync(node.Id, false, default);

        IClusterMemberGate gate = far.Resolve<IClusterMemberGate>();
        Assert.False(await gate.IsEnabledAsync("hotrod"));
        // Both live at the same address; only the one that was named is refused.
        Assert.True(await gate.IsEnabledAsync("auth-anchor"));
        // And a member nobody has ever heard of is accepted: absence is not rejection, because holding
        // the secret already proves membership.
        Assert.True(await gate.IsEnabledAsync("somebody-new"));
    }

    [Fact]
    public async Task EachMemberWritesOnlyItsOwnFile()
    {
        string nodeState = StatePath("kgsm-api");
        string anchorState = StatePath("kgsm-auth");
        Prepare(nodeState);
        Prepare(anchorState);

        await using MemberHost far = await MemberHost.StartAsync("hotbox", Secret);
        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret, dbPath: nodeState);
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, dbPath: anchorState, kind: MemberKind.Anchor);

        await node.Resolve<MemberHandshakeService>().AddMemberAsync(far.Url, null, default);

        Assert.Equal(1, await CountMembersAsync(nodeState));
        Assert.Equal(0, await CountMembersAsync(anchorState));
    }

    private static async Task<long> CountMembersAsync(string path)
    {
        await using var connection = new SqliteConnection(
            new SqliteConnectionStringBuilder { DataSource = path }.ToString());
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM members;";
        return (long)(await command.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task TwoMembersSharingADirectoryStillHoldSeparateRosters()
    {
        // A member's store is named per member, not fixed within a directory. Two members that happen to
        // share one — co-located units pointed at the same place, or a consumer deriving the path from
        // the directory rather than from its own database — would otherwise share a roster and an outbox,
        // which nothing notices until one of them disables somebody.
        string shared = Path.Combine(Path.GetTempPath(), $"kgsm-shared-{Guid.NewGuid():N}");
        Directory.CreateDirectory(shared);

        await using MemberHost far = await MemberHost.StartAsync("hotbox", Secret);
        await using MemberHost node = await MemberHost.StartAsync(
            "hotrod", Secret, dbPath: Path.Combine(shared, "kgsm-api.cluster.db"));
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, dbPath: Path.Combine(shared, "kgsm-auth.cluster.db"),
            kind: MemberKind.Anchor);

        await node.Resolve<MemberHandshakeService>().AddMemberAsync(far.Url, null, default);

        Assert.Single(await node.Resolve<MembersStore>().ListAsync(default));
        Assert.Empty(await anchor.Resolve<MembersStore>().ListAsync(default));
    }
}
