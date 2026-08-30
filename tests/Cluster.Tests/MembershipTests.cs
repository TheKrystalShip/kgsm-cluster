using Microsoft.Extensions.Logging.Abstractions;
using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Membership;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Tests;

public class MembersStoreTests
{
    private static MembersStore Store(TestCluster cluster) => new(cluster.Store);

    private static MemberRow Row(string memberId, string kind = MemberKind.Node) =>
        MemberRow.New(memberId, kind) with { Url = $"http://{memberId}:8080" };

    [Fact]
    public async Task ARowRoundTripsEveryField()
    {
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        var now = new DateTimeOffset(2026, 8, 30, 12, 0, 0, TimeSpan.Zero);

        MemberRow written = Row("member-b") with
        {
            Nickname = "Gaming Box",
            AddressVerified = true,
            Incarnation = 7,
            Status = MemberStatus.Reachable,
            MembershipState = GossipState.Suspect,
            StateChangedAt = now,
            LatencyMs = 12,
            LastSeen = now,
            ApiVersion = "v1",
            Enabled = false,
            Candidates = MemberCandidates.Encode([new MemberCandidate("http://member-b:8080", true)]),
        };
        await store.UpsertAsync(written, default);

        MemberRow? read = await store.GetByMemberIdAsync("member-b", default);
        Assert.Equal(written, read);
    }

    [Fact]
    public async Task AnAnchorIsStoredAsAnAnchor()
    {
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        await store.UpsertAsync(Row("auth-anchor", MemberKind.Anchor), default);

        MemberRow? read = await store.GetByMemberIdAsync("auth-anchor", default);
        Assert.Equal(MemberKind.Anchor, read?.Kind);
    }

    [Fact]
    public async Task TwoIntroductionsForOneMemberConvergeOnOneRow()
    {
        // A simultaneous mutual introduction must leave one row, not two, or the member is counted twice
        // and gossiped about twice.
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);

        MemberRow[] built = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(_ => store.UpsertByMemberIdAsync(
                "member-b", existing => (existing ?? MemberRow.New("member-b", MemberKind.Node)), default)));

        Assert.Single(built.Select(r => r.Id).Distinct());
        Assert.Single(await store.ListAsync(default));
    }

    [Fact]
    public async Task DisablingAMemberKeepsItsRow()
    {
        // Disable is a toggle, not a one-way ban: the row survives so it can be re-enabled.
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        MemberRow row = Row("member-b");
        await store.UpsertAsync(row, default);

        Assert.True(await store.SetEnabledAsync(row.Id, false, default));
        Assert.Empty(await store.ListEnabledAsync(default));
        Assert.Single(await store.ListAsync(default));

        Assert.True(await store.SetEnabledAsync(row.Id, true, default));
        Assert.Single(await store.ListEnabledAsync(default));
    }

    [Fact]
    public async Task AProbeSuccessNeverInventsALatencyOnFailure()
    {
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        MemberRow row = Row("member-b");
        await store.UpsertAsync(row, default);
        var seen = new DateTimeOffset(2026, 8, 30, 12, 0, 0, TimeSpan.Zero);

        await store.UpdateLivenessAsync(row.Id, MemberStatus.Reachable, 12, seen, default);
        await store.UpdateLivenessAsync(row.Id, MemberStatus.Unreachable, null, seen, default);

        MemberRow? after = await store.GetAsync(row.Id, default);
        Assert.Equal(MemberStatus.Unreachable, after?.Status);
        Assert.Null(after?.LatencyMs);
        // A failed probe never advances "last successfully reached", and never erases it either.
        Assert.Equal(seen, after?.LastSeen);
    }

    [Fact]
    public async Task PinningAnAddressIsWhatMarksItVerified()
    {
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        MemberRow row = Row("member-b");
        await store.UpsertAsync(row, default);
        Assert.False((await store.GetAsync(row.Id, default))!.AddressVerified);

        await store.PinAddressAsync(
            row.Id, "http://10.0.0.5:8080", [new MemberCandidate("http://10.0.0.5:8080", true)], default);

        MemberRow? after = await store.GetAsync(row.Id, default);
        Assert.True(after?.AddressVerified);
        Assert.Equal("http://10.0.0.5:8080", after?.Url);
    }

    [Fact]
    public async Task GossipRefreshesAnUnverifiedAddressAndLeavesAProvenOneAlone()
    {
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        var now = DateTimeOffset.UtcNow;

        MemberRow unproven = Row("member-b");
        await store.UpsertAsync(unproven, default);
        await store.UpdateMembershipAsync(
            unproven.Id, GossipState.Alive, 1, now, [new MemberCandidate("http://10.0.0.9:8080", true)], "v1", null, default);
        Assert.Equal("http://10.0.0.9:8080", (await store.GetAsync(unproven.Id, default))!.Url);

        MemberRow proven = Row("member-c");
        await store.UpsertAsync(proven, default);
        await store.PinAddressAsync(proven.Id, "http://10.0.0.3:8080", null, default);
        await store.UpdateMembershipAsync(
            proven.Id, GossipState.Alive, 1, now, [new MemberCandidate("http://10.0.0.9:8080", true)], "v1", null, default);
        // One short gossip round must not unpin an address this member knows works.
        Assert.Equal("http://10.0.0.3:8080", (await store.GetAsync(proven.Id, default))!.Url);
    }

    [Fact]
    public async Task AnInboundContactIsLivenessEvidenceButNotAProbeResult()
    {
        // Hearing FROM a member is not the same as reaching it: the converged axis moves, the first-hand
        // one does not.
        using var cluster = new TestCluster();
        MembersStore store = Store(cluster);
        MemberRow row = Row("member-b") with { MembershipState = GossipState.Suspect };
        await store.UpsertAsync(row, default);

        var now = DateTimeOffset.UtcNow;
        await store.RecordAliveContactAsync("member-b", now, default);

        MemberRow? after = await store.GetAsync(row.Id, default);
        Assert.Equal(GossipState.Alive, after?.MembershipState);
        Assert.Equal(now, after?.LastSeen);
        Assert.Equal(MemberStatus.Unknown, after?.Status);
    }
}

public class GossipStateTests
{
    [Fact]
    public void AMemberHeardAboutButNeverReachedShowsAsJoining()
    {
        Assert.Equal(GossipState.Joining, GossipState.Display(GossipState.Alive, lastSeen: null));
    }

    [Fact]
    public void AMemberReachedFirstHandShowsAsAlive()
    {
        Assert.Equal(GossipState.Alive, GossipState.Display(GossipState.Alive, DateTimeOffset.UtcNow));
    }

    [Theory]
    [InlineData(GossipState.Suspect)]
    [InlineData(GossipState.Dead)]
    [InlineData(GossipState.Left)]
    public void EveryOtherStateIsShownVerbatim(string state)
    {
        Assert.Equal(state, GossipState.Display(state, lastSeen: null));
    }

    [Fact]
    public void ATerminalStateIsReapable()
    {
        Assert.True(GossipState.IsTerminal(GossipState.Dead));
        Assert.True(GossipState.IsTerminal(GossipState.Left));
        Assert.False(GossipState.IsTerminal(GossipState.Alive));
        Assert.False(GossipState.IsTerminal(GossipState.Suspect));
    }
}

public class SelfIncarnationTests
{
    [Fact]
    public void RefutingJumpsPastTheReportBeingRefuted()
    {
        var incarnation = new SelfIncarnation();
        Assert.Equal(6, incarnation.RaiseToRefute(5));
        Assert.Equal(6, incarnation.Current);
    }

    [Fact]
    public void ItNeverRegresses()
    {
        var incarnation = new SelfIncarnation();
        incarnation.RaiseToRefute(10);
        Assert.Equal(11, incarnation.RaiseToRefute(3));
    }

    [Fact]
    public async Task ConcurrentRefutationsStayMonotonic()
    {
        var incarnation = new SelfIncarnation();
        await Task.WhenAll(Enumerable.Range(0, 64).Select(i => Task.Run(() => incarnation.RaiseToRefute(i))));
        Assert.True(incarnation.Current >= 64);
    }

}

public class AdvertisableCandidateTests
{
    [Theory]
    [InlineData("http://127.0.0.1:8098")]
    [InlineData("http://localhost:8098")]
    [InlineData("https://127.0.0.1")]
    [InlineData("http://[::1]:8098")]
    public void ALoopbackAddressIsNeverAdvertised(string url)
    {
        // It means "me" to whoever reads it, so on another machine it connects to that machine — which
        // in a cluster running the same components is plausibly another member of the same kind.
        Assert.Empty(MemberCandidates.Advertisable([new MemberCandidate(url, Client: true)]));
    }

    [Theory]
    [InlineData("http://10.0.0.5:8080")]
    [InlineData("https://auth.thekrystalship.com")]
    [InlineData("http://hotbox.lan:8080")]
    public void AnAddressAnotherMachineCanUseIsAdvertised(string url)
    {
        Assert.Single(MemberCandidates.Advertisable([new MemberCandidate(url, Client: true)]));
    }

    [Fact]
    public void TheRestOfTheListSurvivesTheFilter()
    {
        IReadOnlyList<MemberCandidate> advertisable = MemberCandidates.Advertisable(
        [
            new MemberCandidate("http://127.0.0.1:8098", true),
            new MemberCandidate("https://auth.thekrystalship.com", true),
        ]);
        Assert.Single(advertisable);
        Assert.Equal("https://auth.thekrystalship.com", advertisable[0].Url);
    }

    [Fact]
    public void ALoopbackAddressIsStillStoredAndStillUsable()
    {
        // Two members on one machine reach each other this way, and that is a real topology. The address
        // is kept and the local poller walks it; what it is not is something to tell anybody else.
        string stored = MemberCandidates.Encode([new MemberCandidate("http://127.0.0.1:8098", true)]);
        Assert.Single(MemberCandidates.Decode(stored));
        Assert.Equal("http://127.0.0.1:8098", MemberCandidates.Best(MemberCandidates.Decode(stored)));
    }
}
