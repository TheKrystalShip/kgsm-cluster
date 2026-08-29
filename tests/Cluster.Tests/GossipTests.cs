using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Membership;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// Gossip across real members. Rounds are driven explicitly rather than waited on, so a convergence test
/// asserts what converged rather than how long a timer took.
/// </summary>
public class GossipTests
{
    private const string Secret = "gossip-secret";

    private static async Task GossipRoundsAsync(int rounds, params MemberHost[] hosts)
    {
        for (int i = 0; i < rounds; i++)
            foreach (MemberHost host in hosts)
                await host.Resolve<GossipWorker>().RunRoundAsync(default);
    }

    [Fact]
    public async Task AddingOneMemberEventuallyJoinsAll()
    {
        // The property the whole gossip layer exists for: an operator introduces C to A, and B learns about
        // C without anybody telling it.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost c = await MemberHost.StartAsync("member-c", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(c.Url, null, default);

        await GossipRoundsAsync(6, a, b, c);

        Assert.NotNull(await b.Resolve<MembersStore>().GetByMemberIdAsync("member-c", default));
        Assert.NotNull(await c.Resolve<MembersStore>().GetByMemberIdAsync("member-b", default));
    }

    [Fact]
    public async Task AnAnchorLearnedThroughGossipIsKnownToBeAnAnchor()
    {
        // A member that arrives entirely as hearsay still has to be known as a node or an anchor, or the
        // first thing anybody does with it is wrong.
        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);
        await using MemberHost other = await MemberHost.StartAsync("hotbox", Secret);
        await using MemberHost anchor = await MemberHost.StartAsync("auth-anchor", Secret, kind: MemberKind.Anchor);

        await node.Resolve<MemberHandshakeService>().AddMemberAsync(other.Url, null, default);
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(anchor.Url, null, default);

        await GossipRoundsAsync(6, node, other, anchor);

        MemberRow? asOtherSeesIt = await other.Resolve<MembersStore>().GetByMemberIdAsync("auth-anchor", default);
        Assert.NotNull(asOtherSeesIt);
        Assert.Equal(MemberKind.Anchor, asOtherSeesIt!.Kind);
    }

    [Fact]
    public async Task AMemberLearnedThroughGossipShowsAsJoiningUntilItIsReachedDirectly()
    {
        // Hearsay is not first-hand knowledge, and the roster says so rather than showing a plain alive for
        // a member nobody here has ever actually reached.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost c = await MemberHost.StartAsync("member-c", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(c.Url, null, default);
        // Only A gossips. B has to learn C purely as hearsay: if C ever pushed to B, that inbound call
        // would be first-hand evidence and B would be right to stop calling it joining.
        await GossipRoundsAsync(8, a);

        MemberRow learned = (await b.Resolve<MembersStore>().GetByMemberIdAsync("member-c", default))!;
        Assert.Null(learned.LastSeen);
        Assert.Equal(GossipState.Joining, GossipState.Display(learned.MembershipState, learned.LastSeen));
        // The state that PROPAGATES stays the honest alive it heard — relaying never demotes it.
        Assert.Equal(GossipState.Alive, learned.MembershipState);

        await b.Resolve<MemberLatencyPoller>().RunTickAsync(default);

        MemberRow reached = (await b.Resolve<MembersStore>().GetByMemberIdAsync("member-c", default))!;
        Assert.NotNull(reached.LastSeen);
        Assert.Equal(GossipState.Alive, GossipState.Display(reached.MembershipState, reached.LastSeen));
    }

    [Fact]
    public async Task ProbingPinsTheAddressThatAnsweredAndMarksItVerified()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        await a.Resolve<MemberLatencyPoller>().RunTickAsync(default);

        MemberRow row = (await a.Resolve<MembersStore>().GetByMemberIdAsync("member-b", default))!;
        Assert.True(row.AddressVerified);
        Assert.Equal(MemberStatus.Reachable, row.Status);
        Assert.NotNull(row.LatencyMs);
    }

    [Fact]
    public async Task AMemberThatStoppedAnsweringGoesUnreachableWithoutAFabricatedLatency()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        string bUrl;
        await using (MemberHost b = await MemberHost.StartAsync("member-b", Secret))
        {
            bUrl = b.Url;
            await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
            await a.Resolve<MemberLatencyPoller>().RunTickAsync(default);
        }

        MemberRow before = (await a.Resolve<MembersStore>().GetByMemberIdAsync("member-b", default))!;
        await a.Resolve<MemberLatencyPoller>().RunTickAsync(default);

        MemberRow after = (await a.Resolve<MembersStore>().GetByMemberIdAsync("member-b", default))!;
        Assert.Equal(MemberStatus.Unreachable, after.Status);
        Assert.Null(after.LatencyMs);
        // Last-seen is when it was last actually reached, and a failed probe does not move it.
        Assert.Equal(before.LastSeen, after.LastSeen);
    }

    [Fact]
    public async Task ADisabledMemberIsNeverProbedAndNeverGossipedAbout()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost c = await MemberHost.StartAsync("member-c", Secret);

        MemberAddResult added = await a.Resolve<MemberHandshakeService>().AddMemberAsync(c.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MembersStore>().SetEnabledAsync(added.Member!.Id, false, default);

        await GossipRoundsAsync(6, a);

        // A locally-banned member is not re-injected into the mesh.
        Assert.Null(await b.Resolve<MembersStore>().GetByMemberIdAsync("member-c", default));
    }

    [Fact]
    public async Task GossipLeavesNoDurableRowsBehind()
    {
        // A sync round is best-effort anti-entropy. If it ever started writing to the outbox it would
        // retry a roster to a corpse for the full retry window.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        await GossipRoundsAsync(4, a, b);

        foreach (MemberHost host in new[] { a, b })
        foreach (string memberId in new[] { "member-a", "member-b" })
        {
            Assert.Empty(await host.Resolve<Messaging.ClusterBus>().ListForTargetAsync(memberId, default));
        }
    }

    [Fact]
    public async Task PanelOriginsTravelBetweenMembersWithoutBeingInterpreted()
    {
        // The package carries the list so a panel served from one member reaches every other. What a member
        // does with it is its own business; that it arrives is this package's.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);

        await a.Resolve<SelfIdentityStore>().RecordPanelOriginAsync("https://panel.example.com", default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        Assert.Contains("https://panel.example.com", await b.Resolve<SelfIdentityStore>().PanelOriginsAsync(default));
    }
}
