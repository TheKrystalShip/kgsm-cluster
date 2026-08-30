using TheKrystalShip.KGSM.Cluster;
using Microsoft.Extensions.Logging.Abstractions;
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

    [Fact]
    public async Task AMemberThatGoesSilentIsSuspectedThenDeclaredDeadThenReaped()
    {
        // Failure detection end to end, driven off the clock rather than waited on. A member is suspected
        // only once there has been no evidence for a whole window, dead only after a second, and removed
        // only after the reap window — three steps, so a single missed tick never buries anybody.
        using var cluster = new TestCluster();
        var members = new MembersStore(cluster.Store);
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options);
        var publications = new SelfPublications();
        var gossip = new GossipService(
            members, new ClusterStateStore(cluster.Store, cluster.Options), new SelfIncarnation(), identity,
            new SelfMemberCardSource(cluster.Options, identity, new SelfIncarnation(), publications),
            publications,
            cluster.Options with { SuspectMs = 1, ReapMs = 1 },
            NullLogger<GossipService>.Instance);

        MemberRow row = MemberRow.New("member-b", MemberKind.Node) with
        {
            Url = "http://member-b:8080",
            LastSeen = DateTimeOffset.UtcNow.AddHours(-1),
            StateChangedAt = DateTimeOffset.UtcNow.AddHours(-1),
        };
        await members.UpsertAsync(row, default);

        await gossip.AdvanceFailureTimersAsync(default);
        Assert.Equal(GossipState.Suspect, (await members.GetAsync(row.Id, default))!.MembershipState);

        // Each step is measured from the previous transition, so the clock has to actually move between
        // them. A pass that ran twice in the same instant would escalate twice off one silence.
        await Task.Delay(5);
        await gossip.AdvanceFailureTimersAsync(default);
        Assert.Equal(GossipState.Dead, (await members.GetAsync(row.Id, default))!.MembershipState);

        await Task.Delay(5);
        await gossip.AdvanceFailureTimersAsync(default);
        Assert.Null(await members.GetAsync(row.Id, default));
    }

    [Fact]
    public async Task AMemberStillTalkingToUsIsNeverSuspected()
    {
        // Evidence arrives from either direction. A member we cannot probe but that still calls us is
        // demonstrably alive, which is what stops an asymmetric partition from burying the live side.
        using var cluster = new TestCluster();
        var members = new MembersStore(cluster.Store);
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options);
        var publications = new SelfPublications();
        var gossip = new GossipService(
            members, new ClusterStateStore(cluster.Store, cluster.Options), new SelfIncarnation(), identity,
            new SelfMemberCardSource(cluster.Options, identity, new SelfIncarnation(), publications),
            publications,
            cluster.Options with { SuspectMs = 60_000 },
            NullLogger<GossipService>.Instance);

        MemberRow row = MemberRow.New("member-b", MemberKind.Node) with
        {
            Url = "http://member-b:8080",
            LastSeen = DateTimeOffset.UtcNow.AddHours(-1),
            StateChangedAt = DateTimeOffset.UtcNow.AddHours(-1),
        };
        await members.UpsertAsync(row, default);

        await gossip.RecordInboundContactAsync("member-b", default);
        await gossip.AdvanceFailureTimersAsync(default);

        Assert.Equal(GossipState.Alive, (await members.GetAsync(row.Id, default))!.MembershipState);
    }

    [Fact]
    public void AFalseReportAboutOurselvesIsRefutedRatherThanBelieved()
    {
        // A member cannot be told it is dead. It answers with a higher incarnation, and a strictly higher
        // incarnation always wins the merge, so the correction supersedes the report everywhere it spread.
        var incoming = new SyncMember(
            "member-a", MemberKind.Node, [], Incarnation: 5, GossipState.Dead, "v1");

        MergeOutcome outcome = RosterMerger.Decide(
            incoming, existing: null, myMemberId: "member-a", selfIncarnation: 3, existingFirstHandFresh: false);

        Assert.Equal(MergeAction.RefuteSelf, outcome.Action);
        Assert.Equal(6, outcome.RaiseSelfTo);
    }

    [Fact]
    public void AnAliveReportAboutOurselvesNeedsNoRefutation()
    {
        MergeOutcome outcome = RosterMerger.Decide(
            new SyncMember("member-a", MemberKind.Node, [], 9, GossipState.Alive, "v1"),
            existing: null, myMemberId: "member-a", selfIncarnation: 3, existingFirstHandFresh: false);

        Assert.Equal(MergeAction.Ignore, outcome.Action);
    }

    [Fact]
    public void OurOwnFreshProbeOutranksAnEqualIncarnationRumour()
    {
        // Somebody else's suspicion does not override what this member just confirmed with its own eyes.
        // Only a strictly higher incarnation — the member itself moving on — does.
        MemberRow existing = MemberRow.New("member-b", MemberKind.Node) with { Incarnation = 4 };

        Assert.Equal(MergeAction.Ignore, RosterMerger.Decide(
            new SyncMember("member-b", MemberKind.Node, [], 4, GossipState.Suspect, "v1"),
            existing, "member-a", 0, existingFirstHandFresh: true).Action);

        Assert.Equal(MergeAction.Update, RosterMerger.Decide(
            new SyncMember("member-b", MemberKind.Node, [], 5, GossipState.Suspect, "v1"),
            existing, "member-a", 0, existingFirstHandFresh: true).Action);
    }

    [Fact]
    public void ALocalDisableIsNeverUndoneByGossip()
    {
        // Disabling is this member's own override of the shared-secret trust, so nothing the mesh says
        // resurrects it.
        MemberRow disabled = MemberRow.New("member-b", MemberKind.Node) with { Enabled = false, Incarnation = 1 };

        Assert.Equal(MergeAction.Ignore, RosterMerger.Decide(
            new SyncMember("member-b", MemberKind.Node, [], 99, GossipState.Alive, "v1"),
            disabled, "member-a", 0, existingFirstHandFresh: false).Action);
    }

    [Fact]
    public async Task AnAnchorsPublishedKeyReachesAJoiningMemberAtOnce()
    {
        // A key you verify sessions against is no use arriving a gossip round late: a member that joins
        // and immediately serves a request would refuse a perfectly good session.
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, kind: MemberKind.Anchor);
        anchor.Resolve<SelfPublications>().Publish("auth.publickey", "-----BEGIN PUBLIC KEY-----abc");

        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(anchor.Url, null, default);

        // No gossip round has run.
        MemberRow row = (await node.Resolve<MembersStore>().GetByMemberIdAsync("auth-anchor", default))!;
        Assert.Equal("-----BEGIN PUBLIC KEY-----abc", row.Read("auth.publickey"));
    }

    [Fact]
    public async Task APublishedFactConvergesToAMemberThatOnlyHeardAboutIt()
    {
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, kind: MemberKind.Anchor);
        anchor.Resolve<SelfPublications>().Publish("auth.publickey", "key-v1");

        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(anchor.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await GossipRoundsAsync(8, a);

        MemberRow asBSeesIt = (await b.Resolve<MembersStore>().GetByMemberIdAsync("auth-anchor", default))!;
        Assert.Equal("key-v1", asBSeesIt.Read("auth.publickey"));
    }

    [Fact]
    public async Task TheAssignmentReachesEveryMember()
    {
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, kind: MemberKind.Anchor);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(anchor.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        // The anchor claims it, as it would on first start.
        Assert.True(await anchor.Resolve<ClusterStateStore>()
            .TryClaimAsync(ClusterCapability.Auth, "auth-anchor", default));

        await GossipRoundsAsync(10, anchor, a, b);

        foreach (MemberHost host in new[] { anchor, a, b })
        {
            Assert.Equal(
                "auth-anchor",
                await host.Resolve<ClusterStateStore>().HolderAsync(ClusterCapability.Auth, default));
        }
    }

    [Fact]
    public async Task AJoiningMemberLearnsTheAssignmentBeforeItCouldClaim()
    {
        // The race carrying state at join closes: a second anchor that joined without it would see nobody
        // holding the capability and claim one that is already held.
        await using MemberHost first = await MemberHost.StartAsync(
            "anchor-one", Secret, kind: MemberKind.Anchor);
        await first.Resolve<ClusterStateStore>().TryClaimAsync(ClusterCapability.Auth, "anchor-one", default);

        await using MemberHost second = await MemberHost.StartAsync(
            "anchor-two", Secret, kind: MemberKind.Anchor);
        await second.Resolve<MemberHandshakeService>().AddMemberAsync(first.Url, null, default);

        ClusterStateStore state = second.Resolve<ClusterStateStore>();
        Assert.Equal("anchor-one", await state.HolderAsync(ClusterCapability.Auth, default));
        // So its own claim is refused and it knows to stand down.
        Assert.False(await state.TryClaimAsync(ClusterCapability.Auth, "anchor-two", default));
        Assert.False(await state.IsHolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task AReassignmentTravelsAndTheOldHolderLearnsItIsDemoted()
    {
        await using MemberHost anchor = await MemberHost.StartAsync(
            "anchor-one", Secret, kind: MemberKind.Anchor);
        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(anchor.Url, null, default);
        await anchor.Resolve<ClusterStateStore>().TryClaimAsync(ClusterCapability.Auth, "anchor-one", default);
        await GossipRoundsAsync(6, anchor, node);
        Assert.True(await anchor.Resolve<ClusterStateStore>().IsHolderAsync(ClusterCapability.Auth, default));

        // An admin reassigns from the panel, which runs on the node.
        await node.Resolve<ClusterStateStore>().AssignAsync(ClusterCapability.Auth, "anchor-two", default);
        await GossipRoundsAsync(6, anchor, node);

        Assert.False(await anchor.Resolve<ClusterStateStore>().IsHolderAsync(ClusterCapability.Auth, default));
        Assert.Equal(
            "anchor-two",
            await anchor.Resolve<ClusterStateStore>().HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task AKeyIsReadFromTheHolderAndNotFromWhoeverStatesOne()
    {
        // The provenance rule. Any member holding the cluster secret can state a key; only the member the
        // cluster says holds the capability is believed for it.
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, kind: MemberKind.Anchor);
        anchor.Resolve<SelfPublications>().Publish("auth.publickey", "the-real-key");

        await using MemberHost impostor = await MemberHost.StartAsync("member-x", Secret);
        impostor.Resolve<SelfPublications>().Publish("auth.publickey", "a-substituted-key");

        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(anchor.Url, null, default);
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(impostor.Url, null, default);
        await node.Resolve<ClusterStateStore>().AssignAsync(ClusterCapability.Auth, "auth-anchor", default);

        string? key = await node.Resolve<ClusterFacts>()
            .FromHolderAsync(ClusterCapability.Auth, "auth.publickey", default);

        Assert.Equal("the-real-key", key);
    }

    [Fact]
    public async Task WithNoHolderThereIsNoKeyToRead()
    {
        await using MemberHost impostor = await MemberHost.StartAsync("member-x", Secret);
        impostor.Resolve<SelfPublications>().Publish("auth.publickey", "a-substituted-key");

        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);
        await node.Resolve<MemberHandshakeService>().AddMemberAsync(impostor.Url, null, default);

        // Nobody holds auth, so a member stating a key for it is simply not consulted.
        Assert.Null(await node.Resolve<ClusterFacts>()
            .FromHolderAsync(ClusterCapability.Auth, "auth.publickey", default));
    }

    [Fact]
    public async Task DeletingARowIsUndoneByTheNextRound()
    {
        // Why removal is a state and not a deletion: anti-entropy exists to repair a roster that is
        // missing something, so deleting a row asks gossip to undo the removal, and it obliges.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost gone = await MemberHost.StartAsync("removeme", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(gone.Url, null, default);
        await GossipRoundsAsync(8, a, b);

        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow row = (await rosterA.GetByMemberIdAsync("removeme", default))!;
        await rosterA.DeleteAsync(row.Id, default);
        Assert.Null(await rosterA.GetByMemberIdAsync("removeme", default));

        // B still holds it, and one round hands it straight back.
        await GossipRoundsAsync(4, a, b);
        Assert.NotNull(await rosterA.GetByMemberIdAsync("removeme", default));
    }

    [Fact]
    public async Task AMemberThatHasLeftStaysGoneAcrossTheCluster()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);

        string goneUrl;
        await using (MemberHost gone = await MemberHost.StartAsync("removeme", Secret))
        {
            goneUrl = gone.Url;
            await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
            await a.Resolve<MemberHandshakeService>().AddMemberAsync(gone.Url, null, default);
            await GossipRoundsAsync(8, a, b);
        }

        // It is decommissioned and stopped. The operator removes it on A.
        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow row = (await rosterA.GetByMemberIdAsync("removeme", default))!;
        Assert.True(await rosterA.MarkLeftAsync(row.Id, DateTimeOffset.UtcNow, default));

        await GossipRoundsAsync(6, a, b);

        // B took the departure rather than handing the member back.
        MemberRow asBSeesIt = (await b.Resolve<MembersStore>().GetByMemberIdAsync("removeme", default))!;
        Assert.Equal(GossipState.Left, asBSeesIt.MembershipState);
        Assert.True(GossipState.IsTerminal(asBSeesIt.MembershipState));

        // And A still holds the departure rather than an absence gossip would repair.
        Assert.Equal(
            GossipState.Left,
            (await rosterA.GetByMemberIdAsync("removeme", default))!.MembershipState);
    }

    [Fact]
    public async Task ADepartedMemberIsReapedEverywhereOnceTheWindowPasses()
    {
        using var cluster = new TestCluster();
        var members = new MembersStore(cluster.Store);
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options);
        var publications = new SelfPublications();
        var gossip = new GossipService(
            members, new ClusterStateStore(cluster.Store, cluster.Options), new SelfIncarnation(), identity,
            new SelfMemberCardSource(cluster.Options, identity, new SelfIncarnation(), publications),
            publications,
            cluster.Options with { ReapMs = 1 },
            NullLogger<GossipService>.Instance);

        MemberRow row = MemberRow.New("removeme", MemberKind.Node) with { Url = "http://removeme:8080" };
        await members.UpsertAsync(row, default);
        await members.MarkLeftAsync(row.Id, DateTimeOffset.UtcNow.AddHours(-1), default);

        await gossip.AdvanceFailureTimersAsync(default);

        // The tombstone is what carried the departure; once every member has had it, the row goes.
        Assert.Null(await members.GetAsync(row.Id, default));
    }

    [Fact]
    public async Task AMemberThatIsStillRunningRefutesItsOwnRemovalAndReturns()
    {
        // Not a defect: only a member may raise its own incarnation, and it re-asserts alive above
        // whatever was said about it. That is what stops a live member being buried by a false report,
        // and it means removing one that is still participating is a request the mesh overturns.
        //
        // The refutation reaches it through a THIRD member, and it has to: the member that recorded the
        // departure stops choosing it as a gossip partner, because there is no point syncing with
        // somebody believed gone. So B is what carries the removal to it and its refutation back.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost live = await MemberHost.StartAsync("still-running", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(live.Url, null, default);
        await GossipRoundsAsync(8, a);

        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow row = (await rosterA.GetByMemberIdAsync("still-running", default))!;
        await rosterA.MarkLeftAsync(row.Id, DateTimeOffset.UtcNow, default);

        // Every member that learns the departure stops choosing it as a partner, so the removal only
        // reaches it when IT gossips out — which a running member does.
        await GossipRoundsAsync(10, a, b, live);

        MemberRow after = (await rosterA.GetByMemberIdAsync("still-running", default))!;
        Assert.Equal(GossipState.Alive, after.MembershipState);
        Assert.True(after.Incarnation > row.Incarnation);
    }

    [Fact]
    public async Task AMemberStopsGossipingWithOneItBelievesHasLeft()
    {
        // Which is why a departure sticks at all: the member that recorded it is not asking the departed
        // one for its opinion every few seconds.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost live = await MemberHost.StartAsync("still-running", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(live.Url, null, default);

        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow row = (await rosterA.GetByMemberIdAsync("still-running", default))!;
        await rosterA.MarkLeftAsync(row.Id, DateTimeOffset.UtcNow, default);

        await GossipRoundsAsync(8, a);

        Assert.Equal(
            GossipState.Left,
            (await rosterA.GetByMemberIdAsync("still-running", default))!.MembershipState);
    }

    [Fact]
    public async Task DisablingIsWhatRemovesAMemberThatWillNotLeave()
    {
        // The honest answer for a live member: disable is this member's own override of the shared-secret
        // trust, and no gossip undoes it.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost live = await MemberHost.StartAsync("still-running", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(live.Url, null, default);

        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow row = (await rosterA.GetByMemberIdAsync("still-running", default))!;
        await rosterA.SetEnabledAsync(row.Id, false, default);

        await GossipRoundsAsync(6, a, live);

        Assert.False((await rosterA.GetByMemberIdAsync("still-running", default))!.Enabled);
        Assert.Empty(await rosterA.ListEnabledAsync(default));
    }

    [Fact]
    public async Task ALoopbackAddressPinnedForANeighbourIsNotGossipedOnward()
    {
        // The path a member cannot fix by tidying its own facts: A holds a loopback address for B because
        // an operator pasted one, and gossip carries A's whole roster — so without filtering at the wire
        // that address reaches C, where it means C's own machine.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost c = await MemberHost.StartAsync("member-c", Secret);

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(c.Url, null, default);

        // An operator's loopback pin for B, exactly as a same-machine join leaves it.
        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow rowB = (await rosterA.GetByMemberIdAsync("member-b", default))!;
        await rosterA.PinAddressAsync(
            rowB.Id, "http://127.0.0.1:8098", [new MemberCandidate("http://127.0.0.1:8098", true)], default);

        await GossipRoundsAsync(8, a);

        MemberRow asCSeesIt = (await c.Resolve<MembersStore>().GetByMemberIdAsync("member-b", default))!;
        Assert.DoesNotContain(
            MemberCandidates.Decode(asCSeesIt.Candidates),
            candidate => candidate.Url.Contains("127.0.0.1", StringComparison.Ordinal));

        // And A keeps it, because on A's machine it is the address that works.
        Assert.Contains(
            MemberCandidates.Decode((await rosterA.GetByMemberIdAsync("member-b", default))!.Candidates),
            candidate => candidate.Url == "http://127.0.0.1:8098");
    }

    [Fact]
    public async Task AMembersOwnLoopbackAddressIsNotOfferedOnItsCard()
    {
        await using MemberHost anchor = await MemberHost.StartAsync(
            "auth-anchor", Secret, kind: MemberKind.Anchor, seedOwnAddress: false);
        SelfIdentityStore identity = anchor.Resolve<SelfIdentityStore>();

        // What a same-machine join reflects back, and what a public name adds later.
        await identity.RecordCandidateAsync(
            "http://127.0.0.1:8098", client: true, SelfIdentityStore.OperatorProvenance, default);
        await identity.RecordCandidateAsync(
            "https://auth.thekrystalship.com", client: true, SelfIdentityStore.OperatorProvenance, default);

        MemberCard card = await anchor.Resolve<IMemberCardSource>().BuildAsync(default);

        Assert.Single(card.Candidates);
        Assert.Equal("https://auth.thekrystalship.com", card.Candidates[0].Url);
        // Still known locally: it is a true fact about where this member answers.
        Assert.Contains(await identity.CandidatesAsync(default), c => c.Url == "http://127.0.0.1:8098");
    }

    [Fact]
    public async Task ARowThatLearnedNoAddressGainsOneWhenTheMeshHasIt()
    {
        // The live sequence this comes from: a member joins over loopback, so the member that joined it
        // pins a real address for that pair but filters it when gossiping onward — and everybody else
        // creates a row with no address. When the member later answers somewhere routable, that row has
        // to be able to learn it. Before, it could not: two members holding the same state at the same
        // incarnation neither supersedes the other, so the whole report was ignored and the address with
        // it, for as long as both stayed alive.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost c = await MemberHost.StartAsync("member-c", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(c.Url, null, default);

        MembersStore rosterC = c.Resolve<MembersStore>();
        // C holds a member it has heard of and has no address for, exactly as a filtered loopback leaves it.
        await rosterC.UpsertAsync(
            MemberRow.New("replica-probe", MemberKind.Node) with { MembershipState = GossipState.Alive },
            default);
        MemberRow before = (await rosterC.GetByMemberIdAsync("replica-probe", default))!;
        Assert.Equal("", before.Url);

        // The mesh now carries a routable address for it, at the same state and the same incarnation.
        await c.Resolve<GossipService>().MergeIncomingAsync(
        [
            new SyncMember("replica-probe", MemberKind.Node,
                [new MemberCandidate("http://192.168.1.128:8096", true)],
                before.Incarnation, GossipState.Alive, "v1"),
        ], default);

        MemberRow after = (await rosterC.GetByMemberIdAsync("replica-probe", default))!;
        Assert.Equal("http://192.168.1.128:8096", after.Url);
    }

    [Fact]
    public async Task AProvenAddressIsNotUnpinnedByWhatGossipReports()
    {
        // Learning addressing must not undo a probe. The candidate is taken; the pinned address is not
        // moved by hearsay, because this member has evidence and the report has none.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);
        await a.Resolve<MemberLatencyPoller>().RunTickAsync(default);

        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow proven = (await rosterA.GetByMemberIdAsync("member-b", default))!;
        Assert.True(proven.AddressVerified);

        await a.Resolve<GossipService>().MergeIncomingAsync(
        [
            new SyncMember("member-b", MemberKind.Node,
                [new MemberCandidate("http://10.9.9.9:8080", true)],
                proven.Incarnation, GossipState.Alive, "v1"),
        ], default);

        MemberRow after = (await rosterA.GetByMemberIdAsync("member-b", default))!;
        Assert.Equal(proven.Url, after.Url);
        // The offered address is still kept as something to try if the pinned one stops answering.
        Assert.Contains(
            MemberCandidates.Decode(after.Candidates), candidate => candidate.Url == "http://10.9.9.9:8080");
    }

    [Fact]
    public async Task ADisabledMembersRowIsNotAlteredByGossipAtAll()
    {
        // Disable is this member's own override of the shared-secret trust, and addressing is no
        // exception to it.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        MembersStore rosterA = a.Resolve<MembersStore>();
        MemberRow row = (await rosterA.GetByMemberIdAsync("member-b", default))!;
        await rosterA.SetEnabledAsync(row.Id, false, default);

        await a.Resolve<GossipService>().MergeIncomingAsync(
        [
            new SyncMember("member-b", MemberKind.Node,
                [new MemberCandidate("http://10.9.9.9:8080", true)],
                row.Incarnation, GossipState.Alive, "v1"),
        ], default);

        MemberRow after = (await rosterA.GetByMemberIdAsync("member-b", default))!;
        Assert.DoesNotContain(
            MemberCandidates.Decode(after.Candidates), candidate => candidate.Url == "http://10.9.9.9:8080");
    }
}
