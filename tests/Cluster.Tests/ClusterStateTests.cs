using Microsoft.Extensions.Logging.Abstractions;
using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Membership;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// Which member holds a capability, and what a holder states about itself. The two exist to let an auth
/// anchor be chosen and its signing key found, without either being something a member can simply assert.
/// </summary>
public class ClusterAssignmentTests
{
    private static ClusterStateStore Store(TestCluster cluster) => new(cluster.Store, cluster.Options);

    [Fact]
    public async Task NobodyHoldsACapabilityUntilSomebodyClaimsIt()
    {
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);

        // Absent and held-by-nobody are different answers, and a consumer needs to tell them apart.
        Assert.Null(await state.GetAsync(ClusterCapability.Auth, default));
        Assert.Null(await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task TheFirstClaimTakesIt()
    {
        using var cluster = new TestCluster(memberId: "auth-anchor");
        ClusterStateStore state = Store(cluster);

        Assert.True(await state.TryClaimAsync(ClusterCapability.Auth, "auth-anchor", default));
        Assert.Equal("auth-anchor", await state.HolderAsync(ClusterCapability.Auth, default));
        Assert.True(await state.IsHolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task ASecondClaimIsRefusedRatherThanTakingItOver()
    {
        // The whole reason claiming is not an ordinary write: a second anchor installed on another machine
        // must become a candidate, never a second authority.
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);
        await state.TryClaimAsync(ClusterCapability.Auth, "anchor-one", default);

        Assert.False(await state.TryClaimAsync(ClusterCapability.Auth, "anchor-two", default));
        Assert.Equal("anchor-one", await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task ConcurrentClaimsOnOneMemberLeaveOneHolder()
    {
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);

        bool[] results = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(i => state.TryClaimAsync(ClusterCapability.Auth, $"anchor-{i}", default)));

        Assert.Single(results, won => won);
        Assert.NotNull(await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task AnAdminReassignmentOverwritesAHolder()
    {
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);
        await state.TryClaimAsync(ClusterCapability.Auth, "anchor-one", default);

        ClusterAssignment moved = await state.AssignAsync(ClusterCapability.Auth, "anchor-two", default);

        Assert.Equal("anchor-two", await state.HolderAsync(ClusterCapability.Auth, default));
        // The version rises, which is what carries the decision past every other member's copy.
        Assert.Equal(2, moved.Version);
    }

    [Fact]
    public async Task ACapabilityCanBeDeliberatelyHeldByNobody()
    {
        // Different from never having heard of it: this is a decision, and it has to converge like one.
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);
        await state.TryClaimAsync(ClusterCapability.Auth, "anchor-one", default);

        await state.AssignAsync(ClusterCapability.Auth, "", default);

        Assert.Null(await state.HolderAsync(ClusterCapability.Auth, default));
        Assert.NotNull(await state.GetAsync(ClusterCapability.Auth, default));
        // And it can then be claimed again, because nobody holds it.
        Assert.True(await state.TryClaimAsync(ClusterCapability.Auth, "anchor-two", default));
    }

    [Fact]
    public async Task AHigherVersionSupersedesOnMerge()
    {
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);
        await state.TryClaimAsync(ClusterCapability.Auth, "anchor-one", default);

        IReadOnlyList<string> changed = await state.MergeAsync(
            [new ClusterAssignment(ClusterCapability.Auth, "anchor-two", 9, "some-node")], default);

        Assert.Equal([ClusterCapability.Auth], changed);
        Assert.Equal("anchor-two", await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task AStaleAssignmentIsIgnored()
    {
        using var cluster = new TestCluster();
        ClusterStateStore state = Store(cluster);
        await state.AssignAsync(ClusterCapability.Auth, "anchor-one", default);
        await state.AssignAsync(ClusterCapability.Auth, "anchor-two", default);

        await state.MergeAsync(
            [new ClusterAssignment(ClusterCapability.Auth, "anchor-one", 1, "some-node")], default);

        Assert.Equal("anchor-two", await state.HolderAsync(ClusterCapability.Auth, default));
    }

    [Fact]
    public async Task TwoMembersClaimingAtOnceConvergeOnTheSameHolder()
    {
        // The race the local compare-and-set cannot settle: two anchors on two machines both see nobody
        // holding it and both succeed locally. When their gossip meets, both sides must land on the SAME
        // one, or the loser never learns to stand down.
        using var a = new TestCluster(memberId: "anchor-a");
        using var b = new TestCluster(memberId: "anchor-b");
        ClusterStateStore stateA = Store(a);
        ClusterStateStore stateB = Store(b);

        Assert.True(await stateA.TryClaimAsync(ClusterCapability.Auth, "anchor-a", default));
        Assert.True(await stateB.TryClaimAsync(ClusterCapability.Auth, "anchor-b", default));

        // Exchange, in both directions, as a gossip round does.
        await stateA.MergeAsync(await stateB.ListAsync(default), default);
        await stateB.MergeAsync(await stateA.ListAsync(default), default);

        string? holderA = await stateA.HolderAsync(ClusterCapability.Auth, default);
        string? holderB = await stateB.HolderAsync(ClusterCapability.Auth, default);
        Assert.Equal(holderA, holderB);
        Assert.NotNull(holderA);
    }

    [Fact]
    public async Task TheLoserOfThatRaceCanTellItIsNotTheHolder()
    {
        // Standing down is the consumer's job, but it needs a straight answer to act on.
        using var a = new TestCluster(memberId: "anchor-a");
        using var b = new TestCluster(memberId: "anchor-b");
        ClusterStateStore stateA = Store(a);
        ClusterStateStore stateB = Store(b);
        await stateA.TryClaimAsync(ClusterCapability.Auth, "anchor-a", default);
        await stateB.TryClaimAsync(ClusterCapability.Auth, "anchor-b", default);
        await stateA.MergeAsync(await stateB.ListAsync(default), default);
        await stateB.MergeAsync(await stateA.ListAsync(default), default);

        bool aHolds = await stateA.IsHolderAsync(ClusterCapability.Auth, default);
        bool bHolds = await stateB.IsHolderAsync(ClusterCapability.Auth, default);

        // Exactly one of them believes it holds it, and it is the same one both agree on.
        Assert.True(aHolds ^ bHolds);
    }

    [Fact]
    public void TheMechanismStoresAValuePerKeyAndNoPolicyAboutIt()
    {
        // A map, not a policy: whether a capability may have one holder is the consuming plan's rule.
        var one = new ClusterAssignment("assistant", "member-a", 1, "member-a");
        var two = new ClusterAssignment("bot", "member-b", 1, "member-b");
        Assert.NotEqual(one.Capability, two.Capability);
        Assert.True(one.Supersedes(null));
    }
}

public class PublishedFactsTests
{
    [Fact]
    public void AMemberStatesFactsAboutItself()
    {
        var publications = new SelfPublications();
        publications.Publish("auth.publickey", "-----BEGIN PUBLIC KEY-----");
        Assert.Equal("-----BEGIN PUBLIC KEY-----", publications.Current["auth.publickey"]);
    }

    [Fact]
    public void PublishingAgainReplacesRatherThanAccumulates()
    {
        var publications = new SelfPublications();
        publications.Publish("k", "one");
        publications.Publish("k", "two");
        Assert.Equal("two", publications.Current["k"]);
        Assert.Single(publications.Current);
    }

    [Fact]
    public void WithdrawingRemovesIt()
    {
        var publications = new SelfPublications();
        publications.Publish("k", "v");
        publications.Withdraw("k");
        Assert.Empty(publications.Current);
    }

    [Fact]
    public void AnOversizedFactIsRefusedAtThePointOfPublishing()
    {
        // Every fact rides every gossip round. Refusing here names the caller; truncating on the wire would
        // hand another member a key that is silently wrong.
        var publications = new SelfPublications();
        Assert.Throws<ArgumentException>(
            () => publications.Publish("k", new string('x', SelfPublications.MaxValueBytes + 1)));
    }

    [Fact]
    public void ThereIsACapOnHowManyFactsAMemberStates()
    {
        var publications = new SelfPublications();
        for (int i = 0; i < SelfPublications.MaxFacts; i++)
            publications.Publish($"k{i}", "v");
        Assert.Throws<ArgumentException>(() => publications.Publish("one-too-many", "v"));
        // Replacing an existing key is always allowed, cap or not.
        publications.Publish("k0", "replaced");
        Assert.Equal("replaced", publications.Current["k0"]);
    }

    [Fact]
    public void AnEmptyOrUnreadableStoredSetReadsAsEmpty()
    {
        Assert.Empty(PublishedFacts.Decode(null));
        Assert.Empty(PublishedFacts.Decode(""));
        Assert.Empty(PublishedFacts.Decode("{not json"));
    }

    [Fact]
    public void EncodeAndDecodeRoundTrip()
    {
        string encoded = PublishedFacts.Encode(
            new Dictionary<string, string> { ["auth.publickey"] = "abc", ["auth.kid"] = "k1" });
        IReadOnlyDictionary<string, string> decoded = PublishedFacts.Decode(encoded);
        Assert.Equal("abc", decoded["auth.publickey"]);
        Assert.Equal("k1", decoded["auth.kid"]);
    }

    [Fact]
    public async Task ReapingAMemberThatHoldsACapabilityLeavesTheAssignmentNamingNobodyPresent()
    {
        // The refusal to remove a holder covers the deliberate act. A holder whose machine simply dies
        // reaches the same place without anybody removing anything: it goes suspect, then dead, then the
        // reap window passes and the row goes — while the assignment still names it.
        using var cluster = new TestCluster();
        var members = new MembersStore(cluster.Store);
        var state = new ClusterStateStore(cluster.Store, cluster.Options);
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options);
        var publications = new SelfPublications();
        var gossip = new GossipService(
            members, state, new SelfIncarnation(), identity,
            new SelfMemberCardSource(cluster.Options, identity, new SelfIncarnation(), publications),
            publications,
            cluster.Options with { ReapMs = 1 },
            NullLogger<GossipService>.Instance);

        MemberRow holder = MemberRow.New("auth-anchor", MemberKind.Anchor) with { Url = "http://gone:8080" };
        await members.UpsertAsync(holder, default);
        await state.TryClaimAsync(ClusterCapability.Auth, "auth-anchor", default);

        await members.MarkLeftAsync(holder.Id, DateTimeOffset.UtcNow.AddHours(-1), default);
        await gossip.AdvanceFailureTimersAsync(default);

        Assert.Null(await members.GetByMemberIdAsync("auth-anchor", default));
        // The assignment survives the member it names. Removing it is a decision nothing here may take,
        // so what the package owes is that the state is findable rather than only deducible from a
        // capability quietly not being served.
        Assert.Equal("auth-anchor", await state.HolderAsync(ClusterCapability.Auth, default));

        IReadOnlyList<ClusterAssignment> orphaned =
            await new ClusterFacts(members, state).OrphanedAsync(default);
        Assert.Single(orphaned);
        Assert.Equal(ClusterCapability.Auth, orphaned[0].Capability);
        Assert.Equal("auth-anchor", orphaned[0].MemberId);
    }

    [Fact]
    public async Task AnAssignmentWhoseHolderIsPresentIsNotOrphaned()
    {
        using var cluster = new TestCluster();
        var members = new MembersStore(cluster.Store);
        var state = new ClusterStateStore(cluster.Store, cluster.Options);

        MemberRow holder = MemberRow.New("auth-anchor", MemberKind.Anchor) with { Url = "http://anchor:8080" };
        await members.UpsertAsync(holder, default);
        await state.TryClaimAsync(ClusterCapability.Auth, "auth-anchor", default);

        Assert.Empty(await new ClusterFacts(members, state).OrphanedAsync(default));
    }

    [Fact]
    public async Task ACapabilityHeldByNobodyIsNotOrphaned()
    {
        // Deliberately unheld is a decision, not a dangling reference.
        using var cluster = new TestCluster();
        var members = new MembersStore(cluster.Store);
        var state = new ClusterStateStore(cluster.Store, cluster.Options);
        await state.AssignAsync(ClusterCapability.Auth, "", default);

        Assert.Empty(await new ClusterFacts(members, state).OrphanedAsync(default));
    }
}
