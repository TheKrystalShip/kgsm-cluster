using TheKrystalShip.KGSM.Cluster.Membership;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// Which address a member gives a browser, when it knows more than one.
/// </summary>
/// <remarks>
/// <para>
/// A member learns addresses by being reached at them, and it is also told one. Those disagree the
/// moment a deployment moves — a panel that used to be served from the node and is now served from
/// somewhere else leaves the node still holding the address it was reached at, which by then belongs
/// to the panel rather than to the node.
/// </para>
/// <para>
/// <b>The configured address wins, and it has to win by rule rather than by the order a dictionary
/// happens to enumerate in.</b> A roster hands these to a browser and a browser cannot try the
/// alternatives: the first client-reachable candidate is the whole answer, so the position of the
/// configured one in that list is the contract, not an implementation detail.
/// </para>
/// </remarks>
public sealed class SelfAddressPrecedenceTests
{
    [Fact]
    public async Task What_an_operator_configured_beats_what_a_browser_was_observed_reaching()
    {
        using var cluster = new TestCluster();
        var identity = new SelfIdentityStore(
            cluster.Store, cluster.Options with { PublicBaseUrl = "https://hotrod.example" });

        // What a browser reached this member at, back when the panel was served from it. After a move
        // that name belongs to the panel, and handing it to a browser as a node to drive points the
        // panel at itself.
        await identity.RecordCandidateAsync(
            "https://panel.example", client: true, SelfIdentityStore.BrowserObserved, default);

        IReadOnlyList<MemberCandidate> candidates = await identity.CandidatesAsync(default);

        Assert.Equal(
            "https://hotrod.example",
            MemberCandidates.ClientUrl(candidates));

        // The observed one is kept rather than discarded: it is still an address this member really
        // was reached at, and a member behind more than one name is ordinary.
        Assert.Contains(candidates, c => c.Url == "https://panel.example");
    }

    [Fact]
    public async Task A_member_told_nothing_still_answers_with_what_it_was_reached_at()
    {
        using var cluster = new TestCluster();
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options with { PublicBaseUrl = "" });

        await identity.RecordCandidateAsync(
            "https://observed.example", client: true, SelfIdentityStore.BrowserObserved, default);

        // The common case, and the reason observation exists at all: a member behind a proxy cannot
        // work its own address out, so being reached is how it learns one.
        Assert.Equal(
            "https://observed.example",
            MemberCandidates.ClientUrl(await identity.CandidatesAsync(default)));
    }

    [Fact]
    public async Task An_assigned_name_beats_everything_reflected_and_yields_to_configuration()
    {
        using var cluster = new TestCluster();
        var assigned = new Assigned("https://walter.nodes.example");
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options with { PublicBaseUrl = "" }, [assigned]);

        // An operator's pasted URL is the strongest reflected statement, and it still describes where
        // somebody once reached this member rather than the name the cluster serves it at now.
        await identity.RecordCandidateAsync(
            "https://hotrod.example", client: true, SelfIdentityStore.OperatorProvenance, default);

        Assert.Equal(
            "https://walter.nodes.example",
            MemberCandidates.ClientUrl(await identity.CandidatesAsync(default)));

        var configured = new SelfIdentityStore(
            cluster.Store, cluster.Options with { PublicBaseUrl = "https://panel-configured.example" }, [assigned]);
        Assert.Equal(
            "https://panel-configured.example",
            MemberCandidates.ClientUrl(await configured.CandidatesAsync(default)));
    }

    [Fact]
    public async Task An_assigned_name_no_longer_served_is_no_longer_offered()
    {
        using var cluster = new TestCluster();
        var assigned = new Assigned("https://auth.anchors.example");
        var identity = new SelfIdentityStore(cluster.Store, cluster.Options with { PublicBaseUrl = "" }, [assigned]);

        Assert.Contains(await identity.CandidatesAsync(default), c => c.Url == "https://auth.anchors.example");

        // The capability moved: the name now points at another member, and advertising it here would
        // send every caller to somebody else.
        assigned.Current = [];
        Assert.DoesNotContain(await identity.CandidatesAsync(default), c => c.Url == "https://auth.anchors.example");
    }

    private sealed class Assigned(params string[] addresses) : ISelfAddressSource
    {
        public IReadOnlyList<string> Current { get; set; } = addresses;
        public IReadOnlyList<string> Addresses => Current;
    }

    [Fact]
    public async Task An_address_only_other_members_use_is_never_the_one_a_browser_is_given()
    {
        using var cluster = new TestCluster();
        var identity = new SelfIdentityStore(
            cluster.Store, cluster.Options with { PublicBaseUrl = "", GossipUrl = "http://10.0.0.9:8080" });

        await identity.RecordCandidateAsync(
            "http://192.168.1.128", client: false, SelfIdentityStore.PeerObservedProvenance, default);

        // A secure page cannot fetch a plaintext origin at all, so a peer-to-peer address handed to a
        // browser registers a connection that can only ever read as down — and a panel then reports a
        // healthy machine as one that did not answer.
        Assert.Equal("", MemberCandidates.ClientUrl(await identity.CandidatesAsync(default)));
    }
}
