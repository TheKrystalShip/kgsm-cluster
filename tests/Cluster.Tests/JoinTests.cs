using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Membership;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// The join handshake between two members standing on real ports. What matters here is that joining is
/// symmetric, that an anchor can join at all, and that every refusal names its own reason rather than
/// collapsing into "unreachable".
/// </summary>
public class JoinTests
{
    private const string Secret = "join-secret";

    [Fact]
    public async Task ANodeJoinsANodeAndBothSidesEndUpHoldingTheOther()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);

        MemberAddResult result = await a.Resolve<MemberHandshakeService>()
            .AddMemberAsync(b.Url, nickname: "Box", default);

        Assert.Equal(MemberAddOutcome.Added, result.Outcome);
        Assert.Equal("member-b", result.Member?.MemberId);
        Assert.Equal(MemberKind.Node, result.Member?.Kind);
        Assert.Equal("Box", result.Member?.Nickname);

        // Symmetric: adding B from A leaves the same cluster as adding A from B would have, so B holds A
        // without anybody asking it to.
        MemberRow? mirror = await b.Resolve<MembersStore>().GetByMemberIdAsync("member-a", default);
        Assert.NotNull(mirror);
        Assert.Equal(MemberKind.Node, mirror!.Kind);
    }

    [Fact]
    public async Task AnAnchorJoinsANodeWithNoRouteVersionOfItsOwn()
    {
        // The whole point of the split: a member with no engine, no game servers and no panel is a full
        // member, and the route version is not part of the question.
        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);
        await using MemberHost anchor = await MemberHost.StartAsync("auth-anchor", Secret, kind: MemberKind.Anchor);

        MemberAddResult result = await anchor.Resolve<MemberHandshakeService>()
            .AddMemberAsync(node.Url, nickname: null, default);

        Assert.Equal(MemberAddOutcome.Added, result.Outcome);

        MemberRow? asNodeSeesIt = await node.Resolve<MembersStore>().GetByMemberIdAsync("auth-anchor", default);
        Assert.Equal(MemberKind.Anchor, asNodeSeesIt?.Kind);
        // An anchor states no route version, and nothing fabricates one for it.
        Assert.Equal("", asNodeSeesIt?.ApiVersion);
    }

    [Fact]
    public async Task ANodeJoiningAnAnchorWorksInThatDirectionToo()
    {
        await using MemberHost anchor = await MemberHost.StartAsync("auth-anchor", Secret, kind: MemberKind.Anchor);
        await using MemberHost node = await MemberHost.StartAsync("hotrod", Secret);

        MemberAddResult result = await node.Resolve<MemberHandshakeService>()
            .AddMemberAsync(anchor.Url, nickname: null, default);

        Assert.Equal(MemberAddOutcome.Added, result.Outcome);
        Assert.Equal(MemberKind.Anchor, result.Member?.Kind);
    }

    [Fact]
    public async Task TwoNodesOnDifferentRouteVersionsRefuseEachOther()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret, apiVersion: "v1");
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret, apiVersion: "v2");

        MemberAddResult result = await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        Assert.Equal(MemberAddOutcome.VersionMismatch, result.Outcome);
        Assert.Equal("v2", result.RemoteApiVersion);
        Assert.Null(await a.Resolve<MembersStore>().GetByMemberIdAsync("member-b", default));
    }

    [Fact]
    public async Task AMemberWillNotJoinItself()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        MemberAddResult result = await a.Resolve<MemberHandshakeService>().AddMemberAsync(a.Url, null, default);
        Assert.Equal(MemberAddOutcome.IsSelf, result.Outcome);
    }

    [Fact]
    public async Task AMemberOnADifferentSecretIsNotReachedAtAll()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost stranger = await MemberHost.StartAsync("member-x", "a-different-cluster");

        MemberAddResult result = await a.Resolve<MemberHandshakeService>().AddMemberAsync(stranger.Url, null, default);

        // The far side refuses the token before it ever reads a card, which is indistinguishable from
        // unreachable and is meant to be.
        Assert.Equal(MemberAddOutcome.Unreachable, result.Outcome);
    }

    [Fact]
    public async Task AnUnparseableUrlIsRejectedWithoutACall()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        Assert.Equal(
            MemberAddOutcome.InvalidUrl,
            (await a.Resolve<MemberHandshakeService>().AddMemberAsync("not a url", null, default)).Outcome);
    }

    [Fact]
    public async Task APublicAddressOverPlaintextIsRefused()
    {
        // The cluster secret authenticates but does not encrypt, and what crosses this wire carries identity.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        Assert.Equal(
            MemberAddOutcome.InsecureTransport,
            (await a.Resolve<MemberHandshakeService>().AddMemberAsync("http://example.com", null, default)).Outcome);
    }

    [Theory]
    [InlineData("http://127.0.0.1:8080")]
    [InlineData("http://10.0.0.5:8080")]
    [InlineData("http://192.168.1.4:8080")]
    [InlineData("http://hotbox")]
    [InlineData("http://hotbox.lan")]
    [InlineData("https://panel.example.com")]
    public void APrivateOrEncryptedAddressIsAcceptable(string url)
    {
        Assert.True(MemberHandshakeService.IsTransportAcceptable(url));
    }

    [Theory]
    [InlineData("http://example.com")]
    [InlineData("http://8.8.8.8")]
    [InlineData("ftp://10.0.0.1")]
    [InlineData("not a url")]
    public void APublicPlaintextOrNonHttpAddressIsNot(string url)
    {
        Assert.False(MemberHandshakeService.IsTransportAcceptable(url));
    }

    [Fact]
    public async Task JoiningTwiceLeavesOneRow()
    {
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        MemberHandshakeService handshake = a.Resolve<MemberHandshakeService>();

        await handshake.AddMemberAsync(b.Url, null, default);
        await handshake.AddMemberAsync(b.Url, null, default);

        Assert.Single(await a.Resolve<MembersStore>().ListAsync(default));
    }

    [Fact]
    public async Task JoiningTeachesAMemberAnAddressItCouldNotHaveWorkedOutAlone()
    {
        // A member behind a proxy has no local trace of where it is reached. The operator's URL is handed
        // over in the exchange, and the far side adopts it — which is what lets a member join with nothing
        // configured but the secret.
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);

        Assert.Empty(await b.Resolve<SelfIdentityStore>().CandidatesAsync(default));

        await a.Resolve<MemberHandshakeService>().AddMemberAsync(b.Url, null, default);

        IReadOnlyList<MemberCandidate> learned = await b.Resolve<SelfIdentityStore>().CandidatesAsync(default);
        Assert.Contains(learned, c => c.Url == b.Url.TrimEnd('/') && c.Client);
    }
}
