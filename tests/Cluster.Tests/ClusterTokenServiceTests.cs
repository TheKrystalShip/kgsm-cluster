using Microsoft.Extensions.Logging.Abstractions;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster.Tests;

public class ClusterTokenServiceTests
{
    private static ClusterTokenService Service(string memberId, string secret, string previous = "")
        => new(
            new ClusterOptions { MemberId = memberId, Secret = secret, SecretPrevious = previous, StorePath = "x.db" },
            NullLogger<ClusterTokenService>.Instance);

    [Fact]
    public async Task AMintedTokenValidatesBackToTheMemberThatMintedIt()
    {
        ClusterTokenService a = Service("member-a", "shared");
        ClusterTokenService b = Service("member-b", "shared");

        ClusterPrincipal? principal = await b.ValidateAsync(a.Mint().Token);
        Assert.Equal("member-a", principal?.MemberId);
    }

    [Fact]
    public async Task ATokenSignedWithADifferentSecretIsRejected()
    {
        ClusterPrincipal? principal = await Service("b", "ours").ValidateAsync(Service("a", "theirs").Mint().Token);
        Assert.Null(principal);
    }

    [Fact]
    public async Task ThePreviousSecretIsAcceptedDuringARotationOverlap()
    {
        ClusterTokenService onOldSecret = Service("member-a", "old");
        ClusterTokenService rotated = Service("member-b", secret: "new", previous: "old");

        Assert.Equal("member-a", (await rotated.ValidateAsync(onOldSecret.Mint().Token))?.MemberId);
        // And the new secret still works, which is what makes the overlap usable in either direction.
        Assert.Equal("member-b", (await rotated.ValidateAsync(rotated.Mint().Token))?.MemberId);
    }

    [Fact]
    public async Task OnceTheOverlapIsClearedTheOldSecretStopsWorking()
    {
        ClusterTokenService onOldSecret = Service("member-a", "old");
        Assert.Null(await Service("member-b", "new").ValidateAsync(onOldSecret.Mint().Token));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-jwt")]
    [InlineData("a.b.c")]
    public async Task AMalformedTokenIsRejectedRatherThanThrowing(string token)
    {
        Assert.Null(await Service("member-b", "shared").ValidateAsync(token));
    }

    [Fact]
    public void AnUnclusteredMemberCannotMint()
    {
        // Minting with no secret would produce a token nobody, including this member, could validate.
        Assert.Throws<InvalidOperationException>(() => Service("member-a", "").Mint());
    }

    [Fact]
    public async Task AnUnclusteredMemberValidatesNothing()
    {
        ClusterTokenService clustered = Service("member-a", "shared");
        Assert.Null(await Service("member-b", "").ValidateAsync(clustered.Mint().Token));
    }

    [Fact]
    public void AMintedTokenCarriesItsOwnExpiry()
    {
        MintedClusterToken minted = Service("member-a", "shared").Mint();
        Assert.InRange(minted.ExpiresAt, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddMinutes(2));
    }
}
