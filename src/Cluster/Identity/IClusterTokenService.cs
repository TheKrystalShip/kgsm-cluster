namespace TheKrystalShip.KGSM.Cluster.Identity;

/// <summary>
/// Mints and validates the member service token — the bearer every member-to-member call
/// authenticates with. A short-lived HMAC-signed JWT carrying <c>iss</c> = the sending member's id and
/// <c>aud=cluster</c>, it proves membership of this cluster and nothing more: it is not a person's
/// identity and carries no tier.
/// </summary>
/// <remarks>
/// <b>Attribution, not isolation.</b> Members share one cluster secret, so any member can mint a token
/// bearing another member's <c>iss</c>. The token says which member a call claims to be from, which is
/// what the roster gate and the <c>from</c>-matches-<c>iss</c> check are built on; it does not isolate
/// one member from another. Per-member keypairs are the upgrade that would.
/// </remarks>
public interface IClusterTokenService
{
    /// <summary>
    /// Mint a fresh service token for this member. Throws when the member is not clustered: with no
    /// secret, minting would produce a token nobody, including this same member, could validate.
    /// </summary>
    MintedClusterToken Mint();

    /// <summary>
    /// Validate a presented token: signature against the current secret, then the previous one during a
    /// rotation overlap, plus audience and lifetime. Returns the calling member's identity on success
    /// and <see langword="null"/> on any failure — bad signature, wrong audience, expired, malformed, or
    /// no secret configured. Fail-closed: it never throws and never partially trusts a token.
    /// </summary>
    Task<ClusterPrincipal?> ValidateAsync(string token);
}

/// <summary>A just-minted token and its absolute UTC expiry, so a caller never decodes the JWT to know
/// when to mint another.</summary>
public sealed record MintedClusterToken(string Token, DateTimeOffset ExpiresAt);

/// <summary>The authenticated identity of a member that presented a valid service token: its member id,
/// read from the token's <c>iss</c>.</summary>
public sealed record ClusterPrincipal(string MemberId);
