using System.Net.Http.Headers;

using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// How one member names the person it is acting for on another.
/// </summary>
/// <remarks>
/// <para>
/// A cluster-wide surface answers about machines it does not run, and a person asking it something in
/// Discord holds no session anywhere. So a member calls another <b>as a member</b>, naming the person it
/// is acting for, and the receiving member decides what that person may do by reading its own replica of
/// the cluster's accounts.
/// </para>
/// <para>
/// <b>Who is asserted; what is not.</b> The caller states a handle and nothing else — no tier, no scope,
/// no claim about what should be allowed. A compromised caller can therefore act as somebody it names and
/// never above what that person actually holds, which is strictly narrower than a shared secret that
/// forwards an authority along with an identity.
/// </para>
/// <para>
/// <b>No user credential travels.</b> The caller authenticates as itself with a member service token, so
/// a person's session never leaves the member it was presented to — and a long action cannot fail because
/// a token minted for a browser expired underneath it.
/// </para>
/// <para>
/// The names live here, in the transport every member already speaks, rather than beside the code that
/// resolves a handle to an account: a surface that only ever <em>sends</em> one — a chat surface relaying
/// a turn — needs the spelling and nothing about accounts at all.
/// </para>
/// </remarks>
public static class MemberActing
{
    /// <summary>
    /// The person being acted for, as a <c>provider:subject</c> handle — the same string a session's
    /// <c>sub</c> carries, so the receiving member resolves it exactly as it resolves a sign-in and
    /// records the same actor in its audit.
    /// </summary>
    /// <remarks>
    /// A handle rather than an account id, because an account id names somebody only inside the store that
    /// minted it, while a handle is what every member already keys identity on — and it is what an audit
    /// line has to carry to say who did something.
    /// </remarks>
    public const string ActingHandleHeader = "X-Kgsm-Acting";

    /// <summary>
    /// The claim naming the member that asserted this identity, stamped by the receiver rather than sent
    /// by the caller.
    /// </summary>
    /// <remarks>
    /// Who somebody is and who vouched for them are different facts, and only the second says where an
    /// action came from when the person never touched this machine. Kept out of the audit's actor, which
    /// is the person: a line reading "the assistant did it" would lose the human it did it for.
    /// </remarks>
    public const string ActingMemberClaim = "acting_member";

    /// <summary>The authentication scheme a member-acting call is authenticated under.</summary>
    /// <remarks>
    /// Its own scheme rather than a variant of the session one: what proves the caller, what identifies the
    /// person, and where the tier comes from are all different, and sharing a scheme would mean one handler
    /// holding two unrelated stories about how a request became trusted.
    /// </remarks>
    public const string Scheme = "MemberActing";
}

/// <summary>
/// Authenticating a member-to-member request this member is <b>making</b>.
/// </summary>
/// <remarks>
/// <para>
/// The outbound mirror of <see cref="ClusterRequest.AuthenticateAsync"/>, and public for the same reason:
/// a member with its own member-to-member calls — an assistant reaching a node's Control Panel API, a chat
/// surface relaying a turn — has to present exactly what this package's own callers present. Two
/// implementations of that is two members able to disagree about what the protocol is, which is the drift
/// this package exists to remove.
/// </para>
/// <para>
/// The token is passed in rather than minted here. A fan-out across the roster deliberately mints once and
/// reuses, so minting inside would quietly turn one signature into one per peer.
/// </para>
/// </remarks>
public static class ClusterCall
{
    private const string BearerScheme = "Bearer";

    /// <summary>Present this member's service token — what makes the call a member's rather than anyone's.</summary>
    public static void Authorize(HttpRequestMessage request, MintedClusterToken token)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(token);
        request.Headers.Authorization = new AuthenticationHeaderValue(BearerScheme, token.Token);
    }

    /// <summary>
    /// Present this member's service token and name the person the call acts for.
    /// </summary>
    /// <remarks>
    /// A blank handle writes no header, which leaves an ordinary member-to-member call — one the member
    /// makes for itself. That is the honest shape for a sweep or a warm-up with nobody behind it, and the
    /// receiver refuses anything that needed a person, rather than the caller acting as nobody with a
    /// machine's credential.
    /// </remarks>
    public static void ActFor(HttpRequestMessage request, MintedClusterToken token, string? actingHandle)
    {
        Authorize(request, token);

        if (!string.IsNullOrWhiteSpace(actingHandle))
            request.Headers.TryAddWithoutValidation(MemberActing.ActingHandleHeader, actingHandle);
    }
}
