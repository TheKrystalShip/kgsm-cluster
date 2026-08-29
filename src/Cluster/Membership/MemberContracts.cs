namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// What a member is. A cluster has members, and every member is exactly one of these; it is a deployment
/// choice, not a property of a component — the same code is a node on one machine and an anchor on
/// another.
/// </summary>
public static class MemberKind
{
    /// <summary>Runs the engine and game servers, and hosts leaves.</summary>
    public const string Node = "node";

    /// <summary>Provides one capability to the whole cluster.</summary>
    public const string Anchor = "anchor";

    /// <summary>Whether a value names a kind this build understands. An unrecognized kind is a member from
    /// a build that knows something this one does not, which the protocol version is what refuses.</summary>
    public static bool IsKnown(string? kind) => kind is Node or Anchor;
}

/// <summary>
/// One address a member answers at. Reachability is a property of a <em>pair</em>, not of a member, so a
/// member offers every address it knows of itself and each of its peers pins whichever one answers for it.
/// </summary>
/// <param name="Url">An absolute <c>http(s)</c> address, no trailing slash.</param>
/// <param name="Client">Whether a browser can use this address. The reflection sources are
/// browser-reachable by construction — an operator pastes a URL into a panel, and an observed host is a
/// browser that arrived — so both carry <see langword="true"/>. A configured node-only address and a
/// peer-observed source address do not.</param>
public sealed record MemberCandidate(string Url, bool Client);

/// <summary>
/// The facts only a node has. An anchor sends none of this: it serves no route version of its own, runs no
/// leaves, and has no engine build to report. Absent rather than null-filled, so "I am an anchor" and "I am
/// a build that predates this field" are not the same value on the wire.
/// </summary>
/// <param name="ApiVersion">The node's own route version. Checked only between two nodes — it is a
/// statement about a surface an anchor does not serve.</param>
/// <param name="Build">The node's build identifier.</param>
/// <param name="Capabilities">The ids of the leaves provisioned on the node.</param>
public sealed record NodeFacts(string ApiVersion, string Build, IReadOnlyList<string> Capabilities);

/// <summary>
/// Everything one member needs to know about another — the payload of the symmetric introduce exchange and
/// the body of the identity endpoint's answer.
/// </summary>
/// <param name="MemberId">The member's own stable identity, which its service tokens carry as <c>iss</c>.</param>
/// <param name="Kind">Node or anchor.</param>
/// <param name="Clustered">Whether this member takes part in a cluster at all. A member with no cluster
/// secret answers its identity endpoint honestly and is refused as a member, rather than being joined and
/// then failing every call.</param>
/// <param name="Candidates">Every address the member knows it answers at, most-trusted first.</param>
/// <param name="Incarnation">The member's own monotonic refutation counter.</param>
/// <param name="Protocol">The record version this member speaks (<see cref="ClusterProtocol.Current"/>). A
/// card from a build that predates the field carries <c>0</c>, which is a mismatch, which is the point.</param>
/// <param name="Node">The node-only facts, absent on an anchor.</param>
public sealed record MemberCard(
    string MemberId,
    string Kind,
    bool Clustered,
    IReadOnlyList<MemberCandidate> Candidates,
    long Incarnation,
    int Protocol = 0,
    NodeFacts? Node = null);

/// <summary>An address one member reports back to the other: "this is where I reached you."</summary>
/// <param name="Url">The absolute address.</param>
/// <param name="Provenance"><c>operator</c> — a human pasted this URL into a panel and it answered, the
/// strongest address statement in the system. <c>peer-observed</c> — the source address the receiver saw
/// the request arrive from, a hint that seeds a candidate and that nothing depends on.</param>
public sealed record ReflectedAddress(string Url, string Provenance);

/// <summary>
/// The symmetric join exchange: the introduce endpoint sends this record and answers with the same one.
/// Both sides validate the other's <see cref="Self"/> with the same predicate and record the mirror of what
/// the other records, so adding B from A leaves the identical cluster state as adding A from B.
/// </summary>
/// <param name="Self">The sender's own card.</param>
/// <param name="YouAre">Where the sender reached the receiver. Null when the sender has no address to
/// report — never a fabricated one.</param>
/// <param name="PanelOrigins">
/// Browser origins a member has seen somebody sign in from. Carried, never interpreted: this package
/// transports the list so that a panel served from one member reaches every other without a per-member
/// allowlist, and each member decides for itself what to do with it. A headless member ignores it.
/// </param>
public sealed record IntroduceExchange(
    MemberCard Self,
    ReflectedAddress? YouAre,
    IReadOnlyList<string> PanelOrigins);

/// <summary>
/// Builds this member's card — who it is, what it runs, and every address it knows it answers at. A seam
/// rather than a fixed implementation, because the node facts come from whatever the member has and an
/// anchor supplies none of them.
/// </summary>
public interface IMemberCardSource
{
    /// <summary>This member's card, as it is right now.</summary>
    Task<MemberCard> BuildAsync(CancellationToken ct);
}
