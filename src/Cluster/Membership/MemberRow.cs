namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// One row of this member's own copy of the roster. A cluster is masterless: every member keeps its own
/// copy and there is no authoritative table anywhere. A row arrives from the join handshake and, once
/// gossip is running, converges automatically from members this one was never directly told about.
/// </summary>
/// <param name="Id">This member's local handle on the row, distinct from <see cref="MemberId"/>, which is
/// the other member's own identity. Assigned locally when the row appears, so it is stable even if the far
/// side's address later changes.</param>
/// <param name="MemberId">The other member's own stable identity, as its identity endpoint reports it and
/// as its service tokens carry it. This is the lookup key an inbound call is attributed against.</param>
/// <param name="Kind">Node or anchor.</param>
/// <param name="Url">The address this member calls the other on: the best candidate, pinned once a probe
/// has answered it under the expected member id. Before any candidate has answered it holds the
/// most-trusted one on offer and <see cref="AddressVerified"/> is false, so a caller can tell a proven
/// address from a proposed one. Empty when no address has been offered at all.</param>
/// <param name="Candidates">Every address the member says it answers at, most-trusted first, encoded by
/// <see cref="MemberCandidates"/>.</param>
/// <param name="AddressVerified">Whether <see cref="Url"/> has answered a probe under this
/// <see cref="MemberId"/>. An unverified address is a claim, never reported as a fact.</param>
/// <param name="Nickname">An operator-assigned label. Null leaves a consumer to fall back to the member id.</param>
/// <param name="Incarnation">The member's own monotonic counter, which only that member may raise.</param>
/// <param name="Status">Last-observed <b>first-hand</b> liveness: reachable, unreachable, or unknown — the
/// honest cold-start default before the first probe completes, never fabricated as reachable. Written only
/// by this member's own probes, never by gossip.</param>
/// <param name="MembershipState">The gossip-converged state (see <see cref="GossipState"/>), the other of
/// the two orthogonal liveness axes: <see cref="Status"/> is what this member observed directly,
/// this is what the mesh agreed.</param>
/// <param name="StateChangedAt">When <see cref="MembershipState"/> last transitioned — the clock the
/// failure timers run off. Null until the first transition this member records.</param>
/// <param name="LatencyMs">Round-trip latency of the last successful probe. Null when never reached.</param>
/// <param name="LastSeen">When this member was last successfully reached, or last authenticated an inbound
/// call. Null when neither has happened.</param>
/// <param name="ApiVersion">A node's route version as reported at handshake time. Empty for an anchor.</param>
/// <param name="Enabled">The disable-list flag — the only local override to the shared-secret trust
/// boundary. Flipping it to false rejects that member's calls without removing the row. Absence from the
/// roster is not rejection; only an explicit false here is.</param>
public sealed record MemberRow(
    string Id,
    string MemberId,
    string Kind,
    string Url,
    string Candidates,
    bool AddressVerified,
    string? Nickname,
    long Incarnation,
    string Status,
    string MembershipState,
    DateTimeOffset? StateChangedAt,
    int? LatencyMs,
    DateTimeOffset? LastSeen,
    string ApiVersion,
    bool Enabled)
{
    /// <summary>A fresh row for a member nothing is yet known about beyond its identity.</summary>
    public static MemberRow New(string memberId, string kind) => new(
        Id: "member_" + Guid.NewGuid().ToString("N")[..10],
        MemberId: memberId,
        Kind: kind,
        Url: "",
        Candidates: "",
        AddressVerified: false,
        Nickname: null,
        Incarnation: 0,
        Status: MemberStatus.Unknown,
        MembershipState: GossipState.Alive,
        StateChangedAt: null,
        LatencyMs: null,
        LastSeen: null,
        ApiVersion: "",
        Enabled: true);
}

/// <summary>The first-hand liveness values a probe writes onto <see cref="MemberRow.Status"/>.</summary>
public static class MemberStatus
{
    /// <summary>A probe answered.</summary>
    public const string Reachable = "reachable";

    /// <summary>Every candidate address failed.</summary>
    public const string Unreachable = "unreachable";

    /// <summary>No probe has completed yet. The honest cold-start default.</summary>
    public const string Unknown = "unknown";
}
