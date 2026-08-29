namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// One member as the gossip exchange carries it. Deliberately separate from the durable message envelope:
/// a sync round is best-effort anti-entropy, never persisted to an outbox and never retried to a corpse.
/// </summary>
/// <param name="MemberId">The member's own identity — the merge key, and for a sender's self-entry the
/// identity a refutation is asserted under.</param>
/// <param name="Kind">Node or anchor, so a member learned entirely through gossip is known to be one or the
/// other before anybody reaches it.</param>
/// <param name="Candidates">Every address the member answers at, most-trusted first. Empty when a member
/// knows no address of its own — an honest gap the receiver leaves unfilled rather than guessing one.</param>
/// <param name="Incarnation">The member's own counter — the primary merge ordering key; a strictly higher
/// value always wins.</param>
/// <param name="State">The member's membership state: alive, suspect, dead or left. Never the derived
/// joining, which is a local read-side display only.</param>
/// <param name="ApiVersion">A node's route version, propagated so a gossip-discovered node's version is
/// known before this member authenticates it first-hand. Empty for an anchor, and empty when not yet known.</param>
public sealed record SyncMember(
    string MemberId,
    string Kind,
    IReadOnlyList<MemberCandidate> Candidates,
    long Incarnation,
    string State,
    string ApiVersion);

/// <summary>The sync request — the caller's full roster view, including its own self-entry.</summary>
/// <param name="From">The caller's member id, which equals its service token's <c>iss</c>.</param>
/// <param name="Members">Every member the caller knows: its self-entry plus its peers.</param>
public sealed record SyncRequest(string From, IReadOnlyList<SyncMember> Members);

/// <summary>The sync response — the receiver's full roster view for the caller to merge back, the pull half
/// of push-pull. The same shape as the request.</summary>
public sealed record SyncResponse(string From, IReadOnlyList<SyncMember> Members);
