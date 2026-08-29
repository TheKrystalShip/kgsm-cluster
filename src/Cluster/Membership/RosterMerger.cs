namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>What <see cref="RosterMerger.Decide"/> concluded for one incoming gossip member.</summary>
public enum MergeAction
{
    /// <summary>Do nothing — the report is stale, about a locally-disabled member, or loses the ordering to
    /// what is already held.</summary>
    Ignore,

    /// <summary>A member never heard of — insert it as hearsay, provisional until a probe authenticates it.</summary>
    Insert,

    /// <summary>The report supersedes the current row — adopt its state, incarnation and addressing, but
    /// never its first-hand liveness, which only this member's own probe writes.</summary>
    Update,

    /// <summary>The report is about us and is non-alive at or above our incarnation — refute it by raising
    /// our own and re-asserting alive. No row is written.</summary>
    RefuteSelf,
}

/// <summary>The decision, plus the incarnation to jump to when refuting.</summary>
public readonly record struct MergeOutcome(MergeAction Action, long RaiseSelfTo = 0);

/// <summary>
/// The anti-entropy merge core: for one incoming gossip member against the current row for it, what to do.
/// Side-effect-free and dependency-free, so the rules are testable in isolation from storage and HTTP;
/// <see cref="GossipService"/> is the thin shell that reads the row, calls this, and applies the outcome.
/// </summary>
/// <remarks>
/// <para><b>The ordering.</b> A strictly higher incarnation always wins — that is what lets a returning
/// member beat its own stale dead by re-asserting alive one above it. At equal incarnation the higher
/// <see cref="GossipState.Precedence"/> wins, with one guard below.</para>
/// <para><b>First-hand evidence beats equal-incarnation hearsay.</b> If this member is currently reaching
/// another with its own probe, an equal-incarnation gossiped suspect or dead does not override that: its
/// own eyes outrank somebody else's report. Only a strictly higher incarnation — the member itself, or its
/// operator, moving on — overrides first-hand liveness.</para>
/// <para><b>A local disable is absolute.</b> A row disabled here is never resurrected or altered by gossip:
/// disable is this member's own override of the shared-secret trust.</para>
/// </remarks>
public static class RosterMerger
{
    public static MergeOutcome Decide(
        SyncMember incoming,
        MemberRow? existing,
        string myMemberId,
        long selfIncarnation,
        bool existingFirstHandFresh)
    {
        // A report about ourselves — never a row; refute it if it is stale and negative.
        if (string.Equals(incoming.MemberId, myMemberId, StringComparison.Ordinal))
        {
            if (incoming.State != GossipState.Alive && incoming.Incarnation >= selfIncarnation)
                return new MergeOutcome(MergeAction.RefuteSelf, incoming.Incarnation + 1);
            return new MergeOutcome(MergeAction.Ignore);
        }

        if (existing is null)
            return new MergeOutcome(MergeAction.Insert);

        if (!existing.Enabled)
            return new MergeOutcome(MergeAction.Ignore);

        return Supersedes(incoming, existing, existingFirstHandFresh)
            ? new MergeOutcome(MergeAction.Update)
            : new MergeOutcome(MergeAction.Ignore);
    }

    private static bool Supersedes(SyncMember incoming, MemberRow existing, bool existingFirstHandFresh)
    {
        // A strictly newer incarnation always wins — the refutation channel.
        if (incoming.Incarnation > existing.Incarnation) return true;
        if (incoming.Incarnation < existing.Incarnation) return false;

        // Equal incarnation: fresh first-hand contact outranks hearsay.
        if (existingFirstHandFresh) return false;

        return GossipState.Precedence(incoming.State) > GossipState.Precedence(existing.MembershipState);
    }
}
