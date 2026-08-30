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

    /// <summary>The report is about us, agrees we are alive, and carries an incarnation ahead of our own —
    /// what a restart leaves behind. Climb past it so this member can be heard again. No row is written.</summary>
    CatchUpSelf,
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
/// <para><b>A member's own entry has to be able to move.</b> Everything self-asserted — published facts,
/// addressing, kind — rides the self-entry and is taken only when that entry supersedes. So a member raises
/// its own incarnation when it changes what it says, and climbs past the mesh when the mesh is ahead of it.
/// Neither is refutation; both are the same requirement, that what a member says about itself can be heard
/// more than once.</para>
/// <para><b>A departure is a correction, never a lesson.</b> Terminal states travel so a member still
/// holding an alive row is put right; they are not learned by a member holding no row at all. Reaping is a
/// deletion, and anti-entropy repairs deletions — so a tombstone that inserts is one every member teaches
/// back to whichever member reaped it first, forever.</para>
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
            // The mesh is ahead of us about ourselves, which a restart causes: the counter is not persisted,
            // so it resets to zero while every other member still holds where the previous process reached.
            // Until this member climbs back past that, nothing it says about itself supersedes anything —
            // it is alive, agreed to be alive, and unable to change one word of its own entry.
            if (incoming.Incarnation > selfIncarnation)
                return new MergeOutcome(MergeAction.CatchUpSelf, incoming.Incarnation + 1);
            return new MergeOutcome(MergeAction.Ignore);
        }

        // A member held nowhere here is not taught by a tombstone. A terminal report exists to correct a
        // row that still says alive, and a member with no row has nothing to correct — learning one instead
        // re-creates the row the reaper just dropped, stamped with a fresh state-changed clock, so the
        // tombstone restarts its reap window on every member that hears it and ages out on none of them.
        if (existing is null)
            return new MergeOutcome(
                GossipState.IsTerminal(incoming.State) ? MergeAction.Ignore : MergeAction.Insert);

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
