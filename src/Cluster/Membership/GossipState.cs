namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// The membership states gossip converges on, and their conflict-ordering precedence. Stored on a roster
/// row, exchanged in the sync payload, and ordered by incarnation then precedence when two reports about
/// one member disagree.
/// </summary>
/// <remarks>
/// <b>Two liveness axes, never conflated.</b> A row's first-hand status is what this member's own probe
/// found; these states are what the mesh converged on. <see cref="Joining"/> is deliberately not a stored
/// or gossiped state — it is a derived display value, alive per gossip but never yet reached first-hand, so
/// an unverified member heard about second-hand is never shown a plain alive while the state that
/// propagates onward stays the honest alive it heard.
/// </remarks>
public static class GossipState
{
    /// <summary>Confirmed a member — either authenticated first-hand here, or vouched for by the mesh.</summary>
    public const string Alive = "alive";

    /// <summary>This member's own probes are failing and have not yet escalated. A local hypothesis, never
    /// adopted from gossip to override a member this one can currently reach.</summary>
    public const string Suspect = "suspect";

    /// <summary>Presumed gone. Refutable: the member itself, on return, beats this with a higher
    /// incarnation.</summary>
    public const string Dead = "dead";

    /// <summary>Gracefully departed. Terminal like <see cref="Dead"/> for reaping, distinguished so a clean
    /// leave reads differently from a crash.</summary>
    public const string Left = "left";

    /// <summary>Derived display only, neither stored nor gossiped: gossip says <see cref="Alive"/> but this
    /// member has never reached it first-hand.</summary>
    public const string Joining = "joining";

    /// <summary>
    /// Conflict precedence at equal incarnation — higher wins. A suspicion at the same incarnation overrides
    /// alive; a terminal state overrides both. Refutation across incarnations is handled separately in
    /// <see cref="RosterMerger"/>: a strictly higher incarnation always wins regardless of state, which is
    /// how a returning member beats its own dead.
    /// </summary>
    public static int Precedence(string state) => state switch
    {
        Alive => 0,
        Suspect => 1,
        Left => 2,
        Dead => 3,
        _ => 0,
    };

    /// <summary>States eligible for reaping once the reap window elapses.</summary>
    public static bool IsTerminal(string state) => state is Dead or Left;

    /// <summary>
    /// The derived membership a consumer sees: a member gossip reports alive but this one has never reached
    /// first-hand shows as <see cref="Joining"/>; every other case shows the converged state verbatim. A
    /// pure read-side projection — the stored and gossiped state stays alive, so relaying it never demotes it.
    /// </summary>
    public static string Display(string membershipState, DateTimeOffset? lastSeen) =>
        membershipState == Alive && lastSeen is null ? Joining : membershipState;
}
