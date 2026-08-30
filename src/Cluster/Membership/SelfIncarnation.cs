namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// This member's own monotonic incarnation counter — the mechanism it uses to refute a false suspect or
/// dead that gossip is carrying about it. Only a member may raise its own incarnation, and a strictly
/// higher one always wins the merge, so re-asserting alive one above the stale report supersedes it
/// everywhere it has spread.
/// </summary>
/// <remarks>
/// A process-lifetime value starting at zero, deliberately not persisted. On restart it resets, and the
/// member climbs back above whatever the mesh holds about it from the mesh's own reports — see
/// <see cref="AdoptAheadOf"/> — so its identity recovers with no counter on disk.
/// <para>
/// <b>It also moves when this member changes what it says about itself.</b> A fact or an address is only
/// taken by another member if the entry carrying it supersedes the one already held, and at equal
/// incarnation nothing supersedes. A member that never raises its own counter can therefore never change
/// anything it publishes, on a cluster where it stays perfectly healthy.
/// </para>
/// Thread-safe: the gossip worker and the sync request path both touch it concurrently.
/// </remarks>
public sealed class SelfIncarnation
{
    private long _value;

    /// <summary>The incarnation this member stamps onto its own gossip self-entry.</summary>
    public long Current => Interlocked.Read(ref _value);

    /// <summary>
    /// Refute a stale report about ourselves: if <paramref name="observedIncarnation"/> is at least the
    /// current value, jump one past it so the re-asserted alive strictly supersedes it. A monotonic
    /// compare-and-swap loop — it never regresses and is safe under concurrent callers. Returns the
    /// resulting incarnation, which may be unchanged.
    /// </summary>
    public long RaiseToRefute(long observedIncarnation) => RaiseTo(observedIncarnation + 1);

    /// <summary>
    /// Climb past an incarnation the mesh holds about us that is strictly ahead of our own — the case a
    /// restart creates, since the counter resets to zero while every other member still holds where the
    /// previous process got to. Landing one past it, rather than level with it, is what makes the next
    /// self-entry supersede: level would tie, and a tie is ignored.
    /// </summary>
    /// <remarks>
    /// Strictly ahead, never level, or this runs away: at rest every member reports our own value back to
    /// us, and treating that as a reason to climb would raise the incarnation once per gossip round for as
    /// long as the cluster is healthy. A member holding the cluster secret can push this value up, which is
    /// the same trust boundary every other self-asserted field sits behind.
    /// </remarks>
    public long AdoptAheadOf(long observedIncarnation)
        => observedIncarnation > Current ? RaiseTo(observedIncarnation + 1) : Current;

    /// <summary>
    /// Assert something new about ourselves: step the incarnation up one so this member's next self-entry
    /// supersedes the one every other member is holding. What a member says about itself is otherwise
    /// indistinguishable from what it said last round.
    /// </summary>
    public long Advance() => Interlocked.Increment(ref _value);

    /// <summary>Raise to <paramref name="target"/> if it is ahead of where we are; never regress. A
    /// monotonic compare-and-swap loop, safe under concurrent callers.</summary>
    private long RaiseTo(long target)
    {
        while (true)
        {
            long current = Interlocked.Read(ref _value);
            if (target <= current)
                return current;
            if (Interlocked.CompareExchange(ref _value, target, current) == current)
                return target;
        }
    }
}
