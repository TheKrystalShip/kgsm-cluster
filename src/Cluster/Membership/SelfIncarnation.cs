namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// This member's own monotonic incarnation counter — the mechanism it uses to refute a false suspect or
/// dead that gossip is carrying about it. Only a member may raise its own incarnation, and a strictly
/// higher one always wins the merge, so re-asserting alive one above the stale report supersedes it
/// everywhere it has spread.
/// </summary>
/// <remarks>
/// A process-lifetime value starting at zero, deliberately not persisted. On restart it resets, and the
/// first gossip round in which a returning member hears its own stale report drives
/// <see cref="RaiseToRefute"/> past it — which recovers the member's identity with no counter on disk.
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
    public long RaiseToRefute(long observedIncarnation)
    {
        while (true)
        {
            long current = Interlocked.Read(ref _value);
            if (observedIncarnation < current)
                return current;
            long next = observedIncarnation + 1;
            if (Interlocked.CompareExchange(ref _value, next, current) == current)
                return next;
        }
    }
}
