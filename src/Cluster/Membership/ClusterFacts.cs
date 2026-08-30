namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// Reading what a capability's holder states about itself — the two halves of membership state joined at
/// the one place a consumer actually needs them together.
/// </summary>
public sealed class ClusterFacts(MembersStore members, ClusterStateStore state)
{
    /// <summary>
    /// A fact stated by the member that holds <paramref name="capability"/>, or <see langword="null"/> when
    /// nobody holds it, the holder is not in this member's roster, or it states nothing under that key.
    /// </summary>
    /// <remarks>
    /// <b>This is the safe way to read a published fact, and the reason it exists.</b> Reading a key off
    /// whichever member happens to state it lets any member in the cluster answer for a capability it does
    /// not hold — which for a signing key means substituting the one that sessions are verified against.
    /// Going through the assignment means only the holder is believed, so substituting a key requires
    /// reassigning the capability, and a reassignment is a visible change to cluster state rather than a
    /// silent one.
    /// <para>
    /// It does not make a compromised member harmless: a member holding the cluster secret can reassign a
    /// capability to itself and then be believed. What it removes is the silent version of that.
    /// </para>
    /// </remarks>
    public async Task<string?> FromHolderAsync(string capability, string key, CancellationToken ct)
    {
        string? holder = await state.HolderAsync(capability, ct).ConfigureAwait(false);
        if (holder is null) return null;

        MemberRow? row = await members.GetByMemberIdAsync(holder, ct).ConfigureAwait(false);
        return row?.Read(key);
    }

    /// <summary>
    /// Every capability assigned to a member that is not in this member's roster — an assignment that has
    /// outlived what it names.
    /// </summary>
    /// <remarks>
    /// A holder can leave the roster without anybody removing it: its machine dies, it goes suspect, then
    /// dead, and the reap window passes. The assignment survives, so every member stands by against a
    /// holder that will never answer and the capability is simply not served — with no error anywhere,
    /// because nothing failed. This is how that state is found rather than deduced from a symptom.
    /// <para>
    /// Not an authority on what to do about it. Reassigning is a decision, and a member that has merely
    /// not heard of the holder yet is indistinguishable here from one whose holder is gone — which is why
    /// this reports rather than acts.
    /// </para>
    /// </remarks>
    public async Task<IReadOnlyList<ClusterAssignment>> OrphanedAsync(CancellationToken ct)
    {
        var orphaned = new List<ClusterAssignment>();
        foreach (ClusterAssignment assignment in await state.ListAsync(ct).ConfigureAwait(false))
        {
            if (!assignment.IsHeld) continue;
            if (await members.GetByMemberIdAsync(assignment.MemberId, ct).ConfigureAwait(false) is null)
                orphaned.Add(assignment);
        }
        return orphaned;
    }

    /// <summary>Everything the member holding <paramref name="capability"/> states about itself. Empty when
    /// nobody holds it or the holder is not in this member's roster.</summary>
    public async Task<IReadOnlyDictionary<string, string>> AllFromHolderAsync(
        string capability, CancellationToken ct)
    {
        string? holder = await state.HolderAsync(capability, ct).ConfigureAwait(false);
        if (holder is null) return PublishedFacts.None;

        MemberRow? row = await members.GetByMemberIdAsync(holder, ct).ConfigureAwait(false);
        return row is null ? PublishedFacts.None : PublishedFacts.Decode(row.Published);
    }
}
