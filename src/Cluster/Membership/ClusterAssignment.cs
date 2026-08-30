namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// Which member holds one capability for the whole cluster.
/// </summary>
/// <remarks>
/// <para>
/// <b>A map, not a policy.</b> This stores one value per capability and says nothing about what holding
/// one means. Whether a capability may have exactly one holder, whether a second install is a candidate
/// or an error, and what a holder is expected to do are the consuming component's rules, not this
/// mechanism's.
/// </para>
/// <para>
/// <b>Cluster state, not member state.</b> A member cannot assert who holds a capability the way it
/// asserts its own address, so this cannot ride a member's incarnation. It carries its own version, and a
/// strictly higher version wins.
/// </para>
/// </remarks>
/// <param name="Capability">What is held — <c>auth</c> today.</param>
/// <param name="MemberId">The member holding it. Empty means the capability is deliberately held by
/// nobody, which is a state that must converge like any other and so keeps its version.</param>
/// <param name="Version">Monotonic per capability. A strictly higher version supersedes.</param>
/// <param name="SetBy">The member that recorded this. It breaks a tie between two writes that raced to
/// the same version, so two members assigning at once converge on one answer rather than flapping.</param>
public sealed record ClusterAssignment(string Capability, string MemberId, long Version, string SetBy)
{
    /// <summary>Whether this assignment supersedes <paramref name="other"/>. A higher version wins; at an
    /// equal version the higher member id wins, which is arbitrary but total, and total is what stops two
    /// members trading the value back and forth forever.</summary>
    public bool Supersedes(ClusterAssignment? other)
    {
        if (other is null) return true;
        if (Version != other.Version) return Version > other.Version;
        return string.CompareOrdinal(SetBy, other.SetBy) > 0;
    }

    /// <summary>Whether anybody holds it.</summary>
    public bool IsHeld => !string.IsNullOrWhiteSpace(MemberId);
}

/// <summary>The capabilities a cluster assigns. A capability is a plain string; these are the ones the
/// ecosystem defines.</summary>
public static class ClusterCapability
{
    /// <summary>The cluster's accounts: who holds the store, and the only member that writes it.</summary>
    public const string Auth = "auth";
}
