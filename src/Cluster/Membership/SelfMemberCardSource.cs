namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// This member's card, built from what the package already knows about it: its id, its kind, whether it is
/// clustered, its incarnation, and every address reflected onto it. No node block, because none of what
/// goes in one — a route version, a build, the leaves running here — is the package's to state.
/// </summary>
/// <remarks>
/// This is the whole card for an <see cref="MemberKind.Anchor"/>: an anchor serves no route version of its
/// own and runs no leaves, so there is nothing further to say and nothing further to configure. A node
/// registers its own <see cref="IMemberCardSource"/> over this one, wrapping it to add the node block.
/// </remarks>
public sealed class SelfMemberCardSource(
    ClusterOptions options,
    SelfIdentityStore selfIdentity,
    SelfIncarnation selfIncarnation) : IMemberCardSource
{
    /// <inheritdoc/>
    public async Task<MemberCard> BuildAsync(CancellationToken ct) => new(
        options.MemberId,
        options.Kind,
        options.Enabled,
        await selfIdentity.CandidatesAsync(ct).ConfigureAwait(false),
        selfIncarnation.Current,
        ClusterProtocol.Current);
}
