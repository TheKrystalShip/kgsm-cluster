using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// The roster-backed member gate: a member bearing a validly-signed service token is enabled unless its
/// row here is explicitly disabled.
/// </summary>
/// <remarks>
/// <para>
/// <b>A disable-list, not an allow-list.</b> Absence from the roster is not rejection — holding the
/// cluster secret already proves membership, so a validly-tokened member this one has never heard of is
/// accepted. That is what keeps the mesh working while members still hold a partial view of each other,
/// and it is why a member learned only through gossip can be reached before anybody introduces it.
/// </para>
/// <para>
/// <b>Keyed on member id, never on a machine.</b> Two members on one machine are two members: disabling
/// a node has to leave the anchor beside it running, and keying this by host would disable both while
/// looking like it disabled one.
/// </para>
/// </remarks>
public sealed class RosterMemberGate(MembersStore members, ILogger<RosterMemberGate> logger) : IClusterMemberGate
{
    /// <inheritdoc/>
    public async Task<bool> IsEnabledAsync(string memberId)
    {
        MemberRow? row = await members.GetByMemberIdAsync(memberId, CancellationToken.None).ConfigureAwait(false);
        if (row is not null && !row.Enabled)
        {
            logger.LogDebug("Rejecting cluster call from disabled member {MemberId}.", memberId);
            return false;
        }
        return true;
    }
}
