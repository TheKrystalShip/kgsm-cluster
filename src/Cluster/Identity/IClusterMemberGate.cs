namespace TheKrystalShip.KGSM.Cluster.Identity;

/// <summary>
/// The "is this caller an enabled member" check, applied after a service token has already validated.
/// It is a disable-list, not an allow-list: holding the cluster secret already proves membership, so a
/// validly-tokened member this one has never heard of is accepted, and only an explicit disable is
/// rejected. That is what keeps the mesh working while members still have a partial view of each other.
/// </summary>
/// <remarks>
/// <b>Keyed by member id, never by machine.</b> Two members on one machine are two members, and
/// disabling one must leave the other running. Keying this by host would silently disable a co-located
/// member and only surface much later.
/// </remarks>
public interface IClusterMemberGate
{
    /// <summary>Is this member enabled? Called after the caller's token has validated; this is not
    /// itself an auth check.</summary>
    Task<bool> IsEnabledAsync(string memberId);
}

/// <summary>
/// The gate a member uses before it holds a roster: anything that can present a validly-signed service
/// token is enabled. Correct while the shared secret is the entire trust boundary, and replaced by the
/// roster-backed gate once the member has one.
/// </summary>
public sealed class AllowAllClusterMemberGate : IClusterMemberGate
{
    public Task<bool> IsEnabledAsync(string memberId) => Task.FromResult(true);
}
