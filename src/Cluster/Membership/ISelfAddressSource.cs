namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// Addresses this member has been given at runtime and is serving, such as a name the cluster's DNS
/// anchor assigned it once its certificate is installed.
/// </summary>
/// <remarks>
/// <para>
/// Every address here is one a browser can use: it is offered to other members as a client address,
/// ahead of everything this member learned by reflection and behind only what its operator configured.
/// </para>
/// <para>
/// Read on every resolve and never stored. A name is only true while it is served, and a copy in the
/// member's own table would go on being advertised after the name moved to somebody else.
/// </para>
/// </remarks>
public interface ISelfAddressSource
{
    /// <summary>Absolute <c>https</c> addresses, most-preferred first. Empty when none is served yet.</summary>
    IReadOnlyList<string> Addresses { get; }
}
