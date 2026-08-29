namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// The member-to-member paths, named once so a sender and a receiver can never disagree about them.
/// A member serves these and posts to another member's copy of them.
/// </summary>
public static class ClusterRoutes
{
    /// <summary>The prefix every member-to-member route sits under.</summary>
    public const string Prefix = "/api/v1/members";

    /// <summary>Where a durable envelope is delivered.</summary>
    public const string Inbox = Prefix + "/inbox";
}
