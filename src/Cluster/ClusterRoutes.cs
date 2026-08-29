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

    /// <summary>The best-effort roster exchange.</summary>
    public const string Sync = Prefix + "/sync";

    /// <summary>The symmetric join exchange.</summary>
    public const string Introduce = Prefix + "/introduce";

    /// <summary>Where a member states who it is.</summary>
    public const string Identity = Prefix + "/identity";
}

/// <summary>
/// The version of the record shapes members exchange with each other.
/// </summary>
/// <remarks>
/// Distinct from a member's own HTTP route version, which stays put across builds and which only two nodes
/// have in common at all. Two members can serve the same routes and still disagree about what a record on
/// the wire contains, and that disagreement is otherwise silent: fields one side does not send simply
/// arrive empty, so a member joins and then quietly cannot be reached. This number is what makes the
/// refusal loud instead. Raise it whenever a record exchanged between members changes shape.
/// </remarks>
public static class ClusterProtocol
{
    /// <summary>The protocol this build speaks.</summary>
    public const int Current = 2;
}
