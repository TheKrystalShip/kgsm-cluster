namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// Everything a member needs to take part in a cluster. Constructed by the consuming member from
/// whatever configuration it already reads, so this package binds no configuration of its own and a
/// member keeps one settings file.
/// </summary>
public sealed record ClusterOptions
{
    /// <summary>
    /// This member's own cluster identity — the value its service tokens carry as <c>iss</c>, and the
    /// key every other member attributes its calls against. One identity per member, not per host: two
    /// members on one machine are two members with two ids.
    /// </summary>
    public required string MemberId { get; init; }

    /// <summary>
    /// The shared cluster secret every member holds. Blank means this member is simply not part of a
    /// cluster: <see cref="Enabled"/> is false, the drainer and GC never start a timer, and the token
    /// service mints nothing and validates nothing.
    /// </summary>
    public required string Secret { get; init; }

    /// <summary>
    /// The previous cluster secret, accepted alongside <see cref="Secret"/> during a rotation overlap:
    /// roll every member onto a new secret one at a time, then clear this.
    /// </summary>
    public string SecretPrevious { get; init; } = "";

    /// <summary>
    /// The SQLite file this member keeps its cluster state in — roster, outbox and inbox. Per member,
    /// under the member's own <c>StateDirectory=</c>, which systemd creates owned by the unit's user.
    /// Two members on one machine hold two files and share nothing.
    /// </summary>
    public required string StorePath { get; init; }

    /// <summary>How often (ms) the outbox drainer ticks. Floor 100.</summary>
    public int DrainMs { get; init; } = 1000;

    /// <summary>
    /// How long (days) a pending outbox row keeps being retried before it is dead-lettered, anchored on
    /// the row's creation. Seven days covers any realistic member outage; a message still queued after
    /// that is an operational alarm, not a silent loss. Floor 1.
    /// </summary>
    public int RetryTtlDays { get; init; } = 7;

    /// <summary>
    /// How long (days) a delivered/dead outbox row and an inbox ledger row survive before the GC prunes
    /// them. Must exceed <see cref="RetryTtlDays"/> so a late redelivery of a long-retried message is
    /// still recognized as a duplicate rather than applied a second time; the constructor-side clamp is
    /// the consumer's, and <see cref="Validate"/> enforces it here.
    /// </summary>
    public int RetentionDays { get; init; } = 30;

    /// <summary>How often (ms) the retention GC sweeps. Floor 1000.</summary>
    public int GcMs { get; init; } = 600000;

    /// <summary>
    /// What this member is: a node that runs the engine and game servers, or an anchor that provides one
    /// capability to the whole cluster. A deployment choice, so it is configuration rather than something
    /// the code decides about itself.
    /// </summary>
    public string Kind { get; init; } = Membership.MemberKind.Node;

    /// <summary>
    /// This member's public address, when it has one an operator can state — behind a reverse proxy, the
    /// address it is reached at leaves no local trace, so a configured value is the only way to be certain.
    /// Merged ahead of everything reflected, which is what lets an operator correct a topology where
    /// reflection reports the wrong thing. Blank means "whatever is learned".
    /// </summary>
    public string PublicBaseUrl { get; init; } = "";

    /// <summary>An address only other members use, never a browser. Blank means none.</summary>
    public string GossipUrl { get; init; } = "";

    /// <summary>How often (ms) the gossip loop runs one push-pull round. Floor 250.</summary>
    public int GossipMs { get; init; } = 5000;

    /// <summary>
    /// How often (ms) this member probes every other's identity endpoint. Keep it comfortably below
    /// <see cref="SuspectMs"/> so several probes fall inside one suspect window; a single missed tick should
    /// never be enough to suspect anybody. Floor 250.
    /// </summary>
    public int PollMs { get; init; } = 10000;

    /// <summary>
    /// How long (ms) a member may go without liveness evidence before it is suspected, and then again
    /// before a suspect is declared dead. Floor 1000.
    /// </summary>
    public int SuspectMs { get; init; } = 30000;

    /// <summary>How long (ms) a dead or departed member's row survives before it is removed. Floor 1000.</summary>
    public int ReapMs { get; init; } = 300000;

    /// <summary>
    /// Whether this member is part of a cluster — a non-blank <see cref="Secret"/>. Everything the
    /// package runs is inert when this is false.
    /// </summary>
    public bool Enabled => !string.IsNullOrWhiteSpace(Secret);

    /// <summary>
    /// Applies every floor and the retention-exceeds-TTL rule, returning the corrected options. A
    /// consumer passing values straight from configuration calls this once rather than reimplementing
    /// the clamps, so no two members interpret the same key differently.
    /// </summary>
    public ClusterOptions Validate()
    {
        int retryTtlDays = Math.Max(1, RetryTtlDays);
        return this with
        {
            DrainMs = Math.Max(100, DrainMs),
            RetryTtlDays = retryTtlDays,
            RetentionDays = Math.Max(retryTtlDays + 1, RetentionDays),
            GcMs = Math.Max(1000, GcMs),
            Kind = Membership.MemberKind.IsKnown(Kind) ? Kind : Membership.MemberKind.Node,
            GossipMs = Math.Max(250, GossipMs),
            PollMs = Math.Max(250, PollMs),
            SuspectMs = Math.Max(1000, SuspectMs),
            ReapMs = Math.Max(1000, ReapMs),
        };
    }
}
