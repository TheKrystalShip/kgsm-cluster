namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// One row of the outbox — a single (message, target) delivery, retried on its own schedule. A
/// broadcast to N members is N rows sharing a <see cref="MessageId"/>.
/// </summary>
/// <param name="Id">The row key, <c>&lt;messageId&gt;:&lt;targetId&gt;</c> — unique per delivery.</param>
/// <param name="MessageId">The envelope id, identical across every target of one broadcast.</param>
/// <param name="TargetId">The recipient member's id.</param>
/// <param name="TargetUrl">The recipient's base URL, as it was when the row was written.</param>
/// <param name="Type">The message type.</param>
/// <param name="Payload">The serialized payload.</param>
/// <param name="Status">One of <c>pending</c>, <c>delivered</c>, <c>dead</c>.</param>
/// <param name="Attempts">Incremented per failed send; the backoff is computed from it.</param>
/// <param name="NextAttemptAt">The drainer skips a row whose next attempt is still in the future.</param>
/// <param name="CreatedAt">When the row was enqueued — what the retry TTL is anchored on.</param>
/// <param name="DeliveredAt">Set when a target acknowledged with a <c>2xx</c>.</param>
/// <param name="LastError">The last transient failure or the dead-letter reason, for diagnostics.</param>
public sealed record OutboxRow(
    string Id,
    string MessageId,
    string TargetId,
    string TargetUrl,
    string Type,
    string Payload,
    string Status,
    int Attempts,
    DateTimeOffset NextAttemptAt,
    DateTimeOffset CreatedAt,
    DateTimeOffset? DeliveredAt,
    string? LastError);

/// <summary>The status values an <see cref="OutboxRow"/> takes.</summary>
public static class OutboxStatus
{
    /// <summary>Still to be delivered — the only status the drainer picks up.</summary>
    public const string Pending = "pending";

    /// <summary>A target acknowledged it.</summary>
    public const string Delivered = "delivered";

    /// <summary>Never to be retried: a target rejected it permanently, its stored payload is
    /// unparseable, or it outlived the retry TTL. Always paired with a loud log — a dead row is an
    /// operational signal, not a silent drop.</summary>
    public const string Dead = "dead";
}
