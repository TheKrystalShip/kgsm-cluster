namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// One message-type handler in the bus's discriminated union. A member registers a handler for each
/// type it cares about; the inbox resolves the set once and dispatches by the incoming envelope's
/// <c>type</c>. Adding a message type means adding an implementation, never touching the dispatcher —
/// and a member that registers no handler for a type another member sends drops it safely.
/// </summary>
/// <remarks>
/// <b>Handlers must be idempotent.</b> Applying the same envelope twice has to be harmless. This is the
/// second of the two idempotency layers — the first is the inbox's processed-id ledger — and it is what
/// makes a crash between "handler succeeded" and "ledger row recorded" safe: the redelivery re-runs the
/// handler and the effect lands once. <see cref="HandleAsync"/> should throw only for a genuinely
/// transient failure, such as a locked database; the inbox maps a thrown exception to a <c>500</c> so
/// the sender retries. A permanent condition, such as a malformed payload for a known type, is logged
/// and swallowed, or the sender retries forever against a message this member can never accept.
/// </remarks>
public interface IClusterMessageHandler
{
    /// <summary>The <c>type</c> tag this handler answers to, matched against <see cref="ClusterEnvelope.Type"/>.</summary>
    string Type { get; }

    /// <summary>Apply the envelope's effect locally, under the idempotency contract in the remarks.</summary>
    Task HandleAsync(ClusterEnvelope envelope, CancellationToken ct);
}
