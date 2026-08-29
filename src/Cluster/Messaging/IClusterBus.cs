using System.Text.Json.Serialization.Metadata;

namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// The bus's send seam — the one method a caller depends on to durably announce something to other
/// members.
/// </summary>
public interface IClusterBus
{
    /// <summary>
    /// Enqueue one message, addressed to every target, for durable at-least-once delivery. Mints a
    /// single message id shared by every target's row — this is one broadcast, and the receiving inboxes
    /// all dedupe on the same id — and writes one row per target, each delivered and retried on its own
    /// schedule. Returns as soon as the rows are committed; delivery itself is the drainer's job. An
    /// empty target set is a no-op.
    /// </summary>
    /// <param name="type">The message type, which must match a registered
    /// <see cref="IClusterMessageHandler.Type"/> on a target for it to apply there. A target that knows
    /// no such type drops the message safely.</param>
    /// <param name="payload">The type-specific payload, serialized once and stored identically on every
    /// target's row.</param>
    /// <param name="payloadTypeInfo">
    /// The caller's own source-generated metadata for <typeparamref name="T"/>. The payload's shape
    /// belongs to the caller, so the caller brings the metadata to serialize it: this package never
    /// reflects over a payload type, which is what lets a Native-AOT member send one.
    /// </param>
    /// <remarks>
    /// <b>Read before adding a caller.</b> A durable announcement is at its strongest when its row is
    /// written in the same transaction as the local effect it announces, so a rolled-back local action
    /// leaves no row promising another member something that never happened. This overload does not do
    /// that: it is a standalone gated write, run after the caller's own effect has committed. The gap is
    /// a narrow crash window in which the local effect lands and the fan-out does not — the local action
    /// is never lost, only its announcement. Closing it means an overload that accepts the caller's own
    /// transaction; add it when a caller needs that guarantee rather than ahead of one.
    /// </remarks>
    Task EnqueueAsync<T>(
        string type, T payload, JsonTypeInfo<T> payloadTypeInfo,
        IEnumerable<ClusterTarget> targets, CancellationToken ct);

    /// <summary>
    /// Enqueue an already-serialized payload. The same contract as the typed overload, for a caller
    /// that holds the payload as JSON text and has nothing to serialize.
    /// </summary>
    Task EnqueueJsonAsync(
        string type, string payloadJson, IEnumerable<ClusterTarget> targets, CancellationToken ct);
}
