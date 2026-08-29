using System.Text.Json;
using System.Text.Json.Serialization;

namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// The wire envelope every <c>POST /api/v1/members/inbox</c> body is: camelCase, with
/// <see cref="Payload"/> left as a raw <see cref="JsonElement"/> so each message type's handler
/// deserializes its own payload shape. The discriminated-union tag is <see cref="Type"/>; there is no
/// shared payload base type.
/// </summary>
/// <param name="Id">
/// A UUID minted once by the sender and stable across every retry. It is the dedupe key, and with it
/// the replay defense: a captured-and-replayed envelope is a duplicate id, acknowledged without being
/// applied again.
/// </param>
/// <param name="Type">The tag the inbox dispatches on, matched against a registered
/// <see cref="IClusterMessageHandler.Type"/>.</param>
/// <param name="From">
/// The sending member's id. It must equal the authenticated service token's <c>iss</c>; a mismatch is
/// rejected with <c>403 from_mismatch</c> before the envelope reaches the inbox, so a member cannot
/// send as another member.
/// </param>
/// <param name="Ts">Informational, for diagnostics and ordering hints. Correctness never depends on it.</param>
/// <param name="Payload">The type-specific, camelCase payload, left raw so a handler owns its own
/// payload's shape end to end.</param>
public sealed record ClusterEnvelope(
    string Id,
    string Type,
    string From,
    DateTimeOffset Ts,
    [property: JsonPropertyName("payload")] JsonElement Payload);
