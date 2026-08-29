namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// One outbound delivery target: a member's id plus the base URL the drainer posts
/// <c>/api/v1/members/inbox</c> onto. The caller supplies the target set — usually the roster's enabled,
/// first-hand-alive members — so the messaging half never has to know how membership is decided.
/// </summary>
/// <param name="MemberId">The recipient's cluster member id.</param>
/// <param name="Url">The recipient's base URL, with or without a trailing slash.</param>
public sealed record ClusterTarget(string MemberId, string Url);
