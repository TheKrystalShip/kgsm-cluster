using Microsoft.Extensions.Logging;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// The stateful shell around <see cref="RosterMerger"/>: merges an incoming roster into this member's own,
/// projects that roster into the sync wire shape, and advances the failure timers. Shared by both gossip
/// paths — the receive side of the sync endpoint and the outbound <see cref="GossipWorker"/> loop — so the
/// merge and ordering rules live in exactly one place.
/// </summary>
public sealed class GossipService(
    MembersStore members,
    SelfIncarnation selfIncarnation,
    SelfIdentityStore selfIdentity,
    IMemberCardSource cards,
    ClusterOptions options,
    ILogger<GossipService> logger)
{
    // How recently a first-hand probe must have succeeded for a member's liveness to outrank
    // equal-incarnation gossip. A few poll intervals: long enough to bridge a missed tick, short enough
    // that a truly-gone member stops being fresh quickly.
    private static readonly TimeSpan FirstHandFreshWindow = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Merge an incoming roster into this member's own, one member at a time: insert new hearsay, adopt
    /// superseding state, refute reports about ourselves. Never writes the first-hand liveness triple.
    /// </summary>
    public async Task MergeIncomingAsync(IReadOnlyList<SyncMember> incoming, CancellationToken ct)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        string myMemberId = options.MemberId;

        foreach (SyncMember member in incoming)
        {
            try
            {
                MemberRow? existing = await members.GetByMemberIdAsync(member.MemberId, ct).ConfigureAwait(false);
                bool fresh = existing is not null && IsFirstHandFresh(existing, now);
                MergeOutcome outcome =
                    RosterMerger.Decide(member, existing, myMemberId, selfIncarnation.Current, fresh);

                switch (outcome.Action)
                {
                    case MergeAction.RefuteSelf:
                        long raised = selfIncarnation.RaiseToRefute(member.Incarnation);
                        logger.LogInformation(
                            "refuted a stale {State}@{Incarnation} about self — bumped incarnation to {New}",
                            member.State, member.Incarnation, raised);
                        break;

                    case MergeAction.Insert:
                        await members.UpsertAsync(
                            MemberRow.New(member.MemberId, KindOrNode(member.Kind)) with
                            {
                                Url = MemberCandidates.Best(member.Candidates),
                                Candidates = MemberCandidates.Encode(member.Candidates),
                                Incarnation = member.Incarnation,
                                MembershipState = member.State,
                                StateChangedAt = now,
                                ApiVersion = member.ApiVersion ?? "",
                            }, ct).ConfigureAwait(false);
                        logger.LogInformation(
                            "learned member {MemberId} ({Kind}) via gossip (provisional)",
                            member.MemberId, KindOrNode(member.Kind));
                        break;

                    case MergeAction.Update:
                        await members.UpdateMembershipAsync(
                            existing!.Id, member.State, member.Incarnation, now, member.Candidates,
                            member.ApiVersion, ct).ConfigureAwait(false);
                        logger.LogDebug(
                            "member {MemberId} → {State}@{Incarnation} via gossip",
                            member.MemberId, member.State, member.Incarnation);
                        break;

                    case MergeAction.Ignore:
                    default:
                        break;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "gossip merge failed for member {MemberId} — skipping", member.MemberId);
            }
        }
    }

    /// <summary>
    /// This member's full roster as gossip members: its own self-entry, then every enabled peer. Disabled
    /// members are excluded — a locally-banned member is not re-injected into the mesh. The push half of
    /// push-pull.
    /// </summary>
    public async Task<IReadOnlyList<SyncMember>> BuildLocalRosterAsync(CancellationToken ct)
    {
        MemberCard card = await cards.BuildAsync(ct).ConfigureAwait(false);
        var self = new SyncMember(
            options.MemberId,
            options.Kind,
            await selfIdentity.CandidatesAsync(ct).ConfigureAwait(false),
            selfIncarnation.Current,
            GossipState.Alive,
            card.Node?.ApiVersion ?? "");

        IReadOnlyList<MemberRow> enabled = await members.ListEnabledAsync(ct).ConfigureAwait(false);
        var roster = new List<SyncMember>(enabled.Count + 1) { self };
        foreach (MemberRow row in enabled)
        {
            if (string.Equals(row.MemberId, options.MemberId, StringComparison.Ordinal))
                continue;
            roster.Add(new SyncMember(
                row.MemberId, row.Kind, MemberCandidates.Decode(row.Candidates), row.Incarnation,
                row.MembershipState, row.ApiVersion));
        }
        return roster;
    }

    /// <summary>
    /// Advance the failure timers off the last-evidence clock: an alive member with no liveness evidence
    /// for the suspect window becomes suspect; a suspect still silent past that window becomes dead; a dead
    /// or departed member past the reap window is removed.
    /// <para>
    /// <b>Evidence is mutual and arrives from either direction</b> — this member's own successful probe, or
    /// an authenticated inbound call from the other. Both stamp last-seen. So a member that cannot be
    /// probed but still talks to us stays alive, an asymmetric partition resolves in favour of the
    /// demonstrably-live member, and only a member that goes fully silent dies. Recovery out of suspect
    /// belongs to those promotion paths, not to this.
    /// </para>
    /// <para>
    /// A never-yet-confirmed member learned through gossip gets one suspect window's grace off the time it
    /// was learned, so the poller has a chance to authenticate it before it is suspected.
    /// </para>
    /// </summary>
    public async Task AdvanceFailureTimersAsync(CancellationToken ct)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        IReadOnlyList<MemberRow> rows = await members.ListAsync(ct).ConfigureAwait(false);
        var suspectWindow = TimeSpan.FromMilliseconds(options.SuspectMs);
        var reapWindow = TimeSpan.FromMilliseconds(options.ReapMs);

        foreach (MemberRow row in rows)
        {
            try
            {
                if (row.MembershipState == GossipState.Alive)
                {
                    // Last evidence, or — for a member never confirmed — when it was learned. A member with
                    // neither has nothing vouching for it and is immediately eligible for suspicion.
                    DateTimeOffset? since = row.LastSeen ?? row.StateChangedAt;
                    if (since is null || now - since.Value >= suspectWindow)
                    {
                        await members.UpdateMembershipAsync(
                            row.Id, GossipState.Suspect, row.Incarnation, now, null, null, ct)
                            .ConfigureAwait(false);
                        logger.LogInformation("member {MemberId} → suspect (no liveness evidence)", row.MemberId);
                    }
                }
                else if (row.MembershipState == GossipState.Suspect)
                {
                    if (row.StateChangedAt is { } since && now - since >= suspectWindow)
                    {
                        await members.UpdateMembershipAsync(
                            row.Id, GossipState.Dead, row.Incarnation, now, null, null, ct)
                            .ConfigureAwait(false);
                        logger.LogInformation("member {MemberId} → dead (suspect timeout)", row.MemberId);
                    }
                }
                else if (GossipState.IsTerminal(row.MembershipState))
                {
                    if (row.StateChangedAt is { } since && now - since >= reapWindow)
                    {
                        await members.DeleteAsync(row.Id, ct).ConfigureAwait(false);
                        logger.LogInformation("reaped {MemberId} ({State})", row.MemberId, row.MembershipState);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "failure-timer advance failed for member {MemberId} — skipping", row.MemberId);
            }
        }
    }

    /// <summary>
    /// Record an authenticated inbound call as first-hand liveness evidence: the caller presented a valid
    /// service token and reached us, so promote it to alive and stamp last-seen. This is the mutual half of
    /// failure detection — a member that cannot be probed but still talks to us is demonstrably alive, so
    /// it never falsely dies and an alive/suspect oscillation under an asymmetric partition cannot run
    /// away. A no-op when no row is held yet: the merge inserts it from the sync payload, and a probe
    /// promotes it out of joining once it authenticates.
    /// </summary>
    public Task RecordInboundContactAsync(string fromMemberId, CancellationToken ct) =>
        members.RecordAliveContactAsync(fromMemberId, DateTimeOffset.UtcNow, ct);

    /// <summary>
    /// The freshness predicate the merge consults, exposed so there is one definition of it. A member is
    /// fresh first-hand when there was liveness evidence for it recently — this member's probe succeeded,
    /// or it authenticated an inbound call — so equal-incarnation hearsay cannot override what was just
    /// confirmed directly.
    /// </summary>
    public bool IsFirstHandFresh(MemberRow row, DateTimeOffset now) =>
        row.LastSeen is { } seen && now - seen <= FirstHandFreshWindow;

    // A member from a build that predates the kind field says nothing about what it is. Node is the honest
    // reading: it is what every member was before anchors existed, and the protocol version refuses a build
    // old enough for the question to be live.
    private static string KindOrNode(string? kind) => MemberKind.IsKnown(kind) ? kind! : MemberKind.Node;
}
