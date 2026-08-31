using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>Where a member stands with respect to one capability.</summary>
public enum CapabilityStanding
{
    /// <summary>This member is not in a cluster, so there is no assignment to read and nothing to
    /// stand down from. A standalone install is in this standing and always has been.</summary>
    NotClustered,

    /// <summary>The assignment names this member.</summary>
    Holder,

    /// <summary>The assignment names somebody else, or nobody yet. Either way this member does not
    /// serve the capability.</summary>
    StandingBy,
}

/// <summary>Where a member stands, and who holds the capability if anybody does.</summary>
/// <param name="Holder">The member id the assignment names, or <see langword="null"/> when it names
/// nobody. Always <see langword="null"/> when not clustered.</param>
public readonly record struct CapabilityHolding(CapabilityStanding Standing, string? Holder)
{
    /// <summary>Whether this member serves the capability right now.</summary>
    public bool IsHolder => Standing == CapabilityStanding.Holder;
}

/// <summary>
/// Keeps a member's standing on one capability current: claims it when nobody holds it, re-reads who
/// does, and reports every change of standing to the component that serves it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Claiming is not holding.</b> The claim is compare-and-set against what this member currently
/// knows, so two members that both see no holder both succeed locally; the tie resolves
/// deterministically when their gossip meets and one copy is overwritten. The claim is therefore
/// always followed by a re-read, and a member that finds itself not the holder stands down — which is
/// what keeps a second install a candidate rather than a second authority.
/// </para>
/// <para>
/// <b>Nothing here promotes anything.</b> The claim only ever writes into an empty assignment; a
/// capability somebody already holds is left alone however unreachable that member is. Failover is an
/// admin reassigning, because a member that promoted itself during a partition would produce two of
/// them answering for one thing.
/// </para>
/// <para>
/// <b>A failed read changes nothing.</b> "I could not find out" is not "somebody else holds it", and
/// treating it as the second would make a locked database file look like a reassignment.
/// </para>
/// </remarks>
public abstract class ClusterCapabilityWorker(
    string capability,
    ClusterOptions cluster,
    ClusterStateStore state,
    ILogger logger) : BackgroundService
{
    private CapabilityHolding _standing = new(CapabilityStanding.StandingBy, null);

    /// <summary>The capability this worker claims and follows.</summary>
    protected string Capability { get; } = capability;

    /// <summary>Where this member currently stands. Read by anything that has to refuse while standing by.</summary>
    public CapabilityHolding Standing => _standing;

    /// <summary>
    /// How often the assignment is re-read. Matched to the gossip cadence, because that is what the
    /// answer can change from — reading faster than the thing that moves it buys nothing.
    /// </summary>
    protected TimeSpan Interval =>
        TimeSpan.FromMilliseconds(cluster.GossipMs > 0 ? cluster.GossipMs : 5000);

    /// <summary>
    /// Called once before the loop starts, for whatever a member states about itself regardless of
    /// where it stands. A candidate's facts are published too: a reader resolves the holder first and
    /// takes a fact off that member only, so stating one early is what lets a promotion need no
    /// restart anywhere.
    /// </summary>
    protected virtual void OnStarting() { }

    /// <summary>
    /// Called on every pass, with <paramref name="changed"/> true only when the standing moved.
    /// </summary>
    /// <remarks>
    /// Every pass rather than only on a change, because a side effect outside this member's own state
    /// — a file, a published fact — can be undone by something else and has to be reconciled rather
    /// than written once.
    /// </remarks>
    protected abstract Task OnStandingAsync(CapabilityHolding holding, bool changed, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        OnStarting();

        if (!cluster.Enabled)
        {
            // Not a misconfiguration: a standalone install serves what it serves and has no assignment
            // to read. Said once, because "this machine is not in a cluster" is the thing an operator
            // looking at an unexpected refusal needs to be able to find.
            logger.LogInformation(
                "not part of a cluster — this member serves '{Capability}' for this machine alone", Capability);

            _standing = new CapabilityHolding(CapabilityStanding.NotClustered, null);
            await OnStandingAsync(_standing, changed: true, stoppingToken).ConfigureAwait(false);
            return;
        }

        using var timer = new PeriodicTimer(Interval);
        try
        {
            do
            {
                await EvaluateAsync(stoppingToken).ConfigureAwait(false);
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException)
        {
            // The host is stopping. Not a failure.
        }
    }

    private async Task EvaluateAsync(CancellationToken ct)
    {
        CapabilityHolding holding;
        try
        {
            ClusterAssignment? assignment = await state.GetAsync(Capability, ct).ConfigureAwait(false);

            // Bootstrap: the first member in a cluster with no assignment takes it. Only ever into an
            // empty value, and the re-read below is what settles a race between two of them.
            if (assignment is null || !assignment.IsHeld)
            {
                if (await state.TryClaimAsync(Capability, cluster.MemberId, ct).ConfigureAwait(false))
                {
                    logger.LogInformation(
                        "no member held '{Capability}' — claimed it as {Member}", Capability, cluster.MemberId);
                }
            }

            string? holder = await state.HolderAsync(Capability, ct).ConfigureAwait(false);
            bool isHolder = string.Equals(holder, cluster.MemberId, StringComparison.Ordinal);

            holding = new CapabilityHolding(
                isHolder ? CapabilityStanding.Holder : CapabilityStanding.StandingBy, holder);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "could not read who holds '{Capability}'", Capability);
            return;
        }

        bool changed = holding != _standing;
        _standing = holding;

        if (changed)
            Report(holding);

        await OnStandingAsync(holding, changed, ct).ConfigureAwait(false);
    }

    private void Report(CapabilityHolding holding)
    {
        if (holding.IsHolder)
        {
            logger.LogInformation("this member holds '{Capability}'", Capability);
        }
        else if (holding.Holder is not null)
        {
            // The safeguard: a member that believes it serves a capability the cluster assigns to
            // another stands down and says so, rather than serving on the strength of its own opinion.
            logger.LogWarning(
                "standing by — {Holder} holds '{Capability}', so this member does not serve it",
                holding.Holder, Capability);
        }
        else
        {
            logger.LogWarning(
                "standing by — no member holds '{Capability}' yet, or none has been heard from", Capability);
        }
    }
}
