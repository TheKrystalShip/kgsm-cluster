using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// Prunes delivered and dead outbox rows and the inbox ledger past the retention window. Inert — no
/// timer — when the member is not clustered, then a startup catch-up pass and a periodic loop whose
/// per-tick failures are swallowed, the same posture as the drainer.
/// </summary>
/// <remarks>
/// The inert-when-unclustered choice is for consistency with the drainer rather than safety: an
/// unclustered member would simply find nothing to prune. An opt-in feature nobody configured should
/// not spin a timer that will always find nothing.
/// </remarks>
public sealed class ClusterBusGcWorker(
    ClusterBus bus,
    ClusterOptions options,
    ILogger<ClusterBusGcWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Enabled)
        {
            logger.LogInformation("cluster bus GC inert — this member is not clustered");
            return;
        }

        logger.LogInformation(
            "cluster bus GC: started (interval={IntervalMs}ms, retention={RetentionDays}d)",
            options.GcMs, options.RetentionDays);

        await RunGcAsync(stoppingToken).ConfigureAwait(false);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.GcMs));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await RunGcAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "cluster bus GC: tick failed");
                }
            }
        }
        catch (OperationCanceledException) { /* the host is stopping */ }
    }

    private Task RunGcAsync(CancellationToken ct)
        => bus.PruneAsync(DateTimeOffset.UtcNow.AddDays(-options.RetentionDays), ct);
}
