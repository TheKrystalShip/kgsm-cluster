using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Messaging;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>Registers everything a member needs to take part in a cluster.</summary>
public static class ClusterServiceCollectionExtensions
{
    /// <summary>
    /// Register the cluster halves. Everything is registered unconditionally and everything is inert
    /// when the member holds no cluster secret: the token service mints nothing and validates nothing,
    /// so the inbox endpoint rejects every call before it reaches a handler, and neither background
    /// worker starts a timer. A member therefore wires this in once and joins a cluster later by
    /// gaining a secret, with no second code path for the unclustered case.
    /// </summary>
    /// <param name="services">The member's service collection.</param>
    /// <param name="options">This member's cluster configuration, projected from whatever the member
    /// already reads. Every floor is applied here, so a caller passes raw values.</param>
    public static IServiceCollection AddKgsmCluster(this IServiceCollection services, ClusterOptions options)
    {
        services.AddSingleton(options.Validate());

        services.AddSingleton<ClusterStore>();
        services.AddHostedService(sp => sp.GetRequiredService<ClusterStore>());

        services.AddSingleton<IClusterTokenService, ClusterTokenService>();
        // A member without a roster accepts anything holding the secret. One that has a roster
        // registers its own gate over this, keyed by member id.
        services.TryAddSingleton<IClusterMemberGate, AllowAllClusterMemberGate>();

        services.AddSingleton<ClusterInbox>();

        // The drainer's client carries a short timeout on purpose: a hung member must not hold a drain
        // pass open, because the row simply retries on the next tick.
        services.AddHttpClient(OutboxDrainer.HttpClientName, c => c.Timeout = TimeSpan.FromSeconds(10));
        services.AddSingleton<ClusterBus>();
        services.AddSingleton<IClusterBus>(sp => sp.GetRequiredService<ClusterBus>());
        services.AddSingleton<OutboxDrainer>();
        services.AddHostedService(sp => sp.GetRequiredService<OutboxDrainer>());
        services.AddHostedService<ClusterBusGcWorker>();

        return services;
    }
}
