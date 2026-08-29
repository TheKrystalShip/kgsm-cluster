using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Membership;
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
    /// <remarks>
    /// A node should also register its own <see cref="IMemberCardSource"/>, to put its route version, build
    /// and leaves on its card. An anchor needs none: <see cref="SelfMemberCardSource"/> already states
    /// everything an anchor has to say about itself.
    /// </remarks>
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

        // Membership. The roster is this member's own copy of who else is in the cluster; the handshake
        // joins one; gossip converges the rest; the poller is what turns hearsay into first-hand knowledge.
        // Every named client carries a short timeout, because a hung member must not stall an operator's
        // request or a poll tick.
        services.AddSingleton<MembersStore>();
        services.AddSingleton<SelfIdentityStore>();
        services.AddSingleton<SelfIncarnation>();
        services.AddSingleton<GossipService>();
        // The identity and addresses on a card are the package's own; a route version, a build and a set of
        // leaves are not. So this states the first three and nothing else — which is the complete card for
        // an anchor — and a node registers its own source over it to add the node block.
        services.TryAddSingleton<IMemberCardSource, SelfMemberCardSource>();

        services.AddHttpClient(MemberHandshakeService.HttpClientName, c => c.Timeout = TimeSpan.FromSeconds(10));
        services.AddSingleton<MemberHandshakeService>();

        // Registered as itself as well as a hosted service, the same shape the drainer takes: the loop runs
        // on its own, and a member that has just joined somebody can drive a round immediately rather than
        // waiting out an interval.
        services.AddHttpClient(GossipWorker.HttpClientName, c => c.Timeout = TimeSpan.FromSeconds(10));
        services.AddSingleton<GossipWorker>();
        services.AddHostedService(sp => sp.GetRequiredService<GossipWorker>());

        // Each member runs its own liveness loop. Membership is per member, so borrowing another member's
        // view of who is reachable would make one member's roster depend on another member's process.
        services.AddHttpClient(MemberLatencyPoller.HttpClientName, c => c.Timeout = TimeSpan.FromSeconds(10));
        services.AddSingleton<MemberLatencyPoller>();
        services.AddHostedService(sp => sp.GetRequiredService<MemberLatencyPoller>());

        return services;
    }
}
