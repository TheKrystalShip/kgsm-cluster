using Microsoft.Extensions.Logging.Abstractions;

using TheKrystalShip.KGSM.Cluster.Membership;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// Standing on a capability: taking one nobody holds, and standing down from one somebody else does.
/// Every anchor does this identically, so it is the package's rather than each component's.
/// </summary>
public class CapabilityWorkerTests
{
    /// <summary>A worker that records what it was told rather than acting on it.</summary>
    private sealed class Recording(ClusterOptions cluster, ClusterStateStore state)
        : ClusterCapabilityWorker(ClusterCapability.Assistant, cluster, state, NullLogger.Instance)
    {
        public List<CapabilityHolding> Reported { get; } = [];
        public List<CapabilityHolding> Changes { get; } = [];
        public bool Started { get; private set; }

        protected override void OnStarting() => Started = true;

        protected override Task OnStandingAsync(CapabilityHolding holding, bool changed, CancellationToken ct)
        {
            Reported.Add(holding);
            if (changed)
                Changes.Add(holding);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Start the loop and wait for its first pass. The loop's first evaluation is immediate — the
        /// timer only paces the ones after it — so this waits on the real worker rather than reaching
        /// past it into a method the loop does not use.
        /// </summary>
        public async Task FirstPassAsync()
        {
            await StartAsync(default);

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (Reported.Count == 0)
            {
                timeout.Token.ThrowIfCancellationRequested();
                await Task.Delay(10, timeout.Token);
            }
        }
    }

    private static Recording Worker(TestCluster cluster) =>
        new(cluster.Options, new ClusterStateStore(cluster.Store, cluster.Options));

    [Fact]
    public async Task A_capability_nobody_holds_is_claimed()
    {
        using var cluster = new TestCluster(memberId: "assistant-one");
        Recording worker = Worker(cluster);

        await worker.FirstPassAsync();
        await worker.StopAsync(default);

        Assert.Contains(worker.Changes, h => h.IsHolder && h.Holder == "assistant-one");
    }

    [Fact]
    public async Task A_capability_somebody_else_holds_is_left_alone()
    {
        // The claim only ever writes into an empty assignment. A member that took one already held —
        // however unreachable its holder — would be a second answer to a question with one answer.
        using var cluster = new TestCluster(memberId: "assistant-two");
        var state = new ClusterStateStore(cluster.Store, cluster.Options);
        await state.TryClaimAsync(ClusterCapability.Assistant, "assistant-one", default);

        Recording worker = Worker(cluster);
        await worker.FirstPassAsync();
        await worker.StopAsync(default);

        Assert.Equal("assistant-one", await state.HolderAsync(ClusterCapability.Assistant, default));
        Assert.Contains(worker.Changes, h => h.Standing == CapabilityStanding.StandingBy && h.Holder == "assistant-one");
    }

    [Fact]
    public async Task A_member_that_is_not_clustered_stands_on_its_own()
    {
        // Not standing by and not holding: there is no assignment to read, which is the standing every
        // standalone install is in. Collapsing it into standing-by would make one refuse its own work.
        using var cluster = new TestCluster(secret: "");
        Recording worker = Worker(cluster);

        await worker.FirstPassAsync();
        await worker.StopAsync(default);

        Assert.True(worker.Started);
        Assert.Equal([new CapabilityHolding(CapabilityStanding.NotClustered, null)], worker.Changes);
    }
}
