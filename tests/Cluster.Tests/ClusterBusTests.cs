using System.Text.Json;
using TheKrystalShip.KGSM.Cluster.Messaging;

namespace TheKrystalShip.KGSM.Cluster.Tests;

public class ClusterBusTests
{
    private static readonly ClusterTarget[] TwoTargets =
    [
        new("member-b", "http://b:8080"),
        new("member-c", "http://c:8080/"),
    ];

    [Fact]
    public async Task ABroadcastWritesOneRowPerTargetSharingOneMessageId()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("session.revoke", """{"scope":"all"}""", TwoTargets, default);

        IReadOnlyList<OutboxRow> due = await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default);
        Assert.Equal(2, due.Count);
        Assert.Single(due.Select(r => r.MessageId).Distinct());
        Assert.Equal(["member-b", "member-c"], due.Select(r => r.TargetId).Order());
        Assert.All(due, r => Assert.Equal(OutboxStatus.Pending, r.Status));
    }

    [Fact]
    public async Task AnEmptyTargetSetEnqueuesNothing()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("session.revoke", "{}", [], default);
        Assert.Empty(await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default));
    }

    [Fact]
    public async Task TheTypedOverloadSerializesThroughTheCallersOwnMetadata()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueAsync(
            "test.payload", new TestPayload("all", 7), TestPayloadContext.Default.TestPayload, TwoTargets, default);

        OutboxRow row = (await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default))[0];
        using JsonDocument stored = JsonDocument.Parse(row.Payload);
        // camelCase on the wire, whatever the property is called in C#.
        Assert.Equal("all", stored.RootElement.GetProperty("scope").GetString());
        Assert.Equal(7, stored.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task ARowIsNotDueUntilItsNextAttemptHasPassed()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("t", "{}", [TwoTargets[0]], default);
        OutboxRow row = (await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default))[0];

        await cluster.Bus.MarkTransientFailureAsync(
            row.Id, attempts: 1, DateTimeOffset.UtcNow.AddMinutes(5), "refused", default);

        Assert.Empty(await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default));
        IReadOnlyList<OutboxRow> later = await cluster.Bus.ListDueAsync(
            DateTimeOffset.UtcNow.AddMinutes(6), 100, default);
        Assert.Equal(1, later[0].Attempts);
        Assert.Equal("refused", later[0].LastError);
        // A transient failure never changes the status: the row is still owed.
        Assert.Equal(OutboxStatus.Pending, later[0].Status);
    }

    [Fact]
    public async Task ADeliveredRowIsNeverPickedUpAgain()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("t", "{}", [TwoTargets[0]], default);
        OutboxRow row = (await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default))[0];

        await cluster.Bus.MarkDeliveredAsync(row.Id, DateTimeOffset.UtcNow, default);
        Assert.Empty(await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow.AddDays(1), 100, default));
    }

    [Fact]
    public async Task ADeadRowIsNeverPickedUpAgain()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("t", "{}", [TwoTargets[0]], default);
        OutboxRow row = (await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default))[0];

        await cluster.Bus.MarkDeadAsync(row.Id, "peer rejected: 403", default);
        Assert.Empty(await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow.AddDays(1), 100, default));
    }

    [Fact]
    public async Task MarkingARowThatIsGoneIsSilent()
    {
        // The GC can prune a row out from under a drain pass; that must never throw.
        using var cluster = new TestCluster();
        await cluster.Bus.MarkDeliveredAsync("no-such-row", DateTimeOffset.UtcNow, default);
        await cluster.Bus.MarkDeadAsync("no-such-row", "gone", default);
    }

    [Fact]
    public async Task TheDueScanIsCapped()
    {
        using var cluster = new TestCluster();
        for (int i = 0; i < 5; i++)
            await cluster.Bus.EnqueueJsonAsync("t", "{}", [new ClusterTarget($"m{i}", "http://x")], default);

        Assert.Equal(3, (await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 3, default)).Count);
    }

    [Fact]
    public async Task ThePruneKeepsPendingRowsAndDropsSettledOnes()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("t", "{}", TwoTargets, default);
        IReadOnlyList<OutboxRow> rows = await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default);
        await cluster.Bus.MarkDeliveredAsync(rows[0].Id, DateTimeOffset.UtcNow, default);

        // A cutoff in the future settles everything eligible; the still-pending row is not eligible.
        int deleted = await cluster.Bus.PruneAsync(DateTimeOffset.UtcNow.AddDays(1), default);

        Assert.Equal(1, deleted);
        Assert.Single(await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default));
    }

    [Fact]
    public async Task ThePruneLeavesRowsInsideTheRetentionWindow()
    {
        using var cluster = new TestCluster();
        await cluster.Bus.EnqueueJsonAsync("t", "{}", [TwoTargets[0]], default);
        OutboxRow row = (await cluster.Bus.ListDueAsync(DateTimeOffset.UtcNow, 100, default))[0];
        await cluster.Bus.MarkDeliveredAsync(row.Id, DateTimeOffset.UtcNow, default);

        Assert.Equal(0, await cluster.Bus.PruneAsync(DateTimeOffset.UtcNow.AddDays(-30), default));
    }
}
