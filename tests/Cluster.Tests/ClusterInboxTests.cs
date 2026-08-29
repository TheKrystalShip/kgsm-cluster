using System.Text.Json;
using TheKrystalShip.KGSM.Cluster.Messaging;

namespace TheKrystalShip.KGSM.Cluster.Tests;

public class ClusterInboxTests
{
    private static ClusterEnvelope Envelope(string id = "env-1", string type = "session.revoke")
        => new(id, type, "member-b", DateTimeOffset.UtcNow,
            JsonSerializer.Deserialize("""{"scope":"all"}""", ClusterJsonContext.Default.JsonElement));

    [Fact]
    public async Task AKnownTypeIsDispatchedOnce()
    {
        var handler = new RecordingHandler("session.revoke");
        using var cluster = new TestCluster(handlers: [handler]);

        Assert.Equal(InboxResult.Applied, await cluster.Inbox.ReceiveAsync(Envelope(), default));
        Assert.Single(handler.Received);
    }

    [Fact]
    public async Task ARedeliveredEnvelopeIsAcknowledgedWithoutRunningTheHandlerAgain()
    {
        var handler = new RecordingHandler("session.revoke");
        using var cluster = new TestCluster(handlers: [handler]);

        await cluster.Inbox.ReceiveAsync(Envelope(), default);
        Assert.Equal(InboxResult.Duplicate, await cluster.Inbox.ReceiveAsync(Envelope(), default));
        Assert.Single(handler.Received);
    }

    [Fact]
    public async Task TwoDistinctEnvelopesBothApply()
    {
        var handler = new RecordingHandler("session.revoke");
        using var cluster = new TestCluster(handlers: [handler]);

        await cluster.Inbox.ReceiveAsync(Envelope("env-1"), default);
        await cluster.Inbox.ReceiveAsync(Envelope("env-2"), default);
        Assert.Equal(2, handler.Received.Count);
    }

    [Fact]
    public async Task AnUnknownTypeIsDroppedRatherThanRetriedForever()
    {
        // A newer member may legitimately know a type this one does not. Dropping is what keeps the
        // sender's queue moving instead of wedging it behind a message that can never apply here.
        using var cluster = new TestCluster(handlers: [new RecordingHandler("session.revoke")]);
        Assert.Equal(
            InboxResult.DroppedUnknown,
            await cluster.Inbox.ReceiveAsync(Envelope(type: "something.new"), default));
    }

    [Fact]
    public async Task AnUnknownTypeIsRecordedSoARetransmitIsNotReEvaluated()
    {
        using var cluster = new TestCluster(handlers: []);
        await cluster.Inbox.ReceiveAsync(Envelope(type: "something.new"), default);
        Assert.Equal(
            InboxResult.Duplicate,
            await cluster.Inbox.ReceiveAsync(Envelope(type: "something.new"), default));
    }

    [Fact]
    public async Task AThrowingHandlerRecordsNothingSoTheRedeliveryRunsItAgain()
    {
        var handler = new RecordingHandler("session.revoke", throws: true);
        using var cluster = new TestCluster(handlers: [handler]);

        Assert.Equal(InboxResult.TransientFailure, await cluster.Inbox.ReceiveAsync(Envelope(), default));
        // Nothing was written, so this is a fresh attempt rather than a duplicate.
        Assert.Equal(InboxResult.TransientFailure, await cluster.Inbox.ReceiveAsync(Envelope(), default));
        Assert.Equal(2, handler.Received.Count);
    }

    [Fact]
    public async Task ConcurrentDeliveriesOfOneEnvelopeApplyItOnce()
    {
        var handler = new RecordingHandler("session.revoke");
        using var cluster = new TestCluster(handlers: [handler]);

        InboxResult[] results = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(_ => cluster.Inbox.ReceiveAsync(Envelope(), default)));

        Assert.Single(handler.Received);
        Assert.Single(results, r => r == InboxResult.Applied);
        Assert.Equal(7, results.Count(r => r == InboxResult.Duplicate));
    }

    [Fact]
    public async Task ThePayloadReachesTheHandlerUnchanged()
    {
        var handler = new RecordingHandler("session.revoke");
        using var cluster = new TestCluster(handlers: [handler]);

        await cluster.Inbox.ReceiveAsync(Envelope(), default);
        Assert.Equal("all", handler.Received[0].Payload.GetProperty("scope").GetString());
    }
}
