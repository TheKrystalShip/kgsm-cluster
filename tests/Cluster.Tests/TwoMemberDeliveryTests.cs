using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Messaging;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// The package's self-validation: two members, real HTTP, the cases the durable bus exists for.
/// </summary>
public class TwoMemberDeliveryTests
{
    private const string Secret = "two-member-secret";

    private static async Task<bool> EventuallyAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        DateTime deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(25);
        }
        return condition();
    }

    [Fact]
    public async Task AMessageEnqueuedForAMemberThatIsUpIsDeliveredAndAppliedOnce()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);

        await a.Resolve<IClusterBus>().EnqueueJsonAsync(
            "test.message", """{"note":"hello"}""", [new ClusterTarget("member-b", b.Url)], default);

        Assert.True(await EventuallyAsync(() => b.Handler.Received.Count == 1));
        Assert.Equal("member-a", b.Handler.Received[0].From);
        Assert.Equal("hello", b.Handler.Received[0].Payload.GetProperty("note").GetString());

        // And the sender stops: the row is settled, not still owed.
        Assert.True(await EventuallyAsync(() =>
            a.Resolve<ClusterBus>().ListDueAsync(DateTimeOffset.UtcNow.AddDays(1), 100, default)
                .GetAwaiter().GetResult().Count == 0));
    }

    [Fact]
    public async Task AMessageForAMemberThatIsDownIsHeldAndDeliveredWhenItReturns()
    {
        // The reason the outbox exists. The target is down when the message is issued, and the
        // message must survive that and land when it comes back.
        string bDbPath = Path.Combine(Path.GetTempPath(), $"kgsm-cluster-host-{Guid.NewGuid():N}.db");
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);

        string bUrl;
        await using (MemberHost bFirst = await MemberHost.StartAsync("member-b", Secret, dbPath: bDbPath))
        {
            bUrl = bFirst.Url;
        }

        await a.Resolve<IClusterBus>().EnqueueJsonAsync(
            "test.message", """{"note":"while you were out"}""", [new ClusterTarget("member-b", bUrl)], default);

        // It stays owed while the target is unreachable rather than being lost or erroring the caller.
        await Task.Delay(300);
        IReadOnlyList<OutboxRow> due = await a.Resolve<ClusterBus>()
            .ListDueAsync(DateTimeOffset.UtcNow.AddMinutes(10), 100, default);
        Assert.Single(due);
        Assert.Equal(OutboxStatus.Pending, due[0].Status);
        Assert.True(due[0].Attempts > 0);

        // The port is what makes this the same member returning rather than a different one.
        await using MemberHost bAgain = await MemberHost.StartAsync(
            "member-b", Secret, dbPath: bDbPath, url: bUrl);

        Assert.True(await EventuallyAsync(() => bAgain.Handler.Received.Count == 1, timeoutMs: 15000));
    }

    [Fact]
    public async Task AReplayedEnvelopeIsAcknowledgedWithoutBeingAppliedTwice()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);

        string body = Envelope("env-replay", "test.message", "member-a");
        using HttpClient http = new();

        for (int i = 0; i < 3; i++)
        {
            using HttpResponseMessage response = await PostAsync(http, b.Url, body, a.Resolve<IClusterTokenService>());
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        Assert.Single(b.Handler.Received);
    }

    [Fact]
    public async Task AnUnknownTypeIsAcknowledgedSoTheSendersQueueKeepsMoving()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, Envelope("env-unknown", "type.member.b.has.never.heard.of", "member-a"),
            a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Empty(b.Handler.Received);
    }

    [Fact]
    public async Task NoTokenIsRejected()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        using HttpClient http = new();

        using var content = new StringContent(
            Envelope("e", "test.message", "member-a"), Encoding.UTF8, "application/json");
        using HttpResponseMessage response = await http.PostAsync($"{b.Url}{ClusterRoutes.Inbox}", content);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Empty(b.Handler.Received);
    }

    [Fact]
    public async Task ATokenSignedWithTheWrongSecretIsRejected()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost stranger = await MemberHost.StartAsync("member-x", "a-different-cluster");
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, Envelope("e", "test.message", "member-x"), stranger.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Empty(b.Handler.Received);
    }

    [Fact]
    public async Task ADisabledMemberIsRejected()
    {
        await using MemberHost b = await MemberHost.StartAsync(
            "member-b", Secret, gate: new DenyOneGate("member-a"));
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, Envelope("e", "test.message", "member-a"), a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Empty(b.Handler.Received);
    }

    [Fact]
    public async Task SendingAsAnotherMemberIsRejected()
    {
        // The token proves who is calling; claiming a different `from` is a spoof, not a mistake.
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, Envelope("e", "test.message", "member-c"), a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Empty(b.Handler.Received);
    }

    [Fact]
    public async Task AnOversizedEnvelopeIsRejectedWithoutBeingParsed()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        string huge = Envelope("e", "test.message", "member-a", note: new string('x', 100 * 1024));
        using HttpResponseMessage response = await PostAsync(http, b.Url, huge, a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Empty(b.Handler.Received);
    }

    [Fact]
    public async Task AMalformedBodyIsARequestErrorRatherThanAServerError()
    {
        // A 500 is the only answer that keeps the message in the sender's outbox, so a body that can
        // never be parsed must not be one.
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, "{ not json", a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task AnEnvelopeMissingItsIdIsRejected()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, """{"type":"test.message","from":"member-a","ts":"2026-01-01T00:00:00Z","payload":{}}""",
            a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ATransientHandlerFailureKeepsTheMessageInTheSendersOutbox()
    {
        await using MemberHost b = await MemberHost.StartAsync("member-b", Secret, handlerThrows: true);
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);
        using HttpClient http = new();

        using HttpResponseMessage response = await PostAsync(
            http, b.Url, Envelope("e", "test.message", "member-a"), a.Resolve<IClusterTokenService>());

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
    }

    [Fact]
    public async Task APermanentRejectionDeadLettersRatherThanRetryingForever()
    {
        await using MemberHost b = await MemberHost.StartAsync(
            "member-b", Secret, gate: new DenyOneGate("member-a"));
        await using MemberHost a = await MemberHost.StartAsync("member-a", Secret);

        await a.Resolve<IClusterBus>().EnqueueJsonAsync(
            "test.message", "{}", [new ClusterTarget("member-b", b.Url)], default);

        Assert.True(await EventuallyAsync(() =>
            a.Resolve<ClusterBus>().ListDueAsync(DateTimeOffset.UtcNow.AddDays(1), 100, default)
                .GetAwaiter().GetResult().Count == 0));
    }

    private static string Envelope(string id, string type, string from, string note = "hello")
        => JsonSerializer.Serialize(
            new ClusterEnvelope(id, type, from, DateTimeOffset.UtcNow,
                JsonSerializer.Deserialize($$"""{"note":{{JsonSerializer.Serialize(note)}}}""",
                    ClusterJsonContext.Default.JsonElement)),
            ClusterJsonContext.Default.ClusterEnvelope);

    private static async Task<HttpResponseMessage> PostAsync(
        HttpClient http, string baseUrl, string body, IClusterTokenService tokens)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}{ClusterRoutes.Inbox}")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokens.Mint().Token);
        return await http.SendAsync(request);
    }
}
