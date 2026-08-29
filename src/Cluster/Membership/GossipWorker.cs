using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// The masterless anti-entropy gossip loop: each interval, advance the failure timers, then pick one random
/// enabled, non-terminal member and run a push-pull roster sync with it. Constant work per member per
/// round, and the roster converges in a number of rounds that grows only logarithmically with the cluster.
/// </summary>
/// <remarks>
/// <b>The ephemeral transport.</b> A sync is a plain best-effort round trip — never an outbox row, never
/// retried to a corpse. The durable bus is for messages that must not be lost; this is for a roster that
/// re-converges every few seconds anyway. Inert when the member is not clustered, every round is isolated
/// in its own try/catch so a dead or slow partner degrades to nothing, and outbound auth is a freshly
/// minted service token like every other member-to-member call.
/// </remarks>
public sealed class GossipWorker(
    IHttpClientFactory httpClientFactory,
    GossipService gossip,
    MembersStore members,
    IClusterTokenService tokens,
    ClusterOptions options,
    ILogger<GossipWorker> logger) : BackgroundService
{
    /// <summary>The named client the push-pull sync uses — a short per-request timeout so one hung partner
    /// never stalls a round.</summary>
    public const string HttpClientName = "kgsm-cluster-gossip";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Enabled)
        {
            logger.LogInformation("gossip worker inert — this member is not clustered");
            return;
        }

        logger.LogInformation("gossip worker: started (interval={IntervalMs}ms)", options.GossipMs);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.GossipMs));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await RunRoundAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "gossip worker: round failed");
                }
            }
        }
        catch (OperationCanceledException) { /* the host is stopping */ }
    }

    /// <summary>One round: advance the failure timers, which happens whether or not there is a partner this
    /// round, then push-pull with one random enabled, non-terminal member. Public so a test drives a round
    /// deterministically instead of waiting on the timer.</summary>
    public async Task RunRoundAsync(CancellationToken ct)
    {
        await gossip.AdvanceFailureTimersAsync(ct).ConfigureAwait(false);

        IReadOnlyList<MemberRow> enabled = await members.ListEnabledAsync(ct).ConfigureAwait(false);
        // A member with no address is not a partner: there is nowhere to send a round. It can be here
        // legitimately — gossip carries a member whose own address nobody has established yet — and the
        // failure timers deal with it if it stays that way. Skipping it is what keeps one addressless row
        // from costing every round, failure timers included.
        List<MemberRow> candidates =
        [
            .. enabled.Where(m => !GossipState.IsTerminal(m.MembershipState) && !string.IsNullOrWhiteSpace(m.Url))
        ];
        if (candidates.Count == 0)
            return;

        MemberRow partner = candidates[Random.Shared.Next(candidates.Count)];

        MintedClusterToken token;
        try
        {
            token = tokens.Mint();
        }
        catch (InvalidOperationException)
        {
            // Unreachable in practice, since an unclustered member returns before the loop starts. Failing
            // closed here keeps a mint failure from escaping as something worse.
            return;
        }

        IReadOnlyList<SyncMember> roster = await gossip.BuildLocalRosterAsync(ct).ConfigureAwait(false);
        HttpClient http = httpClientFactory.CreateClient(HttpClientName);
        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(
                HttpMethod.Post, $"{partner.Url.TrimEnd('/')}{ClusterRoutes.Sync}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
            request.Content = JsonContent.Create(
                new SyncRequest(options.MemberId, roster), ClusterJsonContext.Default.SyncRequest);

            response = await http.SendAsync(request, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                logger.LogDebug(
                    "gossip sync with {MemberId} failed: HTTP {Status}",
                    partner.MemberId, (int)response.StatusCode);
                return;
            }

            await using Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            SyncResponse? sync = await JsonSerializer
                .DeserializeAsync(body, ClusterJsonContext.Default.SyncResponse, ct)
                .ConfigureAwait(false);
            if (sync?.Members is not null)
                await gossip.MergeIncomingAsync(sync.Members, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // a real shutdown, not a sync failure
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException or OperationCanceledException)
        {
            // Connect refused, DNS failure, the client's own timeout, a malformed body — all collapse to
            // the one honest "no sync this round". No retry, no durable record.
            logger.LogDebug(ex, "gossip sync with {MemberId} failed", partner.MemberId);
        }
        finally
        {
            response?.Dispose();
        }
    }
}
