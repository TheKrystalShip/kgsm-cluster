using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// Probes every enabled member's identity endpoint on a fixed interval and records the latency and
/// first-hand status. No answer within the timeout, or a non-2xx, is an honest unreachable with a null
/// latency and an untouched last-seen — never a fabricated value. A disabled member is never probed.
/// </summary>
/// <remarks>
/// <b>Each member runs its own.</b> Membership is per member, so borrowing another member's view of who is
/// reachable would make one member's roster depend on another member's process. Inert when this member is
/// not clustered. Each tick probes every enabled member concurrently, each probe isolated in its own
/// try/catch so a dead or slow member degrades to an honest answer rather than stalling its siblings, and
/// the tick body is wrapped too, so the loop survives anything short of cancellation.
/// </remarks>
public sealed class MemberLatencyPoller(
    IHttpClientFactory httpClientFactory,
    MembersStore members,
    IClusterTokenService tokens,
    IMemberCardSource cards,
    ClusterOptions options,
    ILogger<MemberLatencyPoller> logger) : BackgroundService
{
    /// <summary>The named client this poller probes with — a short timeout, so one hung member does not
    /// stall the whole tick.</summary>
    public const string HttpClientName = "kgsm-cluster-latency";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Enabled)
        {
            logger.LogInformation("member latency poller inert — this member is not clustered");
            return;
        }

        logger.LogInformation("member latency poller: started (interval={IntervalMs}ms)", options.PollMs);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.PollMs));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await RunTickAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "member latency poller: tick failed");
                }
            }
        }
        catch (OperationCanceledException) { /* the host is stopping */ }
    }

    /// <summary>
    /// One tick: read the enabled roster, mint a single service token for it — the identity endpoint is
    /// token-authed and a token comfortably outlives one tick, so one mint covers every member — then probe
    /// them all concurrently. Public so a test drives a tick deterministically instead of waiting on the
    /// timer.
    /// </summary>
    public async Task RunTickAsync(CancellationToken ct)
    {
        IReadOnlyList<MemberRow> rows = await members.ListEnabledAsync(ct).ConfigureAwait(false);
        if (rows.Count == 0)
            return;

        MintedClusterToken token = tokens.Mint();
        MemberCard mine = await cards.BuildAsync(ct).ConfigureAwait(false);
        await Task.WhenAll(rows.Select(row => ProbeAsync(row, token, mine, ct))).ConfigureAwait(false);
    }

    /// <summary>
    /// One probe round for one member: walk its candidate addresses in order, the pinned one first, and
    /// stop at the first that answers as that member. The winner is pinned, so the next round — and every
    /// member-to-member call in between — goes straight there. Reachability is a property of a pair, so the
    /// address pinned here is this member's own answer and need not match what anybody else pinned.
    /// </summary>
    private async Task ProbeAsync(MemberRow row, MintedClusterToken token, MemberCard mine, CancellationToken ct)
    {
        try
        {
            HttpClient http = httpClientFactory.CreateClient(HttpClientName);
            string? lastFailure = null;

            foreach (string address in AddressesFor(row))
            {
                (int? latencyMs, MemberCard? identity, string? failure) =
                    await TryAddressAsync(http, address, token, ct).ConfigureAwait(false);

                if (latencyMs is null)
                {
                    lastFailure = failure;
                    continue;
                }

                if (!string.Equals(address, row.Url, StringComparison.OrdinalIgnoreCase))
                    logger.LogInformation("member {MemberId} answers at {Address} — pinning it", row.MemberId, address);

                await members.UpdateLivenessAsync(
                    row.Id, MemberStatus.Reachable, latencyMs.Value, DateTimeOffset.UtcNow, ct)
                    .ConfigureAwait(false);
                if (row.Status != MemberStatus.Reachable)
                {
                    logger.LogInformation(
                        "member {Id} ({MemberId}) is now reachable ({LatencyMs}ms)", row.Id, row.MemberId, latencyMs);
                }

                // First-hand authentication: a member just reached directly is promoted to alive only once
                // its own identity confirms it is a real cluster member under the member id this row is
                // keyed on. Reachability alone is not membership, and an address that answers as somebody
                // else is not this member's address. A body that will not parse still counts as reachable;
                // it just is not vouched for.
                bool authentic = identity is not null
                    && string.Equals(identity.MemberId, row.MemberId, StringComparison.Ordinal)
                    && identity.Clustered
                    && identity.Protocol == ClusterProtocol.Current
                    && VersionAgrees(identity, mine);

                if (authentic)
                {
                    // An address only becomes this member's address once it has answered under its member
                    // id; until then the roster carries it unverified.
                    await members.PinAddressAsync(row.Id, address, identity!.Candidates, ct).ConfigureAwait(false);

                    bool wasAlive = row.MembershipState == GossipState.Alive;
                    await members.PromoteAliveAsync(
                        row.Id, identity.Node?.ApiVersion, DateTimeOffset.UtcNow, ct).ConfigureAwait(false);
                    if (!wasAlive)
                        logger.LogInformation("member {MemberId} promoted alive (first-hand authenticated)", row.MemberId);
                }
                else if (identity is not null)
                {
                    logger.LogDebug(
                        "member {MemberId} reached at {Address} but is not a matching cluster member: " +
                        "id={Id} clustered={Clustered} protocol={Protocol} version={Version}",
                        row.MemberId, address, identity.MemberId, identity.Clustered, identity.Protocol,
                        identity.Node?.ApiVersion ?? "(anchor)");
                }

                return;
            }

            // Every candidate failed. Never a fabricated latency, and last-seen stays whatever it was: a
            // failed probe never advances "last successfully reached".
            string reason = lastFailure ?? "no address to try";
            await members.UpdateLivenessAsync(row.Id, MemberStatus.Unreachable, null, row.LastSeen, ct)
                .ConfigureAwait(false);
            if (row.Status == MemberStatus.Reachable)
                logger.LogInformation("member {Id} ({MemberId}) went unreachable: {Reason}", row.Id, row.MemberId, reason);
            else
                logger.LogDebug("member {Id} ({MemberId}) still unreachable: {Reason}", row.Id, row.MemberId, reason);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "member {Id} ({MemberId}) probe failed unexpectedly", row.Id, row.MemberId);
        }
    }

    // The route version is only meaningful between two nodes; an anchor on either end makes it no part of
    // the question.
    private static bool VersionAgrees(MemberCard theirs, MemberCard mine)
        => theirs.Node is not { } t || mine.Node is not { } m
           || string.Equals(t.ApiVersion, m.ApiVersion, StringComparison.Ordinal);

    /// <summary>The addresses to try, in order: the pinned one first — it worked last time, or it is the
    /// most-trusted thing on offer — then every other candidate advertised.</summary>
    private static IEnumerable<string> AddressesFor(MemberRow row)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(row.Url) && seen.Add(row.Url.TrimEnd('/')))
            yield return row.Url.TrimEnd('/');

        foreach (MemberCandidate candidate in MemberCandidates.Decode(row.Candidates))
        {
            string url = candidate.Url.TrimEnd('/');
            if (!string.IsNullOrWhiteSpace(url) && seen.Add(url))
                yield return url;
        }
    }

    /// <summary>One identity read against one address. Returns the measured latency and the parsed card on
    /// success, or the failure that explains why not.</summary>
    private async Task<(int? LatencyMs, MemberCard? Identity, string? Failure)> TryAddressAsync(
        HttpClient http, string address, MintedClusterToken token, CancellationToken ct)
    {
        long start = Stopwatch.GetTimestamp();
        HttpResponseMessage? response = null;
        try
        {
            // A per-request message rather than a header on the shared named client: probes for every
            // member run concurrently off one client, so a default header would race.
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{address}{ClusterRoutes.Identity}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
            response = await http.SendAsync(request, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // a real shutdown, not a probe failure
        }
        catch (Exception ex)
        {
            // Connect refused, DNS failure, the client's own timeout — all transport failures, all honestly
            // unreachable.
            return (null, null, ex.Message);
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
                return (null, null, $"HTTP {(int)response.StatusCode}");

            int latencyMs = (int)Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            try
            {
                await using Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                MemberCard? card = await JsonSerializer
                    .DeserializeAsync(body, ClusterJsonContext.Default.MemberCard, ct)
                    .ConfigureAwait(false);
                return (latencyMs, card, null);
            }
            catch (JsonException)
            {
                logger.LogDebug("identity body from {Address} did not parse — reachable, not promoted", address);
                return (latencyMs, null, null);
            }
        }
    }
}
