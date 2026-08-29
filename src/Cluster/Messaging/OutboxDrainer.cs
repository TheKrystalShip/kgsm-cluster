using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// Delivers the outbox. A background service that is inert — no timer at all — when the member is not
/// clustered, then runs a startup catch-up pass and a periodic loop whose per-tick failures are
/// swallowed: one bad tick must never kill the drainer, and the next one tries again.
/// </summary>
/// <remarks>
/// <para>
/// <b>Per row.</b> The retry TTL is checked first, so a row too old to be given a fresh chance is
/// dead-lettered without being sent. A <c>2xx</c> marks it delivered. A <c>400</c>, <c>401</c>,
/// <c>403</c> or <c>413</c> is a permanent rejection and dead-letters it loudly — that combination
/// means a local misconfiguration, a wrong secret or a member that disabled us, and it is surfaced
/// rather than retried forever. Anything else — a thrown transport failure, a <c>5xx</c>, any other
/// status — is transient: the attempt count rises, the next attempt moves out by the backoff, the row
/// stays pending.
/// </para>
/// <para>
/// <b>Backoff</b> is capped exponential with jitter. The jitter is what stops every row aimed at one
/// recovered member from retrying in lockstep.
/// </para>
/// </remarks>
public sealed class OutboxDrainer : BackgroundService
{
    /// <summary>The named client this drainer posts with, registered with a short timeout: a hung
    /// member must not hold a drain pass for long, since the row simply retries next tick.</summary>
    public const string HttpClientName = "kgsm-cluster-outbox";

    /// <summary>Max due rows one pass processes, so a single tick cannot balloon. The next tick picks
    /// up whatever is still due.</summary>
    private const int DrainCap = 100;

    private static readonly TimeSpan BackoffBase = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffCap = TimeSpan.FromMinutes(5);

    private static readonly HttpStatusCode[] PermanentRejectCodes =
    [
        HttpStatusCode.BadRequest, HttpStatusCode.Unauthorized, HttpStatusCode.Forbidden,
        HttpStatusCode.RequestEntityTooLarge,
    ];

    private readonly ClusterBus _bus;
    private readonly IClusterTokenService _tokens;
    private readonly ClusterOptions _options;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<OutboxDrainer> _logger;

    public OutboxDrainer(
        ClusterBus bus,
        IClusterTokenService tokens,
        ClusterOptions options,
        IHttpClientFactory httpClientFactory,
        ILogger<OutboxDrainer> logger)
    {
        _bus = bus;
        _tokens = tokens;
        _options = options;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("outbox drainer inert — this member is not clustered");
            return;
        }

        _logger.LogInformation(
            "outbox drainer: started (interval={IntervalMs}ms, retryTtl={RetryTtlDays}d)",
            _options.DrainMs, _options.RetryTtlDays);

        // A member that was down for a while should start on its backlog immediately rather than
        // waiting out a full interval first.
        await RunDrainPassAsync(stoppingToken).ConfigureAwait(false);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_options.DrainMs));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await RunDrainPassAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "outbox drainer: tick failed");
                }
            }
        }
        catch (OperationCanceledException) { /* the host is stopping */ }
    }

    /// <summary>One drain pass: due-scan, then each row in turn. Public so a test drives a pass
    /// deterministically instead of waiting on the timer.</summary>
    public async Task RunDrainPassAsync(CancellationToken ct)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        IReadOnlyList<OutboxRow> due = await _bus.ListDueAsync(now, DrainCap, ct).ConfigureAwait(false);
        foreach (OutboxRow row in due)
        {
            ct.ThrowIfCancellationRequested();
            await ProcessRowAsync(row, ct).ConfigureAwait(false);
        }
    }

    private async Task ProcessRowAsync(OutboxRow row, CancellationToken ct)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;

        if (now - row.CreatedAt > TimeSpan.FromDays(_options.RetryTtlDays))
        {
            await _bus.MarkDeadAsync(row.Id, "retry TTL exceeded", ct).ConfigureAwait(false);
            _logger.LogError(
                "cluster outbox: {Id} (target={Target} type={Type}) exceeded the {Days}-day retry TTL " +
                "— dead-lettered without being sent",
                row.Id, row.TargetId, row.Type, _options.RetryTtlDays);
            return;
        }

        string body;
        try
        {
            var envelope = new ClusterEnvelope(
                row.MessageId, row.Type, _options.MemberId, row.CreatedAt,
                JsonSerializer.Deserialize(row.Payload, ClusterJsonContext.Default.JsonElement));
            body = JsonSerializer.Serialize(envelope, ClusterJsonContext.Default.ClusterEnvelope);
        }
        catch (JsonException ex)
        {
            // The stored payload cannot be parsed, so no number of retries will make this send. Dead-letter
            // rather than loop on a message that can never leave.
            await _bus.MarkDeadAsync(row.Id, $"malformed stored payload: {ex.Message}", ct).ConfigureAwait(false);
            _logger.LogError(ex, "cluster outbox: {Id} has an unparseable stored payload — dead-lettered", row.Id);
            return;
        }

        string url = $"{row.TargetUrl.TrimEnd('/')}{ClusterRoutes.Inbox}";
        HttpResponseMessage? response = null;
        string? transientError = null;
        try
        {
            MintedClusterToken token = _tokens.Mint();
            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json"),
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
            HttpClient http = _httpClientFactory.CreateClient(HttpClientName);
            response = await http.SendAsync(request, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // a real shutdown, not a delivery failure
        }
        catch (Exception ex)
        {
            // Connect refused, DNS failure, the client's own request timeout, or any other
            // transport-level failure. All transient.
            transientError = ex.Message;
        }

        try
        {
            if (response is not null && response.IsSuccessStatusCode)
            {
                await _bus.MarkDeliveredAsync(row.Id, DateTimeOffset.UtcNow, ct).ConfigureAwait(false);
                return;
            }

            if (response is not null && PermanentRejectCodes.Contains(response.StatusCode))
            {
                await _bus.MarkDeadAsync(row.Id, $"member rejected: {(int)response.StatusCode}", ct)
                    .ConfigureAwait(false);
                _logger.LogError(
                    "cluster outbox: {Id} (target={Target}) permanently rejected (HTTP {Status}) — dead-lettered. " +
                    "This is a local misconfiguration signal, a wrong cluster secret or a member that disabled us, " +
                    "not a lost message",
                    row.Id, row.TargetId, (int)response.StatusCode);
                return;
            }

            string error = transientError ?? $"HTTP {(int)response!.StatusCode}";
            int attempts = row.Attempts + 1;
            TimeSpan backoff = ComputeBackoff(attempts);
            await _bus.MarkTransientFailureAsync(
                row.Id, attempts, DateTimeOffset.UtcNow.Add(backoff), error, ct).ConfigureAwait(false);
        }
        finally
        {
            response?.Dispose();
        }
    }

    // min(cap, base * 2^(attempts-1)), plus a jitter of up to a fifth of that delay.
    private static TimeSpan ComputeBackoff(int attempts)
    {
        double multiplier = Math.Pow(2, Math.Max(0, attempts - 1));
        double delayMs = Math.Min(BackoffCap.TotalMilliseconds, BackoffBase.TotalMilliseconds * multiplier);
        double jitterMs = Random.Shared.NextDouble() * delayMs * 0.2;
        return TimeSpan.FromMilliseconds(delayMs + jitterMs);
    }
}
