using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Messaging;

namespace TheKrystalShip.KGSM.Cluster;

/// <summary>
/// The member-to-member wire, served by the package rather than reimplemented by each member. One
/// implementation of the status-code table, the size cap, the token check and the spoof guard, so two
/// members can never disagree about what the protocol is.
/// </summary>
/// <remarks>
/// <para>
/// Mapped onto whatever <see cref="IEndpointRouteBuilder"/> the member already has. These compose
/// beside a member's own controllers and assume nothing about how its host was built, so a member
/// serving MVC routes hosts them unchanged.
/// </para>
/// <para>
/// <b>Every handler is a <see cref="RequestDelegate"/>, and reads its own request and writes its own
/// response.</b> The convenient overloads that take an arbitrary delegate reflect over its parameters
/// and return type to bind them, which a Native-AOT member cannot do. Writing the response by hand is
/// what keeps this package embeddable in every member rather than only the one that runs a JIT.
/// </para>
/// </remarks>
public static class ClusterEndpoints
{
    /// <summary>The envelope size cap. A body larger than this is rejected without being fully read.</summary>
    private const int MaxEnvelopeBytes = 64 * 1024;

    /// <summary>
    /// Map every member-to-member endpoint. Call once during startup, after
    /// <see cref="ClusterServiceCollectionExtensions.AddKgsmCluster"/>.
    /// </summary>
    public static IEndpointRouteBuilder MapClusterEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost(ClusterRoutes.Inbox, InboxAsync).WithName("ClusterInbox").AllowAnonymous();
        return endpoints;
    }

    /// <summary>
    /// Receive one envelope from another member. Authentication is fail-closed and availability is
    /// fail-open: a bad token is rejected and never processed, while a member that is merely down is
    /// the sender's problem to retry. A <c>500</c> is the only answer that keeps a message in the
    /// sender's outbox, so it is reserved for a transient handler failure; a message this member can
    /// never apply answers <c>200</c> or <c>400</c> instead and never wedges the sender's queue.
    /// </summary>
    private static async Task InboxAsync(HttpContext context)
    {
        CancellationToken ct = context.RequestAborted;
        IServiceProvider services = context.RequestServices;
        var tokens = services.GetRequiredService<IClusterTokenService>();
        var gate = services.GetRequiredService<IClusterMemberGate>();
        var inbox = services.GetRequiredService<ClusterInbox>();
        ILogger logger = services.GetRequiredService<ILoggerFactory>()
            .CreateLogger("TheKrystalShip.KGSM.Cluster.Inbox");

        // Reject on an oversized declared length before touching the body at all.
        if (context.Request.ContentLength is long declared && declared > MaxEnvelopeBytes)
        {
            await TooLargeAsync(context, ct).ConfigureAwait(false);
            return;
        }

        string? token = ExtractBearerToken(context.Request);
        if (token is null)
        {
            await ErrorAsync(context, StatusCodes.Status401Unauthorized, "invalid_cluster_token",
                "missing bearer token", ct).ConfigureAwait(false);
            return;
        }

        ClusterPrincipal? principal = await tokens.ValidateAsync(token).ConfigureAwait(false);
        if (principal is null)
        {
            await ErrorAsync(context, StatusCodes.Status401Unauthorized, "invalid_cluster_token",
                "invalid, expired, or unsigned cluster service token", ct).ConfigureAwait(false);
            return;
        }

        if (!await gate.IsEnabledAsync(principal.MemberId).ConfigureAwait(false))
        {
            await ErrorAsync(context, StatusCodes.Status403Forbidden, "member_disabled",
                $"member '{principal.MemberId}' is not an enabled member of this cluster", ct).ConfigureAwait(false);
            return;
        }

        // Read the body under a hard byte cap as well: a chunked request can omit or understate the
        // length the pre-check above read.
        (bool withinLimit, string body) = await ReadBoundedBodyAsync(context.Request.Body, MaxEnvelopeBytes, ct)
            .ConfigureAwait(false);
        if (!withinLimit)
        {
            await TooLargeAsync(context, ct).ConfigureAwait(false);
            return;
        }

        ClusterEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize(body, ClusterJsonContext.Default.ClusterEnvelope);
        }
        catch (JsonException)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "bad_request",
                "the envelope body is not valid JSON", ct).ConfigureAwait(false);
            return;
        }

        if (envelope is null
            || string.IsNullOrWhiteSpace(envelope.Id)
            || string.IsNullOrWhiteSpace(envelope.Type)
            || string.IsNullOrWhiteSpace(envelope.From))
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "bad_request",
                "the envelope is missing one or more of id/type/from", ct).ConfigureAwait(false);
            return;
        }

        if (!string.Equals(envelope.From, principal.MemberId, StringComparison.Ordinal))
        {
            logger.LogWarning(
                "cluster inbox: envelope.from={From} does not match the authenticated token's member {MemberId} " +
                "— rejected",
                envelope.From, principal.MemberId);
            await ErrorAsync(context, StatusCodes.Status403Forbidden, "from_mismatch",
                "envelope.from does not match the authenticated cluster service token", ct).ConfigureAwait(false);
            return;
        }

        InboxResult result = await inbox.ReceiveAsync(envelope, ct).ConfigureAwait(false);
        if (result == InboxResult.TransientFailure)
        {
            await ErrorAsync(context, StatusCodes.Status500InternalServerError, "internal",
                "a transient failure occurred processing this message; retry", ct).ConfigureAwait(false);
            return;
        }

        // Applied, Duplicate and DroppedUnknown all acknowledge: the sender cannot tell them apart and
        // none of them should keep the message in its outbox.
        await WriteJsonAsync(context, StatusCodes.Status200OK, new InboxAck("accepted"),
            ClusterJsonContext.Default.InboxAck, ct).ConfigureAwait(false);
    }

    private static Task TooLargeAsync(HttpContext context, CancellationToken ct)
        => ErrorAsync(context, StatusCodes.Status413PayloadTooLarge, "payload_too_large",
            $"the envelope exceeds the {MaxEnvelopeBytes}-byte limit", ct);

    private static Task ErrorAsync(HttpContext context, int status, string code, string message, CancellationToken ct)
        => WriteJsonAsync(context, status, ClusterError.Of(code, message), ClusterJsonContext.Default.ClusterError, ct);

    private static async Task WriteJsonAsync<T>(
        HttpContext context, int status, T value, JsonTypeInfo<T> typeInfo, CancellationToken ct)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json; charset=utf-8";
        await JsonSerializer.SerializeAsync(context.Response.Body, value, typeInfo, ct).ConfigureAwait(false);
    }

    private static string? ExtractBearerToken(HttpRequest request)
    {
        string? header = request.Headers.Authorization.ToString();
        if (string.IsNullOrWhiteSpace(header)) return null;
        const string prefix = "Bearer ";
        return header.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? header[prefix.Length..].Trim()
            : null;
    }

    /// <summary>
    /// Read at most <paramref name="max"/> bytes. Returns whether the body fitted: a stream that still
    /// has content at the cap is over the limit and its content is discarded rather than parsed.
    /// </summary>
    private static async Task<(bool WithinLimit, string Body)> ReadBoundedBodyAsync(
        Stream stream, int max, CancellationToken ct)
    {
        byte[] buffer = new byte[max + 1];
        int read = 0;
        while (read < buffer.Length)
        {
            int got = await stream.ReadAsync(buffer.AsMemory(read, buffer.Length - read), ct).ConfigureAwait(false);
            if (got == 0) break;
            read += got;
        }
        return read > max ? (false, "") : (true, Encoding.UTF8.GetString(buffer, 0, read));
    }
}
