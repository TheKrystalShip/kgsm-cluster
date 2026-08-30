using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Membership;
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
        endpoints.MapPost(ClusterRoutes.Sync, SyncAsync).WithName("ClusterSync").AllowAnonymous();
        endpoints.MapPost(ClusterRoutes.Introduce, IntroduceAsync).WithName("ClusterIntroduce").AllowAnonymous();
        endpoints.MapGet(ClusterRoutes.Identity, IdentityAsync).WithName("ClusterIdentity").AllowAnonymous();
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
        var inbox = services.GetRequiredService<ClusterInbox>();
        ILogger logger = services.GetRequiredService<ILoggerFactory>()
            .CreateLogger("TheKrystalShip.KGSM.Cluster.Inbox");

        // Reject on an oversized declared length before touching the body at all.
        if (context.Request.ContentLength is long declared && declared > MaxEnvelopeBytes)
        {
            await TooLargeAsync(context, ct).ConfigureAwait(false);
            return;
        }

        (ClusterPrincipal? principal, bool handled) = await AuthenticateAsync(context, ct).ConfigureAwait(false);
        if (handled) return;

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

        if (!string.Equals(envelope.From, principal!.MemberId, StringComparison.Ordinal))
        {
            logger.LogWarning(
                "cluster inbox: envelope.from={From} does not match the authenticated token's member {MemberId} " +
                "— rejected",
                envelope.From, principal!.MemberId);
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

    /// <summary>
    /// The best-effort roster exchange. Token-authed and gated exactly as the inbox is, and deliberately
    /// separate from the durable bus: a round is fire-and-forget anti-entropy with no outbox row and no
    /// retry. The caller pushes its whole roster, this member merges it, and its own roster comes back for
    /// the caller to merge in turn.
    /// </summary>
    private static async Task SyncAsync(HttpContext context)
    {
        CancellationToken ct = context.RequestAborted;
        IServiceProvider services = context.RequestServices;
        var gossip = services.GetRequiredService<GossipService>();
        var options = services.GetRequiredService<ClusterOptions>();

        (ClusterPrincipal? principal, bool handled) = await AuthenticateAsync(context, ct).ConfigureAwait(false);
        if (handled) return;

        (bool withinLimit, string body) =
            await ReadBoundedBodyAsync(context.Request.Body, MaxEnvelopeBytes, ct).ConfigureAwait(false);
        if (!withinLimit)
        {
            await TooLargeAsync(context, ct).ConfigureAwait(false);
            return;
        }

        SyncRequest? request;
        try
        {
            request = JsonSerializer.Deserialize(body, ClusterJsonContext.Default.SyncRequest);
        }
        catch (JsonException)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "bad_request",
                "the sync body is not valid JSON", ct).ConfigureAwait(false);
            return;
        }

        if (request?.Members is null)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "bad_request",
                "the sync body carries no members", ct).ConfigureAwait(false);
            return;
        }

        // Reaching us with a valid token IS liveness evidence, from the one direction a probe cannot
        // supply. It is recorded before the merge so an asymmetric partition resolves in favour of the
        // member that is demonstrably talking.
        await gossip.RecordInboundContactAsync(principal!.MemberId, ct).ConfigureAwait(false);
        await gossip.MergeIncomingAsync(request.Members, request.State, request.From, ct).ConfigureAwait(false);

        IReadOnlyList<SyncMember> roster = await gossip.BuildLocalRosterAsync(ct).ConfigureAwait(false);
        IReadOnlyList<ClusterAssignment> state = await gossip.BuildLocalStateAsync(ct).ConfigureAwait(false);
        await WriteJsonAsync(context, StatusCodes.Status200OK, new SyncResponse(options.MemberId, roster, state),
            ClusterJsonContext.Default.SyncResponse, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The receiving half of the symmetric join. The same predicate the initiator ran is run here over the
    /// caller's card, and this member records the mirror of what the caller recorded, so one round trip
    /// leaves both sides equally informed and joining in either direction leaves the same cluster.
    /// </summary>
    private static async Task IntroduceAsync(HttpContext context)
    {
        CancellationToken ct = context.RequestAborted;
        IServiceProvider services = context.RequestServices;
        var handshake = services.GetRequiredService<MemberHandshakeService>();

        (_, bool handled) = await AuthenticateAsync(context, ct).ConfigureAwait(false);
        if (handled) return;

        (bool withinLimit, string body) =
            await ReadBoundedBodyAsync(context.Request.Body, MaxEnvelopeBytes, ct).ConfigureAwait(false);
        if (!withinLimit)
        {
            await TooLargeAsync(context, ct).ConfigureAwait(false);
            return;
        }

        IntroduceExchange? incoming;
        try
        {
            incoming = JsonSerializer.Deserialize(body, ClusterJsonContext.Default.IntroduceExchange);
        }
        catch (JsonException)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "bad_request",
                "the introduce body is not valid JSON", ct).ConfigureAwait(false);
            return;
        }

        // Where the request came from, as a hint the answer reports back honestly labelled. It is a socket
        // address rather than a URL, so it seeds a candidate and nothing depends on it.
        string? observed = context.Connection.RemoteIpAddress is { } ip
            ? $"{context.Request.Scheme}://{FormatHost(ip)}"
            : null;

        (MemberAddOutcome outcome, IntroduceExchange? answer) =
            await handshake.ReceiveAsync(incoming, observed, ct).ConfigureAwait(false);

        if (outcome != MemberAddOutcome.Added || answer is null)
        {
            (int status, string code, string message) = RefusalFor(outcome);
            // A refusal that only names itself leaves the far side knowing something is wrong and nothing
            // about what to change, so the two values that disagreed travel back with it.
            ClusterErrorDetails? details = outcome == MemberAddOutcome.VersionMismatch
                ? new ClusterErrorDetails(
                    Remote: (await handshake.BuildCardAsync(ct).ConfigureAwait(false)).Node?.ApiVersion,
                    Local: incoming?.Self?.Node?.ApiVersion)
                : null;
            await ErrorAsync(context, status, code, message, ct, details).ConfigureAwait(false);
            return;
        }

        await WriteJsonAsync(context, StatusCodes.Status200OK, answer,
            ClusterJsonContext.Default.IntroduceExchange, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Who this member is. Token-authed like the rest: a member states itself to the cluster, not to
    /// anybody who asks.
    /// </summary>
    private static async Task IdentityAsync(HttpContext context)
    {
        CancellationToken ct = context.RequestAborted;
        var cards = context.RequestServices.GetRequiredService<IMemberCardSource>();

        (_, bool handled) = await AuthenticateAsync(context, ct).ConfigureAwait(false);
        if (handled) return;

        MemberCard card = await cards.BuildAsync(ct).ConfigureAwait(false);
        await WriteJsonAsync(context, StatusCodes.Status200OK, card, ClusterJsonContext.Default.MemberCard, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Validate the caller's service token and the member gate. Returns the caller when it passes; when it
    /// does not, the response has already been written and the caller must return immediately.
    /// </summary>
    private static async Task<(ClusterPrincipal? Principal, bool Handled)> AuthenticateAsync(
        HttpContext context, CancellationToken ct)
    {
        ClusterPrincipal? caller = await ClusterRequest.AuthenticateAsync(context).ConfigureAwait(false);
        return (caller, caller is null);
    }

    /// <summary>The status and code each refusal answers with, so both sides of the handshake name the same
    /// reason and an operator sees the far side's verdict rather than a generic failure.</summary>
    private static (int Status, string Code, string Message) RefusalFor(MemberAddOutcome outcome) => outcome switch
    {
        MemberAddOutcome.IsSelf => (StatusCodes.Status409Conflict, "member_is_self",
            "that member is this one"),
        MemberAddOutcome.VersionMismatch => (StatusCodes.Status409Conflict, "version_mismatch",
            "the two nodes serve different route versions"),
        MemberAddOutcome.ProtocolMismatch => (StatusCodes.Status409Conflict, "protocol_mismatch",
            "the two members speak different cluster protocol versions"),
        MemberAddOutcome.NotCluster => (StatusCodes.Status422UnprocessableEntity, "member_not_cluster",
            "that member takes no part in a cluster"),
        MemberAddOutcome.InsecureTransport => (StatusCodes.Status422UnprocessableEntity, "insecure_transport",
            "a public address was offered over plaintext"),
        MemberAddOutcome.InvalidUrl => (StatusCodes.Status400BadRequest, "invalid_url",
            "that is not an absolute http(s) URL"),
        _ => (StatusCodes.Status502BadGateway, "member_unreachable",
            "that member did not answer"),
    };

    // An IPv6 literal has to be bracketed to be a valid authority.
    private static string FormatHost(System.Net.IPAddress ip)
        => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? $"[{ip}]" : ip.ToString();

    private static Task TooLargeAsync(HttpContext context, CancellationToken ct)
        => ErrorAsync(context, StatusCodes.Status413PayloadTooLarge, "payload_too_large",
            $"the envelope exceeds the {MaxEnvelopeBytes}-byte limit", ct);

    private static Task ErrorAsync(
        HttpContext context, int status, string code, string message, CancellationToken ct,
        ClusterErrorDetails? details = null)
        => WriteJsonAsync(
            context, status, ClusterError.Of(code, message, details), ClusterJsonContext.Default.ClusterError, ct);

    private static async Task WriteJsonAsync<T>(
        HttpContext context, int status, T value, JsonTypeInfo<T> typeInfo, CancellationToken ct)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json; charset=utf-8";
        await JsonSerializer.SerializeAsync(context.Response.Body, value, typeInfo, ct).ConfigureAwait(false);
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
