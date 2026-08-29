using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>Why an introduce exchange did or did not produce a member.</summary>
public enum MemberAddOutcome
{
    /// <summary>Reachable, clustered, version-matched — the row was recorded.</summary>
    Added,

    /// <summary>The candidate URL does not parse as an absolute <c>http(s)</c> URL.</summary>
    InvalidUrl,

    /// <summary>The candidate did not answer, or errored, within the handshake timeout.</summary>
    Unreachable,

    /// <summary>The candidate answered but takes no part in a cluster.</summary>
    NotCluster,

    /// <summary>Two nodes whose route versions differ. Checked only between nodes: it is a statement about
    /// a surface an anchor does not serve.</summary>
    VersionMismatch,

    /// <summary>The candidate speaks a different member-to-member record version. The routes match, so
    /// nothing else would catch it, and what follows a silent join is a member whose gossip arrives
    /// half-empty.</summary>
    ProtocolMismatch,

    /// <summary>The candidate is this member. A member is not its own peer, and a row for self would make
    /// the mesh gossip with a mirror.</summary>
    IsSelf,

    /// <summary>An address outside loopback and the private ranges was offered over plaintext. The cluster
    /// secret authenticates but does not encrypt, and what crosses this wire carries identity.</summary>
    InsecureTransport,
}

/// <summary>
/// The result of one introduce exchange. <see cref="Member"/> is populated only on
/// <see cref="MemberAddOutcome.Added"/>; <see cref="RemoteApiVersion"/> only on a version mismatch, so the
/// refusal can say which two versions disagreed.
/// </summary>
public sealed record MemberAddResult(
    MemberAddOutcome Outcome, MemberRow? Member = null, string? RemoteApiVersion = null);

/// <summary>
/// The symmetric join handshake. Both halves live here so the initiator and the receiver run <em>the
/// same</em> validation over <em>the same</em> record and record the same things: adding B from A leaves
/// the identical cluster state as adding A from B, and a simultaneous mutual introduction leaves one row
/// per member rather than two.
/// <para>
/// The exchange also carries the addresses. A member cannot determine its own public address, so the URL an
/// operator pasted — the one address a human has stated and this member has just proven answers — is handed
/// to the far side, which adopts it. That is what lets a member join a cluster with nothing configured but
/// the shared secret.
/// </para>
/// </summary>
public sealed class MemberHandshakeService(
    IHttpClientFactory httpClientFactory,
    MembersStore members,
    SelfIdentityStore selfIdentity,
    IMemberCardSource cards,
    IClusterTokenService clusterTokens,
    ClusterOptions options,
    ILogger<MemberHandshakeService> logger)
{
    /// <summary>The named client the handshake reaches a candidate with — a short timeout, because a hung
    /// candidate must not stall an operator's request.</summary>
    public const string HttpClientName = "kgsm-cluster-handshake";

    /// <summary>This member's own card.</summary>
    public Task<MemberCard> BuildCardAsync(CancellationToken ct) => cards.BuildAsync(ct);

    /// <summary>
    /// The one predicate both sides run over the other's card. Keeping it a single function is what makes
    /// the handshake symmetric in fact rather than by convention: there is no second implementation to
    /// drift out of step with this one.
    /// </summary>
    public MemberAddOutcome Validate(MemberCard? card, MemberCard? mine)
    {
        if (card is null || string.IsNullOrWhiteSpace(card.MemberId))
            return MemberAddOutcome.Unreachable;

        if (string.Equals(card.MemberId, options.MemberId, StringComparison.Ordinal))
            return MemberAddOutcome.IsSelf;

        if (!card.Clustered)
            return MemberAddOutcome.NotCluster;

        if (card.Protocol != ClusterProtocol.Current)
            return MemberAddOutcome.ProtocolMismatch;

        // The route version is a node's statement about a surface an anchor does not serve, so it is
        // checked only where both sides actually have one. An anchor joining a node compares protocol and
        // nothing else, which is the whole reason an anchor can join at all.
        if (card.Node is { } theirs && mine?.Node is { } ours
            && !string.Equals(theirs.ApiVersion, ours.ApiVersion, StringComparison.Ordinal))
        {
            return MemberAddOutcome.VersionMismatch;
        }

        foreach (MemberCandidate candidate in card.Candidates ?? [])
        {
            if (!IsTransportAcceptable(candidate.Url))
                return MemberAddOutcome.InsecureTransport;
        }

        return MemberAddOutcome.Added;
    }

    /// <summary>
    /// Whether an address may be spoken to. Plaintext is fine inside a machine or a private network, where
    /// the operator already controls the wire; across anything else the cluster secret authenticates but
    /// does not encrypt, and what crosses carries a person's identity.
    /// </summary>
    public static bool IsTransportAcceptable(string? url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri)) return false;
        if (uri.Scheme == Uri.UriSchemeHttps) return true;
        if (uri.Scheme != Uri.UriSchemeHttp) return false;
        return IsLocalOrPrivate(uri.Host);
    }

    private static readonly string[] LocalSuffixes = [".local", ".lan", ".internal", ".home.arpa"];

    private static bool IsLocalOrPrivate(string host)
    {
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase)) return true;

        if (!IPAddress.TryParse(host, out IPAddress? ip))
        {
            // A name rather than an address. A single label resolves only inside a local search domain — a
            // name reachable from the public internet always carries a dot — and the private-use suffixes
            // are local by definition. Anything else is treated as public and must come over TLS; this is
            // decided without a lookup, so a validation predicate never waits on a resolver.
            if (!host.Contains('.')) return true;
            return LocalSuffixes.Any(suffix => host.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));
        }

        if (IPAddress.IsLoopback(ip)) return true;

        if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
        {
            byte[] b = ip.GetAddressBytes();
            return b[0] switch
            {
                10 => true,
                127 => true,
                172 => b[1] >= 16 && b[1] <= 31,
                192 => b[1] == 168,
                169 => b[1] == 254,
                _ => false,
            };
        }

        return ip.IsIPv6LinkLocal || ip.IsIPv6SiteLocal
            || (ip.GetAddressBytes()[0] & 0xFE) == 0xFC;   // fc00::/7 unique-local
    }

    /// <summary>
    /// The initiator half: an operator pasted <paramref name="url"/>, so introduce ourselves to it and
    /// record what comes back. Fail-open on reachability — an unreachable candidate is an honest outcome,
    /// never a thrown exception — and fail-closed on the identity checks, since a mismatch is never added.
    /// </summary>
    public async Task<MemberAddResult> AddMemberAsync(string url, string? nickname, CancellationToken ct)
    {
        string? target = SelfIdentityStore.Normalize(url);
        if (target is null)
            return new MemberAddResult(MemberAddOutcome.InvalidUrl);

        if (!IsTransportAcceptable(target))
            return new MemberAddResult(MemberAddOutcome.InsecureTransport);

        MintedClusterToken token;
        try
        {
            token = clusterTokens.Mint();
        }
        catch (InvalidOperationException)
        {
            // This member holds no cluster secret, so it has no identity to present to anybody and the
            // handshake cannot proceed. Adding a member is meaningless on an unclustered one in the first
            // place; this collapses to the same honest outcome as any other handshake failure.
            logger.LogWarning(
                "member handshake: this member is not clustered — cannot mint a service token for {Url}", url);
            return new MemberAddResult(MemberAddOutcome.Unreachable);
        }

        MemberCard mine = await BuildCardAsync(ct).ConfigureAwait(false);
        var outgoing = new IntroduceExchange(
            mine,
            // The address a human wrote down, which this request is about to prove answers. It is the one
            // thing the far side cannot work out for itself, and the reason it needs no configuration.
            new ReflectedAddress(target, SelfIdentityStore.OperatorProvenance),
            await selfIdentity.PanelOriginsAsync(ct).ConfigureAwait(false));

        IntroduceExchange? incoming;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{target}{ClusterRoutes.Introduce}")
            {
                Content = JsonContent.Create(outgoing, ClusterJsonContext.Default.IntroduceExchange),
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

            HttpClient client = httpClientFactory.CreateClient(HttpClientName);
            using HttpResponseMessage response = await client.SendAsync(request, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                // The far side ran the same predicate and refused. Its verdict is the operator's answer, so
                // it is reported as itself rather than flattened into unreachable.
                (MemberAddOutcome refusal, string? remoteVersion) =
                    await RefusalOutcomeAsync(response, ct).ConfigureAwait(false);
                logger.LogInformation(
                    "member handshake: {Url} answered {Status} on introduce ({Outcome})",
                    url, (int)response.StatusCode, refusal);
                return new MemberAddResult(refusal, RemoteApiVersion: remoteVersion);
            }

            await using Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            incoming = await JsonSerializer
                .DeserializeAsync(body, ClusterJsonContext.Default.IntroduceExchange, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            // Connection refused, DNS failure, TLS failure, or a body that is not valid JSON — all collapse
            // to the one honest "could not reach it".
            logger.LogInformation(ex, "member handshake: {Url} is unreachable", url);
            return new MemberAddResult(MemberAddOutcome.Unreachable);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // The client's own timeout fired: the candidate is slow or hung, not a cancelled request.
            logger.LogInformation("member handshake: {Url} timed out on introduce", url);
            return new MemberAddResult(MemberAddOutcome.Unreachable);
        }

        if (incoming is null)
            return new MemberAddResult(MemberAddOutcome.Unreachable);

        MemberAddOutcome verdict = Validate(incoming.Self, mine);
        if (verdict != MemberAddOutcome.Added)
            return new MemberAddResult(verdict, RemoteApiVersion: incoming.Self?.Node?.ApiVersion);

        // Where the far side saw this request arrive from. Without adopting it, a member that only ever
        // initiates joins never learns any address for itself: nothing local reveals it, and the only other
        // source is somebody introducing themselves here. It would then gossip itself with no address at
        // all, and every member that learned it that way would hold a row nothing can reach.
        if (incoming.YouAre is { } reflection && SelfIdentityStore.Normalize(reflection.Url) is { } ours)
        {
            await selfIdentity
                .RecordCandidateAsync(ours, client: false, SelfIdentityStore.PeerObservedProvenance, ct)
                .ConfigureAwait(false);
        }

        // The far side's origins are as good as our own under the shared secret, and this is the direction
        // the receiving half already covers.
        foreach (string origin in incoming.PanelOrigins ?? [])
            await selfIdentity.RecordPanelOriginAsync(origin, ct).ConfigureAwait(false);

        MemberRow member = await RecordAsync(incoming, target, nickname, ct).ConfigureAwait(false);
        return new MemberAddResult(MemberAddOutcome.Added, member);
    }

    /// <summary>
    /// The receiver half: a member has introduced itself. Runs the same predicate over its card, records the
    /// mirror of what it recorded about us, and answers with our own card, so one round trip leaves both
    /// sides equally informed.
    /// </summary>
    /// <param name="incoming">The caller's exchange record.</param>
    /// <param name="observedAddress">The source address this request arrived from, when one could be
    /// determined — a hint, reported back honestly labelled, never treated as a URL.</param>
    public async Task<(MemberAddOutcome Outcome, IntroduceExchange? Answer)> ReceiveAsync(
        IntroduceExchange? incoming, string? observedAddress, CancellationToken ct)
    {
        if (incoming is null)
            return (MemberAddOutcome.Unreachable, null);

        MemberCard mine = await BuildCardAsync(ct).ConfigureAwait(false);
        MemberAddOutcome verdict = Validate(incoming.Self, mine);
        if (verdict != MemberAddOutcome.Added)
            return (verdict, null);

        // What the caller says about us. It reached this member at that address, so the claim is worth more
        // than anything this member could infer about itself.
        if (incoming.YouAre is { } reflection && SelfIdentityStore.Normalize(reflection.Url) is { } ours)
        {
            await selfIdentity
                .RecordCandidateAsync(ours, client: true, SelfIdentityStore.OperatorProvenance, ct)
                .ConfigureAwait(false);
        }

        // The shared secret is the trust boundary, so another member's origins are as good as our own — and
        // without the merge a person signing in through one member could not reach the others from the same
        // panel.
        foreach (string origin in incoming.PanelOrigins ?? [])
            await selfIdentity.RecordPanelOriginAsync(origin, ct).ConfigureAwait(false);

        // The caller's own candidates are all we have to reach it by — it named no address for itself that
        // we can verify yet, so the row starts unverified and the poller settles it.
        await RecordAsync(incoming, address: null, nickname: null, ct).ConfigureAwait(false);

        var answer = new IntroduceExchange(
            mine,
            observedAddress is null
                ? null
                : new ReflectedAddress(observedAddress, SelfIdentityStore.PeerObservedProvenance),
            await selfIdentity.PanelOriginsAsync(ct).ConfigureAwait(false));

        return (MemberAddOutcome.Added, answer);
    }

    /// <summary>
    /// Write, or refresh, the roster row for the member in <paramref name="exchange"/>, keyed on its member
    /// id so a simultaneous introduction from both directions converges on one row instead of two.
    /// <paramref name="address"/> is the operator-pasted URL when this member initiated — proven reachable
    /// by the exchange that just succeeded, so it leads the candidate list.
    /// </summary>
    private Task<MemberRow> RecordAsync(
        IntroduceExchange exchange, string? address, string? nickname, CancellationToken ct)
    {
        MemberCard card = exchange.Self;
        List<MemberCandidate> candidates = address is null
            ? [.. card.Candidates ?? []]
            : [new MemberCandidate(address, Client: true), .. card.Candidates ?? []];

        DateTimeOffset now = DateTimeOffset.UtcNow;

        return members.UpsertByMemberIdAsync(card.MemberId, Build, ct);

        MemberRow Build(MemberRow? existing)
        {
            string merged = MemberCandidates.Merge(existing?.Candidates, candidates);
            MemberRow row = (existing ?? MemberRow.New(card.MemberId, card.Kind)) with
            {
                MemberId = card.MemberId,
                Kind = MemberKind.IsKnown(card.Kind) ? card.Kind : MemberKind.Node,
                Nickname = nickname ?? existing?.Nickname,
                Candidates = merged,
                Incarnation = Math.Max(card.Incarnation, existing?.Incarnation ?? 0),
                MembershipState = existing?.MembershipState ?? GossipState.Alive,
                StateChangedAt = existing?.StateChangedAt ?? now,
                // An address the far side answered on is proven for THIS exchange; anything it merely
                // listed is a claim the poller settles. Reaching it as part of the handshake is first-hand
                // evidence, so status and last-seen are honest here — the address flag is not, until a
                // probe confirms the member id behind it.
                Status = address is null ? existing?.Status ?? MemberStatus.Unknown : MemberStatus.Reachable,
                LastSeen = address is null ? existing?.LastSeen : now,
                ApiVersion = card.Node?.ApiVersion ?? existing?.ApiVersion ?? "",
                Enabled = existing?.Enabled ?? true,
            };
            return row with
            {
                Url = address ?? (existing is { AddressVerified: true, Url.Length: > 0 }
                    ? existing.Url
                    : MemberCandidates.Best(MemberCandidates.Decode(merged))),
            };
        }
    }

    /// <summary>
    /// Map the far side's refusal back onto the outcome it named, so an operator sees the reason the other
    /// member gave rather than a generic failure — and, where the refusal carried the value that
    /// disagreed, that too. An unreadable body degrades to unreachable.
    /// </summary>
    private static async Task<(MemberAddOutcome Outcome, string? RemoteVersion)> RefusalOutcomeAsync(
        HttpResponseMessage response, CancellationToken ct)
    {
        string code;
        string? remoteVersion = null;
        try
        {
            using JsonDocument document = await JsonDocument
                .ParseAsync(await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false), cancellationToken: ct)
                .ConfigureAwait(false);
            if (!document.RootElement.TryGetProperty("error", out JsonElement error))
                return (MemberAddOutcome.Unreachable, null);

            code = error.TryGetProperty("code", out JsonElement value) ? value.GetString() ?? "" : "";
            if (error.TryGetProperty("details", out JsonElement details)
                && details.TryGetProperty("remote", out JsonElement remote))
            {
                remoteVersion = remote.GetString();
            }
        }
        catch (Exception ex) when (ex is JsonException or HttpRequestException)
        {
            return (MemberAddOutcome.Unreachable, null);
        }

        MemberAddOutcome outcome = code switch
        {
            "member_is_self" => MemberAddOutcome.IsSelf,
            "version_mismatch" => MemberAddOutcome.VersionMismatch,
            "member_not_cluster" => MemberAddOutcome.NotCluster,
            "protocol_mismatch" => MemberAddOutcome.ProtocolMismatch,
            "insecure_transport" => MemberAddOutcome.InsecureTransport,
            _ => MemberAddOutcome.Unreachable,
        };
        return (outcome, remoteVersion);
    }
}
