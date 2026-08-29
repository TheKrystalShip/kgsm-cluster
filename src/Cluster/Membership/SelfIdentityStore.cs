using System.Net;
using Microsoft.Data.Sqlite;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// What this member knows about itself: the addresses it answers at, and the browser origins somebody has
/// signed in from.
/// <para>
/// A member cannot determine its own public address — behind NAT, a reverse proxy or a load balancer, the
/// address it is reached at leaves no local trace. So the addresses here are <b>reflected</b>: whoever
/// demonstrably reached this member reports where they reached it, and that statement is recorded. Two
/// sources carry weight, and both are browser-reachable by construction — the URL an operator pasted into
/// a panel and another member then proved answers, and the scheme and host a browser arrived on.
/// </para>
/// <para>
/// Configuration wins when it is present. A configured public address and a configured member-only address
/// are merged in ahead of anything learned, which is what lets an operator correct a topology where
/// reflection reports the wrong thing. Both are read from options on every resolve rather than copied into
/// the table, so configuration stays the single statement of itself.
/// </para>
/// <para>
/// <b>Panel origins are carried, not interpreted.</b> They travel with the join exchange so a panel served
/// from one member reaches every other without a per-member allowlist, and what a member does with the
/// list — a browser-facing one consults it for CORS, a headless one ignores it — is that member's business.
/// </para>
/// </summary>
public sealed class SelfIdentityStore(ClusterStore store, ClusterOptions options)
{
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    // Reference assignment is atomic; a racing read may briefly see the prior list and converges on the
    // next read. The cache exists because a CORS check consults the origins on the request path.
    private IReadOnlyList<SelfFact>? _facts;

    /// <summary>An address a member answers at.</summary>
    public const string CandidateKind = "candidate";

    /// <summary>A browser origin somebody signed in from.</summary>
    public const string OriginKind = "origin";

    /// <summary>Provenance of an address a human pasted into a panel and a member then proved answers — the
    /// strongest address statement in the system.</summary>
    public const string OperatorProvenance = "operator";

    /// <summary>Provenance of a source address a member saw a request arrive from: a socket address, not a
    /// URL, so it seeds a candidate and nothing depends on it.</summary>
    public const string PeerObservedProvenance = "peer-observed";

    /// <summary>Provenance of an address a browser arrived on. It is a browser-reachable address by
    /// construction, which is exactly what a roster's browser address needs.</summary>
    public const string BrowserObserved = "observed";

    /// <summary>Trust order for candidates. An operator's pasted URL is the strongest statement: a human
    /// wrote it down and another member proved it answers. A peer-observed source address is the weakest.</summary>
    private static int Rank(string provenance) => provenance switch
    {
        "config-client" => 0,
        OperatorProvenance => 1,
        BrowserObserved => 2,
        "config-node" => 3,
        _ => 4,
    };

    /// <summary>Whether an address points back at whoever reads it.</summary>
    private static bool IsLoopback(string url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri)) return false;
        if (string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase)) return true;
        return IPAddress.TryParse(uri.Host, out IPAddress? ip) && IPAddress.IsLoopback(ip);
    }

    /// <summary>
    /// Normalise an address for comparison and storage: no trailing slash, scheme and host lower-cased,
    /// default ports dropped. Null when the input is not an absolute <c>http(s)</c> URL — an unusable
    /// address is discarded, never stored in a mangled form.
    /// </summary>
    public static string? Normalize(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        if (!Uri.TryCreate(raw.Trim(), UriKind.Absolute, out Uri? uri)) return null;
        if (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps) return null;

        var builder = new UriBuilder(uri) { Path = "", Query = "", Fragment = "" };
        if (builder.Uri.IsDefaultPort) builder.Port = -1;
        return builder.Uri.GetLeftPart(UriPartial.Authority).TrimEnd('/');
    }

    /// <summary>
    /// This member's own address candidates, most-trusted first: the configured overrides, then everything
    /// reflected onto it, de-duplicated. Empty is an honest answer — a member nobody has reached yet, that
    /// carries no configured address, knows of no way to be called and says so rather than inventing one.
    /// </summary>
    public async Task<IReadOnlyList<MemberCandidate>> CandidatesAsync(CancellationToken ct)
    {
        var seen = new Dictionary<string, MemberCandidate>(StringComparer.OrdinalIgnoreCase);

        void Offer(string? raw, bool client)
        {
            string? url = Normalize(raw);
            if (url is null || seen.ContainsKey(url)) return;
            seen[url] = new MemberCandidate(url, client);
        }

        Offer(options.PublicBaseUrl, client: true);
        Offer(options.GossipUrl, client: false);

        foreach (SelfFact fact in await FactsAsync(ct).ConfigureAwait(false))
        {
            if (!string.Equals(fact.Kind, CandidateKind, StringComparison.Ordinal)) continue;
            Offer(fact.Value, fact.Client);
        }

        return [.. seen.Values];
    }

    /// <summary>Every browser origin recorded here. Served from the in-memory cache, because a CORS check
    /// reads it on the request path.</summary>
    public async Task<IReadOnlyList<string>> PanelOriginsAsync(CancellationToken ct)
    {
        IReadOnlyList<SelfFact> facts = await FactsAsync(ct).ConfigureAwait(false);
        return [.. facts.Where(f => string.Equals(f.Kind, OriginKind, StringComparison.Ordinal)).Select(f => f.Value)];
    }

    /// <summary>
    /// Load what this member knows about itself into memory. Called once at startup, because a CORS check
    /// reads the cache synchronously and a cold cache reads as "nothing learned" — which would leave a
    /// member that HAS learned an origin answering as though it had not.
    /// </summary>
    public Task PrimeAsync(CancellationToken ct) => FactsAsync(ct);

    /// <summary>
    /// The cached origins, or null when nothing has been loaded yet. Lets a CORS check answer synchronously
    /// without blocking a request thread on the database; a cold cache falls through to whatever allowlist
    /// the member already had, which is the same answer it gave before it learned anything.
    /// </summary>
    public IReadOnlyList<string>? CachedPanelOrigins() =>
        _facts is null
            ? null
            : [.. _facts.Where(f => string.Equals(f.Kind, OriginKind, StringComparison.Ordinal)).Select(f => f.Value)];

    /// <summary>Record an address this member was reached at. Re-recording refreshes the row rather than
    /// adding a second one.</summary>
    public Task RecordCandidateAsync(string url, bool client, string provenance, CancellationToken ct) =>
        RecordAsync(CandidateKind, url, client, provenance, ct);

    /// <summary>Record a browser origin somebody signed in from.</summary>
    public Task RecordPanelOriginAsync(string origin, CancellationToken ct) =>
        RecordAsync(OriginKind, origin, client: true, BrowserObserved, ct);

    private async Task RecordAsync(string kind, string raw, bool client, string provenance, CancellationToken ct)
    {
        string? value = Normalize(raw);
        if (value is null) return;

        // A candidate exists to tell OTHER members where to find this one, and a loopback address means
        // "me" wherever it is read — so observing one says nothing another member could use, and
        // advertising it hands every one of them an address that resolves back to itself. An operator who
        // deliberately types one is a different matter: co-located members are a real topology, and there
        // the address is exactly right.
        if (string.Equals(kind, CandidateKind, StringComparison.Ordinal)
            && !string.Equals(provenance, OperatorProvenance, StringComparison.Ordinal)
            && IsLoopback(value))
        {
            return;
        }

        await _writeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            string id = kind + ":" + value;
            DateTimeOffset now = DateTimeOffset.UtcNow;
            SelfFact? existing = (await ReadFactsAsync(ct).ConfigureAwait(false))
                .FirstOrDefault(f => string.Equals(f.Id, id, StringComparison.Ordinal));

            // A stronger statement upgrades a weaker one: an address first seen as a browser host and later
            // pasted by an operator is thereafter an operator address.
            bool upgrade = existing is null || Rank(provenance) < Rank(existing.Provenance);
            string effectiveProvenance = upgrade ? provenance : existing!.Provenance;
            bool effectiveClient = upgrade ? client : existing!.Client;

            await store.WriteAsync(async (connection, token) =>
            {
                await using SqliteCommand command = connection.CreateCommand();
                command.CommandText = """
                    INSERT OR REPLACE INTO self_facts (id, kind, value, client, provenance, last_seen)
                    VALUES ($id, $kind, $value, $client, $provenance, $lastSeen);
                    """;
                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$kind", kind);
                command.Parameters.AddWithValue("$value", value);
                command.Parameters.AddWithValue("$client", effectiveClient ? 1 : 0);
                command.Parameters.AddWithValue("$provenance", effectiveProvenance);
                command.Parameters.AddWithValue("$lastSeen", SqliteValues.Stamp(now));
                await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }, ct).ConfigureAwait(false);

            _facts = null;
        }
        finally { _writeGate.Release(); }
    }

    private async Task<IReadOnlyList<SelfFact>> FactsAsync(CancellationToken ct)
    {
        if (_facts is { } cached) return cached;
        IReadOnlyList<SelfFact> rows = await ReadFactsAsync(ct).ConfigureAwait(false);
        _facts = rows;
        return rows;
    }

    private async Task<IReadOnlyList<SelfFact>> ReadFactsAsync(CancellationToken ct)
    {
        await using SqliteConnection connection = await store.OpenAsync(ct).ConfigureAwait(false);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT id, kind, value, client, provenance, last_seen FROM self_facts;";

        var rows = new List<SelfFact>();
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new SelfFact(
                reader.GetString(0), reader.GetString(1), reader.GetString(2),
                reader.GetInt32(3) != 0, reader.GetString(4), SqliteValues.ReadStamp(reader, 5)));
        }

        rows.Sort((a, b) =>
        {
            int byRank = Rank(a.Provenance).CompareTo(Rank(b.Provenance));
            return byRank != 0 ? byRank : b.LastSeen.CompareTo(a.LastSeen);
        });
        return rows;
    }
}

/// <summary>One thing this member has learned about itself.</summary>
/// <param name="Id">Kind and value together, so re-learning the same fact refreshes one row.</param>
/// <param name="Kind">A candidate address, or a browser origin.</param>
/// <param name="Value">The normalised address or origin.</param>
/// <param name="Client">Whether a browser can use it.</param>
/// <param name="Provenance">Who said so, which is what ranks it against the other statements.</param>
/// <param name="LastSeen">When it was last stated.</param>
public sealed record SelfFact(
    string Id, string Kind, string Value, bool Client, string Provenance, DateTimeOffset LastSeen);
