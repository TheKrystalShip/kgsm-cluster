using Microsoft.Data.Sqlite;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// The single data-access seam for the roster: the handshake, the enabled-member gate, the fan-out target
/// provider, the liveness poller and gossip all read and write it only through here.
/// </summary>
/// <remarks>
/// A cluster is masterless, so this is <em>this member's own copy</em> of who else is in the cluster — not
/// a shared table, and not authoritative for anybody else.
/// </remarks>
public sealed class MembersStore(ClusterStore store)
{
    private const string Columns =
        "id, member_id, kind, url, candidates, address_verified, nickname, incarnation, status, " +
        "membership_state, state_changed_at, latency_ms, last_seen, api_version, published, enabled";

    private readonly SemaphoreSlim _memberIdGate = new(1, 1);

    /// <summary>Every row, enabled and disabled, in no particular order.</summary>
    public Task<IReadOnlyList<MemberRow>> ListAsync(CancellationToken ct)
        => QueryAsync($"SELECT {Columns} FROM members;", _ => { }, ct);

    /// <summary>
    /// Every enabled row — what the fan-out target provider, the liveness poller and gossip read. A
    /// disabled member receives no traffic and no probe.
    /// </summary>
    public Task<IReadOnlyList<MemberRow>> ListEnabledAsync(CancellationToken ct)
        => QueryAsync($"SELECT {Columns} FROM members WHERE enabled = 1;", _ => { }, ct);

    /// <summary>One row by this member's own local handle, or null.</summary>
    public async Task<MemberRow?> GetAsync(string id, CancellationToken ct)
        => (await QueryAsync($"SELECT {Columns} FROM members WHERE id = $id;",
            c => c.Parameters.AddWithValue("$id", id), ct).ConfigureAwait(false)).FirstOrDefault();

    /// <summary>
    /// One row by the other member's own id — the lookup key for attributing an inbound call, since a
    /// service token's <c>iss</c> is the caller's member id and not this member's local handle on it.
    /// </summary>
    public async Task<MemberRow?> GetByMemberIdAsync(string memberId, CancellationToken ct)
        => (await QueryAsync($"SELECT {Columns} FROM members WHERE member_id = $memberId;",
            c => c.Parameters.AddWithValue("$memberId", memberId), ct).ConfigureAwait(false)).FirstOrDefault();

    /// <summary>Insert or replace one row keyed by its local handle.</summary>
    public Task UpsertAsync(MemberRow row, CancellationToken ct)
        => store.WriteAsync((connection, token) => WriteRowAsync(connection, row, token), ct);

    /// <summary>
    /// Resolve the row for <paramref name="memberId"/>, hand it to <paramref name="build"/> — null when
    /// this member is new to us — and write back what comes out, all under one gate so two introductions
    /// arriving at once converge on a single row instead of both concluding the member is new.
    /// </summary>
    public async Task<MemberRow> UpsertByMemberIdAsync(
        string memberId, Func<MemberRow?, MemberRow> build, CancellationToken ct)
    {
        await _memberIdGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            MemberRow? existing = await GetByMemberIdAsync(memberId, ct).ConfigureAwait(false);
            MemberRow built = build(existing);
            await UpsertAsync(built, ct).ConfigureAwait(false);
            return built;
        }
        finally { _memberIdGate.Release(); }
    }

    /// <summary>
    /// Apply a gossip-converged or failure-timer transition to one row: its state, its incarnation —
    /// which only that member may raise — optionally the addressing and version learned via gossip, and the
    /// transition timestamp. Never touches the first-hand liveness triple, which only a probe writes. A
    /// silent no-op if the row is gone.
    /// </summary>
    public async Task UpdateMembershipAsync(
        string id, string membershipState, long incarnation, DateTimeOffset stateChangedAt,
        IReadOnlyList<MemberCandidate>? candidates, string? apiVersion,
        IReadOnlyDictionary<string, string>? published, CancellationToken ct)
    {
        MemberRow? row = await GetAsync(id, ct).ConfigureAwait(false);
        if (row is null) return;

        MemberRow updated = row with
        {
            MembershipState = membershipState,
            Incarnation = incarnation,
            StateChangedAt = stateChangedAt,
            ApiVersion = string.IsNullOrWhiteSpace(apiVersion) ? row.ApiVersion : apiVersion,
            // Replaced wholesale rather than merged: what a member states about itself is the whole
            // statement, so withdrawing a fact has to be expressible.
            Published = published is null ? row.Published : PublishedFacts.Encode(published),
        };

        if (candidates is { Count: > 0 })
        {
            string merged = MemberCandidates.Merge(row.Candidates, candidates);
            updated = updated with { Candidates = merged };
            // An unverified address follows the offer; a proven one is left alone until a probe says
            // otherwise, so one short gossip round cannot unpin an address this member knows works.
            if (!row.AddressVerified)
                updated = updated with { Url = MemberCandidates.Best(MemberCandidates.Decode(merged)) };
        }

        await UpsertAsync(updated, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Pin the address a member has just answered on, and fold the candidates it offered into what is
    /// already held. This is the only thing that marks an address verified: until a probe has come back as
    /// the right member, the row carries a claim. A silent no-op if the row is gone.
    /// </summary>
    public async Task PinAddressAsync(
        string id, string address, IReadOnlyList<MemberCandidate>? offered, CancellationToken ct)
    {
        MemberRow? row = await GetAsync(id, ct).ConfigureAwait(false);
        if (row is null) return;

        // The address that answered leads the list: it is the one thing here that has been proven, so it is
        // what a later cold start tries first. Whether a BROWSER can also use it is a separate claim this
        // probe says nothing about, so the flag is carried over from whoever offered the address rather
        // than assumed — an address of unknown kind stays member-only, and the roster reports no browser
        // address rather than handing one out that cannot work.
        string merged = MemberCandidates.Merge(row.Candidates, offered);
        bool client = MemberCandidates.Decode(merged)
            .Any(c => c.Client && string.Equals(c.Url, address, StringComparison.OrdinalIgnoreCase));

        await UpsertAsync(row with
        {
            Candidates = MemberCandidates.Merge(merged, [new MemberCandidate(address, client)]),
            Url = address,
            AddressVerified = true,
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Record first-hand liveness for a member that authenticated an inbound call: stamp its last-seen and,
    /// if it was not already, promote it to alive. Leaves the first-hand status alone — that axis is
    /// strictly this member's own outbound probe, and hearing <em>from</em> somebody is not the same as
    /// reaching them. A no-op if no row carries that member id yet.
    /// </summary>
    public async Task RecordAliveContactAsync(string memberId, DateTimeOffset now, CancellationToken ct)
    {
        MemberRow? row = await GetByMemberIdAsync(memberId, ct).ConfigureAwait(false);
        if (row is null) return;
        await UpsertAsync(row with
        {
            MembershipState = GossipState.Alive,
            StateChangedAt = row.MembershipState == GossipState.Alive ? row.StateChangedAt : now,
            LastSeen = now,
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Promote one row to alive — the first-hand authentication the poller applies when it directly reaches
    /// a member that gossip had only reported, or that had gone suspect or dead. Optionally records the
    /// route version the probe read. A no-op if the row is gone. Distinct from
    /// <see cref="UpdateLivenessAsync"/>, which writes only the first-hand triple: this writes the converged
    /// axis.
    /// </summary>
    public async Task PromoteAliveAsync(string id, string? apiVersion, DateTimeOffset now, CancellationToken ct)
    {
        MemberRow? row = await GetAsync(id, ct).ConfigureAwait(false);
        if (row is null) return;
        await UpsertAsync(row with
        {
            MembershipState = GossipState.Alive,
            StateChangedAt = row.MembershipState == GossipState.Alive ? row.StateChangedAt : now,
            ApiVersion = string.IsNullOrWhiteSpace(apiVersion) ? row.ApiVersion : apiVersion,
        }, ct).ConfigureAwait(false);
    }

    /// <summary>Write one liveness sample onto a row. A no-op if the row was disabled or removed mid-poll.</summary>
    public async Task UpdateLivenessAsync(
        string id, string status, int? latencyMs, DateTimeOffset? lastSeen, CancellationToken ct)
    {
        MemberRow? row = await GetAsync(id, ct).ConfigureAwait(false);
        if (row is null) return;
        await UpsertAsync(row with { Status = status, LatencyMs = latencyMs, LastSeen = lastSeen }, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Flip a row's enabled flag — the sole local override to the shared-secret trust boundary. The row
    /// itself is never removed by this, so a disabled member can be re-enabled with one click. False if the
    /// row does not exist.
    /// </summary>
    public async Task<bool> SetEnabledAsync(string id, bool enabled, CancellationToken ct)
    {
        MemberRow? row = await GetAsync(id, ct).ConfigureAwait(false);
        if (row is null) return false;
        await UpsertAsync(row with { Enabled = enabled }, ct).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Record that a member has left the cluster, so the departure travels instead of the row simply
    /// being absent. Sets the terminal <see cref="GossipState.Left"/> one incarnation above what the
    /// member last claimed, so it supersedes the alive every other member is holding, and the failure
    /// timers reap the row on each of them once the reap window passes.
    /// </summary>
    /// <remarks>
    /// <b>Deleting the row instead does not remove anybody.</b> Anti-entropy exists to repair a roster
    /// that is missing something, so an absence is re-learned from the first member that still holds it.
    /// A departure has to be a state that supersedes.
    /// <para>
    /// <b>A member that is still running will refute this and return.</b> That is the refutation channel
    /// working as designed: only a member may raise its own incarnation, and it re-asserts alive above
    /// whatever was said about it, which is what stops a live member being buried by a false report.
    /// Removing a member that is still participating is therefore a request the mesh will overturn —
    /// stop it first, or disable it, which is local and absolute and no gossip undoes.
    /// </para>
    /// </remarks>
    public async Task<bool> MarkLeftAsync(string id, DateTimeOffset now, CancellationToken ct)
    {
        MemberRow? row = await GetAsync(id, ct).ConfigureAwait(false);
        if (row is null) return false;
        await UpsertAsync(row with
        {
            MembershipState = GossipState.Left,
            Incarnation = row.Incarnation + 1,
            StateChangedAt = now,
        }, ct).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Drop a row outright. This is the reaper's primitive — a row already terminal for longer than the
    /// reap window — and it is not how a member is removed: see <see cref="MarkLeftAsync"/> for why a
    /// deletion alone is undone by the next gossip round. False if the row does not exist.
    /// </summary>
    public async Task<bool> DeleteAsync(string id, CancellationToken ct)
    {
        int deleted = await store.WriteAsync(async (connection, token) =>
        {
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "DELETE FROM members WHERE id = $id;";
            command.Parameters.AddWithValue("$id", id);
            return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
        return deleted > 0;
    }

    private static async Task WriteRowAsync(SqliteConnection connection, MemberRow row, CancellationToken ct)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            INSERT OR REPLACE INTO members
                (id, member_id, kind, url, candidates, address_verified, nickname, incarnation, status,
                 membership_state, state_changed_at, latency_ms, last_seen, api_version, published, enabled)
            VALUES ($id, $memberId, $kind, $url, $candidates, $addressVerified, $nickname, $incarnation,
                    $status, $membershipState, $stateChangedAt, $latencyMs, $lastSeen, $apiVersion,
                    $published, $enabled);
            """;
        command.Parameters.AddWithValue("$id", row.Id);
        command.Parameters.AddWithValue("$memberId", row.MemberId);
        command.Parameters.AddWithValue("$kind", row.Kind);
        command.Parameters.AddWithValue("$url", row.Url);
        command.Parameters.AddWithValue("$candidates", row.Candidates);
        command.Parameters.AddWithValue("$addressVerified", row.AddressVerified ? 1 : 0);
        SqliteValues.Bind(command, "$nickname", row.Nickname);
        command.Parameters.AddWithValue("$incarnation", row.Incarnation);
        command.Parameters.AddWithValue("$status", row.Status);
        command.Parameters.AddWithValue("$membershipState", row.MembershipState);
        SqliteValues.Bind(command, "$stateChangedAt",
            row.StateChangedAt is { } changed ? SqliteValues.Stamp(changed) : null);
        SqliteValues.Bind(command, "$latencyMs", row.LatencyMs);
        SqliteValues.Bind(command, "$lastSeen", row.LastSeen is { } seen ? SqliteValues.Stamp(seen) : null);
        command.Parameters.AddWithValue("$apiVersion", row.ApiVersion);
        command.Parameters.AddWithValue("$published", row.Published);
        command.Parameters.AddWithValue("$enabled", row.Enabled ? 1 : 0);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<MemberRow>> QueryAsync(
        string sql, Action<SqliteCommand> bind, CancellationToken ct)
    {
        await using SqliteConnection connection = await store.OpenAsync(ct).ConfigureAwait(false);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        bind(command);

        var rows = new List<MemberRow>();
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new MemberRow(
                Id: reader.GetString(0),
                MemberId: reader.GetString(1),
                Kind: reader.GetString(2),
                Url: reader.GetString(3),
                Candidates: reader.GetString(4),
                AddressVerified: reader.GetInt32(5) != 0,
                Nickname: SqliteValues.ReadStringOrNull(reader, 6),
                Incarnation: reader.GetInt64(7),
                Status: reader.GetString(8),
                MembershipState: reader.GetString(9),
                StateChangedAt: SqliteValues.ReadStampOrNull(reader, 10),
                LatencyMs: reader.IsDBNull(11) ? null : reader.GetInt32(11),
                LastSeen: SqliteValues.ReadStampOrNull(reader, 12),
                ApiVersion: reader.GetString(13),
                Published: reader.GetString(14),
                Enabled: reader.GetInt32(15) != 0));
        }
        return rows;
    }
}
