using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TheKrystalShip.KGSM.Cluster.Storage;

/// <summary>
/// The one SQLite file behind both halves of the package, and the only thing in it that knows SQL
/// connection management. Membership and messaging each own their own tables and read/write them
/// through their own stores; this class owns the file, the schema, and the write gate that serializes
/// every writer against SQLite's single-writer model.
/// </summary>
/// <remarks>
/// <para>
/// <b>One file per member, never per machine.</b> The path comes from <see cref="ClusterOptions.StorePath"/>,
/// which a member points at its own <c>StateDirectory=</c>. Two members on one machine therefore hold
/// two independent files: neither one's roster, outbox or inbox depends on the other's process.
/// </para>
/// <para>
/// <b>Registered as a hosted service</b> so the schema is created once at startup, before any endpoint
/// or worker can reach a table. Every operation also awaits <see cref="EnsureSchemaAsync"/> itself, so a
/// call arriving before startup has run — or from a test host that never starts hosted services — still
/// works.
/// </para>
/// <para>
/// <b>The file belongs to one cluster: the one whose secret it was written under.</b> Its roster, its
/// capability assignments and its queued messages were all learned from that cluster's members, so a
/// member given a different secret opens a file describing a cluster it is no longer in. Carried into
/// the new one, that state is gossiped as current — an old assignment of the accounts competes with
/// the real holder, and the tie-break can hand it the new cluster's accounts. So the file records the
/// secret's fingerprint, and opening it under a different secret discards everything learned from the
/// old cluster before anything reads it. A rotation is not a different cluster: a file recorded under
/// <see cref="ClusterOptions.SecretPrevious"/> is carried over.
/// </para>
/// </remarks>
public sealed class ClusterStore : IHostedService
{
    private readonly string _connectionString;
    private readonly string _secret;
    private readonly string _secretPrevious;
    private readonly ILogger<ClusterStore> _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private bool _ensured;

    public ClusterStore(ClusterOptions options, ILogger<ClusterStore> logger)
    {
        _logger = logger;
        _secret = options.Secret;
        _secretPrevious = options.SecretPrevious;
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = options.StorePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            // Shared across the connections one member opens per operation, so a reader started while a
            // writer holds the file waits rather than failing outright.
            Cache = SqliteCacheMode.Shared,
        }.ToString();
    }

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken ct)
    {
        await EnsureSchemaAsync(ct).ConfigureAwait(false);
        _logger.LogInformation("Cluster store ready.");
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;

    /// <summary>
    /// Open a connection to the member's cluster file, with the schema guaranteed to exist. The caller
    /// owns the connection and disposes it; connections are pooled, so opening one per operation is the
    /// intended usage rather than an expense to avoid.
    /// </summary>
    public async Task<SqliteConnection> OpenAsync(CancellationToken ct)
    {
        await EnsureSchemaAsync(ct).ConfigureAwait(false);
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    /// Run <paramref name="write"/> against an open connection while holding the file's write gate.
    /// SQLite takes one writer at a time; serializing here turns what would surface as an intermittent
    /// "database is locked" into a wait.
    /// </summary>
    public async Task<T> WriteAsync<T>(Func<SqliteConnection, CancellationToken, Task<T>> write, CancellationToken ct)
    {
        await _writeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await using SqliteConnection connection = await OpenAsync(ct).ConfigureAwait(false);
            return await write(connection, ct).ConfigureAwait(false);
        }
        finally { _writeGate.Release(); }
    }

    /// <inheritdoc cref="WriteAsync{T}"/>
    public async Task WriteAsync(Func<SqliteConnection, CancellationToken, Task> write, CancellationToken ct)
        => await WriteAsync<bool>(async (c, token) =>
        {
            await write(c, token).ConfigureAwait(false);
            return true;
        }, ct).ConfigureAwait(false);

    /// <summary>
    /// Bring the file to the shape this build needs, and refuse to run against one that cannot be
    /// brought there.
    /// </summary>
    /// <remarks>
    /// <b><c>CREATE TABLE IF NOT EXISTS</c> creates; it does not alter.</b> A member that has run before
    /// has every table already, so a column added in a later build never appears on it and every query
    /// naming that column fails. The failure is the worst shape available: the member starts, serves, and
    /// answers health, while its gossip and liveness die once per tick in a log nobody reads — joined,
    /// reachable, and not in the mesh at all. So an added column is applied to the table that exists, and
    /// what cannot be applied stops the member rather than being carried on past.
    /// </remarks>
    private async Task EnsureSchemaAsync(CancellationToken ct)
    {
        if (_ensured) return;
        await _ensureGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_ensured) return;
            await using var connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ExecuteAsync(connection, TableSchema, ct).ConfigureAwait(false);
            await ApplyAddedColumnsAsync(connection, ct).ConfigureAwait(false);
            await VerifyAsync(connection, ct).ConfigureAwait(false);
            // Only now: an index over a column the store predates cannot be created, and trying before
            // the column is added is what turns an upgradeable store into an unopenable one.
            await ExecuteAsync(connection, IndexSchema, ct).ConfigureAwait(false);
            await BindToSecretAsync(connection, ct).ConfigureAwait(false);
            _ensured = true;
        }
        finally { _ensureGate.Release(); }
    }

    /// <summary>
    /// Columns added to a table after it first shipped. A member that predates one has the table but not
    /// the column, so each is applied to what is already there. Every entry carries a default, because a
    /// table with rows in it cannot gain a column that has none.
    /// <para>
    /// Append here when adding a column; never edit an entry. What is written here has already run on
    /// somebody's disk.
    /// </para>
    /// </summary>
    private static readonly (string Table, string Column, string Definition)[] AddedColumns =
    [
        ("members", "published", "TEXT NOT NULL DEFAULT ''"),
    ];

    /// <summary>What every table must have for this build's queries to run. Checked after the additions
    /// above, so a shape that cannot be repaired is refused rather than discovered one query at a
    /// time.</summary>
    private static readonly (string Table, string[] Columns)[] RequiredColumns =
    [
        ("outbox", ["id", "message_id", "target_id", "target_url", "type", "payload", "status", "attempts",
            "next_attempt_at", "created_at", "delivered_at", "last_error"]),
        ("inbox", ["id", "from_id", "type", "received_at", "processed_at"]),
        ("members", ["id", "member_id", "kind", "url", "candidates", "address_verified", "nickname",
            "incarnation", "status", "membership_state", "state_changed_at", "latency_ms", "last_seen",
            "api_version", "published", "enabled"]),
        ("self_facts", ["id", "kind", "value", "client", "provenance", "last_seen"]),
        ("cluster_state", ["capability", "member_id", "version", "set_by"]),
        ("store_meta", ["key", "value"]),
    ];

    private static async Task ExecuteAsync(SqliteConnection connection, string sql, CancellationToken ct)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task ApplyAddedColumnsAsync(SqliteConnection connection, CancellationToken ct)
    {
        foreach ((string table, string column, string definition) in AddedColumns)
        {
            IReadOnlySet<string> present = await ColumnsAsync(connection, table, ct).ConfigureAwait(false);
            if (present.Count == 0 || present.Contains(column)) continue;

            await using SqliteCommand alter = connection.CreateCommand();
            // Concatenated from constants above, never from anything a caller supplies.
            alter.CommandText = "ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition + ";";
            await alter.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            _logger.LogInformation("Cluster store: added '{Column}' to '{Table}'.", column, table);
        }
    }

    private async Task VerifyAsync(SqliteConnection connection, CancellationToken ct)
    {
        var missing = new List<string>();
        foreach ((string table, string[] columns) in RequiredColumns)
        {
            IReadOnlySet<string> present = await ColumnsAsync(connection, table, ct).ConfigureAwait(false);
            missing.AddRange(columns.Where(c => !present.Contains(c)).Select(c => $"{table}.{c}"));
        }

        if (missing.Count == 0) return;

        // Stopping here is the point. Carrying on gives a member that starts, serves, answers health, and
        // is silently not in the mesh — which is the failure this check exists to convert into a refusal.
        throw new InvalidOperationException(
            $"The cluster store is missing {string.Join(", ", missing)} and cannot be brought to the shape " +
            "this build needs. It holds only membership and queued messages, both of which re-converge, so " +
            "the repair is to delete the file and let the member re-join.");
    }

    /// <summary>The tables holding what a member learned from its cluster, as opposed to about itself.</summary>
    /// <remarks>
    /// <c>self_facts</c> is not among them: the addresses a member has for itself describe the network
    /// it sits on, which does not change with the cluster it is in.
    /// </remarks>
    private static readonly string[] LearnedFromTheCluster = ["members", "cluster_state", "outbox", "inbox"];

    private const string SecretFingerprintKey = "secret_fingerprint";

    /// <summary>
    /// Record which cluster this file belongs to, and empty it of another cluster's state.
    /// </summary>
    private async Task BindToSecretAsync(SqliteConnection connection, CancellationToken ct)
    {
        // A member with no secret is in no cluster, and there is nothing to bind the file to.
        if (string.IsNullOrWhiteSpace(_secret))
            return;

        string current = ClusterFounding.Fingerprint(_secret);
        string? recorded = await ReadMetaAsync(connection, SecretFingerprintKey, ct).ConfigureAwait(false);

        if (recorded == current)
            return;

        bool rotated = !string.IsNullOrWhiteSpace(_secretPrevious)
            && recorded == ClusterFounding.Fingerprint(_secretPrevious);

        await using SqliteTransaction tx = (SqliteTransaction)await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        // Recorded with nothing discarded when there is no fingerprint yet: a file that predates the
        // record cannot say which cluster it was written under, and the member is far likelier to be
        // upgrading in place than to have changed its secret in the same restart.
        if (recorded is not null && !rotated)
        {
            foreach (string table in LearnedFromTheCluster)
            {
                await using SqliteCommand clear = connection.CreateCommand();
                clear.Transaction = tx;
                // Concatenated from constants above, never from anything a caller supplies.
                clear.CommandText = "DELETE FROM " + table + ";";
                await clear.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }

        await using (SqliteCommand record = connection.CreateCommand())
        {
            record.Transaction = tx;
            record.CommandText = """
                INSERT INTO store_meta (key, value) VALUES ($key, $value)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value;
                """;
            record.Parameters.AddWithValue("$key", SecretFingerprintKey);
            record.Parameters.AddWithValue("$value", current);
            await record.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await tx.CommitAsync(ct).ConfigureAwait(false);

        if (recorded is not null && !rotated)
        {
            _logger.LogWarning(
                "Cluster store: it held the state of a cluster whose secret this member no longer holds. "
                + "Discarded the roster, the capability assignments and the queued messages learned there; "
                + "this member knows nobody until it is added to its new cluster.");
        }
    }

    private static async Task<string?> ReadMetaAsync(SqliteConnection connection, string key, CancellationToken ct)
    {
        await using SqliteCommand read = connection.CreateCommand();
        read.CommandText = "SELECT value FROM store_meta WHERE key = $key;";
        read.Parameters.AddWithValue("$key", key);
        return await read.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
    }

    private static async Task<IReadOnlySet<string>> ColumnsAsync(
        SqliteConnection connection, string table, CancellationToken ct)
    {
        await using SqliteCommand probe = connection.CreateCommand();
        probe.CommandText = $"PRAGMA table_info({table});";
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using SqliteDataReader reader = await probe.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            columns.Add(reader.GetString(1));
        return columns;
    }

    /// <summary>
    /// The package's schema. Every timestamp is stored as an ISO-8601 UTC string, which sorts
    /// lexically in the same order it sorts chronologically, so the drainer's due-scan and the GC's
    /// cutoff are plain string comparisons.
    /// </summary>
    private const string TableSchema = """
        CREATE TABLE IF NOT EXISTS outbox (
            id              TEXT PRIMARY KEY,
            message_id      TEXT NOT NULL,
            target_id       TEXT NOT NULL,
            target_url      TEXT NOT NULL,
            type            TEXT NOT NULL,
            payload         TEXT NOT NULL,
            status          TEXT NOT NULL,
            attempts        INTEGER NOT NULL,
            next_attempt_at TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            delivered_at    TEXT NULL,
            last_error      TEXT NULL
        );


        CREATE TABLE IF NOT EXISTS inbox (
            id           TEXT PRIMARY KEY,
            from_id      TEXT NOT NULL,
            type         TEXT NOT NULL,
            received_at  TEXT NOT NULL,
            processed_at TEXT NULL
        );


        CREATE TABLE IF NOT EXISTS members (
            id               TEXT PRIMARY KEY,
            member_id        TEXT NOT NULL,
            kind             TEXT NOT NULL,
            url              TEXT NOT NULL,
            candidates       TEXT NOT NULL,
            address_verified INTEGER NOT NULL,
            nickname         TEXT NULL,
            incarnation      INTEGER NOT NULL,
            status           TEXT NOT NULL,
            membership_state TEXT NOT NULL,
            state_changed_at TEXT NULL,
            latency_ms       INTEGER NULL,
            last_seen        TEXT NULL,
            api_version      TEXT NOT NULL,
            published        TEXT NOT NULL DEFAULT '',
            enabled          INTEGER NOT NULL
        );

        -- A member id identifies a member; two rows carrying one are the same member counted twice. The
        -- unique index makes the duplicate impossible rather than merely unlikely, which is what lets a
        -- simultaneous mutual introduction converge on one row.

        CREATE TABLE IF NOT EXISTS self_facts (
            id         TEXT PRIMARY KEY,
            kind       TEXT NOT NULL,
            value      TEXT NOT NULL,
            client     INTEGER NOT NULL,
            provenance TEXT NOT NULL,
            last_seen  TEXT NOT NULL
        );


        CREATE TABLE IF NOT EXISTS cluster_state (
            capability TEXT PRIMARY KEY,
            member_id  TEXT NOT NULL,
            version    INTEGER NOT NULL,
            set_by     TEXT NOT NULL
        );

        -- What the file itself is: which cluster's secret it was written under.

        CREATE TABLE IF NOT EXISTS store_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """;

    /// <summary>
    /// The indexes, created after the columns they name are known to exist. An index over a column a
    /// store predates cannot be created, and attempting it before the column is added is what turns an
    /// upgradeable store into an unopenable one.
    /// </summary>
    private const string IndexSchema = """
        CREATE INDEX IF NOT EXISTS ix_outbox_due ON outbox (status, next_attempt_at);
        CREATE INDEX IF NOT EXISTS ix_inbox_received ON inbox (received_at);
        CREATE UNIQUE INDEX IF NOT EXISTS ix_members_member_id ON members (member_id);
        CREATE INDEX IF NOT EXISTS ix_members_enabled ON members (enabled);
        CREATE INDEX IF NOT EXISTS ix_self_facts_kind ON self_facts (kind);
        """;
}
