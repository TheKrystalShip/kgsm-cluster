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
/// </remarks>
public sealed class ClusterStore : IHostedService
{
    private readonly string _connectionString;
    private readonly ILogger<ClusterStore> _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private bool _ensured;

    public ClusterStore(ClusterOptions options, ILogger<ClusterStore> logger)
    {
        _logger = logger;
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
    /// Create every table the package owns, if it is not already there. Idempotent and cheap: a member
    /// that has run before pays one no-op statement per table at startup and nothing thereafter.
    /// </summary>
    private async Task EnsureSchemaAsync(CancellationToken ct)
    {
        if (_ensured) return;
        await _ensureGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_ensured) return;
            await using var connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = Schema;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            _ensured = true;
        }
        finally { _ensureGate.Release(); }
    }

    /// <summary>
    /// The package's schema. Every timestamp is stored as an ISO-8601 UTC string, which sorts
    /// lexically in the same order it sorts chronologically, so the drainer's due-scan and the GC's
    /// cutoff are plain string comparisons.
    /// </summary>
    private const string Schema = """
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

        CREATE INDEX IF NOT EXISTS ix_outbox_due ON outbox (status, next_attempt_at);

        CREATE TABLE IF NOT EXISTS inbox (
            id           TEXT PRIMARY KEY,
            from_id      TEXT NOT NULL,
            type         TEXT NOT NULL,
            received_at  TEXT NOT NULL,
            processed_at TEXT NULL
        );

        CREATE INDEX IF NOT EXISTS ix_inbox_received ON inbox (received_at);

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
            enabled          INTEGER NOT NULL
        );

        -- A member id identifies a member; two rows carrying one are the same member counted twice. The
        -- unique index makes the duplicate impossible rather than merely unlikely, which is what lets a
        -- simultaneous mutual introduction converge on one row.
        CREATE UNIQUE INDEX IF NOT EXISTS ix_members_member_id ON members (member_id);
        CREATE INDEX IF NOT EXISTS ix_members_enabled ON members (enabled);

        CREATE TABLE IF NOT EXISTS self_facts (
            id         TEXT PRIMARY KEY,
            kind       TEXT NOT NULL,
            value      TEXT NOT NULL,
            client     INTEGER NOT NULL,
            provenance TEXT NOT NULL,
            last_seen  TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS ix_self_facts_kind ON self_facts (kind);
        """;
}
