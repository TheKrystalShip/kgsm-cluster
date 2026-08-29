using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>
/// The single reader and writer of the outbox table, and the prune half of the retention sweep.
/// Registered as both <see cref="IClusterBus"/> — the public send seam — and its concrete type, which
/// the drainer and the GC worker use for the store-side helpers that are not part of that seam.
/// </summary>
public sealed class ClusterBus(ClusterStore store, ILogger<ClusterBus> logger) : IClusterBus
{
    /// <inheritdoc/>
    public Task EnqueueAsync<T>(
        string type, T payload, JsonTypeInfo<T> payloadTypeInfo,
        IEnumerable<ClusterTarget> targets, CancellationToken ct)
        => EnqueueJsonAsync(type, JsonSerializer.Serialize(payload, payloadTypeInfo), targets, ct);

    /// <inheritdoc/>
    public async Task EnqueueJsonAsync(
        string type, string payloadJson, IEnumerable<ClusterTarget> targets, CancellationToken ct)
    {
        List<ClusterTarget> targetList = targets as List<ClusterTarget> ?? targets.ToList();
        if (targetList.Count == 0)
            return;

        string messageId = Guid.NewGuid().ToString("N");
        DateTimeOffset now = DateTimeOffset.UtcNow;

        await store.WriteAsync(async (connection, token) =>
        {
            foreach (ClusterTarget target in targetList)
            {
                await using SqliteCommand command = connection.CreateCommand();
                command.CommandText = """
                    INSERT OR REPLACE INTO outbox
                        (id, message_id, target_id, target_url, type, payload, status, attempts,
                         next_attempt_at, created_at, delivered_at, last_error)
                    VALUES ($id, $messageId, $targetId, $targetUrl, $type, $payload, $status, 0,
                            $now, $now, NULL, NULL);
                    """;
                command.Parameters.AddWithValue("$id", $"{messageId}:{target.MemberId}");
                command.Parameters.AddWithValue("$messageId", messageId);
                command.Parameters.AddWithValue("$targetId", target.MemberId);
                command.Parameters.AddWithValue("$targetUrl", target.Url);
                command.Parameters.AddWithValue("$type", type);
                command.Parameters.AddWithValue("$payload", payloadJson);
                command.Parameters.AddWithValue("$status", OutboxStatus.Pending);
                command.Parameters.AddWithValue("$now", SqliteValues.Stamp(now));
                await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
        }, ct).ConfigureAwait(false);

        logger.LogDebug(
            "cluster outbox: enqueued {MessageId} (type={Type}) to {Count} target(s)",
            messageId, type, targetList.Count);
    }

    /// <summary>
    /// The drainer's per-tick due-scan: every pending row whose next attempt has passed, oldest first,
    /// capped so one tick can never balloon.
    /// </summary>
    public async Task<IReadOnlyList<OutboxRow>> ListDueAsync(DateTimeOffset now, int max, CancellationToken ct)
    {
        await using SqliteConnection connection = await store.OpenAsync(ct).ConfigureAwait(false);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT id, message_id, target_id, target_url, type, payload, status, attempts,
                   next_attempt_at, created_at, delivered_at, last_error
            FROM outbox
            WHERE status = $pending AND next_attempt_at <= $now
            ORDER BY created_at
            LIMIT $max;
            """;
        command.Parameters.AddWithValue("$pending", OutboxStatus.Pending);
        command.Parameters.AddWithValue("$now", SqliteValues.Stamp(now));
        command.Parameters.AddWithValue("$max", max);

        var rows = new List<OutboxRow>();
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new OutboxRow(
                reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                reader.GetString(4), reader.GetString(5), reader.GetString(6), reader.GetInt32(7),
                SqliteValues.ReadStamp(reader, 8), SqliteValues.ReadStamp(reader, 9),
                SqliteValues.ReadStampOrNull(reader, 10), SqliteValues.ReadStringOrNull(reader, 11)));
        }
        return rows;
    }

    /// <summary>Mark a row delivered. A no-op if the row is gone — the drainer never throws over a row
    /// pruned out from under it.</summary>
    public Task MarkDeliveredAsync(string id, DateTimeOffset now, CancellationToken ct)
        => ExecuteAsync(
            """
            UPDATE outbox SET status = $status, delivered_at = $now WHERE id = $id;
            """,
            command =>
            {
                command.Parameters.AddWithValue("$status", OutboxStatus.Delivered);
                command.Parameters.AddWithValue("$now", SqliteValues.Stamp(now));
                command.Parameters.AddWithValue("$id", id);
            }, ct);

    /// <summary>Record a transient delivery failure: bump the attempt count, push the next attempt out
    /// by the caller's computed backoff, note the error. The row stays pending and keeps retrying.</summary>
    public Task MarkTransientFailureAsync(
        string id, int attempts, DateTimeOffset nextAttemptAt, string error, CancellationToken ct)
        => ExecuteAsync(
            """
            UPDATE outbox SET attempts = $attempts, next_attempt_at = $next, last_error = $error
            WHERE id = $id;
            """,
            command =>
            {
                command.Parameters.AddWithValue("$attempts", attempts);
                command.Parameters.AddWithValue("$next", SqliteValues.Stamp(nextAttemptAt));
                command.Parameters.AddWithValue("$error", error);
                command.Parameters.AddWithValue("$id", id);
            }, ct);

    /// <summary>Dead-letter a row — it is never retried again. The reason lands in the row for
    /// diagnostics; the caller logs it loudly as well.</summary>
    public Task MarkDeadAsync(string id, string reason, CancellationToken ct)
        => ExecuteAsync(
            """
            UPDATE outbox SET status = $status, last_error = $reason WHERE id = $id;
            """,
            command =>
            {
                command.Parameters.AddWithValue("$status", OutboxStatus.Dead);
                command.Parameters.AddWithValue("$reason", reason);
                command.Parameters.AddWithValue("$id", id);
            }, ct);

    /// <summary>
    /// The retention sweep: delete delivered and dead outbox rows, and inbox ledger rows, older than
    /// the cutoff. The window exceeds the retry TTL (<see cref="ClusterOptions.Validate"/> enforces it),
    /// so a late redelivery of a long-retried message is still recognized as a duplicate. Returns the
    /// total rows deleted across both tables.
    /// </summary>
    public async Task<int> PruneAsync(DateTimeOffset cutoff, CancellationToken ct)
    {
        string stamp = SqliteValues.Stamp(cutoff);
        return await store.WriteAsync(async (connection, token) =>
        {
            int outboxDeleted;
            await using (SqliteCommand command = connection.CreateCommand())
            {
                command.CommandText = """
                    DELETE FROM outbox WHERE status IN ($delivered, $dead) AND created_at < $cutoff;
                    """;
                command.Parameters.AddWithValue("$delivered", OutboxStatus.Delivered);
                command.Parameters.AddWithValue("$dead", OutboxStatus.Dead);
                command.Parameters.AddWithValue("$cutoff", stamp);
                outboxDeleted = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            int inboxDeleted;
            await using (SqliteCommand command = connection.CreateCommand())
            {
                command.CommandText = "DELETE FROM inbox WHERE received_at < $cutoff;";
                command.Parameters.AddWithValue("$cutoff", stamp);
                inboxDeleted = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            logger.LogDebug(
                "cluster bus GC: pruned {Outbox} outbox row(s), {Inbox} inbox row(s)",
                outboxDeleted, inboxDeleted);
            return outboxDeleted + inboxDeleted;
        }, ct).ConfigureAwait(false);
    }

    private Task ExecuteAsync(string sql, Action<SqliteCommand> bind, CancellationToken ct)
        => store.WriteAsync(async (connection, token) =>
        {
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = sql;
            bind(command);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct);
}
