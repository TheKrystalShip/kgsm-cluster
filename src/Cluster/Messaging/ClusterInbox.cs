using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Messaging;

/// <summary>The outcome of <see cref="ClusterInbox.ReceiveAsync"/>. Every value except
/// <see cref="TransientFailure"/> answers <c>200</c> and the sender stops retrying;
/// <see cref="TransientFailure"/> alone answers <c>500</c> and it keeps retrying.</summary>
public enum InboxResult
{
    /// <summary>Freshly dispatched and recorded — the handler ran and succeeded for the first time.</summary>
    Applied,

    /// <summary>A ledger row with this envelope id already existed, so the handler was not run again.</summary>
    Duplicate,

    /// <summary>No registered handler answers this envelope's type. Recorded, so a retransmit is
    /// recognized as a duplicate rather than evaluated again, and dropped: a newer member may
    /// legitimately know a type this one does not, and dropping is what keeps the sender's queue
    /// moving.</summary>
    DroppedUnknown,

    /// <summary>The handler threw, which is taken as transient. Nothing was recorded, so the next
    /// redelivery runs the idempotent handler again from scratch.</summary>
    TransientFailure,
}

/// <summary>
/// The bus's receive, dedupe and dispatch algorithm.
/// </summary>
/// <remarks>
/// <para>
/// <b>The handler runs first, and the ledger row is written only after it returns.</b> The alternative
/// — insert the ledger row, then dispatch, and roll both back together on failure — assumes both land
/// in one transaction. They cannot: a handler's effect lands in a completely different store from this
/// ledger, so no single transaction could ever roll both back. Insert-first would risk marking an
/// envelope processed when the handler that was meant to run for it never completed.
/// </para>
/// <para>
/// Handler-first is safe rather than merely convenient, precisely because every handler is
/// contractually idempotent. If the process dies between the handler succeeding and the row
/// committing, the next redelivery runs the handler again and the effect lands once. That is what the
/// two idempotency layers are for: the ledger is the fast-path dedupe, the idempotent handler is the
/// backstop for the crash window between the two steps.
/// </para>
/// </remarks>
public sealed class ClusterInbox
{
    private readonly ClusterStore _store;
    private readonly ILogger<ClusterInbox> _logger;
    private readonly SemaphoreSlim _receiveGate = new(1, 1);
    private readonly IReadOnlyDictionary<string, IClusterMessageHandler> _handlers;

    public ClusterInbox(
        ClusterStore store,
        IEnumerable<IClusterMessageHandler> handlers,
        ILogger<ClusterInbox> logger)
    {
        _store = store;
        _logger = logger;
        _handlers = handlers.ToDictionary(h => h.Type, StringComparer.Ordinal);
    }

    /// <summary>
    /// Receive one already-authenticated, already-validated envelope: the endpoint has checked the
    /// service token, the member gate, and that <c>from</c> matches the token's <c>iss</c> before
    /// calling this. The whole dedupe-check, dispatch and record sequence for one envelope runs
    /// serialized against every other envelope this member is receiving, which is what makes a
    /// duplicate arriving twice at once safe without a cross-store transaction.
    /// </summary>
    public async Task<InboxResult> ReceiveAsync(ClusterEnvelope envelope, CancellationToken ct)
    {
        await _receiveGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (await AlreadySeenAsync(envelope.Id, ct).ConfigureAwait(false))
            {
                _logger.LogDebug(
                    "cluster inbox: duplicate envelope {Id} (type={Type} from={From}) — acked without re-dispatch",
                    envelope.Id, envelope.Type, envelope.From);
                return InboxResult.Duplicate;
            }

            if (!_handlers.TryGetValue(envelope.Type, out IClusterMessageHandler? handler))
            {
                _logger.LogWarning(
                    "cluster inbox: unknown message type {Type} (id={Id} from={From}) — dropped, acked 200",
                    envelope.Type, envelope.Id, envelope.From);
                // Recorded so a retransmit of this same envelope is a duplicate rather than logged and
                // dropped on every retry. It is never marked processed: there was no handler to succeed.
                await RecordAsync(envelope, processed: false, ct).ConfigureAwait(false);
                return InboxResult.DroppedUnknown;
            }

            try
            {
                await handler.HandleAsync(envelope, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "cluster inbox: transient failure dispatching {Type} (id={Id} from={From}) — not recorded, " +
                    "the sender retries",
                    envelope.Type, envelope.Id, envelope.From);
                return InboxResult.TransientFailure;
            }

            await RecordAsync(envelope, processed: true, ct).ConfigureAwait(false);
            return InboxResult.Applied;
        }
        finally { _receiveGate.Release(); }
    }

    private async Task<bool> AlreadySeenAsync(string id, CancellationToken ct)
    {
        await using SqliteConnection connection = await _store.OpenAsync(ct).ConfigureAwait(false);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM inbox WHERE id = $id LIMIT 1;";
        command.Parameters.AddWithValue("$id", id);
        return await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not null;
    }

    private Task RecordAsync(ClusterEnvelope envelope, bool processed, CancellationToken ct)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        return _store.WriteAsync(async (connection, token) =>
        {
            await using SqliteCommand command = connection.CreateCommand();
            // INSERT OR IGNORE rather than a plain INSERT: a second instance of this class delivering
            // the same envelope concurrently would otherwise fail the key. Whichever attempt's row lands
            // first owns completion, and the effect is idempotent either way.
            command.CommandText = """
                INSERT OR IGNORE INTO inbox (id, from_id, type, received_at, processed_at)
                VALUES ($id, $from, $type, $now, $processed);
                """;
            command.Parameters.AddWithValue("$id", envelope.Id);
            command.Parameters.AddWithValue("$from", envelope.From);
            command.Parameters.AddWithValue("$type", envelope.Type);
            command.Parameters.AddWithValue("$now", SqliteValues.Stamp(now));
            SqliteValues.Bind(command, "$processed", processed ? SqliteValues.Stamp(now) : null);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct);
    }
}
