using Microsoft.Data.Sqlite;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Membership;

/// <summary>
/// The cluster's own state, as this member holds it: which member holds each capability. Converged by
/// gossip like the roster, and like the roster it is this member's copy rather than an authority.
/// </summary>
public sealed class ClusterStateStore(ClusterStore store, ClusterOptions options)
{
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    /// <summary>Every assignment this member currently holds a copy of.</summary>
    public async Task<IReadOnlyList<ClusterAssignment>> ListAsync(CancellationToken ct)
    {
        await using SqliteConnection connection = await store.OpenAsync(ct).ConfigureAwait(false);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT capability, member_id, version, set_by FROM cluster_state;";
        var rows = new List<ClusterAssignment>();
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new ClusterAssignment(
                reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetString(3)));
        }
        return rows;
    }

    /// <summary>One capability's assignment, or <see langword="null"/> when this member has never heard of
    /// one. Absent and held-by-nobody are different answers: the first means "I do not know", the second
    /// means "the cluster decided nobody".</summary>
    public async Task<ClusterAssignment?> GetAsync(string capability, CancellationToken ct)
        => (await ListAsync(ct).ConfigureAwait(false))
            .FirstOrDefault(a => string.Equals(a.Capability, capability, StringComparison.Ordinal));

    /// <summary>The member holding a capability, or <see langword="null"/> when nobody does or this member
    /// has not heard yet.</summary>
    public async Task<string?> HolderAsync(string capability, CancellationToken ct)
        => await GetAsync(capability, ct).ConfigureAwait(false) is { IsHeld: true } a ? a.MemberId : null;

    /// <summary>Whether this member is the one holding a capability.</summary>
    public async Task<bool> IsHolderAsync(string capability, CancellationToken ct)
        => string.Equals(
            await HolderAsync(capability, ct).ConfigureAwait(false), options.MemberId, StringComparison.Ordinal);

    /// <summary>
    /// Claim a capability only if nobody holds it — the bootstrap write, made by the component claiming it
    /// rather than by a person.
    /// </summary>
    /// <returns><see langword="true"/> if the claim was recorded here.</returns>
    /// <remarks>
    /// <b>Succeeding here is not the same as holding it.</b> This is compare-and-set against what this
    /// member currently knows, and two members that both see no holder both succeed locally. The tie is
    /// resolved when their gossip meets, deterministically and the same way on both sides, and the loser's
    /// copy is overwritten. So a claimant must re-read the assignment afterwards and stand down if it is
    /// not the holder — which is exactly the behaviour that keeps a second install a candidate rather than
    /// a second authority.
    /// </remarks>
    public async Task<bool> TryClaimAsync(string capability, string memberId, CancellationToken ct)
    {
        await _writeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ClusterAssignment? existing = await GetAsync(capability, ct).ConfigureAwait(false);
            if (existing is { IsHeld: true })
                return false;

            long version = (existing?.Version ?? 0) + 1;
            await WriteAsync(new ClusterAssignment(capability, memberId, version, options.MemberId), ct)
                .ConfigureAwait(false);
            return true;
        }
        finally { _writeGate.Release(); }
    }

    /// <summary>
    /// Record who holds a capability, overwriting whoever held it — the deliberate reassignment a person
    /// makes. An empty <paramref name="memberId"/> records that nobody holds it, which is a decision and
    /// carries a version like any other.
    /// </summary>
    public async Task<ClusterAssignment> AssignAsync(string capability, string memberId, CancellationToken ct)
    {
        await _writeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ClusterAssignment? existing = await GetAsync(capability, ct).ConfigureAwait(false);
            var assignment = new ClusterAssignment(
                capability, memberId ?? "", (existing?.Version ?? 0) + 1, options.MemberId);
            await WriteAsync(assignment, ct).ConfigureAwait(false);
            return assignment;
        }
        finally { _writeGate.Release(); }
    }

    /// <summary>
    /// Merge assignments learned from another member, taking each only where it supersedes what is held.
    /// Returns the capabilities whose holder changed, so a caller can react to being promoted or demoted
    /// without diffing the whole set itself.
    /// </summary>
    public async Task<IReadOnlyList<string>> MergeAsync(
        IEnumerable<ClusterAssignment>? incoming, CancellationToken ct)
    {
        if (incoming is null) return [];

        await _writeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var changed = new List<string>();
            foreach (ClusterAssignment candidate in incoming)
            {
                if (string.IsNullOrWhiteSpace(candidate.Capability)) continue;
                ClusterAssignment? existing = await GetAsync(candidate.Capability, ct).ConfigureAwait(false);
                if (!candidate.Supersedes(existing)) continue;

                await WriteAsync(candidate, ct).ConfigureAwait(false);
                if (!string.Equals(existing?.MemberId ?? "", candidate.MemberId, StringComparison.Ordinal))
                    changed.Add(candidate.Capability);
            }
            return changed;
        }
        finally { _writeGate.Release(); }
    }

    private Task WriteAsync(ClusterAssignment assignment, CancellationToken ct)
        => store.WriteAsync(async (connection, token) =>
        {
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = """
                INSERT OR REPLACE INTO cluster_state (capability, member_id, version, set_by)
                VALUES ($capability, $memberId, $version, $setBy);
                """;
            command.Parameters.AddWithValue("$capability", assignment.Capability);
            command.Parameters.AddWithValue("$memberId", assignment.MemberId);
            command.Parameters.AddWithValue("$version", assignment.Version);
            command.Parameters.AddWithValue("$setBy", assignment.SetBy);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct);
}
