using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TheKrystalShip.KGSM.Cluster;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Messaging;
using TheKrystalShip.KGSM.Cluster.Storage;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// One member's cluster halves, wired over a throwaway SQLite file. Disposing it deletes the file, so
/// a test never inherits another's rows.
/// </summary>
internal sealed class TestCluster : IDisposable
{
    private readonly string _dbPath;

    public ClusterOptions Options { get; }
    public ClusterStore Store { get; }
    public ClusterBus Bus { get; }
    public ClusterInbox Inbox { get; }
    public IClusterTokenService Tokens { get; }

    public TestCluster(
        string memberId = "member-a",
        string secret = "test-cluster-secret",
        string secretPrevious = "",
        IEnumerable<IClusterMessageHandler>? handlers = null)
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"kgsm-cluster-test-{Guid.NewGuid():N}.db");
        Options = new ClusterOptions
        {
            MemberId = memberId,
            Secret = secret,
            SecretPrevious = secretPrevious,
            StorePath = _dbPath,
        }.Validate();

        Store = new ClusterStore(Options, NullLogger<ClusterStore>.Instance);
        Bus = new ClusterBus(Store, NullLogger<ClusterBus>.Instance);
        Inbox = new ClusterInbox(Store, handlers ?? [], NullLogger<ClusterInbox>.Instance);
        Tokens = new ClusterTokenService(Options, NullLogger<ClusterTokenService>.Instance);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (string path in new[] { _dbPath, _dbPath + "-wal", _dbPath + "-shm" })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
    }
}

/// <summary>A handler that records what it was given, and optionally throws to exercise the transient
/// path.</summary>
internal sealed class RecordingHandler(string type, bool throws = false) : IClusterMessageHandler
{
    public string Type { get; } = type;
    public List<ClusterEnvelope> Received { get; } = [];

    public Task HandleAsync(ClusterEnvelope envelope, CancellationToken ct)
    {
        Received.Add(envelope);
        if (throws) throw new InvalidOperationException("transient");
        return Task.CompletedTask;
    }
}
