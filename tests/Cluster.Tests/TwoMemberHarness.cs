using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TheKrystalShip.KGSM.Cluster.Identity;
using TheKrystalShip.KGSM.Cluster.Membership;
using TheKrystalShip.KGSM.Cluster.Messaging;

namespace TheKrystalShip.KGSM.Cluster.Tests;

/// <summary>
/// One member standing on a real Kestrel port, wired exactly as a member is: the package's own DI
/// registration and its own endpoint mapping, nothing hand-rolled. Delivery between two of these is a
/// real HTTP round trip, which is what makes the down-then-up case meaningful rather than mimed.
/// </summary>
internal sealed class MemberHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly string _dbPath;

    public string MemberId { get; }
    public string Url { get; }
    public RecordingHandler Handler { get; }

    private MemberHost(WebApplication app, string memberId, string url, string dbPath, RecordingHandler handler)
    {
        _app = app;
        _dbPath = dbPath;
        MemberId = memberId;
        Url = url;
        Handler = handler;
    }

    public static async Task<MemberHost> StartAsync(
        string memberId, string secret, string handledType = "test.message", bool handlerThrows = false,
        IClusterMemberGate? gate = null, string? dbPath = null, string? url = null,
        string kind = MemberKind.Node, string apiVersion = "v1")
    {
        dbPath ??= Path.Combine(Path.GetTempPath(), $"kgsm-cluster-host-{Guid.NewGuid():N}.db");
        var handler = new RecordingHandler(handledType, handlerThrows);

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        // Port 0 lets the OS pick, except when a test is deliberately bringing the SAME member back
        // up: a returning member has to answer where it did before, or the sender's queued row is
        // addressed at nobody and the down-then-up case proves nothing.
        builder.WebHost.UseUrls(url ?? "http://127.0.0.1:0");
        builder.Services.AddKgsmCluster(new ClusterOptions
        {
            MemberId = memberId,
            Secret = secret,
            StorePath = dbPath,
            Kind = kind,
            // Fast enough that a test never waits on a tick, slow enough not to spin.
            DrainMs = 100,
            // Gossip and the poller are driven explicitly by the tests that care, so their loops stay out
            // of the way of the ones that do not.
            GossipMs = 3_600_000,
            PollMs = 3_600_000,
        });
        // A node states a route version; an anchor states nothing beyond what the package already holds,
        // which is the whole point of an anchor being able to join.
        if (kind == MemberKind.Node)
            builder.Services.AddSingleton<IMemberCardSource>(sp => new TestNodeCardSource(sp, apiVersion));
        builder.Services.AddSingleton<IClusterMessageHandler>(handler);
        if (gate is not null) builder.Services.AddSingleton(gate);

        WebApplication app = builder.Build();
        app.MapClusterEndpoints();
        await app.StartAsync();

        return new MemberHost(app, memberId, app.Urls.First(), dbPath, handler);
    }

    public T Resolve<T>() where T : notnull => _app.Services.GetRequiredService<T>();

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (string path in new[] { _dbPath, _dbPath + "-wal", _dbPath + "-shm" })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch (IOException) { }
        }
    }
}

/// <summary>A gate that refuses one named member, for the disabled-member case.</summary>
internal sealed class DenyOneGate(string denied) : IClusterMemberGate
{
    public Task<bool> IsEnabledAsync(string memberId)
        => Task.FromResult(!string.Equals(memberId, denied, StringComparison.Ordinal));
}

/// <summary>
/// A node's card: the package's own self-card with the node block a node adds. This is exactly the shape a
/// real node takes — wrap the default source, do not replace it, so the identity and addresses stay the
/// package's answer.
/// </summary>
internal sealed class TestNodeCardSource(IServiceProvider services, string apiVersion) : IMemberCardSource
{
    public async Task<MemberCard> BuildAsync(CancellationToken ct)
    {
        var inner = new SelfMemberCardSource(
            services.GetRequiredService<ClusterOptions>(),
            services.GetRequiredService<SelfIdentityStore>(),
            services.GetRequiredService<SelfIncarnation>(),
            services.GetRequiredService<SelfPublications>());
        MemberCard card = await inner.BuildAsync(ct);
        return card with { Node = new NodeFacts(apiVersion, "test-build", ["monitor"]) };
    }
}
