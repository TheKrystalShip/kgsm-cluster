# kgsm-cluster

`TheKrystalShip.KGSM.Cluster` — cluster membership and durable member-to-member messaging, as a
package any member consumes. Joining a cluster costs this package and nothing else: a member does not
have to be, or run, `kgsm-api`.

## What a cluster is

A cluster is the umbrella. Under it are **members**, and a member is one of two kinds:

- a **node** runs the engine and game servers, and hosts leaves;
- an **anchor** provides one capability to the whole cluster.

A member joins the cluster directly — not through a node, not through an API. Leaf keeps its own
meaning: a component a node runs locally over a unix socket, serving that host. Leaf or anchor is a
deployment choice, not a property of a component.

## What is in the package

| | |
|---|---|
| `Membership` | the roster, the join handshake, gossip, liveness, the enabled-member gate |
| `Messaging` | the envelope, outbox, inbox, drainer, dedupe ledger, dead-lettering, retention |
| `Identity` | member service tokens: mint and validate against the shared cluster secret |
| `Storage` | one SQLite store on `Microsoft.Data.Sqlite`, shared by both halves |

The member-to-member wire ships here too, as minimal-API endpoints a member maps onto the router it
already has. One implementation of the status codes, the size cap, the token check and the spoof
guard, so two members cannot disagree about what the protocol is.

## Using it

```csharp
builder.Services.AddKgsmCluster(new ClusterOptions
{
    MemberId  = "hotrod",
    Secret    = configuration["Cluster:Secret"] ?? "",
    StorePath = Path.Combine(stateDirectory, "cluster.db"),
});

// ... and on the built app, beside whatever else it serves:
app.MapClusterEndpoints();
```

Everything is registered unconditionally and everything is inert without a secret: the token service
mints nothing and validates nothing, so the inbox rejects every call before it reaches a handler, and
neither background worker starts a timer. A member wires this in once and joins a cluster later by
gaining a secret.

To receive a message type, register a handler for it:

```csharp
builder.Services.AddSingleton<IClusterMessageHandler, MyHandler>();
```

A handler must be idempotent. A type nobody registered is acknowledged and dropped, so a newer member
sending a type an older one has never heard of never wedges its queue.

To send one, bring your own serializer metadata — the payload's shape is yours, so this package never
reflects over it:

```csharp
await bus.EnqueueAsync("account.disabled", payload, MyJsonContext.Default.MyPayload, targets, ct);
```

## What being a member costs

- **A durable store.** The outbox is the at-least-once guarantee; a member without one cannot make
  the promise.
- **An inbox endpoint** other members can reach, token-authed and size-capped.
- **A handler per type it cares about**, idempotent by construction.
- **An address other members can reach it at.**

## Delivery semantics

At-least-once, applied exactly once by the dedupe ledger plus idempotent handlers. No ordering:
handlers are commutative, and a type that needs order carries its own sequence and orders in its
handler. A message a member can never accept is dropped or dead-lettered, so it can never block the
queue behind it or retry forever.

## Native AOT

The package is AOT-safe and every member other than `kgsm-api` is an AOT service. Two rules follow
and both are load-bearing: every wire shape is serialized through a source-generated context, and
every endpoint is a `RequestDelegate` that reads its own request and writes its own response. The
convenient routing overloads that accept an arbitrary delegate reflect over its parameters to bind
them, which an AOT member cannot do.

## Building

```bash
dotnet build src/Cluster/Cluster.csproj      # expect 0 warnings, IL2026/IL3050 included
dotnet test  tests/Cluster.Tests/Cluster.Tests.csproj
```

The test suite stands two members on real Kestrel ports and delivers between them over HTTP, so the
case the outbox exists for — a target that is down when the message is issued and returns later — is
exercised rather than mimed.
