# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What this is

`TheKrystalShip.KGSM.Cluster` — one package, published to GitHub Packages, giving any KGSM component
cluster membership and durable member-to-member messaging. It exists so that **joining a cluster does
not require being, or running, `kgsm-api`**. The workspace authority for the shape it establishes is
`../cluster-transport-plan.md`. The protocol — envelope, wire, storage, delivery semantics — is
`docs/cluster-message-bus.md`, in this repo because a contract that lives away from its code drifts
from it unnoticed.

Members, not peers. A cluster has members; a member is a **node** (runs the engine and game servers,
hosts leaves) or an **anchor** (provides one capability to the whole cluster). Node-or-anchor is a
deployment choice, not a property of a component. *Leaf* keeps its own meaning and is not a member.

## Commands

```bash
dotnet build src/Cluster/Cluster.csproj          # must be 0 warnings
dotnet test  tests/Cluster.Tests/Cluster.Tests.csproj
```

## The rules that are load-bearing

**Native AOT is not optional here.** Every member other than `kgsm-api` is an AOT service, so this
package is the one thing they all embed. Two consequences, and breaking either is silent until an ILC
pass in a consumer fails:

- **Every wire shape is serialized through `ClusterJsonContext`.** A type deserialized by this package
  must be registered there or it throws at runtime; there is no reflection fallback.
- **Every endpoint is a `RequestDelegate`.** `MapPost(pattern, someDelegate)` reflects over the
  delegate's parameters and return type to bind them, which raises IL2026/IL3050 and cannot work in an
  AOT member. Handlers read their own request and write their own response.

**A payload's shape belongs to its caller.** `IClusterBus.EnqueueAsync` takes the caller's own
`JsonTypeInfo<T>`; this package never reflects over a payload type. A caller with JSON already in hand
uses `EnqueueJsonAsync`.

**Handlers are idempotent, and that is what makes the inbox correct.** The inbox runs the handler
*first* and writes the ledger row only after it returns. The alternative — insert, then dispatch, then
roll both back on failure — assumes one transaction spans both, and it cannot: a handler's effect
lands in a completely different store. Handler-first plus idempotency means a crash between the two
steps costs a redelivery, not a lost or doubled effect.

**A `500` is the only answer that keeps a message in the sender's outbox.** Reserve it for a transient
handler failure. An unknown type acknowledges with `200`, a malformed envelope answers `400`. Anything
else wedges the sender's queue behind a message it can never deliver.

**The enabled-member gate keys on member id, never on a machine.** Two members on one machine are two
members; disabling one must leave the other running.

**The retention window must exceed the retry TTL.** `ClusterOptions.Validate()` enforces it. A shorter
window lets the ledger forget a message the outbox is still retrying, and the redelivery applies twice.

## Layout

| | |
|---|---|
| `src/Cluster/Membership` | roster, join handshake, gossip, liveness, the enabled-member gate |
| `src/Cluster/Messaging` | envelope, outbox, inbox, drainer, dedupe ledger, dead-lettering, retention |
| `src/Cluster/Identity` | member service tokens: mint and validate |
| `src/Cluster/Storage` | one SQLite store on `Microsoft.Data.Sqlite`, shared by both halves |

`ClusterRoutes` names the member-to-member paths once so a sender and a receiver cannot disagree.
`ClusterProtocol.Current` versions the *record shapes* members exchange, which is separate from any
member's own HTTP route version — raise it whenever a record on the wire between members changes shape,
because that disagreement is otherwise silent.

## Version tracking

The package version lives in `src/Cluster/Cluster.csproj`. A published version is immutable, so
shipping a change is: bump `<Version>` → `../scripts/publish-packages.sh kgsm-cluster` → bump the
consumer's pin. Iterate with prerelease versions (`-dev.1`, `-dev.2`) rather than trying to replace one.
Every bump carries its `CHANGELOG.md` entry in the same commit, and that commit gets an annotated tag:
`git tag -a v<version> -m "kgsm-cluster v<version>"`.

## Testing

The suite stands two members on real Kestrel ports and delivers between them over HTTP. That is
deliberate: the case the outbox exists for is a target that is **down** when a message is issued and
returns later, and only a real restart on the same address proves it. `MemberHost.StartAsync` takes an
explicit `url` for exactly that — a returning member has to answer where it did before, or the
sender's queued row is addressed at nobody and the test proves nothing.
