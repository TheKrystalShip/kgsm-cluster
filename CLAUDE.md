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

**A store that already exists is never altered by `CREATE TABLE IF NOT EXISTS`.** Every member that has
ever been in a cluster has one, so adding a column means adding it to `ClusterStore.AddedColumns` *and*
to `RequiredColumns` — the first applies it to the table that is there, the second refuses to open a
store that still cannot answer this build's queries. Append to `AddedColumns`; never edit an entry,
because what is written there has already run on somebody's disk. Indexes are created last, after the
columns they name exist, or an upgradeable store becomes an unopenable one.

The failure this replaces is the one worth remembering: a member starts, serves, answers health, and
its gossip and liveness die once per tick in a log nobody reads — joined, reachable, and not in the
mesh at all.

**The store is bound to the secret it was written under.** `store_meta` records the secret's fingerprint,
and opening the file under a different secret empties `members`, `cluster_state`, `outbox` and `inbox` —
everything learned from the old cluster — before any read. Carried into a new cluster, an old assignment
gossips as current, and the capability tie-break (higher version, then the member id that sorts later) can
hand it the new cluster's accounts. `self_facts` stays: a member's own addresses describe its network, not
its cluster. A file recorded under `SecretPrevious` is a rotation and keeps everything; a file with no
fingerprint yet is recorded without discarding, because it cannot say which cluster wrote it.

**What a member says about itself is only heard when its incarnation moves.** Published facts, addressing
and kind all ride the self-entry, and an entry at an incarnation the receiver already holds supersedes
nothing. So changing the fact set raises this member's counter, and a member that finds the mesh ahead of it
climbs past — a restart resets the counter to zero while everybody else still holds where the last process
reached. A restart that publishes as many facts as the last process, one of them different, counts back up to
exactly the incarnation the mesh holds; so a member that finds the mesh level with it but holding facts it no
longer states climbs past too (`RestateSelf`). Without all three, a member can never change one word of its
own entry, and the symptom is nothing at all: no error, no log line, the old value simply staying put. Every
raise lands strictly ahead of what was observed, and the level case raises only on a difference, or a healthy
cluster — every member echoing a member's own entry back to it — raises its incarnations once per round
forever.

**Candidate order is the member's own statement, and two writers must not fight over it.** A member lists
its addresses most-preferred first, and that ranking is the only thing carrying which address it wants to be
reached at — it is what a browser is handed. So a relayed row contributes addresses and never their order
(`MemberCandidates.Absorb`), only the subject's own entry re-ranks (`Merge`), and a successful probe records
what it proved in `Url`/`AddressVerified` rather than hoisting it to the front. Any writer that reorders on
something other than the member's own word makes position zero a function of who wrote last, and the symptom
is a panel handing out a different address depending on the round.

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

Two harness details are load-bearing, and both exist because the harness had been hiding real behaviour:

- **A member records an address for itself at startup**, as a deployment does the first time somebody
  reaches it. Without one a member is reachable only by members it introduced itself, and a member
  nobody can reach also cannot refute anything said about it.
- **Members advertise a `.lan` name, not the loopback they bind to**, and `LoopbackResolvingHandler`
  sends the connection where they actually listen. Loopback is never advertised, so a harness that
  advertised it was proving something no deployment does.
