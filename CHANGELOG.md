# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-dev.15]

### Fixed

- **A member could never change what it published.** Facts ride the self-entry and a receiver takes that
  entry only when it supersedes the one already held; at equal incarnation nothing supersedes. Nothing
  raised a member's own incarnation when it published, so the first values a member published on a given
  run were the only ones it could ever deliver, for as long as it stayed healthy. The inversion is the
  tell: the facts moved only if the publisher first went unreachable long enough to be suspected, because
  refutation was the one path that raised the counter.

  Changing the fact set now raises the incarnation carrying it, and republishing an identical value does
  not — so a caller re-stating its facts on a timer costs no rounds. This is what key rotation depends on:
  the anchor publishes the incoming key beside the outgoing one, every verifier takes both, and only then
  does the signer move. `Withdraw` takes effect for the same reason.

- **A restarted member could not be heard about itself again.** The incarnation counter is a process
  lifetime value that resets to zero, and it climbed back only by refuting a non-alive report. A member
  that restarted without ever being suspected — a clean restart of a healthy member, which is the ordinary
  case — sat below what the mesh held about it and had no path back, so nothing it said about itself was
  ever taken again. It now climbs past an incarnation the mesh holds that is strictly ahead of its own.

  Strictly ahead, never level: at rest every member reports our own value back to us, and treating that as
  a reason to climb raises the incarnation once per gossip round for as long as the cluster is healthy.

## [1.0.0-dev.14]

### Fixed

- **A departed member was never forgotten: the reaper dropped its row and the next gossip round learned
  it straight back.** A terminal report about a member the roster did not hold was inserted as a new row,
  stamped with a fresh state-changed time. Members reap on their own clocks, so whichever reaped first
  synced with one that had not yet, re-created the tombstone, and restarted the window it had just
  finished serving — a loop with no end, visible only as a departed member sitting permanently in the
  roster and a `reaped` line followed immediately by a `learned` line for the same member.

  A tombstone is a correction, not a lesson: it travels so a member still holding an alive row is put
  right, and a member holding no row has nothing to correct. Terminal reports about an unheld member are
  ignored; hearsay about a live one still joins the roster as before.

  Beyond the roster's own tidiness, this is what makes a capability's holder observably gone. A roster
  that never forgets anybody is one in which `ClusterFacts.OrphanedAsync` can never report an orphaned
  assignment, so a member standing by against a holder that will never answer looks identical to one
  waiting on a holder that is merely slow.

## [1.0.0-dev.13]

### Fixed

- **A roster row that learned no address could never gain one.** Addressing was adopted only when a
  report superseded on membership state, and two members holding the same state at the same incarnation
  neither supersedes the other — so the whole report was ignored, and every address in it with it, for
  as long as both stayed alive. A member that joined over loopback therefore reached every other member
  as a row with no address, and stayed unreachable to them after it moved somewhere routable, with
  nothing in any log.

  Addressing is now taken from any report about a member the roster holds, whatever that report is worth
  as a claim about state. Where a member answers is an additive fact and the poller settles it;
  membership state is a claim and still has to win its ordering. A proven address is not unpinned by
  hearsay, and a disabled member's row is still untouched by gossip.

## [1.0.0-dev.12]

### Fixed

- **A store written by an earlier build never gained a later build's columns.** `CREATE TABLE IF NOT
  EXISTS` creates and does not alter, so a member that had run before kept the table it had — and every
  query naming `published`, added with per-member facts, threw once per tick. The member started,
  served, answered health, and its gossip and liveness were dead in a log nobody reads: joined,
  reachable, and not in the mesh at all.

  A column added after a table shipped is now applied to the table that exists, and a store that still
  cannot answer this build's queries stops the member instead of being carried on past. Indexes are
  created last, after the columns they name — attempting one first is what turns an upgradeable store
  into an unopenable one.

### Added

- `ClusterRequest.AuthenticateAsync` — the service-token check and the enabled-member gate, for a
  member serving a member-to-member route of its own. It was private, so a member with its own such
  route had to reimplement both, which is two implementations of one rule and two members able to
  disagree about what the protocol is.

## [1.0.0-dev.11]

### Fixed

- **A loopback address is never advertised to another member.** It means "me" to whoever reads it, so
  told to a member on another machine it does not fail to connect — it reaches whatever is on that
  machine's own port, which in a cluster running the same components is plausibly another member of the
  same kind. A member talking to itself while believing it reached somebody else is worse than an
  address that does not answer, because nothing errors.

  Filtered at every point candidates cross the wire, which is two paths and not one: a member's own
  card and gossip self-entry, **and the candidates it holds for its neighbours** — gossip carries a
  member's whole roster, so a loopback pinned for a neighbour reaches every member in the cluster
  regardless of what that neighbour advertises about itself.

  The address is still stored and still used. Two members on one machine reach each other over loopback
  and that is a real topology; what changes is that it is never told to anybody. The limit that follows
  is real and stated: a member reachable *only* over loopback cannot be learned from the mesh, because
  nothing on the wire can express "the loopback of the machine we share".

## [1.0.0-dev.10]

### Added

- `ClusterFacts.OrphanedAsync` — every capability assigned to a member that is no longer in the roster.
  A holder can leave without anybody removing it: its machine dies, it goes suspect, then dead, and the
  reap window passes. The assignment outlives it, every member stands by against a holder that will
  never answer, and nothing errors because nothing failed. This is how that is found rather than
  deduced from a capability quietly not being served.
- Reaping a member that holds a capability logs at error level naming the capability. The reaper knows
  both facts, so destroying the one silently is not something it should do.

## [1.0.0-dev.9]

### Fixed

- **A member could not be removed from a running cluster.** Removal deleted the row, and anti-entropy
  exists to repair a roster that is missing something — so the first member that still held it handed it
  straight back, alive. `MembersStore.MarkLeftAsync` records a terminal state one incarnation above what
  the member last claimed, so the departure supersedes rather than being an absence, and the failure
  timers reap it everywhere once the reap window passes. `DeleteAsync` remains the reaper's primitive and
  says so.

  A member that is still running and still gossiping refutes its own removal and returns. That is the
  refutation channel working — only a member may raise its own incarnation — so removing one that is
  still participating is a request the cluster overturns. Stop it, or disable it.

## [1.0.0-dev.8]

### Added

- **Published facts** — `SelfPublications`, a small map a member states about itself, carried on its card
  and in its gossip entry and adopted under the same ordering as everything else it says about itself. The
  archetype is a public key: a member that signs what others verify has to hand them the key. Capped per
  value and per member, because every fact rides every round.
- **Cluster assignments** — `ClusterStateStore`, which member holds each capability. Cluster state rather
  than member state, so it carries its own version instead of riding an incarnation. `TryClaimAsync` sets
  only if nobody holds it, so a second holder starting elsewhere becomes a candidate rather than a second
  authority; `AssignAsync` is the deliberate overwrite a person makes. A map, not a policy: what holding a
  capability means belongs to whoever consumes it.
- Both travel in the join exchange as well as in gossip, so a member holds them the moment it joins. A
  member that joined without the assignment could believe a capability was unheld and claim one already
  held.
- `ClusterFacts.FromHolderAsync` reads a published fact **from the member the cluster says holds a
  capability**, which is the safe way to read one: any member can state a key, only the holder is believed
  for it.

### Changed

- `ClusterProtocol.Current` is 3.

## [1.0.0-dev.7]

### Added

- `ClusterConfiguration` — the secret's configuration key names, owned here rather than by each member.
  A cluster's members must spell `Cluster__Secret` identically or each reads a blank from the shared
  file and concludes separately, and silently, that it is not clustered. Read by explicit key rather
  than bound onto a type, because binding reflects and no AOT member can.

## [1.0.0-dev.6]

### Added

- `RosterMemberGate` — the roster-backed disable-list, registered by default. Every member has a roster,
  so every member gets the same gate rather than writing its own; it keys on member id, which is what
  lets a machine's node be disabled while the anchor beside it keeps running.

## [1.0.0-dev.5]

### Added

- The gossip worker and the liveness poller are resolvable as themselves, so a member that has just joined
  can drive a round rather than waiting out an interval.

## [1.0.0-dev.4]

### Added

- Membership: the roster, the symmetric join handshake, anti-entropy gossip, per-member liveness polling
  and the enabled-member gate. A member is a **node** or an **anchor**, and an anchor joins on the protocol
  version alone — the route version is a statement about a surface it does not serve.
- The member-to-member wire gains sync, introduce and identity, so joining a cluster needs no code beyond
  this package.
- `SelfMemberCardSource` states everything an anchor has to say about itself. A node registers its own
  source over it to add the node block.
- A refusal carries the values that disagreed, so a version mismatch names both versions rather than only
  itself.

### Fixed

- A member that only ever initiated joins never learned any address for itself, so it gossiped itself with
  none and every member that learned it that way held a row nothing could reach. It now adopts the address
  the far side reflects back.
- One addressless member in the roster cost every gossip round, failure timers included, because it was
  picked as a sync partner and the round threw before reaching them.

## [1.0.0-dev.3]

### Changed

- The protocol version constant lives with the handshake that reads it, so a build carries one
  definition of it rather than two that can disagree about what members speak.

## [1.0.0-dev.2]

### Added

- `ClusterBus.ListForTargetAsync` — every outbox row addressed to one member, whatever its status, so
  outbox depth toward a member and the fate of a settled row are both readable.

## [1.0.0-dev.1]

### Added

- `TheKrystalShip.KGSM.Cluster`: cluster membership and durable member-to-member messaging as a
  package any member consumes.
- Messaging: the wire envelope, a transactional outbox with capped-exponential backoff, dead-lettering
  and a retry TTL, an inbox with a processed-id dedupe ledger and handler dispatch, and a retention
  sweep whose window is held above the retry TTL.
- Identity: HMAC-SHA256 member service tokens, minted per outbound call with a 60-second lifetime and
  validated fail-closed, accepting a previous secret during a rotation overlap.
- The member-to-member wire as minimal-API endpoints, mapped onto the member's own router.
- Storage on `Microsoft.Data.Sqlite`, one file per member under its own `StateDirectory=`.
