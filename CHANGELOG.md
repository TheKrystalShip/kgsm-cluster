# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
