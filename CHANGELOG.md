# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
