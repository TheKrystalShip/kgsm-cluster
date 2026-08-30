# The cluster message bus

**Authority for durable member-to-member messaging.** A reliable, at-least-once, idempotent mail
channel between the members of a KGSM cluster over plain HTTP — no broker, no per-message endpoint.
Membership itself, the roster and the join handshake are the package's other half and are not
described here.

---

## 1 · Why it exists

Some things one member tells another must survive the other being **down** at the moment they are
said. The archetype: somebody clicks "sign out everywhere". The revocation has to reach every member
of the cluster — including one that is currently offline — and take effect when it returns. A
fire-and-forget POST loses that message; a synchronous fan-out fails the whole operation because one
member is unreachable.

The answer is the transactional outbox and inbox: persist the intent locally, then deliver it with
at-least-once retries to a receiver that applies it idempotently. Sized for a small single-owner
cluster that already has HTTP and SQLite, and adding no daemon to run.

**Not a broker.** Not general pub/sub, not exactly-once, not ordered delivery, not request-reply. A
call that needs an answer — reading a member's capacity, fetching its catalog — is an ordinary HTTP
GET and does not come near this. The bus carries what is asynchronous, reply-free and must not be
lost. The roster exchange is deliberately outside it too: gossip is best-effort and re-converges
every few seconds, so putting it here would retry a roster at a corpse for the full retry window.

---

## 2 · Principles

1. **Reuse what is there.** HTTP for transport, SQLite for durability. No broker, no coordinator, no
   extra process.
2. **One endpoint, typed messages.** A discriminated-union envelope on one route. A new capability
   adds a **type**, never a route.
3. **At-least-once, applied once.** Delivery repeats; the receiver dedupes by message id *and* every
   handler is idempotent by contract. Exactly-once is not attempted — it is unachievable over an
   unreliable network, and the idempotent-apply contract makes it unnecessary.
4. **Order-independent.** Messages carry no global order and handlers are commutative. A type that
   needs order carries its own sequence and orders in its handler.
5. **Fail open on availability, fail closed on auth.** A member that is down means queue and retry,
   never an error to the caller. A bad token on the inbox means reject, never process. Two distinct
   paths, and neither is allowed to behave like the other.

---

## 3 · The envelope

```jsonc
{
  "id":      "5f2c…",            // the dedupe key, minted once by the sender
  "type":    "session.revoke",   // the discriminated-union tag
  "from":    "hotrod",           // sender's member id — MUST equal the token's iss
  "ts":      "2026-08-30T12:00:00Z",
  "payload": { … }               // type-specific, camelCase, ISO-8601 Z timestamps
}
```

- `id` is minted once and is stable across every retry. It is what makes redelivery idempotent, and
  it is the replay defence: a captured and replayed envelope is a duplicate id, acknowledged without
  being applied again.
- `from` must equal the authenticated service token's `iss`. A mismatch is `403`: a member may not
  send as another member.
- `ts` is informational. Correctness never depends on it.
- An envelope is at most **64 KiB**. Larger is `413`, rejected without being parsed.

### Adding a type

Register an `IClusterMessageHandler` for it on every member that cares. There is no central registry
and no dispatcher to edit. A member that registers no handler for a type **acknowledges and drops
it** — a newer member may legitimately know a type an older one does not, and dropping is what keeps
the sender's queue moving rather than wedging it behind something that can never apply.

---

## 4 · The wire

### `POST /api/v1/members/inbox`

```
Authorization: Bearer <member service token>
Content-Type: application/json
Body: <envelope>

→ 200 { "status": "accepted" }                      // applied, de-duplicated, or dropped unknown
→ 400 { error: { code: "bad_request" } }            // malformed envelope, or missing id/type/from
→ 401 { error: { code: "invalid_cluster_token" } }  // absent, unsigned, expired or wrong-secret token
→ 403 { error: { code: "member_disabled" } }        // iss is a member this one has disabled
→ 403 { error: { code: "from_mismatch" } }          // from does not equal the token's iss
→ 413 { error: { code: "payload_too_large" } }
→ 500 { error: { code: "internal" } }               // transient handler failure — the sender retries
```

`200` covers a fresh apply, a duplicate and an unknown type alike. The sender cannot tell them apart
and does not need to: all three mean "stop retrying".

**`500` is the only answer that keeps a message in the sender's outbox**, and it is reserved for a
*transient* failure. A message the receiver can never apply is a `200` drop or a `400`, never a
`500`, so it can never wedge the queue behind it.

Every non-2xx carries the same `{error:{code,message,details?}}` envelope the rest of the ecosystem's
HTTP surfaces use, so a member hosting these endpoints beside its own serves one error shape.

---

## 5 · Storage

Two tables in the member's own SQLite file, under its `StateDirectory=`. Timestamps are ISO-8601 UTC
strings, which sort lexically in the order they sort chronologically, so the due-scan and the
retention cutoff are plain string comparisons.

### `outbox` — one row per (message, target)

A broadcast to N members is **N rows**, each delivered and retried on its own schedule.

| Column | Notes |
|---|---|
| `id` | `<messageId>:<targetId>` — unique per delivery |
| `message_id` | the envelope `id`, identical across every target of one broadcast |
| `target_id` · `target_url` | the recipient and the address it was known at |
| `type` · `payload` | the envelope's type and its serialized payload |
| `status` | `pending`, `delivered` or `dead` |
| `attempts` | incremented per failed send; the backoff is computed from it |
| `next_attempt_at` | the drainer skips a row whose next attempt is still ahead |
| `created_at` | what the retry TTL is anchored on |
| `delivered_at` · `last_error` | set on acknowledgement; the last failure or dead-letter reason |

### `inbox` — the processed-id ledger

| Column | Notes |
|---|---|
| `id` | the envelope id — the dedupe key |
| `from_id` · `type` | for diagnostics |
| `received_at` | what the retention sweep is anchored on |
| `processed_at` | set only once a handler has succeeded |

---

## 6 · Sending

`IClusterBus.EnqueueAsync(type, payload, payloadTypeInfo, targets, ct)` writes one row per target and
returns as soon as they are committed. Delivery is the drainer's job.

The payload's shape belongs to the caller, so the caller brings its own source-generated
`JsonTypeInfo` — this package never reflects over a payload type, which is what lets a Native-AOT
member send one. `EnqueueJsonAsync` takes JSON a caller already holds.

Targets are supplied by the caller, not resolved here. A durable, identity-carrying message should go
only to members this one has authenticated **first-hand** — never to one it has merely heard about
through gossip — or the outbox retries a secret-bearing message at a phantom for the full TTL.

**The enqueue does not share the caller's transaction.** It is a gated write, made after the caller's
own effect has committed. The gap is a narrow crash window in which the local effect lands and its
announcement does not; the local action is never lost, only the fan-out. Closing it means an overload
accepting the caller's own transaction, which is worth building when a caller needs that guarantee
and not before.

---

## 7 · Draining

A background service, inert when the member holds no cluster secret. A startup catch-up pass, then a
periodic loop whose per-tick failures are swallowed — one bad tick must never kill the drainer.

Each pass takes up to **100** due rows, oldest first, and processes them in turn. Per row:

- **The TTL is checked first.** A row older than the retry TTL is dead-lettered without being sent,
  loudly. It was never going to be given a fresh chance.
- **`2xx`** → delivered.
- **`400`, `401`, `403`, `413`** → dead-lettered, loudly. These mean a *local* misconfiguration — a
  wrong secret, or a member that has disabled us — and are surfaced rather than retried forever.
- **Anything else** — a thrown transport failure, a `5xx`, any other status — is transient: attempts
  rise, the next attempt moves out by the backoff, the row stays pending.
- **An unparseable stored payload** is dead-lettered too. No number of retries makes it send.

**Backoff** is `min(5 min, 1s · 2^(attempts-1))` plus a jitter of up to a fifth of that delay. The
jitter is what stops every row aimed at one recovered member retrying in lockstep.

### Retention

A sweep prunes delivered and dead outbox rows, and inbox ledger rows, past the retention window.
**That window must exceed the retry TTL** — `ClusterOptions.Validate()` enforces it — or the ledger
forgets a message the outbox is still retrying and the redelivery applies a second time.

---

## 8 · Receiving

```
1. Reject on an oversized declared length, before the body is touched.
2. Authenticate the service token, fail-closed.
3. Check the member gate: an explicitly disabled member is refused.
4. Read the body under a hard byte cap — a chunked request can understate its length.
5. Parse; reject a malformed envelope or one missing id, type or from.
6. Reject from ≠ iss.
7. Dedupe, dispatch, record.
```

**The handler runs first, and the ledger row is written only after it returns.** The alternative —
insert the row, dispatch, roll both back on failure — assumes both land in one transaction. They
cannot: a handler's effect lands in a completely different store. Insert-first would risk marking an
envelope processed when the handler meant to run for it never completed.

Handler-first is safe rather than merely convenient, precisely because every handler is contractually
idempotent. If the process dies between the handler succeeding and the row committing, the next
redelivery runs the handler again and the effect lands once.

**Two layers of idempotency, on purpose.** The ledger stops re-apply in the normal case; idempotent
handlers make a re-apply harmless across the crash window between the two steps.

The whole sequence for one envelope is serialized against every other envelope arriving at the same
member, which is what makes a duplicate delivered twice at once safe without a cross-store
transaction.

---

## 9 · Delivery semantics

- **At-least-once.** Delivered one or more times; applied once, by the ledger and the idempotent
  handler together.
- **No ordering.** Independent messages arrive in any order.
- **No exactly-once, by design.**
- **Poison-message safety.** A message a receiver can never accept is dropped or dead-lettered. It
  can never block the queue behind it, and it can never retry forever.

---

## 10 · Security

- **The inbox is fail-closed.** A service token is required, its `iss` must not be a disabled member,
  and `from` must equal `iss`. No token, wrong secret, disabled member or spoofed `from` means
  rejected, never processed.
- **Replay** is a duplicate id, acknowledged without effect. The ledger subsumes any need for a
  separate nonce or timestamp window.
- **Blast radius.** The bus lets any valid member trigger any registered handler on any other. That
  is within the single-owner trust boundary the shared secret already establishes: the secret buys
  attribution, not isolation, so the bus grants no authority a member does not already have.
- **Size and rate.** The endpoint is authenticated and size-limited. It is not an unauthenticated
  surface.
- **No secrets in payloads.** Envelopes carry identifiers and intents, never credentials.

---

## 11 · Observability

`ClusterBus.ListForTargetAsync` returns every row for one member whatever its status — outbox depth
toward it, and the fate of a settled row. A growing pending count toward one member is the signal
that it is genuinely unreachable beyond its backoff; a non-zero dead count is an operational alarm.

Dead-lettering and permanent rejection log at error level, because both mean something a person has
to fix rather than something that will resolve itself.

---

## 12 · Open items

1. **The back-online optimisation.** Each member polls its peers' liveness on its own loop, and a
   flip from unreachable to reachable could wake the drainer and reset that member's pending rows.
   Nothing does that today: the durable retry loop is the correctness mechanism and delivers
   regardless, so this is latency only.
2. **Anti-entropy backstop.** A periodic reconcile would catch anything ever lost to a dead-letter.
   The retry TTL and a loud dead-letter are the safety net; add reconcile only if a real loss is
   observed.
3. **Rate-limit thresholds** for the inbox, once real message volumes exist.
4. **A transactional enqueue overload**, when a caller needs the guarantee §6 describes.
