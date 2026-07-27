---
ddx:
  id: td-relational-batch-row-byte-admission
  depends_on:
    - prd
    - adr-log-single-source-of-truth
    - adr-full-async-storage-boundaries
    - api-native-client-interface
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-sqlite-native-embedded-durable-mode
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-postgres-native-reference-mode}
    - {kind: informed_by, to: td-sqlite-native-embedded-durable-mode}
  status: proposed
---

# Technical Design: TD-011 Relational Batch Row-and-Byte Admission

**Status**: Proposed; operator review required before contract or implementation work.
**Contract**: API-001 | **ADR**: ADR-013, ADR-015 | **Scope**: relational mutation admission only

## Scope

This proposal decides whether `BatchPush`, `BatchUpdate`, `BatchClaim`, and
`BatchFinalize` need request admission bounded by both logical rows and encoded
bytes. It preserves API-001 per-item outcomes, `request_id` replay, and
read-after-success visibility.

In scope:

- request-envelope row and byte accounting before a relational transaction starts;
- PostgreSQL and SQLite statement-variable and transaction-memory envelopes;
- comparison of no change, reject-before-start, and deterministic chunking;
- behavior for retries, cancellation, and unknown outcomes.

Non-goals:

- downstream API rate, quota, or worker-capacity admission in the claim path;
- changing queue eligibility, ordering, leases, or progress semantics;
- changing API-001 in this proposal;
- adding configuration, schema, source code, or implementation beads before review.

## Decision

**Recommendation**: retain the existing 1,000-row operation ceiling and propose
a backend-neutral **16 MiB canonical encoded-mutation ceiling**. Admission is a
whole-request, reject-before-start check. A request exceeding either bound has
no durable effect and returns API-001's envelope-level capacity error. Do not
silently split one `request_id` across transactions.

The 16 MiB candidate aligns the relational family with the maximum canonical
object-log record size (`crates/fireweed-objectlog/src/segment_integrity.rs`),
so storage composition cannot change whether a native mutation request is
admissible. It is a proposed contract value, not active behavior.

## Governing Alignment

| Authority | Requirement preserved |
|---|---|
| PRD P0-5..8 | Batch-centric push, update, claim, and finalize remain bounded operations with deterministic results. |
| PRD P0-15 | Backpressure protects deployment capacity without defining downstream-work pacing. |
| PRD FR-18..22 | Push/update retain idempotency, ordered per-item results, and bounded replay state. |
| PRD FR-23..28 | Claim/finalize retain one active lease and durable lifecycle transitions. |
| PRD FR-29..35 | Claim/finalize batching remains bounded and preserves deterministic ordering and group/cohort atomicity. |
| PRD non-goal | No downstream-rate token, quota, or admission stage is added to `BatchClaim`. |
| ADR-013 | The durable log remains authoritative; admission cannot create projection-only effects. |
| ADR-015 | Owned mutation data is bounded before async work or a driver transaction starts. |
| API-001 | Success remains durable and read-visible; rejection has no effect; unknown outcomes resolve through one `request_id`. |

## Current Evidence

### Logical row bounds

- `BatchUpdate` rejects more than 1,000 entries before planning
  (`crates/fireweed-engine/src/compose.rs`, `BatchUpdatePort::batch_update`).
- Independent ordered push is bounded at 1,000 items
  (`crates/fireweed-engine/src/port.rs`,
  `MAX_ORDERED_INDEPENDENT_PUSH_ITEMS`).
- Queue definitions persist `max_push_batch_size` and
  `max_claim_batch_size`; API-001 requires positive bounded values.
- Claim and finalize operate on bounded request vectors, but their retained
  payload, metadata, token, and replay bytes are not governed by one common
  request-byte ceiling.

### Measured statement-shape curves

The table records deterministic measurements from the current SQL builders.
`r` is item rows and `g` is item/gate pairs. These are exact bind counts for
the cited statement shape, not host-timing estimates.

| Path | Measured bind curve | Current chunk | Bind count at chunk | Driver ceiling |
|---|---:|---:|---:|---:|
| PostgreSQL item insert | `4 + 14r` | 1,000 rows | 14,004 | 65,535 |
| PostgreSQL gate insert | `2 + 2g` | 5,000 pairs | 10,002 | 65,535 |
| SQLite general item insert | `19r` | 1,500 rows | 28,500 | 32,766 |
| SQLite empty-item insert | `10r` | 1,500 rows | 15,000 | 32,766 |
| SQLite gate insert | `4g` | 1,500 pairs | 6,000 | 32,766 |

Evidence paths:

- `crates/fireweed-postgres/src/relational.rs`: `PG_INSERT_CHUNK`,
  `insert_items`, and `insert_gates`.
- `crates/fireweed-sqlite/src/relational/apply.rs`: `SQLITE_BATCH`,
  `insert_items`, `insert_default_empty_items`, and `insert_gates`.

The curves show that row-count admission prevents bind exhaustion for the
current item shape. It does not bound encoded payload, fields, metadata,
entity documents, request replay outcomes, planner allocations, or driver
message bytes.

### Byte curve

For one request with entries `i = 1..r`, define the canonical admission charge:

```text
request_bytes = fixed_envelope_bytes
              + sum(canonical_entry_bytes(i))
              + canonical_result_reserve(r)
```

`canonical_entry_bytes` includes identifiers, selectors, lease tokens,
priority/schedule fields, payload, metadata, entity document, gate keys, and
field operations after canonical serialization. `canonical_result_reserve`
bounds the stored `request_id` replay result. The charge excludes SQL text and
driver duplication; those are backend working-set measurements and must remain
bounded by implementation-specific chunking.

The required measurement matrix before acceptance is:

| Operation | Rows | Entry shapes | Required observations |
|---|---|---|---|
| BatchPush | 1, 10, 100, 1,000 | empty; 1 KiB; 16 KiB payload; metadata/gates/entity maximums | canonical request/result bytes, peak planner bytes, SQL binds, transaction bytes |
| BatchUpdate | 1, 10, 100, 1,000 | selector-only; payload replace; fields/metadata/gates replace | same observations plus snapshot bytes |
| BatchClaim | 1, 10, 100, configured maximum | ungrouped; grouped; whole cohort | request bytes, returned-item bytes, lock rows, transaction bytes |
| BatchFinalize | 1, 10, 100, 1,000 | complete; retry; failure payload | request/result bytes, replay bytes, transaction bytes |

Every row must run against PostgreSQL-native and SQLite-native with identical
canonical inputs. The result records revision, backend/version, row count,
canonical bytes, payload bytes, bind count, peak process bytes, transaction
duration as capacity evidence, and exact outcome checksum. Timing does not
decide correctness or admission.

## Alternatives

### A. No change

Keep operation-specific row limits and SQL chunking only.

Benefits:

- no contract or implementation change;
- existing small requests are unaffected.

Costs:

- a few large entries can allocate or retain substantially more memory than
  1,000 small entries;
- object-log and relational compositions may reject different request shapes;
- `request_id` replay payloads have no common request-level byte envelope.

Verdict: rejected unless the measurement matrix proves a lower existing bound
already caps every canonical request and replay result.

### B. Reject before start — recommended

Canonicalize and size the entire request before acquiring the queue mutation
gate, beginning a transaction, appending a command, or changing projection
state. Reject if either rows or canonical bytes exceed the contract bound.

Benefits:

- one request has one deterministic admission result on every backend;
- rejection has no durable effect and is safe to retry after reshaping;
- no partial `request_id` record or unknown outcome exists;
- driver chunking remains an internal projection optimization.

Costs:

- canonical sizing adds one bounded pass over the request;
- callers must split oversized workloads and assign distinct request IDs;
- a single item larger than 16 MiB is rejected even when a driver could store it.

Verdict: recommended.

### C. Deterministic internal chunking

Split one client request into stable chunks and execute them sequentially.

Benefits:

- accepts larger logical batches;
- caps each driver statement and transaction working set.

Costs:

- multiple commits cannot preserve API-001's single success/error/unknown
  outcome without a new transaction coordinator;
- cancellation between chunks creates a partial durable effect;
- `request_id` replay must encode chunk progress and resume rules;
- group/cohort claim and finalize atomicity can cross a chunk boundary.

Verdict: rejected for the native mutation contract. Callers may chunk into
separate requests with separate request IDs. Internal SQL statement chunking
inside one already-admitted transaction remains allowed.

## Operation Semantics

| Operation | Row charge | Byte charge | Oversize result | Atomicity consequence |
|---|---:|---:|---|---|
| BatchPush | submitted items | canonical request + reserved ordered result | envelope capacity error | no item accepted; caller may resubmit smaller requests with new request IDs |
| BatchUpdate | update entries | request + snapshot/result reserve | envelope capacity error | no field or schedule change |
| BatchClaim | requested maximum plus selected whole-group/cohort ceiling | request + worst allowed returned-item representation | invalid/capacity error before locks | no lease created; downstream-rate pacing remains absent |
| BatchFinalize | finalize entries | request + reserved replay result | envelope capacity error | no lifecycle transition |

Best-effort per-item outcomes apply only after the envelope passes admission.
Admission does not turn item validation failures into an all-or-nothing batch;
it only decides whether processing may begin.

## Request ID, Retry, and Unknown Outcome

1. Compute the canonical body fingerprint and admission charge from the same
   canonical request representation.
2. If an unexpired `request_id` replay record exists, replay it before applying
   current deployment admission limits; a limit change cannot invalidate a
   previously committed outcome.
3. For a new `request_id`, reject an oversize request before storing a replay
   outcome or beginning durable work.
4. Once admitted, preserve the existing success/error/unknown-outcome contract.
   Timeout or cancellation after the durable boundary resolves through the
   original `request_id`.
5. Reusing the same `request_id` with a smaller or otherwise changed body is a
   request-ID conflict, not a retry of the rejected body.

## Resource and Critical-Section Boundaries

- Canonical sizing happens before the queue mutation gate and before driver
  transaction acquisition.
- The admitted owned request and reserved result bytes count against the
  operation's memory capability until completion or safe handoff.
- PostgreSQL and SQLite may retain their current set-based statement chunks;
  all chunks for one admitted native mutation remain inside its existing
  atomic durable unit.
- No code may await byte capacity while holding a queue serialization lock or
  database transaction. Capacity is acquired or rejected first.
- SQL bind-count assertions remain independent safety checks; byte admission
  does not replace them.

## Security

- Size accounting uses checked arithmetic and fails closed on overflow.
- Admission errors expose the configured limit and requested byte count, not
  payload, metadata, selectors, lease tokens, or SQL.
- A replay lookup remains tenant/queue scoped and authorization precedes any
  returned stored result.
- Byte limits bound memory-amplification denial of service but do not replace
  tenant/global concurrency budgets.

## Testing Required Before Acceptance

- A cross-backend canonical-size fixture proves byte-identical charges for all
  four operations and all matrix entry shapes.
- Boundary tests cover `limit - 1`, `limit`, `limit + 1`, zero rows, 1,000
  rows, one individually oversize item, and arithmetic overflow.
- Rejection tests prove zero log append, zero projection change, zero lease,
  and no replay record.
- Replay tests prove a previously committed outcome survives a lower current
  limit and changed bodies return request-ID conflict.
- PostgreSQL and SQLite tests assert bind counts remain below their driver
  ceilings at the maximum row shape.
- Cancellation tests distinguish pre-admission no-effect from post-admission
  possible commit and replay.
- Claim tests prove there is no downstream-rate, quota, or token-bucket gate.

## Rollout and Rollback

No rollout occurs while this artifact is proposed. If accepted, API-001 must
first define the canonical byte calculation, the 16 MiB value, and the typed
envelope error. Implementation then lands behind conformance tests for every
storage profile.

Rollback removes enforcement only after confirming no deployment depends on
the published bound. SQL statement chunking and the existing 1,000-row safety
ceilings remain in place.

## Risks and Open Review Questions

| Risk / question | Disposition required before acceptance |
|---|---|
| Canonical result reserve undercounts backend-independent replay bytes | Measurement fixture must include maximum ordered per-item outcomes. |
| 16 MiB is too high for constrained deployments | Keep node/tenant concurrency budgets separately configurable; the contract bound is a maximum, not a memory budget. |
| 16 MiB is too low for existing callers | Search published evidence and compatibility tests before API-001 changes; no current contract promises larger requests. |
| Claim response size exceeds request size | Charge the bounded worst-case returned item representation before locks, or define a smaller contract row cap. |
| One transaction internally chunks projection statements | Allowed only while the existing atomic durable unit and replay outcome remain one. |

## Review Disposition

**Proposed**. The recommended choice is row-and-byte reject-before-start with
1,000 rows and 16 MiB canonical encoded bytes. Operator review must accept,
revise, or reject that value and canonical calculation before API-001 or source
changes are authorized. No implementation work is derived by this artifact.
