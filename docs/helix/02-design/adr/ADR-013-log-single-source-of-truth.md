---
ddx:
  id: adr-log-single-source-of-truth
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - adr-orthogonal-log-projection-composition
  review:
    self_hash: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
    deps:
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-orthogonal-log-projection-composition: 72e7c4701c344732c61b2b63043e70024bbff6228b841b8d76dffbb2d5bc4fd5
      adr-queue-as-shard-unit-and-projection-families: ec3e51c1da5d66a2601bbe593a4a45b721eaa0db2284e6bfc27d2222c1ffe0c8
    reviewed_at: "2026-07-18T02:36:05Z"
---

# Architecture Decision Record

**ADR ID**: ADR-013
**Title**: The durable command log is the single source of truth; every projection — including the relational family — is a rebuildable view
**Status**: Accepted
**Related**: ADR-001 (CQRS log/projection — intent ratified), ADR-008 (queue as shard unit & two
projection families — **amended**: the families remain, their authority claim does not), ADR-012
(orthogonal composition), TD-007 (durability), TD-008 (change-record emission — depends on this ADR),
TD-009 (experimentation surface — depends on this ADR).

## Context

ADR-001 already declares the durable command log the source of truth
(ADR-001-cqrs-log-projection-storage-model.md:123-130). The code has drifted into two contradictory
durability contracts sharing one crate:

- **Log-authoritative** (the CQRS intent): `crates/pqueue-postgres/src/lib.rs:6-13` — "the command LOG
  is durable in postgres … The log rows are the source of truth (CQRS); the in-memory projection is a
  derived view that any reopen reconstructs." Write ordering is durable-first (`:14`).
- **DB-authoritative** (the drift): `crates/pqueue-postgres/src/relational.rs:3,2767` — "a
  **DB-authoritative** projection … `pqueue_items` is DB-authoritative"; on reconnect "The item
  projection itself is already durable in `pqueue_items` — nothing to replay" (`:983-987`). The sqlite
  relational family shares this stance.

ADR-008 legitimized the split as "two projection families." The cost has become visible: a projection
that mutates in place with no authoritative log cannot reproduce a prior state, which blocks
branch-at-position, read-as-of-position, and change-record emission guarantees (TD-008/TD-009) for the
entire relational family. External motivation: the Lakebase/LTAP storage model (2026-07-03 review)
demonstrates the same discipline — the log/lake is the sole truth; row stores are caches.

## Decision

**The durable command log is the single, authoritative system of record for every queue. All
projections — including the relational family (`pqueue_items` and peers) — are rebuildable, disposable
views derived solely from the committed log via `ProjectionData::apply_command` /
`ProjectionStore::apply`.** No projection may hold acknowledged state that is not reconstructable by
replaying the log from genesis or from a snapshot at a committed `CommandPosition`.

This ratifies the `ComposedBackend` recovery contract as the universal invariant:
`recovery_high_water` (`crates/pqueue-engine/src/compose.rs:636`) plus tail replay (`:1196-1239`)
must be able to reconstruct any projection, and `resolve_recovery_start` (`:389-404`) governs trust in
a projection's recorded high-water.

**Commit ordering is universal and non-negotiable: (1) the command is fully durable in the log,
(2) the serving projection applies it, (3) only then is success returned to the client.** No backend
may acknowledge before the log commit is durable, and no backend may acknowledge before the
operation's own effects are visible through its serving projection (the response barrier). This holds
for every durability class: the atomic class satisfies it inside one transaction; the log-then-apply
class via the manifest-commit + response-barrier sequence (TD-007 §1). There is no configuration in
which an acknowledged command can be lost or can race its own visibility.

### What changes for the relational family

1. The word "authoritative" applied to `pqueue_items` is retired. The relational projection becomes a
   **materialized cache with a persisted applied-high-water**, exactly like the sqlite hybrid
   projection's `sqlite_high_water`.
2. The Postgres/sqlite relational backends MUST persist the durable command log (they already write
   log rows + `high_water` — `crates/pqueue-postgres/src/lib.rs:105,241`) and MUST implement
   `recovery_high_water` to return their applied position and replay the tail, rather than returning
   "nothing to replay" (`relational.rs:983-987`). The projection tables are truncatable and
   rebuildable from the log.
3. Concurrency correctness that today leans on "the row IS the truth" (`relational.rs:11-19`) is
   re-expressed as "the row is a cache guarded by the same pre-commit validation every other
   projection uses" — the log append remains the ordering/fence authority
   (`compose.rs:1358-1364`).

### The log is mandatory (no production null-log mode)

Every production deployment MUST run with a durable command log. There is no supported log-less or
projection-only durability posture, even where the projection store itself is highly durable (e.g. a
managed Postgres projection): losing acknowledged data is never acceptable for the workloads pqueue
serves, and a projection without a log cannot reproduce a prior state, cannot guarantee change-record
emission, and leaves the acknowledgement path racing projection durability. An earlier draft of this
ADR allowed a named, telemetry-surfaced degraded `null-log` mode; **that allowance is retired
(product-owner decision, 2026-07-05)**.

A no-op log implementation MAY exist **for tests only**. It MUST NOT be selectable through any
production configuration surface (env parsing, Helm values, and static validation MUST reject it).
The losses that made null-log unacceptable are the reasons the log is mandatory:

- **No log replay / no crash-recovery-from-log** — recovery depends entirely on the projection's own
  durability; a corrupt projection is unrecoverable.
- **No branch** (TD-009) — there is no log to copy-on-write or replay into a branch.
- **No read-as-of-position** (TD-009) — no historical positions exist; only "now."
- **No change-record emission guarantee** (TD-008) — the niflheim sink is a log-tail consumer; with no
  log there is no ordered, replayable tail, so downstream history cannot be guaranteed complete or
  ordered.
- **Weakened idempotency/fence recovery** — request-id replay records and instance fences must be
  reconstructed from projection rows, not replayed; any gap in projection durability is a gap in the
  idempotency contract.

## Derived implementation work

The migration itself is intentionally out of scope for this ADR. The follow-up beads derived from this
decision were filed and are now **closed** (bead `pqueue-3c5aa2e0`, "Relational family rebuild-from-log
migration", plus its five children):

- Rework the relational backend recovery path so `recovery_high_water` returns the applied position and
  replays the log tail instead of treating `pqueue_items` as durable truth — **done**; both the Postgres
  and sqlite relational stores implement `recovery_high_water` and describe `pqueue_items` as a
  rebuildable cache (e.g. `crates/pqueue-postgres/src/relational.rs:995,5040`).
- Persist the relational family applied-high-water in both Postgres and sqlite relational projection
  implementations so they are rebuildable caches rather than authoritative stores — **done**.
- Add migration coverage for branch-at-position, read-as-of-position, and change-record emission against
  the rebuilt-from-log relational family — **done** (relational log conformance class green,
  bead `pqueue-219a4ee7`).

One deliberately-scoped exception remains: ADR-012's decision note (2026-07-08, DDx B3.6) **retains** the
monolithic DB-authoritative `SqliteRelationalBackend` as a differential test oracle and benchmark shape.
That does not contradict this ADR — its only non-test construction site is the benchmark harness; it is
not reachable through any production configuration surface (`PQUEUE_PROJECTION_BACKEND` composes every
projection axis, including `sqlite`/`postgres` relational, with a durable log), so the mandatory-log
invariant holds. Its eventual retirement conditions are tracked in ADR-012.

## Consequences

- Positive: one durability contract; branch, read-as-of, and emitted history become possible for every
  family; ADR-001's stated intent is finally true in code.
- Negative: the relational family pays replay cost on cold recovery it currently avoids; migration
  work to add `recovery_high_water` + rebuildability; single-node relational deployments that might
  have preferred a projection-only posture must carry the log's write amplification — accepted, the
  log is mandatory.

## Prerequisite

Making the append-then-apply seam (`compose.rs:1346-1366`) the sole serialization point removes the
relational family's "concurrency-correct by construction" story (`relational.rs:11-19`). The Postgres
high-water guard and `MAX(seq)+1` append allocation carry a documented TOCTOU under connection pooling
(`crates/pqueue-postgres/src/lib.rs:16-46`). **The TOCTOU fix is a hard prerequisite for this ADR to be
safe multi-node**; it was tracked as the blocking bead `pqueue-b59f4897` in this ADR's implementation
chain and is now **closed** (verified as part of the rebuild-from-log migration, bead `pqueue-3c5aa2e0`).
