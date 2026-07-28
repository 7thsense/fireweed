---
ddx:
  id: adr-log-single-source-of-truth
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - adr-orthogonal-log-projection-composition
    - orthogonal-storage-matrix-brief
  review:
    self_hash: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-orthogonal-log-projection-composition: 778fdbadeadce6b52e101bda39921f88b193c5737ea96d4b8ae8e8a424a4e743
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
    reviewed_at: "2026-07-20T00:01:23Z"
---

# Architecture Decision Record

**ADR ID**: ADR-013
**Title**: The durable command log is the single source of truth (Class A); Class B memory log is an explicit weaker durability class
**Status**: Accepted
**Related**: ADR-001 (CQRS log/projection — intent ratified), ADR-008 (queue as shard unit & two
projection families — **amended**: the families remain, their authority claim does not), ADR-012
(orthogonal composition), TD-007 (durability), TD-008 (change-record emission — depends on this ADR),
TD-009 (experimentation surface — depends on this ADR),
`orthogonal-storage-matrix-brief` (durability Class A vs Class B; governing product intent).

## Context

ADR-001 already declares the durable command log the source of truth
(ADR-001-cqrs-log-projection-storage-model.md:123-130). The code has drifted into two contradictory
durability contracts sharing one crate:

- **Log-authoritative** (the CQRS intent): `crates/fireweed-postgres/src/lib.rs:6-13` — "the command LOG
  is durable in postgres … The log rows are the source of truth (CQRS); the in-memory projection is a
  derived view that any reopen reconstructs." Write ordering is durable-first (`:14`).
- **DB-authoritative** (the drift): `crates/fireweed-postgres/src/relational.rs:3,2767` — "a
  **DB-authoritative** projection … `fireweed_items` is DB-authoritative"; on reconnect "The item
  projection itself is already durable in `fireweed_items` — nothing to replay" (`:983-987`). The sqlite
  relational family shares this stance.

ADR-008 legitimized the split as "two projection families." The cost has become visible: a projection
that mutates in place with no authoritative log cannot reproduce a prior state, which blocks
branch-at-position, read-as-of-position, and change-record emission guarantees (TD-008/TD-009) for the
entire relational family. External motivation: the Lakebase/LTAP storage model (2026-07-03 review)
demonstrates the same discipline — the log/lake is the sole truth; row stores are caches.

Separately, the orthogonal storage matrix (`orthogonal-storage-matrix-brief`) makes **log** and
**projection** independent public axes. Every cell remains `LogStore × ProjectionStore`, but
persistence guarantees differ by **durability class**: Class A (durable log backends) vs Class B
(`log=memory`). An earlier absolute ban on any non-durable production log posture conflicted with
explicit Class B product intent; this amendment aligns ADR-013 with that brief.

## Decision

CQRS composition is universal: every supported cell is still
`LogStore × ProjectionStore` (ADR-012), with append → apply → acknowledge for the selected
durability class. There is no architecture that drops `LogStore` and runs projection-only.

Durability semantics split by class:

### Class A — Durable log is the single source of truth

**Applies when log is `sqlite`, `postgres`, `filesystem`, or `s3`.**

**The durable command log is the single, authoritative system of record for every queue. All
projections — including the relational family (`fireweed_items` and peers) — are rebuildable, disposable
views derived solely from the committed log via `ProjectionData::apply_command` /
`ProjectionStore::apply`.** No projection may hold acknowledged state that is not reconstructable by
replaying the log from genesis or from a snapshot at a committed `CommandPosition`.

This ratifies the `ComposedBackend` recovery contract as the Class A invariant:
`recovery_high_water` (`crates/fireweed-engine/src/compose.rs:636`) plus tail replay (`:1196-1239`)
must be able to reconstruct any projection, and `resolve_recovery_start` (`:389-404`) governs trust in
a projection's recorded high-water.

**Commit ordering (Class A): (1) the command is fully durable in the log, (2) the serving projection
applies it, (3) only then is success returned to the client.** No Class A backend may acknowledge
before the log commit is durable, and no backend may acknowledge before the operation's own effects
are visible through its serving projection (the response barrier). The atomic class satisfies this
inside one transaction; the log-then-apply class via the manifest-commit + response-barrier sequence
(TD-007 §1). Client contract: success ⇒ durable on log and visible in serving projection; recovery
via high-water + tail replay; `request_id` resolves ambiguity across crash.

Class A rules for projections, recovery, branch, read-as-of, and change-record-from-log are unchanged
by this amendment relative to the original ADR-013 stance.

### Class B — Memory log (explicit weaker durability class)

**Applies when log is `memory` (paired with any public projection: `memory`, `sqlite`, or `postgres`).**

Class B is a weaker **persistence envelope**, not a second architecture and not “no LogStore”:

1. **`LogStore` still exists** for in-process command ordering, fencing, and the append → apply
   path while the process is alive.
2. **After process death, only the projection remains.** There is no durable log to replay; recovery
   cannot rebuild the projection from the log.
3. **Unavailable under Class B:** log rebuild / crash-recovery-from-log, branch-at-position (TD-009),
   read-as-of-position (TD-009), and change-record emission from a durable log tail (TD-008).
4. **Client contract:** success ⇒ visible in the serving projection; durable **iff** the projection
   itself is durable (`sqlite` / `postgres`). A `memory` × `memory` cell loses both log and
   projection on process death.
5. **Must be explicitly selectable** via the public configuration surface (typed `StorageConfig` /
   Helm axes — see the matrix brief). Configuration MUST NOT silently substitute a no-op or absent
   log; operators and embedders choose `log=memory` knowingly.
6. **Must not claim Class A guarantees.** Docs, preview claims, and conformance must not market
   Class B cells as log-rebuildable, branchable, or change-record-complete from log.

**Commit ordering (Class B):** (1) append to the in-process `LogStore` (ordering/fence authority for
the live process), (2) apply to the serving projection, (3) acknowledge only after projection
visibility. The response barrier still holds for the live process; cross-restart durability is
projection-only.

### Silent null-log remains forbidden

A **silent** or **implicit** log-less / projection-only mode (no `LogStore` in the composition, or a
no-op log slipped in without explicit Class B selection) is not a supported product posture. Class B
replaces that idea with an explicit `log=memory` cell and documented weaker guarantees.

A pure no-op log implementation MAY exist **for tests only**. It MUST NOT be selectable as a
production log backend under another name; the only public non-durable log value is `memory`
(Class B).

### What changes for the relational family

These requirements apply under **Class A** (and whenever a durable log is composed with the
relational projection):

1. The word "authoritative" applied to `fireweed_items` is retired. The relational projection becomes a
   **materialized cache with a persisted applied-high-water**, exactly like the sqlite hybrid
   projection's `sqlite_high_water`.
2. The Postgres/sqlite relational backends MUST persist the durable command log (they already write
   log rows + `high_water` — `crates/fireweed-postgres/src/lib.rs:105,241`) and MUST implement
   `recovery_high_water` to return their applied position and replay the tail, rather than returning
   "nothing to replay" (`relational.rs:983-987`). The projection tables are truncatable and
   rebuildable from the log.
3. Concurrency correctness that today leans on "the row IS the truth" (`relational.rs:11-19`) is
   re-expressed as "the row is a cache guarded by the same pre-commit validation every other
   projection uses" — the log append remains the ordering/fence authority
   (`compose.rs:1358-1364`).

Under Class B with a relational projection, the projection may still persist applied state, but
post-restart authority is projection-only; Class A rebuild-from-log claims do not apply.

## Derived implementation work

The migration itself is intentionally out of scope for this ADR. The follow-up beads derived from this
decision were filed and are now **closed** (bead `pqueue-3c5aa2e0`, "Relational family rebuild-from-log
migration", plus its five children):

- Rework the relational backend recovery path so `recovery_high_water` returns the applied position and
  replays the log tail instead of treating `fireweed_items` as durable truth — **done**; both the Postgres
  and sqlite relational stores implement `recovery_high_water` and describe `fireweed_items` as a
  rebuildable cache (e.g. `crates/fireweed-postgres/src/relational.rs:995,5040`).
- Persist the relational family applied-high-water in both Postgres and sqlite relational projection
  implementations so they are rebuildable caches rather than authoritative stores — **done**.
- Add migration coverage for branch-at-position, read-as-of-position, and change-record emission against
  the rebuilt-from-log relational family — **done** (relational log conformance class green,
  bead `pqueue-219a4ee7`).

One deliberately-scoped exception remains: ADR-012's decision note (2026-07-08, DDx B3.6) **retains** the
monolithic DB-authoritative `SqliteRelationalBackend` as a differential test oracle and benchmark shape.
That does not contradict this ADR — its only non-test construction site is the benchmark harness; it is
not reachable through any production configuration surface. Production composition remains
`LogStore × ProjectionStore` (Class A durable log or explicit Class B `memory` log). Its eventual
retirement conditions are tracked in ADR-012.

Class B wiring, matrix completeness, and public messaging are governed by
`orthogonal-storage-matrix-brief` (Phase 0+), not by reopening the relational rebuild migration.

## Consequences

- Positive (Class A): one log-authoritative durability contract; branch, read-as-of, and emitted
  history become possible for every projection family; ADR-001's durable-log intent holds in code.
- Positive (Class B): embedders and tests may select `log=memory` explicitly for ephemeral or
  projection-durable-only deployments without inventing a second architecture; CQRS composition
  stays intact.
- Negative (Class A): the relational family pays replay cost on cold recovery; single-node
  deployments that might have preferred projection-only authority still carry durable-log write
  amplification when Class A is selected.
- Negative (Class B): no log rebuild, branch, read-as-of, or change-record-from-log; operators must
  not confuse Class B with Class A; preview and support claims must name the durability class.

## Prerequisite

Making the append-then-apply seam (`compose.rs:1346-1366`) the sole serialization point removes the
relational family's "concurrency-correct by construction" story (`relational.rs:11-19`). The Postgres
high-water guard and `MAX(seq)+1` append allocation carry a documented TOCTOU under connection pooling
(`crates/fireweed-postgres/src/lib.rs:16-46`). **The TOCTOU fix is a hard prerequisite for this ADR to be
safe multi-node under Class A**; it was tracked as the blocking bead `pqueue-b59f4897` in this ADR's
implementation chain and is now **closed** (verified as part of the rebuild-from-log migration, bead
`pqueue-3c5aa2e0`).
