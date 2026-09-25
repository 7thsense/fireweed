---
ddx:
  id: external-interface-contract-research
  status: draft
---

# External interface contract: research

**Date**: 2026-09-22
**Status**: draft research, not normative
**Question**: What exactly is Fireweed's external interface contract? Is it
tight enough that the same contract could be implemented directly on
PostgreSQL in an isolated unit, without that unit bending the
S3 object-log × Turso production design and without deviating in behavior?

## Verdict

No. Today the external interface is not a contract. It is everything that
has accumulated in `crates/fireweed`, tied to one backend's internals, and
described by documents that contradict the code and each other.

| Measure | Fireweed today | Seventh Sense equivalent |
| --- | --- | --- |
| Queue-facing methods | 78 on `Fireweed` + 10 `operator_*` + 4 on `ProjectionControl` | 3–5 per queue trait |
| Free constructors / helpers | 12 (`open*`, `select_active_scope_from_prefix`) | n/a (DI) |
| Public types | ~40 defined in `fireweed` + ~115 re-exported from core/engine | a case class per queue |
| Backend internals exported | `pub mod turso_compose` (backend structs, assembly fns) | none |
| Methods Fireweed's own Seventh Sense workload tests call | 12 | — |
| Methods Snorri calls (pinned `v0.31.21`) | ~24 | — |

The semantic core in API-001 is reasonable, and it can be kept. What
surrounds it is not a contract: aliases, stubs, test fixtures, retired
selectors kept only so they can be rejected, backend introspection, and
methods that have no normative text at all.

## 1. The baseline: what Seventh Sense actually needs

The Seventh Sense Postgres queues (`7thsense/core/api/src/main/scala/ss/api/`):

| Trait | Methods |
| --- | --- |
| `TJobsQueue` / `TConnectorsQueue` | `create(items)` (insert, conflict-ignore on natural key); `handleNext(type, asOf, timeout)(State => State)` (take one due, eligible item `FOR UPDATE SKIP LOCKED`, run effect, then update `next_at`/`inactivity_count` or delete); `deleteByIds` |
| `TMarketoEvent*EnrichmentQueue` | `create`; `takeNextTasks(asOf, max)` (whole groups by lead, oldest first, within a group-count limit); `hasNextTasks(asOf, maxGroups, maxWait)`; `updateRetryInfo(tasks)`; `delete(ids)` |
| `TCallbackQueuedActionsPersistence` | `createOrUpdate(actions)`; `findByJobId(jobId, max)`; `deleteByIds` |
| `TQueueReader` / `TQueueWriter` (Kafka) | `stream(params)(batch => BatchResult)`; `offerAll(items)` |

Four primitives cover all of these:

1. **Idempotent create / upsert** keyed by a caller identity.
2. **Take**: due (`not_before <= asOf`), eligible (the gate predicate, e.g. account
   enabled and connector configured), ordered (oldest first), bounded, and
   exclusive. Groups can be whole.
3. **Settle** a taken item: done (delete), or reschedule with new state
   and `not_before` (retry/backoff, recurring re-arm).
4. **Delete** by key.

FEAT-seventh-sense-validation (FR-44..47c, FR-55) maps onto exactly these:
timestamp priority, eligibility as gates, reschedule, group claim, cohort,
and recurring re-arm.

## 2. What the "contract" is today (verified)

### 2.1 Surface layering

```
Fireweed                              facade.rs:1020  (78 pub methods)
 ├─ Arc<dyn FireweedDataPlane>        facade.rs:16    (65 trait methods)
 ├─ Arc<dyn FireweedBatchPlane / MutationPlane / OperatorPlane>
 ├─ Option<ProjectionLifecycleHandle>
 └─ snapshot_policies: Mutex<HashMap<..>>   (facade-local state)
        │ sole impl
        ▼
RuntimeCore<B: LibBackend>            facade.rs:560
        │ LibBackend = 21 engine traits    lib.rs:3055
        │ (incl. SnapshotStore, HistoricalProjectionRead,
        │  ProjectionRead, UpdateFieldsPort, ControlPlaneStore)
        ▼
AtomicTursoBackend<L> / DerivedObjectLogTursoBackend   (turso_compose.rs, pub)
PostgresRelationalBackend (15.2k lines)  PostgresBackend (log-replay)
```

Every storage implementation must satisfy a 21-trait bound that includes log and
projection concepts. This is the mechanism that has already bent the design
(§4).

### 2.2 Classification of the public methods

| Class | Methods | Notes |
| --- | --- | --- |
| **Semantic core** | `create_queue`, `queue_definition`, `push_batch_with_request_id`, `upsert`, `claim_with`/`claim_at` (one of), `claim_by_query`, `claim_by_item_ids`, `commit`, `renew`, `batch_update`, `set_gates`, `purge`, `live_items`, `metrics`, `explain_commit` | ~15. Everything the baseline needs is here. |
| **Aliases / convenience** | `push`, `push_batch`, `push_with_request_id` (→ push batch w/ request id); `ack` ≡ `complete`; `nack`, `retry`, `release`, `retry_after`, `nack_retry_after`, `fail` (→ `commit` outcomes); `claim`, `claim_response_with`, `claim_response_at`, `claim_by_query_at` (→ one claim); `reclaim_expired`/`_at`, `rearm`/`_at`/`_after`; `discover`/`discover_active_scopes`/`_stamped`; `query_index`/`_unique`/`_typed`/`_unique_typed`; `live_item` (→ `live_items`); `side_record`/`side_records`/`side_records_by_prefix` | ~30. Each alias has to be implemented and tested for every backend; `FireweedDataPlane` carries them all. |
| **Backend leaks** | `commit_capabilities` (`DurabilityClass`), `hot_projection_capabilities`, `current_position` (`CommandPosition`), `projection_control`, `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `compact_log_behind_snapshot`, `snapshot_now`, `latest_snapshot_info`, `last_rebuild_stats`, `peek` | These describe the object-log + projection design, not a queue. |
| **Stubs / fixtures** | `last_rebuild_stats` → always `None` (facade.rs:1434); `compact_log_behind_snapshot` → always `Unavailable` after checks (facade.rs:1394–1419), which API-005 §Queue operations forbids; `open_product` starts/reuses a MinIO test env and panics on error (lib.rs:5981); `should_snapshot` takes `log_command_count` from the *caller* (facade.rs:1379) | Test and scaffolding code published as API. |
| **Overlapping mutation primitives** | `batch_update`, `mutate_items`, `bounded_mutation`, `reschedule`, `set_gates`, `commit`, `commit_multi_claim`, `fail`, `purge`, `operator_repair` | Ten ways to change item state. API-001 distinguishes some by authority level; none are specified as compositions of fewer primitives. |
| **Hot-projection query surface** (API-004) | `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `metrics_by_query` | Used by Snorri. Real product surface, but it is a separate facet. |
| **Operator** (API-002) | 10 `operator_*` | No application consumer. Admin plane. |
| **Coordination** | `ownership`, `renew_owned`, `claim_across_queues`, `reassign`, `claimed`, `retained_items` | Mostly unused outside tests. |
| **Retired config kept for rejection** | `LogConfig` (5 variants, 1 valid), `ProjectionStoreConfig` (4, 1 valid), `ProjectionConfig` (all retired), `ResponseBarrier` (1 variant), `ObjectLogAuthority` (1 variant), `StorageConfig::sqlite_projection_deferred_flush_chunk` (must be `None`), `PostgresMode`, `PostgresRuntimeConfig`, `PostgresCoordinationConfig`, `open_objectlog` | Types that exist so that validation can reject them. |

`pub mod turso_compose` (lib.rs:52, default feature `turso`) exports
`AtomicTursoBackend`, `DerivedObjectLogTursoBackend`, `assemble_*_turso`,
`open_turso_projection` returning `TursoRelational`, and
`rebuild_filesystem_turso_projection`. This violates ADR-009 and API-005
("callers never name a backend").

### 2.3 Document ↔ code drift

- API-005 says `open_postgres*` "reject before storage I/O with
  `RETIRED_STORAGE_CELL`". `open_postgres_runtime` (lib.rs:6230, feature
  `postgres`) opens a working `PostgresRelationalBackend` or log-replay backend.
- API-005's body says only S3 × Turso validates; its validation checklist
  says "document the full 4×3 matrix; filesystem and s3 are first-class logs".
- The API-005 error table directs callers to `open_objectlog_postgres_async`, a
  retired helper. The contract still reasons about `BlockingLibBackend`, which
  has been removed.
- These methods are absent from the API-005 inventory: the snapshot family,
  `side_records*`, `retained_items`, `claim_response_*`, `peek`; `reschedule`
  appears only in the Snorri list.
- These methods have no normative semantics in any API doc: `metrics_by_query`,
  the `query_index*` family, `select_active_scope_from_prefix`,
  `side_records_by_prefix`, `retained_items`.
- `open_objectlog`'s docstring describes an object-log runtime; its body
  returns `RETIRED_STORAGE_CELL`.
- Snorri (the named release-acceptance client) pins `v0.31.21` and imports
  `open_memory`, `open_sqlite`, `open_objectlog_sqlite`, which do not exist on
  `main`. The acceptance gate is not being exercised against the current
  surface.

### 2.4 Backend baked into semantics

API-001 principle 3 (line 90): "a successful mutating response means the
accepted effects are durable **on the object log** … On `AsyncProjection`, a later
ordinary claim polls applied rows and may be empty." API-005 §Concurrency 5:
"durable on the S3 object log". API-001 principle 2 requires each queue to expose
"a serializable mutation history", which is a log-shaped obligation.

The underlying semantic is legitimate: **bounded-stale visibility; an empty
claim is a poll, not a failure**. It is stated in terms of the mechanism, and no
bound is given. A Postgres implementation can satisfy it trivially (staleness 0),
but only if it is stated as a semantic.

## 3. Where executable semantics live

| Suite | Tests against | Depth | Portable to a direct-PG impl? |
| --- | --- | --- | --- |
| `fireweed-conformance` (8.3k lines) | Engine ports: `ConformanceBackend = … + SnapshotStore + LogRead` (lib.rs:97–135) | Deep: replay, fencing, cohorts, claim ordering, unknown-outcome | No. It requires log and snapshot traits. |
| `crates/fireweed/tests/support/public_interface.rs` (2.9k lines) | `&Fireweed`: `run(cell, &Fireweed, expect_projection_control)` | Shallow breadth: "did not lease exactly one item" per method | Yes in shape. It has backend knobs (`expect_projection_control`, `expect_atomic_commit`). |
| Other facade tests (`p5as3_*`, `p3b_*`, `public_interface_conformance.rs`) | `Fireweed` | Mixed | Mixed in with Turso-specific assertions (`projection_sequence`, `tail_commands_replayed`, `RETIRED_STORAGE_CELL`, `apply_lag_max_commands` messages) |

The deep semantic specification is executable only against the log+projection
engine. The public-level suite has the right shape but only smoke depth.

## 4. The degradation has already happened once

`PostgresRelationalBackend` is a direct-Postgres implementation. It was
plugged in at the **engine-port** seam, and that seam absorbed it:

- `LibBackend` requires `SnapshotStore`, `HistoricalProjectionRead`, `ProjectionRead`
  of every backend, including ones with no log or projection.
- Lowest-common-denominator defaults: `ProjectionRead::pending_summary/pending_page`
  materialize all pending items in memory by default (port.rs:510–555,
  "preserves source compatibility for external backends").
- Capability branching: `Backend::supports_gates() -> false` default,
  `hot_projection_capabilities()` all-false default, `DurabilityClass`
  Atomic/EventualApply, `commit_capabilities` flags. These are then re-exported
  as public API.
- Dialect forks: relational apply is shared for SQLite/Turso and forked for
  Postgres (`one-projection-cleanup.md`).

**Lesson: the seam for a second implementation must be the external contract,
not an internal port.** If the second implementation shares a mid-level
abstraction, that abstraction will grow to fit both designs.

## 5. Proposed shape

### 5.1 Three layers, one of them normative

1. **Contract kernel**: the only thing both implementations implement. It is small,
   batch-first, and every operation takes a `request_id`.
2. **Derived helpers**: aliases and conveniences, implemented **once** on top
   of the kernel in a shared crate. They cannot drift because neither
   implementation provides them.
3. **Implementation-private admin**: projection control, snapshots,
   compaction, positions, and capability introspection. These come from the
   backend-specific constructor as a separate handle and are never on the
   contract type.

### 5.2 Candidate kernel (for discussion)

| Op | Subsumes | Baseline primitive |
| --- | --- | --- |
| `create_queue(def)` / `queue_definition` | `ensure_queue` (helper) | — |
| `push(queue, request_id, items)` → per-item outcome | `push`, `push_batch`, `*_with_request_id`; `upsert` is a per-item key disposition (**decision**) | create / upsert |
| `claim(queue, request_id, ClaimRequest { selector: Priority \| Query \| ItemIds, max, lease, at, compatibility })` → `Claimed` | all 9 claim variants | take |
| `settle(queue, request_id, entries[{claim_ref, Complete \| Fail \| Retry{not_before} \| Release \| Rearm, patch, side_records}])` → per-entry outcome | `commit`, `commit_multi_claim`, `ack`, `complete`, `nack`, `retry*`, `release`, `fail`, `rearm*` | settle |
| `renew(queue, claim_refs, lease)` | `renew`, cohort renew | — |
| `update(queue, request_id, BatchUpdateRequest)` | `batch_update`, `reschedule`; relation to `mutate_items` and `bounded_mutation` is a **decision** | reschedule |
| `set_gates(queue, changes)` | — | eligibility |
| `purge(queue, ids)` | `purge`, `operator_purge`? | delete |
| `get(queue, keys \| ids)` | `live_item(s)`, `claimed`, `peek`? | — |
| `metrics(queue)` | — | — |
| `explain(queue, request_id)` | `explain_commit` | — |

That is roughly 11 operations, compared with 78+10+4 today. The optional facets
below would be named separately, and each would be all-or-nothing per
implementation: **Query** (API-004 range/aggregate/bucket/index/metrics-by-query,
bounded mutation), **Discovery** (active scopes), **Operator** (API-002),
**Side records** (if not folded into `settle`).

### 5.3 Contract-level semantics to pin down

- **Visibility.** Mutations are durable when acknowledged. Their effects on claim
  and read are visible within a bounded but unspecified staleness, and an empty
  claim is a poll. A stronger implementation (Postgres, staleness 0) conforms. The
  weaker Turso model defines the contract.
  - *Risk:* clients developed against the stronger implementation silently depend on
    read-your-writes. Mitigation: the contract test suite runs every
    implementation under an **injected-lag adversary**, or the direct-PG
    implementation gets an optional lag-injection mode for client testing.
- **Durability.** Remove the phrase "object log". Say: "acknowledged ⇒ survives
  process loss; replay with the same `request_id` returns the recorded outcome for
  at least `request_id_retention_ms`."
- **History** (API-001 principle 2). Decide whether a queue mutation history is an
  *external* obligation (change records, TD-008) or an internal mechanism.
  This single sentence decides whether a direct-PG implementation needs an outbox.
- Items the semantics audit flagged as underspecified: cohort-expiry timing
  (lazy or eager), whether request-id retention is exact or at-least,
  torn-read tolerance for bucket null counts, and batch/resume semantics for
  large operator selectors.
- **Ordering.** The tie-break rules in API-001's eligibility precedence must be
  exact, so that both implementations return the same claim order for the same
  state.

### 5.4 Isolation rules for a direct-PG implementation

1. **Seam = the kernel trait** in a small crate (DTOs + trait + `EngineError`). No
   engine types, no storage traits.
2. **Shared semantics = pure functions**: validation, eligibility precedence,
   retry/backoff math, request-body fingerprinting, cohort rules. These go in a
   crate with no I/O and no traits that storage implements. Both implementations
   call them; neither shapes them.
3. **Dependency direction**: `fireweed-pg` → {kernel, pure-semantics}. It never
   depends on engine, turso, objectlog, or relational. Nothing in the
   production path depends on `fireweed-pg`.
4. **The kernel crate grows only by spec**: every public item traces to a
   normative clause (a manifest check in CI). An addition "because Postgres
   needs it" without a spec clause is rejected.
5. **Certification = one black-box contract suite** parameterized over a
   constructor, with no backend knobs. It holds deep semantic scenarios lifted
   from `fireweed-conformance` plus the breadth harness. Backend-specific tests
   stay in backend crates.
6. **Retire the port-level Postgres backends** (`PostgresBackend` log-replay,
   `PostgresRelationalBackend`) when the new unit lands. Their SQL is useful
   material to harvest; their seam is the problem.

## 6. Decisions needed from the owner

1. **Contract scope.** Is the kernel the baseline four primitives plus
   renew/gates/metrics/explain, with API-004 query, discovery, operator, and
   multi-queue claim as optional facets? Or must every implementation provide all
   of them?
2. **Upsert.** Keep it as a separate operation, or make it a per-item key
   disposition on `push`?
3. **Mutation primitives.** Which of `batch_update`, `mutate_items`, and
   `bounded_mutation` survive as contract operations?
4. **Side records.** Are they part of `settle` (Snorri's usage), or a separate
   facet?
5. **Mutation history.** Is it an external obligation or internal?
6. **Compatibility.** Is a pre-1.0 breaking cut of the facade acceptable (ADR-023
   precedent: hard cutover, no alias layer)? And is Snorri re-pinned as part of it?

## 7. Suggested sequence

1. Write the kernel contract (a new API doc, or a rewrite of API-005 with a
   trimmed API-001). Move backend-specific text to ADR-024 / TD-010.
2. Produce a per-method disposition table (kernel / helper / facet /
   backend-admin / delete) against §2.2.
3. Extract the kernel crate and the black-box contract suite. Run the suite
   against S3 × Turso until it passes with no backend knobs.
4. Build the isolated Postgres unit against the kernel.

Steps 1–3 improve the production backend on their own, whether or not step 4
happens.
