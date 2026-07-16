---
ddx:
  id: plan-hybrid-sqlite-inmemory-projection
  depends_on:
    - adr-orthogonal-log-projection-composition
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
    - tp-verification-acceptance-criteria
  review:
    self_hash: eefa6005730f6a31933ab8d9c7ddee9412a09d88d252b1b3bbb91f2d2febea06
    deps:
      adr-orthogonal-log-projection-composition: 46327f801156492ee0a1ad0038b730dea7fcef4ebe00641e8f7d9d5f86f8b3f2
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
      td-storage-architecture-backend-contracts: 430d0dc1f83fa62aeb19948efd2a84f5c31df7d15195e51c8296c93c711919f5
      tp-verification-acceptance-criteria: ef7d361e7736e99e509f94bbc0b0d435eef558851bc6272527781efa91e5ec08
    reviewed_at: "2026-07-16T22:35:21Z"
---

# Hybrid SQLite + In-Memory Projection Implementation Plan

> **Status (2026-07, post-v0.11.0): EXECUTED.** This plan is retained as the record of intent; the
> "Current State" and "Goal" sections below describe the repository as it was when the plan was
> written. What shipped since:
>
> - `objectlog/hybrid` landed and was released in **v0.6.0** (docs/releases/v0.6.0.md), wired through
>   `PQUEUE_PROJECTION_BACKEND=hybrid` (`crates/pqueue-server/src/env_config.rs`) with the
>   SQLite-first apply, projection-image hydration recovery, fail-closed poisoning, and durable
>   request-id replay this plan specifies.
> - Two sibling profiles followed under TD-004: `objectlog/hybrid-strict` (SQLite durable **before**
>   memory apply on the group-commit path) and `objectlog/hybrid-async` (deferred async SQLite
>   checkpoint with debt/backpressure admission gating, high-water withholding, and debt-gated
>   terminal-item retention advancement), both wired in `env_config.rs`/`lib.rs` and implemented in
>   `crates/pqueue-sqlite/src/relational.rs`, current through **v0.11.0**.
> - Known open residual (unchanged from this plan's snapshot-authority stance): object-log
>   **segment-object reclamation remains deferred** pending a bounded-recovery retention floor
>   (bead `pqueue-b5cc2bc7`); segment expiry stays disabled, exactly as required below.

## Goal

Implement the missing `Hybrid` projection axis described by ADR-012: a single
`ProjectionStore` that serves hot reads and pre-commit validation from the
in-process `ProjectionData` model while durably applying every committed batch to
`SqliteProjectionStore`. The first wired runtime target is
`PQUEUE_LOG_BACKEND=objectlog` with `PQUEUE_PROJECTION_BACKEND=hybrid`.

This is not already complete. The current repository has `objectlog/inmemory`
and `objectlog/sqlite` as separate runtime profiles. It does not have one
hybrid projection axis that combines local in-memory serving speed with durable
SQLite snapshot/high-water recovery.

## Current State

- `pqueue-projection::InMemoryProjection` is the fastest read model and already
  implements the full `ProjectionStore` surface over per-queue `ProjectionData`.
- `pqueue-sqlite::SqliteProjectionStore` persists queue definitions, item state,
  metrics, commit-class state, and per-queue recovery high-water. Its
  `apply_committed_batch` applies a sealed segment in one SQLite transaction and
  idempotently skips already-applied prefixes.
- `pqueue-engine::ComposedBackend` already has a group-commit path that buffers
  pushes, force-seals before read-modify-write operations, applies each sealed
  batch through `ProjectionStore::apply`, and acknowledges only after apply.
- `pqueue-server` currently parses projection values `inmemory` and `sqlite`.
  It wires `objectlog/inmemory` and `objectlog/sqlite`, but not `hybrid`.
- Existing tests cover SQLite batch atomicity, segmented object-log CAS and
  group commit, conformance suites, and release-tier object-log performance
  evidence. They do not exercise a hot-memory-plus-SQLite projection under load,
  restart, and injected apply/recovery failures.

## Scope

In scope:

- Add a `HybridProjectionStore` implementation, preferably in `pqueue-sqlite`,
  that wraps `InMemoryProjection` plus `SqliteProjectionStore`.
- Add recovery helpers needed to rebuild the hot in-memory projection from the
  durable SQLite projection high-water and the object-log tail.
- Wire `ProjectionSpec::Hybrid { path }`, env parsing, typed config, server
  startup, Helm/static validation, and runtime documentation.
- Update TD-001, ADR-012, TD-004, deployment docs, and test-plan references so
  hybrid has a normative contract, not just code.
- Add focused, conformance, chaos, and load tests that prove transactional
  integrity and local-serving performance under object-log group commit.
- Merge each bead to `main` using DDx merge policy and cut a release after the
  queue is empty and validation evidence is attached.

Out of scope:

- Changing pqueue's queue-as-shard ownership model.
- Using the object-storage segmented log as a tiny per-operation commit log.
  Normal data-plane traffic must wait for packed object-log group commit before
  durable acknowledgement. Rare explicit sync/control flushes are permitted, but
  release evidence must show they do not dominate object count, request count, or
  storage utilization.
- Adding cross-node active/active serving of one queue.
- Replacing the object-log segmented substrate or release-tier MinIO evidence
  harness except where needed to add hybrid rows.

## Chosen Approach

### Projection Axis

Add `HybridProjectionStore` with this invariant:

1. `ensure_shard` creates or validates the SQLite projection row and ensures the
   in-memory shard exists with the same queue definition.
2. `apply(positions, commands)` writes the full batch to SQLite first through
   `SqliteProjectionStore::apply_committed_batch`, then applies the same batch to
   `InMemoryProjection`.
3. All hot reads and validation methods delegate to `InMemoryProjection`.
4. Recovery methods delegate durable catalog/high-water/counter restoration to
   SQLite, then rebuild memory by replaying authoritative commands through the
   hybrid apply path.

The SQLite-first apply order is deliberate. If the process fails after SQLite
commit but before memory apply, memory is transient and rebuilt on reopen. If the
process fails before SQLite commit, no success response can be returned because
`ProjectionStore::apply` has failed, so recovery replays the log tail. Memory is
never allowed to be ahead of SQLite for acknowledged commands.

### Recovery

The generic `ComposedBackend::recover` must produce a hot in-memory projection
without replaying a multi-million-command log from genesis. The hybrid store
therefore needs a recovery path that:

- recovers queue definitions and high-water from `SqliteProjectionStore`;
- restores item-id counters from the SQLite snapshot;
- rehydrates `InMemoryProjection` to the SQLite high-water without requiring the
  object-log genesis tail;
- replays only object-log commands beyond the SQLite high-water.

The concrete mechanism is a new projection-image seam, not an implementation
choice left to a worker:

- add a typed `ProjectionImage` import/export API in `pqueue-projection`;
- add `ProjectionData::from_image(definition, image)` and
  `ProjectionData::to_image()` tests that round-trip item lifecycle, lease
  state, indexes, side records, instance fences, queue paused state, and metrics;
- add `SqliteProjectionStore::export_projection_image(shard)` that reads the
  durable SQLite rows at the current high-water into that image;
- add `InMemoryProjection::hydrate_shard(definition, image)` so
  `HybridProjectionStore::ensure_shard` can build memory before
  `recovery_high_water` lets `ComposedBackend::recover` skip the historical log
  prefix.

`HybridProjectionStore::recovery_high_water` MUST return SQLite's high-water
only after the in-memory shard has been hydrated to that same image. If hydration
fails or has not run, it MUST return `None` or fail closed so recovery replays
from genesis rather than serving an empty hot projection.

The image seam must preserve secondary indexes, lease state, side records,
instance fences, metrics, and queue paused state. A partial rehydration that only
loads pending items is not acceptable.

### Snapshot Authority

The object log remains the packed campaign-shape/replay authority for the
object-storage profile. The local SQLite file is the owner-local restart
accelerator and recovery high-water source for `objectlog/hybrid`; it is not
permission to delete object-log segments by itself. High-churn transactional
commands such as claim/finalize must not force one object-storage segment per
batch command. The implementation must delay normal acknowledgements until the
command is included in a packed durable object-log segment and manifest.
Operators may force a low-volume sync/control flush, but those flushes must be
measured separately and must not define the normal data-plane cost profile.

Release evidence must report object-storage cost shape, not just latency:
segment/object count, total object-log bytes, mean/max object size, segment-size
utilization against the configured target, PUT/COPY/POST/LIST, GET, and
DELETE/CANCEL counts, and an S3-style estimated request plus retained-storage
cost using price inputs written into the evidence row. LIST count is billable
request count, including S3 pagination pages, not merely logical manifest-list
calls. A single object-log command is acceptable only when it represents a large
batch of resident campaign work; tiny one-command objects in normal data-plane
traffic are a release blocker.

For the first hybrid release, segment expiry MUST remain disabled unless a
separate object-store snapshot is written and validated. TD-004 must be amended
to say that `objectlog/hybrid` has two recovery modes:

- normal owner-local restart: hydrate memory from local SQLite, then replay only
  the object-log tail beyond SQLite high-water;
- local-disk loss or new owner without the SQLite projection file: recreate
  SQLite and memory by replaying the retained object log from genesis.

Object-store SQLite snapshots and segment expiry can be a later optimization,
but the hybrid release must not claim segment-retention reduction without that
feature and its recovery tests.

### Partial Apply And Poisoning

`HybridProjectionStore::apply` writes SQLite first and memory second. If SQLite
fails, nothing is acknowledged and recovery replays the object-log tail. If
SQLite succeeds but the memory apply fails in the same process, the hybrid store
MUST enter a poisoned state:

- the current operation returns `EngineError::Storage` and is not reported as a
  success response;
- all subsequent reads, validation, and writes fail closed with a storage error;
- the process must restart to hydrate memory from SQLite and resume serving;
- tests must prove the store cannot continue with memory behind SQLite.

This fail-closed rule handles the same-process gap between durable SQLite apply
and transient memory apply. A successful response is legal only after both
SQLite and memory have applied the command, so read-after-success visibility is
preserved.

### Request-Id Replay

Hybrid cannot rely only on the generic in-memory `QueueIdempotencyCache`. A
commit-but-unreturned request must converge after restart. The implementation
must add durable push request-id replay for object-log/hybrid:

- `CommandEnvelope::request_id` plus the pushed command body are enough to
  reconstruct the push fingerprint and response item ids from the durable log;
- during recovery, replayed request-id pushes within retention must repopulate
  the generic idempotency cache, or `HybridProjectionStore` must persist the
  equivalent rows into SQLite during `apply`;
- a same-body retry must return the original ids without a second append;
- a different-body retry under the same `request_id` must return
  `RequestIdConflict`;
- tests must cover crash after manifest commit, crash after SQLite commit before
  memory apply, and restart before retry.

The bead that wires runtime support is not complete until this replay behavior
is implemented or the supported API surface explicitly rejects request-id pushes
for the hybrid profile, which is not acceptable for release.

### Runtime Wiring

`PQUEUE_PROJECTION_BACKEND=hybrid` uses the same
`PQUEUE_SQLITE_PROJECTION_PATH` as `sqlite`. The server should reject unsupported
log pairings initially unless they are intentionally implemented and tested.
The release target is:

- `objectlog/hybrid`: supported and release-gated;
- `memory/hybrid`, `sqlite/hybrid`, `postgres/hybrid`: rejected unless a bead
  explicitly adds and tests them.

Runtime wiring will use the generic group-commit composition:

`ComposedBackend<pqueue_objectlog::ObjectLog, HybridProjectionStore, InProcessControlPlane>`

The existing `pqueue-objectlog::ObjectLog::open_group_commit` log axis exposes
the segmented production substrate, segment configuration, counters, durable
queue definitions, and `LogStore` group-commit hooks. The server bead must add a
generic flusher task that calls `flush_tick` at
`group_commit_flush_interval_ms()`, plumb `debug_segments` and recovery-tail
configuration where applicable, and expose segment counters for evidence. Do not
add a third segmented monolith unless this path is proven unable to preserve the
current object-log runtime contract.

### Performance Model

Hot serving must remain in-memory for claim selection, `peek`, `pending`,
`metrics`, live-item lookup, secondary-index lookup, and pre-commit validation.
SQLite work must be amortized on sealed segment apply, not on every read. The
load tests must compare hybrid to `objectlog/inmemory` and `objectlog/sqlite`
with the same segment configuration and report:

- push throughput and p50/p95/p99 acknowledgement latency within 20% of
  `objectlog/inmemory` for the same segment settings in the release-tier run;
- claim/finalize p95 latency within 20% of `objectlog/inmemory` for hot reads
  after the initial SQLite apply cost is amortized;
- segment batch density and object PUT count;
- recovery elapsed time and tail length after a large resident set, with normal
  owner-local restart avoiding full-genesis replay;
- max memory rehydrate time from SQLite snapshot.

Recovery has numeric gates:

- smoke gate: 100k resident items, local SQLite file present, restart hydrate +
  object-log tail replay completes in <= 5 seconds and replays <= 1,000
  object-log commands;
- release-tier gate: 10M resident items, local SQLite file present, restart
  hydrate + object-log tail replay completes in <= 60 seconds and replays <=
  max(10,000 commands, 0.1% of resident items);
- disk-loss gate: with the SQLite projection file removed, recovery from the
  retained object log may be slower, but it must reconstruct exact metrics,
  indexes, leases, and request-id replay state with zero invariant violations.

## Work Breakdown

1. Specs and contract: update ADR-012, TD-001, TD-004, TP-003, and deployment
   docs with the hybrid contract, failure model, env surface, snapshot authority,
   durable request-id replay, and release gates.
2. Projection image seam: add `ProjectionImage` export/import APIs and tests in
   `pqueue-projection` and `pqueue-sqlite`.
3. Hybrid projection core: implement `HybridProjectionStore`, SQLite-first
   apply, poison-on-memory-failure, hot-read delegation, recovery high-water,
   and focused divergence tests.
4. Durable request-id replay: recover or persist push request-id outcomes for
   object-log hybrid so committed-but-unreturned retries converge after restart.
5. Object-log runtime wiring: add `ProjectionSpec::Hybrid`, env parsing,
   `objectlog/hybrid` startup through the generic group-commit composition,
   flusher integration, Helm/static render support, and RESP reopen tests.
6. Transaction and chaos testing: add crash/failpoint or injected-error tests
   for SQLite-commit-before-memory-apply, apply rejection, replay overlap,
   request-id replay, force-seal before claim, stale epoch, and restart after
   large batches.
7. Conformance and parity: run the shared conformance suites against
   object-log hybrid, including eventual-apply transaction-contract scenarios
   and secondary-index behavior.
8. Load and scale evidence: add smoke-safe benches plus release-tier ignored
   tests for million-member campaign reshape/report flows, recovery, and segment
   density; publish the evidence under `docs/perf/`.
9. Release integration: make the release gate include hybrid smoke evidence,
   update release notes/version metadata, merge to `main`, push, tag, and publish
   the release.

## Validation

Every implementation bead must include focused commands. The final release gate
must include at least:

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo test -p pqueue-sqlite --test sqlite_projection_tests -- --nocapture`
- `cargo test -p pqueue-server --test server -- --nocapture`
- `cargo test -p pqueue-objectlog --test composed_group_commit -- --nocapture`
- `cargo test -p pqueue-conformance`
- hybrid-specific chaos tests with injected apply/recovery failures, including
  fail-closed poisoning after SQLite success and memory failure
- hybrid-specific request-id replay tests for committed-but-unreturned pushes
- hybrid-specific server/runtime tests for
  `PQUEUE_LOG_BACKEND=objectlog PQUEUE_PROJECTION_BACKEND=hybrid`
- release-tier ignored performance runs that write evidence to `docs/perf/`

The release is not complete until the docs state the exact version, the hybrid
profile is wired in the runtime contract, the release artifact is published, and
`main` contains the tracker updates and closing commits for all hybrid beads.

## Risks

- Rehydrating `ProjectionData` from SQLite rows will expose missing constructors
  or private fields. Limit the new surface to a typed `ProjectionImage` import
  and export API with round-trip tests.
- Delegating reads to memory means any divergence between SQLite and memory is
  dangerous. Tests must compare both projections after every chaotic boundary.
- Generic composed group-commit must preserve the production segmented object-log
  contract. The runtime bead must prove flusher cadence, counters, segment
  config, recovery-tail behavior, and RESP serving before the old segmented
  paths are treated as equivalent.
- Durable request-id replay currently lives in SQLite relational code and
  generic in-memory caches, not in object-log apply. Hybrid release depends on
  closing this gap for push-with-request-id.
- Multi-million-member release tests may be too slow for ordinary CI. Provide
  smoke defaults plus explicit release-tier commands, and commit evidence from
  the release-tier run.
- Existing open Lakebase beads are unrelated to hybrid. They must not be closed
  as part of this release unless their acceptance criteria are actually met.

## Open Questions

- Whether `HybridProjectionStore` should live entirely in `pqueue-sqlite` or in a
  small new composition crate. Default: `pqueue-sqlite`, because it owns
  `SqliteProjectionStore` and already depends on `pqueue-projection`.
- The exact release version. Default: the next minor after `v0.5.0`, because this
  adds a new supported storage profile. (Resolved: shipped as `v0.6.0`.)
