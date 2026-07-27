---
ddx:
  id: td-batch-shape-and-critical-section-audit
  depends_on:
    - product-vision
    - prd
    - concerns
    - api-native-client-interface
    - adr-log-single-source-of-truth
    - adr-full-async-storage-boundaries
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-s3-object-log-sqlite-projection-mode
    - td-sqlite-native-embedded-durable-mode
    - td-object-log-turso-projection
  links:
    - {kind: informed_by, to: product-vision}
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-postgres-native-reference-mode}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: informed_by, to: td-sqlite-native-embedded-durable-mode}
    - {kind: informed_by, to: td-object-log-turso-projection}
  status: proposed
---

# Technical Design: TD-011 Batch Shape and Critical-Section Audit

**Disposition**: No architecture change recommended. This audit is `status: proposed` pending operator
review. It authorizes no source, schema, API, configuration, deployment, or implementation-bead change.

## Scope

Audit the current mutation stack from API request through planning, durable append, projection apply,
idempotency publication, metrics maintenance, and acknowledgement. The audited operations are push,
BatchUpdate, claim, finalize, metrics, request-id replay, object-log group commit, SQLite, Turso, and
Postgres.

The audit distinguishes:

- required linear CPU work, such as validating each item and encoding each result;
- bounded chunk work, where statements or awaits grow only at a declared protocol/bind limit;
- batch-shape violations, where one public batch becomes one database call, append, task, await, or
  acknowledgement per item;
- critical-section violations, where unrelated queues share a lock across storage or network I/O.

Out of scope: changing API-001 semantics, adding admission policy, selecting a new backend, changing
transaction authority, weakening per-item outcomes, or creating implementation work before review.

## Governing Alignment

| Authority | Required invariant | Audit interpretation |
|---|---|---|
| Product Vision | Batch-centric execution; backend-independent transaction integrity | A batch may do O(n) CPU work, but storage work is set-based or bind-bounded and commits as one durable unit. |
| PRD P0-5..8; FR-18..35 | Batched push/update/claim/finalize with exact per-item results, leases, retries, and progress | Result cardinality may be per item; I/O cardinality must not silently become per item. |
| API-001 | Success is durable and visible; `request_id` replays the stored outcome | Idempotency lookup, append/apply, response publication, and acknowledgement remain in one ordered boundary. |
| ADR-013 | Durable log first, serving projection response barrier second | Grouping cannot acknowledge before log durability and required serving-state visibility. |
| ADR-015 | Runtime-neutral async ports; blocking stores execute whole transactions off-reactor | No reactor-thread blocking and no lock guard held across an unrelated queue's await or provider I/O. |
| TD-001 | One external contract across composed profiles | Batch-shape tests belong to the backend eligibility/conformance boundary, not only adapter-local tests. |

## Technical Approach

### Batch-shape invariants

1. One API batch produces one durable transaction or one ordered group-commit contribution. Internal
   command vectors may contain one command per accepted item when replay ordering requires it.
2. Validation, serialization, hashing, and result assembly may be O(n) CPU work. They must not spawn one
   task or perform one storage round trip per item.
3. Database statements are constant for the operation or grow only as `ceil(n / declared_chunk_size)`.
   Chunk sizes are tied to protocol/bind limits and tested at 1, boundary, and boundary+1.
4. Queue-local serialization may cover selector resolution through append/apply. A process-wide lock must
   not cover provider I/O or serialize independent queues unless the physical backend is a documented
   single-writer store.
5. Batch acknowledgement is one caller-visible completion after durable log append and the required
   serving projection barrier. Per-item acknowledgements are forbidden.
6. Metrics, gates, typed indexes, group summaries, and idempotency are part of the same batch audit; moving
   N work into a trigger or replay table does not make it batch-shaped.

### Evidence method

The audit inspected the named symbols at revision `e67c3df8`. Statement-shape tests count as structural
evidence; env-gated live tests are capacity evidence only when their environment and result are recorded.
No throughput or latency claim is made by this document.

## End-to-End Inventory

| Surface | Planning / append shape | Projection / side-work shape | Disposition |
|---|---|---|---|
| Push | `ComposedBackend::push` validates items in-process, creates one `Push` envelope, and calls one append boundary (`crates/fireweed-engine/src/compose.rs:4245`). Request-id push resolves replay before new admission (`compose.rs:4309`). | Postgres `insert_items` uses multi-row chunks and bulk sequence allocation (`crates/fireweed-postgres/src/relational.rs:2709`); SQLite does the same (`crates/fireweed-sqlite/src/relational/apply.rs:185`); Turso declares bind-safe push chunks (`crates/fireweed-turso/src/projection.rs:218`). | Aligned. Linear validation/encoding is not I/O amplification. |
| BatchUpdate | The composed path takes one snapshot, builds accepted `UpdateFields` envelopes, and calls `commit_locked_batch` once (`compose.rs:5194`). Native Postgres locks all targets in one query and publishes one request outcome (`crates/fireweed-postgres/src/relational.rs:8299`). | Native Postgres has a 1,000-item one-select/one-command-insert/one-projection-update probe (`relational.rs:14475`). Composed SQLite/Turso apply batches still iterate command envelopes inside one transaction (`crates/fireweed-sqlite/src/relational/recovery.rs:912`; `crates/fireweed-turso/src/projection.rs:1423`); no equivalent BatchUpdate statement-count proof was found. | Evidence gap E1. Do not infer per-item I/O from the command loop, but require a trace at 1/100/1,000 before declaring this path aligned. |
| Claim | The composed item path selects a bounded candidate vector and appends one `Claim` command (`compose.rs:4640`). Postgres uses one `FOR UPDATE SKIP LOCKED` CTE that selects and leases the batch (`crates/fireweed-postgres/src/relational.rs:7295`). | Claimed-row enrichment uses batch reads; SQLite tests statement count at 1/100/1,000 (`crates/fireweed-sqlite/src/relational/backend.rs:4067`). | SQL shape aligned. Concurrency is limited by findings C1/C2 below. |
| Finalize | The composed path validates a vector and emits one `Finalize` command (`compose.rs:4921`). | Postgres and SQLite partition outcomes by resulting state and update item-id sets, not one row at a time (`crates/fireweed-postgres/src/relational.rs:3450`; `crates/fireweed-sqlite/src/relational/apply.rs:1150`). Turso uses bind-bounded item sets and schedule chunks (`crates/fireweed-turso/src/projection.rs:1279`). | Aligned. Per-item retry-state classification is required CPU work. |
| Metrics | Reads use maintained queue counters for unconstrained metrics (`crates/fireweed-postgres/src/relational.rs:4761`; `crates/fireweed-sqlite/src/relational/query.rs:1163`). | Postgres maintenance is a row-level trigger on `fireweed_items` (`relational.rs:530`, `relational.rs:621`). | Candidate violation V1: batch SQL still invokes trigger work per affected row. Measure before redesign. |
| Request-id replay | Composed push and BatchUpdate check retained outcomes before admitting new work (`compose.rs:4309`, `compose.rs:5194`). Postgres BatchUpdate locks its cursor, checks replay, mutates, appends command evidence, and records the outcome in one transaction (`relational.rs:8299`). | Projection replay records are committed with applied state; retries add no new mutation debt. | Aligned. Replay lookup must stay ahead of admission/backpressure. |
| Object-log group commit | `GroupCommitCoordinator::enqueue` accepts a serialized request under byte admission, and `run_prepared_commit` elects one driver (`crates/fireweed-objectlog/src/async_commit.rs:1184`). | A seal applies the envelope vector once and completes request waiters after high-water/projection advance (`crates/fireweed-engine/src/compose.rs:2530`). | Aligned. One waiter per request is not one acknowledgement per item. |
| SQLite | One connection/transaction is the physical writer boundary (`crates/fireweed-sqlite/src/relational/backend.rs:528`). Set-based tests cover grouped push, claim/finalize, item reads, gates, side records, and bounded mutation (`backend.rs:4060`). | Statement growth is constant or bind-bounded; the single writer is a declared SQLite constraint. | Aligned except E1. |
| Turso | One async writer connection is guarded by `tokio::sync::Mutex` (`crates/fireweed-turso/src/projection.rs:1423`). | Push/index/gate/finalize helpers use declared bind-safe chunks; structural tests cover push and group-summary boundaries (`projection.rs:4265`). | Aligned except E1. No task-per-item path found. |
| Postgres | SQL is pool-ready and set-based, including atomic sequence allocation and row-locked claim. | The production facade still owns one synchronous `Client` behind one `Mutex<Inner>` (`crates/fireweed-postgres/src/relational.rs:18`, `relational.rs:5401`). | Critical-section violation C2; accepted architecture already requires pool + whole-transaction blocking dispatch. |

## Critical-Section Audit

### C1 — Legacy composed append/apply mutex

`ComposedBackend` stores log, projection, idempotency caches, command sequence, and every queue's group
coordinator in one `Mutex<Inner>` (`crates/fireweed-engine/src/compose.rs:1706`, `compose.rs:1804`).
`commit_locked_batch` calls log append and projection apply while that guard is held (`compose.rs:3363`).
The queue-local `KeyedQueueGate` prevents same-queue races, but the global inner mutex can still serialize
unrelated queues and can cover object-store I/O on the legacy synchronous composition.

Disposition: not a new architecture decision. ADR-015 and TD-010 already require native async axes,
detached blocking/provider work, and queue-local mutation gates. Operator review should decide whether the
legacy path is test-only/compatibility-only or still reachable by production profiles. The acceptable end
state is no provider I/O under the global guard; this audit does not select a migration sequence.

### C2 — Postgres single-client mutex

The Postgres relational module explicitly records that two claims cannot run concurrently because one
synchronous `Client` is held behind `Mutex<Inner>` (`crates/fireweed-postgres/src/relational.rs:18-32`).
The claim CTE is concurrency-safe but its `SKIP LOCKED` behavior is not exercised concurrently through the
facade (`relational.rs:7295`).

Disposition: confirmed implementation gap under accepted ADR-015/TD-002 authority. A connection pool and
whole-transaction blocking dispatch are required before production concurrency claims are supported. No
new locking or transaction semantics are proposed here.

### V1 — Postgres row-trigger amplification

`fireweed_items_metrics_delta` is `FOR EACH ROW` and performs counted-item, queue-metric, and due-pending
maintenance (`crates/fireweed-postgres/src/relational.rs:530-626`). The typed-index component trigger is
also `FOR EACH ROW` (`relational.rs:499-529`). Set-based caller SQL therefore still incurs row-count-scaled
server work and may create hot counter rows for large batches.

Disposition: unresolved performance risk, not a correctness defect. Before any design change, measure
trigger calls, rows touched, lock waits, WAL bytes, and transaction time for push/update/claim/finalize at
1/100/1,000 rows. Compare the current row triggers with transition-table statement triggers and explicit
set-based maintenance. Preserve atomic metrics/index visibility and rollback as non-negotiable constraints.

## Component Changes

No component change is authorized by this audit.

If operator review accepts the findings, the destination authority remains the existing artifacts:

- C1 and C2: implementation planning under ADR-015, TD-001, TD-002, and TD-010;
- E1: conformance/statement-shape coverage for composed SQLite and Turso BatchUpdate;
- V1: a measured technical spike before any trigger design proposal.

No API, schema, configuration, telemetry field, or adapter signature is defined here.

## Security

Batch-shape changes must preserve tenant/queue predicates, lease and epoch fences, request fingerprints,
and atomic rollback. Secrets and payload bodies are excluded from statement-count/trace evidence. This
audit changes no authentication, authorization, encryption, or retention rule.

## Performance

| Evidence | Required measurement | Pass interpretation |
|---|---|---|
| E1 composed BatchUpdate | Statement/await count at 1, 100, 1,000 accepted updates, including gate/index replacement | Constant or declared bind-bounded growth; one durable append batch and one caller acknowledgement. |
| C1 composed concurrency | Two queues with overlapping provider latency | Independent queues do not wait on one global append/apply guard. |
| C2 Postgres concurrency | Contended multi-connection claims under ordinary load | `SKIP LOCKED` produces disjoint leases; no reactor blocking; transaction affinity preserved. |
| V1 trigger cost | Trigger calls, rows, WAL bytes, lock waits, transaction time at 1/100/1,000 | Capacity evidence identifies the crossover; no universal host-speed gate. |

Absolute throughput and latency remain topology-bound capacity observations, not release correctness bars.

## Testing

- [ ] `ddx doc validate` validates this proposed artifact and its links.
- [ ] Existing Postgres `batch_update_1000_uses_one_target_select_command_insert_and_projection_update`
  remains the native relational structural baseline.
- [ ] Existing SQLite statement-count tests at `backend.rs:4060-4127` remain green.
- [ ] Existing Turso chunk-boundary tests at `projection.rs:4265` remain green.
- [ ] Review records whether C1 is production-reachable and whether V1 warrants a measured spike.
- [ ] No source, schema, API, configuration, deployment, or implementation-bead diff exists in this bead.

## Migration & Rollback

There is no migration. Rollback is deletion of this proposed artifact before acceptance. If later work is
approved, each change must retain an independently reversible compatibility path and the existing
transaction/idempotency conformance gates.

## Risks

| Risk | Prob. | Impact | Mitigation |
|---|---:|---:|---|
| Treating command-vector loops as proof of per-item I/O | M | M | Require statement/await tracing; do not classify from source shape alone. |
| Optimizing triggers weakens atomic metrics or typed-index visibility | M | H | Keep maintenance in the mutation transaction and test rollback/failure cuts. |
| Pooling Postgres breaks transaction affinity or moves blocking calls onto the reactor | M | H | Dispatch whole transactions and retain one checked-out connection per transaction. |
| Removing global serialization exposes a latent cross-queue shared-state race | M | H | Separate queue-local state from bounded global coordination and add concurrent two-queue tests. |

## Review Gate

Operator review must choose one of:

1. accept the no-architecture-change disposition and authorize separate measured/planning work for E1,
   C1, C2, and V1;
2. accept the audit but record one or more findings as an intentional bounded limitation; or
3. reject a finding with file/symbol or measured evidence.

Until that review is recorded, this artifact remains proposed and no implementation or conformance bead
may be derived from it.
