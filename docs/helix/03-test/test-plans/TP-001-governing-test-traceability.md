---
ddx:
  id: tp-governing-test-traceability
  depends_on:
    - prd
    - api-native-client-interface
    - api-operator-repair-contract
    - api-workload-integration-profiles
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-rust-workspace-and-toolchain-policy
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-sharding-and-shard-ownership
    - td-resp-wire-adapter
    - td-s3-object-log-sqlite-projection-mode
    - tp-scale-substantiation
    - api-fireweed-kernel-contract
  review:
    self_hash: 1f28a43e7928388a17e76507cc38e3b5ffc411860ecba33bba16fb8f320aa209
    deps:
      adr-auth-tenancy-and-storage-isolation: 1d4296498a187d4d4c3bb4a4e37647f7193036c3a20ab2a8b66154a45937ece2
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-granularity-mapping-and-claim-domain: c44740df25ed32569e86f4c3459ed07839d33325367cdafc49fb1317deab606d
      adr-queue-as-shard-unit-and-projection-families: 64a7c7b0e2e5f4caa2c7d775b84c87a9a1e4484ae3df9dccbe3d145d22681a7e
      adr-rust-workspace-and-toolchain-policy: df18055fca75337c9dc5e7e099d14afd44428e4883a0784af0a0c1e2192e98f6
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      api-operator-repair-contract: f4fdf0e4e8e29431d8ff2f89a8bc843497dca743ad5e32439996a4ad7c611197
      api-workload-integration-profiles: 3c3dd594f1723e987015d4790634b1088016f5f41a049e661eba4b752cfb4c39
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      td-postgres-native-reference-mode: 4088399aca0beb5840c4ace629039ad655bf4ff928bc22f86072b94857bdbd3e
      td-resp-wire-adapter: 351f429307d19a5b4ba3c3e39e9d9168e999a5b067b07d15f67636e5fd5d8040
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-sharding-and-shard-ownership: f7309199e3810447398caa11f2f0241ce05c035e9b9d0241aada4ad3759582e1
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
      tp-scale-substantiation: 23f20e8dab88330e4ddd165a0d2230151b7ef0f99ca16c016671558ed5719686
    reviewed_at: "2026-08-07T11:25:30Z"
---

# Test Plan: TP-001 Governing Test Traceability

## Public cell (ADR-024)

ADR-024 supersedes the 2026-09-17 storage-selector amendment for public
selectors. The public product is one cell: S3 object-log × Turso projection
(`s3 log × turso projection`). `ResponseBarrier` has only `AsyncProjection`.
The other eleven axis pairs and `Strict` are not a roadmap. Class A durability
is the object log. Turso is rebuildable through `projection_control` and is
not the command log.

The 2026-09-17 amendment is historical. It does not define current selectors.
Historical DDx IDs, requirement IDs, artifact names, and original measurements
retain their identity. SQLite selectors stay retired and are not a differential
reference.


## Kernel contract (API-006)

[API-006](../../02-design/contracts/API-006-fireweed-kernel-contract.md) is the
contract a consumer depends on, and it supersedes API-001 to API-005 (owner
decision, 2026-09-23 design discussion). The PRD's requirements follow it, and
it changes this plan in four ways:

- **Black-box contract suite.** `fireweed-contract-suite` runs in-process
  against `Arc<dyn Store>` through `run_all(constructor, reopen, Δ, clock,
  fault_hook)`, with at least one scenario per MUST clause, each citing its
  clause id, and every scenario run a second time through a torn-read adversary
  for `stats` and `aggregate` (API-006 CS1–CS3, CS6). It is the functional
  qualification of every `Store` implementation. Port-level, workflow and
  consumer suites add evidence; they never replace, skip or weaken it.
- **No backend knobs.** The suite has no per-backend clause, knob, capability
  probe or backend identity (CS4). Every implementation passes or fails the
  same scenarios.
- **Operator ban.** CI bans the `Operator` trait and the admin handle in Snorri,
  in the workload and loadgen crates, and in the contract suite (OP4, CS5). A
  separate operator suite covers OP1–OP3.
- **Performance gates.** C9, the claim cost, and the S2 cost bound are checked
  by the claim-path and storage gates derived from the spikes in
  `docs/helix/02-design/spikes/` (SPIKE-002 to SPIKE-006; TP-003 §3.12,
  AC-PERF-1 to AC-PERF-6 and AC-PERF-8), not by the black-box suite.
  AC-PERF-7, the per-queue commit cost on MinIO, is a recorded measurement,
  not a gate.

TP-003 §3.12 holds the pass bars (AC-KC-1 to AC-KC-5, AC-PERF-1 to AC-PERF-8).
The rows and suites below that test retired surfaces are superseded; immutable
evidence may keep their names, but they no longer qualify a build:

| Earlier row, layer or suite | Disposition | Current binding |
|---|---|---|
| Library (facade) integration layer (`crates/fireweed/tests/**`, the API-005 verb list) | Superseded | `fireweed-contract-suite` through `Fireweed`, which wraps `Arc<dyn Store>` 1:1 |
| Wire (RESP) integration layer; per-queue ownership + routing row; `routing_redirect_tests` | Retired | The RESP front is deleted and there is no remote API; queue ownership is implementation evidence on the admin handle |
| FR-2 to FR-6 rows; `core_priority_model_tests` | Superseded | Rank ordering and the immutable definition: C4, ORD2, D1–D6 |
| FR-8, FR-9, FR-12, FR-13 row; `storage_conformance_progress_tests`, `per_queue_progress_tests`; the bounded-relaxed half of `product_workflow_generic_priority_bounded_relaxed_e2e` | Retired (PRD FR-5, FR-8, FR-9, FR-12, FR-13) | Claim order and C5a progress in the contract suite |
| FR-7/14, FR-10/11/15/17, FR-16, FR-18 to FR-22, FR-20, FR-23 to FR-28, FR-29 to FR-35, FR-33 to FR-37 rows | Superseded | The matching API-006 clauses in the contract suite: C1, C4, W9, V4; F1–F3; W3, W5, RQ1–RQ4; W4, W8; L1–L6, C6, DUR1; C2–C7; W4, W5. Retry exhaustion is retired |
| FR-38, FR-39 row; `postgres_retention_tests` | Retired (PRD FR-38, FR-39) | Records live until deleted; `delete_queue` closes a queue |
| FR (g6 cohort) row; `storage_conformance_cohort_tests`, `service_cohort_tests`, `product_workflow_callback_cohort_e2e` | Retired (PRD FR-32a to FR-32c); FR-47c is reworded, not retired | FR-47c's callback batch: atomic group insert, `WholeGroups` claim and a caller count check (W2, C4, C6a) |
| FR-48 (g4 discovery) and `fireweed_group_summary` rows; `storage_conformance_discovery_tests`, `service_discovery_tests` | Retired (PRD FR-48) | `stats.groups` (S2) |
| FR-23, FR-49 to FR-55 (g5 recurring) row; `core_recurrence_rearm_tests`, `service_recurrence_purge_tests`, `product_workflow_jobs_connectors_recurring_e2e` | Superseded | Re-rank on settle (W5) and `Delete` (W7) |
| FR-31a, FR-32 (g1 whole-group) and ADR-008 co-residency rows; `storage_conformance_group_batching_tests`, `service_group_batching_tests` | Superseded | `WholeGroups` (C4, C6a, C7) |
| FR-15, FR-17 (g2 dynamic gates) row; `storage_conformance_gate_tests`, `service_gate_tests` | Superseded | Gate ops in write units (W9, C1) |
| FR-47a / ADR-004 granularity row | Superseded | Tenant, queue, stage, group, declared fields and gates (PRD FR-47a) |
| API-002 rows (repair, redrive, bulk purge, archive/retention, async operations, operator authorization); `operator_*` suites; `product_workflow_operator_repair_redrive_e2e` | Superseded | `force_clear` in the operator suite (OP1–OP3). Pause, redrive and purge are writes, and archive is `delete_queue`; async operations and operator authorization are retired |
| Eligibility Precedence row | Superseded | C1 |
| API-001 idempotency and claimed-item response shape rows; `claimed_item_shape_conformance_tests` | Superseded | RQ1–RQ4, C8; `Record` and C3 |
| API-001 auth row; `service_auth_tenant_tests` | Retired | The contract is in-process and has no principal; queue isolation is ISO1 |
| API-003 workload profile row | Superseded | API-006; no replacement profile exists yet. `product_workflow_scheduled_action_delivery_e2e` migrates to API-006 terms |

The FR-1, FR-43, durability, public-cell, response-barrier, TD-002 to TD-005,
queue-density and ADR-003 rows stand as implementation evidence. The Scope
section's requirement that TP-002 E0/E1 workloads declare a `progress_bound_ms`
is retired with the progress bound; those workloads assert claim order and C5a
progress instead.

## Scope

This plan defines the minimum test coverage required before the first fireweed
implementation can be considered aligned with the current HELIX frame and
design. It maps PRD functional areas and design decisions to named test suites,
including the full gap-closure surface: group-cardinality / whole-group claim
(ADR-004, API-001 `group_batching`), dynamic eligibility gates (`SetGates`),
active-scope discovery (`DiscoverActiveScopes`), recurring / never-terminal items
(`rearm`, `PurgeItems`), atomic complete-cohort claim (`cohort_policy`,
`whole_cohort`), granularity (ADR-004), per-queue ownership with queue-local
progress and client routing (TD-003, TD-006), and the
orthogonal five-log × three-projection storage matrix (TD-001/API-005).

This is the governing implementation and release test plan. Exact Rust function
names may evolve, but implementation beads must preserve the suite intent and
cite the relevant requirement IDs.

Scale, queue-density, horizontal-envelope evidence, and object-log latency/cost
evidence (the portable progress/capacity contract, ≥1000-active-queue density,
cross-queue scale-out, and object-log commit-latency-bound matrix) are owned by the **scale-substantiation test plan**
(`tp-scale-substantiation`, TP-002, evidence records E0–E3). This governing plan
references TP-002 for those records rather than restating them; the two plans are
complementary and non-overlapping.

Every TP-002 E0/E1 workload declares a positive `progress_bound_ms` in the
queue definition, reads the persisted definition back, and proves zero
accepted-to-claim or discovery-age violations of that declared bound. Latency
percentiles and rates remain capacity observations; they do not replace the
queue's persisted progress contract.

## Test Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| Kernel contract (black-box) | `fireweed-contract-suite`, run by each `Store` implementation's tests | The API-006 conformance suite: `run_all` over the constructor (both `DeletePolicy` values), `reopen`, Δ, a manual `Clock` and the post-commit fault hook; one scenario per MUST clause, run directly and through the torn-read adversary; no backend knobs (TP-003 AC-KC-1 to AC-KC-3). |
| Operator | a separate operator suite that takes the admin handle | API-006 OP1–OP3 (`force_clear`). The contract suite, Snorri, and the workload and loadgen crates never link the `Operator` trait or the admin handle; a CI guard enforces it (TP-003 AC-KC-4, AC-KC-5). |
| Core unit | `crates/fireweed-core/src/**`, `crates/fireweed-engine/**` | Pure validation, priority encoding, lifecycle, retry, idempotency, version rules, and the engine's decision helpers + dependency-direction guard. |
| Backend conformance | `crates/fireweed-conformance/**` (run by each adapter's `tests/`) | The shared no-stub port-conformance suite executes against the public cell `s3--turso` (ADR-024). It includes transaction-contract scenarios (object-log durability, rejection-no-effect, unknown-outcome replay, empty claim as a poll), reopen from the object log, lease, claim, finalize, renew/reassign, purge, and projection-read scenarios. Required routes fail rather than silently skip when a fixture or feature is absent. |
| Postgres integration | `crates/fireweed-postgres/tests/**` | The durable-log postgres adapter (TD-004 template) against a real DB, env-gated on `FIREWEED_PG_TEST_URL`: the full conformance suite + a reconnect/durability replay test. |
| Wire (RESP) integration | `crates/fireweed-resp/tests/**` | End-to-end over real TCP with an off-the-shelf `redis` client: XADD/XREADGROUP/XACK/XPENDING/XCLAIM/XAUTOCLAIM/XLEN/XDEL/XINFO, error tokens, and Invariant-1/2 reconcile (ADR-007 RESP face). |
| Library (facade) integration | `crates/fireweed/tests/**` | The ergonomic Rust library face: every verb (push/claim/ack/nack/fail/renew/reassign/rearm/purge/peek/claimed/metrics) over real backends. |
| Composition root | `crates/fireweed-server/tests/**` | The wired server: backend selection, background reclaim driver, graceful drain, and end-to-end drivability by a stock client. |
| Security | engine + wire + backend integration tests | Denied paths, tenant isolation, lease-token handling, and payload/log safety. |

## Requirement Coverage Matrix

| Requirement | Governing Artifact | Required Test Evidence |
|-------------|--------------------|------------------------|
| API-006 conformance | API-006 §4, §6, §9 / TP-003 §3.12 | `fireweed-contract-suite` passes on every `Store` implementation with at least one scenario per MUST clause, each citing its clause id, including every CS6 scenario; every scenario also passes through the torn-read adversary; zero skips; no per-backend clause, knob or capability probe (AC-KC-1 to AC-KC-3). |
| API-006 operator separation | API-006 §7 / TP-003 §3.12 | The CI guard finds no reference to the `Operator` trait or the admin handle in Snorri, the workload and loadgen crates, or the contract suite; the operator suite passes OP1–OP3 (AC-KC-4, AC-KC-5). |
| API-006 claim cost (C9) and S2 cost bound | API-006 C9, S2 / TP-003 §3.12 (evidence: SPIKE-002 to SPIKE-006 in `docs/helix/02-design/spikes/`) | Stage-bound claim plans and bounded `stats.groups` reads, per-claim leases, statement discipline, the lean claim row, same-run scale ratios and queue count per file pass AC-PERF-1 to AC-PERF-6 and AC-PERF-8; the per-queue commit cost on MinIO is recorded (AC-PERF-7, a measurement). |
| FR-56 queue metadata | PRD FR-56 / API-006 D1, W13, R4, §5.2 | The contract suite covers a new queue's empty metadata at version 0, a metadata CAS inside a unit with record writes, its rejection rolling the unit back, the 64 KiB bound (LIM1), and fresh `read{queue: true}` (CS6). |
| FR-57 `delete_queue` | PRD FR-57 / API-006 DQ1–DQ5 | The contract suite runs `delete_queue` under both policies on a non-empty queue with a live claim: `NotPermitted` for a full delete under `ArchiveOnly` on an open and on an archived queue, `QueueClosed` for every other op afterwards, including after `reopen`, and `already_closed` on repeat (CS6). |
| FR-58 operator separation | PRD FR-58 / API-006 OP1–OP4 | The operator suite passes OP1–OP3 through the admin handle, and the CI guard finds no `Operator` or admin-handle reference in Snorri, the workload and loadgen crates, or the contract suite (TP-003 AC-KC-4, AC-KC-5). |
| FR-1 | PRD queue namespace isolation | Create two queues in one tenant and same queue IDs in different tenants; routing, metrics, and storage keys remain isolated. |
| FR-2, FR-3, FR-4 | PRD priority model | Create timestamp and non-timestamp queues; valid priorities encode to deterministic `priority_sort`; invalid priorities fail. |
| FR-5, FR-6 | PRD ordering mode immutability | Create strict and bounded-relaxed queues; attempts to mutate priority model or ordering mode fail. |
| FR-7, FR-14 | PRD strict ordering | Strict claim returns eligible items by priority plus tie-breaker under concurrent workers. |
| FR-8, FR-9, FR-12, FR-13 | PRD bounded-relaxed progress | Relaxed claim may reorder within bounds but cannot starve oldest eligible items beyond `progress_bound_ms`. |
| FR-10, FR-11, FR-15, FR-17 | PRD eligibility | Future `not_before`, retry backoff, active lease, and metadata blockers prevent claim; lease expiry preserves progress clock. |
| FR-16 | PRD payload/metadata | Opaque payload and caller metadata round-trip through push and claim without queue-owned interpretation. |
| FR-18, FR-19, FR-21, FR-22 | PRD/API idempotent push | Duplicate `client_item_key` converges; repeated `request_id` replays; conflicting request body fails; retention expiry bounds dedupe. |
| FR-20 | PRD/API batch update | Pending items update priority, `not_before`, payload, and metadata; leased and terminal items return per-item conflicts. |
| FR-23, FR-24, FR-25, FR-26, FR-27, FR-28 | PRD lease lifecycle | Claim creates one active lease; concurrent claims never duplicate; crash/retry after lease expiry redelivers; state survives restart. |
| FR-29, FR-30, FR-31, FR-32, FR-35 | PRD batch/group claim | Batch limit enforced; same-group and metadata-compatible claims return deterministic compatible batches without group starvation. |
| FR-33, FR-34, FR-36, FR-37 | PRD finalize/retry | Complete, fail, retry, release, stale lease, terminal conflict, and retry exhaustion produce correct per-item results. |
| FR-38, FR-39 | PRD terminal durability/retention | Terminal complete/failed facts persist with final command position and are deleted only after configured retention windows. |
| FR-40, FR-41, FR-42 | PRD observability | Metrics expose lifecycle counts, active leases, retry backlog, oldest eligible age, progress-bound risk, throughput, and latency. |
| FR-43 | PRD noisy-neighbor isolation | Hot queue or tenant load does not prevent another queue from meeting claim latency and progress bounds under configured limits. |
| FR-44, FR-45, FR-46, FR-47 | PRD Seventh Sense validation | Timestamp scheduled work can be pushed early, updated later, gated through metadata, and claimed in compatible batches. |
| FR-47a, ADR-004 granularity | ADR-004 / API-001 / ADR-008 | The four client axes (tenant/queue/group/metadata) hold; physical placement (queue owner; the internal `hash(tenant,queue)%N` item-table partition) is never client-visible; claim result order is deterministic within the effective claim domain; 7thsense `job_id`→`group_key` (non-cohort) and `callback_id`→`group_key` (cohort) topologies validate. |
| ADR-008 group co-residency by construction | ADR-008 / ADR-004 / TD-001 / TD-002 | The queue is the unit of sharding, so all items of a `group_key` are co-resident on the queue's single owner **by construction** (no `group_co_residency` flag, no `hash(group_key) mod shard_count`); `whole_group`/`whole_cohort` claims are owner-local and atomic; `same_group_key` remains an item-level filter (may return a partial group). |
| FR-31a, FR-32 (g1 whole-group) | API-001 `group_batching` / TD-001 / TD-002 | `compatibility.group_batching` returns up to `max_groups` whole eligible groups, atomically per group, ordered by the group representative; `same_group_key` remains an item filter (not whole-group); `max_eligible_group_size` enforced at push; `batch-too-large` when the next group will not fit. |
| FR (g6 cohort) | API-001 `cohort_policy`/`whole_cohort` / TD-001 / TD-002 / TD-004 | A `whole_cohort` claim leases one complete, claim-eligible cohort atomically under one shared cohort lease; members are never individually claimable; cohort key = `group_key`; `completion_bound_ms <= progress_bound_ms` enforced; idempotent under duplicate push; cohort never leaks members to other claim units. |
| FR-15, FR-17 (g2 dynamic gates) | API-001 Eligibility Precedence / `SetGates` / TD-002 | `gate_keys`/`SetGates` flip queue-scoped gate state O(1) without rewriting item rows; gate predicate is evaluated at claim/discovery time (anti-join); a blocked gate makes matching items ineligible without accruing progress-bound age beyond policy; one Eligibility Precedence definition is the sole eligibility source. |
| FR-23, FR-49–FR-55 (g5 recurring) | API-001 `recurrence`/`rearm`/`PurgeItems` / TD-002 / TD-004 | `rearm` releases the lease, sets `eligible_since = max(commit_time, not_before)`, resets per-cycle retry, bumps version without terminating; `recurrence.until` drains; in-band `PurgeItems` (P0, per-key/`item_id`) removes the item with tombstone + replay safety; recurring metrics served from the metrics projection (idle excluded from oldest-eligible age and retry backlog); recurrence and cohort are mutually exclusive. |
| FR-48 (g4 discovery) | API-001 `DiscoverActiveScopes` / TD-002 / TD-003 | Tenant-scoped top-N across queues, or owner-local group ranking for one queue, by oldest-eligible age via the single `fireweed_group_summary` projection; gate-current (advance past blocked, not exclude); `as_of` = the owner's observed frontier (min over queues read for the tenant-wide case); per-queue authorization; advisory for routing (`BatchClaim` remains authoritative). |
| FR (per-queue ownership + routing) | TD-003 / TD-006 / ADR-008 | A queue is owned by one node; claims are single-owner-local (no fan-out / k-way merge); single active lease across owner reassignment/drain; a deposed owner's append is epoch-fenced; a wrong-node command is `-MOVED`-redirected to the owner and converges in one hop. (Cross-queue scale magnitude → TP-002 E2.) |
| API-002 operator repair | API-002 / TD-001 / TD-002 | `PauseQueue`/`ResumeQueue` stop/resume claims durably; `RepairItems` (reschedule/force_*/clear_lease) mutates leased/terminal items and fences the active lease; `force_release` preserves the progress clock (FR-11); every repair is a durable command and bumps `item_version`. |
| API-002 redrive (DLQ) | API-002 / TD-002 | `RedriveItems` returns terminal `failed` items by selector to eligible with `retry_count_mode` semantics; redriven items get `eligible_since = commit time`; `max_affected`/`expected_match_count` guards enforced; large spans run async and converge. |
| API-002 bulk purge | API-002 / TD-001 / TD-002 | `PurgeQueueItems` (selector, bulk) is distinct from native per-key `PurgeItems`; writes tombstones; `dry_run` is side-effect-free; runs queue-local on the owner in bounded batches that re-drive and converge; purge of a leased item fences the lease; idempotent (`not_found` on absent). |
| API-002 archive / retention | API-002 / TD-002 | `ArchiveItems` exports/marks-retained before purge (idempotent); `RunRetention` reclaims only within policy. |
| API-002 async operations | API-002 | Selector mutation returns one `operation_id`; replayed `request_id` returns the same id (no second op); `GetOperation` progress is exact at terminal state; `partial`/`failed` is resumable and converges; `CancelOperation` never rolls back committed batches. |
| API-002 operator authorization | API-002 / ADR-002 | `operator:inspect`/`operator:repair`/`operator:purge`/`admin:queue` are deny-by-default and distinct from data-plane grants; a data-plane principal cannot pause, repair, redrive, or purge; denied paths return `operator-forbidden`; audit record emitted without payloads. |
| Eligibility Precedence (single home) | API-001 | The Eligibility Precedence subsection is the only definition of "eligible"/"active"; g1/g4/g5/g6/g7 reference it by name; no second eligibility definition exists in any doc. |
| `fireweed_group_summary` (single projection) | TD-001 / TD-002 / TD-004 | Exactly one per-group summary projection keyed `(tenant_id, queue_id, group_key)` (owner-local; one row per group); `oldest_eligible_at` exact-on-read through the gate predicate; counts MAY lag; the former `fireweed_active_scope_summary` does not exist. |
| API-001 idempotency | API-001 | Request replay, request conflict, claim replay while leases are active, and request-expired after leases end. |
| API-001 auth | API-001 / ADR-002 | Principal authorized for tenant A cannot access tenant B routes or storage-backed data. |
| API-001 claimed-item response shape | API-001 | Every `BatchClaim` result returns the documented field set (`item_id`, `client_item_key`, `item_version`, `lease_token`, `lease_expires_at`, `priority`); conditional fields (`not_before`, `group_key`, `payload`, `metadata`, `gate_keys`) are present/omitted per the rules; `gate_keys` appear only on `gate_keys=dynamic` queues; `whole_cohort` results omit the per-item `lease_token`; the shared conformance now re-claims after `update_fields` and verifies the current `fields` map in the claimed-item shape. |
| API-003 workload integration profile | API-003 / API-001 / API-002 | The scheduled-batch-delivery profile maps producer/worker/finalize obligations onto native primitives; finalize maps only to the five outcomes (`complete`/`fail`/`retry`/`release`/`rearm`); the downstream-rate non-goal is preserved (caller-driven pacing only); archive/retention defers to API-002. Anchored by `product_workflow_scheduled_action_delivery_e2e`. |
| TD-001 durability | TD-001 / API-005 / ADR-024 | After success, the object log recovers the command. Turso rebuilds through `projection_control` and is not the command log. There is no public Class B row. |
| Public storage cell | API-005 / ADR-024 | The only cell is `s3--turso`. It opens, runs lifecycle and transaction assertions, and reports a result. A retired selector must fail before I/O. Missing fixture, disabled required feature, or ignored test is a failure, not a skip. |
| TD-001 backend conformance (conformance-as-contract) | TD-001 / ADR-024 | The public cell passes the shared conformance suite before it is selectable: ordering, eligibility, claim atomicity, idempotency, lease/epoch fencing, per-queue progress, and object-log replay. |
| Response barriers | API-005 / ADR-024 | The only barrier is `AsyncProjection`. AC-TXN-5A is the gate. `Strict` is not a disposition. A missing async spec fails validation before I/O. |
| TD-002 Postgres fencing | TD-002 | Stale `assignment_epoch` appends are rejected; current epoch appends succeed. |
| TD-002 Postgres locking | TD-002 | `FOR UPDATE SKIP LOCKED` claim tests prove single active lease under concurrent workers. |
| TD-003 queue ownership | TD-003 | Deterministic queue-to-owner assignment (target vs active owner), durable epoch fence at acquire, stale-epoch append reject, graceful drain without loss/duplication, interrupted-drain single-writer safety, reassignment recovery from snapshot + log tail, per-queue local progress, and stalled/unowned-queue visibility. |
| TD-004 object-log backend | TD-004 / ADR-001 | Group-commit ack boundary (no command acked before its manifest commit), native conditional-write manifest fencing against the current control-plane (queue) epoch, fail-closed rejection of providers without that capability, in-flight claim reservation, replay-response idempotency, projection snapshot + bounded log-tail recovery, and parity on the shared TD-001 backend conformance suite (incl. group co-residency by construction, cohort, gates, and the queue-scoped single-owner command path). Cost/ack/recovery magnitude → TP-002 E3. |
| TD-005 standalone sqlite backend | TD-005 / ADR-006 | Single-file durable backend: atomic single-transaction append+apply (strict read-after-write, one WAL fsync ack boundary), epoch bootstrap (log/control-plane lockstep) and bump-on-open fencing, single-writer ownership (second opener rejected), reopen recovery preserves committed state (no log-tail replay needed), parity with the in-memory reference on the item-lifecycle conformance dimensions (`shared_conformance`), and the embedder delivery-adapter conformance (`embedder_delivery_conformance`) mapping to 7snx `assert_delivery_queue_adapter_conformance`. NOTE: `client_item_key` convergence is the embedder adapter's responsibility (fireweed converges by `item_id`); see bead pqueue-9ff01321. |
| Queue density / bounded per-node resources | ADR-002 / ADR-003 / TD-001 / TD-002 / TD-003 / TD-004 | Per-queue background work (lease-expiry sweeps, progress-bound aggregation, summary recompute, recurring rearm, idempotency/retention GC) is multiplexed onto bounded shared per-node pools — never one task/loop/connection per queue; per-queue projection handles are LRU-bounded. Density magnitude (≥1000 active queues/node) → TP-002 E2 `queue_density_single_node_tests`. |
| ADR-003 Rust policy | ADR-003 | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, dependency checks, unsafe denial, and the bounded-per-node-background-work rule run/verified in CI. |

## Named Test Suites

Implementation beads should create or extend these suites:

- `kernel_contract_suite` (`fireweed-contract-suite`, API-006 §9; direct and
  torn-read runs)
- `kernel_operator_suite` (API-006 OP1–OP3, through the admin handle)
- `operator_ban_guard` (CI guard for API-006 OP4)
- `claim_path_performance_gates` (TP-003 AC-PERF-1 to AC-PERF-8)
- `core_domain_tests`
- `core_priority_model_tests`
- `core_lifecycle_transition_tests`
- `core_idempotency_tests`
- `core_eligibility_precedence_tests`
- `core_recurrence_rearm_tests`
- `storage_conformance_durability_tests`
- `storage_matrix_transaction_contract_tests` (`s3--turso` only; zero
  missing, ignored, or fixture-skipped rows)
- `storage_conformance_claim_tests`
- `storage_conformance_progress_tests`
- `storage_conformance_group_batching_tests`
- `storage_conformance_cohort_tests`
- `storage_conformance_gate_tests`
- `claimed_item_shape_conformance_tests`
- `storage_conformance_discovery_tests`
- `storage_conformance_ownership_routing_tests`
- `fault_injection_harness_tests`
- `postgres_schema_migration_tests`
- `postgres_transaction_flow_tests`
- `postgres_concurrency_claim_tests`
- `postgres_group_coresidency_tests`
- `postgres_retention_tests`
- `queue_ownership_fencing_tests`
- `queue_reassignment_drain_tests`
- `per_queue_progress_tests`
- `routing_redirect_tests`
- `object_log_commit_recovery_tests`
- `sqlite_projection_tests`
- `sqlite_backend_tests`
- `shared_conformance`
- `embedder_delivery_conformance`
- `service_auth_tenant_tests`
- `service_api_error_semantics_tests`
- `service_gate_tests`
- `service_group_batching_tests`
- `service_cohort_tests`
- `service_recurrence_purge_tests`
- `service_discovery_tests`
- `service_metrics_ground_truth_tests`
- `invariant_stress_matrix_tests`
- `operator_repair_tests`
- `operator_redrive_tests`
- `operator_purge_tests`
- `operator_async_operation_tests`
- `operator_auth_denied_path_tests`
- `performance_hot_queue_10m_tests`
- `performance_batch_operation_tests`
- `product_workflow_scheduled_action_delivery_e2e`
- `product_workflow_marketo_group_batching_e2e`
- `product_workflow_callback_cohort_e2e`
- `product_workflow_jobs_connectors_recurring_e2e`
- `product_workflow_worker_crash_recovery_e2e`
- `product_workflow_noisy_neighbor_scale_e2e`
- `product_workflow_operator_repair_redrive_e2e`
- `product_workflow_generic_priority_bounded_relaxed_e2e`
- `product_workflow_downstream_pacing_non_goal_e2e`
- `seventh_sense_validation_tests` (Seventh-Sense-shaped subset:
  scheduled-action, Marketo group-batching, callback cohort, and recurring
  jobs/connectors workflows)
- `product_validation_tests` (P0/core release suite over AC-E2E-1..6 and
  AC-E2E-8..9)
- `operator_validation_tests` (P1/operator release suite over AC-E2E-7 and
  operator suites)

The Kernel contract section above lists the suites in this list that API-006
supersedes or retires.

Scale, density, and object-log performance suites (`performance_single_deployment_baseline_tests`,
`performance_cross_queue_scale_out_tests`, `queue_density_single_node_tests`,
`object_log_latency_cost_matrix_tests`, `object_log_commit_recovery_tests`,
`external_transaction_contract_matrix_tests`, `recurrence_scale_both_profiles_tests`)
are owned by TP-002 (`tp-scale-substantiation`); see that plan for their pass bars —
except `external_transaction_contract_matrix_tests`, whose acceptance bars are the
AC-TXN rows in TP-003 §3.10. Historical
`docs/perf/evidence/tp003-ac-txn-matrix*.jsonl` records predate the canonical
older register and cannot by themselves qualify `s3--turso` (ADR-024).

## Performance Evidence

Performance testing must include:

- batch push, update, claim, renew, and finalize throughput;
- p95 and p99 latency with telemetry enabled;
- 10M-item hot queue with mixed pending, future, leased, retry, complete, and
  failed states;
- skewed priority distribution;
- group-heavy compatibility claims;
- one hot queue/tenant alongside one small eligible queue;
- Postgres claim query plans showing index use instead of full scans.
- the API-006 claim-path and storage gates (TP-003 §3.12): stage-bound claim
  plans with no stage-blind statement, per-claim lease cost, statement
  discipline, the lean claim row, 1M-to-10M and 100-queue same-run ratios, and
  queue count per file; plus the per-queue commit cost on MinIO at 1, 10, 100
  and 1,000 queues, recorded as a measurement.

## Manual or Deferred Evidence

The following are not required before the first implementation bead, but must be
covered before claiming product validation:

- Seventh Sense production scheduling SLA for concrete `progress_bound_ms`
  validation.
- The operator repair/redrive/purge/archive surface is now **designed** (API-002)
  and its coverage rows + suites are listed above. It is a P1 *build* priority:
  it is required before claiming the operator-enabled product surface verified,
  but it does not block the P0/core v1 verification gate. A compatibility-adapter
  (SQS-shaped) surface and a P2 operator dashboard remain deferred. (Native
  `DiscoverActiveScopes` and native in-band `PurgeItems` are P0/native-service
  and covered above.)
- The later Seventh Sense migration design (mapping existing queue-like tables to
  `BatchPush`/`BatchUpdate`), once it exists. The absence of that migration design
  does **not** defer the generic product workflow suites above; they use PRD-owned
  representative shapes and must run before v1 verification.
- Kafka/Redpanda and DynamoDB backend conformance (later design targets).

Object-log + SQLite projection performance is NOT deferred: it is a committed v1
profile, covered by TP-002 E2/E3.

## Exit Criteria

Before implementation beads are filed, each bead must cite at least one
governing artifact and at least one named test suite from this plan. Before a
backend implementation is accepted, the corresponding conformance, integration,
security, and performance evidence must exist or the gap must be explicitly
recorded as a deferred non-v1 profile.
