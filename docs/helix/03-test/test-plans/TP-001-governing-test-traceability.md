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
  review:
    self_hash: a987698e797f33f52168aba5ba54f41bcc18bd3fcabe278af085afdea7b82768
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
      api-workload-integration-profiles: 3206a0ad7896fa01deb790f1dca95bddab1cbe9d8f69a761cfb041a34498450e
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-postgres-native-reference-mode: 1b657638258f7d3fa15e46b7536d33d766ade1a0948a32598dc5c9ae65b7828b
      td-resp-wire-adapter: d33d11d4e7e087384828e3ca3289d4f0b7bb6aefd88a4245ddb7f441f0706bc6
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
      td-sharding-and-shard-ownership: b98590bc7a51f8e904052d64aaa6ab4d8a9c9729d155d17ee0823ffcf6b64a0d
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
      tp-scale-substantiation: e0ca180cb81c98e7c451341f1ea912bf152ac2c75d422a3b315516fc9f8ee7d3
    reviewed_at: "2026-07-20T20:00:41Z"
---

# Test Plan: TP-001 Governing Test Traceability

## Scope

This plan defines the minimum test coverage required before the first pqueue
implementation can be considered aligned with the current HELIX frame and
design. It maps PRD functional areas and design decisions to named test suites,
including the full gap-closure surface: group-cardinality / whole-group claim
(ADR-004, API-001 `group_batching`), dynamic eligibility gates (`SetGates`),
active-scope discovery (`DiscoverActiveScopes`), recurring / never-terminal items
(`rearm`, `PurgeItems`), atomic complete-cohort claim (`cohort_policy`,
`whole_cohort`), granularity (ADR-004), per-queue ownership with queue-local
progress and client routing (TD-003, TD-006), and the
`object_log_sqlite_projection` second backend (TD-004).

This is a pre-implementation test plan. Exact Rust function names may change
when the workspace is created, but implementation beads must preserve the suite
intent and cite the relevant requirement IDs.

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
| Core unit | `crates/pqueue-core/src/**`, `crates/pqueue-engine/**` | Pure validation, priority encoding, lifecycle, retry, idempotency, version rules, and the engine's decision helpers + dependency-direction guard. |
| Backend conformance | `crates/pqueue-conformance/**` (run by each adapter's `tests/`) | The shared no-stub port-conformance suite executed against every backend combination: memory/sqlite, postgres, object-log/in-memory projection, and object-log/SQLite projection where implemented. It includes backend-independent transaction-contract scenarios (success-visible, rejection-no-effect, unknown-outcome replay), durability, replay, lease, claim, finalize, renew/reassign, purge, and projection-read scenarios. |
| Postgres integration | `crates/pqueue-postgres/tests/**` | The durable-log postgres adapter (TD-004 template) against a real DB, env-gated on `PQUEUE_PG_TEST_URL`: the full conformance suite + a reconnect/durability replay test. |
| Wire (RESP) integration | `crates/pqueue-resp/tests/**` | End-to-end over real TCP with an off-the-shelf `redis` client: XADD/XREADGROUP/XACK/XPENDING/XCLAIM/XAUTOCLAIM/XLEN/XDEL/XINFO, error tokens, and Invariant-1/2 reconcile (ADR-007 RESP face). |
| Library (facade) integration | `crates/pqueue/tests/**` | The ergonomic Rust library face: every verb (push/claim/ack/nack/fail/renew/reassign/rearm/purge/peek/claimed/metrics) over real backends. |
| Composition root | `crates/pqueue-server/tests/**` | The wired server: backend selection, background reclaim driver, graceful drain, and end-to-end drivability by a stock client. |
| Security | engine + wire + backend integration tests | Denied paths, tenant isolation, lease-token handling, and payload/log safety. |

## Requirement Coverage Matrix

| Requirement | Governing Artifact | Required Test Evidence |
|-------------|--------------------|------------------------|
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
| FR-15, FR-17a (g2 dynamic gates) | API-001 Eligibility Precedence / `SetGates` / TD-002 | `gate_keys`/`SetGates` flip queue-scoped gate state O(1) without rewriting item rows; gate predicate is evaluated at claim/discovery time (anti-join); a blocked gate makes matching items ineligible without accruing progress-bound age beyond policy; one Eligibility Precedence definition is the sole eligibility source. |
| FR-23, FR-49–FR-55 (g5 recurring) | API-001 `recurrence`/`rearm`/`PurgeItems` / TD-002 / TD-004 | `rearm` releases the lease, sets `eligible_since = max(commit_time, not_before)`, resets per-cycle retry, bumps version without terminating; `recurrence.until` drains; in-band `PurgeItems` (P0, per-key/`item_id`) removes the item with tombstone + replay safety; recurring metrics served from the metrics projection (idle excluded from oldest-eligible age and retry backlog); recurrence and cohort are mutually exclusive. |
| FR-48 (g4 discovery) | API-001 `DiscoverActiveScopes` / TD-002 / TD-003 | Tenant-scoped top-N across queues, or owner-local group ranking for one queue, by oldest-eligible age via the single `pqueue_group_summary` projection; gate-current (advance past blocked, not exclude); `as_of` = the owner's observed frontier (min over queues read for the tenant-wide case); per-queue authorization; advisory for routing (`BatchClaim` remains authoritative). |
| FR (per-queue ownership + routing) | TD-003 / TD-006 / ADR-008 | A queue is owned by one node; claims are single-owner-local (no fan-out / k-way merge); single active lease across owner reassignment/drain; a deposed owner's append is epoch-fenced; a wrong-node command is `-MOVED`-redirected to the owner and converges in one hop. (Cross-queue scale magnitude → TP-002 E2.) |
| API-002 operator repair | API-002 / TD-001 / TD-002 | `PauseQueue`/`ResumeQueue` stop/resume claims durably; `RepairItems` (reschedule/force_*/clear_lease) mutates leased/terminal items and fences the active lease; `force_release` preserves the progress clock (FR-11); every repair is a durable command and bumps `item_version`. |
| API-002 redrive (DLQ) | API-002 / TD-002 | `RedriveItems` returns terminal `failed` items by selector to eligible with `retry_count_mode` semantics; redriven items get `eligible_since = commit time`; `max_affected`/`expected_match_count` guards enforced; large spans run async and converge. |
| API-002 bulk purge | API-002 / TD-001 / TD-002 | `PurgeQueueItems` (selector, bulk) is distinct from native per-key `PurgeItems`; writes tombstones; `dry_run` is side-effect-free; runs queue-local on the owner in bounded batches that re-drive and converge; purge of a leased item fences the lease; idempotent (`not_found` on absent). |
| API-002 archive / retention | API-002 / TD-002 | `ArchiveItems` exports/marks-retained before purge (idempotent); `RunRetention` reclaims only within policy. |
| API-002 async operations | API-002 | Selector mutation returns one `operation_id`; replayed `request_id` returns the same id (no second op); `GetOperation` progress is exact at terminal state; `partial`/`failed` is resumable and converges; `CancelOperation` never rolls back committed batches. |
| API-002 operator authorization | API-002 / ADR-002 | `operator:inspect`/`operator:repair`/`operator:purge`/`admin:queue` are deny-by-default and distinct from data-plane grants; a data-plane principal cannot pause, repair, redrive, or purge; denied paths return `operator-forbidden`; audit record emitted without payloads. |
| Eligibility Precedence (single home) | API-001 | The Eligibility Precedence subsection is the only definition of "eligible"/"active"; g1/g4/g5/g6/g7 reference it by name; no second eligibility definition exists in any doc. |
| `pqueue_group_summary` (single projection) | TD-001 / TD-002 / TD-004 | Exactly one per-group summary projection keyed `(tenant_id, queue_id, group_key)` (owner-local; one row per group); `oldest_eligible_at` exact-on-read through the gate predicate; counts MAY lag; the former `pqueue_active_scope_summary` does not exist. |
| API-001 idempotency | API-001 | Request replay, request conflict, claim replay while leases are active, and request-expired after leases end. |
| API-001 auth | API-001 / ADR-002 | Principal authorized for tenant A cannot access tenant B routes or storage-backed data. |
| API-001 claimed-item response shape | API-001 | Every `BatchClaim` result returns the documented field set (`item_id`, `client_item_key`, `item_version`, `lease_token`, `lease_expires_at`, `priority`); conditional fields (`not_before`, `group_key`, `payload`, `metadata`, `gate_keys`) are present/omitted per the rules; `gate_keys` appear only on `gate_keys=dynamic` queues; `whole_cohort` results omit the per-item `lease_token`; the shared conformance now re-claims after `update_fields` and verifies the current `fields` map in the claimed-item shape. |
| API-003 workload integration profile | API-003 / API-001 / API-002 | The scheduled-batch-delivery profile maps producer/worker/finalize obligations onto native primitives; finalize maps only to the five outcomes (`complete`/`fail`/`retry`/`release`/`rearm`); the downstream-rate non-goal is preserved (caller-driven pacing only); archive/retention defers to API-002. Anchored by `product_workflow_scheduled_action_delivery_e2e`. |
| TD-001 durability | TD-001 | Kill process after acknowledged append; replay or committed Postgres rows preserve the command and projection state. |
| TD-001 backend conformance (conformance-as-contract) | TD-001 / ADR-008 | Every backend passes the shared conformance suite before it is selectable: the **core** class (substrate-independent behavior incl. ordering, eligibility, claim atomicity, idempotency, lease/epoch fencing, per-queue progress) binds every backend; the **log** class (replay/snapshot+tail/segment-commit) binds log-bearing backends; the **relational reconnect-after-crash** class binds the transactional-authoritative relational projection. The two projection families are held behaviorally identical by this suite. |
| TD-002 Postgres fencing | TD-002 | Stale `assignment_epoch` appends are rejected; current epoch appends succeed. |
| TD-002 Postgres locking | TD-002 | `FOR UPDATE SKIP LOCKED` claim tests prove single active lease under concurrent workers. |
| TD-003 queue ownership | TD-003 | Deterministic queue-to-owner assignment (target vs active owner), durable epoch fence at acquire, stale-epoch append reject, graceful drain without loss/duplication, interrupted-drain single-writer safety, reassignment recovery from snapshot + log tail, per-queue local progress, and stalled/unowned-queue visibility. |
| TD-004 object-log backend | TD-004 / ADR-001 | Group-commit ack boundary (no command acked before its manifest commit), manifest-CAS fencing against the current control-plane (queue) epoch (and the Postgres-pointer fallback on no-CAS stores), in-flight claim reservation, replay-response idempotency, SQLite snapshot + bounded log-tail recovery, and parity on the shared TD-001 backend conformance suite (incl. group co-residency by construction, cohort, gates, and the queue-scoped single-owner command path). Cost/ack/recovery magnitude → TP-002 E3. |
| TD-005 standalone sqlite backend | TD-005 / ADR-006 | Single-file durable backend: atomic single-transaction append+apply (strict read-after-write, one WAL fsync ack boundary), epoch bootstrap (log/control-plane lockstep) and bump-on-open fencing, single-writer ownership (second opener rejected), reopen recovery preserves committed state (no log-tail replay needed), parity with the in-memory reference on the item-lifecycle conformance dimensions (`shared_conformance`), and the embedder delivery-adapter conformance (`embedder_delivery_conformance`) mapping to 7snx `assert_delivery_queue_adapter_conformance`. NOTE: `client_item_key` convergence is the embedder adapter's responsibility (pqueue converges by `item_id`); see bead pqueue-9ff01321. |
| Queue density / bounded per-node resources | ADR-002 / ADR-003 / TD-001 / TD-002 / TD-003 / TD-004 | Per-queue background work (lease-expiry sweeps, progress-bound aggregation, summary recompute, recurring rearm, idempotency/retention GC) is multiplexed onto bounded shared per-node pools — never one task/loop/connection per queue; per-queue projection handles are LRU-bounded. Density magnitude (≥1000 active queues/node) → TP-002 E2 `queue_density_single_node_tests`. |
| ADR-003 Rust policy | ADR-003 | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, dependency checks, unsafe denial, and the bounded-per-node-background-work rule run/verified in CI. |

## Named Test Suites

Implementation beads should create or extend these suites:

- `core_domain_tests`
- `core_priority_model_tests`
- `core_lifecycle_transition_tests`
- `core_idempotency_tests`
- `core_eligibility_precedence_tests`
- `core_recurrence_rearm_tests`
- `storage_conformance_durability_tests`
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

Scale, density, and object-log performance suites (`performance_single_deployment_baseline_tests`,
`performance_cross_queue_scale_out_tests`, `queue_density_single_node_tests`,
`object_log_latency_cost_matrix_tests`, `object_log_commit_recovery_tests`,
`external_transaction_contract_matrix_tests`, `recurrence_scale_both_profiles_tests`)
are owned by TP-002 (`tp-scale-substantiation`); see that plan for their pass bars —
except `external_transaction_contract_matrix_tests`, whose acceptance bars are the
AC-TXN rows in TP-003 §3.10 (implemented and evidenced as of v0.11.0, with the
segment-object-reclamation residual tracked as bead `pqueue-b5cc2bc7`; evidence in
`docs/perf/evidence/tp003-ac-txn-matrix*.jsonl`).

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
