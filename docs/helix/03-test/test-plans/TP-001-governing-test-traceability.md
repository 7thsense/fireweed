---
ddx:
  id: tp-governing-test-traceability
  depends_on:
    - prd
    - api-native-client-interface
    - api-operator-repair-contract
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-rust-workspace-and-toolchain-policy
    - adr-granularity-mapping-and-claim-domain
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-sharding-and-shard-ownership
    - td-s3-object-log-sqlite-projection-mode
    - tp-scale-substantiation
  review:
    self_hash: 1df6ca1830db0b53ee8aaeca8fa73fab6fbbd578a4718757616815c985ae06ae
    deps:
      adr-auth-tenancy-and-storage-isolation: 032d34fcd4b1f8f9635686537cf579808d339f92494ecdfa56ca18462d338ad9
      adr-cqrs-log-projection-storage-model: 709f701130b5bd00666a1abeef4fb104555a623d39b9fec1fdb9b3167789de10
      adr-granularity-mapping-and-claim-domain: ba2d4c26c9fcaa4470ea65b61eff20cf382b6bba9e261cbd453f13122bfbc7c8
      adr-rust-workspace-and-toolchain-policy: 1f0c7eb647424e5ff2875cf5726f5de88b88276fabd7f203424ace231c1f6ab2
      api-native-client-interface: f90b0c65a65c4b088b9b04cb28ca0d5b0d174acf7cdfc326bcd859d79c7d1762
      api-operator-repair-contract: 65ec2e36500a6c404ae53af1a65da26fcdcc0a07e0ef1578bae30ec94f2be6e6
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
      td-postgres-native-reference-mode: 443e433bb2fa0ac55f95cb9ad02d35f8486e5e015967fb69807a3a50b97474c3
      td-s3-object-log-sqlite-projection-mode: ad13dfdb71f453157fc867e42582d9abfa99718beeb07c88c65e42cda2907ecf
      td-sharding-and-shard-ownership: f962d0f302d06d256b30abad82b1da033df39b89630763b8be3a3954bc502aa7
      td-storage-architecture-backend-contracts: 5980a5612e178fc0828f567f21efaafd9d49cf7e62b2d8655bf7b9ef32e97d8d
      tp-scale-substantiation: 1e6b2b70c2f613ac9999e7e295c2c2845c76b2d69eaed81f949785d2ab5d51a7
    reviewed_at: "2026-06-16T17:42:59Z"
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
`whole_cohort`), granularity / group co-residency (ADR-004), multi-shard claim
with cross-shard queue-global progress (TD-001, TD-003), and the
`object_log_sqlite_projection` second backend (TD-004).

This is a pre-implementation test plan. Exact Rust function names may change
when the workspace is created, but implementation beads must preserve the suite
intent and cite the relevant requirement IDs.

Scale, queue-density, and horizontal-envelope evidence (the per-queue throughput
floor, ≥1000-active-queue density, multi-shard scale-out, and object-log
cost/ack/recovery) are owned by the **scale-substantiation test plan**
(`tp-scale-substantiation`, TP-002, evidence records E0–E3). This governing plan
references TP-002 for those records rather than restating them; the two plans are
complementary and non-overlapping.

## Test Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| Core unit | `crates/pqueue-core/src/**` | Pure validation, priority encoding, lifecycle, retry, idempotency, and version rules. |
| Storage conformance | `crates/pqueue-storage/tests/**` | Backend-independent durability, replay, lease, idempotency, and progress scenarios. |
| Postgres integration | `crates/pqueue-postgres/tests/**` | TD-002 schema, transaction, locking, retention, and fencing behavior against real Postgres. |
| Service/API integration | `crates/pqueue-service/tests/**` | HTTP route, auth, tenant, error shape, and API-001 response behavior. |
| Performance | `benches/**` or `crates/*/benches/**` | Batch throughput, claim latency, 10M-item projection/query fixtures, telemetry overhead. |
| Security | service and backend integration tests | Denied paths, tenant isolation, lease-token handling, and payload/log safety. |

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
| FR-47a, ADR-004 granularity | ADR-004 / API-001 | The four client axes (tenant/queue/group/metadata) hold; `shard_id` is never client-visible; claim result order is deterministic within the effective claim domain; 7thsense `job_id`→`group_key` (non-cohort) and `callback_id`→`group_key` (cohort) topologies validate. |
| ADR-004 group co-residency | ADR-004 / TD-001 / TD-002 | `group_co_residency=true` ⇒ all items of a `group_key` resolve to one shard (`hash(group_key) mod shard_count`); a non-co-resident `group_key` is an item-level filter only (may return a partial group) and never routes to a single shard. |
| FR-31a, FR-32 (g1 whole-group) | API-001 `group_batching` / TD-001 / TD-002 | `compatibility.group_batching` returns up to `max_groups` whole eligible groups, atomically per group, ordered by the group representative; `same_group_key` remains an item filter (not whole-group); `max_eligible_group_size` enforced at push; `batch-too-large` when the next group will not fit. |
| FR (g6 cohort) | API-001 `cohort_policy`/`whole_cohort` / TD-001 / TD-002 / TD-004 | A `whole_cohort` claim leases one complete, claim-eligible cohort atomically under one shared cohort lease; members are never individually claimable; cohort key = `group_key`; `completion_bound_ms <= progress_bound_ms` enforced; idempotent under duplicate push; cohort never leaks members to other claim units. |
| FR-15, FR-17a (g2 dynamic gates) | API-001 Eligibility Precedence / `SetGates` / TD-002 | `gate_keys`/`SetGates` flip queue-scoped gate state O(1) without rewriting item rows; gate predicate is evaluated at claim/discovery time (anti-join); a blocked gate makes matching items ineligible without accruing progress-bound age beyond policy; one Eligibility Precedence definition is the sole eligibility source. |
| FR-23, FR-49–FR-55 (g5 recurring) | API-001 `recurrence`/`rearm`/`PurgeItems` / TD-002 / TD-004 | `rearm` releases the lease, sets `eligible_since = max(commit_time, not_before)`, resets per-cycle retry, bumps version without terminating; `recurrence.until` drains; in-band `PurgeItems` (P0, per-key/`item_id`) removes the item with tombstone + replay safety; recurring metrics served from the metrics projection (idle excluded from oldest-eligible age and retry backlog); recurrence and cohort are mutually exclusive. |
| FR-48 (g4 discovery) | API-001 `DiscoverActiveScopes` / TD-002 / TD-003 | Tenant-scoped top-N across queues, or queue-global group ranking across shards, by oldest-eligible age via the single `pqueue_group_summary` projection; gate-current (advance past blocked, not exclude); `as_of` = min observed frontier across shards; per-queue authorization; advisory for routing (`BatchClaim` remains authoritative). |
| FR (multi-shard) | TD-001 / TD-003 | A queue with `shard_count > 1` fans out non-group claims with a deterministic k-way merge within global `max_items`; single active lease across rebalance/drain; cross-shard queue-global `oldest_eligible_age_ms` = max effective age over shards; stale-epoch appends rejected. (Scale magnitude → TP-002 E2.) |
| API-002 operator repair | API-002 / TD-001 / TD-002 | `PauseQueue`/`ResumeQueue` stop/resume claims durably; `RepairItems` (reschedule/force_*/clear_lease) mutates leased/terminal items and fences the active lease; `force_release` preserves the progress clock (FR-11); every repair is a durable command and bumps `item_version`. |
| API-002 redrive (DLQ) | API-002 / TD-002 | `RedriveItems` returns terminal `failed` items by selector to eligible with `retry_count_mode` semantics; redriven items get `eligible_since = commit time`; `max_affected`/`expected_match_count` guards enforced; large spans run async and converge. |
| API-002 bulk purge | API-002 / TD-001 / TD-002 | `PurgeQueueItems` (selector, bulk) is distinct from native per-key `PurgeItems`; writes tombstones; `dry_run` is side-effect-free; multi-shard split + partial-commit re-drive converges; purge of a leased item fences the lease; idempotent (`not_found` on absent). |
| API-002 archive / retention | API-002 / TD-002 | `ArchiveItems` exports/marks-retained before purge (idempotent); `RunRetention` reclaims only within policy. |
| API-002 async operations | API-002 | Selector mutation returns one `operation_id`; replayed `request_id` returns the same id (no second op); `GetOperation` progress is exact at terminal state; `partial`/`failed` is resumable and converges; `CancelOperation` never rolls back committed shards. |
| API-002 operator authorization | API-002 / ADR-002 | `operator:inspect`/`operator:repair`/`operator:purge`/`admin:shard` are deny-by-default and distinct from data-plane grants; a data-plane principal cannot pause, repair, redrive, or purge; denied paths return `operator-forbidden`; audit record emitted without payloads. |
| Eligibility Precedence (single home) | API-001 | The Eligibility Precedence subsection is the only definition of "eligible"/"active"; g1/g4/g5/g6/g7 reference it by name; no second eligibility definition exists in any doc. |
| `pqueue_group_summary` (single projection) | TD-001 / TD-002 / TD-004 | Exactly one shard-scoped per-group summary projection `(tenant_id, queue_id, shard_id, group_key)`; `oldest_eligible_at` exact-on-read through the gate predicate; counts MAY lag; the former `pqueue_active_scope_summary` does not exist. |
| API-001 idempotency | API-001 | Request replay, request conflict, claim replay while leases are active, and request-expired after leases end. |
| API-001 auth | API-001 / ADR-002 | Principal authorized for tenant A cannot access tenant B routes or storage-backed data. |
| TD-001 durability | TD-001 | Kill process after acknowledged append; replay or committed Postgres rows preserve the command and projection state. |
| TD-001 backend conformance | TD-001 | Every backend passes shared conformance before it is selectable by backend profile. |
| TD-002 Postgres fencing | TD-002 | Stale `assignment_epoch` appends are rejected; current epoch appends succeed. |
| TD-002 Postgres locking | TD-002 | `FOR UPDATE SKIP LOCKED` claim tests prove single active lease under concurrent workers. |
| TD-003 shard ownership | TD-003 | Deterministic assignment (target vs active owner), durable epoch fence at acquire, stale-epoch append reject, graceful drain without loss/duplication, interrupted-drain single-writer safety, reassignment recovery from snapshot + log tail, cross-shard queue-global progress aggregation, and stalled/unowned-shard visibility. |
| TD-004 object-log backend | TD-004 / ADR-001 | Group-commit ack boundary (no command acked before its manifest commit), manifest-CAS fencing against the current control-plane epoch (and the Postgres-pointer fallback on no-CAS stores), in-flight claim reservation, replay-response idempotency, SQLite snapshot + bounded log-tail recovery, and parity on the shared TD-001 backend conformance suite (incl. group co-residency, cohort, gates, multi-shard rows). Cost/ack/recovery magnitude → TP-002 E3. |
| Queue density / bounded per-node resources | ADR-002 / ADR-003 / TD-001 / TD-002 / TD-003 / TD-004 | Per-queue and per-`(queue,shard)` background work (lease-expiry sweeps, cross-shard progress aggregation, summary recompute, recurring rearm, idempotency/retention GC) is multiplexed onto bounded shared per-node pools — never one task/loop/connection per queue or shard; per-shard projection handles are LRU-bounded. Density magnitude (≥1000 active queues/node) → TP-002 E2 `queue_density_single_node_tests`. |
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
- `storage_conformance_discovery_tests`
- `storage_conformance_multi_shard_tests`
- `fault_injection_harness_tests`
- `postgres_schema_migration_tests`
- `postgres_transaction_flow_tests`
- `postgres_concurrency_claim_tests`
- `postgres_group_coresidency_tests`
- `postgres_retention_tests`
- `sharding_assignment_fencing_tests`
- `sharding_rebalance_drain_tests`
- `cross_shard_progress_tests`
- `object_log_commit_recovery_tests`
- `sqlite_projection_tests`
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
`performance_multi_shard_scale_out_tests`, `queue_density_single_node_tests`,
`object_log_commit_recovery_tests` cost/ack rows, `recurrence_scale_both_profiles_tests`)
are owned by TP-002 (`tp-scale-substantiation`); see that plan for their pass bars.

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
