---
ddx:
  id: build-implementation-plan
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
    - tp-governing-test-traceability
    - tp-scale-substantiation
    - tp-verification-acceptance-criteria
  review:
    self_hash: 55528ea72af327659536b155d61bda5984387104871c7e38707173f7aad5c542
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      td-postgres-native-reference-mode: b58232f3c0b56c50bc1e5f01e13afc71ed1c333987498bbabc88c322f80b36e0
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
      td-storage-architecture-backend-contracts: 430d0dc1f83fa62aeb19948efd2a84f5c31df7d15195e51c8296c93c711919f5
      tp-governing-test-traceability: 8ecccaec72a8214b0e3f1a411cc6d642a096398e09c4c0b90d19ad4f3cebb094
      tp-scale-substantiation: 39792548c579ce686ad8f57017bfcd49f56fe584443ffedd29baf149ba641cb0
      tp-verification-acceptance-criteria: ef7d361e7736e99e509f94bbc0b0d435eef558851bc6272527781efa91e5ec08
    reviewed_at: "2026-07-11T01:06:39Z"
---

# Build Plan: BUILD-001 Implementation Sequence

> **Superseded-as-target note (ADR-008 reframe).** The intra-queue-shard build
> items below — the `shard_count` policy fields (B-010), the `group_co_residency`
> shape work (B-051), the cross-shard portions of discovery (B-054), **B-072
> "Multi-shard claim, progress, discovery, and command convergence" and its
> entire "Multi-Shard Sub-Decomposition"**, the cross-shard convergence pieces of
> B-080/B-081, and INV-9's `shard_count > 1` placement — describe the **prior
> intra-queue-shard build**. Under ADR-008 (the queue is the unit of sharding)
> they are **retired as targets**: horizontal scale is cross-queue (per-queue
> ownership + routing, TD-003/TD-006), there is no `shard_count` /
> `group_co_residency`, claims are single-owner-local (no fan-out / k-way merge),
> and the per-group summary is keyed `(tenant, queue, group_key)`. This build plan
> will be **re-decomposed** for the per-queue model in the later code-build phase
> (the spec cascade was reframed first, doc-only); the durable substrate items
> (core types, durable log + projection, object-log group-commit/manifest/fence/
> recovery, conformance suite) carry forward. Treat the multi-shard beads here as
> historical sequencing, not the current target.

## Scope

This is the canonical build sequencing artifact for pqueue's first
implementation. It translates the PRD, API contracts, ADRs, TDs, and test plans
into bounded implementation slices and DDx beads. It does not add product or
design decisions; when a slice needs a decision not present in the governing
artifacts, the bead must stop and request a doc update instead of inventing
scope.

**Governing Artifacts**:

- `docs/helix/01-frame/prd.md`
- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/contracts/API-002-operator-repair-contract.md`
- `docs/helix/02-design/adr/ADR-001-cqrs-log-projection-storage-model.md`
- `docs/helix/02-design/adr/ADR-002-auth-tenancy-and-storage-isolation.md`
- `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`
- `docs/helix/02-design/adr/ADR-004-granularity-mapping-and-claim-domain.md`
- `docs/helix/02-design/technical-designs/TD-001-storage-architecture-backend-contracts.md`
- `docs/helix/02-design/technical-designs/TD-002-postgres-native-reference-mode.md`
- `docs/helix/02-design/technical-designs/TD-003-sharding-and-shard-ownership.md`
- `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
- `docs/helix/03-test/test-plans/TP-001-governing-test-traceability.md`
- `docs/helix/03-test/test-plans/TP-002-scale-substantiation.md`
- `docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md`

**Out of Scope for BUILD-001**:

- SQS-shaped compatibility adapter.
- Hosted dashboard.
- Kafka/Redpanda and DynamoDB backend implementations.
- Seventh Sense migration design from existing tables into pqueue commands.
- Production `progress_bound_ms` value selection; tests use configured fixture
  bounds until the external SLA lands.
  <!-- pqueue-deferral: progress_bound_ms; owner=Erik; reason="external production SLA input pending"; recheck=2026-07-15 -->

## Shared Constraints

- Rust workspace and crate dependency flow follow ADR-003. `pqueue-core` remains
  runtime-free and has no pqueue crate dependencies.
- `#![forbid(unsafe_code)]` is enforced in all initial crates; any exception
  needs a later ADR/TD.
- The native API is API-001. API-002 is a designed P1 operator surface: it is
  required before claiming operator support, but it does not block P0/core v1
  verification.
- Every storage backend must pass the same TD-001 conformance suite before it is
  selectable by backend profile.
- Every per-queue and per-`(queue,shard)` background path must multiplex onto
  bounded shared per-node resources per ADR-003: lease-expiry sweeps, summary
  recompute, recurring rearm, retention/idempotency GC, cross-shard aggregation,
  and projection handles must never create one task, loop, connection, or open
  projection handle per queue or shard. B-071 validates this late, but every
  earlier slice that adds background work must honor it from the start.
- P0/core v1 verification is TP-003 §7 items 1-6: core invariants INV-1..10,
  conformance, CI gates, TP-002 E0-E3, every TP-003 §3 P0 AC at its stated bar
  as owned by individual slice gates, and `product_validation_tests` over
  AC-E2E-1..6 and AC-E2E-8..9.
- Operator-enabled verification is TP-003 §7 item 7: API-002 suites plus
  AC-E2E-7.
- AC-CLAIM-6 is split by product surface: active/expired/stale-token renewal is
  P0 and owned by B-041; fenced-after-operator renewal is P1 and owned by B-100
  with INV-11. P0 ledger evidence must not claim the operator-fenced assertion.
- Product E2E smoke (`PQUEUE_E2E_SCALE=smoke`) is a per-PR gate once a suite
  exists. Release E2E uses the row-specific release shapes in TP-003 §3.11.

## Implementation Slices

| Slice | Area | Governing Artifacts | Depends On | Validation Gate | Notes |
|-------|------|---------------------|------------|-----------------|-------|
| B-001 | Workspace and CI foundation | ADR-003, TP-003 §5 | None | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` | Creates the Rust toolchain, ADR-003 initial crates (`pqueue-core`, `pqueue-storage`, `pqueue-postgres`, `pqueue-service`, `pqueue-client`), CI scaffolding, dependency policy, unsafe denial, and coverage/property/fuzz placeholders. The second-backend crates are added later by B-080. |
| B-010 | Core API/domain types | API-001, API-002, ADR-004 | B-001 | `cargo test -p pqueue-core core_domain_tests` | IDs, queue definitions, priority values, metadata, group/cohort/recurrence config, API result/error types; includes CreateQueue shape validations for mutually exclusive recurrence/cohort fields, `completion_bound_ms <= progress_bound_ms`, `group_co_residency` preconditions, and shard-count policy fields. |
| B-011 | Core priority and ordering | PRD FR-2..FR-8, API-001, TP-003 AC-CORE-1 | B-010 | `cargo test -p pqueue-core core_priority_model_tests` | Includes timestamp and non-timestamp priority models so pqueue is not timestamp-only. |
| B-012 | Core lifecycle, idempotency, retry, eligibility | API-001, TP-003 AC-CORE-2..4, AC-CLAIM-3 | B-010 | `cargo test -p pqueue-core core_lifecycle_transition_tests core_idempotency_tests core_eligibility_precedence_tests core_recurrence_rearm_tests` | Establishes the pure state machine before storage. AC-CLAIM-3 coverage here is the pure Eligibility Precedence evaluator; dynamic gate runtime coverage completes in B-050. |
| B-020 | Storage traits and conformance harness | TD-001, TP-003 §4 | B-010..B-012 | `cargo test -p pqueue-storage storage_conformance --no-default-features` | Defines `LogStore`, `ProjectionStore`, `SnapshotStore`, `ControlPlaneStore`, command envelopes, positions, fixtures, and a reference in-memory backend so the conformance harness is executable before real backends exist. |
| B-021 | Fault-injection harness | TP-003 §2, §3.10, §3.11 | B-020 | `cargo test -p pqueue-storage fault_injection_harness_tests` | Shared replay, partial-append, deterministic failure scheduler, commit/apply/response crash points, and reusable worker/service process-kill hooks used by AC-TXN-1..6, AC-CLAIM-2, AC-SHARD-3, AC-E2E-2/3/5/7. |
| B-030 | Postgres control plane | TD-002, ADR-002 | B-020 | `cargo test -p pqueue-postgres postgres_schema_migration_tests postgres_transaction_flow_tests` | Queue definitions, tenant scope, backend profile, shard assignments, assignment epochs, and CreateQueue cross-field enforcement. Single-shard reference mode gets a static initial assignment and epoch so B-041 can fence appends before the TD-003 ownership lifecycle lands. |
| B-040 | Postgres-native append/projection/write path | TD-001, TD-002, API-001 | B-030 | `cargo test -p pqueue-postgres postgres_transaction_flow_tests` | `BatchPush`, `BatchUpdate`, command/idempotency records, terminal retention basics. |
| B-041 | Postgres-native claim/renew/finalize path | TD-002, TP-003 AC-CLAIM-1..5, AC-CLAIM-6 base renewal | B-040, B-021 | `cargo test -p pqueue-postgres postgres_concurrency_claim_tests`; `cargo test -p pqueue-storage storage_conformance_claim_tests storage_conformance_progress_tests` | Single active lease, lease renewal for active/expired/stale tokens, expiry redelivery, strict/bounded-relaxed claim, finalize outcomes, and progress-bound guards. Operator-fenced lease renewal belongs to B-100. Lease-expiry/background paths use bounded shared workers. |
| B-042 | Postgres-native durability/idempotency/replay | TD-001, TD-002, TP-003 INV-5, INV-10 | B-041 | `cargo test -p pqueue-storage storage_conformance_durability_tests` | Kill-after-ack, replay response, request conflict, retention windows. |
| B-043 | Canonical per-group summary projection | TD-001, TD-002, ADR-004, TP-003 AC-DISC-1 single-shard basis, AC-OBS-1 postgres basis | B-041 | `cargo test -p pqueue-postgres storage_conformance_progress_tests storage_conformance_discovery_tests` | Creates and maintains the single `pqueue_group_summary` projection for push/update/claim/finalize/retry mutation paths, exact `oldest_eligible_at`, bounded approximate counts, and shard-scoped keys. B-050/B-053 extend summary maintenance for gate flips, rearm, and purge. |
| B-044 | Retention, compaction, and GC correctness | TD-001, TD-002, PRD FR-22/38/39 | B-042, B-043 | `cargo test -p pqueue-postgres postgres_retention_tests` | Request-idempotency, item-key, terminal, command-log, tombstone, and audit-window retention; deletes only when replay/audit windows permit; GC uses bounded shared workers. |
| B-060 | API-001 service and client foundation | API-001, ADR-002, TP-003 AC-SEC-1..2 | B-041 | `cargo test -p pqueue-service service_api_error_semantics_tests service_auth_tenant_tests`; `cargo test -p pqueue-client` | HTTP/JSON app foundation, auth context, tenant isolation, lease-token hashing, shared route/handler scaffolding, and client facade used by feature slices. |
| B-050 | Dynamic gates and eligibility projections | API-001, TD-002, TP-003 AC-GATE-1..2, AC-LAT-2 | B-043, B-060 | `cargo test -p pqueue-postgres storage_conformance_gate_tests`; `cargo test -p pqueue-service service_gate_tests`; `cargo test -p pqueue-client` | `SetGates`, gate anti-join, no item-row rewrite, exact oldest-eligible behavior, API-001 route/handler/client binding, and timing hooks for AC-LAT-2 gate-flip latency; gate/summary background recompute uses bounded shared workers. |
| B-051 | Group batching and per-group summary consumers | ADR-004, API-001, TD-002, TP-003 AC-GRP-1..2 | B-043, B-050, B-060 | `cargo test -p pqueue-postgres storage_conformance_group_batching_tests postgres_group_coresidency_tests`; `cargo test -p pqueue-service service_group_batching_tests`; `cargo test -p pqueue-client` | `group_co_residency`, whole-group atomic claim, `same_group_key` as item filter, API-001 claim-option route/handler/client binding, and use of the canonical summary projection. |
| B-052 | Cohort claims | API-001, ADR-004, TD-002, TP-003 AC-COH-1..2 | B-051, B-060 | `cargo test -p pqueue-postgres storage_conformance_cohort_tests`; `cargo test -p pqueue-service service_cohort_tests`; `cargo test -p pqueue-client` | Complete cohort atomic lease, incomplete expiry, no member leakage, and API-001 cohort route/handler/client binding. |
| B-053 | Recurring queues and native purge | API-001, TD-002, TP-003 AC-REC-1..3 | B-041, B-043, B-060 | `cargo test -p pqueue-core core_recurrence_rearm_tests`; `cargo test -p pqueue-storage storage_conformance_durability_tests`; `cargo test -p pqueue-service service_recurrence_purge_tests`; `cargo test -p pqueue-client` | `rearm`, per-cycle retry reset, idle metrics, `PurgeItems`, tombstone/replay safety, API-001 recurrence/purge route/handler/client binding, and summary recompute for rearmed/purged items. |
| B-054 | Active-scope discovery and metrics | API-001, TD-002, TD-003, TP-003 AC-DISC-1 single-shard, AC-OBS-1 postgres profile, AC-LAT-3 | B-043, B-050..B-053, B-060 | `cargo test -p pqueue-storage storage_conformance_discovery_tests`; `cargo test -p pqueue-service service_discovery_tests service_metrics_ground_truth_tests` | Single-shard Top-N ranking, exact oldest age, bounded count lag, auth-filtered service discovery, and query-plan assertions for no full scan; summary aggregation uses bounded shared workers. Cross-shard portions of AC-DISC-1 and AC-DISC-2 are owned by B-072. |
| B-061 | Product E2E smoke harness | TP-001, TP-003 §3.11 | B-060, B-054, B-021 | `PQUEUE_BACKEND_PROFILE=postgres_native PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows -- --ignored` | Implements shared `product_workflows` binary, env knobs, ledger output, seeds, smoke fixture scale, and service/worker fault hooks for crash-recovery workflows. |
| B-062 | Benchmark and scale evidence harness | TP-001 performance suites, TP-002 E0..E2, TP-003 AC-LAT-1..4 | B-060, B-054 | `cargo test -p pqueue-service performance_batch_operation_tests performance_hot_queue_10m_tests`; release: `performance_single_deployment_baseline_tests` | Creates perf/scale runners, env knobs, seeds, instance/profile ledger fields, AC-LAT micro-bars, query-plan capture, and the E1/E2 measurement framework. Full E2 execution waits for B-071/B-081. |
| B-070 | Shard ownership lifecycle | TD-003, TD-001, TP-003 AC-SHARD-3 | B-030, B-021 | `cargo test -p pqueue-storage sharding_assignment_fencing_tests sharding_rebalance_drain_tests` | Owner registry, worker registration/heartbeat, target-vs-active owner, acquire/renew/begin-drain/release shard lease, stale-epoch reject, graceful/interrupted drain, recovery hooks. |
| B-072 | Multi-shard claim, progress, discovery, and command convergence | TD-003, TD-001, TP-003 AC-SHARD-1..2, AC-DISC-1 cross-shard, AC-DISC-2 | B-070, B-054, B-021 | `cargo test -p pqueue-storage cross_shard_progress_tests storage_conformance_multi_shard_tests multi_shard_claim_order_replay_tests` | Fan-out claim, deterministic k-way merge, cross-shard queue-global progress aggregation, stalled-shard visibility, non-co-resident group aggregation across shards, cross-shard active-scope discovery, claim-intent partial-failure/replay convergence, envelope-scope request expiry, and queue-scoped multi-shard command convergence for `SetGates` and native `PurgeItems` spans. |
| B-071 | Queue density resource model | ADR-003, TD-003, TP-002 E2 | B-072, B-062 | `cargo test -p pqueue-storage queue_density_single_node_tests -- --ignored` | Bounded shared pools/sweepers and LRU handles; no one task/loop/connection per queue/shard. |
| B-080 | Object-log durable log and SQLite projection | TD-004, TD-001, TP-003 §4 | B-072 | `cargo test -p pqueue-objectlog object_log_commit_recovery_tests`; `cargo test -p pqueue-sqlite sqlite_projection_tests` | Adds `pqueue-objectlog` and `pqueue-sqlite` workspace crates per TD-001 step 6. Implements group commit, manifest CAS/current epoch fence, Postgres manifest-pointer fallback for no-CAS stores, production rejection of one-object-per-command, config rejection/fallback for stores without required conditional-write behavior, apply-before-return, replay response, SQLite projection, and cross-shard command convergence visibility gates. |
| B-081 | Object-log conformance parity, metrics, product smoke, and recovery | TD-004, TP-002 E3, TP-003 AC-OBS-1 and AC-TXN-1..6 object-log profiles | B-080, B-062, B-061 | `cargo test -p pqueue-storage storage_conformance_multi_shard_tests --features object-log`; `cargo test -p pqueue-objectlog object_log_commit_recovery_tests`; `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows -- --ignored`; release: E3 benchmark + AC-TXN matrix | Snapshot + log-tail recovery, bounded apply lag, object-log latency/cost/recovery evidence, fallback-fence E3 row, transaction-contract crash-point matrix, multi-shard command convergence, object-log metrics ground truth, product-E2E smoke matrix extension, and parity with the shared TD-001 conformance suite. |
| B-090 | P0 product workflow release gates | PRD, TP-003 AC-E2E-1..6, AC-E2E-8..9, INV-1..10 | B-061, B-062, B-071, B-080, B-081 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows -- --ignored`; `cargo test -p pqueue-service seventh_sense_validation_tests invariant_stress_matrix_tests -- --ignored`; release: TP-002 E0-E3, `performance_cross_queue_scale_out_tests` (the ADR-008 replacement for the retired `performance_multi_shard_scale_out_tests`), `recurrence_scale_both_profiles_tests` | Scheduled action, group batching, cohort, recurring, crash recovery, noisy neighbor, generic priority, downstream pacing, Seventh-Sense-shaped subset, recurrence under scale on both backend profiles, and INV-1..10 under the TP-003 §2 stress matrix. |
| B-110 | Standalone durable sqlite backend (TD-005) | TD-005, ADR-006, TP-001 TD-005 row | B-080 | `cargo test -p pqueue-sqlite`; `cargo test -p pqueue-sqlite --test shared_conformance --test embedder_delivery_conformance --test sqlite_backend_tests`; `cargo test -p pqueue-service --lib runtime` | Unified single-transaction `SqliteBackend` (atomic append+apply on one connection, one WAL fsync ack boundary, strict read-after-write), atomic `claim` (single `attempts` increment; `batch_claim` omitted from the surface), epoch bootstrap + bump-on-open fencing, single-writer ownership (second opener rejected), no-replay reopen recovery, the `sqlite` `BackendProfile` wired into the service config/readiness (config-plumbing; the service does not yet construct/serve the backend), shared conformance parity with the in-memory reference, and the embedder delivery-adapter conformance suite. `client_item_key` convergence is the embedder adapter's responsibility (pqueue converges by `item_id`). The 7snx host switch off the in-memory backend (bead pqueue-a4846118) is a deferred cross-repo follow-up (requires publishing pqueue + bumping the git rev). |
| B-100 | API-002 operator surface | API-002, ADR-002, TP-003 AC-OP-1..9, AC-CLAIM-6 operator-fenced renewal, INV-11 | B-060, B-050..B-053, B-021 | `cargo test -p pqueue-service operator_repair_tests operator_redrive_tests operator_purge_tests operator_async_operation_tests operator_auth_denied_path_tests` | P1 operator support: pause/resume, repair, redrive, bulk purge/archive, async ops, inspection/auth, and rejection of stale/fenced lease renewals after operator mutation. |
| B-101 | Operator product workflow gate | API-002, TP-003 AC-E2E-7 | B-100, B-061 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows operator_repair_redrive_e2e -- --ignored` | Required before claiming operator-enabled product surface verified. |

## Product E2E Coverage Map

Each TP-003 product workflow has an explicit owning smoke bead. B-061 owns the
shared harness and `postgres_native` smoke execution; the feature slices below
own the behavior that makes each workflow pass. B-081 owns extending the same
smoke matrix to `object_log_sqlite_projection` after the object-log backend
lands, and release-scale execution is gated by B-090 for P0/core workflows or
B-101 for P1/operator workflows.

| TP-003 AC | Suite Name | Smoke Owner | Release Owner | Required Feature Slices |
|-----------|------------|-------------|---------------|-------------------------|
| AC-E2E-1 | `product_workflow_scheduled_action_delivery_e2e` | B-061 | B-090 | B-041, B-050, B-060 |
| AC-E2E-2 | `product_workflow_marketo_group_batching_e2e` | B-061 | B-090 | B-051, B-021 |
| AC-E2E-3 | `product_workflow_callback_cohort_e2e` | B-061 | B-090 | B-052, B-021 |
| AC-E2E-4 | `product_workflow_jobs_connectors_recurring_e2e` | B-061 | B-090 | B-053 |
| AC-E2E-5 | `product_workflow_worker_crash_recovery_e2e` | B-061 | B-090 | B-021, B-041, B-042 |
| AC-E2E-6 | `product_workflow_noisy_neighbor_scale_e2e` | B-061 | B-090 | B-054 for smoke; B-062, B-071, B-080, B-081 for release-scale evidence |
| AC-E2E-7 | `product_workflow_operator_repair_redrive_e2e` | B-101 | B-101 | B-100, B-021 |
| AC-E2E-8 | `product_workflow_generic_priority_bounded_relaxed_e2e` | B-061 | B-090 | B-011, B-041, B-060 |
| AC-E2E-9 | `product_workflow_downstream_pacing_non_goal_e2e` | B-061 | B-090 | B-041, B-060 |

The B-061 implementation should create one independently addressable test case
per suite above, not a single all-purpose scenario. Product E2E beads created
from this plan must name the suite they extend in `suite:<name>` labels and
must run with at least `PQUEUE_BACKEND_PROFILE=postgres_native`,
`PQUEUE_E2E_SCALE=smoke`, and a fixed `PQUEUE_E2E_SEED`.

## Invariant Stress Ownership

Unit, conformance, and product workflow slices prove individual invariants at
small or feature scale. B-090 owns the P0/core aggregate
`invariant_stress_matrix_tests` command at the TP-003 §2 stress matrix for
INV-1..10 on every committed backend profile. B-100/B-101 own the P1/operator
INV-11 stress path and do not block the P0/core release gate.

| Invariant | Primary Feature Evidence | Aggregate Owner |
|-----------|--------------------------|-----------------|
| INV-1 single active lease | B-041 / AC-CLAIM-1 | B-090 |
| INV-2 no lost work | B-021, B-042, AC-E2E-5 | B-090 |
| INV-3 no conflicting terminal | B-012, B-042, AC-E2E-5 | B-090 |
| INV-4 progress bound | B-041, B-050, AC-CLAIM-3/5, AC-GATE-2 | B-090 |
| INV-5 idempotency | B-012, B-042, B-053 | B-090 |
| INV-6 ordering | B-011, B-041, B-072 | B-090 |
| INV-7 group/cohort atomicity | B-051, B-052 | B-090 |
| INV-8 tenant isolation | B-030, B-060 | B-090 for P0 paths; B-101 for operator paths |
| INV-9 group co-residency | B-030, B-051 for shape validation; B-070/B-072 for `shard_count > 1` placement invariance | B-090 |
| INV-10 durable ack | B-042, B-080, B-081 | B-090 |
| INV-11 lease fence on operator action | B-100 | B-101 |

## Multi-Shard Sub-Decomposition

> **Retired as a target (ADR-008).** This sub-decomposition is for the prior
> intra-queue-shard model and is superseded by per-queue ownership + cross-queue
> scale-out (TD-003/TD-006); it will be replaced by a per-queue ownership / routing
> / cross-queue decomposition in the later code-build phase. Retained as historical
> sequencing — see the superseded-as-target note at the top of this plan.

B-072 must be broken into focused beads rather than filed as one broad shard
task. Each child bead depends on B-070, B-054, and B-021 unless the dependency is
not relevant to that sub-behavior.

| Sub-Bead Scope | TP-003 Coverage | Validation |
|----------------|-----------------|------------|
| Fan-out claim and deterministic k-way merge | AC-SHARD-1, INV-6 | `cargo test -p pqueue-storage storage_conformance_multi_shard_tests multi_shard_claim_order_replay_tests` |
| Cross-shard progress and stalled-shard visibility | AC-SHARD-2, INV-4 | `cargo test -p pqueue-storage cross_shard_progress_tests` |
| Cross-shard active-scope discovery | AC-DISC-1 cross-shard | `cargo test -p pqueue-storage storage_conformance_discovery_tests` |
| Non-co-resident group aggregation | AC-DISC-2 | `cargo test -p pqueue-storage storage_conformance_discovery_tests storage_conformance_group_batching_tests` |
| Claim-intent replay and multi-shard command convergence | INV-2, INV-5, INV-10 | `cargo test -p pqueue-storage multi_shard_claim_order_replay_tests storage_conformance_multi_shard_tests` |

## Object-Log Sub-Decomposition

B-080/B-081 must be split into backend construction, parity, smoke, and evidence
beads rather than filed as one object-log task.

| Sub-Bead Scope | Owning Slice | Validation |
|----------------|--------------|------------|
| Group commit, manifest CAS, and epoch fencing | B-080 | `cargo test -p pqueue-objectlog object_log_commit_recovery_tests` |
| No-CAS fallback and invalid production config rejection | B-080 | `cargo test -p pqueue-objectlog object_log_commit_recovery_tests` |
| SQLite projection, apply-before-return, and bounded apply lag hooks | B-080 | `cargo test -p pqueue-sqlite sqlite_projection_tests` |
| Cross-shard command convergence visibility | B-080 | `cargo test -p pqueue-storage storage_conformance_multi_shard_tests --features object-log` |
| Shared conformance parity and AC-OBS-1 object-log metrics | B-081 | `cargo test -p pqueue-storage storage_conformance_multi_shard_tests --features object-log`; `cargo test -p pqueue-service service_metrics_ground_truth_tests` |
| Product E2E smoke matrix extension | B-081 | `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows -- --ignored` |
| E3 object-log latency/cost/recovery evidence | B-081 | TP-002 E3 release benchmark and `object_log_latency_cost_matrix_tests` |

## Release-Gate Sub-Decomposition

B-090 should create one release-gate bead per P0/core product workflow plus
separate aggregate evidence beads. Each release E2E bead must enumerate the
`PQUEUE_BACKEND_PROFILE` and topology matrix required by TP-003 §3.11; commands
below are suite selectors, not the complete profile matrix. B-101 owns the
P1/operator release workflow.

| Sub-Bead Scope | Owning Slice | Validation |
|----------------|--------------|------------|
| AC-E2E-1 release scheduled action delivery | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_scheduled_action_delivery_e2e -- --ignored` |
| AC-E2E-2 release Marketo group batching | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_marketo_group_batching_e2e -- --ignored` |
| AC-E2E-3 release callback cohort | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_callback_cohort_e2e -- --ignored` |
| AC-E2E-4 release jobs/connectors recurring | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_jobs_connectors_recurring_e2e -- --ignored` |
| AC-E2E-5 release crash recovery | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_worker_crash_recovery_e2e -- --ignored` |
| AC-E2E-6 release noisy-neighbor scale | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_noisy_neighbor_scale_e2e -- --ignored` |
| AC-E2E-8 release generic priority and bounded-relaxed | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_generic_priority_bounded_relaxed_e2e -- --ignored` |
| AC-E2E-9 release downstream pacing non-goal | B-090 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_downstream_pacing_non_goal_e2e -- --ignored` |
| Seventh-Sense-shaped product subset | B-090 | `cargo test -p pqueue-service seventh_sense_validation_tests -- --ignored` |
| INV-1..10 stress matrix | B-090 | `cargo test -p pqueue-service invariant_stress_matrix_tests -- --ignored` |
| TP-002 E2 cross-queue scale-out | B-090 | `performance_cross_queue_scale_out_tests` (replaces the retired `performance_multi_shard_scale_out_tests` under the ADR-008 reframe); if the published multiple is unresolved, stop for a doc update instead of inventing it. Re-measured live post-ADR-008 on kind, 2026-07-08: `docs/perf/evidence/tp002-e2-cross-queue-remeasured.jsonl` |
| TP-002 E0-E3 aggregate evidence | B-090 | TP-002 release benchmark commands for E0, E1, E2, and E3 |
| AC-E2E-7 operator repair/redrive | B-101 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_operator_repair_redrive_e2e -- --ignored` |

## Issue Decomposition

Work is tracked with `ddx bead`, not custom issue files. Create one epic bead per
implementation slice group only when it helps dependency management; otherwise
create task beads directly from the table above.

The implementation slice table is ordered by dependency readiness. Numeric
suffixes are stable identifiers, not a sorting rule; if they differ from
dependency order, follow the `Depends On` column.

**Required labels per build bead**:

- `helix`
- `activity:build`
- `kind:build`
- `area:<crate-or-subsystem>`
- `suite:<canonical TP-001 suite>` when a named suite is created or extended

**Required references per build bead**:

- This build plan: `build-implementation-plan`
- Nearest governing artifact via `--set spec-id=<ddx-id>`
- TP-003 `AC-*` / `INV-*` IDs in the bead description and acceptance criteria
- Exact command(s) to run, including `PQUEUE_BACKEND_PROFILE`,
  `PQUEUE_E2E_SCALE`, and seed when relevant

**Dependency rules**:

- Beads that implement storage behavior depend on the relevant conformance
  harness bead.
- Failure/replay/kill beads depend on B-021.
- Performance, latency, and scale-evidence beads depend on B-062.
- Product workflow beads depend on B-061 and the underlying feature slice.
- Product workflow crash-recovery and any service/worker process-kill beads
  depend on B-021 directly or through B-061.
- Object-log product workflow smoke depends on B-081 and reuses the B-061
  harness with `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection`.
- AC-E2E-6 release beads depend on B-062, B-071, B-080, and B-081.
- Operator beads are P1 and depend on the P0 service/storage foundations, but
  they are not dependencies of the P0/core v1 release gate.
- Beads that add background work must explicitly state how the implementation
  preserves bounded shared per-node workers, pools, loops, connections, and
  projection handles before B-071 verifies the aggregate resource model.
- Fuzz and Loom ownership follows the slice that first introduces the target:
  priority decode fuzz in B-011, command-envelope decode fuzz in B-020,
  operator selector fuzz in B-100, and concurrent structure Loom tests in the
  slice that introduces each shared pool or lock-free structure, especially
  B-071.

## Validation Plan

- Per-PR gates: ADR-003 formatting/lints/tests/dependency checks, claimed
  unit/integration ACs, product E2E smoke once a product suite exists.
- Release gates: TP-003 §5 release column, backend conformance 100% for
  `postgres_native` and `object_log_sqlite_projection`, TP-002 E0-E3, and
  `product_validation_tests` at release shape.
- Release gate reproducibility: `scripts/ci/release-gate.sh` validates TP-002
  E0-E3 from source-backed DDx beads, not from pre-existing `target/` files:
  E0/E1 = `pqueue-7e2b3132`, E2 = `pqueue-9afd88cc,pqueue-76d92a33`, and
  E3 = `pqueue-b1abd895,pqueue-472a09d4`. The gate regenerates the aggregate
  `product_validation_tests` ledger before strict ledger validation.
- Operator-enabled release: all P0/core release gates plus API-002 operator
  suites and `operator_validation_tests`.
- Every bead must add or extend tests before claiming behavior complete. A bead
  touching storage, concurrency, claim, lease, operator, or scale behavior cannot
  close on `cargo test --workspace` alone.
- Verification ledger evidence must include command, exit status, profile,
  scale, seed, environment, measured numbers, and named TP-001 suite.

## Risks and Rollbacks

| Risk | Impact | Response | Rollback |
|------|--------|----------|----------|
| E2E release bars arrive before scale infrastructure exists | H | File smoke E2E beads early but wire release-shape beads behind B-062/B-071/B-080/B-081 dependencies | Keep smoke jobs in CI; do not claim release ledger evidence |
| Object-log backend hides correctness differences behind projection lag | H | Enforce apply-before-return for own operations, AC-TXN crash-point coverage, `request_id` replay, `L_apply` for unrelated readers, and shared conformance parity | Disable object-log profiles for new queues |
| Postgres-native is mistaken for horizontal-scale evidence | H | Treat Postgres multi-shard as comparator-only; TP-002 E2 headline requires object-log multi-shard | Mark comparator results as non-gating |
| Fault-injection harness is under-scoped inside a feature bead | H | File B-021 before failure-heavy features; make E2E failure beads depend on it | Disable failure-heavy E2E release jobs until harness exists |
| Benchmark harness is under-scoped inside release beads | H | File B-062 before latency, E1/E2, and product release beads; make B-090 depend on it | Keep product smoke gates only; do not claim E0-E3 |
| P1 operator work blocks P0/core verification | M | Keep AC-E2E-7 in `operator_validation_tests`, not `product_validation_tests` | Defer B-100/B-101 without changing P0/core release gate |
| Metrics counters drift from exact state | M | AC-OBS-1 compares exact fields and bounded approximate fields against ground truth | Fail release gate; do not rely on approximate counts for correctness |
| Beads become too broad | M | Split by crate/suite/AC; each bead names in-scope and out-of-scope files | Reopen/split bead before retrying |

## Exit Criteria

BUILD-001 is complete when:

- all P0/core implementation beads B-001 through B-090 are closed with ledger
  evidence;
- `postgres_native`, `object_log_inmemory_projection`, and
  `object_log_sqlite_projection` pass 100% of TD-001 conformance for the
  profiles claimed by the release;
- TP-002 E0-E3 pass;
- TP-003 §5 P0/core release gates and `product_validation_tests` are green;
- P1/operator beads B-100 and B-101 are either closed with
  `operator_validation_tests` green or explicitly left open as non-blocking P1
  work; and
- no bead depends on chat history or scratch files for executable context.

Completion evidence as of 2026-06-16:

- `bash scripts/ci/verify-build-closure.sh --aggregate pqueue-131eadfa`
  reports live closure verified. (Originally the B-090 P0 aggregate pqueue-fa406e7d,
  repointed to the release epic after that closed bead was pruned from the tracker.)
- `bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3 --tp002-e0e1-source pqueue-7e2b3132 --tp002-e2-source pqueue-9afd88cc,pqueue-76d92a33 --tp002-e3-source pqueue-b1abd895,pqueue-472a09d4`
  passes from source-backed evidence and regenerates
  `target/pqueue-ledger/product_validation.jsonl`.
- Local deployment validation passed for both committed backend profiles:
  `local_postgres_deployment_smoke_tests` with Docker cleanup and
  `local_object_log_deployment_smoke_tests`.
- Product workflow smoke validation passed for both
  `postgres_native` (seed 1701) and `object_log_sqlite_projection` (seed 1801),
  with emitted workflow ledgers validated by `pqueue-verify-ledger --strict`.

Addendum as of v0.11.0 (2026-07): TP-002 E2 was reframed to cross-queue
scale-out (ADR-008) and re-measured live on a multi-node kind cluster; the
current E2 evidence is `docs/perf/evidence/tp002-e2-cross-queue-remeasured.jsonl`
(validates via `pqueue-verify-ledger --strict --require-evidence E2`). The
released runtime additionally ships the TD-004 hybrid projection profiles
(`objectlog/hybrid`, `objectlog/hybrid-strict`, `objectlog/hybrid-async`) beyond
the BUILD-001 committed profiles above; their gates are owned by TP-003 §4.1 and
the hybrid implementation plan, not by this build plan.
