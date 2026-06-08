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
---

# Build Plan: BUILD-001 Implementation Sequence

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
- P0/core v1 verification is TP-003 §7 items 1-6: core invariants, conformance,
  CI gates, TP-002 E0-E3, and `product_validation_tests` over AC-E2E-1..6 and
  AC-E2E-8..9.
- Operator-enabled verification is TP-003 §7 item 7: API-002 suites plus
  AC-E2E-7.
- Product E2E smoke (`PQUEUE_E2E_SCALE=smoke`) is a per-PR gate once a suite
  exists. Release E2E uses the row-specific release shapes in TP-003 §3.10.

## Implementation Slices

| Slice | Area | Governing Artifacts | Depends On | Validation Gate | Notes |
|-------|------|---------------------|------------|-----------------|-------|
| B-001 | Workspace and CI foundation | ADR-003, TP-003 §5 | None | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` | Creates the Rust toolchain, crates, CI scaffolding, dependency policy, unsafe denial, and coverage/property/fuzz placeholders. |
| B-010 | Core API/domain types | API-001, API-002, ADR-004 | B-001 | `cargo test -p pqueue-core core_domain` | IDs, queue definitions, priority values, metadata, group/cohort/recurrence config, API result/error types. |
| B-011 | Core priority and ordering | PRD FR-2..FR-8, API-001, TP-003 AC-CORE-1 | B-010 | `cargo test -p pqueue-core core_priority_model_tests` | Includes timestamp and non-timestamp priority models so pqueue is not timestamp-only. |
| B-012 | Core lifecycle, idempotency, retry, eligibility | API-001, TP-003 AC-CORE-2..4, AC-CLAIM-3 | B-010 | `cargo test -p pqueue-core core_lifecycle_transition_tests core_idempotency_tests core_eligibility_precedence_tests core_recurrence_rearm_tests` | Establishes the pure state machine before storage. |
| B-020 | Storage traits and conformance harness | TD-001, TP-003 §4 | B-010..B-012 | `cargo test -p pqueue-storage storage_conformance --no-default-features` | Defines `LogStore`, `ProjectionStore`, `SnapshotStore`, `ControlPlaneStore`, command envelopes, positions, fixtures. |
| B-021 | Fault-injection harness | TP-003 §2, §3.10 | B-020 | `cargo test -p pqueue-storage fault_injection_harness_tests` | Shared process-kill, replay, partial-append, and deterministic failure scheduler used by AC-CLAIM-2, AC-SHARD-3, AC-E2E-2/3/5/7. |
| B-030 | Postgres control plane | TD-002, ADR-002 | B-020 | `cargo test -p pqueue-postgres postgres_schema_migration_tests postgres_transaction_flow_tests` | Queue definitions, tenant scope, backend profile, shard assignments, assignment epochs. |
| B-040 | Postgres-native append/projection/write path | TD-001, TD-002, API-001 | B-030 | `cargo test -p pqueue-postgres postgres_transaction_flow_tests` | `BatchPush`, `BatchUpdate`, command/idempotency records, terminal retention basics. |
| B-041 | Postgres-native claim/renew/finalize path | TD-002, TP-003 AC-CLAIM-1..6 | B-040, B-021 | `cargo test -p pqueue-postgres postgres_concurrency_claim_tests`; `cargo test -p pqueue-storage storage_conformance_claim_tests` | Single active lease, lease renewal, expiry redelivery, strict/bounded-relaxed claim, finalize outcomes. |
| B-042 | Postgres-native durability/idempotency/replay | TD-001, TD-002, TP-003 INV-5, INV-10 | B-041 | `cargo test -p pqueue-storage storage_conformance_durability_tests` | Kill-after-ack, replay response, request conflict, retention windows. |
| B-050 | Dynamic gates and eligibility projections | API-001, TD-002, TP-003 AC-GATE-1..2 | B-041 | `cargo test -p pqueue-postgres storage_conformance_gate_tests` | `SetGates`, gate anti-join, no item-row rewrite, exact oldest-eligible behavior. |
| B-051 | Group batching and per-group summary | ADR-004, API-001, TD-002, TP-003 AC-GRP-1..2 | B-050 | `cargo test -p pqueue-postgres storage_conformance_group_batching_tests postgres_group_coresidency_tests` | `group_co_residency`, whole-group atomic claim, `same_group_key` as item filter. |
| B-052 | Cohort claims | API-001, ADR-004, TD-002, TP-003 AC-COH-1..2 | B-051 | `cargo test -p pqueue-postgres storage_conformance_cohort_tests` | Complete cohort atomic lease, incomplete expiry, no member leakage. |
| B-053 | Recurring queues and native purge | API-001, TD-002, TP-003 AC-REC-1..3 | B-041 | `cargo test -p pqueue-postgres core_recurrence_rearm_tests storage_conformance_durability_tests` | `rearm`, per-cycle retry reset, idle metrics, `PurgeItems`, tombstone/replay safety. |
| B-054 | Active-scope discovery and metrics | API-001, TD-002, TD-003, TP-003 AC-DISC-1..2, AC-OBS-1 | B-050..B-053 | `cargo test -p pqueue-service service_discovery_tests service_metrics_ground_truth_tests` | Top-N ranking, non-co-resident cross-shard aggregation, exact oldest age, bounded count lag. |
| B-060 | API-001 service and client | API-001, ADR-002, TP-003 AC-SEC-1..2 | B-041 | `cargo test -p pqueue-service service_api_error_semantics_tests service_auth_tenant_tests`; `cargo test -p pqueue-client` | HTTP/JSON routes, auth context, tenant isolation, lease-token hashing, client facade. |
| B-061 | Product E2E smoke harness | TP-001, TP-003 §3.10 | B-060, B-054 | `PQUEUE_BACKEND_PROFILE=postgres_native PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows -- --ignored` | Implements shared `product_workflows` binary, env knobs, ledger output, seeds, smoke fixture scale. |
| B-070 | Sharding and shard ownership | TD-003, TD-001, TP-003 AC-SHARD-1..3 | B-054, B-021 | `cargo test -p pqueue-storage sharding_assignment_fencing_tests sharding_rebalance_drain_tests cross_shard_progress_tests` | Assignment, epoch fences, fan-out claim, k-way merge, cross-shard queue-global progress. |
| B-071 | Queue density resource model | ADR-003, TD-003, TP-002 E2 | B-070 | `cargo test -p pqueue-storage queue_density_single_node_tests -- --ignored` | Bounded shared pools/sweepers and LRU handles; no one task/loop/connection per queue/shard. |
| B-080 | Object-log durable log and SQLite projection | TD-004, TD-001, TP-003 §4 | B-070 | `cargo test -p pqueue-storage object_log_commit_recovery_tests storage_conformance_durability_tests` | Group commit, manifest CAS/current epoch fence, apply-before-return, replay response, SQLite projection. |
| B-081 | Object-log conformance parity and recovery | TD-004, TP-002 E3 | B-080 | `cargo test -p pqueue-storage storage_conformance_multi_shard_tests object_log_commit_recovery_tests`; release: E3 benchmark | Snapshot + log-tail recovery, bounded apply lag, object-log cost/ack/recovery evidence. |
| B-090 | P0 product workflow release gates | PRD, TP-003 AC-E2E-1..6, AC-E2E-8..9 | B-061, B-070, B-081 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows -- --ignored` plus TP-002 E0-E3 | Scheduled action, group batching, cohort, recurring, crash recovery, noisy neighbor, generic priority, downstream pacing. |
| B-100 | API-002 operator surface | API-002, ADR-002, TP-003 AC-OP-1..9 | B-060, B-050..B-053, B-021 | `cargo test -p pqueue-service operator_repair_tests operator_redrive_tests operator_purge_tests operator_async_operation_tests operator_auth_denied_path_tests` | P1 operator support: pause/resume, repair, redrive, bulk purge/archive, async ops, inspection/auth. |
| B-101 | Operator product workflow gate | API-002, TP-003 AC-E2E-7 | B-100, B-061 | `PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows operator_repair_redrive_e2e -- --ignored` | Required before claiming operator-enabled product surface verified. |

## Issue Decomposition

Work is tracked with `ddx bead`, not custom issue files. Create one epic bead per
implementation slice group only when it helps dependency management; otherwise
create task beads directly from the table above.

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
- Product workflow beads depend on B-061 and the underlying feature slice.
- AC-E2E-6 release beads depend on B-070, B-071, B-080, and B-081.
- Operator beads are P1 and depend on the P0 service/storage foundations, but
  they are not dependencies of the P0/core v1 release gate.

## Validation Plan

- Per-PR gates: ADR-003 formatting/lints/tests/dependency checks, claimed
  unit/integration ACs, product E2E smoke once a product suite exists.
- Release gates: TP-003 §5 release column, backend conformance 100% for
  `postgres_native` and `object_log_sqlite_projection`, TP-002 E0-E3, and
  `product_validation_tests` at release shape.
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
| E2E release bars arrive before scale infrastructure exists | H | File smoke E2E beads early but wire release-shape beads behind B-070/B-071/B-080/B-081 dependencies | Keep smoke jobs in CI; do not claim release ledger evidence |
| Object-log backend hides correctness differences behind eventual projection lag | H | Enforce apply-before-return for own operations, `L_apply` for unrelated readers, and shared conformance parity | Disable `object_log_sqlite_projection` for new queues |
| Postgres-native is mistaken for horizontal-scale evidence | H | Treat Postgres multi-shard as comparator-only; TP-002 E2 headline requires object-log multi-shard | Mark comparator results as non-gating |
| Fault-injection harness is under-scoped inside a feature bead | H | File B-021 before failure-heavy features; make E2E failure beads depend on it | Disable failure-heavy E2E release jobs until harness exists |
| P1 operator work blocks P0/core verification | M | Keep AC-E2E-7 in `operator_validation_tests`, not `product_validation_tests` | Defer B-100/B-101 without changing P0/core release gate |
| Metrics counters drift from exact state | M | AC-OBS-1 compares exact fields and bounded approximate fields against ground truth | Fail release gate; do not rely on approximate counts for correctness |
| Beads become too broad | M | Split by crate/suite/AC; each bead names in-scope and out-of-scope files | Reopen/split bead before retrying |

## Exit Criteria

BUILD-001 is complete when:

- all P0/core implementation beads B-001 through B-090 are closed with ledger
  evidence;
- `postgres_native` and `object_log_sqlite_projection` pass 100% of TD-001
  conformance;
- TP-002 E0-E3 pass;
- TP-003 §5 P0/core release gates and `product_validation_tests` are green;
- P1/operator beads B-100 and B-101 are either closed with
  `operator_validation_tests` green or explicitly left open as non-blocking P1
  work; and
- no bead depends on chat history or scratch files for executable context.

