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

## Purpose

This plan decomposes the accepted pqueue design into an ordered, dependency-aware
set of **beads** (portable work items) that a `ddx work` worker drains. Each bead
cites its governing artifact(s) and the `AC-*` / `INV-*` acceptance criteria from
TP-003 that mechanically gate its completion (`ddx bead ac-check`). A bead is not
"done" until its cited acceptance criteria are green and recorded in the
verification ledger (TP-003 §6).

Crate boundaries and toolchain are fixed by ADR-003; storage flows by TD-001/002;
sharding by TD-003; the object-log backend by TD-004; client/operator contracts by
API-001/API-002. This plan adds no new design decisions — it sequences them.

## Definition of Done (every bead)

1. Code + tests land behind the ADR-003 crate boundaries (inward-flowing deps).
2. The bead's cited `AC-*` pass at their TP-003 bars; cited `INV-*` show 0
   violations at the applicable cadence (per-PR vs release, TP-003 §5).
3. Per-PR CI gates green (`fmt`, `clippy -D warnings`, `test`, `deny`, `machete`,
   `forbid(unsafe_code)`, coverage thresholds).
4. The verification ledger entry records command, exit status, environment, seed,
   measured numbers vs bar, and the named TP-001 suite(s).
5. Review passes (`ddx work` runs review on by default).

## Epic / Bead DAG

Beads are grouped into epics. `→` denotes a hard dependency. Foundational epics
unblock the rest; gap-feature epics depend on the core claim engine.

### E0 — Workspace & CI foundation  *(no deps)*

- B-001 Cargo workspace + `rust-toolchain.toml` + five crates (`pqueue-core`,
  `-storage`, `-postgres`, `-service`, `-client`) per ADR-003; inward dep graph
  enforced. AC: builds; dep-graph lint. INV: n/a.
- B-002 CI quality-gate pipeline = TP-003 §5 per-PR set (`fmt`, `clippy -D
  warnings`, `test`, `deny`, `machete`, `forbid(unsafe_code)`, coverage harness,
  property/fuzz scaffolding, flaky-rate harness). AC: the gate set runs green on
  empty crates.

### E1 — `pqueue-core` domain  *(→ E0)*

- B-010 API-001 operation structs + domain types (ids, `priority`, `not_before`,
  `metadata`, `group_key`, lifecycle enum). AC-CORE-2.
- B-011 Priority encoding `priority_sort` for all four models + tie-breaker.
  **AC-CORE-1** (`props ≥ 1,000,000`).
- B-012 Lifecycle state machine + transition validation. **AC-CORE-2**; INV-3.
- B-013 Idempotency (request fingerprint, `client_item_key` convergence,
  `item_version` rules). **AC-CORE-3**; INV-5 (unit tier).
- B-014 Retry policy + exhaustion → terminal. **AC-CORE-4**.
- B-015 Eligibility Precedence evaluator (conditions 0–5, single home). AC-CLAIM-3
  inputs.

### E2 — `pqueue-storage` traits & conformance harness  *(→ E1)*

- B-020 Capability traits (`LogStore`, `ProjectionStore`, `SnapshotStore`,
  `ControlPlaneStore`), command envelopes, command positions, durability
  profiles. AC: trait surface compiles; harness skeleton.
- B-021 Shared backend conformance harness (the TD-001 scenario set as
  backend-agnostic fixtures). AC: §4 conformance scenarios enumerated and runnable
  against a mock.

### E3 — Postgres control plane  *(→ E2)*

- B-030 Postgres `ControlPlaneStore`: queue defs, shard assignments, backend
  profile, epochs (TD-002 schema). AC: tenant-scoped create/read; **INV-8** (unit).

### E4 — Postgres-native claim engine (TD-002)  *(→ E3)*  **[reference correctness backend]**

- B-040 `pqueue_items` projection + required indexes; `BatchPush`/`BatchUpdate`
  transaction flows. AC-CORE-3 (integration); INV-10.
- B-041 `BatchClaim` with `FOR UPDATE SKIP LOCKED`, lease creation, single active
  lease. **AC-CLAIM-1, AC-CLAIM-4, AC-CLAIM-5**; **INV-1, INV-6**.
- B-042 `BatchRenewLeases` + `BatchFinalize` (complete/fail/retry/release) +
  lease expiry redelivery. **AC-CLAIM-2**; INV-2.
- B-043 Idempotency + retention/compaction (request-id, item-key, terminal).
  AC-CORE-3; **INV-5**.
- B-044 Durable ack + replay/crash recovery. **INV-10**; durable-append conformance.
- B-045 Backend conformance: postgres_native passes 100% of §4 scenarios.

### E5 — Gap features on postgres-native  *(→ E4)*

- B-050 Dynamic gates `gate_keys`/`SetGates` + `pqueue_gate_state` (O(1) flip).
  **AC-GATE-1, AC-GATE-2**.
- B-051 Whole-group `group_batching` + `pqueue_group_summary` (shard-scoped).
  **AC-GRP-1, AC-GRP-2**; INV-7.
- B-052 Complete-cohort `cohort_policy`/`whole_cohort` + `pqueue_cohorts`.
  **AC-COH-1, AC-COH-2**; INV-7.
- B-053 Recurring `rearm`/`recurrence`/native `PurgeItems`. **AC-REC-1..3**.
- B-054 `DiscoverActiveScopes` over the single summary projection.
  **AC-DISC-1**.

### E6 — Multi-shard & ownership (TD-003)  *(→ E4, E5)*

- B-060 Deterministic shard assignment + storage-backed leases + epoch fencing.
  **AC-SHARD-3**; INV-1 across rebalance.
- B-061 Multi-shard fan-out claim + deterministic k-way merge + cross-shard
  queue-global progress aggregation. **AC-SHARD-1, AC-SHARD-2**; **INV-4, INV-9**.
- B-062 Graceful drain / rebalance / recovery; stalled-shard visibility. AC-SHARD-3.
- B-063 Queue density: bounded shared per-node resources (one pool, batched
  sweeper, LRU handles). Feeds **TP-002 E2** `queue_density_single_node_tests`.

### E7 — Service surface (API-001)  *(→ E4)*

- B-070 `pqueue-service` HTTP/JSON routes + `PrincipalContext`/`Authorizer` wiring
  + error-shape (RFC 9457). AC: route + error-semantics suites.
- B-071 Tenant isolation + lease-token hashing + audit. **AC-SEC-1, AC-SEC-2**;
  **INV-8**.
- B-072 `pqueue-client` facade (embedded + HTTP).

### E8 — Operator surface (API-002)  *(→ E5, E7)*

- B-080 Pause/resume (Eligibility Precedence condition 0) + admin state. **AC-OP-6**.
- B-081 `RepairItems` (force_*/clear_lease, lease fence). **AC-OP-1**; **INV-11**.
- B-082 `RedriveItems` (DLQ, `eligible_since=max(commit,not_before)`). **AC-OP-2**.
- B-083 Bulk `PurgeQueueItems` + `ArchiveItems` + `RunRetention` + selector
  guards. **AC-OP-3, AC-OP-7**.
- B-084 Async operation model (`operation_id`, resumable convergence, cancel).
  **AC-OP-4**.
- B-085 Operator inspection + cohort-wholeness targeting + operator authz.
  **AC-OP-5, AC-OP-8, AC-OP-9**.

### E9 — Object-log second backend (TD-004)  *(→ E4, E6)*

- B-090 Group-commit pipeline, manifest CAS fencing, in-flight reservations,
  replay-response idempotency. Conformance parity with postgres_native.
- B-091 SQLite projection (shard-scoped summary, gates, cohorts), snapshot +
  bounded replay, LRU handles. **INV-10** on object-log.
- B-092 Object-log backend passes 100% of §4 conformance.

### E10 — Scale, density & end-to-end validation  *(→ E6, E8, E9)*

- B-100 Benchmark harness; **TP-002 E0/E1** (per-queue floor, single-deployment).
- B-101 **TP-002 E2** (multi-shard scale-out + ≥1000-queue density).
- B-102 **TP-002 E3** (object-log cost/ack/recovery); **AC-LAT-1..4**.
- B-103 **AC-SEN** Seventh Sense end-to-end (timestamp schedule + mutable update +
  gates + group/cohort batches + redrive); INV-1..4.

## Sequencing & parallelism

Critical path: E0 → E1 → E2 → E3 → E4 → {E5, E7} → {E6, E8} → E9 → E10. Within an
epic, beads with no mutual dependency may be worked in parallel by the queue
drain. E5 (gap features) and E7 (service) can proceed in parallel once E4 lands;
E6 (multi-shard) and E8 (operator) once E5/E7 land. E10 is the release gate.

## Worker workflow

1. Beads are filed in the project bead tracker (`ddx bead` / `bd`), labeled with
   their epic, cited `AC-*`/`INV-*`, governing artifact, and dependencies.
2. `ddx bead ready` shows execution-ready beads (deps satisfied).
3. `ddx work` drains the ready queue: it runs `ddx try` per bead with review on,
   commits on success, and respects retry/no-progress stop conditions.
4. `ddx bead ac-check` mechanically verifies the bead's acceptance criteria
   against the working tree before close.
5. Release gates (soak, scale E0–E3, loom-exhaustive, conformance 100%) run per
   TP-003 §5 release cadence before v1 is declared "verified" (TP-003 §7).

## Exit Criteria

BUILD-001 is complete when every epic's beads are closed with their `AC-*` green,
the §4 conformance gate is 100% on both committed backends, the TP-003 §5 release
gates are green, and TP-002 E0–E3 + AC-SEN pass (TP-003 §7 = v1 "verified").
