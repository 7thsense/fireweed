---
ddx:
  id: tp-scale-substantiation
  depends_on:
    - prd
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - td-storage-architecture-backend-contracts
    - td-sharding-and-shard-ownership
  review:
    self_hash: ed173bd7adce26c78059c7d347ecb31bfbea8a7e8f679b11f3d9761ddb4fb3d3
    deps:
      adr-cqrs-log-projection-storage-model: 9a9570ebe2718bf637c73564018e3702bc4473bcbf5a6499b52b7e1937bd0b83
      adr-queue-as-shard-unit-and-projection-families: 77d1e2feb6a27e0a093564e3f07247cd8cc2c6fba6c3d20b5eeade568ba25964
      prd: a910dd5fb95102767b4ddf81115569d39d85c7e082a40c62ce424dea73ca8533
      td-sharding-and-shard-ownership: 6bf3dcc75c94fefa35af4ed9f1859e76b76df3f171a89622fcb24888d92c93e4
      td-storage-architecture-backend-contracts: a0053226d680acddfc3b606ec106c47ffb09167374940dc8282607e46b8df96e
    reviewed_at: "2026-06-25T04:21:18Z"
---

# Test Plan: TP-002 Scale Substantiation

## Scope

This plan defines the scale evidence required to substantiate every horizontal-
scale, write-rate, and hot-queue claim made across the pqueue frame and design.
It is the canonical home for the scale evidence-record scheme (E0–E3), the
benchmark pass bars, the requirement-coverage rows for the cross-queue scale-out
mechanism, the named scale test suites, and the docs-lint scale-claim checklist.

This plan exists because the PRD asserts horizontal scale beyond a single
database, but the PRD must name no storage backend, scale mechanism, or query
(prd "Scale Substantiation"). Those claims are made publishable only by
reference to the evidence records defined here. Backend names and mechanism IDs
live in ADR-001, ADR-008 (`adr-queue-as-shard-unit-and-projection-families`),
TD-001, TD-002, TD-003 (`td-sharding-and-shard-ownership`), and
TD-004 (`td-s3-object-log-sqlite-projection-mode`); this plan binds them to
measurable benchmarks. Per ADR-008 the queue is the unit of sharding, so
horizontal scale is **cross-queue** — distributing queues across owner nodes —
not intra-queue sharding.

This is a pre-implementation test plan. Exact Rust function and harness names may
change when the workspace is created, but implementation beads must preserve the
evidence-record intent and cite the relevant evidence IDs.

The general lifecycle, conformance, idempotency, and per-backend coverage live in
the governing test traceability plan (`tp-governing-test-traceability`). This
plan covers only the scale-substantiation surface and references that plan's
shared backend conformance suite (TD-001) rather than restating it.

## The Scale-Claim Rule

> **Scale-Claim Rule**: Every scale claim MUST reference an **evidence record**
> that names (a) the deployment shape (single-deployment Tier-1 vs cross-queue
> Tier-2), (b) the workload envelope, and (c) the design/test artifact plus
> benchmark that substantiates it. The PRD and product-vision MUST NOT name a
> storage backend, profile, scale mechanism, or SQL; they reference the evidence
> record by ID. Backend names live only in ADR-001 / ADR-008 / TD-001 / TD-002 /
> TD-003 / TD-004 / TP-002.

The two v1 scale envelopes both deliver and both substantiate:

| Envelope | Deployment shape | Delivered by | Evidence record |
|----------|------------------|--------------|-----------------|
| **Tier-1 (single-deployment)** | one storage deployment, one queue owned by one node | `postgres_native` (TD-002) | **E1** vs the per-queue throughput floor **E0** |
| **Tier-2 (cross-queue horizontal)** | N queues distributed across N independent owner nodes (per-queue ownership leases), each queue's progress bound local to its owner | per-queue ownership (TD-003) + cross-queue distribution (ADR-008) + object-log local-projection profiles (TD-004) | **E2** (cross-queue scale-out) and **E3** (object-log latency/cost + recovery) |

## Scale Evidence Records

Every PRD/ADR/TD scale claim references one of these records.

**Evidence-ID convention**: this plan owns the canonical IDs **E0–E3**. All
documents MUST reference these canonical E-IDs; no document mints its own
evidence IDs.

Release-gate mapping as of 2026-06-16 (**pre-ADR-008 build record**):

| Evidence ID | Source bead(s) |
|-------------|----------------|
| E0, E1 | `pqueue-7e2b3132` |
| E2 | `pqueue-9afd88cc`, `pqueue-76d92a33` |
| E3 | `pqueue-b1abd895`, `pqueue-472a09d4` |

> **Build-record note (ADR-008 reframe).** The E2 source beads above measured the
> *prior* intra-queue-shard build (single-queue-over-N-shards scale-out). Under
> ADR-008 (queue is the unit of sharding) the **E2 requirement is reframed to
> cross-queue scale-out** (below); the E0/E1/E3 records keep their meaning. The
> prior E2 measurement stands as a historical attestation of the retired
> multi-shard mechanism; the reframed cross-queue E2 must be re-measured in the
> later build phase before a horizontal-scale claim cites it. E0, E1, and E3 (the
> per-queue floor, the single-deployment envelope, and the object-log
> latency/cost/recovery profile) are unaffected by the reframe.

`scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3` validates
these source beads directly when invoked with the corresponding
`--tp002-*-source` flags. The gate may also scan generated ledger rows, but the
source mapping is the reproducible release authority from a clean checkout.

### E0 — Per-queue throughput floor (stated requirement)

E0 is the fixed scale **requirement**, not a measurement of any existing system:

> **Every queue MUST sustain at least 10,000,000 (10M) accepted items/hr** —
> covering both ingest (push/update) and claim/finalize — **and this per-queue
> capability MUST hold at any deployment scale**: increasing the number of queues
> or the total deployment load MUST NOT reduce any individual queue below the
> floor.

Origin: Seventh Sense already schedules at ≥10M/hr, so pqueue must meet or exceed
that for any queue, at any scale. The floor is a target the system is built to,
and E1/E2/E3 validate that the system meets it on each profile; there is no
"measure the old system first" gate. A representative Seventh Sense item/payload
band and ingest/claim/finalize operation mix are used to drive the benchmarks
(stated in E1).

**What "preserved for every queue at any scale" means (read once, applies
everywhere it is repeated).** The floor is a per-queue *capability* that stays
**reachable by any queue** — whichever queue is the hot one can hit ≥10M items/hr
— and that adding queues or load MUST NOT drop a queue below what it could
otherwise reach or cause a progress-bound violation. It does **NOT** mean every
active queue runs at the floor simultaneously: at the queue-density point (≥1000
active queues on one node) aggregate single-node throughput is bounded by the
node, not 1000× the floor; multi-node deployment provides aggregate headroom.
Benchmark fixtures MUST keep this distinction explicit — one designated hot queue
driven to the floor while the other ~999 stay active — and MUST NOT assert a
1000× single-node aggregate.

### E1 — Tier-1 single-deployment envelope (pass/fail)

Backend: `postgres_native` (TD-002). Deployment: one Postgres, one queue owned by one node.

| Parameter | Value |
|-----------|-------|
| Batch sizes | push/update/claim/finalize at 1, 100, and max-configured batch size |
| Item / payload | representative Seventh Sense item and payload band |
| Operation mix | representative Seventh Sense ingest / claim / finalize ratio |
| Group cardinality / skew | group-heavy and skewed-priority profiles |
| Telemetry | enabled |
| Postgres sizing | stated instance class, CPU, memory, IOPS, pool |
| Resident set | 10M items including terminal retained rows under retention policy |
| Pass: throughput | a single queue sustains ≥ 10M items/hr (the E0 floor) for ingest and for claim/finalize |
| Pass: latency | sub-second p95 and p99 for batch push/update/claim/finalize |

### E2 — Tier-2 cross-queue scale-out (pass/fail)

Mechanism: per-queue ownership (TD-003) + cross-queue distribution (ADR-008) —
many queues spread across many owner nodes; each queue is a single-owner,
single-hop claim (no intra-queue sharding, no scatter-gather).
**Backend: object-log local projection (TD-004) is REQUIRED** for the headline
horizontal evidence. The release matrix MUST include both
`object_log_inmemory_projection` and `object_log_sqlite_projection` unless one is
explicitly marked unsupported by the implementation phase; **`postgres_native`
MAY additionally be run as a comparator** but does not on its own satisfy E2 (per
ADR-001 "Scale Claim Scoping", `postgres_native` alone is not evidence for the
horizontal envelope).

| Parameter | Value |
|-----------|-------|
| Owner counts | benchmark at ≥ 3 owner-node counts (e.g. 2, 4, 8 owners), distributing a fixed-per-owner number of active queues across them |
| Pass: cross-queue scale-out (measurable bar) | aggregate accepted write/claim rate across the queue population MUST scale **strictly monotonic non-decreasing with owner-node count** (2 → 4 → 8 owners) at ≈ linear efficiency (each added owner adds throughput; default bar: aggregate at 8 owners ≥ **3.5×** the 2-owner aggregate, i.e. ≥ ~70% cross-node scaling efficiency), while every individual queue independently holds its per-queue floor. A *single* queue does NOT exceed one owner's throughput (ADR-008); scale comes from more queues on more owners, and a producer that outgrows one owner partitions its stream across multiple queues. The published headline multiple is a user decision (see Open Questions). |
| Pass: queue density (≥1000 active queues, single-node target) | a single node hosts **≥ 1000 concurrently active queues** with: (a) every active queue meeting its queue-global progress bound; (b) no cross-queue degradation as the active-queue count grows to ≥ 1000 (noisy-neighbor isolation, FR-43); (c) any single queue still able to reach the per-queue floor (≥10M items/hr) when it is the hot queue while the other ~999 stay active; (d) per-queue background work (lease-expiry sweeps, progress-bound aggregation, summary recompute, recurring rearm, idempotency/retention GC) multiplexed onto bounded shared per-node pools, never one loop/connection per queue. Aggregate single-node throughput is reported, NOT required to equal 1000× the per-queue floor; multi-node deployment provides aggregate headroom. |
| Pass: per-queue floor preserved at scale (E0 invariant) | adding queues or total load — including at the ≥1000-active-queue density point — MUST NOT drop any individual queue below its reachable per-queue floor or cause a progress-bound violation. This is the "every queue at any scale" guarantee (noisy-neighbor isolation under load, FR-43). |
| Pass: per-queue local progress | each queue's oldest-eligible item is claimed before `progress_bound_ms` from its own owner's local computation (queue-global, D1 / FR-12); there is no cross-shard aggregation. |
| Pass: owner failover / fencing | killing a queue's owner: after lease expiry a new owner acquires a strictly greater epoch, the deposed owner's append is fenced, and the queue recovers from snapshot + log tail with no lost/double work (TD-003); a queue left unowned past `progress_bound_ms` surfaces as a progress-bound violation in metrics (FR-41) and `DiscoverActiveScopes` (TD-003). |
| Pass: single lease | no item double-leased across an owner reassignment/drain (TD-003). |
| Pass: routing redirect | a client addressing a queue on the wrong node is redirected (`-MOVED`-style) to the current owner and converges in a single hop; a stale/misrouted write is fenced, never corrupting state (TD-006 §1A). |

### E3 — Object-log latency/cost + recovery (pass/fail)

Backend: `object_log_inmemory_projection` and `object_log_sqlite_projection`
(TD-004). Reported against the per-queue throughput floor E0.

| Parameter | Value |
|-----------|-------|
| Commit-latency-bound sweep | run at ≥ 4 configured bounds, including low-latency, balanced, and cost-optimized values (for example 1 ms, 5 ms, 20 ms, 100 ms or implementation-equivalent documented values) |
| Pass: ack latency | p95/p99 group-commit ack reported for each bound and projection variant, within stated budget relative to the configured `segment_max_latency_ms` / `max_commit_latency_ms` window |
| Pass: throughput | sustained items/hr at or above the E0 per-queue floor (≥10M items/hr per queue) reported alongside the ack-latency distribution |
| Pass: cost | $/billion-commands and object/log requests per billion commands reported for each latency bound; the cost-optimized point beats `postgres_native` at high sustained volume (ADR-001 cost table) |
| Pass: recovery | rebuild 10M-item SQLite projection from snapshot + log tail within stated recovery-window budget |
| Pass: manifest fencing | a stale-epoch writer's manifest CAS commit is rejected; on a no-CAS object store the Postgres-held manifest pointer enforces the same fence (TD-004) |
| Pass: transaction contract | success-visible, rejection-no-effect, and unknown-outcome replay invariants hold under the same bound sweep; no latency setting may weaken TP-003 transaction invariants |

### Recurrence under scale (both backend profiles)

Run the recurrence scale row under BOTH the Postgres-native profile (E1 shape)
and the object-log + SQLite profile (E2/E3 shape). This row substantiates that
recurring/never-terminal items participate in the scale envelopes without special
handling (recurring items participate in the per-queue local oldest-eligible
computation like any item).

| Benchmark | Required Evidence (both profiles) |
|-----------|-----------------------------------|
| Recurrence under scale (D4) | (a) **High-frequency immediate rearm** (`not_before` = now tight loop) sustains target throughput without version-monotonicity or projection corruption; (b) **idle recurring inventory** of N idle re-armed items does not inflate active-scope discovery, busy-poll, or `oldest_eligible_age_ms`, and `recurring_pending` is reported within its documented lag; (c) **purge under load** (targeted + `force` while leased), queue-local (one owner) and idempotent by `request_id`, completes within bound and leaves consistent tombstones. |

## Requirement Coverage Matrix

These rows extend the governing test traceability plan with the scale mechanism.
P0 items are referenced by name (not number) to stay robust to PRD renumbering.

| Requirement | Governing Artifact | Required Test Evidence |
|-------------|--------------------|------------------------|
| PRD P0 horizontal-distribution item | PRD / TD-001 / TD-003 / ADR-008 | E2 cross-queue scale-out: aggregate rate scales monotonically with owner-node count; the E0 per-queue floor holds for every queue under K-queue concurrency; per-queue local progress holds; single lease across owner reassignment. |
| PRD P0 performance-at-scale item | PRD / TD-001 / TD-002 / TD-004 | E1 (single queue ≥ E0 floor of 10M items/hr, sub-second p95/p99) and E2 (aggregate scales beyond one deployment's ceiling by distributing queues across owners AND the E0 floor is preserved for every queue at any scale, while preserving each queue's local progress bound). |
| PRD P0 queue-density item | PRD / TD-001 / TD-002 / TD-003 / TD-004 | E2 queue density: ≥1000 concurrently active queues on a single node, each meeting its progress bound, no cross-queue degradation, any one able to reach the per-queue floor, and per-queue background work multiplexed onto bounded shared per-node pools (`queue_density_single_node_tests`). |
| TD-003 queue ownership | TD-003 | Deterministic queue-to-owner assignment, epoch fencing of a stale owner, graceful drain without loss/duplication, recovery, and stalled-queue visibility. |
| TD-004 object-log backend | TD-004 / ADR-001 | E3 latency/cost/recovery; commit-latency-bound sweep; manifest-CAS (or Postgres-pointer fallback) current-epoch fencing; passes the shared TD-001 backend conformance suite. |
| Per-queue local progress (D1) | TD-001 / TD-003 | Each queue's oldest-eligible age is computed locally on its owner (gate-aware); the oldest item is claimed before the bound; no cross-shard aggregation. |
| TD-006 client routing | TD-006 / TD-003 | A wrong-node command is `-MOVED`-redirected to the queue's owner and converges in one hop; a stale/misrouted write is fenced, never corrupting state. |
| Recurrence under scale (D4) | TD-001 / TD-002 / TD-004 | Recurrence scale row passes under both backend profiles: high-frequency rearm, idle inventory bound, queue-local purge under load. |
| Shared backend conformance | TD-001 | `postgres_native`, `object_log_inmemory_projection`, and `object_log_sqlite_projection` pass the same TD-001 shared backend conformance suite (core + transaction contract + log / relational-reconnect-durability classes, including group/cohort, `same_group_key`, ownership/fence, and recovery rows) before any is selectable by backend profile. |

## Named Test Suites

Implementation beads should create or extend these suites:

- `queue_ownership_fencing_tests`
- `queue_reassignment_drain_tests`
- `per_queue_progress_tests`
- `routing_redirect_tests`
- `object_log_commit_recovery_tests`
- `object_log_latency_cost_matrix_tests`
- `external_transaction_contract_matrix_tests`
- `performance_cross_queue_scale_out_tests` (replaces the retired `performance_multi_shard_scale_out_tests`)
- `performance_single_deployment_baseline_tests`
- `queue_density_single_node_tests`
- `recurrence_scale_both_profiles_tests`

## Scale Evidence Requirements

Scale benchmarking must include:

- single-deployment write/throughput/latency meeting the per-queue floor E0
  (≥10M items/hr per queue) (E1);
- cross-queue scale-out at ≥ 3 owner-node counts, reported as aggregate accepted
  write/claim rate per owner count, scaling monotonically with owner count (E2);
- the E0 per-queue floor preserved for every queue under concurrent load as the
  active-queue count and total load grow (the "every queue at any scale"
  guarantee, E2);
- queue density: ≥1000 concurrently active queues on a single node, each meeting
  its progress bound with no cross-queue degradation, any one able to reach the
  per-queue floor, and all per-queue background work multiplexed onto bounded
  shared per-node pools (E2, `queue_density_single_node_tests`);
- per-queue local progress: each queue's oldest-eligible item claimed before the
  bound from its owner's local computation (E2);
- owner failover/fencing and stalled/unowned-queue visibility as a progress-bound
  violation, with epoch-fenced recovery and no double-lease across reassignment
  (E2 / TD-003);
- client routing redirect convergence in a single hop and fence-safety of a
  misrouted write (E2 / TD-006);
- object-log group-commit ack latency across the commit-latency-bound sweep,
  $/command and object/log requests per billion commands at high volume, and
  10M-item projection rebuild time for each committed object-log projection
  variant (E3);
- manifest-CAS fencing, including the Postgres-held manifest-pointer fallback on
  no-CAS object stores (E3);
- external transaction-contract invariants under the E3 latency-bound sweep, so
  lower latency or lower cost configurations cannot publish weaker semantics;
- recurrence under scale on both backend profiles.

## Manual or Deferred Evidence

The following are not required before the first implementation bead but must be
covered before claiming product validation:

- Seventh Sense production scheduling SLA for concrete `progress_bound_ms`
  validation.
- P1 operator redrive, purge, repair, and archive APIs, and any P1
  operator/compatibility-adapter discovery surface. (The native
  `DiscoverActiveScopes` operation is P0/native-service per PRD and API-001 and is
  NOT deferred; only operator/adapter-facing discovery surfaces remain P1.)
- Kafka/Redpanda and DynamoDB backend conformance (later design targets).

Object-log and SQLite projection scale profiles are NO LONGER deferred: they are
committed v1 evidence via E2/E3.

## Scale-Claim Review Checklist (docs lint)

A document fails review if any of the following hold:

- It asserts "horizontal scale", a write rate, or "10M hot queue" without
  referencing an evidence record (E0–E3) that names deployment shape + workload
  envelope + substantiating artifact.
- A PRD or product-vision scale sentence names a storage backend, profile, scale
  mechanism, or SQL.
- A scale claim in any document lacks an E-record ID.

Reviewers MUST reject documents matching any rule above.

## Exit Criteria

Before scale claims are published, the referencing evidence records must pass
against the E0 per-queue floor (≥10M items/hr per queue, preserved for every
queue at any scale): E1 for the single-deployment envelope, E2 for the horizontal
envelope (including the every-queue-at-any-scale floor under K-queue concurrency),
and E3 for the object-log latency/cost/recovery profile. A scale claim in any
document must cite at least one evidence record (E0–E3) and, where it asserts a
benchmark outcome, the named scale test suite that produces it. A horizontal-scale
claim MUST NOT be substantiated by `postgres_native` alone.

## Open Questions

1. **Tier-2 published pass bar**: the E0 floor (≥10M items/hr per queue) is fixed;
   beyond it, is the default cross-queue scale-out headline "aggregate at 8 owners
   ≥ 3.5× the 2-owner aggregate (≥ ~70% cross-node efficiency), monotonic across
   2/4/8 owners" the bar you want, or a different efficiency target?
2. **Tier-2 comparator scope for E2**: object-log is required for E2; do you also
   want a `postgres_native` cross-queue comparator run (N single-owner Postgres
   queues across N nodes) so E2 and E3 can share a harness, or object-log only?

Resolved: the queue-density target is **≥1000 concurrently active queues on a
single node** (density + floor-reachable: every active queue meets its progress
bound and any one can reach the per-queue floor; the single node is not required
to sustain 1000× the floor in aggregate — multi-node provides aggregate
headroom).
