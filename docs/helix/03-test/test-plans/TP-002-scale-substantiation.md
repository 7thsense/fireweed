---
ddx:
  id: tp-scale-substantiation
  depends_on:
    - prd
    - adr-cqrs-log-projection-storage-model
    - td-storage-architecture-backend-contracts
---

# Test Plan: TP-002 Scale Substantiation

## Scope

This plan defines the scale evidence required to substantiate every horizontal-
scale, write-rate, and hot-queue claim made across the pqueue frame and design.
It is the canonical home for the scale evidence-record scheme (E0–E3), the
benchmark pass bars, the requirement-coverage rows for the multi-shard mechanism,
the named scale test suites, and the docs-lint scale-claim checklist.

This plan exists because the PRD asserts horizontal scale beyond a single
database, but the PRD must name no storage backend, shard mechanism, or query
(prd "Scale Substantiation"). Those claims are made publishable only by
reference to the evidence records defined here. Backend names and mechanism IDs
live in ADR-001, TD-001, TD-002, TD-003 (`td-sharding-and-shard-ownership`), and
TD-004 (`td-s3-object-log-sqlite-projection-mode`); this plan binds them to
measurable benchmarks.

This is a pre-implementation test plan. Exact Rust function and harness names may
change when the workspace is created, but implementation beads must preserve the
evidence-record intent and cite the relevant evidence IDs.

The general lifecycle, conformance, idempotency, and per-backend coverage live in
the governing test traceability plan (`tp-governing-test-traceability`). This
plan covers only the scale-substantiation surface and references that plan's
shared backend conformance suite (TD-001) rather than restating it.

## The Scale-Claim Rule

> **Scale-Claim Rule**: Every scale claim MUST reference an **evidence record**
> that names (a) the deployment shape (single-deployment Tier-1 vs multi-shard
> Tier-2), (b) the workload envelope, and (c) the design/test artifact plus
> benchmark that substantiates it. The PRD and product-vision MUST NOT name a
> storage backend, profile, shard mechanism, or SQL; they reference the evidence
> record by ID. Backend names live only in ADR-001 / TD-001 / TD-002 / TD-003 /
> TD-004 / TP-002.

The two v1 scale envelopes both deliver and both substantiate:

| Envelope | Deployment shape | Delivered by | Evidence record |
|----------|------------------|--------------|-----------------|
| **Tier-1 (single-deployment)** | one storage deployment, one shard set on one DB | `postgres_native` (TD-002) | **E1** vs the per-queue throughput floor **E0** |
| **Tier-2 (multi-shard horizontal)** | N shards across independent storage units, control-plane-lease ownership, cross-shard queue-global progress | multi-shard claim (TD-001) + sharding & shard ownership (TD-003) + `object_log_sqlite_projection` (TD-004) | **E2** (scale-out) and **E3** (object-log cost/ack + recovery) |

## Scale Evidence Records

Every PRD/ADR/TD scale claim references one of these records.

**Evidence-ID convention**: this plan owns the canonical IDs **E0–E3**. All
documents MUST reference these canonical E-IDs; no document mints its own
evidence IDs.

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

Backend: `postgres_native` (TD-002). Deployment: one Postgres, one shard set.

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

### E2 — Tier-2 multi-shard scale-out (pass/fail)

Mechanism: multi-shard claim (TD-001) + sharding & shard ownership (TD-003).
**Backend: `object_log_sqlite_projection` (TD-004) is REQUIRED** for the headline
horizontal evidence; **multi-shard `postgres_native` (independent DBs per shard)
MAY additionally be run as a comparator** but does not on its own satisfy E2 (per
ADR-001 "Scale Claim Scoping", `postgres_native` alone is not evidence for the
horizontal envelope).

| Parameter | Value |
|-----------|-------|
| Shard counts | benchmark at ≥ 3 shard counts (e.g. 2, 4, 8) across independent storage units |
| Pass: single-queue scale-out (measurable bar) | a single queue's aggregate accepted write/claim rate at 8 shards MUST be **≥ 4× the single-deployment E1 ceiling** (i.e. ≥ ~50% per-shard scaling efficiency), AND MUST be strictly monotonic non-decreasing across the tested shard counts (2 → 4 → 8). This default bar replaces the unmeasurable "scales with shard count" wording; the published headline multiple is a user decision (see Open Questions). |
| Pass: queue density (≥1000 active queues, single-node target) | a single node hosts **≥ 1000 concurrently active queues** with: (a) every active queue meeting its queue-global progress bound; (b) no cross-queue degradation as the active-queue count grows to ≥ 1000 (noisy-neighbor isolation, FR-43); (c) any single queue still able to reach the per-queue floor (≥10M items/hr) when it is the hot queue while the other ~999 stay active; (d) per-queue and per-`(queue,shard)` background work (lease-expiry sweeps, cross-shard progress aggregation, summary recompute, recurring rearm, idempotency/retention GC) multiplexed onto bounded shared per-node pools, never one loop/connection per queue or per shard. Aggregate single-node throughput is reported, NOT required to equal 1000× the per-queue floor; multi-node deployment provides aggregate headroom. |
| Pass: per-queue floor preserved at scale (E0 invariant) | adding queues or total load — including at the ≥1000-active-queue density point — MUST NOT drop any individual queue below its reachable per-queue floor or cause a progress-bound violation. This is the "every queue at any scale" guarantee (noisy-neighbor isolation under load, FR-43). |
| Pass: cross-shard progress | with one hot shard and one cold shard holding the queue-global oldest-eligible item, that item is claimed before `progress_bound_ms` (queue-global, D1 / FR-12) |
| Pass: stalled-shard visibility | a shard left unowned/draining past `progress_bound_ms` surfaces as a progress-bound violation in metrics (FR-41) and `DiscoverActiveScopes` (TD-003) |
| Pass: ordering | strict queue: fan-out claim returns a deterministic k-way-merged ordered batch within the global `max_items`, inspecting all relevant shards; bounded-relaxed queue: ordering within the declared relaxation bound |
| Pass: single lease | no item double-leased across a rebalance/drain (TD-003) |
| Pass: claim replay | replayed fan-out `request_id` converges to the same lease set across shards; a partial-failure first attempt re-attempts under the recorded claim-intent plan; `request-expired` only once all leases across all shards are inactive |

### E3 — Object-log cost/ack + recovery (pass/fail)

Backend: `object_log_sqlite_projection` (TD-004). Reported against the per-queue
throughput floor E0.

| Parameter | Value |
|-----------|-------|
| Pass: ack latency | p95/p99 group-commit ack across ≥ 2 segment sizes within stated budget (relative to the configured `segment_max_latency_ms` window) |
| Pass: throughput | sustained items/hr at or above the E0 per-queue floor (≥10M items/hr per queue) reported alongside the ack-latency distribution |
| Pass: cost | $/billion-commands beats `postgres_native` at high sustained volume (ADR-001 cost table) |
| Pass: recovery | rebuild 10M-item SQLite projection from snapshot + log tail within stated recovery-window budget |
| Pass: manifest fencing | a stale-epoch writer's manifest CAS commit is rejected; on a no-CAS object store the Postgres-held manifest pointer enforces the same fence (TD-004) |

### Recurrence under scale (both backend profiles)

Run the recurrence scale row under BOTH the Postgres-native profile (E1 shape)
and the object-log + SQLite profile (E2/E3 shape). This row substantiates that
recurring/never-terminal items participate in the scale envelopes without special
handling (recurring items participate in cross-shard queue-global oldest-eligible
aggregation like any item).

| Benchmark | Required Evidence (both profiles) |
|-----------|-----------------------------------|
| Recurrence under scale (D4) | (a) **High-frequency immediate rearm** (`not_before` = now tight loop) sustains target throughput without version-monotonicity or projection corruption; (b) **idle recurring inventory** of N idle re-armed items does not inflate active-scope discovery, busy-poll, or `oldest_eligible_age_ms`, and `recurring_pending` is reported within its documented lag; (c) **purge under load** (targeted + `force` while leased), including multi-shard split and partial-commit replay, completes within bound and leaves consistent tombstones. |

## Requirement Coverage Matrix

These rows extend the governing test traceability plan with the scale mechanism.
P0 items are referenced by name (not number) to stay robust to PRD renumbering.

| Requirement | Governing Artifact | Required Test Evidence |
|-------------|--------------------|------------------------|
| PRD P0 horizontal-distribution item | PRD / TD-001 / TD-003 | E2 multi-shard scale-out: a single queue scales to the ≥ 4×-at-8-shards bar; the E0 per-queue floor holds for every queue under K-queue concurrency; cross-shard progress holds; single lease across rebalance. |
| PRD P0 performance-at-scale item | PRD / TD-001 / TD-002 / TD-004 | E1 (single queue ≥ E0 floor of 10M items/hr, sub-second p95/p99) and E2 (single queue scales beyond one deployment's ceiling AND the E0 floor is preserved for every queue at any scale, while preserving the queue-global progress bound). |
| PRD P0 queue-density item | PRD / TD-001 / TD-002 / TD-003 / TD-004 | E2 queue density: ≥1000 concurrently active queues on a single node, each meeting its progress bound, no cross-queue degradation, any one able to reach the per-queue floor, and per-queue/per-shard background work multiplexed onto bounded shared per-node pools (`queue_density_single_node_tests`). |
| TD-003 shard ownership | TD-003 | Deterministic assignment, epoch fencing of a stale owner, graceful drain without loss/duplication, recovery, and stalled-shard visibility. |
| TD-004 object-log backend | TD-004 / ADR-001 | E3 cost/ack/recovery; manifest-CAS (or Postgres-pointer fallback) fencing; passes the shared TD-001 backend conformance suite including multi-shard rows. |
| Cross-shard progress (D1) | TD-001 / TD-003 | Queue-global oldest-eligible = max effective (gate-aware) age across shards; cold-shard oldest item claimed before the bound under a hot shard. |
| Recurrence under scale (D4) | TD-001 / TD-002 / TD-004 | Recurrence scale row passes under both backend profiles: high-frequency rearm, idle inventory bound, purge under load with multi-shard partial-commit replay. |
| Shared backend conformance | TD-001 | Both `postgres_native` and `object_log_sqlite_projection` pass the same TD-001 shared backend conformance suite (including group co-residency, cohort, `same_group_key`, and the multi-shard rows) before either is selectable by backend profile. |

## Named Test Suites

Implementation beads should create or extend these suites:

- `sharding_assignment_fencing_tests`
- `sharding_rebalance_drain_tests`
- `cross_shard_progress_tests`
- `multi_shard_claim_order_replay_tests`
- `object_log_commit_recovery_tests`
- `performance_multi_shard_scale_out_tests`
- `performance_single_deployment_baseline_tests`
- `queue_density_single_node_tests`
- `recurrence_scale_both_profiles_tests`

## Scale Evidence Requirements

Scale benchmarking must include:

- single-deployment write/throughput/latency meeting the per-queue floor E0
  (≥10M items/hr per queue) (E1);
- multi-shard scale-out at ≥ 3 shard counts across independent storage units,
  reported as aggregate accepted write/claim rate per shard count (E2);
- the E0 per-queue floor preserved for every queue under concurrent load as the
  active-queue count and total load grow (the "every queue at any scale"
  guarantee, E2);
- queue density: ≥1000 concurrently active queues on a single node, each meeting
  its progress bound with no cross-queue degradation, any one able to reach the
  per-queue floor, and all per-queue/per-shard background work multiplexed onto
  bounded shared per-node pools (E2, `queue_density_single_node_tests`);
- cross-shard queue-global progress under a hot shard plus a cold shard holding
  the queue-global oldest-eligible item (E2);
- stalled/draining/unowned shard visibility as a progress-bound violation (E2 /
  TD-003);
- deterministic k-way-merged claim ordering for strict queues across all relevant
  shards, and bounded relaxation for bounded-relaxed queues (E2);
- fan-out claim replay convergence and committed-partial-set behavior under
  per-shard partial failure (E2);
- object-log group-commit ack latency across ≥ 2 segment sizes, $/command at high
  volume, and 10M-item projection rebuild time (E3);
- manifest-CAS fencing, including the Postgres-held manifest-pointer fallback on
  no-CAS object stores (E3);
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
- A PRD or product-vision scale sentence names a storage backend, profile, shard
  mechanism, or SQL.
- A scale claim in any document lacks an E-record ID.

Reviewers MUST reject documents matching any rule above.

## Exit Criteria

Before scale claims are published, the referencing evidence records must pass
against the E0 per-queue floor (≥10M items/hr per queue, preserved for every
queue at any scale): E1 for the single-deployment envelope, E2 for the horizontal
envelope (including the every-queue-at-any-scale floor under K-queue concurrency),
and E3 for the object-log cost/ack/recovery profile. A scale claim in any
document must cite at least one evidence record (E0–E3) and, where it asserts a
benchmark outcome, the named scale test suite that produces it. A horizontal-scale
claim MUST NOT be substantiated by `postgres_native` alone.

## Open Questions

1. **Tier-2 published pass bar**: the E0 floor (≥10M items/hr per queue) is fixed;
   beyond it, is the default single-queue scale-out headline "≥ 4× single-deployment
   E1 ceiling at 8 shards, monotonic across 2/4/8" the multiple you want, or a
   different fixed multiple / efficiency target?
2. **Tier-2 comparator scope for E2**: object-log is required for E2; do you also
   want the multi-shard `postgres_native` comparator run (independent DBs per
   shard) so E2 and E3 can share a harness, or object-log only?

Resolved: the queue-density target is **≥1000 concurrently active queues on a
single node** (density + floor-reachable: every active queue meets its progress
bound and any one can reach the per-queue floor; the single node is not required
to sustain 1000× the floor in aggregate — multi-node provides aggregate
headroom).
