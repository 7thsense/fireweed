# Build Plan — Queue Is the Unit of Sharding (code phase, P0–P4)

**Status:** plan (code-build phase). Governs a `/loop` over the **source tree**.
**Branch:** `build/queue-as-unit-of-sharding` (off the ADR-008 spec cascade).
**Supersedes:** BUILD-001's "Multi-Shard Sub-Decomposition" and its multi-shard beads (B-072 et al.),
which are retired-as-target (BUILD-001 superseded-as-target note). This plan re-decomposes the build for
the per-queue model.

## 0. Goal and current reality

ADR-008 reframed the spec to **the queue is the unit of sharding** (per-queue ownership; two projection
families held by conformance; pluggable control plane; cross-queue scale; TD-006 routing). The spec
cascade is done (C0–C12, doc-only). The **code** has not reached it:

- Hexagonal engine + ports are built (`Backend`/`ClaimPort`/`UpsertPort`/`PushPort`/`ProjectionRead`/
  `ReclaimDriver`/`ControlPlaneStore`/`SnapshotStore`, in `pqueue-engine/src/port.rs`).
- **One projection family only:** every backend (memory, sqlite, postgres, objectlog) delegates to the
  shared in-memory log-replay `pqueue-projection::ProjectionData`, rebuilt from a command log on start.
  There is **no** relational `pqueue_items`/`FOR UPDATE SKIP LOCKED` projection — the RAM-ceiling that
  ADR-008 §Context cost #2 exists to remove.
- Everything is keyed `ShardKey` with `ShardId::ZERO` (a degenerate single shard).
- **No** control-plane/lease/epoch-fence/ownership code is wired (`ControlPlaneStore` is a trait with
  `create_queue`/`queue_definition`/`current_epoch` only — no lease ops, no epoch enforcement on append).
- **No** routing.

**The seam already supports the relational family:** a backend implements `ClaimPort`/`UpsertPort`/
`ProjectionRead` and the `Backend::write` apply-UoW; today they delegate to `ProjectionData`, the
relational family implements them against `pqueue_items` SQL instead. P1 is "implement the ports against
SQL," not a new seam.

## 1. Phases

| Phase | Delivers | Depends on |
|-------|----------|------------|
| **P0 — Per-queue re-key** | Code matches ADR-008 §1 keying: the owned/log/projection unit is `(tenant_id, queue_id)`; `ShardId`/`ShardKey`/`::ZERO` removed. | — |
| **P1 — Relational projection family** ⭐ | DB-resident `pqueue_items` + SQL claim as a 2nd projection family (sqlite + postgres); the relational-reconnect conformance class; removes the RAM ceiling. | P0 |
| **P2 — Per-queue ownership + fencing (TD-003)** | One node owns a queue; `assignment_epoch` + Single Authoritative Fencing Rule; `ControlPlaneStore` lease ops + Postgres impl; drain/reassignment/recovery. | P0, P1 |
| **P3 — Client routing (TD-006)** | Stock clients reach the owner: 16384-slot map, `-MOVED` to recorded `active_owner`, serve-only-under-lease, drain split. | P2 |
| **P4 — Cross-queue scale evidence (TP-002)** | Re-measured E1/E2/E3 + queue density; retire `performance_multi_shard_scale_out_tests`. | P1, P2, P3 |

**Deferred (out of this plan, ADR-008 §4):** the no-Postgres / object-store `ControlPlaneStore` — gated
on an S3-CAS multi-object-atomicity spike that must clear ADR-001's "Postgres preferred / must justify"
bar before it is built.

**Natural cut line: after P1.** P0–P1 deliver the headline value (large queues no longer RAM-bound) on a
single-node deployment that needs no ownership or routing. P2–P4 are the multi-node cross-queue story.

## 2. Bead work-list

Each bead is independently committable, has a `cargo`-gated acceptance, and is keyed to a spec artifact.
Beads are filed in ddx (labels `build-q`, `helix-04`, `area:<crate>`, `phase:P*`). The loop implements
the next dependency-ready bead → `cargo fmt` + `clippy -D warnings` + the bead's `cargo test` gate →
review → commit → `ddx bead close`.

### P0 — Per-queue re-key
- **BQ-01 per-queue re-key (whole workspace) + remove `shard_count`.** **One ATOMIC bead** — re-keying the
  engine trait signatures breaks every adapter until all are updated, so it must compile as a unit
  (convergence-review B1; the original BQ-02/BQ-03 are folded in and closed). (1) Remove `ShardId` +
  `ShardId::ZERO`; `QueueKey = (tenant_id, queue_id)` is the unit; `CommandPosition { queue, backend_epoch,
  sequence }`; drop `CommandEnvelope.shard_id`; re-key every port + `pqueue-projection` + all four driven
  adapters (schemas drop the shard column, `(tenant,queue,seq)` PK) + `pqueue-resp` + `pqueue` lib +
  `pqueue-server` + `pqueue-conformance`. (2) Remove `shard_count` + `deployment_max_shard_count` from
  `QueueDefinition`/`CreateQueue`/validation/the config-identity hash (ADR-008 §1; `pqueue-core/src/domain.rs`
  — convergence-review B2). *Acc:* `cargo build --workspace`; full `cargo test` green; `clippy --all-targets
  -D warnings`; no `ShardId`/`ShardKey`/`shard_count` symbol remains.

### P1 — Relational projection family ⭐
- **BQ-10 relational family in the conformance harness.** Relax/split the `ConformanceBackend` umbrella
  bound so a **log-optional** relational backend (projection IS the authority, no separate
  `LogRead`/`SnapshotStore`) qualifies; split the flat `conformance_suite!` into **core** (every family)
  + **log-replay-addon** (`pause_and_fence_reconstruct_from_log`, `high_water_*`, `snapshots_*`) +
  **relational-reconnect-addon** (new) (convergence-review I1). *Acc:* the three suite macros compile + run;
  existing backends still pass their classes.
- **BQ-11a sqlite relational schema + lifecycle apply-as-SQL.** `pqueue_items` (TD-002 columns) as a 2nd
  DB-authoritative projection; the 14-command apply-UoW as SQL INSERT/UPDATE. *Acc:* lifecycle commands
  round-trip item state (unit + core-non-claim subset).
- **BQ-11b sqlite relational claim CTE + eligibility.** Serialized SQL claim (TD-002 CTE) + Eligibility
  Precedence in SQL. *Acc:* core claim/lease/eligibility conformance at parity with the in-memory reference.
- **BQ-11c sqlite relational group_summary + idempotency/tombstone.** `pqueue_group_summary`
  `(tenant,queue,group_key)`; idempotency + `client_item_key` tombstone, maintained in-transaction. *Acc:*
  idempotency replay, dup-push convergence, purge tombstone, group_summary scenarios pass.
  *DELIVERED:* `pqueue_group_summary` (refresh-from-items in-tx on every grouped-item-affecting arm; consumer
  BQ-14) + `pqueue_item_key_retention` (terminal-purge tombstone → dup-push convergence). dup-push convergence
  for live keys was already done (UpsertPort, BQ-11a/b). **DEFERRED → BQ-11e:** data-plane request-id
  idempotency (`pqueue_request_idempotency`) — no orchestration port carries a `request_id` yet (all
  envelopes `request_id:None`; `QueueIdempotencyCache` is operator-repair-only), so the table would be dead
  code; needs a request-id-carrying port (cross-cutting). group_summary/retention are RELATIONAL-ONLY
  (kept out of the shared core class so the two families stay identical on core; BQ-13 must respect this).
- **BQ-11d sqlite relational reconnect recovery.** Reopen→committed-state, no log replay (the
  relational-reconnect class). *Acc:* relational-reconnect suite passes on sqlite.
- **BQ-12 postgres relational projection (live-DB-gated).** Same schema in Postgres; real `FOR UPDATE SKIP
  LOCKED` CTE; pool + `spawn_blocking` **fixing the recorded high-water TOCTOU** (convergence-review I4).
  *Acc:* env-gated `cargo test -p pqueue-postgres` passes core + relational-reconnect **incl. a
  contended-writer test**; non-gated SQL-assembly subset runs without a DB; deferred live-DB evidence noted.
- **BQ-13 two-family parity.** Both families identical on core; relational also passes relational-reconnect.
  Without a live DB, parity evidence is **sqlite-relational vs in-memory only**; postgres half
  deferred-with-reason (convergence-review I3). *Acc:* the conformance matrix passes per backend's class.
- **BQ-14 group/cohort/gate/discovery on the relational projection.** whole_group/whole_cohort owner-local;
  `SetGates` exact-on-read anti-join; `DiscoverActiveScopes` owner-local ranking. *Acc:* group-batching,
  cohort, gate, discovery tests pass on the relational backend.

### P2 — Per-queue ownership + fencing (TD-003)
- **BQ-20 epoch fence.** `assignment_epoch` on durable schemas; the Single Authoritative Fencing Rule:
  append/claim reject any non-current epoch (durably fenced at acquire), both projection families. *Acc:*
  stale-epoch-reject + post-advance/pre-segment fence conformance scenarios.
- **BQ-21 ControlPlaneStore lease ops.** `register_owner`/heartbeat, `resolve_queue_owner` (HRW over the
  live owner set → target+active+epoch+state), `acquire_queue_lease` (strictly-greater epoch + durable
  fence), `renew_queue_lease`, `begin_drain`, `release_queue_lease`; the C4b seam invariants (single
  active lease, monotonic epoch, atomic acquire→fence, fail-closed). *Acc:* lease-lifecycle unit tests +
  the seam-invariant tests.
- **BQ-22 postgres control plane.** Transactional `ControlPlaneStore` impl (the default). *Acc:*
  control-plane integration tests (env-gated).
- **BQ-23 drain/reassignment/recovery + owner-liveness.** Wire ownership into `pqueue-server`; graceful
  drain, reassignment recovery (snapshot/log-tail or relational-reconnect), stalled-queue/owner-liveness
  guard. *Acc:* TD-003 scenarios — reassignment recovery, drain, interrupted drain, target-vs-active,
  single-writer-under-contention, stalled-queue visibility.
- **BQ-24 per-node density bounds.** Batched lease renewal, one assignment poll per node, LRU-bounded
  per-queue projection handles, shared per-node sweepers. *Acc:* `queue_density_single_node_tests`
  (bounded resources as queue count → 1000).

### P3 — Client routing (TD-006)
- **BQ-30 slot map + bootstrap.** `slot = crc16("{tenant/queue}") % 16384`; `CLUSTER SLOTS`/`SHARDS`
  reply. *Acc:* a stock redis-cluster client bootstraps; slot computation matches the client's.
- **BQ-31 MOVED redirect + lease gate + authz.** Redirect to recorded `active_owner`; serve-only-under-
  live-current-epoch-lease; authz-before-redirect (`-NOPERM` before `-MOVED`); fence-safe staleness. *Acc:*
  AC-ROUTE-1 (one-hop convergence; misrouted write fenced; read bounded-stale).
- **BQ-32 drain command-split.** In-flight (XACK/same-consumer XCLAIM/own-PEL XAUTOCLAIM/renew) stay;
  new claims (`XREADGROUP >`/cross-consumer XCLAIM) get retryable `-ERR pqueue unavailable` until handoff.
  *Acc:* drain redirect test; no worker redirected mid-lease.

### P4 — Cross-queue scale evidence (TP-002)
- **BQ-40 cross-queue scale-out.** `performance_cross_queue_scale_out_tests`: aggregate rate monotonic
  with owner-node count; per-queue floor held. *Acc:* the suite passes its bars.
- **BQ-41 queue density.** `queue_density_single_node_tests` at ≥1000 active queues/node. *Acc:* density
  bars + no cross-queue degradation.
- **BQ-42 object-log E3 + retire old suite.** Re-run object-log cost/ack/recovery (E3); delete
  `performance_multi_shard_scale_out_tests`. *Acc:* E3 evidence row; old suite gone.
- **BQ-43 release gate.** E0 floor preserved across all; TP-002 E0–E3 + TP-003 P0/core gates green from
  source-backed evidence. *Acc:* `scripts/ci/release-gate.sh` (or its successor) green.

## 3. Loop invariants
- **Real code + tests.** Every bead lands compiling, `clippy -D warnings` clean, and its `cargo test`
  gate green. No stubs/`todo!()`/silent no-ops behind a passing test.
- **One bead per commit** on `build/queue-as-unit-of-sharding` (Co-Authored-By). Close the bead after.
- **Dependency order.** Implement the next ready bead (deps satisfied) only.
- **Env-gated work** (live Postgres `PQUEUE_PG_TEST_URL`): run the non-gated subset, mark the gated part
  deferred-with-reason in the bead/commit if no DB is available; do not fake evidence.
- **Fresh-eyes review** for the substantive design beads (BQ-10/-11/-12/-21/-23/-31); documented
  self-review for mechanical ones. Record the verdict in the commit.
- **Spec is the contract.** Build to the ADR-008 cascade; if a bead reveals a spec gap/ambiguity, fix the
  spec (small, reviewed) or escalate — don't silently diverge.

## 4. Progress
- [x] BQ-01 (P0, atomic; folds in the old BQ-02/03)
- [x] BQ-10 · [x] BQ-11a · [x] BQ-11b · [x] BQ-11c · [x] BQ-11d · [x] BQ-12 (built; live-DB + contended-writer deferred, no PQUEUE_PG_TEST_URL) · [x] BQ-13 (matrix documented + head-to-head sqlite-relational-vs-in-memory parity test; postgres half live-DB-deferred) · [ ] **BQ-14 — BLOCKED** on a cross-cutting port-surface + API-001 decision (claim-compatibility / gate / discovery ports do not exist; see bead pqueue-2961924a) · [ ] BQ-11e (deferred: request-id idempotency, needs request_id port)   (P1)

> **P1 CUT LINE — product-owner decision: BUILD THE PORT-SURFACE EPIC.** The core P1 deliverable (the
> DB-authoritative relational projection family, both backends, full core-class parity, relational-reconnect,
> `group_summary`/`retention` substrate) is **COMPLETE and green** (BQ-01–13; the single-node RAM-ceiling
> fix). **BQ-14** (group-batching/cohort/gate/discovery) was blocked on ports that don't exist; the product
> owner chose to build that cross-cutting port surface (bringing the code up to what API-001 already
> specifies for Batch Claim). **BQ-14 is decomposed into ordered build beads** (the original
> pqueue-74155103 stays as the umbrella):
>  - **BQ-14a** ✅ (pqueue-54b27fdd) — `ClaimRequest` carries `ClaimCompatibility` through `ClaimPort` + all 5
>    backends + facade; engine resolves `ClaimUnit` via the existing `validate_claim_compatibility`;
>    item-level claim unchanged at parity. *(foundational; everything claim-related depends on it)*
>  - **BQ-14b** ✅ (pqueue-b3276967) — relational `group_batching` + `same_group_key` selection via
>    `pqueue_group_summary`. *(dep 14a)*
>  - **BQ-14c** ✅ (pqueue-12eef939) — `pqueue_cohorts` projection + `whole_cohort` all-or-nothing claim. *(dep 14a)*
>  - **BQ-14d** ✅ (pqueue-3c64d86e) — relational gate projection (`pqueue_item_gates` + `pqueue_gate_state`)
>    + exact-on-read eligibility anti-join (item/group/cohort); `SetGates` command + `PushItem.gate_keys`.
>    Relational-only (in-memory no-op, parity preserved). Fresh-eyes GO-with-conditions (Postgres
>    `FOR UPDATE`+`NOT EXISTS` confirmed safe); operator-facing enforcement guard deferred → pqueue-d3ad4b22.
>  - **BQ-14e** ✅ (pqueue-fde32048) — `DiscoveryPort::discover_active_scopes` (relational) rolling up
>    `pqueue_group_summary` into ranked `ActiveScope`s (owner-local oldest-first; Group detail / Queue
>    rollup) via the existing `active_scope` domain logic. Relational-only (parity preserved). Fresh-eyes
>    GO-with-conditions: at-risk reported `None` (deferred, not a fabricated 0); pause-agnostic + lag caveat
>    (pqueue-64351bdd) documented. **BQ-14 epic complete (a–e).**
- [ ] BQ-20 · [ ] BQ-21 · [ ] BQ-22 · [ ] BQ-23 · [ ] BQ-24   (P2)
- [ ] BQ-30 · [ ] BQ-31 · [ ] BQ-32   (P3)
- [ ] BQ-40 · [ ] BQ-41 · [ ] BQ-42 · [ ] BQ-43   (P4)

> Per-bead dependency edges live in ddx (`ddx bead ready` computes the next implementable bead). Phase
> deps in §1 are the coarse view. Convergence review (2026-06-25): GO-WITH-CONDITIONS, all four must-fix
> applied (B1 atomic P0, B2 shard_count, I1 conformance refactor, I2 BQ-11 split, I3/I4 postgres gating).
> P4 bench suites (`performance_cross_queue_scale_out_tests`, `queue_density_single_node_tests`) are
> **net-new** authoring (the old multi-shard suites lived in the deleted service/storage crates), likely
> in the untracked `crates/pqueue-bench` — adopt or replace it at P4.
