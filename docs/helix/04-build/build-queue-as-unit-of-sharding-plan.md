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
  with owner-node count; per-queue floor held. *Acc:* the suite passes its bars. **DONE** — in-process
  owner-independence measurement in `crates/pqueue-bench/tests/` (independent `MemoryBackend` owners over
  disjoint queues on real OS threads; measured, not constant-writer). Asserts: no cross-owner contention
  (aggregate non-regressing 1→2→4→8), parallel scale-out ≥60% of ideal vs the 2-owner baseline (spec-shaped,
  core-scaled; measured 3.55× @8 on 12 cores), worst-single-queue E0 floor held. Also un-rotted pqueue-bench
  (orphaned/unbuildable → self-contained workspace; fixed 3 rot-compile errors in `src/main.rs`). HEADLINE
  E2 (object-log backend, REAL multi-node, ≥3.5×@8 cross-node efficiency) honestly DEFERRED to live run
  `pqueue-f1d107de`; gate-wiring + e2-source reconciliation recorded on BQ-43. Fresh-eyes GO-with-conditions;
  in-scope conditions (min-not-avg floor, spec-shaped baseline, tightened tolerance, honest labels) applied.
- **BQ-41 queue density.** `queue_density_single_node_tests` at ≥1000 active queues/node. *Acc:* density
  bars + no cross-queue degradation. **DONE** — two-phase suite in `crates/pqueue-bench/tests/`. Phase 1
  (single-threaded residency ladder 0→100→1000): ≥1000 queues concurrently resident (verified via
  `metrics()`), hot-path per-op cost flat across resident count (rules out an O(total_queues) scan),
  correctness isolation (neighbours undisturbed), hot clears the E0 floor. Phase 2 (bounded real-thread
  pool driving 1000 queues concurrently on the **same** shared `Mutex<State>` node): the genuine FR-43 bar —
  hot queue holds the E0 floor under real contention (measured claim 56% of unloaded baseline, still ~43×
  the floor). Fresh-eyes: prior NO-GO (single-threaded idle neighbours made "no-degradation" structurally
  trivial) → fixed by the concurrent phase + "active"→"resident" relabel; re-review **GO**. DEFERRED (not
  claimed): bar (d) bounded shared per-node pools, progress-bound-active under a live sweeper, durable-backend
  density → recorded on BQ-c33c367e + the live run; bead acceptance reconciled to the in-process scope.
- **BQ-42 object-log E3 + retire old suite.** Re-run object-log cost/ack/recovery (E3); delete
  `performance_multi_shard_scale_out_tests`. *Acc:* E3 evidence row; old suite gone. **DONE** — recreated
  the spec-named `object_log_commit_recovery_tests` (deleted with pqueue-service) in `crates/pqueue-objectlog/
  tests/`, driving the real `ObjectLogBackend`: measured ingest 102k/s + claim+ack 151k/s (both ≫ E0 floor),
  per-commit ack-latency distribution (reported), and recovery (drop+reopen rebuilds the full resident set
  purely from the durable log — verified `pending==N` from disk). Old suite already gone (hexagonal migration;
  TP-002 marks it retired). Fresh-eyes GO-with-conditions (prior BLOCKING: recovery is full-genesis replay
  not snapshot+tail, and the projection is in-memory not SQLite — both now DISCLOSED, not papered). Deferred:
  group-commit ack/cost/snapshot+tail/SQLite-projection/10M-in-S3 → pqueue-2f9ebac3; manifest-CAS →
  pqueue-e5c6d6fc; E3 source-mapping + ledger-row → BQ-43 (gate stale). `#[ignore]` at-scale rebuild test added.
- **BQ-43 release gate.** E0 floor preserved across all; TP-002 E0–E3 + TP-003 P0/core gates green from
  source-backed evidence. *Acc:* `scripts/ci/release-gate.sh` (or its successor) green. **IN PROGRESS** —
  investigation found the entire release-evidence subsystem (ledger emitter, `pqueue-verify-ledger` binary,
  E0/E1 postgres `product_validation_tests`) was DELETED with pqueue-service in the hexagonal migration; the
  gate scripts still reference deleted crates. **Product-owner decision: rebuild the subsystem.** Decomposed
  into sub-beads (build-q, P4):
  - **BQ-43a** (pqueue-b5a49bd0) **DONE** — new `crates/pqueue-release` crate: verification-ledger row schema
    (TP-001/002/003; the 11 fields release-gate.sh validates) + JSONL `append_row` emitter + `pqueue-verify-ledger`
    binary (`--strict`/`--require-evidence`, rejects failed/untraceable/malformed/empty, asserts E0–E3 present).
    Fresh-eyes GO-with-conditions (atomic single-`write_all` append; `--require-evidence` implies `--strict`) applied.
  - **BQ-43b** (pqueue-721d91b3) **DONE** — `performance_single_deployment_baseline_tests` drives live
    postgres_native, measures E0 throughput + E1 batch-op p95/p99, asserts correctness always, emits E0/E1
    ledger rows (real measured values; LOUD-skips without a DB). Two lanes: SMOKE (default, smoke-tier rows,
    no perf hard-fail — a bridge DB isn't a perf env) vs PERF (`PQUEUE_PERF_ENV`, hard-asserts + release-tier).
    **MEASURED FINDING:** postgres_native is ~20-40× under the E0 floor on a non-provisioned DB (relational
    per-item INSERT round-trips) → backend batch-write optimization + provisioned perf-env run tracked on
    pqueue-d3371502. Fresh-eyes GO-with-conditions: the gate tier-enforcement (smoke must not green release;
    current `release-gate.sh` ignores `evidence_tier`) is a BLOCKING **BQ-43e** condition.
  - **BQ-43c** (pqueue-28c704e2) **DONE** — the three measured suites emit a verification-ledger row from
    their REAL measured values (`<suite>.jsonl`) and assert it strict-validates. Rows are `evidence_tier=smoke`
    (in-process / file-backed reference) so they're recorded but do NOT satisfy a release E0–E3 gate;
    pqueue-release gained an `evidence_tier` field + tier-aware `verify_ledger` (fresh-eyes condition: smoke
    must not green the headline). Live release-tier E2/E3 come from pqueue-f1d107de / pqueue-2f9ebac3.
  - **BQ-43d** (pqueue-f0dc083e) — `product_validation_tests` AC-E2E-1..9 rebuilt on the current lib facade.
    Decomposed per-workflow (all smoke-tier, ac_ids ledger rows): **BQ-43d.9 DONE** (pqueue-940190e6 — harness
    + AC-E2E-9 downstream-pacing non-goal: proves no rate/admission state by lifecycle accounting + claim
    returns min(max,eligible) across wall-clock pauses + payload round-trip; fresh-eyes GO-with-conditions
    applied). **BQ-43d.8 DONE** (pqueue-7e323937 — AC-E2E-8: strict int64-descending 0-inversions reordering
    proof + opaque payload/metadata round-trip + no-Seventh-Sense; bounded-relaxed accepted+progresses but
    selects strict-equivalently — rank-error-bound feature deferred to pqueue-b725d3ee; fresh-eyes
    GO-with-conditions applied). **BQ-43d.1 DONE** (pqueue-be7f8ea5 — AC-E2E-1 scheduled-action delivery: not_before scheduling/eligibility gating, timestamp-ascending order, single delivery (INV-1), renew commits+preserves lease, bidirectional tenant no-leak, terminal metrics; reschedule/gating/group-claim/redelivery-vs-renew deferred to pqueue-7a96f929; fresh-eyes GO-with-conditions applied). TODO: .2 (pqueue-2919c2c5 Marketo group-batching), .3 (pqueue-d9efa9cd cohort), .4 (pqueue-cac31cd7
    recurring-singleton), .5 (pqueue-b7d3a803 crash-recovery — live-process subset deferred), .6 (pqueue-3e62f414
    noisy-neighbor — discovery/latency-at-scale deferred).
  - **BQ-43e** (pqueue-0ee83e73) — rewire `scripts/ci` gates to current crates + reconcile evidence sources; closes BQ-43.

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
- [x] BQ-20 · [x] BQ-21 · [x] BQ-22 · [x] BQ-23 (core) · [x] BQ-24 (core)   (P2 ✅)
  - **BQ-24** ✅-core (pqueue-981930d8, 6c76587) — engine density primitives: `ResidentQueues<H>`
    (LRU-bounded hot working set — bounded as queue count → 1000) + `renew_all_resident` renewal sweep
    (renewed/fenced/errored partition). Fresh-eyes GO-with-conditions: phantom acceptance test
    (`queue_density_single_node_tests`, a retired multi-shard artifact) reconciled to the engine tests;
    "bounded resources" qualified (cardinality + a release-proof test); "batched" honestly relabelled a
    per-resident sweep; assignment-poll + shared-sweeper deferred to **pqueue-c33c367e**.
  - **BQ-23** ✅-core (pqueue-8e1ab4fc, a47e8fb) — engine ownership primitives: `acquire_and_fence`
    (lease↔storage-fence binding) + `owner_liveness_violation` stalled-queue guard predicate, with
    end-to-end seam tests. Fresh-eyes GO-with-conditions: the overclaim that it "closes the BQ-20/21/22
    deferral" was corrected — only the raw append SEAM is fenced; the real claim/push hot-path stamping +
    full pqueue-server wiring + the 6 TD-003 acceptance scenarios + FR-41 observability are HONESTLY DEFERRED
    to **pqueue-c33c367e** (server ownership runtime). Two-counter acquire→fence non-atomicity documented.
  - **BQ-22** ✅ (pqueue-d09c9292, e1ac605) — transactional postgres `PostgresControlPlane` (durable
    `pqueue_workers` + `pqueue_queue_owner` authority record; each op one `FOR UPDATE` txn). Extracted the
    lease state machine into pure shared engine functions (one authority for both impls); made
    register/heartbeat/resolve/lease fallible. Fresh-eyes NO-GO→GO: fixed B1 (genesis concurrent-acquire
    race — materialize-then-lock), I2 (fail-closed: surface DB errors), I3 (two-connection contention test).
    SQL-shape unit tests + env-gated behavioral tests (live-DB deferred). Epoch↔storage-fence binding = BQ-23.
  - **BQ-21** ✅ (pqueue-3222f668, 041f032) — pluggable `QueueControlPlane` trait + `InMemoryControlPlane`
    reference: `OwnerId`, lease lifecycle (register/heartbeat, HRW `resolve_queue_owner`, acquire/renew/
    begin_drain/release) + the C4b seam invariants (single active lease, monotonic epoch, atomic
    acquire→fence, fail-closed) with 14 tests incl. a threaded contention test. Fresh-eyes GO-with-conditions
    applied (begin_drain expected_epoch, acquire non-idempotency contract, OwnerResolution Option-epoch, the
    two-epoch separation documented). Postgres impl = BQ-22; server/hot-path epoch wiring = BQ-23.
  - **BQ-20** ✅ (pqueue-be15632d, 97f5828) — durable `assignment_epoch` (queues/relational_cursor columns,
    objectlog `epoch.json`, in-memory `LogData`) + `acquire_epoch` (strict durable advance) + the
    `LogWriter::append` epoch fence (reject non-current, both families) + 2 conformance scenarios
    (stale-reject + post-advance/pre-segment) on all 6 backends. Fresh-eyes GO-with-conditions applied:
    hot-path owner-epoch stamping deferred to BQ-21 (documented at fast-path sites); `read_from` now carries
    true per-entry epoch (objectlog per-entry epoch + manifest-CAS tracked → pqueue-e5c6d6fc).
- [x] BQ-30 · [x] BQ-31 (decision core) · [x] BQ-32 (classifier)   (P3 routing core ✅)
  - **BQ-32** ✅-core (pqueue-ac3a5202, c4a3087) — drain command classifier `DrainClass`/`drain_class`/
    `is_new_claim_on_drain` (TD-006 §1A): XREADGROUP `>`→NewClaim / explicit-id→InFlight, XACK/XADD/XDEL→
    InFlight, XAUTOCLAIM→NewClaim (pqueue always re-delivers idle entries), XCLAIM→RuntimeConsumerDependent.
    Fresh-eyes GO-with-conditions (no blocking; `>` wire-safety clears): misattributed quote fixed (TD-003 not
    TD-006), XAUTOCLAIM reclassified, the mixed-XCLAIM per-entry-split interface gap documented + recorded on
    **pqueue-c33c367e**; wired drain-redirect + no-mid-lease acceptance owned by the follow-up.
  - **BQ-31** ✅-core (pqueue-4cb0d507, 15bd8a7) — pure routing `RouteDecision`/`route` (TD-006 §1A): authz-
    first (`-NOPERM` before any placement reveal), serve-only-under-live-current-epoch-lease, `-MOVED` to the
    recorded `active_owner` with the LITERAL-key slot (closes the BQ-30 wire-key trap, no loop), drain split.
    Fresh-eyes GO-with-conditions (no blocking — authz-before-reveal + slot-trap both sound); overclaim
    corrected ("placement revealed" not "ownership consulted"), fresh-resolution precondition documented.
    Live dispatch wiring + AC-ROUTE-1 integration (one-hop convergence + misrouted-write-fenced + bounded-
    stale read) deferred to **pqueue-c33c367e**.
  - **BQ-30** ✅ (pqueue-4fc67b83, 5edf87a) — Redis CRC16 + `hash_slot` (keyHashSlot hash-tag rule) +
    `queue_routing_key`/`queue_slot` + `ClusterNode` + CLUSTER SLOTS/SHARDS/NODES/INFO/MYID/KEYSLOT
    single-node bootstrap, wired into RESP dispatch. Proven by a real `redis::cluster::ClusterClient`
    bootstrap+route e2e (+ unit tests vs Redis reference vectors). Fresh-eyes GO-with-conditions: CRC16
    independently verified correct; the wire-key (`tenant:queue`) vs routing-key (`{tenant/queue}`)
    slot-mismatch reconciliation is recorded as a **BQ-31 (pqueue-4cb0d507)** prerequisite for `-MOVED`.
    Multi-node slot→owner view + per-queue `-MOVED` = BQ-31; live topology = server-runtime.
- [x] BQ-40 · [x] BQ-41 · [x] BQ-42 · [ ] BQ-43   (P4)

> Per-bead dependency edges live in ddx (`ddx bead ready` computes the next implementable bead). Phase
> deps in §1 are the coarse view. Convergence review (2026-06-25): GO-WITH-CONDITIONS, all four must-fix
> applied (B1 atomic P0, B2 shard_count, I1 conformance refactor, I2 BQ-11 split, I3/I4 postgres gating).
> P4 bench suites (`performance_cross_queue_scale_out_tests`, `queue_density_single_node_tests`) are
> **net-new** authoring (the old multi-shard suites lived in the deleted service/storage crates), likely
> in the untracked `crates/pqueue-bench` — adopt or replace it at P4.
