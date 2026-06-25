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
- **BQ-01 engine types/ports re-key.** Remove `ShardId` + `ShardId::ZERO`; make `QueueKey =
  (tenant_id, queue_id)` the unit. `CommandPosition { queue: QueueKey, backend_epoch, sequence }`; drop
  `CommandEnvelope.shard_id`; re-key `ClaimRequest`, `LogRead`/`ProjectionRead`/`SnapshotRef`,
  `ControlPlaneStore::current_epoch(&QueueKey)`. *Acc:* `cargo build` workspace; `cargo test -p
  pqueue-engine -p pqueue-core`; `! grep -rE 'ShardId|ShardKey' crates/*/src` (no symbol remains).
- **BQ-02 driven-adapter + projection re-key.** `pqueue-projection` + memory/sqlite/postgres/objectlog:
  maps keyed `QueueKey`; durable schemas drop the shard column; `(tenant,queue,seq)` PKs. *Acc:* `cargo
  test -p pqueue-projection -p pqueue-memory -p pqueue-sqlite -p pqueue-objectlog`; the env-gated
  `pqueue-postgres` build compiles.
- **BQ-03 driving-adapter + harness re-key.** `pqueue-resp`, `pqueue` lib, `pqueue-server`,
  `pqueue-conformance`. *Acc:* full `cargo test` workspace green + `cargo clippy --all-targets -D
  warnings`.

### P1 — Relational projection family ⭐
- **BQ-10 relational seam + conformance class.** Decide how a backend declares a DB-authoritative
  projection (the ports already permit it; add the `relational-reconnect` durability/conformance class
  to `pqueue-conformance` — core suite that every family passes + a reconnect-after-crash suite that
  substitutes for log-replay). *Acc:* `relational_reconnect_suite!` skeleton compiles + runs; design note
  committed.
- **BQ-11 sqlite relational projection.** `pqueue_items` (TD-002 columns), serialized SQL claim (the
  TD-002 claim CTE; SQLite serializes writers), `pqueue_group_summary`, idempotency/tombstone tables,
  lifecycle apply as SQL UPDATEs; log-optional single-writer; reopen→committed-state recovery. *Acc:*
  sqlite relational mode passes the core conformance suite at parity with the in-memory reference +
  relational-reconnect.
- **BQ-12 postgres relational projection.** Same schema in Postgres; real `FOR UPDATE SKIP LOCKED` claim
  CTE; connection pool + `spawn_blocking` (fix the sync-client-in-tokio limitation). *Acc:* `cargo test
  -p pqueue-postgres` (env-gated `PQUEUE_PG_TEST_URL`) passes core + relational-reconnect; non-gated
  subset runs without a DB.
- **BQ-13 two-family parity.** Hold the in-memory log-replay family and the relational family
  behaviorally identical on the core class; relational backends additionally pass relational-reconnect,
  log-bearing backends pass log-replay. *Acc:* the full conformance matrix passes per backend's class.
- **BQ-14 group/cohort/gate/discovery on the relational projection.** `pqueue_group_summary` keyed
  `(tenant,queue,group_key)`; whole_group / whole_cohort owner-local; `SetGates` exact-on-read anti-join;
  `DiscoverActiveScopes` owner-local ranking. *Acc:* group-batching, cohort, gate, and discovery
  conformance/service tests pass on the relational backend.

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
- [ ] BQ-01 · [ ] BQ-02 · [ ] BQ-03   (P0)
- [ ] BQ-10 · [ ] BQ-11 · [ ] BQ-12 · [ ] BQ-13 · [ ] BQ-14   (P1)
- [ ] BQ-20 · [ ] BQ-21 · [ ] BQ-22 · [ ] BQ-23 · [ ] BQ-24   (P2)
- [ ] BQ-30 · [ ] BQ-31 · [ ] BQ-32   (P3)
- [ ] BQ-40 · [ ] BQ-41 · [ ] BQ-42 · [ ] BQ-43   (P4)
