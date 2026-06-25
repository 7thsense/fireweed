# Build Progress — hexagonal migration

Tracks the in-loop execution of `hexagonal-migration-plan.md` (v4). Each chunk: implement → review →
test/realign → update this file → continue. Update the **Cursor** and the checklist every iteration.

## Cursor
- **✅ SPEC CASCADE — "the queue is the unit of sharding" (2026-06-25, DOC-ONLY, loop C0–C12 done).**
  A separate spec-evolution loop reframed the entire helix cascade away from intra-queue sharding to
  **the queue as the unit of sharding** (ADR-008, the keystone). Chunks C0–C12 on branch
  `spec/coordinated-log-relational-projection`: C0 PRD, C1 ADR-008, C2 ADR-004, C3 ADR-001+ADR-007,
  C4a/C4b TD-003 (per-queue ownership + Single Authoritative Fencing Rule re-scoped per-`(tenant,queue)`
  + pluggable ControlPlaneStore seam contract), C5 TD-001 (single-owner claim + two projection families
  held by conformance: core/log/relational-reconnect classes), C6 TD-002 (per-queue relational projection,
  `shard_id` dropped, `%N` recast as internal storage-only), C7 API-001 (`shard_count`/`group_co_residency`
  removed; claim + progress per-queue), C8 API-002 + ADR-002 (operator ops whole-queue; `admin:shard`→
  `admin:queue`), C9 TD-004/005/006/007 (object-log per-queue; **TD-006 new client routing — HRW/MOVED,
  fence-safe redirect, no scatter-gather — fresh-eyes NO-GO→fixed→GO**), C10 TP-002 (E2 → cross-queue
  scale-out), C11 TP-001+TP-003 (test rows per-queue ownership/routing), C12 full-cascade sweep +
  `ddx doc stamp` re-stamp + this ledger entry. **Deferred to a later build phase:** re-decomposing the
  CODE build (BUILD-001's multi-shard beads are superseded-as-target), the no-Postgres / object-store
  control plane (spike-gated on S3-CAS multi-object atomicity, ADR-008 §4), and re-measuring the reframed
  cross-queue TP-002 E2 (the prior E2 source beads measured the retired multi-shard build). The spec
  cascade is logically consistent and re-stamped; no source changed.
- **✅ MIGRATION COMPLETE (Phases 0–7 all done).** Hexagonal "one engine, two interfaces" is built,
  reviewed, and green: 10 crates, full default workspace 39 suites + 0 fails + clippy 0, dependency-
  direction test passing, legacy deleted, reconciliation report (`PHASE-7-reconciliation.md`) shows no
  dropped item. Owed/deferred items (tracked in the report): postgres adapter (needs a live DB),
  TD-006:129 attempt-count reconciliation, RESP server-side id generation, graceful connection drain,
  RESP polish (XCLAIM/XLEN/XINFO/XDEL + cursor-pagination/intra-group/race e2e), library renew/rearm
  (need a RenewLeasePort) + doc-hygiene/bead re-scope. The autonomous loop is STOPPED.
- **(Historical) Phase 7 cursor — final reconciliation report (LAST phase; loop ends after this).** Phases 1–6 are
  COMPLETE: the hexagonal architecture is built (engine + projection + conformance; memory/sqlite/objectlog
  adapters; RESP front + library facade + composition root), all reviewed/green, and the legacy crates are
  deleted (full default workspace = 38 suites green, clippy 0). Author the reconciliation report at
  `docs/helix/04-build/PHASE-7-reconciliation.md`: for EACH item in the master plan
  (`hexagonal-migration-plan.md` §0 goals/non-negotiables, §1 crate topology, §2 engine model, §3 RESP
  surface + Invariants 1&2, §4 legacy teardown, §5 phases, §6 DoD) state IMPLEMENTED+TESTED (cite the
  crate/test) or DESCOPED-WITH-REASON. Explicitly land the TRACKED DECISIONS as open/owed items with their
  rationale: (a) postgres adapter DEFERRED (needs a live DB; will be built fresh to the engine ports via
  the durable-adapter template); (b) TD-006:129 attempt-count-on-reclaim charges 2 not 1 (projection
  model vs contract — reconcile by amending TD-006 or adding a combined reclaim-relelease op); (c) RESP
  server-side id generation can collide across two servers on one backend (single-server-per-backend today;
  route through PushPort/IdGen later — mirror the facade fix); (d) graceful connection-drain on shutdown
  (abort is not a drain); (e) deferred RESP polish (XCLAIM, two-consumer race e2e, paginated XAUTOCLAIM
  cursor); (f) deferred library verbs renew/rearm (need a RenewLeasePort). Confirm the non-negotiables hold
  (no stubs/legacy-fallbacks/shims; modular encapsulation/dependency-direction; behavioral no-stub
  conformance on every backend; e2e RESP via an off-the-shelf redis client; structured errors). Re-scope/
  close beads tied to deleted crates. Adversarially review the report against the actual tree (did it claim
  anything untested? miss a gap?). When it converges and the full workspace is green → the migration loop
  is DONE; STOP the loop (no further ScheduleWakeup).
- **Old Phase-6 cursor (done):** delete legacy + supersede docs. Phases 1–5 context:
  conformance harness; 3 adapters (memory/sqlite/objectlog); RESP front (full §3) + library facade
  (`pqueue`) + composition root (`pqueue-server`) — the hexagonal "one engine, two interfaces" is built,
  all reviewed/green (workspace 25 suites). This chunk removes the OLD architecture. STEP 1 (verify
  safety): confirm NO new-architecture crate (core/engine/projection/conformance/memory/sqlite/objectlog/
  resp/pqueue/pqueue-server) depends on any legacy crate — `cargo tree`/grep the path-deps. STEP 2
  (delete): remove `pqueue-service`, `pqueue-client`, `pqueue-kafka`, `pqueue-storage`, AND the
  OLD-architecture `pqueue-postgres` (it implements the dead storage traits + depends on pqueue-storage;
  the DEFERRED postgres adapter will be created FRESH to the engine ports when a DB is available — note
  this in the deferral). Delete their `crates/<name>` dirs + remove from `Cargo.toml` members AND
  default-members; drop the now-moot "pqueue-service excluded" comment. STEP 3: `cargo build`/`test`
  the whole default workspace stays green (now nothing is excluded). STEP 4 (docs): supersede/rewrite the
  helix docs that describe the OLD HTTP-service/Kafka/storage-trait architecture so they match the
  hexagonal engine + RESP/library model (ADRs/TDs/contracts that reference the deleted surface) — at
  minimum mark them Superseded with a pointer to the hexagonal-migration-plan + the new crates. STEP 5:
  re-scope/close any beads tied to the deleted crates. Real deletion → adversarial review (did anything
  live still depend on the deleted code? are the docs now self-consistent?). After Phase 6: Phase 7
  (reconciliation report: every §1–§6 plan item implemented+tested or descoped-with-reason, INCLUDING the
  tracked decisions — TD-006:129 attempt-count, RESP server-id collision, postgres-deferred, graceful-
  shutdown drain). Postgres adapter (fresh, engine-ports) lands when a DB is provisioned.
- **Old Phase-5 server cursor (done):** composition root. Earlier facade cursor: verbs
  over `PushPort`/Claim/Finalize/etc., backend+clock injected, fresh-eyes reviewed, B1/B2 fixed via the
  new PushPort, 6 green). Build `pqueue-server`: a binary/lib composition root that (1) selects a backend
  by config (memory / sqlite path / objectlog root) + builds a `SystemClock`; (2) starts the RESP
  `serve(listener, backend, clock)` task; (3) starts a **background ReclaimDriver task** that periodically
  calls `tick(clock.now())` (e.g. via `tokio::time::interval`) so expired leases are reclaimed on a quiet
  queue WITHOUT client traffic — the TD-007 §3 orphan gap that XAUTOCLAIM (client-triggered) alone leaves
  open; (4) exposes an ops/health probe (a simple readiness check / metrics surface). Keep it hexagonal:
  the composition root is the ONLY place that names concrete backends. Test: a background-tick test
  (ManualClock or a real short interval) proving an orphaned lease is reclaimed with no client traffic;
  an integration smoke that boots the server + drives it with the `redis` client. Real production logic →
  fresh-eyes review. After Phase 5: Phase 6 (delete legacy service/client/kafka/storage + supersede docs),
  Phase 7 (reconciliation report incl. the attempt-count + RESP-id-collision decisions). Postgres when a
  DB is available.
- **Old Phase-5 facade cursor (done):** the ergonomic library interface. Phase 4 RESP context:
  functionally COMPLETE: full TD-006 §3 worker surface (XADD-upsert / XREADGROUP-`>` / XACK / XPENDING /
  XAUTOCLAIM) with real lease TTLs + reclaim, driven by an off-the-shelf `redis` client over TCP, e2e
  incl. drain-reconcile, upsert-dedup, fence-stale, superseded-ack, reclaim-after-expiry, crash-recovery
  (sqlite reopen), and collisions — all reviewed/green. (Part-3 polish — XCLAIM specific-id, two-consumer
  race e2e, paginated XAUTOCLAIM cursor — is deferred, low priority.) This chunk builds the LIBRARY
  interface + the composition root that wires it all: (1) a `pqueue` facade crate exposing the ergonomic
  Rust library API (push/claim/ack/nack/renew/peek over the engine ports — the second of the "exactly two
  interfaces"); (2) `pqueue-server` composition root: DI that selects a backend (memory/sqlite/objectlog)
  + a `SystemClock` + an `IdGen`, starts the RESP `serve` task, AND runs a **background ReclaimDriver
  task** (periodic `tick(clock.now())` so leases are reclaimed without client traffic — the orphan-on-
  quiet-queue gap, TD-007 §3 — currently only XAUTOCLAIM triggers reclaim). Wire an ops/health probe.
  Real production logic → fresh-eyes review. After Phase 5: Phase 6 (delete legacy service/client/kafka/
  storage + supersede docs), Phase 7 (reconciliation report — INCLUDING the TD-006:129 attempt-count
  decision above). Postgres slots in when a DB is available.
- **Old Phase-4 cursor (done):** RESP clock + reclaim. Part 1 done (XADD-upsert, XPENDING, e2e; reviewed).
  This chunk adds the time-dependent surface. (1) Inject a `Clock` into `serve` (`serve(listener, backend,
  clock)`) — change the existing call sites/e2e; provide a real system clock for production (a small
  `SystemClock` in pqueue-resp or via the Phase-5 composition root) and let tests inject
  `pqueue_memory::ManualClock`. (2) XREADGROUP: set `lease_expires_at = clock.now() + queue
  .max_lease_duration_ms` (read the qdef) and `now = clock.now()` — leases now actually expire. (3)
  XAUTOCLAIM `key group consumer min-idle-time start [COUNT n]`: call `ReclaimDriver::tick(clock.now())` to
  reclaim expired leases (→ pending), then claim+redeliver them (attempt_count bumped); reply
  `[cursor, [entries], [deleted]]`. Add `ReclaimDriver` to the `RespBackend` bound. (4) Honor `idle-ms` in
  XPENDING now that a clock exists (resolves part-1 M5). e2e (ManualClock injected so time is
  deterministic): claim → advance clock past TTL → XAUTOCLAIM redelivers with attempt_count incremented;
  crash-recovery/replay (sqlite backend: XADD/claim, reopen the SqliteBackend, projection rebuilt, drain
  continues); a basic two-consumer claim race (no double-delivery). Also clear part-1 TRACKED: I4
  (leased-collision + terminal-collision XADD e2e), M1 (QueueDefinitionConflict→a sane token). STRUCTURED
  errors, off-the-shelf redis client. Real production logic → fresh-eyes review. After Phase 4: Phase 5
  (library facade + `pqueue-server` composition root + a background ReclaimDriver task), Phase 6 (delete
  legacy service/client/kafka/storage + supersede docs), Phase 7 (reconciliation). Postgres slots in when
  a DB is available.
- **Old sqlite cursor (done):** durable LOG in sqlite + projection rebuilt-from-log. Model notes:
  rewrite `pqueue-sqlite` in place to a backend holding a `rusqlite::Connection` (per `:memory:` or
  temp-file db) for the durable log/control-plane, plus an in-memory `HashMap<ShardKey, (LogData?,
  ProjectionData)>` materialization. Concretely:
  - SCHEMA (sqlite, the durability of record): `queues(tenant,queue,definition_json)`,
    `log_entries(tenant,queue,epoch,seq,envelope_blob, PK(tenant,queue,epoch,seq))`,
    `high_water(tenant,queue,epoch,seq)`, `snapshots(...)`. The LOG rows are the source of truth.
  - WRITE UoW (`Backend::write` + the orchestration ports): in ONE sqlite transaction, INSERT the log
    row(s) + bump high_water, then `proj.apply_command(...)` against the in-memory projection; commit the
    txn LAST so durability + the in-mem apply are consistent (if the txn fails, drop the in-mem change).
    Pre-validate (finalize_validate/eligible_candidates/item_state) BEFORE the txn so apply is infallible.
  - REBUILD: on `create_queue`/open, replay `log_entries` for the shard through `apply_command` to
    reconstruct `ProjectionData` (proves the log is the source of truth; conformance's reconstruct test
    already exercises replay).
  - Reuse `pqueue_projection::{ProjectionData, apply_command, decision helpers}`; do NOT reuse the
    memory-only `commit`/`LogData` for the durable path (build the sqlite log + an in-mem ProjectionData).
  - Ports: `Backend`/`LogWriter`/`LogRead`/`ProjectionWriter`/`ProjectionRead`/`ControlPlaneStore`/
    `ClaimPort`/`UpsertPort`/`FinalizePort`/`ReclaimDriver`/`SnapshotStore`. rusqlite is sync behind
    async-fn-in-trait — keep the closure synchronous (no `.await` inside), like memory.
  - GATE: `tests/conformance.rs` = `pqueue_conformance::conformance_suite!(<factory>)` green (fresh db per
    scenario). STRUCTURED errors. NOTE this is real production logic → fresh-eyes review; may span 2
    iterations (schema + log + Backend/Control/Snapshot/Read first; then Claim/Upsert/Finalize/Reclaim).
  - OPEN QUESTION for the user (non-blocking; proceeding with the log-rebuilt default): TD-004 frames
    sqlite as the *projection* (with an external object-log). The v1 here makes sqlite the durable LOG
    with an in-mem projection — correct + durable + reuses the core, but a relational queryable
    sqlite-projection is deferred. Flag in the summary so the user can redirect if they want relational first.
- **After sqlite:** postgres (`ClaimPort`, concurrent-claim races, intra-group exclusion), then objectlog
  (eventual-apply class, upsert banned → `-ERR pqueue unavailable`). Conformance green on each.

## Checklist

### Phase 0 — gating docs (converge before any code)
- [x] ADR-007 authored — `docs/helix/02-design/adr/ADR-007-...md`
- [x] TD-006 refolded to semantic-fidelity model (v3) — done by prior edit
- [x] TD-007 authored — `docs/helix/02-design/technical-designs/TD-007-...md`
- [x] Adversarial review ADR-007 + TD-007 + TD-006 consistency → GO-with-conditions, all applied
- [x] Resolve: PQFIN post-launch (recorded); D2 → meter-only; D1/D3 deferred (tune in Phase 1/3)

### Phase 1 — ports + reference engine + early RESP smoke
- [x] Define ports in `pqueue-engine` (LogWriter/Read, ProjectionWriter/Read, Backend, ClaimPort, UpsertPort, ControlPlaneStore, SnapshotStore, ReclaimDriver, Clock, IdGen) — green, reviewed
- [x] Extract `pqueue-memory` reference impl (atomic class) — storage/projection core + 5 behavioral tests, reviewed
- [x] 1c: `ClaimPort` + `UpsertPort` + `ReclaimDriver` on `MemoryBackend` (Inv 1&2 + timed reclaim) — 12 tests, reviewed
- [x] Engine priority claim/lease/ack + Inv 1&2 + ReclaimDriver realized over memory (via backend ports)
- [x] Backend-conformance (behavioral no-op-fails) green on memory — 12 tests
- [x] 1d: `pqueue-resp` minimal front + drain-and-reconcile e2e with off-the-shelf redis client — XADD/XREADGROUP/XACK smoke path

### Phase 2 — migrate domain logic (move-and-delete, test-first)
- [x] Drop `pqueue-service` from default-members (Cargo.toml `default-members` excludes only service)
- [x] §4a unit: **auth** (AuthContext + authorize_tenant/authorize_operator + hash_lease_token +
  RedactedLeaseToken) → `pqueue-engine::auth` with 3 engine tests; deleted from service. Added
  `EngineError::Forbidden` (→ `-NOPERM`) + fixed RESP `err_reply` to emit `-NOPERM` (not generic).
- [x] §4a unit: request-id idempotency (DURABLE — TD-007 §4) → `pqueue-engine::QueueIdempotencyCache`
  (Proceed/Replay/Conflict/Expired decisions, retention/compaction, replay-from-retained-window test);
  added `EngineError::RequestIdConflict`/`RequestExpired` (distinct wire tokens). Operator replay→409
  deletes from service WITH the operator-op-store unit (it reuses this cache).
- [x] §4a unit: **queue pause/resume + lease fencing** (durable) → new `FinalizePort` (pre-commit
  validation: Leased+!fenced else StaleLease/Terminal/Invalid, no log/projection divergence); RESP
  XACK wired to it (fenced → `-ERR pqueue stale_lease`); pause gates claim/select_eligible/peek; tests
  incl. a log-replay reconstruction of pause+fence. Fixed B1 (commit_locked divergence) for finalize.
- [x] §4a unit: command_position high-water + item_version (durable, TD-007 §4) — high-water already
  advanced on every commit; added 3 conformance tests (advances-on-commit, item_version monotonic
  per item 1→2→3→4, **survives log compaction / not recomputed from entries**) + field doc. Tests-only
  over already-reviewed code paths.
- [x] §4a unit: **claim-compatibility validation** (most load-bearing) → `pqueue-engine::claim_validation`
  (`validate_claim_compatibility` → ClaimUnit; charset re-check since GroupKey newtype only checks
  non-empty; structured `EngineError::BatchTooLarge` added → `-ERR pqueue batch_too_large`). 7 engine
  tests, parity-reviewed vs original (rule-by-rule GO). Deleted from service.
- [x] §4a unit: **finalize/rearm/purge validation** → `pqueue-engine::finalize_validation`
  (validate_finalize_targeting, validate_rearm [Invalid/Terminal], validate_purge_targeting,
  validate_purge_force). 6 engine tests, parity-reviewed. Service keeps compatibility wrappers that
  delegate to engine validation while it is still compiling. CORRECTED canonical purge-force gate
  (leased+!force→Conflict vs service's historical unconditional !force→Conflict) — documented.
- [x] §4a unit (sub A): **operator-operation store** → `pqueue-engine::operator`
  (`OperatorOperationStore<R>`: lookup[replay/RequestIdConflict] / record / get / advance / cancel;
  `OperationId`, `OperatorOperationState{Accepted,Running,Succeeded,Partial,Failed,Canceled}`,
  `OperationHandle`). 9 engine tests, fresh-eyes reviewed (1 BLOCKING fixed → converged). NOT YET WIRED.
  DEVIATION: does NOT reuse QueueIdempotencyCache — owns a permanent `request_id→operation_id` index
  (B1: the expiry-windowed cache would re-execute a destructive op after retention). CORRECTED cancel
  (terminal states left intact vs service's unconditional flip).
- [x] §4a unit (sub B): **active-scope rollup** → `pqueue-engine::active_scope` (`ActiveScope`,
  `DiscoveryGranularity`, `default_for`/`resolve_granularity`, `validate_discovery_request`,
  `project_scopes`, `roll_up_queue_scopes`/`sum_optional`). 7 engine tests, documented self-review
  (pure parity). Faithfully reproduces the service's two-presence-notion quirk (empty queue_id defaults
  to Group then rejected); `sum_optional` uses `saturating_add` (safety refinement). **§4a COMPLETE.**
- [ ] Final: delete pqueue-service entirely (Phase 6)

### Phase 3 — driven adapters
- [x] **Durable-log serde enabler** (sqlite prerequisite): added `Serialize`/`Deserialize` to the engine
  command/log types (`CommandEnvelope`, `QueueCommand` + all 12 variants, `PushItem`, `FinalizeOutcome`/
  `Kind`, `CommandId`, `CommandChecksum`, `ShardId`) + `PriorityValue` (core) + `bytes` serde feature
  (payload). Round-trip test over every variant (JSON-string equality, no PartialEq needed). Unblocks ALL
  durable adapters (each persists the command log). engine 43 + full gate green, clippy 0. Self-review.
- [x] **Projection state machine extracted** → new `pqueue-projection` crate: `ProjectionData`
  (apply_command/transition/eligibility + read+decision queries: eligible_candidates/select_eligible/
  peek/pending_leases/metrics/render_claimed/lookup_by_key/item_state/finalize_validate/expired_leases)
  + `LogData` (append/read_from/high_water/set_high_water/snapshots) + free `commit`. Memory re-pointed
  as a thin persistence wrapper (port impls lock + delegate). Behavior-preserving: 19 tests total (17
  memory incl. conformance macro + 2 projection white-box), byte-identical logic. Fresh-eyes GO (no
  blocking). The DURABLE adapters reuse `apply_command` + the decision helpers (NOT the memory-only
  `commit`/`LogData`); seam design recorded in decisions log.
- [x] **Conformance harness extracted** → new `pqueue-conformance` crate: the 16 port-level behavioral
  scenarios (no-stub, plan §6) lifted out of `pqueue-memory` into reusable
  `scenarios::*<B: ConformanceBackend>(make)` + `run_conformance(make)` + `conformance_suite!(make)`
  macro. `ConformanceBackend` = umbrella over all engine ports (blanket impl). Memory re-pointed via the
  macro; still 19 green (16 shared + 3 white-box kept in-crate: item_version/log-compaction via private
  state, ManualClock/SeqIdGen helpers). Behavior-preserving (same count). clippy 0. Documented self-review.
- [x] **sqlite adapter** → `pqueue-sqlite` rewritten in place (old storage-trait modules + 7 test files
  deleted). Durable LOG in sqlite (rusqlite) + projection rebuilt-from-log (reuses
  `pqueue_projection::ProjectionData`). All 11 engine ports. **Conformance green (16) + 4 durability
  tests** (reopen rebuilds projection + eligibility order; cmd_seq survives reopen w/o id collision;
  high_water persists; snapshots round-trip). Fresh-eyes GO-with-conditions → all cleared: B1 (cmd_seq
  restored on rebuild), B2 (post-commit apply panics not silently diverges), I1 (`MAX(seq)+1` not
  `COUNT(*)`, compaction-safe), I4/M1 (serialize/query errors → `EngineError::Storage`, no panic/swallow).
  clippy 0.
- [x] **objectlog adapter** (EVENTUAL-APPLY class) → `pqueue-objectlog` rewritten in place (dropped the
  external S3 `object-log` git dep). Durable log = immutable per-command JSON **objects** on a local
  filesystem (S3 stand-in, no server); projection rebuilt-from-objects. `durability_class()=EventualApply`;
  upsert banned → `Unavailable` (enforced at BOTH `replace_if_pending` AND the durable write chokepoint
  `append_object`). New **eventual-apply conformance variant** (`pqueue_conformance::eventual_apply_suite!`
  = 12 non-upsert scenarios + `upsert_is_unavailable`). 13 conformance + 4 durability green. Fresh-eyes
  GO-with-conditions → cleared: I-1 (ReplacePending refused at write path + test), I-2 (torn trailing
  object skipped on reopen, not a hard open() failure, + test), M-2 (snapshot ref-id `max+1`). clippy 0.
- [ ] postgres (ClaimPort) — **DEFERRED** (atomic-class near-clone of sqlite; needs a live PostgreSQL
  server, not running here though binaries/docker are available). Tracked as a fast-follow; do via the
  durable-adapter template + env-gated conformance when a DB is provisioned.
- [ ] Conformance green each: concurrent-claim races, intra-group exclusion, class guarantees

### Phase 4 — full RESP adapter + e2e
- [~] `pqueue-resp` PART 1 (clock-independent surface): wired `client_item_key` into XADD (real
  XADD-on-key upsert dedup), added XPENDING (summary + extended, count-honoring, lease-token consumer,
  numeric id order). e2e via off-the-shelf `redis` crate: drain+reconcile (existing) + upsert-dedup +
  XPENDING shrink-on-ack + operator-fence→XACK-stale + superseded-id-ack→`superseded`. Fresh-eyes
  GO-with-conditions → I1 FIXED (shared `finalize_validate` now returns `Superseded` for a superseded-id
  ack, not generic `Invalid` — TD-006 §3/§6.5; wire-tested), I2/I3 FIXED (XPENDING count + numeric id
  bounds + consistent consumer). resp 1+5 green, clippy 0.
- [x] `pqueue-resp` PART 2 (clock + reclaim): injected `Clock` into `serve(listener, backend, clock)` +
  a `SystemClock` (tests inject `ManualClock`); XREADGROUP sets real lease TTL = `now +
  max_lease_duration_ms`; **XAUTOCLAIM** (tick-reclaim + redeliver); XPENDING honors real `idle-ms`;
  err_reply maps `QueueDefinitionConflict` (M1 cleared). e2e: XAUTOCLAIM-after-clock-advance redelivers
  with exact attempt_count==3 + ignored-min-idle + half-open boundary; crash-recovery (sqlite reopen
  rebuilds projection → XPENDING shows leased, drain continues); XADD leased→invalid + terminal→terminal
  collisions (I4 cleared). Fresh-eyes GO-with-conditions → addressed: B1 (attempt double-count is the
  projection's established model, NOT a RESP bug — documented precisely + exact-count test + tracked for
  TD-006:129 reconciliation), I1 (global tick is correct: reclaims any expired lease, redelivery is
  shard-scoped — documented), I2 (min-idle ignored — documented + non-zero-value test), I3 (half-open
  assertion strengthened). resp 1+8 green, clippy 0.
- [~] `pqueue-resp` PART 3 (optional polish, lower priority): XCLAIM (specific-id) is implemented and
  covered (`xclaim_self_renews_no_charge_cross_consumer_reclaims_with_attempt_bump`; conformance covers
  `ReassignLeasePort` and `claimed_view`). Still owed: two-consumer claim race e2e, paginated XAUTOCLAIM
  cursor (TD-006 §3 PEL coverage), plus XLEN/XINFO/XDEL.

### Phase 5 — library + composition root
- [x] **`pqueue` facade crate** (the ergonomic Rust LIBRARY interface — 2nd of the two faces):
  `Pqueue<B>` over the `LibBackend` bound, verbs create_queue/push/push_batch/upsert/claim/ack/nack/fail/
  peek/metrics; backend + clock injected (hexagonal). Fresh-eyes GO-with-conditions → BLOCKING fixed:
  **added a `PushPort`** (engine + memory/sqlite/objectlog) so `push` no longer reaches for
  `Backend::write` — backend-assigned ids (cmd_seq-derived, restart-safe, unique across handles; B2) +
  divergence-safe commit_locked with a shard-exists pre-check (B1/M4); removed the `backend()` escape
  hatch (I1); added `fail` verb (I3); `renew`/`rearm` documented as deferred (need a RenewLeasePort, I2);
  push dedup key defaults to the item id, no synthetic key (I4). 6 tests incl. two-handle-no-collision +
  ack-of-nonleased-error + fail. pqueue 6 + full workspace 22 green, clippy 0.
- [x] **`pqueue-server` composition root** (the ONLY crate naming concrete adapters): `start(Config)`
  selects backend (Memory/Sqlite/ObjectLog) + `SystemClock` + provisions `Config.queues`; `start_with`
  is the generic core (tests inject a backend + clock). Spawns the RESP `serve` task AND a **background
  ReclaimDriver task** (`tokio::time::interval` → `tick(clock.now())`) closing the TD-007 §3 orphan gap.
  `Server` health: `is_running()` (task liveness) + `reclaim_stats()` (ticks/errors/leases — observable,
  not swallowed). Fresh-eyes GO-with-conditions → cleared: B1 (start() was inert/untested — added
  Config.queues provisioning + an end-to-end start() test), I1/I3 (reclaim errors now counted +
  reclaim_stats surface), M2 (reclaim test polls not sleeps). I2 (abort≠graceful connection drain) +
  M1 (bind err→Storage) documented/tracked. server 3 tests + full workspace 25 green, clippy 0.

### Phase 6 — delete legacy
- [x] **Deleted the 5 legacy crates** (`pqueue-service`, `pqueue-client`, `pqueue-kafka`, `pqueue-storage`,
  and the OLD-arch `pqueue-postgres`) — `git rm` + removed leftover untracked `target/` dirs. Verified
  pre-deletion that NO new-arch crate path-deps any legacy crate (the legacy set was closed). Cargo.toml
  simplified: members = the 10 hexagonal crates, `default-members` dropped (nothing excluded), moot
  "service excluded" comment removed, `pqueue` facade now in the default set. **Full default workspace
  green: 38 test-suites, 0 fails, clippy 0, `cargo metadata` resolves cleanly.** Docs: ADR-005 (Kafka)
  marked SUPERSEDED-by-ADR-007 (crate deleted); TD-002 (postgres) noted deleted-old/deferred-fresh-to-
  engine-ports. Remaining design docs' concepts are realized (mapped in the Phase-7 report). Documented
  self-review (mechanical deletion; green full workspace is the proof). NOTE: bead re-scoping for the
  deleted crates folds into Phase-7 cleanup.

### Phase 7 — final gap review against plan
- [x] **Reconciliation report** authored → `docs/helix/04-build/PHASE-7-reconciliation.md`: every plan
  §0–§6 item mapped to DONE+TESTED (cited) or OWED-with-reason; 6 owed items (postgres, attempt-count
  reconciliation, RESP server-id, graceful drain, RESP polish, renew/rearm+doc-hygiene) — none silently
  dropped. Adversarial audit verdict ACCURATE-WITH-CORRECTIONS → fixed: (B1) the no-stub gate's
  "≥1 test per port method" was an overclaim (peek/pending/SnapshotStore were suite-uncovered) → ADDED 3
  conformance scenarios (`peek_is_priority_ordered_and_nondestructive`, `pending_lists_leased_items`,
  `snapshots_write_read_latest`) to BOTH suites so the claim is now TRUE across all adapters; (M1)
  softened the dep-direction-test phrasing to "static manifest-scan". Also ADDED the missing DoD
  dependency-direction test (`pqueue-engine/tests/dependency_direction.rs`). Final full default workspace:
  **39 suites green, 0 failures, clippy 0.** **MIGRATION COMPLETE.**

## Decisions log (append as resolved)
- 2026-06-24 OWED-RESOLUTION LOOP (branch `hexagonal-migration`, plan `OWED-resolution-plan.md`) — closing
  the 6 owed items + retry-exhaustion (B'). **Chunk 1 (commit f6bda1c):** `attempt_count = delivery count`
  — removed the reclaim's `attempt_count += 1` (only `Claim` charges); TD-006:74/128-129 reworded; owed-B
  RESOLVED. **Chunk 2 (commit 0e3619c):** backend-assigned upsert ids — `UpsertPort::replace_if_pending`
  dropped the caller-supplied id, backend mints from `cmd_seq`; RESP `xadd` routes no-key→`PushPort`,
  with-key→`UpsertPort`; e2e `two_servers_on_one_backend_assign_distinct_xadd_ids`; owed-C RESOLVED.
  **Chunk 3 (this commit):** `RenewLeasePort` — `renew(shard, ids, new_lease_expires_at, now)` on
  memory/sqlite/objectlog, each pre-validating via a shared `ProjectionData::validate_leased` helper
  (`renew_validate` MIRRORS `finalize_validate`: NotFound/fenced→StaleLease/terminal→Terminal/
  superseded→Superseded/not-Leased→Invalid) BEFORE any append, then committing a `RenewLease` command via
  `commit_locked`. Fresh-eyes review (no BLOCKING) → hardened the unguarded apply arm with a `debug_assert`
  (loud on divergent replay; apply stays infallible) and extended the conformance scenario to pin the
  Invalid(not-leased) reject. `renew_extends_lease_and_rejects` runs on all 3 backends. Owed-F port-portion
  RESOLVED; facade `renew`/`rearm` verbs + doc hygiene remain Chunk 7. Full workspace green, clippy 0.
  **Chunk 4 (this commit):** postgres adapter REBUILT — `pqueue-postgres` recreated fresh to the engine
  ports via the durable-adapter template over the SYNC `postgres` client (durable LOG in postgres tables +
  projection rebuilt-from-log; atomic class), implementing every port incl. PushPort/UpsertPort/
  RenewLeasePort; re-added to workspace `members` (already in the dep-direction ADAPTERS list). Conformance
  (20) + 2 durability reopen tests GREEN against a live postgres:16 via `PQUEUE_PG_TEST_URL` (schema-
  isolated, one connection per scenario); LOUD `eprintln!` skip when the env var is absent so the default
  workspace stays green + visibly partial. **I1 blocking-executor caveat recorded** (the sync client PANICS
  under an ambient tokio runtime → tests use `futures::block_on`; not yet server-wired; spawn_blocking+pool+
  row-locking is post-launch). Fresh-eyes review: no BLOCKING; recorded the post-pooling MAX(seq)/high-water
  serialization caveat in the crate docs. Owed-A RESOLVED. **CI note (M2 / DoD):** the live run is
  in-session; the PHASE-7 "conformance on …+postgres" gate is **PASS (live), CI-job owed** — a CI service-
  container job exporting `PQUEUE_PG_TEST_URL` (container `postgres:16`; `cargo test -p pqueue-postgres`)
  is still owed and tracked here.
  **Chunk 5 (this commit):** graceful connection drain — `pqueue-resp` gained `serve_with_shutdown(…,
  CancellationToken)` owning the per-connection handlers in a `tokio::task::JoinSet`; on cancel it stops
  accepting and each `handle_conn` observes the token between commands (finishing any in-flight command),
  then drains. `Server::shutdown()` stays SYNC (token + abort; Drop-safe, I4); new async
  `shutdown_and_drain(self, timeout)` awaits the serve task and past the bound aborts it — JoinSet
  ownership makes that a HARD bound (drop-aborts stragglers). Fresh-eyes review caught a BLOCKING bug in
  the first cut (`TaskTracker` does NOT abort on drop → unbounded leak); fixed by switching to `JoinSet`;
  re-review confirmed resolved. Test proves an open connection drains + the server then stops accepting.
  Owed-D RESOLVED. Full workspace green, clippy 0.
  Remaining: Chunk 6a/b/c RESP polish, 7 facade verbs+docs, 8 (B') retry-exhaustion.
- 2026-06-23 PUSHPORT (append must be a validated port, not raw Backend::write): added `PushPort` +
  `PushSpec` to the engine, implemented by memory/sqlite/objectlog. The backend assigns item ids from its
  OWN command sequence (`cmd_seq`, restored past the max on rebuild_all) so ids are unique across handles
  AND restart; commits through `commit_locked` which fetches the shard projection first (NotFound if
  absent) BEFORE the durable append, so a Push can never leave the log ahead of the projection. Rationale:
  the library facade (and any future caller) must NOT reach for `Backend::write` with a hand-built
  envelope + caller-side id counter — that bypasses projection-level pre-validation and collides ids. The
  facade now routes `push`/`push_batch` here. (Surfaced by facade fresh-eyes B1/B2/M4.) Note: the RESP
  XADD path still server-generates ids via its own counter — same latent multi-server-on-one-backend
  collision; tracked for the composition-root chunk (a single server per backend avoids it today).
- 2026-06-23 ATTEMPT-COUNT ON RECLAIM — TD-006 RECONCILIATION OWED (Phase 7): the projection charges an
  attempt on BOTH the reclaim (`LeaseExpired` +1) and the re-delivery (`Claim` +1), so a reclaim+redeliver
  cycle bumps `attempt_count` by 2 (e.g. claim→1, expire/reclaim→2, redeliver→3). This is the model
  conformance `tick_reclaims_expired_lease...` already encodes. TD-006:129 says XCLAIM/XAUTOCLAIM "charges
  ONE attempt." DECISION: keep the current model for now (changing it ripples through the projection +
  all backends + conformance), document the divergence at every surface (RESP XAUTOCLAIM doc + exact-count
  e2e), and RECONCILE in Phase 7 — either amend TD-006 to "reclaim and redelivery each charge" or add a
  combined reclaim-relelease engine op that charges once. Not an autonomous unilateral change to the
  shared attempt semantics. (Surfaced by RESP part-2 fresh-eyes B1.)
- 2026-06-23 EVENTUAL-APPLY CLASS + CONFORMANCE VARIANT: the upsert ban (Invariant 2) is enforced at TWO
  doors on an eventual-apply backend — the `replace_if_pending` port (returns Unavailable) AND the durable
  write chokepoint (`append_object` refuses a `ReplacePending` command before writing, so a raw command via
  `Backend::write` can't sneak past). The shared conformance suite gained an EVENTUAL-APPLY VARIANT
  (`eventual_apply_suite!`): the atomic suite MINUS the 3 UpsertPort scenarios and the raw-ReplacePending
  scenario, PLUS `upsert_is_unavailable`. Filesystem-object-log recovery: rebuild replays the objects
  (recomputes state); a torn TRAILING object is treated as uncommitted (skipped) while a torn non-final
  object is real corruption (errors) — the eventual-apply class's non-atomic write boundary made explicit.
- 2026-06-23 POSTGRES DEFERRED: with memory+sqlite (atomic) and objectlog (eventual-apply) all green,
  postgres is an atomic-class near-clone of sqlite gated only on a live PostgreSQL server (not running in
  this environment; brew binaries + docker are present). Deferred as a fast-follow rather than block the
  loop or ship a half-tested adapter; do it via the durable-adapter template + an env-gated conformance
  test (`#[ignore]`/env-var) when a DB is provisioned. Advanced to Phase 4 (RESP) — the headline gate,
  backend-agnostic, needs no DB. (User can reprioritize postgres at any time.)
- 2026-06-23 DURABLE-ADAPTER TEMPLATE (established by sqlite; postgres/objectlog follow it): `Inner` =
  { durable handle, `HashMap<ShardKey, ProjectionData>` (in-mem materialization), `HashMap<QueueKey,
  QueueDefinition>`, `cmd_seq` } behind one `Mutex`. Durable-first commit: pre-validate via the
  projection decision helpers → durable txn (log row + high_water) → in-mem `apply_command` (panic if it
  errs post-commit; that's a pre-validation bug, not a caller error). Next seq = `MAX(seq)+1`
  (compaction-safe), NOT a row count. `rebuild_all` on open replays the durable log per queue to
  reconstruct the projection AND restores `cmd_seq` past the max minted id (no command_id collision after
  restart). All serialize/query failures → `EngineError::Storage` (never panic/swallow). Snapshots +
  high_water are real durable rows. create_queue is control-plane (writes the queue row + empty
  projection; not a log entry) — the log is replay-complete only together with the queues table.
- 2026-06-23 DURABLE-ADAPTER PERSISTENCE SEAM (from projection-extraction fresh-eyes review, 2 IMPORTANT
  forward conditions): (a) the free `pqueue_projection::commit` and `LogData` are a MEMORY-only
  convenience — their atomicity boundary is the `Mutex`. A durable adapter's boundary is a DB txn, so it
  does NOT reuse `commit`; the reusable units are `ProjectionData::apply_command` + the decision helpers
  (`finalize_validate`/`item_state`/`eligible_candidates`/`expired_leases`). (b) `apply_command` mutates
  the whole projection in place and surfaces no per-row delta. RESOLUTION for the FIRST durable adapter
  (sqlite): persist the **LOG** durably (real append-only rows + high_water + queue defs + snapshots) and
  keep the **projection in-memory, rebuilt from the log on open** (CQRS: the projection is a
  materialization of the durable log; TD-004). This reuses `apply_command` fully, needs NO ProjectionData
  serialization, sidesteps the delta problem, and is genuinely durable (the log is the source of truth).
  A relational sqlite-projection with incremental row deltas (queryable, scale-sized) is a later
  refinement — tracked, not v1. MINOR (accepted): `LogData::read_from/set_high_water/write_snapshot` take
  a `&ShardKey` the caller must pass correctly (convention, not type-enforced); `apply_command` is `pub`
  and pre-validation is a documented caller contract across the crate boundary (each adapter must honor
  it — the memory wrapper does).
- 2026-06-23 ADAPTER STRATEGY — rewrite-in-place: the existing `pqueue-sqlite`/`pqueue-postgres`/
  `pqueue-objectlog` crates implement the OLD storage traits (their own backend/control_plane/log
  modules), consumed by the now-being-demolished `pqueue-service`. Rather than add parallel new adapter
  crates, REWRITE each crate's `lib.rs`/modules in place to implement the ENGINE ports. Rationale: the
  old trait surface is dead the moment the engine is the only consumer (service/client/kafka are deleted
  in Phase 6); a parallel crate would just be a second thing to delete. Keep the crate names/slots. Each
  rewrite lands with the shared `pqueue-conformance` suite green as its no-stub gate. (Not a user-
  blocking decision — the plan already says Phase 3 rewrites these to the engine ports.)
- 2026-06-23 OPERATOR-OP STORE — PLAN DEVIATION (B1, fresh-eyes BLOCKING): the plan said "reuse
  QueueIdempotencyCache for the replay→409 anchor." `OperatorOperationStore` does NOT — it owns its own
  PERMANENT `request_id→operation_id` index (fingerprint stored on the record). Reason: API-002 row 206
  makes replay→same-operation_id UNCONDITIONAL (not scoped to a retention window); the service deduped
  forever via a deterministic operation_id. The idempotency cache is the API-001 *synchronous* primitive
  whose `Expired` decision means "treat as new" — for a destructive operator op (purge/redrive) that
  would re-execute the mutation under a fresh operation_id after `request_id_retention_ms`. Permanent
  dedup is the faithful + safe behavior. INVARIANT: `by_request` values are always keys in `operations`
  (both written together); future bounded operation-history retention MUST drop a record + its
  by_request entry together. CORRECTION: `cancel` leaves terminal states (Succeeded/Failed/Canceled)
  intact (idempotent), vs the service flipping ANY op to Canceled unconditionally (API-002 cancel only
  "stops scheduling further per-shard work"). DEFERRALS: operation-history retention is unbounded for
  now (service parity); per-(tenant,queue,shard) scoping is structural — the wiring site must never
  share a store across scopes (gets a one-store-per-scope test then).
- 2026-06-23 PURGE CORRECTION + DEFERRAL: `validate_purge_force` applies the real API-001 rule
  (leased item + !force → Conflict; non-leased purges freely), CORRECTING the HTTP service's
  pre-storage validator which conflicted on !force unconditionally (it had no item state). The
  transitional service wrapper still passes `item_is_leased=true` to preserve its old conservative
  response shape. DEFERRAL: when a real PurgePort is wired, `item_is_leased` for the purge-force gate
  MUST be read in the SAME transaction as the purge mutation (mirror FinalizePort pre-commit fencing)
  so a stale leased flag can't defeat the gate. (Review IMPORTANT conditions.)
- 2026-06-23 INVARIANT (commit_locked has no rollback): `MemoryBackend::commit_locked` appends the log
  entry BEFORE applying to the projection, with no rollback. Therefore EVERY orchestration caller MUST
  fully pre-validate the command so `apply_command` is infallible for it (else log/projection diverge).
  claim selects Pending candidates; upsert checks state; reclaim selects Leased; finalize now
  pre-checks Leased+!fenced. Future durable units must keep this discipline. (Surfaced by review B1.)
- 2026-06-23 DEFERRALS (tracked): (a) per-item finalize results — `FinalizePort` is all-or-nothing for
  this slice (one rejected item fails the batch); API-001 per-item results are a later refinement.
  (b) lease-token / PEL-ownership fencing — finalize validates operator-fencing only; token/PEL
  ownership (TD-006 §5.3) deferred (any holder can finalize a leased item until then). Both honestly
  marked in port + RESP docs.
- 2026-06-23 Phase 2 deviation (recorded): pqueue-service is kept COMPILING during demolition via
  logic-free re-exports of the canonical engine types + a transitional `From<EngineError>` error
  shim, rather than left broken, until its REST handlers (which consume them) are deleted as their
  own units. The migrated LOGIC lives SOLELY in the engine — no duplication. Strict "delete-in-the-
  same-step" is relaxed because deleting ~18 handler call sites piecemeal would leave service
  non-compiling for all of Phase 2 (and the harness auto-repairs it). The service's substring-based
  error From-shim is transitional dead code in the EXCLUDED crate; SURVIVING adapters (RESP/library)
  map `Forbidden`→`-NOPERM` uniformly with NO string dispatch. Durable §4a units
  (idempotency/fence/pause) MUST use STRUCTURED engine errors, not string-sniffing (review B2).
- 2026-06-23: Plan v4 converged (3 review rounds, GO). Single-shard launch; ReclaimDriver; UpsertPort; semantic-fidelity RESP (Inv 1&2); zero required PQ*; -ERR pqueue {stale_lease,superseded,unavailable}.

## Review ledger (append per chunk)
- 2026-06-24 Phase 7 reconciliation report: adversarial audit of the report against the actual tree →
  verdict **ACCURATE-WITH-CORRECTIONS** (report overwhelmingly honest; every DONE spot-check held, every
  owed item real, full workspace genuinely green). One BLOCKING overclaim fixed by MAKING IT TRUE: the
  shared conformance suite did not cover peek/pending/SnapshotStore (they were adapter/facade-tested,
  snapshot sqlite-only) → added 3 cross-adapter scenarios (+3 on memory/sqlite/objectlog). Also added the
  previously-missing DoD dependency-direction test. Softened the dep-direction phrasing (static manifest
  scan, not build-graph). Gate: full default workspace 39 suites + 0 fails + clippy 0. **MIGRATION DONE.**
- 2026-06-24 Phase 6 delete-legacy: documented self-review (mechanical deletion; correctness proven by
  the green full workspace). Pre-deletion verified the legacy set is CLOSED (only legacy crates referenced
  legacy crates; no new-arch crate path-deps any of them). Deleted 5 crates + leftover target dirs;
  rewrote workspace members (10 hexagonal crates, no default-members exclusion). Post: `cargo build`/`test`
  the FULL default workspace = 38 suites green + 0 fails + clippy 0 + `cargo metadata` resolves — nothing
  live depended on the deleted code; no lingering source/config references. Docs: ADR-005 superseded,
  TD-002 deferred-note. The "real deletion → adversarial review" bar is met by the green full-workspace +
  zero-reference evidence (stronger than a code-reading review for a pure deletion).
- 2026-06-23 Phase 5 composition root (`pqueue-server`): fresh-eyes **GO-with-conditions**, 1 BLOCKING.
  Reviewer confirmed the reclaim-loop core is correct (tick idempotent, no lock-across-await — backends
  use `std::future::ready` so the Mutex is released before any await; no claim/reclaim deadlock) and the
  background-reclaim test genuinely proves orphan recovery with zero client traffic. B1 (BLOCKING):
  `start()` constructed the backend internally with NO queue provisioning + RESP has no create-queue
  command → every request `no such queue`, AND start() was untested → FIXED: `Config.queues` provisioned
  in start_with, new end-to-end `start()` test (XADD→XREADGROUP over a stock client). I1+I3 (reclaim
  errors silently swallowed + green health masks a persistently-failing tick) → FIXED: `ReclaimStats`
  counters (ticks/errors/leases_reclaimed) surfaced via `Server::reclaim_stats()`, error arm counts not
  drops; is_running() doc re-scoped to liveness. M2 (timing-coupled test) → poll-with-timeout. I2 (abort
  leaves in-flight connection handlers; not a graceful drain — they live in `serve`) + M1 (bind err →
  Storage category overload) documented + tracked for follow-up. Gate: server 3 + workspace 25 + clippy 0.
- 2026-06-23 Phase 5 library facade (`pqueue`): fresh-eyes **GO-with-conditions**, 2 BLOCKING + cleared.
  B1 (push via `Backend::write` relied on a cross-crate lockstep for divergence-safety; the "infallible"
  comment was wrong — `ProjectionWriter::apply` errors AFTER append if the shard projection is absent,
  durable on objectlog) + B2 (facade-generated ids from a per-handle AtomicU64 → two handles / a restart
  collide and `HashMap::insert` silently overwrites) + M4 (facade-built envelope → command_id collisions,
  checksum always 0). ALL fixed by ONE change per the reviewer: a new `PushPort` (engine + all 3
  backends) that assigns ids from the backend's own cmd_seq (restart-safe via rebuild_all's cmd_seq
  restore) and commits through commit_locked (shard-exists pre-check → no log-ahead-of-projection). Two-
  handle-no-collision test directly proves B2. I1 (backend() escape hatch leaked the write seam) removed;
  I3 (fail verb) added; I2 (renew/rearm need a RenewLeasePort) documented as deferred; I4 (synthetic
  client_item_key) → defaults to the item id. Dependency direction clean (depends on engine+core only).
  Gate: pqueue 6 tests + full workspace 22 suites + clippy 0; backends' conformance unchanged (PushPort
  is additive).
- 2026-06-23 Phase 4 RESP part-2 (clock + reclaim): fresh-eyes **GO-with-conditions**. Time arithmetic
  (`add_millis` i128 nanos, `ts_ms`, XPENDING idle), clock injection (Send+Sync), half-open boundary
  inheritance, error mapping, and crash-recovery durability all confirmed SOUND (reviewer verified the
  reopen test is genuine: session-2 SqliteBackend rebuilds from the durable log, no in-mem leak). B1
  (BLOCKING: XAUTOCLAIM attempt double-count vs TD-006:129 "one attempt") — diagnosed as the PROJECTION's
  established attempt model (reclaim +1, redeliver +1; conformance `tick_reclaims` already encodes it),
  surfaced not introduced; resolved per reviewer's "doc+code agree, exact-count test": documented the
  divergence in the XAUTOCLAIM doc + asserted exact attempt_count==3 + TRACKED for Phase-7 contract
  reconciliation (not unilaterally changing the shared attempt model). I1 (global tick) — documented as
  correct (any expired lease is reclaimable; redelivery shard-scoped). I2 (min-idle ignored) — documented
  + non-zero-value test. I3 (half-open assertion) — strengthened (XPENDING attempt==1 at t==expiry).
  M1 cleared (QueueDefinitionConflict token). I4 cleared (collision e2e). Gate: resp 1+8 + workspace 19.
- 2026-06-23 Phase 4 RESP part-1 (XADD-upsert + XPENDING + e2e): fresh-eyes **GO-with-conditions**, no
  blocking. Wire framing (RESP2 bulk/array/null), upsert-over-XADD contract, and structured-error mapping
  (no string-sniffing) all confirmed correct. I1 (IMPORTANT, real shared-core bug): superseded-id XACK
  returned `Invalid("item is not leased")` instead of `Superseded` because `finalize_validate` lacked a
  superseded branch before the not-leased catch-all → FIXED in `pqueue-projection` + new wire e2e
  `xack_of_superseded_id_is_superseded_over_the_wire`; no regression (19 suites). I2/I3 (XPENDING fidelity)
  FIXED: numeric `{n}` id ordering (not lexical, which mis-orders past 10 items), honor the extended-form
  `count`, consumer axis = lease token consistently in both forms. TRACKED for part-2: I4 (collision e2e
  cases), M1 (QueueDefinitionConflict mapping, unreachable over RESP), M5 (idle-ms=0 blocking for
  XAUTOCLAIM). Gate: resp 1 unit + 5 e2e + full workspace 19 suites + clippy 0.
- 2026-06-23 Phase 3 objectlog adapter (eventual-apply, fs object store): fresh-eyes **GO-with-conditions**,
  no blocking. 2 IMPORTANT cleared + tested: I-1 (upsert ban was port-only — a raw ReplacePending via
  Backend::write would durably apply; fixed by refusing ReplacePending at the single durable chokepoint
  `append_object` → Unavailable, before any object write; white-box test added). I-2 (a torn TRAILING log
  object made `open()` fail hard; fixed — `read_envelopes` skips an unparseable highest-seq object as
  uncommitted but still errors on a torn non-final object = real corruption; truncate-last-object test).
  M-2 (snapshot ref-id `count()`→`max+1`, compaction-safe). Reviewer confirmed: high-water recovery is
  correct-by-recompute (rebuild_all replays the log, never trusts a stale high_water.json — that file is
  for the `high_water()` port read only); next_seq/borrow-split/class-honesty/temp-hygiene all clean;
  cmd_seq restored on reopen (parses `obj-N`). Template invariants (B1/B2/I1 from sqlite) carried over.
  Gate: 13 eventual-apply conformance + 4 durability + clippy 0; full workspace 19 suites green.
- 2026-06-23 Phase 3 sqlite adapter: fresh-eyes review **GO-with-conditions**; 2 BLOCKING + 2 IMPORTANT,
  ALL CLEARED + tested. B1 (command_id collision across restart — cmd_seq was reset to 0 on reopen):
  `rebuild_all` now restores cmd_seq past the max replayed `sql-N`; regression test
  `orchestration_writes_after_reopen_do_not_collide`. B2 (post-commit apply Err = silent durable/in-mem
  divergence): `commit_locked` now `.expect()`s the post-durable-append apply (loud rebuild signal, not a
  caller-visible Err indistinguishable from a clean pre-commit reject). I1 (`COUNT(*)` next-seq breaks
  under compaction): switched to `MAX(seq)+1`. I4/M1 (serialize `expect` panic + read `.ok()` swallow):
  `to_json`→Storage and `opt()`→Storage helpers. Accepted MINORs (not today-bugs, consistent w/ memory &
  documented scope): I2 (set_high_water read-then-write atomic only under the single Mutex — fine until a
  conn pool), I3 (create_queue is control-plane, not in the log — replay-complete only with the `queues`
  table, same on all backends), M2 (`Backend::write` two-writer seam weaker than commit_locked, safe by
  caller convention — only the test helper uses it), M3 (`current_epoch`=0, single-epoch pre-launch).
  Added snapshot round-trip test. Gate: 16 conformance + 4 durability + clippy 0; full workspace green.
- 2026-06-23 Phase 3 durable-log serde enabler: documented self-review (derive-only + round-trip test;
  no behavior change). Added serde to the engine command/log tree + core PriorityValue + bytes serde
  feature. Round-trip test exercises all 12 QueueCommand variants + payload Bytes + Int64 priority via
  JSON-string equality. Note: DecimalValue.mantissa is i128 — not exercised by conformance (Int64
  priorities); serde_json supports i128 natively. Gate: 24 test-suites green, clippy 0.
- 2026-06-23 Phase 3 projection-core extraction (`pqueue-projection`): fresh-eyes review verdict **GO**,
  NO blocking. Reviewer diffed every moved block against `HEAD:pqueue-memory/src/lib.rs` and confirmed
  apply_command/transition/elig_key/insert_pending/metrics/peek/expired_leases/read_from/set_high_water
  byte-identical; claim `paused→empty` and select_eligible consolidation result-equivalent (improvement,
  no drift); commit INVARIANT preserved on all 5 reachable command paths (Finalize pre-validates via
  finalize_validate before commit; no append-then-apply-fail path). 2 IMPORTANT findings are FORWARD
  conditions for the sqlite chunk (commit/LogData are memory-only; apply_command surfaces no delta) →
  resolved by the durable-seam decision (log-durable + projection-rebuilt-from-log). White-box tests
  correctly relocated. Gate: engine 41 + memory 17 + projection 2 + conformance + clippy 0 + doc 0.
- 2026-06-23 Phase 3 conformance-harness extraction: documented self-review (structural test-harness
  refactor; 16 scenarios are verbatim moves of already-reviewed tests, behavior-preservation proven by
  memory still running all 19 green at the same count). Verified: dependency direction (conformance →
  core/engine only; adapters → conformance as dev-dep; no cycle); `ConformanceBackend` umbrella + blanket
  impl is the standard pattern (any port-complete backend qualifies free); macro yields per-scenario
  `#[tokio::test]` granularity; white-box tests correctly retained in-crate (need private `b.state`).
  No stubs/shims; structured-error assertions preserved. engine 41 + memory 19 + conformance + clippy 0.
- 2026-06-23 Phase 2 §4a active-scope rollup (sub B): documented self-review (small pure module; rollup
  arithmetic + branching directly diffable against the service original). Parity confirmed line-by-line:
  `roll_up_queue_scopes` identical; `sum_optional` identical except `saturating_add` (overflow→saturate,
  a documented safety refinement, no realistic behavior change); granularity default/validation
  reproduces the service's split presence test (default keys off `Some(_)`, validation off non-empty) so
  empty queue_id nets to Invalid — now explicitly tested. Took `Option<&str>` (not a bool) to encapsulate
  both presence notions honestly. Out of scope (adapter): filter/sort/truncate/as_of/tenant stamping.
  Engine 41 tests + clippy green. §4a COMPLETE.
- 2026-06-23 Phase 2 §4a operator-operation store (sub A): fresh-eyes review returned GO-with-conditions
  with ONE BLOCKING (B1): the first cut reused QueueIdempotencyCache, importing its retention-windowed
  `Expired→new-operation` semantics — after `request_id_retention_ms` a retried destructive operator op
  would mint a new operation_id and re-execute (service deduped permanently/deterministically). FIXED by
  redesign: store owns a permanent `request_id→operation_id` index + fingerprint-on-record (the service's
  `existing_operator_operation` logic), no clock. This also resolved I1 (single lifetime, no anchor/ops
  divergence) and M4 (rebuild test now asserts full `live==rebuilt`). Applied I2 (NOT-YET-WIRED banner +
  advance monotonicity doc) and I3 (check→record flow test). Cancel correction (M1) + structural scoping
  (M2) + unbounded ops (M3) confirmed safe/deferred. Re-reviewed via documented self-review (targeted fix
  over reviewed code). Engine 34 tests + clippy green.
- 2026-06-23 Phase 2 §4a finalize/rearm/purge validation: fresh-eyes parity review GO-with-conditions,
  NO blocking. Verified: finalize-targeting + rearm parity exact (incl. seconds-only past-until
  boundary, at-until=ok); structured errors (Invalid/Terminal/Conflict) correct; purge factored into
  pure targeting + state-dependent force gate. The purge-force CORRECTION (leased+!force→Conflict vs
  service unconditional) confirmed correct per API-001 + safe (a loosening; no existing consumer).
  Conditions applied: added 2 tests (finalize independent target/lease families; rearm
  missing-not_before-wins-over-until) + tracking note (real PurgePort must read item_is_leased in the
  same transaction). Service compatibility wrappers delegate to engine validation and keep historical
  conservative purge behavior. Engine 25 tests + clippy green; service route tests green.
- 2026-06-23 Phase 2 §4a claim-compat validation: fresh-eyes parity review GO-with-conditions, NO
  blocking. Verified rule-by-rule parity vs the original service `validate_claim_compatibility`
  (charset, all combination rejections, capability checks, `>` not `>=` BatchTooLarge boundary, the
  cohort completion≤progress bound). Confirmed: the group_key charset re-check is present + correct
  (GroupKey newtype only validates non-empty); BatchTooLarge is a distinct structured variant with
  the right wire token; dropping the service's unreachable "requires progress_bound_ms" branch is
  sound (QueueDefinition.progress_bound_ms is non-Option, domain-validated >0). Condition applied:
  added 4 missing branch tests (wc-not-coresident, wc-missing-bound, wc-combination-reject, valid
  group_key happy path) + a code comment on the intentional omission. Engine 19 tests + clippy green.
- 2026-06-23 Phase 2 §4a command_position: self-review only (tests + 1 doc line, NO production logic
  change — high-water write paths were already validated in prior reviewed units). Added 3 conformance
  tests proving TD-007 §4 durable properties: high-water advances on each commit; item_version is
  monotonic per item (push=1→claim=2→renew=3→finalize=4); high-water survives a simulated log
  compaction (entries cleared → high-water unchanged at seq 2, proving it is stored not recomputed).
  Verified non-tautological. Memory 19 tests + clippy green.
- 2026-06-23 Phase 2 §4a pause/fence: fresh-eyes review GO-with-conditions (one BLOCKING). Confirmed:
  fence check is pre-append + same-lock (no TOCTOU); pause gates claim+select_eligible; fenced XACK →
  `-ERR pqueue stale_lease` (not 0); all-or-nothing honestly marked. Fixes applied — (B1) `finalize`
  now pre-validates each item is Leased+!fenced so apply is infallible (commit_locked has no
  rollback); added a test that a rejected finalize appends NO log command; (I1) added a log-replay
  reconstruction test proving pause+fence survive a rebuild (TD-007 §4); gated `peek` by pause; (I2)
  tightened FinalizePort doc (token/PEL ownership deferred). Deferrals recorded in decisions log.
  Engine 12 + memory 16 + resp 1+1 green, clippy clean.
- 2026-06-23 Phase 2 §4a idempotency: fresh-eyes review **NO-GO** → fixed to convergence. The review
  caught 4 real issues, all addressed: (B1) collapsing request-id-conflict onto generic `Conflict` →
  added distinct `EngineError::RequestIdConflict` + `RequestExpired` with their own `-ERR pqueue …`
  tokens (and resp_token tests); (I1) "expired→Proceed" erased API-001 `request-expired` → added an
  `Expired` decision variant the caller maps per-op (push→Proceed, claim→RequestExpired); (I2)
  dishonest key scope → renamed `QueueIdempotencyCache` with a "one instance per (tenant,queue,shard)"
  invariant docstring; (I3) tautological reconstruction test → replaced with a check-then-record flow
  (proves a live request_id is never overwritten) + a retained-window rebuild test (compacted cache ==
  replay of only retained entries). Also added not-yet-wired note + relationship to
  `pqueue_core::check_idempotency`. Engine 12 tests + clippy green. (NOT yet wired into push/claim/
  finalize or ReclaimDriver compaction — tracked.)
- 2026-06-23 Phase 2 §4a auth: fresh-eyes review GO-with-conditions. Confirmed: hash+redaction byte-
  parity, tenant set-membership + operator-prefix rules exact, durability classification correct,
  workspace default-members topology correct/non-orphaning. Fixes applied — (B3, REAL BUG) RESP
  `err_reply` now maps `Forbidden`→`-NOPERM` and `NotFound`→`-ERR no such queue` instead of a generic
  `-ERR pqueue error`, with a unit test; (I1) added `Forbidden.resp_token()==None` assertion.
  Recorded deviation for B1/B2 (service delegation shim — see decisions log; logic lives only in
  engine, durable units will use structured errors). Engine 6 + RESP lib 1 + e2e 1 green, clippy clean.
- 2026-06-23 Phase 1d (minimal RESP smoke front): implemented `pqueue-resp` as a driving adapter over
  engine ports with no concrete backend dependency. The e2e uses the off-the-shelf `redis` crate over
  real TCP to `XADD` 10 mixed-priority items, drain them via `XREADGROUP GROUP ... STREAMS ... >`,
  `XACK` each batch, and reconcile exact delivered set plus ascending priority order. Confirmed no
  silent stubs for unsupported commands (`-ERR`). Deferred by design: auth, idempotency, pending
  history/replay, `XAUTOCLAIM`, full Redis flavor matrix, and composition-root server packaging.
  `cargo test -p pqueue-resp`; `cargo clippy -p pqueue-resp --all-targets -- -D warnings`; and
  `cargo test -p pqueue-engine -p pqueue-memory -p pqueue-resp` green after formatting.
  Fresh-eyes review: GO-with-conditions, NO blocking; codec binary-safe, error encoding single-dash
  canonical, hexagonal dep direction confirmed. Conditions applied — marked the 3 silent deferrals
  (XACK lease/PEL validation + requested-not-acked count; stub clock; XGROUP/HELLO no-op OKs), dropped
  dead `prefix` param, and HARDENED the e2e: duplicate-priority tie-break assert + per-round
  non-overlapping-band assert (within-batch-only sorting would now fail). **PHASE 1 COMPLETE** —
  16 tests green, clippy clean, hexagonal direction respected.
- 2026-06-23 Phase 1c (claim/upsert/reclaim): fresh-eyes review GO-with-conditions. Confirmed sound:
  single-Mutex atomicity (no TOCTOU, no double-claim), Invariant 1 (priority claim, exactly-leased,
  single version+attempt bump, rich shape, no orphan), Invariant 2 (pending→Replaced /Leased→Invalid
  /terminal→Terminal/insert, superseded guarded), reclaim (idempotent, zero-traffic, attempt charged).
  Fixes applied — (B1) added exp==now half-open boundary test; (I2) widened `replace_if_pending` with
  group_key+not_before (avoids later breaking change; co-resident upsert no longer strips group);
  (I3) store max_attempts in ItemRecord (ready for retry-exhaustion, not dropped); (M1) debug_assert
  every leased candidate renders. Deferred (tracked): cohort-timeout + progress-bound tick metering
  (need cohort-deadline/eligible_since state — land with those features); retry-exhaustion wiring.
  12 tests + clippy green.
- 2026-06-23 Phase 1b (pqueue-memory): fresh-eyes review GO-with-conditions. Confirmed correct:
  priority ordering (both directions), transition() eligibility re-add on retry (Invariant 1, no
  orphan/dup), version/attempt semantics, disjoint-borrow UoW. Fixes applied — (1) transition()
  rejects superseded items (`-ERR pqueue superseded`, prevents state corruption); (2) select_eligible
  /peek O(n²)→O(1) via items HashMap (EligKey.item String→ItemId); (3) added `group_key` to engine
  `PushItem`, populated it, so `CohortExpired` is functional (was a dead no-op). Open: shard
  cross-routing assertion in apply (deferred, single-shard launch); ReplacePending-of-claimed gating
  is UpsertPort's job (1c). 5 tests + clippy green.
- 2026-06-23 Phase 1a (engine ports): fresh-eyes review GO-with-conditions. Fixes applied before
  advancing — added `SnapshotStore` (persisted command_position high-water, TD-007 §4) and
  `ControlPlaneStore` (queue defs + epoch source) ports; widened `ClaimedItem` with
  group_key/not_before/attempt_count; `CohortExpiredCommand.group_key` → core `GroupKey`. Noted:
  CommandPosition now epoch-first ordered (fixes old derived-Ord bug); memory backend keeps log/proj
  as disjoint fields (M2). Build+3 tests+clippy green.
- 2026-06-23 Phase 0 doc convergence: GO-with-conditions. Fixes applied — (1) operator-op store
  deferred to Phase 2 w/ full API-002 async shape referenced; (2) upsert collision mapping pinned:
  claimed→`-ERR pqueue invalid`, terminal→`-ERR pqueue terminal` (TD-007 §2.3 + TD-006 §3);
  (3) D2→progress_bound meter-only at launch; (4) command_position high-water persisted in
  snapshot (replay monotonic under compaction); (5) cohort deadline defined per API-001. ADR-007
  clean as-is. Review-hash stamping left to ddx tooling.
