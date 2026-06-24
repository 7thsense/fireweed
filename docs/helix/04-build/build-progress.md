# Build Progress — hexagonal migration

Tracks the in-loop execution of `hexagonal-migration-plan.md` (v4). Each chunk: implement → review →
test/realign → update this file → continue. Update the **Cursor** and the checklist every iteration.

## Cursor
- **Now:** Phase 2 — next §4a unit: **command_position high-water + item_version** (durable, TD-007
  §4). Today: `item_version` is a per-item counter (correct per API-001) and the SnapshotStore has
  `high_water`/`set_high_water` (monotonic) but the memory backend never advances the high-water on
  commit. This unit should: (a) advance the persisted `command_position` high-water in `commit_locked`
  (or apply path) so it tracks the latest committed position; (b) prove it is read from the snapshot,
  not recomputed from a (possibly compacted) log (TD-007 §4) — a test that the high-water survives and
  is monotonic across commits; (c) confirm `item_version` monotonicity per item. Keep it tight.
  **pause/fence unit DONE** (FinalizePort, B1 fix, reconstruction test, reviewed). **idempotency DONE**.
  **auth DONE**.
- **After this unit:** operator-op store (API-002 async model, reuses the idempotency cache) +
  QueueCatalog metrics/scopes; claim/finalize/rearm/purge validation; then Phase 3 driven adapters.

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
- [ ] §4a unit: command_position (item_version source, persisted high-water)
- [ ] §4a unit: operator-operation store (API-002 async model) ; QueueCatalog metrics/scopes
- [ ] §4a unit: claim/finalize/rearm/purge validation
- [ ] Final: delete pqueue-service entirely (Phase 6)

### Phase 3 — driven adapters
- [ ] sqlite, postgres (ClaimPort), objectlog (eventual-apply, upsert banned)
- [ ] Conformance green each: concurrent-claim races, intra-group exclusion, class guarantees

### Phase 4 — full RESP adapter + e2e
- [ ] `pqueue-resp`; full §3 e2e (cursor loop, crash recovery, fence, race) — headline gate

### Phase 5 — library + composition root
- [ ] `pqueue` facade + `pqueue-server` (DI, ReclaimDriver task, ops probe)

### Phase 6 — delete legacy
- [ ] Remove service/client/kafka + tests; dissolve storage; supersede/rewrite docs; re-scope beads

### Phase 7 — final gap review against plan
- [ ] Reconciliation report: every §1–§6 item implemented+tested or descoped-with-reason

## Decisions log (append as resolved)
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
