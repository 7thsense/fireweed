# Build Progress — hexagonal migration

Tracks the in-loop execution of `hexagonal-migration-plan.md` (v4). Each chunk: implement → review →
test/realign → update this file → continue. Update the **Cursor** and the checklist every iteration.

## Cursor
- **Now:** Phase 1d — stand up a minimal `pqueue-resp` RESP/TCP front over `MemoryBackend`
  (XADD→upsert/push, XREADGROUP `>`→claim, XACK→finalize-complete, XAUTOCLAIM→reclaim) and run the
  §3 **drain-and-reconcile** e2e with an **off-the-shelf redis client** (the `redis` crate): produce
  N mixed-priority, drain via XREADGROUP to empty, assert delivered-set == produced-set, priority
  order, no hang. **Phase 1c DONE**: ClaimPort (atomic select+lease, priority, rich ClaimedItem),
  UpsertPort (pending→replace / claimed→Invalid / terminal→Terminal / insert; widened to carry
  group_key+not_before), ReclaimDriver::tick (lease-expiry reclaim with zero client traffic,
  idempotent, half-open boundary); 12 behavioral tests + clippy green; all 4 review conditions applied.
- **After 1d:** Phase 2 (migrate domain logic from pqueue-service, move-and-delete).

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
- [ ] 1d: `pqueue-resp` minimal front + drain-and-reconcile e2e with off-the-shelf redis client

### Phase 2 — migrate domain logic (move-and-delete, test-first)
- [ ] Drop `pqueue-service` from default-members
- [ ] Per §4a unit: engine test → move durable → delete service path (no shim)

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
- 2026-06-23: Plan v4 converged (3 review rounds, GO). Single-shard launch; ReclaimDriver; UpsertPort; semantic-fidelity RESP (Inv 1&2); zero required PQ*; -ERR pqueue {stale_lease,superseded,unavailable}.

## Review ledger (append per chunk)
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
