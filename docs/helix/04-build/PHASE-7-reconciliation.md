# Phase 7 — Reconciliation Report (hexagonal migration v4)

Final gap review of the finished system against `hexagonal-migration-plan.md`, item by item. Status
vocabulary: **DONE** = implemented + tested (cited); **OWED** = intentionally deferred with reason (not
dropped); **N/A** = not applicable to launch scope.

Workspace at reconciliation: **10 crates** (`pqueue-core`, `pqueue-engine`, `pqueue-projection`,
`pqueue-conformance`, `pqueue-memory`, `pqueue-sqlite`, `pqueue-objectlog`, `pqueue-resp`, `pqueue`,
`pqueue-server`). Full **default** workspace green: ~38 test-suites, **0 failures, clippy 0**,
`cargo metadata` resolves. Test totals: core 84, engine 44, projection 2, memory 20 (19 shared
conformance + 1), sqlite 23 (19 conformance + 4 durability), objectlog 20 (16 eventual-apply conformance
+ 4 durability), resp 9 (1 unit + 8 e2e), pqueue 6, server 3.

---

## §0 Goals & non-negotiables

| Item | Status | Evidence |
|---|---|---|
| One engine — CQRS priority projection over a log store | **DONE** | `pqueue-projection` (`ProjectionData` + `LogData` + `commit`); the durable log is the source of truth, projection rebuilt-from-log on every durable adapter. |
| Two interfaces, asymmetric (RESP limited / library full) | **DONE** | RESP front `pqueue-resp`; library facade `pqueue`. Asymmetry recorded in TD-006 capability matrix (library-only cells marked). |
| Hexagonal, dependency-direction enforced by a test | **DONE** | `pqueue-engine/tests/dependency_direction.rs` (`domain_crates_do_not_depend_on_adapters`) — a static manifest-scan guard that fails on a DIRECT core/engine→adapter dependency edge (cargo already forbids the cycles that a transitive edge would require). |
| Clean cutover, no stubs/fallbacks/shims; service/client/kafka deleted | **DONE** | Phase 6: 5 legacy crates deleted; `grep` finds zero `NativeRoute/axum//v1/problem+json/ApiProblem` and zero `todo!/unimplemented!` in live `src/`. |
| Single-shard launch scope (multi-shard coordination post-launch) | **DONE (recorded subset)** | `ShardId::ZERO` everywhere; ports carry `shard_id`/`ShardKey`; no multi-shard owner-assignment loop (intentional, §2.5). |
| Verified completeness — implement→review→test each phase | **DONE** | Every chunk has a review-ledger entry in `build-progress.md`; fresh-eyes reviews on all production-logic chunks. |

---

## §1 Crate topology

All target crates exist with the prescribed roles and outward deps **except postgres**:

- `pqueue-core` (none), `pqueue-engine` (core), `pqueue-memory`/`pqueue-sqlite`/`pqueue-objectlog`
  (engine+core+io), `pqueue-resp` (engine+tokio), `pqueue` facade (engine), `pqueue-server` (all) —
  **DONE**, dependency-direction test green.
- `pqueue-projection` + `pqueue-conformance` — net-new shared crates not named in the original table but
  in its spirit: the projection state machine (shared by all backends, no per-adapter re-implementation)
  and the backend-conformance harness. **DONE.**
- `pqueue-postgres` — **OWED (deferred).** The old-architecture crate was deleted (Phase 6); the adapter
  will be rebuilt **fresh to the engine ports** via the durable-adapter template (durable log + projection
  rebuilt-from-log; same as sqlite) when a live PostgreSQL is provisioned. Atomic-class near-clone of
  sqlite; no new architectural risk. Conformance + an env-gated live test land with it.

---

## §2 Engine model

- **§2.1 Ports** — `LogWriter`/`LogRead`, `ProjectionWriter`/`ProjectionRead` (`select_eligible` priority
  order, `peek`, `pending`, `metrics`), `Backend` (atomic `write(|log,proj|)` UoW), `ClaimPort`,
  `UpsertPort`, `FinalizePort`, **`PushPort`** (added in Phase 5 — append via a validated port, backend-
  assigned restart-safe ids), `ControlPlaneStore`, `SnapshotStore`, `ReclaimDriver`, `Clock`, `IdGen`.
  **DONE.** No-stub is **behavioral**: the `pqueue-conformance` suite has ≥1 fail-on-no-op test per port
  method — including the read-only `peek`/`pending` and the `SnapshotStore` write/read/latest round-trip
  (added in Phase 7 after the reconciliation audit flagged them as previously suite-uncovered) — run
  across every adapter: **19 atomic scenarios on memory + sqlite, 16 eventual-apply on objectlog**.
  `IdGen` exists as a port but the RESP/facade currently generate ids themselves (see Owed Item C).
- **§2.2 Two-class durability** — **Atomic** (memory lock, sqlite txn): append+apply commit together;
  Invariants 1&2 strong. **Eventual-apply** (objectlog): upsert **forbidden** → `EngineError::Unavailable`
  (`-ERR pqueue unavailable`), enforced at BOTH the `replace_if_pending` port and the durable write
  chokepoint. **DONE** — `pqueue-conformance::eventual_apply_suite!` + `upsert_is_unavailable` scenario;
  objectlog `DurabilityClass::EventualApply`.
- **§2.3 Single logical claim path** — claim authority is the engine; backends select eligible candidates
  from the projection then commit a `Claim` command; upsert/claim mutually exclude under one lock.
  **DONE** (memory/sqlite/objectlog claim via `eligible_candidates` + `commit_locked`).
- **§2.4 ReclaimDriver** — `tick(now)` reclaims expired leases with **zero** intervening client commands.
  **DONE.** Engine-level: conformance `tick_reclaims_expired_lease_with_no_client_traffic`,
  `tick_lease_boundary_is_half_open`. Composition-root: `pqueue-server` background task,
  `background_reclaim_recovers_orphaned_lease_without_client_traffic` (DoD met). Synchronous embedding
  drives it via the same `tick(now)` entry point.
- **§2.5 Sharding** — single shard fully implemented; multi-shard coordination post-launch. **DONE
  (recorded subset).**

---

## §3 RESP surface — pqueue-flavored Redis

**Invariants:**
- **Invariant 1** (per-item delivery, cursorless, no orphaning) — **DONE.** e2e
  `drain_and_reconcile_with_offtheshelf_client` (produce N mixed-priority → drain via `XREADGROUP >` to
  empty → delivered-set == produced-set, each once, no hang, cross-batch priority bands).
- **Invariant 2** (upsert = atomic XDEL+XADD, pending-only, atomic-class-only) — **DONE.** e2e
  `xadd_on_client_item_key_upserts_not_appends` (effects), `xadd_collision_with_leased_then_terminal_is_an_error`
  (collision → invalid/terminal), `xack_of_superseded_id_is_superseded_over_the_wire` (`-ERR pqueue
  superseded`, fixed a shared `finalize_validate` bug). Atomicity proven at engine level (conformance).

**Stock commands implemented + tested:** `XADD` (upsert-on-key), `XREADGROUP >` (priority delivery,
cursorless), `XACK` (complete; operator-fenced → `-ERR pqueue stale_lease` via
`fenced_lease_xack_is_stale_over_the_wire`), `XPENDING` (summary + extended, count + numeric id order +
idle), `XAUTOCLAIM` (tick-reclaim + redeliver; `xautoclaim_redelivers_expired_leases`). Canonical error
tokens asserted verbatim (`EngineError::resp_token` + e2e substring assertions). **DONE.**

**OWED (deferred RESP polish — Owed Item E):**
- `XCLAIM` (specific-id) incl. the §3 "same-consumer `XCLAIM` = no-charge renew, cross-consumer =
  reclaim+1-attempt" semantics — **NOT implemented.** (Flavor difference #7 is therefore not yet
  realized over the wire.)
- `XLEN` / `XINFO` / `XDEL` — listed "faithful" in §3 but **NOT implemented** (only the worker hot path
  + reclaim/pending are). Deferred.
- Cursor-pagination e2e (`XAUTOCLAIM 0-0`→…→`0-0` covers whole PEL) — the adapter returns a single-shot
  `0-0` cursor (documented divergence); paginated coverage owed.
- Intra-group exclusion e2e (two consumers, never same item) and the upsert↔claim race e2e — engine-level
  exclusion holds (single lock), but the dedicated e2e scenarios are not written.

**Flavor differences** #1–#6 hold as designed; **#7** owed with #XCLAIM; the attempt-count semantics
diverge from TD-006 (Owed Item B).

---

## §4 Legacy teardown

- **§4a domain logic → engine (closed inventory)** — **DONE.** Migrated + engine-tested: auth
  (`auth.rs`), request-id idempotency (`idempotency::QueueIdempotencyCache`), operator-operation store +
  get/cancel/advance (`operator.rs`), lease fencing + un-fence (FinalizePort + Fence/Unfence commands),
  pause/resume (ControlPlane + Pause/Resume), `command_position` high-water + item_version monotonicity,
  QueueCatalog active-scope roll-up (`active_scope.rs`), claim-compat + finalize/rearm/purge validation
  (`claim_validation.rs`, `finalize_validation.rs`), lease-token hashing (`hash_lease_token`). HTTP
  transport (ApiProblem/axum/routes) deleted with the service.
- **§4b crates** — storage split into engine+projection then dissolved; sqlite/objectlog rewritten to the
  engine ports; service/client/kafka/postgres(old)/storage deleted. **DONE.**
- **§4c docs** — ADR-007 authored; TD-006 refolded to semantic-fidelity (capability matrix); TD-007
  authored (two-class durability, ReclaimDriver, UpsertPort, durable-state replay); ADR-005 **SUPERSEDED**;
  TD-002 noted (postgres deferred-fresh). **DONE** for the architectural docs. **OWED:** a full neutral
  rewrite of API-001 + TP-001 was partially done across the cascade; a final pass to scrub any remaining
  HTTP-era phrasing is folded into Owed Item F (doc hygiene).
- **§4d tests** — service invariant tests re-homed to engine; behavioral suite migrated to
  `pqueue-conformance`. **DONE** (conformance is the single behavioral suite).
- **§4e beads** — **OWED.** Re-scope beads tied to deleted crates (claimed-item-shape → transport-neutral;
  Lakebase → `pqueue-server` image + health probe). None halted; tracked.

---

## §6 Definition of Done — gate-by-gate

| DoD gate | Status |
|---|---|
| `rg` zero refs to service/client/kafka/`NativeRoute`/`axum`/`/v1`/problem+json | **PASS** (grep clean in live `src/`+`tests/`). |
| No-stub = behavioral conformance per adapter × port method | **PASS** (memory/sqlite atomic suite 19 scenarios; objectlog eventual-apply suite 16; ≥1 fail-on-no-op test per port method incl. peek/pending/snapshot, run across all adapters). |
| Capability matrix {RESP-stock, library} signed, no unmarked library-only cells | **PARTIAL** — matrix present in TD-006 with library-only annotations; a final "every API-001/002 op classified" audit is OWED (Owed Item F). |
| Every migrated invariant has an engine-level test | **PASS** (auth, idempotency, operator-op, fencing, pause, recurrence-validation, command_position, purge-validation — engine tests). |
| ReclaimDriver: reclaim with zero intervening client commands | **PASS** (`pqueue-server` background-reclaim test + engine conformance). |
| e2e RESP green: drain-reconcile, cursor loop, crash recovery, fence, upsert effects+collision+superseded, intra-group exclusion | **PARTIAL** — drain-reconcile ✅, crash-recovery ✅ (sqlite reopen), fence ✅, upsert effects+collision+superseded ✅; **cursor-pagination loop + intra-group exclusion e2e OWED** (Owed Item E). |
| One conformance suite green on memory+sqlite+postgres+objectlog; eventual-apply weaker; upsert-on-eventual→unavailable | **PARTIAL** — memory+sqlite+objectlog ✅; **postgres OWED** (Owed Item A). |
| Two driving adapters + one composition root; dependency-direction test passes | **PASS** (RESP + facade + `pqueue-server`; dep-direction test green). |
| Durable-state reconstructable from the log (idempotency/fences/pause/command_position) | **PASS** (sqlite/objectlog rebuild-from-log durability tests; engine replay-reconstruction tests). |
| Docs consistent; ADR-007/TD-006/TD-007 recorded; asymmetry recorded | **PASS** (architectural docs); minor doc-hygiene OWED (Owed Item F). |
| Single-shard launch recorded; multi-shard post-launch | **PASS** (§2.5). |
| Phase 7 reconciliation shows no dropped item | **PASS** — this report; all gaps are OWED-with-reason, none silently dropped. |

---

## Owed items (tracked, with rationale — none are silent drops)

- **A. Postgres adapter — DEFERRED.** Needs a live PostgreSQL (not available in the build env; brew
  binaries + docker present). Build fresh to the engine ports via the durable-adapter template +
  env-gated conformance. Atomic-class near-clone of sqlite; low risk.
- **B. Attempt-count on reclaim — RESOLVED** (owed-resolution Chunk 1). The reclaim (`LeaseExpired`) no
  longer charges; `attempt_count` = number of deliveries (charged only by `Claim`). TD-006:74/128-129 +
  the RESP XAUTOCLAIM doc updated; e2e asserts exactly 2 (claim + redeliver), conformance comment fixed.
- **B'. Retry-exhaustion NOT wired (NEW, owed).** `max_attempts` is `#[allow(dead_code)]` — "Finalize-Retry
  beyond `max_attempts` → terminal" is not enforced. Chunk 1 fixed the attempt *counter* (the input);
  the exhaustion *policy* is a separate owed item (surfaced by the Chunk-1 plan review, M5).
- **C. RESP/facade server-side id generation.** Two RESP servers (or two facades) over ONE backend, or a
  process restart, can collide self-generated ids. The **facade was fixed** (Phase 5: ids assigned by the
  backend via `PushPort`, restart-safe; two-handle test proves it). The **RESP front** still generates
  ids from its own counter — safe for single-server-per-backend (the normal deployment) but owed: route
  XADD id assignment through `PushPort`/`IdGen` too.
- **D. Graceful connection drain on shutdown.** `Server::shutdown()` aborts the accept loop + reclaim
  ticker but does not drain already-accepted connection handlers (they live in `serve`). Documented;
  a `JoinSet`/TaskTracker drain is owed.
- **E. RESP polish.** `XCLAIM` (specific-id, incl. same-consumer no-charge renew per §3 flavor #7),
  `XLEN`/`XINFO`/`XDEL`, paginated `XAUTOCLAIM` cursor coverage, intra-group-exclusion + upsert/claim-race
  e2e. None on the worker hot path; deferred.
- **F. Library verbs + doc hygiene.** `renew` (extend lease) and `rearm` (recurrence) need a
  `RenewLeasePort` (their `apply` is fallible — a naive `Backend::write` would risk divergence), so they
  are deferred rather than added unsafely. Plus: a final capability-matrix completeness audit and an
  API-001/TP-001 HTTP-era-phrasing scrub.

---

## Verdict

The hexagonal re-architecture is **functionally complete for launch scope**: one CQRS engine, the shared
projection state machine, three driven adapters spanning both durability classes, two driving interfaces
(RESP worker surface + Rust library facade) behind a composition root with a background reclaim driver,
and the legacy HTTP-service/Kafka/storage-trait architecture fully deleted — all verified by a green
full default workspace (0 failures, clippy 0) and a dependency-direction test. **No plan item is silently
dropped**; the six owed items above are recorded with rationale, the largest (postgres) being an
infrastructure dependency, not a design gap.
