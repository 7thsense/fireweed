# Phase 7 — Reconciliation Report (hexagonal migration v4)

> **HISTORICAL (June 2026 hexagonal-migration era).** This report and its "zero owed items" verdict
> are accurate **only** for the hexagonal migration that completed 2026-06. The project has since
> cascaded through ADR-008 (queue = unit of sharding) → ADR-009…014 (Fjord change-log surface,
> orthogonal composition, typed indexes) and beyond. **Do not read this as current status.** Current
> status lives in `docs/helix/04-build/DEPLOYMENT-READINESS.md`, `gap-closure-plan.md`, and the live
> `.ddx` tracker.

Final gap review of the finished system against `hexagonal-migration-plan.md`, item by item. Status
vocabulary: **DONE** = implemented + tested (cited); **OWED** = intentionally deferred with reason (not
dropped); **N/A** = not applicable to launch scope.

Workspace at reconciliation: **10 crates** (`fireweed-core`, `fireweed-engine`, `fireweed-projection`,
`fireweed-conformance`, `fireweed-memory`, `fireweed-sqlite`, `fireweed-objectlog`, `fireweed-resp`, `fireweed`,
`fireweed-server`). Full **default** workspace green: ~38 test-suites, **0 failures, clippy 0**,
`cargo metadata` resolves. Test totals: core 84, engine 44, projection 2, memory 20 (19 shared
conformance + 1), sqlite 23 (19 conformance + 4 durability), objectlog 20 (16 eventual-apply conformance
+ 4 durability), resp 9 (1 unit + 8 e2e), fireweed 6, server 3.

---

## §0 Goals & non-negotiables

| Item | Status | Evidence |
|---|---|---|
| One engine — CQRS priority projection over a log store | **DONE** | `fireweed-projection` (`ProjectionData` + `LogData` + `commit`); the durable log is the source of truth, projection rebuilt-from-log on every durable adapter. |
| Two interfaces, asymmetric (RESP limited / library full) | **DONE** | RESP front `fireweed-resp`; library facade `fireweed`. Asymmetry recorded in TD-006 capability matrix (library-only cells marked). |
| Hexagonal, dependency-direction enforced by a test | **DONE** | `fireweed-engine/tests/dependency_direction.rs` (`domain_crates_do_not_depend_on_adapters`) — a static manifest-scan guard that fails on a DIRECT core/engine→adapter dependency edge (cargo already forbids the cycles that a transitive edge would require). |
| Clean cutover, no stubs/fallbacks/shims; service/client/kafka deleted | **DONE** | Phase 6: 5 legacy crates deleted; `grep` finds zero `NativeRoute/axum//v1/problem+json/ApiProblem` and zero `todo!/unimplemented!` in live `src/`. |
| Single-shard launch scope (multi-shard coordination post-launch) | **DONE (recorded subset)** | `ShardId::ZERO` everywhere; ports carry `shard_id`/`ShardKey`; no multi-shard owner-assignment loop (intentional, §2.5). |
| Verified completeness — implement→review→test each phase | **DONE** | Every chunk has a review-ledger entry in `build-progress.md`; fresh-eyes reviews on all production-logic chunks. |

---

## §1 Crate topology

All target crates exist with the prescribed roles and outward deps **except postgres**:

- `fireweed-core` (none), `fireweed-engine` (core), `fireweed-memory`/`fireweed-sqlite`/`fireweed-objectlog`
  (engine+core+io), `fireweed-resp` (engine+tokio), `fireweed` facade (engine), `fireweed-server` (all) —
  **DONE**, dependency-direction test green.
- `fireweed-projection` + `fireweed-conformance` — net-new shared crates not named in the original table but
  in its spirit: the projection state machine (shared by all backends, no per-adapter re-implementation)
  and the backend-conformance harness. **DONE.**
- `fireweed-postgres` — **DONE** (owed-resolution Chunk 4). Rebuilt **fresh to the engine ports** via the
  durable-adapter template (durable log + projection rebuilt-from-log; same as sqlite) over the sync
  `postgres` client; the full conformance suite + a reconnect/durability test run green against a live DB
  (env-gated on `FIREWEED_PG_TEST_URL`, loud-skip otherwise). The blocking-executor caveat (the sync client
  must not be driven from a tokio runtime) is recorded in the crate docs; not yet server-wired.

---

## §2 Engine model

- **§2.1 Ports** — `LogWriter`/`LogRead`, `ProjectionWriter`/`ProjectionRead` (`select_eligible` priority
  order, `peek`, `pending`, `metrics`), `Backend` (atomic `write(|log,proj|)` UoW), `ClaimPort`,
  `UpsertPort`, `FinalizePort`, **`PushPort`** (added in Phase 5 — append via a validated port, backend-
  assigned restart-safe ids), `ControlPlaneStore`, `SnapshotStore`, `ReclaimDriver`, `Clock`, `IdGen`.
  **DONE.** No-stub is **behavioral**: the `fireweed-conformance` suite has ≥1 fail-on-no-op test per port
  method — including the read-only `peek`/`pending` and the `SnapshotStore` write/read/latest round-trip
  (added in Phase 7 after the reconciliation audit flagged them as previously suite-uncovered) — run
  across every adapter; both durability classes exercise the same inherent operation scenarios.
  `IdGen` exists as a port but the RESP/facade currently generate ids themselves (see Owed Item C).
- **§2.2 Two-class durability** — **Atomic** (memory lock, sqlite txn): append+apply commit together.
  **Eventual-apply** (objectlog): the log commit and projection barrier remain distinct, but the profile
  exposes the same inherent operation surface. **DONE** — `fireweed-conformance::eventual_apply_suite!`
  runs the shared upsert and field-mutation scenarios; objectlog remains
  `DurabilityClass::EventualApply` for its visibility and recovery guarantees.
- **§2.3 Single logical claim path** — claim authority is the engine; backends select eligible candidates
  from the projection then commit a `Claim` command; upsert/claim mutually exclude under one lock.
  **DONE** (memory/sqlite/objectlog claim via `eligible_candidates` + `commit_locked`).
- **§2.4 ReclaimDriver** — `tick(now)` reclaims expired leases with **zero** intervening client commands.
  **DONE.** Engine-level: conformance `tick_reclaims_expired_lease_with_no_client_traffic`,
  `tick_lease_boundary_is_half_open`. Composition-root: `fireweed-server` background task,
  `background_reclaim_recovers_orphaned_lease_without_client_traffic` (DoD met). Synchronous embedding
  drives it via the same `tick(now)` entry point.
- **§2.5 Sharding** — single shard fully implemented; multi-shard coordination post-launch. **DONE
  (recorded subset).**

---

## §3 RESP surface — fireweed-flavored Redis

**Invariants:**
- **Invariant 1** (per-item delivery, cursorless, no orphaning) — **DONE.** e2e
  `drain_and_reconcile_with_offtheshelf_client` (produce N mixed-priority → drain via `XREADGROUP >` to
  empty → delivered-set == produced-set, each once, no hang, cross-batch priority bands).
- **Invariant 2** (upsert = atomic XDEL+XADD, pending-only, every storage class) — **DONE.** e2e
  `xadd_on_client_item_key_upserts_not_appends` (effects), `xadd_collision_with_leased_then_terminal_is_an_error`
  (collision → invalid/terminal), `xack_of_superseded_id_is_superseded_over_the_wire` (`-ERR fireweed
  superseded`, fixed a shared `finalize_validate` bug). Atomicity proven at engine level (conformance).

**Stock commands implemented + tested:** `XADD` (upsert-on-key), `XREADGROUP >` (priority delivery,
cursorless), `XACK` (complete; operator-fenced → `-ERR fireweed stale_lease` via
`fenced_lease_xack_is_stale_over_the_wire`), `XPENDING` (summary + extended, count + numeric id order +
idle), `XAUTOCLAIM` (tick-reclaim + redeliver; `xautoclaim_redelivers_expired_leases`). Canonical error
tokens asserted verbatim (`EngineError::resp_token` + e2e substring assertions). **DONE.**

**RESP polish — Owed Item E — RESOLVED** (owed-resolution Chunks 6a/6b/6c). Everything below is DONE; the
only remaining sliver is the `XINFO CONSUMERS`/`XINFO … FULL` subcommands (a documented, non-launch read
nicety — `STREAM`/`GROUPS` are implemented).
- `XCLAIM` (specific-id) incl. the §3 "same-consumer `XCLAIM` = no-charge renew, cross-consumer =
  reclaim+1-attempt" semantics — **DONE.** `ReassignLeasePort` is implemented across memory, sqlite,
  objectlog, and postgres; `claimed_view` backs the rich RESP reply; conformance scenarios
  `reassign_swaps_token_and_charges_attempt` and `claimed_view_renders_leased_items` run across adapter
  suites; RESP e2e `xclaim_self_renews_no_charge_cross_consumer_reclaims_with_attempt_bump` proves
  self-claim renews without attempt charge and cross-consumer claim transfers ownership with +1 attempt.
- `XLEN` / `XINFO` / `XDEL` — **DONE** (owed-resolution Chunk 6b). `XLEN` returns the LIVE entry count
  (pending + in-flight) over `metrics`; `XINFO STREAM`/`GROUPS` summarize over `metrics`/`pending`
  (CONSUMERS/FULL owed; `last-delivered-id` is `0-0` — no meaningful stream-id high-water, a documented
  §3 divergence). `XDEL` hard-deletes via a new `PurgePort` (`force = true` like Redis), backed by the
  infallible `PurgeItems` command + the API-001 force gate (a leased purge needs force) — implemented on
  all four backends, ids de-duplicated so a repeated id counts once. Conformance
  `purge_removes_present_items_and_gates_leased` (incl. mixed-batch all-or-nothing gate + de-dup) runs
  across adapters; RESP e2e `xlen_xdel_xinfo_over_offtheshelf_client` drives all three via the stock
  client.
- Cursor-pagination `XAUTOCLAIM` — **DONE** (owed-resolution Chunk 6c). The handler was rewritten from the
  single-shot `0-0` into a real PEL scan: it snapshots the in-flight (leased) entries in a numeric id
  order (`id_order` was also fixed — it was silently sorting lexically, mis-ordering past 10 items + the
  `XPENDING` min/max), pages a `COUNT`-sized window from the `start` cursor, reclaims the idle (lease-
  expired) entries in the window to the calling consumer via `ReassignLeasePort` (a re-delivery, +1
  attempt, id preserved so the cursor is stable), and returns the next-entry cursor or `0-0` at the tail.
  `COUNT 0` is rejected. e2e `xautoclaim_paginates_the_pel_cursor` produces 12 entries (crossing the
  10-entry boundary), pages `0-0`→…→`0-0` with `COUNT 5`, and asserts every entry is reclaimed exactly
  once. Divergences (direct-transfer vs the ReclaimDriver's return-to-pending; all-or-nothing page on a
  racing ack) are documented in the handler.
- Intra-group exclusion + upsert↔claim race e2e — **DONE** (owed-resolution Chunk 6c).
  `two_consumers_in_a_group_never_get_the_same_item` drains one group with two concurrent consumers and
  asserts disjoint delivery + exactly-once coverage; `upsert_and_claim_race_stays_consistent` races an
  `XADD`-on-key against a concurrent claim and asserts exactly one live entry survives (the single-writer
  engine serializes them either way).

**Flavor differences** #1–#7 hold as designed; the attempt-count semantics now match TD-006:129 for
`XCLAIM`/`XAUTOCLAIM` redelivery and preserve the no-charge same-consumer renew divergence documented in
§3.

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
  `fireweed-conformance`. **DONE** (conformance is the single behavioral suite).
- **§4e beads** — **OWED.** Re-scope beads tied to deleted crates (claimed-item-shape → transport-neutral;
  Lakebase → `fireweed-server` image + health probe). None halted; tracked.

---

## §6 Definition of Done — gate-by-gate

| DoD gate | Status |
|---|---|
| `rg` zero refs to service/client/kafka/`NativeRoute`/`axum`/`/v1`/problem+json | **PASS** (grep clean in live `src/`+`tests/`). |
| No-stub = behavioral conformance per adapter × port method | **PASS** (atomic and eventual-apply suites exercise the same inherent operation surface; ≥1 fail-on-no-op test per port method incl. peek/pending/snapshot, run across all adapters). |
| Capability matrix {RESP-stock, library} signed, no unmarked library-only cells | **PARTIAL** — matrix present in TD-006 with library-only annotations; a final "every API-001/002 op classified" audit is OWED (Owed Item F). |
| Every migrated invariant has an engine-level test | **PASS** (auth, idempotency, operator-op, fencing, pause, recurrence-validation, command_position, purge-validation — engine tests). |
| ReclaimDriver: reclaim with zero intervening client commands | **PASS** (`fireweed-server` background-reclaim test + engine conformance). |
| e2e RESP green: drain-reconcile, cursor loop, crash recovery, fence, upsert effects+collision+superseded, intra-group exclusion | **PARTIAL** — drain-reconcile ✅, crash-recovery ✅ (sqlite reopen), fence ✅, upsert effects+collision+superseded ✅; **cursor-pagination loop + intra-group exclusion e2e OWED** (Owed Item E). |
| One conformance suite green on memory+sqlite+postgres+objectlog with backend-independent inherent operations | **PARTIAL** — memory+sqlite+objectlog ✅; **postgres OWED** (Owed Item A). |
| Two driving adapters + one composition root; dependency-direction test passes | **PASS** (RESP + facade + `fireweed-server`; dep-direction test green). |
| Durable-state reconstructable from the log (idempotency/fences/pause/command_position) | **PASS** (sqlite/objectlog rebuild-from-log durability tests; engine replay-reconstruction tests). |
| Docs consistent; ADR-007/TD-006/TD-007 recorded; asymmetry recorded | **PASS** (architectural docs); minor doc-hygiene OWED (Owed Item F). |
| Single-shard launch recorded; multi-shard post-launch | **PASS** (§2.5). |
| Phase 7 reconciliation shows no dropped item | **PASS** — this report; all gaps are OWED-with-reason, none silently dropped. |

---

## Owed items (tracked, with rationale — none are silent drops)

- **A. Postgres adapter — RESOLVED** (owed-resolution Chunk 4). `fireweed-postgres` was rebuilt fresh to the
  engine ports via the durable-adapter template (durable command LOG in postgres tables + projection
  rebuilt-from-log; atomic class) over the SYNC `postgres` client, implementing EVERY port incl. PushPort
  + UpsertPort(new) + RenewLeasePort, and re-added to workspace `members`. The full conformance suite (20
  scenarios) + 2 durability reopen tests run GREEN against a live postgres:16 (schema-isolated, one
  connection per scenario). Without `FIREWEED_PG_TEST_URL` they LOUD-skip (`eprintln!` + pass) so a green
  default run is visibly partial, never a hidden pass. **Blocking-executor caveat (I1) recorded** in the
  crate docs + here: the sync client runs its own internal tokio runtime per call and PANICS if driven
  from an ambient tokio runtime (tests use `futures::executor::block_on`); the launch posture is single-
  node durable-log + in-mem projection (guarantees identical to sqlite), and `fireweed-server` does NOT yet
  wire postgres into its selector, so no tokio path reaches it. Production refinement (spawn_blocking +
  pool + row-level locking for the MAX(seq)/high-water serialization the process Mutex provides today) is a
  recorded POST-LAUNCH item. **CI gate (M2):** the live run is in-session; `PHASE-7` marks the
  "conformance on …+postgres" gate **PASS (live), CI-job owed** — a `FIREWEED_PG_TEST_URL` service-container
  job is still owed (see build-progress).
- **B. Attempt-count on reclaim — RESOLVED** (owed-resolution Chunk 1). The reclaim (`LeaseExpired`) no
  longer charges; `attempt_count` = number of deliveries (charged only by `Claim`). TD-006:74/128-129 +
  the RESP XAUTOCLAIM doc updated; e2e asserts exactly 2 (claim + redeliver), conformance comment fixed.
- **B'. Retry-exhaustion — RESOLVED** (owed-resolution Chunk 8). The `Finalize` apply arm's `Retry` branch
  now calls the canonical `fireweed_core::failure_event(attempt_count, max_attempts)` predicate: a
  `Finalize{Retry}` that has used all `max_attempts` deliveries (`attempt_count >= max_attempts`) goes
  TERMINAL (`Failed`) instead of back to pending; a retry under the bound returns it to pending (claimable
  again). The `#[allow(dead_code)]` on `ItemRecord.max_attempts` is removed. The decision is a pure function
  of the replayed projection, so apply stays infallible (both Leased→Pending and Leased→Failed are legal)
  and replays deterministically. Conformance `retry_beyond_max_attempts_goes_terminal` runs on every
  backend (incl. live postgres): `max_attempts=2` proves under→pending+claimable, at→Failed+not-claimable+
  further-finalize-`Terminal`; a `max_attempts=1` case pins the `>=` boundary (one delivery, first retry
  exhausts). **Scope boundary (documented in the apply arm + acknowledged here):** only the EXPLICIT-retry
  path is bounded. `Release`/`Rearm` are intentionally unbounded, and the **claim/reclaim path is NOT
  attempt-bounded** — an item whose lease repeatedly EXPIRES (`LeaseExpired`→pending→re-`Claim`, +1 each)
  can exceed `max_attempts` deliveries without terminating; bounding that lease-drop poison-loop at
  claim-time is a separate, owed policy (`max_attempts==0` is already rejected at queue creation, so the
  apply arm needs no guard for it).
- **C. RESP/facade server-side id generation — RESOLVED** (owed-resolution Chunk 2). `UpsertPort::
  replace_if_pending` no longer takes a caller-supplied id; the backend assigns it from its own `cmd_seq`
  (restart-safe) and returns it in `UpsertOutcome`. RESP `xadd`-without-key routes through `PushPort`,
  -with-key through the new `UpsertPort`; the facade `upsert` drops its counter. e2e
  `two_servers_on_one_backend_assign_distinct_xadd_ids` proves two RESP servers on one backend mint
  distinct ids and both items coexist.
- **D. Graceful connection drain on shutdown — RESOLVED** (owed-resolution Chunk 5). `fireweed-resp`'s new
  `serve_with_shutdown` takes a `CancellationToken` and owns the per-connection handlers in a
  `tokio::task::JoinSet`; on cancel it stops accepting and each handler observes the token BETWEEN commands
  (finishing any in-flight command first), then the drain awaits them all. `Server::shutdown()` stays SYNC
  (signals the token + aborts; safe from `Drop`, I4); a new async `shutdown_and_drain(self, timeout)`
  awaits the serve task and, past the bound, aborts it — and because the serve loop OWNS the handlers in a
  `JoinSet`, that abort drops the set and HARD-aborts any handler still running (a real bound, not the
  best-effort a `TaskTracker` would give — the fresh-eyes review caught and corrected a `TaskTracker`
  first cut). Test `shutdown_and_drain_drains_in_flight_then_stops_accepting` proves an open connection
  drains gracefully and the server then stops accepting; the 3 sync `shutdown()`/`Drop` call sites are
  unaffected.
- **E. RESP polish.** `XCLAIM` specific-id is resolved by `ReassignLeasePort`, `claimed_view`, shared
  conformance, and RESP e2e. Still owed: `XLEN`/`XINFO`/`XDEL`, paginated `XAUTOCLAIM` cursor coverage,
  intra-group-exclusion e2e, and upsert/claim-race e2e.
- **F. Library verbs + doc hygiene — RESOLVED** (port in Chunk 3; facade verbs + audit + doc scrub in
  Chunk 7). The `RenewLeasePort`/`ReassignLeasePort`/`PurgePort` exist and pre-validate via the shared
  `validate_leased` helper (mirrors `finalize_validate`: NotFound / fenced→StaleLease / terminal→Terminal /
  superseded→Superseded / not-Leased→Invalid) BEFORE any log append.
  - **F.2a facade verbs** — the `fireweed` facade now exposes the full surface: `renew` (RenewLeasePort, no
    attempt charge), `reassign` (ReassignLeasePort, +1 delivery + fresh token), `rearm` (`Finalize{Rearm}`
    → re-queue + reset attempt_count), `purge` (PurgePort, force gate + count), and `claimed`
    (`ProjectionRead::claimed_view`). Five facade tests added (every new verb exercised over a real
    backend); the DEFERRED-verbs note + the raw-backend escape hatch are gone.
  - **F.2b capability matrix** — audited TD-006 §3: every API-001/002 op is classified `RESP-stock` /
    `library-only-intentional` / `n/a` with no unmarked library-only cell (claim variants, renew, fail/
    retry/release/rearm, reclaim via XCLAIM/XAUTOCLAIM, rich metrics, XDEL, force-purge all marked).
  - **F.2c doc hygiene** — API-001 now carries a "Realized surfaces (ADR-007)" note: the contract is
    realized through the **two** built faces (Rust library + RESP); the HTTP/JSON `/v1` route table is kept
    as a transport-neutral illustration but marked NOT-built (the legacy `fireweed-service` was deleted).
    TP-001's Test-Layers table was rewritten off the deleted `fireweed-storage`/`fireweed-service` crates onto
    the real locations (conformance/postgres/resp-e2e/facade/server).
  - **F.2d beads** — re-scoping recorded here (the tracker's `close` asserts full acceptance, which these
    do not all meet): the claimed-item-shape beads (`pqueue-9c77d5e7`, `pqueue-922eaf00`, acceptance keyed
    to the deleted `fireweed-client`/`fireweed-storage` suites) are **superseded** by the transport-neutral
    `fireweed_engine::ClaimedItem` — the claim path returns the full shape across all backends and conformance
    (`claim_returns_priority_ordered_rich_items`, `claimed_view_renders_leased_items`) + the facade
    `claimed` verb cover it; `metadata`/`gate_keys`/whole-cohort remain intentionally deferred to the
    API-003 epic (`pqueue-f6fbde17`). The Lakebase/connect-helper/profile beads (`pqueue-692471c5`,
    `pqueue-607be5bf`, `pqueue-2f57fbe4`, `pqueue-ea625701`, `pqueue-9cdafdaa`) now target the rebuilt
    `fireweed-postgres` + `fireweed-server` + RESP/library faces rather than the deleted `fireweed-service`; the
    rebuilt postgres adapter is `NoTls`-only, so the Lakebase TLS seam is genuine remaining product work.

---

## Verdict

The hexagonal re-architecture is **functionally complete for launch scope**: one CQRS engine, the shared
projection state machine, **four** driven adapters spanning both durability classes (memory/sqlite/
postgres atomic + objectlog eventual-apply), two driving interfaces (RESP worker surface + full Rust
library facade) behind a composition root with a background reclaim driver and a graceful drain, and the
legacy HTTP-service/Kafka/storage-trait architecture fully deleted — all verified by a green full default
workspace (0 failures, clippy 0), a dependency-direction test, and live postgres conformance.

**Owed-item status (owed-resolution loop) — ZERO open items.** All six original owed items **A–F** plus the
newly-surfaced **B′** are **RESOLVED**: A postgres adapter, B attempt-count, B′ retry-exhaustion, C server-
side ids, D graceful drain, E RESP polish (XCLAIM/XLEN/XDEL/XINFO/paginated-XAUTOCLAIM/race+exclusion), F
library verbs + doc hygiene. **No plan item is silently dropped.** Two scope boundaries are recorded, not
dropped: the postgres backend is not yet server-wired (blocking-executor caveat) and is `NoTls`-only
(Lakebase TLS seam is owed product work); and retry-exhaustion bounds the explicit-retry path only — the
lease-drop reclaim poison-loop is a separate owed policy (B′ scope note). The DoD CI job that runs postgres
conformance against `FIREWEED_PG_TEST_URL` in CI (vs the in-session live run) is also still owed.
