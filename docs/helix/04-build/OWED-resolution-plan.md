# Owed-Items Resolution Plan (post-migration)

> **HISTORICAL (June 2026 hexagonal-migration era).** Resolves the owed items of the completed
> hexagonal migration only. Superseded by the later ADR-008…014 cascade. **Not current status** — see
> `DEPLOYMENT-READINESS.md`, `gap-closure-plan.md`, and the live `.ddx` tracker.

Resolves the six owed items from `PHASE-7-reconciliation.md`. Each chunk: implement → review (fresh-eyes
for production logic) → test (full default workspace green + clippy 0) → **commit**. Sequenced so each
port/semantic change lands BEFORE the postgres adapter, so postgres is built once against the final port
set (no re-touch). Green-gate after every chunk; `git commit` per converged chunk.

## Non-negotiables (unchanged)
No stubs/fallbacks/shims; hexagonal dependency direction (dep-direction test stays green); behavioral
no-stub conformance on EVERY backend (incl. postgres); e2e RESP via the off-the-shelf `redis` client;
STRUCTURED engine errors; commit-invariant (pre-validate so apply is infallible).

---

## Sequence

> **Per-chunk bookkeeping (M3):** every chunk also flips its specific gate in `PHASE-7-reconciliation.md`
> (so a mid-plan stop leaves an accurate report) and `git commit`s when green.

### Chunk 1 — B: attempt-count = delivery count (TD-006:129 reconciliation)
**Problem (verified):** `apply_command` bumps `attempt_count` in BOTH the `Claim` arm (`fireweed-projection`
~316) AND the `LeaseExpired` arm (~368), so a reclaim+redeliver charges 2; TD-006 says one.
**INVARIANT (the explicit semantic):** `attempt_count` = the number of times the item was **handed to a
worker** — i.e. it increments **only** in the `Claim` apply arm. A reclaim (`LeaseExpired`) returns the item
to pending and is NOT a delivery, so it does not charge. Poison detection is preserved: each redelivery is a
fresh `Claim` and increments. (This is a unilateral but correct change to a shared field; TD-006 is amended
to match, not the reverse.)
**Do:** remove the `attempt_count += 1` from the `LeaseExpired` arm only (leave `Claim`'s bump). Verify
`Finalize{Retry/Release}` does NOT separately charge an attempt (it should not — retry/nack returns to
pending without a delivery; if it does, that's a SEPARATE pre-existing question — record it, don't fix it
here). **Complete test-update set:** RESP e2e `xautoclaim_redelivers_expired_leases` `assert_eq!(attempt,3)`
→ **2** (claim 1 + redeliver 1; the reclaim no longer charges); conformance
`tick_reclaims_expired_lease_with_no_client_traffic` — fix its inline "reclaim=2" comment (it has no hard
`assert_eq!` on the count, but the comment must not lie); RESP `xautoclaim` doc-comment; TD-006 lines ~74 +
~128-129 wording → "one attempt per reclaim+redeliver cycle (the redelivery charges; the reclaim does not)".
**OUT OF SCOPE (M5, flag — NOT delivered here):** retry-exhaustion ("Finalize-Retry beyond `max_attempts` →
terminal") is unwired (`max_attempts` is `#[allow(dead_code)]`); this chunk fixes the *input* counter, not
the exhaustion policy. Record as a NEW owed item, not silently assumed done.
**Accept:** conformance + resp green; TD-006 + code agree on the invariant.

### Chunk 2 — C: backend-assigned upsert ids (RESP server-id collision)
**Problem:** `UpsertPort::replace_if_pending` takes a caller-supplied `new_item_id`; the RESP front + facade
generate it from their own counters → two servers/handles on one backend (or a restart) collide.
**Do:** change `UpsertPort::replace_if_pending` to NOT take `new_item_id` — the backend assigns it from its
own `cmd_seq` (restart-safe, like PushPort) and returns it in `UpsertOutcome::{Inserted,Replaced}` (which
already carry the id — verified, so the call sites that destructure the reply are unaffected). Ripple: the 3
backend impls, the conformance upsert scenarios (incl. `upsert_is_unavailable`'s id literal), RESP `xadd`,
facade `upsert`. RESP `xadd`-without-key → route through `PushPort` (backend-assigned); `xadd`-with-key →
the new `UpsertPort`. Confirm no e2e asserts a SPECIFIC returned-id shape (XADD replies are captured as
opaque `String` — safe).
**Accept:** no caller passes an item id into a write; resp + facade + conformance green; add a
**two-servers-one-backend** XADD e2e (TWO `serve()` listeners over ONE `Arc<Backend>` — not two backends) →
ids don't collide, both items coexist.

### Chunk 3 — F.1: `RenewLeasePort` (enables renew everywhere)
**Problem:** no pre-validating renew path; `RenewLease`'s apply arm (~320) only guards `NotFound` and does
NOT check fenced/terminal/superseded/leased — so a naive `Backend::write(RenewLease)` can both diverge AND
silently renew an invalid item.
**Do:** add a `renew_validate(&self, ids) -> EngineResult<()>` helper on `ProjectionData` that **mirrors
`finalize_validate` EXACTLY** (NotFound / fenced→StaleLease / terminal→Terminal / **superseded→Superseded** /
not-Leased→Invalid). Add `RenewLeasePort::renew(shard, ids, new_lease_expires_at, now)` to the engine,
implemented by memory/sqlite/objectlog: `renew_validate` first (append nothing on reject), then commit a
`RenewLease` command via `commit_locked`. Add a conformance scenario (`renew_extends_lease_and_rejects_*`)
to both suites covering the happy path + a fenced/superseded reject.
**Accept:** renew is divergence-safe, rejection semantics IDENTICAL to finalize, conformance-tested on every
backend.

### Chunk 4 — A: postgres adapter (DEFERRED → DONE) — RISKIEST; resolve I1/I2 as preconditions
**Provision:** start a local PostgreSQL (docker preferred: `docker run -d --name fireweed-pg -p 5433:5432 -e
POSTGRES_PASSWORD=fireweed postgres:16`; fall back to `pg_ctl`/`initdb`). Export
`FIREWEED_PG_TEST_URL=postgres://postgres:fireweed@127.0.0.1:5433/postgres`.
**Build:** re-create `fireweed-postgres` to the engine ports via the durable-adapter template (durable LOG in
postgres + projection rebuilt-from-log; atomic class), SYNC `postgres` crate. Implement EVERY port incl.
PushPort + UpsertPort(new) + RenewLeasePort. Schema mirrors sqlite (BIGINT, `$N` params, `ON CONFLICT … DO
UPDATE`, `COALESCE(MAX(seq),-1)+1`). Errors → `Storage`. **M1:** `fireweed-postgres` is ALREADY in the
dep-direction `ADAPTERS` list — the only action is re-adding it to workspace `members`.
**I1 — blocking-in-async DECISION (record it):** the port bodies do blocking postgres calls inside
`std::future::ready` (mirroring sqlite). For CONFORMANCE this is safe — each scenario is a separate
`#[tokio::test]` with its own runtime + its own connection, and calls are sequential within a scenario, so
no single runtime is starved. For a PRODUCTION `fireweed-server` serving many connections over ONE postgres,
synchronous network calls on Tokio worker threads WOULD starve the runtime → **documented caveat**: the
postgres backend requires a blocking-friendly executor; the production refinement (wrap in
`spawn_blocking` + a connection pool, or the relational-projection `FOR UPDATE SKIP LOCKED` multi-node mode)
is a recorded POST-LAUNCH optimization. Launch postgres = single-node durable-log + in-mem projection,
identical guarantees to sqlite. This is NOT a silent sqlite-mirror — the caveat is written in the crate
docs + the reconciliation report.
**Conformance + I2 (no silent never-run):** `tests/conformance.rs` env-gated on `FIREWEED_PG_TEST_URL`. **M2:**
each scenario gets its OWN connection + a UNIQUE schema (`connect_fresh` opens a fresh connection, `CREATE
SCHEMA fireweed_test_<pid>_<atomic>; SET search_path` — NOT a shared connection mutating search_path, which would
race under cargo's concurrent tests). If the env var is ABSENT the test prints a LOUD
`eprintln!("POSTGRES CONFORMANCE SKIPPED — set FIREWEED_PG_TEST_URL")` and returns (so a green run is visibly
partial, not a hidden pass). **DoD addition:** "postgres conformance runs in CI" — add a documented
`make test-postgres` / CI service-container note; do NOT mark the PHASE-7 "conformance on …+postgres" gate
PASS on the in-session run alone — mark it "PASS (live), CI-job owed".
RUN it in-session against the provisioned DB: all 19 atomic scenarios + a durability reopen test green.
**Accept:** postgres conformance green against a live DB (loud skip otherwise); default workspace green
without one; blocking caveat recorded.

### Chunk 5 — D: graceful connection drain on shutdown
**Problem:** `serve` spawns detached per-connection tasks; `Server::shutdown()` aborts only the accept loop +
ticker, leaking in-flight handlers.
**Do:** add `tokio-util` (dep); `serve` owns a `TaskTracker` (or `JoinSet`) for connection tasks + takes a
`CancellationToken`; on cancel it stops accepting and `tracker.wait()`s the in-flight handlers. **I4 — keep
`Server::shutdown()` SYNC** (signals the token + aborts the accept loop; safe to call from `Drop`); add a
SEPARATE `async fn shutdown_and_drain(self, timeout)` that awaits the tracker with a bounded (Config-
configurable) timeout. `Drop` keeps the sync abort; the 3 existing sync `shutdown()` test calls are
unaffected.
**Accept:** a test that an in-flight request completes (or is bounded-cancelled) on `shutdown_and_drain`; no
leaked tasks; sync `shutdown()` + `Drop` still work.

### Chunk 6a — E.1: XCLAIM (renew + reclaim) — depends on Chunk 3 + the Chunk-1 attempt model
**I5 — command path:** add a `ReassignLease` command (`{ item_ids, new_lease_token, new_lease_expires_at }`,
pre-validated: must be Leased + not fenced/superseded) that swaps the lease token to the new consumer AND
charges exactly ONE attempt (a `Claim`-equivalent bump). `XCLAIM key group consumer min-idle id…`:
same-consumer (token unchanged) → **renew** via `RenewLeasePort` (no attempt charge, §3 flavor #7);
cross-consumer → `ReassignLease` (charge 1, per TD-006:129). e2e: self-XCLAIM renews without bumping
attempt; other-consumer XCLAIM reclaims with attempt+1.
**Accept:** XCLAIM both semantics + e2e green.

### Chunk 6b — E.2: read-only surface XLEN / XDEL / XINFO
**XLEN** = total via `metrics`; **XDEL** = `PurgeItems` command (apply is INFALLIBLE — `remove`-if-present,
no pre-validation divergence risk; still structured-reply); **XINFO STREAM/GROUPS** = queue/lease summary
(scope to STREAM + GROUPS only — a documented divergence; carry the §3 flavor note that last-delivered-id is
not a meaningful high-water). e2e per command.
**Accept:** the three commands + e2e green.

### Chunk 6c — E.3+E.4: paginated XAUTOCLAIM cursor + race/exclusion e2e
**Paginated XAUTOCLAIM:** real entry-id-ordered continuation cursor over the PEL (new machinery — the
current handler is single-shot `0-0`); stop at `0-0` when the PEL is covered. e2e pages `0-0`→…→`0-0` and
asserts full coverage. **e2e:** intra-group exclusion (two consumers, concurrent `XREADGROUP >`, never same
item) + upsert↔claim race (best-effort).
**Accept:** cursor pagination + both e2e green.

### Chunk 7 — F.2: library verbs + capability audit + doc hygiene + beads
- **F.2a** facade `renew` (via RenewLeasePort) + `rearm` (Finalize{Rearm} — already a FinalizeKind, add the
  verb); facade tests.
- **F.2b** capability-matrix audit: confirm every API-001/API-002 op is classified RESP-stock / library-only /
  n-a in TD-006 with NO unmarked library-only cell; fix gaps.
- **F.2c** doc hygiene: scrub remaining HTTP-era phrasing from API-001/TP-001; ensure docs self-consistent.
- **F.2d** re-scope/close beads tied to the deleted crates (claimed-item-shape → transport-neutral; Lakebase →
  `fireweed-server` image + health probe).
**Accept:** reconciliation owed-item F → resolved; update `PHASE-7-reconciliation.md` to mark all six owed
items RESOLVED.

---

## Definition of done (whole plan)
All six owed items resolved + tested; full default workspace green (clippy 0, dep-direction test green);
postgres conformance green against a live DB; `PHASE-7-reconciliation.md` updated to show zero owed items;
each chunk committed. Then the loop stops.
