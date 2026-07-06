---
ddx:
  id: build-library-canonical-coordination-plan
  depends_on:
    - adr-engine-enforced-coordination-and-encapsulated-library-surface
    - td-sharding-and-shard-ownership
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-embedded-engine-integration-and-public-surface
  status: draft
  review:
    self_hash: aba8b07892cbfc73213491ef0285e2b6851da283824a3987c090d52b956e287b
    deps:
      adr-embedded-engine-integration-and-public-surface: e18689f92ad1070a9d3e96253f41b6d0a3fe67eb9b6eb80f5df07ac24e56c7cc
      adr-engine-enforced-coordination-and-encapsulated-library-surface: 36c73add90f1c464172040dd7c926608f49c5a263b2bf03d9dd03103d8a5b6c2
      adr-hexagonal-architecture-and-two-interfaces: 02e04b32110f57e05ea80a7b6ce642cba655866e14302db6a8b0d1de0f62d012
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-06T00:56:00Z"
---

# Library Canonical Coordination — Implementation Plan

Status: **CONVERGED** (review round 2 — verifier confirmed all blocking items resolved against the real
`commit_locked`/`append_durable` seam; execution-detail clarifications folded in). Ready to execute.
Governing: **ADR-009** (engine-enforced coordination + encapsulated library surface), **TD-003**
(ownership & fencing, incl. the new In-Process Library Owner-Runtime section), ADR-007/ADR-008.

> **v2 changelog:** B1 retargeted to the real write seam (`commit_locked`/`append_durable`/projection
> `commit`, not `Backend::write`/`LogWriter::append`); B1 scope widened to **all** log-appending ports and
> its gate to the full compile blast radius; B3 adds `open_memory`/`open_objectlog` + a doc-hidden test
> injection constructor + the `Pqueue<MemoryBackend>` annotation migration and the dev-dep→optional-dep
> reversal; B5 pins the runtime-refuse predicate to a **capability** (atomic acquire→fence), not a backend
> name, and names the refuse-test backend; added N6 (library adds no authn/authz) and error-name mapping.

## 0. Goal & non-negotiables

Make the in-process Rust library (`Pqueue`) a **first-class owner-runtime** that resolves ownership and
operates under an engine-enforced epoch fence on every queue-addressed op, and make the **published
`pqueue` crate the only external surface** to the engine. Non-negotiables (from ADR-009 / TD-003):

- **N1 — Engine-enforced fence, below the ports, on every log-appending op.** Every queue-addressed write
  port (Push, Claim, Finalize, Upsert, Renew, Reassign, Purge, and the ReclaimDriver) checks the owner's
  **cached acquire-time** `fence_epoch` (never a re-read of current), **at commit time inside the atomic
  UoW**.
- **N2 — Behavior-preserving for single-owner.** A sole-owner `Pqueue` keeps today's semantics via a
  degenerate constant-ownership / always-current session (`expected_epoch = None` ⇒ stamp current, no
  fence) — existing tests stay green behaviorally.
- **N3 — No accidental reach-around.** No safe path from published `pqueue` to a raw `PushPort`/`ClaimPort`/
  `FinalizePort` call. Enforced structurally (publish topology + private constructor + guard tests).
- **N4 — Honest backend scope.** Multi-instance shared-store competition is **postgres-only**, gated on a
  single durable epoch; object-log is single-owner; memory/sqlite are single-process. Runtime-refuse keys
  on a **capability** (see N4a), not a backend name.
- **N4a — Refuse predicate is capability-based.** A `Pqueue` constructed for **multi-owner** operation
  MUST runtime-refuse a backend/control-plane pair that does not present the **atomic acquire→fence**
  capability (one durable epoch advanced atomically at acquire). The in-process `InMemoryControlPlane` +
  `MemoryBackend` pair *presents* the capability **non-durably** (single in-process epoch, no cross-process
  durability), so it is admissible for **in-process logic** tests but is **not** a durable multi-process
  deployment. The refuse path is exercised against a pair that genuinely lacks the capability
  (sqlite-local: no shared durable control plane) — see B5.
- **N5 — Every step tests green and is committed before the next.** Full-workspace `cargo build`/`clippy`
  green + the step's `cargo test` gate at each commit. No step claims a guarantee it hasn't tested.
- **N6 — The library adds no authn/authz (ADR-009 Decision 6).** This work deliberately changes nothing
  about authentication/authorization: the embedding host owns its trust boundary, and a multi-tenant host
  still authorizes per ADR-002. No B-step adds, relaxes, or routes authz through the library; this is an
  asserted no-change boundary, not an omission.

**Error-name mapping:** the internal engine variant is `EngineError::EpochFenced`; the TD-003 / conformance
wire/code name is `queue-epoch-stale`. They are the same condition — tests assert whichever the layer
exposes (engine/library: `EpochFenced`; RESP/conformance: `queue-epoch-stale`).

## 1. Environment & testability

| Capability | How tested | Available here |
|---|---|---|
| Engine/library/memory/sqlite semantics | in-process unit + conformance | yes (cargo) |
| Library multi-instance **logic** (target-affinity, fence-at-commit, OwnedElsewhere, drain) | two `Pqueue` handles over one shared in-process `MemoryBackend` + `InMemoryControlPlane` (asserts against `fence_epoch`, the storage epoch — never `lease_epoch`, which holds a different integer) | yes (cargo) |
| Postgres single-durable-epoch (BQ-23) + **durable** multi-instance | `PQUEUE_PG_TEST_URL` against a live DB | yes via `docker run postgres` + set env (LOUD-skips otherwise) |
| Encapsulation guard | manifest scan + `trybuild` compile-fail (and/or `cargo-public-api`) | yes (cargo) |

Postgres steps spin up a throwaway container, e.g.
`docker run -d --rm -p 5433:5432 -e POSTGRES_PASSWORD=pq postgres:18` then
`PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres`.

## 2. Build sequence (DAG)

Execution order lands **in-process-testable** semantics first, isolating postgres-gated durability.
Dependencies: B2→B1; B3→B2; B4 independent of B1-B3 (postgres durability) but B5 depends on B1-B4.

### B1 — Engine fence threading (L4 core) · task #5
- **Seam (corrected).** The data-plane write ports do **not** flow through `Backend::write`/
  `LogWriter::append` (only the conformance `commit` helper does). They flow through each backend's
  `commit_locked` → `append_durable` (sqlite/postgres) / `pqueue_projection::commit` (memory), which today
  **self-stamp the current epoch with no fence**. B1 adds the fence **there**.
- **Change:** thread `expected_epoch: Option<u64>` from every queue-addressed write port — `PushPort`,
  `ClaimPort` (via `ClaimRequest`), `FinalizePort`, **`UpsertPort`, `RenewLeasePort`, `ReassignLeasePort`,
  `PurgePort`, `ReclaimDriver`** (all append through `commit_locked`) — into `commit_locked`/
  `append_durable`/projection `commit`, and add `if let Some(e) = expected_epoch { if e != current_epoch {
  return Err(EpochFenced) } }` evaluated **at commit inside the UoW** (mirroring the existing
  `SqlLogWriter::append` check at `sqlite/src/lib.rs:317`). `None` ⇒ stamp current, no fence (N2). The
  epoch source is the caller's session (B2). Rewrite the `port.rs:33-34` + `append_durable` BQ-20 contract
  notes to match. Note: `ReclaimDriver::tick` is **multi-shard** (it sweeps all owned queues and commits
  per shard), so it supplies **each owned shard's** cached acquire-time epoch at that shard's
  `commit_locked` — not one tick-level `Option<u64>`.
- **Blast radius (must all compile, N5):** traits `port.rs` (`ClaimRequest`, the 8 write ports); commit
  paths `pqueue-projection/src/lib.rs:248`, `pqueue-memory/src/lib.rs:128`, `pqueue-sqlite/src/lib.rs:138`
  + `relational.rs`, `pqueue-postgres/src/lib.rs:144` + `relational.rs`, `pqueue-objectlog/src/lib.rs`;
  callers `pqueue/src/lib.rs` (152/202/248…), `pqueue-resp/src/lib.rs` (512/568/838), `pqueue-conformance`
  (`claim_req` + `scenarios.rs` finalize sites), `pqueue-bench`.
- **Acceptance:** (a) a stale supplied epoch is rejected `EpochFenced` at commit, state unmutated, on
  memory + sqlite, for **each** write port; (b) `None`/sole-owner path behaves exactly as today; (c)
  conformance core suite green for memory + sqlite.
- **Gate:** `cargo build --workspace --all-targets` (full blast radius compiles) +
  `cargo test -p pqueue-engine -p pqueue-memory -p pqueue-sqlite -p pqueue-objectlog -p pqueue -p pqueue-resp -p pqueue-conformance`
  + `cargo clippy --all-targets -- -D warnings`.
- **Testable:** in-env.

### B2 — Library coordination session · task #6
- **Change:** `Pqueue` gains an `OwnerId` + control-plane handle + cached `OwnedSession`; per-op **resolve
  ladder** (serve-if-owner / acquire-if-target / `OwnedElsewhere` / drain-split); **target-affinity**
  acquire policy; **bounded per-node** renew/heartbeat driver (host-spawned; one bounded driver, no
  task-per-queue); **re-resolve (not retry)** after timeout; `OwnedElsewhere{owner, epoch}` return value;
  **sole-owner degenerate policy** (constant ownership + `expected_epoch = None`) as the default for
  single-instance. Supplies B1's `Some(fence_epoch)` from the cached session for multi-owner mode.
- **Acceptance:** unit tests over memory + `InMemoryControlPlane` for each ladder branch; superseded owner
  fenced **at commit** with its cached `fence_epoch` (asserted against the storage epoch, not `lease_epoch`);
  **target-affinity no-thrash** (non-target returns `OwnedElsewhere`, `assignment_epoch` does not
  ping-pong); **data-path fail-closed** (expired-lease owner fenced regardless of renew); drain split
  (serve in-flight, refuse new claim); the "MUST NOT append to a queue it has not acquired" clause asserted
  via the `OwnedElsewhere` branch (no append occurs); sole-owner unchanged (N2). **Bounded driver**:
  a structural assertion that the renew driver count is O(1) per node, not O(queues) (cheap test; if
  infeasible as a unit test, explicitly mark owed to TP-002 E2 `queue_density_single_node_tests`).
- **Gate:** `cargo test -p pqueue`; clippy 0; existing single-owner call sites green.
- **Testable:** in-env.

### B3 — Encapsulate the published surface · task #7
- **Change:** static feature-gated per-backend constructors **`Pqueue::open_memory()` /
  `open_sqlite(path)` / `open_postgres(cfg)` / `open_objectlog(cfg)`** → `Pqueue<impl LibBackend>` (backend
  built internally, opaque); `Pqueue::new(Arc<B>, …)` → `pub(crate)`; **a `#[doc(hidden)] pub fn
  with_backend(...)` (or `internal` feature-gated) injection constructor** so `tests/` integration and
  bench crates that must inject a backend still compile; `publish = false` on `pqueue-engine`/
  `pqueue-projection`/`pqueue-conformance`/adapter crates, `pqueue` the only published crate; add the
  adapter crates as `optional = true` real deps of `pqueue` behind features (today `pqueue/Cargo.toml` has
  `pqueue-memory`/`pqueue-objectlog` as **dev-deps** and `pqueue-sqlite`/`pqueue-postgres` **absent**, so
  this promotes two and adds two; rewrite the `src/lib.rs:11-13` "never on a concrete backend" doc and the
  dependency-direction guard accordingly); `#[doc(hidden)]` port traits + marker supertrait; split guard
  tests.
- **Call-site migration (must land in the same commits, N5):** every `Pqueue::new(...)` /
  `Pqueue<MemoryBackend>` site in `pqueue/tests/facade.rs`, `pqueue/tests/product_validation_tests.rs`,
  `pqueue-bench/src/main.rs`, `pqueue-bench/tests/queue_density_single_node_tests.rs`,
  `pqueue-bench/tests/performance_cross_queue_scale_out_tests.rs` → use `open_memory()`/`with_backend()`
  and `Pqueue<impl LibBackend>`/generic annotations.
- **Acceptance:** (a) manifest-scan guard asserts publish topology (only `pqueue` publishable); (b)
  `trybuild` compile-fail proves a `pqueue`-only downstream cannot name a port or reach `.backend`; (c)
  feature subsets build (`--no-default-features --features sqlite` doesn't compile postgres/objectlog);
  (d) no public path to a port-bearing handle; (e) all migrated test/bench crates green.
- **Gate:** `cargo test -p pqueue --no-default-features --features sqlite` and `--features postgres`;
  `cargo build --workspace --all-targets`; dependency-direction test extended + green; clippy 0.
- **Testable:** in-env.

### B4 — BQ-23: single durable epoch on postgres · task #4
- **Change:** collapse the control-plane lease epoch and the storage append-fence epoch into **one durable
  value advanced atomically in the acquire transaction** (the acquire txn IS the durable fence); the
  data-plane append validates against that single value. Removes the two-counter hazard
  (`ownership.rs:23-30`, `postgres/src/control_plane.rs:16-19`).
- **Acceptance:** acquire advances exactly one durable epoch; a superseded owner's append is `EpochFenced`
  (`queue-epoch-stale` at the wire) with no crash window; existing postgres tests green; new test for the
  acquire→fence atomicity.
- **Gate:** `PQUEUE_PG_TEST_URL=… cargo test -p pqueue-postgres` (docker pg); clippy 0.
- **Testable:** docker-postgres.

### B5 — Multi-instance conformance + final alignment · task #8
- **Change:** add a multi-instance suite: (a) **in-process** semantics over shared `MemoryBackend` +
  `InMemoryControlPlane` — admissible because that pair presents the atomic acquire→fence capability
  non-durably (N4a) — testing target-affinity, fence at commit, ownership migration on expiry/drain,
  data-path fail-closed; (b) **runtime-refuse** test against a capability-lacking pair (**sqlite-local**:
  no shared durable control plane) — a multi-owner `Pqueue` MUST refuse to construct; (c) **postgres-gated**
  durable two-instance competition (real durability, restart). Final alignment gate.
- **Acceptance:** suites (a) and (b) green in-env; (c) green under docker-pg (LOUD-skip ⇒ owed, never
  "done"); full workspace green + clippy 0 + dependency-direction + new guard tests; a final review
  confirms the implementation matches ADR-009 Decisions 1-6 (D6 = no-change, asserted) + TD-003
  §In-Process Library Owner-Runtime MUSTs + conformance rows, with no overstated guarantee.
- **Gate:** full `cargo test` (+ docker-pg run); `cargo clippy --all-targets -- -D warnings`.
- **Testable:** in-env (a, b) + docker-postgres (c).

## 3. Per-step protocol (N5)

For each B-step: implement → `cargo build --workspace --all-targets` + the step's `cargo test` gate +
`cargo clippy --all-targets -- -D warnings` → if green, **adversarial self-review of the diff**
(`/code-review` or a grounded reviewer) → fix → re-test → **commit** with a bead-style message → mark the
task complete → next step. Never advance on red.

## 4. Risks

| Risk | Mitigation |
|---|---|
| Port signature change (B1) ripples through 6 backend files + pqueue + resp + conformance + bench | Epoch is additive `Option<u64>` with a `None` sole-owner default (N2); B1's gate is `cargo build --workspace --all-targets` so the full blast radius must compile before B1 commits. |
| `Pqueue::new` privatization breaks `tests/`/bench crates (`pub(crate)` is invisible to them) | B3 ships `open_memory()` + a `#[doc(hidden)] pub with_backend()` injection ctor and migrates every listed call site / `Pqueue<MemoryBackend>` annotation in the same commit. |
| B3 reverses today's dep direction (backends are dev-deps of `pqueue`) | Promote to `optional = true` real deps behind features; no cycle (backends depend on engine, not `pqueue`); rewrite the lib doc + dependency-direction guard. |
| Postgres unavailable mid-run | docker pg is available; if it ever isn't, B4/B5(c) LOUD-skip and are flagged **owed**, never claimed done. |
| `EpochFenced` vs `queue-epoch-stale` assertion mismatch | Mapping stated in §0; assert the name the layer exposes. |
| Reordering vs ADR-009 "L4+BQ-23 ship together" | The *production* guarantee is claimed only once B1+B4 both land (B5 gate); B1 alone is tested on memory/sqlite (single in-process epoch source), not claimed as the postgres guarantee. |
| B5a meaningless if it asserts the wrong epoch | B5a asserts against `OwnedSession.fence_epoch` (storage epoch) only; `lease_epoch` and `fence_epoch` hold different integers and must not be conflated. |

## 5. Bead / task mapping

| Step | Task | Bead area |
|---|---|---|
| B1 | #5 | pqueue-engine, pqueue-memory, pqueue-sqlite, pqueue-objectlog, pqueue-conformance (+compile: postgres, pqueue, resp, bench) |
| B2 | #6 | pqueue |
| B3 | #7 | pqueue (+ workspace manifests, test/bench migration) |
| B4 | #4 | pqueue-postgres |
| B5 | #8 | pqueue-conformance, workspace |

## 6. Exit criteria (Phase 4 done)

1. B1–B5 all green at their gates; each committed after test+review.
2. Full workspace `cargo test` green, `cargo clippy --all-targets -- -D warnings` clean, dependency-direction
   + new guard tests green. Postgres-gated suites (B4, B5c) run under docker-pg and pass; if a run ever
   skips for lack of a DB it is recorded **owed**, never counted as done (consistent with the Risk row).
3. A final alignment review confirms the code satisfies every MUST in TD-003 §In-Process Library
   Owner-Runtime and ADR-009 Decisions 1-6 (Decision 6 verified as a deliberate no-change, N6), with no
   overstated guarantee.

## 7. Execution status — COMPLETE (with accepted deferrals)

All B-steps landed green (full-workspace build + `clippy -D warnings` + step gate at each commit):

| Step | Commit | Result |
|---|---|---|
| B1a claim fence | `b5fce9b` | claim fenced at commit (memory+sqlite), tested |
| B1b push fence | `cdcf94d` | push fenced |
| B1b rest | `0ee2b83` | finalize/upsert/renew/reassign/purge threaded; finalize fenced |
| B2a library session | `c3727e8` | coordinated owner; superseded instance fenced on the data path |
| B2b policy | `7ebf7ec` | target-affinity, `Ownership` value, fence-recovery, bounded `renew_owned` |
| B3a encapsulation | `ff1d867` | `open_*` constructors + feature-gated adapters + guard test |
| B4 BQ-23 | `267f21e` | lease+storage epoch bound into one durable value on postgres, tested |
| B5 multi-instance | `11666ac` | durable two-instance fence over shared postgres (B1+B2+B4 end-to-end) |
| B3b hardening | `51490af` | doc-hidden injection ctor + ports; guard locks it |

A grounded final-alignment review confirmed: **no correctness bugs**; every ADR-009 coordination Decision
(1, 3, 4, 6) and every TD-003 In-Process Library Owner-Runtime MUST (library-is-owner, cached-acquire-epoch,
single-durable-epoch, data-path-fail-closed, target-affinity, bounded-per-node) is **implemented and
tested**. Decision 2 (encapsulation) is enforced as far as feasible — see OWED-1.

### OWED — resolution status

| # | Item | Status |
|---|---|---|
| OWED-1 | `publish=false` hard wall | **INFEASIBLE (documented)** — pqueue depends on pqueue-engine, so cargo can't publish pqueue with an unpublishable dep. Encapsulation is strong-by-default (curated surface + doc-hidden `new`/ports + `open_*` + guard), not absolute (OD-6). `B3b` (`51490af`). |
| OWED-2 | Runtime-refuse multi-owner on a non-atomic backend (N4a/OD-2) | **RESOLVED** (`0abb398`) — `with_control_plane(.., instance_id, ..)` returns `Result` and refuses a control plane whose `binds_storage_epoch()` is false; `with_control_plane_in_process` (doc-hidden) for in-process logic. The instance-id signals a durable multi-instance deployment. |
| OWED-3 | Drain split (serve in-flight / refuse new claim) | **RESOLVED** (`24b01fb`) — `renew_owned` observes a `Draining` lease; `claim` then refuses with retryable `Unavailable` while finalize/renew/push continue. |
| OWED-4 | Relational backend fence threading | **RESOLVED** (`0959645`) — both relational backends fence at `commit_command` + the claim CTE; the BQ-23 binding generalizes to bind whichever epoch column the paired backend uses; relational multi-instance test proves cross-instance visibility + claim handoff + durable fence. |
| OWED-5 | RESP server acquire-runtime (`pqueue-c33c367e`) | **OUT OF SCOPE (pre-existing)** — this was a library-only plan; the engine `route` decision exists and B1+B4 unblock the server-runtime follow-up, tracked separately. |
| OWED-6 | Coordinated `open_*` / `open_postgres` | **RESOLVED** (`2e7414b`) — `open_postgres` (sole-owner) + `open_postgres_coordinated` (builds backend + binding control plane internally) behind the opt-in `postgres` feature; no dependency cycle. |

**Remaining (newly-found, tracked):** the relational backend mints item ids from a per-connection sequence,
so two connections each *pushing* a fresh item can collide on `pqueue_items_pkey`; full concurrent
multi-writer push needs a DB-sequence-based globally-unique id (the fence, cross-instance visibility, and
claim handoff are unaffected — proven). And OWED-1/OWED-5 above (infeasible / pre-existing-out-of-scope).
