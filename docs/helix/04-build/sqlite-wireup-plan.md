# Plan: Wired-up SQLite backend (TD-005 / ADR-006) — v2 (post adversarial review)

Scope decisions (locked with user):
- **B6 = pqueue-side only**: land tasks 1–3 + the embedder delivery-adapter conformance
  suite (bead `pqueue-9ff01321`). The 7snx edit + git-rev bump (bead `pqueue-a4846118`)
  is **deferred but ON the critical path** — see "B6 honesty caveat" below.
- **B4 = shared generic harness**, instantiated for memory + sqlite.
- **Durability = `synchronous=FULL` default** (TD-005), with a `normal` knob.

Beads under epic `pqueue-a7c02fd2`: Task1→`a18df4c5`, Task2→`87d98fdc`,
Task3→`3b6f857e`, Task4→`9ff01321`.

### Verified current state (with file:line)
- `SqliteLogStore`/`SqliteProjectionStore`/`SqliteControlPlaneStore` each own a SEPARATE
  `Mutex<Connection>` → append + apply are separate txns today.
- TWO projections exist: `SqliteProjection` (lib.rs:34 — group/cohort, owns
  `pqueue_applied_position(id,sequence)` lib.rs:350) and `SqliteProjectionStore`
  (projection.rs:26 — full item lifecycle, NO applied-position table). **The wired backend
  composes `SqliteProjectionStore` (projection.rs).** TD-005's "reuses SqliteProjection"
  wording is stale; TD-005 will be amended to name `SqliteProjectionStore`.
- Epoch bug: control plane assigns shard `epoch 1` (control_plane.rs:72-80); log defaults
  a new shard to `epoch 0` (log.rs:97) and lazily self-registers at `epoch 0` (log.rs:109).
- `batch_claim` BOTH selects AND leases AND increments `attempts` (projection.rs:111-127);
  the `BatchClaim` apply arm ALSO leases + increments (projection.rs:222-241). Applying a
  `BatchClaim` command to already-`batch_claim`'d rows double-increments `attempts`.
- `BackendProfile` (runtime.rs:46) is a config/readiness enum ONLY — the service never
  constructs a store from it (no `LogStore`/`ProjectionStore` use in pqueue-service/src).
- Exhaustive `match backend_profile` at runtime.rs:285-291 and :314-330; ledger validator
  rejects unknown profiles at verification_ledger.rs:415 (`_ => Err`); two-profile strings
  in `UnsupportedBackendProfile` Display (runtime.rs:197), `help_text` (runtime.rs:921),
  doc comment (runtime.rs:42). Scale/attestation matrices hardcode the 2 committed
  profiles (product_validation_tests.rs:66, invariant_stress_matrix_tests.rs:59,
  seventh_sense_validation_tests.rs:10, recurrence_scale_both_profiles_tests.rs:9).
- 7snx (`/Users/erik/Projects/7snx`) pins pqueue by git rev; depends on
  `pqueue-core/-postgres/-storage` only; `PqueueDeliveryQueue.commit()` does append THEN
  apply as two awaits (lib.rs:752-760); `claim_batch_at` calls `batch_claim` then commits
  a `BatchClaim` (lib.rs:642-666) — the double-increment pattern above; dedupe by
  `client_item_key` is adapter-level (`pushed_client_keys`, lib.rs:545,603), NOT pqueue.

---

## Task 1 — Unified single-txn `SqliteBackend` (bead a18df4c5)

**1a. Refactor: extract shared-transaction cores (behavior-preserving).**
- `log.rs`: extract `append_batch` body → `pub(crate) fn append_into_tx(tx: &Transaction,
  shard, expected_epoch, commands) -> Result<AppendBatchResult, LogStoreError>`.
  `SqliteLogStore::append_batch` = lock → `conn.transaction()` → core → `commit()`.
  Keep log.rs:97/109 epoch-0 semantics EXACTLY (standalone store + existing log tests must
  stay green: storage_conformance.rs:329 expects `current 0`).
- `projection.rs`: extract the apply loop → `pub(crate) fn apply_into_tx(tx: &Transaction,
  shard, commands) -> Result<(), ProjectionError>`. `apply_committed` calls it in its tx.
- **Unified error**: define `pub enum SqliteBackendError { Log(LogStoreError),
  Projection(ProjectionError), ControlPlane(ControlPlaneError), Storage(String) }` with
  `From` impls; `SqliteBackend` methods return it.

**1b. `backend.rs`: `SqliteBackend`** — one `Mutex<Connection>`, union schema (log +
projection.rs + control plane). NO new `pqueue_applied_position` table (avoids the lib.rs
collision; not needed — see recovery).
- `open(path, SqliteDurability)`: PRAGMA WAL + synchronous (FULL default / NORMAL knob) +
  busy_timeout; init union schema; **acquire single-writer ownership** (see 1e); NO replay
  needed (see 1d).
- `create_queue(def)`: control-plane insert + **epoch bootstrap in the SAME tx** — insert
  `pqueue_log_shard(epoch=1)` for each shard so log & control-plane agree (fixes 0-vs-1).
- `append_and_apply(shard, expected_epoch, commands) -> Result<AppendBatchResult, _>`
  (**headline**): one tx → `append_into_tx` → `apply_into_tx(shard, commands)` → single
  `commit()` (one WAL fsync). Strict read-after-write. Synchronous body, `!Send` guard held
  with NO `.await` → future stays `Send` (matches log.rs:91-93 invariant).
- **`claim(shard, max_items, lease_token, now, expires) -> Vec<ItemId>`** (resolves the
  double-lease BLOCKER): in ONE tx, SELECT eligible pending ids (read-only, no mutation),
  build a `BatchClaim` command, `append_into_tx` + `apply_into_tx` it (leases + increments
  `attempts` exactly ONCE), commit. **`SqliteBackend`'s public surface OMITS `batch_claim`
  entirely** — `claim` is the only claim entry point, so the leasing-then-re-apply
  double-increment is structurally unreachable for embedders (not merely discouraged by
  prose). `SqliteProjectionStore::batch_claim` stays (standalone trait conformance +
  object-log mode) but is not on the unified backend surface.
- `metrics`, `read_from`: read on the shared connection.
- **finalize / renew-lease / expire**: thin helpers on `SqliteBackend` that build the
  `BatchFinalize` / `BatchRenewLeases` / `LeaseExpired` envelope and route through
  `append_and_apply` (so the embedder need not hand-build envelopes the way 7snx does at
  7snx lib.rs:670-707). Settle this surface in Task 1 so Task 4 / the future 7snx adapter
  build on a stable contract.

**1c. Epoch on open** (TD-005:98-99 "bump on open"): bump each existing shard's epoch in
lockstep across `pqueue_log_shard` AND `pqueue_shard_assignment` within `open`'s tx.
Rationale documented: single-writer means this is observability/restart-fencing, not CAS.
`append_and_apply` accepts `expected_epoch: Option<u64>` (None = no fence, matching the
embedder pattern); callers that fence read current epoch from the control plane.

**1d. Recovery — resolved as "no replay needed".** Because append+apply commit atomically
in ONE file, the persisted projection is ALWAYS current with the log on reopen; there is no
committed-but-unapplied window. So: NO applied-position table, NO replay loop. The
"reopen-recovery" guarantee = reopen reads the persisted projection directly. **TD-005 will
be amended** (lines 122-126) to state recovery is unnecessary under single-file atomic
commit, replacing the replay-tail wording. This removes untestable dead code the reviewers
flagged.

**1e. Single-writer ownership enforcement** (TD-005:99-101,146 — was missing): `open`
acquires an exclusive lock so a second opener is rejected (WAL alone permits concurrent
writers, so this is explicit). Mechanism left to implementation (e.g.
`PRAGMA locking_mode=EXCLUSIVE` holding the lock, or an advisory lock file) + an ownership
row for observability. **Gate: a test where a second `open()` on the same path returns an
error.**

**Tests (pqueue-sqlite unit):** atomic read-after-write; file-backed reopen preserves
push/claim/finalize state; epoch bootstrap (no `StaleEpoch` after create_queue);
`claim` increments `attempts` exactly once (parity with memory); rollback atomicity (apply
failure → no orphan log row); second-open rejection.

## Task 2 — B3: wire `sqlite` BackendProfile (bead 87d98fdc) — config plumbing only

**Explicit scope**: the service does NOT yet construct/serve a storage backend from any
profile; this task adds parse/config/readiness plumbing only (it does NOT make the HTTP
service serve sqlite traffic). Stated so "done" isn't mistaken for a served backend.
- `BackendProfile::Sqlite`; `as_str()="sqlite"`; `parse("sqlite")`.
- Update ALL two-profile contract surfaces: `UnsupportedBackendProfile` Display
  (runtime.rs:197), `help_text` (runtime.rs:921), doc comment (runtime.rs:42), and the
  tests asserting those strings (runtime.rs:1014,1035).
- Fill BOTH exhaustive matches (runtime.rs:285-291, :314-330) with a `Sqlite` arm.
- New `SqliteRuntimeConfig { db_path: PathBuf, synchronous }` from `PQUEUE_SQLITE_DB_PATH`
  (required) + `PQUEUE_SQLITE_SYNCHRONOUS` (full|normal, default full).
- `ReadinessCheck::Sqlite(..)` arm (db path openable / parent writable).
- **Ledger validator: NO change needed** (corrected in review). The profile checks in
  verification_ledger.rs are suite-scoped (only the object-log / scale / attestation suites,
  e.g. `validate_object_log_e3_row` gated by `suite=="object_log_commit_recovery_tests"` at
  verification_ledger.rs:234); there is no general profile allowlist every row passes
  through. Since sqlite is excluded from those matrices (below), no validator edit applies —
  verify no `validate_semantics` branch fires for any sqlite suite.
- **Decision — sqlite is NOT added to the scale/attestation evidence matrices** (those stay
  postgres_native + object_log_sqlite_projection per BUILD-001). The hardcoded 2-profile
  arrays/asserts are intentionally left as-is; documented so it's a choice, not an omission.
- Update `docs/helix/04-build/DEPLOYMENT-READINESS.md` supported-backends (3rd durable).
- Tests: parse/as_str/from_getter (path required, synchronous default + invalid value).

## Task 3 — B4: shared conformance against sqlite (bead 3b6f857e)

- **Shared harness location — implementer's choice, no feature-gating on production
  crates** (no tokio/test-support leak into release builds). Preferred: a shared
  `tests/support` harness module (matches the existing repo pattern, e.g.
  `crates/pqueue-service/tests/support/`) reused across crates via `#[path]` include; fall
  back to a dev-only `crates/pqueue-conformance` crate only if cross-crate include proves
  awkward. Either way the harness is generic over a small `ConformanceBackend` factory
  exposing create_queue / append_and_apply / claim / metrics / read_from (one logical
  backend; sqlite's log+projection share a connection only via the unified backend → Task 3
  depends on Task 1).
- Provide two adapters: `MemoryConformanceBackend` (wraps the three Memory stores) and
  `SqliteConformanceBackend` (wraps `SqliteBackend`). **Both adapters' `claim` MUST be
  single-increment** (select read-only → append+apply one `BatchClaim`); the memory backend
  has the SAME double-increment shape (memory.rs:308 + 229), so the adapter must NOT mimic
  7snx's current `batch_claim`+`commit(BatchClaim)` two-step. (Note: this means the new
  `claim` contract corrects a latent double-increment in 7snx's current memory path — out of
  scope to fix in 7snx here.)
- **Do NOT re-point the existing `storage_conformance.rs` memory tests** (avoids the
  gratuitous-churn / coverage-regression risk R2 flagged). Instead: (a) run the shared
  harness against BOTH adapters in the new crate (proves the harness is faithful to memory
  AND that sqlite reaches parity), keeping the legacy memory suite untouched as a
  regression anchor.
- `crates/pqueue-sqlite/tests/sqlite_conformance.rs` (or in the conformance crate) adds the
  file-backed reopen-recovery case.
- Confirm parity points the reviewers named: unknown-shard `claim` → `QueueNotFound`
  (projection.rs:70 vs memory); `attempts` single-increment; which cases run file-backed
  (recovery/durability) vs `:memory:` (logic).

## Task 4 — B6 pqueue-side: embedder delivery-adapter conformance (bead 9ff01321)

**Acknowledged: this includes designing a small public embedder surface, not just a test.**
- Design + export an embedder-facing surface on `SqliteBackend` an embedder can drive like
  Memory: create_queue, `append_and_apply` push/finalize helpers, `claim`, `metrics`,
  `summary`. Method/error contract reviewed as part of this task (depends on Task 1).
- Embedder delivery-adapter conformance suite against sqlite: push → claim → finalize;
  retry + expired-lease re-pending; terminal-failure semantics; **idempotent re-push by
  `item_id`** (pqueue's actual guarantee). Add a TP-001 coverage row.
- **client_item_key**: cite 7snx evidence (dedupe is adapter-level: `pushed_client_keys`,
  7snx lib.rs:545,603). pqueue dedupes by `item_id` only (projection.rs:203). Bead 9ff01321
  says "convergence by client_item_key" — FLAG to the owner (do not silently narrow): the
  pqueue suite tests item_id idempotency; client_item_key convergence is the adapter's
  responsibility. Recommend amending the bead text or adding an explicit adapter-layer row.

**B6 honesty caveat**: pqueue-side closure does NOT satisfy TD-005's completion evidence
"the embedder delivery-adapter conformance suite passes on the `sqlite` backend" /
durability-on-restart FOR 7snx — that only manifests when 7snx's `PqueueDeliveryQueue`
switches off Memory (bead `a4846118`), which requires pushing pqueue + bumping the git rev.
That bead remains REQUIRED for full B6 acceptance; this plan explicitly leaves it open.

**7snx integration shape (for Task 4 surface design)**: `SqliteBackend` is ONE unified
object with inherent `append_and_apply`/`claim` methods — it does NOT expose separable
`log`/`projection` trait fields the way 7snx's Memory `PqueueDeliveryQueue` is built
(7snx lib.rs:544-545). So the future 7snx Sqlite variant (bead a4846118) is a PARALLEL
struct calling `SqliteBackend::claim()`, not a field swap. Design Task 4's surface to that
shape now so the deferred adapter can adopt it without rework.

## TD-005 doc amendments (must re-stamp ddx metadata)
The plan changes TD-005 semantics, so edits MUST be paired with a ddx review re-stamp
(TD-005 frontmatter carries `self_hash` + `reviewed_at`; editing the body without
re-stamping leaves the doc in stale-hash drift the ddx gate flags). Edits:
- Recovery (TD-005:122-126): replace replay-tail wording with "no replay needed under
  single-file atomic append+apply".
- **Conformance list (TD-005:108)**: remove/redefine the "replay" dimension so it does not
  contradict the no-replay decision (reviewer caught this internal contradiction).
- Naming: clarify the wired backend composes `SqliteProjectionStore` (projection.rs), not
  the lib.rs `SqliteProjection`.
- Retention (TD-005:127-128): mark as deferred future work (see below).
- Re-run the ddx stamp for TD-005 after edits.

## Out of scope (deferred, with TD-005 amendments)
- **Retention/pruning of `pqueue_command_log`** (TD-005:127-128): no retention wiring
  exists; the new log table is unbounded as built. Defer via a new follow-up bead; amend
  TD-005 to mark retention as future work. Durability/reopen do not depend on it.
- Snapshot store (TD-005 says optional/no-op for v1) — unchanged.

## Gates
Each task: implement → `cargo fmt` + `cargo clippy --all-targets -D warnings` + targeted
`cargo test` → self-review → proceed. Final: full workspace `cargo test` + clippy.
