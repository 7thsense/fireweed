# fireweed Gap-Closure Plan (2026-07-08, rev 2 — post-codex + root-cause)

Restores a green, buildable, honestly-tested baseline, finishes the ADR-012 composed-backend
architecture, then closes every outstanding vision, evidence, CI, and doc gap. Sequenced by
dependency: Phase 0 unblocks all later phases. Each work item becomes one DDx bead with mechanical,
Rust-native acceptance criteria (gates are `rustup run 1.92.0 cargo build/test/clippy -p <crate>` +
named snake_case `#[test]` fns; NO `go test`, NO `lefthook`).

## Verified current state (evidence-anchored)
- `cargo build --workspace` FAILS: only `fireweed-server` pulls `rdkafka` (`Cargo.toml:30`,
  `features=["cmake-build"]`) → vendored librdkafka needs `curl/curl.h`+cmake. `--exclude
  fireweed-server` builds in ~42s.
- `cargo test --workspace` RED at HEAD. Root causes (verified, not inferred):
  - `callback_cohort_e2e` (`product_validation_tests.rs:1871`→panic 1920) and `noisy_neighbor_scale_e2e`
    (`:2157`→panic 2186): tests were pointed at `composed_sqlite_backend_in_memory()` by commit
    `d7f48cbf`; partial revert `6a34647c` missed these two. Whole-cohort claim + `discover_active_scopes`
    exist ONLY on the monolith `SqliteRelationalBackend` (`relational.rs:6598`, `:6341`); ComposedBackend
    returns `Unavailable` (`compose.rs:1976`→`claim_validation.rs:140`; DiscoveryPort empty body
    `compose.rs:3336`).
  - `claimed_item_shape_reflects_update_fields_after_reclaim` ×4: scenario (`scenarios.rs:1869`) pushes
    an item with `gate_keys`; `ComposedBackend::supports_gates()==false` → `validate_gate_command`
    returns `Unavailable` (`command.rs:147`, `compose.rs:1657`). Guard checks `is_atomic()` not
    `supports_gates()` (`lib.rs:316`). Never passed on composed families (born in `3dce8dc8`).
  - `object_log_sqlite::recovery_tests` ×3 and `performance_object_log_hybrid_tests` ×3: deterministic
    (NOT flaky); currently build-blocked by rdkafka → unverified. No git evidence of regression.
  - `change_record_sink_rejected_without_durable_cursor_store` (`server.rs:1388`): hardcoded port 9092;
    `start()` spawns the embedded broker (`lib.rs:1306`) BEFORE the durable-cursor validation
    (`change_record_sink.rs:675`), so a port collision yields the wrong error + leaks the broker task.
- Bead queue stalled: 6 open epic-parents (execution-eligible:false), 4 proposed leaves circuit-broken
  against the red tree + unconfigured `lefthook` AC.
- Toolchain: pinned 1.92.0 IS installed; invoke via `rustup run 1.92.0 cargo …` (brew 1.96 shadows PATH).

## Decisions (owner, 2026-07-08)
1. **rdkafka/librdkafka is deleted.** Embedded change-log produces IN-PROCESS to the fjord broker's
   shared log handle — no loopback socket, no wire round-trip. External-Kafka mode, if kept, uses
   pure-Rust `rskafka`.
2. **Port cohort-claim, active-scope discovery, and gate support onto `ComposedBackend`** (delegating
   to a relational-capable projection-store axis) so the composed path reaches parity and the product
   tests pass on composed backends — finishing ADR-012 "composed-only".
3. Plan covers **everything**, including scale/evidentiary epics.

---

## Phase 0 — Unblock: buildable + green tree (critical path)

### rdkafka removal (split per codex; the embedded produce path has 5 real sub-problems)

**B0.1a — Delete rdkafka dep + JSON payload path; introduce an in-process `ChangeRecordSink` seam.**
- Remove `rdkafka` from `fireweed-server/Cargo.toml`; delete `Cargo.lock` entries; `rg rdkafka crates/`
  returns zero. Replace the `FutureProducer` field/usage in `FjordChangeRecordSink`
  (`change_record_sink.rs:344,378,402`) with an in-process log handle (`Arc<dyn LogBackend>`).
- AC: `rustup run 1.92.0 cargo build -p fireweed-server` succeeds on a box with NO libcurl-dev/cmake;
  `rg -n rdkafka` finds nothing under `crates/` or `Cargo.lock`.

**B0.1b — Share the embedded broker's log handle with the sink.**
- Today `spawn_embedded_fjord_broker` builds its own `EmbeddedFjordSurface` and returns only a
  `JoinHandle` (`lib.rs:377`), and `start()` builds a separate surface it discards (`lib.rs:1217`).
  Refactor so ONE `EmbeddedFjordSurface` (its `Arc<dyn LogBackend>`) is owned by `start()` and handed
  BOTH to the broker (`HeimqServer`) AND to the sink, so appends feed the broker's consumers.
- AC: a test proves the sink and the broker share the same log (a record appended by the sink is
  visible to a broker fetch).

**B0.1c — Encode change records as valid Kafka record batches (ADR-014 consumer contract).**
- `FjordLog::append` parses bytes via `RecordBatchView::from_bytes` (`fjord-broker/src/lib.rs:436`),
  and `LogBackend::append(topic, partition, records)` (`heimq-broker storage/mod.rs:107`) — there is NO
  `TopicLog::append(bytes)`. Build a Kafka RecordBatch encoder that sets, per ADR-014 §"Normative
  consumer contract" (`ADR-014:92`): record key `"{item_id}:{backend_epoch}:{sequence}"`, headers
  (`fireweed-tenant-id`,`fireweed-queue-id`,`fireweed-item-id`,`fireweed-backend-epoch`,`fireweed-sequence`,`fireweed-command-kind`), and
  the TD-008 `ChangeRecord` JSON payload; single partition `0` per queue topic.
- Ensure topics exist before first append: `FjordLog::append` returns `TopicNotFound` if absent
  (register/create via the shared `HeimqServer` startup, `create_topics`/`register_embedded_fjord_topics`
  `lib.rs:244`).
- AC: unit test round-trips the encoder ↔ `RecordBatchView::from_bytes`; key/headers/payload/partition
  match the contract.

**B0.1d — Replace the rdkafka-based consumer assertions with a pure-Rust verification.**
- `tests/fjord_surface.rs:557` asserts partition 0, monotonic offsets, stable idempotency keys, JSON
  payload, headers via an rdkafka consumer. Rewrite those assertions using a pure-Rust consumer
  (`rskafka`) OR the broker's in-process read API — preserving every contract assertion.
- AC: `rustup run 1.92.0 cargo test -p fireweed-server` fjord-surface tests pass with zero rdkafka.

**B0.1e — External-Kafka mode + config-mode split + ADR-014 thread.**
- `ChangeRecordSinkConfig` requires an endpoint when enabled (`change_record_sink.rs:20,455,680`).
  Redesign into explicit modes: `Embedded` (in-process, default), `ExternalKafka` (opt-in, pure-Rust
  `rskafka`, feature-gated), `Http`/`Niflheim`, `Disabled`. Update ADR-014 §Decision + invariant #4 in
  the SAME phase (not deferred) to describe in-process produce + optional rskafka external seam; note
  librdkafka is removed.
- AC: config selects each mode; embedded needs no endpoint; ADR-014 matches the built reality;
  `ddx doc` graph consistent.

### Green the tree (composed-backend parity + test/prod fixes)

**B0.2 — ComposedBackend: whole-cohort / whole-group / same-group claim parity.**
- Add an explicit rich-claim-selection API to the `ProjectionStore` axis (default returns
  `Unavailable`; implemented by `SqliteRelational` porting `select_whole_cohort`/whole-group/
  same-group from `relational.rs:6598`). `ComposedBackend::claim` (`compose.rs:1975-2054`) validates
  the unit from the queue def, then for non-item units delegates to the projection-axis selection and
  **emits `QueueCommand::CohortClaim`** (not the plain `Claim` it currently always builds at
  `compose.rs:2021-2054`) with cohort response fields, so the `CohortClaim` apply arm updates
  `fireweed_cohorts` leased state (`relational.rs:2568-2592`); log-replay stores still return
  `Unavailable`.
- AC: composed-relational conformance scenarios exercise **whole-cohort, whole-group, AND
  same_group_key** claim; `callback_cohort_e2e` passes on a composed relational backend.

**B0.3 — ComposedBackend: `discover_active_scopes` parity.**
- Add an explicit discovery API to the `ProjectionStore` axis (default `Unavailable`; implemented by
  `SqliteRelational` porting `relational.rs:6341`); `ComposedBackend`'s `DiscoveryPort`
  (`compose.rs:3336-3339` empty default) delegates to it.
- AC: `noisy_neighbor_scale_e2e` passes on a composed relational backend; a discovery conformance/unit
  test asserts rollup.

**B0.4 — ComposedBackend: gate support parity + conformance guard.**
- Add `supports_gates()` reflecting the projection store's gate capability; **remove the hard-coded
  `validate_gate_command(false, …)` rejection at all three composed append sites**
  (`compose.rs:1196-1215`, `:1514-1527`, `:1656-1667`) so gate-bearing pushes/`SetGates` are accepted
  when the store advertises gates; **implement `SetGatesPort` on composed** (`compose.rs:3332-3335`
  empty default → delegate to the relational apply arm `relational.rs:3238-3260`). Keep the conformance
  scenario guarded on `supports_gates()` (skip, not fail, on non-gate backends).
- AC: `claimed_item_shape_reflects_update_fields_after_reclaim` passes on gate-capable composed backends
  and is cleanly skipped on non-gate backends; no `is_atomic()`-only guard remains for gate scenarios.

**B0.5 — Reconcile the product-validation test fixtures to composed backends.**
- Point `callback_cohort_e2e` (call site `product_validation_tests.rs:1870-1872`) and
  `noisy_neighbor_scale_e2e` (`:2155-2158`) at `composed_sqlite_relational_in_memory()` (feature-complete
  after B0.2/B0.3), NOT the monolith; update the crate imports (`product_validation_tests.rs:39-41`,
  currently `composed_sqlite_backend*` only).
- AC: both product tests pass on the composed relational backend.

**B0.6 — Fix `start()` change-record ordering + broker port.**
- Validate the durable-cursor-store config BEFORE `spawn_embedded_fjord_broker` in `start()`
  (`lib.rs:1292-1356`); abort/await the broker task on the error path (no leak). Give the embedded
  broker an ephemeral port when not operator-pinned.
- AC: `change_record_sink_rejected_without_durable_cursor_store` passes; `cargo test -p fireweed-server`
  is stable across 3 runs with default parallelism.

**B0.7 — Verify the build-blocked recovery + hybrid tests (now that fireweed-server builds).**
- Run `object_log_sqlite::recovery_tests` ×3 and `performance_object_log_hybrid_tests` ×3. If green:
  close as verified. If red: root-cause + fix (separate bead — likely real logic).
- AC: all six pass, or a follow-up fix bead is filed with the diagnosis.

**B0.8 — Phase-0 green gate.**
- AC: `rustup run 1.92.0 cargo test --workspace` (non-env-gated) green; `rustup run 1.92.0 cargo clippy
  --workspace --all-targets -- -D warnings` clean.

## Phase 1 — CI integrity & hygiene
- **B1.1** Make the enforcing gate actually block: prove `scripts/ci/pr-gate.sh --mode enforcing`
  returns non-zero on a seeded failing test; ensure the job is a required check; get main green.
- **B1.2** Export `FIREWEED_PG_TEST_URL` for the full `cargo test --workspace` CI step (not just the
  single proof test) so the ~83 postgres tests actually run against the `postgres:16` service.
- **B1.3** Add a MinIO/S3 CI lane so `segmented_s3_substrate_tests` execute (not skip).
- **B1.4** Retire Go-style ACs + `lefthook` from the DDx AC generator (Rust idioms only); decide
  `go_root_test.go`+`go.mod` (wire into CI as an artifact-assertion job + fix the non-existent
  `fireweed-kafka` reference, OR delete). No bead AC references `go test`/`lefthook` after this.
- **B1.5** Snake_case the ~53 `fn Test[A-Z]` Rust fns (or `#[allow]`), so `-D warnings` stays clean.
- **B1.6** Document reproducible dev build (toolchain 1.92.0; post-B0.1 no system libs; fjord/heimq/
  object-log dep provenance).

## Phase 2 — Finish open Fjord/ADR-014 + TD-008 (subsumes the stuck beads)
- **B2.1** Split `fjord.listen` (bind) from external `kafka.bootstrap` (require `kafka://`; reject
  schemeless). Supersedes pqueue-0f9be25f/2734d269/415824be. (Simplified by B0.1.)
- **B2.2** Wire-format/producer hygiene: stable command-kind encoding (not `Debug`); remove
  `block_in_place` hot path + the fixed ~400ms startup sleep (event-driven readiness). Supersedes
  pqueue-60f3bccc/b4992731; drop vacuous pqueue-b9cbe0b6.
- **B2.3** Wire SQLite terminal reap into `ReclaimDriver::tick` (TD-008 CL-6; retention AND emission
  cursor, opt-out = retention only). Supersedes pqueue-15937d2e/daecad34/509a70d0.
- **B2.4** Tracker cleanup: close/supersede the stuck beads, clear circuit breakers, re-home the epics
  onto these beads; `ddx bead ready` returns the intended set.

## Phase 3 — Vision evidentiary / scale epics
- **B3.1** Create the AC-TXN suites named by TP-001/TP-003 that don't exist
  (`external_transaction_contract_matrix_tests`, `fault_injection_harness_tests`), covering AC-TXN-1..7
  across all backend combinations + evidence JSONL.
- **B3.2** Durable-backend queue density (1000 queues) on object_log_sqlite + postgres (not just
  in-memory) + evidence JSONL; remove the in-memory-only caveat.
- **B3.3** Re-measure cross-queue TP-002 E2 at full scale post-ADR-008 + evidence; update TP-002.
- **B3.4** Postgres production hardening: server-wire under tokio (spawn_blocking + pool + row-locking
  past the blocking-executor caveat); kind-helm smoke for postgres/postgres + postgres/sqlite combos;
  TLS/Lakebase seam (NoTls → TLS).
- **B3.5** Resolve API-004 hot-projection-query-surface status (cancelled): mark the contract
  Superseded/Descoped with a pointer, or implement — consistent with the tracker.
- **B3.6** (ADR-012 completion) Retire the `SqliteRelationalBackend` monolith once B0.2–B0.4 give the
  composed path parity, OR record why it's kept — so there's a single owner of relational features.

## Phase 4 — Docs & tracker reconciliation
- **B4.1** DEPLOYMENT-READINESS ↔ code: reflect postgres wired + `CommitTransitionPort` implemented;
  fix the four "open" beads that are actually closed.
- **B4.2** Single release-version source of truth (Cargo 0.9.0 vs v0.2.x vs v0.3.0).
- **B4.3** Banner PHASE-7-reconciliation + OWED-resolution-plan as historical (hexagonal era); resolve
  the documented false-close (pqueue-1515d288).
- **B4.4** helix-evolve: thread the rdkafka→in-process + composed-parity changes through ADR-014/ADR-005/
  ADR-012/TD-008 so the artifact stack matches the built reality.

## Execution order & method
Phase 0 first, B0.1a→e before B0.7 (fireweed-server must build to verify). B0.2/B0.3/B0.4 are real engine
work — each gets a fresh-eyes/codex review before commit. Within a phase beads are largely independent
(parallel sub-agents). Postgres/S3 ACs run against docker-compose (`postgres:16`) + a MinIO container.
Gate every commit on `rustup run 1.92.0 cargo test`/`clippy` for the touched crate(s), full-workspace
green before closing a phase.
