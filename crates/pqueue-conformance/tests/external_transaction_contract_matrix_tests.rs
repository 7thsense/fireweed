//! `external_transaction_contract_matrix_tests` (TP-001 lines 148/199, TP-002 line 222) — the
//! API-001 external-transaction contract (TP-003 §3.10, AC-TXN-1..7) run across backend PROFILES under
//! fault injection, using the reusable [`pqueue_conformance::fault`] harness.
//!
//! Each row is a REAL run: the scenario functions drive live backends through the [`Backend::write`]
//! commit seam and the [`PushPort::push_with_request_id`] idempotency path, simulate process kills by
//! drop+reopen of durable state, and assert the invariant. Every row (pass / n-a / documented-gap) is
//! written to `docs/perf/evidence/tp003-ac-txn-matrix.jsonl` so the evidence reflects exactly this run.
//!
//! # Honest coverage map (see per-row `assertions`/`detail` in the evidence JSONL)
//!
//! | AC | memory | sqlite-log | sqlite-relational | objectlog | objectlog+sqlite | postgres (env) |
//! |----|--------|-----------|-------------------|-----------|------------------|----------------|
//! | AC-TXN-1 durable+visible (per-op reopen) | n/a (non-durable) | ✓‡ | ✓ (all 8 ops) | ✓‡ | ✓‡ | ✓‡ |
//! | AC-TXN-2 rejection no-effect | ✓§ (in-proc; restart n/a) | ✓§ | ✓§ | ✓§ | ✓§ | ✓§ |
//! | AC-TXN-3 unknown-outcome replay | ✓‖ | partial¶ | n/a (unified store, no cut window) | ✓‖ | ✓‖ | partial¶ |
//! | AC-TXN-4 objectlog crash-point matrix | — | — | — | ✓ (5 substrate + 2 composed cut points)* | — | — |
//! | AC-TXN-5 hybrid-strict poison + replay | — | — | — | | ✓ (real hybrid-strict server write path)† | — |
//! | AC-TXN-5A hybrid-async success barrier | — | — | — | | partial (projection cut points)† | — |
//! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | | — |
//! | AC-TXN-7 latency-bound invariance | — | — | — | pass (AC-TXN-1/2/3 + AC-TXN-6 parity across force-seal vs group-commit)^ | | — |
//!
//! AC-TXN-3's request_id-replay-across-restart is a REAL assertion on EVERY durable profile (atomic AND
//! eventual-apply): `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log for
//! both durability classes (this suite's B3.1 run closed the earlier atomic-composed-log gap in
//! `crates/pqueue-engine/src/compose.rs`).
//!
//! Status semantics: a row is `pass` when every op the backend actually SUPPORTS has its checkpoint and only
//! capability-N/A clauses remain (an op the backend genuinely cannot perform — a class/capability property);
//! a row is `partial` only when a coverage-`GAP` remains (a SUPPORTED requirement the suite does not yet
//! exercise). See `record` below. `pass` never covers an untested supported requirement.
//!
//! `‡` AC-TXN-1 (row 206) checkpoints kill-after-success for ALL eight named mutating ops — CreateQueue,
//! BatchPush, BatchUpdate, SetGates, BatchClaim, BatchRenewLeases, BatchFinalize, PurgeItems. `sqlite_relational`
//! (atomic AND gate-capable) exercises EVERY op for real, so it is an unqualified `pass`. The other durable
//! profiles are also `pass`, with the ops they genuinely cannot perform recorded as capability-N/A (NOT a
//! coverage gap): SetGates needs a gate-capable backend, so the non-gate log/hybrid profiles
//! (`supports_gates()==false`, gate state being a relational-only feature) record it capability-N/A; and
//! BatchUpdate is atomic-class only, so the eventual-apply objectlog / object_log_sqlite profiles (which
//! return `Unavailable`) record it capability-N/A. `memory` is non-durable so kill/restart is `n/a` wholesale.
//!
//! `§` AC-TXN-2 (row 207) now drives the FULL rejection-class surface — per-item-invalid finalize,
//! unknown-id renew, request-id-conflict, envelope-invalid batches (charset-invalid group_key +
//! structurally-invalid group_batching), stale-lease/operator-fenced conflict, capacity/batch-limit
//! (`BatchTooLarge`) + unavailable (upsert on eventual-apply) paths, and the commit-timeout/abort path. The
//! pure-reject classes each assert 0 durable commands + 0 visible effect (re-verified after restart+replay on
//! the durable profiles) while an accepted sibling keeps normal success (a validly-leased sibling finalizes).
//! The commit-timeout path is the DANGEROUS one and is modelled at the real append→apply window
//! (`CutPoint::AfterAppendBeforeApply`): the command lands durably but its projection apply never runs
//! (unknown outcome, 0 half-applied in-process), and on drop+reopen recovery replays the durable tail EXACTLY
//! ONCE (item committed once, 0 duplicate/partial state transitions) — the same contract AC-TXN-3 proves.
//! Two capability-N/A clauses (never a silent pass): upsert->Unavailable cannot occur on the ATOMIC profiles
//! (upsert is available there — the `BatchTooLarge` capacity path covers capacity), and the append→apply
//! commit-timeout window does not exist on the UNIFIED relational store (`sqlite_relational`: stage-only
//! append, append+apply in one transaction — same N/A as AC-TXN-3); on `memory` the restart/recovery
//! re-verifications are capability-N/A (non-durable). So every row is `pass` (no coverage-GAP). The standalone
//! `ac_txn_2_*_has_no_durable_effect` tests exercise each class per-profile (the commit-timeout test asserts
//! the composed-log profiles achieve real exactly-once recovery, not merely a no-effect abort).
//! `‖`/`¶` AC-TXN-3 (row 208): this engine has exactly TWO request_id-bearing mutating ops — PUSH
//! (`push_with_request_id`) and `commit_transition` (the authoritative claimed-work commit). PUSH request_id
//! exactly-once replay is now proven at ALL FOUR cut points, including the previously-item-level mid-pipeline
//! `AfterAppendBeforeApply` cut, which `RequestIdReplayProbe::build_request_id_push_envelope` makes
//! request_id-bearing: the durable-but-unapplied envelope carries the request_id, and recovery rebuilds the
//! push-idempotency map from it so a retry by request_id replays the one committed result (0 duplicate
//! transitions). The classic ports (claim/renew/finalize/update_fields/purge/replace_if_pending) carry NO
//! request_id (dedup is item/lease/version based) → capability-N/A, covered by AC-TXN-1 durability + AC-TXN-6
//! parity. `‖` `memory`/`objectlog`/`object_log_sqlite` are `pass`: memory (non-durable) proves the in-proc
//! PUSH + commit_transition replays with the restart cuts capability-N/A; the eventual-apply objectlog family
//! covers PUSH at all four cuts and records commit_transition capability-N/A (`Unavailable` — no atomic
//! transition boundary). `¶` `sqlite_log`/`postgres` are `partial`: they are durable AND atomic, so
//! commit_transition IS a supported request_id-bearing op there, and recovery now rebuilds the
//! commit-transition idempotency record from the durable log (`rebuild_commit_idempotency_from_log`, the
//! symmetric twin of the push rebuild — the committed per-entry `EntryRecovery` is reconstructed from the
//! durable commit envelopes and the whole-body fingerprint is the one stamped on them at commit time). So an
//! ALL-COMMITTED commit_transition's cross-restart request_id replay is PROVEN at BOTH restart cut points —
//! `AfterApplyBeforeResponse` (commit fully, kill, reopen) and `AfterAppendBeforeApply` (append the
//! request_id-bearing commit envelope via `build_request_id_commit_envelope`, kill before apply, reopen): a
//! same-body retry replays the exact per-entry outcome, a different body → RequestIdConflict, and the input is
//! finalized exactly once (0 duplicate). The residual `partial`: a MIXED committed+rejected commit is NOT
//! faithfully replayed across restart — a rejected entry mutates/appends nothing durable, so recovery can only
//! reconstruct the committed entries (a short vec). The engine does NOT silently replay it: the replay path
//! guards on `recovery.len() == body.len()`, so a mixed retry safely RE-EXECUTES (committed input stays
//! finalized exactly once, 0 duplicate) instead of returning a misleading short outcome. That honest residual
//! is recorded as a `GAP` (→ `partial`), tracked in pqueue-db60657d (faithful mixed replay needs durable
//! rejection records, a deferred wire-format change). (`sqlite_relational` stays
//! `n/a`: its unified store couples append+apply in one transaction, so there is no mid-pipeline cut window.)
//!
//! `*` AC-TXN-4 (`pass`) covers TP-003 §3.10 row 209 at BOTH architectural layers. The 5 substrate-internal
//! instants drive [`pqueue_objectlog::ObjectLog`]'s `LogStore` impl DIRECTLY (bypassing `ComposedBackend`)
//! with the [`pqueue_objectlog::FaultHook`] seam, striking cut points strictly INSIDE the segmented
//! substrate's own commit pipeline that the public `Backend::write` seam cannot reach: `BeforeSegmentWrite`,
//! `AfterSegmentWriteBeforeManifest`, `AfterManifestBeforeAck` (whose "0 duplicate active leases" clause
//! replays the recovered log through a fresh projection and asserts exactly one ACTIVE lease in the projected
//! serving image, not just one durable Claim log command), `DuringOwnerReassignment`, `DuringSnapshotWrite`.
//! The two COMPOSED-LAYER projection-apply instants row 209 also names — "during projection apply" and "after
//! projection apply before response" — live one layer up in `pqueue-engine`'s `ComposedBackend` (which applies
//! a batch only after `LogStore::append` returned `Ok`), so they need the engine's [`pqueue_engine::ComposeFaultPoint`]
//! seam (`DuringProjectionApply` / `AfterApplyBeforeResponse`): `ac_txn_4_composed_projection_apply_crash`
//! strikes each against the composed OBJECTLOG backend through the `Backend::write` unit-of-work seam and, on
//! drop+reopen recovery, asserts the row-209 outcomes on the RECONSTRUCTED PROJECTED STATE (3 accepted items
//! preserved, the faulted Claim+Push replay EXACTLY ONCE → projected `leased`/`pending` counts not log-row
//! counts, 0 duplicate active leases, stale-epoch commits EpochFenced). ("During manifest CAS" collapses into
//! the atomic create-only PUT already bracketed by the two manifest cut points.) So all 7 named cut points are
//! genuinely struck — no remaining coverage `GAP`.
//!
//! `†` AC-TXN-5/5A add the analogous seam on the PROJECTION side —
//! [`pqueue_sqlite::HybridFaultHook`] on `HybridProjectionStore` — for the instants the public seam cannot
//! isolate: a fault strictly between the durable SQLite commit and the in-memory apply
//! (`AfterSqliteCommitBeforeMemoryApply`), a memory-apply failure (`DuringMemoryApply`), one strictly before
//! the durable SQLite apply (`BeforeSqliteApply`), and one strictly inside the deferred async SQLite
//! checkpoint (`DuringAsyncSqliteApply`, installed + triggered via `flush_deferred` in AC-TXN-5A). AC-TXN-5 is
//! now `pass`: bead pqueue-da1965d7 WIRED the `objectlog/hybrid-strict` server profile
//! (`PQUEUE_PROJECTION_BACKEND=hybrid-strict`, `HybridProjectionStore::with_strict_apply`), and
//! `ac_txn_5_hybrid_strict_poison_on_real_server_path` drives all four clauses (SQLite-failure/no-success +
//! tail replay, SQLite-commit-then-memory-fail poison fail-closed, restart rehydration from the SQLite
//! ProjectionImage, request-id replay/conflict) through that real group-commit composed write pipeline
//! (`apply_live_owned` → strict `apply_durable_then_memory`) — closing the prior real-server-path GAP. The
//! `ac_txn_5_hybrid_strict_poison_replay_scenario` row remains as the direct ProjectionStore-layer companion.
//! AC-TXN-5A stays `partial` on ONE remaining caveat — backpressure: its fail-closed clause is proven against
//! the standalone [`pqueue_sqlite::HybridAsyncMonitor`], but pqueue-server opens `HybridProjectionStore`
//! WITHOUT wiring that monitor/thresholds into the composed write path (the `hybrid-async` arm merely logs the
//! resolved thresholds), so TD-004's hard-debt admission/high-water/retention gate is NOT yet enforced
//! end-to-end on the server (tracked follow-up), recorded as an explicit `GAP` assertion in that row.
//!
//! `^` AC-TXN-7 (row 213): the commit-latency bound is not a correctness knob. `ac_txn_7_latency_sweep_scenario`
//! repeats the transaction-contract invariant-bearing scenarios across the objectlog composition's two
//! commit-latency-bound WRITE REGIMES — force-seal (`ObjectLog::open`, synchronous seal-per-append) vs
//! group-commit (`ObjectLog::open_group_commit`, `SegmentConfig::new(1,1)`, co-buffered ack-after-seal) — and
//! asserts 0 invariant delta. It sweeps AC-TXN-1 (success-visible), AC-TXN-2 (rejection-no-effect) and
//! AC-TXN-3 (unknown-outcome replay) — the exact triad TP-002 E3 row 204 names as the transaction invariants
//! required "under the same bound sweep" — plus the AC-TXN-6 parity run DIRECTLY across the two regimes (its
//! observable-state teeth: identical final visible metrics, `select_eligible` order, pending/active-lease set,
//! per-item terminal-outcome records, and per-request_id idempotency behavior). AC-TXN-4 (object-log-SUBSTRATE
//! crash-point matrix, force-seal-pipeline-specific cut points) and AC-TXN-5/5A (hybrid-projection family,
//! group-commit-substrate-only) are capability-N/A for a cross-regime comparison (NOT a GAP) and stay covered
//! at their native settings. The numeric ≥4-bound latency sweep of E3 row 198 (1/5/20/100 ms) is a
//! latency/COST performance benchmark (it needs a runtime flusher driving `flush_tick`; the runtime-free
//! transaction-contract scenarios ack synchronously) measured by `performance_object_log_e3_live_tests` /
//! `composed_group_commit`, where the numeric bound changes ack timing, never what commits.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pqueue_conformance::fault::{
    AcEvidence, AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
    ac_txn_3_commit_transition_request_id, ac_txn_3_mid_pipeline_request_id_bearing,
    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, durable_command_count, write_evidence,
};
use pqueue_core::{ClientItemKey, RequestId};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, CommandPosition, ComposeFaultHook, ComposeFaultPoint,
    ComposedBackend, ControlPlaneStore, EngineError, InProcessControlPlane, LogStore,
    ProjectionRead, ProjectionSnapshot, ProjectionStore, PushCommand, PushPort, QueueCommand,
};
use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
use pqueue_sqlite::{
    BackpressureLevel, HybridAsyncThresholds, HybridFaultCutPoint, HybridFaultHook,
    HybridProjectionStore,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A base directory unique to THIS process+profile-instance. The tag-keyed factories below join the
/// per-scenario `tag` onto this base, so reopening with the same tag recovers the same durable store
/// while different tags (independent AC-TXN cut-point phases) get isolated stores.
fn base_dir(profile: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-ac-txn-{profile}-{}-{n}",
        std::process::id()
    ))
}

fn sqlite_log_factory() -> impl Fn(&str) -> pqueue_sqlite::ComposedSqliteBackend {
    let base = base_dir("sqlite");
    move |tag: &str| {
        let path = base.join(format!("{tag}.db"));
        std::fs::create_dir_all(&base).ok();
        pqueue_sqlite::composed_sqlite_backend(path.to_str().unwrap())
            .expect("open composed sqlite-log")
    }
}

/// The composed sqlite-RELATIONAL backend (unified sqlite log + relational projection): atomic durability
/// class AND gate-capable (`supports_gates()==true`, `SqliteRelational` materializes the gate tables). It is
/// the profile that genuinely exercises BOTH the atomic-only op (BatchUpdate) and the gate-only op (SetGates)
/// under kill/reopen — the log-replay `sqlite_log` and the eventual-apply objectlog profiles can do neither.
/// File-backed with reopen-same-path recovery, so the drop+reopen "process kill" simulation works.
fn sqlite_relational_factory() -> impl Fn(&str) -> pqueue_sqlite::ComposedSqliteRelationalBackend {
    let base = base_dir("sqlite-relational");
    move |tag: &str| {
        std::fs::create_dir_all(&base).ok();
        let path = base.join(format!("{tag}.db"));
        pqueue_sqlite::composed_sqlite_relational(path.to_str().unwrap())
            .expect("open composed sqlite-relational")
    }
}

fn objectlog_factory() -> impl Fn(&str) -> pqueue_objectlog::ComposedObjectLogBackend {
    let base = base_dir("objectlog");
    move |tag: &str| {
        pqueue_objectlog::composed_objectlog_backend(base.join(tag))
            .expect("open composed objectlog")
    }
}

fn objectlog_group_commit_factory() -> impl Fn(&str) -> pqueue_objectlog::ComposedObjectLogBackend {
    let base = base_dir("objectlog-gc");
    move |tag: &str| {
        pqueue_objectlog::composed_objectlog_backend_group_commit(
            base.join(tag),
            SegmentConfig::new(1, 1).unwrap(),
        )
        .expect("open composed objectlog group-commit")
    }
}

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

fn objectlog_sqlite_factory() -> impl Fn(&str) -> HybridBackend {
    let base = base_dir("objectlog-sqlite");
    move |tag: &str| {
        let root = base.join(tag);
        std::fs::create_dir_all(&root).ok();
        let sqlite = root.join("projection.sqlite");
        ComposedBackend::new(
            ObjectLog::open_group_commit(&root, SegmentConfig::new(1, 1).unwrap())
                .expect("open object log"),
            HybridProjectionStore::open(sqlite.to_str().unwrap()).expect("open hybrid projection"),
            InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid")
    }
}

const DURABLE: TxnCaps = TxnCaps {
    durable_reopen: true,
};
const NON_DURABLE: TxnCaps = TxnCaps {
    durable_reopen: false,
};

/// Record one AC-TXN outcome into the evidence buffer, tracking failures for the final assertion.
///
/// Two distinct concepts are kept apart so status never overclaims coverage yet never penalises a backend
/// for a capability it cannot have:
/// * **coverage-GAP** — the backend SUPPORTS an op/cut-point but the suite does not exercise it. Any `GAP`
///   assertion forces the row to `partial`. This is the honesty gate: a `pass` never covers an untested
///   SUPPORTED requirement.
/// * **capability-N/A** — the backend genuinely cannot perform the op (a class/capability property, e.g.
///   BatchUpdate is atomic-class-only so eventual-apply profiles return `Unavailable`; SetGates needs a
///   gate-capable backend; a non-durable profile cannot kill/restart). A `capability-N/A` assertion is a
///   truthful declaration, NOT a coverage hole, so it does NOT force `partial`: a row is still `pass` when
///   every op the backend actually supports is exercised.
///
/// So a row is `partial` iff it carries a coverage-`GAP`; capability-`N/A` clauses are recorded verbatim for
/// audit but do not downgrade a row that otherwise fully exercises its supported surface.
fn record(
    records: &mut Vec<AcEvidence>,
    failures: &mut Vec<String>,
    ac: &'static str,
    backend: &str,
    outcome: Result<Vec<String>, String>,
) {
    match outcome {
        Ok(assertions) => {
            let partial = assertions.iter().any(|a| a.contains("GAP"));
            records.push(AcEvidence {
                ac,
                backend: backend.to_string(),
                result: if partial { "partial" } else { "pass" },
                detail: String::new(),
                assertions,
            });
        }
        Err(reason) => {
            failures.push(format!("{ac} [{backend}]: {reason}"));
            records.push(AcEvidence {
                ac,
                backend: backend.to_string(),
                result: "fail",
                detail: reason,
                assertions: vec![],
            });
        }
    }
}

fn record_na(records: &mut Vec<AcEvidence>, ac: &'static str, backend: &str, detail: &str) {
    records.push(AcEvidence {
        ac,
        backend: backend.to_string(),
        result: "n/a",
        detail: detail.to_string(),
        assertions: vec![],
    });
}

// ---------------------------------------------------------------------------
// AC-TXN-4: object-log-internal crash-point matrix (TP-003 §3.10 row 209)
// ---------------------------------------------------------------------------
//
// `pqueue_conformance` deliberately depends only on the domain (engine + core) — adapters depend on IT,
// not the reverse (see the crate doc) — so this scenario cannot live in `pqueue_conformance::fault`
// alongside the AC-TXN-1..3/6 scenarios; it lives here, in the objectlog-specific test binary, which
// already carries `pqueue_objectlog` as a dev-dependency. It drives `ObjectLog`'s `LogStore` impl DIRECTLY
// (bypassing `ComposedBackend` entirely) so the [`FaultHook`] strikes instants strictly INSIDE the
// segmented substrate's own commit pipeline that the public `Backend::write` seam cannot reach.

/// Crashes (`Err`) every time the pipeline reaches `target`; a no-op at every other cut point.
struct CrashAt(FaultCutPoint);

impl FaultHook for CrashAt {
    fn fault_point(&self, cut: FaultCutPoint) -> pqueue_engine::EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!(
                "fault-injection: crash at {cut:?}"
            )))
        } else {
            Ok(())
        }
    }
}

/// The composed-layer analogue of [`CrashAt`] (AC-TXN-4 row 209): crashes (`Err`) when the
/// [`ComposedBackend`] projection-apply step reaches `target`, a no-op at the other composed cut point.
struct ComposeCrashAt(ComposeFaultPoint);

impl ComposeFaultHook for ComposeCrashAt {
    fn fault_point(&self, cut: ComposeFaultPoint) -> pqueue_engine::EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!(
                "fault-injection: composed projection-apply crash at {cut:?}"
            )))
        } else {
            Ok(())
        }
    }
}

fn objectlog_direct(base: &std::path::Path, tag: &str) -> (std::path::PathBuf, ObjectLog) {
    let root = base.join(tag);
    let log = ObjectLog::open(root.clone()).expect("open object log");
    (root, log)
}

fn ac_txn_4_push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
    pqueue_conformance::envelope(
        pqueue_engine::QueueCommand::Push(pqueue_engine::PushCommand {
            items: vec![pqueue_conformance::item(id, key, 5)],
        }),
        vec![],
    )
}

macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            return Err(format!($($arg)*));
        }
    };
}

/// AC-TXN-4 (TP-003 §3.10 row 209): strike the 5 object-log-substrate-internal cut points here AND the 2
/// composed-layer projection-apply cut points via [`ac_txn_4_composed_projection_apply_crash`], asserting the
/// row's outcomes hold at each: 0 lost accepted items, 0 duplicate active leases, committed commands replay
/// exactly once, orphan segments are ignored by replay, and stale-epoch commits are rejected.
async fn ac_txn_4_crash_point_matrix() -> AcOutcome {
    let mut asserts = Vec::new();
    let shard = pqueue_conformance::shard();
    let base = base_dir("objectlog-direct");

    // --- BeforeSegmentWrite: nothing durable — 0 lost accepted items (nothing was ever accepted). ---
    {
        let (_root, mut log) = objectlog_direct(&base, "before-seg");
        log.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::BeforeSegmentWrite))));
        let err = log.append(&shard, &[ac_txn_4_push_env("1", "kx")], 0);
        ensure!(err.is_err(), "BeforeSegmentWrite must abort the append");
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.is_empty(),
            "BeforeSegmentWrite left {} durable commands, expected 0",
            entries.len()
        );
    }
    asserts.push("BeforeSegmentWrite: 0 durable commands (0 lost accepted items)".into());

    // --- AfterSegmentWriteBeforeManifest: a durable orphan segment must be ignored by replay, and a clean
    // retry afterward must not be confused by it (0 lost, 0 duplicated).
    {
        let (_root, mut log) = objectlog_direct(&base, "after-seg-before-manifest");
        log.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let before = log.counters().objects_put;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterSegmentWriteBeforeManifest,
        ))));
        let err = log.append(&shard, &[ac_txn_4_push_env("1", "orphan")], 0);
        ensure!(
            err.is_err(),
            "AfterSegmentWriteBeforeManifest must abort the append"
        );
        ensure!(
            log.counters().objects_put > before,
            "the segment object was not genuinely durably written before the fault struck"
        );
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.is_empty(),
            "the orphan segment surfaced on replay ({} entries)",
            entries.len()
        );
        log.set_fault_hook(None);
        log.append(&shard, &[ac_txn_4_push_env("2", "real")], 0)
            .map_err(|e| format!("retry append: {e:?}"))?;
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1,
            "orphan segment not cleanly ignored on retry; got {} entries",
            entries.len()
        );
    }
    asserts.push(
        "AfterSegmentWriteBeforeManifest: orphan segments durably written but ignored by replay (0 lost, 0 duplicated)"
            .into(),
    );

    // --- AfterManifestBeforeAck: committed commands replay exactly once. A Claim command crashed the SAME
    // way must not resurrect as two leases (0 duplicate active leases).
    {
        let (root, mut log) = objectlog_direct(&base, "after-manifest-before-ack");
        log.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterManifestBeforeAck,
        ))));
        let err = log.append(&shard, &[ac_txn_4_push_env("1", "committed-unacked")], 0);
        ensure!(err.is_err(), "AfterManifestBeforeAck must abort the append");
        drop(log);

        let mut log2 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
        log2.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let entries = log2
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1,
            "committed-but-unacked command did not replay exactly once; got {} entries",
            entries.len()
        );

        log2.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterManifestBeforeAck,
        ))));
        let claim_env = pqueue_conformance::envelope(
            pqueue_engine::QueueCommand::Claim(ClaimCommand {
                item_ids: vec![pqueue_core::ItemId::new("1").unwrap()],
                lease_token: pqueue_core::LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: pqueue_conformance::ts(500),
            }),
            vec![pqueue_core::ItemId::new("1").unwrap()],
        );
        let claim_err = log2.append(&shard, &[claim_env], 0);
        ensure!(
            claim_err.is_err(),
            "the claim's AfterManifestBeforeAck fault must abort the append"
        );
        drop(log2);

        let mut log3 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
        log3.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let all_entries = log3
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        let claim_entries = all_entries
            .iter()
            .filter(|(_, env)| matches!(env.command, pqueue_engine::QueueCommand::Claim(_)))
            .count();
        ensure!(
            claim_entries == 1,
            "expected exactly 1 committed claim command, got {}",
            claim_entries
        );

        // "0 duplicate active leases" is a PROJECTED-state invariant, not a log-count one: replay the
        // recovered durable log through a fresh projection and assert the reconstructed serving image holds
        // exactly ONE ACTIVE lease for the item (no duplicate lease state survives the lost-ack recovery),
        // rather than merely counting durable Claim log commands.
        let mut projection = HybridProjectionStore::in_memory()
            .map_err(|e| format!("open reconstruction projection: {e:?}"))?;
        ProjectionStore::ensure_shard(&mut projection, &pqueue_conformance::qdef())
            .map_err(|e| format!("ensure_shard (reconstruction): {e:?}"))?;
        let positions: Vec<CommandPosition> = all_entries.iter().map(|(p, _)| p.clone()).collect();
        let commands: Vec<pqueue_engine::CommandEnvelope> =
            all_entries.iter().map(|(_, e)| e.clone()).collect();
        ProjectionStore::apply(&mut projection, &positions, &commands)
            .map_err(|e| format!("replay-apply into reconstruction projection: {e:?}"))?;
        let m = ProjectionStore::metrics(&projection, &shard)
            .map_err(|e| format!("reconstruction metrics: {e:?}"))?;
        ensure!(
            m.leased == 1 && m.pending == 0 && m.complete == 0 && m.failed == 0,
            "0 duplicate active leases: the replayed projection must show exactly ONE active lease for the item; got {m:?}"
        );
    }
    asserts.push(
        "AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery; the reconstructed projection holds exactly ONE active lease (0 duplicate active leases in projected state, not just a log-count)"
            .into(),
    );

    // --- DuringOwnerReassignment: the epoch-fence commit survives a lost ack; stale-epoch commits reject.
    {
        let (root, mut log) = objectlog_direct(&base, "owner-reassignment");
        log.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::DuringOwnerReassignment,
        ))));
        let err = log.acquire_epoch(&shard);
        ensure!(
            err.is_err(),
            "DuringOwnerReassignment must abort acquire_epoch"
        );
        drop(log);

        let mut log2 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
        log2.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let epoch = log2
            .current_epoch(&shard)
            .map_err(|e| format!("current_epoch: {e:?}"))?;
        ensure!(
            epoch == 1,
            "the fence entry must durably commit even though the acquirer's ack was lost; got epoch {epoch}"
        );
        let stale = log2.append(&shard, &[ac_txn_4_push_env("1", "stale")], 0);
        ensure!(
            matches!(stale, Err(EngineError::EpochFenced)),
            "a write at the superseded epoch must be fenced; got {stale:?}"
        );
        log2.append(&shard, &[ac_txn_4_push_env("2", "current")], 1)
            .map_err(|e| format!("write at current epoch: {e:?}"))?;
    }
    asserts.push(
        "DuringOwnerReassignment: epoch-fence commit survives a lost ack; stale-epoch commits rejected, current-epoch commits succeed"
            .into(),
    );

    // --- DuringSnapshotWrite: a lost snapshot write must not lose or corrupt the command log.
    {
        let (_root, mut log) = objectlog_direct(&base, "snapshot-write");
        log.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let positions = log
            .append(&shard, &[ac_txn_4_push_env("1", "before-snapshot")], 0)
            .map_err(|e| format!("append: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
        let err = log.write_snapshot(
            &shard,
            positions[0].clone(),
            ProjectionSnapshot { payload: vec![9] },
        );
        ensure!(
            err.is_err(),
            "DuringSnapshotWrite must abort the snapshot write"
        );
        let latest = log
            .latest_snapshot(&shard)
            .map_err(|e| format!("latest_snapshot: {e:?}"))?;
        ensure!(
            latest.is_none(),
            "a failed snapshot write left a committed snapshot ref"
        );
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1,
            "a lost snapshot write must not lose the command log; got {} entries",
            entries.len()
        );
    }
    asserts.push(
        "DuringSnapshotWrite: a lost snapshot write leaves the command log fully intact (0 lost items)".into(),
    );

    // TP-003 §3.10 row 209 also names the two COMPOSED-LAYER projection-apply instants — "during projection
    // apply" and "after projection apply before response". These are NOT internal to the segmented object-log
    // substrate the 5 cuts above drive directly; they live one layer up, in the `pqueue-engine`
    // ComposedBackend projection-apply step. They now have a REAL cut point via the engine's `ComposeFaultPoint`
    // fault seam, struck against the composed OBJECTLOG backend through the `Backend::write` unit-of-work seam
    // and asserted on the RECONSTRUCTED PROJECTED STATE after drop+reopen recovery (see
    // `ac_txn_4_composed_projection_apply_crash`).
    asserts.extend(
        ac_txn_4_composed_projection_apply_crash(ComposeFaultPoint::DuringProjectionApply).await?,
    );
    asserts.extend(
        ac_txn_4_composed_projection_apply_crash(ComposeFaultPoint::AfterApplyBeforeResponse)
            .await?,
    );
    // "During manifest CAS" collapses to the ATOMIC create-only PUT already bracketed by
    // AfterSegmentWriteBeforeManifest (lost -> orphan) and AfterManifestBeforeAck (won -> committed), so it
    // needs no separate cut point.
    asserts.push(
        "row 209 'during manifest CAS' collapses into the atomic create-only PUT bracketed by AfterSegmentWriteBeforeManifest (lost -> orphan) and AfterManifestBeforeAck (won -> committed); no separate cut point is needed".into(),
    );

    Ok(asserts)
}

/// AC-TXN-4 as its own dedicated test (bead pqueue-3b981b92): the object-log-internal crash-point matrix
/// must pass standalone, independent of the aggregate `ac_txn_contract_matrix` evidence run.
#[tokio::test]
async fn ac_txn_4_objectlog_crash_point_matrix() {
    let outcome = ac_txn_4_crash_point_matrix().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-4 crash-point matrix failed: {:?}",
        outcome.err()
    );
}

/// AC-TXN-4 composed-layer projection-apply cut points (TP-003 §3.10 row 209). The two projection-apply
/// instants row 209 names live in the `pqueue-engine` [`ComposedBackend`] apply step, ABOVE the segmented
/// object-log substrate (whose 5 internal cut points `ac_txn_4_crash_point_matrix` covers). This drives the
/// composed OBJECTLOG backend through the real [`Backend::write`] unit-of-work seam and injects a crash at
/// `cut` via the engine's [`ComposeFaultPoint`] fault hook:
///
/// * [`ComposeFaultPoint::DuringProjectionApply`] — the durable log append has returned Ok, then the fault
///   fires WHILE applying the committed batch to the projection, before it durably advances (the in-memory
///   log-replay projection never advances; recovery rebuilds it from the durable log tail).
/// * [`ComposeFaultPoint::AfterApplyBeforeResponse`] — the projection has fully applied + durably advanced,
///   then the fault fires before the caller's success response (the in-process apply is discarded on drop;
///   recovery replays the durable log to the same exactly-once projected state).
///
/// After the crash the backend is dropped and REOPENED from durable state, and every row-209 outcome is
/// asserted on the RECONSTRUCTED PROJECTED STATE (the recovered serving image), NOT on log-row counts:
/// 0 lost accepted items, committed commands replay EXACTLY ONCE (projected `leased`/`pending`, not a
/// durable Claim/Push log-row count), 0 duplicate active leases (the claimed item is not re-handed-out), and
/// stale-epoch commits are rejected while current-epoch commits succeed.
async fn ac_txn_4_composed_projection_apply_crash(cut: ComposeFaultPoint) -> AcOutcome {
    let base = base_dir("objectlog-compose");
    let root = base.join("txn4-compose");
    // Reopen the SAME durable root each phase (the drop+reopen "process kill/restart" mechanism, identical to
    // AC-TXN-1). Recovery-on-open rebuilds the in-memory projection by replaying the durable object log.
    let open = || {
        pqueue_objectlog::composed_objectlog_backend(root.clone())
            .map_err(|e| format!("open composed objectlog: {e:?}"))
    };
    let shard = pqueue_conformance::shard();
    let qkey = pqueue_conformance::qkey();
    let rid = RequestId::new("ac-txn-4-compose").unwrap();

    // --- Seed: 3 items durably ACCEPTED (acknowledged via request_id) and fully applied. These are the
    // "accepted items" that recovery must never lose. Distinct priorities so claim selection is deterministic.
    let acked = {
        let a = open()?;
        a.create_queue(pqueue_conformance::qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let acked = a
            .push_with_request_id(
                &shard,
                rid.clone(),
                vec![
                    pqueue_conformance::fault::spec("txn4c-a1", 9),
                    pqueue_conformance::fault::spec("txn4c-a2", 5),
                    pqueue_conformance::fault::spec("txn4c-a3", 1),
                ],
                pqueue_conformance::ts(1),
                None,
            )
            .await
            .map_err(|e| format!("seed push_with_request_id: {e:?}"))?;
        ensure!(acked.len() == 3, "seed accepted 3 items, got {}", acked.len());
        acked
    };
    let a1 = acked[0];

    // --- Faulted commit: reopen (recovering the accepted items), install the composed-layer fault hook, and
    // drive ONE raw unit-of-work that appends a Claim(a1)+Push(a4) batch DURABLY and applies it. The hook
    // aborts the apply at `cut`, so the caller sees NO success (write returns Err).
    {
        let b = open()?;
        let m = b
            .metrics(&qkey)
            .await
            .map_err(|e| format!("metrics after seed reopen: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete, m.failed) == (3, 0, 0, 0),
            "seed accepted items not recovered before the cut; got pending={} leased={} complete={} failed={}",
            m.pending,
            m.leased,
            m.complete,
            m.failed
        );
        b.set_fault_hook(Some(Arc::new(ComposeCrashAt(cut))));
        let epoch = b
            .current_epoch(&shard)
            .await
            .map_err(|e| format!("current_epoch: {e:?}"))?;
        let batch = vec![
            pqueue_conformance::envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![a1],
                    lease_token: pqueue_core::LeaseToken::new("compose-lease-1").unwrap(),
                    lease_expires_at: pqueue_conformance::ts(500),
                }),
                vec![a1],
            ),
            ac_txn_4_push_env("900000004", "txn4c-a4"),
        ];
        let res = b
            .write(move |lw, pw| {
                let pos = lw.append(&pqueue_conformance::shard(), &batch, epoch)?;
                pw.apply(&pos, &batch)?;
                Ok(pos)
            })
            .await;
        ensure!(
            res.is_err(),
            "{cut:?} must abort the composed commit (the apply is torn, so no client-visible success); got {res:?}"
        );
    }

    // --- Recovery: reopen from durable state. The projection is rebuilt purely from the durable log, so any
    // half-applied in-process state from the crash is discarded. Assert the row-209 outcomes on the
    // RECONSTRUCTED PROJECTED STATE.
    {
        let c = open()?;
        let m = c
            .metrics(&qkey)
            .await
            .map_err(|e| format!("metrics after recovery: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete, m.failed) == (3, 1, 0, 0),
            "{cut:?} recovery projected state wrong: expected pending=3 leased=1 (3 accepted items preserved, the faulted Claim(a1)+Push(a4) replay EXACTLY ONCE), got pending={} leased={} complete={} failed={}",
            m.pending,
            m.leased,
            m.complete,
            m.failed
        );
        // 0 duplicate active leases as a PROJECTED-STATE invariant: `leased == 1` above already proves a1 is
        // leased exactly once (a duplicate replay would show leased=2 or resurface a1 as pending); additionally
        // a re-claim must NOT hand a1 out again (it is genuinely held, not a phantom lease).
        let reclaim = c
            .claim(pqueue_conformance::claim_req(10, 1500, 700))
            .await
            .map_err(|e| format!("reclaim: {e:?}"))?;
        ensure!(
            !reclaim.items.iter().any(|it| it.item_id == a1),
            "0 duplicate active leases violated: the recovered claim's item a1 was handed out a second time"
        );
    }

    // --- stale-epoch commits are rejected. Model it faithfully: a NEW owner fences the queue by acquiring a
    // fresh epoch on the durable manifest (advance_epoch_object). A stale writer at the superseded epoch is
    // then rejected EpochFenced from the durable manifest tail, while a commit at the current epoch succeeds.
    let new_epoch = {
        let mut owner2 = ObjectLog::open(root.clone()).map_err(|e| format!("open new owner: {e:?}"))?;
        owner2
            .ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard (new owner): {e:?}"))?;
        owner2
            .acquire_epoch(&shard)
            .map_err(|e| format!("acquire_epoch (new owner fences the queue): {e:?}"))?
    };
    ensure!(
        new_epoch >= 1,
        "the new owner's acquire_epoch must supersede the genesis epoch; got {new_epoch}"
    );
    {
        let d = open()?;
        let cur_epoch = d
            .current_epoch(&shard)
            .await
            .map_err(|e| format!("current_epoch after fence: {e:?}"))?;
        ensure!(
            cur_epoch == new_epoch,
            "the recovered backend must observe the fenced epoch from the durable manifest; got {cur_epoch}, expected {new_epoch}"
        );
        let stale = ac_txn_4_push_env("900000005", "txn4c-stale");
        let stale_res = d
            .write(move |lw, pw| {
                let pos = lw.append(&pqueue_conformance::shard(), std::slice::from_ref(&stale), 0)?;
                pw.apply(&pos, std::slice::from_ref(&stale))?;
                Ok(pos)
            })
            .await;
        ensure!(
            matches!(stale_res, Err(EngineError::EpochFenced)),
            "a commit at the superseded epoch (0) must be EpochFenced; got {stale_res:?}"
        );
        let cur = ac_txn_4_push_env("900000006", "txn4c-cur");
        d.write(move |lw, pw| {
            let pos =
                lw.append(&pqueue_conformance::shard(), std::slice::from_ref(&cur), cur_epoch)?;
            pw.apply(&pos, std::slice::from_ref(&cur))?;
            Ok(pos)
        })
        .await
        .map_err(|e| format!("current-epoch commit after fence: {e:?}"))?;
    }

    let label = match cut {
        ComposeFaultPoint::DuringProjectionApply => "DuringProjectionApply",
        ComposeFaultPoint::AfterApplyBeforeResponse => "AfterApplyBeforeResponse",
    };
    Ok(vec![format!(
        "{label} (composed projection-apply cut, ComposeFaultPoint on ComposedBackend): the durable append committed, the projection apply was torn at this instant, and no client-visible success was returned; drop+reopen recovery rebuilds the projection from the durable log to EXACTLY the right serving image — 3 accepted items preserved (0 lost accepted items), the faulted Claim(a1)+Push(a4) replay EXACTLY ONCE (projected leased==1, pending==3 — a PROJECTED-STATE count, not a durable log-row count), 0 duplicate active leases (a1 is not re-handed-out on reclaim), and stale-epoch commits are EpochFenced while current-epoch commits succeed"
    )])
}

/// AC-TXN-4 composed cut point 1 (TP-003 §3.10 row 209) as a standalone test: a crash strictly DURING the
/// composed projection apply, before the projection durably advances, recovers to exactly-once projected state.
#[tokio::test]
async fn ac_txn_4_during_projection_apply_crash() {
    let outcome =
        ac_txn_4_composed_projection_apply_crash(ComposeFaultPoint::DuringProjectionApply).await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-4 DuringProjectionApply composed cut failed: {:?}",
        outcome.err()
    );
}

/// AC-TXN-4 composed cut point 2 (TP-003 §3.10 row 209) as a standalone test: a crash AFTER the composed
/// projection apply durably advanced but before the response, recovers to exactly-once projected state.
#[tokio::test]
async fn ac_txn_4_after_apply_before_response_crash() {
    let outcome =
        ac_txn_4_composed_projection_apply_crash(ComposeFaultPoint::AfterApplyBeforeResponse).await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-4 AfterApplyBeforeResponse composed cut failed: {:?}",
        outcome.err()
    );
}

// ---------------------------------------------------------------------------
// AC-TXN-1 per-op kill-after-success checkpoints (TP-003 §3.10 row 206), as standalone tests so each
// named mutating op's kill-after-success step is independently satisfiable (bead pqueue-b943a44b),
// independent of the aggregate `ac_txn_contract_matrix` evidence run. Each runs the durable in-process
// profiles; the atomic profiles (sqlite_log) exercise the op for real, the eventual-apply profiles
// (objectlog, object_log_sqlite) exercise the honest N/A path for BatchUpdate. SetGates is N/A on all of
// these non-gate composed profiles (gate state is relational-only) and the test asserts that N/A holds.

#[tokio::test]
async fn ac_txn_1_kill_after_create_queue() {
    let sr =
        pqueue_conformance::fault::ac_txn_1_kill_after_create_queue(sqlite_relational_factory())
            .await;
    assert!(sr.is_ok(), "sqlite_relational: {:?}", sr.err());
    let sq =
        pqueue_conformance::fault::ac_txn_1_kill_after_create_queue(sqlite_log_factory()).await;
    assert!(sq.is_ok(), "sqlite_log: {:?}", sq.err());
    let ol = pqueue_conformance::fault::ac_txn_1_kill_after_create_queue(objectlog_factory()).await;
    assert!(ol.is_ok(), "objectlog: {:?}", ol.err());
    let ols =
        pqueue_conformance::fault::ac_txn_1_kill_after_create_queue(objectlog_sqlite_factory())
            .await;
    assert!(ols.is_ok(), "object_log_sqlite: {:?}", ols.err());
}

#[tokio::test]
async fn ac_txn_1_kill_after_batch_update() {
    // sqlite_relational + sqlite_log are atomic: BatchUpdate is GENUINELY exercised (durable + visible after
    // reopen), never capability-N/A.
    for (name, outcome) in
        [
            (
                "sqlite_relational",
                pqueue_conformance::fault::ac_txn_1_kill_after_batch_update(
                    sqlite_relational_factory(),
                )
                .await,
            ),
            (
                "sqlite_log",
                pqueue_conformance::fault::ac_txn_1_kill_after_batch_update(sqlite_log_factory())
                    .await,
            ),
        ]
    {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        assert!(
            outcome.as_ref().unwrap().iter().all(|a| !a.contains("N/A")),
            "BatchUpdate must be genuinely exercised on the atomic {name} profile, not N/A: {outcome:?}"
        );
    }
    // objectlog / object_log_sqlite are eventual-apply: BatchUpdate is atomic-only → capability-N/A.
    for (name, outcome) in [
        (
            "objectlog",
            pqueue_conformance::fault::ac_txn_1_kill_after_batch_update(objectlog_factory()).await,
        ),
        (
            "object_log_sqlite",
            pqueue_conformance::fault::ac_txn_1_kill_after_batch_update(objectlog_sqlite_factory())
                .await,
        ),
    ] {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        assert!(
            outcome
                .as_ref()
                .unwrap()
                .iter()
                .any(|a| a.contains("capability-N/A")),
            "BatchUpdate must be capability-N/A on the eventual-apply {name} profile: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn ac_txn_1_kill_after_set_gates() {
    // sqlite_relational is gate-capable + atomic: SetGates is GENUINELY exercised (the blocked gate survives
    // kill/reopen and keeps the gated item unclaimable), never capability-N/A.
    let sr =
        pqueue_conformance::fault::ac_txn_1_kill_after_set_gates(sqlite_relational_factory()).await;
    assert!(sr.is_ok(), "sqlite_relational: {:?}", sr.err());
    assert!(
        sr.as_ref().unwrap().iter().all(|a| !a.contains("N/A")),
        "SetGates must be genuinely exercised on the gate-capable sqlite_relational profile, not N/A: {sr:?}"
    );
    // The remaining composed profiles are non-gate (gate state is relational-only), so SetGates is genuinely
    // capability-N/A on each — assert the honest capability-N/A path (never a silent pass).
    for (name, outcome) in [
        (
            "sqlite_log",
            pqueue_conformance::fault::ac_txn_1_kill_after_set_gates(sqlite_log_factory()).await,
        ),
        (
            "objectlog",
            pqueue_conformance::fault::ac_txn_1_kill_after_set_gates(objectlog_factory()).await,
        ),
        (
            "object_log_sqlite",
            pqueue_conformance::fault::ac_txn_1_kill_after_set_gates(objectlog_sqlite_factory())
                .await,
        ),
    ] {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        assert!(
            outcome
                .as_ref()
                .unwrap()
                .iter()
                .any(|a| a.contains("capability-N/A")),
            "SetGates must be capability-N/A on the non-gate {name} profile: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn ac_txn_1_kill_after_purge_items() {
    let sr =
        pqueue_conformance::fault::ac_txn_1_kill_after_purge_items(sqlite_relational_factory())
            .await;
    assert!(sr.is_ok(), "sqlite_relational: {:?}", sr.err());
    let sq = pqueue_conformance::fault::ac_txn_1_kill_after_purge_items(sqlite_log_factory()).await;
    assert!(sq.is_ok(), "sqlite_log: {:?}", sq.err());
    let ol = pqueue_conformance::fault::ac_txn_1_kill_after_purge_items(objectlog_factory()).await;
    assert!(ol.is_ok(), "objectlog: {:?}", ol.err());
    let ols =
        pqueue_conformance::fault::ac_txn_1_kill_after_purge_items(objectlog_sqlite_factory())
            .await;
    assert!(ols.is_ok(), "object_log_sqlite: {:?}", ols.err());
}

// ---------------------------------------------------------------------------
// AC-TXN-2 rejection-class checkpoints (TP-003 §3.10 row 207), as standalone tests so each rejection class
// named in the row is independently satisfiable (bead pqueue-9b799403), independent of the aggregate
// `ac_txn_contract_matrix` evidence run. Each runs the durable composed profiles (sqlite_log, objectlog,
// object_log_sqlite) AND composed_sqlite_relational (atomic + gate-capable). No class emits a coverage-GAP;
// where a class genuinely cannot occur on a backend the scenario records capability-N/A (asserted below).

#[tokio::test]
async fn ac_txn_2_envelope_invalid_batch_has_no_durable_effect() {
    let sr = pqueue_conformance::fault::ac_txn_2_envelope_invalid_batch(
        sqlite_relational_factory(),
        DURABLE,
    )
    .await;
    assert!(sr.is_ok(), "sqlite_relational: {:?}", sr.err());
    let sq =
        pqueue_conformance::fault::ac_txn_2_envelope_invalid_batch(sqlite_log_factory(), DURABLE)
            .await;
    assert!(sq.is_ok(), "sqlite_log: {:?}", sq.err());
    let ol =
        pqueue_conformance::fault::ac_txn_2_envelope_invalid_batch(objectlog_factory(), DURABLE)
            .await;
    assert!(ol.is_ok(), "objectlog: {:?}", ol.err());
    let ols = pqueue_conformance::fault::ac_txn_2_envelope_invalid_batch(
        objectlog_sqlite_factory(),
        DURABLE,
    )
    .await;
    assert!(ols.is_ok(), "object_log_sqlite: {:?}", ols.err());
    for outcome in [&sr, &sq, &ol, &ols] {
        assert!(
            outcome.as_ref().unwrap().iter().all(|a| !a.contains("GAP")),
            "envelope-invalid batch must not carry a coverage GAP: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn ac_txn_2_stale_lease_conflict_has_no_durable_effect() {
    let sr = pqueue_conformance::fault::ac_txn_2_stale_lease_conflict(
        sqlite_relational_factory(),
        DURABLE,
    )
    .await;
    assert!(sr.is_ok(), "sqlite_relational: {:?}", sr.err());
    let sq =
        pqueue_conformance::fault::ac_txn_2_stale_lease_conflict(sqlite_log_factory(), DURABLE)
            .await;
    assert!(sq.is_ok(), "sqlite_log: {:?}", sq.err());
    let ol = pqueue_conformance::fault::ac_txn_2_stale_lease_conflict(objectlog_factory(), DURABLE)
        .await;
    assert!(ol.is_ok(), "objectlog: {:?}", ol.err());
    let ols = pqueue_conformance::fault::ac_txn_2_stale_lease_conflict(
        objectlog_sqlite_factory(),
        DURABLE,
    )
    .await;
    assert!(ols.is_ok(), "object_log_sqlite: {:?}", ols.err());
    for outcome in [&sr, &sq, &ol, &ols] {
        assert!(
            outcome.as_ref().unwrap().iter().all(|a| !a.contains("GAP")),
            "stale-lease conflict must not carry a coverage GAP: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn ac_txn_2_capacity_unavailable_path_has_no_durable_effect() {
    // Atomic backends: upsert is available -> the upsert->Unavailable class is capability-N/A; the
    // BatchTooLarge capacity path is exercised for real.
    for (name, outcome) in [
        (
            "sqlite_relational",
            pqueue_conformance::fault::ac_txn_2_capacity_unavailable_path(
                sqlite_relational_factory(),
                DURABLE,
            )
            .await,
        ),
        (
            "sqlite_log",
            pqueue_conformance::fault::ac_txn_2_capacity_unavailable_path(
                sqlite_log_factory(),
                DURABLE,
            )
            .await,
        ),
    ] {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        let asserts = outcome.as_ref().unwrap();
        assert!(
            asserts.iter().all(|a| !a.contains("GAP")),
            "{name} capacity/unavailable must not carry a coverage GAP: {outcome:?}"
        );
        assert!(
            asserts.iter().any(|a| a.contains("capability-N/A")),
            "{name} (atomic) must record upsert->Unavailable as capability-N/A: {outcome:?}"
        );
    }
    // Eventual-apply backends: upsert genuinely refuses with Unavailable (exercised, not N/A).
    for (name, outcome) in [
        (
            "objectlog",
            pqueue_conformance::fault::ac_txn_2_capacity_unavailable_path(
                objectlog_factory(),
                DURABLE,
            )
            .await,
        ),
        (
            "object_log_sqlite",
            pqueue_conformance::fault::ac_txn_2_capacity_unavailable_path(
                objectlog_sqlite_factory(),
                DURABLE,
            )
            .await,
        ),
    ] {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        let asserts = outcome.as_ref().unwrap();
        assert!(
            asserts.iter().all(|a| !a.contains("GAP")),
            "{name} capacity/unavailable must not carry a coverage GAP: {outcome:?}"
        );
        assert!(
            asserts
                .iter()
                .any(|a| a.contains("unavailable path: upsert")),
            "{name} (eventual-apply) must exercise the upsert->Unavailable path: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn ac_txn_2_commit_timeout_path_has_no_durable_effect() {
    let sr = pqueue_conformance::fault::ac_txn_2_commit_timeout_path(
        sqlite_relational_factory(),
        DURABLE,
    )
    .await;
    assert!(sr.is_ok(), "sqlite_relational: {:?}", sr.err());
    let sq = pqueue_conformance::fault::ac_txn_2_commit_timeout_path(sqlite_log_factory(), DURABLE)
        .await;
    assert!(sq.is_ok(), "sqlite_log: {:?}", sq.err());
    let ol =
        pqueue_conformance::fault::ac_txn_2_commit_timeout_path(objectlog_factory(), DURABLE).await;
    assert!(ol.is_ok(), "objectlog: {:?}", ol.err());
    let ols = pqueue_conformance::fault::ac_txn_2_commit_timeout_path(
        objectlog_sqlite_factory(),
        DURABLE,
    )
    .await;
    assert!(ols.is_ok(), "object_log_sqlite: {:?}", ols.err());
    for outcome in [&sr, &sq, &ol, &ols] {
        assert!(
            outcome.as_ref().unwrap().iter().all(|a| !a.contains("GAP")),
            "commit-timeout path must not carry a coverage GAP: {outcome:?}"
        );
    }
    // The composed log+projection profiles must strike the REAL append→apply window and prove exactly-once
    // recovery — not merely a no-effect abort.
    for (name, outcome) in [
        ("sqlite_log", &sq),
        ("objectlog", &ol),
        ("object_log_sqlite", &ols),
    ] {
        assert!(
            outcome
                .as_ref()
                .unwrap()
                .iter()
                .any(|a| a.contains("recovered EXACTLY ONCE")),
            "{name} must back its pass with the real AfterAppendBeforeApply exactly-once recovery: {outcome:?}"
        );
    }
    // The unified rebuildable-cache store has no append→apply window — capability-N/A (honest, not a GAP).
    assert!(
        sr.as_ref()
            .unwrap()
            .iter()
            .any(|a| a.contains("capability-N/A")),
        "sqlite_relational (unified store) must record the append→apply window as capability-N/A: {sr:?}"
    );
}

// ---------------------------------------------------------------------------
// AC-TXN-3 request_id unknown-outcome replay across every op + cut point (TP-003 §3.10 row 208), as
// standalone tests (bead pqueue-48a4af85), independent of the aggregate `ac_txn_contract_matrix` evidence
// run. Part 1 (mid-pipeline cut is now request_id-bearing) and Part 2 (per-op coverage: push all cuts;
// commit_transition per reachable cut; classic ops capability-N/A).
// ---------------------------------------------------------------------------

/// Part 1: the mid-pipeline `AfterAppendBeforeApply` cut is now `request_id`-BEARING (not item-level). On
/// every durable composed-log profile, a kill in the append→apply window leaves the request_id-bearing push
/// durable-but-unapplied, and a retry by `request_id` after reopen replays the ONE committed result. Also
/// proven on the eventual-apply objectlog / object_log_sqlite substrates (recovery rebuilds the push
/// idempotency map from the durable log on BOTH durability classes).
#[tokio::test]
async fn ac_txn_3_mid_pipeline_cut_is_request_id_bearing() {
    for (name, outcome) in [
        (
            "sqlite_log",
            ac_txn_3_mid_pipeline_request_id_bearing(sqlite_log_factory()).await,
        ),
        (
            "objectlog",
            ac_txn_3_mid_pipeline_request_id_bearing(objectlog_factory()).await,
        ),
        (
            "object_log_sqlite",
            ac_txn_3_mid_pipeline_request_id_bearing(objectlog_sqlite_factory()).await,
        ),
    ] {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        let asserts = outcome.unwrap();
        assert!(
            asserts
                .iter()
                .any(|a| a.contains("request_id-bearing") && a.contains("AfterAppendBeforeApply")),
            "{name} must prove the mid-pipeline cut is request_id-bearing: {asserts:?}"
        );
        assert!(
            asserts.iter().all(|a| !a.contains("GAP")),
            "{name} mid-pipeline request_id-bearing proof must carry no GAP: {asserts:?}"
        );
    }
}

/// Part 2: request_id replay across every op + cut point, per the honest engine capability map. PUSH is the
/// only op fully covered at all four cuts; `commit_transition` is covered per its reachable cuts (in-process
/// on atomic; capability-N/A on eventual-apply; cross-restart is a recorded engine limitation); the classic
/// ports carry no request_id (capability-N/A).
#[tokio::test]
async fn ac_txn_3_request_id_replay_all_ops_all_cut_points() {
    // Durable composed-log profiles: PUSH covered at ALL FOUR cut points (incl. the request_id-bearing
    // mid-pipeline cut), and the classic-ops capability-N/A note is present.
    for (name, outcome) in [
        (
            "sqlite_log",
            ac_txn_3_unknown_outcome_replay(sqlite_log_factory(), DURABLE).await,
        ),
        (
            "objectlog",
            ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await,
        ),
        (
            "object_log_sqlite",
            ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await,
        ),
    ] {
        assert!(outcome.is_ok(), "{name}: {:?}", outcome.err());
        let a = outcome.unwrap();
        // PUSH: all four cut points present.
        assert!(
            a.iter().any(|s| s.contains("PUSH BeforeAppend"))
                && a.iter()
                    .any(|s| s.contains("PUSH AfterAppendBeforeApply (request_id-bearing)"))
                && a.iter()
                    .any(|s| s.contains("PUSH AfterApplyBeforeResponse")),
            "{name} must cover PUSH request_id replay at all four cut points: {a:?}"
        );
        // Classic ops carry no request_id -> capability-N/A (never a silent pass, never a fake request_id).
        assert!(
            a.iter().any(|s| s.contains("capability-N/A")
                && s.contains("claim / renew / finalize")
                && s.contains("carry NO request_id")),
            "{name} must record the classic ops as capability-N/A for request_id replay: {a:?}"
        );
    }

    // commit_transition per-backend: atomic backends prove in-process replay (and honestly record the
    // cross-restart limitation as a GAP); eventual-apply backends record it capability-N/A (Unavailable).
    let sqlite_ct = ac_txn_3_commit_transition_request_id(sqlite_log_factory(), DURABLE).await;
    assert!(sqlite_ct.is_ok(), "sqlite_log ct: {:?}", sqlite_ct.err());
    let sqlite_ct = sqlite_ct.unwrap();
    assert!(
        sqlite_ct
            .iter()
            .any(|s| s.contains("IN-PROCESS request_id replay proven")),
        "sqlite_log (atomic) must prove in-process commit_transition request_id replay: {sqlite_ct:?}"
    );
    // ALL-COMMITTED commit_transition replays across restart at BOTH cut points — a real win we keep proving.
    assert!(
        sqlite_ct
            .iter()
            .any(|s| s.contains("AfterApplyBeforeResponse across-restart request_id replay PROVEN")),
        "sqlite_log must prove all-committed commit_transition AfterApplyBeforeResponse across-restart replay: {sqlite_ct:?}"
    );
    assert!(
        sqlite_ct.iter().any(|s| s.contains(
            "AfterAppendBeforeApply (request_id-bearing) across-restart request_id replay PROVEN"
        )),
        "sqlite_log must prove all-committed commit_transition AfterAppendBeforeApply across-restart replay: {sqlite_ct:?}"
    );
    // But a MIXED committed+rejected commit is NOT faithfully replayed across restart (rejected entries append
    // nothing durable): the retry safely re-executes (0 duplicate) instead. That residual is recorded as a
    // GAP so `record()` classifies sqlite_log/postgres AC-TXN-3 as `partial`, not an overclaimed `pass`.
    assert!(
        sqlite_ct
            .iter()
            .any(|s| s.contains("mixed committed+rejected across-restart is SAFE")),
        "sqlite_log must prove the mixed-commit across-restart safety invariant (0 duplicate, no false-complete): {sqlite_ct:?}"
    );
    assert!(
        sqlite_ct.iter().any(|s| s
            .contains("GAP (mixed committed+rejected commit_transition across-restart replay)")),
        "sqlite_log must honestly record the mixed-commit faithful-replay residual as a GAP: {sqlite_ct:?}"
    );

    let ol_ct = ac_txn_3_commit_transition_request_id(objectlog_factory(), DURABLE).await;
    assert!(ol_ct.is_ok(), "objectlog ct: {:?}", ol_ct.err());
    assert!(
        ol_ct
            .unwrap()
            .iter()
            .any(|s| s.contains("capability-N/A") && s.contains("commit_transition")),
        "objectlog (eventual-apply) must record commit_transition as capability-N/A (Unavailable)"
    );

    // memory (non-durable, atomic): in-process commit_transition replay covered; restart cuts capability-N/A.
    let mem_ct = ac_txn_3_commit_transition_request_id(
        |_: &str| pqueue_memory::composed_memory_backend(),
        NON_DURABLE,
    )
    .await;
    assert!(mem_ct.is_ok(), "memory ct: {:?}", mem_ct.err());
    let mem_ct = mem_ct.unwrap();
    assert!(
        mem_ct
            .iter()
            .any(|s| s.contains("IN-PROCESS request_id replay proven"))
            && mem_ct.iter().all(|s| !s.contains("GAP")),
        "memory must prove in-process commit_transition replay with no GAP (restart is capability-N/A): {mem_ct:?}"
    );
}

// ---------------------------------------------------------------------------
// AC-TXN-5 / AC-TXN-5A: hybrid-strict / hybrid-async projection-apply fault seams (TP-003 §3.10 rows
// 210-211)
// ---------------------------------------------------------------------------
//
// Mirrors AC-TXN-4's honesty split: the object-log side already has its own internal `FaultHook`
// (`pqueue_objectlog::segmented`, AC-TXN-4); this section adds the analogous seam on the PROJECTION side —
// `pqueue_sqlite::HybridFaultHook` — striking instants strictly INSIDE `HybridProjectionStore`'s own apply
// pipeline (between the durable SQLite commit and the in-memory apply, and inside the deferred async SQLite
// checkpoint apply) that neither `Backend::write` nor `PushPort::push_with_request_id` can isolate. Where a
// cut point genuinely IS reachable through the public seam (a memory-apply failure struck from inside
// `apply_live`, or a crash in the commit→apply window), these scenarios drive it through the real
// `ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>` instead, same as the rest of
// this suite.

/// Crashes (`Err`) every time the hybrid projection's apply pipeline reaches `target`; a no-op at every
/// other cut point. The `HybridProjectionStore` analogue of `CrashAt` above (AC-TXN-4).
struct HybridCrashAt(HybridFaultCutPoint);

impl HybridFaultHook for HybridCrashAt {
    fn fault_point(&self, cut: HybridFaultCutPoint) -> pqueue_engine::EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!(
                "fault-injection: crash at {cut:?}"
            )))
        } else {
            Ok(())
        }
    }
}

/// Assemble a fresh `objectlog/hybrid` composed backend at `root` with `hook` installed on its
/// `HybridProjectionStore` BEFORE the first command lands, so the fault strikes the very first apply.
fn objectlog_hybrid_with_fault_hook(
    root: &std::path::Path,
    hook: Arc<dyn HybridFaultHook>,
) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite_path = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap())
        .expect("open object log");
    let hybrid =
        HybridProjectionStore::open(sqlite_path.to_str().unwrap()).expect("open hybrid projection");
    hybrid.set_fault_hook(Some(hook));
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid with fault hook installed")
}

fn hybrid_push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
    pqueue_conformance::envelope(
        QueueCommand::Push(PushCommand {
            items: vec![pqueue_conformance::item(id, key, 5)],
        }),
        vec![],
    )
}

/// **AC-TXN-5** (TP-003 §3.10 row 210, `objectlog/hybrid-strict`): a fault struck strictly BETWEEN the
/// durable SQLite commit and the in-memory apply — the ordering `HybridProjectionStore::apply` uses — must
/// poison the store fail-closed until restart, and a restart must hydrate memory from the (already
/// consistent) durable SQLite `ProjectionImage` without re-appending anything. Drives `HybridProjectionStore`
/// DIRECTLY via `ProjectionStore` (bypassing `ComposedBackend`) for the poison/restart-hydration clauses,
/// the same honest bypass AC-TXN-4 uses on the object-log side, because the public seam cannot isolate this
/// exact instant; request-id semantics are then proven end-to-end through the real composed backend.
async fn ac_txn_5_hybrid_strict_poison_replay_scenario() -> AcOutcome {
    let mut asserts = Vec::new();
    let shard = pqueue_conformance::shard();
    let base = base_dir("hybrid-strict-poison");
    std::fs::create_dir_all(&base).ok();
    let path = base.join("projection.sqlite");
    let pos0 = CommandPosition::new(shard.clone(), 0, 0);
    let env0 = hybrid_push_env("1", "kx");

    // --- AfterSqliteCommitBeforeMemoryApply: SQLite durably committed; the fault fires before memory
    // observes it. The store must poison and fail every subsequent op closed, before any restart. ---
    {
        let mut hybrid =
            HybridProjectionStore::open(path.to_str().unwrap()).expect("open hybrid projection");
        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        hybrid.set_fault_hook(Some(Arc::new(HybridCrashAt(
            HybridFaultCutPoint::AfterSqliteCommitBeforeMemoryApply,
        ))));
        let err = ProjectionStore::apply(
            &mut hybrid,
            std::slice::from_ref(&pos0),
            std::slice::from_ref(&env0),
        );
        ensure!(
            err.is_err(),
            "a SQLite-commit-success/memory-apply-fail must not return success"
        );
        let after = ProjectionStore::metrics(&hybrid, &shard);
        ensure!(
            after.is_err(),
            "the store must fail closed (reads included) after the poison, before restart; got {after:?}"
        );
    }
    asserts.push(
        "AfterSqliteCommitBeforeMemoryApply: a durable SQLite commit whose memory apply never runs poisons the store; every subsequent read fails closed before restart".into(),
    );

    // --- restart: reopen from the SAME durable SQLite file (no fault hook) and confirm memory hydrates
    // from the SQLite ProjectionImage exactly, then that a same-(position,body) retry replays the original
    // result without a second append. ---
    {
        let mut hybrid =
            HybridProjectionStore::open(path.to_str().unwrap()).expect("reopen hybrid projection");
        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
            .map_err(|e| format!("ensure_shard after restart: {e:?}"))?;
        let m = ProjectionStore::metrics(&hybrid, &shard)
            .map_err(|e| format!("metrics after restart: {e:?}"))?;
        ensure!(
            m.pending == 1,
            "restart must hydrate memory from the durable SQLite ProjectionImage (pending=1); got {m:?}"
        );

        ProjectionStore::apply(
            &mut hybrid,
            std::slice::from_ref(&pos0),
            std::slice::from_ref(&env0),
        )
        .map_err(|e| format!("same-body retry apply: {e:?}"))?;
        let m2 = ProjectionStore::metrics(&hybrid, &shard)
            .map_err(|e| format!("metrics after retry: {e:?}"))?;
        ensure!(
            m2.pending == 1,
            "same-body retry must replay the original result without a second append; got pending={}",
            m2.pending
        );
    }
    asserts.push(
        "restart hydrates memory from the durable SQLite ProjectionImage; a same-(position,body) retry replays the original result without a second append".into(),
    );

    // --- request-id conflict semantics: proven end-to-end through a FRESH, healthy instance of the exact
    // same objectlog/hybrid backend combination (the layer that owns request_id idempotency/conflict
    // detection), independent of the poisoned instance above. ---
    {
        let make = objectlog_sqlite_factory();
        let backend = make("rid-conflict");
        backend
            .create_queue(pqueue_conformance::qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let rid = RequestId::new("ac-txn-5-rid").unwrap();
        let body = vec![pqueue_conformance::fault::spec("ac-txn-5-a", 1)];
        let first = backend
            .push_with_request_id(
                &shard,
                rid.clone(),
                body.clone(),
                pqueue_conformance::ts(1),
                None,
            )
            .await
            .map_err(|e| format!("first request-id push: {e:?}"))?;
        let replay = backend
            .push_with_request_id(&shard, rid.clone(), body, pqueue_conformance::ts(2), None)
            .await
            .map_err(|e| format!("same-body retry: {e:?}"))?;
        ensure!(
            replay == first,
            "same-body retry under the same request_id must replay the original result"
        );
        let conflict = backend
            .push_with_request_id(
                &shard,
                rid,
                vec![pqueue_conformance::fault::spec("ac-txn-5-different", 2)],
                pqueue_conformance::ts(3),
                None,
            )
            .await;
        ensure!(
            matches!(conflict, Err(EngineError::RequestIdConflict)),
            "a conflicting body under the same request_id must return request-id-conflict; got {conflict:?}"
        );
    }
    asserts.push(
        "request-id semantics on the objectlog/hybrid substrate: same-body retry replays the original result; conflicting body returns request-id-conflict".into(),
    );

    // Real-server-path coverage (bead pqueue-da1965d7): the AfterSqliteCommitBeforeMemoryApply poison instant
    // above is struck on `HybridProjectionStore::apply` (`apply_durable_then_memory`, SQLite-first) — the
    // direct ProjectionStore view. The SAME SQLite-first ordering is now WIRED as the `objectlog/hybrid-strict`
    // server profile (`PQUEUE_PROJECTION_BACKEND=hybrid-strict`, `HybridProjectionStore::with_strict_apply`),
    // and `ac_txn_5_hybrid_strict_poison_on_real_server_path` drives all four clauses through that real
    // group-commit composed write pipeline (`apply_live_owned` → strict `apply_durable_then_memory`). So this
    // cut is no longer verified at the ProjectionStore layer ONLY — the prior real-server-path GAP is closed.
    asserts.push(
        "real-server-path cut is WIRED and proven separately: the `objectlog/hybrid-strict` server profile (with_strict_apply) runs this exact SQLite-first ordering on the group-commit write path; see ac_txn_5_hybrid_strict_poison_on_real_server_path".into(),
    );

    Ok(asserts)
}

#[tokio::test]
async fn ac_txn_5_hybrid_strict_poison_replay() {
    let outcome = ac_txn_5_hybrid_strict_poison_replay_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-5 hybrid-strict poison/replay failed: {:?}",
        outcome.err()
    );
}

// ---------------------------------------------------------------------------
// AC-TXN-5 on the REAL server write path (bead pqueue-da1965d7)
// ---------------------------------------------------------------------------
//
// The scenario above strikes the `AfterSqliteCommitBeforeMemoryApply` cut on `HybridProjectionStore::apply`
// DIRECTLY (bypassing `ComposedBackend`), which — until this bead — was the only place that SQLite-first
// ordering ran. This scenario instead drives every clause through the REAL `objectlog/hybrid-strict` server
// write pipeline: a group-commit `ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>`
// with `with_strict_apply(true)` (the exact composition `pqueue-server` builds for
// `PQUEUE_PROJECTION_BACKEND=hybrid-strict`). Pushes go through `push_with_request_id`, so the sealed batch
// is applied by the composed group-commit distribute path (`apply_live_owned` → strict
// `apply_durable_then_memory`), and the fault cuts land on that real pipeline. Assertions are on PROJECTED
// STATE (`metrics`/`live_items`), never log-row counts, and restart is a real drop+reopen at the same root.

/// An arm-able [`HybridFaultHook`]: crashes at `cut` only while `armed`, so a test can push the first command
/// through cleanly, ARM the cut, then push the command that must strike it. Toggled through the `Arc` the test
/// keeps after installing a clone on the store (the hook uses interior mutability behind the store's mutex).
struct ArmableHybridHook {
    cut: HybridFaultCutPoint,
    armed: AtomicBool,
}

impl ArmableHybridHook {
    fn new(cut: HybridFaultCutPoint) -> Arc<Self> {
        Arc::new(Self {
            cut,
            armed: AtomicBool::new(false),
        })
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }
}

impl HybridFaultHook for ArmableHybridHook {
    fn fault_point(&self, cut: HybridFaultCutPoint) -> pqueue_engine::EngineResult<()> {
        if self.armed.load(Ordering::SeqCst) && cut == self.cut {
            Err(EngineError::Storage(format!(
                "fault-injection: crash at {cut:?}"
            )))
        } else {
            Ok(())
        }
    }
}

/// Open (or reopen) the REAL `objectlog/hybrid-strict` composed backend at `root`: the segmented
/// group-commit object log + a `HybridProjectionStore` in STRICT mode (`with_strict_apply(true)` — SQLite
/// durable BEFORE hot memory on the write path), recovery-on-open. `hook`, when present, is installed on the
/// store BEFORE recover so it can strike the very first apply; a restart passes `None` for a clean replay.
fn objectlog_hybrid_strict_composed(
    root: &std::path::Path,
    hook: Option<Arc<dyn HybridFaultHook>>,
) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite_path = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap())
        .expect("open object log");
    let hybrid = HybridProjectionStore::open(sqlite_path.to_str().unwrap())
        .expect("open hybrid projection")
        .with_strict_apply(true);
    if let Some(hook) = hook {
        hybrid.set_fault_hook(Some(hook));
    }
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-strict")
}

/// Push one single-item body through the real composed write path under its own `request_id`.
async fn strict_push(
    backend: &HybridBackend,
    key: &str,
) -> Result<Vec<pqueue_core::ItemId>, EngineError> {
    let rid = RequestId::new(format!("ac-txn-5-real-{key}")).unwrap();
    backend
        .push_with_request_id(
            &pqueue_conformance::shard(),
            rid,
            vec![pqueue_conformance::fault::spec(key, 5)],
            pqueue_conformance::ts(1),
            None,
        )
        .await
}

/// **AC-TXN-5 on the real server write path** (bead pqueue-da1965d7, TP-003 §3.10 row 210,
/// `objectlog/hybrid-strict`). Drives the wired hybrid-strict composition — the composition `pqueue-server`
/// builds for `PQUEUE_PROJECTION_BACKEND=hybrid-strict` — and proves ALL four clauses on that real pipeline:
///
/// 1. **SQLite failure → no success + tail replays.** A `BeforeSqliteApply` fault aborts the durable SQLite
///    apply: the push returns no success, the store stays healthy (no poison), and because the object-log
///    manifest entry is already durable, a restart replays the tail and the command is neither lost nor
///    duplicated.
/// 2. **SQLite-commit-then-memory-fail → poison fail-closed.** An `AfterSqliteCommitBeforeMemoryApply` fault
///    poisons the store: the push returns no success, subsequent reads AND writes fail closed (serving stops).
/// 3. **Restart → memory rehydrated from the SQLite `ProjectionImage`.** A real drop+reopen at the same root
///    rebuilds hot memory from durable SQLite; the durably-committed-but-never-memory-applied command from
///    clause 2 is now visible (projected state correct after reopen).
/// 4. **Request-id replay/conflict.** A same-body retry returns the original ids; a conflicting body under
///    the same `request_id` returns `RequestIdConflict`.
///
/// Every assertion is on PROJECTED STATE (`metrics.pending` / `live_items`), never log-row counts.
async fn ac_txn_5_hybrid_strict_poison_on_real_server_path_scenario() -> AcOutcome {
    let mut asserts = Vec::new();
    let shard = pqueue_conformance::shard();
    let key_a = ClientItemKey::new("a").unwrap();
    let key_b = ClientItemKey::new("b").unwrap();

    // --- Clause 1: SQLite failure returns no success AND the tail replays (no lost/duplicated commands). ---
    {
        let base = base_dir("hybrid-strict-real-sqlite-fail");
        let root = base.join("run");
        let hook = ArmableHybridHook::new(HybridFaultCutPoint::BeforeSqliteApply);
        {
            let backend =
                objectlog_hybrid_strict_composed(&root, Some(hook.clone() as Arc<dyn HybridFaultHook>));
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .map_err(|e| format!("create_queue: {e:?}"))?;
            // A commits cleanly on the real strict write path.
            strict_push(&backend, "a")
                .await
                .map_err(|e| format!("push A: {e:?}"))?;
            let m = backend
                .metrics(&shard)
                .await
                .map_err(|e| format!("metrics after A: {e:?}"))?;
            ensure!(m.pending == 1, "A must be visible (pending=1); got {m:?}");
            // Arm the SQLite-apply fault and push B: the object-log manifest entry commits durably, but the
            // strict SQLite apply aborts, so the push returns no success.
            hook.arm();
            let b = strict_push(&backend, "b").await;
            ensure!(
                b.is_err(),
                "a SQLite-apply failure must return no success on the real write path; got {b:?}"
            );
            // The store is NOT poisoned by a pre-commit SQLite failure: it keeps serving, and B is simply not
            // yet applied (memory == SQLite == {A}).
            hook.disarm();
            let m = backend
                .metrics(&shard)
                .await
                .map_err(|e| format!("metrics after failed B: {e:?}"))?;
            ensure!(
                m.pending == 1,
                "a SQLite-apply failure must leave the store healthy with B unapplied (pending=1); got {m:?}"
            );
        }
        // Restart: the object-log tail beyond the SQLite high-water replays B exactly once.
        {
            let backend = objectlog_hybrid_strict_composed(&root, None);
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .map_err(|e| format!("create_queue after restart: {e:?}"))?;
            let m = backend
                .metrics(&shard)
                .await
                .map_err(|e| format!("metrics after restart: {e:?}"))?;
            ensure!(
                m.pending == 2,
                "restart must replay the committed tail (B) exactly once (pending=2, no lost/duplicated); got {m:?}"
            );
            let live = backend
                .live_items(&shard, &[key_a.clone(), key_b.clone()])
                .await
                .map_err(|e| format!("live_items after restart: {e:?}"))?;
            ensure!(
                live.len() == 2 && live[0].is_some() && live[1].is_some(),
                "both A and B must be live after tail-replay recovery; got {live:?}"
            );
        }
    }
    asserts.push(
        "real hybrid-strict write path: a SQLite-apply failure returns no success and leaves the store healthy; a restart replays the durable object-log tail so the command is neither lost nor duplicated (projected pending=2)".into(),
    );

    // --- Clause 2 + 3: SQLite-commit-then-memory-fail poisons fail-closed; restart rehydrates from SQLite. ---
    {
        let base = base_dir("hybrid-strict-real-poison");
        let root = base.join("run");
        let hook = ArmableHybridHook::new(HybridFaultCutPoint::AfterSqliteCommitBeforeMemoryApply);
        {
            let backend =
                objectlog_hybrid_strict_composed(&root, Some(hook.clone() as Arc<dyn HybridFaultHook>));
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .map_err(|e| format!("create_queue: {e:?}"))?;
            strict_push(&backend, "a")
                .await
                .map_err(|e| format!("push A: {e:?}"))?;
            // Arm the post-SQLite-commit / pre-memory-apply cut and push B: SQLite commits B durably, then the
            // memory apply faults, so the store poisons and the push returns no success.
            hook.arm();
            let b = strict_push(&backend, "b").await;
            ensure!(
                b.is_err(),
                "a SQLite-commit-then-memory-fail must return no success; got {b:?}"
            );
            // Serving stops: reads fail closed.
            let m = backend.metrics(&shard).await;
            ensure!(
                m.is_err(),
                "a poisoned store must fail reads closed (serving stops); got {m:?}"
            );
            // And writes fail closed: the high-water/serving does not advance past the poison.
            hook.disarm();
            let c = strict_push(&backend, "c").await;
            ensure!(
                c.is_err(),
                "a poisoned store must fail new writes closed until restart; got {c:?}"
            );
        }
        // Restart: memory rehydrates from the durable SQLite ProjectionImage — B's durably-committed effect
        // (never applied to the pre-restart memory image) is now visible.
        {
            let backend = objectlog_hybrid_strict_composed(&root, None);
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .map_err(|e| format!("create_queue after restart: {e:?}"))?;
            let m = backend
                .metrics(&shard)
                .await
                .map_err(|e| format!("metrics after restart: {e:?}"))?;
            ensure!(
                m.pending == 2,
                "restart must rehydrate memory from the SQLite ProjectionImage including B's durable commit (pending=2); got {m:?}"
            );
            let live = backend
                .live_items(&shard, &[key_a.clone(), key_b.clone()])
                .await
                .map_err(|e| format!("live_items after restart: {e:?}"))?;
            ensure!(
                live.len() == 2 && live[0].is_some() && live[1].is_some(),
                "both A and the poisoned-then-recovered B must be live after restart; got {live:?}"
            );
        }
    }
    asserts.push(
        "real hybrid-strict write path: a SQLite-commit-then-memory-apply failure poisons the store fail-closed (reads AND writes error, serving stops); a restart rehydrates memory from the durable SQLite ProjectionImage so the poisoned command's durable effect is recovered (projected pending=2)".into(),
    );

    // --- Clause 4: request-id UNKNOWN-OUTCOME replay/conflict ACROSS the strict cut + a real restart (bead
    // pqueue-da1965d7 review; TP-003 §3.10 row 210 + TD-004 durable request-id replay). This is the AC-TXN-5
    // -specific case, NOT a clean in-process replay: a request_id-bearing push is struck at
    // AfterSqliteCommitBeforeMemoryApply, so it is durable-in-SQLite + durable-on-the-object-log but returns
    // Err (an UNKNOWN outcome to the caller — the poison means no success is returned). A real drop+reopen
    // then rebuilds the push `request_id -> result` idempotency map from the durable object log
    // (`rebuild_push_idempotency_from_log`), so a retry of the SAME request_id must REPLAY the one original
    // item id (0 duplicate transitions) and a conflicting body must return request-id-conflict. ---
    {
        let base = base_dir("hybrid-strict-real-rid-unknown");
        let root = base.join("run");
        let rid = RequestId::new("ac-txn-5-unknown").unwrap();
        let body = vec![pqueue_conformance::fault::spec("rid-item", 7)];
        let rid_key = ClientItemKey::new("rid-item").unwrap();

        // (1) The request_id push is struck at the strict post-SQLite-commit / pre-memory-apply cut: the
        // command commits durably to SQLite AND the object log, but the memory apply faults, so the store
        // poisons and the caller sees Err — a committed-but-unreturned unknown outcome.
        {
            let hook = ArmableHybridHook::new(HybridFaultCutPoint::AfterSqliteCommitBeforeMemoryApply);
            hook.arm();
            let backend =
                objectlog_hybrid_strict_composed(&root, Some(hook.clone() as Arc<dyn HybridFaultHook>));
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .map_err(|e| format!("create_queue: {e:?}"))?;
            let unknown = backend
                .push_with_request_id(&shard, rid.clone(), body.clone(), pqueue_conformance::ts(1), None)
                .await;
            ensure!(
                unknown.is_err(),
                "a request_id push struck at the strict cut must return Err (unknown outcome, no success); got {unknown:?}"
            );
        }
        // (2) Real restart at the same durable root: recovery rehydrates memory from the SQLite
        // ProjectionImage AND rebuilds the push idempotency map from the durable object log. The
        // durably-committed-but-unreturned item is present exactly once.
        {
            let backend = objectlog_hybrid_strict_composed(&root, None);
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .map_err(|e| format!("create_queue after restart: {e:?}"))?;
            let live = backend
                .live_items(&shard, &[rid_key.clone()])
                .await
                .map_err(|e| format!("live_items after restart: {e:?}"))?;
            ensure!(
                live.len() == 1 && live[0].is_some(),
                "the unknown-outcome item must be durably present exactly once after restart; got {live:?}"
            );
            let original_id = live[0].as_ref().unwrap().item_id;
            let m = backend
                .metrics(&shard)
                .await
                .map_err(|e| format!("metrics after restart: {e:?}"))?;
            ensure!(
                m.pending == 1,
                "restart must recover exactly the one durable item (pending=1); got {m:?}"
            );

            // (3) Retry the SAME request_id with the SAME body → REPLAYS the ONE original result (same item
            // id) with 0 duplicate state transitions (projected pending stays 1 — asserted on state, not log
            // rows).
            let replay = backend
                .push_with_request_id(&shard, rid.clone(), body.clone(), pqueue_conformance::ts(2), None)
                .await
                .map_err(|e| format!("unknown-outcome same-body retry: {e:?}"))?;
            ensure!(
                replay == vec![original_id],
                "the same-body retry must replay the ONE original item id (not re-mint); got {replay:?} vs original {original_id:?}"
            );
            let m2 = backend
                .metrics(&shard)
                .await
                .map_err(|e| format!("metrics after replay: {e:?}"))?;
            ensure!(
                m2.pending == 1,
                "the replay must not create a duplicate (projected pending stays 1); got {m2:?}"
            );

            // (4) Retry the SAME request_id with a DIFFERENT body → request-id-conflict.
            let conflict = backend
                .push_with_request_id(
                    &shard,
                    rid,
                    vec![pqueue_conformance::fault::spec("rid-item-different", 8)],
                    pqueue_conformance::ts(3),
                    None,
                )
                .await;
            ensure!(
                matches!(conflict, Err(EngineError::RequestIdConflict)),
                "a conflicting body under the same request_id must return request-id-conflict; got {conflict:?}"
            );
        }
    }
    asserts.push(
        "real hybrid-strict write path: a request_id push struck at the strict cut is durable-but-unreturned (unknown outcome); after a real drop+reopen the log-rebuilt push idempotency REPLAYS the ONE original item id (0 duplicate transitions, projected pending=1) and a conflicting body returns request-id-conflict".into(),
    );

    Ok(asserts)
}

#[tokio::test]
async fn ac_txn_5_hybrid_strict_poison_on_real_server_path() {
    let outcome = ac_txn_5_hybrid_strict_poison_on_real_server_path_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-5 hybrid-strict on the real server write path failed: {:?}",
        outcome.err()
    );
}

/// Open the REAL `objectlog/hybrid-async` composed backend at `root` with the TD-004 debt monitor ARMED —
/// the exact composition `pqueue-server` builds for `PQUEUE_PROJECTION_BACKEND=hybrid-async`
/// (`open_objectlog_hybrid_backend(.., strict=false, Some(thresholds))` → `HybridProjectionStore::open(..)
/// .with_deferred_flush_chunk(..).with_async_monitor(thresholds)`), recovery-on-open. `flush_chunk` bounds
/// how many deferred commands one `flush_deferred` drains so a drain test can step the backlog down one
/// command at a time. No background flusher is spawned here, so the deferred-checkpoint backlog accumulates
/// under the test's control — exactly the real apply-lag signal the monitor folds in on every live apply.
fn objectlog_hybrid_async_composed(
    root: &std::path::Path,
    thresholds: HybridAsyncThresholds,
    flush_chunk: usize,
) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite_path = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap())
        .expect("open object log");
    let hybrid = HybridProjectionStore::open(sqlite_path.to_str().unwrap())
        .expect("open hybrid projection")
        .with_deferred_flush_chunk(flush_chunk)
        .with_async_monitor(thresholds);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-async")
}

/// Push one single-item body through the REAL composed group-commit write path (no `request_id`, the
/// co-buffering hot path). Returns the admission result so a caller can assert a Hard-debt rejection.
async fn async_push(
    backend: &HybridBackend,
    key: &str,
) -> Result<Vec<pqueue_core::ItemId>, EngineError> {
    backend
        .push(
            &pqueue_conformance::shard(),
            vec![pqueue_conformance::fault::spec(key, 5)],
            pqueue_conformance::ts(1),
            None,
        )
        .await
}

/// Push one single-item body under `rid` through the REAL composed request-id write path. Same `rid` + same
/// `key` is a same-body idempotent retry (must replay the committed ids); a fresh `rid` is genuinely new work.
async fn async_push_rid(
    backend: &HybridBackend,
    rid: &str,
    key: &str,
) -> Result<Vec<pqueue_core::ItemId>, EngineError> {
    backend
        .push_with_request_id(
            &pqueue_conformance::shard(),
            RequestId::new(rid.to_string()).unwrap(),
            vec![pqueue_conformance::fault::spec(key, 5)],
            pqueue_conformance::ts(1),
            None,
        )
        .await
}

/// **AC-TXN-5A idempotent replay under debt** on the REAL server-wired hybrid-async composition (TD-004:361,
/// replay-safety). Under Hard async-apply debt, a same-body retry of an ALREADY-COMMITTED `request_id` MUST
/// REPLAY its committed ids (an idempotent replay of durable work adds ZERO new debt), NOT be rejected —
/// while a brand-new `request_id` (genuinely new work) is still rejected with typed backpressure. This proves
/// the admission gate sits AFTER the idempotency replay resolution, not before it.
async fn ac_txn_5a_idempotent_replay_under_debt_scenario() -> AcOutcome {
    let base = base_dir("hybrid-async-idempotent-replay-under-debt");
    let thresholds =
        HybridAsyncThresholds::new(5, 1_000_000, 1_000_000, 3_600_000, 3).expect("valid thresholds");
    let backend = objectlog_hybrid_async_composed(&base.join("run"), thresholds, 1024);
    backend
        .create_queue(pqueue_conformance::qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    let level = |b: &HybridBackend| b.with_projection(|p| p.async_backpressure_level());

    // Commit a request_id'd push while debt is clear: its result is now durable + idempotency-recorded, and
    // it deferred 1 command toward the backlog.
    let committed = async_push_rid(&backend, "rid-committed", "rc")
        .await
        .map_err(|e| format!("commit rid push: {e:?}"))?;
    ensure!(committed.len() == 1, "the committed rid push must mint 1 id");

    // Drive the backlog to the hard budget (5): 1 already deferred + 4 plain new-work pushes → Hard.
    for i in 0..4u64 {
        async_push(&backend, &format!("filler-{i}"))
            .await
            .map_err(|e| format!("filler push {i}: {e:?}"))?;
    }
    ensure!(
        level(&backend) == Some(BackpressureLevel::Hard),
        "5 deferred commands must trip Hard backpressure; got {:?}",
        level(&backend)
    );
    let durable_before = durable_command_count(&backend).await?;

    // (1) Same-body retry of the ALREADY-COMMITTED request_id under Hard debt: MUST replay the original ids
    // (not Unavailable), and add NO new durable command.
    let replay = async_push_rid(&backend, "rid-committed", "rc").await;
    ensure!(
        replay.as_ref().map(Vec::as_slice) == Ok(committed.as_slice()),
        "an idempotent same-body retry of a committed request_id must REPLAY the original ids under Hard debt, not be rejected; got {replay:?}"
    );
    ensure!(
        durable_command_count(&backend).await? == durable_before,
        "an idempotent replay must add NO durable command"
    );

    // (2) A brand-new request_id push (genuinely new work) is still rejected CLOSED with typed backpressure,
    // and adds no durable command.
    let fresh = async_push_rid(&backend, "rid-fresh", "rf").await;
    ensure!(
        matches!(fresh, Err(EngineError::Unavailable)),
        "a brand-new request_id push must be rejected with typed backpressure under Hard debt; got {fresh:?}"
    );
    ensure!(
        durable_command_count(&backend).await? == durable_before,
        "the rejected new-work push must add NO durable command"
    );

    Ok(vec![
        "idempotent replay under debt (real server-wired composition): under Hard backpressure a same-body retry of an already-committed request_id REPLAYS its committed ids (0 new durable commands), while a brand-new request_id push is rejected with typed backpressure (Unavailable) — the admission gate sits after idempotency replay resolution".into(),
    ])
}

/// **AC-TXN-5A hard-debt admission** on the REAL server-wired hybrid-async composition (TD-004:361). Drives
/// GENUINE async-apply debt — the deferred-checkpoint backlog the monitor observes on every live apply, NOT
/// a test poke — over the hard budget, then proves a NEW mutating push fails CLOSED with the typed retryable
/// backpressure error AND leaves no durable/projected effect (asserts on state, not just the error).
async fn ac_txn_5a_hard_debt_admission_scenario() -> AcOutcome {
    let shard = pqueue_conformance::shard();
    let base = base_dir("hybrid-async-hard-debt-admission");
    // Hard budget = 5 deferred commands; every other metric is set out of reach so the apply-lag backlog is
    // the sole trip. flush_chunk large (drain-in-one) — this scenario never drains.
    let thresholds =
        HybridAsyncThresholds::new(5, 1_000_000, 1_000_000, 3_600_000, 3).expect("valid thresholds");
    let backend = objectlog_hybrid_async_composed(&base.join("run"), thresholds, 1024);
    backend
        .create_queue(pqueue_conformance::qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;

    // Drive real debt: 5 single-item pushes. Each seals + live-applies to hot memory and defers its durable
    // SQLite checkpoint (no flusher runs), so the deferred backlog climbs to the hard budget (5). All 5 are
    // admitted (debt below/at budget is admitted; the trip gates the NEXT admission).
    for i in 0..5u64 {
        async_push(&backend, &format!("hd-{i}"))
            .await
            .map_err(|e| format!("push {i} must be admitted below budget: {e:?}"))?;
    }
    ensure!(
        backend.with_projection(|p| p.async_backpressure_level()) == Some(BackpressureLevel::Hard),
        "5 deferred commands must trip Hard async-apply backpressure on the real composition"
    );
    let pending_before = backend
        .metrics(&shard)
        .await
        .map_err(|e| format!("metrics before: {e:?}"))?
        .pending;
    ensure!(
        pending_before == 5,
        "the 5 admitted pushes must be projected (pending=5); got {pending_before}"
    );
    let durable_before = durable_command_count(&backend).await?;

    // A NEW mutating push under Hard debt fails CLOSED with the typed retryable backpressure error...
    let rejected = async_push(&backend, "hd-rejected").await;
    ensure!(
        matches!(rejected, Err(EngineError::Unavailable)),
        "a push over the hard debt budget must be rejected with the typed retryable backpressure error (Unavailable); got {rejected:?}"
    );
    // ...and left NO durable and NO projected effect (assert on state, not merely the error type).
    let durable_after = durable_command_count(&backend).await?;
    ensure!(
        durable_after == durable_before,
        "the rejected push must add NO durable command (before={durable_before}, after={durable_after})"
    );
    let pending_after = backend
        .metrics(&shard)
        .await
        .map_err(|e| format!("metrics after: {e:?}"))?
        .pending;
    ensure!(
        pending_after == 5,
        "the rejected push must not be applied/acknowledged (pending stays 5); got {pending_after}"
    );

    Ok(vec![
        "hard-debt admission (real server-wired composition): 5 real deferred-checkpoint commands trip Hard backpressure; a new mutating push fails closed with the typed retryable error (Unavailable) and adds 0 durable + 0 projected commands".into(),
    ])
}

/// **AC-TXN-5A high-water / retention withholding** on the REAL server-wired hybrid-async composition
/// (TD-004:361). Establishes a genuine durable SQLite high-water, drives real debt over budget, proves the
/// lagging high-water AND retention advancement are WITHHELD while Hard, then drains the backlog and proves
/// both advance the instant debt clears below the release band.
async fn ac_txn_5a_high_water_withhold_scenario() -> AcOutcome {
    let shard = pqueue_conformance::shard();
    let base = base_dir("hybrid-async-high-water-withhold");
    // Hard budget = 6; release band = strictly below 50% (< 3). flush_chunk = 1 so the drain steps the
    // backlog down one command at a time and the Hard→Clear release lands on an exact backlog value.
    let thresholds =
        HybridAsyncThresholds::new(6, 1_000_000, 1_000_000, 3_600_000, 3).expect("valid thresholds");
    let backend = objectlog_hybrid_async_composed(&base.join("run"), thresholds, 1);
    backend
        .create_queue(pqueue_conformance::qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;

    let deferred = |b: &HybridBackend| b.with_projection(|p| p.deferred_command_count());
    let level = |b: &HybridBackend| b.with_projection(|p| p.async_backpressure_level());
    let high_water = |b: &HybridBackend| {
        b.with_projection(|p| ProjectionStore::recovery_high_water(p, &shard))
    };
    // The wired retention gate the REAL reap path (`reap_terminal_items_locked` →
    // `ProjectionStore::retention_may_advance`) consults before advancing terminal-item retention / segment
    // expiry. See the honest GAP below: the gate is wired into production, but no retention-ADVANCEMENT path
    // is implemented for `objectlog/hybrid-async` yet (the hybrid store does not override `reap_terminal_items`
    // and object-log segment expiry is unimplemented), so the gate's downstream effect is not exercisable.
    let retention_gate_open = |b: &HybridBackend| b.with_projection(|p| p.async_retention_may_advance());

    // Establish a NON-None durable SQLite high-water: 2 pushes (below budget), then drain so the checkpoint
    // advances the durable high-water. This is the high-water that MUST later be withheld under debt.
    for i in 0..2u64 {
        async_push(&backend, &format!("hw-a-{i}"))
            .await
            .map_err(|e| format!("seed push {i}: {e:?}"))?;
    }
    while deferred(&backend) > 0 {
        backend
            .flush_deferred_projection()
            .map_err(|e| format!("seed flush: {e:?}"))?;
    }
    let hw_seed = high_water(&backend).map_err(|e| format!("seed high-water: {e:?}"))?;
    ensure!(
        hw_seed.is_some(),
        "a drained checkpoint must advertise a real durable high-water before debt; got {hw_seed:?}"
    );
    ensure!(
        level(&backend) != Some(BackpressureLevel::Hard) && retention_gate_open(&backend),
        "the store must be below backpressure with the retention gate open after the seed drain"
    );

    // Drive real debt over budget: 6 more pushes with NO flush. The deferred backlog hits 6 == hard budget.
    for i in 0..6u64 {
        async_push(&backend, &format!("hw-b-{i}"))
            .await
            .map_err(|e| format!("debt push {i}: {e:?}"))?;
    }
    ensure!(
        level(&backend) == Some(BackpressureLevel::Hard),
        "6 deferred commands must trip Hard backpressure; got {:?}",
        level(&backend)
    );
    // While Hard: the lagging high-water is WITHHELD (None) even though the underlying durable high-water is
    // still `hw_seed` (REAL, observable on `recovery_high_water`), and the retention gate the real reap path
    // consults is CLOSED.
    ensure!(
        high_water(&backend)
            .map_err(|e| format!("withheld high-water: {e:?}"))?
            .is_none(),
        "the lagging high-water must be withheld (None) while async-apply debt is Hard"
    );
    ensure!(
        !retention_gate_open(&backend),
        "the retention gate consulted by the real reap path must be CLOSED while async-apply debt is Hard"
    );
    ensure!(
        backend.with_projection(|p| ProjectionStore::recovery_backpressured(p, &shard)),
        "recovery must report hard backpressure so replay does not trust the lagging high-water"
    );

    // Drain one command at a time. Hysteresis holds Hard (high-water withheld, retention gate closed) until
    // the backlog clears below the release band; then BOTH release in the same step and the high-water is now
    // strictly ahead of the pre-debt seed (it advanced through the drained batch).
    loop {
        if level(&backend) == Some(BackpressureLevel::Hard) {
            ensure!(
                high_water(&backend)
                    .map_err(|e| format!("mid-drain high-water: {e:?}"))?
                    .is_none(),
                "high-water must stay withheld while still Hard (deferred={})",
                deferred(&backend)
            );
            ensure!(
                !retention_gate_open(&backend),
                "the retention gate must stay closed while still Hard (deferred={})",
                deferred(&backend)
            );
            ensure!(
                deferred(&backend) > 0,
                "backlog must remain drainable while Hard — never releases"
            );
            backend
                .flush_deferred_projection()
                .map_err(|e| format!("drain flush: {e:?}"))?;
        } else {
            let hw_cleared = high_water(&backend).map_err(|e| format!("cleared high-water: {e:?}"))?;
            ensure!(
                hw_cleared.is_some(),
                "the high-water must advance the instant debt clears below the release band"
            );
            ensure!(
                retention_gate_open(&backend),
                "the retention gate must reopen the instant debt clears below the release band"
            );
            let (seed, cleared) = (hw_seed.as_ref().unwrap(), hw_cleared.as_ref().unwrap());
            ensure!(
                cleared.sequence > seed.sequence,
                "the released high-water must be strictly ahead of the withheld seed (advanced through the drained batch); seed={:?} cleared={:?}",
                seed,
                cleared
            );
            break;
        }
    }

    Ok(vec![
        "high-water withholding (real server-wired composition): a real deferred backlog over budget withholds the lagging durable high-water (None, observed on recovery_high_water) and recovery reports hard backpressure while Hard; the high-water advances strictly ahead of the withheld seed exactly once the drain clears debt below the release band".into(),
        "retention gate wired (real path, effect not yet exercisable): the retention gate the real reap path (reap_terminal_items_locked → retention_may_advance) consults is CLOSED under Hard debt and reopens once debt clears".into(),
        "GAP (retention advancement not exercised end-to-end): objectlog/hybrid-async implements no retention-ADVANCEMENT path to gate — the hybrid store does not override reap_terminal_items (trait-default no-op) and object-log segment expiry is unimplemented — so while the TD-004 retention gate is wired into the real reap path, its downstream withholding effect cannot be observed/asserted end-to-end on this backend; tracked follow-up (implement + prove hybrid-async retention/segment-expiry under debt)".into(),
    ])
}

/// **AC-TXN-5A** (TP-003 §3.10 row 211, `objectlog/hybrid-async`): the success barrier is manifest commit
/// PLUS a completed synchronous memory apply — a memory-apply failure must not return success even though
/// the manifest commit is durable; the deferred SQLite checkpoint applies a whole ordered batch exactly
/// once; a crash before memory apply resolves as unknown-outcome by `request_id` (delegated to the generic
/// AC-TXN-3 harness on this exact substrate); and async apply debt over budget fails new admission closed.
async fn ac_txn_5a_hybrid_async_success_barrier_scenario() -> AcOutcome {
    let mut asserts = Vec::new();
    let shard = pqueue_conformance::shard();

    // --- (a) success barrier: a memory-apply failure must not return success, even though the object-log
    // manifest commit that preceded it IS durable. ---
    {
        let base = base_dir("hybrid-async-success-barrier");
        let backend = objectlog_hybrid_with_fault_hook(
            &base.join("run"),
            Arc::new(HybridCrashAt(HybridFaultCutPoint::DuringMemoryApply)),
        );
        backend
            .create_queue(pqueue_conformance::qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let rid = RequestId::new("ac-txn-5a-barrier").unwrap();
        let body = vec![pqueue_conformance::fault::spec("ac-txn-5a-barrier-item", 5)];
        let err = backend
            .push_with_request_id(&shard, rid, body, pqueue_conformance::ts(1), None)
            .await;
        ensure!(
            err.is_err(),
            "manifest commit ALONE, without a completed synchronous memory apply, must not return success"
        );
        let durable = durable_command_count(&backend).await?;
        ensure!(
            durable == 1,
            "the manifest commit is durable on the object log even though the success barrier withheld success; got {durable} durable commands"
        );
    }
    asserts.push(
        "success barrier: a memory-apply failure withholds success even though the preceding manifest commit is durable".into(),
    );

    // --- (b) ordered exactly-once batch apply: several live-applied deferred commands drain in ONE flush,
    // in order, and the SQLite logical high-water advances through the whole batch exactly once. ---
    {
        let base = base_dir("hybrid-async-ordered-batch");
        std::fs::create_dir_all(&base).ok();
        let path = base.join("projection.sqlite");
        let mut hybrid =
            HybridProjectionStore::open(path.to_str().unwrap()).expect("open hybrid projection");
        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
            .map_err(|e| format!("ensure_shard: {e:?}"))?;

        let batch: Vec<(CommandPosition, pqueue_engine::CommandEnvelope)> = (0..3)
            .map(|i: u64| {
                let id = (i + 1).to_string();
                (
                    CommandPosition::new(shard.clone(), 0, i),
                    hybrid_push_env(&id, &format!("k{id}")),
                )
            })
            .collect();
        for (pos, env) in &batch {
            ProjectionStore::apply_live(
                &mut hybrid,
                std::slice::from_ref(pos),
                std::slice::from_ref(env),
            )
            .map_err(|e| format!("apply_live: {e:?}"))?;
        }
        ensure!(
            hybrid.deferred_command_count() == 3,
            "all 3 live-applied commands must be queued for deferred SQLite apply before any flush; got {}",
            hybrid.deferred_command_count()
        );
        ProjectionStore::flush_deferred(&mut hybrid)
            .map_err(|e| format!("flush_deferred: {e:?}"))?;
        ensure!(
            hybrid.deferred_command_count() == 0,
            "one flush must drain the whole ordered batch exactly once; {} left deferred",
            hybrid.deferred_command_count()
        );
        let hw = ProjectionStore::recovery_high_water(&hybrid, &shard)
            .map_err(|e| format!("recovery_high_water: {e:?}"))?;
        ensure!(
            hw == Some(CommandPosition::new(shard.clone(), 0, 2)),
            "the SQLite logical high-water must advance through the whole ordered batch (0,1,2) exactly once; got {hw:?}"
        );
        // A second flush with nothing pending is a true no-op — no duplicate SQLite work, no re-advance.
        ProjectionStore::flush_deferred(&mut hybrid)
            .map_err(|e| format!("second flush_deferred: {e:?}"))?;
        let hw2 = ProjectionStore::recovery_high_water(&hybrid, &shard)
            .map_err(|e| format!("recovery_high_water after no-op flush: {e:?}"))?;
        ensure!(hw2 == hw, "a no-op flush must not move the high-water");
    }
    asserts.push(
        "ordered batching: 3 live-applied commands drain in one flush, the SQLite high-water advances through the whole batch exactly once, and a no-op flush does not re-advance it".into(),
    );

    // --- (b2) async SQLite checkpoint fault (DuringAsyncSqliteApply): a fault struck strictly INSIDE the
    // deferred async-apply — the real hybrid-async background checkpoint step, reached only via
    // `flush_deferred` — must NOT silently drop the batch. The deferred commands stay queued, the store
    // poisons fail-closed (so it never keeps retrying against a possibly-corrupt SQLite image), and every
    // subsequent op errors until restart. This actually installs + triggers the DuringAsyncSqliteApply cut
    // the profile declares, rather than only the AfterSqliteCommitBeforeMemoryApply / DuringMemoryApply
    // instants. ---
    {
        let base = base_dir("hybrid-async-checkpoint-fault");
        std::fs::create_dir_all(&base).ok();
        let path = base.join("projection.sqlite");
        let mut hybrid =
            HybridProjectionStore::open(path.to_str().unwrap()).expect("open hybrid projection");
        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let pos = CommandPosition::new(shard.clone(), 0, 0);
        let env = hybrid_push_env("1", "kx");
        ProjectionStore::apply_live(
            &mut hybrid,
            std::slice::from_ref(&pos),
            std::slice::from_ref(&env),
        )
        .map_err(|e| format!("apply_live: {e:?}"))?;
        ensure!(
            hybrid.deferred_command_count() == 1,
            "the live-applied command must be queued for deferred async SQLite apply; got {}",
            hybrid.deferred_command_count()
        );
        hybrid.set_fault_hook(Some(Arc::new(HybridCrashAt(
            HybridFaultCutPoint::DuringAsyncSqliteApply,
        ))));
        let flush = ProjectionStore::flush_deferred(&mut hybrid);
        ensure!(
            flush.is_err(),
            "a fault struck DURING the async SQLite checkpoint apply must not report flush success"
        );
        ensure!(
            hybrid.deferred_command_count() == 1,
            "the faulted async batch must stay queued (untouched), not silently drop; got {} deferred",
            hybrid.deferred_command_count()
        );
        hybrid.set_fault_hook(None);
        let after = ProjectionStore::metrics(&hybrid, &shard);
        ensure!(
            after.is_err(),
            "the async-apply fault must poison the store fail-closed (reads included) until restart; got {after:?}"
        );
    }
    asserts.push(
        "async-apply fault (DuringAsyncSqliteApply): a fault inside the deferred SQLite checkpoint keeps the ordered batch queued (0 silently dropped) and poisons the store fail-closed until restart".into(),
    );

    // --- (c) unknown-outcome-by-request_id: delegated to the generic AC-TXN-3 harness run against this
    // exact objectlog/hybrid substrate — a crash after the durable append but before the response is
    // observed resolves the request_id replay to the ONE committed result after restart. ---
    let txn3 = ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await?;
    asserts.push(format!(
        "unknown-outcome-by-request_id (delegated to AC-TXN-3 on the objectlog/hybrid substrate): {} assertions held",
        txn3.len()
    ));

    // --- (d) backpressure fail-closed on the REAL server-wired composition (TD-004:361, prior GAP now
    // CLOSED): the `hybrid-async` arm of `pqueue-server` now arms the `HybridAsyncMonitor` inside the
    // `HybridProjectionStore` write path (`with_async_monitor`), so real deferred-checkpoint debt gates real
    // mutating admission (`admit_mutation`) and withholds the lagging `recovery_high_water` + retention. Both
    // facets are proven end-to-end on the composition `start()` builds, driven by genuine apply-lag rather
    // than a standalone-monitor poke. See `ac_txn_5a_hard_debt_fails_mutating_admission` and
    // `ac_txn_5a_debt_withholds_high_water_and_retention` (also run standalone). ---
    asserts.extend(ac_txn_5a_hard_debt_admission_scenario().await?);
    asserts.extend(ac_txn_5a_idempotent_replay_under_debt_scenario().await?);
    asserts.extend(ac_txn_5a_high_water_withhold_scenario().await?);

    Ok(asserts)
}

#[tokio::test]
async fn ac_txn_5a_hybrid_async_success_barrier() {
    let outcome = ac_txn_5a_hybrid_async_success_barrier_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-5A hybrid-async success barrier failed: {:?}",
        outcome.err()
    );
}

/// AC-TXN-5A acceptance test 2 (bead pqueue-c21635b9): on the REAL server-wired hybrid-async composition,
/// real async-apply debt over the hard budget fails a new mutating push CLOSED with the typed backpressure
/// error and leaves no durable/projected effect.
#[tokio::test]
async fn ac_txn_5a_hard_debt_fails_mutating_admission() {
    let outcome = ac_txn_5a_hard_debt_admission_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-5A hard-debt admission gate failed: {:?}",
        outcome.err()
    );
}

/// AC-TXN-5A acceptance test 3 (bead pqueue-c21635b9): on the REAL server-wired hybrid-async composition,
/// real debt over budget withholds the lagging recovery high-water and retention advancement until the
/// backlog drains below the release band, at which point both advance.
#[tokio::test]
async fn ac_txn_5a_debt_withholds_high_water_and_retention() {
    let outcome = ac_txn_5a_high_water_withhold_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-5A high-water/retention withholding failed: {:?}",
        outcome.err()
    );
}

/// AC-TXN-5A replay-safety (bead pqueue-c21635b9, codex review): under Hard debt an idempotent same-body
/// retry of an already-committed request_id REPLAYS (not rejected), while a brand-new request_id is rejected.
#[tokio::test]
async fn ac_txn_5a_idempotent_replay_admitted_under_debt() {
    let outcome = ac_txn_5a_idempotent_replay_under_debt_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-5A idempotent replay under debt failed: {:?}",
        outcome.err()
    );
}

/// AC-TXN-7 (TP-003 §3.10 row 213): the commit-latency bound is NOT a correctness knob. Repeat the
/// transaction-contract invariant-bearing scenarios across the objectlog composition's two commit-latency-
/// bound WRITE REGIMES and assert 0 invariant delta — the observable state must be IDENTICAL across settings,
/// only latency/cost metadata may differ.
///
/// **The two swept settings** (the family where the commit-latency bound is a real knob — the plain
/// `ComposedObjectLogBackend`):
/// * **force-seal** — [`objectlog_factory`] (`ObjectLog::open`): the synchronous seal-per-`append` path, the
///   minimal-latency extreme (the seal fires immediately, so the ack lands on the append).
/// * **group-commit** — [`objectlog_group_commit_factory`] (`ObjectLog::open_group_commit`,
///   `SegmentConfig::new(1,1)`): the co-buffered ack-after-seal cost-optimized write path.
///
/// These two regimes are the commit-latency-bound settings the **runtime-free** transaction-contract
/// scenarios can exercise SYNCHRONOUSLY: force-seal acks on the per-append seal; group-commit with
/// `target_bytes = 1` acks on the immediate size-seal. TP-002 E3's numeric ≥4-bound latency sweep (row 198,
/// e.g. 1/5/20/100 ms) is a LATENCY/COST performance benchmark that needs a runtime flusher driving
/// `flush_tick` (a below-threshold buffer only acks once the latency window fires) — it is measured by
/// `performance_object_log_e3_live_tests` / `composed_group_commit`, and there the numeric bound changes ONLY
/// ack timing, never WHAT commits. TP-002 E3 row 204 names the transaction invariants required "under the same
/// bound sweep" as exactly success-visible (AC-TXN-1), rejection-no-effect (AC-TXN-2), and unknown-outcome
/// replay (AC-TXN-3); this scenario sweeps all three PLUS the AC-TXN-6 cross-combination parity across the two
/// regimes.
///
/// Equality proof: AC-TXN-1/2/3 return the set of invariants they PROVED (each string is pushed only after its
/// `ensure!` held), so an identical vector across the two regimes means the identical invariant set held under
/// each; a regime that took any different branch (weakened/added an invariant) would diverge and fail here.
/// AC-TXN-6 parity adds the strong observable-state teeth: it drives the identical op history + failure
/// schedule against BOTH regimes and asserts the final visible metrics, `select_eligible` order, pending/
/// active-lease set, per-item terminal-outcome records, and per-request_id idempotency-record behavior are
/// byte-identical.
async fn ac_txn_7_latency_sweep_scenario() -> AcOutcome {
    let mut asserts = Vec::new();

    // --- AC-TXN-1 (success durable+visible) at each commit-latency regime; the proven-invariant set must be
    // identical. ---
    let fs1 = ac_txn_1_success_durable_visible(objectlog_factory()).await?;
    let gc1 = ac_txn_1_success_durable_visible(objectlog_group_commit_factory()).await?;
    ensure!(
        fs1 == gc1,
        "AC-TXN-1 success-visible invariants diverge across commit-latency settings:\n force-seal={fs1:?}\n group-commit={gc1:?}"
    );
    asserts.push(format!(
        "AC-TXN-1 (success durable+visible): identical proven-invariant set across force-seal and group-commit ({} assertions each)",
        fs1.len()
    ));

    // --- AC-TXN-2 (rejection no-effect) at each commit-latency regime. ---
    let fs2 = ac_txn_2_rejection_no_effect(objectlog_factory(), DURABLE).await?;
    let gc2 = ac_txn_2_rejection_no_effect(objectlog_group_commit_factory(), DURABLE).await?;
    ensure!(
        fs2 == gc2,
        "AC-TXN-2 rejection-no-effect invariants diverge across commit-latency settings:\n force-seal={fs2:?}\n group-commit={gc2:?}"
    );
    asserts.push(format!(
        "AC-TXN-2 (rejection no-effect): identical proven-invariant set across force-seal and group-commit ({} assertions each)",
        fs2.len()
    ));

    // --- AC-TXN-3 (unknown-outcome replay) at each commit-latency regime. ---
    let fs3 = ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await?;
    let gc3 = ac_txn_3_unknown_outcome_replay(objectlog_group_commit_factory(), DURABLE).await?;
    ensure!(
        fs3 == gc3,
        "AC-TXN-3 unknown-outcome replay invariants diverge across commit-latency settings:\n force-seal={fs3:?}\n group-commit={gc3:?}"
    );
    asserts.push(format!(
        "AC-TXN-3 (unknown-outcome replay): identical proven-invariant set across force-seal and group-commit ({} assertions each)",
        fs3.len()
    ));

    // --- AC-TXN-6 (cross-combination parity) run DIRECTLY across the two commit-latency regimes: the strong
    // observable-state equality proof. force-seal vs group-commit must produce the IDENTICAL final visible
    // metrics (incl. complete/failed terminal counts), `select_eligible` order, pending/active-lease set,
    // per-item terminal-outcome records reconstructed from the durable log, and per-request_id idempotency-
    // record behavior (same-body replay returns the original result, conflicting body -> RequestIdConflict, no
    // phantom commit). Only latency/cost metadata may differ. ---
    let parity = ac_txn_6_parity(objectlog_factory(), objectlog_group_commit_factory()).await?;
    asserts.extend(
        parity
            .into_iter()
            .map(|a| format!("AC-TXN-6 parity(force-seal vs group-commit): {a}")),
    );

    // --- Honest per-AC coverage of the remaining ACs. These are capability-N/A for a cross-REGIME comparison
    // (a truthful declaration, NOT a coverage GAP — see `record`): they do not live on the plain-objectlog
    // force-seal-vs-group-commit axis, so they cannot be swept across it. ---
    asserts.push(
        "capability-N/A (cross-regime comparison not applicable, not a coverage hole): AC-TXN-4 is the object-log-SUBSTRATE-internal crash-point matrix whose FaultCutPoints live in the force-seal `append` pipeline; the group-commit write path is a structurally different pipeline (gc_enqueue/gc_seal + externalized flush) with different cut points, so the identical cut-point matrix cannot be replayed under group-commit. AC-TXN-4's invariants are exercised at the force-seal setting (AC-TXN-4 row); the group-commit substrate's own crash recovery is covered by the `composed_group_commit` reopen tests.".into(),
    );
    asserts.push(
        "capability-N/A (cross-regime comparison not applicable, not a coverage hole): AC-TXN-5 / AC-TXN-5A are hybrid-projection (HybridProjectionStore) invariants; the hybrid composition exists only on the group-commit substrate (there is no force-seal hybrid variant), so they are not a force-seal-vs-group-commit comparison. They are covered on the hybrid family (AC-TXN-5 / AC-TXN-5A rows).".into(),
    );

    Ok(asserts)
}

/// AC-TXN-7 acceptance test (bead pqueue-1bcf0104): the E3 commit-latency-bound sweep does not change the
/// transaction-contract invariants. Sweeps AC-TXN-1/2/3 + the AC-TXN-6 parity across force-seal vs
/// group-commit and asserts 0 invariant delta.
#[tokio::test]
async fn ac_txn_7_invariants_unchanged_across_latency_sweep() {
    let outcome = ac_txn_7_latency_sweep_scenario().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-7 commit-latency-bound sweep invariance failed: {:?}",
        outcome.err()
    );
}

#[tokio::test]
async fn ac_txn_contract_matrix() {
    let mut records: Vec<AcEvidence> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    // --- memory (composed in-memory, atomic, NON-durable dev profile) ---
    record_na(
        &mut records,
        "AC-TXN-1",
        "memory",
        "non-durable in-memory dev profile: kill/restart durability is not applicable",
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "memory",
        ac_txn_2_rejection_no_effect(
            |_: &str| pqueue_memory::composed_memory_backend(),
            NON_DURABLE,
        )
        .await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "memory",
        ac_txn_3_unknown_outcome_replay(
            |_: &str| pqueue_memory::composed_memory_backend(),
            NON_DURABLE,
        )
        .await,
    );

    // --- sqlite-log (composed SqliteLog + in-memory projection, atomic, durable) ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-1",
        "sqlite_log",
        ac_txn_1_success_durable_visible(sqlite_log_factory()).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "sqlite_log",
        ac_txn_2_rejection_no_effect(sqlite_log_factory(), DURABLE).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "sqlite_log",
        ac_txn_3_unknown_outcome_replay(sqlite_log_factory(), DURABLE).await,
    );

    // --- sqlite_relational (unified sqlite log + relational projection, atomic, durable, GATE-CAPABLE) ---
    // The only matrix profile that supports BOTH the atomic-only BatchUpdate AND the gate-only SetGates, so
    // AC-TXN-1 here exercises EVERY row-206 mutating op for real under kill/reopen (no capability-N/A).
    record(
        &mut records,
        &mut failures,
        "AC-TXN-1",
        "sqlite_relational",
        ac_txn_1_success_durable_visible(sqlite_relational_factory()).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "sqlite_relational",
        ac_txn_2_rejection_no_effect(sqlite_relational_factory(), DURABLE).await,
    );
    // NB: AC-TXN-3 is intentionally NOT run on `sqlite_relational`. Its `AfterAppendBeforeApply` cut injects a
    // durable-but-unapplied window via the raw `inject_commit` seam (append, skip apply, reopen, replay). The
    // UNIFIED relational store has no such window: its log axis IS its projection axis (`SqliteRelational` on
    // both), so `Backend::write`'s append+apply commit together in one relational transaction and there is no
    // durable log entry that reopens as unapplied. The mid-pipeline cut is architecturally inapplicable here,
    // not a coverage gap — the composed log+projection profiles (sqlite_log/objectlog/postgres) cover AC-TXN-3.
    record_na(
        &mut records,
        "AC-TXN-3",
        "sqlite_relational",
        "capability-N/A: unified relational store couples log-append and projection-apply in one transaction, so AC-TXN-3's AfterAppendBeforeApply durable-but-unapplied cut point has no window here (log axis IS projection axis)",
    );

    // --- objectlog (composed ObjectLog + in-memory projection, eventual-apply, durable) ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-1",
        "objectlog",
        ac_txn_1_success_durable_visible(objectlog_factory()).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "objectlog",
        ac_txn_2_rejection_no_effect(objectlog_factory(), DURABLE).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "objectlog",
        ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await,
    );

    // --- object_log_sqlite (hybrid, eventual-apply, durable) — a COMMITTED profile ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-1",
        "object_log_sqlite",
        ac_txn_1_success_durable_visible(objectlog_sqlite_factory()).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "object_log_sqlite",
        ac_txn_2_rejection_no_effect(objectlog_sqlite_factory(), DURABLE).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "object_log_sqlite",
        ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await,
    );

    // --- AC-TXN-4 object-log-internal crash-point matrix (5 reachable cut points; see module doc). ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-4",
        "objectlog",
        ac_txn_4_crash_point_matrix().await,
    );

    // --- AC-TXN-5 hybrid-strict poison + restart hydration + request-id semantics (see module doc). The
    // real-server-path row (bead pqueue-da1965d7) drives all four clauses through the WIRED
    // `objectlog/hybrid-strict` composed backend (`with_strict_apply(true)`), closing the prior
    // real-server-path GAP; the ProjectionStore-layer row remains as the direct-apply companion view. ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-5",
        "object_log_sqlite(hybrid-strict, real server write path)",
        ac_txn_5_hybrid_strict_poison_on_real_server_path_scenario().await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-5",
        "object_log_sqlite(hybrid-strict, ProjectionStore-layer)",
        ac_txn_5_hybrid_strict_poison_replay_scenario().await,
    );

    // --- AC-TXN-5A hybrid-async success barrier + ordered batching + backpressure (see module doc). ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-5A",
        "object_log_sqlite(hybrid-async)",
        ac_txn_5a_hybrid_async_success_barrier_scenario().await,
    );

    // --- AC-TXN-6 cross-combination parity (sqlite-log[atomic] vs object_log_sqlite[eventual]) ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-6",
        "sqlite_log|object_log_sqlite",
        ac_txn_6_parity(sqlite_log_factory(), objectlog_sqlite_factory()).await,
    );

    // --- AC-TXN-7 (row 213): the commit-latency bound is not a correctness knob. Sweep AC-TXN-1/2/3 + the
    // AC-TXN-6 parity across the objectlog composition's two commit-latency-bound write regimes (force-seal vs
    // group-commit) and assert 0 invariant delta. See `ac_txn_7_latency_sweep_scenario` for the full scope
    // (incl. why AC-TXN-4 / AC-TXN-5 / AC-TXN-5A are capability-N/A for a cross-regime comparison). ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-7",
        "objectlog(force-seal|group-commit)",
        ac_txn_7_latency_sweep_scenario().await,
    );

    let path = write_evidence("tp003-ac-txn-matrix.jsonl", &records).expect("write evidence jsonl");
    eprintln!("AC-TXN evidence written to {}", path.display());
    for r in &records {
        eprintln!(
            "  [{}] {} => {} ({} assertions)",
            r.result,
            r.ac,
            r.backend,
            r.assertions.len()
        );
    }
    assert!(
        failures.is_empty(),
        "AC-TXN matrix failures:\n{}",
        failures.join("\n")
    );
}

/// Postgres rows run in a SEPARATE non-tokio test because the sync postgres client panics under a tokio
/// runtime (see `pqueue-postgres/tests/conformance.rs`). Env-gated on `PQUEUE_PG_TEST_URL`; LOUD-skips
/// when absent. Postgres composed-log is atomic + in-memory projection, so it carries the same
/// documented request_id-across-restart gap as sqlite-log.
#[test]
fn ac_txn_contract_matrix_postgres() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "AC-TXN POSTGRES SKIPPED (external_transaction_contract_matrix_tests) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };

    // A tag-keyed durable reopen factory: the per-AC prefix + the per-phase `tag` name one postgres
    // schema, so reopening with the same tag recovers the same durable rows while different phases get
    // isolated schemas. Schema identifiers are sanitized (postgres identifiers dislike hyphens).
    let pg_factory = |prefix: String, url: String| {
        let run = COUNTER.fetch_add(1, Ordering::SeqCst);
        move |tag: &str| {
            let sch = format!(
                "pq_actxn_{prefix}_{}_{run}_{}",
                std::process::id(),
                tag.replace('-', "_")
            );
            pqueue_postgres::composed_postgres_backend_in_schema(&url, &sch)
                .expect("connect postgres")
        }
    };

    let mut records: Vec<AcEvidence> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    record(
        &mut records,
        &mut failures,
        "AC-TXN-1",
        "postgres",
        futures::executor::block_on(ac_txn_1_success_durable_visible(pg_factory(
            "txn1".into(),
            url.clone(),
        ))),
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "postgres",
        futures::executor::block_on(ac_txn_2_rejection_no_effect(
            pg_factory("txn2".into(), url.clone()),
            DURABLE,
        )),
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "postgres",
        futures::executor::block_on(ac_txn_3_unknown_outcome_replay(
            pg_factory("txn3".into(), url.clone()),
            DURABLE,
        )),
    );

    let path =
        write_evidence("tp003-ac-txn-matrix-postgres.jsonl", &records).expect("write pg evidence");
    eprintln!("AC-TXN postgres evidence written to {}", path.display());
    for r in &records {
        eprintln!(
            "  [{}] {} => {} ({} assertions)",
            r.result,
            r.ac,
            r.backend,
            r.assertions.len()
        );
    }
    assert!(
        failures.is_empty(),
        "AC-TXN postgres failures:\n{}",
        failures.join("\n")
    );
}
