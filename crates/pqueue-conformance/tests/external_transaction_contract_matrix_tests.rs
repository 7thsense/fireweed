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
//! | AC-TXN-2 rejection no-effect | partial (in-proc) | partial§ | partial§ | partial§ | partial§ | partial§ |
//! | AC-TXN-3 unknown-outcome replay | partial (in-proc) | partial§ | n/a (unified store, no cut window) | partial§ | partial§ | partial§ |
//! | AC-TXN-4 objectlog crash-point matrix | — | — | — | partial (5 internal cut points)* | — | — |
//! | AC-TXN-5 hybrid poison + replay | — | — | — | | partial (projection cut points)† | — |
//! | AC-TXN-5A hybrid-async success barrier | — | — | — | | partial (projection cut points)† | — |
//! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | | — |
//! | AC-TXN-7 latency-bound invariance | — | — | — | partial (force-seal vs group-commit) | | — |
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
//! `§` AC-TXN-2 (row 207) drives per-item-invalid/unknown-id/request-id-conflict
//! rejections + sibling survival but not envelope-invalid batches, stale-lease, capacity/unavailable, or
//! commit-timeout paths; AC-TXN-3 (row 208) proves request_id exactly-once replay for PUSH at BeforeAppend/
//! AfterResponse/AfterApplyBeforeResponse (the AfterAppendBeforeApply mid-pipeline cut is item-level only —
//! the raw seam carries no request_id) but not request_id replay for claim/renew/finalize/update/purge at
//! every cut. The specific assertions each row DOES make are genuine and pass; the label is honest about scope.
//!
//! `*` AC-TXN-4 (`partial`) drives [`pqueue_objectlog::ObjectLog`]'s `LogStore` impl DIRECTLY (bypassing
//! `ComposedBackend`) with the [`pqueue_objectlog::FaultHook`] seam added for this row, striking 5 instants
//! strictly INSIDE the segmented substrate's own commit pipeline that the public `Backend::write` seam
//! cannot reach: `BeforeSegmentWrite`, `AfterSegmentWriteBeforeManifest`, `AfterManifestBeforeAck` (whose
//! "0 duplicate active leases" clause now replays the recovered log through a fresh projection and asserts
//! exactly one ACTIVE lease in the projected serving image, not just one durable Claim log command),
//! `DuringOwnerReassignment`, `DuringSnapshotWrite` (see `ac_txn_4_crash_point_matrix` below). It is `partial`
//! (not `pass`) because TP-003 §3.10 row 209 also names two projection-apply instants — "during projection
//! apply" and "after projection apply before response" — that live in a distinct architectural layer
//! (`pqueue-engine`'s `ComposedBackend`, which applies a batch only after `LogStore::append` returned `Ok`)
//! rather than inside the segmented substrate; those are exercised by AC-TXN-5/5A (`DuringMemoryApply`) and
//! AC-TXN-3 (`AfterApplyBeforeResponse`). ("During manifest CAS" collapses into the atomic create-only PUT
//! already bracketed by the two manifest cut points.) The row's `GAP` assertion states this explicitly.
//!
//! `†` AC-TXN-5/5A (both `partial`; see `ac_txn_5_hybrid_strict_poison_replay_scenario` /
//! `ac_txn_5a_hybrid_async_success_barrier_scenario` below) add the analogous seam on the PROJECTION side —
//! [`pqueue_sqlite::HybridFaultHook`] on `HybridProjectionStore` — for the instants the public seam cannot
//! isolate: a fault strictly between the durable SQLite commit and the in-memory apply
//! (`AfterSqliteCommitBeforeMemoryApply`), a memory-apply failure (`DuringMemoryApply`), and one strictly
//! inside the deferred async SQLite checkpoint (`DuringAsyncSqliteApply`, now actually installed + triggered
//! via `flush_deferred` in AC-TXN-5A). Two honest caveats keep these `partial`, not `pass`: (1) real-server
//! path — the `AfterSqliteCommitBeforeMemoryApply` poison instant is the SQLite-first `apply`
//! (`apply_durable_then_memory`) ordering, which NO real server pipeline runs: both the `hybrid` and
//! `hybrid-async` runtime profiles compose `with_group_commit(true)` and apply MEMORY-FIRST via
//! `apply_live_owned` (deferring SQLite to `flush_deferred`), and pqueue-server wires no `hybrid-strict`
//! profile — so that cut is verified at the ProjectionStore layer only; (2) backpressure — AC-TXN-5A's
//! fail-closed clause is proven against the standalone [`pqueue_sqlite::HybridAsyncMonitor`], but pqueue-server
//! opens `HybridProjectionStore` WITHOUT wiring that monitor/thresholds into the composed write path (the
//! `hybrid-async` arm merely logs the resolved thresholds), so TD-004's hard-debt admission/high-water/
//! retention gate is NOT yet enforced end-to-end on the server (tracked follow-up). Both caveats are recorded
//! as explicit `GAP` assertions in the evidence rows.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::fault::{
    AcEvidence, AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, durable_command_count, write_evidence,
};
use pqueue_core::RequestId;
use pqueue_engine::{
    ClaimCommand, CommandPosition, ComposedBackend, ControlPlaneStore, EngineError,
    InProcessControlPlane, LogStore, ProjectionSnapshot, ProjectionStore, PushCommand, PushPort,
    QueueCommand,
};
use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
use pqueue_sqlite::{
    HybridAsyncDebt, HybridAsyncMonitor, HybridAsyncThresholds, HybridFaultCutPoint,
    HybridFaultHook, HybridProjectionStore,
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

/// AC-TXN-4: strike 5 object-log-internal cut points and assert TP-003 §3.10's outcomes hold at each:
/// 0 lost accepted items, 0 duplicate active leases, committed commands replay exactly once, orphan
/// segments are ignored by replay, and stale-epoch commits are rejected.
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

    // Honest coverage note: TP-003 §3.10 row 209 also names "during projection apply", "after projection
    // apply before response", and "during manifest CAS/fallback commit". "During manifest CAS" collapses to
    // the ATOMIC create-only PUT already bracketed by AfterSegmentWriteBeforeManifest (lost -> orphan) and
    // AfterManifestBeforeAck (won -> committed), so it needs no separate cut point. The two projection-apply
    // instants are NOT internal to the segmented object-log substrate this row drives directly: they live in
    // the `pqueue-engine` ComposedBackend projection-apply step (in-memory projection for this profile) and
    // are exercised as the success barrier / poison instants by AC-TXN-5/5A (hybrid `DuringMemoryApply`) and
    // as restart-replay by AC-TXN-3 (`AfterApplyBeforeResponse`). This row therefore honestly covers the 5
    // substrate-internal cut points only, not the composed-layer projection-apply instants.
    asserts.push(
        "GAP (row 209 scope): covers the 5 object-log-substrate-internal cut points; the composed-layer 'during projection apply' + 'after projection apply before response' instants are covered by AC-TXN-5/5A (DuringMemoryApply) and AC-TXN-3 (AfterApplyBeforeResponse), and 'during manifest CAS' collapses into the two bracketing manifest cut points".into(),
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
    let sq = pqueue_conformance::fault::ac_txn_1_kill_after_create_queue(sqlite_log_factory()).await;
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
    for (name, outcome) in [
        (
            "sqlite_relational",
            pqueue_conformance::fault::ac_txn_1_kill_after_batch_update(sqlite_relational_factory())
                .await,
        ),
        (
            "sqlite_log",
            pqueue_conformance::fault::ac_txn_1_kill_after_batch_update(sqlite_log_factory()).await,
        ),
    ] {
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
    let sr = pqueue_conformance::fault::ac_txn_1_kill_after_set_gates(sqlite_relational_factory())
        .await;
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

    // Honest real-server-path caveat: the AfterSqliteCommitBeforeMemoryApply poison instant above is struck
    // on `HybridProjectionStore::apply` (`apply_durable_then_memory`, SQLite-first). NO real server pipeline
    // runs that ordering: both the `hybrid` and `hybrid-async` runtime profiles compose the backend with
    // `with_group_commit(true)` and apply MEMORY-FIRST via `apply_live_owned` (deferring SQLite to
    // `flush_deferred`), and pqueue-server wires no `hybrid-strict` profile (env_config accepts only
    // inmemory|sqlite|hybrid|hybrid-async|postgres). So this SQLite-first poison/fail-closed instant is
    // verified at the ProjectionStore layer, not on a real server write pipeline.
    asserts.push(
        "GAP (real-server-path caveat): AfterSqliteCommitBeforeMemoryApply is the SQLite-first `apply` ordering, verified at the ProjectionStore layer only; the real `hybrid`/`hybrid-async` server profiles apply memory-first via apply_live_owned (with_group_commit) and no `hybrid-strict` profile is wired in pqueue-server, so this exact cut is not on a real server pipeline".into(),
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

    // --- (d) backpressure fail-closed: once async apply debt trips Hard backpressure, new mutations are
    // rejected retryably and the lagging high-water is withheld until the backlog drains below budget. ---
    {
        let thresholds =
            HybridAsyncThresholds::new(100, 1_000_000, 100, 60_000, 3).expect("valid thresholds");
        let mut monitor = HybridAsyncMonitor::new(thresholds);
        let hw = CommandPosition::new(shard.clone(), 0, 41);
        monitor.observe(
            HybridAsyncDebt {
                apply_lag_commands: 10,
                ..Default::default()
            },
            0,
        );
        ensure!(
            monitor.admit_mutation().is_ok(),
            "clear debt must admit mutations"
        );
        monitor.observe(
            HybridAsyncDebt {
                apply_lag_commands: 100,
                ..Default::default()
            },
            1,
        );
        ensure!(
            matches!(monitor.admit_mutation(), Err(EngineError::Unavailable)),
            "debt over the hard budget must reject new admission with a retryable error"
        );
        ensure!(
            monitor.recovery_high_water_safe(Some(hw)).is_none(),
            "the lagging high-water must not be advertised while debt is over budget"
        );
    }
    asserts.push(
        "backpressure fail-closed (component-level): async apply debt over the hard budget rejects new mutation admission (Unavailable) and withholds the lagging high-water".into(),
    );
    // Honest server-wiring caveat: the assertion above proves the TD-004 admission/high-water/retention gate
    // ONLY against the standalone `HybridAsyncMonitor` component. pqueue-server opens `HybridProjectionStore`
    // WITHOUT threading a monitor/thresholds into the composed write path — `open_objectlog_hybrid_backend`
    // constructs no monitor, and the `hybrid-async` arm merely LOGS the resolved thresholds (lib.rs:1455).
    // No `admit_mutation` call gates a real push/claim, and no observed debt withholds `recovery_high_water`
    // on the server pipeline. So TD-004:361's "hard debt fails mutating admission / high-water / retention"
    // is NOT proven end-to-end on the server; it is a tracked follow-up (wire monitor+thresholds into the
    // backend write path).
    asserts.push(
        "GAP (server not wired): the TD-004 hard-debt admission/high-water/retention gate is proven only against the standalone HybridAsyncMonitor; pqueue-server opens HybridProjectionStore without wiring the monitor/thresholds into the write path (lib.rs merely logs them), so debt does not yet gate real admission — tracked follow-up".into(),
    );

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

    // --- AC-TXN-5 hybrid-strict poison + restart hydration + request-id semantics (see module doc). ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-5",
        "object_log_sqlite(hybrid, ProjectionStore-layer)",
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

    // --- AC-TXN-7 latency-bound is not a correctness knob (objectlog force-seal vs group-commit) ---
    // Repeat AC-TXN-3 under both commit-latency-bound settings; the invariants must be identical.
    let force_seal = ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await;
    let group_commit =
        ac_txn_3_unknown_outcome_replay(objectlog_group_commit_factory(), DURABLE).await;
    match (force_seal, group_commit) {
        (Ok(a), Ok(b)) => {
            let same = a == b;
            let mut assertions = vec![format!(
                "force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: {same}"
            )];
            // Honest scope note: TP-003 §3.10 row 213 requires repeating AC-TXN-1..6 across the full TP-002 E3
            // commit-latency-bound sweep. This row repeats only AC-TXN-3 across only two objectlog latency
            // settings (force-seal vs group-commit) and asserts the invariants are identical.
            assertions.push(
                "GAP (row 213 sweep coverage): repeats AC-TXN-3 only (not the full AC-TXN-1..6) across two commit-latency-bound settings (objectlog force-seal vs group-commit), not the full TP-002 E3 sweep".into(),
            );
            assertions.extend(a);
            if !same {
                failures.push(
                    "AC-TXN-7 [objectlog]: latency-bound setting changed AC-TXN-3 invariants"
                        .into(),
                );
            }
            records.push(AcEvidence {
                ac: "AC-TXN-7",
                backend: "objectlog(force-seal|group-commit)".into(),
                result: if same { "partial" } else { "fail" },
                detail: "AC-TXN-3 invariance across commit-latency-bound settings".into(),
                assertions,
            });
        }
        (fs, gc) => {
            if let Err(e) = fs {
                record(
                    &mut records,
                    &mut failures,
                    "AC-TXN-7",
                    "objectlog(force-seal)",
                    Err(e),
                );
            }
            if let Err(e) = gc {
                record(
                    &mut records,
                    &mut failures,
                    "AC-TXN-7",
                    "objectlog(group-commit)",
                    Err(e),
                );
            }
        }
    }

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
