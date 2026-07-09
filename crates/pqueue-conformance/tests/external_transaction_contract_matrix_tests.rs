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
//! | AC | memory | sqlite-log | objectlog | objectlog+sqlite | postgres (env) |
//! |----|--------|-----------|-----------|------------------|----------------|
//! | AC-TXN-1 durable+visible (per-op reopen) | n/a (non-durable) | ✓ | ✓ | ✓ | ✓ |
//! | AC-TXN-2 rejection no-effect | partial (in-proc) | ✓ | ✓ | ✓ | ✓ |
//! | AC-TXN-3 unknown-outcome replay | partial (in-proc) | ✓ full | ✓ full | ✓ full | ✓ full |
//! | AC-TXN-4 objectlog crash-point matrix | — | — | ✓ (5 internal cut points)* | — | — |
//! | AC-TXN-5 hybrid-strict poison + replay | — | — | | ✓ (projection cut points)† | — |
//! | AC-TXN-5A hybrid-async success barrier | — | — | | ✓ (projection cut points)† | — |
//! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | — |
//! | AC-TXN-7 latency-bound invariance | — | — | partial (force-seal vs group-commit) | | — |
//!
//! AC-TXN-3's request_id-replay-across-restart is a REAL assertion on EVERY durable profile (atomic AND
//! eventual-apply): `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log for
//! both durability classes (this suite's B3.1 run closed the earlier atomic-composed-log gap in
//! `crates/pqueue-engine/src/compose.rs`).
//!
//! `*` AC-TXN-4 drives [`pqueue_objectlog::ObjectLog`]'s `LogStore` impl DIRECTLY (bypassing
//! `ComposedBackend`) with the [`pqueue_objectlog::FaultHook`] seam added for this row, striking 5 instants
//! strictly INSIDE the segmented substrate's own commit pipeline that the public `Backend::write` seam
//! cannot reach: `BeforeSegmentWrite`, `AfterSegmentWriteBeforeManifest`, `AfterManifestBeforeAck`,
//! `DuringOwnerReassignment`, `DuringSnapshotWrite` (see `ac_txn_4_crash_point_matrix` below). One instant
//! named in TP-003 §3.10 row 209 — a crash strictly DURING the composed backend's projection-apply step —
//! lives in a distinct architectural layer (`pqueue-engine`'s `ComposedBackend`, which applies a batch only
//! after `LogStore::append` already returned `Ok`) and is not internal to this crate; it stays a documented
//! follow-up rather than a fake pass here.
//!
//! `†` AC-TXN-5/5A (see `ac_txn_5_hybrid_strict_poison_replay_scenario` /
//! `ac_txn_5a_hybrid_async_success_barrier_scenario` below) add the analogous seam on the PROJECTION side —
//! [`pqueue_sqlite::HybridFaultHook`] on `HybridProjectionStore` — for the instants the public seam cannot
//! isolate (a fault strictly between the durable SQLite commit and the in-memory apply, and one strictly
//! inside the deferred async SQLite checkpoint), driving `HybridProjectionStore` DIRECTLY via
//! `ProjectionStore` for those clauses. Where a cut point genuinely IS reachable through the public seam (a
//! memory-apply failure struck from inside `apply_live`, or a crash in the commit→apply window covered by
//! AC-TXN-3), these scenarios drive it through the real `ComposedBackend<ObjectLog, HybridProjectionStore,
//! InProcessControlPlane>` instead. Backpressure fail-closed (AC-TXN-5A) is proven directly against
//! [`pqueue_sqlite::HybridAsyncMonitor`], the component that implements TD-004's admission-gating contract.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::fault::{
    AcEvidence, AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, durable_command_count, write_evidence,
};
use pqueue_core::RequestId;
use pqueue_engine::{
    ClaimCommand, CommandPosition, ComposedBackend, ControlPlaneStore, EngineError,
    InProcessControlPlane, LogStore, ProjectionSnapshot, ProjectionStore, PushPort, QueueCommand,
    PushCommand,
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
    std::env::temp_dir().join(format!("pqueue-ac-txn-{profile}-{}-{n}", std::process::id()))
}

fn sqlite_log_factory() -> impl Fn(&str) -> pqueue_sqlite::ComposedSqliteBackend {
    let base = base_dir("sqlite");
    move |tag: &str| {
        let path = base.join(format!("{tag}.db"));
        std::fs::create_dir_all(&base).ok();
        pqueue_sqlite::composed_sqlite_backend(path.to_str().unwrap()).expect("open composed sqlite-log")
    }
}

fn objectlog_factory() -> impl Fn(&str) -> pqueue_objectlog::ComposedObjectLogBackend {
    let base = base_dir("objectlog");
    move |tag: &str| {
        pqueue_objectlog::composed_objectlog_backend(base.join(tag)).expect("open composed objectlog")
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

const DURABLE: TxnCaps = TxnCaps { durable_reopen: true };
const NON_DURABLE: TxnCaps = TxnCaps { durable_reopen: false };

/// Record one AC-TXN outcome into the evidence buffer, tracking failures for the final assertion. A
/// passing row whose assertions include an explicit "N/A" (a clause inapplicable to the profile) or a
/// "GAP" note is recorded as `partial`, never `pass`, so status never overclaims coverage.
fn record(
    records: &mut Vec<AcEvidence>,
    failures: &mut Vec<String>,
    ac: &'static str,
    backend: &str,
    outcome: Result<Vec<String>, String>,
) {
    match outcome {
        Ok(assertions) => {
            let partial = assertions
                .iter()
                .any(|a| a.contains("N/A") || a.contains("GAP"));
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
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
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
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
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
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
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
        let claim_entries = log3
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries
            .into_iter()
            .filter(|(_, env)| matches!(env.command, pqueue_engine::QueueCommand::Claim(_)))
            .count();
        ensure!(
            claim_entries == 1,
            "0 duplicate active leases: expected exactly 1 committed claim command, got {}",
            claim_entries
        );
    }
    asserts.push(
        "AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery (0 duplicate active leases)"
            .into(),
    );

    // --- DuringOwnerReassignment: the epoch-fence commit survives a lost ack; stale-epoch commits reject.
    {
        let (root, mut log) = objectlog_direct(&base, "owner-reassignment");
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::DuringOwnerReassignment,
        ))));
        let err = log.acquire_epoch(&shard);
        ensure!(err.is_err(), "DuringOwnerReassignment must abort acquire_epoch");
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
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        let positions = log
            .append(&shard, &[ac_txn_4_push_env("1", "before-snapshot")], 0)
            .map_err(|e| format!("append: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
        let err = log.write_snapshot(
            &shard,
            positions[0].clone(),
            ProjectionSnapshot { payload: vec![9] },
        );
        ensure!(err.is_err(), "DuringSnapshotWrite must abort the snapshot write");
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
fn objectlog_hybrid_with_fault_hook(root: &std::path::Path, hook: Arc<dyn HybridFaultHook>) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite_path = root.join("projection.sqlite");
    let log =
        ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("open object log");
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
            .push_with_request_id(&shard, rid.clone(), body.clone(), pqueue_conformance::ts(1), None)
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
        ProjectionStore::flush_deferred(&mut hybrid).map_err(|e| format!("flush_deferred: {e:?}"))?;
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
        ProjectionStore::flush_deferred(&mut hybrid).map_err(|e| format!("second flush_deferred: {e:?}"))?;
        let hw2 = ProjectionStore::recovery_high_water(&hybrid, &shard)
            .map_err(|e| format!("recovery_high_water after no-op flush: {e:?}"))?;
        ensure!(hw2 == hw, "a no-op flush must not move the high-water");
    }
    asserts.push(
        "ordered batching: 3 live-applied commands drain in one flush, the SQLite high-water advances through the whole batch exactly once, and a no-op flush does not re-advance it".into(),
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
        ensure!(monitor.admit_mutation().is_ok(), "clear debt must admit mutations");
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
        "backpressure fail-closed: async apply debt over the hard budget rejects new mutation admission and withholds the lagging high-water".into(),
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
        ac_txn_2_rejection_no_effect(|_: &str| pqueue_memory::composed_memory_backend(), NON_DURABLE).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "memory",
        ac_txn_3_unknown_outcome_replay(|_: &str| pqueue_memory::composed_memory_backend(), NON_DURABLE).await,
    );

    // --- sqlite-log (composed SqliteLog + in-memory projection, atomic, durable) ---
    record(&mut records, &mut failures, "AC-TXN-1", "sqlite_log",
        ac_txn_1_success_durable_visible(sqlite_log_factory()).await);
    record(&mut records, &mut failures, "AC-TXN-2", "sqlite_log",
        ac_txn_2_rejection_no_effect(sqlite_log_factory(), DURABLE).await);
    record(&mut records, &mut failures, "AC-TXN-3", "sqlite_log",
        ac_txn_3_unknown_outcome_replay(sqlite_log_factory(), DURABLE).await);

    // --- objectlog (composed ObjectLog + in-memory projection, eventual-apply, durable) ---
    record(&mut records, &mut failures, "AC-TXN-1", "objectlog",
        ac_txn_1_success_durable_visible(objectlog_factory()).await);
    record(&mut records, &mut failures, "AC-TXN-2", "objectlog",
        ac_txn_2_rejection_no_effect(objectlog_factory(), DURABLE).await);
    record(&mut records, &mut failures, "AC-TXN-3", "objectlog",
        ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await);

    // --- object_log_sqlite (hybrid, eventual-apply, durable) — a COMMITTED profile ---
    record(&mut records, &mut failures, "AC-TXN-1", "object_log_sqlite",
        ac_txn_1_success_durable_visible(objectlog_sqlite_factory()).await);
    record(&mut records, &mut failures, "AC-TXN-2", "object_log_sqlite",
        ac_txn_2_rejection_no_effect(objectlog_sqlite_factory(), DURABLE).await);
    record(&mut records, &mut failures, "AC-TXN-3", "object_log_sqlite",
        ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await);

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
        "object_log_sqlite(hybrid-strict)",
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
    record(&mut records, &mut failures, "AC-TXN-6", "sqlite_log|object_log_sqlite",
        ac_txn_6_parity(sqlite_log_factory(), objectlog_sqlite_factory()).await);

    // --- AC-TXN-7 latency-bound is not a correctness knob (objectlog force-seal vs group-commit) ---
    // Repeat AC-TXN-3 under both commit-latency-bound settings; the invariants must be identical.
    let force_seal = ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await;
    let group_commit = ac_txn_3_unknown_outcome_replay(objectlog_group_commit_factory(), DURABLE).await;
    match (force_seal, group_commit) {
        (Ok(a), Ok(b)) => {
            let same = a == b;
            let mut assertions = vec![format!("force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: {same}")];
            assertions.extend(a);
            if !same {
                failures.push("AC-TXN-7 [objectlog]: latency-bound setting changed AC-TXN-3 invariants".into());
            }
            records.push(AcEvidence { ac: "AC-TXN-7", backend: "objectlog(force-seal|group-commit)".into(), result: if same {"partial"} else {"fail"}, detail: "AC-TXN-3 invariance across commit-latency-bound settings".into(), assertions });
        }
        (fs, gc) => {
            if let Err(e) = fs { record(&mut records, &mut failures, "AC-TXN-7", "objectlog(force-seal)", Err(e)); }
            if let Err(e) = gc { record(&mut records, &mut failures, "AC-TXN-7", "objectlog(group-commit)", Err(e)); }
        }
    }

    let path = write_evidence("tp003-ac-txn-matrix.jsonl", &records).expect("write evidence jsonl");
    eprintln!("AC-TXN evidence written to {}", path.display());
    for r in &records {
        eprintln!("  [{}] {} => {} ({} assertions)", r.result, r.ac, r.backend, r.assertions.len());
    }
    assert!(failures.is_empty(), "AC-TXN matrix failures:\n{}", failures.join("\n"));
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
            pqueue_postgres::composed_postgres_backend_in_schema(&url, &sch).expect("connect postgres")
        }
    };

    let mut records: Vec<AcEvidence> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    record(&mut records, &mut failures, "AC-TXN-1", "postgres",
        futures::executor::block_on(ac_txn_1_success_durable_visible(pg_factory("txn1".into(), url.clone()))));
    record(&mut records, &mut failures, "AC-TXN-2", "postgres",
        futures::executor::block_on(ac_txn_2_rejection_no_effect(pg_factory("txn2".into(), url.clone()), DURABLE)));
    record(&mut records, &mut failures, "AC-TXN-3", "postgres",
        futures::executor::block_on(ac_txn_3_unknown_outcome_replay(pg_factory("txn3".into(), url.clone()), DURABLE)));

    let path = write_evidence("tp003-ac-txn-matrix-postgres.jsonl", &records).expect("write pg evidence");
    eprintln!("AC-TXN postgres evidence written to {}", path.display());
    for r in &records {
        eprintln!("  [{}] {} => {} ({} assertions)", r.result, r.ac, r.backend, r.assertions.len());
    }
    assert!(failures.is_empty(), "AC-TXN postgres failures:\n{}", failures.join("\n"));
}
