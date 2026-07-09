//! `fault_injection_harness_tests` (TP-001 line 148) — the reusable fault-injection CAPABILITY and its
//! own tests. This suite proves the harness itself: that [`pqueue_conformance::fault::inject_commit`]
//! genuinely drives a backend's commit pipeline through the [`Backend::write`] unit-of-work seam and
//! injects a crash at the AC-TXN-3 cut points, producing the durable footprint each cut point claims.
//!
//! Because the only commit seam the engine exposes is the `write` closure, the harness expresses each cut
//! point as a decision inside that closure and simulates a process kill by dropping the handle and
//! reopening the SAME durable state. These tests exercise the capability against a non-durable profile
//! (memory — in-process invariants only) and two durable profiles (composed sqlite-log, composed
//! object-log) so the cut-point mechanics are validated on the recovery path they exist to model.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::fault::{CutPoint, durable_command_count, inject_commit, spec};
use pqueue_conformance::{envelope, item, qdef, qkey, shard, ts};
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, EngineError, EngineResult, InProcessControlPlane, LogStore,
    ProjectionRead, ProjectionSnapshot, PushCommand, QueueCommand,
};
use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
use pqueue_sqlite::HybridProjectionStore;
use std::sync::Arc;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "pqueue-fault-harness-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

// --- durable factories (stable location => drop+reopen recovers the same state) ---

fn sqlite_log_factory() -> impl Fn() -> pqueue_sqlite::ComposedSqliteBackend {
    let path = unique_dir("sqlite").with_extension("db");
    let path = path.to_str().unwrap().to_string();
    move || pqueue_sqlite::composed_sqlite_backend(&path).expect("open composed sqlite-log")
}

fn objectlog_factory() -> impl Fn() -> pqueue_objectlog::ComposedObjectLogBackend {
    let root = unique_dir("objectlog");
    move || pqueue_objectlog::composed_objectlog_backend(root.clone()).expect("open composed objectlog")
}

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

fn objectlog_sqlite_factory() -> impl Fn() -> HybridBackend {
    let root = unique_dir("objectlog-sqlite");
    let sqlite = root.join("projection.sqlite");
    move || {
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

// ---------------------------------------------------------------------------
// Capability tests: BeforeAppend produces zero durable effect on every backend.
// ---------------------------------------------------------------------------

async fn assert_before_append_is_inert<B>(make: &impl Fn() -> B)
where
    B: pqueue_conformance::ConformanceCore + pqueue_engine::LogRead,
{
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "kx", 5)],
        }),
        vec![],
    );
    let killed = inject_commit(&a, env, CutPoint::BeforeAppend).await;
    assert!(killed.is_err(), "BeforeAppend must not commit the write");
    assert_eq!(
        durable_command_count(&a).await.unwrap(),
        0,
        "BeforeAppend must leave zero durable commands"
    );
    assert_eq!(
        a.metrics(&qkey()).await.unwrap().pending,
        0,
        "BeforeAppend must leave zero visible state"
    );
}

#[tokio::test]
async fn before_append_is_inert_memory() {
    assert_before_append_is_inert(&pqueue_memory::composed_memory_backend).await;
}

#[tokio::test]
async fn before_append_is_inert_sqlite_log() {
    assert_before_append_is_inert(&sqlite_log_factory()).await;
}

#[tokio::test]
async fn before_append_is_inert_objectlog() {
    assert_before_append_is_inert(&objectlog_factory()).await;
}

// ---------------------------------------------------------------------------
// Capability tests: AfterAppendBeforeApply durably appends, leaves the in-process
// projection unapplied, and REPLAYS EXACTLY ONCE when the durable backend reopens.
// ---------------------------------------------------------------------------

async fn assert_after_append_replays_once<B>(make: &impl Fn() -> B)
where
    B: pqueue_conformance::ConformanceCore + pqueue_engine::LogRead,
{
    // Session 1: append durably but skip apply (kill in the commit->apply window).
    {
        let a = make();
        a.create_queue(qdef()).await.unwrap();
        let env = envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ky", 8)],
            }),
            vec![],
        );
        let pos = inject_commit(&a, env, CutPoint::AfterAppendBeforeApply)
            .await
            .expect("append half of the commit succeeds");
        assert!(!pos.is_empty(), "append returned a durable position");
        assert_eq!(
            durable_command_count(&a).await.unwrap(),
            1,
            "the command is durable on the log after the commit->apply kill"
        );
        // The in-process projection was deliberately NOT applied (models the lost apply/response).
        assert_eq!(
            a.metrics(&qkey()).await.unwrap().pending,
            0,
            "apply was skipped, so the in-process projection has not yet applied the command"
        );
    }
    // Session 2: reopen the same durable state; recovery replays the tail exactly once.
    let b = make();
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "recovery replayed the durable-but-unapplied command exactly once"
    );
    assert_eq!(
        b.select_eligible(&shard(), ts(100), 10).await.unwrap().len(),
        1,
        "no duplicate state transition after recovery replay"
    );
}

#[tokio::test]
async fn after_append_before_apply_replays_once_sqlite_log() {
    assert_after_append_replays_once(&sqlite_log_factory()).await;
}

#[tokio::test]
async fn after_append_before_apply_replays_once_objectlog() {
    assert_after_append_replays_once(&objectlog_factory()).await;
}

#[tokio::test]
async fn after_append_before_apply_replays_once_objectlog_sqlite() {
    assert_after_append_replays_once(&objectlog_sqlite_factory()).await;
}

// ---------------------------------------------------------------------------
// Capability test: the full commit (append+apply+Ok) is visible in-process.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_commit_is_visible_in_process_memory() {
    let a = pqueue_memory::composed_memory_backend();
    a.create_queue(qdef()).await.unwrap();
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "kz", 3)],
        }),
        vec![],
    );
    let pos = inject_commit(&a, env, CutPoint::AfterResponse)
        .await
        .expect("full commit succeeds");
    assert!(!pos.is_empty());
    assert_eq!(
        a.metrics(&qkey()).await.unwrap().pending,
        1,
        "a full commit is visible in-process before the response returns"
    );
    assert_eq!(durable_command_count(&a).await.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// The harness drives the real request_id idempotency path too (AfterApplyBeforeResponse
// / AfterResponse): a committed-then-lost response replays exactly once on reopen.
// ---------------------------------------------------------------------------

/// The lost-response `request_id` replay: a committed-then-lost response must replay the ONE committed
/// result on reopen. `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log
/// for BOTH durability classes — the ATOMIC composed-log profile (`sqlite_log`, exercised here) as well
/// as the EVENTUAL-APPLY profile (`objectlog_sqlite`). Before B3.1, atomic composed-log recovery dropped
/// the idempotency map (the rebuild was gated on `DurabilityClass::EventualApply` in
/// `crates/pqueue-engine/src/compose.rs`), so the `sqlite_log` arm below re-executed and returned a fresh
/// id — an INV-14 violation this bead's engine fix closed.
async fn assert_lost_response_replays_once<B>(make: &impl Fn() -> B)
where
    B: pqueue_conformance::ConformanceCore,
{
    let rid = pqueue_core::RequestId::new("harness-lost").unwrap();
    let body = vec![spec("harness-lost", 4)];
    let committed = {
        let a = make();
        a.create_queue(qdef()).await.unwrap();
        a.push_with_request_id(&shard(), rid.clone(), body.clone(), ts(1), None)
            .await
            .unwrap()
    };
    let b = make();
    let replay = b
        .push_with_request_id(&shard(), rid, body, ts(2), None)
        .await
        .unwrap();
    assert_eq!(replay, committed, "lost-response retry replays the committed ids");
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "no duplicate committed result after a lost-response retry"
    );
}

#[tokio::test]
async fn lost_response_replays_once_objectlog_sqlite() {
    assert_lost_response_replays_once(&objectlog_sqlite_factory()).await;
}

/// Regression guard for the B3.1 engine fix: atomic composed-log recovery must rebuild push-idempotency.
#[tokio::test]
async fn lost_response_replays_once_sqlite_log() {
    assert_lost_response_replays_once(&sqlite_log_factory()).await;
}

// ---------------------------------------------------------------------------
// Object-log INTERNAL cut points (TP-003 §3.10 AC-TXN-4): segment write, manifest CAS/ack, owner
// reassignment (epoch-fence CAS), and snapshot write. These are instants strictly INSIDE
// `SegmentedObjectLog`'s own commit pipeline that the public `Backend::write` seam above cannot reach —
// that seam can only crash before or after a whole `append`/`acquire_epoch`/`write_snapshot` call, not at
// the durable sub-steps inside one. Driven directly against `ObjectLog`'s `LogStore` impl (bypassing
// `ComposedBackend`) so the fault strikes the pipeline itself.
// ---------------------------------------------------------------------------

/// Crashes (`Err`) every time the pipeline reaches `target`; a no-op at every other cut point.
struct CrashAt(FaultCutPoint);

impl FaultHook for CrashAt {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!(
                "fault-injection: crash at {cut:?}"
            )))
        } else {
            Ok(())
        }
    }
}

fn objectlog_direct(tag: &str) -> (std::path::PathBuf, ObjectLog) {
    let root = unique_dir(tag);
    let log = ObjectLog::open(root.clone()).expect("open object log");
    (root, log)
}

fn push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
    envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item(id, key, 5)],
        }),
        vec![],
    )
}

#[test]
fn objectlog_internal_before_segment_write_is_inert() {
    let (_root, mut log) = objectlog_direct("cut-before-seg");
    log.ensure_shard(&shard()).unwrap();
    log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::BeforeSegmentWrite))));
    let err = log.append(&shard(), &[push_env("1", "kx")], 0);
    assert!(err.is_err(), "BeforeSegmentWrite must abort the append");
    assert_eq!(
        log.read_from(&shard(), None, 100).unwrap().entries.len(),
        0,
        "BeforeSegmentWrite must leave 0 durable commands (0 lost accepted items)"
    );
}

#[test]
fn objectlog_internal_after_segment_write_before_manifest_orphans_the_segment() {
    let (_root, mut log) = objectlog_direct("cut-after-seg-before-manifest");
    log.ensure_shard(&shard()).unwrap();
    let before = log.counters().objects_put;
    log.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::AfterSegmentWriteBeforeManifest,
    ))));
    let err = log.append(&shard(), &[push_env("1", "orphan")], 0);
    assert!(
        err.is_err(),
        "AfterSegmentWriteBeforeManifest must abort the append"
    );
    assert!(
        log.counters().objects_put > before,
        "the segment object was genuinely durably written before the fault struck"
    );
    assert_eq!(
        log.read_from(&shard(), None, 100).unwrap().entries.len(),
        0,
        "the orphan segment (durable but unnamed by any manifest entry) must never surface on replay"
    );
    // Retry cleanly: recovery/replay must not be confused by the orphan segment left behind.
    log.set_fault_hook(None);
    let positions = log
        .append(&shard(), &[push_env("2", "real")], 0)
        .expect("retry after the orphaned segment succeeds");
    assert_eq!(positions.len(), 1);
    assert_eq!(
        log.read_from(&shard(), None, 100).unwrap().entries.len(),
        1,
        "exactly the retried command is visible; the orphan never resurfaces"
    );
}

#[test]
fn objectlog_internal_after_manifest_before_ack_replays_exactly_once() {
    let (root, mut log) = objectlog_direct("cut-after-manifest-before-ack");
    log.ensure_shard(&shard()).unwrap();
    log.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::AfterManifestBeforeAck,
    ))));
    let err = log.append(&shard(), &[push_env("1", "committed-unacked")], 0);
    assert!(
        err.is_err(),
        "the caller must observe the fault (the ack — and any downstream projection apply — was lost)"
    );
    drop(log);

    // Reopen: recovery re-derives all state from the durable manifest tail, not from stale in-memory
    // bookkeeping (the crash struck strictly after the manifest CAS had already committed).
    let mut log2 = ObjectLog::open(root.clone()).expect("reopen object log");
    log2.ensure_shard(&shard()).unwrap();
    let entries = log2.read_from(&shard(), None, 100).unwrap().entries;
    assert_eq!(
        entries.len(),
        1,
        "committed commands replay exactly once on recovery, even though the ack was lost"
    );

    // A fresh append after recovery must not collide with or duplicate the recovered command.
    let positions = log2
        .append(&shard(), &[push_env("2", "after-recovery")], 0)
        .expect("append after recovery");
    assert_eq!(positions.len(), 1);
    let entries2 = log2.read_from(&shard(), None, 100).unwrap().entries;
    assert_eq!(
        entries2.len(),
        2,
        "exactly-once: the recovered command plus the new one, 0 duplicate state transitions"
    );
}

#[test]
fn objectlog_internal_owner_reassignment_fence_survives_lost_ack_and_fences_stale_epoch() {
    let (root, mut log) = objectlog_direct("cut-owner-reassignment");
    log.ensure_shard(&shard()).unwrap();
    assert_eq!(log.current_epoch(&shard()).unwrap(), 0);

    log.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::DuringOwnerReassignment,
    ))));
    let err = log.acquire_epoch(&shard());
    assert!(
        err.is_err(),
        "the acquirer must observe the fault (the epoch-fence ack was lost)"
    );
    drop(log);

    let mut log2 = ObjectLog::open(root.clone()).expect("reopen object log");
    log2.ensure_shard(&shard()).unwrap();
    assert_eq!(
        log2.current_epoch(&shard()).unwrap(),
        1,
        "the fence entry durably committed even though the acquirer's own ack was lost"
    );

    // Stale-epoch commits are rejected from the durable manifest tail, not from in-memory state.
    let stale = log2.append(&shard(), &[push_env("1", "stale-epoch")], 0);
    assert!(
        matches!(stale, Err(EngineError::EpochFenced)),
        "a write at the superseded epoch must be fenced; got {stale:?}"
    );
    assert_eq!(
        log2.read_from(&shard(), None, 100).unwrap().entries.len(),
        0,
        "a fenced write must append nothing"
    );

    // The new epoch is fully usable.
    let ok = log2.append(&shard(), &[push_env("2", "current-epoch")], 1);
    assert!(
        ok.is_ok(),
        "a write at the new current epoch must succeed: {ok:?}"
    );
}

#[test]
fn objectlog_internal_snapshot_write_failure_leaves_the_log_intact() {
    let (_root, mut log) = objectlog_direct("cut-snapshot-write");
    log.ensure_shard(&shard()).unwrap();
    let positions = log
        .append(&shard(), &[push_env("1", "before-snapshot")], 0)
        .expect("append before snapshot");

    log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
    let err = log.write_snapshot(
        &shard(),
        positions[0].clone(),
        ProjectionSnapshot {
            payload: vec![1, 2, 3],
        },
    );
    assert!(
        err.is_err(),
        "DuringSnapshotWrite must abort the snapshot write"
    );
    assert!(
        log.latest_snapshot(&shard()).unwrap().is_none(),
        "a failed snapshot write must leave no committed snapshot ref"
    );
    assert_eq!(
        log.read_from(&shard(), None, 100).unwrap().entries.len(),
        1,
        "the command log itself is untouched by a lost snapshot write (0 lost items)"
    );

    // Retry cleanly.
    log.set_fault_hook(None);
    log.write_snapshot(
        &shard(),
        positions[0].clone(),
        ProjectionSnapshot {
            payload: vec![1, 2, 3],
        },
    )
    .expect("retry succeeds");
    assert!(log.latest_snapshot(&shard()).unwrap().is_some());
}
