//! Engine integration conformance: retention-floor and source-pin invariants across
//! deleted-prefix fail-closed, retained floor/head replay recovery, and source-pin block/release
//! (governing: docs/perf/design/manifest-compaction-hotpath.md:374,
//!  docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224).
//!
//! pqueue-c33c367e evaluation: the deferred server acquire-runtime wiring does not change
//! the rollout safety envelope for retention-floor and source-pin guarantees — the permanent
//! head remains the stale-writer fence and the watermark remains a read-cost helper, not the
//! ownership fence (docs/perf/design/manifest-compaction-hotpath.md:388). This test operates
//! below that wire so every invariant applies to both pre- and post-wiring code.
//!
//! AC-MAP:
//!   TestConformanceRetentionFloorSourcePinEngineInvariant:
//!     - Retention-floor and source-pin guarantees survive engine integration through
//!       the full ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>.
//!     - Deleted-prefix fail-closed: reads below the retention floor return Storage error.
//!     - Retained floor/head replay recovery: reopen preserves floor, fail-closed, and tail.
//!     - Source-pin: branch pins block reclamation; release enables fail-closed.
//!     - Behind-image: deleted SQLite projection causes recovery to fail closed.
//!     - Hybrid-strict exercises deletion; hybrid-async proves it retains until
//!       the complete frontier authority adapter lands.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::{qdef, qkey, ts};
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, EngineError, InProcessControlPlane, LogRead, LogStore,
    MaintenanceStopReason, PushPort, PushSpec, ReclaimDriver,
};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
use pqueue_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

static COUNTER: AtomicU64 = AtomicU64::new(0);

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

#[derive(Clone, Copy, Debug)]
enum ProjectionMode {
    HybridStrict,
    HybridAsync,
}

fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-engine-rfsp-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

fn make_mode(root: &Path, mode: ProjectionMode) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = match mode {
        ProjectionMode::HybridStrict => HybridProjectionStore::open(sqlite.to_str().unwrap())
            .expect("hybrid")
            .with_strict_apply(true),
        ProjectionMode::HybridAsync => HybridProjectionStore::open(sqlite.to_str().unwrap())
            .expect("hybrid")
            .with_deferred_flush_chunk(1)
            .with_async_monitor(clear_thresholds()),
    };
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new()).with_group_commit(true)
}

fn drain(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend.flush_deferred_projection().expect("flush");
    }
}

async fn acquire_maintenance_owner(backend: &HybridBackend) {
    let epoch = backend
        .acquire_epoch(&qkey())
        .await
        .expect("acquire log owner");
    assert_eq!(
        backend.with_log(|log| log.maintenance_owner_epoch(&qkey())),
        Some(epoch),
        "retention maintenance requires a positively acquired local owner"
    );
}

fn floor_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &qkey()))
        .expect("retention_floor")
        .map(|p| p.sequence)
}

fn floor_pos(backend: &HybridBackend) -> Option<pqueue_engine::CommandPosition> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &qkey()))
        .expect("retention_floor")
}

async fn push_commands(backend: &HybridBackend, prefix: &str, n: u64, at_ts: i64) {
    for i in 0..n {
        backend
            .push(
                &qkey(),
                vec![PushSpec {
                    client_item_key: Some(
                        pqueue_core::ClientItemKey::new(format!("{prefix}-{i}")).unwrap(),
                    ),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(at_ts),
                None,
            )
            .await
            .expect("push");
    }
}

async fn create_trimmed_backend(root: &Path, mode: ProjectionMode) -> HybridBackend {
    let backend = make_mode(root, mode).recover().expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    acquire_maintenance_owner(&backend).await;

    // Push 4 old commands (seq 0..3) at ts=10 (past retention at tick time).
    push_commands(&backend, "old", 4, 10).await;
    drain(&backend);

    // Push 4 fresh commands (seq 4..7) at ts=10_000 (within retention at tick time).
    push_commands(&backend, "fresh", 4, 10_000).await;
    drain(&backend);

    // Tick at ts=10_000: cutoff = 10_000_000 - 3_600_000 - 5_000 = 6_395_000ms.
    // Seq 0..3 (committed ~10_000ms) expired; seq 4..7 (committed ~10_000_000ms) retained.
    backend.tick(ts(10_000)).await.unwrap();
    backend
}

fn branch_qkey() -> pqueue_engine::QueueKey {
    pqueue_engine::QueueKey::new(
        qkey().tenant_id.clone(),
        pqueue_core::QueueId::new("branch-rfsp-engine").unwrap(),
    )
}

fn branch_def() -> pqueue_core::QueueDefinition {
    let mut d = qdef();
    d.queue_id = branch_qkey().queue_id.clone();
    d
}

async fn retention_floor_fail_closed_and_recovery_impl(root: &Path, mode: ProjectionMode) {
    let backend = create_trimmed_backend(root, mode).await;

    // Floor advanced through seq 3.
    let floor = floor_seq(&backend).expect("floor advanced after trim");
    assert_eq!(floor, 3, "retention floor advanced through seq 3");

    // Fail-closed below the floor.
    let genesis_err = backend
        .read_from(&qkey(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after trim");
    assert!(
        matches!(&genesis_err, EngineError::Storage(m) if m.contains("read below retention floor")),
        "read_from genesis: expected Storage, got {genesis_err:?}"
    );

    let from_zero_err = backend
        .read_from(
            &qkey(),
            Some(pqueue_engine::CommandPosition::new(qkey(), 0, 0)),
            100,
        )
        .await
        .expect_err("read_from(0) must fail closed");
    assert!(
        matches!(&from_zero_err, EngineError::Storage(m) if m.contains("read below retention floor")),
        "read_from(0): expected Storage, got {from_zero_err:?}"
    );

    // Above-floor reads succeed (seq 4..7).
    let above = backend
        .read_from(&qkey(), floor_pos(&backend), 100)
        .await
        .expect("read_from(floor) above floor must succeed");
    let above_seqs: Vec<u64> = above.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        above_seqs,
        vec![4, 5, 6, 7],
        "above-floor read returns the surviving tail"
    );
    drop(backend);
}

async fn retained_floor_head_replay_impl(root: &Path, mode: ProjectionMode) {
    let reopened = make_mode(root, mode).recover().expect("recover");

    let floor = floor_seq(&reopened).expect("floor survived reopen");
    assert_eq!(floor, 3, "retention floor persisted after reopen");

    let reopen_err = reopened
        .read_from(&qkey(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after reopen");
    assert!(
        matches!(&reopen_err, EngineError::Storage(m) if m.contains("read below retention floor")),
        "after reopen: expected Storage, got {reopen_err:?}"
    );

    let tail = reopened
        .read_from(&qkey(), floor_pos(&reopened), 100)
        .await
        .expect("read_from(floor) after reopen must succeed");
    let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        tail_seqs,
        vec![4, 5, 6, 7],
        "above-floor read succeeds after reopen with complete tail"
    );
}

async fn behind_image_fails_closed_impl(root: &Path, mode: ProjectionMode) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
    }
    let err = match make_mode(root, mode).recover() {
        Ok(_) => panic!("recovery over a deleted projection must fail closed"),
        Err(err) => err,
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("retention floor") && msg.contains("behind"),
        "deleted projection must fail closed with retention-floor behind error, got {msg}"
    );
}

async fn source_pin_blocks_reclamation_impl(root: &Path, mode: ProjectionMode) {
    let backend = make_mode(root, mode).recover().expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    acquire_maintenance_owner(&backend).await;

    // Push 4 commands (seq 0..3) at ts=10.
    push_commands(&backend, "pin", 4, 10).await;
    drain(&backend);

    // Create a branch pin at seq 0.
    let pin_pos = pqueue_engine::CommandPosition::new(qkey(), 0, 0);
    backend
        .with_log(|l| l.branch(&qkey(), &branch_def(), &pin_pos, 1_000_000_000_000, 10_000))
        .expect("create branch");

    // Push 2 fresh commands (seq 4..5) with recent timestamp to form a readable tail.
    push_commands(&backend, "pin-fresh", 2, 1_000_000).await;
    drain(&backend);

    // Trim with the pin active. The pinned segment should survive.
    backend
        .trim_reclaimable_segments(&qkey(), 1_000, ts(1_000_000))
        .expect("trim with live pin");

    // The branch pin must be registered after branch creation.
    let pinned = backend
        .with_log(|l| l.lowest_branch_pinned_below(&qkey(), 3, 10_000))
        .expect("lowest_branch_pinned_below");
    assert!(
        pinned.is_some(),
        "a branch pin should be registered after branch creation"
    );

    // Above-floor reads succeed — the fresh commands survive.
    if let Some(ref fp) = floor_pos(&backend) {
        let tail = backend
            .read_from(&qkey(), Some(fp.clone()), 100)
            .await
            .expect("read_from above floor succeeds with pin");
        assert!(
            !tail.entries.is_empty(),
            "above-floor tail readable with pin"
        );
        assert!(
            tail.entries.iter().all(|(p, _)| p.sequence > fp.sequence),
            "above-floor tail contains only commands above the floor"
        );
    }

    // Release the pin and re-trim. The previously-pinned segment is now reclaimed.
    backend
        .with_log(|l| l.discard_branch(&qkey(), &branch_qkey()))
        .expect("discard branch");
    backend
        .trim_reclaimable_segments(&qkey(), 1_000, ts(1_000_000))
        .expect("trim after pin release");

    // Now reads below the floor fail closed.
    let err = backend
        .read_from(&qkey(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after pin release");
    assert!(
        matches!(&err, EngineError::Storage(m) if m.contains("read below retention floor")),
        "after pin release: expected Storage, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Bead pqueue-d7134740: engine live source pin survives reopen + pinned data
// readable + release fail-closed (governing:
//  docs/perf/design/manifest-compaction-hotpath.md:374,
//  docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224).
// ---------------------------------------------------------------------------

/// Find the on-disk path of the source seq-0 segment file under `root`.
/// The seq-0 segment filename contains `s00000000000000000000-` (the pid/attempt
/// suffix distinguishes it from branch-owned copies). Returns `None` if no such
/// file exists.
fn find_source_seq0_seg_file(root: &Path) -> Option<PathBuf> {
    walk_find_seg(root, "s00000000000000000000-")
}

/// Recursively walk `dir` looking for a `.seg` file whose name contains `needle`.
fn walk_find_seg(dir: &Path, needle: &str) -> Option<PathBuf> {
    let rd = std::fs::read_dir(dir).ok()?;
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = walk_find_seg(&path, needle) {
                return Some(found);
            }
        } else if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.contains(needle)
            && name.ends_with(".seg")
        {
            return Some(path);
        }
    }
    None
}

async fn engine_live_pin_reopen_and_release_impl(root: &Path, mode: ProjectionMode) {
    let tag = match mode {
        ProjectionMode::HybridStrict => "strict",
        ProjectionMode::HybridAsync => "async",
    };

    // --- Phase 1: setup + trim with live pin ---
    let backend = make_mode(root, mode).recover().expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    acquire_maintenance_owner(&backend).await;

    // Push 4 old commands (seq 0..3) at early timestamps so they expire.
    push_commands(&backend, "elpr-old", 4, 10).await;
    drain(&backend);

    // Create a branch pin at seq 0 protecting the first segment.
    let pin_pos = pqueue_engine::CommandPosition::new(qkey(), 0, 0);
    backend
        .with_log(|l| l.branch(&qkey(), &branch_def(), &pin_pos, 1_000_000_000_000, 10_000))
        .expect("create branch");

    // Push 2 fresh commands (seq 4..5) within retention for a readable tail.
    push_commands(&backend, "elpr-fresh", 2, 1_000_000).await;
    drain(&backend);

    // Trim: pin protects seq 0, unpinned seq 1..3 reclaimed, floor advances.
    backend
        .trim_reclaimable_segments(&qkey(), 1_000, ts(1_000_000))
        .expect("trim with live pin");

    // Capture the exact source seq-0 segment path on disk for filesystem-level
    // assertions across reopen and reclamation.
    let seq0_seg = find_source_seq0_seg_file(root)
        .unwrap_or_else(|| panic!("{tag}: source seq-0 segment must exist after trim with pin"));

    // EXACT PINNED BOUNDARY: lowest_branch_pinned_below identifies seq 0.
    let pinned_before = backend
        .with_log(|l| l.lowest_branch_pinned_below(&qkey(), 3, 10_000))
        .expect("lowest_branch_pinned_below before reopen");
    assert_eq!(
        pinned_before,
        Some(0),
        "{tag}: exact pinned seq before reopen is 0"
    );

    // Floor advanced through reclaimed unpinned segments (1..3).
    let floor_before = floor_seq(&backend).expect("floor before reopen");
    assert_eq!(
        floor_before, 3,
        "{tag}: floor at last reclaimed seq 3 before reopen"
    );

    // Verify pinned data is accessible via the branch queue before reopen.
    let branch_before = backend
        .read_from(&branch_qkey(), None, 100)
        .await
        .expect("{tag}: branch queue readable before reopen");
    let branch_seqs_before: Vec<u64> = branch_before
        .entries
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert!(
        branch_seqs_before.contains(&0),
        "{tag}: pinned seq 0 readable via branch before reopen, got seqs {branch_seqs_before:?}"
    );

    // Tail readable after trim.
    let tail_before = backend
        .read_from(&qkey(), floor_pos(&backend), 100)
        .await
        .expect("{tag}: tail readable before reopen");
    let tail_seqs_before: Vec<u64> = tail_before
        .entries
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        tail_seqs_before,
        vec![4, 5],
        "{tag}: tail seq 4..5 before reopen"
    );

    drop(backend);

    // --- Phase 2: REOPEN with live pin ---
    let reopened = make_mode(root, mode).recover().expect("recover");
    acquire_maintenance_owner(&reopened).await;

    // AC1: exact pinned boundary survives reopen.
    let pinned_after = reopened
        .with_log(|l| l.lowest_branch_pinned_below(&qkey(), 3, 10_000))
        .expect("lowest_branch_pinned_below after reopen");
    assert_eq!(
        pinned_after,
        Some(0),
        "{tag}: pinned seq 0 survives reopen (AC1)"
    );

    // AC1: persisted floor survives reopen.
    let floor_after = floor_seq(&reopened).expect("floor after reopen");
    assert_eq!(floor_after, 3, "{tag}: floor persisted after reopen (AC1)");

    // AC1: retained tail is readable after reopen.
    let tail_after = reopened
        .read_from(&qkey(), floor_pos(&reopened), 100)
        .await
        .expect("{tag}: tail readable after reopen (AC1)");
    let tail_seqs_after: Vec<u64> = tail_after.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        tail_seqs_after,
        vec![4, 5],
        "{tag}: retained tail seq 4..5 readable after reopen (AC1)"
    );

    // AC1: exact source segment file persists on disk after reopen.
    assert!(
        seq0_seg.exists(),
        "{tag}: source seq-0 segment must exist on disk after reopen (AC1)"
    );

    // AC2: below-floor pinned source data remains readable via the branch queue.
    // The branch holds a durable copy of the pinned source segment (seq 0).
    let branch_after = reopened
        .read_from(&branch_qkey(), None, 100)
        .await
        .expect("{tag}: branch queue readable after reopen (AC2)");
    let branch_seqs_after: Vec<u64> = branch_after
        .entries
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert!(
        branch_seqs_after.contains(&0),
        "{tag}: pinned seq 0 readable via branch after reopen (AC2), got seqs {branch_seqs_after:?}"
    );

    // --- Phase 3: RELEASE pin + reclaim ---
    reopened
        .with_log(|l| l.discard_branch(&qkey(), &branch_qkey()))
        .expect("discard branch");

    reopened
        .trim_reclaimable_segments(&qkey(), 1_000, ts(1_000_000))
        .expect("trim after pin release");

    // AC3: exact source segment file is reclaimed after pin release + trim.
    assert!(
        !seq0_seg.exists(),
        "{tag}: source seq-0 segment must be absent after discard_branch and re-trim (AC3)"
    );

    // AC3: no branch pin remains after release.
    let pinned_after_release = reopened
        .with_log(|l| l.lowest_branch_pinned_below(&qkey(), 3, 10_000))
        .expect("lowest_branch_pinned_below after release");
    assert_eq!(
        pinned_after_release, None,
        "{tag}: no branch pin after release (AC3)"
    );

    // AC3: floor at seq 3 after pin release.
    let floor_after_release = floor_seq(&reopened).expect("floor after release");
    assert_eq!(
        floor_after_release, 3,
        "{tag}: floor at seq 3 after pin release (AC3)"
    );

    // AC3: retained tail data remains complete after release.
    let tail_after_release = reopened
        .read_from(&qkey(), floor_pos(&reopened), 100)
        .await
        .expect("{tag}: tail readable after pin release (AC3)");
    let tail_seqs: Vec<u64> = tail_after_release
        .entries
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        tail_seqs,
        vec![4, 5],
        "{tag}: retained tail seq 4..5 after release (AC3)"
    );

    // AC3: below-floor reads fail closed after pin release.
    let err = reopened
        .read_from(&qkey(), None, 100)
        .await
        .expect_err("{tag}: genesis read must fail closed after pin release (AC3)");
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "{tag}: expected Storage(read below retention floor) after pin release, got {err:?} (AC3)"
    );

    drop(reopened);
}

/// TestConformanceEngineLiveSourcePinSurvivesReopen (AC1):
/// engine integration reopens the hybrid-strict backend with a live
/// source pin and proves the pin remains active (lowest_branch_pinned_below, floor,
/// and above-floor tail all survive reopen).
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceEngineLiveSourcePinSurvivesReopen() {
    for mode in [ProjectionMode::HybridStrict] {
        let tag = match mode {
            ProjectionMode::HybridStrict => "eng-ac1-strict",
            ProjectionMode::HybridAsync => "eng-ac1-async",
        };
        let root = base_dir(tag);
        engine_live_pin_reopen_and_release_impl(&root, mode).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestConformanceEnginePinnedBelowFloorDataReadable (AC2):
/// after reopen and before release, the exact below-floor pinned source data
/// remains readable via the branch queue — not merely the above-floor tail.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceEnginePinnedBelowFloorDataReadable() {
    for mode in [ProjectionMode::HybridStrict] {
        let tag = match mode {
            ProjectionMode::HybridStrict => "eng-ac2-strict",
            ProjectionMode::HybridAsync => "eng-ac2-async",
        };
        let root = base_dir(tag);
        engine_live_pin_reopen_and_release_impl(&root, mode).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestConformanceEnginePinReleaseFailClosed (AC3):
/// releasing the pin and reclaiming makes below-floor reads fail closed while
/// the retained tail remains complete.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceEnginePinReleaseFailClosed() {
    for mode in [ProjectionMode::HybridStrict] {
        let tag = match mode {
            ProjectionMode::HybridStrict => "eng-ac3-strict",
            ProjectionMode::HybridAsync => "eng-ac3-async",
        };
        let root = base_dir(tag);
        engine_live_pin_reopen_and_release_impl(&root, mode).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Test the retention-floor and source-pin invariants on hybrid-strict through
/// the full engine ComposedBackend integration,
/// covering deleted-prefix fail-closed, retained floor/head replay recovery,
/// behind-image failure, and source-pin block/release guarantees.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceRetentionFloorSourcePinEngineInvariant() {
    for mode in [ProjectionMode::HybridStrict] {
        let tag = match mode {
            ProjectionMode::HybridStrict => "strict",
            ProjectionMode::HybridAsync => "async",
        };

        // --- fail-closed + recovery (shared root) ---
        let root = base_dir(tag);
        retention_floor_fail_closed_and_recovery_impl(&root, mode).await;
        retained_floor_head_replay_impl(&root, mode).await;
        behind_image_fails_closed_impl(&root, mode).await;
        std::fs::remove_dir_all(&root).ok();

        // --- source-pin (independent root) ---
        let root_pin = base_dir(&format!("{tag}-pin"));
        source_pin_blocks_reclamation_impl(&root_pin, mode).await;
        std::fs::remove_dir_all(&root_pin).ok();
    }
}

#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceHybridAsyncRetentionWaitsForCompleteFrontierProof() {
    let root = base_dir("async-frontier-retain");
    let backend = make_mode(&root, ProjectionMode::HybridAsync)
        .recover()
        .expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    acquire_maintenance_owner(&backend).await;
    push_commands(&backend, "old", 4, 10).await;
    drain(&backend);
    push_commands(&backend, "fresh", 2, 10_000).await;
    drain(&backend);
    let report = backend.tick(ts(10_000)).await.unwrap();
    assert_eq!(
        report.maintenance.stopped_by,
        Some(MaintenanceStopReason::FrontierProofMissing)
    );
    assert_eq!(floor_seq(&backend), None);
    assert_eq!(
        backend
            .read_from(&qkey(), None, 100)
            .await
            .unwrap()
            .entries
            .len(),
        6,
        "frontier-missing async maintenance retains the complete log"
    );
    drop(backend);
    let reopened = make_mode(&root, ProjectionMode::HybridAsync)
        .recover()
        .expect("reopen retained async log");
    assert_eq!(floor_seq(&reopened), None);
    assert_eq!(
        reopened
            .read_from(&qkey(), None, 100)
            .await
            .unwrap()
            .entries
            .len(),
        6,
        "frontier-missing async history remains readable after reopen"
    );
    drop(reopened);
    std::fs::remove_dir_all(&root).ok();
}
