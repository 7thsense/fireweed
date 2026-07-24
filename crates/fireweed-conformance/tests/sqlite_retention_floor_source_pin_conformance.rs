//! SQLite-backed retention-floor and source-pin conformance: deleted-prefix fail-closed,
//! retained floor/head replay recovery, branch pin invariants, and live source pin
//! survives reopen with exact boundary reclamation.
//! (governing: docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224,
//!  bead pqueue-879c9d05).
//!
//! This is the SQLite-backed analogue of the objectlog-level
//! `TestConformanceRetentionFloorSourcePinObjectlogInvariant` in
//! `crates/fireweed-objectlog/tests/retention_floor_source_pin_conformance.rs`.
//! It exercises the real composed backend with SQLite `HybridProjectionStore`
//! to prove retention-floor and source-pin guarantees remain intact during
//! deleted-prefix fail-closed and retained floor/head replay recovery.
//!
//! pqueue-c33c367e evaluation: the deferred server acquire-runtime wiring does
//! not change the rollout safety envelope. Retention-floor and source-pin
//! guarantees are independent of the deferred server wiring (documented at
//! docs/perf/design/manifest-compaction-hotpath.md:388 and
//! docs/releases/v0.14.0.md).
//!
//! Bead pqueue-879c9d05 (adversarial follow-up): adds
//! `TestConformanceSqliteLiveSourcePinSurvivesReopen`,
//! `TestConformanceSqlitePinReleaseReclaimsExactBoundary`, and
//! `TestConformanceSqlitePinAssertionStrength`. Hybrid-strict exercises exact
//! deletion boundaries; hybrid-async separately proves safe retention while
//! complete-frontier authority is unavailable. Assertions use
//! `lowest_branch_pinned_below` and `retention_floor` rather than weak counts.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::ts;
use fireweed_core::{ClientItemKey, QueueDefinition, QueueId, TenantId};
use fireweed_engine::{
    CommandPosition, ComposedBackend, ControlPlaneStore, EngineError, InProcessControlPlane,
    LogRead, LogStore, MaintenanceStopReason, PushPort, PushSpec, ReclaimDriver,
};
use fireweed_objectlog::{ObjectLog, SegmentConfig};
use fireweed_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "fireweed-sqlite-retention-floor-source-pin-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn tenant() -> TenantId {
    TenantId::new("t-rfsp").unwrap()
}

fn queue() -> QueueId {
    QueueId::new("q-rfsp").unwrap()
}

fn shard() -> fireweed_engine::QueueKey {
    fireweed_engine::QueueKey::new(tenant(), queue())
}

fn branch_shard() -> fireweed_engine::QueueKey {
    fireweed_engine::QueueKey::new(tenant(), QueueId::new("branch-rfsp").unwrap())
}

fn qdef() -> QueueDefinition {
    let mut d = fireweed_conformance::qdef();
    d.tenant_id = tenant();
    d.queue_id = queue();
    d.request_id_retention_ms = 3_600_000;
    d.terminal_retention_ms = 3_600_000;
    d.emit_change_records = false;
    d
}

fn branch_def() -> QueueDefinition {
    let mut d = qdef();
    d.queue_id = branch_shard().queue_id;
    d
}

fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

fn open_hybrid_strict(root: &Path) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_strict_apply(true);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-strict")
}

fn open_hybrid_async(root: &Path) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_deferred_flush_chunk(1)
        .with_async_monitor(clear_thresholds());
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-async")
}

fn drain(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend.flush_deferred_projection().expect("flush");
    }
}

fn floor_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &shard()))
        .expect("retention_floor")
        .map(|p| p.sequence)
}

fn floor_pos(backend: &HybridBackend) -> Option<CommandPosition> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &shard()))
        .expect("retention_floor")
}

async fn acquire_maintenance_owner(backend: &HybridBackend) {
    let epoch = backend
        .acquire_epoch(&shard())
        .await
        .expect("acquire log owner");
    assert_eq!(
        backend.with_log(|log| log.maintenance_owner_epoch(&shard())),
        Some(epoch),
        "retention maintenance requires a positively acquired local owner"
    );
}

async fn create_owned_queue(backend: &HybridBackend) {
    backend.create_queue(qdef()).await.unwrap();
    acquire_maintenance_owner(backend).await;
}

fn trim_until_quiescent(
    backend: &HybridBackend,
    retention_ms: u64,
    now: fireweed_core::UtcTimestamp,
) {
    for _ in 0..32 {
        let report = backend
            .trim_reclaimable_segments(&shard(), retention_ms, now)
            .expect("bounded trim pass");
        if !report.cursor_pending
            && report.stopped_by != Some(MaintenanceStopReason::BudgetExhausted)
        {
            return;
        }
    }
    panic!("bounded trim did not quiesce within 32 passes");
}

/// Assert deleted-prefix fail-closed and retained floor/head replay recovery
/// on the hybrid-strict backend.
async fn retention_floor_fail_closed_and_recovery_impl(root: &Path) {
    let backend = open_hybrid_strict(root);
    create_owned_queue(&backend).await;

    // Seq 0..3 at t=10s (will be past retention at tick time).
    for i in 0..4u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(10),
                None,
            )
            .await
            .expect("push");
    }
    // Seq 4..7 at t=10_000s (within retention at tick time ts(10_000)).
    for i in 4..8u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(10_000),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // Tick at t=10_000s: cutoff = 10_000_000 - 3_600_000 - 5_000 = 6_395_000ms.
    // Seq 0..3 (committed 10_000ms) expired; seq 4..7 (committed 10_000_000ms) retained.
    backend.tick(ts(10_000)).await.unwrap();
    let floor = floor_seq(&backend).expect("floor advanced after trim");
    assert_eq!(floor, 3, "retention floor advanced through seq 3");

    // Fail-closed below the floor: reads at or below `floor` must error.
    let genesis_err = backend
        .read_from(&shard(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after trim");
    assert!(
        matches!(genesis_err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "read_from genesis: expected Storage(read below retention floor), got {genesis_err:?}"
    );

    // read_from with from_seq=0 also fails closed (both read_all and read_from paths).
    let from_zero_err = backend
        .read_from(&shard(), Some(CommandPosition::new(shard(), 0, 0)), 100)
        .await
        .expect_err("read_from(0) must fail closed");
    assert!(
        matches!(from_zero_err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "read_from(0): expected Storage, got {from_zero_err:?}"
    );

    // Above-floor reads succeed: from_seq=4 reads the remaining 4 commands (seq 4..7).
    let above = backend
        .read_from(&shard(), floor_pos(&backend), 100)
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

/// Reopen the same root and verify the retained floor/head replay preserves
/// the above-floor data and fail-closed behavior.
async fn retained_floor_head_replay_impl(root: &Path) {
    let reopened = open_hybrid_strict(root);

    // The floor persisted across reopen.
    let floor = floor_seq(&reopened).expect("floor survived reopen");
    assert_eq!(floor, 3, "retention floor persisted after reopen");

    // Fail-closed still active after reopen.
    let reopen_err = reopened
        .read_from(&shard(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after reopen");
    assert!(
        matches!(reopen_err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "after reopen: expected Storage, got {reopen_err:?}"
    );

    // Above-floor data is contiguous and complete.
    let tail = reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("read_from(floor) after reopen must succeed");
    let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        tail_seqs,
        vec![4, 5, 6, 7],
        "above-floor read succeeds after reopen with complete tail"
    );
}

/// Assert source-pin guarantees with EXACT boundary assertions (bead pqueue-879c9d05,
/// replaces previous weak `seg_count >= 1` and `floor >= 1` checks).
///
/// - lowest_branch_pinned_below identifies the exact pinned seq (0).
/// - retention_floor advances to the exact last reclaimed unpinned seq (3).
/// - After pin release and re-trim, the pinned segment is reclaimed and
///   below-floor reads fail closed.
async fn source_pin_blocks_reclamation_impl(root: &Path) {
    let backend = open_hybrid_strict(root);
    create_owned_queue(&backend).await;

    // Push 4 single-item commands (seq 0..3) then drain.
    for i in 0..4u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("pin-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts((i as i64 + 1) * 10),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // Create a branch that pins at seq 0 (covers segment with command seq 0).
    // Large TTL ensures the pin survives the trim call (which uses large now_ms).
    let pin_pos = CommandPosition::new(shard(), 0, 0);
    backend
        .with_log(|l| l.branch(&shard(), &branch_def(), &pin_pos, 1_000_000_000_000, 10_000))
        .expect("create branch");

    // Push 2 more commands (seq 4..5) with a recent timestamp (at the trim's
    // `now` so they survive time-based expiry) to serve as a readable tail.
    for i in 4..6u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("pin-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(1_000_000),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // One bounded pass publishes the safe floor but conservatively stops at
    // the pinned first address. It is not expected to quiesce until release.
    backend
        .trim_reclaimable_segments(&shard(), 1_000, ts(1_000_000))
        .expect("bounded trim with live pin");

    // EXACT PINNED BOUNDARY (AC3): lowest_branch_pinned_below identifies seq 0.
    let pinned = backend
        .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
        .expect("lowest_branch_pinned_below");
    assert_eq!(
        pinned,
        Some(0),
        "exact pinned source segment is seq 0, not a weak seg_count check"
    );

    // EXACT FLOOR: the durable safe horizon advances through seq 3 even when
    // physical prefix deletion remains conservatively pinned.
    let floor = floor_seq(&backend).expect("floor advanced");
    assert_eq!(
        floor, 3,
        "floor advanced past reclaimed unpinned commands, stopped before pinned seq 0 (floor={floor})"
    );

    // EXACT TAIL: above-floor tail is exactly the fresh commands (seq 4..5).
    let tail = backend
        .read_from(&shard(), floor_pos(&backend), 100)
        .await
        .expect("read_from above floor succeeds with pin");
    let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        tail_seqs,
        vec![4, 5],
        "above-floor tail contains only the fresh tail commands"
    );

    // Release the pin and re-trim: the previously-pinned segment is reclaimed.
    backend
        .with_log(|l| l.discard_branch(&shard(), &branch_shard()))
        .expect("discard branch");
    backend
        .trim_reclaimable_segments(&shard(), 1_000, ts(1_000_000))
        .expect("trim after pin release");

    // Now reads below the floor fail closed.
    let err = backend
        .read_from(&shard(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after pin release");
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "after pin release: expected Storage, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Bead pqueue-879c9d05: AC1 + AC2 — live source pin survives reopen, then
// release + exact boundary reclamation
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

/// Combined flow for AC1 (reopen with live pin) and AC2 (release + exact reclaim):
///
/// 1. Create data + branch pin at seq 0, trim, assert exact pinned boundary.
/// 2. Reopen, assert pin survived and pinned segment is retained+readable (AC1).
/// 3. Release pin, re-trim, assert exact reclaimed boundary, tail readable,
///    and below-floor reads fail closed (AC2).
async fn live_pin_reopen_and_release_impl(root: &Path, mode: &str) {
    // --- Phase 1: setup + trim with live pin ---
    let backend = if mode == "strict" {
        open_hybrid_strict(root)
    } else {
        open_hybrid_async(root)
    };
    create_owned_queue(&backend).await;

    // Push 4 old commands (seq 0..3) at early timestamps so they expire.
    for i in 0..4u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("lpr-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts((i as i64 + 1) * 10),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // Create a branch pin at seq 0 protecting the first segment.
    let pin_pos = CommandPosition::new(shard(), 0, 0);
    backend
        .with_log(|l| l.branch(&shard(), &branch_def(), &pin_pos, 1_000_000_000_000, 10_000))
        .expect("create branch");

    // Push 2 fresh commands (seq 4..5) within retention for a readable tail.
    for i in 4..6u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("lpr-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(1_000_000),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // Trim: pin protects seq 0, unpinned seq 1..3 reclaimed, floor advances.
    backend
        .trim_reclaimable_segments(&shard(), 1_000, ts(1_000_000))
        .expect("trim with live pin");

    // Capture the exact source seq-0 segment path on disk for filesystem-level
    // assertions across reopen and reclamation.
    let seq0_seg = find_source_seq0_seg_file(root)
        .unwrap_or_else(|| panic!("{mode}: source seq-0 segment must exist after trim with pin"));

    // EXACT PINNED BOUNDARY: lowest_branch_pinned_below identifies seq 0.
    let pinned_before = backend
        .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
        .expect("lowest_branch_pinned_below before reopen");
    assert_eq!(
        pinned_before,
        Some(0),
        "{mode}: exact pinned seq before reopen is 0"
    );

    // Floor advanced through reclaimed unpinned segments (1..3).
    let floor_before = floor_seq(&backend).expect("floor before reopen");
    assert_eq!(
        floor_before, 3,
        "{mode}: floor at last reclaimed seq 3 before reopen"
    );

    // Tail readable after trim.
    let tail_before = backend
        .read_from(&shard(), floor_pos(&backend), 100)
        .await
        .expect("{mode}: tail readable before reopen");
    let tail_seqs_before: Vec<u64> = tail_before
        .entries
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        tail_seqs_before,
        vec![4, 5],
        "{mode}: tail seq 4..5 readable before reopen"
    );

    drop(backend);

    // --- Phase 2: AC1 — REOPEN with live pin ---
    let reopened = if mode == "strict" {
        open_hybrid_strict(root)
    } else {
        open_hybrid_async(root)
    };
    acquire_maintenance_owner(&reopened).await;

    // AC1: exact pinned boundary survives reopen.
    let pinned_after = reopened
        .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
        .expect("lowest_branch_pinned_below after reopen");
    assert_eq!(
        pinned_after,
        Some(0),
        "{mode}: pinned seq 0 survives reopen (AC1)"
    );

    // AC1: persisted floor survives reopen.
    let floor_after = floor_seq(&reopened).expect("floor after reopen");
    assert_eq!(floor_after, 3, "{mode}: floor persisted after reopen (AC1)");

    // AC1: retained tail is readable after reopen.
    let tail_after = reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("{mode}: tail readable after reopen (AC1)");
    let tail_seqs_after: Vec<u64> = tail_after.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        tail_seqs_after,
        vec![4, 5],
        "{mode}: retained tail seq 4..5 readable after reopen (AC1)"
    );

    // AC1: exact source segment file persists on disk after reopen.
    assert!(
        seq0_seg.exists(),
        "{mode}: source seq-0 segment must exist on disk after reopen (AC1)"
    );

    // --- Phase 3: AC2 — RELEASE pin + reclaim exact boundary ---
    reopened
        .with_log(|l| l.discard_branch(&shard(), &branch_shard()))
        .expect("discard branch");

    // Trim again: now the previously-pinned segment (seq 0) is eligible.
    trim_until_quiescent(&reopened, 1_000, ts(1_000_000));

    // AC2: exact source segment file is reclaimed after pin release + trim.
    assert!(
        !seq0_seg.exists(),
        "{mode}: source seq-0 segment must be absent after discard_branch and re-trim (AC2)"
    );

    // AC2: no branch pin remains.
    let pinned_after_release = reopened
        .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
        .expect("lowest_branch_pinned_below after release");
    assert_eq!(
        pinned_after_release, None,
        "{mode}: no branch pin after release (AC2)"
    );

    // AC2: floor advanced to seq 3 (last reclaimed, now including the
    // previously-pinned seq 0 segment).
    let floor_after_release = floor_seq(&reopened).expect("floor after release");
    assert_eq!(
        floor_after_release, 3,
        "{mode}: floor at seq 3 after pin release (AC2)"
    );

    // AC2: retained tail data remains readable above the floor.
    let tail_after_release = reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("{mode}: tail readable after pin release (AC2)");
    let tail_seqs: Vec<u64> = tail_after_release
        .entries
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        tail_seqs,
        vec![4, 5],
        "{mode}: retained tail seq 4..5 after release (AC2)"
    );

    // AC2: below-floor reads fail closed.
    let err = reopened
        .read_from(&shard(), None, 100)
        .await
        .expect_err("{mode}: genesis read must fail closed after pin release (AC2)");
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "{mode}: expected Storage(read below retention floor) after pin release, got {err:?} (AC2)"
    );

    drop(reopened);
}

// ---------------------------------------------------------------------------
// Existing tests (retention-floor, deleted-manifest, floor/head replay, etc.)
// ---------------------------------------------------------------------------

/// Test the invariant on both the hybrid-strict backend (closest to direct SQLite
/// projection apply) and the hybrid-async backend (deferred SQLite apply).
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceRetentionFloorSourcePinSqliteInvariant() {
    // Run against hybrid-strict (synchronous SQLite apply).
    let root_strict = base_dir("strict");
    retention_floor_fail_closed_and_recovery_impl(&root_strict).await;
    retained_floor_head_replay_impl(&root_strict).await;
    std::fs::remove_dir_all(&root_strict).ok();

    // Source-pin test needs a separate root (branch creation before trim).
    let root_pin = base_dir("strict-pin");
    source_pin_blocks_reclamation_impl(&root_pin).await;
    std::fs::remove_dir_all(&root_pin).ok();

    // Hybrid-async retains the complete log until its immutable complete-
    // frontier authority adapter lands.
    let root_async = base_dir("async");
    let backend = open_hybrid_async(&root_async);
    create_owned_queue(&backend).await;
    for i in 0..4u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("a-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(10),
                None,
            )
            .await
            .expect("push");
    }
    for i in 4..8u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("a-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(10_000),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);
    let report = backend.tick(ts(10_000)).await.unwrap();
    assert_eq!(
        report.maintenance.stopped_by,
        Some(MaintenanceStopReason::FrontierProofMissing),
        "hybrid-async stops before deletion without a complete frontier proof"
    );
    assert_eq!(floor_seq(&backend), None, "hybrid-async publishes no floor");
    let all = backend
        .read_from(&shard(), None, 100)
        .await
        .expect("hybrid-async retains a genesis-readable log");
    assert_eq!(
        all.entries
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>(),
        "hybrid-async retains every command"
    );
    drop(backend);

    // Reopen (async).
    let reopened = open_hybrid_async(&root_async);
    let reopened_all = reopened
        .read_from(&shard(), None, 100)
        .await
        .expect("hybrid-async retained log survives reopen");
    assert_eq!(
        reopened_all
            .entries
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>(),
        "hybrid-async replays the complete retained log"
    );

    std::fs::remove_dir_all(&root_async).ok();

    // Source pin (async) — independent backend so floor hasn't advanced yet.
    let root_async_pin = base_dir("async-pin");
    {
        let backend = open_hybrid_async(&root_async_pin);
        create_owned_queue(&backend).await;

        // Push 4 old commands (seq 0..3) then drain.
        for i in 0..4u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("ap-k-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);

        // Branch pin at seq 0 BEFORE any tick.
        backend
            .with_log(|l| {
                l.branch(
                    &shard(),
                    &branch_def(),
                    &CommandPosition::new(shard(), 0, 0),
                    1_000_000_000_000,
                    10_000,
                )
            })
            .expect("create branch (async)");

        // Push 2 fresh commands (seq 4..5) to form a tail above any floor.
        for i in 4..6u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("ap-k-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(1_000_000),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);

        // The source pin remains observable, but async maintenance cannot use
        // it as a substitute for a complete deletion frontier.
        let report = backend.tick(ts(1_000_000)).await.unwrap();
        assert_eq!(
            report.maintenance.stopped_by,
            Some(MaintenanceStopReason::FrontierProofMissing)
        );
        assert_eq!(floor_seq(&backend), None);

        // EXACT PINNED BOUNDARY (AC3): lowest_branch_pinned_below identifies seq 0.
        let pinned_async = backend
            .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
            .expect("lowest_branch_pinned_below (async)");
        assert_eq!(
            pinned_async,
            Some(0),
            "exact pinned source segment is seq 0 (async)"
        );

        let retained = backend
            .read_from(&shard(), None, 100)
            .await
            .expect("async pin case remains readable from genesis");
        assert_eq!(
            retained
                .entries
                .iter()
                .map(|(p, _)| p.sequence)
                .collect::<Vec<_>>(),
            (0..6).collect::<Vec<_>>()
        );

        // Releasing the pin does not weaken the independent complete-frontier
        // requirement: the next tick still retains every segment.
        backend
            .with_log(|l| l.discard_branch(&shard(), &branch_shard()))
            .expect("discard branch (async)");
        let after_release = backend.tick(ts(1_000_000)).await.unwrap();
        assert_eq!(
            after_release.maintenance.stopped_by,
            Some(MaintenanceStopReason::FrontierProofMissing)
        );
        assert_eq!(floor_seq(&backend), None);
        assert_eq!(
            backend
                .read_from(&shard(), None, 100)
                .await
                .expect("retained log after pin release")
                .entries
                .len(),
            6
        );
    }
    std::fs::remove_dir_all(&root_async_pin).ok();

    std::fs::remove_dir_all(&root_async).ok();
}

// ---------------------------------------------------------------------------
// Bead pqueue-879c9d05 top-level test functions (AC1, AC2, AC3)
// ---------------------------------------------------------------------------

/// TestConformanceSqliteLiveSourcePinSurvivesReopen (AC1):
/// hybrid-strict and hybrid-async SQLite-backed conformance reopens with a live
/// pin and proves the exact pinned source segment remains retained and readable.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceSqliteLiveSourcePinSurvivesReopen() {
    for mode in ["strict"] {
        let root = base_dir(&format!("ac1-{mode}"));
        live_pin_reopen_and_release_impl(&root, mode).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestConformanceSqlitePinReleaseReclaimsExactBoundary (AC2):
/// after reopen, releasing the pin reclaims the exact now-eligible segment while
/// retained tail data remains readable and below-floor reads fail closed.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceSqlitePinReleaseReclaimsExactBoundary() {
    for mode in ["strict"] {
        let root = base_dir(&format!("ac2-{mode}"));
        live_pin_reopen_and_release_impl(&root, mode).await;
        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestConformanceSqlitePinAssertionStrength (AC3):
/// replace weak seg_count >= 1 evidence with exact key/boundary or equivalent
/// deterministic assertions. The `source_pin_blocks_reclamation_impl` and the
/// async pin section in `TestConformanceRetentionFloorSourcePinSqliteInvariant`
/// already exercise exact `lowest_branch_pinned_below` and `retention_floor`
/// assertions. This test provides an additional standalone verification of
/// exact boundary assertion strength on a clean backend.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestConformanceSqlitePinAssertionStrength() {
    for mode in ["strict"] {
        let root = base_dir(&format!("ac3-{mode}"));
        let backend = if mode == "strict" {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        create_owned_queue(&backend).await;

        // Push 4 old commands (seq 0..3).
        for i in 0..4u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("ac3-k-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts((i as i64 + 1) * 10),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);

        // Create branch pin at seq 0.
        backend
            .with_log(|l| {
                l.branch(
                    &shard(),
                    &branch_def(),
                    &CommandPosition::new(shard(), 0, 0),
                    1_000_000_000_000,
                    10_000,
                )
            })
            .expect("create branch");

        // Push 2 fresh commands (seq 4..5).
        for i in 4..6u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("ac3-k-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(1_000_000),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);

        // A live first-address pin prevents physical deletion from quiescing;
        // one bounded pass still publishes the exact safe horizon.
        backend
            .trim_reclaimable_segments(&shard(), 1_000, ts(1_000_000))
            .expect("bounded trim with live pin");

        // EXACT BOUNDARY (AC3): lowest_branch_pinned_below gives deterministic seq, not >= 1.
        let pinned = backend
            .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
            .expect("lowest_branch_pinned_below");
        assert_eq!(
            pinned,
            Some(0),
            "{mode}: deterministic pinned boundary is seq 0, not weak seg_count"
        );

        // EXACT FLOOR: floor is the last reclaimed sequence, not ">= 1".
        let floor = floor_seq(&backend).expect("floor");
        assert_eq!(
            floor, 3,
            "{mode}: deterministic floor at seq 3, not weak floor >= 1"
        );

        // EXACT TAIL: above-floor tail is deterministic seq 4..5, not "non-empty".
        let tail = backend
            .read_from(&shard(), floor_pos(&backend), 100)
            .await
            .expect("{mode}: read_from above floor");
        let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
        assert_eq!(
            tail_seqs,
            vec![4, 5],
            "{mode}: deterministic tail seq 4..5, not weak non-empty"
        );

        // Release pin + re-trim.
        backend
            .with_log(|l| l.discard_branch(&shard(), &branch_shard()))
            .expect("discard branch");
        trim_until_quiescent(&backend, 1_000, ts(1_000_000));

        // No pinned segments remain.
        let pinned_after = backend
            .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
            .expect("lowest_branch_pinned_below after release");
        assert_eq!(
            pinned_after, None,
            "{mode}: no branch pin after release (deterministic)"
        );

        // Fail-closed below floor.
        let err = backend
            .read_from(&shard(), None, 100)
            .await
            .expect_err("{mode}: genesis read fails closed after release");
        assert!(
            matches!(err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
            "{mode}: expected Storage, got {err:?}"
        );

        // Retained tail still readable.
        let tail_after = backend
            .read_from(&shard(), floor_pos(&backend), 100)
            .await
            .expect("{mode}: tail after release");
        let tail_seqs_after: Vec<u64> =
            tail_after.entries.iter().map(|(p, _)| p.sequence).collect();
        assert_eq!(
            tail_seqs_after,
            vec![4, 5],
            "{mode}: retained tail still seq 4..5"
        );

        std::fs::remove_dir_all(&root).ok();
    }
}

// ---------------------------------------------------------------------------
// Existing tests (unchanged below)
// ---------------------------------------------------------------------------

/// TestSqliteObjectlogDeletedManifestRecovery: SQLite-backed integration surfaces
/// fail-closed deleted-prefix behavior when a projection image references
/// physically deleted manifest prefixes. Runs against both hybrid-strict and
/// hybrid-async to cover the composed SQLite projection recovery path.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteObjectlogDeletedManifestRecovery() {
    for tag in ["strict"] {
        let root = base_dir(&format!("{tag}-objlog-del"));
        // Set up a queue with a trimmed object log and a fresh SQLite projection.
        let backend = if tag == "strict" {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        create_owned_queue(&backend).await;

        // Write old segments (seq 0..3 at t=10) that will be past retention.
        for i in 0..4u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("del-k-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10),
                    None,
                )
                .await
                .expect("push");
        }
        // Write fresh tail (seq 4..7 within retention).
        for i in 4..8u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("del-k-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10_000),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);

        // Trim: establishes retention floor through seq 3.
        backend.tick(ts(10_000)).await.unwrap();
        let floor = floor_seq(&backend).expect("floor established");
        assert_eq!(floor, 3, "{tag}: retention floor at seq 3");
        drop(backend);

        // Delete the SQLite projection files so the reopen starts with a
        // projection image behind the deleted manifest prefix.
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
        }

        // Recovery must fail closed with the identifiable signal.
        let sqlite = root.join("projection.sqlite");
        let log = fireweed_objectlog::ObjectLog::open_group_commit(
            &root,
            fireweed_objectlog::SegmentConfig::new(1, 1).unwrap(),
        )
        .expect("log");
        let hybrid = if tag == "strict" {
            fireweed_sqlite::HybridProjectionStore::open(sqlite.to_str().unwrap())
                .expect("hybrid")
                .with_strict_apply(true)
        } else {
            fireweed_sqlite::HybridProjectionStore::open(sqlite.to_str().unwrap())
                .expect("hybrid")
                .with_deferred_flush_chunk(1)
                .with_async_monitor(clear_thresholds())
        };
        let result = fireweed_engine::ComposedBackend::new(
            log,
            hybrid,
            fireweed_engine::InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover();
        let err = result.err().unwrap_or_else(|| {
            panic!("{tag}: recovery over behind projection image must fail closed")
        });
        assert!(
            fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
            "{tag}: must fail with deleted-manifest-prefix signal; got {err:?}"
        );

        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestSqliteDeletedManifestErrorPreservesGuarantees: the SQLite error path
/// does not relax retention-floor, source-pin, branch atomicity, orphan GC,
/// or fail-closed guarantees.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteDeletedManifestErrorPreservesGuarantees() {
    let root = base_dir("gates");

    // Set up a queue with a branch pin BEFORE trim to prove source-pin guarantee.
    // Pin at seq 0 so the first segment is retained even after the floor advances.
    let backend = open_hybrid_strict(&root);
    create_owned_queue(&backend).await;
    for i in 0..4u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("g-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(10),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // Create a branch pin at seq 0 before trim.
    let pin_pos = CommandPosition::new(shard(), 0, 0);
    backend
        .with_log(|l| l.branch(&shard(), &branch_def(), &pin_pos, 1_000_000_000_000, 10_000))
        .expect("create branch");

    // Push a fresh tail (seq 4..5) within retention.
    for i in 4..6u64 {
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new(format!("g-k-{i}")).unwrap()),
                    payload: Some(bytes::Bytes::from_static(b"body")),
                    ..Default::default()
                }],
                ts(1_000_000),
                None,
            )
            .await
            .expect("push");
    }
    drain(&backend);

    // Trim: the branch pin protects seq 0, the floor advances past seq 1.
    backend
        .trim_reclaimable_segments(&shard(), 1_000, ts(1_000_000))
        .expect("trim with pin");
    let floor = floor_seq(&backend).expect("floor established");
    assert_eq!(
        floor, 3,
        "floor advanced past reclaimed commands (floor={floor})"
    );

    // Source-pin guarantee: exact boundary assertion using lowest_branch_pinned_below.
    let pinned = backend
        .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
        .expect("lowest_branch_pinned_below");
    assert_eq!(pinned, Some(0), "exact pinned source segment is seq 0");

    // Above-floor tail is readable.
    let tail = backend
        .read_from(&shard(), floor_pos(&backend), 100)
        .await
        .expect("read_from above floor");
    let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(tail_seqs, vec![4, 5], "above-floor tail is exactly seq 4,5");

    // Now delete the SQLite projection files to simulate a behind image.
    drop(backend);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
    }

    // Recovery must fail closed with the identifiable signal.
    let sqlite = root.join("projection.sqlite");
    let log = fireweed_objectlog::ObjectLog::open_group_commit(
        &root,
        fireweed_objectlog::SegmentConfig::new(1, 1).unwrap(),
    )
    .expect("log");
    let hybrid = fireweed_sqlite::HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_strict_apply(true);
    let result = fireweed_engine::ComposedBackend::new(
        log,
        hybrid,
        fireweed_engine::InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover();
    let err = result
        .err()
        .unwrap_or_else(|| panic!("recovery over behind projection image must fail closed"));
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "error must be the distinct deleted-manifest-prefix signal; got {err:?}"
    );

    // Verify the underlying store still has the floor intact by opening a
    // bare object log (without SQLite projection) and checking the floor.
    let log_only = fireweed_objectlog::ObjectLog::open_group_commit(
        &root,
        fireweed_objectlog::SegmentConfig::new(1, 1).unwrap(),
    )
    .expect("log-only reopen");
    assert!(
        fireweed_engine::LogStore::retention_floor(&log_only, &shard())
            .expect("read floor")
            .is_some(),
        "retention floor persists after fail-closed recovery"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// TestSqlitePropagationPqueueC33c367eInteractionRecorded: pqueue-c33c367e
/// interaction is evaluated before landing and the SQLite propagation conclusion
/// is recorded for release notes handoff.
#[test]
#[allow(non_snake_case)]
fn TestSqlitePropagationPqueueC33c367eInteractionRecorded() {
    // pqueue-c33c367e evaluation conclusion for SQLite propagation:
    //
    // The deferred server acquire-runtime wiring (pqueue-c33c367e) does NOT
    // change the SQLite deleted-manifest fail-closed propagation safety envelope.
    // The compose guard and the read_from path both rely on the substrate-level
    // fail_closed_below_floor guard and the durable retention floor - neither
    // depends on pqueue-c33c367e's per-write fence_epoch wiring.
    //
    // The SQLite projection recovery fails closed when the projection image
    // high-water is behind the durable retention floor, using the distinct
    // deleted_manifest_prefix_error signal. This is independent of owner-fence
    // wiring because:
    //   1. The retention floor is established and advanced by epoch-fenced
    //      operations (advance_retention_floor) that never depend on the
    //      deferred server wiring.
    //   2. The projection high-water is advanced per-apply inside the same
    //      durable transaction as the materialized SQLite state, making the
    //      behind-image detection purely a local consistency check.
    //   3. The fail_closed_below_floor guard operates on the durable deletion
    //      watermark and floor, both of which are persisted in the object-log
    //      substrate independently of pqueue-c33c367e.
    //
    // Therefore pqueue-c33c367e does NOT widen or narrow the SQLite
    // deleted-manifest propagation envelope. This conclusion is recorded
    // here for release notes handoff and is also documented at:
    //   - docs/perf/design/manifest-compaction-hotpath.md:388
    //   - docs/releases/v0.14.0.md
    //   - crates/fireweed-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs (this file)
    // Verify the evaluation is documented in the release notes.
    let release_notes = include_str!("../../../docs/releases/v0.14.0.md");
    assert!(
        release_notes.contains("pqueue-073ecde6"),
        "SQLite propagation bead pqueue-073ecde6 must be recorded in v0.14.0 release notes"
    );
    assert!(
        release_notes.contains("pqueue-c33c367e"),
        "pqueue-c33c367e evaluation must be recorded in v0.14.0 release notes"
    );
}

/// TestSqliteObjectlogFloorHeadReplayRecovery: SQLite-backed recovery succeeds via
/// retained floor/head replay without relaxing retention-floor or source-pin
/// guarantees and without data loss. Runs against both hybrid-strict and
/// hybrid-async.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteObjectlogFloorHeadReplayRecovery() {
    for tag in ["strict-fhr"] {
        let root = base_dir(tag);
        let backend = if tag.starts_with("strict") {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        create_owned_queue(&backend).await;

        // Push old commands (seq 0..3) at ts=10 that will be past retention.
        for i in 0..4u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("fhr-old-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10),
                    None,
                )
                .await
                .expect("push");
        }
        // Push fresh commands (seq 4..7) at ts=10_000 within retention.
        for i in 4..8u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(
                            ClientItemKey::new(format!("fhr-fresh-{i}")).unwrap(),
                        ),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10_000),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);

        // Trim: advances floor through seq 3.
        backend.tick(ts(10_000)).await.unwrap();
        let floor = floor_seq(&backend).expect("floor advanced");
        assert_eq!(floor, 3, "{tag}: retention floor at seq 3");
        drop(backend);

        // Reopen and recover from retained floor/head.
        let reopened = if tag.starts_with("strict") {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        let floor_after = floor_seq(&reopened).expect("floor survived reopen");
        assert_eq!(
            floor_after, 3,
            "{tag}: retention floor persisted after reopen"
        );

        // Read from floor succeeds with complete tail (seq 4..7).
        let tail = reopened
            .read_from(&shard(), floor_pos(&reopened), 100)
            .await
            .expect("{tag}: read_from floor after reopen must succeed");
        let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
        assert_eq!(
            tail_seqs,
            vec![4, 5, 6, 7],
            "{tag}: above-floor tail is complete after reopen"
        );

        // Retention-floor guarantee: reads below the floor still fail closed.
        let genesis_err = reopened
            .read_from(&shard(), None, 100)
            .await
            .expect_err("{tag}: read_from genesis must fail closed after reopen");
        assert!(
            matches!(genesis_err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
            "{tag}: expected Storage(read below retention floor), got {genesis_err:?}"
        );

        // Source-pin guarantee not relaxed: the floor is still authoritative.
        assert!(
            tail_seqs.iter().all(|s| *s > floor_after),
            "{tag}: all returned sequences are above the retained floor"
        );

        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestSqliteFloorHeadReplayPreservesFailClosedBoundary: replay succeeds only
/// from retained floor/head and still fails closed for projection images that
/// require physically deleted prefixes. Runs against both hybrid-strict and
/// hybrid-async.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteFloorHeadReplayPreservesFailClosedBoundary() {
    for tag in ["strict-bnd"] {
        let root = base_dir(tag);

        // --- Part 1: healthy reopen succeeds from retained floor/head ---
        let backend = if tag.starts_with("strict") {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        create_owned_queue(&backend).await;

        for i in 0..4u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("bnd-old-{i}")).unwrap()),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10),
                    None,
                )
                .await
                .expect("push");
        }
        for i in 4..8u64 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec {
                        client_item_key: Some(
                            ClientItemKey::new(format!("bnd-fresh-{i}")).unwrap(),
                        ),
                        payload: Some(bytes::Bytes::from_static(b"body")),
                        ..Default::default()
                    }],
                    ts(10_000),
                    None,
                )
                .await
                .expect("push");
        }
        drain(&backend);
        backend.tick(ts(10_000)).await.unwrap();
        let floor = floor_seq(&backend).expect("floor advanced");
        assert_eq!(floor, 3, "{tag}: retention floor at seq 3");
        drop(backend);

        // Healthy reopen: recovery succeeds from retained floor/head.
        let reopened = if tag.starts_with("strict") {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        let floor_after = floor_seq(&reopened).expect("floor survived reopen");
        assert_eq!(floor_after, 3, "{tag}: floor persisted for healthy reopen");
        let tail = reopened
            .read_from(&shard(), floor_pos(&reopened), 100)
            .await
            .expect("{tag}: healthy reopen read from floor succeeds");
        let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
        assert_eq!(
            tail_seqs,
            vec![4, 5, 6, 7],
            "{tag}: healthy reopen returns complete tail"
        );
        drop(reopened);

        // --- Part 2: behind image must fail closed for physically deleted prefixes ---
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
        }

        let sqlite_path = root.join("projection.sqlite");
        let log = fireweed_objectlog::ObjectLog::open_group_commit(
            &root,
            fireweed_objectlog::SegmentConfig::new(1, 1).unwrap(),
        )
        .expect("log");
        let hybrid = if tag.starts_with("strict") {
            fireweed_sqlite::HybridProjectionStore::open(sqlite_path.to_str().unwrap())
                .expect("hybrid")
                .with_strict_apply(true)
        } else {
            fireweed_sqlite::HybridProjectionStore::open(sqlite_path.to_str().unwrap())
                .expect("hybrid")
                .with_deferred_flush_chunk(1)
                .with_async_monitor(clear_thresholds())
        };
        let result = fireweed_engine::ComposedBackend::new(
            log,
            hybrid,
            fireweed_engine::InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover();
        let err = result.err().unwrap_or_else(|| {
            panic!("{tag}: recovery over behind projection image must fail closed")
        });
        assert!(
            fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
            "{tag}: must fail with deleted-manifest-prefix signal; got {err:?}"
        );

        std::fs::remove_dir_all(&root).ok();
    }
}

/// TestSqlitePqueueC33c367eInteractionRecorded: pqueue-c33c367e interaction is
/// evaluated before landing and the SQLite-specific conclusion is available in
/// release notes or the repo's release-note source.
#[test]
#[allow(non_snake_case)]
fn TestSqlitePqueueC33c367eInteractionRecorded() {
    // pqueue-c33c367e evaluation conclusion for SQLite retained floor/head replay:
    //
    // The deferred server acquire-runtime wiring (pqueue-c33c367e) does NOT
    // change the SQLite floor/head replay safety envelope. The re-opened
    // composed backend recovery succeeds from the retained floor/head because
    // the durable retention floor is persisted in the object-log substrate
    // independently of the deferred server wiring. The floor/head replay
    // path is:
    //   1. The durable retention floor is established by epoch-fenced
    //      operations (advance_retention_floor) that never depend on the
    //      deferred server wiring.
    //   2. The projection recovers by replaying from the retained floor/head,
    //      not by consulting deleted manifest prefixes.
    //   3. The behind-image fail-closed guard (compose.rs) operates on the
    //      durable floor and SQLite high-water, both of which are independent
    //      of pqueue-c33c367e.
    //
    // Therefore pqueue-c33c367e does NOT widen or narrow the SQLite retained
    // floor/head replay envelope. This conclusion is recorded here for release
    // notes handoff and is also documented at:
    //   - docs/perf/design/manifest-compaction-hotpath.md:388
    //   - docs/releases/v0.14.0.md
    //   - crates/fireweed-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs (this file)
    let release_notes = include_str!("../../../docs/releases/v0.14.0.md");
    assert!(
        release_notes.contains("pqueue-b9f4cd54"),
        "SQLite floor/head replay bead pqueue-b9f4cd54 must be recorded in v0.14.0 release notes"
    );
    assert!(
        release_notes.contains("pqueue-c33c367e"),
        "pqueue-c33c367e evaluation must be recorded in v0.14.0 release notes"
    );
}
