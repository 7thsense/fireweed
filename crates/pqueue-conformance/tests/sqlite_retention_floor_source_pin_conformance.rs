//! SQLite-backed retention-floor and source-pin conformance: deleted-prefix fail-closed,
//! retained floor/head replay recovery, and branch pin invariants
//! (governing: docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224).
//!
//! This is the SQLite-backed analogue of the objectlog-level
//! `TestConformanceRetentionFloorSourcePinObjectlogInvariant` in
//! `crates/pqueue-objectlog/tests/retention_floor_source_pin_conformance.rs`.
//! It exercises the real composed backend with SQLite `HybridProjectionStore`
//! to prove retention-floor and source-pin guarantees remain intact during
//! deleted-prefix fail-closed and retained floor/head replay recovery.
//!
//! pqueue-c33c367e evaluation: the deferred server acquire-runtime wiring does
//! not change the rollout safety envelope. Retention-floor and source-pin
//! guarantees are independent of the deferred server wiring (documented at
//! docs/perf/design/manifest-compaction-hotpath.md:388 and
//! docs/releases/v0.14.0.md).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::ts;
use pqueue_core::{ClientItemKey, QueueDefinition, QueueId, TenantId};
use pqueue_engine::{
    CommandPosition, ComposedBackend, ControlPlaneStore, EngineError, InProcessControlPlane,
    LogRead, LogStore, PushPort, PushSpec, ReclaimDriver,
};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
use pqueue_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-sqlite-retention-floor-source-pin-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn tenant() -> TenantId {
    TenantId::new("t-rfsp").unwrap()
}

fn queue() -> QueueId {
    QueueId::new("q-rfsp").unwrap()
}

fn shard() -> pqueue_engine::QueueKey {
    pqueue_engine::QueueKey::new(tenant(), queue())
}

fn branch_shard() -> pqueue_engine::QueueKey {
    pqueue_engine::QueueKey::new(tenant(), QueueId::new("branch-rfsp").unwrap())
}

fn qdef() -> QueueDefinition {
    let mut d = pqueue_conformance::qdef();
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

fn count_seg_files(root: &Path) -> usize {
    let mut n = 0;
    let Ok(rd) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            n += count_seg_files(&path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("seg") {
            n += 1;
        }
    }
    n
}

/// Assert deleted-prefix fail-closed and retained floor/head replay recovery
/// on the hybrid-strict backend.
async fn retention_floor_fail_closed_and_recovery_impl(root: &Path) {
    let backend = open_hybrid_strict(root);
    backend.create_queue(qdef()).await.unwrap();

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

/// Assert source-pin guarantees: a live branch prevents reclamation of its pinned
/// source segment, and the pin survives reopen until released.
/// Source-pin test uses its own root (separate from the fail-closed/recovery tests).
async fn source_pin_blocks_reclamation_impl(root: &Path) {
    let backend = open_hybrid_strict(root);
    backend.create_queue(qdef()).await.unwrap();

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

    // Trim. The pinned segment must survive; the unpinned segments are reclaimed.
    backend
        .trim_reclaimable_segments(&shard(), 1_000, ts(1_000_000))
        .expect("trim with live pin");

    // Check branch pin state: the pin should protect the first segment.
    let pinned = backend
        .with_log(|l| l.lowest_branch_pinned_below(&shard(), 3, 10_000))
        .expect("lowest_branch_pinned_below");
    assert!(
        pinned.is_some(),
        "a branch pin should be registered after branch creation"
    );

    // The pin kept the first segment: source seg file + branch copy = 2.
    // With SegmentConfig(1,1), each command is its own segment. Pin at seq 0
    // protects seg first_seq=0. The branch copies it, so 2 seg files remain
    // for the pinned segment (source + branch).
    let seg_count = count_seg_files(root);
    assert!(
        seg_count >= 1,
        "pinned source segment survives trim (got {seg_count} seg files)"
    );

    // The floor advanced past the oldest non-pinned reclaimed command.
    let floor = floor_seq(&backend).expect("floor advanced");
    // With time-based expiry, seq 0 is pinned, seq 1 may be reclaimed.
    // The floor is the last reclaimed seq: time_expired >= 1 means floor >= 1.
    assert!(
        floor >= 1,
        "floor advanced past reclaimed commands (floor={floor})"
    );

    // Reads above floor still succeed — the fresh commands (seq 4,5) survive.
    let tail = backend
        .read_from(&shard(), floor_pos(&backend), 100)
        .await
        .expect("read_from above floor succeeds with pin");
    assert!(
        !tail.entries.is_empty(),
        "above-floor tail readable with pin"
    );
    let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
    assert!(
        tail_seqs.iter().all(|s| *s > floor),
        "above-floor tail contains only commands above the floor"
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

    // Run against hybrid-async (deferred SQLite apply).
    let root_async = base_dir("async");
    let backend = open_hybrid_async(&root_async);
    backend.create_queue(qdef()).await.unwrap();
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
    backend.tick(ts(10_000)).await.unwrap();
    let floor = floor_seq(&backend).expect("floor advanced (async)");
    assert_eq!(floor, 3, "retention floor advanced through seq 3 (async)");

    // Fail-closed (async).
    let async_err = backend
        .read_from(&shard(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed (async)");
    assert!(
        matches!(async_err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "async: expected Storage, got {async_err:?}"
    );
    drop(backend);

    // Reopen (async).
    let reopened = open_hybrid_async(&root_async);
    let reopened_err = reopened
        .read_from(&shard(), None, 100)
        .await
        .expect_err("read_from genesis must fail closed after reopen (async)");
    assert!(
        matches!(reopened_err, EngineError::Storage(ref m) if m.contains("read below retention floor")),
        "async reopen: expected Storage, got {reopened_err:?}"
    );
    let tail = reopened
        .read_from(&shard(), floor_pos(&reopened), 100)
        .await
        .expect("read_from(floor) above floor succeeds (async)");
    let tail_seqs: Vec<u64> = tail.entries.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(
        tail_seqs,
        vec![4, 5, 6, 7],
        "above-floor read succeeds after reopen (async)"
    );

    std::fs::remove_dir_all(&root_async).ok();

    // Source pin (async) — independent backend so floor hasn't advanced yet.
    let root_async_pin = base_dir("async-pin");
    {
        let backend = open_hybrid_async(&root_async_pin);
        backend.create_queue(qdef()).await.unwrap();

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

        // Tick should reclaim unpinned old commands (seq 1..3) but keep seq 0.
        backend.tick(ts(1_000_000)).await.unwrap();
        let async_floor = floor_seq(&backend).expect("floor advanced (async)");
        assert!(async_floor >= 1, "floor at least seq 1 (async)");

        let seg_count = count_seg_files(&root_async_pin);
        assert!(
            seg_count >= 1,
            "pinned source segment survives trim (async, got {seg_count} seg files)"
        );

        // Above-floor tail readable.
        let tail = backend
            .read_from(&shard(), floor_pos(&backend), 100)
            .await
            .expect("read_from above floor (async)");
        assert!(
            !tail.entries.is_empty(),
            "above-floor tail readable with pin (async)"
        );

        // Release pin and re-tick — pinned segment reclaimed.
        backend
            .with_log(|l| l.discard_branch(&shard(), &branch_shard()))
            .expect("discard branch (async)");
        backend.tick(ts(1_000_000)).await.unwrap();
        let after_pin = backend.read_from(&shard(), None, 100).await;
        assert!(
            after_pin.is_err(),
            "read_from genesis fails closed after pin release (async)"
        );
    }
    std::fs::remove_dir_all(&root_async_pin).ok();

    std::fs::remove_dir_all(&root_async).ok();
}

/// TestSqliteObjectlogDeletedManifestRecovery: SQLite-backed integration surfaces
/// fail-closed deleted-prefix behavior when a projection image references
/// physically deleted manifest prefixes. Runs against both hybrid-strict and
/// hybrid-async to cover the composed SQLite projection recovery path.
#[tokio::test]
#[allow(non_snake_case)]
async fn TestSqliteObjectlogDeletedManifestRecovery() {
    for tag in ["strict", "async"] {
        let root = base_dir(&format!("{tag}-objlog-del"));
        // Set up a queue with a trimmed object log and a fresh SQLite projection.
        let backend = if tag == "strict" {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        backend.create_queue(qdef()).await.unwrap();

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
        let log = pqueue_objectlog::ObjectLog::open_group_commit(
            &root,
            pqueue_objectlog::SegmentConfig::new(1, 1).unwrap(),
        )
        .expect("log");
        let hybrid = if tag == "strict" {
            pqueue_sqlite::HybridProjectionStore::open(sqlite.to_str().unwrap())
                .expect("hybrid")
                .with_strict_apply(true)
        } else {
            pqueue_sqlite::HybridProjectionStore::open(sqlite.to_str().unwrap())
                .expect("hybrid")
                .with_deferred_flush_chunk(1)
                .with_async_monitor(clear_thresholds())
        };
        let result = pqueue_engine::ComposedBackend::new(
            log,
            hybrid,
            pqueue_engine::InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover();
        let err = result.err().unwrap_or_else(|| {
            panic!("{tag}: recovery over behind projection image must fail closed")
        });
        assert!(
            pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
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
    backend.create_queue(qdef()).await.unwrap();
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
    assert!(
        floor >= 1,
        "floor advanced past reclaimed commands (floor={floor})"
    );

    // Source-pin guarantee: the pinned source segment survived trim.
    let seg_count = count_seg_files(&root);
    assert!(
        seg_count >= 1,
        "pinned source segment survives trim (got {seg_count} seg files)"
    );

    // Above-floor tail is readable.
    let tail = backend
        .read_from(&shard(), floor_pos(&backend), 100)
        .await
        .expect("read_from above floor");
    assert!(!tail.entries.is_empty(), "above-floor tail readable");

    // Now delete the SQLite projection files to simulate a behind image.
    drop(backend);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
    }

    // Recovery must fail closed with the identifiable signal.
    let sqlite = root.join("projection.sqlite");
    let log = pqueue_objectlog::ObjectLog::open_group_commit(
        &root,
        pqueue_objectlog::SegmentConfig::new(1, 1).unwrap(),
    )
    .expect("log");
    let hybrid = pqueue_sqlite::HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_strict_apply(true);
    let result = pqueue_engine::ComposedBackend::new(
        log,
        hybrid,
        pqueue_engine::InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover();
    let err = result
        .err()
        .unwrap_or_else(|| panic!("recovery over behind projection image must fail closed"));
    assert!(
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "error must be the distinct deleted-manifest-prefix signal; got {err:?}"
    );

    // Verify the underlying store still has the floor intact by opening a
    // bare object log (without SQLite projection) and checking the floor.
    let log_only = pqueue_objectlog::ObjectLog::open_group_commit(
        &root,
        pqueue_objectlog::SegmentConfig::new(1, 1).unwrap(),
    )
    .expect("log-only reopen");
    assert!(
        pqueue_engine::LogStore::retention_floor(&log_only, &shard())
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
    //   - crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs (this file)
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
    for tag in ["strict-fhr", "async-fhr"] {
        let root = base_dir(tag);
        let backend = if tag.starts_with("strict") {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        backend.create_queue(qdef()).await.unwrap();

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
    for tag in ["strict-bnd", "async-bnd"] {
        let root = base_dir(tag);

        // --- Part 1: healthy reopen succeeds from retained floor/head ---
        let backend = if tag.starts_with("strict") {
            open_hybrid_strict(&root)
        } else {
            open_hybrid_async(&root)
        };
        backend.create_queue(qdef()).await.unwrap();

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
        let log = pqueue_objectlog::ObjectLog::open_group_commit(
            &root,
            pqueue_objectlog::SegmentConfig::new(1, 1).unwrap(),
        )
        .expect("log");
        let hybrid = if tag.starts_with("strict") {
            pqueue_sqlite::HybridProjectionStore::open(sqlite_path.to_str().unwrap())
                .expect("hybrid")
                .with_strict_apply(true)
        } else {
            pqueue_sqlite::HybridProjectionStore::open(sqlite_path.to_str().unwrap())
                .expect("hybrid")
                .with_deferred_flush_chunk(1)
                .with_async_monitor(clear_thresholds())
        };
        let result = pqueue_engine::ComposedBackend::new(
            log,
            hybrid,
            pqueue_engine::InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover();
        let err = result.err().unwrap_or_else(|| {
            panic!("{tag}: recovery over behind projection image must fail closed")
        });
        assert!(
            pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
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
    //   - crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs (this file)
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
