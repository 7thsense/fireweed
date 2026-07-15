// Shared hybrid-async test fixtures for behind-image fail-closed and recovery conformance tests
// (bead pqueue-a2957adb).
//
// Provides reusable backend construction, state inspection helpers, and scenario primitives
// that both fail-closed and recovery conformance test authors consume. This module contains
// **no assertion logic** — only the builder/inspection layer that test authors compose with
// their own assertions.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{QueueDefinition, RequestId};
use pqueue_engine::{
    CommandPosition, ComposedBackend, EngineError, InProcessControlPlane, LogStore, ProjectionRead,
    ProjectionStore, PushPort, PushSpec, QueueKey,
};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
use pqueue_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

use super::{qdef, shard as crate_shard, ts};

// ---------------------------------------------------------------------------
// Type alias
// ---------------------------------------------------------------------------

/// The composed-backend type for all objectlog/hybrid-async and objectlog/hybrid-strict tests.
pub type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

// ---------------------------------------------------------------------------
// Counter + temp directories
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a unique, isolated temporary directory for a test named `tag`. Each call
/// returns a new path so concurrent tests never collide.
pub fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-hybrid-async-{tag}-{}-{n}",
        std::process::id()
    ))
}

// ---------------------------------------------------------------------------
// Shared test shard
// ---------------------------------------------------------------------------

/// The shared single-queue shard key. Delegates to the crate-level [`shard`](crate::shard).
pub fn shard() -> QueueKey {
    crate_shard()
}

// ---------------------------------------------------------------------------
// Thresholds / configuration builders
// ---------------------------------------------------------------------------

/// Generous debt thresholds that keep the async-apply backpressure at `Clear`
/// under any reasonable test workload (apply-lag budget = 10k, timeouts = 1e9 ms).
pub fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

/// A queue definition with a **short** request-id and terminal retention (1 hour, in ms) so the
/// logical clock can step past the retention window within test timescales. Change-record emission
/// is OFF because reaping change records is orthogonal to the fixture's concerns.
pub fn qdef_short_retention() -> QueueDefinition {
    let mut d = qdef();
    d.request_id_retention_ms = 3_600_000;
    d.terminal_retention_ms = 3_600_000;
    d.emit_change_records = false;
    d
}

// ---------------------------------------------------------------------------
// Backend construction
// ---------------------------------------------------------------------------

/// Open the hybrid-async composed backend at `root` with `thresholds`:
///
/// - Group-commit ON
/// - `SegmentConfig(1, 1)` — one command per segment
/// - `flush_chunk = 1` — deferred backlog drains one command at a time
/// - Recovery runs on open (panics on error).
pub fn open_hybrid(root: &Path, thresholds: HybridAsyncThresholds) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_deferred_flush_chunk(1)
        .with_async_monitor(thresholds);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid-async")
}

/// Open the hybrid-strict composed backend at `root`:
///
/// - Group-commit ON
/// - `SegmentConfig(1, 1)` — one command per segment
/// - Strict projection ordering (SQLite-first, no async-apply debt monitor)
/// - Recovery runs on open (panics on error).
pub fn open_hybrid_strict(root: &Path) -> HybridBackend {
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

/// Open the hybrid-async backend on the **raw/synchronous append path** (`ObjectLog::open`,
/// NOT group-commit): every write force-seals its own segment immediately. `committed_at_ms` is
/// stamped from the batch's `max(created_at)`, not from group-commit's deferred flush.
///
/// Recovery runs on open (panics on error).
pub fn open_hybrid_raw(root: &Path, thresholds: HybridAsyncThresholds) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open(root).expect("raw log");
    let hybrid = HybridProjectionStore::open(sqlite.to_str().unwrap())
        .expect("hybrid")
        .with_deferred_flush_chunk(1)
        .with_async_monitor(thresholds);
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
        .recover()
        .expect("recover raw objectlog/hybrid-async")
}

// ---------------------------------------------------------------------------
// Projection mode enum + dispatch
// ---------------------------------------------------------------------------

/// The two projection substrate modes exercised by conformance tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionMode {
    /// Hybrid-async: SQLite deferred apply with async-apply debt monitor.
    HybridAsync,
    /// Hybrid-strict: SQLite-first synchronous apply (strict ordering).
    HybridStrict,
}

/// Open a backend by projection mode, using [`clear_thresholds`] for the async variant.
pub fn open_mode(root: &Path, mode: ProjectionMode) -> HybridBackend {
    match mode {
        ProjectionMode::HybridAsync => open_hybrid(root, clear_thresholds()),
        ProjectionMode::HybridStrict => open_hybrid_strict(root),
    }
}

// ---------------------------------------------------------------------------
// Push helpers
// ---------------------------------------------------------------------------

/// Push a default [`PushSpec`] under `key` at logical timestamp `at_s`. Panics on error.
/// The `key` parameter is used in the panic message for diagnostics.
pub async fn push(backend: &HybridBackend, key: &str, at_s: i64) -> Vec<pqueue_core::ItemId> {
    backend
        .push(&shard(), vec![PushSpec::default()], ts(at_s), None)
        .await
        .unwrap_or_else(|e| panic!("push {key}: {e:?}"))
}

/// Push under `rid` with a default [`PushSpec`] at `at_s`. Returns the `EngineResult` so
/// consumers can decide whether to unwrap or assert expected errors.
pub async fn push_rid(
    backend: &HybridBackend,
    rid: &str,
    _key: &str,
    at_s: i64,
) -> Result<Vec<pqueue_core::ItemId>, EngineError> {
    backend
        .push_with_request_id(
            &shard(),
            RequestId::new(rid.to_string()).unwrap(),
            vec![PushSpec::default()],
            ts(at_s),
            None,
        )
        .await
}

// ---------------------------------------------------------------------------
// Drain helpers
// ---------------------------------------------------------------------------

/// Fully drain the deferred projection backlog: repeatedly flush until
/// `deferred_command_count` reaches zero. A no-op on hybrid-strict.
pub fn drain(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend.flush_deferred_projection().expect("flush");
    }
}

// ---------------------------------------------------------------------------
// State-inspection helpers
// ---------------------------------------------------------------------------

/// The SQLite checkpoint high-water sequence — the highest command sequence that has been
/// durably checkpointed into the SQLite projection image. Returns `None` when no checkpoint
/// has ever been written.
pub fn checkpoint_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_projection(|p| ProjectionStore::recovery_high_water(p, &shard()))
        .expect("recovery_high_water")
        .map(|p| p.sequence)
}

/// The durable retention floor sequence — the highest sequence whose segment objects have
/// been reclaimed. Returns `None` on a never-trimmed log.
pub fn floor_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &shard()))
        .expect("retention_floor")
        .map(|p| p.sequence)
}

/// The durable retention floor as a full [`CommandPosition`]. Returns `None` on a
/// never-trimmed log.
pub fn floor_pos(backend: &HybridBackend) -> Option<CommandPosition> {
    backend
        .with_log(|l| LogStore::retention_floor(l, &shard()))
        .expect("retention_floor")
}

/// Count of segment delete operations the log store has performed.
pub fn delete_count(backend: &HybridBackend) -> u64 {
    backend.with_log(|l| l.counters().delete_count)
}

/// Count of segment objects the log store currently tracks.
pub fn object_count(backend: &HybridBackend) -> u64 {
    backend.with_log(|l| l.counters().object_count)
}

/// Count the `.seg` files physically present under `root` (recursive walk). Used as durable
/// evidence that below-floor segment objects were (or were not) actually reclaimed.
pub fn count_seg_files(root: &Path) -> usize {
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

/// Check whether a file with exactly `name` exists anywhere under `root` (recursive walk).
pub fn walk_has_file(root: &Path, name: &str) -> bool {
    let Ok(rd) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if walk_has_file(&path, name) {
                return true;
            }
        } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
            return true;
        }
    }
    false
}

/// Return the current `pending` count from `QueueMetrics`.
pub async fn pending(backend: &HybridBackend) -> u64 {
    backend.metrics(&shard()).await.expect("metrics").pending
}
