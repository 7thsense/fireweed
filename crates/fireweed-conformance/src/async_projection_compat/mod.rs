// Shared async-projection fixtures for behind-image fail-closed and recovery conformance tests.
//
// Product path: [`fireweed_objectlog::AsyncObjectLogMemoryBackend`] (filesystem object log ×
// in-memory projection). Rusqlite hybrid/sqlite projection products are retired.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_core::{QueueDefinition, RequestId};
use fireweed_engine::{
    AsyncProjectionSpec, CommandPosition, EngineError, ProjectionRead, PushPort, PushSpec, QueueKey,
};
use fireweed_objectlog::{AsyncObjectLogMemoryBackend, FlushConfig, flush_config_from_segment};

use super::{qdef, shard as crate_shard, ts};

/// Filesystem object-log × memory projection product used by async-projection fixtures.
pub type LegacySqliteBackend = AsyncObjectLogMemoryBackend;

// ---------------------------------------------------------------------------
// Counter + temp directories
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a unique, isolated temporary directory for a test named `tag`.
pub fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "fireweed-async-projection-{tag}-{}-{n}",
        std::process::id()
    ))
}

// ---------------------------------------------------------------------------
// Shared test shard
// ---------------------------------------------------------------------------

/// The shared single-queue shard key.
pub fn shard() -> QueueKey {
    crate_shard()
}

// ---------------------------------------------------------------------------
// Thresholds / configuration builders
// ---------------------------------------------------------------------------

/// Generous debt thresholds that keep the async-apply backpressure at `Clear`.
pub fn clear_thresholds() -> AsyncProjectionSpec {
    AsyncProjectionSpec::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

/// A queue definition with short request-id and terminal retention (1 hour, in ms).
pub fn qdef_short_retention() -> QueueDefinition {
    let mut d = qdef();
    d.request_id_retention_ms = 3_600_000;
    d.terminal_retention_ms = 3_600_000;
    d.emit_change_records = false;
    d
}

fn flush_one() -> FlushConfig {
    flush_config_from_segment(1, 1)
}

fn open_sync(root: &Path, async_spec: Option<AsyncProjectionSpec>) -> LegacySqliteBackend {
    std::fs::create_dir_all(root).ok();
    let open = match async_spec {
        Some(spec) => AsyncObjectLogMemoryBackend::open_local_with_async_projection(
            root,
            flush_one(),
            0,
            spec,
        ),
        None => AsyncObjectLogMemoryBackend::open_local_with_node_id(root, flush_one(), 0),
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(open)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(open)
        }
    }
    .expect("open AsyncObjectLogMemoryBackend")
}

// ---------------------------------------------------------------------------
// Backend construction
// ---------------------------------------------------------------------------

/// Open the async projection product at `root` with `thresholds`.
pub fn open_async_projection(root: &Path, thresholds: AsyncProjectionSpec) -> LegacySqliteBackend {
    open_sync(root, Some(thresholds))
}

/// Open the strict projection product at `root`.
#[allow(dead_code)]
pub fn open_strict_projection(root: &Path) -> LegacySqliteBackend {
    open_sync(root, None)
}

/// Open the async projection product with a small flush window (LogEngine owns co-buffering).
#[allow(dead_code)]
pub fn open_async_projection_raw(
    root: &Path,
    thresholds: AsyncProjectionSpec,
) -> LegacySqliteBackend {
    open_async_projection(root, thresholds)
}

// ---------------------------------------------------------------------------
// Projection mode enum + dispatch
// ---------------------------------------------------------------------------

/// The two projection substrate modes exercised by conformance tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ProjectionMode {
    /// AsyncProjection: SQLite deferred apply with async-apply debt monitor.
    AsyncProjection,
    /// Strict: SQLite-first synchronous apply (strict ordering).
    Strict,
}

/// Open a backend by projection mode, using [`clear_thresholds`] for the async variant.
#[allow(dead_code)]
pub fn open_mode(root: &Path, mode: ProjectionMode) -> LegacySqliteBackend {
    match mode {
        ProjectionMode::AsyncProjection => open_async_projection(root, clear_thresholds()),
        ProjectionMode::Strict => open_strict_projection(root),
    }
}

// ---------------------------------------------------------------------------
// Push helpers
// ---------------------------------------------------------------------------

/// Push a default [`PushSpec`] under `key` at logical timestamp `at_s`. Panics on error.
pub async fn push(
    backend: &LegacySqliteBackend,
    key: &str,
    at_s: i64,
) -> Vec<fireweed_core::ItemId> {
    backend
        .push(&shard(), vec![PushSpec::default()], ts(at_s), None)
        .await
        .unwrap_or_else(|e| panic!("push {key}: {e:?}"))
}

/// Push under `rid` with a default [`PushSpec`] at `at_s`.
#[allow(dead_code)]
pub async fn push_rid(
    backend: &LegacySqliteBackend,
    rid: &str,
    _key: &str,
    at_s: i64,
) -> Result<Vec<fireweed_core::ItemId>, EngineError> {
    backend
        .push_with_request_id(
            &shard(),
            RequestId::new(rid.to_string()).unwrap(),
            vec![PushSpec::default()],
            ts(at_s),
            None,
        )
        .await
        .map(|outcome| outcome.into_item_ids())
}

// ---------------------------------------------------------------------------
// Drain helpers
// ---------------------------------------------------------------------------

/// Fully drain the deferred projection backlog.
pub fn drain(_backend: &LegacySqliteBackend) {}

// ---------------------------------------------------------------------------
// State-inspection helpers
// ---------------------------------------------------------------------------

/// Projection checkpoint high-water sequence (memory projection has none).
#[allow(dead_code)]
pub fn checkpoint_seq(_backend: &LegacySqliteBackend) -> Option<u64> {
    None
}

/// Retention floor sequence — not yet exposed on LogEngine legacy SQLite product; always `None`.
#[allow(dead_code)]
pub fn floor_seq(_backend: &LegacySqliteBackend) -> Option<u64> {
    None
}

/// Retention floor position — not yet exposed on LogEngine legacy SQLite product; always `None`.
#[allow(dead_code)]
pub fn floor_pos(_backend: &LegacySqliteBackend) -> Option<CommandPosition> {
    None
}

/// Segment delete count — not yet exposed on LogEngine legacy SQLite product; always `0`.
#[allow(dead_code)]
pub fn delete_count(_backend: &LegacySqliteBackend) -> u64 {
    0
}

/// Segment object count — not yet exposed on LogEngine legacy SQLite product; always `0`.
#[allow(dead_code)]
pub fn object_count(_backend: &LegacySqliteBackend) -> u64 {
    0
}

/// Count the `.seg` files physically present under `root` (recursive walk).
#[allow(dead_code)]
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

/// Check whether a file with exactly `name` exists anywhere under `root`.
#[allow(dead_code)]
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
#[allow(dead_code)]
pub async fn pending(backend: &LegacySqliteBackend) -> u64 {
    backend.metrics(&shard()).await.expect("metrics").pending
}
