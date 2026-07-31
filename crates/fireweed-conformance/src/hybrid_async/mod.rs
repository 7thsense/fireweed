// Shared hybrid-async test fixtures for behind-image fail-closed and recovery conformance tests.
//
// Product path: [`fireweed_objectlog::AsyncObjectLogHybridBackend`] (LogEngine × hybrid projection).
// The retired dual-stack `ComposedBackend<ObjectLog, HybridProjectionStore, …>` surface is gone.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_core::{QueueDefinition, RequestId};
use fireweed_engine::{
    CommandPosition, EngineError, ProjectionRead, ProjectionStore, PushPort, PushSpec, QueueKey,
};
use fireweed_objectlog::{
    AsyncObjectLogHybridBackend, FlushConfig, HybridProductConfig, flush_config_from_segment,
};
use fireweed_sqlite::HybridAsyncThresholds;

use super::{qdef, shard as crate_shard, ts};

// ---------------------------------------------------------------------------
// Type alias
// ---------------------------------------------------------------------------

/// The hybrid product type for objectlog/hybrid-async and objectlog/hybrid-strict tests.
pub type HybridBackend = AsyncObjectLogHybridBackend;

// ---------------------------------------------------------------------------
// Counter + temp directories
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a unique, isolated temporary directory for a test named `tag`.
pub fn base_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "fireweed-hybrid-async-{tag}-{}-{n}",
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
pub fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
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

fn open_sync(
    root: &Path,
    hybrid: HybridProductConfig,
) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let path = sqlite.to_str().expect("utf8 projection path");
    let open = AsyncObjectLogHybridBackend::open(root, path, flush_one(), 0, hybrid);
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
    .expect("open AsyncObjectLogHybridBackend")
}

// ---------------------------------------------------------------------------
// Backend construction
// ---------------------------------------------------------------------------

/// Open the hybrid-async product at `root` with `thresholds`.
pub fn open_hybrid(root: &Path, thresholds: HybridAsyncThresholds) -> HybridBackend {
    open_sync(
        root,
        HybridProductConfig {
            deferred_flush_chunk: 1,
            strict: false,
            async_monitor: Some(thresholds),
        },
    )
}

/// Open the hybrid-strict product at `root`.
#[allow(dead_code)]
pub fn open_hybrid_strict(root: &Path) -> HybridBackend {
    open_sync(
        root,
        HybridProductConfig {
            deferred_flush_chunk: 1,
            strict: true,
            async_monitor: None,
        },
    )
}

/// Open the hybrid-async product with a small flush window (LogEngine owns co-buffering).
#[allow(dead_code)]
pub fn open_hybrid_raw(root: &Path, thresholds: HybridAsyncThresholds) -> HybridBackend {
    // LogEngine products always use FlushConfig; "raw" maps to the same open with unit flush knobs.
    open_hybrid(root, thresholds)
}

// ---------------------------------------------------------------------------
// Projection mode enum + dispatch
// ---------------------------------------------------------------------------

/// The two projection substrate modes exercised by conformance tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ProjectionMode {
    /// Hybrid-async: SQLite deferred apply with async-apply debt monitor.
    HybridAsync,
    /// Hybrid-strict: SQLite-first synchronous apply (strict ordering).
    HybridStrict,
}

/// Open a backend by projection mode, using [`clear_thresholds`] for the async variant.
#[allow(dead_code)]
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
pub async fn push(backend: &HybridBackend, key: &str, at_s: i64) -> Vec<fireweed_core::ItemId> {
    backend
        .push(&shard(), vec![PushSpec::default()], ts(at_s), None)
        .await
        .unwrap_or_else(|e| panic!("push {key}: {e:?}"))
}

/// Push under `rid` with a default [`PushSpec`] at `at_s`.
#[allow(dead_code)]
pub async fn push_rid(
    backend: &HybridBackend,
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
pub fn drain(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend
            .try_flush_deferred_projection()
            .expect("flush deferred projection");
    }
}

// ---------------------------------------------------------------------------
// State-inspection helpers
// ---------------------------------------------------------------------------

/// The SQLite checkpoint high-water sequence.
#[allow(dead_code)]
pub fn checkpoint_seq(backend: &HybridBackend) -> Option<u64> {
    backend
        .with_projection(|p| ProjectionStore::recovery_high_water(p, &shard()))
        .expect("recovery_high_water")
        .map(|p| p.sequence)
}

/// Retention floor sequence — not yet exposed on LogEngine hybrid product; always `None`.
#[allow(dead_code)]
pub fn floor_seq(_backend: &HybridBackend) -> Option<u64> {
    None
}

/// Retention floor position — not yet exposed on LogEngine hybrid product; always `None`.
#[allow(dead_code)]
pub fn floor_pos(_backend: &HybridBackend) -> Option<CommandPosition> {
    None
}

/// Segment delete count — not yet exposed on LogEngine hybrid product; always `0`.
#[allow(dead_code)]
pub fn delete_count(_backend: &HybridBackend) -> u64 {
    0
}

/// Segment object count — not yet exposed on LogEngine hybrid product; always `0`.
#[allow(dead_code)]
pub fn object_count(_backend: &HybridBackend) -> u64 {
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
pub async fn pending(backend: &HybridBackend) -> u64 {
    backend.metrics(&shard()).await.expect("metrics").pending
}
