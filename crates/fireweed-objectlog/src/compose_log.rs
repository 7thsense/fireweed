//! Product assembly helpers for object-log × projection cells.
//!
//! Historical module path kept for call-site stability. Product backends are LogEngine async
//! products ([`crate::AsyncObjectLogMemoryBackend`]), not the retired in-tree segmented substrate.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fireweed_engine::{EngineError, EngineResult};
use object_log::{BlobStore, S3BlobStore};

use crate::ObjectLogEngineStore;
use crate::SegmentConfig;
use crate::async_product::AsyncObjectLogMemoryBackend;
use crate::flush_config_from_segment;

/// Product type for composed object-log × in-memory projection (LogEngine async product).
///
/// Historical name kept for call-site stability; implementation is
/// [`crate::AsyncObjectLogMemoryBackend`] (program A).
pub type ComposedObjectLogBackend = AsyncObjectLogMemoryBackend;

/// Drive a LogEngine open future from a sync facade boundary.
///
/// Flavor-safe bridge: never uses [`tokio::task::block_in_place`] (which panics on
/// current-thread runtimes, including every default `#[tokio::test]`). When a Tokio
/// handle of any flavor is already present, the future runs on a dedicated OS thread
/// with a private current-thread runtime. When no runtime is present, a private
/// current-thread runtime is built on the calling thread.
///
/// All sync objectlog open paths (`composed_objectlog_backend`,
/// `open_object_log_engine_*_sync`, and facade `open_objectlog` /
/// `open_objectlog_sqlite` via this helper) inherit this behavior.
pub fn block_on_objectlog<F, T>(fut: F) -> EngineResult<T>
where
    F: Future<Output = EngineResult<T>> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => {
            // Dedicated thread + private runtime: safe under current-thread and multi-thread.
            // `thread::scope` keeps non-'static futures (e.g. open helpers that borrow locals) valid.
            std::thread::scope(|s| {
                s.spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            EngineError::Storage(format!("tokio runtime for objectlog open: {e}"))
                        })?;
                    rt.block_on(fut)
                })
                .join()
                .map_err(|_| EngineError::Storage("objectlog open thread panicked".into()))?
            })
        }
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    EngineError::Storage(format!("tokio runtime for objectlog open: {e}"))
                })?;
            rt.block_on(fut)
        }
    }
}

/// Open [`ObjectLogEngineStore`] on a local filesystem root with namespace-prefixed keys.
pub async fn open_object_log_engine_local(
    root: impl AsRef<Path>,
    namespace: &str,
    target_bytes: usize,
    max_latency_ms: u64,
) -> EngineResult<ObjectLogEngineStore> {
    let flush = flush_config_from_segment(target_bytes, max_latency_ms);
    let ns = sanitize_namespace(namespace);
    let root = root.as_ref().join(&ns);
    ObjectLogEngineStore::open_local(root, flush).await
}

/// Open [`ObjectLogEngineStore`] against an S3-compatible endpoint with namespace-prefixed keys.
#[allow(clippy::too_many_arguments)]
pub async fn open_object_log_engine_s3(
    endpoint: &str,
    region: &str,
    bucket: &str,
    access_key_id: &str,
    secret_access_key: &str,
    namespace: &str,
    target_bytes: usize,
    max_latency_ms: u64,
    allow_insecure_http: bool,
) -> EngineResult<ObjectLogEngineStore> {
    if endpoint.starts_with("http://") && !allow_insecure_http {
        return Err(EngineError::Invalid(
            "insecure S3-compatible HTTP endpoint was not explicitly allowed",
        ));
    }
    let flush = flush_config_from_segment(target_bytes, max_latency_ms);
    let ns = sanitize_namespace(namespace);
    let blob: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
        endpoint,
        region,
        bucket,
        access_key_id,
        secret_access_key,
    ));
    ObjectLogEngineStore::open_with_blob(
        blob,
        format!("{ns}/fwlog/"),
        format!("{ns}/fwmeta/"),
        flush,
    )
    .await
}

/// Sync open of a local LogEngine axis (facade composition root).
pub fn open_object_log_engine_local_sync(
    root: impl AsRef<Path>,
    namespace: &str,
    target_bytes: usize,
    max_latency_ms: u64,
) -> EngineResult<ObjectLogEngineStore> {
    let root = root.as_ref().to_path_buf();
    let namespace = namespace.to_owned();
    block_on_objectlog(open_object_log_engine_local(
        root,
        &namespace,
        target_bytes,
        max_latency_ms,
    ))
}

/// Sync open of an S3 LogEngine axis (facade composition root).
#[allow(clippy::too_many_arguments)]
pub fn open_object_log_engine_s3_sync(
    endpoint: &str,
    region: &str,
    bucket: &str,
    access_key_id: &str,
    secret_access_key: &str,
    namespace: &str,
    target_bytes: usize,
    max_latency_ms: u64,
    allow_insecure_http: bool,
) -> EngineResult<ObjectLogEngineStore> {
    let endpoint = endpoint.to_owned();
    let region = region.to_owned();
    let bucket = bucket.to_owned();
    let access_key_id = access_key_id.to_owned();
    let secret_access_key = secret_access_key.to_owned();
    let namespace = namespace.to_owned();
    block_on_objectlog(open_object_log_engine_s3(
        &endpoint,
        &region,
        &bucket,
        &access_key_id,
        &secret_access_key,
        &namespace,
        target_bytes,
        max_latency_ms,
        allow_insecure_http,
    ))
}

fn sanitize_namespace(namespace: &str) -> String {
    // Hex-encode so blob key prefixes stay path-safe across local FS and S3.
    namespace
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Assemble the product object-log backend rooted at `root` (LogEngine × in-memory projection).
///
/// Prefer the native-async [`crate::composed_objectlog_memory_async`] when already on a Tokio runtime.
/// This sync entry point uses [`block_on_objectlog`] (dedicated thread + private runtime when a
/// Tokio handle is present), so it is safe under current-thread and multi-thread runtimes.
pub fn composed_objectlog_backend(
    root: impl Into<PathBuf>,
) -> EngineResult<ComposedObjectLogBackend> {
    let root = root.into();
    // Default linger matches a modest group-commit latency window.
    open_async_objectlog_product_sync(root, 256 * 1024, 50)
}

/// Assemble the product object-log backend with group-commit flush knobs from [`SegmentConfig`].
/// LogEngine owns co-buffering via [`object_log::FlushConfig`]; no external `flush_tick` is required.
pub fn composed_objectlog_backend_group_commit(
    root: impl Into<PathBuf>,
    config: SegmentConfig,
) -> EngineResult<ComposedObjectLogBackend> {
    let root = root.into();
    open_async_objectlog_product_sync(root, config.target_bytes, config.max_latency_ms)
}

fn open_async_objectlog_product_sync(
    root: PathBuf,
    target_bytes: usize,
    max_latency_ms: u64,
) -> EngineResult<ComposedObjectLogBackend> {
    block_on_objectlog(crate::composed_objectlog_memory_async(
        root,
        target_bytes,
        max_latency_ms,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC: sync objectlog open under default `#[tokio::test]` (current-thread) must not panic.
    #[tokio::test]
    async fn composed_objectlog_backend_opens_under_current_thread_runtime() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-objlog-ct-open-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temp root");
        let backend = composed_objectlog_backend(&root)
            .expect("open objectlog product under current-thread runtime");
        drop(backend);
        let _ = std::fs::remove_dir_all(&root);
    }
}
