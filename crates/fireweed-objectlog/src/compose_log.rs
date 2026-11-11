//! Product assembly helpers for object-log × projection cells.
//!
//! Historical module path kept for call-site stability. Product backends are LogEngine async
//! products ([`crate::AsyncObjectLogMemoryBackend`]), not the retired in-tree segmented substrate.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::ObjectLogEngineStore;
use crate::SegmentConfig;
use crate::async_product::AsyncObjectLogMemoryBackend;
use crate::flush_config_from_segment;
use fireweed_engine::{
    DispatchError, EngineError, EngineResult, OwnedTaskDispatcher, OwnedTaskFactory, TaskOutcome,
    task_outcome_channel,
};

/// Product type for composed object-log × in-memory projection (LogEngine async product).
///
/// Historical name kept for call-site stability; implementation is
/// [`crate::AsyncObjectLogMemoryBackend`] (program A).
pub type ComposedObjectLogBackend = AsyncObjectLogMemoryBackend;

/// Process-wide multi-thread Tokio runtime for object-log open and subsequent I/O.
///
/// LogEngine background work (flush, blob I/O) is runtime-bound. Opening on a throwaway
/// current-thread runtime then dropping it left products with dead background tasks, so later
/// ops failed closed (`Unavailable`). One long-lived multi-thread runtime hosts open + all
/// later `block_on` traffic (including the library BlockingLibBackend workers) and the
/// [`ObjectLogTaskDispatcher`] that owns typed queue operations.
pub(crate) fn objectlog_shared_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("fireweed-objectlog")
            .build()
            .expect("fireweed objectlog shared tokio runtime")
    })
}

/// Owned-task dispatcher that drives factories on [`objectlog_shared_runtime`].
///
/// Replaces [`fireweed_engine::InlineOwnedTaskDispatcher`] for LogEngine products: the inline
/// dispatcher uses `futures::executor::block_on` on a bare OS thread, which has no Tokio reactor
/// and cannot complete `tokio::fs` / LogEngine I/O.
#[derive(Default)]
pub struct ObjectLogTaskDispatcher {
    state: Arc<DispatcherState>,
}
#[derive(Default)]
struct DispatcherState {
    admission: Mutex<(bool, usize)>,
    changed: tokio::sync::Notify,
}
struct AcceptedTask(Arc<DispatcherState>);
impl Drop for AcceptedTask {
    fn drop(&mut self) {
        self.0
            .admission
            .lock()
            .expect("object-log dispatcher poisoned")
            .1 -= 1;
        self.0.changed.notify_waiters();
    }
}
impl ObjectLogTaskDispatcher {
    pub fn new() -> Self {
        Self::default()
    }
}
impl OwnedTaskDispatcher for ObjectLogTaskDispatcher {
    fn submit<T: Send + 'static>(
        &self,
        factory: OwnedTaskFactory<T>,
    ) -> Result<TaskOutcome<T>, DispatchError> {
        let mut state = self
            .state
            .admission
            .lock()
            .expect("object-log dispatcher poisoned");
        if state.0 {
            return Err(DispatchError::Closed);
        }
        state.1 += 1;
        let accepted = AcceptedTask(self.state.clone());
        drop(state);
        let (sender, outcome) = task_outcome_channel();
        objectlog_shared_runtime().spawn(async move {
            let _accepted = accepted;
            sender.send(factory().await);
        });
        Ok(outcome)
    }
    fn close(&self) {
        self.state
            .admission
            .lock()
            .expect("object-log dispatcher poisoned")
            .0 = true;
    }
    fn is_closed(&self) -> bool {
        self.state
            .admission
            .lock()
            .expect("object-log dispatcher poisoned")
            .0
    }
    fn drain(&self) -> TaskOutcome<()> {
        let (sender, outcome) = task_outcome_channel();
        let state = self.state.clone();
        objectlog_shared_runtime().spawn(async move {
            loop {
                let changed = state.changed.notified();
                if state
                    .admission
                    .lock()
                    .expect("object-log dispatcher poisoned")
                    .1
                    == 0
                {
                    break;
                }
                changed.await;
            }
            sender.send(());
        });
        outcome
    }
}

#[cfg(test)]
mod dispatcher_drain_tests {
    use super::*;
    #[tokio::test]
    async fn drain_waits_for_accepted_work_even_when_caller_drops() {
        let dispatcher = ObjectLogTaskDispatcher::new();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let outcome = dispatcher
            .submit(Box::new(move || {
                Box::pin(async move {
                    entered.send(()).unwrap();
                    released.await.unwrap();
                })
            }))
            .unwrap();
        started.await.unwrap();
        drop(outcome);
        dispatcher.close();
        assert!(dispatcher.submit(Box::new(|| Box::pin(async {}))).is_err());
        let mut drain = Box::pin(dispatcher.drain());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .unwrap()
            .unwrap();
    }
}

/// Drive a future on the process-wide object-log runtime (open + durable ops).
///
/// Flavor-safe: never uses [`tokio::task::block_in_place`]. When a Tokio handle is already
/// present (including default `#[tokio::test]` current-thread), the future is run on a
/// dedicated OS thread calling into the shared multi-thread runtime so we never nest
/// `block_on` on the caller's runtime. When no handle is present, the shared runtime is
/// used directly from the calling thread.
///
/// All sync objectlog open paths (`composed_objectlog_backend`,
/// and `open_object_log_engine_*_sync`)
/// inherit this behavior. Library I/O workers
/// should also drive LogEngine futures through [`block_on_objectlog_future`].
pub fn block_on_objectlog<F, T>(fut: F) -> EngineResult<T>
where
    F: Future<Output = EngineResult<T>> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => std::thread::scope(|s| {
            s.spawn(move || objectlog_shared_runtime().block_on(fut))
                .join()
                .map_err(|_| EngineError::Storage("objectlog open thread panicked".into()))?
        }),
        Err(_) => objectlog_shared_runtime().block_on(fut),
    }
}

/// Drive any `Send` future on the process-wide object-log runtime.
///
/// Used by the library BlockingLibBackend so open and ops share one reactor and LogEngine
/// background tasks stay alive for the process lifetime.
pub fn block_on_objectlog_future<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => {
            // Dedicated thread keeps non-'static borrows valid via thread::scope and avoids
            // nesting block_on on the caller's (possibly current-thread) runtime.
            std::thread::scope(|s| {
                s.spawn(move || objectlog_shared_runtime().block_on(fut))
                    .join()
                    .unwrap_or_else(|_| {
                        panic!("fireweed objectlog runtime thread panicked");
                    })
            })
        }
        Err(_) => objectlog_shared_runtime().block_on(fut),
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
    ObjectLogEngineStore::open_s3_with_prefixes(
        endpoint,
        region,
        bucket,
        access_key_id,
        secret_access_key,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_local_memory_product_empty_root() {
        let root = std::env::temp_dir().join(format!(
            "fw-open-local-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let flush = object_log::FlushConfig {
            linger: std::time::Duration::ZERO,
            ..object_log::FlushConfig::default()
        };
        let b = crate::AsyncObjectLogMemoryBackend::open_local(&root, flush)
            .await
            .expect("open_local empty");
        drop(b);
        let _ = std::fs::remove_dir_all(&root);
    }
    #[tokio::test]
    async fn generic_bridge_remains_safe_inside_owned_runtime_future() {
        let workers = fireweed_queue_spool::ScratchWorkers::new(1, 1).unwrap();
        let target = objectlog_shared_runtime().handle().clone();
        let expected = target.id();
        let result = workers
            .run_on_runtime(target, move || async move {
                tokio::task::yield_now().await;
                let physical = std::thread::current().id();
                let output = block_on_objectlog_future(async move {
                    assert_ne!(std::thread::current().id(), physical);
                    assert_eq!(tokio::runtime::Handle::current().id(), expected);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    17
                });
                assert_eq!(std::thread::current().id(), physical);
                Ok(output)
            })
            .await
            .unwrap();
        assert_eq!(result, 17);
        workers.close_and_drain().await.unwrap();
    }
}
