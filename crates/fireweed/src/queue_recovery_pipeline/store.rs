//! Hidden complete native port. Public factory qualification remains separate.
use super::*;
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    Clock,
    queue_contract::{OwnedQueueValue, QueueFuture, QueueStore},
    queue_memory::{
        administrative_envelope_preflight, administrative_groups_preflight, incoming_headers,
    },
};
use fireweed_queue_spool::QueueOperationRuntime;
use std::{future::Future, sync::Mutex};
struct StoreInner {
    pipeline: Mutex<Option<Arc<QueueRecoveryPipeline>>>,
    runtime: Arc<QueueOperationRuntime>,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
    diagnostics: Arc<RecoveryCounters>,
    /// Held for the whole of a shutdown. A second concurrent shutdown holds a
    /// pipeline clone while it drains, so without this gate the first caller
    /// could take the pipeline, find it not yet the last owner, and return
    /// before the other caller's drop released the final resources.
    shutdown_gate: tokio::sync::Mutex<()>,
}
#[derive(Clone)]
pub struct NativeQueueStore {
    inner: Arc<StoreInner>,
}
struct DispatchTask<F> {
    operation: Option<F>,
    pipeline: Arc<QueueRecoveryPipeline>,
    job: Arc<QueueReservation>,
}
impl<F> DispatchTask<F> {
    async fn execute<T, O: Send, Fut>(mut self) -> QueueResult<OwnedQueueValue<T, O>>
    where
        F: FnOnce(Arc<QueueRecoveryPipeline>, Arc<QueueReservation>) -> Fut,
        Fut: Future<Output = QueueResult<OwnedQueueValue<T, O>>>,
    {
        let operation = self.operation.take().expect("one owned dispatch");
        let result = operation(self.pipeline.clone(), self.job.clone()).await;
        drop(self);
        result
    }
}
impl NativeQueueStore {
    /// Takes exclusive top-level scheduling ownership. Underlying resources must
    /// be quiescent and transferred by the internal factory; external aliases
    /// can retain SDK handles beyond shutdown and are not qualified here.
    pub fn new(
        pipeline: Arc<QueueRecoveryPipeline>,
        limits: QueueLimits,
        retained_bytes: u64,
        clock: Arc<dyn Clock>,
    ) -> QueueResult<Self> {
        limits.validate()?;
        let pipeline = Arc::try_unwrap(pipeline)
            .map_err(|_| QueueError::Invalid("native pipeline is already shared".into()))?;
        let runtime = QueueOperationRuntime::new(
            pipeline.operation_slots.available_permits(),
            retained_bytes,
        )?;
        let diagnostics = pipeline.diagnostics.clone();
        Ok(Self {
            inner: Arc::new(StoreInner {
                pipeline: Mutex::new(Some(Arc::new(pipeline))),
                runtime,
                limits,
                clock,
                diagnostics,
                shutdown_gate: tokio::sync::Mutex::new(()),
            }),
        })
    }
    /// Counters only: retaining this handle never retains store resources.
    pub fn recovery_diagnostics(&self) -> Arc<RecoveryCounters> {
        self.inner.diagnostics.clone()
    }
    async fn dispatch<T: Send + 'static, O: Send + 'static, F, Fut>(
        &self,
        bytes: u64,
        operation: F,
    ) -> QueueResult<T>
    where
        F: FnOnce(Arc<QueueRecoveryPipeline>, Arc<QueueReservation>) -> Fut + Send + 'static,
        Fut: Future<Output = QueueResult<OwnedQueueValue<T, O>>> + Send + 'static,
    {
        let job = Arc::new(self.inner.runtime.reserve(bytes)?);
        let pipeline = self
            .inner
            .pipeline
            .lock()
            .map_err(|_| unknown("store ownership poisoned"))?
            .as_ref()
            .cloned()
            .ok_or(QueueError::Busy)?;
        let task = DispatchTask {
            operation: Some(operation),
            pipeline,
            job,
        };
        let result = tokio::spawn(task.execute())
            .await
            .map_err(|_| unknown("owned store task failed"))??;
        Ok(result.into_value())
    }
    fn input(&self, encoded: u64, headers: u64) -> QueueResult<u64> {
        encoded
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers.checked_mul(2)?))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)
    }
}
macro_rules! operation {
    ($method:ident,$request:ty,$output:ty,$native:ident,$clock:tt) => {
        fn $method(&self, request:$request)->QueueFuture<'_,$output> {
            Box::pin(async move {
                request.preflight(&self.inner.limits)?;
                let bytes=self.input(encoded_len(&request)?, incoming_headers(&request)?)?;
                let limits=self.inner.limits;
                let clock=self.inner.clock.clone();
                self.dispatch(bytes,move|pipeline,job| async move {
                    operation!(@call pipeline,request,limits,clock,job,$native,$clock).await.map(|response|response.into_owned())
                }).await
            })
        }
    };
    (@call $p:ident,$r:ident,$l:ident,$c:ident,$j:ident,$n:ident,true)=>{$p.$n($r,$l,$c,$j)};
    (@call $p:ident,$r:ident,$l:ident,$c:ident,$j:ident,$n:ident,false)=>{{drop($c);$p.$n($r,$l,$j)}};
}
impl QueueStore for NativeQueueStore {
    fn provision(&self, request: ProvisionRequest) -> QueueFuture<'_, ()> {
        Box::pin(async move {
            request.validate(&self.inner.limits)?;
            let bytes = self.input(encoded_len(&request)?, incoming_headers(&request)?)?;
            let limits = self.inner.limits;
            self.dispatch(bytes, move |pipeline, job| async move {
                let charge = job.owned_buffer(bytes)?;
                pipeline
                    .provision(request.clone(), limits, job.clone(), charge)
                    .await?;
                Ok(OwnedQueueValue::new((), (request, job)))
            })
            .await
        })
    }
    operation!(insert, InsertRequest, Vec<InsertOutcome>, insert, true);
    operation!(claim, ClaimRequest, Vec<ClaimedWork>, claim, true);
    fn commit(&self, request: CommitBatch) -> QueueFuture<'_, Vec<CommitReceipt>> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            administrative_envelope_preflight(&request, &self.inner.limits)?;
            let bytes = self.input(encoded_len(&request)?, incoming_headers(&request)?)?;
            let limits = self.inner.limits;
            let clock = self.inner.clock.clone();
            self.dispatch(bytes, move |pipeline, job| async move {
                pipeline
                    .commit_batch(request, limits, clock, job)
                    .await
                    .map(|response| response.into_owned())
            })
            .await
        })
    }
    fn preview(&self, request: PreviewRequest) -> QueueFuture<'_, PreviewResult> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            administrative_groups_preflight(
                &request.queue,
                std::slice::from_ref(&request.group),
                &self.inner.limits,
            )?;
            let bytes = self.input(encoded_len(&request)?, incoming_headers(&request)?)?;
            let limits = self.inner.limits;
            let clock = self.inner.clock.clone();
            self.dispatch(bytes, move |pipeline, job| async move {
                pipeline
                    .preview(request, limits, clock, job)
                    .await
                    .map(|response| response.into_owned())
            })
            .await
        })
    }
    operation!(stats, StatsRequest, QueueStats, stats, true);
    operation!(
        read_state,
        StateReadRequest,
        Vec<Option<StateRecord>>,
        read_state,
        false
    );
    operation!(query, QueryRequest, QueryPage, query, true);
    fn read_commit(&self, request: ReceiptReadRequest) -> QueueFuture<'_, Vec<ReceiptLookup>> {
        Box::pin(async move {
            request.validate(&self.inner.limits)?;
            let bytes = self.input(encoded_len(&request)?, incoming_headers(&request)?)?;
            let limits = self.inner.limits;
            self.dispatch(bytes, move |pipeline, job| async move {
                pipeline
                    .read_commit(request, limits, job)
                    .await
                    .map(|response| response.into_owned())
            })
            .await
        })
    }
    fn shutdown(&self) -> QueueFuture<'_, ()> {
        Box::pin(async move {
            let _one_shutdown_at_a_time = self.inner.shutdown_gate.lock().await;
            self.inner.runtime.close_and_drain().await?;
            let queues = self
                .inner
                .pipeline
                .lock()
                .map_err(|_| unknown("store ownership poisoned"))?
                .as_ref()
                .cloned();
            if let Some(queues) = queues {
                queues.close_queues().await;
            }
            let workers = self
                .inner
                .pipeline
                .lock()
                .map_err(|_| unknown("store ownership poisoned"))?
                .as_ref()
                .map(|p| p.workers.clone());
            if let Some(workers) = workers {
                workers.close_and_drain().await?;
            }
            let mut ownership = self
                .inner
                .pipeline
                .lock()
                .map_err(|_| unknown("store ownership poisoned"))?;
            let pipeline = ownership.take();
            // Serialize completion through the final resource drop as well as
            // the removal, so concurrent shutdown cannot return early.
            drop(pipeline);
            drop(ownership);
            Ok(())
        })
    }
}

#[cfg(test)]
impl NativeQueueStore {
    pub(super) fn pipeline_for_test(&self) -> Arc<QueueRecoveryPipeline> {
        self.inner
            .pipeline
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .expect("open store")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_objectlog::queue_native_log::QueueNativeLogOptions;
    /// Bounds every wait so a lost wakeup fails the test instead of consuming
    /// the CI job.
    async fn within<T>(what: &str, wait: impl Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(60), wait)
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_dispatch_and_cancelled_shutdown_retain_final_ownership() {
        abandoned_dispatch_and_cancelled_shutdown().await;
    }
    /// Two shutdowns race while the first pipeline owner is mid-drop; neither
    /// may return before that drop completes. The race window is a few
    /// instructions wide, so repeat the scenario.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_shutdowns_wait_for_final_drop_repeatedly() {
        for _ in 0..100 {
            abandoned_dispatch_and_cancelled_shutdown().await;
        }
    }
    async fn abandoned_dispatch_and_cancelled_shutdown() {
        static SCENARIO: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = SCENARIO.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "fireweed-port-drain-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let log = Arc::new(
            QueueNativeLog::local(
                &root.join("log"),
                "native".to_owned(),
                QueueNativeLogOptions::for_queue_limits(&QueueLimits::default(), 8, 1, 4096, None)
                    .unwrap(),
            )
            .unwrap(),
        );
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let admission = fireweed_engine::queue_admission::QueueAdmission::new(1, 1 << 20).unwrap();
        let init = Arc::new(admission.reserve(0).unwrap());
        drop(init);
        let scratch_root = fireweed_queue_spool::ScratchRoot::open(root.join("scratch"), &workers)
            .await
            .unwrap();
        let pipeline = QueueRecoveryPipeline::new(
            log,
            root.join("projections"),
            workers,
            QueuePipelineOptions {
                operations: 1,
                hard_record_bytes: 1 << 20,
            },
            scratch_root,
            fireweed_queue_spool::ScratchDiskQuota::new(1 << 20).unwrap(),
            super::super::tests::ownership::TestOwnership::new().handle("only"),
        )
        .unwrap();
        assert!(matches!(
            NativeQueueStore::new(
                pipeline.clone(),
                QueueLimits::default(),
                1 << 20,
                Arc::new(fireweed_conformance::queue_contract::TestClock::default())
            ),
            Err(QueueError::Invalid(_))
        ));
        let drop_entered = Arc::new(tokio::sync::Notify::new());
        // A channel, not a barrier: if an assertion fails before the send, the
        // sender drops and releases the blocked hook, so the runtime can shut
        // down and the test reports the failure instead of hanging.
        let (drop_release, hook_release) = std::sync::mpsc::channel::<()>();
        let ready = drop_entered.clone();
        *pipeline.before_drop.lock().unwrap() = Some(Box::new(move || {
            ready.notify_one();
            let _ = hook_release.recv();
        }));
        let store = NativeQueueStore::new(
            pipeline,
            QueueLimits::default(),
            1 << 20,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
        )
        .unwrap();
        let diagnostic_handle = store.recovery_diagnostics();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let caller = store.clone();
        let ready = entered.clone();
        let unblock = release.clone();
        let task = tokio::spawn(async move {
            caller
                .dispatch(7, move |_pipeline, job| async move {
                    let charge = job.owned_buffer(11)?;
                    ready.notify_one();
                    unblock.notified().await;
                    Ok(OwnedQueueValue::new(vec![1u8; 11], (charge, job)))
                })
                .await
        });
        within("dispatch entry", entered.notified()).await;
        task.abort();
        let _ = task.await;
        assert_eq!(store.inner.runtime.usage().unwrap(), (1, 18));
        let mut first = Box::pin(store.shutdown());
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        assert!(matches!(
            first.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(first);
        assert!(matches!(
            store.inner.runtime.reserve(0),
            Err(QueueError::Busy)
        ));
        let a = store.clone();
        let b = store.clone();
        let drain_a = tokio::spawn(async move { a.shutdown().await });
        let drain_b = tokio::spawn(async move { b.shutdown().await });
        release.notify_one();
        within("final drop entry", drop_entered.notified()).await;
        assert!(!drain_a.is_finished());
        assert!(!drain_b.is_finished());
        drop_release.send(()).unwrap();
        within("first shutdown", drain_a).await.unwrap().unwrap();
        within("second shutdown", drain_b).await.unwrap().unwrap();
        assert_eq!(store.inner.runtime.usage().unwrap(), (0, 0));
        assert!(store.inner.pipeline.lock().unwrap().is_none());
        store.shutdown().await.unwrap();
        drop(store);
        assert_eq!(Arc::strong_count(&diagnostic_handle), 1);
        assert_eq!(diagnostic_handle.snapshot().attempts, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
