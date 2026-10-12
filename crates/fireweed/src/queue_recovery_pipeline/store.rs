//! Hidden complete native port. Public factory qualification remains separate.
use super::*;
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    Clock,
    queue_contract::{OwnedQueueValue, QueueFuture, QueueStore},
    queue_memory::{
        administrative_envelope_preflight, administrative_groups_preflight, incoming_headers,
    },
    queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
};
use fireweed_queue_spool::QueueOperationRuntime;
use sha2::{Digest, Sha256};
use std::{future::Future, sync::Mutex};
struct StoreInner {
    pipeline: Mutex<Option<Arc<QueueRecoveryPipeline>>>,
    runtime: Arc<QueueOperationRuntime>,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
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
        Ok(Self {
            inner: Arc::new(StoreInner {
                pipeline: Mutex::new(Some(Arc::new(pipeline))),
                runtime,
                limits,
                clock,
            }),
        })
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
            self.dispatch(bytes, move |pipeline, job| async move {
                let token = fireweed_engine::generate_query_lease_token()
                    .map_err(|e| unknown(&e.to_string()))?;
                let candidate_secret = Sha256::digest(token.as_str().as_bytes()).into();
                let raw = encode_definition(&request.schema)?;
                let definition = AdmittedQueueDefinition::from_raw(&raw, &job)?;
                let namespace = pipeline
                    .store
                    .bind_api007_namespace(&request.queue, &job)
                    .await?;
                let original = pipeline
                    .store
                    .provision_api007_genesis(
                        fireweed_objectlog::Api007GenesisRequest {
                            namespace,
                            definition,
                            candidate_secret,
                            max_record_bytes: pipeline.hard_record_bytes,
                            metadata_bytes: pipeline.metadata_bytes,
                        },
                        &job,
                        &pipeline.workers,
                    )
                    .await?;
                if original.format() != QueueLogFormat::ChecksummedV2 {
                    return Err(unknown("legacy genesis cannot provision native port"));
                }
                if original.definition().schema() != &request.schema {
                    return Err(QueueError::DefinitionConflict);
                }
                Ok(OwnedQueueValue::new((), (original, request, job)))
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
            self.inner.runtime.close_and_drain().await?;
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
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_dispatch_and_cancelled_shutdown_retain_final_ownership() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("fireweed-port-drain-{nonce}"));
        std::fs::create_dir_all(&root).unwrap();
        let log = Arc::new(
            ObjectLogEngineStore::open_local(
                root.join("log"),
                fireweed_objectlog::FlushConfig::default(),
            )
            .await
            .unwrap(),
        );
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let admission = fireweed_engine::queue_admission::QueueAdmission::new(1, 1 << 20).unwrap();
        let init = Arc::new(admission.reserve(0).unwrap());
        let projection = QueueProjection::open(&root.join("projection.db"), &init, &workers)
            .await
            .unwrap();
        drop(init);
        let scratch_root = fireweed_queue_spool::ScratchRoot::open(root.join("scratch"), &workers)
            .await
            .unwrap();
        let pipeline = QueueRecoveryPipeline::new(
            log,
            projection,
            workers,
            QueuePipelineOptions {
                operations: 1,
                metadata_bytes: 65536,
                hard_record_bytes: 1 << 20,
            },
            scratch_root,
            fireweed_queue_spool::ScratchDiskQuota::new(1 << 20).unwrap(),
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
        let drop_release = Arc::new(std::sync::Barrier::new(2));
        let ready = drop_entered.clone();
        let release = drop_release.clone();
        *pipeline.before_drop.lock().unwrap() = Some(Box::new(move || {
            ready.notify_one();
            release.wait();
        }));
        let store = NativeQueueStore::new(
            pipeline,
            QueueLimits::default(),
            1 << 20,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
        )
        .unwrap();
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
        entered.notified().await;
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
        drop_entered.notified().await;
        assert!(!drain_a.is_finished());
        assert!(!drain_b.is_finished());
        drop_release.wait();
        drain_a.await.unwrap().unwrap();
        drain_b.await.unwrap().unwrap();
        assert_eq!(store.inner.runtime.usage().unwrap(), (0, 0));
        assert!(store.inner.pipeline.lock().unwrap().is_none());
        store.shutdown().await.unwrap();
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
