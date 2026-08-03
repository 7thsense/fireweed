//! Owned execution boundary for the synchronous durable library backends.
//!
//! Engine ports are async-shaped, but the composed SQLite, object-log, and
//! PostgreSQL implementations perform synchronous durable work when their
//! futures are polled. This adapter transfers every complete port operation to
//! a fixed set of owned OS workers before polling it. Queue affinity preserves
//! queue-local order; bounded per-worker channels provide finite admission;
//! accepted jobs survive caller cancellation. Production handles share one
//! process-wide bounded pool, so dropping a handle never joins durable-I/O
//! threads on an async runtime worker or multiplies threads per queue/backend.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::JoinHandle;

use bytes::Bytes;
use fireweed_core::*;
use fireweed_engine::*;

const DEFAULT_WORKERS: usize = 8;
const DEFAULT_PENDING_PER_WORKER: usize = 64;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Drain jobs on a dedicated OS worker.
///
/// Each job is catch_unwind'd so a single panic cannot permanently disconnect the shared pool.
/// Async work is driven via [`block_on_worker`] (Tokio reactor when `objectlog` is enabled).
fn run_blocking_worker(receiver: mpsc::Receiver<Job>) {
    while let Ok(job) = receiver.recv() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}

/// Drive a complete port future on a library I/O worker thread.
///
/// Object-log products use Tokio-backed I/O (`tokio::fs`, LogEngine). `futures::executor::block_on`
/// does **not** drive the Tokio reactor, so opens/ops hang or fail closed. With the `objectlog`
/// feature each worker thread owns a private current-thread runtime and uses it to `block_on`.
fn block_on_worker<T>(future: impl Future<Output = T> + Send) -> T
where
    T: Send,
{
    #[cfg(feature = "objectlog")]
    {
        // Same process-wide multi-thread runtime used to open LogEngine products so background
        // flush/I/O tasks and subsequent ops share one reactor for the process lifetime.
        fireweed_objectlog::block_on_objectlog_future(future)
    }
    #[cfg(not(feature = "objectlog"))]
    {
        futures::executor::block_on(future)
    }
}

struct WorkerPool {
    senders: Mutex<Option<WorkerSenders>>,
    _workers: Mutex<Option<Vec<JoinHandle<()>>>>,
}

struct WorkerSenders {
    data: Vec<mpsc::SyncSender<Job>>,
}

/// Cloneable handle for non-port lifecycle operations that must use the same
/// owned blocking boundary as queue operations.
#[cfg(all(feature = "objectlog", any(feature = "postgres", feature = "sqlite")))]
#[derive(Clone)]
pub(crate) struct OwnedBlockingExecutor {
    pool: Arc<WorkerPool>,
}

#[cfg(all(feature = "objectlog", any(feature = "postgres", feature = "sqlite")))]
impl OwnedBlockingExecutor {
    pub(crate) fn run<T, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + use<T, F>
    where
        T: Send + 'static,
        F: FnOnce() -> EngineResult<T> + Send + 'static,
    {
        self.pool.submit_data(0, operation)
    }
}

impl WorkerPool {
    fn new(worker_count: usize, pending_per_worker: usize) -> EngineResult<Self> {
        if worker_count == 0 || pending_per_worker == 0 {
            return Err(EngineError::Invalid(
                "blocking worker bounds must be non-zero",
            ));
        }
        let mut data_senders = Vec::with_capacity(worker_count);
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let (sender, receiver) = mpsc::sync_channel::<Job>(pending_per_worker);
            let worker = match std::thread::Builder::new()
                .name(format!("fireweed-library-io-{index}"))
                .spawn(move || run_blocking_worker(receiver))
            {
                Ok(worker) => worker,
                Err(error) => {
                    drop(data_senders);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(EngineError::Storage(error.to_string()));
                }
            };
            data_senders.push(sender);
            workers.push(worker);
        }
        Ok(Self {
            senders: Mutex::new(Some(WorkerSenders { data: data_senders })),
            _workers: Mutex::new(Some(workers)),
        })
    }

    fn submit_data<T, F>(
        &self,
        worker: usize,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + use<T, F>
    where
        T: Send + 'static,
        F: FnOnce() -> EngineResult<T> + Send + 'static,
    {
        let sender = self
            .senders
            .lock()
            .expect("blocking worker senders poisoned")
            .as_ref()
            .ok_or(EngineError::Unavailable)
            .map(|senders| senders.data[worker % senders.data.len()].clone());
        Self::submit_to(sender, operation)
    }

    async fn submit_to<T, F>(
        sender: EngineResult<mpsc::SyncSender<Job>>,
        operation: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> EngineResult<T> + Send + 'static,
    {
        let (result_tx, result_rx) = futures::channel::oneshot::channel();
        let job: Job = Box::new(move || {
            let _ = result_tx.send(operation());
        });
        sender?.try_send(job).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => EngineError::Backpressure {
                resource: "composed durable operations",
            },
            mpsc::TrySendError::Disconnected(_) => EngineError::Unavailable,
        })?;
        result_rx
            .await
            .map_err(|_| EngineError::Storage("composed durable operation worker stopped".into()))?
    }

    fn worker_count(&self) -> usize {
        self.senders
            .lock()
            .expect("blocking worker senders poisoned")
            .as_ref()
            .map_or(1, |senders| senders.data.len())
    }

    #[cfg(test)]
    fn shutdown(&self) {
        self.senders
            .lock()
            .expect("blocking worker senders poisoned")
            .take();
        if let Some(workers) = self
            ._workers
            .lock()
            .expect("blocking workers poisoned")
            .take()
        {
            for worker in workers {
                let _ = worker.join();
            }
        }
    }
}

fn shared_worker_pool() -> EngineResult<Arc<WorkerPool>> {
    static POOL: OnceLock<Result<Arc<WorkerPool>, String>> = OnceLock::new();
    match POOL.get_or_init(|| {
        WorkerPool::new(DEFAULT_WORKERS, DEFAULT_PENDING_PER_WORKER)
            .map(Arc::new)
            .map_err(|error| error.to_string())
    }) {
        Ok(pool) => Ok(Arc::clone(pool)),
        Err(error) => Err(EngineError::Storage(error.clone())),
    }
}

/// Executor over the process-wide library I/O pool **without** wrapping a product backend.
///
/// Used for projection lifecycle / control-plane offload on compositions whose ports already
/// run natively async (object-log LogEngine products). Prefer this over
/// [`BlockingLibBackend`] when the open path must not install the process-wide port bridge
/// (fireweed-8a023735 / API-005).
#[cfg(any(
    all(feature = "objectlog", feature = "postgres"),
    all(feature = "objectlog", feature = "sqlite")
))]
pub(crate) fn shared_executor() -> EngineResult<OwnedBlockingExecutor> {
    Ok(OwnedBlockingExecutor {
        pool: shared_worker_pool()?,
    })
}

/// Complete, bounded blocking boundary for the library's full backend surface.
pub(crate) struct BlockingLibBackend<B: super::LibBackend + 'static> {
    inner: Option<Arc<B>>,
    pool: Arc<WorkerPool>,
}

impl<B: super::LibBackend + 'static> BlockingLibBackend<B> {
    pub(crate) fn new(inner: Arc<B>) -> EngineResult<Self> {
        Ok(Self {
            inner: Some(inner),
            pool: shared_worker_pool()?,
        })
    }

    fn worker(queue: &QueueKey, workers: usize) -> usize {
        queue_worker_partition(queue, workers)
    }

    fn dispatch<T, Fut, F>(
        &self,
        queue: QueueKey,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + use<T, Fut, F, B>
    where
        T: Send + 'static,
        Fut: Future<Output = EngineResult<T>> + Send + 'static,
        F: FnOnce(Arc<B>) -> Fut + Send + 'static,
    {
        let inner = Arc::clone(self.inner.as_ref().expect("blocking backend is active"));
        let pool = Arc::clone(&self.pool);
        let worker = Self::worker(&queue, pool.worker_count());
        pool.submit_data(worker, move || block_on_worker(operation(inner)))
    }

    fn global_queue(seed: impl Into<String>) -> QueueKey {
        QueueKey::new(
            TenantId::new("fireweed-internal").expect("valid tenant"),
            QueueId::new(seed).expect("valid queue"),
        )
    }
}

impl<B: super::LibBackend + 'static> Drop for BlockingLibBackend<B> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let job: Job = Box::new(move || drop(inner));
        let sender = self
            .pool
            .senders
            .lock()
            .expect("blocking worker senders poisoned")
            .as_ref()
            .map(|senders| senders.data[0].clone());
        let Some(sender) = sender else {
            let _ = std::thread::Builder::new()
                .name("fireweed-library-drop".into())
                .spawn(job);
            return;
        };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(job) | mpsc::TrySendError::Disconnected(job)) => {
                let _ = std::thread::Builder::new()
                    .name("fireweed-library-drop".into())
                    .spawn(job);
            }
        }
    }
}

impl<B: super::LibBackend + 'static> Backend for BlockingLibBackend<B> {
    fn durability_class(&self) -> DurabilityClass {
        self.inner
            .as_ref()
            .expect("blocking backend is active")
            .durability_class()
    }
    fn supports_gates(&self) -> bool {
        self.inner
            .as_ref()
            .expect("blocking backend is active")
            .supports_gates()
    }
    fn commit_capabilities(&self) -> CommitCapabilities {
        self.inner
            .as_ref()
            .expect("blocking backend is active")
            .commit_capabilities()
    }
    fn commit_raw(
        &self,
        request: RawCommitRequest,
    ) -> impl Future<Output = EngineResult<RawCommitOutcome>> + Send {
        let queue = request.shard().clone();
        self.dispatch(queue, move |inner| async move {
            inner.commit_raw(request).await
        })
    }
}

impl<B: super::LibBackend + 'static> PushPort for BlockingLibBackend<B> {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let queue = shard.clone();
        self.dispatch(queue.clone(), move |inner| async move {
            inner.push(&queue, items, now, expected_epoch).await
        })
    }
    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<fireweed_engine::PushBatchOutcome>> + Send {
        let queue = shard.clone();
        self.dispatch(queue.clone(), move |inner| async move {
            inner
                .push_with_request_id(&queue, request_id, items, now, expected_epoch)
                .await
        })
    }
}

impl<B: super::LibBackend + 'static> ClaimPort for BlockingLibBackend<B> {
    fn claim(&self, req: ClaimRequest) -> impl Future<Output = EngineResult<Claimed>> + Send {
        self.dispatch(req.shard.clone(), move |inner| async move {
            inner.claim(req).await
        })
    }
}

impl<B: super::LibBackend + 'static> UpsertPort for BlockingLibBackend<B> {
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: std::collections::BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<UpsertOutcome>> + Send {
        let queue = shard.clone();
        let key = client_item_key.clone();
        self.dispatch(queue.clone(), move |inner| async move {
            inner
                .replace_if_pending(
                    &queue,
                    &key,
                    priority,
                    group_key,
                    not_before,
                    payload,
                    fields,
                    metadata,
                    entity,
                    now,
                    expected_epoch,
                )
                .await
        })
    }
}

impl<B: super::LibBackend + 'static> UpdateFieldsPort for BlockingLibBackend<B> {
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: std::collections::BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.update_fields(
                &q,
                item_id,
                field_ops,
                payload,
                entity,
                expected_item_version,
                now,
                expected_epoch,
            )
            .await
        })
    }
}
impl<B: super::LibBackend + BatchUpdatePort + 'static> BatchUpdatePort for BlockingLibBackend<B> {
    fn batch_update(
        &self,
        shard: &QueueKey,
        request: BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<BatchUpdateResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.batch_update(&q, request, now, expected_epoch).await
        })
    }
}
impl<B: super::LibBackend + ItemMutationPort + 'static> ItemMutationPort for BlockingLibBackend<B> {
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<ItemMutationResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |inner| async move {
            inner.mutate_items(&q, request, expected_epoch).await
        })
    }
}
impl<B: super::LibBackend + 'static> FinalizePort for BlockingLibBackend<B> {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.finalize(&q, outcomes, now, expected_epoch).await
        })
    }
}
impl<B: super::LibBackend + 'static> CommitTransitionPort for BlockingLibBackend<B> {
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.commit_transition(&q, transition, now, expected_epoch)
                .await
        })
    }
}
impl<B: super::LibBackend + 'static> RecoveryReadPort for BlockingLibBackend<B> {
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.explain_commit(&q, request_id).await
        })
    }
    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Bytes>>> + Send {
        let q = shard.clone();
        let key = key.to_vec();
        self.dispatch(
            q.clone(),
            move |i| async move { i.side_record(&q, &key).await },
        )
    }
}
impl<B: super::LibBackend + 'static> RenewLeasePort for BlockingLibBackend<B> {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.renew(&q, item_ids, new_lease_expires_at, now, expected_epoch)
                .await
        })
    }
}
impl<B: super::LibBackend + 'static> ReassignLeasePort for BlockingLibBackend<B> {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.reassign(
                &q,
                item_ids,
                new_lease_token,
                new_lease_expires_at,
                now,
                expected_epoch,
            )
            .await
        })
    }
}
impl<B: super::LibBackend + 'static> ReclaimPort for BlockingLibBackend<B> {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.reclaim_expired(&q, limit, now, expected_epoch).await
        })
    }
}
impl<B: super::LibBackend + 'static> ReschedulePort for BlockingLibBackend<B> {
    fn reschedule(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        set_priority: ScheduleUpdate<PriorityValue>,
        set_not_before: ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.reschedule(
                &q,
                item_id,
                set_priority,
                set_not_before,
                expected_item_version,
                now,
                expected_epoch,
            )
            .await
        })
    }
}
impl<B: super::LibBackend + 'static> PurgePort for BlockingLibBackend<B> {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.purge(&q, item_ids, force, now, expected_epoch).await
        })
    }
}
impl<B: super::LibBackend + 'static> SetGatesPort for BlockingLibBackend<B> {
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.set_gates(&q, command, now, expected_epoch).await
        })
    }
}

impl<B: super::LibBackend + 'static> IndexQueryPort for BlockingLibBackend<B> {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let q = shard.clone();
        let index = index.to_owned();
        let key = key.to_vec();
        self.dispatch(q.clone(), move |i| async move {
            i.index_get_unique(&q, &index, &key).await
        })
    }
    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let q = shard.clone();
        let index = index.to_owned();
        let key = key.to_vec();
        self.dispatch(q.clone(), move |i| async move {
            i.index_lookup(&q, &index, &key).await
        })
    }
}

impl<B: super::LibBackend + 'static> ProjectionRead for BlockingLibBackend<B> {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.select_eligible(&q, now, limit).await
        })
    }
    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move { i.peek(&q, limit).await })
    }
    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move { i.pending(&q).await })
    }
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<PendingSummary>> + Send {
        let q = shard.clone();
        self.dispatch(
            q.clone(),
            move |i| async move { i.pending_summary(&q).await },
        )
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<PendingPage>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.pending_page(&q, start, limit).await
        })
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let q = shard.clone();
        let consumer = consumer.cloned();
        self.dispatch(q.clone(), move |i| async move {
            i.pending_range(&q, start, end, consumer.as_ref(), limit)
                .await
        })
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let q = shard.clone();
        let ids = ids.to_vec();
        self.dispatch(q.clone(), move |i| async move {
            i.pending_by_ids(&q, &ids).await
        })
    }
    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let q = shard.clone();
        let ids = ids.to_vec();
        self.dispatch(
            q.clone(),
            move |i| async move { i.claimed_view(&q, &ids).await },
        )
    }
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let q = shard.clone();
        let keys = keys.to_vec();
        self.dispatch(
            q.clone(),
            move |i| async move { i.live_items(&q, &keys).await },
        )
    }
    fn metrics(&self, queue: &QueueKey) -> impl Future<Output = EngineResult<QueueMetrics>> + Send {
        let q = queue.clone();
        self.dispatch(q.clone(), move |i| async move { i.metrics(&q).await })
    }
    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let q = shard.clone();
        let c = emission_cursor.cloned();
        self.dispatch(q.clone(), move |i| async move {
            i.terminal_emission_metrics(&q, now, emit_change_records, c.as_ref())
                .await
        })
    }
}

impl<B: super::LibBackend + 'static> DiscoveryPort for BlockingLibBackend<B> {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<Vec<ActiveScope>>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.discover_active_scopes(&q, granularity, now).await
        })
    }
}

impl<B: super::LibBackend + 'static> HotProjectionQueryPort for BlockingLibBackend<B> {
    fn hot_projection_capabilities(&self, shard: &QueueKey) -> QueryCapabilityFlags {
        self.inner
            .as_ref()
            .expect("blocking backend is active")
            .hot_projection_capabilities(shard)
    }
    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> impl Future<Output = EngineResult<RangeScanResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.range_scan(&q, request).await
        })
    }
    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> impl Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.grouped_aggregate(&q, request).await
        })
    }
    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl Future<Output = EngineResult<QueueMetrics>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.metrics_by_query(&q, request).await
        })
    }
    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> impl Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.declared_bucket_segment(&q, request).await
        })
    }
    fn bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
        context: BoundedMutationContext,
    ) -> impl Future<Output = EngineResult<BoundedMutationResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.bounded_mutation(&q, request, context).await
        })
    }
    fn claim_by_query(
        &self,
        shard: &QueueKey,
        request: ClaimByQueryRequest,
        context: ClaimByQueryContext,
    ) -> impl Future<Output = EngineResult<Claimed>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.claim_by_query(&q, request, context).await
        })
    }
    fn claim_by_item_ids(
        &self,
        shard: &QueueKey,
        request: fireweed_core::ClaimByItemIdsRequest,
        context: ClaimByQueryContext,
    ) -> impl Future<Output = EngineResult<fireweed_engine::ClaimByItemIdsResponse>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.claim_by_item_ids(&q, request, context).await
        })
    }
}

impl<B: super::LibBackend + 'static> ControlPlaneStore for BlockingLibBackend<B> {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let q = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.dispatch(q, move |i| async move { i.create_queue(definition).await })
    }
    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
        let q = key.clone();
        self.dispatch(
            q.clone(),
            move |i| async move { i.queue_definition(&q).await },
        )
    }
    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let tenant = tenant.clone();
        let q = Self::global_queue(format!("list-{}", tenant.as_str()));
        self.dispatch(q, move |i| async move { i.list_queues(&tenant).await })
    }
    fn current_epoch(&self, shard: &QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move { i.current_epoch(&q).await })
    }
    fn acquire_epoch(&self, shard: &QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move { i.acquire_epoch(&q).await })
    }
    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.fence_epoch(&q, target_epoch).await
        })
    }
    fn hydrate_projection_for_ownership(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.hydrate_projection_for_ownership(&q).await
        })
    }
}

impl<B: super::LibBackend + 'static> HistoricalProjectionRead for BlockingLibBackend<B> {
    type AsOfProjection = B::AsOfProjection;
    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<CommandPosition>> + Send {
        let q = shard.clone();
        self.dispatch(
            q.clone(),
            move |i| async move { i.current_position(&q).await },
        )
    }
    fn read_as_of<T, F>(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> impl Future<Output = EngineResult<T>> + Send
    where
        T: Send + 'static,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send + 'static,
    {
        let q = shard.clone();
        self.dispatch(q.clone(), move |i| async move {
            i.read_as_of(&q, position, query).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    fn admit<T>(future: &mut std::pin::Pin<Box<impl Future<Output = EngineResult<T>>>>) {
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }

    #[test]
    fn accepted_job_survives_response_cancellation_and_explicit_shutdown_drains() {
        let pool = WorkerPool::new(1, 1).unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_job = Arc::clone(&ran);
        let mut future = Box::pin(pool.submit_data(0, move || {
            std::thread::sleep(Duration::from_millis(20));
            ran_job.store(true, Ordering::Release);
            Ok(())
        }));
        admit(&mut future);
        drop(future);
        pool.shutdown();
        assert!(ran.load(Ordering::Acquire));
    }

    #[test]
    fn admission_is_finite() {
        let pool = WorkerPool::new(1, 1).unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let mut first = Box::pin(pool.submit_data(0, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        }));
        admit(&mut first);
        started_rx.recv().unwrap();
        let mut second = Box::pin(pool.submit_data(0, || Ok(())));
        admit(&mut second);
        let rejected = pool.submit_data(0, || Ok(()));
        assert!(matches!(
            futures::executor::block_on(rejected),
            Err(EngineError::Backpressure { .. })
        ));
        release_tx.send(()).unwrap();
        futures::executor::block_on(first).unwrap();
        futures::executor::block_on(second).unwrap();
        pool.shutdown();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durable_job_runs_off_reactor_while_reactor_keeps_advancing() {
        let pool = WorkerPool::new(1, 1).unwrap();
        let reactor_thread = std::thread::current().id();
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticking = Arc::clone(&ticks);
        let ticker = tokio::spawn(async move {
            for _ in 0..32 {
                ticking.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });
        let worker_thread = pool
            .submit_data(0, move || {
                std::thread::sleep(Duration::from_millis(25));
                Ok(std::thread::current().id())
            })
            .await
            .unwrap();
        ticker.await.unwrap();
        assert_ne!(worker_thread, reactor_thread);
        assert_eq!(ticks.load(Ordering::Relaxed), 32);
        pool.shutdown();
    }
}
