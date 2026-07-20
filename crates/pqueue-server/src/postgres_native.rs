//! # Whole-operation blocking boundary for synchronous durable backends.
//!
//! [`pqueue_postgres::PostgresBackend`] is built on the **sync** `postgres` client, which drives its own
//! internal tokio runtime per call. Calling it from a Tokio worker thread either PANICS ("cannot start a
//! runtime from within a runtime") or blocks the reactor. This wrapper is the deliberate blocking boundary:
//! every engine-port method delegates one complete operation to [`tokio::task::spawn_blocking`], so
//! filesystem, database, and object-store work runs off reactor workers. The
//! inner backend is `Arc`-shared (`Send + Sync`) so the moved closures satisfy `spawn_blocking`'s
//! `Send + 'static` bound; the postgres backend already serializes its single connection behind an internal
//! `Mutex`, so concurrent blocking tasks queue safely.
//!
//! A finite executor owns admission, FIFO queue gates, and running permits. Once submitted, the blocking
//! job owns all of them and drives the complete inner future, preserving cancellation-after-submit.
//!
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition, QueueId,
    RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    ClaimPort, ClaimRequest, Claimed, ClaimedItem, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, EngineError, EngineResult, FinalizeOutcome, FinalizePort, ItemView,
    LeaseView, LiveItemView, ProjectionRead, PurgePort, PushPort, PushSpec, QueueKey, QueueMetrics,
    ReassignLeasePort, ReclaimDriver, RenewLeasePort, TerminalEmissionMetrics, TickReport,
    UpsertOutcome, UpsertPort,
};
use pqueue_postgres::PostgresBackend;
use pqueue_resp::RespBackend;

const DEFAULT_BLOCKING_OPERATIONS: usize = 8;
const DEFAULT_QUEUED_OPERATIONS: usize = 1024;
const DEFAULT_QUEUED_OPERATIONS_PER_QUEUE: usize = 32;

fn global_operation_key(index: usize) -> QueueKey {
    QueueKey::new(
        TenantId::new("pqueue-internal").expect("valid internal tenant"),
        QueueId::new(format!("blocking-global-{index}")).expect("valid internal queue"),
    )
}

struct BlockingCapacity {
    running: Arc<tokio::sync::Semaphore>,
    outstanding: Arc<tokio::sync::Semaphore>,
    queue_gates: std::sync::Mutex<HashMap<QueueKey, Weak<tokio::sync::Mutex<()>>>>,
    queue_admission: std::sync::Mutex<HashMap<QueueKey, Weak<tokio::sync::Semaphore>>>,
    closed: AtomicBool,
    closed_token: tokio_util::sync::CancellationToken,
    start_gate: std::sync::Mutex<()>,
    started: AtomicUsize,
    drained: tokio::sync::Notify,
}

impl BlockingCapacity {
    fn new(max_running: NonZeroUsize, max_queued: usize) -> Self {
        Self {
            running: Arc::new(tokio::sync::Semaphore::new(max_running.get())),
            outstanding: Arc::new(tokio::sync::Semaphore::new(
                max_running.get().saturating_add(max_queued),
            )),
            queue_gates: std::sync::Mutex::new(HashMap::new()),
            queue_admission: std::sync::Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            closed_token: tokio_util::sync::CancellationToken::new(),
            start_gate: std::sync::Mutex::new(()),
            started: AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
        }
    }

    fn gate(&self, queue: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut gates = self
            .queue_gates
            .lock()
            .expect("blocking operation queue gates poisoned");
        if let Some(gate) = gates.get(queue).and_then(Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        gates.insert(queue.clone(), Arc::downgrade(&gate));
        gate
    }

    fn queue_admission(&self, queue: &QueueKey) -> Arc<tokio::sync::Semaphore> {
        let mut admission = self
            .queue_admission
            .lock()
            .expect("blocking operation queue admission poisoned");
        if let Some(permits) = admission.get(queue).and_then(Weak::upgrade) {
            return permits;
        }
        admission.retain(|_, permits| permits.strong_count() > 0);
        let permits = Arc::new(tokio::sync::Semaphore::new(
            DEFAULT_QUEUED_OPERATIONS_PER_QUEUE,
        ));
        admission.insert(queue.clone(), Arc::downgrade(&permits));
        permits
    }
}

struct StartedOperation {
    capacity: Arc<BlockingCapacity>,
}

impl Drop for StartedOperation {
    fn drop(&mut self) {
        if self.capacity.started.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.capacity.drained.notify_waiters();
        }
    }
}

/// Cloneable lifecycle handle retained by [`crate::Server`]. Closing rejects new work and causes queued
/// operations still waiting for execution to leave; draining waits only for jobs that crossed the started
/// boundary.
#[derive(Clone)]
pub struct PostgresBlockingLifecycle {
    capacity: Arc<BlockingCapacity>,
}

impl PostgresBlockingLifecycle {
    pub fn close(&self) {
        let _start_gate = self
            .capacity
            .start_gate
            .lock()
            .expect("blocking operation start gate poisoned");
        self.capacity.closed.store(true, Ordering::Release);
        self.capacity.closed_token.cancel();
    }

    pub async fn drain_started(&self) {
        loop {
            let notified = self.capacity.drained.notified();
            if self.capacity.started.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Execution-safe `pqueue-server` wrapper around [`PostgresBackend`]: blocking stores construct and first
/// poll each whole operation on the blocking pool, while native-async stores stay on Tokio workers.
///
/// [`RespBackend`]: pqueue_resp::RespBackend
pub struct PostgresWholeOperationAdapter<B: Send + Sync + 'static> {
    // `Option` so [`Drop`] can move the inner backend off the reactor (see the `Drop` impl). It is `Some`
    // for the wrapper's whole lifetime and only taken once, during drop. The `Send + Sync + 'static` bound
    // (always satisfied — `B` is a `RespBackend`) lets [`Drop`] move the inner `Arc<B>` onto a plain OS
    // thread to close the sync postgres connection off any reactor worker.
    inner: Option<Vec<Arc<B>>>,
    capacity: Arc<BlockingCapacity>,
    mode: ExecutionMode,
}

#[derive(Clone, Copy)]
enum ExecutionMode {
    BlockingWholeOperation,
    NativeAsync,
}

/// Back-compat alias: the blocking wrapper around the monolithic [`PostgresBackend`]. The composition root
/// now wraps the composed postgres backend ([`pqueue_postgres::ComposedPostgresBackend`]) in the same
/// generic [`PostgresWholeOperationAdapter`].
pub type PostgresNativeBackend = PostgresWholeOperationAdapter<PostgresBackend>;

impl<B: RespBackend> PostgresWholeOperationAdapter<B> {
    /// Wrap an already-constructed backend. The backend's `connect`/`with_node_id` must run on a non-reactor
    /// thread (the composition root connects inside `spawn_blocking`).
    pub fn new(inner: B) -> Self {
        Self::from_arc(Arc::new(inner))
    }

    /// Wrap an existing `Arc` so the caller can keep another handle to the same backend instance.
    pub fn from_arc(inner: Arc<B>) -> Self {
        Self::from_arcs(vec![inner])
    }

    /// Build one fixed-size backend pool. Queue-key hashing provides stable affinity, so a queue's
    /// in-memory projection and mutation order stay on one backend while unrelated queues can use other
    /// owned connections. Pool size is fixed at construction and independent of queue count.
    pub fn from_arcs(inner: Vec<Arc<B>>) -> Self {
        assert!(!inner.is_empty(), "blocking backend pool must be non-empty");
        Self::with_capacity(
            inner,
            NonZeroUsize::new(DEFAULT_BLOCKING_OPERATIONS).expect("nonzero"),
            DEFAULT_QUEUED_OPERATIONS,
        )
    }

    /// Wrap a native-async backend. Its futures are constructed and polled only on Tokio workers; they
    /// never enter the blocking pool. The same bounded admission, ordering, cancellation, and drain
    /// lifecycle still applies.
    pub fn from_native_arc(inner: Arc<B>) -> Self {
        let mut adapter = Self::from_arcs(vec![inner]);
        adapter.mode = ExecutionMode::NativeAsync;
        adapter
    }

    /// Construct the bounded whole-operation boundary used by production blocking stores.
    pub fn with_capacity(inner: Vec<Arc<B>>, max_running: NonZeroUsize, max_queued: usize) -> Self {
        Self {
            inner: Some(inner),
            capacity: Arc::new(BlockingCapacity::new(max_running, max_queued)),
            mode: ExecutionMode::BlockingWholeOperation,
        }
    }

    pub fn lifecycle(&self) -> PostgresBlockingLifecycle {
        PostgresBlockingLifecycle {
            capacity: Arc::clone(&self.capacity),
        }
    }

    /// A fresh `Arc` handle to move into a `spawn_blocking` closure.
    fn arc_for(&self, queue: &QueueKey) -> Arc<B> {
        let inner = self.inner.as_ref().expect("backend present until drop");
        Arc::clone(&inner[Self::pool_index(queue, inner.len())])
    }

    pub(crate) fn backend_for_queue(&self, queue: &QueueKey) -> Arc<B> {
        self.arc_for(queue)
    }

    fn pool_index(queue: &QueueKey, pool_len: usize) -> usize {
        pqueue_engine::queue_worker_partition(queue, pool_len)
    }

    fn arcs(&self) -> Vec<Arc<B>> {
        self.inner
            .as_ref()
            .expect("backend present until drop")
            .clone()
    }

    fn dispatch<T, F, Fut>(
        &self,
        queue: QueueKey,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = EngineResult<T>> + Send + 'static,
    {
        let capacity = Arc::clone(&self.capacity);
        let mode = self.mode;
        async move {
            if capacity.closed.load(Ordering::Acquire) {
                return Err(EngineError::Unavailable);
            }
            // Queue-local admission is independent of the global bound so one hot queue cannot reserve
            // every outstanding slot while unrelated queues are ready to run.
            let queue_outstanding = capacity
                .queue_admission(&queue)
                .try_acquire_owned()
                .map_err(|_| EngineError::Backpressure {
                    resource: "queue blocking storage operations",
                })?;
            // Finite admission happens before waiting for the queue gate. Waiters consume no running
            // blocking slot, and Tokio's mutex preserves FIFO order for one queue.
            let outstanding = capacity
                .outstanding
                .clone()
                .try_acquire_owned()
                .map_err(|_| EngineError::Backpressure {
                    resource: "blocking storage operations",
                })?;
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            // Admission is the ownership boundary. From this point onward an owned task holds the request
            // and both queued permits, so dropping the caller cannot silently discard an accepted mutation.
            // Shutdown cancels tasks still waiting for their queue/running slot and drains tasks that started.
            tokio::spawn(async move {
                let gate = capacity.gate(&queue);
                let queue_guard = tokio::select! {
                    biased;
                    _ = capacity.closed_token.cancelled() => {
                        let _ = result_tx.send(Err(EngineError::Unavailable));
                        return;
                    },
                    guard = gate.lock_owned() => guard,
                };
                if capacity.closed.load(Ordering::Acquire) {
                    let _ = result_tx.send(Err(EngineError::Unavailable));
                    return;
                }
                let running = tokio::select! {
                    biased;
                    _ = capacity.closed_token.cancelled() => {
                        let _ = result_tx.send(Err(EngineError::Unavailable));
                        return;
                    },
                    permit = capacity.running.clone().acquire_owned() => {
                        match permit {
                            Ok(permit) => permit,
                            Err(_) => {
                                let _ = result_tx.send(Err(EngineError::Unavailable));
                                return;
                            }
                        }
                    }
                };
                {
                    // Serialize the closed check with `close`: once close returns, every operation is
                    // either represented in `started` (and therefore drained) or has been rejected.
                    let _start_gate = capacity
                        .start_gate
                        .lock()
                        .expect("blocking operation start gate poisoned");
                    if capacity.closed.load(Ordering::Acquire) {
                        let _ = result_tx.send(Err(EngineError::Unavailable));
                        return;
                    }
                    capacity.started.fetch_add(1, Ordering::AcqRel);
                }
                let started = StartedOperation {
                    capacity: Arc::clone(&capacity),
                };
                let _queue_guard = queue_guard;
                let _running = running;
                let _outstanding = outstanding;
                let _queue_outstanding = queue_outstanding;
                let _started = started;
                let result = match mode {
                    ExecutionMode::NativeAsync => operation().await,
                    ExecutionMode::BlockingWholeOperation => {
                        match tokio::task::spawn_blocking(move || {
                            // Blocking backends expose async-shaped ports for interface uniformity. Drive
                            // the complete future on this blocking worker: a transient Pending (for example,
                            // on a flusher-owned coordination lock) must not move its synchronous tail back
                            // onto a Tokio reactor thread.
                            futures::executor::block_on(operation())
                        })
                        .await
                        {
                            Ok(result) => result,
                            Err(error) => Err(EngineError::Storage(format!(
                                "blocking storage operation task failed: {error}"
                            ))),
                        }
                    }
                };
                let _ = result_tx.send(result);
            });
            result_rx.await.map_err(|_| {
                EngineError::Storage("blocking storage operation responder dropped".into())
            })?
        }
    }
}

impl<B: Send + Sync + 'static> Drop for PostgresWholeOperationAdapter<B> {
    fn drop(&mut self) {
        // The sync postgres `Client::drop` does a blocking `block_on` to close the connection. If the final
        // `Arc` drops on a Tokio worker thread that PANICS ("cannot start a runtime from within a runtime").
        // Move the inner backend to a plain OS thread (no runtime context) so the close runs cleanly. If
        // other (transient `spawn_blocking`) clones still exist this is just a cheap refcount decrement, and
        // the true close happens whenever the last clone drops — always on a blocking thread, never a
        // reactor worker.
        if let Some(inner) = self.inner.take() {
            std::thread::spawn(move || drop(inner));
        }
    }
}

impl<B: RespBackend> PushPort for PostgresWholeOperationAdapter<B> {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner.push(&shard, items, now, expected_epoch).await
        })
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner
                .push_with_request_id(&shard, request_id, items, now, expected_epoch)
                .await
        })
    }
}

impl<B: RespBackend> ClaimPort for PostgresWholeOperationAdapter<B> {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let queue = req.shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move { inner.claim(req).await })
    }
}

impl<B: RespBackend> UpsertPort for PostgresWholeOperationAdapter<B> {
    #[allow(clippy::too_many_arguments)]
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        let client_item_key = client_item_key.clone();
        self.dispatch(queue, move || async move {
            inner
                .replace_if_pending(
                    &shard,
                    &client_item_key,
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

impl<B: RespBackend> FinalizePort for PostgresWholeOperationAdapter<B> {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner.finalize(&shard, outcomes, now, expected_epoch).await
        })
    }
}

impl<B: RespBackend> RenewLeasePort for PostgresWholeOperationAdapter<B> {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner
                .renew(&shard, item_ids, new_lease_expires_at, now, expected_epoch)
                .await
        })
    }
}

impl<B: RespBackend> ReassignLeasePort for PostgresWholeOperationAdapter<B> {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner
                .reassign(
                    &shard,
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

impl<B: RespBackend> PurgePort for PostgresWholeOperationAdapter<B> {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner
                .purge(&shard, item_ids, force, now, expected_epoch)
                .await
        })
    }
}

impl<B: RespBackend> ReclaimDriver for PostgresWholeOperationAdapter<B> {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let inners = self.arcs();
        async move {
            let mut aggregate = TickReport::default();
            for (index, inner) in inners.into_iter().enumerate() {
                let report = self
                    .dispatch(global_operation_key(index), move || async move {
                        inner.tick(now).await
                    })
                    .await?;
                aggregate.leases_reclaimed += report.leases_reclaimed;
                aggregate.cohorts_expired += report.cohorts_expired;
                aggregate.items_promoted += report.items_promoted;
                aggregate.progress_bound_breaches += report.progress_bound_breaches;
                aggregate.maintenance.merge(report.maintenance);
            }
            Ok(aggregate)
        }
    }
}

impl<B: RespBackend> ControlPlaneStore for PostgresWholeOperationAdapter<B> {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner.create_queue(definition).await
        })
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let key = key.clone();
        let queue = key.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(
            queue,
            move || async move { inner.queue_definition(&key).await },
        )
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let tenant = tenant.clone();
        let inners = self.arcs();
        async move {
            let mut queues = Vec::new();
            for (index, inner) in inners.into_iter().enumerate() {
                let tenant = tenant.clone();
                let mut found = self
                    .dispatch(global_operation_key(index), move || async move {
                        inner.list_queues(&tenant).await
                    })
                    .await?;
                queues.append(&mut found);
            }
            queues.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            queues.dedup();
            Ok(queues)
        }
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(
            queue,
            move || async move { inner.current_epoch(&shard).await },
        )
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(
            queue,
            move || async move { inner.acquire_epoch(&shard).await },
        )
    }
}

impl<B: RespBackend> ProjectionRead for PostgresWholeOperationAdapter<B> {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move {
            inner.select_eligible(&shard, now, limit).await
        })
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(
            queue,
            move || async move { inner.peek(&shard, limit).await },
        )
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        self.dispatch(queue, move || async move { inner.pending(&shard).await })
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        let ids = ids.to_vec();
        self.dispatch(queue, move || async move {
            inner.claimed_view(&shard, &ids).await
        })
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        let keys = keys.to_vec();
        self.dispatch(queue, move || async move {
            inner.live_items(&shard, &keys).await
        })
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let queue = queue.clone();
        let operation_queue = queue.clone();
        let inner = self.arc_for(&operation_queue);
        self.dispatch(operation_queue, move || async move {
            inner.metrics(&queue).await
        })
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let shard = shard.clone();
        let queue = shard.clone();
        let inner = self.arc_for(&queue);
        // `emission_cursor` is a borrow that cannot outlive this method, but the `spawn_blocking`
        // closure is `'static`. Clone it into an owned value moved into the closure, then re-borrow
        // inside so the delegated call still receives `Option<&CommandPosition>`.
        let emission_cursor = emission_cursor.cloned();
        self.dispatch(queue, move || async move {
            inner
                .terminal_emission_metrics(
                    &shard,
                    now,
                    emit_change_records,
                    emission_cursor.as_ref(),
                )
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqueue_core::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
        PriorityTieBreaker, RecurrencePolicy, RetryPolicy,
    };
    use pqueue_engine::{Backend, ComposeFaultHook, ComposeFaultPoint, RawCommitRequest};
    use pqueue_objectlog::segmented::{FaultCutPoint, FaultHook, InMemoryBlobStore, SegmentConfig};
    use tokio::sync::oneshot;

    fn drive_blocking<F: Future>(future: F) -> F::Output {
        futures::executor::block_on(future)
    }

    fn queue(name: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn adapter(
        running: usize,
        queued: usize,
    ) -> Arc<PostgresWholeOperationAdapter<impl RespBackend>> {
        Arc::new(PostgresWholeOperationAdapter::with_capacity(
            vec![Arc::new(crate::composed_memory_backend())],
            NonZeroUsize::new(running).unwrap(),
            queued,
        ))
    }

    fn definition(name: &str) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new(name).unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn push_spec() -> PushSpec {
        PushSpec {
            client_item_key: None,
            priority: None,
            group_key: None,
            not_before: None,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity: None,
        }
    }

    struct BlockingApplyHook {
        entered: std::sync::Mutex<Option<oneshot::Sender<()>>>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl ComposeFaultHook for BlockingApplyHook {
        fn fault_point(&self, cut: ComposeFaultPoint) -> EngineResult<()> {
            if cut == ComposeFaultPoint::DuringProjectionApply {
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                self.release.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }
    }

    struct BlockingSegmentHook {
        entered: std::sync::Mutex<Option<oneshot::Sender<()>>>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl FaultHook for BlockingSegmentHook {
        fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
            if cut == FaultCutPoint::BeforeSegmentWrite {
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                self.release.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_storage_does_not_block_runtime_or_another_queue() {
        let adapter = adapter(2, 2);
        let (a_started_tx, a_started_rx) = oneshot::channel();
        let (release_a_tx, release_a_rx) = std::sync::mpsc::channel();
        let a = Arc::clone(&adapter);
        let a_task = tokio::spawn(async move {
            a.dispatch(queue("a"), move || async move {
                let _ = a_started_tx.send(());
                release_a_rx.recv().unwrap();
                Ok(())
            })
            .await
        });
        a_started_rx.await.unwrap();

        // A Tokio task still runs while queue A is synchronously blocked.
        let (heartbeat_tx, heartbeat_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = heartbeat_tx.send(());
        });
        heartbeat_rx.await.unwrap();

        // Queue B owns an independent FIFO gate and can use the second bounded slot.
        adapter
            .dispatch(queue("b"), || async { Ok::<_, EngineError>(()) })
            .await
            .unwrap();
        release_a_tx.send(()).unwrap();
        a_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_future_never_moves_to_blocking_thread() {
        let adapter = PostgresWholeOperationAdapter::from_native_arc(Arc::new(
            crate::composed_memory_backend(),
        ));
        let runtime_thread = std::thread::current().id();
        let observed = adapter
            .dispatch(queue("native"), || async move {
                tokio::task::yield_now().await;
                Ok::<_, EngineError>(std::thread::current().id())
            })
            .await
            .unwrap();
        assert_eq!(observed, runtime_thread);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_future_tail_stays_off_reactor_after_pending() {
        let adapter = adapter(1, 1);
        let runtime_thread = std::thread::current().id();
        let (pending_tx, pending_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let worker = Arc::clone(&adapter);
        let operation = tokio::spawn(async move {
            worker
                .dispatch(queue("pending"), move || async move {
                    let _ = pending_tx.send(());
                    resume_rx.await.unwrap();
                    Ok::<_, EngineError>(std::thread::current().id())
                })
                .await
        });
        pending_rx.await.unwrap();
        resume_tx.send(()).unwrap();
        let tail_thread = operation.await.unwrap().unwrap();
        assert_ne!(tail_thread, runtime_thread);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_sqlite_pool_allows_queue_b_while_queue_a_apply_is_blocked() {
        let path = std::env::temp_dir().join(format!(
            "pqueue-sqlite-pool-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = path.to_str().unwrap().to_string();
        let mut raw = Vec::new();
        for _ in 0..4 {
            raw.push(pqueue_sqlite::composed_sqlite_backend(&path).unwrap());
        }
        let mut a_name = "a0".to_string();
        let a_index =
            loop {
                let key = QueueKey::new(
                    TenantId::new("tenant").unwrap(),
                    QueueId::new(&a_name).unwrap(),
                );
                let index = PostgresWholeOperationAdapter::<
                pqueue_sqlite::ComposedSqliteBackend,
            >::pool_index(&key, raw.len());
                if index == 0 {
                    break index;
                }
                a_name.push('a');
            };
        let mut b_name = "b0".to_string();
        loop {
            let key = QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new(&b_name).unwrap(),
            );
            if PostgresWholeOperationAdapter::<pqueue_sqlite::ComposedSqliteBackend>::pool_index(
                &key,
                raw.len(),
            ) != a_index
            {
                break;
            }
            b_name.push('b');
        }
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        raw[a_index].set_fault_hook(Some(Arc::new(BlockingApplyHook {
            entered: std::sync::Mutex::new(Some(entered_tx)),
            release: std::sync::Mutex::new(release_rx),
        })));
        let raw: Vec<_> = raw.into_iter().map(Arc::new).collect();
        let a_backend = Arc::clone(&raw[a_index]);
        let adapter = Arc::new(PostgresWholeOperationAdapter::from_arcs(raw));
        let a_def = definition(&a_name);
        let b_def = definition(&b_name);
        let a_queue = QueueKey::new(a_def.tenant_id.clone(), a_def.queue_id.clone());
        let b_queue = QueueKey::new(b_def.tenant_id.clone(), b_def.queue_id.clone());
        adapter.create_queue(a_def).await.unwrap();
        adapter.create_queue(b_def).await.unwrap();
        let a_task = tokio::task::spawn_blocking(move || {
            drive_blocking(a_backend.commit_raw(RawCommitRequest::new(a_queue, Vec::new(), 0)))
        });
        entered_rx.await.unwrap();
        adapter
            .push(
                &b_queue,
                vec![push_spec()],
                UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        a_task.await.unwrap().unwrap();
        drop(adapter);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_objectlog_pool_allows_queue_b_while_queue_a_store_is_blocked() {
        let store = Arc::new(InMemoryBlobStore::new());
        let config = SegmentConfig::new(1, 1_000).unwrap();
        let mut raw = Vec::new();
        for _ in 0..4 {
            raw.push(
                crate::SegmentedObjectLogInMemoryBackend::open_with_blob_store(
                    store.clone(),
                    config,
                )
                .unwrap(),
            );
        }
        let a_name = "object-a";
        let a_queue = queue(a_name);
        let a_index =
            PostgresWholeOperationAdapter::<crate::SegmentedObjectLogInMemoryBackend>::pool_index(
                &a_queue,
                raw.len(),
            );
        let mut b_name = "object-b".to_string();
        let b_queue = loop {
            let candidate = queue(&b_name);
            if PostgresWholeOperationAdapter::<crate::SegmentedObjectLogInMemoryBackend>::pool_index(
                &candidate,
                raw.len(),
            ) != a_index
            {
                break candidate;
            }
            b_name.push('b');
        };
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        raw[a_index].set_object_log_fault_hook(Some(Arc::new(BlockingSegmentHook {
            entered: std::sync::Mutex::new(Some(entered_tx)),
            release: std::sync::Mutex::new(release_rx),
        })));
        let raw: Vec<_> = raw.into_iter().map(Arc::new).collect();
        let flushers: Vec<_> = raw.iter().map(|backend| backend.spawn_flusher()).collect();
        let adapter = Arc::new(PostgresWholeOperationAdapter::from_arcs(raw));
        adapter.create_queue(definition(a_name)).await.unwrap();
        adapter
            .create_queue(definition(b_queue.queue_id.as_str()))
            .await
            .unwrap();
        let a = Arc::clone(&adapter);
        let a_task = tokio::spawn(async move {
            a.push(
                &a_queue,
                vec![push_spec()],
                UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                None,
            )
            .await
        });
        entered_rx.await.unwrap();
        adapter
            .push(
                &b_queue,
                vec![push_spec()],
                UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        a_task.await.unwrap().unwrap();
        for flusher in flushers {
            flusher.abort();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_objectlog_sqlite_pool_allows_queue_b_while_queue_a_store_is_blocked() {
        let projection_path = std::env::temp_dir().join(format!(
            "pqueue-segmented-sqlite-pool-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = Arc::new(InMemoryBlobStore::new());
        let config = SegmentConfig::new(1, 1_000).unwrap();
        let mut raw = Vec::new();
        for _ in 0..4 {
            raw.push(
                crate::SegmentedObjectLogSqliteBackend::open_with_blob_store(
                    store.clone(),
                    projection_path.to_str().unwrap(),
                    config,
                )
                .unwrap(),
            );
        }
        let a_name = "object-sqlite-a";
        let a_queue = queue(a_name);
        let a_index =
            PostgresWholeOperationAdapter::<crate::SegmentedObjectLogSqliteBackend>::pool_index(
                &a_queue,
                raw.len(),
            );
        let mut b_name = "object-sqlite-b".to_string();
        let b_queue = loop {
            let candidate = queue(&b_name);
            if PostgresWholeOperationAdapter::<crate::SegmentedObjectLogSqliteBackend>::pool_index(
                &candidate,
                raw.len(),
            ) != a_index
            {
                break candidate;
            }
            b_name.push('b');
        };
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        raw[a_index].set_object_log_fault_hook(Some(Arc::new(BlockingSegmentHook {
            entered: std::sync::Mutex::new(Some(entered_tx)),
            release: std::sync::Mutex::new(release_rx),
        })));
        let raw: Vec<_> = raw.into_iter().map(Arc::new).collect();
        let flushers: Vec<_> = raw.iter().map(|backend| backend.spawn_flusher()).collect();
        let adapter = Arc::new(PostgresWholeOperationAdapter::from_arcs(raw));
        adapter.create_queue(definition(a_name)).await.unwrap();
        adapter
            .create_queue(definition(b_queue.queue_id.as_str()))
            .await
            .unwrap();
        let a = Arc::clone(&adapter);
        let a_task = tokio::spawn(async move {
            a.push(
                &a_queue,
                vec![push_spec()],
                UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                None,
            )
            .await
        });
        entered_rx.await.unwrap();
        adapter
            .push(
                &b_queue,
                vec![push_spec()],
                UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        a_task.await.unwrap().unwrap();
        for flusher in flushers {
            flusher.abort();
        }
        drop(adapter);
        let _ = std::fs::remove_file(projection_path);
    }

    async fn assert_hybrid_pool_progress(
        label: &str,
        strict: bool,
        monitor: Option<crate::HybridAsyncThresholds>,
    ) {
        let path = std::env::temp_dir().join(format!(
            "pqueue-{label}-pool-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = Arc::new(InMemoryBlobStore::new());
        let mut raw = Vec::new();
        for index in 0..4 {
            raw.push(
                crate::open_objectlog_hybrid_backend(
                    store.clone(),
                    &path,
                    SegmentConfig::new(1, 1_000).unwrap(),
                    crate::DEFAULT_RECOVERY_MAX_TAIL,
                    0,
                    pqueue_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
                    strict,
                    monitor,
                    pqueue_engine::BufferedByteBudget::new(
                        pqueue_engine::BufferedByteBudgetConfig::new(1_048_576).unwrap(),
                    ),
                    1_048_576,
                    Some((index, 4)),
                )
                .unwrap(),
            );
        }
        let a_name = format!("{label}-a");
        let a_queue = queue(&a_name);
        let a_index = PostgresWholeOperationAdapter::<crate::ObjectLogHybridBackend>::pool_index(
            &a_queue,
            raw.len(),
        );
        let mut b_name = format!("{label}-b");
        let b_queue = loop {
            let candidate = queue(&b_name);
            if PostgresWholeOperationAdapter::<crate::ObjectLogHybridBackend>::pool_index(
                &candidate,
                raw.len(),
            ) != a_index
            {
                break candidate;
            }
            b_name.push('b');
        };
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        raw[a_index].set_fault_hook(Some(Arc::new(BlockingApplyHook {
            entered: std::sync::Mutex::new(Some(entered_tx)),
            release: std::sync::Mutex::new(release_rx),
        })));
        let a_backend = Arc::clone(&raw[a_index]);
        let adapter = Arc::new(PostgresWholeOperationAdapter::from_arcs(raw));
        adapter.create_queue(definition(&a_name)).await.unwrap();
        adapter
            .create_queue(definition(b_queue.queue_id.as_str()))
            .await
            .unwrap();
        let a_task = tokio::task::spawn_blocking(move || {
            drive_blocking(a_backend.commit_raw(RawCommitRequest::new(a_queue, Vec::new(), 0)))
        });
        entered_rx.await.unwrap();
        adapter
            .push(
                &b_queue,
                vec![push_spec()],
                UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        a_task.await.unwrap().unwrap();
        drop(adapter);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_production_hybrid_mode_progresses_queue_b_while_queue_a_is_blocked() {
        assert_hybrid_pool_progress("hybrid", false, None).await;
        assert_hybrid_pool_progress("hybrid-strict", true, None).await;
        assert_hybrid_pool_progress(
            "hybrid-async",
            false,
            Some(crate::HybridAsyncThresholds::default()),
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aborting_caller_after_submit_does_not_cancel_storage_operation() {
        let adapter = adapter(1, 1);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        let worker = Arc::clone(&adapter);
        let caller = tokio::spawn(async move {
            worker
                .dispatch(queue("a"), move || async move {
                    let _ = started_tx.send(());
                    release_rx.recv().unwrap();
                    let _ = committed_tx.send(());
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        caller.abort();
        release_tx.send(()).unwrap();
        committed_rx.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aborting_caller_does_not_discard_an_admitted_queued_operation() {
        let adapter = adapter(1, 2);
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let first_worker = Arc::clone(&adapter);
        let first = tokio::spawn(async move {
            first_worker
                .dispatch(queue("a"), move || async move {
                    let _ = first_started_tx.send(());
                    release_first_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        first_started_rx.await.unwrap();

        let (submitted_tx, submitted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        let queued_worker = Arc::clone(&adapter);
        let queued_caller = tokio::spawn(async move {
            let _ = submitted_tx.send(());
            queued_worker
                .dispatch(queue("a"), move || async move {
                    let _ = committed_tx.send(());
                    Ok(())
                })
                .await
        });
        // The spawned caller continues in the same poll through non-blocking admission and into the owned
        // queue waiter. Receiving this signal therefore establishes that the request has been submitted.
        submitted_rx.await.unwrap();
        queued_caller.abort();
        release_first_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        committed_rx.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn same_queue_operations_enter_blocking_execution_in_fifo_order() {
        let adapter = adapter(2, 2);
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let first_worker = Arc::clone(&adapter);
        let first = tokio::spawn(async move {
            first_worker
                .dispatch(queue("a"), move || async move {
                    let _ = first_started_tx.send(());
                    release_first_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        first_started_rx.await.unwrap();

        let (second_submitted_tx, second_submitted_rx) = oneshot::channel();
        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let second_worker = Arc::clone(&adapter);
        let second = tokio::spawn(async move {
            let _ = second_submitted_tx.send(());
            second_worker
                .dispatch(queue("a"), move || async move {
                    let _ = second_started_tx.send(());
                    Ok(())
                })
                .await
        });
        // Sending `submitted` and then waiting for the held queue gate happen in one poll. No elapsed-time
        // assumption is involved: by the time this receiver wakes, the second operation is queued.
        second_submitted_rx.await.unwrap();
        assert!(matches!(
            second_started_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        release_first_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        second_started_rx.await.unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finite_admission_returns_backpressure_without_starting_extra_work() {
        let adapter = adapter(1, 0);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = Arc::clone(&adapter);
        let first = tokio::spawn(async move {
            worker
                .dispatch(queue("a"), move || async move {
                    let _ = started_tx.send(());
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();

        let error = adapter
            .dispatch(queue("b"), || async { Ok::<_, EngineError>(()) })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            EngineError::Backpressure {
                resource: "blocking storage operations"
            }
        ));
        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hot_queue_cannot_consume_cold_queue_admission() {
        let adapter = adapter(2, 100);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = Arc::clone(&adapter);
        let first = tokio::spawn(async move {
            worker
                .dispatch(queue("hot"), move || async move {
                    let _ = started_tx.send(());
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        let mut queued = Vec::new();
        for _ in 1..DEFAULT_QUEUED_OPERATIONS_PER_QUEUE {
            let worker = Arc::clone(&adapter);
            let (submitted_tx, submitted_rx) = oneshot::channel();
            queued.push(tokio::spawn(async move {
                let _ = submitted_tx.send(());
                worker
                    .dispatch(queue("hot"), || async { Ok::<_, EngineError>(()) })
                    .await
            }));
            submitted_rx.await.unwrap();
        }
        let hot_error = adapter
            .dispatch(queue("hot"), || async { Ok::<_, EngineError>(()) })
            .await
            .unwrap_err();
        assert!(matches!(
            hot_error,
            EngineError::Backpressure {
                resource: "queue blocking storage operations"
            }
        ));
        adapter
            .dispatch(queue("cold"), || async { Ok::<_, EngineError>(()) })
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        for task in queued {
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_rejects_queued_work_and_drain_waits_for_started_work() {
        let adapter = adapter(1, 2);
        let lifecycle = adapter.lifecycle();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = Arc::clone(&adapter);
        let started = tokio::spawn(async move {
            worker
                .dispatch(queue("a"), move || async move {
                    let _ = started_tx.send(());
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        let queued_worker = Arc::clone(&adapter);
        let (queued_submitted_tx, queued_submitted_rx) = oneshot::channel();
        let queued = tokio::spawn(async move {
            let _ = queued_submitted_tx.send(());
            queued_worker
                .dispatch(queue("b"), || async { Ok::<_, EngineError>(()) })
                .await
        });
        queued_submitted_rx.await.unwrap();
        lifecycle.close();
        let mut drain = Box::pin(lifecycle.drain_started());
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx))).await,
            std::task::Poll::Pending
        ));
        release_tx.send(()).unwrap();
        started.await.unwrap().unwrap();
        drain.await;
        assert!(matches!(
            queued.await.unwrap(),
            Err(EngineError::Unavailable)
        ));
        assert!(matches!(
            adapter
                .dispatch(queue("c"), || async { Ok::<_, EngineError>(()) })
                .await,
            Err(EngineError::Unavailable)
        ));
    }
}
