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
use std::sync::Arc;

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

fn global_operation_key() -> QueueKey {
    QueueKey::new(
        TenantId::new("pqueue-internal").expect("valid internal tenant"),
        QueueId::new("blocking-global").expect("valid internal queue"),
    )
}

struct BlockingCapacity {
    running: Arc<tokio::sync::Semaphore>,
    outstanding: Arc<tokio::sync::Semaphore>,
    queue_gates: std::sync::Mutex<HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
}

impl BlockingCapacity {
    fn new(max_running: NonZeroUsize, max_queued: usize) -> Self {
        Self {
            running: Arc::new(tokio::sync::Semaphore::new(max_running.get())),
            outstanding: Arc::new(tokio::sync::Semaphore::new(
                max_running.get().saturating_add(max_queued),
            )),
            queue_gates: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn gate(&self, queue: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        self.queue_gates
            .lock()
            .expect("blocking operation queue gates poisoned")
            .entry(queue.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

/// Blocking-safe `pqueue-server` wrapper around [`PostgresBackend`]: implements the full [`RespBackend`]
/// port surface by delegating every call through [`spawn_blocking`](tokio::task::spawn_blocking).
///
/// [`RespBackend`]: pqueue_resp::RespBackend
pub struct PostgresWholeOperationAdapter<B: Send + Sync + 'static> {
    // `Option` so [`Drop`] can move the inner backend off the reactor (see the `Drop` impl). It is `Some`
    // for the wrapper's whole lifetime and only taken once, during drop. The `Send + Sync + 'static` bound
    // (always satisfied — `B` is a `RespBackend`) lets [`Drop`] move the inner `Arc<B>` onto a plain OS
    // thread to close the sync postgres connection off any reactor worker.
    inner: Option<Arc<B>>,
    capacity: Arc<BlockingCapacity>,
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
        Self::with_capacity(
            inner,
            NonZeroUsize::new(DEFAULT_BLOCKING_OPERATIONS).expect("nonzero"),
            DEFAULT_QUEUED_OPERATIONS,
        )
    }

    /// Construct the bounded whole-operation boundary used by production blocking stores.
    pub fn with_capacity(inner: Arc<B>, max_running: NonZeroUsize, max_queued: usize) -> Self {
        Self {
            inner: Some(inner),
            capacity: Arc::new(BlockingCapacity::new(max_running, max_queued)),
        }
    }

    /// A fresh `Arc` handle to move into a `spawn_blocking` closure.
    fn arc(&self) -> Arc<B> {
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
        async move {
            // Finite admission happens before waiting for the queue gate. Waiters consume no running
            // blocking slot, and Tokio's mutex preserves FIFO order for one queue.
            let outstanding = capacity
                .outstanding
                .clone()
                .try_acquire_owned()
                .map_err(|_| EngineError::Backpressure {
                    resource: "blocking storage operations",
                })?;
            let queue_guard = capacity.gate(&queue).lock_owned().await;
            let running = capacity
                .running
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| EngineError::Unavailable)?;
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                // Ownership of the queue gate, both permits, request data and future crosses the submit
                // boundary. Dropping the caller's JoinHandle cannot cancel this started operation.
                let _queue_guard = queue_guard;
                let _running = running;
                let _outstanding = outstanding;
                runtime.block_on(operation())
            })
            .await
            .map_err(|error| {
                EngineError::Storage(format!("blocking storage operation task failed: {error}"))
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let queue = req.shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        self.dispatch(global_operation_key(), move || async move {
            inner.tick(now).await
        })
    }
}

impl<B: RespBackend> ControlPlaneStore for PostgresWholeOperationAdapter<B> {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let inner = self.arc();
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.dispatch(queue, move || async move {
            inner.create_queue(definition).await
        })
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let inner = self.arc();
        let key = key.clone();
        let queue = key.clone();
        self.dispatch(
            queue,
            move || async move { inner.queue_definition(&key).await },
        )
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let inner = self.arc();
        let tenant = tenant.clone();
        self.dispatch(global_operation_key(), move || async move {
            inner.list_queues(&tenant).await
        })
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
        self.dispatch(
            queue,
            move || async move { inner.current_epoch(&shard).await },
        )
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
        self.dispatch(queue, move || async move {
            inner.select_eligible(&shard, now, limit).await
        })
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
        self.dispatch(
            queue,
            move || async move { inner.peek(&shard, limit).await },
        )
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
        self.dispatch(queue, move || async move { inner.pending(&shard).await })
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
        let keys = keys.to_vec();
        self.dispatch(queue, move || async move {
            inner.live_items(&shard, &keys).await
        })
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let inner = self.arc();
        let queue = queue.clone();
        let operation_queue = queue.clone();
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
        let inner = self.arc();
        let shard = shard.clone();
        let queue = shard.clone();
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
    use tokio::sync::oneshot;

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
            Arc::new(crate::composed_memory_backend()),
            NonZeroUsize::new(running).unwrap(),
            queued,
        ))
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
}
