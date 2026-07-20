//! # `postgres_native` runtime wrapper — the blocking boundary for the SYNC postgres adapter.
//!
//! [`pqueue_postgres::PostgresBackend`] is built on the **sync** `postgres` client, which drives its own
//! internal tokio runtime per call. Calling it from a Tokio worker thread either PANICS ("cannot start a
//! runtime from within a runtime") or blocks the reactor. This wrapper is the deliberate blocking boundary:
//! every engine-port method delegates to [`tokio::task::spawn_blocking`], so the sync DB work runs on
//! Tokio's blocking-thread pool (which holds no runtime `EnterGuard`) instead of on a reactor worker. The
//! inner backend is `Arc`-shared (`Send + Sync`) so the moved closures satisfy `spawn_blocking`'s
//! `Send + 'static` bound; the postgres backend already serializes its single connection behind an internal
//! `Mutex`, so concurrent blocking tasks queue safely.
//!
//! The inner sync port bodies compute before returning their immediately-resolved future. Inside
//! `spawn_blocking` we poll that future once and reject a pending result as an adapter contract violation;
//! no nested executor or runtime is entered.
//!
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

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

/// Drive a sync postgres closure on Tokio's blocking pool (off the reactor), mapping a join failure to a
/// structured storage error.
async fn blocking<T, F>(f: F) -> EngineResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> EngineResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| EngineError::Storage(format!("postgres blocking task join failed: {e}")))?
}

fn resolve_eager<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("sync Postgres adapter returned a pending future"),
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
}

/// Back-compat alias: the blocking wrapper around the monolithic [`PostgresBackend`]. The composition root
/// now wraps the composed postgres backend ([`pqueue_postgres::ComposedPostgresBackend`]) in the same
/// generic [`PostgresWholeOperationAdapter`].
pub type PostgresNativeBackend = PostgresWholeOperationAdapter<PostgresBackend>;

impl<B: RespBackend> PostgresWholeOperationAdapter<B> {
    /// Wrap an already-constructed backend. The backend's `connect`/`with_node_id` must run on a non-reactor
    /// thread (the composition root connects inside `spawn_blocking`).
    pub fn new(inner: B) -> Self {
        Self {
            inner: Some(Arc::new(inner)),
        }
    }

    /// Wrap an existing `Arc` so the caller can keep another handle to the same backend instance.
    pub fn from_arc(inner: Arc<B>) -> Self {
        Self { inner: Some(inner) }
    }

    /// A fresh `Arc` handle to move into a `spawn_blocking` closure.
    fn arc(&self) -> Arc<B> {
        self.inner
            .as_ref()
            .expect("backend present until drop")
            .clone()
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
        blocking(move || resolve_eager(inner.push(&shard, items, now, expected_epoch)))
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
        blocking(move || {
            resolve_eager(inner.push_with_request_id(
                &shard,
                request_id,
                items,
                now,
                expected_epoch,
            ))
        })
    }
}

impl<B: RespBackend> ClaimPort for PostgresWholeOperationAdapter<B> {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let inner = self.arc();
        blocking(move || resolve_eager(inner.claim(req)))
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
        let client_item_key = client_item_key.clone();
        blocking(move || {
            resolve_eager(inner.replace_if_pending(
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
            ))
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
        blocking(move || resolve_eager(inner.finalize(&shard, outcomes, now, expected_epoch)))
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
        blocking(move || {
            resolve_eager(inner.renew(&shard, item_ids, new_lease_expires_at, now, expected_epoch))
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
        blocking(move || {
            resolve_eager(inner.reassign(
                &shard,
                item_ids,
                new_lease_token,
                new_lease_expires_at,
                now,
                expected_epoch,
            ))
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
        blocking(move || resolve_eager(inner.purge(&shard, item_ids, force, now, expected_epoch)))
    }
}

impl<B: RespBackend> ReclaimDriver for PostgresWholeOperationAdapter<B> {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let inner = self.arc();
        blocking(move || resolve_eager(inner.tick(now)))
    }
}

impl<B: RespBackend> ControlPlaneStore for PostgresWholeOperationAdapter<B> {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let inner = self.arc();
        blocking(move || resolve_eager(inner.create_queue(definition)))
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let inner = self.arc();
        let key = key.clone();
        blocking(move || resolve_eager(inner.queue_definition(&key)))
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let inner = self.arc();
        let tenant = tenant.clone();
        blocking(move || resolve_eager(inner.list_queues(&tenant)))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || resolve_eager(inner.current_epoch(&shard)))
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || resolve_eager(inner.acquire_epoch(&shard)))
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
        blocking(move || resolve_eager(inner.select_eligible(&shard, now, limit)))
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || resolve_eager(inner.peek(&shard, limit)))
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || resolve_eager(inner.pending(&shard)))
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let ids = ids.to_vec();
        blocking(move || resolve_eager(inner.claimed_view(&shard, &ids)))
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let keys = keys.to_vec();
        blocking(move || resolve_eager(inner.live_items(&shard, &keys)))
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let inner = self.arc();
        let queue = queue.clone();
        blocking(move || resolve_eager(inner.metrics(&queue)))
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
        // `emission_cursor` is a borrow that cannot outlive this method, but the `spawn_blocking`
        // closure is `'static`. Clone it into an owned value moved into the closure, then re-borrow
        // inside so the delegated call still receives `Option<&CommandPosition>`.
        let emission_cursor = emission_cursor.cloned();
        blocking(move || {
            resolve_eager(inner.terminal_emission_metrics(
                &shard,
                now,
                emit_change_records,
                emission_cursor.as_ref(),
            ))
        })
    }
}
