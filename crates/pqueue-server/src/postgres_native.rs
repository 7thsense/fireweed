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
//! The inner port bodies compute eagerly and return `std::future::ready` (the blocking happens when the
//! method is *called*, not when its future is awaited), so inside `spawn_blocking` we drive that ready
//! future to completion with [`futures::executor::block_on`] — off the Tokio reactor entirely.
//!
//! [`Backend::write`] is the one exception: its closure bound is `Send` but not `'static`, so it cannot be
//! moved into `spawn_blocking`. It is also NOT on the RESP-driven hot path (the front uses the individual
//! ports — push/claim/finalize/renew/reassign/purge/upsert/tick/control-plane/projection-read — never
//! `Backend::write`). We run it on a fresh scoped OS thread ([`std::thread::scope`]) so the postgres
//! client's internal runtime still starts cleanly (no `EnterGuard` on that thread); it is documented as the
//! non-reactor-isolated path because it is never invoked by the server runtime.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition, QueueId,
    RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimPort, ClaimRequest, Claimed, ClaimedItem, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort, ItemView, LeaseView,
    LiveItemView, LogWriter, ProjectionRead, ProjectionWriter, PurgePort, PushPort, PushSpec,
    QueueKey, QueueMetrics, ReassignLeasePort, ReclaimDriver, RenewLeasePort, TickReport,
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

/// Blocking-safe `pqueue-server` wrapper around [`PostgresBackend`]: implements the full [`RespBackend`]
/// port surface by delegating every call through [`spawn_blocking`](tokio::task::spawn_blocking).
///
/// [`RespBackend`]: pqueue_resp::RespBackend
pub struct BlockingBackend<B: Send + Sync + 'static> {
    // `Option` so [`Drop`] can move the inner backend off the reactor (see the `Drop` impl). It is `Some`
    // for the wrapper's whole lifetime and only taken once, during drop. The `Send + Sync + 'static` bound
    // (always satisfied — `B` is a `RespBackend`) lets [`Drop`] move the inner `Arc<B>` onto a plain OS
    // thread to close the sync postgres connection off any reactor worker.
    inner: Option<Arc<B>>,
}

/// Back-compat alias: the blocking wrapper around the monolithic [`PostgresBackend`]. The composition root
/// now wraps the composed postgres backend ([`pqueue_postgres::ComposedPostgresBackend`]) in the same
/// generic [`BlockingBackend`].
pub type PostgresNativeBackend = BlockingBackend<PostgresBackend>;

impl<B: RespBackend> BlockingBackend<B> {
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

    /// A borrow of the inner backend for the cheap synchronous descriptor methods.
    fn backend(&self) -> &B {
        self.inner.as_ref().expect("backend present until drop")
    }
}

impl<B: Send + Sync + 'static> Drop for BlockingBackend<B> {
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

impl<B: RespBackend> Backend for BlockingBackend<B> {
    fn durability_class(&self) -> DurabilityClass {
        self.backend().durability_class()
    }

    fn supports_gates(&self) -> bool {
        self.backend().supports_gates()
    }

    fn commit_capabilities(&self) -> pqueue_engine::CommitCapabilities {
        self.backend().commit_capabilities()
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        // `f` is `Send` but not `'static`, so it cannot move into `spawn_blocking`. `write` is never on the
        // RESP-driven path; run it on a fresh scoped OS thread so the postgres client's internal runtime
        // starts cleanly (no `EnterGuard`), accepting that the caller's thread is parked for the duration.
        let inner = self.arc();
        let result = std::thread::scope(|scope| {
            scope
                .spawn(move || futures::executor::block_on(inner.write(f)))
                .join()
                .map_err(|_| EngineError::Storage("postgres write worker panicked".into()))
        })
        .and_then(|r| r);
        std::future::ready(result)
    }
}

impl<B: RespBackend> PushPort for BlockingBackend<B> {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || {
            futures::executor::block_on(inner.push(&shard, items, now, expected_epoch))
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
        blocking(move || {
            futures::executor::block_on(inner.push_with_request_id(
                &shard,
                request_id,
                items,
                now,
                expected_epoch,
            ))
        })
    }
}

impl<B: RespBackend> ClaimPort for BlockingBackend<B> {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let inner = self.arc();
        blocking(move || futures::executor::block_on(inner.claim(req)))
    }
}

impl<B: RespBackend> UpsertPort for BlockingBackend<B> {
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
            futures::executor::block_on(inner.replace_if_pending(
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

impl<B: RespBackend> FinalizePort for BlockingBackend<B> {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || {
            futures::executor::block_on(inner.finalize(&shard, outcomes, now, expected_epoch))
        })
    }
}

impl<B: RespBackend> RenewLeasePort for BlockingBackend<B> {
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
            futures::executor::block_on(inner.renew(
                &shard,
                item_ids,
                new_lease_expires_at,
                now,
                expected_epoch,
            ))
        })
    }
}

impl<B: RespBackend> ReassignLeasePort for BlockingBackend<B> {
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
            futures::executor::block_on(inner.reassign(
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

impl<B: RespBackend> PurgePort for BlockingBackend<B> {
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
        blocking(move || {
            futures::executor::block_on(inner.purge(&shard, item_ids, force, now, expected_epoch))
        })
    }
}

impl<B: RespBackend> ReclaimDriver for BlockingBackend<B> {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let inner = self.arc();
        blocking(move || futures::executor::block_on(inner.tick(now)))
    }
}

impl<B: RespBackend> ControlPlaneStore for BlockingBackend<B> {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let inner = self.arc();
        blocking(move || futures::executor::block_on(inner.create_queue(definition)))
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let inner = self.arc();
        let key = key.clone();
        blocking(move || futures::executor::block_on(inner.queue_definition(&key)))
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let inner = self.arc();
        let tenant = tenant.clone();
        blocking(move || futures::executor::block_on(inner.list_queues(&tenant)))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || futures::executor::block_on(inner.current_epoch(&shard)))
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || futures::executor::block_on(inner.acquire_epoch(&shard)))
    }
}

impl<B: RespBackend> ProjectionRead for BlockingBackend<B> {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || futures::executor::block_on(inner.select_eligible(&shard, now, limit)))
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || futures::executor::block_on(inner.peek(&shard, limit)))
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        blocking(move || futures::executor::block_on(inner.pending(&shard)))
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let ids = ids.to_vec();
        blocking(move || futures::executor::block_on(inner.claimed_view(&shard, &ids)))
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let inner = self.arc();
        let shard = shard.clone();
        let keys = keys.to_vec();
        blocking(move || futures::executor::block_on(inner.live_items(&shard, &keys)))
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let inner = self.arc();
        let queue = queue.clone();
        blocking(move || futures::executor::block_on(inner.metrics(&queue)))
    }
}
