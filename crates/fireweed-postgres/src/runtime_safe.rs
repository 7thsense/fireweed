//! Adapter-private whole-operation offload for product backends that use the sync
//! `postgres` client.
//!
//! ## Why this exists (fireweed-ca319318)
//!
//! Public Fireweed must not wrap postgres matrix cells in process-wide
//! `BlockingLibBackend` (API-005 / epic fireweed-0a103d61). The sync `postgres`
//! client still panics if driven from a Tokio worker ("cannot start a runtime
//! from within a runtime") and blocks under poll if polled bare.
//!
//! This module provides an **adapter-owned** offload: each product handle gets
//! its own [`BoundedBlockingExecutor`] (not the process-wide `fireweed-library-io-*`
//! pool). Every complete port operation is submitted to that executor; the
//! reactor only awaits a oneshot-style future.
//!
//! ## Residual (document, do not hide)
//!
//! - Substrate remains the **sync** `postgres::Client` (`PostgresLog`,
//!   `PostgresRelational`, `PostgresRelationalBackend`).
//! - Desired end-state per inventory: wire in-tree actors
//!   (`AsyncPostgresLog`, `AsyncPostgresRelationalProjection`) into the product
//!   axes and drop this whole-op wrapper.
//! - This is **not** the product concurrency model; per-queue serialization still
//!   lives in the engine. Cross-queue progress does not use a process-global
//!   mutation mutex as architecture.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::*;
use fireweed_engine::*;

/// Bounds for backends that may be wrapped for adapter-private offload.
pub trait ProductBackend:
    Backend
    + PushPort
    + ClaimPort
    + UpsertPort
    + UpdateFieldsPort
    + FinalizePort
    + CommitTransitionPort
    + RecoveryReadPort
    + RenewLeasePort
    + ReassignLeasePort
    + ReclaimPort
    + ReschedulePort
    + PurgePort
    + SetGatesPort
    + ProjectionRead
    + HistoricalProjectionRead
    + IndexQueryPort
    + DiscoveryPort
    + HotProjectionQueryPort
    + ControlPlaneStore
    + Send
    + Sync
{
}

impl<T> ProductBackend for T where
    T: Backend
        + PushPort
        + ClaimPort
        + UpsertPort
        + UpdateFieldsPort
        + FinalizePort
        + CommitTransitionPort
        + RecoveryReadPort
        + RenewLeasePort
        + ReassignLeasePort
        + ReclaimPort
        + ReschedulePort
        + PurgePort
        + SetGatesPort
        + ProjectionRead
        + HistoricalProjectionRead
        + IndexQueryPort
        + DiscoveryPort
        + HotProjectionQueryPort
        + ControlPlaneStore
        + Send
        + Sync
{
}

/// Default in-flight cap for adapter-private whole-operation offload.
pub const DEFAULT_RUNTIME_SAFE_IN_FLIGHT: usize = 8;

/// Complete, bounded, **adapter-owned** offload boundary for postgres product backends.
///
/// Unlike process-wide `BlockingLibBackend`, each instance owns its
/// [`BoundedBlockingExecutor`] — no shared process worker pool.
pub struct RuntimeSafeBackend<B: ProductBackend + 'static> {
    inner: Option<Arc<B>>,
    executor: BoundedBlockingExecutor,
}

impl<B: ProductBackend + 'static> RuntimeSafeBackend<B> {
    /// Wrap an already-constructed product backend.
    ///
    /// Connect/open of the inner backend must not run on a Tokio worker (use
    /// `spawn_blocking` / `open_async` at the facade construction boundary).
    pub fn new(inner: Arc<B>) -> EngineResult<Self> {
        Self::with_in_flight(inner, DEFAULT_RUNTIME_SAFE_IN_FLIGHT)
    }

    pub fn with_in_flight(inner: Arc<B>, max_in_flight: usize) -> EngineResult<Self> {
        Ok(Self {
            inner: Some(inner),
            executor: BoundedBlockingExecutor::new(max_in_flight)?,
        })
    }

    /// Cloneable admission boundary for control-plane sequences that must also
    /// stay off the reactor (coordinated postgres opens).
    pub fn executor(&self) -> BoundedBlockingExecutor {
        self.executor.clone()
    }

    fn offload<T, Fut, F>(
        &self,
        _queue: QueueKey,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        T: Send + 'static,
        Fut: Future<Output = EngineResult<T>> + Send + 'static,
        F: FnOnce(Arc<B>) -> Fut + Send + 'static,
    {
        let inner = Arc::clone(self.inner.as_ref().expect("runtime-safe backend is active"));
        // Clone the executor so the returned future does not borrow `&self`.
        let executor = self.executor.clone();
        async move {
            executor
                .execute(move || futures::executor::block_on(operation(inner)))
                .await
        }
    }

    fn global_queue(seed: impl Into<String>) -> QueueKey {
        QueueKey::new(
            TenantId::new("fireweed-internal").expect("valid tenant"),
            QueueId::new(seed).expect("valid queue"),
        )
    }
}

impl<B: ProductBackend + 'static> Drop for RuntimeSafeBackend<B> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        // Drop the sync postgres client off any ambient Tokio worker.
        let _ = std::thread::Builder::new()
            .name("fireweed-postgres-drop".into())
            .spawn(move || drop(inner));
    }
}

impl<B: ProductBackend + 'static> Backend for RuntimeSafeBackend<B> {
    fn durability_class(&self) -> DurabilityClass {
        self.inner
            .as_ref()
            .expect("runtime-safe backend is active")
            .durability_class()
    }
    fn supports_gates(&self) -> bool {
        self.inner
            .as_ref()
            .expect("runtime-safe backend is active")
            .supports_gates()
    }
    fn commit_capabilities(&self) -> CommitCapabilities {
        self.inner
            .as_ref()
            .expect("runtime-safe backend is active")
            .commit_capabilities()
    }
    fn commit_raw(
        &self,
        request: RawCommitRequest,
    ) -> impl Future<Output = EngineResult<RawCommitOutcome>> + Send {
        let queue = request.shard().clone();
        self.offload(queue, move |inner| async move {
            inner.commit_raw(request).await
        })
    }
}

impl<B: ProductBackend + 'static> PushPort for RuntimeSafeBackend<B> {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let queue = shard.clone();
        self.offload(queue.clone(), move |inner| async move {
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
        self.offload(queue.clone(), move |inner| async move {
            inner
                .push_with_request_id(&queue, request_id, items, now, expected_epoch)
                .await
        })
    }
}

impl<B: ProductBackend + 'static> ClaimPort for RuntimeSafeBackend<B> {
    fn claim(&self, req: ClaimRequest) -> impl Future<Output = EngineResult<Claimed>> + Send {
        self.offload(req.shard.clone(), move |inner| async move {
            inner.claim(req).await
        })
    }
}

impl<B: ProductBackend + 'static> UpsertPort for RuntimeSafeBackend<B> {
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
        self.offload(queue.clone(), move |inner| async move {
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

impl<B: ProductBackend + 'static> UpdateFieldsPort for RuntimeSafeBackend<B> {
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
        self.offload(q.clone(), move |i| async move {
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
impl<B: ProductBackend + BatchUpdatePort + 'static> BatchUpdatePort for RuntimeSafeBackend<B> {
    fn batch_update(
        &self,
        shard: &QueueKey,
        request: BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<BatchUpdateResponse>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.batch_update(&q, request, now, expected_epoch).await
        })
    }
}
impl<B: ProductBackend + ItemMutationPort + 'static> ItemMutationPort for RuntimeSafeBackend<B> {
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<ItemMutationResponse>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |inner| async move {
            inner.mutate_items(&q, request, expected_epoch).await
        })
    }
}
impl<B: ProductBackend + 'static> FinalizePort for RuntimeSafeBackend<B> {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.finalize(&q, outcomes, now, expected_epoch).await
        })
    }
}
impl<B: ProductBackend + 'static> CommitTransitionPort for RuntimeSafeBackend<B> {
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.commit_transition(&q, transition, now, expected_epoch)
                .await
        })
    }
}
impl<B: ProductBackend + 'static> RecoveryReadPort for RuntimeSafeBackend<B> {
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
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
        self.offload(
            q.clone(),
            move |i| async move { i.side_record(&q, &key).await },
        )
    }
}
impl<B: ProductBackend + 'static> RenewLeasePort for RuntimeSafeBackend<B> {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.renew(&q, item_ids, new_lease_expires_at, now, expected_epoch)
                .await
        })
    }
}
impl<B: ProductBackend + 'static> ReassignLeasePort for RuntimeSafeBackend<B> {
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
        self.offload(q.clone(), move |i| async move {
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
impl<B: ProductBackend + 'static> ReclaimPort for RuntimeSafeBackend<B> {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.reclaim_expired(&q, limit, now, expected_epoch).await
        })
    }
}
impl<B: ProductBackend + 'static> ReschedulePort for RuntimeSafeBackend<B> {
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
        self.offload(q.clone(), move |i| async move {
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
impl<B: ProductBackend + 'static> PurgePort for RuntimeSafeBackend<B> {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.purge(&q, item_ids, force, now, expected_epoch).await
        })
    }
}
impl<B: ProductBackend + 'static> SetGatesPort for RuntimeSafeBackend<B> {
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.set_gates(&q, command, now, expected_epoch).await
        })
    }
}

impl<B: ProductBackend + 'static> IndexQueryPort for RuntimeSafeBackend<B> {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let q = shard.clone();
        let index = index.to_owned();
        let key = key.to_vec();
        self.offload(q.clone(), move |i| async move {
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
        self.offload(q.clone(), move |i| async move {
            i.index_lookup(&q, &index, &key).await
        })
    }
}

impl<B: ProductBackend + 'static> ProjectionRead for RuntimeSafeBackend<B> {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.select_eligible(&q, now, limit).await
        })
    }
    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move { i.peek(&q, limit).await })
    }
    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move { i.pending(&q).await })
    }
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<PendingSummary>> + Send {
        let q = shard.clone();
        self.offload(
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
        self.offload(q.clone(), move |i| async move {
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
        self.offload(q.clone(), move |i| async move {
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
        self.offload(q.clone(), move |i| async move {
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
        self.offload(
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
        self.offload(
            q.clone(),
            move |i| async move { i.live_items(&q, &keys).await },
        )
    }
    fn metrics(&self, queue: &QueueKey) -> impl Future<Output = EngineResult<QueueMetrics>> + Send {
        let q = queue.clone();
        self.offload(q.clone(), move |i| async move { i.metrics(&q).await })
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
        self.offload(q.clone(), move |i| async move {
            i.terminal_emission_metrics(&q, now, emit_change_records, c.as_ref())
                .await
        })
    }
}

impl<B: ProductBackend + 'static> DiscoveryPort for RuntimeSafeBackend<B> {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<Vec<ActiveScope>>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.discover_active_scopes(&q, granularity, now).await
        })
    }
}

impl<B: ProductBackend + 'static> HotProjectionQueryPort for RuntimeSafeBackend<B> {
    fn hot_projection_capabilities(&self, shard: &QueueKey) -> QueryCapabilityFlags {
        self.inner
            .as_ref()
            .expect("runtime-safe backend is active")
            .hot_projection_capabilities(shard)
    }
    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> impl Future<Output = EngineResult<RangeScanResponse>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.range_scan(&q, request).await
        })
    }
    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> impl Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.grouped_aggregate(&q, request).await
        })
    }
    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl Future<Output = EngineResult<QueueMetrics>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.metrics_by_query(&q, request).await
        })
    }
    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> impl Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
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
        self.offload(q.clone(), move |i| async move {
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
        self.offload(q.clone(), move |i| async move {
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
        self.offload(q.clone(), move |i| async move {
            i.claim_by_item_ids(&q, request, context).await
        })
    }
}

impl<B: ProductBackend + 'static> ControlPlaneStore for RuntimeSafeBackend<B> {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let q = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.offload(q, move |i| async move { i.create_queue(definition).await })
    }
    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
        let q = key.clone();
        self.offload(
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
        self.offload(q, move |i| async move { i.list_queues(&tenant).await })
    }
    fn current_epoch(&self, shard: &QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move { i.current_epoch(&q).await })
    }
    fn acquire_epoch(&self, shard: &QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move { i.acquire_epoch(&q).await })
    }
    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl Future<Output = EngineResult<u64>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.fence_epoch(&q, target_epoch).await
        })
    }
    fn hydrate_projection_for_ownership(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let q = shard.clone();
        self.offload(q.clone(), move |i| async move {
            i.hydrate_projection_for_ownership(&q).await
        })
    }
}

impl<B: ProductBackend + 'static> HistoricalProjectionRead for RuntimeSafeBackend<B> {
    type AsOfProjection = B::AsOfProjection;
    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl Future<Output = EngineResult<CommandPosition>> + Send {
        let q = shard.clone();
        self.offload(
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
        self.offload(q.clone(), move |i| async move {
            i.read_as_of(&q, position, query).await
        })
    }
}
