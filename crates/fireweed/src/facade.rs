use super::*;

type FacadeFuture<'a, T> = Pin<Box<dyn Future<Output = EngineResult<T>> + Send + 'a>>;

trait FireweedDataPlane: Send + Sync {
    fn ownership<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, Ownership>;
    fn renew_owned(&self) -> EngineResult<()>;
    fn create_queue(&self, definition: QueueDefinition) -> FacadeFuture<'_, CreateQueueOutcome>;
    fn queue_definition<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, QueueDefinition>;
    fn ensure_queue<'a>(
        &'a self,
        queue: &'a QueueKey,
        template: &'a QueueTemplate,
    ) -> Pin<Box<dyn Future<Output = Result<EnsureQueueOutcome, EnsureQueueError>> + Send + 'a>>;
    fn push<'a>(&'a self, queue: &'a QueueKey, item: NewItem) -> FacadeFuture<'a, ItemId>;
    fn push_with_request_id<'a>(
        &'a self,
        queue: &'a QueueKey,
        request_id: RequestId,
        item: NewItem,
    ) -> FacadeFuture<'a, ItemId>;
    fn push_batch<'a>(
        &'a self,
        queue: &'a QueueKey,
        items: Vec<NewItem>,
    ) -> FacadeFuture<'a, Vec<ItemId>>;
    fn push_batch_with_request_id<'a>(
        &'a self,
        queue: &'a QueueKey,
        request_id: RequestId,
        items: Vec<NewItem>,
    ) -> FacadeFuture<'a, Vec<ItemId>>;
    fn upsert<'a>(
        &'a self,
        queue: &'a QueueKey,
        key: ClientItemKey,
        item: NewItem,
    ) -> FacadeFuture<'a, UpsertOutcome>;
    fn claim_with<'a>(
        &'a self,
        queue: &'a QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> FacadeFuture<'a, Vec<ClaimedItem>>;
    fn claim<'a>(
        &'a self,
        queue: &'a QueueKey,
        max: usize,
        lease_ms: u64,
    ) -> FacadeFuture<'a, Vec<ClaimedItem>>;
    fn claim_response_with<'a>(
        &'a self,
        queue: &'a QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> FacadeFuture<'a, Claimed>;
    fn claim_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimAt,
    ) -> FacadeFuture<'a, Vec<ClaimedItem>>;
    fn claim_response_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimAt,
    ) -> FacadeFuture<'a, Claimed>;
    fn claim_across_queues(
        &self,
        targets: Vec<MultiQueueClaimTarget>,
        limits: MultiQueueClaimLimits,
    ) -> FacadeFuture<'_, Vec<MultiQueueClaimResult>>;
    fn claim_by_query<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimByQueryRequest,
    ) -> FacadeFuture<'a, Claimed>;
    fn claim_by_query_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimByQueryRequest,
        at: ClaimByQueryAt,
    ) -> FacadeFuture<'a, Claimed>;
    fn ack<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()>;
    fn complete<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()>;
    fn nack<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>, how: Nack)
    -> FacadeFuture<'a, ()>;
    fn retry<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        not_before: Option<UtcTimestamp>,
    ) -> FacadeFuture<'a, ()>;
    fn release<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()>;
    fn nack_retry_after<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        delay_ms: u64,
    ) -> FacadeFuture<'a, ()>;
    fn retry_after<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        delay_ms: u64,
    ) -> FacadeFuture<'a, ()>;
    fn commit<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: CommitRequest,
    ) -> FacadeFuture<'a, Vec<EntryOutcome>>;
    fn commit_multi_claim<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: MultiClaimCommitRequest,
    ) -> FacadeFuture<'a, Vec<EntryOutcome>>;
    fn commit_capabilities(&self, queue: &QueueKey) -> EngineResult<CommitCapabilities>;
    fn explain_commit<'a>(
        &'a self,
        queue: &'a QueueKey,
        request_id: RequestId,
    ) -> FacadeFuture<'a, Option<CommitRecovery>>;
    fn side_record<'a>(
        &'a self,
        queue: &'a QueueKey,
        key: &'a [u8],
    ) -> FacadeFuture<'a, Option<Bytes>>;
    fn peek<'a>(&'a self, queue: &'a QueueKey, limit: usize) -> FacadeFuture<'a, Vec<ItemView>>;
    fn current_position<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, CommandPosition>;
    fn discover_active_scopes<'a>(
        &'a self,
        queue: &'a QueueKey,
        granularity: DiscoveryGranularity,
    ) -> FacadeFuture<'a, Vec<ActiveScope>>;
    fn discover_active_scopes_stamped<'a>(
        &'a self,
        queue: &'a QueueKey,
        granularity: DiscoveryGranularity,
    ) -> FacadeFuture<'a, ActiveScopeDiscovery>;
    fn discover<'a>(
        &'a self,
        queue: &'a QueueKey,
        granularity: DiscoveryGranularity,
    ) -> FacadeFuture<'a, Vec<ActiveScope>>;
    fn live_item<'a>(
        &'a self,
        queue: &'a QueueKey,
        key: ClientItemKey,
    ) -> FacadeFuture<'a, Option<LiveItemView>>;
    fn live_items<'a>(
        &'a self,
        queue: &'a QueueKey,
        keys: Vec<ClientItemKey>,
    ) -> FacadeFuture<'a, Vec<Option<LiveItemView>>>;
    fn query_index_unique<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        key: Vec<Vec<u8>>,
    ) -> FacadeFuture<'a, Option<IndexHit>>;
    fn query_index<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        key: Vec<Vec<u8>>,
    ) -> FacadeFuture<'a, Vec<IndexHit>>;
    fn query_index_unique_typed<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        values: &'a [serde_json::Value],
    ) -> FacadeFuture<'a, Option<IndexHit>>;
    fn query_index_typed<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        values: &'a [serde_json::Value],
    ) -> FacadeFuture<'a, Vec<IndexHit>>;
    fn fail<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()>;
    fn renew<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        lease_ms: u64,
    ) -> FacadeFuture<'a, ()>;
    fn reassign<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        lease_ms: u64,
    ) -> FacadeFuture<'a, ()>;
    fn update_fields<'a>(
        &'a self,
        queue: &'a QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
    ) -> FacadeFuture<'a, u64>;
    fn update<'a>(
        &'a self,
        queue: &'a QueueKey,
        item_id: ItemId,
        priority: ScheduleUpdate<PriorityValue>,
        not_before: ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
    ) -> FacadeFuture<'a, u64>;
    fn set_gates<'a>(
        &'a self,
        queue: &'a QueueKey,
        gate_keys: Vec<String>,
        blocked: bool,
    ) -> FacadeFuture<'a, ()>;
    fn reclaim_expired<'a>(
        &'a self,
        queue: &'a QueueKey,
        limit: Option<usize>,
    ) -> FacadeFuture<'a, Vec<ItemId>>;
    fn reclaim_expired_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
    ) -> FacadeFuture<'a, Vec<ItemId>>;
    fn rearm<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()>;
    fn rearm_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        not_before: UtcTimestamp,
    ) -> FacadeFuture<'a, ()>;
    fn rearm_after<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        delay_ms: u64,
    ) -> FacadeFuture<'a, ()>;
    fn purge<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> FacadeFuture<'a, u64>;
    fn claimed<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: &'a [ItemId],
    ) -> FacadeFuture<'a, Vec<ClaimedItem>>;
    fn metrics<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, QueueMetrics>;
    fn metrics_by_query<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: MetricsByQueryRequest,
    ) -> FacadeFuture<'a, QueueMetrics>;
    fn hot_projection_capabilities(&self, queue: &QueueKey) -> QueryCapabilityFlags;
    fn range_scan<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: RangeScanRequest,
    ) -> FacadeFuture<'a, RangeScanResponse>;
    fn grouped_aggregate<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: GroupedAggregateRequest,
    ) -> FacadeFuture<'a, GroupedAggregateResponse>;
    fn declared_bucket_segment<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> FacadeFuture<'a, DeclaredBucketSegmentResponse>;
    fn bounded_mutation<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: BoundedMutationRequest,
    ) -> FacadeFuture<'a, BoundedMutationResponse>;
}

trait FireweedBatchPlane: Send + Sync {
    fn batch_update<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: BatchUpdateRequest,
    ) -> FacadeFuture<'a, BatchUpdateResponse>;
}

trait FireweedMutationPlane: Send + Sync {
    fn mutate_items<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ItemMutationRequest,
    ) -> FacadeFuture<'a, ItemMutationResponse>;
}

impl<B: LibBackend + ItemMutationPort + 'static> FireweedMutationPlane for RuntimeCore<B> {
    fn mutate_items<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ItemMutationRequest,
    ) -> FacadeFuture<'a, ItemMutationResponse> {
        Box::pin(RuntimeCore::mutate_items(self, queue, request))
    }
}

impl<B: LibBackend + BatchUpdatePort + 'static> FireweedBatchPlane for RuntimeCore<B> {
    fn batch_update<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: BatchUpdateRequest,
    ) -> FacadeFuture<'a, BatchUpdateResponse> {
        Box::pin(RuntimeCore::batch_update(self, queue, request))
    }
}

impl<B: LibBackend + 'static> FireweedDataPlane for RuntimeCore<B> {
    fn ownership<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, Ownership> {
        Box::pin(RuntimeCore::ownership(self, queue))
    }
    fn renew_owned(&self) -> EngineResult<()> {
        RuntimeCore::renew_owned(self)
    }
    fn create_queue(&self, definition: QueueDefinition) -> FacadeFuture<'_, CreateQueueOutcome> {
        Box::pin(RuntimeCore::create_queue(self, definition))
    }
    fn queue_definition<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, QueueDefinition> {
        Box::pin(RuntimeCore::queue_definition(self, queue))
    }
    fn ensure_queue<'a>(
        &'a self,
        queue: &'a QueueKey,
        template: &'a QueueTemplate,
    ) -> Pin<Box<dyn Future<Output = Result<EnsureQueueOutcome, EnsureQueueError>> + Send + 'a>>
    {
        Box::pin(RuntimeCore::ensure_queue(self, queue, template))
    }
    fn push<'a>(&'a self, queue: &'a QueueKey, item: NewItem) -> FacadeFuture<'a, ItemId> {
        Box::pin(RuntimeCore::push(self, queue, item))
    }
    fn push_with_request_id<'a>(
        &'a self,
        queue: &'a QueueKey,
        request_id: RequestId,
        item: NewItem,
    ) -> FacadeFuture<'a, ItemId> {
        Box::pin(RuntimeCore::push_with_request_id(
            self, queue, request_id, item,
        ))
    }
    fn push_batch<'a>(
        &'a self,
        queue: &'a QueueKey,
        items: Vec<NewItem>,
    ) -> FacadeFuture<'a, Vec<ItemId>> {
        Box::pin(RuntimeCore::push_batch(self, queue, items))
    }
    fn push_batch_with_request_id<'a>(
        &'a self,
        queue: &'a QueueKey,
        request_id: RequestId,
        items: Vec<NewItem>,
    ) -> FacadeFuture<'a, Vec<ItemId>> {
        Box::pin(RuntimeCore::push_batch_with_request_id(
            self, queue, request_id, items,
        ))
    }
    fn upsert<'a>(
        &'a self,
        queue: &'a QueueKey,
        key: ClientItemKey,
        item: NewItem,
    ) -> FacadeFuture<'a, UpsertOutcome> {
        Box::pin(RuntimeCore::upsert(self, queue, key, item))
    }
    fn claim_with<'a>(
        &'a self,
        queue: &'a QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> FacadeFuture<'a, Vec<ClaimedItem>> {
        Box::pin(RuntimeCore::claim_with(
            self,
            queue,
            max,
            lease_ms,
            compatibility,
        ))
    }
    fn claim<'a>(
        &'a self,
        queue: &'a QueueKey,
        max: usize,
        lease_ms: u64,
    ) -> FacadeFuture<'a, Vec<ClaimedItem>> {
        Box::pin(RuntimeCore::claim(self, queue, max, lease_ms))
    }
    fn claim_response_with<'a>(
        &'a self,
        queue: &'a QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> FacadeFuture<'a, Claimed> {
        Box::pin(RuntimeCore::claim_response_with(
            self,
            queue,
            max,
            lease_ms,
            compatibility,
        ))
    }
    fn claim_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimAt,
    ) -> FacadeFuture<'a, Vec<ClaimedItem>> {
        Box::pin(RuntimeCore::claim_at(self, queue, request))
    }
    fn claim_response_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimAt,
    ) -> FacadeFuture<'a, Claimed> {
        Box::pin(RuntimeCore::claim_response_at(self, queue, request))
    }
    fn claim_across_queues(
        &self,
        targets: Vec<MultiQueueClaimTarget>,
        limits: MultiQueueClaimLimits,
    ) -> FacadeFuture<'_, Vec<MultiQueueClaimResult>> {
        Box::pin(RuntimeCore::claim_across_queues(self, targets, limits))
    }
    fn claim_by_query<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimByQueryRequest,
    ) -> FacadeFuture<'a, Claimed> {
        Box::pin(RuntimeCore::claim_by_query(self, queue, request))
    }
    fn claim_by_query_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: ClaimByQueryRequest,
        at: ClaimByQueryAt,
    ) -> FacadeFuture<'a, Claimed> {
        Box::pin(RuntimeCore::claim_by_query_at(self, queue, request, at))
    }
    fn ack<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::ack(self, queue, ids))
    }
    fn complete<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::complete(self, queue, ids))
    }
    fn nack<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        how: Nack,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::nack(self, queue, ids, how))
    }
    fn retry<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        not_before: Option<UtcTimestamp>,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::retry(self, queue, ids, not_before))
    }
    fn release<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::release(self, queue, ids))
    }
    fn nack_retry_after<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        delay_ms: u64,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::nack_retry_after(self, queue, ids, delay_ms))
    }
    fn retry_after<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        delay_ms: u64,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::retry_after(self, queue, ids, delay_ms))
    }
    fn commit<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: CommitRequest,
    ) -> FacadeFuture<'a, Vec<EntryOutcome>> {
        Box::pin(RuntimeCore::commit(self, queue, request))
    }
    fn commit_multi_claim<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: MultiClaimCommitRequest,
    ) -> FacadeFuture<'a, Vec<EntryOutcome>> {
        Box::pin(RuntimeCore::commit_multi_claim(self, queue, request))
    }
    fn commit_capabilities(&self, queue: &QueueKey) -> EngineResult<CommitCapabilities> {
        RuntimeCore::commit_capabilities(self, queue)
    }
    fn explain_commit<'a>(
        &'a self,
        queue: &'a QueueKey,
        request_id: RequestId,
    ) -> FacadeFuture<'a, Option<CommitRecovery>> {
        Box::pin(RuntimeCore::explain_commit(self, queue, request_id))
    }
    fn side_record<'a>(
        &'a self,
        queue: &'a QueueKey,
        key: &'a [u8],
    ) -> FacadeFuture<'a, Option<Bytes>> {
        Box::pin(RuntimeCore::side_record(self, queue, key))
    }
    fn peek<'a>(&'a self, queue: &'a QueueKey, limit: usize) -> FacadeFuture<'a, Vec<ItemView>> {
        Box::pin(RuntimeCore::peek(self, queue, limit))
    }
    fn current_position<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, CommandPosition> {
        Box::pin(RuntimeCore::current_position(self, queue))
    }
    fn discover_active_scopes<'a>(
        &'a self,
        queue: &'a QueueKey,
        granularity: DiscoveryGranularity,
    ) -> FacadeFuture<'a, Vec<ActiveScope>> {
        Box::pin(RuntimeCore::discover_active_scopes(
            self,
            queue,
            granularity,
        ))
    }
    fn discover_active_scopes_stamped<'a>(
        &'a self,
        queue: &'a QueueKey,
        granularity: DiscoveryGranularity,
    ) -> FacadeFuture<'a, ActiveScopeDiscovery> {
        Box::pin(RuntimeCore::discover_active_scopes_stamped(
            self,
            queue,
            granularity,
        ))
    }
    fn discover<'a>(
        &'a self,
        queue: &'a QueueKey,
        granularity: DiscoveryGranularity,
    ) -> FacadeFuture<'a, Vec<ActiveScope>> {
        Box::pin(RuntimeCore::discover(self, queue, granularity))
    }
    fn live_item<'a>(
        &'a self,
        queue: &'a QueueKey,
        key: ClientItemKey,
    ) -> FacadeFuture<'a, Option<LiveItemView>> {
        Box::pin(RuntimeCore::live_item(self, queue, key))
    }
    fn live_items<'a>(
        &'a self,
        queue: &'a QueueKey,
        keys: Vec<ClientItemKey>,
    ) -> FacadeFuture<'a, Vec<Option<LiveItemView>>> {
        Box::pin(RuntimeCore::live_items(self, queue, keys))
    }
    fn query_index_unique<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        key: Vec<Vec<u8>>,
    ) -> FacadeFuture<'a, Option<IndexHit>> {
        Box::pin(RuntimeCore::query_index_unique(self, queue, index, key))
    }
    fn query_index<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        key: Vec<Vec<u8>>,
    ) -> FacadeFuture<'a, Vec<IndexHit>> {
        Box::pin(RuntimeCore::query_index(self, queue, index, key))
    }
    fn query_index_unique_typed<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        values: &'a [serde_json::Value],
    ) -> FacadeFuture<'a, Option<IndexHit>> {
        Box::pin(RuntimeCore::query_index_unique_typed(
            self, queue, index, values,
        ))
    }
    fn query_index_typed<'a>(
        &'a self,
        queue: &'a QueueKey,
        index: &'a str,
        values: &'a [serde_json::Value],
    ) -> FacadeFuture<'a, Vec<IndexHit>> {
        Box::pin(RuntimeCore::query_index_typed(self, queue, index, values))
    }
    fn fail<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::fail(self, queue, ids))
    }
    fn renew<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        lease_ms: u64,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::renew(self, queue, ids, lease_ms))
    }
    fn reassign<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        lease_ms: u64,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::reassign(self, queue, ids, lease_ms))
    }
    fn update_fields<'a>(
        &'a self,
        queue: &'a QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
    ) -> FacadeFuture<'a, u64> {
        Box::pin(RuntimeCore::update_fields(
            self,
            queue,
            item_id,
            field_ops,
            payload,
            entity,
            expected_item_version,
        ))
    }
    fn update<'a>(
        &'a self,
        queue: &'a QueueKey,
        item_id: ItemId,
        priority: ScheduleUpdate<PriorityValue>,
        not_before: ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
    ) -> FacadeFuture<'a, u64> {
        Box::pin(RuntimeCore::update(
            self,
            queue,
            item_id,
            priority,
            not_before,
            expected_item_version,
        ))
    }
    fn set_gates<'a>(
        &'a self,
        queue: &'a QueueKey,
        gate_keys: Vec<String>,
        blocked: bool,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::set_gates(self, queue, gate_keys, blocked))
    }
    fn reclaim_expired<'a>(
        &'a self,
        queue: &'a QueueKey,
        limit: Option<usize>,
    ) -> FacadeFuture<'a, Vec<ItemId>> {
        Box::pin(RuntimeCore::reclaim_expired(self, queue, limit))
    }
    fn reclaim_expired_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
    ) -> FacadeFuture<'a, Vec<ItemId>> {
        Box::pin(RuntimeCore::reclaim_expired_at(self, queue, limit, now))
    }
    fn rearm<'a>(&'a self, queue: &'a QueueKey, ids: Vec<ItemId>) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::rearm(self, queue, ids))
    }
    fn rearm_at<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        not_before: UtcTimestamp,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::rearm_at(self, queue, ids, not_before))
    }
    fn rearm_after<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        delay_ms: u64,
    ) -> FacadeFuture<'a, ()> {
        Box::pin(RuntimeCore::rearm_after(self, queue, ids, delay_ms))
    }
    fn purge<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> FacadeFuture<'a, u64> {
        Box::pin(RuntimeCore::purge(self, queue, ids, force))
    }
    fn claimed<'a>(
        &'a self,
        queue: &'a QueueKey,
        ids: &'a [ItemId],
    ) -> FacadeFuture<'a, Vec<ClaimedItem>> {
        Box::pin(RuntimeCore::claimed(self, queue, ids))
    }
    fn metrics<'a>(&'a self, queue: &'a QueueKey) -> FacadeFuture<'a, QueueMetrics> {
        Box::pin(RuntimeCore::metrics(self, queue))
    }
    fn metrics_by_query<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: MetricsByQueryRequest,
    ) -> FacadeFuture<'a, QueueMetrics> {
        Box::pin(RuntimeCore::metrics_by_query(self, queue, request))
    }
    fn hot_projection_capabilities(&self, queue: &QueueKey) -> QueryCapabilityFlags {
        RuntimeCore::hot_projection_capabilities(self, queue)
    }
    fn range_scan<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: RangeScanRequest,
    ) -> FacadeFuture<'a, RangeScanResponse> {
        Box::pin(RuntimeCore::range_scan(self, queue, request))
    }
    fn grouped_aggregate<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: GroupedAggregateRequest,
    ) -> FacadeFuture<'a, GroupedAggregateResponse> {
        Box::pin(RuntimeCore::grouped_aggregate(self, queue, request))
    }
    fn declared_bucket_segment<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> FacadeFuture<'a, DeclaredBucketSegmentResponse> {
        Box::pin(RuntimeCore::declared_bucket_segment(self, queue, request))
    }
    fn bounded_mutation<'a>(
        &'a self,
        queue: &'a QueueKey,
        request: BoundedMutationRequest,
    ) -> FacadeFuture<'a, BoundedMutationResponse> {
        Box::pin(RuntimeCore::bounded_mutation(self, queue, request))
    }
}

/// Concrete, backend-erased Fireweed library handle.
pub struct Fireweed {
    inner: Arc<dyn FireweedDataPlane>,
    batch: Arc<dyn FireweedBatchPlane>,
    mutation: Arc<dyn FireweedMutationPlane>,
    projection: Option<ProjectionLifecycleHandle>,
}

impl fmt::Debug for Fireweed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fireweed").finish_non_exhaustive()
    }
}

impl Fireweed {
    pub(crate) fn from_runtime<B: LibBackend + BatchUpdatePort + ItemMutationPort + 'static>(
        queue: RuntimeCore<B>,
    ) -> Self {
        let queue = Arc::new(queue);
        Self {
            inner: queue.clone(),
            batch: queue.clone(),
            mutation: queue,
            projection: None,
        }
    }

    pub(crate) fn from_runtime_with_projection<
        B: LibBackend + BatchUpdatePort + ItemMutationPort + 'static,
    >(
        queue: RuntimeCore<B>,
        projection: ProjectionLifecycleHandle,
    ) -> Self {
        let queue = Arc::new(queue);
        Self {
            inner: queue.clone(),
            batch: queue.clone(),
            mutation: queue,
            projection: Some(projection),
        }
    }

    pub fn projection_control(&self) -> Option<ProjectionControl<'_>> {
        self.projection.as_ref().and_then(|inner| {
            let capabilities = inner.lifecycle_capabilities();
            (capabilities.verify_projection
                || capabilities.delete_projection
                || capabilities.rebuild_projection)
                .then_some(ProjectionControl { inner })
        })
    }

    #[cfg(test)]
    pub(crate) fn test_buffered_group_commit_commands(&self) -> Option<usize> {
        self.projection
            .as_ref()
            .and_then(ProjectionLifecycleHandle::buffered_group_commit_commands)
    }

    pub async fn ownership(&self, queue: &QueueKey) -> EngineResult<Ownership> {
        self.inner.ownership(queue).await
    }
    pub fn renew_owned(&self) -> EngineResult<()> {
        self.inner.renew_owned()
    }
    pub async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        self.inner.create_queue(definition).await
    }
    pub async fn queue_definition(&self, queue: &QueueKey) -> EngineResult<QueueDefinition> {
        self.inner.queue_definition(queue).await
    }
    pub async fn ensure_queue(
        &self,
        queue: &QueueKey,
        template: &QueueTemplate,
    ) -> Result<EnsureQueueOutcome, EnsureQueueError> {
        self.inner.ensure_queue(queue, template).await
    }
    pub async fn push(&self, queue: &QueueKey, item: NewItem) -> EngineResult<ItemId> {
        self.inner.push(queue, item).await
    }
    pub async fn push_with_request_id(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
        item: NewItem,
    ) -> EngineResult<ItemId> {
        self.inner
            .push_with_request_id(queue, request_id, item)
            .await
    }
    pub async fn push_batch(
        &self,
        queue: &QueueKey,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        self.inner.push_batch(queue, items).await
    }
    pub async fn push_batch_with_request_id(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        self.inner
            .push_batch_with_request_id(queue, request_id, items)
            .await
    }
    pub async fn upsert(
        &self,
        queue: &QueueKey,
        key: ClientItemKey,
        item: NewItem,
    ) -> EngineResult<UpsertOutcome> {
        self.inner.upsert(queue, key, item).await
    }
    pub async fn claim_with(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> EngineResult<Vec<ClaimedItem>> {
        self.inner
            .claim_with(queue, max, lease_ms, compatibility)
            .await
    }
    pub async fn claim(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
    ) -> EngineResult<Vec<ClaimedItem>> {
        self.inner.claim(queue, max, lease_ms).await
    }
    pub async fn claim_response_with(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> EngineResult<Claimed> {
        self.inner
            .claim_response_with(queue, max, lease_ms, compatibility)
            .await
    }
    pub async fn claim_at(
        &self,
        queue: &QueueKey,
        request: ClaimAt,
    ) -> EngineResult<Vec<ClaimedItem>> {
        self.inner.claim_at(queue, request).await
    }
    pub async fn claim_response_at(
        &self,
        queue: &QueueKey,
        request: ClaimAt,
    ) -> EngineResult<Claimed> {
        self.inner.claim_response_at(queue, request).await
    }
    pub async fn claim_across_queues(
        &self,
        targets: Vec<MultiQueueClaimTarget>,
        limits: MultiQueueClaimLimits,
    ) -> EngineResult<Vec<MultiQueueClaimResult>> {
        self.inner.claim_across_queues(targets, limits).await
    }
    pub async fn claim_by_query(
        &self,
        queue: &QueueKey,
        request: ClaimByQueryRequest,
    ) -> EngineResult<Claimed> {
        self.inner.claim_by_query(queue, request).await
    }
    pub async fn claim_by_query_at(
        &self,
        queue: &QueueKey,
        request: ClaimByQueryRequest,
        at: ClaimByQueryAt,
    ) -> EngineResult<Claimed> {
        self.inner.claim_by_query_at(queue, request, at).await
    }
    pub async fn ack(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.inner.ack(queue, ids.into_iter().collect()).await
    }
    pub async fn complete(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.inner.complete(queue, ids.into_iter().collect()).await
    }
    pub async fn nack(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        how: Nack,
    ) -> EngineResult<()> {
        self.inner.nack(queue, ids.into_iter().collect(), how).await
    }
    pub async fn retry(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        not_before: Option<UtcTimestamp>,
    ) -> EngineResult<()> {
        self.inner
            .retry(queue, ids.into_iter().collect(), not_before)
            .await
    }
    pub async fn release(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.inner.release(queue, ids.into_iter().collect()).await
    }
    pub async fn nack_retry_after(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        delay_ms: u64,
    ) -> EngineResult<()> {
        self.inner
            .nack_retry_after(queue, ids.into_iter().collect(), delay_ms)
            .await
    }
    pub async fn retry_after(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        delay_ms: u64,
    ) -> EngineResult<()> {
        self.inner
            .retry_after(queue, ids.into_iter().collect(), delay_ms)
            .await
    }
    pub async fn commit(
        &self,
        queue: &QueueKey,
        request: CommitRequest,
    ) -> EngineResult<Vec<EntryOutcome>> {
        self.inner.commit(queue, request).await
    }
    pub async fn commit_multi_claim(
        &self,
        queue: &QueueKey,
        request: MultiClaimCommitRequest,
    ) -> EngineResult<Vec<EntryOutcome>> {
        self.inner.commit_multi_claim(queue, request).await
    }
    pub fn commit_capabilities(&self, queue: &QueueKey) -> EngineResult<CommitCapabilities> {
        self.inner.commit_capabilities(queue)
    }
    pub async fn explain_commit(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
    ) -> EngineResult<Option<CommitRecovery>> {
        self.inner.explain_commit(queue, request_id).await
    }
    pub async fn side_record(&self, queue: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        self.inner.side_record(queue, key).await
    }
    pub async fn peek(&self, queue: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        self.inner.peek(queue, limit).await
    }
    pub async fn current_position(&self, queue: &QueueKey) -> EngineResult<CommandPosition> {
        self.inner.current_position(queue).await
    }
    pub async fn discover_active_scopes(
        &self,
        queue: &QueueKey,
        granularity: DiscoveryGranularity,
    ) -> EngineResult<Vec<ActiveScope>> {
        self.inner.discover_active_scopes(queue, granularity).await
    }
    pub async fn discover_active_scopes_stamped(
        &self,
        queue: &QueueKey,
        granularity: DiscoveryGranularity,
    ) -> EngineResult<ActiveScopeDiscovery> {
        self.inner
            .discover_active_scopes_stamped(queue, granularity)
            .await
    }
    pub async fn discover(
        &self,
        queue: &QueueKey,
        granularity: DiscoveryGranularity,
    ) -> EngineResult<Vec<ActiveScope>> {
        self.inner.discover(queue, granularity).await
    }
    pub async fn live_item(
        &self,
        queue: &QueueKey,
        key: ClientItemKey,
    ) -> EngineResult<Option<LiveItemView>> {
        self.inner.live_item(queue, key).await
    }
    pub async fn live_items(
        &self,
        queue: &QueueKey,
        keys: Vec<ClientItemKey>,
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        self.inner.live_items(queue, keys).await
    }
    pub async fn query_index_unique(
        &self,
        queue: &QueueKey,
        index: &str,
        key: Vec<Vec<u8>>,
    ) -> EngineResult<Option<IndexHit>> {
        self.inner.query_index_unique(queue, index, key).await
    }
    pub async fn query_index(
        &self,
        queue: &QueueKey,
        index: &str,
        key: Vec<Vec<u8>>,
    ) -> EngineResult<Vec<IndexHit>> {
        self.inner.query_index(queue, index, key).await
    }
    pub async fn query_index_unique_typed(
        &self,
        queue: &QueueKey,
        index: &str,
        values: &[serde_json::Value],
    ) -> EngineResult<Option<IndexHit>> {
        self.inner
            .query_index_unique_typed(queue, index, values)
            .await
    }
    pub async fn query_index_typed(
        &self,
        queue: &QueueKey,
        index: &str,
        values: &[serde_json::Value],
    ) -> EngineResult<Vec<IndexHit>> {
        self.inner.query_index_typed(queue, index, values).await
    }
    pub async fn fail(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.inner.fail(queue, ids.into_iter().collect()).await
    }
    pub async fn renew(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        lease_ms: u64,
    ) -> EngineResult<()> {
        self.inner
            .renew(queue, ids.into_iter().collect(), lease_ms)
            .await
    }
    pub async fn reassign(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        lease_ms: u64,
    ) -> EngineResult<()> {
        self.inner
            .reassign(queue, ids.into_iter().collect(), lease_ms)
            .await
    }
    pub async fn update_fields(
        &self,
        queue: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
    ) -> EngineResult<u64> {
        self.inner
            .update_fields(
                queue,
                item_id,
                field_ops,
                payload,
                entity,
                expected_item_version,
            )
            .await
    }

    pub async fn batch_update(
        &self,
        queue: &QueueKey,
        request: BatchUpdateRequest,
    ) -> EngineResult<BatchUpdateResponse> {
        if request.updates.is_empty() {
            return Err(EngineError::Invalid("empty batch update"));
        }
        self.batch.batch_update(queue, request).await
    }
    pub async fn mutate_items(
        &self,
        queue: &QueueKey,
        request: ItemMutationRequest,
    ) -> EngineResult<ItemMutationResponse> {
        self.mutation.mutate_items(queue, request).await
    }
    pub async fn update(
        &self,
        queue: &QueueKey,
        item_id: ItemId,
        priority: ScheduleUpdate<PriorityValue>,
        not_before: ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
    ) -> EngineResult<u64> {
        self.inner
            .update(queue, item_id, priority, not_before, expected_item_version)
            .await
    }
    pub async fn set_gates(
        &self,
        queue: &QueueKey,
        gate_keys: Vec<String>,
        blocked: bool,
    ) -> EngineResult<()> {
        self.inner.set_gates(queue, gate_keys, blocked).await
    }
    pub async fn reclaim_expired(
        &self,
        queue: &QueueKey,
        limit: Option<usize>,
    ) -> EngineResult<Vec<ItemId>> {
        self.inner.reclaim_expired(queue, limit).await
    }
    pub async fn reclaim_expired_at(
        &self,
        queue: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ItemId>> {
        self.inner.reclaim_expired_at(queue, limit, now).await
    }
    pub async fn rearm(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.inner.rearm(queue, ids.into_iter().collect()).await
    }
    pub async fn rearm_at(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        not_before: UtcTimestamp,
    ) -> EngineResult<()> {
        self.inner
            .rearm_at(queue, ids.into_iter().collect(), not_before)
            .await
    }
    pub async fn rearm_after(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        delay_ms: u64,
    ) -> EngineResult<()> {
        self.inner
            .rearm_after(queue, ids.into_iter().collect(), delay_ms)
            .await
    }
    pub async fn purge(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        force: bool,
    ) -> EngineResult<u64> {
        self.inner
            .purge(queue, ids.into_iter().collect(), force)
            .await
    }
    pub async fn claimed(
        &self,
        queue: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<ClaimedItem>> {
        self.inner.claimed(queue, ids).await
    }
    pub async fn metrics(&self, queue: &QueueKey) -> EngineResult<QueueMetrics> {
        self.inner.metrics(queue).await
    }
    pub async fn metrics_by_query(
        &self,
        queue: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        self.inner.metrics_by_query(queue, request).await
    }
    pub fn hot_projection_capabilities(&self, queue: &QueueKey) -> QueryCapabilityFlags {
        self.inner.hot_projection_capabilities(queue)
    }
    pub async fn range_scan(
        &self,
        queue: &QueueKey,
        request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        self.inner.range_scan(queue, request).await
    }
    pub async fn grouped_aggregate(
        &self,
        queue: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        self.inner.grouped_aggregate(queue, request).await
    }
    pub async fn declared_bucket_segment(
        &self,
        queue: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        self.inner.declared_bucket_segment(queue, request).await
    }
    pub async fn bounded_mutation(
        &self,
        queue: &QueueKey,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationResponse> {
        self.inner.bounded_mutation(queue, request).await
    }
}

/// Borrowed maintenance interface for a configured disposable projection.
pub struct ProjectionControl<'a> {
    inner: &'a ProjectionLifecycleHandle,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionControlCapabilities {
    pub verify: bool,
    pub delete: bool,
    pub rebuild: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionVerification {
    pub compatible: bool,
    pub projection_sequence: u64,
    pub authoritative_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionRebuild {
    pub snapshot_used: bool,
    pub tail_commands_replayed: u64,
    pub projection_sequence: u64,
}

impl ProjectionControl<'_> {
    pub fn capabilities(&self) -> ProjectionControlCapabilities {
        let capabilities = self.inner.lifecycle_capabilities();
        ProjectionControlCapabilities {
            verify: capabilities.verify_projection,
            delete: capabilities.delete_projection,
            rebuild: capabilities.rebuild_projection,
        }
    }
    pub async fn verify(&self) -> EngineResult<ProjectionVerification> {
        self.inner
            .verify_projection()
            .await
            .map(|result| ProjectionVerification {
                compatible: result.compatible,
                projection_sequence: result.projection_sequence,
                authoritative_sequence: result.authoritative_sequence,
            })
    }
    pub async fn delete(&self) -> EngineResult<()> {
        self.inner.delete_projection().await
    }
    pub async fn rebuild(&self) -> EngineResult<ProjectionRebuild> {
        self.inner
            .rebuild_projection()
            .await
            .map(|result| ProjectionRebuild {
                snapshot_used: result.snapshot_used,
                tail_commands_replayed: result.tail_commands_replayed,
                projection_sequence: result.projection_sequence,
            })
    }
}
