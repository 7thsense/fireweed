//! Shared product-port surface for LogEngine objectlog compositions.
//!
//! Mirrors the planners/ops that [`fireweed_engine::AsyncLogReplayBackend`] implements so
//! AsyncComposedBackend objectlog products expose the same upsert / batch-update /
//! hot-projection-query / index-query surface (fireweed-dd6cbcde). Callers plan via these
//! helpers, then submit the resulting envelopes through their product commit path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByItemIdClass,
    ClaimByItemIdsDisposition, ClaimByItemIdsOutcome, ClaimByItemIdsRequest, ClaimByQueryRequest,
    ClientItemKey, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, ItemId, ItemState, LeaseToken, Metadata,
    MetricsByQueryRequest, PriorityValue, QueryCapabilityFlags, RangeScanRequest,
    RangeScanResponse, RequestId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncControlPlane, BatchUpdateOutcome, BatchUpdateRequest, BatchUpdateResponse,
    BoundedMutationContext, ClaimByItemIdsResponse, ClaimByQueryContext, ClaimCommand, Claimed,
    ClaimedItem, CommandChecksum, CommandEnvelope, EngineError, EngineResult, IdGen,
    IdempotencyDecision, InProcessControlPlane, InProcessProjectionStore, IndexHit, PayloadUpdate,
    ProjectionStore, PushCommand, PushItem, QueueCommand, QueueCounters, QueueIdempotencyCache,
    QueueKey, QueueMetrics, ReplacePendingCommand, RequestOutcome, UpdateFieldsCommand,
    UpsertOutcome, batch_update_body_hash, claim_by_item_ids_body_hash, claim_by_query_body_hash,
    compile_entity_schema, generate_query_lease_token, plan_batch_update, request_expires_at,
    validate_entity,
};

use crate::async_product::SeqIdGen;

/// Claim-by-query request-id cache.
pub type ClaimByQueryIdempotency =
    Arc<Mutex<HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken)>>>>;

/// Claim-by-item-ids request-id cache.
pub type ClaimByItemIdsIdempotency = Arc<
    Mutex<
        HashMap<
            QueueKey,
            QueueIdempotencyCache<(Vec<ItemId>, LeaseToken, Vec<ClaimByItemIdsOutcome>)>,
        >,
    >,
>;

/// BatchUpdate request-id cache.
pub type BatchUpdateIdempotency =
    Arc<Mutex<HashMap<QueueKey, QueueIdempotencyCache<BatchUpdateResponse>>>>;

pub fn new_claim_by_query_idempotency() -> ClaimByQueryIdempotency {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn new_claim_by_item_ids_idempotency() -> ClaimByItemIdsIdempotency {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn new_batch_update_idempotency() -> BatchUpdateIdempotency {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn make_envelope(
    ids: &SeqIdGen,
    command: QueueCommand,
    item_ids: Vec<ItemId>,
    created_at: UtcTimestamp,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: ids.next_command_id(),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at,
    }
}

enum UpsertPlan {
    Insert(PushItem),
    Replace { existing_id: ItemId, item: PushItem },
}

/// Plan + envelope for a single upsert (`replace_if_pending`).
pub struct PreparedUpsert {
    pub envelopes: Vec<CommandEnvelope>,
    pub outcome: UpsertOutcome,
}

/// Plan a pending-key upsert against the live projection.
#[allow(
    clippy::too_many_arguments,
    reason = "upsert planning keeps every caller-supplied mutation field explicit"
)]
pub async fn prepare_upsert<P>(
    projection: &InProcessProjectionStore<P>,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    counters: &QueueCounters,
    node_id: u8,
    epoch: u64,
    shard: &QueueKey,
    client_item_key: ClientItemKey,
    priority: Option<PriorityValue>,
    group_key: Option<GroupKey>,
    not_before: Option<UtcTimestamp>,
    payload: Option<Bytes>,
    fields: BTreeMap<String, Bytes>,
    metadata: Metadata,
    entity: Option<serde_json::Value>,
    now: UtcTimestamp,
) -> EngineResult<PreparedUpsert>
where
    P: ProjectionStore + Send + 'static,
{
    let def = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    let schema = def
        .entity_schema
        .as_ref()
        .and_then(|esd| esd.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()?;
    validate_entity(schema.as_ref(), entity.as_ref())?;
    let max_attempts = def.retry_policy.max_attempts;
    let counter_base = counters.reserve(shard, epoch, 1);
    let new_item_id = ItemId::mint(epoch, node_id, counter_base);
    let item = PushItem {
        client_item_key: client_item_key.clone(),
        item_id: new_item_id,
        priority,
        not_before,
        group_key,
        max_attempts,
        payload,
        fields,
        metadata,
        cohort_size: None,
        gate_keys: Vec::new(),
        entity_document: entity,
    };

    let plan = projection.with_store(|projection| -> EngineResult<_> {
        let existing = ProjectionStore::lookup_by_key(projection, shard, &client_item_key)?;
        match existing {
            None => {
                ProjectionStore::index_validate(
                    projection,
                    shard,
                    &item.item_id,
                    &item.fields,
                    item.entity_document.as_ref(),
                    None,
                )?;
                Ok(UpsertPlan::Insert(item))
            }
            Some(existing_id) => {
                let state = ProjectionStore::item_state(projection, shard, &existing_id)?
                    .ok_or(EngineError::NotFound)?;
                match state {
                    ItemState::Pending => {
                        ProjectionStore::index_validate_replace(
                            projection,
                            shard,
                            &existing_id,
                            &item,
                        )?;
                        Ok(UpsertPlan::Replace { existing_id, item })
                    }
                    ItemState::Leased => Err(EngineError::Invalid("collision with claimed item")),
                    ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                }
            }
        }
    })?;

    match plan {
        UpsertPlan::Insert(item) => {
            let envelope = make_envelope(
                ids,
                QueueCommand::Push(PushCommand { items: vec![item] }),
                vec![new_item_id],
                now,
            );
            Ok(PreparedUpsert {
                envelopes: vec![envelope],
                outcome: UpsertOutcome::Inserted {
                    item_id: new_item_id,
                },
            })
        }
        UpsertPlan::Replace { existing_id, item } => {
            let envelope = make_envelope(
                ids,
                QueueCommand::ReplacePending(ReplacePendingCommand {
                    client_item_key,
                    superseded_item_id: existing_id,
                    replacement: item,
                }),
                vec![new_item_id],
                now,
            );
            Ok(PreparedUpsert {
                envelopes: vec![envelope],
                outcome: UpsertOutcome::Replaced {
                    new_item_id,
                    superseded_item_id: existing_id,
                },
            })
        }
    }
}

/// Result of planning a batch update (before log append).
pub enum PreparedBatchUpdate {
    Replay(BatchUpdateResponse),
    Proceed {
        envelopes: Vec<CommandEnvelope>,
        response: BatchUpdateResponse,
        request_id: RequestId,
        fingerprint: BodyHash,
        expires_at: UtcTimestamp,
    },
}

/// Plan an API-001 BatchUpdate.
#[allow(
    clippy::too_many_arguments,
    reason = "batch planning keeps projection, authority, and idempotency inputs explicit"
)]
pub async fn prepare_batch_update<P>(
    projection: &InProcessProjectionStore<P>,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    batch_update_idempotency: &BatchUpdateIdempotency,
    supports_gates: bool,
    shard: &QueueKey,
    request: BatchUpdateRequest,
    now: UtcTimestamp,
) -> EngineResult<PreparedBatchUpdate>
where
    P: ProjectionStore + Send + 'static,
{
    if request.updates.is_empty() {
        return Err(EngineError::Invalid("empty batch update"));
    }
    if request.updates.len() > 1_000 {
        return Err(EngineError::BatchTooLarge);
    }

    let fingerprint = batch_update_body_hash(&request)?;
    let definition = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    let expires_at = request_expires_at(now, definition.request_id_retention_ms);
    let refs = request
        .updates
        .iter()
        .map(|update| update.item_ref.clone())
        .collect::<Vec<_>>();
    let request_id = request.request_id.clone();

    match batch_update_idempotency
        .lock()
        .expect("batch_update idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .check(&request_id, fingerprint, now)
    {
        IdempotencyDecision::Replay(response) => {
            return Ok(PreparedBatchUpdate::Replay(response));
        }
        IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
        IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
    }
    if let Some(response) = projection.with_store_mut(|p| {
        ProjectionStore::replay_durable_batch_update(p, shard, &request_id, fingerprint.0, now)
    })? {
        batch_update_idempotency
            .lock()
            .expect("batch_update idempotency poisoned")
            .entry(shard.clone())
            .or_default()
            .record(request_id, fingerprint, response.clone(), expires_at);
        return Ok(PreparedBatchUpdate::Replay(response));
    }

    let snapshot = projection.with_store(|projection| {
        ProjectionStore::batch_update_snapshot(projection, shard, &refs)
    })?;
    let mut plan = plan_batch_update(&definition, supports_gates, request.updates, snapshot);
    let candidate_commands = plan
        .commands
        .iter()
        .map(|(_, command)| command.clone())
        .collect::<Vec<_>>();
    let accepted = projection.with_store(|projection| {
        ProjectionStore::batch_update_preflight(projection, shard, &candidate_commands)
    })?;
    if accepted.len() != candidate_commands.len() {
        return Err(EngineError::Storage(
            "batch update preflight returned a mismatched result count".into(),
        ));
    }
    plan.commands = plan
        .commands
        .into_iter()
        .zip(accepted)
        .filter_map(|((outcome_index, command), accepted)| {
            if accepted {
                Some((outcome_index, command))
            } else {
                plan.outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                None
            }
        })
        .collect();

    let response = BatchUpdateResponse {
        request_id: request_id.clone(),
        results: plan.outcomes,
    };
    let response_payload = serde_json::to_string(&response)
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let mut envelopes = plan
        .commands
        .into_iter()
        .map(|(_, command)| {
            let item_id = command.item_id;
            make_envelope(ids, QueueCommand::UpdateFields(command), vec![item_id], now)
        })
        .collect::<Vec<_>>();
    if envelopes.is_empty() {
        envelopes.push(make_envelope(
            ids,
            QueueCommand::WriteSideRecords(fireweed_engine::WriteSideRecordsCommand::default()),
            Vec::new(),
            now,
        ));
    }
    let marker = envelopes
        .first_mut()
        .expect("batch update always emits a command or marker");
    marker.request_id = Some(request_id.clone());
    marker.request_fingerprint = Some(fingerprint.0);
    marker.request_outcome = Some(RequestOutcome::BatchUpdate { response_payload });

    Ok(PreparedBatchUpdate::Proceed {
        envelopes,
        response,
        request_id,
        fingerprint,
        expires_at,
    })
}

pub fn record_batch_update_idempotency(
    batch_update_idempotency: &BatchUpdateIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: BodyHash,
    response: BatchUpdateResponse,
    expires_at: UtcTimestamp,
) {
    batch_update_idempotency
        .lock()
        .expect("batch_update idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .record(request_id, fingerprint, response, expires_at);
}

/// Index get-unique / lookup over the live projection.
pub fn index_get_unique<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    index: &str,
    key: &[Vec<u8>],
) -> EngineResult<Option<IndexHit>>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::index_get_unique(p, shard, index, key))
}

pub fn index_lookup<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    index: &str,
    key: &[Vec<u8>],
) -> EngineResult<Vec<IndexHit>>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::index_lookup(p, shard, index, key))
}

pub fn hot_projection_capabilities<P>(
    projection: &InProcessProjectionStore<P>,
    _shard: &QueueKey,
) -> QueryCapabilityFlags
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(ProjectionStore::hot_projection_capabilities)
}

pub fn range_scan<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    request: RangeScanRequest,
) -> EngineResult<RangeScanResponse>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::range_scan(p, shard, request))
}

pub fn grouped_aggregate<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    request: GroupedAggregateRequest,
) -> EngineResult<GroupedAggregateResponse>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::grouped_aggregate(p, shard, request))
}

pub fn metrics_by_query<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    request: MetricsByQueryRequest,
) -> EngineResult<QueueMetrics>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::metrics_by_query(p, shard, request))
}

pub fn declared_bucket_segment<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    request: DeclaredBucketSegmentRequest,
) -> EngineResult<DeclaredBucketSegmentResponse>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::declared_bucket_segment(p, shard, request))
}

/// Planned bounded mutation: zero or more UpdateFields envelopes + response.
pub struct PreparedBoundedMutation {
    pub envelopes: Vec<(CommandEnvelope, ItemId, u64)>,
    pub response: BoundedMutationResponse,
}

pub fn prepare_bounded_mutation<P>(
    projection: &InProcessProjectionStore<P>,
    ids: &SeqIdGen,
    shard: &QueueKey,
    request: BoundedMutationRequest,
    context: BoundedMutationContext,
) -> EngineResult<PreparedBoundedMutation>
where
    P: ProjectionStore + Send + 'static,
{
    let plan =
        projection.with_store(|p| ProjectionStore::plan_bounded_mutation(p, shard, request))?;
    let mut envelopes = Vec::with_capacity(plan.updates.len());
    for update in plan.updates {
        let item_id = update.command.item_id;
        let expected = update.expected_item_version;
        projection.with_store(|p| {
            ProjectionStore::update_fields_validate(p, shard, &item_id, Some(expected))?;
            ProjectionStore::index_validate_update(
                p,
                shard,
                &item_id,
                &update.command.field_ops,
                update.command.set_entity_document.as_ref(),
            )
        })?;
        let envelope = make_envelope(
            ids,
            QueueCommand::UpdateFields(update.command),
            vec![item_id],
            context.now,
        );
        envelopes.push((envelope, item_id, expected));
    }
    Ok(PreparedBoundedMutation {
        envelopes,
        response: plan.response,
    })
}

/// Result of planning claim_by_query.
#[allow(
    clippy::large_enum_variant,
    reason = "the internal plan result avoids allocating the single-use proceed path"
)]
pub enum PreparedClaimByQuery {
    Replay(Claimed),
    Proceed {
        envelope: CommandEnvelope,
        item_ids: Vec<ItemId>,
        lease_token: LeaseToken,
        request_id: RequestId,
        fingerprint: BodyHash,
        replay_expires_at: UtcTimestamp,
    },
}

pub async fn prepare_claim_by_query<P>(
    projection: &InProcessProjectionStore<P>,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    claim_by_query_idempotency: &ClaimByQueryIdempotency,
    shard: &QueueKey,
    request: ClaimByQueryRequest,
    context: ClaimByQueryContext,
) -> EngineResult<PreparedClaimByQuery>
where
    P: ProjectionStore + Send + 'static,
{
    let definition = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    if request.max_items == 0 || u64::from(request.max_items) > definition.max_claim_batch_size {
        return Err(EngineError::Invalid("invalid claim_by_query max_items"));
    }
    if request.lease_duration_ms == 0
        || request.lease_duration_ms > definition.max_lease_duration_ms
    {
        return Err(EngineError::Invalid(
            "invalid claim_by_query lease_duration_ms",
        ));
    }
    let request_id = request
        .request_id
        .clone()
        .ok_or(EngineError::Invalid("claim_by_query request_id required"))?;
    let fingerprint = claim_by_query_body_hash(&request)?;
    let expires_at = request_expires_at(context.now, definition.request_id_retention_ms);

    match claim_by_query_idempotency
        .lock()
        .expect("claim_by_query idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .check_conflict_first(&request_id, fingerprint, context.now)
    {
        IdempotencyDecision::Replay((item_ids, lease_token)) => {
            let items =
                projection.with_store(|p| ProjectionStore::render_claimed(p, shard, &item_ids))?;
            if items.len() != item_ids.len()
                || items
                    .iter()
                    .any(|item| item.lease_expires_at <= context.now)
            {
                return Err(EngineError::RequestExpired);
            }
            for item in &items {
                if item.lease_token.as_ref() != Some(&lease_token) {
                    return Err(EngineError::RequestExpired);
                }
            }
            return Ok(PreparedClaimByQuery::Replay(Claimed {
                items,
                ..Default::default()
            }));
        }
        IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
        IdempotencyDecision::Expired => return Err(EngineError::RequestExpired),
        IdempotencyDecision::Proceed => {}
    }

    let eligible: HashSet<ItemId> = projection
        .with_store(|p| {
            ProjectionStore::eligible_candidates(p, shard, context.eligibility_at(), usize::MAX)
        })?
        .into_iter()
        .collect();
    let page_size = request.max_items.clamp(1, 1_000);
    let mut cursor = None;
    let mut item_ids = Vec::new();
    while item_ids.len() < request.max_items as usize {
        let page = projection.with_store(|p| {
            ProjectionStore::range_scan(
                p,
                shard,
                RangeScanRequest {
                    index: request.index.clone(),
                    filters: request.filters.clone(),
                    order_by: vec![request.order_by.clone()],
                    page_size,
                    cursor,
                },
            )
        })?;
        item_ids.extend(
            page.rows
                .into_iter()
                .map(|row| row.item_id)
                .filter(|item_id| eligible.contains(item_id)),
        );
        item_ids.truncate(request.max_items as usize);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    let lease_expires_at = context.lease_expires_at(request.lease_duration_ms);
    let (lease_token, claim_item_ids) = if item_ids.is_empty() {
        (
            LeaseToken::new("empty-claim").expect("valid token"),
            Vec::new(),
        )
    } else {
        (generate_query_lease_token()?, item_ids.clone())
    };

    let mut envelope = make_envelope(
        ids,
        QueueCommand::Claim(ClaimCommand {
            item_ids: claim_item_ids.clone(),
            lease_token: lease_token.clone(),
            lease_expires_at,
            worker_id: Some(request.worker_id.clone()),
        }),
        claim_item_ids.clone(),
        context.now,
    );
    envelope.request_id = Some(request_id.clone());
    envelope.request_fingerprint = Some(fingerprint.0);
    envelope.request_outcome = Some(RequestOutcome::ClaimByQuery {
        item_ids: claim_item_ids.clone(),
        lease_token: lease_token.clone(),
        worker_id: Some(request.worker_id.clone()),
    });

    let replay_expires_at = if claim_item_ids.is_empty() {
        expires_at
    } else {
        expires_at.max(lease_expires_at)
    };

    Ok(PreparedClaimByQuery::Proceed {
        envelope,
        item_ids: claim_item_ids,
        lease_token,
        request_id,
        fingerprint,
        replay_expires_at,
    })
}

pub fn record_claim_by_query_idempotency(
    claim_by_query_idempotency: &ClaimByQueryIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: BodyHash,
    item_ids: Vec<ItemId>,
    lease_token: LeaseToken,
    replay_expires_at: UtcTimestamp,
) {
    claim_by_query_idempotency
        .lock()
        .expect("claim_by_query idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .record(
            request_id,
            fingerprint,
            (item_ids, lease_token),
            replay_expires_at,
        );
}

pub fn render_claimed<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    item_ids: &[ItemId],
) -> EngineResult<Vec<ClaimedItem>>
where
    P: ProjectionStore + Send + 'static,
{
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    projection.with_store(|p| ProjectionStore::render_claimed(p, shard, item_ids))
}

/// Result of planning claim_by_item_ids.
#[allow(
    clippy::large_enum_variant,
    reason = "the internal plan result avoids allocating the single-use proceed path"
)]
pub enum PreparedClaimByItemIds {
    Replay(ClaimByItemIdsResponse),
    Proceed {
        envelope: CommandEnvelope,
        claim_item_ids: Vec<ItemId>,
        lease_token: LeaseToken,
        outcomes: Vec<ClaimByItemIdsOutcome>,
        request_id: RequestId,
        fingerprint: BodyHash,
        replay_expires_at: UtcTimestamp,
    },
}

pub async fn prepare_claim_by_item_ids<P>(
    projection: &InProcessProjectionStore<P>,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    claim_by_item_ids_idempotency: &ClaimByItemIdsIdempotency,
    shard: &QueueKey,
    request: ClaimByItemIdsRequest,
    context: ClaimByQueryContext,
) -> EngineResult<PreparedClaimByItemIds>
where
    P: ProjectionStore + Send + 'static,
{
    let definition = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    if request.item_ids.is_empty() {
        return Err(EngineError::Invalid("claim_by_item_ids item_ids required"));
    }
    let mut seen = HashSet::new();
    let distinct: Vec<ItemId> = request
        .item_ids
        .iter()
        .copied()
        .filter(|id| seen.insert(*id))
        .collect();
    if u64::try_from(distinct.len()).unwrap_or(u64::MAX) > definition.max_claim_batch_size {
        return Err(EngineError::Invalid(
            "claim_by_item_ids exceeds max_claim_batch_size",
        ));
    }
    if request.lease_duration_ms == 0
        || request.lease_duration_ms > definition.max_lease_duration_ms
    {
        return Err(EngineError::Invalid(
            "invalid claim_by_item_ids lease_duration_ms",
        ));
    }
    let request_id = request.request_id.clone();
    let fingerprint = claim_by_item_ids_body_hash(&request)?;
    let expires_at = request_expires_at(context.now, definition.request_id_retention_ms);

    match claim_by_item_ids_idempotency
        .lock()
        .expect("claim_by_item_ids idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .check_conflict_first(&request_id, fingerprint, context.now)
    {
        IdempotencyDecision::Replay((claimed_ids, lease_token, outcomes)) => {
            let items = if claimed_ids.is_empty() {
                Vec::new()
            } else {
                projection
                    .with_store(|p| ProjectionStore::render_claimed(p, shard, &claimed_ids))?
            };
            if items.len() != claimed_ids.len()
                || items
                    .iter()
                    .any(|item| item.lease_expires_at <= context.now)
            {
                return Err(EngineError::RequestExpired);
            }
            for item in &items {
                if item.lease_token.as_ref() != Some(&lease_token) {
                    return Err(EngineError::RequestExpired);
                }
            }
            return Ok(PreparedClaimByItemIds::Replay(ClaimByItemIdsResponse {
                items,
                outcomes,
            }));
        }
        IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
        IdempotencyDecision::Expired => return Err(EngineError::RequestExpired),
        IdempotencyDecision::Proceed => {}
    }

    let eligibility_at = context.eligibility_at();
    let mut outcomes = Vec::with_capacity(distinct.len());
    let mut claimable: Vec<ItemId> = Vec::new();
    for item_id in &distinct {
        let class = projection.with_store(|p| {
            ProjectionStore::classify_claim_by_item_id(p, shard, item_id, eligibility_at)
        })?;
        match class {
            ClaimByItemIdClass::Claimable => {
                claimable.push(*item_id);
                outcomes.push(ClaimByItemIdsOutcome {
                    item_id: *item_id,
                    disposition: ClaimByItemIdsDisposition::Claimed,
                });
            }
            other => {
                outcomes.push(ClaimByItemIdsOutcome {
                    item_id: *item_id,
                    disposition: other.into(),
                });
            }
        }
    }

    let lease_expires_at = context.lease_expires_at(request.lease_duration_ms);
    let (lease_token, claim_item_ids) = if claimable.is_empty() {
        (
            request.lease_token.clone().unwrap_or_else(|| {
                LeaseToken::new("empty-claim-by-item-ids").expect("valid token")
            }),
            Vec::new(),
        )
    } else if let Some(token) = request.lease_token.clone() {
        (token, claimable)
    } else {
        (generate_query_lease_token()?, claimable)
    };

    let mut envelope = make_envelope(
        ids,
        QueueCommand::Claim(ClaimCommand {
            item_ids: claim_item_ids.clone(),
            lease_token: lease_token.clone(),
            lease_expires_at,
            worker_id: Some(request.worker_id.clone()),
        }),
        claim_item_ids.clone(),
        context.now,
    );
    envelope.request_id = Some(request_id.clone());
    envelope.request_fingerprint = Some(fingerprint.0);
    envelope.request_outcome = Some(RequestOutcome::ClaimByItemIds {
        claimed_item_ids: claim_item_ids.clone(),
        lease_token: lease_token.clone(),
        outcomes: outcomes.clone(),
        worker_id: Some(request.worker_id.clone()),
    });

    let replay_expires_at = if claim_item_ids.is_empty() {
        expires_at
    } else {
        expires_at.max(lease_expires_at)
    };

    Ok(PreparedClaimByItemIds::Proceed {
        envelope,
        claim_item_ids,
        lease_token,
        outcomes,
        request_id,
        fingerprint,
        replay_expires_at,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the idempotency record mirrors the complete durable claim outcome"
)]
pub fn record_claim_by_item_ids_idempotency(
    claim_by_item_ids_idempotency: &ClaimByItemIdsIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: BodyHash,
    claim_item_ids: Vec<ItemId>,
    lease_token: LeaseToken,
    outcomes: Vec<ClaimByItemIdsOutcome>,
    replay_expires_at: UtcTimestamp,
) {
    claim_by_item_ids_idempotency
        .lock()
        .expect("claim_by_item_ids idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .record(
            request_id,
            fingerprint,
            (claim_item_ids, lease_token, outcomes),
            replay_expires_at,
        );
}

/// Plan update_fields (validate + envelope). Returns new item_version after apply is caller's job.
#[allow(
    clippy::too_many_arguments,
    reason = "update planning keeps the complete public mutation request explicit"
)]
pub async fn prepare_update_fields<P>(
    projection: &InProcessProjectionStore<P>,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    shard: &QueueKey,
    item_id: ItemId,
    field_ops: BTreeMap<String, Option<Bytes>>,
    payload: PayloadUpdate,
    entity: Option<serde_json::Value>,
    expected_item_version: Option<u64>,
    now: UtcTimestamp,
) -> EngineResult<CommandEnvelope>
where
    P: ProjectionStore + Send + 'static,
{
    use fireweed_engine::validate_api001_reserved_write_fields;
    validate_api001_reserved_write_fields(&field_ops)?;
    let def = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    let schema = def
        .entity_schema
        .as_ref()
        .and_then(|esd| esd.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()?;
    validate_entity(schema.as_ref(), entity.as_ref())?;
    projection.with_store(|p| {
        ProjectionStore::update_fields_validate(p, shard, &item_id, expected_item_version)?;
        ProjectionStore::index_validate_update(p, shard, &item_id, &field_ops, entity.as_ref())
    })?;
    Ok(make_envelope(
        ids,
        QueueCommand::UpdateFields(UpdateFieldsCommand {
            item_id,
            field_ops,
            payload,
            set_priority: Default::default(),
            set_not_before: Default::default(),
            set_entity_document: entity,
            set_fields: None,
            set_metadata: None,
            set_gate_keys: None,
            api001_batch: false,
        }),
        vec![item_id],
        now,
    ))
}

/// Plan a reschedule (priority/not_before) UpdateFields envelope.
#[allow(
    clippy::too_many_arguments,
    reason = "reschedule planning keeps both schedule dimensions and version fence explicit"
)]
pub fn prepare_reschedule<P>(
    projection: &InProcessProjectionStore<P>,
    ids: &SeqIdGen,
    shard: &QueueKey,
    item_id: ItemId,
    set_priority: fireweed_engine::ScheduleUpdate<PriorityValue>,
    set_not_before: fireweed_engine::ScheduleUpdate<UtcTimestamp>,
    expected_item_version: Option<u64>,
    now: UtcTimestamp,
) -> EngineResult<CommandEnvelope>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| {
        ProjectionStore::update_fields_validate(p, shard, &item_id, expected_item_version)
    })?;
    Ok(make_envelope(
        ids,
        QueueCommand::UpdateFields(UpdateFieldsCommand {
            item_id,
            field_ops: BTreeMap::new(),
            payload: PayloadUpdate::Keep,
            set_priority,
            set_not_before,
            set_entity_document: None,
            set_fields: None,
            set_metadata: None,
            set_gate_keys: None,
            api001_batch: false,
        }),
        vec![item_id],
        now,
    ))
}

pub fn item_version_after<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    item_id: ItemId,
) -> EngineResult<u64>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| {
        ProjectionStore::item_version(p, shard, &item_id)?.ok_or(EngineError::NotFound)
    })
}
