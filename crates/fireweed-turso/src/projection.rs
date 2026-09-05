// The engine port deliberately spells futures as RPITIT; mirror that signature without refining the
// implementation's public return type to `async fn`.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, CohortId, GroupKey, IndexDeclaration, ItemId, ItemState, LeaseToken, Metadata,
    QueueDefinition, QueueIndex, RequestId, UtcTimestamp, WorkerId, is_retry_exhausted,
};
use fireweed_engine::{
    AsyncProjectionStore, BatchUpdateResponse, BatchUpdateSnapshotItem, ClaimCompatibility,
    ClaimRef, ClaimUnit, ClaimedItem, CohortLeaseTarget, CommandEnvelope, CommandPosition,
    CreateQueueOutcome, EngineError, EngineResult, FinalizeKind, FinalizeTarget,
    IdempotencyDecision, IndexHit, ItemMutationPlan, ItemMutationRequest, ItemMutationResponse,
    ItemView, LeaseView, LiveItemView, PayloadUpdate, PendingPage, PendingSummary, PushFingerprint,
    PushItem, QueueCommand, QueueKey, QueueMetrics, RenewTarget, RequestOutcome,
    ResolvedItemMutationAction, RichClaimSelection, ScheduleUpdate, TerminalEmissionMetrics,
    UpdateFieldsCommand,
};
use fireweed_projection::{ProjectionData, ProjectionImage, ProjectionImageItem};
use fireweed_relational::{
    RelRow, RelTx, RelValue, TokenOp, async_projection as sql, elig_sort, entity_from_json,
    fields_from_json, fields_to_json, lease_hash, metadata_from_json, metadata_to_json, nanos_ts,
    parse_priority, parse_state, ts_nanos, ts_nanos_opt,
};
use tokio::sync::Mutex;
use turso::{Connection, Value, transaction::TransactionBehavior};

use crate::{
    TursoApplyPhaseObservation, TursoRelational,
    local::{ConsumerLeaseIndex, TursoBatchUpdateStatementShape},
};

fn storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
}

fn text(value: &Value) -> EngineResult<String> {
    match value {
        Value::Text(value) => Ok(value.clone()),
        other => Err(storage(format!("expected text, got {other:?}"))),
    }
}

fn integer(value: &Value) -> EngineResult<i64> {
    match value {
        Value::Integer(value) => Ok(*value),
        other => Err(storage(format!("expected integer, got {other:?}"))),
    }
}

fn blob(value: &Value) -> EngineResult<Vec<u8>> {
    match value {
        Value::Blob(value) => Ok(value.clone()),
        other => Err(storage(format!("expected blob, got {other:?}"))),
    }
}

fn nonnegative_u64(value: i64, field: &str) -> EngineResult<u64> {
    u64::try_from(value).map_err(|_| storage(format!("negative or invalid {field}: {value}")))
}

fn nonnegative_u32(value: i64, field: &str) -> EngineResult<u32> {
    u32::try_from(value).map_err(|_| storage(format!("negative or invalid {field}: {value}")))
}

fn optional_text(value: &Value) -> EngineResult<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value.clone())),
        other => Err(storage(format!("expected optional text, got {other:?}"))),
    }
}

fn optional_integer(value: &Value) -> EngineResult<Option<i64>> {
    match value {
        Value::Null => Ok(None),
        Value::Integer(value) => Ok(Some(*value)),
        other => Err(storage(format!("expected optional integer, got {other:?}"))),
    }
}

fn optional_blob(value: &Value) -> EngineResult<Option<Vec<u8>>> {
    match value {
        Value::Null => Ok(None),
        Value::Blob(value) => Ok(Some(value.clone())),
        other => Err(storage(format!("expected optional blob, got {other:?}"))),
    }
}

async fn one_row(
    connection: &Connection,
    query: &str,
    params: Vec<Value>,
) -> EngineResult<Option<Vec<Value>>> {
    let mut rows = connection.query(query, params).await.map_err(storage)?;
    let Some(row) = rows.next().await.map_err(storage)? else {
        return Ok(None);
    };
    let mut values = Vec::with_capacity(row.column_count());
    for index in 0..row.column_count() {
        values.push(row.get_value(index).map_err(storage)?);
    }
    Ok(Some(values))
}

async fn ensure_shard_owned(
    writer: Arc<Mutex<Connection>>,
    definition: QueueDefinition,
) -> EngineResult<CreateQueueOutcome> {
    let mut connection = writer.lock().await;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .map_err(storage)?;
    let tenant = definition.tenant_id.as_str().to_string();
    let queue = definition.queue_id.as_str().to_string();
    let encoded = serde_json::to_string(&definition).map_err(storage)?;
    let created = transaction
        .execute(
            sql::INSERT_QUEUE_IF_ABSENT,
            vec![
                Value::Text(tenant.clone()),
                Value::Text(queue.clone()),
                Value::Text(encoded),
            ],
        )
        .await
        .map_err(storage)?
        == 1;
    let row = one_row(
        &transaction,
        sql::SELECT_QUEUE_DEFINITION,
        vec![tenant.clone().into(), queue.clone().into()],
    )
    .await?
    .ok_or_else(|| storage("queue insert-or-read returned no durable definition"))?;
    let stored: QueueDefinition = serde_json::from_str(&text(&row[0])?).map_err(storage)?;
    if stored != definition {
        transaction.rollback().await.map_err(storage)?;
        return Err(EngineError::QueueDefinitionConflict);
    }
    if created {
        transaction
            .execute(
                sql::INSERT_CURSOR_IF_ABSENT,
                vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
            )
            .await
            .map_err(storage)?;
    }
    let cursor = one_row(
        &transaction,
        sql::SELECT_CURSOR_STATE,
        vec![tenant.into(), queue.into()],
    )
    .await?
    .ok_or_else(|| storage("queue exists without its relational cursor"))?;
    for (index, name) in ["next_seq", "next_item_seq", "assignment_epoch"]
        .into_iter()
        .enumerate()
    {
        nonnegative_u64(integer(&cursor[index])?, name)?;
    }
    transaction.commit().await.map_err(storage)?;
    Ok(CreateQueueOutcome {
        created,
        definition: stored,
    })
}

fn validate_minimal_command(envelope: &CommandEnvelope) -> EngineResult<()> {
    match &envelope.command {
        QueueCommand::CreateQueue(_)
        | QueueCommand::Claim(_)
        | QueueCommand::RenewLease(_)
        | QueueCommand::ReassignLease(_)
        | QueueCommand::Finalize(_)
        | QueueCommand::LeaseExpired(_)
        | QueueCommand::FenceLease(_)
        | QueueCommand::UnfenceLease(_)
        | QueueCommand::ReplacePending(_)
        | QueueCommand::UpdateFields(_)
        | QueueCommand::UpdateFieldsBatch(_)
        | QueueCommand::PauseQueue(_)
        | QueueCommand::ResumeQueue
        | QueueCommand::PurgeItems(_)
        | QueueCommand::SetGates(_)
        | QueueCommand::WriteSideRecords(_)
        | QueueCommand::AdvanceInstanceFence(_)
        | QueueCommand::MutateItems(_) => {}
        QueueCommand::Push(_)
        | QueueCommand::CohortClaim(_)
        | QueueCommand::CohortRenewLease(_)
        | QueueCommand::CohortFinalize(_)
        | QueueCommand::CohortExpired(_) => {}
    }
    Ok(())
}

fn cohort_id_for(group_key: &str, now: i64) -> String {
    format!("coh:{group_key}:{now}")
}

fn cohort_retention_until(definition: &QueueDefinition, now: i64) -> i64 {
    now.saturating_add(
        i64::try_from(definition.terminal_retention_ms)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000),
    )
}

fn index_is_unique(index: &QueueIndex) -> bool {
    match &index.declaration {
        IndexDeclaration::Single(definition) => definition.unique,
        IndexDeclaration::Compound(definition) => definition.unique,
    }
}

// Stay below SQLite's conservative 999-variable profile even when Turso is
// configured with a larger limit. Each accepted push lowers to one statement
// per bounded chunk, never one future/statement per item, gate, or index row.
const PUSH_ITEM_CHUNK: usize = 47; // 47 * 19 binds = 893
const PUSH_GATE_CHUNK: usize = 225; // 225 * 4 binds = 900
const PUSH_INDEX_CHUNK: usize = 180; // 180 * 5 binds = 900
const UNIQUE_CHECK_CHUNK: usize = 448; // 2 common + 448 * 2 binds = 898
const GROUP_SUMMARY_CHUNK: usize = 897; // tenant + queue + now + 897 group binds = 900
const VALIDATION_ITEM_CHUNK: usize = 897; // tenant + queue + 897 item-id binds = 899
const PUSH_IDENTITY_CHECK_CHUNK: usize = 448; // tenant + queue + now + 448 * 2 inputs = 899
const GROUP_COUNT_CHUNK: usize = 898; // tenant + queue + 898 group binds = 900
const COHORT_READ_CHUNK: usize = 898; // tenant + queue + 898 group binds = 900
const COHORT_GENERATION_WRITE_CHUNK: usize = 90; // 90 * 10 row binds = 900
const COHORT_ACTIVE_WRITE_CHUNK: usize = 224; // tenant + queue + 224 * 4 updates = 898
const SCHEDULE_UPDATE_CHUNK: usize = 299; // tenant + queue + 299 * 3 updates = 899
const GATE_BLOCK_WRITE_CHUNK: usize = 300; // 300 * 3 row binds = 900
const GATE_UNBLOCK_WRITE_CHUNK: usize = 898; // tenant + queue + 898 gate binds = 900
const SIDE_RECORD_WRITE_CHUNK: usize = 225; // 225 * 4 row binds = 900
const KEY_RETENTION_WRITE_CHUNK: usize = 180; // 180 * 5 row binds = 900
const CURSOR_UPDATE_CHUNK: usize = 225; // 225 * 4 row binds = 900
const API001_UPDATE_CHUNK: usize = 89; // tenant + queue + 89 * 10 row binds = 892
const API001_GATE_DELETE_CHUNK: usize = 898; // tenant + queue + 898 item binds = 900

fn values_rows(rows: usize, columns: usize) -> String {
    let row = format!("({})", vec!["?"; columns].join(","));
    vec![row; rows].join(",")
}

fn numbered_values_rows(rows: usize, columns: usize, first_bind: usize) -> String {
    (0..rows)
        .map(|row| {
            let offset = first_bind + row * columns;
            format!(
                "({})",
                (0..columns)
                    .map(|column| format!("?{}", offset + column))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

async fn validation_rows_by_item(
    connection: &Connection,
    tenant: &str,
    queue: &str,
    ids: &[ItemId],
    columns: &str,
) -> EngineResult<HashMap<ItemId, Vec<Value>>> {
    debug_assert!(ids.len() <= VALIDATION_ITEM_CHUNK);
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = (0..ids.len())
        .map(|index| format!("?{}", index + 3))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "SELECT item_id,{columns} FROM fireweed_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND item_id IN ({placeholders})"
    );
    let mut params = Vec::with_capacity(ids.len() + 2);
    params.push(Value::Text(tenant.to_string()));
    params.push(Value::Text(queue.to_string()));
    params.extend(ids.iter().map(|id| Value::Text(id.to_string())));
    let mut rows = connection.query(&query, params).await.map_err(storage)?;
    let mut by_item = HashMap::with_capacity(ids.len());
    while let Some(row) = rows.next().await.map_err(storage)? {
        let item_id = ItemId::new(row.get::<String>(0).map_err(storage)?).map_err(storage)?;
        let mut values = Vec::with_capacity(row.column_count().saturating_sub(1));
        for index in 1..row.column_count() {
            values.push(row.get_value(index).map_err(storage)?);
        }
        by_item.insert(item_id, values);
    }
    Ok(by_item)
}

fn typed_index_keys(
    indexes: &[QueueIndex],
    index_fields: &std::collections::BTreeMap<String, fireweed_core::TypedValue>,
    entity: Option<&serde_json::Value>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    fireweed_engine::index_fields::typed_index_keys_for_item(indexes, index_fields, entity)
}

async fn check_typed_unique_conflicts(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    indexes: &[QueueIndex],
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    let unique = keys
        .iter()
        .filter(|(name, _)| {
            indexes
                .iter()
                .find(|index| index.name == *name)
                .is_some_and(index_is_unique)
        })
        .collect::<Vec<_>>();
    for chunk in unique.chunks(UNIQUE_CHECK_CHUNK) {
        let mut params = Vec::with_capacity(chunk.len() * 2 + 2);
        for (name, key) in chunk {
            params.extend([Value::Text(name.clone()), Value::Blob(key.clone())]);
        }
        params.extend([
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
        ]);
        if one_row(
            transaction,
            &format!(
                "WITH incoming(index_name,index_key) AS (VALUES {}) \
                 SELECT 1 FROM fireweed_item_index existing JOIN incoming \
                 ON existing.index_name=incoming.index_name AND existing.index_key=incoming.index_key \
                 WHERE existing.tenant_id=? AND existing.queue_id=? LIMIT 1",
                values_rows(chunk.len(), 2)
            ),
            params,
        )
        .await?
        .is_some()
        {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

async fn insert_typed_index_rows(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    item_id: &str,
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    for chunk in keys.chunks(PUSH_INDEX_CHUNK) {
        let mut params = Vec::with_capacity(chunk.len() * 5);
        for (name, key) in chunk {
            params.extend([
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Text(name.clone()),
                Value::Blob(key.clone()),
                Value::Text(item_id.to_string()),
            ]);
        }
        transaction
            .execute(
                format!(
                    "INSERT INTO fireweed_item_index \
                     (tenant_id,queue_id,index_name,index_key,item_id) VALUES {} \
                     ON CONFLICT(tenant_id,queue_id,index_name,item_id) DO UPDATE SET \
                     index_key=excluded.index_key",
                    values_rows(chunk.len(), 5)
                ),
                params,
            )
            .await
            .map_err(storage)?;
    }
    Ok(())
}

async fn delete_typed_index_rows(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    ids: &[ItemId],
) -> EngineResult<()> {
    execute_for_items(
        transaction,
        sql::delete_item_indexes,
        vec![tenant.to_string().into(), queue.to_string().into()],
        ids,
    )
    .await
    .map(|_| ())
}

async fn replace_typed_indexes_for_entity(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    indexes: &[QueueIndex],
    item_id: ItemId,
    entity: &serde_json::Value,
) -> EngineResult<std::collections::BTreeMap<String, fireweed_core::TypedValue>> {
    let extracted =
        fireweed_engine::index_fields::extract_index_fields_from_entity(indexes, entity)?;
    let keys = typed_index_keys(indexes, &extracted, None)?;
    delete_typed_index_rows(transaction, tenant, queue, std::slice::from_ref(&item_id)).await?;
    check_typed_unique_conflicts(transaction, tenant, queue, indexes, &keys).await?;
    insert_typed_index_rows(transaction, tenant, queue, &item_id.to_string(), &keys).await?;
    Ok(extracted)
}

async fn maintain_typed_indexes_on_insert(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    indexes: &[QueueIndex],
    items: &[PushItem],
    persist: bool,
) -> EngineResult<()> {
    let mut batch_unique: HashMap<(String, Vec<u8>), String> = HashMap::new();
    let mut rows = Vec::with_capacity(items.len());
    let mut unique_rows = Vec::new();
    for item in items {
        let item_id = item.item_id.to_string();
        let keys = typed_index_keys(indexes, &item.index_fields, item.entity_document.as_ref())?;
        for (name, key) in &keys {
            let unique = indexes
                .iter()
                .find(|index| index.name == *name)
                .is_some_and(index_is_unique);
            if !unique {
                continue;
            }
            let batch_key = (name.clone(), key.clone());
            if batch_unique
                .insert(batch_key, item_id.clone())
                .is_some_and(|previous| previous != item_id)
            {
                return Err(EngineError::Conflict);
            }
            unique_rows.push((name.clone(), key.clone()));
        }
        rows.push((item_id, keys));
    }

    for chunk in unique_rows.chunks(UNIQUE_CHECK_CHUNK) {
        let mut parameters: Vec<Value> = Vec::with_capacity(2 + chunk.len() * 2);
        for (name, key) in chunk {
            parameters.push(name.clone().into());
            parameters.push(Value::Blob(key.clone()));
        }
        parameters.push(tenant.to_string().into());
        parameters.push(queue.to_string().into());
        let query = format!(
            "WITH incoming(index_name,index_key) AS (VALUES {}) \
             SELECT 1 FROM fireweed_item_index existing JOIN incoming \
             ON existing.index_name=incoming.index_name AND existing.index_key=incoming.index_key \
             WHERE existing.tenant_id=? AND existing.queue_id=? LIMIT 1",
            values_rows(chunk.len(), 2)
        );
        if one_row(transaction, &query, parameters).await?.is_some() {
            return Err(EngineError::Conflict);
        }
    }

    if !persist {
        return Ok(());
    }
    let rows: Vec<_> = rows
        .into_iter()
        .flat_map(|(item_id, keys)| {
            keys.into_iter()
                .map(move |(name, key)| (item_id.clone(), name, key))
        })
        .collect();
    for chunk in rows.chunks(PUSH_INDEX_CHUNK) {
        let mut parameters: Vec<Value> = Vec::with_capacity(chunk.len() * 5);
        for (item_id, name, key) in chunk {
            parameters.extend([
                tenant.to_string().into(),
                queue.to_string().into(),
                name.clone().into(),
                Value::Blob(key.clone()),
                item_id.clone().into(),
            ]);
        }
        transaction
            .execute(
                format!(
                    "INSERT INTO fireweed_item_index \
                     (tenant_id,queue_id,index_name,index_key,item_id) VALUES {} \
                     ON CONFLICT(tenant_id,queue_id,index_name,item_id) DO UPDATE SET \
                     index_key=excluded.index_key",
                    values_rows(chunk.len(), 5)
                ),
                parameters,
            )
            .await
            .map_err(storage)?;
    }
    Ok(())
}

async fn upsert_cohorts(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    items: &[PushItem],
    now: i64,
) -> EngineResult<()> {
    let mut cohort_order = Vec::new();
    let mut cohorts: HashMap<String, (i64, i64)> = HashMap::new();
    for item in items {
        if let (Some(group), Some(size)) = (&item.group_key, item.cohort_size) {
            let size = i64::try_from(size).map_err(|_| EngineError::Conflict)?;
            let group = group.as_str().to_string();
            let entry = cohorts.entry(group.clone()).or_insert_with(|| {
                cohort_order.push(group);
                (size, 0)
            });
            if entry.0 != size {
                return Err(EngineError::Conflict);
            }
            entry.1 += 1;
        }
    }
    let mut generation_rows = Vec::new();
    let mut active_rows = Vec::new();
    for groups in cohort_order.chunks(COHORT_READ_CHUNK) {
        let placeholders = (0..groups.len())
            .map(|offset| format!("?{}", offset + 3))
            .collect::<Vec<_>>()
            .join(",");
        let mut params = vec![tenant.to_string().into(), queue.to_string().into()];
        params.extend(groups.iter().cloned().map(Value::Text));
        let mut rows = transaction
            .query(
                format!(
                    "SELECT group_key,cohort_size,member_count,state,retention_until \
                     FROM fireweed_cohorts WHERE tenant_id=?1 AND queue_id=?2 \
                     AND group_key IN ({placeholders})"
                ),
                params,
            )
            .await
            .map_err(storage)?;
        let mut existing = HashMap::with_capacity(groups.len());
        while let Some(row) = rows.next().await.map_err(storage)? {
            existing.insert(
                row.get::<String>(0).map_err(storage)?,
                (
                    row.get::<i64>(1).map_err(storage)?,
                    row.get::<i64>(2).map_err(storage)?,
                    row.get::<String>(3).map_err(storage)?,
                    optional_integer(&row.get_value(4).map_err(storage)?)?,
                ),
            );
        }
        for group in groups {
            let (size, added) = cohorts[group];
            match existing.get(group) {
                None => {
                    if added > size {
                        return Err(EngineError::Conflict);
                    }
                    let state = if added >= size { "complete" } else { "forming" };
                    generation_rows.push((
                        group.clone(),
                        cohort_id_for(group, now),
                        size,
                        added,
                        state,
                    ));
                }
                Some((old_size, old_count, old_state, retention)) if old_state == "terminal" => {
                    if retention.is_some_and(|until| until > now) || added > size {
                        return Err(EngineError::Conflict);
                    }
                    let state = if added >= size { "complete" } else { "forming" };
                    generation_rows.push((
                        group.clone(),
                        cohort_id_for(group, now),
                        size,
                        added,
                        state,
                    ));
                }
                Some((old_size, old_count, old_state, _)) => {
                    if *old_size != size || old_count.saturating_add(added) > *old_size {
                        return Err(EngineError::Conflict);
                    }
                    let count = old_count + added;
                    let state = if old_state == "leased" {
                        "leased"
                    } else if count >= *old_size {
                        "complete"
                    } else {
                        "forming"
                    };
                    active_rows.push((group.clone(), count, state));
                }
            }
        }
    }

    for chunk in generation_rows.chunks(COHORT_GENERATION_WRITE_CHUNK) {
        let mut params = Vec::with_capacity(chunk.len() * 10);
        for (group, cohort_id, size, count, state) in chunk {
            params.extend([
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Text(group.clone()),
                Value::Text(cohort_id.clone()),
                Value::Integer(*size),
                Value::Integer(*count),
                Value::Text((*state).to_string()),
                Value::Integer(now),
                if *state == "complete" {
                    Value::Integer(now)
                } else {
                    Value::Null
                },
                Value::Integer(now),
            ]);
        }
        transaction
            .execute(
                format!(
                    "INSERT INTO fireweed_cohorts \
                     (tenant_id,queue_id,group_key,cohort_id,cohort_size,member_count,state,\
                      cohort_created_at,first_eligible_at,created_at) VALUES {} \
                     ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
                      cohort_id=excluded.cohort_id,cohort_size=excluded.cohort_size,\
                      member_count=excluded.member_count,state=excluded.state,\
                      cohort_created_at=excluded.cohort_created_at,\
                      first_eligible_at=excluded.first_eligible_at,expire_command_pos=NULL,\
                      cohort_lease_token_hash=NULL,retention_until=NULL,created_at=excluded.created_at",
                    values_rows(chunk.len(), 10)
                ),
                params,
            )
            .await
            .map_err(storage)?;
    }
    for chunk in active_rows.chunks(COHORT_ACTIVE_WRITE_CHUNK) {
        let mut params = Vec::with_capacity(chunk.len() * 4 + 2);
        for (group, count, state) in chunk {
            params.extend([
                Value::Text(group.clone()),
                Value::Integer(*count),
                Value::Text((*state).to_string()),
                if *state == "complete" {
                    Value::Integer(now)
                } else {
                    Value::Null
                },
            ]);
        }
        params.extend([
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
        ]);
        transaction
            .execute(
                format!(
                    "WITH updates(group_key,member_count,state,completed_at) AS (VALUES {}) \
                     UPDATE fireweed_cohorts AS c SET member_count=u.member_count,state=u.state,\
                      first_eligible_at=CASE WHEN u.state='complete' AND c.first_eligible_at IS NULL \
                      THEN u.completed_at ELSE c.first_eligible_at END \
                     FROM updates AS u WHERE c.group_key=u.group_key \
                      AND c.tenant_id=? AND c.queue_id=?",
                    values_rows(chunk.len(), 4)
                ),
                params,
            )
            .await
            .map_err(storage)?;
    }
    Ok(())
}

async fn cohort_item_ids(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    cohort_id: &CohortId,
) -> EngineResult<(GroupKey, Vec<ItemId>)> {
    let row = one_row(
        transaction,
        "SELECT group_key FROM fireweed_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
        vec![
            tenant.to_string().into(),
            queue.to_string().into(),
            cohort_id.as_str().to_string().into(),
        ],
    )
    .await?
    .ok_or(EngineError::NotFound)?;
    let group = GroupKey::new(text(&row[0])?).map_err(storage)?;
    let mut rows = transaction
        .query(
            "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
             AND superseded=0 AND cohort_size IS NOT NULL AND lifecycle_state NOT IN ('Complete','Failed') \
             ORDER BY priority_sort,created_seq",
            vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Text(group.as_str().to_string()),
            ],
        )
        .await
        .map_err(storage)?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        ids.push(ItemId::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?);
    }
    Ok((group, ids))
}

async fn groups_for_items(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    ids: &[ItemId],
) -> EngineResult<Vec<GroupKey>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut groups = HashSet::new();
    for chunk in ids.chunks(VALIDATION_ITEM_CHUNK) {
        let mut params = vec![
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
        ];
        append_item_ids(&mut params, chunk);
        let mut rows = transaction
            .query(
                format!(
                    "SELECT DISTINCT group_key FROM fireweed_items WHERE tenant_id=? AND queue_id=? \
                     AND group_key IS NOT NULL AND item_id IN ({})",
                    vec!["?"; chunk.len()].join(",")
                ),
                params,
            )
            .await
            .map_err(storage)?;
        while let Some(row) = rows.next().await.map_err(storage)? {
            groups.insert(
                GroupKey::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?,
            );
        }
    }
    Ok(groups.into_iter().collect())
}

async fn relect_group_summaries(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    groups: &[GroupKey],
    now: i64,
) -> EngineResult<()> {
    const GATE_ANTI_JOIN: &str = " AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
         JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
         AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
         AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id)";
    let mut writes = Vec::with_capacity(groups.len());
    for group in groups {
        let params = vec![
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
            Value::Text(group.as_str().to_string()),
            Value::Integer(now),
        ];
        let count_row = one_row(
            transaction,
            &format!(
                "SELECT COUNT(*), MIN(eligible_since) FROM fireweed_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
                 AND group_key=?3 AND (not_before IS NULL OR not_before<=?4){GATE_ANTI_JOIN}"
            ),
            params.clone(),
        )
        .await?;
        let (count, oldest) = match count_row {
            Some(row) => (integer(&row[0])?, optional_integer(&row[1])?),
            None => (0, None),
        };
        let head = one_row(
            transaction,
            &format!(
                "SELECT item_id,priority_sort,created_at FROM fireweed_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
                 AND group_key=?3 AND (not_before IS NULL OR not_before<=?4){GATE_ANTI_JOIN} \
                 ORDER BY priority_sort, created_seq, item_id LIMIT 1"
            ),
            params,
        )
        .await?;
        writes.push((group, count, oldest, head));
    }
    for chunk in writes.chunks(GROUP_SUMMARY_CHUNK) {
        let values = vec!["(?,?,?,?,NULL,?,?,?,?,0,?)"; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO fireweed_group_summary \
             (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
              rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
             VALUES {values} \
             ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
              oldest_eligible_at=excluded.oldest_eligible_at,rep_progress_guard_sort=excluded.rep_progress_guard_sort,\
              rep_priority_sort=excluded.rep_priority_sort,rep_created_at=excluded.rep_created_at,\
              rep_item_id=excluded.rep_item_id,eligible_item_count=excluded.eligible_item_count,\
              at_risk_count=excluded.at_risk_count,updated_at=excluded.updated_at"
        );
        let mut params = Vec::with_capacity(chunk.len() * 9);
        for (group, count, oldest, head) in chunk {
            params.push(Value::Text(tenant.to_string()));
            params.push(Value::Text(queue.to_string()));
            params.push(Value::Text(group.as_str().to_string()));
            match head {
                Some(row) => {
                    params.push(oldest.map_or(Value::Null, Value::Integer));
                    params.push(row[1].clone());
                    params.push(row[2].clone());
                    params.push(row[0].clone());
                    params.push(Value::Integer(*count));
                    params.push(Value::Integer(now));
                }
                None => {
                    params.push(Value::Null);
                    params.push(Value::Null);
                    params.push(Value::Null);
                    params.push(Value::Null);
                    params.push(Value::Integer(0));
                    params.push(Value::Integer(now));
                }
            }
        }
        transaction.execute(sql, params).await.map_err(storage)?;
    }
    Ok(())
}

async fn queue_paused(transaction: &Connection, tenant: &str, queue: &str) -> EngineResult<bool> {
    let row = one_row(
        transaction,
        "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
        vec![tenant.to_string().into(), queue.to_string().into()],
    )
    .await?
    .ok_or(EngineError::NotFound)?;
    Ok(integer(&row[0])? != 0)
}

async fn refresh_due_group_summaries(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    now: i64,
) -> EngineResult<()> {
    let mut rows = transaction
        .query(
            "SELECT DISTINCT i.group_key FROM fireweed_items i \
             LEFT JOIN fireweed_group_summary gs ON gs.tenant_id=i.tenant_id \
             AND gs.queue_id=i.queue_id AND gs.group_key=i.group_key \
             WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.lifecycle_state='Pending' \
             AND i.superseded=0 AND i.group_key IS NOT NULL AND i.eligible_since IS NOT NULL \
             AND (i.not_before IS NULL OR i.not_before<=?3) \
             AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gstate \
             ON gstate.tenant_id=ig.tenant_id AND gstate.queue_id=ig.queue_id \
             AND gstate.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id \
             AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
             AND (gs.group_key IS NULL OR gs.oldest_eligible_at IS NULL OR gs.rep_item_id IS NULL \
                  OR NOT EXISTS (SELECT 1 FROM fireweed_items r \
                    WHERE r.tenant_id=i.tenant_id AND r.queue_id=i.queue_id AND r.item_id=gs.rep_item_id \
                      AND r.lifecycle_state='Pending' AND r.superseded=0)) \
             ORDER BY i.group_key LIMIT 128",
            vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Integer(now),
            ],
        )
        .await
        .map_err(storage)?;
    let mut groups = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        groups.push(GroupKey::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?);
    }
    drop(rows);
    relect_group_summaries(transaction, tenant, queue, &groups, now).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn group_eligible_items(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    group: &GroupKey,
    now: i64,
    limit: usize,
    cohort: bool,
    compatibility: &ClaimCompatibility,
) -> EngineResult<GroupEligibility> {
    let cohort_predicate = if cohort {
        "cohort_size IS NOT NULL"
    } else {
        "cohort_size IS NULL"
    };
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut rows = transaction
        .query(
            format!(
                "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
                 AND group_key=?3 AND lifecycle_state='Pending' AND superseded=0 \
                 AND {cohort_predicate} AND (not_before IS NULL OR not_before<=?4) \
                 AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
                 JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id \
                 AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=fireweed_items.tenant_id AND ig.queue_id=fireweed_items.queue_id \
                 AND ig.item_id=fireweed_items.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
                 WHERE NOT EXISTS (SELECT 1 FROM json_each(fireweed_items.metadata) actual \
                   WHERE actual.key=wanted.key AND actual.value=wanted.value AND actual.type=wanted.type)) \
                 ORDER BY priority_sort,created_seq,item_id LIMIT ?5"
            ),
            vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Text(group.as_str().to_string()),
                Value::Integer(now),
                Value::Integer(limit as i64),
                Value::Text(metadata_filter),
            ],
        )
        .await
        .map_err(storage)?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        ids.push(ItemId::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?);
    }
    Ok(GroupEligibility { item_ids: ids })
}

struct GroupEligibility {
    item_ids: Vec<ItemId>,
}

async fn select_group_batching(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    now: i64,
    max_items: usize,
    max_groups: u32,
    compatibility: &ClaimCompatibility,
) -> EngineResult<Vec<ItemId>> {
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let row_limit = max_items.saturating_add(1) as i64;
    let mut rows = transaction
        .query(
            "WITH candidate_raw AS MATERIALIZED (SELECT s.group_key,e.priority_sort rep_priority_sort,\
               e.created_at rep_created_at,e.item_id rep_item_id,e.created_seq,ROW_NUMBER() OVER \
               (PARTITION BY s.group_key ORDER BY e.priority_sort,e.created_seq,e.item_id) rn \
               FROM fireweed_group_summary s JOIN fireweed_items e ON e.tenant_id=?1 AND e.queue_id=?2 \
                 AND e.group_key=s.group_key WHERE s.tenant_id=?1 AND s.queue_id=?2 \
               AND s.oldest_eligible_at IS NOT NULL AND e.lifecycle_state='Pending' AND e.superseded=0 \
               AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
               AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
                 JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                 AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id \
                 AND ig.queue_id=e.queue_id AND ig.item_id=e.item_id) \
               AND NOT EXISTS (SELECT 1 FROM json_each(?5) wanted WHERE NOT EXISTS \
                 (SELECT 1 FROM json_each(e.metadata) actual WHERE actual.key=wanted.key \
                  AND actual.value=wanted.value AND actual.type=wanted.type)) \
               AND NOT EXISTS (SELECT 1 FROM fireweed_items leased WHERE leased.tenant_id=?1 \
                 AND leased.queue_id=?2 AND leased.group_key=s.group_key AND leased.superseded=0 \
                 AND leased.cohort_size IS NULL AND leased.lifecycle_state='Leased')), \
             candidate AS MATERIALIZED (SELECT group_key,rep_priority_sort,rep_created_at,rep_item_id \
               FROM candidate_raw WHERE rn=1 ORDER BY rep_priority_sort,created_seq,rep_item_id,group_key LIMIT ?4), \
             eligible AS MATERIALIZED (SELECT c.group_key,c.rep_priority_sort,c.rep_created_at,\
               c.rep_item_id,i.item_id,i.priority_sort,i.created_seq FROM candidate c \
               JOIN fireweed_items i ON i.tenant_id=?1 AND i.queue_id=?2 AND i.group_key=c.group_key \
               WHERE i.lifecycle_state='Pending' AND i.superseded=0 AND i.cohort_size IS NULL \
                 AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
                 AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
                   ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                   WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
                 AND NOT EXISTS (SELECT 1 FROM json_each(?5) wanted WHERE NOT EXISTS \
                   (SELECT 1 FROM json_each(i.metadata) actual WHERE actual.key=wanted.key \
                    AND actual.value=wanted.value AND actual.type=wanted.type)) \
               ORDER BY c.rep_priority_sort,c.rep_created_at,c.rep_item_id,c.group_key,\
                 i.priority_sort,i.created_seq,i.item_id LIMIT ?6), \
             grouped AS (SELECT group_key,rep_priority_sort,rep_created_at,rep_item_id,COUNT(*) item_count,\
               json_group_array(item_id) item_ids FROM eligible GROUP BY group_key,rep_priority_sort,\
               rep_created_at,rep_item_id) SELECT item_count,item_ids,SUM(item_count) OVER \
               (ORDER BY rep_priority_sort,rep_created_at,rep_item_id,group_key) running_count \
               FROM grouped ORDER BY rep_priority_sort,rep_created_at,rep_item_id,group_key",
            vec![
                tenant.to_string().into(),
                queue.to_string().into(),
                Value::Integer(now),
                Value::Integer(i64::from(max_groups)),
                Value::Text(metadata_filter),
                Value::Integer(row_limit),
            ],
        )
        .await
        .map_err(storage)?;
    let mut selected = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        let count =
            usize::try_from(integer(&row.get_value(0).map_err(storage)?)?).map_err(storage)?;
        if count > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        let running =
            usize::try_from(integer(&row.get_value(2).map_err(storage)?)?).map_err(storage)?;
        if running > max_items {
            break;
        }
        let ids: Vec<String> =
            serde_json::from_str(&text(&row.get_value(1).map_err(storage)?)?).map_err(storage)?;
        selected.extend(
            ids.into_iter()
                .map(|id| ItemId::new(id).map_err(storage))
                .collect::<EngineResult<Vec<_>>>()?,
        );
    }
    Ok(selected)
}

async fn select_same_group(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    now: i64,
    max_items: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<Vec<ItemId>> {
    let required_group = compatibility
        .group_key
        .as_ref()
        .map_or(Value::Null, |group| Value::Text(group.as_str().to_string()));
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut rows = transaction.query(
        "WITH candidate AS (SELECT s.group_key FROM fireweed_group_summary s WHERE s.tenant_id=?1 \
         AND s.queue_id=?2 AND s.oldest_eligible_at IS NOT NULL AND (?5 IS NULL OR s.group_key=?5) \
         AND EXISTS (SELECT 1 FROM fireweed_items e WHERE e.tenant_id=?1 AND e.queue_id=?2 \
           AND e.group_key=s.group_key AND e.lifecycle_state='Pending' AND e.superseded=0 \
           AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
           AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
             JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id AND ig.queue_id=e.queue_id \
             AND ig.item_id=e.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
             WHERE NOT EXISTS (SELECT 1 FROM json_each(e.metadata) actual \
               WHERE actual.key=wanted.key AND actual.value=wanted.value AND actual.type=wanted.type))) \
         ORDER BY s.rep_priority_sort,s.rep_created_at,s.rep_item_id,s.group_key LIMIT 1) \
         SELECT i.item_id FROM candidate c JOIN fireweed_items i ON i.tenant_id=?1 AND i.queue_id=?2 \
         AND i.group_key=c.group_key WHERE i.lifecycle_state='Pending' AND i.superseded=0 \
         AND i.cohort_size IS NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
         AND i.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
           JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
           AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id \
           AND ig.item_id=i.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
           WHERE NOT EXISTS (SELECT 1 FROM json_each(i.metadata) actual WHERE actual.key=wanted.key \
             AND actual.value=wanted.value AND actual.type=wanted.type)) \
         ORDER BY i.priority_sort,i.created_seq,i.item_id LIMIT ?4",
        vec![tenant.to_string().into(),queue.to_string().into(),Value::Integer(now),
             Value::Integer(max_items as i64),required_group,Value::Text(metadata_filter)],
    ).await.map_err(storage)?;
    let mut selected = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        selected.push(ItemId::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?);
    }
    Ok(selected)
}

async fn select_whole_cohort(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    now: i64,
    max_items: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<RichClaimSelection> {
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let mut rows = transaction.query(
        "SELECT c.group_key,c.cohort_id,c.cohort_size FROM fireweed_cohorts c \
         WHERE c.tenant_id=?1 AND c.queue_id=?2 AND c.state='complete' \
         AND (SELECT COUNT(*) FROM fireweed_items a WHERE a.tenant_id=?1 AND a.queue_id=?2 \
           AND a.group_key=c.group_key AND a.superseded=0 AND a.cohort_size IS NOT NULL \
           AND a.lifecycle_state NOT IN ('Complete','Failed'))=c.cohort_size \
         AND NOT EXISTS (SELECT 1 FROM fireweed_items i WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.group_key=c.group_key AND i.superseded=0 AND i.cohort_size IS NOT NULL \
           AND i.lifecycle_state NOT IN ('Complete','Failed') AND NOT (i.lifecycle_state='Pending' \
             AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig JOIN fireweed_gate_state gs \
               ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
               WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
             AND NOT EXISTS (SELECT 1 FROM json_each(?4) wanted WHERE NOT EXISTS \
               (SELECT 1 FROM json_each(i.metadata) actual WHERE actual.key=wanted.key \
                AND actual.value=wanted.value AND actual.type=wanted.type)))) \
         ORDER BY c.cohort_created_at,c.group_key LIMIT 1",
        vec![tenant.to_string().into(),queue.to_string().into(),Value::Integer(now),
             Value::Text(metadata_filter)],
    ).await.map_err(storage)?;
    let Some(row) = rows.next().await.map_err(storage)? else {
        return Ok(RichClaimSelection::default());
    };
    let group = text(&row.get_value(0).map_err(storage)?)?;
    let cohort_id = text(&row.get_value(1).map_err(storage)?)?;
    let size = usize::try_from(integer(&row.get_value(2).map_err(storage)?)?).map_err(storage)?;
    drop(rows);
    if size > max_items {
        return Err(EngineError::BatchTooLarge);
    }
    let group = GroupKey::new(group).map_err(storage)?;
    let eligible = group_eligible_items(
        transaction,
        tenant,
        queue,
        &group,
        now,
        size,
        true,
        compatibility,
    )
    .await?;
    Ok(RichClaimSelection {
        item_ids: eligible.item_ids,
        cohort_id: Some(CohortId::new(cohort_id).map_err(storage)?),
    })
}

async fn cohort_state(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    cohort_id: &CohortId,
) -> EngineResult<String> {
    let row = one_row(
        transaction,
        "SELECT state FROM fireweed_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
        vec![
            tenant.to_string().into(),
            queue.to_string().into(),
            cohort_id.as_str().to_string().into(),
        ],
    )
    .await?
    .ok_or(EngineError::NotFound)?;
    text(&row[0])
}

fn append_item_ids(params: &mut Vec<Value>, ids: &[ItemId]) {
    params.extend(ids.iter().map(|item| Value::Text(item.to_string())));
}

async fn execute_for_items<F>(
    transaction: &Connection,
    query_for: F,
    params: Vec<Value>,
    ids: &[ItemId],
) -> EngineResult<u64>
where
    F: Fn(usize) -> String,
{
    let chunk_size = 900_usize
        .checked_sub(params.len())
        .filter(|size| *size > 0)
        .ok_or_else(|| storage("item statement has no bind capacity"))?;
    let mut changed = 0_u64;
    for chunk in ids.chunks(chunk_size) {
        let mut chunk_params = params.clone();
        append_item_ids(&mut chunk_params, chunk);
        changed = changed.saturating_add(
            transaction
                .execute(query_for(chunk.len()), chunk_params)
                .await
                .map_err(storage)?,
        );
    }
    Ok(changed)
}

async fn update_item_schedules(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    schedules: &[(ItemId, Option<i64>, i64)],
) -> EngineResult<()> {
    for chunk in schedules.chunks(SCHEDULE_UPDATE_CHUNK) {
        let mut params = Vec::with_capacity(chunk.len() * 3 + 2);
        for (item_id, not_before, eligible_since) in chunk {
            params.extend([
                Value::Text(item_id.to_string()),
                not_before.map_or(Value::Null, Value::Integer),
                Value::Integer(*eligible_since),
            ]);
        }
        params.extend([
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
        ]);
        transaction
            .execute(
                format!(
                    "WITH schedules(item_id,not_before,eligible_since) AS (VALUES {}) \
                     UPDATE fireweed_items AS i SET not_before=s.not_before,\
                      eligible_since=s.eligible_since FROM schedules AS s \
                     WHERE i.item_id=s.item_id AND i.tenant_id=? AND i.queue_id=?",
                    values_rows(chunk.len(), 3)
                ),
                params,
            )
            .await
            .map_err(storage)?;
    }
    Ok(())
}

async fn retry_info_by_item(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    ids: &[ItemId],
) -> EngineResult<HashMap<ItemId, (i64, i64)>> {
    let mut info = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(GROUP_COUNT_CHUNK) {
        let mut params = vec![
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
        ];
        append_item_ids(&mut params, chunk);
        let mut rows = transaction
            .query(&sql::select_retry_info(chunk.len()), params)
            .await
            .map_err(storage)?;
        while let Some(row) = rows.next().await.map_err(storage)? {
            info.insert(
                ItemId::new(row.get::<String>(0).map_err(storage)?).map_err(storage)?,
                (
                    row.get::<i64>(1).map_err(storage)?,
                    row.get::<i64>(2).map_err(storage)?,
                ),
            );
        }
    }
    Ok(info)
}

async fn extend_claim_by_query_replays(
    transaction: &Connection,
    shard: &QueueKey,
    renewed_item_ids: &[ItemId],
    renewed_expires_at: UtcTimestamp,
) -> EngineResult<()> {
    if renewed_item_ids.is_empty() {
        return Ok(());
    }
    let tenant = shard.tenant_id.as_str().to_string();
    let queue = shard.queue_id.as_str().to_string();
    let renewed = serde_json::to_string(
        &renewed_item_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .map_err(storage)?;
    transaction
        .execute(
            "UPDATE fireweed_request_idempotency SET expires_at=max(expires_at,?4) \
         WHERE tenant_id=?1 AND queue_id=?2 AND operation='claim_by_query' AND request_id IN ( \
           SELECT edge.request_id FROM fireweed_claim_replay_items edge \
           JOIN json_each(?3) renewed ON renewed.value=edge.item_id \
           WHERE edge.tenant_id=?1 AND edge.queue_id=?2 GROUP BY edge.request_id \
           HAVING COUNT(*)=(SELECT COUNT(*) FROM fireweed_claim_replay_items all_edges \
             WHERE all_edges.tenant_id=?1 AND all_edges.queue_id=?2 \
               AND all_edges.request_id=edge.request_id))",
            vec![
                tenant.into(),
                queue.into(),
                renewed.into(),
                Value::Integer(ts_nanos(renewed_expires_at)),
            ],
        )
        .await
        .map_err(storage)?;
    Ok(())
}

async fn definition_in_transaction(
    connection: &Connection,
    shard: &QueueKey,
) -> EngineResult<QueueDefinition> {
    let row = one_row(
        connection,
        sql::SELECT_QUEUE_DEFINITION,
        vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
        ],
    )
    .await?
    .ok_or(EngineError::NotFound)?;
    serde_json::from_str(&text(&row[0])?).map_err(storage)
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn record_statement(
    shape: Option<&Arc<std::sync::Mutex<TursoBatchUpdateStatementShape>>>,
    sql: &str,
    bind_count: usize,
) {
    if let Some(shape) = shape {
        shape
            .lock()
            .expect("Turso statement-shape mutex poisoned")
            .record(sql, bind_count);
    }
}

#[derive(Default)]
struct RelApplyPhaseTotals {
    row_read_us: u64,
    update_side_us: u64,
}

struct ObservedTursoRel<'a> {
    inner: crate::tx::TursoRel<'a>,
    statement_shape: Option<Arc<std::sync::Mutex<TursoBatchUpdateStatementShape>>>,
    phases: Arc<std::sync::Mutex<RelApplyPhaseTotals>>,
}

impl RelTx for ObservedTursoRel<'_> {
    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
        let started = Instant::now();
        let result = self.inner.execute(sql, params);
        let elapsed = duration_us(started.elapsed());
        let mut phases = self
            .phases
            .lock()
            .expect("Turso RelTx phase mutex poisoned");
        phases.update_side_us = phases.update_side_us.saturating_add(elapsed);
        drop(phases);
        record_statement(self.statement_shape.as_ref(), sql, params.len());
        result
    }

    fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>> {
        let started = Instant::now();
        let result = self.inner.query(sql, params);
        let elapsed = duration_us(started.elapsed());
        let mut phases = self
            .phases
            .lock()
            .expect("Turso RelTx phase mutex poisoned");
        let normalized = sql.trim_start().to_ascii_uppercase();
        if normalized.starts_with("UPDATE")
            || (normalized.starts_with("WITH") && normalized.contains(" UPDATE "))
        {
            phases.update_side_us = phases.update_side_us.saturating_add(elapsed);
        } else {
            phases.row_read_us = phases.row_read_us.saturating_add(elapsed);
        }
        drop(phases);
        record_statement(self.statement_shape.as_ref(), sql, params.len());
        result
    }
}

fn collect_api001_updates(commands: &[CommandEnvelope]) -> Option<Vec<&UpdateFieldsCommand>> {
    let mut updates = Vec::new();
    for envelope in commands {
        match &envelope.command {
            QueueCommand::UpdateFields(update)
                if update.api001_batch && update.set_entity_document.is_none() =>
            {
                updates.push(update);
            }
            QueueCommand::UpdateFieldsBatch(batch)
                if batch
                    .updates
                    .iter()
                    .all(|update| update.api001_batch && update.set_entity_document.is_none()) =>
            {
                updates.extend(batch.updates.iter());
            }
            QueueCommand::UpdateFields(_) | QueueCommand::UpdateFieldsBatch(_) => {
                return None;
            }
            _ => {}
        }
    }
    Some(updates)
}

async fn apply_owned(
    writer: Arc<Mutex<Connection>>,
    live_tokens: Arc<Mutex<BTreeMap<(QueueKey, ItemId), LeaseToken>>>,
    live_tokens_by_consumer: Arc<Mutex<ConsumerLeaseIndex>>,
    last_batch_update_shape: Arc<std::sync::Mutex<Option<TursoBatchUpdateStatementShape>>>,
    last_apply_phase: Arc<std::sync::Mutex<Option<TursoApplyPhaseObservation>>>,
    grouped_shards_slot: Arc<std::sync::Mutex<HashSet<QueueKey>>>,
    claim_scan_hints_slot: Arc<std::sync::Mutex<HashMap<QueueKey, i64>>>,
    claim_scan_default_fifo_slot: Arc<std::sync::Mutex<HashMap<QueueKey, bool>>>,
    positions: Vec<CommandPosition>,
    commands: Vec<CommandEnvelope>,
    enforce_live_epoch: bool,
) -> EngineResult<()> {
    let total_started = Instant::now();
    if positions.len() != commands.len() {
        return Err(storage("positions/commands length mismatch"));
    }
    let api001_updates = collect_api001_updates(&commands);
    let api001_updates = api001_updates.filter(|updates| !updates.is_empty());
    let statement_shape = api001_updates.as_ref().map(|updates| {
        Arc::new(std::sync::Mutex::new(TursoBatchUpdateStatementShape::new(
            updates.len(),
        )))
    });
    *last_batch_update_shape
        .lock()
        .expect("Turso statement-shape mutex poisoned") = None;
    let writer_wait_started = Instant::now();
    let mut connection = writer.lock().await;
    let writer_wait_us = duration_us(writer_wait_started.elapsed());
    let mut grouped_shards = grouped_shards_slot
        .lock()
        .expect("Turso grouped-shard mutex poisoned")
        .clone();
    let mut claim_scan_hints = claim_scan_hints_slot
        .lock()
        .expect("Turso claim-scan-hint mutex poisoned")
        .clone();
    let mut claim_scan_default_fifo = claim_scan_default_fifo_slot
        .lock()
        .expect("Turso claim-scan-fifo mutex poisoned")
        .clone();
    let mut queues: HashMap<QueueKey, QueueDefinition> = HashMap::new();
    let mut cursor_seeds = HashMap::new();
    let begin_started = Instant::now();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .map_err(storage)?;
    let begin_us = duration_us(begin_started.elapsed());
    let mut token_ops = Vec::new();

    let cursor_definition_started = Instant::now();
    // Fence the complete live batch before executing any command. A later position may
    // target a queue already seen in the batch, so checking only while initializing
    // `next_by_queue` would allow that later stale epoch to mutate state.
    if enforce_live_epoch {
        let mut floors = HashMap::new();
        for position in &positions {
            let floor = match floors.get(&position.queue) {
                Some(floor) => *floor,
                None => {
                    record_statement(statement_shape.as_ref(), sql::SELECT_CURSOR, 2);
                    let row = one_row(
                        &transaction,
                        sql::SELECT_CURSOR,
                        vec![
                            position.queue.tenant_id.as_str().to_string().into(),
                            position.queue.queue_id.as_str().to_string().into(),
                        ],
                    )
                    .await?
                    .ok_or(EngineError::NotFound)?;
                    cursor_seeds.insert(position.queue.clone(), integer(&row[0])?);
                    let floor = nonnegative_u64(integer(&row[1])?, "assignment epoch")?;
                    floors.insert(position.queue.clone(), floor);
                    floor
                }
            };
            if position.backend_epoch < floor {
                transaction.rollback().await.map_err(storage)?;
                return Err(EngineError::EpochFenced);
            }
            floors.insert(position.queue.clone(), floor.max(position.backend_epoch));
        }
    }

    for envelope in &commands {
        if let Err(error) = validate_minimal_command(envelope) {
            transaction.rollback().await.map_err(storage)?;
            return Err(error);
        }
    }
    for position in &positions {
        if !queues.contains_key(&position.queue) {
            record_statement(statement_shape.as_ref(), sql::SELECT_QUEUE_DEFINITION, 2);
            let definition = definition_in_transaction(&transaction, &position.queue).await?;
            queues.insert(position.queue.clone(), definition);
        }
    }
    let cursor_definition_us = duration_us(cursor_definition_started.elapsed());
    let hop_txn = transaction.clone();
    let rel_phases = Arc::new(std::sync::Mutex::new(RelApplyPhaseTotals::default()));
    let rel_phases_for_hop = Arc::clone(&rel_phases);
    let statement_shape_for_hop = statement_shape.clone();
    let relational_started = Instant::now();
    let relational_result = crate::tx::run_reltx_blocking(move || {
        let applied = if let Some(statement_shape) = statement_shape_for_hop {
            let rel = ObservedTursoRel {
                inner: crate::tx::TursoRel(&hop_txn),
                statement_shape: Some(statement_shape),
                phases: rel_phases_for_hop,
            };
            fireweed_relational::apply_committed_batch_sql_with_cursor_seeds(
                &rel,
                &queues,
                &mut grouped_shards,
                &mut claim_scan_hints,
                &mut claim_scan_default_fifo,
                &mut token_ops,
                &positions,
                &commands,
                &cursor_seeds,
            )?
        } else {
            let rel = crate::tx::TursoRel(&hop_txn);
            fireweed_relational::apply_committed_batch_sql_with_cursor_seeds(
                &rel,
                &queues,
                &mut grouped_shards,
                &mut claim_scan_hints,
                &mut claim_scan_default_fifo,
                &mut token_ops,
                &positions,
                &commands,
                &cursor_seeds,
            )?
        };
        Ok::<_, EngineError>((
            applied,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            token_ops,
        ))
    })
    .await;
    let (applied_api001, next_grouped, next_hints, next_fifo, next_tokens) = match relational_result
    {
        Ok(result) => result,
        Err(error) => {
            transaction.rollback().await.map_err(storage)?;
            return Err(error);
        }
    };
    grouped_shards = next_grouped;
    claim_scan_hints = next_hints;
    claim_scan_default_fifo = next_fifo;
    token_ops = next_tokens;
    let relational_us = duration_us(relational_started.elapsed());
    let (row_read_us, update_side_us) = {
        let rel_phase = rel_phases.lock().expect("Turso RelTx phase mutex poisoned");
        (rel_phase.row_read_us, rel_phase.update_side_us)
    };
    let transform_bridge_us =
        relational_us.saturating_sub(row_read_us.saturating_add(update_side_us));
    let commit_started = Instant::now();
    transaction.commit().await.map_err(storage)?;
    let commit_us = duration_us(commit_started.elapsed());
    *grouped_shards_slot
        .lock()
        .expect("Turso grouped-shard mutex poisoned") = grouped_shards;
    *claim_scan_hints_slot
        .lock()
        .expect("Turso claim-scan-hint mutex poisoned") = claim_scan_hints;
    *claim_scan_default_fifo_slot
        .lock()
        .expect("Turso claim-scan-fifo mutex poisoned") = claim_scan_default_fifo;
    if applied_api001 {
        *last_batch_update_shape
            .lock()
            .expect("Turso statement-shape mutex poisoned") = statement_shape
            .as_ref()
            .map(|shape| *shape.lock().expect("Turso statement-shape mutex poisoned"));
    }
    let phase_observation = TursoApplyPhaseObservation {
        writer_wait_us,
        begin_us,
        cursor_definition_us,
        row_read_us,
        transform_bridge_us,
        update_side_us,
        commit_us,
        total_us: duration_us(total_started.elapsed()),
    };
    *last_apply_phase
        .lock()
        .expect("Turso apply-phase mutex poisoned") = Some(phase_observation);
    let mut tokens = live_tokens.lock().await;
    let mut by_consumer = live_tokens_by_consumer.lock().await;
    for op in token_ops {
        match op {
            TokenOp::Set(shard, item, token) => {
                if let Some(old) = tokens.insert((shard.clone(), item), token.clone()) {
                    by_consumer.remove(&(shard.clone(), old.as_str().to_string(), item));
                }
                by_consumer.insert((shard, token.as_str().to_string(), item), ());
            }
            TokenOp::Clear(shard, item) => {
                if let Some(old) = tokens.remove(&(shard.clone(), item)) {
                    by_consumer.remove(&(shard, old.as_str().to_string(), item));
                }
            }
        }
    }
    Ok(())
}

impl TursoRelational {
    /// Atomically create the queue or return its exact durable definition.
    pub async fn create_or_read_queue(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        ensure_shard_owned(Arc::clone(&self.writer), definition).await
    }

    /// Class B / reopen mint floor: greatest durable item id for `shard`.
    ///
    /// Prefers `fireweed_id_high_water` (survives terminal reaping); falls back to
    /// `MAX(item_id)` on live rows so memory-log reopen never remints existing ids.
    pub async fn recovery_counter_high_water(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Option<ItemId>> {
        let tenant = shard.tenant_id.as_str().to_string();
        let queue = shard.queue_id.as_str().to_string();
        let connection = self.writer.lock().await;
        // Prefer the monotonic high-water table when present.
        if let Some(row) = one_row(
            &connection,
            "SELECT item_id FROM fireweed_id_high_water WHERE tenant=?1 AND queue=?2",
            vec![tenant.clone().into(), queue.clone().into()],
        )
        .await?
        {
            let id = text(&row[0])?;
            return Ok(Some(
                ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
            ));
        }
        // Fall back to the greatest live item id (string-encoded; length-then-lex order
        // matches sqlite id_high_water advance semantics for decimal item ids).
        let Some(row) = one_row(
            &connection,
            "SELECT item_id FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 \
             ORDER BY length(item_id) DESC, item_id DESC LIMIT 1",
            vec![tenant.into(), queue.into()],
        )
        .await?
        else {
            return Ok(None);
        };
        let id = text(&row[0])?;
        Ok(Some(
            ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
        ))
    }

    pub(crate) async fn purge_items_validate(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
        force: bool,
    ) -> EngineResult<Vec<ItemId>> {
        let connection = self.writer.lock().await;
        let mut unique = Vec::with_capacity(ids.len());
        let mut seen = HashSet::with_capacity(ids.len());
        for id in ids {
            if seen.insert(*id) {
                unique.push(*id);
            }
        }
        let mut present = Vec::with_capacity(unique.len());
        let tenant = shard.tenant_id.as_str();
        let queue = shard.queue_id.as_str();
        for chunk in unique.chunks(VALIDATION_ITEM_CHUNK) {
            let rows =
                validation_rows_by_item(&connection, tenant, queue, chunk, "lifecycle_state")
                    .await?;
            for id in chunk {
                let Some(row) = rows.get(id) else {
                    continue;
                };
                let state = parse_state(&text(&row[0])?).map_err(storage)?;
                fireweed_engine::validate_purge_force(state == ItemState::Leased, force)?;
                present.push(*id);
            }
        }
        Ok(present)
    }

    /// RESP/server read surface over the same native-async projection used by commit apply.
    pub async fn server_peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        let limit = i64::try_from(limit).map_err(storage)?;
        let rows = self
            .query(
                "SELECT item_id,client_item_key,priority,item_version FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
             ORDER BY priority_sort,created_seq LIMIT ?3",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    limit.into(),
                ],
            )
            .await
            .map_err(storage)?;
        rows.into_iter()
            .map(|row| {
                Ok(ItemView {
                    item_id: ItemId::new(text(&row.values[0])?).map_err(storage)?,
                    client_item_key: ClientItemKey::new(text(&row.values[1])?).map_err(storage)?,
                    priority: parse_priority(optional_text(&row.values[2])?)?,
                    item_version: nonnegative_u64(integer(&row.values[3])?, "item_version")?,
                })
            })
            .collect()
    }

    pub async fn server_pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let rows = self
            .query(
                "SELECT item_id,lease_expires_at,retry_count FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' AND superseded=0 \
             ORDER BY item_id",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                ],
            )
            .await
            .map_err(storage)?;
        let tokens = self.live_tokens.lock().await;
        let mut pending = Vec::new();
        for row in rows {
            let item_id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
            let Some(lease_token) = tokens.get(&(shard.clone(), item_id)).cloned() else {
                continue;
            };
            let Some(expires) = optional_integer(&row.values[1])? else {
                continue;
            };
            pending.push(LeaseView {
                item_id,
                lease_token,
                lease_expires_at: nanos_ts(expires),
                attempt_count: nonnegative_u32(integer(&row.values[2])?, "retry_count")?,
            });
        }
        Ok(pending)
    }

    pub async fn server_pending_summary(&self, shard: &QueueKey) -> EngineResult<PendingSummary> {
        use std::ops::Bound::Included;
        let tokens = self.live_tokens.lock().await;
        let bounds = (
            Included((shard.clone(), ItemId::from_u64(0))),
            Included((shard.clone(), ItemId::from_u64(u64::MAX))),
        );
        let mut count = 0u64;
        let mut min_id = None;
        let mut max_id = None;
        let mut consumers = BTreeMap::<String, (LeaseToken, u64)>::new();
        for ((_, id), token) in tokens.range(bounds) {
            count += 1;
            min_id.get_or_insert(*id);
            max_id = Some(*id);
            let entry = consumers
                .entry(token.as_str().to_string())
                .or_insert_with(|| (token.clone(), 0));
            entry.1 += 1;
        }
        Ok(PendingSummary {
            count,
            min_id,
            max_id,
            consumers: consumers.into_values().collect(),
        })
    }

    pub async fn server_pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<LeaseView>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_id = HashMap::<ItemId, (i64, u32)>::with_capacity(ids.len());
        for chunk in ids.chunks(500) {
            let placeholders = (0..chunk.len())
                .map(|index| format!("?{}", index + 3))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT item_id,lease_expires_at,retry_count FROM fireweed_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' \
                 AND lease_expires_at IS NOT NULL AND item_id IN ({placeholders})"
            );
            let mut params = vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
            ];
            params.extend(chunk.iter().map(|id| id.to_string().into()));
            for row in self.query(sql, params).await.map_err(storage)? {
                let id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
                by_id.insert(
                    id,
                    (
                        integer(&row.values[1])?,
                        nonnegative_u32(integer(&row.values[2])?, "retry_count")?,
                    ),
                );
            }
        }
        let tokens = self.live_tokens.lock().await;
        Ok(ids
            .iter()
            .filter_map(|id| {
                let token = tokens.get(&(shard.clone(), *id))?;
                let (expires, attempts) = by_id.get(id)?;
                Some(LeaseView {
                    item_id: *id,
                    lease_token: token.clone(),
                    lease_expires_at: nanos_ts(*expires),
                    attempt_count: *attempts,
                })
            })
            .collect())
    }

    pub async fn server_pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        use std::ops::Bound::Included;
        let tokens = self.live_tokens.lock().await;
        let ids: Vec<_> = tokens
            .range((
                Included((shard.clone(), start.unwrap_or_else(|| ItemId::from_u64(0)))),
                Included((shard.clone(), ItemId::from_u64(u64::MAX))),
            ))
            .map(|((_, id), _)| *id)
            .take(limit.saturating_add(1))
            .collect();
        drop(tokens);
        let next = ids.get(limit).copied();
        let entries = self
            .server_pending_by_ids(shard, &ids[..ids.len().min(limit)])
            .await?;
        Ok(PendingPage { entries, next })
    }

    pub async fn server_pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        use std::ops::Bound::Included;
        let start = start.unwrap_or_else(|| ItemId::from_u64(0));
        let end = end.unwrap_or_else(|| ItemId::from_u64(u64::MAX));
        let ids: Vec<_> = if let Some(consumer) = consumer {
            self.live_tokens_by_consumer
                .lock()
                .await
                .range((
                    Included((shard.clone(), consumer.as_str().to_string(), start)),
                    Included((shard.clone(), consumer.as_str().to_string(), end)),
                ))
                .map(|((_, _, id), _)| *id)
                .take(limit)
                .collect()
        } else {
            self.live_tokens
                .lock()
                .await
                .range((
                    Included((shard.clone(), start)),
                    Included((shard.clone(), end)),
                ))
                .map(|((_, id), _)| *id)
                .take(limit)
                .collect()
        };
        self.server_pending_by_ids(shard, &ids).await
    }

    pub async fn server_update_snapshot(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_key = HashMap::with_capacity(keys.len());
        for chunk in keys.chunks(VALIDATION_ITEM_CHUNK) {
            let mut params = vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
            ];
            params.extend(
                chunk
                    .iter()
                    .map(|key| Value::Text(key.as_str().to_string())),
            );
            let placeholders = (0..chunk.len())
                .map(|offset| format!("?{}", offset + 3))
                .collect::<Vec<_>>()
                .join(",");
            let rows = self
                .query(
                    format!(
                        "SELECT item_id,client_item_key,item_version,lifecycle_state,fenced \
                         FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
                         AND client_item_key IN ({placeholders}) \
                         AND lifecycle_state IN ('Pending','Leased') AND superseded=0"
                    ),
                    params,
                )
                .await
                .map_err(storage)?;
            for row in rows {
                let values = &row.values;
                let key = ClientItemKey::new(text(&values[1])?).map_err(storage)?;
                by_key.insert(
                    key.clone(),
                    BatchUpdateSnapshotItem {
                        item_id: ItemId::new(text(&values[0])?).map_err(storage)?,
                        client_item_key: key,
                        item_version: nonnegative_u64(integer(&values[2])?, "item_version")?,
                        state: parse_state(&text(&values[3])?).map_err(storage)?,
                        fenced: integer(&values[4])? != 0,
                        superseded: false,
                    },
                );
            }
        }
        Ok(keys.iter().filter_map(|key| by_key.remove(key)).collect())
    }

    pub async fn server_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let rows = self.query(
                "SELECT item_id,client_item_key,item_version,lifecycle_state,priority,group_key,not_before,retry_count,payload,fields \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=0 LIMIT 1",
                vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), key.as_str().to_string().into()],
            ).await.map_err(storage)?;
            let Some(row) = rows.first() else {
                result.push(None);
                continue;
            };
            let values = &row.values;
            result.push(Some(LiveItemView {
                item_id: ItemId::new(text(&values[0])?).map_err(storage)?,
                client_item_key: ClientItemKey::new(text(&values[1])?).map_err(storage)?,
                item_version: nonnegative_u64(integer(&values[2])?, "item_version")?,
                lifecycle_state: parse_state(&text(&values[3])?).map_err(storage)?,
                priority: parse_priority(optional_text(&values[4])?)?,
                group_key: optional_text(&values[5])?
                    .map(GroupKey::new)
                    .transpose()
                    .map_err(storage)?,
                not_before: optional_integer(&values[6])?.map(nanos_ts),
                attempt_count: nonnegative_u32(integer(&values[7])?, "retry_count")?,
                payload: optional_blob(&values[8])?.map(Bytes::from),
                fields: fields_from_json(text(&values[9])?)?,
            }));
        }
        Ok(result)
    }

    pub async fn server_metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        let rows = self.query(
            "SELECT lifecycle_state,COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND superseded=0 GROUP BY lifecycle_state",
            vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into()],
        ).await.map_err(storage)?;
        let mut metrics = QueueMetrics::default();
        for row in rows {
            let count = nonnegative_u64(integer(&row.values[1])?, "lifecycle count")?;
            match text(&row.values[0])?.as_str() {
                "Pending" => metrics.pending = count,
                "Leased" => metrics.leased = count,
                "Complete" => metrics.complete = count,
                "Failed" => metrics.failed = count,
                _ => {}
            }
        }
        metrics.resident_terminal_count = metrics.complete.saturating_add(metrics.failed);
        Ok(metrics)
    }

    pub async fn server_terminal_emission_metrics(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<TerminalEmissionMetrics> {
        let metrics = self.server_metrics(shard).await?;
        Ok(TerminalEmissionMetrics {
            resident_terminal_count: metrics.resident_terminal_count,
            emission_lag_commands: 0,
            emission_oldest_unemitted_age_ms: 0,
        })
    }
}

impl AsyncProjectionStore for TursoRelational {
    fn supports_gates(&self) -> bool {
        true
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        async move {
            ensure_shard_owned(writer, definition).await?;
            Ok(())
        }
    }

    fn admit_mutation(
        &self,
        _shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let reader = self.reader.clone();
        async move {
            let connection = reader.lock().await;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let result = async {
                let definition = definition_in_transaction(&connection, &shard).await?;
                let mut keys = HashSet::new();
                let mut item_ids = HashSet::new();
                let mut group_order = Vec::new();
                let mut grouped = HashMap::<String, u64>::new();
                for item in &items {
                    if !keys.insert(item.client_item_key.as_str().to_string())
                        || !item_ids.insert(item.item_id.to_string())
                    {
                        return Err(EngineError::Conflict);
                    }
                    if item.cohort_size.is_some() && item.group_key.is_none() {
                        return Err(EngineError::Invalid("cohort_size requires group_key"));
                    }
                    if let Some(group) = &item.group_key {
                        let group = group.as_str().to_string();
                        let added = grouped.entry(group.clone()).or_insert_with(|| {
                            group_order.push(group);
                            0
                        });
                        *added += 1;
                    }
                }

                for chunk in items.chunks(PUSH_IDENTITY_CHECK_CHUNK) {
                    let mut params = vec![
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                        Value::Integer(ts_nanos(now)),
                    ];
                    for item in chunk {
                        params.extend([
                            Value::Text(item.item_id.to_string()),
                            Value::Text(item.client_item_key.as_str().to_string()),
                        ]);
                    }
                    let conflict = one_row(
                        &connection,
                        &format!(
                            "WITH requested(item_id,client_item_key) AS (VALUES {}) \
                             SELECT 1 FROM requested r WHERE EXISTS (SELECT 1 FROM fireweed_items i \
                               WHERE i.tenant_id=?1 AND i.queue_id=?2 AND \
                               (i.item_id=r.item_id OR (i.client_item_key=r.client_item_key AND i.superseded=0))) \
                             OR EXISTS (SELECT 1 FROM fireweed_item_key_retention k \
                               WHERE k.tenant_id=?1 AND k.queue_id=?2 \
                               AND k.client_item_key=r.client_item_key AND k.expires_at>?3) LIMIT 1",
                            numbered_values_rows(chunk.len(), 2, 4)
                        ),
                        params,
                    )
                    .await?;
                    if conflict.is_some() {
                        return Err(EngineError::Conflict);
                    }
                }

                if let Some(max) = definition.max_eligible_group_size {
                    for chunk in group_order.chunks(GROUP_COUNT_CHUNK) {
                        let placeholders = (0..chunk.len())
                            .map(|offset| format!("?{}", offset + 3))
                            .collect::<Vec<_>>()
                            .join(",");
                        let mut params =
                            vec![Value::Text(tenant.clone()), Value::Text(queue.clone())];
                        params.extend(chunk.iter().cloned().map(Value::Text));
                        let mut rows = connection
                            .query(
                                format!(
                                    "SELECT group_key,COUNT(*) FROM fireweed_items \
                                     WHERE tenant_id=?1 AND queue_id=?2 \
                                     AND group_key IN ({placeholders}) \
                                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 \
                                     GROUP BY group_key"
                                ),
                                params,
                            )
                            .await
                            .map_err(storage)?;
                        let mut counts = HashMap::with_capacity(chunk.len());
                        while let Some(row) = rows.next().await.map_err(storage)? {
                            counts.insert(
                                row.get::<String>(0).map_err(storage)?,
                                nonnegative_u64(
                                    row.get::<i64>(1).map_err(storage)?,
                                    "group count",
                                )?,
                            );
                        }
                        for group in chunk {
                            if counts
                                .get(group)
                                .copied()
                                .unwrap_or_default()
                                .saturating_add(grouped[group])
                                > max
                            {
                                return Err(EngineError::Conflict);
                            }
                        }
                    }
                }
                maintain_typed_indexes_on_insert(
                    &connection,
                    &tenant,
                    &queue,
                    &definition.typed_indexes,
                    &items,
                    false,
                )
                .await?;
                Ok(())
            }.await;
            result
        }
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<bool>> + Send {
        let reader = self.reader.clone();
        async move {
            let connection = reader.lock().await;
            let row = one_row(
                &connection,
                "SELECT pause_drain_intake FROM queues WHERE tenant=?1 AND queue=?2",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                ],
            )
            .await?;
            row.map(|row| integer(&row[0]).map(|paused| paused != 0))
                .unwrap_or(Err(EngineError::NotFound))
        }
    }

    fn push_idempotency(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: PushFingerprint,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send
    {
        let reader = self.reader.clone();
        async move {
            let connection = reader.lock().await;
            let row = one_row(&connection,
                "SELECT request_fingerprint,response_payload,expires_at FROM fireweed_request_idempotency \
                 WHERE tenant_id=?1 AND queue_id=?2 AND operation='push' AND request_id=?3",
                vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), request_id.as_str().to_string().into()]).await?;
            let Some(row) = row else {
                return Ok(IdempotencyDecision::Proceed);
            };
            if integer(&row[2])? <= ts_nanos(now) {
                return Ok(IdempotencyDecision::Expired);
            }
            let stored = blob(&row[0])?;
            if stored != fingerprint.canonical_sha256
                && stored != fingerprint.legacy_body_hash.0.to_be_bytes()
            {
                return Ok(IdempotencyDecision::Conflict);
            }
            let raw: Vec<String> = serde_json::from_str(&text(&row[1])?).map_err(storage)?;
            let ids = raw
                .into_iter()
                .map(|id| ItemId::new(id).map_err(storage))
                .collect::<EngineResult<Vec<_>>>()?;
            Ok(IdempotencyDecision::Replay(ids))
        }
    }

    fn renew_validate(
        &self,
        shard: QueueKey,
        targets: Vec<RenewTarget>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        async move {
            let connection = writer.lock().await;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let now_nanos = ts_nanos(now);
            for chunk in targets.chunks(VALIDATION_ITEM_CHUNK) {
                let rows = validation_rows_by_item(
                    &connection,
                    &tenant,
                    &queue,
                    &chunk.iter().map(|target| target.item_id).collect::<Vec<_>>(),
                    "lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash",
                )
                .await?;
                for target in chunk {
                    let row = rows.get(&target.item_id).ok_or(EngineError::NotFound)?;
                    let state = parse_state(&text(&row[0])?).map_err(storage)?;
                    if integer(&row[1])? != 0 {
                        return Err(EngineError::StaleLease);
                    }
                    if state.is_terminal() {
                        return Err(EngineError::Terminal);
                    }
                    if integer(&row[2])? != 0 {
                        return Err(EngineError::Superseded);
                    }
                    if !matches!(row[3], Value::Null) {
                        return Err(EngineError::Invalid("cohort member requires cohort lease"));
                    }
                    if state != ItemState::Leased {
                        return Err(EngineError::Invalid("item is not leased"));
                    }
                    if blob(&row[5])? != lease_hash(&target.lease_token)
                        || matches!(row[4], Value::Null)
                        || integer(&row[4])? < now_nanos
                    {
                        return Err(EngineError::StaleLease);
                    }
                }
            }
            Ok(())
        }
    }

    fn commit_validate(
        &self,
        shard: QueueKey,
        claim_refs: Vec<ClaimRef>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        async move {
            let connection = writer.lock().await;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let now_nanos = ts_nanos(now);
            for chunk in claim_refs.chunks(VALIDATION_ITEM_CHUNK) {
                let rows = validation_rows_by_item(
                    &connection,
                    &tenant,
                    &queue,
                    &chunk.iter().map(|c| c.item_id).collect::<Vec<_>>(),
                    "lifecycle_state,fenced,superseded,lease_expires_at,lease_token_hash,item_version",
                )
                .await?;
                for claim_ref in chunk {
                    let row = rows.get(&claim_ref.item_id).ok_or(EngineError::NotFound)?;
                    let state = parse_state(&text(&row[0])?).map_err(storage)?;
                    if integer(&row[1])? != 0 {
                        return Err(EngineError::StaleLease);
                    }
                    if state.is_terminal() {
                        return Err(EngineError::Terminal);
                    }
                    if integer(&row[2])? != 0 {
                        return Err(EngineError::Superseded);
                    }
                    if state != ItemState::Leased {
                        return Err(EngineError::Invalid("item is not leased"));
                    }
                    if blob(&row[4])? != lease_hash(&claim_ref.lease_token)
                        || matches!(row[3], Value::Null)
                        || integer(&row[3])? < now_nanos
                    {
                        return Err(EngineError::StaleLease);
                    }
                    if integer(&row[5])? as u64 != claim_ref.item_version {
                        return Err(EngineError::Conflict);
                    }
                }
            }
            Ok(())
        }
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<FinalizeTarget>,
        now: UtcTimestamp,
        _default_max_attempts: u32,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::FinalizeLeaseMember>>> + Send
    {
        let reader = self.reader.clone();
        async move {
            let connection = reader.lock().await;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let now_nanos = ts_nanos(now);
            let mut attempts = Vec::with_capacity(targets.len());
            for chunk in targets.chunks(VALIDATION_ITEM_CHUNK) {
                let rows = validation_rows_by_item(
                    &connection,
                    &tenant,
                    &queue,
                    &chunk.iter().map(|target| target.item_id).collect::<Vec<_>>(),
                    "lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash,item_version,retry_count,max_attempts",
                )
                .await?;
                for target in chunk {
                    let row = rows.get(&target.item_id).ok_or(EngineError::NotFound)?;
                    let state = parse_state(&text(&row[0])?).map_err(storage)?;
                    if integer(&row[1])? != 0 {
                        return Err(EngineError::StaleLease);
                    }
                    if state.is_terminal() {
                        return Err(EngineError::Terminal);
                    }
                    if integer(&row[2])? != 0 {
                        return Err(EngineError::Superseded);
                    }
                    if !matches!(row[3], Value::Null) {
                        return Err(EngineError::Invalid("cohort member requires cohort lease"));
                    }
                    if state != ItemState::Leased {
                        return Err(EngineError::Invalid("item is not leased"));
                    }
                    if blob(&row[5])? != lease_hash(&target.lease_token)
                        || matches!(row[4], Value::Null)
                        || integer(&row[4])? < now_nanos
                    {
                        return Err(EngineError::StaleLease);
                    }
                    let version = integer(&row[6])?;
                    if version < 0 || version as u64 != target.item_version {
                        return Err(EngineError::Conflict);
                    }
                    attempts.push(fireweed_engine::FinalizeLeaseMember {
                        item_id: target.item_id,
                        attempt_count: nonnegative_u32(integer(&row[7])?, "retry_count")?,
                        max_attempts: nonnegative_u32(integer(&row[8])?, "max_attempts")?,
                    });
                }
            }
            Ok(attempts)
        }
    }

    fn cohort_lease_validate(
        &self,
        shard: QueueKey,
        target: CohortLeaseTarget,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::CohortLeaseMember>>> + Send
    {
        let writer = self.writer.clone();
        async move {
            let mut connection = writer.lock().await;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage)?;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let result = async {
                let row = one_row(
                    &transaction,
                    "SELECT group_key,state,cohort_size,member_count,cohort_lease_token_hash \
                     FROM fireweed_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                    vec![
                        tenant.clone().into(),
                        queue.clone().into(),
                        target.cohort_id.as_str().to_string().into(),
                    ],
                )
                .await?
                .ok_or(EngineError::NotFound)?;
                let group = text(&row[0])?;
                let state = text(&row[1])?;
                let expected = integer(&row[2])?;
                let recorded = integer(&row[3])?;
                if state == "terminal" {
                    return Err(EngineError::Terminal);
                }
                if state != "leased" {
                    return Err(EngineError::Invalid("cohort is not leased"));
                }
                if blob(&row[4])? != lease_hash(&target.cohort_lease_token) {
                    return Err(EngineError::StaleLease);
                }
                if expected <= 0 || expected != recorded {
                    return Err(EngineError::Conflict);
                }
                let mut rows = transaction
                    .query(
                        "SELECT item_id,lifecycle_state,fenced,superseded,lease_expires_at,retry_count,max_attempts \
                         FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
                         AND cohort_size IS NOT NULL AND superseded=0 \
                         AND lifecycle_state NOT IN ('Complete','Failed') \
                         ORDER BY priority_sort,created_seq",
                        vec![Value::Text(tenant), Value::Text(queue), Value::Text(group)],
                    )
                    .await
                    .map_err(storage)?;
                let mut item_ids = Vec::new();
                while let Some(row) = rows.next().await.map_err(storage)? {
                    let state =
                        parse_state(&row.get::<String>(1).map_err(storage)?).map_err(storage)?;
                    if row.get::<i64>(2).map_err(storage)? != 0 {
                        return Err(EngineError::StaleLease);
                    }
                    if state.is_terminal() {
                        return Err(EngineError::Terminal);
                    }
                    if row.get::<i64>(3).map_err(storage)? != 0 {
                        return Err(EngineError::Superseded);
                    }
                    if state != ItemState::Leased {
                        return Err(EngineError::Invalid("cohort member is not leased"));
                    }
                    let expires = row.get_value(4).map_err(storage)?;
                    if matches!(expires, Value::Null) || integer(&expires)? < ts_nanos(now) {
                        return Err(EngineError::StaleLease);
                    }
                    item_ids.push(fireweed_engine::CohortLeaseMember {
                        item_id: ItemId::new(row.get::<String>(0).map_err(storage)?)
                            .map_err(storage)?,
                        attempt_count: nonnegative_u32(
                            row.get::<i64>(5).map_err(storage)?,
                            "retry_count",
                        )?,
                        max_attempts: nonnegative_u32(
                            row.get::<i64>(6).map_err(storage)?,
                            "max_attempts",
                        )?,
                    });
                }
                if i64::try_from(item_ids.len()).map_err(storage)? != expected {
                    return Err(EngineError::Conflict);
                }
                Ok(item_ids)
            }
            .await;
            let rollback = transaction.rollback().await.map_err(storage);
            match (result, rollback) {
                (_, Err(error)) => Err(error),
                (Err(error), Ok(())) => Err(error),
                (Ok(item_ids), Ok(())) => Ok(item_ids),
            }
        }
    }

    fn purge_validate(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move { self.purge_items_validate(&shard, &ids, force).await }
    }

    fn expired_leases(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let writer = self.writer.clone();
        async move {
            if max == 0 {
                return Ok(Vec::new());
            }
            let limit = i64::try_from(max).map_err(storage)?;
            let connection = writer.lock().await;
            let mut rows = connection
                .query(
                    "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
                 AND lifecycle_state='Leased' AND cohort_size IS NULL AND fenced=0 AND superseded=0 \
                 AND lease_expires_at IS NOT NULL \
                 AND lease_expires_at<?3 ORDER BY item_id LIMIT ?4",
                    vec![
                        Value::Text(shard.tenant_id.as_str().to_string()),
                        Value::Text(shard.queue_id.as_str().to_string()),
                        Value::Integer(ts_nanos(now)),
                        Value::Integer(limit),
                    ],
                )
                .await
                .map_err(storage)?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await.map_err(storage)? {
                ids.push(ItemId::new(row.get::<String>(0).map_err(storage)?).map_err(storage)?);
            }
            Ok(ids)
        }
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        let tokens = self.live_tokens.clone();
        let by_consumer = self.live_tokens_by_consumer.clone();
        let shape = self.last_batch_update_shape.clone();
        let phase = self.last_apply_phase.clone();
        let grouped_shards = self.grouped_shards.clone();
        let claim_scan_hints = self.claim_scan_hints.clone();
        let claim_scan_default_fifo = self.claim_scan_default_fifo.clone();
        async move {
            apply_owned(
                writer,
                tokens,
                by_consumer,
                shape,
                phase,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                positions,
                commands,
                true,
            )
            .await
        }
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        let tokens = self.live_tokens.clone();
        let by_consumer = self.live_tokens_by_consumer.clone();
        let shape = self.last_batch_update_shape.clone();
        let phase = self.last_apply_phase.clone();
        let grouped_shards = self.grouped_shards.clone();
        let claim_scan_hints = self.claim_scan_hints.clone();
        let claim_scan_default_fifo = self.claim_scan_default_fifo.clone();
        async move {
            apply_owned(
                writer,
                tokens,
                by_consumer,
                shape,
                phase,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                positions,
                commands,
                false,
            )
            .await
        }
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let writer = self.writer.clone();
        async move {
            let mut connection = writer.lock().await;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .await
                .map_err(storage)?;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let paused = one_row(
                &transaction,
                "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
                vec![tenant.clone().into(), queue.clone().into()],
            )
            .await?;
            let Some(paused) = paused else {
                transaction.rollback().await.map_err(storage)?;
                return Err(EngineError::NotFound);
            };
            if integer(&paused[0])? != 0 {
                transaction.commit().await.map_err(storage)?;
                return Ok(Vec::new());
            }
            let mut rows = transaction
                .query(
                    sql::SELECT_ELIGIBLE,
                    vec![
                        Value::Text(tenant),
                        Value::Text(queue),
                        Value::Integer(ts_nanos(now)),
                        Value::Integer(i64::try_from(max).map_err(storage)?),
                    ],
                )
                .await
                .map_err(storage)?;
            let mut eligible = Vec::new();
            while let Some(row) = rows.next().await.map_err(storage)? {
                eligible.push(
                    ItemId::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?,
                );
            }
            drop(rows);
            transaction.commit().await.map_err(storage)?;
            Ok(eligible)
        }
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let writer = self.writer.clone();
        async move {
            if max == 0 {
                return Ok(Vec::new());
            }
            if compatibility.group_key.is_none() && compatibility.metadata_equals.is_empty() {
                return self.eligible_candidates(shard, now, max).await;
            }
            let mut connection = writer.lock().await;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .await
                .map_err(storage)?;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let result =
                async {
                    if queue_paused(&transaction, &tenant, &queue).await? {
                        return Ok(Vec::new());
                    }
                    let required_group = compatibility
                        .group_key
                        .as_ref()
                        .map_or(Value::Null, |group| Value::Text(group.as_str().to_string()));
                    let metadata_filter = metadata_to_json(&Metadata::from_entries(
                        compatibility.metadata_equals.clone(),
                    ))?;
                    let mut rows = transaction
                        .query(
                            "SELECT item_id FROM fireweed_items \
                             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' \
                             AND superseded=0 AND cohort_size IS NULL \
                             AND (not_before IS NULL OR not_before<=?3) AND eligible_since IS NOT NULL \
                             AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
                               JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id \
                               AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                               WHERE ig.tenant_id=fireweed_items.tenant_id \
                               AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
                             AND (?5 IS NULL OR group_key=?5) \
                             AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
                               WHERE NOT EXISTS (SELECT 1 FROM json_each(fireweed_items.metadata) actual \
                                 WHERE actual.key=wanted.key AND actual.value=wanted.value \
                                   AND actual.type=wanted.type)) \
                             ORDER BY priority_sort,created_seq LIMIT ?4",
                            vec![
                                tenant.clone().into(),
                                queue.clone().into(),
                                Value::Integer(ts_nanos(now)),
                                Value::Integer(max as i64),
                                required_group,
                                Value::Text(metadata_filter),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                    let mut selected = Vec::new();
                    while let Some(row) = rows.next().await.map_err(storage)? {
                        selected.push(
                            ItemId::new(text(&row.get_value(0).map_err(storage)?)?)
                                .map_err(storage)?,
                        );
                    }
                    Ok(selected)
                }
                .await;
            let rollback = transaction.rollback().await.map_err(storage);
            match (result, rollback) {
                (_, Err(error)) => Err(error),
                (Err(error), Ok(())) => Err(error),
                (Ok(selected), Ok(())) => Ok(selected),
            }
        }
    }

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: ClaimUnit,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl std::future::Future<Output = EngineResult<RichClaimSelection>> + Send {
        let writer = self.writer.clone();
        async move {
            if matches!(unit, ClaimUnit::Item) {
                return Err(EngineError::Unavailable);
            }
            let mut connection = writer.lock().await;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage)?;
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let now = ts_nanos(now);
            let result = async {
                if matches!(unit, ClaimUnit::WholeGroup | ClaimUnit::SameGroupKey) {
                    refresh_due_group_summaries(&transaction, &tenant, &queue, now).await?;
                }
                if queue_paused(&transaction, &tenant, &queue).await? {
                    return Ok(RichClaimSelection::default());
                }
                match unit {
                    ClaimUnit::Item => unreachable!("item unit rejected before transaction"),
                    ClaimUnit::WholeGroup => {
                        let max_groups = compatibility
                            .group_batching
                            .as_ref()
                            .map(|batching| batching.max_groups)
                            .unwrap_or(0);
                        Ok(RichClaimSelection {
                            item_ids: select_group_batching(
                                &transaction,
                                &tenant,
                                &queue,
                                now,
                                max_items,
                                max_groups,
                                &compatibility,
                            )
                            .await?,
                            cohort_id: None,
                        })
                    }
                    ClaimUnit::SameGroupKey => Ok(RichClaimSelection {
                        item_ids: select_same_group(
                            &transaction,
                            &tenant,
                            &queue,
                            now,
                            max_items,
                            &compatibility,
                        )
                        .await?,
                        cohort_id: None,
                    }),
                    ClaimUnit::WholeCohort => {
                        select_whole_cohort(
                            &transaction,
                            &tenant,
                            &queue,
                            now,
                            max_items,
                            &compatibility,
                        )
                        .await
                    }
                }
            }
            .await;
            let rollback = transaction.rollback().await.map_err(storage);
            match (result, rollback) {
                (_, Err(error)) => Err(error),
                (Err(error), Ok(())) => Err(error),
                (Ok(selection), Ok(())) => Ok(selection),
            }
        }
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        async move {
            // SQL is the serving authority. Bearer tokens come from fireweed_lease_bearers,
            // not the leftover process live_tokens map.
            let visible = ids.clone();
            let mut tokens = HashMap::<ItemId, LeaseToken>::new();
            let mut item_rows = HashMap::<ItemId, Vec<Value>>::with_capacity(visible.len());
            let mut gate_keys = HashMap::<ItemId, Vec<String>>::new();
            for chunk in visible.chunks(500) {
                let placeholders = (0..chunk.len())
                    .map(|index| format!("?{}", index + 3))
                    .collect::<Vec<_>>()
                    .join(",");
                let mut params = vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                ];
                params.extend(chunk.iter().map(|id| id.to_string().into()));
                let item_sql = format!(
                    "SELECT item_id,client_item_key,item_version,priority,group_key,not_before,\
                     lease_expires_at,retry_count,max_attempts,payload,fields,metadata,entity_document,index_fields \
                     FROM fireweed_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' \
                     AND item_id IN ({placeholders})"
                );
                for row in self
                    .query(item_sql, params.clone())
                    .await
                    .map_err(storage)?
                {
                    let id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
                    item_rows.insert(id, row.values[1..].to_vec());
                }
                let gate_sql = format!(
                    "SELECT item_id,gate_key FROM fireweed_item_gates WHERE tenant_id=?1 \
                     AND queue_id=?2 AND item_id IN ({placeholders}) ORDER BY item_id,gate_key"
                );
                for row in self
                    .query(gate_sql, params.clone())
                    .await
                    .map_err(storage)?
                {
                    let id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
                    gate_keys.entry(id).or_default().push(text(&row.values[1])?);
                }
                let bearer_sql = format!(
                    "SELECT item_id,lease_token FROM fireweed_lease_bearers \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id IN ({placeholders})"
                );
                for row in self.query(bearer_sql, params).await.map_err(storage)? {
                    let id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
                    let token = LeaseToken::new(text(&row.values[1])?).map_err(storage)?;
                    tokens.insert(id, token);
                }
            }
            let mut claimed = Vec::new();
            for id in ids {
                let Some(token) = tokens.get(&id).cloned() else {
                    continue;
                };
                let Some(values) = item_rows.get(&id) else {
                    continue;
                };
                let Some(expires) = optional_integer(&values[5])? else {
                    continue;
                };
                claimed.push(ClaimedItem {
                    item_id: id,
                    client_item_key: ClientItemKey::new(text(&values[0])?).map_err(storage)?,
                    item_version: nonnegative_u64(integer(&values[1])?, "item_version")?,
                    priority: parse_priority(optional_text(&values[2])?)?,
                    group_key: optional_text(&values[3])?
                        .map(GroupKey::new)
                        .transpose()
                        .map_err(storage)?,
                    not_before: optional_integer(&values[4])?.map(nanos_ts),
                    lease_token: Some(token),
                    lease_expires_at: nanos_ts(expires),
                    attempt_count: nonnegative_u32(integer(&values[6])?, "retry_count")?,
                    max_attempts: nonnegative_u32(integer(&values[7])?, "max_attempts")?,
                    payload: optional_blob(&values[8])?.map(Bytes::from),
                    fields: fields_from_json(text(&values[9])?)?,
                    metadata: metadata_from_json(text(&values[10])?)?,
                    entity: fireweed_engine::index_fields::echo_entity_document(
                        entity_from_json(optional_text(&values[11])?)?,
                        &fireweed_engine::index_fields::decode_index_fields_blob(
                            optional_blob(&values[12])?.as_deref(),
                        )?,
                    )?,
                    gate_keys: gate_keys.remove(&id).unwrap_or_default(),
                });
            }
            Ok(claimed)
        }
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send {
        async move {
            let rows = self
                .query(
                    sql::SELECT_ITEM_STATE,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        id.to_string().into(),
                    ],
                )
                .await
                .map_err(storage)?;
            rows.first()
                .map(|row| parse_state(&text(&row.values[0])?))
                .transpose()
        }
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        async move {
            let rows = self
                .query(
                    sql::SELECT_ITEM_VERSION,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        id.to_string().into(),
                    ],
                )
                .await
                .map_err(storage)?;
            rows.first()
                .map(|row| {
                    integer(&row.values[0]).and_then(|value| nonnegative_u64(value, "item_version"))
                })
                .transpose()
        }
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        async move {
            let rows = self
                .query(
                    sql::SELECT_CURSOR,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                    ],
                )
                .await
                .map_err(storage)?;
            let Some(row) = rows.first() else {
                return Ok(None);
            };
            let next = integer(&row.values[0])?;
            let epoch = integer(&row.values[1])?;
            if next <= 0 {
                return Ok(None);
            }
            Ok(Some(CommandPosition::new(
                shard,
                u64::try_from(epoch).map_err(storage)?,
                u64::try_from(next - 1).map_err(storage)?,
            )))
        }
    }

    fn instance_fence(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        async move {
            let rows = self
                .query(
                    sql::SELECT_INSTANCE_FENCE,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        Value::Blob(key),
                    ],
                )
                .await
                .map_err(storage)?;
            rows.first()
                .map(|row| {
                    integer(&row.values[0])
                        .and_then(|value| nonnegative_u64(value, "instance_fence"))
                })
                .transpose()
        }
    }

    fn index_validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let reader = self.reader.clone();
        async move {
            let connection = reader.lock().await;
            let definition = definition_in_transaction(&connection, &shard).await?;
            if definition.typed_indexes.is_empty() {
                return Ok(());
            }
            // Validation-only pass (persist = false): within-batch duplicates plus the same
            // DB-level unique lookups the apply arm runs at insert time.
            maintain_typed_indexes_on_insert(
                &connection,
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                &definition.typed_indexes,
                &items,
                false,
            )
            .await
        }
    }

    fn replay_durable_commit(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> impl std::future::Future<
        Output = EngineResult<Option<Vec<fireweed_engine::CommitOutcomeEntry>>>,
    > + Send {
        async move {
            let tenant = shard.tenant_id.as_str().to_string();
            let queue = shard.queue_id.as_str().to_string();
            let rows = self
                .query(
                    sql::SELECT_COMMIT_REPLAY,
                    vec![
                        tenant.clone().into(),
                        queue.clone().into(),
                        request_id.as_str().to_string().into(),
                    ],
                )
                .await
                .map_err(storage)?;
            let Some(row) = rows.first() else {
                return Ok(None);
            };
            let stored_fingerprint = blob(&row.values[0])?;
            let response_payload = text(&row.values[1])?;
            let expires_at = integer(&row.values[2])?;
            if expires_at <= ts_nanos(now) {
                // Mirror fireweed-sqlite: an elapsed retained row replays as None and is removed
                // (the SQL keeps the `expires_at` guard so a concurrent fresh re-record survives).
                self.execute(
                    sql::DELETE_EXPIRED_COMMIT_REPLAY,
                    vec![
                        tenant.into(),
                        queue.into(),
                        request_id.as_str().to_string().into(),
                        ts_nanos(now).into(),
                    ],
                )
                .await
                .map_err(storage)?;
                return Ok(None);
            }
            if stored_fingerprint != fingerprint.to_be_bytes() {
                return Err(EngineError::RequestIdConflict);
            }
            serde_json::from_str(&response_payload)
                .map(Some)
                .map_err(storage)
        }
    }

    fn read_durable_commit(
        &self,
        shard: QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<
        Output = EngineResult<Option<Vec<fireweed_engine::CommitOutcomeEntry>>>,
    > + Send {
        async move {
            let rows = self
                .query(
                    sql::SELECT_COMMIT_RECOVERY,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        request_id.as_str().to_string().into(),
                    ],
                )
                .await
                .map_err(storage)?;
            rows.first()
                .map(|row| serde_json::from_str(&text(&row.values[0])?).map_err(storage))
                .transpose()
        }
    }

    fn side_record(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        async move {
            let rows = self
                .query(
                    sql::SELECT_SIDE_RECORD,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        Value::Blob(key),
                    ],
                )
                .await
                .map_err(storage)?;
            rows.first()
                .map(|row| blob(&row.values[0]).map(Bytes::from))
                .transpose()
        }
    }

    fn side_records_by_prefix(
        &self,
        shard: QueueKey,
        prefix: Vec<u8>,
        page_size: usize,
        cursor: Option<Vec<u8>>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::SideRecordPage>> + Send
    {
        async move {
            // Server-side page bound, mirroring `fireweed-sqlite`'s `SIDE_RECORD_MAX_PAGE_SIZE`.
            const SIDE_RECORD_MAX_PAGE_SIZE: usize = 1_000;
            let page_size = page_size.min(SIDE_RECORD_MAX_PAGE_SIZE);
            let start = cursor.unwrap_or_else(|| prefix.clone());
            let limit = (page_size as i64).saturating_add(1);
            let rows = self
                .query(
                    sql::SELECT_SIDE_RECORDS_BY_PREFIX,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        Value::Blob(start),
                        limit.into(),
                    ],
                )
                .await
                .map_err(storage)?;
            let mut entries = Vec::new();
            let mut next_cursor = None;
            for row in rows {
                let key = blob(&row.values[0])?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if entries.len() == page_size {
                    next_cursor = Some(key);
                    break;
                }
                let payload = blob(&row.values[1])?;
                entries.push((key, Bytes::from(payload)));
            }
            Ok(fireweed_engine::SideRecordPage {
                entries,
                next_cursor,
            })
        }
    }

    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        async move {
            self.query(sql::SELECT_DEFINITIONS, vec![])
                .await
                .map_err(storage)?
                .into_iter()
                .map(|row| serde_json::from_str(&text(&row.values[0])?).map_err(storage))
                .collect()
        }
    }
}

/// Port-planning reads for the composed Turso products (bead fireweed-82211ac4): upsert key
/// lookup, update-fields guards, retained item-mutation replay, typed-index probes, and the
/// full-image loader behind `plan_item_mutation`.
impl TursoRelational {
    /// Live (non-superseded) item id for `client_item_key`, if any (upsert planning read; mirrors
    /// `fireweed-sqlite`'s `lookup_active_by_key`).
    pub async fn lookup_active_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        let rows = self
            .query(
                sql::SELECT_ACTIVE_BY_KEY,
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    client_item_key.as_str().to_string().into(),
                ],
            )
            .await
            .map_err(storage)?;
        rows.first()
            .map(|row| ItemId::new(text(&row.values[0])?).map_err(storage))
            .transpose()
    }

    /// Pre-append guard for `update_fields` (mirrors `fireweed-sqlite`'s
    /// `update_fields_validate_sql`): fenced → StaleLease, terminal → Terminal, superseded →
    /// Superseded, version mismatch → Conflict, absent → NotFound.
    pub async fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        let rows = self
            .query(
                sql::SELECT_UPDATE_FIELDS_GUARD,
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    id.to_string().into(),
                ],
            )
            .await
            .map_err(storage)?;
        let row = rows.first().ok_or(EngineError::NotFound)?;
        let state = parse_state(&text(&row.values[0])?).map_err(storage)?;
        if integer(&row.values[2])? != 0 {
            return Err(EngineError::StaleLease);
        }
        if state.is_terminal() {
            return Err(EngineError::Terminal);
        }
        if integer(&row.values[1])? != 0 {
            return Err(EngineError::Superseded);
        }
        let version = nonnegative_u64(integer(&row.values[3])?, "item_version")?;
        if expected_item_version.is_some_and(|expected| expected != version) {
            return Err(EngineError::Conflict);
        }
        Ok(())
    }

    /// Retained item-mutation response for request-id replay (mirrors `fireweed-sqlite`'s
    /// `replay_durable_item_mutation`: an elapsed row reads as `None` and is removed; a reused id
    /// with a different body is `RequestIdConflict`).
    pub async fn replay_durable_item_mutation(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<ItemMutationResponse>> {
        let tenant = shard.tenant_id.as_str().to_string();
        let queue = shard.queue_id.as_str().to_string();
        let rows = self
            .query(
                sql::SELECT_ITEM_MUTATION_REPLAY,
                vec![
                    tenant.clone().into(),
                    queue.clone().into(),
                    request_id.as_str().to_string().into(),
                ],
            )
            .await
            .map_err(storage)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let stored_fingerprint = blob(&row.values[0])?;
        let response_payload = text(&row.values[1])?;
        let positions_json = text(&row.values[2])?;
        let expires_at = integer(&row.values[3])?;
        if expires_at <= ts_nanos(now) {
            self.execute(
                sql::DELETE_EXPIRED_ITEM_MUTATION_REPLAY,
                vec![
                    tenant.into(),
                    queue.into(),
                    request_id.as_str().to_string().into(),
                    ts_nanos(now).into(),
                ],
            )
            .await
            .map_err(storage)?;
            return Ok(None);
        }
        if stored_fingerprint != fingerprint.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        let mut response: ItemMutationResponse =
            serde_json::from_str(&response_payload).map_err(storage)?;
        let decoded: Vec<(u64, u64)> = serde_json::from_str(&positions_json).map_err(storage)?;
        response.position = decoded
            .last()
            .map(|(epoch, sequence)| CommandPosition::new(shard.clone(), *epoch, *sequence));
        Ok(Some(response))
    }

    /// Materialize the queue's complete in-memory image and lift it into the shared
    /// [`ProjectionData`] planner state (async analogue of `fireweed-sqlite`'s
    /// `projection_data_sql`). Cost is O(resident queue), matching the sqlite relational planner.
    pub async fn projection_data(&self, shard: &QueueKey) -> EngineResult<ProjectionData> {
        let definition = {
            let connection = self.reader.lock().await;
            definition_in_transaction(&connection, shard).await?
        };
        let tenant = Value::Text(shard.tenant_id.as_str().to_string());
        let queue = Value::Text(shard.queue_id.as_str().to_string());
        let cursor = self
            .query(
                sql::SELECT_CURSOR_STATE,
                vec![tenant.clone(), queue.clone()],
            )
            .await
            .map_err(storage)?;
        let cursor = &cursor.first().ok_or(EngineError::NotFound)?.values;
        let next_seq = nonnegative_u64(integer(&cursor[0])?, "next_seq")?;
        let next_item_seq = nonnegative_u64(integer(&cursor[1])?, "next_item_seq")?;
        let assignment_epoch = nonnegative_u64(integer(&cursor[2])?, "assignment_epoch")?;
        let queue_row = self
            .query(
                "SELECT paused,pause_drain_intake FROM queues WHERE tenant=?1 AND queue=?2",
                vec![tenant.clone(), queue.clone()],
            )
            .await
            .map_err(storage)?;
        let queue_row = &queue_row.first().ok_or(EngineError::NotFound)?.values;
        let paused = integer(&queue_row[0])? != 0;
        let pause_drain_intake = integer(&queue_row[1])? != 0;

        let mut gates: HashMap<String, Vec<String>> = HashMap::new();
        for row in self
            .query(
                "SELECT item_id,gate_key FROM fireweed_item_gates \
                 WHERE tenant_id=?1 AND queue_id=?2 ORDER BY item_id,gate_key",
                vec![tenant.clone(), queue.clone()],
            )
            .await
            .map_err(storage)?
        {
            gates
                .entry(text(&row.values[0])?)
                .or_default()
                .push(text(&row.values[1])?);
        }
        let mut blocked_gates = BTreeSet::new();
        for row in self
            .query(
                "SELECT gate_key FROM fireweed_gate_state WHERE tenant_id=?1 AND queue_id=?2",
                vec![tenant.clone(), queue.clone()],
            )
            .await
            .map_err(storage)?
        {
            blocked_gates.insert(text(&row.values[0])?);
        }

        let rows = self
            .query(
                "SELECT item_id,client_item_key,lifecycle_state,priority,not_before,eligible_since,\
                 group_key,cohort_size,payload,fields,metadata,entity_document,retry_count,\
                 item_version,lease_expires_at,worker_id,fenced,superseded,max_attempts,\
                 created_seq,last_command_sequence,terminal_at,terminal_command_epoch \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
                 ORDER BY created_seq,item_id",
                vec![tenant.clone(), queue.clone()],
            )
            .await
            .map_err(storage)?;
        let live_tokens = self.live_tokens.lock().await;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let values = row.values;
            let item_id_text = text(&values[0])?;
            let item_id = ItemId::new(&item_id_text).map_err(storage)?;
            items.push(ProjectionImageItem {
                item_id,
                client_item_key: ClientItemKey::new(text(&values[1])?).map_err(storage)?,
                state: parse_state(&text(&values[2])?).map_err(storage)?,
                priority: parse_priority(optional_text(&values[3])?)?,
                not_before: optional_integer(&values[4])?.map(nanos_ts),
                eligible_since: optional_integer(&values[5])?.map(nanos_ts),
                group_key: optional_text(&values[6])?
                    .map(|value| GroupKey::new(value).map_err(storage))
                    .transpose()?,
                cohort_size: optional_integer(&values[7])?
                    .map(|value| nonnegative_u64(value, "cohort_size"))
                    .transpose()?,
                payload: optional_blob(&values[8])?.map(Bytes::from),
                fields: fields_from_json(text(&values[9])?)?,
                metadata: metadata_from_json(text(&values[10])?)?,
                gate_keys: gates.remove(&item_id_text).unwrap_or_default(),
                index_fields: Default::default(),
                entity_document: optional_text(&values[11])?
                    .map(|value| serde_json::from_str(&value).map_err(storage))
                    .transpose()?,
                attempt_count: nonnegative_u32(integer(&values[12])?, "retry_count")?,
                item_version: nonnegative_u64(integer(&values[13])?, "item_version")?,
                lease_token: live_tokens.get(&(shard.clone(), item_id)).cloned(),
                lease_expires_at: optional_integer(&values[14])?.map(nanos_ts),
                lease_is_cohort: optional_integer(&values[7])?.is_some(),
                worker_id: optional_text(&values[15])?
                    .map(|value| WorkerId::new(value).map_err(storage))
                    .transpose()?,
                fenced: integer(&values[16])? != 0,
                superseded: integer(&values[17])? != 0,
                max_attempts: nonnegative_u32(integer(&values[18])?, "max_attempts")?,
                created_seq: nonnegative_u64(integer(&values[19])?, "created_seq")?,
                terminal_at: optional_integer(&values[21])?.map(nanos_ts),
                terminal_position: optional_integer(&values[22])?
                    .map(|epoch| -> EngineResult<CommandPosition> {
                        Ok(CommandPosition::new(
                            shard.clone(),
                            nonnegative_u64(epoch, "terminal_command_epoch")?,
                            nonnegative_u64(integer(&values[20])?, "last_command_sequence")?,
                        ))
                    })
                    .transpose()?,
            });
        }
        drop(live_tokens);

        let mut side_records = BTreeMap::new();
        for row in self
            .query(
                "SELECT key,payload FROM fireweed_side_records \
                 WHERE tenant_id=?1 AND queue_id=?2 ORDER BY key",
                vec![tenant.clone(), queue.clone()],
            )
            .await
            .map_err(storage)?
        {
            side_records.insert(blob(&row.values[0])?, Bytes::from(blob(&row.values[1])?));
        }
        let mut instance_fences = BTreeMap::new();
        for row in self
            .query(
                "SELECT instance_key,fence FROM fireweed_instance_fences \
                 WHERE tenant_id=?1 AND queue_id=?2 ORDER BY instance_key",
                vec![tenant, queue],
            )
            .await
            .map_err(storage)?
        {
            instance_fences.insert(
                blob(&row.values[0])?,
                nonnegative_u64(integer(&row.values[1])?, "instance_fence")?,
            );
        }
        let image = ProjectionImage {
            high_water: (next_seq > 0).then(|| {
                CommandPosition::new(shard.clone(), assignment_epoch, next_seq.saturating_sub(1))
            }),
            paused,
            pause_drain_intake,
            blocked_gates,
            next_seq: next_item_seq,
            items,
            side_records,
            instance_fences,
            metrics: self.server_metrics(shard).await?,
        };
        ProjectionData::from_image(&definition, image)
    }

    /// Resolve and validate one backend-erased mutation against a single immutable queue image
    /// via the shared in-memory planner (the same planner the sqlite relational backend uses).
    pub async fn plan_item_mutation(
        &self,
        shard: &QueueKey,
        request: &ItemMutationRequest,
    ) -> EngineResult<ItemMutationPlan> {
        self.projection_data(shard)
            .await?
            .plan_item_mutation(request)
    }

    /// ADR-010 §6 unique typed-index point read (mirrors the postgres relational implementation).
    pub async fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        let qi = self.typed_index_declaration(shard, index).await?;
        if !index_is_unique(&qi) {
            return Err(EngineError::Invalid("secondary index is not unique"));
        }
        let hits = self.typed_index_hits(shard, &qi, index, key, 1).await?;
        Ok(hits.into_iter().next())
    }

    /// ADR-010 §6 typed-index multi-hit lookup (mirrors the postgres relational implementation).
    pub async fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        let qi = self.typed_index_declaration(shard, index).await?;
        self.typed_index_hits(shard, &qi, index, key, -1).await
    }

    async fn typed_index_declaration(
        &self,
        shard: &QueueKey,
        index: &str,
    ) -> EngineResult<QueueIndex> {
        let definition = {
            let connection = self.reader.lock().await;
            definition_in_transaction(&connection, shard).await?
        };
        definition
            .typed_indexes
            .iter()
            .find(|qi| qi.name == index)
            .cloned()
            .ok_or(EngineError::Invalid("unknown secondary index"))
    }

    async fn typed_index_hits(
        &self,
        shard: &QueueKey,
        qi: &QueueIndex,
        index: &str,
        key: &[Vec<u8>],
        limit: i64,
    ) -> EngineResult<Vec<IndexHit>> {
        let expected_arity = match &qi.declaration {
            IndexDeclaration::Single(_) => 1,
            IndexDeclaration::Compound(definition) => definition.fields.len(),
        };
        if key.len() != expected_arity {
            return Err(EngineError::Invalid("secondary index key arity mismatch"));
        }
        let canonical = fireweed_relational::typed_lookup_canonical_key(qi, key)?;
        self.query(
            sql::SELECT_INDEX_HITS,
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                index.to_string().into(),
                Value::Blob(canonical),
                limit.into(),
            ],
        )
        .await
        .map_err(storage)?
        .into_iter()
        .map(|row| {
            Ok(IndexHit {
                item_id: ItemId::new(text(&row.values[0])?).map_err(storage)?,
                client_item_key: ClientItemKey::new(text(&row.values[1])?).map_err(storage)?,
                item_version: nonnegative_u64(integer(&row.values[2])?, "item_version")?,
            })
        })
        .collect()
    }
}

#[cfg(test)]
mod push_batch_lowering_tests {
    use fireweed_conformance::item;
    use fireweed_core::{IndexDeclaration, IndexDef, IndexType, ItemId, QueueIndex};

    use super::{
        COHORT_ACTIVE_WRITE_CHUNK, COHORT_GENERATION_WRITE_CHUNK, COHORT_READ_CHUNK,
        GATE_BLOCK_WRITE_CHUNK, GROUP_COUNT_CHUNK, GROUP_SUMMARY_CHUNK, KEY_RETENTION_WRITE_CHUNK,
        PUSH_GATE_CHUNK, PUSH_IDENTITY_CHECK_CHUNK, PUSH_INDEX_CHUNK, PUSH_ITEM_CHUNK,
        SCHEDULE_UPDATE_CHUNK, SIDE_RECORD_WRITE_CHUNK, UNIQUE_CHECK_CHUNK, VALIDATION_ITEM_CHUNK,
        index_is_unique, typed_index_keys,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct StatementShape {
        item_inserts: usize,
        gate_inserts: usize,
        unique_checks: usize,
        index_inserts: usize,
    }

    fn statement_shape(
        items: &[fireweed_engine::PushItem],
        indexes: &[QueueIndex],
    ) -> StatementShape {
        let gate_rows = items.iter().map(|item| item.gate_keys.len()).sum::<usize>();
        let keys = items
            .iter()
            .flat_map(|item| {
                typed_index_keys(indexes, &item.index_fields, item.entity_document.as_ref())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let unique_rows = keys
            .iter()
            .filter(|(name, _)| {
                indexes
                    .iter()
                    .find(|index| index.name == *name)
                    .is_some_and(index_is_unique)
            })
            .count();
        StatementShape {
            item_inserts: items.len().div_ceil(PUSH_ITEM_CHUNK),
            gate_inserts: gate_rows.div_ceil(PUSH_GATE_CHUNK),
            unique_checks: unique_rows.div_ceil(UNIQUE_CHECK_CHUNK),
            index_inserts: keys.len().div_ceil(PUSH_INDEX_CHUNK),
        }
    }

    fn indexed_gated_items(count: usize) -> (Vec<fireweed_engine::PushItem>, Vec<QueueIndex>) {
        let items = (0..count)
            .map(|offset| {
                let mut item = item(
                    &ItemId::from_u64(offset as u64 + 1).to_string(),
                    &format!("batch-{offset}"),
                    0,
                );
                item.gate_keys = vec![format!("gate-{offset}")];
                item.entity_document = Some(serde_json::json!({
                    "email": format!("user-{offset}@example.com")
                }));
                item
            })
            .collect();
        let indexes = vec![QueueIndex {
            name: "by_email".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "email".to_string(),
                index_type: IndexType::String,
                unique: true,
            }),
        }];
        (items, indexes)
    }

    #[test]
    fn accepted_push_statement_count_is_constant_within_item_chunk() {
        let (one, indexes) = indexed_gated_items(1);
        let (full_chunk, _) = indexed_gated_items(PUSH_ITEM_CHUNK);
        let expected = StatementShape {
            item_inserts: 1,
            gate_inserts: 1,
            unique_checks: 1,
            index_inserts: 1,
        };
        assert_eq!(statement_shape(&one, &indexes), expected);
        assert_eq!(statement_shape(&full_chunk, &indexes), expected);

        let (over_chunk, _) = indexed_gated_items(PUSH_ITEM_CHUNK + 1);
        assert_eq!(
            statement_shape(&over_chunk, &indexes),
            StatementShape {
                item_inserts: 2,
                gate_inserts: 1,
                unique_checks: 1,
                index_inserts: 1,
            },
            "only crossing the declared bind-safe chunk adds an item statement"
        );
    }

    #[test]
    fn group_summary_await_count_grows_only_at_bind_safe_chunk_boundaries() {
        let awaited_statements = |groups: usize| groups.div_ceil(GROUP_SUMMARY_CHUNK);
        assert_eq!(awaited_statements(0), 0);
        assert_eq!(awaited_statements(1), 1);
        assert_eq!(awaited_statements(GROUP_SUMMARY_CHUNK), 1);
        assert_eq!(awaited_statements(GROUP_SUMMARY_CHUNK + 1), 2);
        assert_eq!(awaited_statements(GROUP_SUMMARY_CHUNK * 10), 10);
    }

    #[test]
    fn lease_validation_select_count_grows_only_at_bind_safe_chunk_boundaries() {
        let awaited_selects = |items: usize| items.div_ceil(VALIDATION_ITEM_CHUNK);
        assert_eq!(awaited_selects(0), 0);
        assert_eq!(awaited_selects(1), 1);
        assert_eq!(awaited_selects(100), 1);
        assert_eq!(awaited_selects(1_000), 2);

        let source = include_str!("projection.rs");
        let helper = source
            .split("async fn validation_rows_by_item(")
            .nth(1)
            .unwrap()
            .split("fn typed_index_keys(")
            .next()
            .unwrap();
        assert_eq!(helper.matches(".query(").count(), 1);

        let purge = source
            .split("pub(crate) async fn purge_items_validate(")
            .nth(1)
            .unwrap()
            .split("/// RESP/server read surface")
            .next()
            .unwrap();
        // renew_validate is followed by commit_validate (also set-based), then finalize_validate.
        // Bound each body to the next sibling method so an intervening validation helper is not
        // attributed to renew.
        let renew = source
            .split("fn renew_validate(")
            .nth(1)
            .unwrap()
            .split("fn commit_validate(")
            .next()
            .unwrap();
        let finalize = source
            .split("fn finalize_validate(")
            .nth(1)
            .unwrap()
            .split("fn cohort_lease_validate(")
            .next()
            .unwrap();
        for operation in [purge, renew, finalize] {
            assert_eq!(operation.matches("validation_rows_by_item(").count(), 1);
            assert!(!operation.contains("one_row("));
        }
    }

    #[test]
    fn mutation_round_trips_scale_by_bind_chunks_not_input_cardinality() {
        #[derive(Debug, PartialEq, Eq)]
        struct RoundTrips {
            push_identity_reads: usize,
            group_count_reads: usize,
            cohort_reads: usize,
            cohort_generation_writes: usize,
            cohort_active_writes: usize,
            schedule_writes: usize,
            gate_block_writes: usize,
            side_record_writes: usize,
            retention_writes: usize,
        }
        let shape = |cardinality: usize| RoundTrips {
            push_identity_reads: cardinality.div_ceil(PUSH_IDENTITY_CHECK_CHUNK),
            group_count_reads: cardinality.div_ceil(GROUP_COUNT_CHUNK),
            cohort_reads: cardinality.div_ceil(COHORT_READ_CHUNK),
            cohort_generation_writes: cardinality.div_ceil(COHORT_GENERATION_WRITE_CHUNK),
            cohort_active_writes: cardinality.div_ceil(COHORT_ACTIVE_WRITE_CHUNK),
            schedule_writes: cardinality.div_ceil(SCHEDULE_UPDATE_CHUNK),
            gate_block_writes: cardinality.div_ceil(GATE_BLOCK_WRITE_CHUNK),
            side_record_writes: cardinality.div_ceil(SIDE_RECORD_WRITE_CHUNK),
            retention_writes: cardinality.div_ceil(KEY_RETENTION_WRITE_CHUNK),
        };
        assert_eq!(
            shape(1),
            RoundTrips {
                push_identity_reads: 1,
                group_count_reads: 1,
                cohort_reads: 1,
                cohort_generation_writes: 1,
                cohort_active_writes: 1,
                schedule_writes: 1,
                gate_block_writes: 1,
                side_record_writes: 1,
                retention_writes: 1,
            }
        );
        assert_eq!(
            shape(100),
            RoundTrips {
                push_identity_reads: 1,
                group_count_reads: 1,
                cohort_reads: 1,
                cohort_generation_writes: 2,
                cohort_active_writes: 1,
                schedule_writes: 1,
                gate_block_writes: 1,
                side_record_writes: 1,
                retention_writes: 1,
            }
        );
        assert_eq!(
            shape(1_000),
            RoundTrips {
                push_identity_reads: 3,
                group_count_reads: 2,
                cohort_reads: 2,
                cohort_generation_writes: 12,
                cohort_active_writes: 5,
                schedule_writes: 4,
                gate_block_writes: 4,
                side_record_writes: 5,
                retention_writes: 6,
            }
        );

        let source = include_str!("projection.rs");
        let cohorts = source
            .split("async fn upsert_cohorts(")
            .nth(1)
            .unwrap()
            .split("async fn cohort_item_ids(")
            .next()
            .unwrap();
        assert!(!cohorts.contains("for (group, (size, added))"));
        assert!(cohorts.contains("cohort_order.chunks(COHORT_READ_CHUNK)"));
        assert!(cohorts.contains("generation_rows.chunks(COHORT_GENERATION_WRITE_CHUNK)"));
        assert!(cohorts.contains("active_rows.chunks(COHORT_ACTIVE_WRITE_CHUNK)"));

        let validate = source
            .split("fn validate_push(")
            .nth(1)
            .unwrap()
            .split("fn pause_blocks_intake(")
            .next()
            .unwrap();
        let item_validation = validate
            .split("for item in &items {")
            .nth(1)
            .unwrap()
            .split("for chunk in items.chunks")
            .next()
            .unwrap();
        assert!(!item_validation.contains(".await"));
        assert!(validate.contains("items.chunks(PUSH_IDENTITY_CHECK_CHUNK)"));
        assert!(validate.contains("group_order.chunks(GROUP_COUNT_CHUNK)"));
    }

    #[test]
    fn rich_claim_and_replay_lowering_is_set_based_and_request_bounded() {
        let source = include_str!("projection.rs");
        let group = source
            .split("async fn select_group_batching(")
            .nth(1)
            .unwrap()
            .split("async fn select_same_group(")
            .next()
            .unwrap();
        assert_eq!(group.matches(".query(").count(), 1);
        assert!(group.contains("LIMIT ?4"));
        assert!(group.contains("LIMIT ?6"));
        assert!(!group.contains("OFFSET"));
        assert!(!group.contains("group_eligible_items("));

        let cohort = source
            .split("async fn select_whole_cohort(")
            .nth(1)
            .unwrap()
            .split("async fn cohort_state(")
            .next()
            .unwrap();
        assert!(cohort.contains("LIMIT 1"));
        assert!(!cohort.contains("for ("));
        assert!(!cohort.contains("OFFSET"));

        let replay = source
            .split("async fn extend_claim_by_query_replays(")
            .nth(1)
            .unwrap()
            .split("async fn definition_in_transaction(")
            .next()
            .unwrap();
        assert_eq!(replay.matches(".execute(").count(), 1);
        assert!(!replay.contains(".query("));
        assert!(!replay.contains("for request_id"));
        for cardinality in [1_usize, 100, 1_000] {
            let awaited_sql_statements = usize::from(cardinality > 0);
            assert_eq!(awaited_sql_statements, 1);
        }
    }
}

#[cfg(test)]
mod deterministic_cancellation_tests {
    use std::future;
    use std::sync::Arc;

    use fireweed_conformance::{envelope, item, qdef, ts};
    use fireweed_core::{BodyHash, ItemId, ItemState, RequestId};
    use fireweed_engine::{
        AsyncProjectionStore, CommandPosition, IdempotencyDecision, PushCommand, PushFingerprint,
        QueueCommand, QueueKey, RequestOutcome, push_items_fingerprint_sha256,
    };
    use tokio::sync::oneshot;

    use super::TursoRelational;

    fn replayable_push(
        shard: &QueueKey,
        id: ItemId,
        sequence: u64,
        request_id: &str,
        item_count: usize,
    ) -> (
        CommandPosition,
        fireweed_engine::CommandEnvelope,
        RequestId,
        PushFingerprint,
    ) {
        let items = (0..item_count)
            .map(|offset| {
                item(
                    &id.as_u64().saturating_add(offset as u64).to_string(),
                    &format!("cancel-key-{id}-{offset}"),
                    0,
                )
            })
            .collect::<Vec<_>>();
        let ids = items.iter().map(|item| item.item_id).collect::<Vec<_>>();
        let fingerprint = PushFingerprint {
            canonical_sha256: push_items_fingerprint_sha256(&items).unwrap(),
            legacy_body_hash: BodyHash(7),
        };
        let request_id = RequestId::new(request_id).unwrap();
        let mut command = envelope(QueueCommand::Push(PushCommand { items }), ids.clone());
        command.request_id = Some(request_id.clone());
        command.request_fingerprint = Some(fingerprint.legacy_body_hash.0);
        command.request_outcome = Some(RequestOutcome::Push { item_ids: ids });
        (
            CommandPosition::new(shard.clone(), 0, sequence),
            command,
            request_id,
            fingerprint,
        )
    }

    async fn replay(
        store: &TursoRelational,
        shard: &QueueKey,
        request_id: RequestId,
        fingerprint: PushFingerprint,
    ) -> IdempotencyDecision<Vec<ItemId>> {
        AsyncProjectionStore::push_idempotency(store, shard.clone(), request_id, fingerprint, ts(1))
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_started_and_resolved_cancellation_cuts_do_not_strand_writer_or_outcome() {
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let store = Arc::new(TursoRelational::in_memory().await.unwrap());
        AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
            .await
            .unwrap();

        // Queued cut: the writer is held before the apply future can start a transaction.
        let guard = store.writer.lock().await;
        let queued_id = ItemId::new("401").unwrap();
        let (position, command, request_id, fingerprint) =
            replayable_push(&shard, queued_id, 0, "queued-cut", 1);
        let queued_store = Arc::clone(&store);
        let (entered_tx, entered_rx) = oneshot::channel();
        let queued = tokio::spawn(async move {
            entered_tx.send(()).unwrap();
            AsyncProjectionStore::apply_live(queued_store.as_ref(), vec![position], vec![command])
                .await
        });
        entered_rx.await.unwrap();
        tokio::task::yield_now().await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        drop(guard);
        assert_eq!(
            AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), queued_id)
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            replay(store.as_ref(), &shard, request_id, fingerprint).await,
            IdempotencyDecision::Proceed
        ));

        // Started cut: wait until apply owns the writer, then abort while a deliberately large transaction
        // is being staged. The driver rolls back or finishes atomically; either outcome is replayable.
        let started_id = ItemId::new("500").unwrap();
        let (position, command, request_id, fingerprint) =
            replayable_push(&shard, started_id, 0, "started-cut", 512);
        let started_store = Arc::clone(&store);
        let started = tokio::spawn(async move {
            AsyncProjectionStore::apply_live(started_store.as_ref(), vec![position], vec![command])
                .await
        });
        while store.writer.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        started.abort();
        let _ = started.await;
        let state = AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), started_id)
            .await
            .unwrap();
        match state {
            None => assert!(matches!(
                replay(store.as_ref(), &shard, request_id, fingerprint).await,
                IdempotencyDecision::Proceed
            )),
            Some(ItemState::Pending) => assert!(matches!(
                replay(store.as_ref(), &shard, request_id, fingerprint).await,
                IdempotencyDecision::Replay(_)
            )),
            other => panic!("unexpected started-cut state: {other:?}"),
        }

        let next = AsyncProjectionStore::recovery_high_water(store.as_ref(), shard.clone())
            .await
            .unwrap()
            .map_or(0, |position| position.sequence + 1);
        let resolved_id = ItemId::new("2000").unwrap();
        let (position, command, request_id, fingerprint) =
            replayable_push(&shard, resolved_id, next, "resolved-cut", 1);
        let resolved_store = Arc::clone(&store);
        let (resolved_tx, resolved_rx) = oneshot::channel();
        let resolved = tokio::spawn(async move {
            let result = AsyncProjectionStore::apply_live(
                resolved_store.as_ref(),
                vec![position],
                vec![command],
            )
            .await;
            resolved_tx.send(()).unwrap();
            future::pending::<()>().await;
            result
        });
        resolved_rx.await.unwrap();
        resolved.abort();
        assert!(resolved.await.unwrap_err().is_cancelled());
        assert_eq!(
            AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), resolved_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        assert!(matches!(
            replay(store.as_ref(), &shard, request_id.clone(), fingerprint).await,
            IdempotencyDecision::Replay(_)
        ));
        assert!(matches!(
            replay(
                store.as_ref(),
                &shard,
                request_id,
                PushFingerprint {
                    canonical_sha256: [0xff; 32],
                    legacy_body_hash: BodyHash(u64::MAX),
                },
            )
            .await,
            IdempotencyDecision::Conflict
        ));
    }
}

#[cfg(test)]
mod item_mutation_tests {
    use bytes::Bytes;
    use fireweed_conformance::{envelope, item, qdef, ts};
    use fireweed_core::{ItemState, LeaseToken, RequestId};
    use fireweed_engine::{
        AsyncProjectionStore, ClaimCommand, CommandPosition, GateChange, ItemMutationResponse,
        ItemMutationSummary, MutateItemsCommand, PushCommand, QueueCommand, QueueKey,
        RequestOutcome, ResolvedItemMutation, ResolvedItemMutationAction, ResolvedItemValues,
    };
    use turso::Value;

    use super::ts_nanos;
    use crate::{TursoConfig, TursoRelational};

    fn replacement(
        pushed: &fireweed_engine::PushItem,
        version: u64,
        payload: &'static [u8],
    ) -> ResolvedItemMutation {
        let mut fields = pushed.fields.clone();
        fields.insert("phase".to_string(), Bytes::from_static(b"mutated"));
        ResolvedItemMutation {
            item_id: pushed.item_id,
            action: ResolvedItemMutationAction::Replace(Box::new(ResolvedItemValues {
                state: ItemState::Pending,
                item_version: version,
                priority: pushed.priority.clone(),
                not_before: Some(ts(50)),
                eligible_since: ts(50),
                payload: Some(Bytes::from_static(payload)),
                fields,
                metadata: pushed.metadata.clone(),
                gate_keys: vec!["item-block".to_string()],
                index_fields: Default::default(),
                entity_document: pushed.entity_document.clone(),
                invalidate_lease: false,
            })),
        }
    }

    fn mutation_envelope(
        item_mutations: Vec<ResolvedItemMutation>,
        request_id: &str,
        fingerprint: u64,
    ) -> fireweed_engine::CommandEnvelope {
        let item_ids = item_mutations.iter().map(|item| item.item_id).collect();
        let request_id = RequestId::new(request_id).unwrap();
        let response = ItemMutationResponse {
            request_id: request_id.clone(),
            position: None,
            dry_run: false,
            results: Vec::new(),
            selectors: Vec::new(),
            summary: ItemMutationSummary::default(),
        };
        let mut command = envelope(
            QueueCommand::MutateItems(MutateItemsCommand {
                items: item_mutations,
                gate_changes: vec![GateChange {
                    gate_keys: vec!["queue-block".to_string()],
                    blocked: true,
                }],
            }),
            item_ids,
        );
        command.created_at = ts(10);
        command.request_id = Some(request_id);
        command.request_fingerprint = Some(fingerprint);
        command.request_outcome = Some(RequestOutcome::ItemMutation {
            response_payload: serde_json::to_string(&response).unwrap(),
        });
        command
    }

    #[tokio::test]
    async fn resolved_mutation_is_durable_and_exact_replay_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("item-mutation.db");
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let pushed = item("700", "mutation-key", 7);
        let store = TursoRelational::open(TursoConfig::local(&path))
            .await
            .unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition.clone())
            .await
            .unwrap();
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 0)],
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![pushed.clone()],
                }),
                vec![pushed.item_id],
            )],
        )
        .await
        .unwrap();

        let mutation = mutation_envelope(vec![replacement(&pushed, 2, b"durable")], "mut-1", 91);
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 1)],
            vec![mutation.clone()],
        )
        .await
        .unwrap();

        let rows = store
            .query(
                "SELECT lifecycle_state,item_version,payload,not_before,eligible_since \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                    Value::Text(pushed.item_id.to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Text("Pending".to_string()));
        assert_eq!(rows[0].values[1], Value::Integer(2));
        assert_eq!(rows[0].values[2], Value::Blob(b"durable".to_vec()));
        assert_eq!(rows[0].values[3], Value::Integer(ts_nanos(ts(50))));
        assert_eq!(rows[0].values[4], Value::Integer(ts_nanos(ts(50))));
        let gates = store
            .query(
                "SELECT gate_key FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 \
                 AND item_id=?3 ORDER BY gate_key",
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                    Value::Text(pushed.item_id.to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(gates[0].values[0], Value::Text("item-block".to_string()));
        let replay_rows = store
            .query(
                "SELECT request_fingerprint,response_payload,command_positions \
                 FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
                 AND operation='item_mutation' AND request_id='mut-1'",
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(replay_rows.len(), 1);
        assert_eq!(
            replay_rows[0].values[0],
            Value::Blob(91_u64.to_be_bytes().to_vec())
        );
        assert_eq!(replay_rows[0].values[2], Value::Text("[[0,1]]".to_string()));

        // Reusing a retained request id for another body must roll back the resolved row update and the
        // projection cursor together with the conflicting durable outcome.
        let conflicting = mutation_envelope(vec![replacement(&pushed, 3, b"wrong")], "mut-1", 92);
        let error = AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 2)],
            vec![conflicting],
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            fireweed_engine::EngineError::RequestIdConflict
        ));
        assert_eq!(
            AsyncProjectionStore::item_version(&store, shard.clone(), pushed.item_id)
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            AsyncProjectionStore::recovery_high_water(&store, shard.clone())
                .await
                .unwrap()
                .unwrap()
                .sequence,
            1
        );

        drop(store);
        let reopened = TursoRelational::open(TursoConfig::local(&path))
            .await
            .unwrap();
        assert_eq!(
            AsyncProjectionStore::item_version(&reopened, shard.clone(), pushed.item_id)
                .await
                .unwrap(),
            Some(2)
        );
        AsyncProjectionStore::apply_recovery(
            &reopened,
            vec![CommandPosition::new(shard.clone(), 0, 1)],
            vec![mutation],
        )
        .await
        .unwrap();
        assert_eq!(
            AsyncProjectionStore::item_version(&reopened, shard, pushed.item_id)
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn mutation_batch_conflict_rolls_back_every_item_and_cursor() {
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let first = item("801", "first", 1);
        let second = item("802", "second", 2);
        let store = TursoRelational::in_memory().await.unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition)
            .await
            .unwrap();
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 0)],
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![first.clone(), second.clone()],
                }),
                vec![first.item_id, second.item_id],
            )],
        )
        .await
        .unwrap();

        let invalid = mutation_envelope(
            vec![
                replacement(&first, 2, b"first-new"),
                replacement(&second, 3, b"second-new"),
            ],
            "mut-conflict",
            92,
        );
        let error = AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 1)],
            vec![invalid],
        )
        .await
        .unwrap_err();
        assert!(matches!(error, fireweed_engine::EngineError::Conflict));
        assert_eq!(
            AsyncProjectionStore::item_version(&store, shard.clone(), first.item_id)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            AsyncProjectionStore::item_version(&store, shard.clone(), second.item_id)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            AsyncProjectionStore::recovery_high_water(&store, shard.clone())
                .await
                .unwrap()
                .unwrap()
                .sequence,
            0
        );

        let valid = mutation_envelope(
            vec![
                replacement(&first, 2, b"first-new"),
                replacement(&second, 2, b"second-new"),
            ],
            "mut-valid",
            93,
        );
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 1)],
            vec![valid],
        )
        .await
        .unwrap();
        assert_eq!(
            AsyncProjectionStore::item_version(&store, shard, second.item_id)
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn required_active_transition_invalidates_lease_and_purge_is_atomic() {
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let leased = item("901", "leased", 1);
        let purged = item("902", "purged", 2);
        let lease_token = LeaseToken::new("mutation-lease").unwrap();
        let store = TursoRelational::in_memory().await.unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition)
            .await
            .unwrap();
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 0)],
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![leased.clone(), purged.clone()],
                }),
                vec![leased.item_id, purged.item_id],
            )],
        )
        .await
        .unwrap();
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 1)],
            vec![envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![leased.item_id],
                    lease_token: lease_token.clone(),
                    lease_expires_at: ts(100),
                    worker_id: None,
                }),
                vec![leased.item_id],
            )],
        )
        .await
        .unwrap();
        assert!(
            store
                .live_tokens
                .lock()
                .await
                .contains_key(&(shard.clone(), leased.item_id))
        );

        let mut terminal = replacement(&leased, 3, b"terminal");
        let ResolvedItemMutationAction::Replace(values) = &mut terminal.action else {
            unreachable!()
        };
        values.state = ItemState::Complete;
        values.not_before = None;
        values.invalidate_lease = true;
        let purge = ResolvedItemMutation {
            item_id: purged.item_id,
            action: ResolvedItemMutationAction::Purge,
        };
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 2)],
            vec![mutation_envelope(vec![terminal, purge], "mut-active", 94)],
        )
        .await
        .unwrap();

        assert_eq!(
            AsyncProjectionStore::item_state(&store, shard.clone(), leased.item_id)
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
        assert_eq!(
            AsyncProjectionStore::item_version(&store, shard.clone(), leased.item_id)
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&store, shard.clone(), purged.item_id)
                .await
                .unwrap(),
            None
        );
        assert!(
            !store
                .live_tokens
                .lock()
                .await
                .contains_key(&(shard.clone(), leased.item_id))
        );
        let rows = store
            .query(
                "SELECT lease_token_hash,lease_expires_at,worker_id,terminal_at,\
                 terminal_command_epoch FROM fireweed_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                    Value::Text(leased.item_id.to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            rows[0].values,
            vec![
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Integer(ts_nanos(ts(10))),
                Value::Integer(0),
            ]
        );
        let retained = store
            .query(
                "SELECT item_id FROM fireweed_item_key_retention \
                 WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key='purged'",
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            retained[0].values[0],
            Value::Text(purged.item_id.to_string())
        );
    }
}
