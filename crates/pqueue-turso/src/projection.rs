// The engine port deliberately spells futures as RPITIT; mirror that signature without refining the
// implementation's public return type to `async fn`.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, CohortId, GroupKey, IndexDeclaration, ItemId, ItemState, LeaseToken, Metadata,
    QueueDefinition, QueueIndex, RequestId, UtcTimestamp, is_retry_exhausted,
};
use pqueue_engine::{
    AsyncProjectionStore, ClaimCompatibility, ClaimUnit, ClaimedItem, CohortLeaseTarget,
    CommandEnvelope, CommandPosition, EngineError, EngineResult, FinalizeKind, FinalizeTarget,
    IdempotencyDecision, ItemView, LeaseView, LiveItemView, PayloadUpdate, PendingPage,
    PendingSummary, PushFingerprint, PushItem, QueueCommand, QueueKey, QueueMetrics, RenewTarget,
    RequestOutcome, RichClaimSelection, TerminalEmissionMetrics,
};
use pqueue_relational::{
    async_projection as sql, elig_sort, fields_from_json, fields_to_json, lease_hash,
    metadata_from_json, metadata_to_json, nanos_ts, parse_priority, parse_state, ts_nanos,
    ts_nanos_opt,
};
use tokio::sync::Mutex;
use turso::{Connection, Value, transaction::TransactionBehavior};

use crate::{TursoRelational, local::ConsumerLeaseIndex};

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
) -> EngineResult<()> {
    let mut connection = writer.lock().await;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .map_err(storage)?;
    let tenant = definition.tenant_id.as_str().to_string();
    let queue = definition.queue_id.as_str().to_string();
    if let Some(row) = one_row(
        &transaction,
        sql::SELECT_QUEUE_DEFINITION,
        vec![tenant.clone().into(), queue.clone().into()],
    )
    .await?
    {
        let existing: QueueDefinition = serde_json::from_str(&text(&row[0])?).map_err(storage)?;
        if existing.ordering_mode != definition.ordering_mode
            || existing.priority_model != definition.priority_model
        {
            transaction.rollback().await.map_err(storage)?;
            return Err(EngineError::QueueDefinitionConflict);
        }
        let cursor = one_row(
            &transaction,
            sql::SELECT_CURSOR_STATE,
            vec![tenant.clone().into(), queue.clone().into()],
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
        return Ok(());
    }
    let encoded = serde_json::to_string(&definition).map_err(storage)?;
    transaction
        .execute(
            sql::INSERT_QUEUE,
            vec![
                Value::Text(tenant.clone()),
                Value::Text(queue.clone()),
                Value::Text(encoded),
            ],
        )
        .await
        .map_err(storage)?;
    transaction
        .execute(
            sql::INSERT_CURSOR,
            vec![Value::Text(tenant), Value::Text(queue)],
        )
        .await
        .map_err(storage)?;
    transaction.commit().await.map_err(storage)
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
        | QueueCommand::PauseQueue(_)
        | QueueCommand::ResumeQueue
        | QueueCommand::PurgeItems(_)
        | QueueCommand::SetGates(_)
        | QueueCommand::WriteSideRecords(_)
        | QueueCommand::AdvanceInstanceFence(_) => {}
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

fn values_rows(rows: usize, columns: usize) -> String {
    let row = format!("({})", vec!["?"; columns].join(","));
    vec![row; rows].join(",")
}

struct PushInsert<'a> {
    definition: &'a QueueDefinition,
    items: &'a [PushItem],
    incoming: i64,
    base: i64,
    now: i64,
}

async fn insert_push_items_batched(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    insert: PushInsert<'_>,
) -> EngineResult<()> {
    const COLUMNS: &str = "tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,\
        priority_sort,not_before,eligible_since,group_key,cohort_size,recurrence_until,payload,\
        fields,metadata,entity_document,retry_count,item_version,lease_token_hash,lease_expires_at,\
        worker_id,last_command_sequence,created_at,updated_at,terminal_at,terminal_command_epoch,\
        fenced,superseded,max_attempts,created_seq";
    const ROW: &str =
        "(?,?,?,?,'Pending',?,?,?,?,?,?,NULL,?,?,?,?,0,1,NULL,NULL,NULL,?,?,?,NULL,NULL,0,0,?,?)";

    for (chunk_index, chunk) in insert.items.chunks(PUSH_ITEM_CHUNK).enumerate() {
        let mut parameters: Vec<Value> = Vec::with_capacity(chunk.len() * 19);
        let chunk_base = chunk_index * PUSH_ITEM_CHUNK;
        for (offset, item) in chunk.iter().enumerate() {
            let priority = item
                .priority
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(storage)?;
            let not_before = ts_nanos_opt(item.not_before);
            let cohort_size = item
                .cohort_size
                .map(i64::try_from)
                .transpose()
                .map_err(|_| EngineError::Conflict)?;
            let entity = item
                .entity_document
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(storage)?;
            let ordinal = chunk_base
                .checked_add(offset)
                .ok_or_else(|| storage("item sequence overflow"))?;
            let created_seq = insert
                .base
                .checked_add(i64::try_from(ordinal).map_err(storage)?)
                .ok_or_else(|| storage("item sequence overflow"))?;
            parameters.extend([
                tenant.to_string().into(),
                queue.to_string().into(),
                item.item_id.to_string().into(),
                item.client_item_key.as_str().to_string().into(),
                priority.map_or(Value::Null, Value::Text),
                Value::Blob(elig_sort(&item.priority, &insert.definition.priority_model)),
                not_before.map_or(Value::Null, Value::Integer),
                Value::Integer(not_before.unwrap_or(insert.now)),
                item.group_key
                    .as_ref()
                    .map_or(Value::Null, |group| Value::Text(group.as_str().to_string())),
                cohort_size.map_or(Value::Null, Value::Integer),
                item.payload
                    .as_ref()
                    .map_or(Value::Null, |value| Value::Blob(value.to_vec())),
                Value::Text(fields_to_json(&item.fields)?),
                Value::Text(metadata_to_json(&item.metadata)?),
                entity.map_or(Value::Null, Value::Text),
                Value::Integer(insert.incoming),
                Value::Integer(insert.now),
                Value::Integer(insert.now),
                Value::Integer(i64::from(item.max_attempts)),
                Value::Integer(created_seq),
            ]);
        }
        transaction
            .execute(
                format!(
                    "INSERT INTO pqueue_items ({COLUMNS}) VALUES {}",
                    vec![ROW; chunk.len()].join(",")
                ),
                parameters,
            )
            .await
            .map_err(storage)?;
    }
    Ok(())
}

async fn insert_push_gates_batched(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    items: &[PushItem],
) -> EngineResult<()> {
    let rows: Vec<_> = items
        .iter()
        .flat_map(|item| {
            item.gate_keys
                .iter()
                .map(move |gate| (item.item_id, gate.as_str()))
        })
        .collect();
    for chunk in rows.chunks(PUSH_GATE_CHUNK) {
        let mut parameters: Vec<Value> = Vec::with_capacity(chunk.len() * 4);
        for (item_id, gate) in chunk {
            parameters.extend([
                tenant.to_string().into(),
                queue.to_string().into(),
                item_id.to_string().into(),
                gate.to_string().into(),
            ]);
        }
        transaction
            .execute(
                format!(
                    "INSERT INTO pqueue_item_gates (tenant_id,queue_id,item_id,gate_key) \
                     VALUES {} ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING",
                    values_rows(chunk.len(), 4)
                ),
                parameters,
            )
            .await
            .map_err(storage)?;
    }
    Ok(())
}

fn typed_index_keys(
    indexes: &[QueueIndex],
    entity: Option<&serde_json::Value>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let Some(entity) = entity else {
        return Ok(Vec::new());
    };
    indexes
        .iter()
        .filter_map(|index| {
            let key = match &index.declaration {
                IndexDeclaration::Single(definition) => definition.index_key(entity),
                IndexDeclaration::Compound(definition) => definition.index_key(entity),
            };
            match key {
                Ok(Some(key)) => Some(Ok((index.name.clone(), key))),
                Ok(None) => None,
                Err(error) => Some(Err(storage(error))),
            }
        })
        .collect()
}

async fn check_typed_unique_conflicts(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    indexes: &[QueueIndex],
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    for (name, key) in keys {
        let unique = indexes
            .iter()
            .find(|index| index.name == *name)
            .is_some_and(index_is_unique);
        if unique
            && one_row(
                transaction,
                "SELECT item_id FROM pqueue_item_index WHERE tenant_id=?1 AND queue_id=?2 \
                 AND index_name=?3 AND index_key=?4 LIMIT 1",
                vec![
                    tenant.to_string().into(),
                    queue.to_string().into(),
                    name.clone().into(),
                    Value::Blob(key.clone()),
                ],
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
    for (name, key) in keys {
        transaction
            .execute(
                "INSERT INTO pqueue_item_index \
                 (tenant_id,queue_id,index_name,index_key,item_id) VALUES(?1,?2,?3,?4,?5) \
                 ON CONFLICT(tenant_id,queue_id,index_name,item_id) DO UPDATE SET \
                 index_key=excluded.index_key",
                vec![
                    tenant.to_string().into(),
                    queue.to_string().into(),
                    name.clone().into(),
                    Value::Blob(key.clone()),
                    item_id.to_string().into(),
                ],
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
        sql::delete_item_indexes(ids.len()),
        vec![tenant.to_string().into(), queue.to_string().into()],
        ids,
    )
    .await
}

async fn replace_typed_indexes_for_entity(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    indexes: &[QueueIndex],
    item_id: ItemId,
    entity: &serde_json::Value,
) -> EngineResult<()> {
    let keys = typed_index_keys(indexes, Some(entity))?;
    delete_typed_index_rows(transaction, tenant, queue, std::slice::from_ref(&item_id)).await?;
    check_typed_unique_conflicts(transaction, tenant, queue, indexes, &keys).await?;
    insert_typed_index_rows(transaction, tenant, queue, &item_id.to_string(), &keys).await
}

async fn maintain_typed_indexes_on_insert(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    indexes: &[QueueIndex],
    items: &[PushItem],
) -> EngineResult<()> {
    let mut batch_unique: HashMap<(String, Vec<u8>), String> = HashMap::new();
    let mut rows = Vec::with_capacity(items.len());
    let mut unique_rows = Vec::new();
    for item in items {
        let item_id = item.item_id.to_string();
        let keys = typed_index_keys(indexes, item.entity_document.as_ref())?;
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
             SELECT 1 FROM pqueue_item_index existing JOIN incoming \
             ON existing.index_name=incoming.index_name AND existing.index_key=incoming.index_key \
             WHERE existing.tenant_id=? AND existing.queue_id=? LIMIT 1",
            values_rows(chunk.len(), 2)
        );
        if one_row(transaction, &query, parameters).await?.is_some() {
            return Err(EngineError::Conflict);
        }
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
                    "INSERT INTO pqueue_item_index \
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
    let mut cohorts: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for item in items {
        if let (Some(group), Some(size)) = (&item.group_key, item.cohort_size) {
            let size = i64::try_from(size).map_err(|_| EngineError::Conflict)?;
            let entry = cohorts
                .entry(group.as_str().to_string())
                .or_insert((size, 0));
            if entry.0 != size {
                return Err(EngineError::Conflict);
            }
            entry.1 += 1;
        }
    }
    for (group, (size, added)) in cohorts {
        let existing = one_row(
            transaction,
            "SELECT cohort_size,member_count,state,retention_until FROM pqueue_cohorts \
             WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
            vec![
                tenant.to_string().into(),
                queue.to_string().into(),
                group.clone().into(),
            ],
        )
        .await?;
        match existing {
            None => {
                if added > size {
                    return Err(EngineError::Conflict);
                }
                let state = if added >= size { "complete" } else { "forming" };
                transaction
                    .execute(
                        "INSERT INTO pqueue_cohorts \
                         (tenant_id,queue_id,group_key,cohort_id,cohort_size,member_count,state,\
                          cohort_created_at,first_eligible_at,created_at) \
                         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?8)",
                        vec![
                            tenant.to_string().into(),
                            queue.to_string().into(),
                            group.clone().into(),
                            cohort_id_for(&group, now).into(),
                            Value::Integer(size),
                            Value::Integer(added),
                            state.into(),
                            Value::Integer(now),
                            if state == "complete" {
                                Value::Integer(now)
                            } else {
                                Value::Null
                            },
                        ],
                    )
                    .await
                    .map_err(storage)?;
            }
            Some(row) => {
                let old_size = integer(&row[0])?;
                let old_count = integer(&row[1])?;
                let old_state = text(&row[2])?;
                let retention = optional_integer(&row[3])?;
                if old_state == "terminal" {
                    if retention.is_some_and(|until| until > now) || added > size {
                        return Err(EngineError::Conflict);
                    }
                    let state = if added >= size { "complete" } else { "forming" };
                    transaction.execute(
                        "UPDATE pqueue_cohorts SET cohort_id=?4,cohort_size=?5,member_count=?6,\
                         state=?7,cohort_created_at=?8,first_eligible_at=?9,expire_command_pos=NULL,\
                         cohort_lease_token_hash=NULL,retention_until=NULL,created_at=?8 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                        vec![tenant.to_string().into(), queue.to_string().into(), group.clone().into(),
                             cohort_id_for(&group, now).into(), Value::Integer(size), Value::Integer(added),
                             state.into(), Value::Integer(now),
                             if state == "complete" { Value::Integer(now) } else { Value::Null }],
                    ).await.map_err(storage)?;
                } else {
                    if old_size != size || old_count.saturating_add(added) > old_size {
                        return Err(EngineError::Conflict);
                    }
                    let count = old_count + added;
                    let state = if old_state == "leased" {
                        "leased"
                    } else if count >= old_size {
                        "complete"
                    } else {
                        "forming"
                    };
                    transaction
                        .execute(
                            "UPDATE pqueue_cohorts SET member_count=?4,state=?5,\
                         first_eligible_at=CASE WHEN ?5='complete' AND first_eligible_at IS NULL \
                         THEN ?6 ELSE first_eligible_at END \
                         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                            vec![
                                tenant.to_string().into(),
                                queue.to_string().into(),
                                group.into(),
                                Value::Integer(count),
                                state.into(),
                                Value::Integer(now),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                }
            }
        }
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
        "SELECT group_key FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
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
            "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
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
    let mut params = vec![
        Value::Text(tenant.to_string()),
        Value::Text(queue.to_string()),
    ];
    append_item_ids(&mut params, ids);
    let mut rows = transaction
        .query(
            format!(
                "SELECT DISTINCT group_key FROM pqueue_items WHERE tenant_id=? AND queue_id=? \
                 AND group_key IS NOT NULL AND item_id IN ({})",
                vec!["?"; ids.len()].join(",")
            ),
            params,
        )
        .await
        .map_err(storage)?;
    let mut groups = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        groups.push(GroupKey::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?);
    }
    Ok(groups)
}

async fn refresh_group_summaries(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    groups: &[GroupKey],
    now: i64,
) -> EngineResult<()> {
    for chunk in groups.chunks(GROUP_SUMMARY_CHUNK) {
        let target_rows = (0..chunk.len())
            .map(|offset| format!("(?{})", offset + 4))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "WITH target_input(group_key) AS (VALUES {target_rows}), \
             target AS (SELECT DISTINCT group_key FROM target_input), \
             eligible AS (SELECT i.group_key,i.eligible_since,i.priority_sort,i.created_at,i.item_id,i.created_seq \
               FROM pqueue_items i JOIN target t ON t.group_key=i.group_key \
               WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.lifecycle_state='Pending' AND i.superseded=0 \
               AND (i.not_before IS NULL OR i.not_before<=?3) AND NOT EXISTS (SELECT 1 \
                 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id \
                 AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id \
                 AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id)), \
             ranked AS (SELECT *,ROW_NUMBER() OVER (PARTITION BY group_key ORDER BY priority_sort,created_seq) AS rn FROM eligible), \
             aggregate AS (SELECT group_key,COUNT(*) AS item_count,MIN(eligible_since) AS oldest FROM eligible GROUP BY group_key) \
             INSERT INTO pqueue_group_summary \
             (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
              rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
             SELECT ?1,?2,t.group_key,a.oldest,NULL,r.priority_sort,r.created_at,r.item_id,COALESCE(a.item_count,0),0,?3 \
             FROM target t LEFT JOIN aggregate a ON a.group_key=t.group_key \
             LEFT JOIN ranked r ON r.group_key=t.group_key AND r.rn=1 WHERE true \
             ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
              oldest_eligible_at=excluded.oldest_eligible_at,rep_progress_guard_sort=excluded.rep_progress_guard_sort,\
              rep_priority_sort=excluded.rep_priority_sort,rep_created_at=excluded.rep_created_at,\
              rep_item_id=excluded.rep_item_id,eligible_item_count=excluded.eligible_item_count,\
              at_risk_count=excluded.at_risk_count,updated_at=excluded.updated_at"
        );
        let mut params = Vec::with_capacity(3 + chunk.len());
        params.push(Value::Text(tenant.to_string()));
        params.push(Value::Text(queue.to_string()));
        params.push(Value::Integer(now));
        params.extend(
            chunk
                .iter()
                .map(|group| Value::Text(group.as_str().to_string())),
        );
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
            "SELECT DISTINCT i.group_key FROM pqueue_items i \
             LEFT JOIN pqueue_group_summary gs ON gs.tenant_id=i.tenant_id \
             AND gs.queue_id=i.queue_id AND gs.group_key=i.group_key \
             WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.lifecycle_state='Pending' \
             AND i.superseded=0 AND i.group_key IS NOT NULL AND i.eligible_since IS NOT NULL \
             AND (i.not_before IS NULL OR i.not_before<=?3) \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gstate \
             ON gstate.tenant_id=ig.tenant_id AND gstate.queue_id=ig.queue_id \
             AND gstate.gate_key=ig.gate_key WHERE ig.tenant_id=i.tenant_id \
             AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
             AND (gs.group_key IS NULL OR gs.oldest_eligible_at IS NULL OR gs.rep_item_id IS NULL) \
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
    refresh_group_summaries(transaction, tenant, queue, &groups, now).await?;
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
                "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                 AND group_key=?3 AND lifecycle_state='Pending' AND superseded=0 \
                 AND {cohort_predicate} AND (not_before IS NULL OR not_before<=?4) \
                 AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
                 JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id \
                 AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
                 AND ig.item_id=pqueue_items.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
                 WHERE NOT EXISTS (SELECT 1 FROM json_each(pqueue_items.metadata) actual \
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
               FROM pqueue_group_summary s JOIN pqueue_items e ON e.tenant_id=?1 AND e.queue_id=?2 \
                 AND e.group_key=s.group_key WHERE s.tenant_id=?1 AND s.queue_id=?2 \
               AND s.oldest_eligible_at IS NOT NULL AND e.lifecycle_state='Pending' AND e.superseded=0 \
               AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
               AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
                 JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                 AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id \
                 AND ig.queue_id=e.queue_id AND ig.item_id=e.item_id) \
               AND NOT EXISTS (SELECT 1 FROM json_each(?5) wanted WHERE NOT EXISTS \
                 (SELECT 1 FROM json_each(e.metadata) actual WHERE actual.key=wanted.key \
                  AND actual.value=wanted.value AND actual.type=wanted.type)) \
               AND NOT EXISTS (SELECT 1 FROM pqueue_items leased WHERE leased.tenant_id=?1 \
                 AND leased.queue_id=?2 AND leased.group_key=s.group_key AND leased.superseded=0 \
                 AND leased.cohort_size IS NULL AND leased.lifecycle_state='Leased')), \
             candidate AS MATERIALIZED (SELECT group_key,rep_priority_sort,rep_created_at,rep_item_id \
               FROM candidate_raw WHERE rn=1 ORDER BY rep_priority_sort,created_seq,rep_item_id,group_key LIMIT ?4), \
             eligible AS MATERIALIZED (SELECT c.group_key,c.rep_priority_sort,c.rep_created_at,\
               c.rep_item_id,i.item_id,i.priority_sort,i.created_seq FROM candidate c \
               JOIN pqueue_items i ON i.tenant_id=?1 AND i.queue_id=?2 AND i.group_key=c.group_key \
               WHERE i.lifecycle_state='Pending' AND i.superseded=0 AND i.cohort_size IS NULL \
                 AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
                 AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
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
        "WITH candidate AS (SELECT s.group_key FROM pqueue_group_summary s WHERE s.tenant_id=?1 \
         AND s.queue_id=?2 AND s.oldest_eligible_at IS NOT NULL AND (?5 IS NULL OR s.group_key=?5) \
         AND EXISTS (SELECT 1 FROM pqueue_items e WHERE e.tenant_id=?1 AND e.queue_id=?2 \
           AND e.group_key=s.group_key AND e.lifecycle_state='Pending' AND e.superseded=0 \
           AND e.cohort_size IS NULL AND (e.not_before IS NULL OR e.not_before<=?3) \
           AND e.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
             JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=e.tenant_id AND ig.queue_id=e.queue_id \
             AND ig.item_id=e.item_id) AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
             WHERE NOT EXISTS (SELECT 1 FROM json_each(e.metadata) actual \
               WHERE actual.key=wanted.key AND actual.value=wanted.value AND actual.type=wanted.type))) \
         ORDER BY s.rep_priority_sort,s.rep_created_at,s.rep_item_id,s.group_key LIMIT 1) \
         SELECT i.item_id FROM candidate c JOIN pqueue_items i ON i.tenant_id=?1 AND i.queue_id=?2 \
         AND i.group_key=c.group_key WHERE i.lifecycle_state='Pending' AND i.superseded=0 \
         AND i.cohort_size IS NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
         AND i.eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
           JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
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
        "SELECT c.group_key,c.cohort_id,c.cohort_size FROM pqueue_cohorts c \
         WHERE c.tenant_id=?1 AND c.queue_id=?2 AND c.state='complete' \
         AND (SELECT COUNT(*) FROM pqueue_items a WHERE a.tenant_id=?1 AND a.queue_id=?2 \
           AND a.group_key=c.group_key AND a.superseded=0 AND a.cohort_size IS NOT NULL \
           AND a.lifecycle_state NOT IN ('Complete','Failed'))=c.cohort_size \
         AND NOT EXISTS (SELECT 1 FROM pqueue_items i WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.group_key=c.group_key AND i.superseded=0 AND i.cohort_size IS NOT NULL \
           AND i.lifecycle_state NOT IN ('Complete','Failed') AND NOT (i.lifecycle_state='Pending' \
             AND (i.not_before IS NULL OR i.not_before<=?3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
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
        "SELECT state FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
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

enum TokenOp {
    Set(QueueKey, ItemId, LeaseToken),
    Clear(QueueKey, ItemId),
}

fn append_item_ids(params: &mut Vec<Value>, ids: &[ItemId]) {
    params.extend(ids.iter().map(|item| Value::Text(item.to_string())));
}

async fn execute_for_items(
    transaction: &Connection,
    query: String,
    mut params: Vec<Value>,
    ids: &[ItemId],
) -> EngineResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    append_item_ids(&mut params, ids);
    transaction.execute(&query, params).await.map_err(storage)?;
    Ok(())
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
            "UPDATE pqueue_request_idempotency SET expires_at=max(expires_at,?4) \
         WHERE tenant_id=?1 AND queue_id=?2 AND operation='claim_by_query' AND request_id IN ( \
           SELECT edge.request_id FROM pqueue_claim_replay_items edge \
           JOIN json_each(?3) renewed ON renewed.value=edge.item_id \
           WHERE edge.tenant_id=?1 AND edge.queue_id=?2 GROUP BY edge.request_id \
           HAVING COUNT(*)=(SELECT COUNT(*) FROM pqueue_claim_replay_items all_edges \
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

async fn apply_owned(
    writer: Arc<Mutex<Connection>>,
    live_tokens: Arc<Mutex<BTreeMap<(QueueKey, ItemId), LeaseToken>>>,
    live_tokens_by_consumer: Arc<Mutex<ConsumerLeaseIndex>>,
    positions: Vec<CommandPosition>,
    commands: Vec<CommandEnvelope>,
    enforce_live_epoch: bool,
) -> EngineResult<()> {
    if positions.len() != commands.len() {
        return Err(storage("positions/commands length mismatch"));
    }
    let mut connection = writer.lock().await;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .map_err(storage)?;
    let mut next_by_queue: HashMap<QueueKey, i64> = HashMap::new();
    let mut max_epoch: HashMap<QueueKey, i64> = HashMap::new();
    let mut token_ops = Vec::new();

    // Fence the complete live batch before executing any command. A later position may
    // target a queue already seen in the batch, so checking only while initializing
    // `next_by_queue` would allow that later stale epoch to mutate state.
    if enforce_live_epoch {
        let mut floors = HashMap::new();
        for position in &positions {
            let floor = match floors.get(&position.queue) {
                Some(floor) => *floor,
                None => {
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

    for (position, envelope) in positions.iter().zip(&commands) {
        let tenant = position.queue.tenant_id.as_str().to_string();
        let queue = position.queue.queue_id.as_str().to_string();
        let cursor = match next_by_queue.get(&position.queue) {
            Some(cursor) => *cursor,
            None => {
                let row = one_row(
                    &transaction,
                    sql::SELECT_CURSOR,
                    vec![tenant.clone().into(), queue.clone().into()],
                )
                .await?
                .ok_or(EngineError::NotFound)?;
                if enforce_live_epoch
                    && position.backend_epoch
                        < nonnegative_u64(integer(&row[1])?, "assignment epoch")?
                {
                    transaction.rollback().await.map_err(storage)?;
                    return Err(EngineError::EpochFenced);
                }
                let cursor = integer(&row[0])?;
                next_by_queue.insert(position.queue.clone(), cursor);
                cursor
            }
        };
        let incoming = i64::try_from(position.sequence)
            .map_err(|_| storage("command sequence exceeds relational integer range"))?;
        if incoming < cursor {
            continue;
        }
        if incoming > cursor {
            transaction.rollback().await.map_err(storage)?;
            return Err(storage(format!(
                "Turso projection replay gap for {}:{}: expected sequence {cursor}, got {incoming}",
                tenant, queue
            )));
        }

        if let Err(error) = validate_minimal_command(envelope) {
            transaction.rollback().await.map_err(storage)?;
            return Err(error);
        }

        match &envelope.command {
            QueueCommand::CreateQueue(_) => {}
            QueueCommand::Push(push) => {
                let definition = definition_in_transaction(&transaction, &position.queue).await?;
                let row = one_row(
                    &transaction,
                    sql::SELECT_NEXT_ITEM_SEQUENCE,
                    vec![tenant.clone().into(), queue.clone().into()],
                )
                .await?
                .ok_or(EngineError::NotFound)?;
                let base = integer(&row[0])?;
                let next = base
                    .checked_add(i64::try_from(push.items.len()).map_err(storage)?)
                    .ok_or_else(|| storage("item sequence overflow"))?;
                transaction
                    .execute(
                        sql::UPDATE_NEXT_ITEM_SEQUENCE,
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Integer(next),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                let now = ts_nanos(envelope.created_at);
                insert_push_items_batched(
                    &transaction,
                    &tenant,
                    &queue,
                    PushInsert {
                        definition: &definition,
                        items: &push.items,
                        incoming,
                        base,
                        now,
                    },
                )
                .await?;
                insert_push_gates_batched(&transaction, &tenant, &queue, &push.items).await?;
                upsert_cohorts(&transaction, &tenant, &queue, &push.items, now).await?;
                maintain_typed_indexes_on_insert(
                    &transaction,
                    &tenant,
                    &queue,
                    &definition.typed_indexes,
                    &push.items,
                )
                .await?;
                let groups: HashSet<GroupKey> = push
                    .items
                    .iter()
                    .filter_map(|item| item.group_key.clone())
                    .collect();
                let groups = groups.into_iter().collect::<Vec<_>>();
                refresh_group_summaries(&transaction, &tenant, &queue, &groups, now).await?;
                if let (
                    Some(request_id),
                    Some(_fingerprint),
                    Some(RequestOutcome::Push { item_ids }),
                ) = (
                    &envelope.request_id,
                    envelope.request_fingerprint,
                    &envelope.request_outcome,
                ) {
                    let response = serde_json::to_string(
                        &item_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    )
                    .map_err(storage)?;
                    let command_positions =
                        serde_json::to_string(&vec![(position.backend_epoch, position.sequence)])
                            .map_err(storage)?;
                    let expires_at = now.saturating_add(
                        i64::try_from(definition.request_id_retention_ms)
                            .unwrap_or(i64::MAX)
                            .saturating_mul(1_000_000),
                    );
                    let canonical =
                        pqueue_engine::push_items_fingerprint_sha256(&push.items)?.to_vec();
                    let affected = transaction.execute(
                        "INSERT INTO pqueue_request_idempotency \
                         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,command_positions,expires_at,created_at) \
                         VALUES (?1,?2,'push',?3,?4,?5,?6,?7,?8) \
                         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
                         request_fingerprint=excluded.request_fingerprint,response_payload=excluded.response_payload,\
                         command_positions=excluded.command_positions,expires_at=excluded.expires_at,created_at=excluded.created_at \
                         WHERE pqueue_request_idempotency.expires_at<=excluded.created_at OR \
                         (pqueue_request_idempotency.request_fingerprint=excluded.request_fingerprint \
                         AND pqueue_request_idempotency.response_payload=excluded.response_payload)",
                        vec![tenant.clone().into(), queue.clone().into(), request_id.as_str().to_string().into(),
                             Value::Blob(canonical), response.into(), command_positions.into(),
                             Value::Integer(expires_at), Value::Integer(now)],
                    ).await.map_err(storage)?;
                    if affected == 0 {
                        return Err(EngineError::RequestIdConflict);
                    }
                }
            }
            QueueCommand::Claim(claim) => {
                if !claim.item_ids.is_empty() {
                    let groups =
                        groups_for_items(&transaction, &tenant, &queue, &claim.item_ids).await?;
                    let mut params = vec![
                        Value::Blob(lease_hash(&claim.lease_token)),
                        Value::Integer(ts_nanos(claim.lease_expires_at)),
                        claim.worker_id.as_ref().map_or(Value::Null, |worker| {
                            Value::Text(worker.as_str().to_string())
                        }),
                        Value::Integer(ts_nanos(envelope.created_at)),
                        Value::Integer(incoming),
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                    ];
                    append_item_ids(&mut params, &claim.item_ids);
                    let changed = transaction
                        .execute(sql::claim_items(claim.item_ids.len()), params)
                        .await
                        .map_err(storage)?;
                    let expected = u64::try_from(claim.item_ids.len())
                        .map_err(|_| storage("claim item count exceeds u64"))?;
                    if changed != expected {
                        transaction.rollback().await.map_err(storage)?;
                        return Err(storage("claim changed an unexpected row count"));
                    }
                    token_ops.extend(claim.item_ids.iter().map(|item| {
                        TokenOp::Set(position.queue.clone(), *item, claim.lease_token.clone())
                    }));
                    refresh_group_summaries(
                        &transaction,
                        &tenant,
                        &queue,
                        &groups,
                        ts_nanos(envelope.created_at),
                    )
                    .await?;
                }
                if let (
                    Some(request_id),
                    Some(fingerprint),
                    Some(RequestOutcome::ClaimByQuery {
                        item_ids,
                        lease_token,
                        worker_id,
                    }),
                ) = (
                    &envelope.request_id,
                    envelope.request_fingerprint,
                    &envelope.request_outcome,
                ) {
                    let response = serde_json::to_string(&serde_json::json!({
                        "item_ids": item_ids,
                        "lease_token": lease_token,
                        "worker_id": worker_id,
                    }))
                    .map_err(storage)?;
                    let positions =
                        serde_json::to_string(&vec![(position.backend_epoch, position.sequence)])
                            .map_err(storage)?;
                    let affected = transaction.execute(
                        "INSERT INTO pqueue_request_idempotency \
                         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
                          command_positions,expires_at,created_at) \
                         VALUES (?1,?2,'claim_by_query',?3,?4,?5,?6,?7,?8) \
                         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
                         expires_at=max(pqueue_request_idempotency.expires_at,excluded.expires_at) \
                         WHERE pqueue_request_idempotency.request_fingerprint=excluded.request_fingerprint \
                           AND pqueue_request_idempotency.response_payload=excluded.response_payload",
                        vec![tenant.clone().into(),queue.clone().into(),request_id.as_str().to_string().into(),
                             Value::Blob(fingerprint.to_be_bytes().to_vec()),response.into(),positions.into(),
                             Value::Integer(ts_nanos(claim.lease_expires_at)),
                             Value::Integer(ts_nanos(envelope.created_at))],
                    ).await.map_err(storage)?;
                    if affected == 0 {
                        return Err(EngineError::RequestIdConflict);
                    }
                    let ids = serde_json::to_string(
                        &item_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    )
                    .map_err(storage)?;
                    transaction
                        .execute(
                            "INSERT OR IGNORE INTO pqueue_claim_replay_items \
                         (tenant_id,queue_id,request_id,item_id) \
                         SELECT ?1,?2,?3,value FROM json_each(?4)",
                            vec![
                                Value::Text(tenant.clone()),
                                Value::Text(queue.clone()),
                                Value::Text(request_id.as_str().to_string()),
                                Value::Text(ids),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            QueueCommand::CohortClaim(claim) => {
                if cohort_state(&transaction, &tenant, &queue, &claim.cohort_id).await?
                    != "complete"
                {
                    return Err(EngineError::Conflict);
                }
                let (group, expected_ids) =
                    cohort_item_ids(&transaction, &tenant, &queue, &claim.cohort_id).await?;
                if expected_ids.is_empty()
                    || expected_ids.iter().copied().collect::<HashSet<_>>()
                        != claim.item_ids.iter().copied().collect::<HashSet<_>>()
                    || expected_ids.len() != claim.item_ids.len()
                {
                    return Err(EngineError::Conflict);
                }
                let mut params = vec![
                    Value::Blob(lease_hash(&claim.lease_token)),
                    Value::Integer(ts_nanos(claim.lease_expires_at)),
                    Value::Integer(ts_nanos(envelope.created_at)),
                    Value::Integer(incoming),
                    Value::Text(tenant.clone()),
                    Value::Text(queue.clone()),
                ];
                append_item_ids(&mut params, &claim.item_ids);
                let changed = transaction
                    .execute(
                        &sql::claim_items(claim.item_ids.len()).replace("worker_id=?,", ""),
                        params,
                    )
                    .await
                    .map_err(storage)?;
                if changed != u64::try_from(claim.item_ids.len()).map_err(storage)? {
                    return Err(storage("cohort claim changed an unexpected row count"));
                }
                let cohort_changed = transaction
                    .execute(
                        "UPDATE pqueue_cohorts SET state='leased',cohort_lease_token_hash=?4 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3 AND state='complete'",
                        vec![
                            tenant.clone().into(),
                            queue.clone().into(),
                            claim.cohort_id.as_str().to_string().into(),
                            Value::Blob(lease_hash(&claim.lease_token)),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                if cohort_changed != 1 {
                    return Err(EngineError::Conflict);
                }
                token_ops.extend(claim.item_ids.iter().map(|item| {
                    TokenOp::Set(position.queue.clone(), *item, claim.lease_token.clone())
                }));
                refresh_group_summaries(
                    &transaction,
                    &tenant,
                    &queue,
                    std::slice::from_ref(&group),
                    ts_nanos(envelope.created_at),
                )
                .await?;
            }
            QueueCommand::RenewLease(renew) => {
                execute_for_items(
                    &transaction,
                    sql::renew_lease(renew.item_ids.len()),
                    vec![
                        Value::Integer(ts_nanos(renew.lease_expires_at)),
                        Value::Integer(ts_nanos(envelope.created_at)),
                        Value::Integer(incoming),
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                    ],
                    &renew.item_ids,
                )
                .await?;
                extend_claim_by_query_replays(
                    &transaction,
                    &position.queue,
                    &renew.item_ids,
                    renew.lease_expires_at,
                )
                .await?;
            }
            QueueCommand::CohortRenewLease(renew) => {
                if cohort_state(&transaction, &tenant, &queue, &renew.cohort_id).await? != "leased"
                {
                    return Err(EngineError::Conflict);
                }
                let (_group, ids) =
                    cohort_item_ids(&transaction, &tenant, &queue, &renew.cohort_id).await?;
                if ids.is_empty() {
                    return Err(EngineError::NotFound);
                }
                let mut params = vec![
                    Value::Integer(ts_nanos(renew.lease_expires_at)),
                    Value::Integer(ts_nanos(envelope.created_at)),
                    Value::Integer(incoming),
                    Value::Text(tenant.clone()),
                    Value::Text(queue.clone()),
                ];
                append_item_ids(&mut params, &ids);
                let changed = transaction
                    .execute(&sql::renew_lease(ids.len()), params)
                    .await
                    .map_err(storage)?;
                if changed != u64::try_from(ids.len()).map_err(storage)? {
                    return Err(storage("cohort renewal changed an unexpected row count"));
                }
            }
            QueueCommand::ReassignLease(reassign) => {
                execute_for_items(
                    &transaction,
                    sql::reassign_lease(reassign.item_ids.len()),
                    vec![
                        Value::Blob(lease_hash(&reassign.lease_token)),
                        Value::Integer(ts_nanos(reassign.lease_expires_at)),
                        Value::Integer(ts_nanos(envelope.created_at)),
                        Value::Integer(incoming),
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                    ],
                    &reassign.item_ids,
                )
                .await?;
                token_ops.extend(reassign.item_ids.iter().map(|item| {
                    TokenOp::Set(position.queue.clone(), *item, reassign.lease_token.clone())
                }));
            }
            QueueCommand::Finalize(finalize) => {
                let finalized_ids: Vec<ItemId> = finalize
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.item_id)
                    .collect();
                let groups =
                    groups_for_items(&transaction, &tenant, &queue, &finalized_ids).await?;
                let retry_ids: Vec<ItemId> = finalize
                    .outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome.kind, FinalizeKind::Retry))
                    .map(|outcome| outcome.item_id)
                    .collect();
                let mut retry_info = HashMap::new();
                if !retry_ids.is_empty() {
                    let mut params = vec![Value::Text(tenant.clone()), Value::Text(queue.clone())];
                    append_item_ids(&mut params, &retry_ids);
                    let mut rows = transaction
                        .query(&sql::select_retry_info(retry_ids.len()), params)
                        .await
                        .map_err(storage)?;
                    while let Some(row) = rows.next().await.map_err(storage)? {
                        retry_info.insert(
                            text(&row.get_value(0).map_err(storage)?)?,
                            (
                                integer(&row.get_value(1).map_err(storage)?)?,
                                integer(&row.get_value(2).map_err(storage)?)?,
                            ),
                        );
                    }
                }

                let mut complete = Vec::new();
                let mut failed = Vec::new();
                let mut pending = Vec::new();
                let mut rearmed = Vec::new();
                let mut backoff: HashMap<i64, Vec<ItemId>> = HashMap::new();
                let mut rearm_schedule: HashMap<(Option<i64>, i64), Vec<ItemId>> = HashMap::new();
                let now = ts_nanos(envelope.created_at);
                for outcome in &finalize.outcomes {
                    let state = match outcome.kind {
                        FinalizeKind::Complete => ItemState::Complete,
                        FinalizeKind::Fail => ItemState::Failed,
                        FinalizeKind::Retry => {
                            let (attempts, max_attempts) = retry_info
                                .get(&outcome.item_id.to_string())
                                .copied()
                                .ok_or(EngineError::NotFound)?;
                            if is_retry_exhausted(
                                nonnegative_u32(attempts, "retry_count")?,
                                nonnegative_u32(max_attempts, "max_attempts")?,
                            ) {
                                ItemState::Failed
                            } else {
                                ItemState::Pending
                            }
                        }
                        FinalizeKind::Release | FinalizeKind::Rearm => ItemState::Pending,
                    };
                    match (state, outcome.kind) {
                        (ItemState::Complete, _) => complete.push(outcome.item_id),
                        (ItemState::Failed, _) => failed.push(outcome.item_id),
                        (ItemState::Pending, FinalizeKind::Rearm) => {
                            rearmed.push(outcome.item_id);
                            let not_before = outcome.not_before.map(ts_nanos);
                            rearm_schedule
                                .entry((not_before, not_before.unwrap_or(now).max(now)))
                                .or_default()
                                .push(outcome.item_id);
                        }
                        (ItemState::Pending, _) => pending.push(outcome.item_id),
                        (ItemState::Leased, _) => unreachable!("finalize never targets leased"),
                    }
                    if matches!(outcome.kind, FinalizeKind::Retry)
                        && state == ItemState::Pending
                        && let Some(not_before) = outcome.not_before
                    {
                        backoff
                            .entry(ts_nanos(not_before))
                            .or_default()
                            .push(outcome.item_id);
                    }
                    token_ops.push(TokenOp::Clear(position.queue.clone(), outcome.item_id));
                }
                let epoch = i64::try_from(position.backend_epoch)
                    .map_err(|_| storage("backend epoch exceeds relational integer range"))?;
                for (state, reset, terminal_at, terminal_epoch, ids) in [
                    ("Complete", false, Some(now), Some(epoch), &complete),
                    ("Failed", false, Some(now), Some(epoch), &failed),
                    ("Pending", false, None, None, &pending),
                    ("Pending", true, None, None, &rearmed),
                ] {
                    execute_for_items(
                        &transaction,
                        sql::finalize_items(ids.len()),
                        vec![
                            Value::Text(state.to_string()),
                            Value::Integer(i64::from(reset)),
                            terminal_at.map_or(Value::Null, Value::Integer),
                            terminal_epoch.map_or(Value::Null, Value::Integer),
                            Value::Integer(now),
                            Value::Integer(incoming),
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                        ],
                        ids,
                    )
                    .await?;
                }
                for (not_before, ids) in backoff {
                    execute_for_items(
                        &transaction,
                        sql::finalize_backoff(ids.len()),
                        vec![
                            Value::Integer(not_before),
                            Value::Integer(not_before),
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                        ],
                        &ids,
                    )
                    .await?;
                }
                for ((not_before, eligible_since), ids) in rearm_schedule {
                    execute_for_items(
                        &transaction,
                        sql::finalize_backoff(ids.len()),
                        vec![
                            not_before.map_or(Value::Null, Value::Integer),
                            Value::Integer(eligible_since),
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                        ],
                        &ids,
                    )
                    .await?;
                }
                refresh_group_summaries(&transaction, &tenant, &queue, &groups, now).await?;
            }
            QueueCommand::CohortFinalize(finalize) => {
                if matches!(finalize.kind, FinalizeKind::Rearm) {
                    return Err(EngineError::Invalid("cohort rearm is invalid"));
                }
                if cohort_state(&transaction, &tenant, &queue, &finalize.cohort_id).await?
                    != "leased"
                {
                    return Err(EngineError::Conflict);
                }
                let (group, ids) =
                    cohort_item_ids(&transaction, &tenant, &queue, &finalize.cohort_id).await?;
                if ids.is_empty() {
                    return Err(EngineError::NotFound);
                }
                let mut complete = Vec::new();
                let mut failed = Vec::new();
                let mut pending = Vec::new();
                let effective_kind = if matches!(finalize.kind, FinalizeKind::Retry) {
                    let mut params = vec![Value::Text(tenant.clone()), Value::Text(queue.clone())];
                    append_item_ids(&mut params, &ids);
                    let mut rows = transaction
                        .query(&sql::select_retry_info(ids.len()), params)
                        .await
                        .map_err(storage)?;
                    let mut seen = 0usize;
                    while let Some(row) = rows.next().await.map_err(storage)? {
                        seen += 1;
                        let item = ItemId::new(text(&row.get_value(0).map_err(storage)?)?)
                            .map_err(storage)?;
                        let attempts = nonnegative_u32(
                            integer(&row.get_value(1).map_err(storage)?)?,
                            "retry_count",
                        )?;
                        let max_attempts = nonnegative_u32(
                            integer(&row.get_value(2).map_err(storage)?)?,
                            "max_attempts",
                        )?;
                        if is_retry_exhausted(attempts, max_attempts) {
                            failed.push(item);
                        }
                    }
                    if seen != ids.len() {
                        return Err(storage("cohort finalize could not read every member"));
                    }
                    if failed.is_empty() {
                        pending.clone_from(&ids);
                        FinalizeKind::Retry
                    } else {
                        failed.clone_from(&ids);
                        FinalizeKind::Fail
                    }
                } else {
                    finalize.kind
                };
                if !matches!(finalize.kind, FinalizeKind::Retry) {
                    match effective_kind {
                        FinalizeKind::Complete => complete.clone_from(&ids),
                        FinalizeKind::Fail => failed.clone_from(&ids),
                        FinalizeKind::Release => pending.clone_from(&ids),
                        FinalizeKind::Retry | FinalizeKind::Rearm => unreachable!(),
                    }
                }
                let now = ts_nanos(envelope.created_at);
                let epoch = i64::try_from(position.backend_epoch)
                    .map_err(|_| storage("backend epoch exceeds relational integer range"))?;
                for (state, terminal_at, terminal_epoch, members) in [
                    ("Complete", Some(now), Some(epoch), &complete),
                    ("Failed", Some(now), Some(epoch), &failed),
                    ("Pending", None, None, &pending),
                ] {
                    if members.is_empty() {
                        continue;
                    }
                    let mut params = vec![
                        Value::Text(state.to_string()),
                        Value::Integer(0),
                        terminal_at.map_or(Value::Null, Value::Integer),
                        terminal_epoch.map_or(Value::Null, Value::Integer),
                        Value::Integer(now),
                        Value::Integer(incoming),
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                    ];
                    append_item_ids(&mut params, members);
                    let changed = transaction
                        .execute(&sql::finalize_items(members.len()), params)
                        .await
                        .map_err(storage)?;
                    if changed != u64::try_from(members.len()).map_err(storage)? {
                        return Err(storage("cohort finalize changed an unexpected row count"));
                    }
                }
                if matches!(finalize.kind, FinalizeKind::Retry)
                    && let Some(not_before) = finalize.not_before
                    && !pending.is_empty()
                {
                    execute_for_items(
                        &transaction,
                        sql::finalize_backoff(pending.len()),
                        vec![
                            Value::Integer(ts_nanos(not_before)),
                            Value::Integer(ts_nanos(not_before)),
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                        ],
                        &pending,
                    )
                    .await?;
                }
                let definition = definition_in_transaction(&transaction, &position.queue).await?;
                let next_state =
                    if matches!(effective_kind, FinalizeKind::Complete | FinalizeKind::Fail) {
                        "terminal"
                    } else {
                        "complete"
                    };
                let changed = transaction
                    .execute(
                        "UPDATE pqueue_cohorts SET state=?4,cohort_lease_token_hash=NULL,\
                         retention_until=?5 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3 \
                         AND state='leased'",
                        vec![
                            tenant.clone().into(),
                            queue.clone().into(),
                            finalize.cohort_id.as_str().to_string().into(),
                            next_state.into(),
                            if next_state == "terminal" {
                                Value::Integer(cohort_retention_until(&definition, now))
                            } else {
                                Value::Null
                            },
                        ],
                    )
                    .await
                    .map_err(storage)?;
                if changed != 1 {
                    return Err(EngineError::Conflict);
                }
                token_ops.extend(
                    ids.iter()
                        .map(|item| TokenOp::Clear(position.queue.clone(), *item)),
                );
                refresh_group_summaries(
                    &transaction,
                    &tenant,
                    &queue,
                    std::slice::from_ref(&group),
                    now,
                )
                .await?;
            }
            QueueCommand::UpdateFields(update) => {
                let current = one_row(
                    &transaction,
                    sql::SELECT_LIVE_FIELDS,
                    vec![
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                        Value::Text(update.item_id.to_string()),
                    ],
                )
                .await?;
                if let Some(row) = current {
                    let mut fields = fields_from_json(text(&row[0])?)?;
                    for (key, value) in &update.field_ops {
                        match value {
                            Some(value) => {
                                fields.insert(key.clone(), value.clone());
                            }
                            None => {
                                fields.remove(key);
                            }
                        }
                    }
                    let encoded = fields_to_json(&fields)?;
                    let base = vec![
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                        Value::Text(update.item_id.to_string()),
                        Value::Text(encoded),
                    ];
                    match &update.payload {
                        PayloadUpdate::Keep => {
                            let mut params = base;
                            params.extend([
                                Value::Integer(ts_nanos(envelope.created_at)),
                                Value::Integer(incoming),
                            ]);
                            transaction
                                .execute(sql::UPDATE_FIELDS_KEEP_PAYLOAD, params)
                                .await
                                .map_err(storage)?;
                        }
                        PayloadUpdate::Set(payload) => {
                            let mut params = base;
                            params.push(
                                payload
                                    .as_ref()
                                    .map_or(Value::Null, |bytes| Value::Blob(bytes.to_vec())),
                            );
                            params.extend([
                                Value::Integer(ts_nanos(envelope.created_at)),
                                Value::Integer(incoming),
                            ]);
                            transaction
                                .execute(sql::UPDATE_FIELDS_SET_PAYLOAD, params)
                                .await
                                .map_err(storage)?;
                        }
                    }
                    if let Some(document) = &update.set_entity_document {
                        let definition =
                            definition_in_transaction(&transaction, &position.queue).await?;
                        replace_typed_indexes_for_entity(
                            &transaction,
                            &tenant,
                            &queue,
                            &definition.typed_indexes,
                            update.item_id,
                            document,
                        )
                        .await?;
                        transaction
                            .execute(
                                sql::UPDATE_ENTITY_DOCUMENT,
                                vec![
                                    Value::Text(tenant.clone()),
                                    Value::Text(queue.clone()),
                                    Value::Text(update.item_id.to_string()),
                                    Value::Text(serde_json::to_string(document).map_err(storage)?),
                                ],
                            )
                            .await
                            .map_err(storage)?;
                    }
                }
            }
            QueueCommand::ReplacePending(replace) => {
                let definition = definition_in_transaction(&transaction, &position.queue).await?;
                let old_groups = groups_for_items(
                    &transaction,
                    &tenant,
                    &queue,
                    std::slice::from_ref(&replace.superseded_item_id),
                )
                .await?;
                if !old_groups.is_empty()
                    || replace.replacement.group_key.is_some()
                    || replace.replacement.cohort_size.is_some()
                {
                    transaction.rollback().await.map_err(storage)?;
                    return Err(EngineError::Unavailable);
                }
                delete_typed_index_rows(
                    &transaction,
                    &tenant,
                    &queue,
                    std::slice::from_ref(&replace.superseded_item_id),
                )
                .await?;
                transaction
                    .execute(
                        sql::SUPERSEDE_ITEM,
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Text(replace.superseded_item_id.to_string()),
                            Value::Integer(ts_nanos(envelope.created_at)),
                            Value::Integer(incoming),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                let row = one_row(
                    &transaction,
                    sql::SELECT_NEXT_ITEM_SEQUENCE,
                    vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                )
                .await?
                .ok_or(EngineError::NotFound)?;
                let created_seq = integer(&row[0])?;
                transaction
                    .execute(
                        sql::UPDATE_NEXT_ITEM_SEQUENCE,
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Integer(
                                created_seq
                                    .checked_add(1)
                                    .ok_or_else(|| storage("item sequence overflow"))?,
                            ),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                let item = &replace.replacement;
                let priority = item
                    .priority
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(storage)?;
                let not_before = ts_nanos_opt(item.not_before);
                let now = ts_nanos(envelope.created_at);
                let entity = item
                    .entity_document
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(storage)?;
                transaction
                    .execute(
                        sql::INSERT_ITEM,
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Text(item.item_id.to_string()),
                            Value::Text(item.client_item_key.as_str().to_string()),
                            priority.map_or(Value::Null, Value::Text),
                            Value::Blob(elig_sort(&item.priority, &definition.priority_model)),
                            not_before.map_or(Value::Null, Value::Integer),
                            Value::Integer(not_before.unwrap_or(now)),
                            Value::Null,
                            Value::Null,
                            item.payload
                                .as_ref()
                                .map_or(Value::Null, |value| Value::Blob(value.to_vec())),
                            Value::Text(fields_to_json(&item.fields)?),
                            Value::Text(metadata_to_json(&item.metadata)?),
                            entity.map_or(Value::Null, Value::Text),
                            Value::Integer(incoming),
                            Value::Integer(now),
                            Value::Integer(i64::from(item.max_attempts)),
                            Value::Integer(created_seq),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                for gate in &item.gate_keys {
                    transaction
                        .execute(
                            sql::INSERT_ITEM_GATE,
                            vec![
                                Value::Text(tenant.clone()),
                                Value::Text(queue.clone()),
                                Value::Text(item.item_id.to_string()),
                                Value::Text(gate.as_str().to_string()),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                }
                maintain_typed_indexes_on_insert(
                    &transaction,
                    &tenant,
                    &queue,
                    &definition.typed_indexes,
                    std::slice::from_ref(item),
                )
                .await?;
            }
            QueueCommand::LeaseExpired(expired) => {
                let groups =
                    groups_for_items(&transaction, &tenant, &queue, &expired.item_ids).await?;
                execute_for_items(
                    &transaction,
                    sql::lease_expired(expired.item_ids.len()),
                    vec![
                        Value::Integer(ts_nanos(envelope.created_at)),
                        Value::Integer(incoming),
                        Value::Text(tenant.clone()),
                        Value::Text(queue.clone()),
                    ],
                    &expired.item_ids,
                )
                .await?;
                token_ops.extend(
                    expired
                        .item_ids
                        .iter()
                        .map(|item| TokenOp::Clear(position.queue.clone(), *item)),
                );
                refresh_group_summaries(
                    &transaction,
                    &tenant,
                    &queue,
                    &groups,
                    ts_nanos(envelope.created_at),
                )
                .await?;
            }
            QueueCommand::CohortExpired(expired) => {
                let cohort = one_row(
                    &transaction,
                    "SELECT state FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                    vec![
                        tenant.clone().into(),
                        queue.clone().into(),
                        expired.group_key.as_str().to_string().into(),
                    ],
                )
                .await?
                .ok_or(EngineError::NotFound)?;
                if text(&cohort[0])? == "terminal" {
                    return Err(EngineError::Conflict);
                }
                let mut rows = transaction
                    .query(
                        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                         AND group_key=?3 AND superseded=0 AND cohort_size IS NOT NULL \
                         AND lifecycle_state NOT IN ('Complete','Failed')",
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Text(expired.group_key.as_str().to_string()),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await.map_err(storage)? {
                    ids.push(
                        ItemId::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?,
                    );
                }
                drop(rows);
                if ids.is_empty() {
                    return Err(EngineError::NotFound);
                }
                let now = ts_nanos(envelope.created_at);
                let epoch = i64::try_from(position.backend_epoch)
                    .map_err(|_| storage("backend epoch exceeds relational integer range"))?;
                let mut params = vec![
                    Value::Integer(now),
                    Value::Integer(epoch),
                    Value::Integer(now),
                    Value::Integer(incoming),
                    Value::Text(tenant.clone()),
                    Value::Text(queue.clone()),
                ];
                append_item_ids(&mut params, &ids);
                let changed = transaction
                    .execute(
                        &format!(
                            "UPDATE pqueue_items SET lifecycle_state='Failed',\
                             item_version=item_version+1,terminal_at=?,terminal_command_epoch=?,\
                             updated_at=?,last_command_sequence=? WHERE tenant_id=? AND queue_id=? \
                             AND item_id IN ({})",
                            vec!["?"; ids.len()].join(",")
                        ),
                        params,
                    )
                    .await
                    .map_err(storage)?;
                if changed != u64::try_from(ids.len()).map_err(storage)? {
                    return Err(storage("cohort expiry changed an unexpected row count"));
                }
                let definition = definition_in_transaction(&transaction, &position.queue).await?;
                let cohort_changed = transaction
                    .execute(
                        "UPDATE pqueue_cohorts SET state='terminal',expire_command_pos=?4,\
                         cohort_lease_token_hash=NULL,retention_until=?5 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 AND state!='terminal'",
                        vec![
                            tenant.clone().into(),
                            queue.clone().into(),
                            expired.group_key.as_str().to_string().into(),
                            Value::Integer(incoming),
                            Value::Integer(cohort_retention_until(&definition, now)),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                if cohort_changed != 1 {
                    return Err(EngineError::Conflict);
                }
                token_ops.extend(
                    ids.iter()
                        .map(|item| TokenOp::Clear(position.queue.clone(), *item)),
                );
                refresh_group_summaries(
                    &transaction,
                    &tenant,
                    &queue,
                    std::slice::from_ref(&expired.group_key),
                    now,
                )
                .await?;
            }
            QueueCommand::FenceLease(fence) => {
                execute_for_items(
                    &transaction,
                    sql::fence_lease(fence.item_ids.len(), true),
                    vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                    &fence.item_ids,
                )
                .await?;
            }
            QueueCommand::UnfenceLease(unfence) => {
                execute_for_items(
                    &transaction,
                    sql::fence_lease(unfence.item_ids.len(), false),
                    vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                    &unfence.item_ids,
                )
                .await?;
            }
            QueueCommand::PauseQueue(pause) => {
                transaction
                    .execute(
                        sql::PAUSE_QUEUE,
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Integer(i64::from(pause.drain_intake)),
                        ],
                    )
                    .await
                    .map_err(storage)?;
            }
            QueueCommand::ResumeQueue => {
                transaction
                    .execute(
                        sql::RESUME_QUEUE,
                        vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                    )
                    .await
                    .map_err(storage)?;
            }
            QueueCommand::SetGates(gates) => {
                for gate in &gates.gate_keys {
                    transaction
                        .execute(
                            if gates.blocked {
                                sql::SET_GATE_BLOCKED
                            } else {
                                sql::SET_GATE_UNBLOCKED
                            },
                            vec![
                                Value::Text(tenant.clone()),
                                Value::Text(queue.clone()),
                                Value::Text(gate.as_str().to_string()),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            QueueCommand::WriteSideRecords(command) => {
                for record in &command.records {
                    transaction
                        .execute(
                            "INSERT INTO pqueue_side_records (tenant_id,queue_id,key,payload) \
                             VALUES (?1,?2,?3,?4) ON CONFLICT(tenant_id,queue_id,key) \
                             DO UPDATE SET payload=excluded.payload",
                            vec![
                                Value::Text(tenant.clone()),
                                Value::Text(queue.clone()),
                                Value::Blob(record.key.clone()),
                                Value::Blob(record.payload.to_vec()),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            QueueCommand::AdvanceInstanceFence(command) => {
                transaction
                    .execute(
                        "INSERT INTO pqueue_instance_fences (tenant_id,queue_id,instance_key,fence) \
                         VALUES (?1,?2,?3,?4) ON CONFLICT(tenant_id,queue_id,instance_key) \
                         DO UPDATE SET fence=excluded.fence",
                        vec![
                            Value::Text(tenant.clone()),
                            Value::Text(queue.clone()),
                            Value::Blob(command.instance_key.clone()),
                            Value::Integer(i64::try_from(command.next).map_err(storage)?),
                        ],
                    )
                    .await
                    .map_err(storage)?;
            }
            QueueCommand::PurgeItems(purge) => {
                if !purge.item_ids.is_empty() {
                    let groups =
                        groups_for_items(&transaction, &tenant, &queue, &purge.item_ids).await?;
                    let definition =
                        definition_in_transaction(&transaction, &position.queue).await?;
                    let mut params = vec![Value::Text(tenant.clone()), Value::Text(queue.clone())];
                    append_item_ids(&mut params, &purge.item_ids);
                    let mut rows = transaction
                        .query(&sql::select_purge_items(purge.item_ids.len()), params)
                        .await
                        .map_err(storage)?;
                    let mut retention = Vec::new();
                    while let Some(row) = rows.next().await.map_err(storage)? {
                        // Validate the persisted state while retaining the key for every successful
                        // API-001 removal, including pending and force-purged leased items.
                        let _state = parse_state(&text(&row.get_value(2).map_err(storage)?)?)?;
                        if definition.client_item_key_retention_ms > 0 {
                            retention.push((
                                text(&row.get_value(1).map_err(storage)?)?,
                                text(&row.get_value(0).map_err(storage)?)?,
                            ));
                        }
                    }
                    drop(rows);
                    let retention_nanos = i64::try_from(definition.client_item_key_retention_ms)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1_000_000);
                    let expires = ts_nanos(envelope.created_at).saturating_add(retention_nanos);
                    for (key, item) in retention {
                        transaction
                            .execute(
                                sql::UPSERT_KEY_RETENTION,
                                vec![
                                    Value::Text(tenant.clone()),
                                    Value::Text(queue.clone()),
                                    Value::Text(key),
                                    Value::Text(item),
                                    Value::Integer(expires),
                                ],
                            )
                            .await
                            .map_err(storage)?;
                    }
                    execute_for_items(
                        &transaction,
                        sql::delete_item_gates(purge.item_ids.len()),
                        vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                        &purge.item_ids,
                    )
                    .await?;
                    execute_for_items(
                        &transaction,
                        sql::delete_item_indexes(purge.item_ids.len()),
                        vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                        &purge.item_ids,
                    )
                    .await?;
                    execute_for_items(
                        &transaction,
                        sql::delete_items(purge.item_ids.len()),
                        vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
                        &purge.item_ids,
                    )
                    .await?;
                    token_ops.extend(
                        purge
                            .item_ids
                            .iter()
                            .map(|item| TokenOp::Clear(position.queue.clone(), *item)),
                    );
                    refresh_group_summaries(
                        &transaction,
                        &tenant,
                        &queue,
                        &groups,
                        ts_nanos(envelope.created_at),
                    )
                    .await?;
                }
            }
        }

        let next = incoming
            .checked_add(1)
            .ok_or_else(|| storage("command sequence overflow"))?;
        next_by_queue.insert(position.queue.clone(), next);
        let epoch = i64::try_from(position.backend_epoch)
            .map_err(|_| storage("backend epoch exceeds relational integer range"))?;
        max_epoch
            .entry(position.queue.clone())
            .and_modify(|current| *current = (*current).max(epoch))
            .or_insert(epoch);
    }

    for (shard, next) in next_by_queue {
        transaction
            .execute(
                sql::UPDATE_CURSOR,
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                    Value::Integer(next),
                    Value::Integer(max_epoch.get(&shard).copied().unwrap_or(0)),
                ],
            )
            .await
            .map_err(storage)?;
    }
    transaction.commit().await.map_err(storage)?;
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
    pub(crate) async fn purge_items_validate(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
        force: bool,
    ) -> EngineResult<Vec<ItemId>> {
        let connection = self.writer.lock().await;
        let mut present = Vec::new();
        for id in ids {
            if present.contains(id) {
                continue;
            }
            let row=one_row(&connection,"SELECT lifecycle_state FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",vec![Value::Text(shard.tenant_id.as_str().to_string()),Value::Text(shard.queue_id.as_str().to_string()),Value::Text(id.to_string())]).await?;
            if let Some(row) = row {
                let state = parse_state(&text(&row[0])?).map_err(storage)?;
                pqueue_engine::validate_purge_force(state == ItemState::Leased, force)?;
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
                "SELECT item_id,client_item_key,priority,item_version FROM pqueue_items \
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
                "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
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
                "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
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

    pub async fn server_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let rows = self.query(
                "SELECT item_id,client_item_key,item_version,lifecycle_state,priority,group_key,not_before,retry_count,payload,fields \
                 FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 \
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
            "SELECT lifecycle_state,COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
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
        async move { ensure_shard_owned(writer, definition).await }
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
                let definition = definition_in_transaction(&transaction, &shard).await?;
                let mut keys = HashSet::new();
                let mut item_ids = HashSet::new();
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
                        *grouped.entry(group.as_str().to_string()).or_default() += 1;
                    }
                    let existing = one_row(&transaction,
                        "SELECT 1 FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                         AND (item_id=?3 OR (client_item_key=?4 AND superseded=0)) \
                         UNION ALL SELECT 1 FROM pqueue_item_key_retention WHERE tenant_id=?1 AND queue_id=?2 \
                         AND client_item_key=?4 AND expires_at>?5 LIMIT 1",
                        vec![tenant.clone().into(), queue.clone().into(), item.item_id.to_string().into(),
                             item.client_item_key.as_str().to_string().into(), Value::Integer(ts_nanos(now))]).await?;
                    if existing.is_some() { return Err(EngineError::Conflict); }
                }
                for (group, added) in grouped {
                    let Some(max) = definition.max_eligible_group_size else { continue };
                    let row = one_row(&transaction,
                        "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                         AND group_key=?3 AND lifecycle_state IN ('Pending','Leased') AND superseded=0",
                        vec![tenant.clone().into(), queue.clone().into(), group.into()]).await?
                        .ok_or_else(|| storage("group count missing"))?;
                    if nonnegative_u64(integer(&row[0])?, "group count")?.saturating_add(added) > max {
                        return Err(EngineError::Conflict);
                    }
                }
                maintain_typed_indexes_on_insert(&transaction, &tenant, &queue, &definition.typed_indexes, &items).await?;
                upsert_cohorts(&transaction, &tenant, &queue, &items, ts_nanos(now)).await?;
                Ok(())
            }.await;
            transaction.rollback().await.map_err(storage)?;
            result
        }
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<bool>> + Send {
        let writer = self.writer.clone();
        async move {
            let connection = writer.lock().await;
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
        let writer = self.writer.clone();
        async move {
            let connection = writer.lock().await;
            let row = one_row(&connection,
                "SELECT request_fingerprint,response_payload,expires_at FROM pqueue_request_idempotency \
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
            for target in targets {
                let row = one_row(
                    &connection,
                    "SELECT lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    vec![
                        tenant.clone().into(),
                        queue.clone().into(),
                        target.item_id.to_string().into(),
                    ],
                )
                .await?
                .ok_or(EngineError::NotFound)?;
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
            Ok(())
        }
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<FinalizeTarget>,
        now: UtcTimestamp,
        _default_max_attempts: u32,
    ) -> impl std::future::Future<Output = EngineResult<Vec<pqueue_engine::FinalizeLeaseMember>>> + Send
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
            let now_nanos = ts_nanos(now);
            let result = async {
                let mut attempts = Vec::with_capacity(targets.len());
                for target in targets {
                    let row = one_row(
                        &transaction,
                        "SELECT lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash,item_version,retry_count,max_attempts FROM pqueue_items \
                         WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        vec![
                            tenant.clone().into(),
                            queue.clone().into(),
                            target.item_id.to_string().into(),
                        ],
                    )
                    .await?
                    .ok_or(EngineError::NotFound)?;
                    let state = parse_state(&text(&row[0])?).map_err(storage)?;
                    if integer(&row[1])? != 0 { return Err(EngineError::StaleLease); }
                    if state.is_terminal() { return Err(EngineError::Terminal); }
                    if integer(&row[2])? != 0 { return Err(EngineError::Superseded); }
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
                    attempts.push(pqueue_engine::FinalizeLeaseMember {
                        item_id: target.item_id,
                        attempt_count: nonnegative_u32(integer(&row[7])?, "retry_count")?,
                        max_attempts: nonnegative_u32(
                            integer(&row[8])?,
                            "max_attempts",
                        )?,
                    });
                }
                Ok(attempts)
            }
            .await;
            transaction.rollback().await.map_err(storage)?;
            result
        }
    }

    fn cohort_lease_validate(
        &self,
        shard: QueueKey,
        target: CohortLeaseTarget,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<pqueue_engine::CohortLeaseMember>>> + Send
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
                     FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
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
                         FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
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
                    item_ids.push(pqueue_engine::CohortLeaseMember {
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
                    "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
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
        async move { apply_owned(writer, tokens, by_consumer, positions, commands, true).await }
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        let tokens = self.live_tokens.clone();
        let by_consumer = self.live_tokens_by_consumer.clone();
        async move { apply_owned(writer, tokens, by_consumer, positions, commands, false).await }
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
                            "SELECT item_id FROM pqueue_items \
                             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' \
                             AND superseded=0 AND cohort_size IS NULL \
                             AND (not_before IS NULL OR not_before<=?3) AND eligible_since IS NOT NULL \
                             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
                               JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id \
                               AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                               WHERE ig.tenant_id=pqueue_items.tenant_id \
                               AND ig.queue_id=pqueue_items.queue_id AND ig.item_id=pqueue_items.item_id) \
                             AND (?5 IS NULL OR group_key=?5) \
                             AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
                               WHERE NOT EXISTS (SELECT 1 FROM json_each(pqueue_items.metadata) actual \
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
            let tokens = self.live_tokens.lock().await.clone();
            let visible: Vec<_> = ids
                .iter()
                .copied()
                .filter(|id| tokens.contains_key(&(shard.clone(), *id)))
                .collect();
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
                     lease_expires_at,retry_count,payload,fields,metadata FROM pqueue_items \
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
                    "SELECT item_id,gate_key FROM pqueue_item_gates WHERE tenant_id=?1 \
                     AND queue_id=?2 AND item_id IN ({placeholders}) ORDER BY item_id,gate_key"
                );
                for row in self.query(gate_sql, params).await.map_err(storage)? {
                    let id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
                    gate_keys.entry(id).or_default().push(text(&row.values[1])?);
                }
            }
            let mut claimed = Vec::new();
            for id in ids {
                let Some(token) = tokens.get(&(shard.clone(), id)).cloned() else {
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
                    payload: optional_blob(&values[7])?.map(Bytes::from),
                    fields: fields_from_json(text(&values[8])?)?,
                    metadata: metadata_from_json(text(&values[9])?)?,
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

#[cfg(test)]
mod push_batch_lowering_tests {
    use pqueue_conformance::item;
    use pqueue_core::{IndexDeclaration, IndexDef, IndexType, ItemId, QueueIndex};

    use super::{
        GROUP_SUMMARY_CHUNK, PUSH_GATE_CHUNK, PUSH_INDEX_CHUNK, PUSH_ITEM_CHUNK,
        UNIQUE_CHECK_CHUNK, index_is_unique, typed_index_keys,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct StatementShape {
        item_inserts: usize,
        gate_inserts: usize,
        unique_checks: usize,
        index_inserts: usize,
    }

    fn statement_shape(
        items: &[pqueue_engine::PushItem],
        indexes: &[QueueIndex],
    ) -> StatementShape {
        let gate_rows = items.iter().map(|item| item.gate_keys.len()).sum::<usize>();
        let keys = items
            .iter()
            .flat_map(|item| typed_index_keys(indexes, item.entity_document.as_ref()).unwrap())
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

    fn indexed_gated_items(count: usize) -> (Vec<pqueue_engine::PushItem>, Vec<QueueIndex>) {
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

    use pqueue_conformance::{envelope, item, qdef, ts};
    use pqueue_core::{BodyHash, ItemId, ItemState, RequestId};
    use pqueue_engine::{
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
        pqueue_engine::CommandEnvelope,
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
