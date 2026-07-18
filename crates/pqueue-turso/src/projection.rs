// The engine port deliberately spells futures as RPITIT; mirror that signature without refining the
// implementation's public return type to `async fn`.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, CohortId, GroupKey, IndexDeclaration, ItemId, ItemState, LeaseToken,
    QueueDefinition, QueueIndex, RequestId, UtcTimestamp, is_retry_exhausted,
};
use pqueue_engine::{
    AsyncProjectionStore, ClaimCompatibility, ClaimUnit, ClaimedItem, CommandEnvelope,
    CommandPosition, EngineError, EngineResult, FinalizeKind, IdempotencyDecision, PayloadUpdate,
    PushFingerprint, PushItem, QueueCommand, QueueKey, RequestOutcome, RichClaimSelection,
};
use pqueue_relational::{
    async_projection as sql, claim_by_query_replay_item_ids, elig_sort, fields_from_json,
    fields_to_json, lease_hash, metadata_from_json, metadata_to_json, nanos_ts, parse_priority,
    parse_state, ts_nanos, ts_nanos_opt,
};
use tokio::sync::Mutex;
use turso::{Connection, Value, transaction::TransactionBehavior};

use crate::TursoRelational;

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
        | QueueCommand::SetGates(_) => {}
        QueueCommand::Push(_)
        | QueueCommand::CohortClaim(_)
        | QueueCommand::CohortRenewLease(_)
        | QueueCommand::CohortFinalize(_)
        | QueueCommand::CohortExpired(_) => {}
        _ => return Err(EngineError::Unavailable),
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
    for item in items {
        let item_id = item.item_id.to_string();
        let keys = typed_index_keys(indexes, item.entity_document.as_ref())?;
        check_typed_unique_conflicts(transaction, tenant, queue, indexes, &keys).await?;
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
        }
        rows.push((item_id, keys));
    }
    for (item_id, keys) in rows {
        insert_typed_index_rows(transaction, tenant, queue, &item_id, &keys).await?;
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

async fn refresh_group_summary(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    group: &GroupKey,
    now: i64,
) -> EngineResult<()> {
    let params = vec![
        tenant.to_string().into(),
        queue.to_string().into(),
        group.as_str().to_string().into(),
        Value::Integer(now),
    ];
    let aggregate = one_row(
        transaction,
        "SELECT COUNT(*),MIN(eligible_since) FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 AND lifecycle_state='Pending' \
         AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
         ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
         WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
         AND ig.item_id=pqueue_items.item_id)",
        params.clone(),
    )
    .await?
    .ok_or_else(|| storage("group aggregate returned no row"))?;
    let representative = one_row(
        transaction,
        "SELECT priority_sort,created_at,item_id FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 AND lifecycle_state='Pending' \
         AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
         ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
         WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
         AND ig.item_id=pqueue_items.item_id) ORDER BY priority_sort,created_seq LIMIT 1",
        params,
    )
    .await?;
    let (priority, created, item) = representative
        .map_or((Value::Null, Value::Null, Value::Null), |row| {
            (row[0].clone(), row[1].clone(), row[2].clone())
        });
    transaction
        .execute(
            "INSERT INTO pqueue_group_summary \
             (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,\
              rep_priority_sort,rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
             VALUES(?1,?2,?3,?4,NULL,?5,?6,?7,?8,0,?9) \
             ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
              oldest_eligible_at=excluded.oldest_eligible_at,\
              rep_progress_guard_sort=excluded.rep_progress_guard_sort,\
              rep_priority_sort=excluded.rep_priority_sort,rep_created_at=excluded.rep_created_at,\
              rep_item_id=excluded.rep_item_id,eligible_item_count=excluded.eligible_item_count,\
              at_risk_count=excluded.at_risk_count,updated_at=excluded.updated_at",
            vec![
                tenant.to_string().into(),
                queue.to_string().into(),
                group.as_str().to_string().into(),
                aggregate[1].clone(),
                priority,
                created,
                item,
                aggregate[0].clone(),
                Value::Integer(now),
            ],
        )
        .await
        .map_err(storage)?;
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
    for group in groups {
        refresh_group_summary(transaction, tenant, queue, &group, now).await?;
    }
    Ok(())
}

async fn candidate_groups(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
) -> EngineResult<Vec<GroupKey>> {
    let mut rows = transaction
        .query(
            "SELECT group_key FROM pqueue_group_summary WHERE tenant_id=?1 AND queue_id=?2 \
             AND oldest_eligible_at IS NOT NULL \
             ORDER BY rep_priority_sort,rep_created_at,rep_item_id",
            vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
            ],
        )
        .await
        .map_err(storage)?;
    let mut groups = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        groups.push(GroupKey::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?);
    }
    Ok(groups)
}

async fn candidate_groups_for_claim(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    now: i64,
    compatibility: &ClaimCompatibility,
) -> EngineResult<Vec<GroupKey>> {
    if compatibility.metadata_equals.is_empty() {
        return candidate_groups(transaction, tenant, queue).await;
    }
    const PAGE_SIZE: i64 = 128;
    let mut groups = Vec::new();
    let mut seen = HashSet::new();
    let mut offset = 0_i64;
    loop {
        let mut rows = transaction
            .query(
                "SELECT group_key,metadata FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                 AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
                 AND group_key IS NOT NULL AND (not_before IS NULL OR not_before<=?3) \
                 AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
                 JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
                 AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=pqueue_items.tenant_id \
                 AND ig.queue_id=pqueue_items.queue_id AND ig.item_id=pqueue_items.item_id) \
                 ORDER BY priority_sort,created_seq LIMIT ?4 OFFSET ?5",
                vec![
                    tenant.to_string().into(),
                    queue.to_string().into(),
                    Value::Integer(now),
                    Value::Integer(PAGE_SIZE),
                    Value::Integer(offset),
                ],
            )
            .await
            .map_err(storage)?;
        let mut page_len = 0_i64;
        while let Some(row) = rows.next().await.map_err(storage)? {
            page_len += 1;
            let group =
                GroupKey::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?;
            if compatibility
                .group_key
                .as_ref()
                .is_some_and(|required| required != &group)
            {
                continue;
            }
            let metadata = metadata_from_json(text(&row.get_value(1).map_err(storage)?)?)?;
            if compatibility
                .metadata_equals
                .iter()
                .all(|(key, expected)| metadata.get(key) == Some(expected))
                && seen.insert(group.clone())
            {
                groups.push(group);
            }
        }
        drop(rows);
        if page_len < PAGE_SIZE {
            return Ok(groups);
        }
        offset += PAGE_SIZE;
    }
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
    const PAGE_SIZE: i64 = 128;
    let mut ids = Vec::new();
    let mut eligible_count = 0_usize;
    let mut offset = 0_i64;
    loop {
        let mut rows = transaction
            .query(
            format!(
                "SELECT item_id,metadata FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                 AND group_key=?3 AND lifecycle_state='Pending' AND superseded=0 \
                 AND {cohort_predicate} AND (not_before IS NULL OR not_before<=?4) \
                 AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
                 JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id \
                 AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
                 AND ig.item_id=pqueue_items.item_id) ORDER BY priority_sort,created_seq LIMIT ?5 OFFSET ?6"
            ),
            vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Text(group.as_str().to_string()),
                Value::Integer(now),
                Value::Integer(PAGE_SIZE),
                Value::Integer(offset),
            ],
        )
        .await
        .map_err(storage)?;
        let mut page_len = 0_i64;
        while let Some(row) = rows.next().await.map_err(storage)? {
            page_len += 1;
            let metadata = metadata_from_json(text(&row.get_value(1).map_err(storage)?)?)?;
            if compatibility
                .metadata_equals
                .iter()
                .all(|(key, expected)| metadata.get(key) == Some(expected))
            {
                eligible_count += 1;
                if ids.len() < limit {
                    ids.push(
                        ItemId::new(text(&row.get_value(0).map_err(storage)?)?).map_err(storage)?,
                    );
                }
            }
        }
        drop(rows);
        if page_len < PAGE_SIZE {
            return Ok(GroupEligibility {
                item_ids: ids,
                eligible_count,
            });
        }
        offset += PAGE_SIZE;
    }
}

struct GroupEligibility {
    item_ids: Vec<ItemId>,
    eligible_count: usize,
}

async fn active_group_member_count(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    group: &GroupKey,
    cohort: bool,
) -> EngineResult<usize> {
    let cohort_predicate = if cohort {
        "cohort_size IS NOT NULL"
    } else {
        "cohort_size IS NULL"
    };
    let row = one_row(
        transaction,
        &format!(
            "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
             AND superseded=0 AND {cohort_predicate} AND lifecycle_state NOT IN ('Complete','Failed')"
        ),
        vec![
            tenant.to_string().into(),
            queue.to_string().into(),
            group.as_str().to_string().into(),
        ],
    )
    .await?
    .ok_or_else(|| storage("active group count returned no row"))?;
    usize::try_from(integer(&row[0])?).map_err(storage)
}

async fn group_has_active_lease(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    group: &GroupKey,
) -> EngineResult<bool> {
    let row = one_row(
        transaction,
        "SELECT EXISTS(SELECT 1 FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
         AND group_key=?3 AND superseded=0 AND cohort_size IS NULL AND lifecycle_state='Leased')",
        vec![
            tenant.to_string().into(),
            queue.to_string().into(),
            group.as_str().to_string().into(),
        ],
    )
    .await?
    .ok_or_else(|| storage("group lease contention check returned no row"))?;
    Ok(integer(&row[0])? != 0)
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
    let mut selected = Vec::new();
    let mut used = 0_u32;
    for group in candidate_groups_for_claim(transaction, tenant, queue, now, compatibility).await? {
        if used >= max_groups {
            break;
        }
        let eligible = group_eligible_items(
            transaction,
            tenant,
            queue,
            &group,
            now,
            max_items.saturating_add(1),
            false,
            compatibility,
        )
        .await?;
        if group_has_active_lease(transaction, tenant, queue, &group).await? {
            continue;
        }
        if eligible.item_ids.is_empty() {
            continue;
        }
        if eligible.eligible_count > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        if selected.len().saturating_add(eligible.item_ids.len()) > max_items {
            break;
        }
        selected.extend(eligible.item_ids);
        used += 1;
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
    for group in candidate_groups_for_claim(transaction, tenant, queue, now, compatibility).await? {
        if compatibility
            .group_key
            .as_ref()
            .is_some_and(|required| required != &group)
        {
            continue;
        }
        let eligible = group_eligible_items(
            transaction,
            tenant,
            queue,
            &group,
            now,
            max_items,
            false,
            compatibility,
        )
        .await?;
        if !eligible.item_ids.is_empty() {
            return Ok(eligible.item_ids);
        }
    }
    Ok(Vec::new())
}

async fn select_whole_cohort(
    transaction: &Connection,
    tenant: &str,
    queue: &str,
    now: i64,
    max_items: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<RichClaimSelection> {
    let mut rows = transaction
        .query(
            "SELECT group_key,cohort_id,cohort_size FROM pqueue_cohorts \
             WHERE tenant_id=?1 AND queue_id=?2 AND state='complete' \
             ORDER BY cohort_created_at,group_key",
            vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
            ],
        )
        .await
        .map_err(storage)?;
    let mut cohorts = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        cohorts.push((
            text(&row.get_value(0).map_err(storage)?)?,
            text(&row.get_value(1).map_err(storage)?)?,
            integer(&row.get_value(2).map_err(storage)?)?,
        ));
    }
    drop(rows);
    for (group, cohort_id, size) in cohorts {
        let size = usize::try_from(size).map_err(|_| storage("invalid cohort size"))?;
        let member_row = one_row(
            transaction,
            "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND group_key=?3 AND superseded=0 AND cohort_size IS NOT NULL \
             AND lifecycle_state NOT IN ('Complete','Failed')",
            vec![
                tenant.to_string().into(),
                queue.to_string().into(),
                group.clone().into(),
            ],
        )
        .await?
        .ok_or_else(|| storage("cohort member count returned no row"))?;
        if usize::try_from(integer(&member_row[0])?).map_err(storage)? != size {
            continue;
        }
        let group_key = GroupKey::new(group).map_err(storage)?;
        let eligible = group_eligible_items(
            transaction,
            tenant,
            queue,
            &group_key,
            now,
            size.saturating_add(1),
            true,
            compatibility,
        )
        .await?;
        if eligible.eligible_count != size
            || eligible.eligible_count
                != active_group_member_count(transaction, tenant, queue, &group_key, true).await?
        {
            continue;
        }
        if size > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        return Ok(RichClaimSelection {
            item_ids: eligible.item_ids,
            cohort_id: Some(CohortId::new(cohort_id).map_err(storage)?),
        });
    }
    Ok(RichClaimSelection::default())
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
    let renewed: HashSet<ItemId> = renewed_item_ids.iter().copied().collect();
    let mut rows = transaction
        .query(
            sql::SELECT_CLAIM_BY_QUERY_REPLAYS,
            vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
        )
        .await
        .map_err(storage)?;
    let mut request_ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        let request_id = text(&row.get_value(0).map_err(storage)?)?;
        let payload = text(&row.get_value(1).map_err(storage)?)?;
        let replay_ids = claim_by_query_replay_item_ids(&payload)?;
        if !replay_ids.is_empty() && replay_ids.iter().all(|item| renewed.contains(item)) {
            request_ids.push(request_id);
        }
    }
    drop(rows);
    for request_id in request_ids {
        transaction
            .execute(
                sql::EXTEND_CLAIM_BY_QUERY_REPLAY,
                vec![
                    Value::Text(tenant.clone()),
                    Value::Text(queue.clone()),
                    Value::Text(request_id),
                    Value::Integer(ts_nanos(renewed_expires_at)),
                ],
            )
            .await
            .map_err(storage)?;
    }
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
    live_tokens: Arc<Mutex<HashMap<(QueueKey, ItemId), LeaseToken>>>,
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
                for (offset, item) in push.items.iter().enumerate() {
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
                    transaction
                        .execute(
                            sql::INSERT_ITEM,
                            vec![
                                tenant.clone().into(),
                                queue.clone().into(),
                                item.item_id.to_string().into(),
                                item.client_item_key.as_str().to_string().into(),
                                priority.map_or(Value::Null, Value::Text),
                                Value::Blob(elig_sort(&item.priority, &definition.priority_model)),
                                not_before.map_or(Value::Null, Value::Integer),
                                Value::Integer(not_before.unwrap_or(now)),
                                item.group_key.as_ref().map_or(Value::Null, |group| {
                                    Value::Text(group.as_str().to_string())
                                }),
                                cohort_size.map_or(Value::Null, Value::Integer),
                                item.payload
                                    .as_ref()
                                    .map_or(Value::Null, |value| Value::Blob(value.to_vec())),
                                Value::Text(fields_to_json(&item.fields)?),
                                Value::Text(metadata_to_json(&item.metadata)?),
                                entity.map_or(Value::Null, Value::Text),
                                Value::Integer(incoming),
                                Value::Integer(now),
                                Value::Integer(i64::from(item.max_attempts)),
                                Value::Integer(
                                    base.checked_add(i64::try_from(offset).map_err(storage)?)
                                        .ok_or_else(|| storage("item sequence overflow"))?,
                                ),
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
                }
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
                for group in groups {
                    refresh_group_summary(&transaction, &tenant, &queue, &group, now).await?;
                }
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
                    for group in groups {
                        refresh_group_summary(
                            &transaction,
                            &tenant,
                            &queue,
                            &group,
                            ts_nanos(envelope.created_at),
                        )
                        .await?;
                    }
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
                refresh_group_summary(
                    &transaction,
                    &tenant,
                    &queue,
                    &group,
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
                        (ItemState::Pending, FinalizeKind::Rearm) => rearmed.push(outcome.item_id),
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
                let now = ts_nanos(envelope.created_at);
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
                for group in groups {
                    refresh_group_summary(&transaction, &tenant, &queue, &group, now).await?;
                }
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
                if matches!(finalize.kind, FinalizeKind::Retry) {
                    let mut params = vec![Value::Text(tenant.clone()), Value::Text(queue.clone())];
                    append_item_ids(&mut params, &ids);
                    let mut rows = transaction
                        .query(&sql::select_retry_info(ids.len()), params)
                        .await
                        .map_err(storage)?;
                    while let Some(row) = rows.next().await.map_err(storage)? {
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
                        } else {
                            pending.push(item);
                        }
                    }
                    if failed.len() + pending.len() != ids.len() {
                        return Err(storage("cohort finalize could not read every member"));
                    }
                } else {
                    match finalize.kind {
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
                    if matches!(finalize.kind, FinalizeKind::Complete | FinalizeKind::Fail) {
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
                refresh_group_summary(&transaction, &tenant, &queue, &group, now).await?;
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
                for group in groups {
                    refresh_group_summary(
                        &transaction,
                        &tenant,
                        &queue,
                        &group,
                        ts_nanos(envelope.created_at),
                    )
                    .await?;
                }
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
                refresh_group_summary(&transaction, &tenant, &queue, &expired.group_key, now)
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
                        let state = parse_state(&text(&row.get_value(2).map_err(storage)?)?)?;
                        if state.is_terminal() && definition.client_item_key_retention_ms > 0 {
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
                    for group in groups {
                        refresh_group_summary(
                            &transaction,
                            &tenant,
                            &queue,
                            &group,
                            ts_nanos(envelope.created_at),
                        )
                        .await?;
                    }
                }
            }
            _ => unreachable!("validated minimal command set"),
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
    for op in token_ops {
        match op {
            TokenOp::Set(shard, item, token) => {
                tokens.insert((shard, item), token);
            }
            TokenOp::Clear(shard, item) => {
                tokens.remove(&(shard, item));
            }
        }
    }
    Ok(())
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

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        let tokens = self.live_tokens.clone();
        async move { apply_owned(writer, tokens, positions, commands, true).await }
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        let tokens = self.live_tokens.clone();
        async move { apply_owned(writer, tokens, positions, commands, false).await }
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
                    let mut selected = Vec::new();
                    const PAGE_SIZE: i64 = 128;
                    let mut offset = 0_i64;
                    loop {
                        let mut rows = transaction
                            .query(
                                sql::SELECT_ELIGIBLE_FILTERABLE,
                                vec![
                                    tenant.clone().into(),
                                    queue.clone().into(),
                                    Value::Integer(ts_nanos(now)),
                                    Value::Integer(PAGE_SIZE),
                                    Value::Integer(offset),
                                ],
                            )
                            .await
                            .map_err(storage)?;
                        let mut page_len = 0_i64;
                        while let Some(row) = rows.next().await.map_err(storage)? {
                            page_len += 1;
                            let group_key = optional_text(&row.get_value(1).map_err(storage)?)?;
                            if compatibility.group_key.as_ref().is_some_and(|required| {
                                group_key.as_deref() != Some(required.as_str())
                            }) {
                                continue;
                            }
                            let metadata =
                                metadata_from_json(text(&row.get_value(2).map_err(storage)?)?)?;
                            if compatibility
                                .metadata_equals
                                .iter()
                                .all(|(key, expected)| metadata.get(key) == Some(expected))
                            {
                                selected.push(
                                    ItemId::new(text(&row.get_value(0).map_err(storage)?)?)
                                        .map_err(storage)?,
                                );
                                if selected.len() == max {
                                    return Ok(selected);
                                }
                            }
                        }
                        drop(rows);
                        if page_len < PAGE_SIZE {
                            return Ok(selected);
                        }
                        offset += PAGE_SIZE;
                    }
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
            let mut claimed = Vec::new();
            for id in ids {
                let Some(token) = tokens.get(&(shard.clone(), id)).cloned() else {
                    continue;
                };
                let rows = self
                    .query(
                        sql::SELECT_CLAIMED_ITEM,
                        vec![
                            shard.tenant_id.as_str().to_string().into(),
                            shard.queue_id.as_str().to_string().into(),
                            id.to_string().into(),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                let Some(row) = rows.first() else { continue };
                let values = &row.values;
                let Some(expires) = optional_integer(&values[5])? else {
                    continue;
                };
                let gate_rows = self
                    .query(
                        sql::SELECT_ITEM_GATES,
                        vec![
                            shard.tenant_id.as_str().to_string().into(),
                            shard.queue_id.as_str().to_string().into(),
                            id.to_string().into(),
                        ],
                    )
                    .await
                    .map_err(storage)?;
                let gate_keys = gate_rows
                    .iter()
                    .map(|row| text(&row.values[0]))
                    .collect::<EngineResult<Vec<_>>>()?;
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
                    gate_keys,
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
