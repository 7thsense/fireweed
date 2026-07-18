// The engine port deliberately spells futures as RPITIT; mirror that signature without refining the
// implementation's public return type to `async fn`.
#![allow(clippy::manual_async_fn)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, QueueDefinition, UtcTimestamp,
    is_retry_exhausted,
};
use pqueue_engine::{
    AsyncProjectionStore, ClaimedItem, CommandEnvelope, CommandPosition, EngineError, EngineResult,
    FinalizeKind, PayloadUpdate, QueueCommand, QueueKey,
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
        QueueCommand::Push(push) => {
            if push
                .items
                .iter()
                .any(|item| item.group_key.is_some() || item.cohort_size.is_some())
            {
                return Err(EngineError::Unavailable);
            }
        }
        _ => return Err(EngineError::Unavailable),
    }
    Ok(())
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
                if !definition.typed_indexes.is_empty() {
                    transaction.rollback().await.map_err(storage)?;
                    return Err(EngineError::Unavailable);
                }
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
            }
            QueueCommand::Claim(claim) => {
                if !claim.item_ids.is_empty() {
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
                }
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
                        if !definition.typed_indexes.is_empty() {
                            transaction.rollback().await.map_err(storage)?;
                            return Err(EngineError::Unavailable);
                        }
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
                if !definition.typed_indexes.is_empty()
                    || replace.replacement.group_key.is_some()
                    || replace.replacement.cohort_size.is_some()
                {
                    transaction.rollback().await.map_err(storage)?;
                    return Err(EngineError::Unavailable);
                }
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
            }
            QueueCommand::LeaseExpired(expired) => {
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
            QueueCommand::PauseQueue(_) => {
                transaction
                    .execute(
                        sql::PAUSE_QUEUE,
                        vec![Value::Text(tenant.clone()), Value::Text(queue.clone())],
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

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let writer = self.writer.clone();
        let tokens = self.live_tokens.clone();
        async move { apply_owned(writer, tokens, positions, commands).await }
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        self.apply_live(positions, commands)
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
