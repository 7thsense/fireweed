// The engine port deliberately spells futures as RPITIT; mirror that signature without refining the
// implementation's public return type to `async fn`.
#![allow(clippy::manual_async_fn)]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, QueueDefinition, UtcTimestamp,
};
use pqueue_engine::{
    AsyncProjectionStore, ClaimedItem, CommandEnvelope, CommandPosition, EngineError, EngineResult,
    QueueCommand, QueueKey,
};
use pqueue_relational::{
    async_projection as sql, elig_sort, fields_from_json, fields_to_json, lease_hash,
    metadata_from_json, metadata_to_json, nanos_ts, parse_priority, parse_state, ts_nanos,
    ts_nanos_opt,
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

fn validate_minimal_commands(commands: &[CommandEnvelope]) -> EngineResult<()> {
    for envelope in commands {
        match &envelope.command {
            QueueCommand::CreateQueue(_) | QueueCommand::Claim(_) => {}
            QueueCommand::Push(push) => {
                if push.items.iter().any(|item| {
                    item.group_key.is_some()
                        || item.cohort_size.is_some()
                        || !item.gate_keys.is_empty()
                }) {
                    return Err(EngineError::Unavailable);
                }
            }
            _ => return Err(EngineError::Unavailable),
        }
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
    live_tokens: Arc<Mutex<HashMap<ItemId, LeaseToken>>>,
    positions: Vec<CommandPosition>,
    commands: Vec<CommandEnvelope>,
) -> EngineResult<()> {
    if positions.len() != commands.len() {
        return Err(storage("positions/commands length mismatch"));
    }
    validate_minimal_commands(&commands)?;
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
                                Value::Integer(item.max_attempts as i64),
                                Value::Integer(
                                    base.checked_add(i64::try_from(offset).map_err(storage)?)
                                        .ok_or_else(|| storage("item sequence overflow"))?,
                                ),
                            ],
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            QueueCommand::Claim(claim) => {
                if claim.item_ids.is_empty() {
                    next_by_queue.insert(position.queue.clone(), incoming + 1);
                    continue;
                }
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
                params.extend(
                    claim
                        .item_ids
                        .iter()
                        .map(|item| Value::Text(item.to_string())),
                );
                let changed = transaction
                    .execute(sql::claim_items(claim.item_ids.len()), params)
                    .await
                    .map_err(storage)?;
                if changed != claim.item_ids.len() as u64 {
                    transaction.rollback().await.map_err(storage)?;
                    return Err(storage("claim changed an unexpected row count"));
                }
                token_ops.extend(
                    claim
                        .item_ids
                        .iter()
                        .map(|item| (*item, claim.lease_token.clone())),
                );
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
    for (item, token) in token_ops {
        tokens.insert(item, token);
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
        async move {
            let paused = self
                .query(
                    "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                    ],
                )
                .await
                .map_err(storage)?;
            let Some(paused) = paused.first() else {
                return Err(EngineError::NotFound);
            };
            if integer(&paused.values[0])? != 0 {
                return Ok(Vec::new());
            }
            let rows = self
                .query(
                    sql::SELECT_ELIGIBLE,
                    vec![
                        shard.tenant_id.as_str().to_string().into(),
                        shard.queue_id.as_str().to_string().into(),
                        ts_nanos(now).into(),
                        i64::try_from(max).map_err(storage)?.into(),
                    ],
                )
                .await
                .map_err(storage)?;
            rows.into_iter()
                .map(|row| ItemId::new(text(&row.values[0])?).map_err(storage))
                .collect()
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
                let Some(token) = tokens.get(&id).cloned() else {
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
                claimed.push(ClaimedItem {
                    item_id: id,
                    client_item_key: ClientItemKey::new(text(&values[0])?).map_err(storage)?,
                    item_version: integer(&values[1])? as u64,
                    priority: parse_priority(optional_text(&values[2])?)?,
                    group_key: optional_text(&values[3])?
                        .map(GroupKey::new)
                        .transpose()
                        .map_err(storage)?,
                    not_before: optional_integer(&values[4])?.map(nanos_ts),
                    lease_token: Some(token),
                    lease_expires_at: nanos_ts(expires),
                    attempt_count: integer(&values[6])? as u32,
                    payload: optional_blob(&values[7])?.map(Bytes::from),
                    fields: fields_from_json(text(&values[8])?)?,
                    metadata: metadata_from_json(text(&values[9])?)?,
                    gate_keys: Vec::new(),
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
                .map(|row| integer(&row.values[0]).map(|value| value as u64))
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
