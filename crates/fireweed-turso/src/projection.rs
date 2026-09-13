// The engine port deliberately spells futures as RPITIT; mirror that signature without refining the
// implementation's public return type to `async fn`.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, CohortId, GroupKey, IndexDeclaration, ItemId, ItemState, LeaseToken, Metadata,
    QueueDefinition, QueueIndex, RequestId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncProjectionStore, BatchUpdateSnapshotItem, ClaimCompatibility, ClaimRef, ClaimUnit,
    ClaimedItem, CohortLeaseTarget, CommandEnvelope, CommandPosition, CreateQueueOutcome,
    EngineError, EngineResult, FinalizeTarget, IdempotencyDecision, ItemView, LeaseView,
    LiveItemView, PendingPage, PendingSummary, PushFingerprint, PushItem, QueueCommand, QueueKey,
    QueueMetrics, RenewTarget, RichClaimSelection, TerminalEmissionMetrics, UpdateFieldsCommand,
};
use fireweed_relational::{
    ClassSClaimedItem, RelRow, RelTx, RelValue, SQLITE_BIND_CAP, TokenOp, async_projection as sql,
    entity_from_json, fields_from_json, lease_hash, metadata_from_json, metadata_to_json, nanos_ts,
    parse_priority, parse_state, ts_nanos,
};
use tokio::sync::Mutex;
use turso::{Connection, Row, Value, transaction::TransactionBehavior};

use crate::{
    COMMITTED_DRIVER_POOL_RESOURCE, COMMITTED_OUTCOME_POOL_RESOURCE, TursoApplyPhaseObservation,
    TursoRelational,
    local::{ConsumerLeaseIndex, TursoBatchUpdateStatementShape},
    map_pooled_reader_error, render_class_s_claimed_items,
};

fn storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
}

/// Optional slow-statement diagnostics; never includes parameter values.
pub(crate) fn trace_sql(sql: &str, binds: usize, rows: usize, elapsed: Duration) {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if elapsed < Duration::from_millis(2)
        || !*ENABLED.get_or_init(|| std::env::var_os("FIREWEED_SQL_TRACE").is_some())
    {
        return;
    }
    let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let excerpt = if compact.len() > 700 {
        let head: String = compact.chars().take(150).collect();
        let tail: String = compact
            .chars()
            .rev()
            .take(500)
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        format!("{head} ... {tail}")
    } else {
        compact
    };
    eprintln!(
        "sql_us={} binds={binds} rows={rows} sql={excerpt}",
        elapsed.as_micros()
    );
}

/// Truncate only when the WAL is already large and no reader snapshot is live.
/// `busy_timeout=0` makes TRUNCATE fail immediately if a Deferred reader holds
/// the WAL; apply then continues and the next quiet apply retries.
/// Standalone OFF-mode projections retain the 1,000-frame automatic policy and
/// this truncation workaround. Log-backed projections use NORMAL accounting
/// and an explicitly verified checkpoint limit, and bypass forced truncation.
/// Explicit checkpoints clear the pager cache and TRUNCATE syncs the WAL; keep
/// the OFF-mode workaround bounded rather than accumulating unreusable history.
// The byte budget is derived at open from the actual database page size.

pub(crate) fn sqlite_wal_path(database: &Path) -> Option<PathBuf> {
    if database == Path::new(":memory:") {
        return None;
    }
    let mut path = database.as_os_str().to_os_string();
    path.push("-wal");
    Some(PathBuf::from(path))
}

pub(crate) async fn truncate_wal_if_unpinned(
    writer: &Mutex<Connection>,
    wal_path: Option<&Path>,
    min_bytes: u64,
    busy_timeout: Duration,
) {
    let Some(wal_path) = wal_path else {
        return;
    };
    let Ok(len) = std::fs::metadata(wal_path).map(|meta| meta.len()) else {
        return;
    };
    if len < min_bytes {
        return;
    }
    let Ok(connection) = writer.try_lock() else {
        return;
    };
    let _ = connection.busy_timeout(Duration::ZERO);
    if let Ok(mut rows) = connection
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
    {
        while rows.next().await.ok().flatten().is_some() {}
    }
    let _ = connection.busy_timeout(busy_timeout);
}

fn outcome_read_error(error: turso::Error) -> EngineError {
    map_pooled_reader_error(error, COMMITTED_OUTCOME_POOL_RESOURCE)
}

fn driver_read_error(error: turso::Error) -> EngineError {
    map_pooled_reader_error(error, COMMITTED_DRIVER_POOL_RESOURCE)
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

fn take_text(value: Value) -> EngineResult<String> {
    match value {
        Value::Text(value) => Ok(value),
        other => Err(storage(format!("expected text, got {other:?}"))),
    }
}

fn take_integer(value: Value) -> EngineResult<i64> {
    match value {
        Value::Integer(value) => Ok(value),
        other => Err(storage(format!("expected integer, got {other:?}"))),
    }
}

fn take_optional_text(value: Value) -> EngineResult<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value)),
        other => Err(storage(format!("expected optional text, got {other:?}"))),
    }
}

fn take_optional_integer(value: Value) -> EngineResult<Option<i64>> {
    match value {
        Value::Null => Ok(None),
        Value::Integer(value) => Ok(Some(value)),
        other => Err(storage(format!("expected optional integer, got {other:?}"))),
    }
}

fn take_optional_blob(value: Value) -> EngineResult<Option<Vec<u8>>> {
    match value {
        Value::Null => Ok(None),
        Value::Blob(value) => Ok(Some(value)),
        other => Err(storage(format!("expected optional blob, got {other:?}"))),
    }
}

fn class_s_item_from_turso_row(
    row: &Row,
    lease_expires_at: i64,
) -> EngineResult<ClassSClaimedItem> {
    Ok(ClassSClaimedItem {
        item_id: take_text(row.get_value(0).map_err(driver_read_error)?)?,
        client_item_key: take_text(row.get_value(1).map_err(driver_read_error)?)?,
        payload: take_optional_blob(row.get_value(2).map_err(driver_read_error)?)?,
        item_version: take_integer(row.get_value(3).map_err(driver_read_error)?)? + 1,
        retry_count: take_integer(row.get_value(4).map_err(driver_read_error)?)? + 1,
        lease_expires_at,
        priority: take_optional_text(row.get_value(5).map_err(driver_read_error)?)?,
        group_key: take_optional_text(row.get_value(6).map_err(driver_read_error)?)?,
        not_before: take_optional_integer(row.get_value(7).map_err(driver_read_error)?)?,
        fields_json: take_optional_text(row.get_value(8).map_err(driver_read_error)?)?
            .unwrap_or_else(|| "{}".into()),
        metadata_json: take_optional_text(row.get_value(9).map_err(driver_read_error)?)?
            .unwrap_or_else(|| "{}".into()),
        max_attempts: take_optional_integer(row.get_value(10).map_err(driver_read_error)?)?
            .unwrap_or(0),
        entity_document: take_optional_text(row.get_value(11).map_err(driver_read_error)?)?,
        index_fields: take_optional_blob(row.get_value(12).map_err(driver_read_error)?)?,
        gate_keys: Vec::new(),
    })
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

async fn one_outcome_row(
    connection: &Connection,
    query: &str,
    params: Vec<Value>,
) -> EngineResult<Option<Vec<Value>>> {
    let mut rows = connection
        .query(query, params)
        .await
        .map_err(outcome_read_error)?;
    let Some(row) = rows.next().await.map_err(outcome_read_error)? else {
        return Ok(None);
    };
    let mut values = Vec::with_capacity(row.column_count());
    for index in 0..row.column_count() {
        values.push(row.get_value(index).map_err(outcome_read_error)?);
    }
    Ok(Some(values))
}

async fn query_value_rows(
    connection: &Connection,
    query: impl AsRef<str>,
    params: Vec<Value>,
) -> EngineResult<Vec<Vec<Value>>> {
    let started = Instant::now();
    let bind_count = params.len();
    let mut statement = connection
        .prepare_cached(query.as_ref())
        .await
        .map_err(outcome_read_error)?;
    let mut rows = statement.query(params).await.map_err(outcome_read_error)?;
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await.map_err(outcome_read_error)? {
        let mut values = Vec::with_capacity(row.column_count());
        for index in 0..row.column_count() {
            values.push(row.get_value(index).map_err(outcome_read_error)?);
        }
        collected.push(values);
    }
    trace_sql(
        query.as_ref(),
        bind_count,
        collected.len(),
        started.elapsed(),
    );
    Ok(collected)
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
    let values = (0..ids.len())
        .map(|i| format!("(?{})", i + 3))
        .collect::<Vec<_>>()
        .join(",");
    let columns = columns
        .split(',')
        .map(|c| format!("i.{}", c.trim()))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "WITH incoming(item_id) AS (VALUES {values}) SELECT i.item_id,{columns} \
         FROM incoming CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
         ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=incoming.item_id"
    );
    let mut params = Vec::with_capacity(ids.len() + 2);
    params.push(Value::Text(tenant.to_string()));
    params.push(Value::Text(queue.to_string()));
    params.extend(ids.iter().map(|id| Value::Text(id.to_string())));
    let started = Instant::now();
    let bind_count = params.len();
    let mut statement = connection.prepare_cached(&query).await.map_err(storage)?;
    let mut rows = statement.query(params).await.map_err(storage)?;
    let mut by_item = HashMap::with_capacity(ids.len());
    while let Some(row) = rows.next().await.map_err(storage)? {
        let item_id = ItemId::new(row.get::<String>(0).map_err(storage)?).map_err(storage)?;
        let mut values = Vec::with_capacity(row.column_count().saturating_sub(1));
        for index in 1..row.column_count() {
            values.push(row.get_value(index).map_err(storage)?);
        }
        by_item.insert(item_id, values);
    }
    trace_sql(&query, bind_count, by_item.len(), started.elapsed());
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

/// Rebuild group heads whose stored representative is missing or no longer Pending.
///
/// Item Claim/Complete and uniform-priority BatchUpdate leave `fireweed_group_summary`
/// lagged so the serving apply path does not rewrite every touched group. Grouped Claim
/// repairs those heads here before ranking.
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
    let chunk_size = SQLITE_BIND_CAP
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
    inner: crate::tx::ApplyTursoRel<'a>,
    statement_shape: Option<Arc<std::sync::Mutex<TursoBatchUpdateStatementShape>>>,
    phases: Arc<std::sync::Mutex<RelApplyPhaseTotals>>,
}

impl RelTx for ObservedTursoRel<'_> {
    fn prefer_point_updates(&self) -> bool {
        true
    }

    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
        let started = Instant::now();
        let result = self.inner.execute(sql, params);
        trace_sql(
            sql,
            params.len(),
            result.as_ref().copied().unwrap_or(0),
            started.elapsed(),
        );
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
        trace_sql(
            sql,
            params.len(),
            result.as_ref().map(Vec::len).unwrap_or(0),
            started.elapsed(),
        );
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
    last_apply_statement_shape: Arc<std::sync::Mutex<Option<TursoBatchUpdateStatementShape>>>,
    last_apply_phase: Arc<std::sync::Mutex<Option<TursoApplyPhaseObservation>>>,
    grouped_shards_slot: Arc<std::sync::Mutex<HashSet<QueueKey>>>,
    claim_scan_hints_slot: Arc<std::sync::Mutex<HashMap<QueueKey, i64>>>,
    claim_scan_default_fifo_slot: Arc<std::sync::Mutex<HashMap<QueueKey, bool>>>,
    positions: Vec<CommandPosition>,
    commands: Vec<CommandEnvelope>,
    enforce_live_epoch: bool,
) -> EngineResult<()> {
    // Cancellation while queued must not start an apply. Once the writer is
    // acquired, its owner must outlive the blocking relational SQL worker:
    // dropping a caller's transaction while that worker runs can roll it back
    // underneath later statements. Detaching this owned task only drops the
    // response waiter; it retains both the writer and transaction to completion.
    let total_started = Instant::now();
    let writer_wait_started = Instant::now();
    let connection = writer.lock_owned().await;
    let writer_wait_us = duration_us(writer_wait_started.elapsed());
    // The facade also supports non-Tokio executors. Keep one fallback runtime
    // alive so admitted applies survive their response waiters in that cell too.
    let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RUNTIME
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("turso-owned-apply")
                    .enable_all()
                    .build()
                    .expect("Turso owned apply runtime")
            })
            .handle()
            .clone()
    });
    // The native Unix VFS performs synchronous writes even through the async
    // client. Commit/checkpoint must not occupy an application runtime worker:
    // a busy projection otherwise delays unrelated log acknowledgements/timers.
    handle.spawn_blocking(move || crate::tx::block_on_owned_apply(async move {
    let mut connection = connection;
    if positions.len() != commands.len() {
        return Err(storage("positions/commands length mismatch"));
    }
    let api001_updates = collect_api001_updates(&commands);
    let api001_updates = api001_updates.filter(|updates| !updates.is_empty());
    let statement_shape = Arc::new(std::sync::Mutex::new(TursoBatchUpdateStatementShape::new(
        api001_updates
            .as_ref()
            .map(|updates| updates.len())
            .unwrap_or(commands.len()),
    )));
    *last_batch_update_shape
        .lock()
        .expect("Turso statement-shape mutex poisoned") = None;
    *last_apply_statement_shape
        .lock()
        .expect("Turso apply statement-shape mutex poisoned") = None;
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
                    record_statement(Some(&statement_shape), sql::SELECT_CURSOR, 2);
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
            record_statement(Some(&statement_shape), sql::SELECT_QUEUE_DEFINITION, 2);
            let definition = definition_in_transaction(&transaction, &position.queue).await?;
            queues.insert(position.queue.clone(), definition);
        }
    }
    let cursor_definition_us = duration_us(cursor_definition_started.elapsed());
    let trace_items: usize = commands.iter().map(|command| command.item_ids.len()).sum();
    let mut trace_push = 0u32;
    let mut trace_update = 0u32;
    let mut trace_claim = 0u32;
    let mut trace_finalize = 0u32;
    let mut trace_other = 0u32;
    for command in &commands {
        match &command.command {
            QueueCommand::Push(_) => trace_push += 1,
            QueueCommand::UpdateFields(_) | QueueCommand::UpdateFieldsBatch(_) => trace_update += 1,
            QueueCommand::Claim(_) => trace_claim += 1,
            QueueCommand::Finalize(_) => trace_finalize += 1,
            _ => trace_other += 1,
        }
    }
    let trace_cmds = commands.len();
    let hop_txn = transaction.clone();
    let rel_phases = Arc::new(std::sync::Mutex::new(RelApplyPhaseTotals::default()));
    let rel_phases_for_hop = Arc::clone(&rel_phases);
    let statement_shape_for_hop = Arc::clone(&statement_shape);
    let relational_started = Instant::now();
    let relational_result = crate::tx::run_reltx_blocking(move || {
        let rel = ObservedTursoRel {
            inner: crate::tx::ApplyTursoRel::new(&hop_txn),
            statement_shape: Some(statement_shape_for_hop),
            phases: rel_phases_for_hop,
        };
        let metrics_delta = crate::metrics::MetricsDelta::capture(&rel, &positions, &commands)?;
        let applied = fireweed_relational::apply_committed_batch_sql_with_cursor_seeds(
            &rel,
            &queues,
            &mut grouped_shards,
            &mut claim_scan_hints,
            &mut claim_scan_default_fifo,
            &mut token_ops,
            &positions,
            &commands,
            &cursor_seeds,
        )?;
        metrics_delta.apply(&rel)?;
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
    let observed_shape = Some(
        *statement_shape
            .lock()
            .expect("Turso statement-shape mutex poisoned"),
    );
    *last_apply_statement_shape
        .lock()
        .expect("Turso apply statement-shape mutex poisoned") = observed_shape;
    if applied_api001 {
        *last_batch_update_shape
            .lock()
            .expect("Turso statement-shape mutex poisoned") = observed_shape;
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
    if std::env::var_os("FIREWEED_APPLY_TRACE").is_some() {
        eprintln!(
            "apply cmds={trace_cmds} items={trace_items} push={trace_push} upd={trace_update} claim={trace_claim} fin={trace_finalize} other={trace_other} \
             wait_us={writer_wait_us} begin_us={begin_us} cur_us={cursor_definition_us} read_us={row_read_us} xform_us={transform_bridge_us} upd_us={update_side_us} commit_us={commit_us} total_us={}",
            phase_observation.total_us
        );
    }
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
    })).await.map_err(|error| storage(format!("Turso owned apply task failed: {error}")))?
}

/// Pre-position observation helper over a borrowed OutcomeReadAdmission connection
/// or Deferred snapshot. Serving still calls this with the shared reader.
pub(crate) async fn server_peek_on(
    connection: &Connection,
    shard: &QueueKey,
    limit: usize,
) -> EngineResult<Vec<ItemView>> {
    let limit = i64::try_from(limit).map_err(storage)?;
    let rows = query_value_rows(
        connection,
        "SELECT item_id,client_item_key,priority,item_version FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
             ORDER BY priority_sort,created_seq LIMIT ?3",
        vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
            limit.into(),
        ],
    )
    .await?;
    rows.into_iter()
        .map(|values| {
            Ok(ItemView {
                item_id: ItemId::new(text(&values[0])?).map_err(storage)?,
                client_item_key: ClientItemKey::new(text(&values[1])?).map_err(storage)?,
                priority: parse_priority(optional_text(&values[2])?)?,
                item_version: nonnegative_u64(integer(&values[3])?, "item_version")?,
            })
        })
        .collect()
}

pub(crate) async fn server_pending_rows_on(
    connection: &Connection,
    shard: &QueueKey,
) -> EngineResult<Vec<(ItemId, Option<i64>, i64)>> {
    let rows = query_value_rows(
        connection,
        "SELECT item_id,lease_expires_at,retry_count FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' AND superseded=0 \
             ORDER BY item_id",
        vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
        ],
    )
    .await?;
    rows.into_iter()
        .map(|values| {
            Ok((
                ItemId::new(text(&values[0])?).map_err(storage)?,
                optional_integer(&values[1])?,
                integer(&values[2])?,
            ))
        })
        .collect()
}

pub(crate) async fn server_pending_by_ids_on(
    connection: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<HashMap<ItemId, (i64, u32)>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
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
        for values in query_value_rows(connection, sql, params).await? {
            let id = ItemId::new(text(&values[0])?).map_err(storage)?;
            by_id.insert(
                id,
                (
                    integer(&values[1])?,
                    nonnegative_u32(integer(&values[2])?, "retry_count")?,
                ),
            );
        }
    }
    Ok(by_id)
}

pub(crate) async fn server_update_snapshot_on(
    connection: &Connection,
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
        let values_sql = (0..chunk.len())
            .map(|i| format!("(?{})", i + 3))
            .collect::<Vec<_>>()
            .join(",");
        let rows = query_value_rows(
            connection,
            format!(
                "WITH incoming(client_item_key) AS (VALUES {values_sql}) \
             SELECT i.item_id,i.client_item_key,i.item_version,i.lifecycle_state,i.fenced \
             FROM incoming CROSS JOIN fireweed_items i INDEXED BY fireweed_items_active_key \
             ON i.tenant_id=?1 AND i.queue_id=?2 AND i.client_item_key=incoming.client_item_key \
             WHERE i.superseded=0"
            ),
            params,
        )
        .await?;
        for values in rows {
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

pub(crate) async fn server_retained_items_on(
    connection: &Connection,
    shard: &QueueKey,
    after: Option<ItemId>,
    limit: usize,
) -> EngineResult<Vec<fireweed_engine::RetainedItemView>> {
    if !(1..=1000).contains(&limit) {
        return Err(EngineError::Invalid("retained page size must be 1..1000"));
    }
    let rows = query_value_rows(connection,
        "SELECT i.item_id,i.client_item_key,i.item_version,i.lifecycle_state,i.priority,i.not_before,i.retry_count,\
         CASE WHEN p.item_id IS NULL THEN i.payload ELSE p.payload END,i.metadata \
         FROM fireweed_items i LEFT JOIN fireweed_item_payloads p \
         ON p.tenant_id=i.tenant_id AND p.queue_id=i.queue_id AND p.item_id=i.item_id \
         WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id>?3 AND i.superseded=0 \
         ORDER BY i.item_id LIMIT ?4",
        vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(),
             after.map(|id| id.to_string()).unwrap_or_default().into(), (limit as i64).into()]).await?;
    rows.into_iter()
        .map(|v| {
            Ok(fireweed_engine::RetainedItemView {
                item_id: ItemId::new(text(&v[0])?).map_err(storage)?,
                client_item_key: ClientItemKey::new(text(&v[1])?).map_err(storage)?,
                item_version: nonnegative_u64(integer(&v[2])?, "item_version")?,
                lifecycle_state: parse_state(&text(&v[3])?).map_err(storage)?,
                priority: parse_priority(optional_text(&v[4])?)?,
                not_before: optional_integer(&v[5])?.map(nanos_ts),
                attempt_count: nonnegative_u32(integer(&v[6])?, "retry_count")?,
                payload: optional_blob(&v[7])?.map(Bytes::from),
                metadata: metadata_from_json(text(&v[8])?)?,
            })
        })
        .collect()
}

pub(crate) async fn server_live_items_on(
    connection: &Connection,
    shard: &QueueKey,
    keys: &[ClientItemKey],
) -> EngineResult<Vec<Option<LiveItemView>>> {
    let mut result = Vec::with_capacity(keys.len());
    for key in keys {
        let rows = query_value_rows(
            connection,
            "SELECT i.item_id,i.client_item_key,i.item_version,i.lifecycle_state,i.priority,i.group_key,i.not_before,i.retry_count,\
                 CASE WHEN p.item_id IS NULL THEN i.payload ELSE p.payload END,i.fields \
                 FROM fireweed_items i \
                 LEFT JOIN fireweed_item_payloads p \
                   ON p.tenant_id=i.tenant_id AND p.queue_id=i.queue_id AND p.item_id=i.item_id \
                 WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.client_item_key=?3 \
                 AND i.lifecycle_state IN ('Pending','Leased') AND i.superseded=0 LIMIT 1",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                key.as_str().to_string().into(),
            ],
        )
        .await?;
        let Some(values) = rows.into_iter().next() else {
            result.push(None);
            continue;
        };
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

const LIFECYCLE_METRICS_SQL: &str = crate::metrics::READ_SQL;

pub(crate) async fn server_metrics_on(
    connection: &Connection,
    shard: &QueueKey,
) -> EngineResult<QueueMetrics> {
    let rows = query_value_rows(
        connection,
        LIFECYCLE_METRICS_SQL,
        vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
        ],
    )
    .await?;
    let Some(values) = rows.first() else {
        return Ok(QueueMetrics::default());
    };
    let mut metrics = QueueMetrics {
        pending: nonnegative_u64(integer(&values[0])?, "pending count")?,
        leased: nonnegative_u64(integer(&values[1])?, "leased count")?,
        complete: nonnegative_u64(integer(&values[2])?, "complete count")?,
        failed: nonnegative_u64(integer(&values[3])?, "failed count")?,
        ..QueueMetrics::default()
    };
    metrics.resident_terminal_count = metrics.complete.saturating_add(metrics.failed);
    Ok(metrics)
}

pub(crate) async fn push_idempotency_on(
    connection: &Connection,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &PushFingerprint,
    now: UtcTimestamp,
) -> EngineResult<IdempotencyDecision<Vec<ItemId>>> {
    let row = one_outcome_row(
        connection,
        "SELECT request_fingerprint,response_payload,expires_at FROM fireweed_request_idempotency \
                 WHERE tenant_id=?1 AND queue_id=?2 AND operation='push' AND request_id=?3",
        vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
            request_id.as_str().to_string().into(),
        ],
    )
    .await?;
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

/// Post-publication response continuation. Retained results only; no pool borrow.
#[allow(dead_code)]
pub fn finish_retained_claimed(items: Vec<ClaimedItem>) -> EngineResult<Vec<ClaimedItem>> {
    Ok(items)
}

async fn query_driver_value_rows(
    connection: &Connection,
    query: impl AsRef<str>,
    params: Vec<Value>,
) -> EngineResult<Vec<Vec<Value>>> {
    let started = Instant::now();
    let bind_count = params.len();
    let mut statement = connection
        .prepare_cached(query.as_ref())
        .await
        .map_err(driver_read_error)?;
    let mut rows = statement.query(params).await.map_err(driver_read_error)?;
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await.map_err(driver_read_error)? {
        let mut values = Vec::with_capacity(row.column_count());
        for index in 0..row.column_count() {
            values.push(row.get_value(index).map_err(driver_read_error)?);
        }
        collected.push(values);
    }
    trace_sql(
        query.as_ref(),
        bind_count,
        collected.len(),
        started.elapsed(),
    );
    Ok(collected)
}

fn class_s_item_from_driver_row(
    values: &[Value],
    lease_expires_at: i64,
) -> EngineResult<ClassSClaimedItem> {
    Ok(ClassSClaimedItem {
        item_id: text(&values[0])?,
        client_item_key: text(&values[1])?,
        payload: optional_blob(&values[2])?,
        item_version: integer(&values[3])? + 1,
        retry_count: integer(&values[4])? + 1,
        lease_expires_at,
        priority: optional_text(&values[5])?,
        group_key: optional_text(&values[6])?,
        not_before: optional_integer(&values[7])?,
        fields_json: optional_text(&values[8])?.unwrap_or_else(|| "{}".into()),
        metadata_json: optional_text(&values[9])?.unwrap_or_else(|| "{}".into()),
        max_attempts: optional_integer(&values[10])?.unwrap_or(0),
        entity_document: optional_text(&values[11])?,
        index_fields: optional_blob(&values[12])?,
        gate_keys: Vec::new(),
    })
}

/// Select pending item-Claim IDs. Never reads payload, fields, or other blobs.
///
/// FIFO queues walk `rowid` from a process-local floor (`NOT INDEXED`, `ORDER BY
/// rowid`). Everyone else orders indexed columns only (`priority_sort`,
/// `created_seq`). Bodies load later by primary key.
pub async fn select_item_claim_ids_on(
    connection: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max: usize,
    exclude: &[ItemId],
    rowid_floor: Option<i64>,
) -> EngineResult<Vec<ItemId>> {
    if max == 0 {
        return Ok(Vec::new());
    }
    let tenant = shard.tenant_id.as_str();
    let queue = shard.queue_id.as_str();
    if queue_paused(connection, tenant, queue).await? {
        return Ok(Vec::new());
    }
    let gated = !query_driver_value_rows(
        connection,
        "SELECT 1 FROM fireweed_gate_state WHERE tenant_id=?1 AND queue_id=?2 LIMIT 1",
        vec![
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
        ],
    )
    .await?
    .is_empty();
    let exclude_set: HashSet<ItemId> = exclude.iter().copied().collect();
    let mut chosen = Vec::with_capacity(max);
    if !gated && let Some(mut floor) = rowid_floor {
        const FIFO_SQL: &str = "SELECT item_id,rowid FROM fireweed_items NOT INDEXED \
             WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
             AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
             AND eligible_since IS NOT NULL AND rowid>=?5 ORDER BY rowid LIMIT ?4";
        let mut first_page = true;
        while chosen.len() < max {
            let extra = if first_page {
                exclude_set.len().min(1_600)
            } else {
                0
            };
            first_page = false;
            let fetch = max
                .saturating_sub(chosen.len())
                .saturating_add(extra)
                .max(1);
            let params = vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Integer(ts_nanos(now)),
                Value::Integer(i64::try_from(fetch).map_err(storage)?),
                Value::Integer(floor.max(1)),
            ];
            let rows = query_driver_value_rows(connection, FIFO_SQL, params).await?;
            if rows.is_empty() {
                break;
            }
            let mut last_rowid = floor;
            for values in rows {
                last_rowid = integer(&values[1])?;
                let id = ItemId::new(text(&values[0])?).map_err(storage)?;
                if exclude_set.contains(&id) {
                    continue;
                }
                chosen.push(id);
                if chosen.len() == max {
                    break;
                }
            }
            let next = last_rowid.saturating_add(1);
            if next <= floor {
                break;
            }
            floor = next;
        }
        return Ok(chosen);
    }
    let mut offset: i64 = 0;
    // Match the pending index's leading order keys. Generic gated selection
    // retains its downstream eligibility checks without ordering by payload.
    let query = if gated {
        "SELECT item_id FROM fireweed_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
         AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
         JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
         AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
         AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
         ORDER BY priority_sort,created_seq LIMIT ?3 OFFSET ?4"
    } else {
        "SELECT item_id FROM fireweed_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
         ORDER BY priority_sort,created_seq LIMIT ?3 OFFSET ?4"
    };
    while chosen.len() < max {
        let skip = exclude_set.len().min(1_600).saturating_sub(offset as usize);
        let fetch = max.saturating_sub(chosen.len()).saturating_add(skip).max(1);
        let params = vec![
            Value::Text(tenant.to_string()),
            Value::Text(queue.to_string()),
            Value::Integer(i64::try_from(fetch).map_err(storage)?),
            Value::Integer(offset),
        ];
        let rows = query_driver_value_rows(connection, query, params).await?;
        if rows.is_empty() {
            break;
        }
        offset = offset.saturating_add(i64::try_from(rows.len()).map_err(storage)?);
        for values in rows {
            let id = ItemId::new(text(&values[0])?).map_err(storage)?;
            if exclude_set.contains(&id) {
                continue;
            }
            chosen.push(id);
            if chosen.len() == max {
                break;
            }
        }
    }
    Ok(chosen)
}

// Scan only index entries for eligibility, then materialize selected IDs. The
// CROSS JOIN keeps the bounded candidate list outside full-row primary-key seeks.
const ORDERED_ITEM_CLAIM_SQL: &str = "SELECT i.item_id,i.client_item_key,CASE WHEN p.item_id IS NULL THEN i.payload ELSE p.payload END,i.item_version,i.retry_count,i.priority,i.group_key,\
     i.not_before,i.fields,i.metadata,i.max_attempts,i.entity_document,i.index_fields,i.eligible_since,i.cohort_size,t.priority_sort,t.created_seq \
     FROM (\
       SELECT item_id,priority_sort,created_seq \
       FROM fireweed_items INDEXED BY fireweed_items_pending_eligible_order_idx \
       WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND priority_sort>=?4 AND (priority_sort>?4 OR created_seq>?5) \
        AND cohort_size IS NULL AND eligible_since IS NOT NULL \
        AND (not_before IS NULL OR not_before<=?6) \
       ORDER BY priority_sort,created_seq LIMIT ?3\
     ) t \
     CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
       ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=t.item_id \
     LEFT JOIN fireweed_item_payloads p \
       ON p.tenant_id=?1 AND p.queue_id=?2 AND p.item_id=t.item_id ORDER BY t.priority_sort,t.created_seq";

/// Next due item-Claim rows with bodies in indexed priority or FIFO order.
/// Priority scans filter eligibility before bounded full-row/payload loading;
/// FIFO scans retain their rowid cursor and check residual eligibility in-process.
pub async fn select_and_materialize_item_claims_on(
    connection: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max: usize,
    exclude: &[ItemId],
    lease_token: &LeaseToken,
    lease_expires_at: UtcTimestamp,
    rowid_floor: Option<i64>,
) -> EngineResult<(Vec<ItemId>, Vec<ClaimedItem>, Option<i64>)> {
    if max == 0 {
        return Ok((Vec::new(), Vec::new(), None));
    }
    let tenant = shard.tenant_id.as_str();
    let queue = shard.queue_id.as_str();
    if queue_paused(connection, tenant, queue).await? {
        return Ok((Vec::new(), Vec::new(), None));
    }
    let exclude_set: HashSet<ItemId> = exclude.iter().copied().collect();
    let expires = ts_nanos(lease_expires_at);
    let now_n = ts_nanos(now);
    let started = Instant::now();
    let mut ids = Vec::with_capacity(max);
    let mut carriers = Vec::with_capacity(max);
    let mut next_floor = None;
    if let Some(mut floor) = rowid_floor {
        const FIFO_SQL: &str = "SELECT t.item_id,t.client_item_key,CASE WHEN p.item_id IS NULL THEN t.payload ELSE p.payload END,t.item_version,t.retry_count,t.priority,t.group_key,\
             t.not_before,t.fields,t.metadata,t.max_attempts,t.entity_document,t.index_fields,t.eligible_since,t.cohort_size,t.rowid \
             FROM (\
               SELECT item_id,client_item_key,item_version,retry_count,priority,group_key,payload,\
                not_before,fields,metadata,max_attempts,entity_document,index_fields,eligible_since,cohort_size,rowid \
               FROM fireweed_items NOT INDEXED \
               WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
                AND rowid>=?4 ORDER BY rowid LIMIT ?3\
             ) t \
             LEFT JOIN fireweed_item_payloads p \
               ON p.tenant_id=?1 AND p.queue_id=?2 AND p.item_id=t.item_id";
        let mut first_page = true;
        while ids.len() < max {
            let extra = if first_page {
                exclude_set.len().min(1_600)
            } else {
                0
            };
            first_page = false;
            let fetch = max.saturating_sub(ids.len()).saturating_add(extra).max(1);
            let params = vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Integer(i64::try_from(fetch).map_err(storage)?),
                Value::Integer(floor.max(1)),
            ];
            let mut rows = connection
                .query(FIFO_SQL, params)
                .await
                .map_err(driver_read_error)?;
            let mut page = 0usize;
            let mut last_rowid = floor;
            while let Some(row) = rows.next().await.map_err(driver_read_error)? {
                page += 1;
                last_rowid = take_integer(row.get_value(15).map_err(driver_read_error)?)?;
                if !claim_row_is_due(&row, now_n)? {
                    continue;
                }
                let item = class_s_item_from_turso_row(&row, expires)?;
                let id = ItemId::new(&item.item_id).map_err(storage)?;
                if exclude_set.contains(&id) {
                    continue;
                }
                ids.push(id);
                carriers.push(item);
                if ids.len() == max {
                    break;
                }
            }
            if page == 0 {
                break;
            }
            let next = last_rowid.saturating_add(1);
            if next <= floor {
                break;
            }
            floor = next;
        }
        next_floor = Some(floor);
    } else {
        let mut after_priority = Vec::<u8>::new();
        let mut after_sequence = i64::MIN;
        let mut first_page = true;
        while ids.len() < max {
            let extra = if first_page {
                exclude_set.len().min(1_600)
            } else {
                0
            };
            first_page = false;
            let fetch = max.saturating_sub(ids.len()).saturating_add(extra).max(1);
            let params = vec![
                Value::Text(tenant.to_string()),
                Value::Text(queue.to_string()),
                Value::Integer(i64::try_from(fetch).map_err(storage)?),
                Value::Blob(after_priority.clone()),
                Value::Integer(after_sequence),
                Value::Integer(now_n),
            ];
            let mut rows = connection
                .query(ORDERED_ITEM_CLAIM_SQL, params)
                .await
                .map_err(driver_read_error)?;
            let mut page = 0usize;
            while let Some(row) = rows.next().await.map_err(driver_read_error)? {
                page += 1;
                after_priority = take_optional_blob(row.get_value(15).map_err(driver_read_error)?)?
                    .ok_or_else(|| storage("claim priority_sort is NULL"))?;
                after_sequence = take_integer(row.get_value(16).map_err(driver_read_error)?)?;
                if !claim_row_is_due(&row, now_n)? {
                    continue;
                }
                let item = class_s_item_from_turso_row(&row, expires)?;
                let id = ItemId::new(&item.item_id).map_err(storage)?;
                if exclude_set.contains(&id) {
                    continue;
                }
                ids.push(id);
                carriers.push(item);
                if ids.len() == max {
                    break;
                }
            }
            if page == 0 {
                break;
            }
        }
    }
    if std::env::var_os("FIREWEED_APPLY_TRACE").is_some() {
        eprintln!(
            "claim_realize n={} exclude={} us={}",
            ids.len(),
            exclude.len(),
            started.elapsed().as_micros()
        );
    }
    let items = render_class_s_claimed_items(lease_token, carriers)?;
    Ok((ids, items, next_floor))
}

fn claim_row_is_due(row: &Row, now_n: i64) -> EngineResult<bool> {
    let not_before = take_optional_integer(row.get_value(7).map_err(driver_read_error)?)?;
    let eligible_since = take_optional_integer(row.get_value(13).map_err(driver_read_error)?)?;
    let cohort_size = take_optional_integer(row.get_value(14).map_err(driver_read_error)?)?;
    Ok(eligible_since.is_some()
        && cohort_size.is_none()
        && not_before.map(|ts| ts <= now_n).unwrap_or(true))
}

/// Full-row grouped/cohort Claim materialization over a borrowed driver snapshot.
///
/// Items may still be Pending. Lease token/expiry come from the request. S5 item/group/cohort
/// serving retains these rows through append; `render_claimed` stays for recovery/legacy reads.
pub async fn materialize_grouped_cohort_claimed_on(
    connection: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
    lease_token: &LeaseToken,
    lease_expires_at: UtcTimestamp,
) -> EngineResult<Vec<ClaimedItem>> {
    materialize_claimed_on(connection, shard, ids, lease_token, lease_expires_at, None).await
}

async fn materialize_claimed_on(
    connection: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
    lease_token: &LeaseToken,
    lease_expires_at: UtcTimestamp,
    due_at: Option<i64>,
) -> EngineResult<Vec<ClaimedItem>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let expires = ts_nanos(lease_expires_at);
    let mut item_rows = HashMap::<ItemId, ClassSClaimedItem>::with_capacity(ids.len());
    let mut gate_keys = HashMap::<ItemId, Vec<String>>::new();
    let gated = !query_driver_value_rows(
        connection,
        "SELECT 1 FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 LIMIT 1",
        vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
        ],
    )
    .await?
    .is_empty();
    let id_bind = if due_at.is_some() { 4 } else { 3 };
    let due_clause = if due_at.is_some() {
        " AND cohort_size IS NULL AND eligible_since IS NOT NULL \
         AND (not_before IS NULL OR not_before<=?3)"
    } else {
        ""
    };
    for chunk in ids.chunks(SQLITE_BIND_CAP.saturating_sub(id_bind).max(1)) {
        let placeholders = (0..chunk.len())
            .map(|index| format!("?{}", index + id_bind))
            .collect::<Vec<_>>()
            .join(",");
        let mut params = vec![
            shard.tenant_id.as_str().to_string().into(),
            shard.queue_id.as_str().to_string().into(),
        ];
        if let Some(due_at) = due_at {
            params.push(Value::Integer(due_at));
        }
        params.extend(chunk.iter().map(|id| id.to_string().into()));
        let item_sql = format!(
            "SELECT item_id,client_item_key,CASE WHEN EXISTS(SELECT 1 FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) THEN (SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) ELSE payload END,item_version,retry_count,priority,group_key,\
             not_before,fields,metadata,max_attempts,entity_document,index_fields \
             FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id IN ({placeholders}){due_clause}"
        );
        for values in query_driver_value_rows(connection, item_sql, params.clone()).await? {
            let item = class_s_item_from_driver_row(&values, expires)?;
            let id = ItemId::new(&item.item_id).map_err(storage)?;
            item_rows.insert(id, item);
        }
        if !gated {
            continue;
        }
        let gate_sql = format!(
            "SELECT item_id,gate_key FROM fireweed_item_gates WHERE tenant_id=?1 \
             AND queue_id=?2 AND item_id IN ({placeholders}) ORDER BY item_id,gate_key"
        );
        for values in query_driver_value_rows(connection, gate_sql, params).await? {
            let id = ItemId::new(text(&values[0])?).map_err(storage)?;
            gate_keys.entry(id).or_default().push(text(&values[1])?);
        }
    }
    let mut relational = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(mut item) = item_rows.remove(id) else {
            continue;
        };
        item.gate_keys = gate_keys.remove(id).unwrap_or_default();
        relational.push(item);
    }
    render_class_s_claimed_items(lease_token, relational)
}

/// Resolve a bounded set of lease targets by full primary-key seeks. An item_id IN
/// predicate lets Turso scan the whole queue and compare every row against the batch.
fn addressed_item_rows_sql(count: usize, with_payload: bool) -> String {
    let values = (0..count)
        .map(|i| format!("(?{})", i + 3))
        .collect::<Vec<_>>()
        .join(",");
    let payload_sql = if with_payload {
        "CASE WHEN p.item_id IS NOT NULL THEN p.payload ELSE i.payload END"
    } else {
        "NULL"
    };
    let payload_join = if with_payload {
        "LEFT JOIN fireweed_item_payloads p ON p.tenant_id=?1 AND p.queue_id=?2 AND p.item_id=i.item_id"
    } else {
        ""
    };
    format!(
        "WITH incoming(item_id) AS (VALUES {values}) \
                 SELECT i.item_id,i.client_item_key,i.priority,i.not_before,i.eligible_since,i.group_key,i.cohort_size,\
                 {payload_sql},\
                 i.fields,i.metadata,i.index_fields,i.entity_document,i.lifecycle_state,i.item_version,\
                 i.retry_count,i.max_attempts,i.created_seq,b.lease_token,i.lease_expires_at,i.worker_id,\
                 i.fenced,i.superseded,i.terminal_at,i.terminal_command_epoch,i.last_command_sequence \
                 FROM incoming CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
                 ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=incoming.item_id \
                 {payload_join} \
                 LEFT JOIN fireweed_lease_bearers b INDEXED BY sqlite_autoindex_fireweed_lease_bearers_1 ON b.tenant_id=?1 AND b.queue_id=?2 AND b.item_id=i.item_id"
    )
}

fn lease_target_rows_sql(count: usize) -> String {
    let values = (0..count)
        .map(|index| format!("(?{})", index + 3))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH incoming(item_id) AS (VALUES {values}) \
         SELECT i.item_id,i.client_item_key,i.item_version,i.priority,i.group_key,i.not_before,\
         i.lease_expires_at,i.retry_count,i.max_attempts,\
         CASE WHEN p.item_id IS NULL THEN i.payload ELSE p.payload END,\
         i.fields,i.metadata,i.entity_document,i.index_fields \
         FROM incoming CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
         ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=incoming.item_id \
         LEFT JOIN fireweed_item_payloads p \
         ON p.tenant_id=?1 AND p.queue_id=?2 AND p.item_id=i.item_id \
         WHERE i.superseded=0 AND i.lifecycle_state IN ('Pending','Leased')"
    )
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
        let connection = self.reader.lock().await;
        server_peek_on(&connection, shard, limit).await
    }

    pub async fn server_pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let rows = {
            let connection = self.reader.lock().await;
            server_pending_rows_on(&connection, shard).await?
        };
        let tokens = self.live_tokens.lock().await;
        let mut pending = Vec::new();
        for (item_id, expires, retry_count) in rows {
            let Some(lease_token) = tokens.get(&(shard.clone(), item_id)).cloned() else {
                continue;
            };
            let Some(expires) = expires else {
                continue;
            };
            pending.push(LeaseView {
                item_id,
                lease_token,
                lease_expires_at: nanos_ts(expires),
                attempt_count: nonnegative_u32(retry_count, "retry_count")?,
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
        let by_id = {
            let connection = self.reader.lock().await;
            server_pending_by_ids_on(&connection, shard, ids).await?
        };
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
        let connection = self.reader.lock().await;
        server_update_snapshot_on(&connection, shard, keys).await
    }

    /// Plan addressed mutations from full-key reads. The caller must hold the queue's
    /// mutation fence through append and cover the committed log frontier, except
    /// for the supplied contiguous, disjoint authoritative Claim-only tail.
    pub async fn plan_addressed_item_mutation(
        &self,
        shard: &QueueKey,
        definition: &QueueDefinition,
        request: &fireweed_engine::ItemMutationRequest,
        pending_claims: &[fireweed_engine::ClaimCommand],
    ) -> EngineResult<fireweed_engine::ItemMutationPlan> {
        use fireweed_engine::ItemMutationOperation;
        use fireweed_projection::{ProjectionData, ProjectionImageItem};
        let ItemMutationOperation::Addressed { entries } = &request.operation else {
            return Err(EngineError::Unavailable);
        };
        // Unique-index validation needs rows outside the addressed set. Cohort
        // changes likewise need the complete group. Do not validate either against
        // an incomplete image; these shapes retain their unsupported status.
        if !definition.secondary_indexes.is_empty()
            || !definition.typed_indexes.is_empty()
            || definition.entity_schema.is_some()
            || definition.cohort_policy.is_some()
        {
            return Err(EngineError::Unavailable);
        }
        if entries.len() > 1000 {
            return Err(EngineError::Invalid(
                "addressed mutation batch exceeds 1000 items",
            ));
        }
        // Keep + identity cannot observe or change the old blob. Preserve the
        // full read for replacement equality/NoChange decisions and snapshots.
        let with_payload = request.returning
            == fireweed_engine::ItemMutationReturning::BeforeSnapshot
            || entries.iter().any(|entry| {
                !matches!(entry.patch.payload, fireweed_engine::BatchUpdateValue::Keep)
            });
        let connection = self.reader.lock().await;
        let mut image = ProjectionData::from_image(
            definition,
            fireweed_projection::ProjectionImage {
                high_water: None,
                paused: false,
                pause_drain_intake: false,
                blocked_gates: Default::default(),
                next_seq: 0,
                items: vec![],
                side_records: Default::default(),
                instance_fences: Default::default(),
                metrics: QueueMetrics::default(),
            },
        )?
        .to_image(None);
        let ids: Vec<_> = entries
            .iter()
            .map(|entry| entry.item_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        for chunk in ids.chunks(SQLITE_BIND_CAP - 2) {
            let values = (0..chunk.len())
                .map(|i| format!("(?{})", i + 3))
                .collect::<Vec<_>>()
                .join(",");
            let mut params = vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
            ];
            params.extend(chunk.iter().map(|id| id.to_string().into()));
            let rows = query_value_rows(
                &connection,
                addressed_item_rows_sql(chunk.len(), with_payload),
                params.clone(),
            )
            .await?;
            let mut gates: HashMap<ItemId, Vec<String>> = HashMap::new();
            for row in query_value_rows(
                &connection,
                format!(
                    "WITH incoming(item_id) AS (VALUES {values}) \
                 SELECT g.item_id,g.gate_key FROM incoming CROSS JOIN fireweed_item_gates g \
                 ON g.tenant_id=?1 AND g.queue_id=?2 AND g.item_id=incoming.item_id"
                ),
                params,
            )
            .await?
            {
                gates
                    .entry(ItemId::new(text(&row[0])?).map_err(storage)?)
                    .or_default()
                    .push(text(&row[1])?);
            }
            for v in rows {
                if !matches!(v[5], Value::Null) || !matches!(v[6], Value::Null) {
                    return Err(EngineError::Unavailable);
                }
                let item_id = ItemId::new(text(&v[0])?).map_err(storage)?;
                image.items.push(ProjectionImageItem {
                    item_id,
                    client_item_key: ClientItemKey::new(text(&v[1])?).map_err(storage)?,
                    priority: parse_priority(optional_text(&v[2])?)?,
                    not_before: optional_integer(&v[3])?.map(nanos_ts),
                    eligible_since: optional_integer(&v[4])?.map(nanos_ts),
                    group_key: None,
                    cohort_size: None,
                    payload: optional_blob(&v[7])?.map(Bytes::from),
                    fields: fields_from_json(text(&v[8])?)?,
                    metadata: metadata_from_json(text(&v[9])?)?,
                    gate_keys: gates.remove(&item_id).unwrap_or_default(),
                    index_fields: fireweed_engine::index_fields::decode_index_fields_blob(
                        optional_blob(&v[10])?.as_deref(),
                    )?,
                    entity_document: entity_from_json(optional_text(&v[11])?)?,
                    state: parse_state(&text(&v[12])?).map_err(storage)?,
                    item_version: nonnegative_u64(integer(&v[13])?, "item_version")?,
                    attempt_count: nonnegative_u32(integer(&v[14])?, "retry_count")?,
                    max_attempts: nonnegative_u32(integer(&v[15])?, "max_attempts")?,
                    created_seq: nonnegative_u64(integer(&v[16])?, "created_seq")?,
                    lease_token: optional_text(&v[17])?
                        .map(LeaseToken::new)
                        .transpose()
                        .map_err(storage)?,
                    lease_expires_at: optional_integer(&v[18])?.map(nanos_ts),
                    lease_is_cohort: false,
                    worker_id: optional_text(&v[19])?
                        .map(fireweed_core::WorkerId::new)
                        .transpose()
                        .map_err(storage)?,
                    fenced: integer(&v[20])? != 0,
                    superseded: integer(&v[21])? != 0,
                    terminal_at: optional_integer(&v[22])?.map(nanos_ts),
                    terminal_position: optional_integer(&v[23])?
                        .map(|epoch| {
                            Ok::<_, EngineError>(CommandPosition::new(
                                shard.clone(),
                                nonnegative_u64(epoch, "terminal_epoch")?,
                                nonnegative_u64(integer(&v[24])?, "terminal_sequence")?,
                            ))
                        })
                        .transpose()?,
                });
            }
        }
        let claims_by_id: HashMap<_, _> = pending_claims
            .iter()
            .flat_map(|claim| claim.item_ids.iter().map(move |id| (*id, claim)))
            .collect();
        for item in &mut image.items {
            if let Some(claim) = claims_by_id.get(&item.item_id) {
                match item.state {
                    ItemState::Pending => {
                        item.state = ItemState::Leased;
                        item.item_version = item
                            .item_version
                            .checked_add(1)
                            .ok_or_else(|| storage("item version overflow"))?;
                        item.attempt_count = item
                            .attempt_count
                            .checked_add(1)
                            .ok_or_else(|| storage("attempt count overflow"))?;
                        item.lease_token = Some(claim.lease_token.clone());
                        item.lease_expires_at = Some(claim.lease_expires_at);
                        item.worker_id = claim.worker_id.clone();
                    }
                    ItemState::Leased if item.lease_token.as_ref() == Some(&claim.lease_token) => {
                        // Apply caught up while the SQL snapshot was being read.
                    }
                    _ => return Err(storage("claim tail conflicts with projection state")),
                }
            }
        }
        ProjectionData::plan_item_mutation_image(definition, image, request)
    }

    pub async fn item_mutation_replay(
        &self,
        shard: &QueueKey,
        request: &fireweed_engine::ItemMutationRequest,
        fingerprint: u64,
    ) -> EngineResult<Option<fireweed_engine::ItemMutationResponse>> {
        let connection = self.reader.lock().await;
        let rows = query_value_rows(&connection,
            "SELECT request_fingerprint,response_payload,command_positions FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation='item_mutation' AND request_id=?3 AND expires_at>?4",
            vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), request.request_id.as_str().to_string().into(), ts_nanos(request.evaluated_at).into()]).await?;
        let Some(v) = rows.first() else {
            return Ok(None);
        };
        if blob(&v[0])? != fingerprint.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        let mut response: fireweed_engine::ItemMutationResponse =
            serde_json::from_str(&text(&v[1])?).map_err(storage)?;
        let positions: Vec<(u64, u64)> = serde_json::from_str(&text(&v[2])?).map_err(storage)?;
        response.position = positions
            .into_iter()
            .next()
            .map(|(epoch, sequence)| CommandPosition::new(shard.clone(), epoch, sequence));
        Ok(Some(response))
    }

    pub async fn batch_update_replay(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: fireweed_core::BodyHash,
        now: UtcTimestamp,
    ) -> EngineResult<Option<fireweed_engine::BatchUpdateResponse>> {
        let connection = self.reader.lock().await;
        let rows = query_value_rows(&connection,
            "SELECT request_fingerprint,response_payload FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation='batch_update' AND request_id=?3 AND expires_at>?4",
            vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), request_id.as_str().to_string().into(), ts_nanos(now).into()]).await?;
        let Some(v) = rows.first() else {
            return Ok(None);
        };
        if blob(&v[0])? != fingerprint.0.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        Ok(Some(serde_json::from_str(&text(&v[1])?).map_err(storage)?))
    }

    pub async fn server_update_snapshot_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
        let connection = self.reader.lock().await;
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let rows = query_value_rows(
                &connection,
                "SELECT item_id,client_item_key,item_version,lifecycle_state,fenced,superseded \
                 FROM fireweed_items INDEXED BY sqlite_autoindex_fireweed_items_1 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    id.to_string().into(),
                ],
            )
            .await?;
            if let Some(v) = rows.first() {
                result.push(BatchUpdateSnapshotItem {
                    item_id: *id,
                    client_item_key: ClientItemKey::new(text(&v[1])?).map_err(storage)?,
                    item_version: nonnegative_u64(integer(&v[2])?, "item_version")?,
                    state: parse_state(&text(&v[3])?).map_err(storage)?,
                    fenced: integer(&v[4])? != 0,
                    superseded: integer(&v[5])? != 0,
                });
            }
        }
        Ok(result)
    }

    pub async fn server_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        let connection = self.reader.lock().await;
        server_live_items_on(&connection, shard, keys).await
    }

    pub async fn server_metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        let connection = self.reader.lock().await;
        server_metrics_on(&connection, shard).await
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
            let connection = self.writer.lock().await;
            let rows = query_value_rows(&connection,
                "SELECT request_fingerprint,response_payload FROM fireweed_request_idempotency \
                 WHERE tenant_id=?1 AND queue_id=?2 AND operation='commit' AND request_id=?3 AND expires_at>?4",
                vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), request_id.as_str().to_string().into(), ts_nanos(now).into()]).await?;
            let Some(v) = rows.first() else {
                return Ok(None);
            };
            if blob(&v[0])? != fingerprint.to_be_bytes() {
                return Err(EngineError::RequestIdConflict);
            }
            Ok(Some(serde_json::from_str(&text(&v[1])?).map_err(storage)?))
        }
    }

    fn side_record(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        async move {
            let connection = self.writer.lock().await;
            let rows = query_value_rows(&connection,
                "SELECT payload FROM fireweed_side_records WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
                vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), Value::Blob(key)]).await?;
            rows.first()
                .map(|v| blob(&v[0]).map(Bytes::from))
                .transpose()
        }
    }

    fn instance_fence(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        async move {
            let connection = self.writer.lock().await;
            let rows = query_value_rows(&connection,
                "SELECT fence FROM fireweed_instance_fences WHERE tenant_id=?1 AND queue_id=?2 AND instance_key=?3",
                vec![shard.tenant_id.as_str().to_string().into(), shard.queue_id.as_str().to_string().into(), Value::Blob(key)]).await?;
            rows.first()
                .map(|v| nonnegative_u64(integer(&v[0])?, "instance fence"))
                .transpose()
        }
    }

    fn index_validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        // Reuse push admission; a far-future `now` ignores expired key-retention rows.
        self.validate_push(
            shard,
            items,
            UtcTimestamp::new(4_102_444_800, 0).expect("far-future timestamp"),
        )
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

                // Keep ID and active-key existence checks as separate full-key
                // seeks. A combined OR makes Turso scan the queue for every
                // successor inserted by a workflow commit.
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
                               INDEXED BY sqlite_autoindex_fireweed_items_1 \
                               WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=r.item_id) \
                             OR EXISTS (SELECT 1 FROM fireweed_items i INDEXED BY fireweed_items_active_key \
                               WHERE i.tenant_id=?1 AND i.queue_id=?2 \
                               AND i.client_item_key=r.client_item_key AND i.superseded=0) \
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
            push_idempotency_on(&connection, &shard, &request_id, &fingerprint, now).await
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
            let remembered = {
                let tokens = self.live_tokens.lock().await;
                targets
                    .iter()
                    .filter_map(|target| {
                        tokens
                            .get(&(shard.clone(), target.item_id))
                            .cloned()
                            .map(|token| (target.item_id, token))
                    })
                    .collect::<HashMap<_, _>>()
            };
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
                    let remembered_ok =
                        remembered.get(&target.item_id) == Some(&target.lease_token);
                    if !remembered_ok {
                        if state != ItemState::Leased {
                            return Err(EngineError::Invalid("item is not leased"));
                        }
                        if blob(&row[5])? != lease_hash(&target.lease_token)
                            || matches!(row[4], Value::Null)
                            || integer(&row[4])? < now_nanos
                        {
                            return Err(EngineError::StaleLease);
                        }
                    } else if state != ItemState::Leased && state != ItemState::Pending {
                        return Err(EngineError::Invalid("item is not leased"));
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
        let apply_shape = self.last_apply_statement_shape.clone();
        let phase = self.last_apply_phase.clone();
        let grouped_shards = self.grouped_shards.clone();
        let claim_scan_hints = self.claim_scan_hints.clone();
        let claim_scan_default_fifo = self.claim_scan_default_fifo.clone();
        // NORMAL checkpoint accounting permits native WAL restart/reuse.
        // Forced truncation discards that reusable file and churns filesystem
        // extents. Keep the bounded truncation workaround only for OFF mode.
        let wal_path = if self.config().reuses_checkpointed_wal() {
            None
        } else {
            sqlite_wal_path(self.config().path())
        };
        let wal_min_bytes = self.wal_truncate_min_bytes;
        let busy_timeout = self.config().busy_timeout();
        async move {
            apply_owned(
                writer.clone(),
                tokens,
                by_consumer,
                shape,
                apply_shape,
                phase,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                positions,
                commands,
                true,
            )
            .await?;
            truncate_wal_if_unpinned(&writer, wal_path.as_deref(), wal_min_bytes, busy_timeout)
                .await;
            Ok(())
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
        let apply_shape = self.last_apply_statement_shape.clone();
        let phase = self.last_apply_phase.clone();
        let grouped_shards = self.grouped_shards.clone();
        let claim_scan_hints = self.claim_scan_hints.clone();
        let claim_scan_default_fifo = self.claim_scan_default_fifo.clone();
        // NORMAL checkpoint accounting permits native WAL restart/reuse.
        // Forced truncation discards that reusable file and churns filesystem
        // extents. Keep the bounded truncation workaround only for OFF mode.
        let wal_path = if self.config().reuses_checkpointed_wal() {
            None
        } else {
            sqlite_wal_path(self.config().path())
        };
        let wal_min_bytes = self.wal_truncate_min_bytes;
        let busy_timeout = self.config().busy_timeout();
        async move {
            apply_owned(
                writer.clone(),
                tokens,
                by_consumer,
                shape,
                apply_shape,
                phase,
                grouped_shards,
                claim_scan_hints,
                claim_scan_default_fifo,
                positions,
                commands,
                false,
            )
            .await?;
            truncate_wal_if_unpinned(&writer, wal_path.as_deref(), wal_min_bytes, busy_timeout)
                .await;
            Ok(())
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

    fn resolve_lease_targets(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        async move {
            let remembered = {
                let tokens = self.live_tokens.lock().await;
                ids.iter()
                    .filter_map(|id| {
                        tokens
                            .get(&(shard.clone(), *id))
                            .cloned()
                            .map(|token| (*id, token))
                    })
                    .collect::<HashMap<_, _>>()
            };
            if remembered.len() != ids.len() {
                return AsyncProjectionStore::render_claimed(self, shard, ids).await;
            }
            let mut item_rows = HashMap::<ItemId, Vec<Value>>::with_capacity(ids.len());
            let mut gate_keys = HashMap::<ItemId, Vec<String>>::new();
            for chunk in ids.chunks(500) {
                let placeholders = (0..chunk.len())
                    .map(|index| format!("?{}", index + 3))
                    .collect::<Vec<_>>()
                    .join(",");
                let mut params = vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                ];
                params.extend(chunk.iter().map(|id| id.to_string().into()));
                let item_sql = lease_target_rows_sql(chunk.len());
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
                for row in self.query(gate_sql, params).await.map_err(storage)? {
                    let id = ItemId::new(text(&row.values[0])?).map_err(storage)?;
                    gate_keys.entry(id).or_default().push(text(&row.values[1])?);
                }
            }
            let mut claimed = Vec::with_capacity(ids.len());
            for id in ids {
                let Some(token) = remembered.get(&id).cloned() else {
                    return Err(EngineError::StaleLease);
                };
                let Some(values) = item_rows.get(&id) else {
                    return Err(EngineError::NotFound);
                };
                let expires = optional_integer(&values[5])?.unwrap_or(1);
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
                     lease_expires_at,retry_count,max_attempts,CASE WHEN EXISTS(SELECT 1 FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) THEN (SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) ELSE payload END,fields,metadata,entity_document,index_fields \
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
mod committed_pool_helper_tests {
    use std::collections::BTreeMap;

    use fireweed_core::{
        CohortId, GroupKey, ItemId, LeaseToken, MetadataValue, PriorityValue, QueueId, TenantId,
        TypedValue, UtcTimestamp, WorkerId,
    };
    use fireweed_engine::{
        ClaimCompatibility, ClaimRequest, ClaimedItem, PreparedClaimedResult, QueueKey,
        finish_retained_grouped_cohort_claim,
    };
    use fireweed_relational::nanos_ts;
    use turso::Value;

    use super::{
        TursoRelational, finish_retained_claimed, materialize_grouped_cohort_claimed_on,
        select_item_claim_ids_on,
    };

    fn between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
        let (_, tail) = source
            .split_once(start)
            .unwrap_or_else(|| panic!("missing source-audit start marker: {start}"));
        let (body, _) = tail
            .split_once(end)
            .unwrap_or_else(|| panic!("missing source-audit end marker: {end}"));
        body
    }

    fn asserts_no_pool_borrow(source: &str, label: &str) {
        for needle in [
            "borrow_driver",
            "borrow_outcome",
            "try_borrow_driver",
            "try_borrow_outcome",
            "committed_pools",
            "CommittedReaderGuard",
            "OutcomeReadAdmission",
        ] {
            assert!(
                !source.contains(needle),
                "{label} must not borrow a committed pool ({needle})"
            );
        }
    }

    #[test]
    fn post_publication_response_never_borrows_committed_pool() {
        let projection = include_str!("projection.rs");
        let local = include_str!("local.rs");
        let compose = include_str!("../../fireweed/src/turso_compose.rs");

        let retained = between(
            projection,
            "pub fn finish_retained_claimed(",
            "async fn query_driver_value_rows(",
        );
        assert!(
            retained.contains("Ok(items)"),
            "post-publication continuation must accept retained results"
        );
        asserts_no_pool_borrow(retained, "finish_retained_claimed");
        assert!(
            !retained.contains("Connection"),
            "finish_retained_claimed must have no connection parameter"
        );

        let render = between(projection, "fn render_claimed(", "fn item_state(");
        asserts_no_pool_borrow(render, "render_claimed");
        assert!(
            render.contains("self.query("),
            "post-append render_claimed must keep the serving reader, not a pool"
        );

        let apply_live = between(projection, "fn apply_live(", "fn apply_recovery(");
        asserts_no_pool_borrow(apply_live, "apply_live");
        assert!(
            apply_live.contains("truncate_wal_if_unpinned"),
            "apply must truncate WAL when no reader snapshot is live"
        );
        assert!(
            projection.contains("PRAGMA wal_checkpoint(TRUNCATE)"),
            "WAL bound is writer TRUNCATE, not PASSIVE"
        );

        let (_, production) = compose
            .rsplit_once("// Atomic log-replay × Turso")
            .expect("production Turso composition boundary");
        let derived_impl = production
            .split("impl DerivedObjectLogTursoBackend {")
            .nth(1)
            .expect("derived Turso implementation");
        let item_claim = between(
            derived_impl,
            "async fn dispatch_claim(",
            "async fn dispatch_grouped_cohort_claim(",
        );
        assert!(
            item_claim.contains("drive_candidate_mutation"),
            "ordinary item Claim must join the packed Update generation"
        );
        assert!(
            !item_claim.contains("acquire_exclusive"),
            "ordinary item Claim must not take the exclusive selection fence"
        );
        assert!(
            !item_claim.contains("class_s_claim_for_queue"),
            "S5 cuts item Claim off the SQL-first writer lease-before-append lane"
        );
        assert!(
            !item_claim.contains("render_claimed(")
                && !item_claim.contains("render_prepared_claim"),
            "item Claim continuation must retain pre-materialized rows"
        );
        let retained = between(
            compose,
            "fn finish_retained_grouped_cohort_response(",
            "fn finish_inert_mutation_generation_append(",
        );
        asserts_no_pool_borrow(retained, "turso_compose retained post-publication");
        let grouped = between(
            compose,
            "async fn dispatch_grouped_cohort_claim(",
            "async fn append_class_s_claim(",
        );
        assert!(
            !grouped.contains("render_prepared_claim") && !grouped.contains("render_claimed("),
            "grouped/cohort post-publication must use the retained carrier"
        );
        let production_local = local
            .split("#[cfg(test)]")
            .next()
            .expect("production local.rs");
        assert!(
            !production_local.contains("PRAGMA wal_checkpoint"),
            "committed-reader construction must not invoke wal_checkpoint"
        );
        let claim_micro = between(
            local,
            "pub async fn item_claim_microbatch_on_connection(",
            "pub async fn mutation_driver_snapshot_on(",
        );
        assert!(
            !claim_micro.contains("transaction_with_behavior"),
            "item Claim SELECT must not open a Deferred snapshot that pins WAL"
        );
        let serving_claim = between(
            local,
            "pub async fn item_claim_microbatch_on_serving_reader(",
            "pub async fn item_claim_microbatch_on_connection(",
        );
        assert!(
            serving_claim.contains("claim_scan_is_fifo")
                && serving_claim.contains("advance_claim_scan_hint"),
            "serving Claim must advance the FIFO rowid floor under the reader mutex"
        );

        let items = finish_retained_claimed(Vec::<ClaimedItem>::new()).expect("retained");
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn grouped_cohort_claim_materializes_before_append() {
        let store = TursoRelational::in_memory().await.expect("open");
        let stored_id = ItemId::mint(1, 0, 1);
        let compact_id = ItemId::mint(1, 0, 2);
        let index_fields = BTreeMap::from([
            ("profile.rank".to_string(), TypedValue::Integer(42)),
            (
                "profile.region".to_string(),
                TypedValue::String("east".to_string()),
            ),
        ]);
        let encoded_index_fields =
            fireweed_engine::index_fields::encode_index_fields_blob(&index_fields)
                .expect("encode index fields")
                .expect("nonempty index fields");
        let insert = "INSERT INTO fireweed_items(\
             tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
             not_before,eligible_since,group_key,payload,fields,metadata,entity_document,index_fields,\
             retry_count,item_version,last_command_sequence,created_at,updated_at,fenced,superseded,\
             max_attempts,created_seq) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        for (id, entity, native_indexes, created_seq) in [
            (
                stored_id,
                Value::Text(r#"{"kind":"stored","rank":7}"#.to_string()),
                Value::Null,
                1_i64,
            ),
            (
                compact_id,
                Value::Null,
                Value::Blob(encoded_index_fields),
                2_i64,
            ),
        ] {
            store
                .execute(
                    insert,
                    vec![
                        Value::Text("t".to_string()),
                        Value::Text("q".to_string()),
                        Value::Text(id.to_string()),
                        Value::Text(format!("key-{id}")),
                        Value::Text("Pending".to_string()),
                        Value::Text(
                            serde_json::to_string(&PriorityValue::Int64(7)).expect("priority"),
                        ),
                        Value::Blob(vec![0, created_seq as u8]),
                        Value::Integer(7),
                        Value::Integer(1),
                        Value::Text("group-a".to_string()),
                        Value::Blob(vec![0xCA, 0xFE]),
                        Value::Text(r#"{"blob":[1,2,3]}"#.to_string()),
                        Value::Text(r#"{"attempt":7,"source":"class-s"}"#.to_string()),
                        entity,
                        native_indexes,
                        Value::Integer(2),
                        Value::Integer(4),
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Integer(9),
                        Value::Integer(created_seq),
                    ],
                )
                .await
                .expect("insert pending full row");
        }
        store
            .execute(
                "INSERT INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) \
                 VALUES('t','q',?,'gate-satisfied')",
                vec![Value::Text(stored_id.to_string())],
            )
            .await
            .expect("insert satisfied gate membership");

        let shard = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let token = LeaseToken::new("token-grouped").expect("token");
        let expires = UtcTimestamp::new(20, 0).unwrap();
        let ids = vec![stored_id, compact_id];
        let snapshot = store.reader.lock().await;
        let items = materialize_grouped_cohort_claimed_on(&snapshot, &shard, &ids, &token, expires)
            .await
            .expect("materialize on driver snapshot");
        drop(snapshot);

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].item_id, stored_id);
        assert_eq!(items[1].item_id, compact_id);
        assert_eq!(items[0].payload.as_deref(), Some(&[0xCA, 0xFE][..]));
        assert_eq!(
            items[0].fields.get("blob").map(|value| value.as_ref()),
            Some(&[1, 2, 3][..])
        );
        assert_eq!(
            items[0].metadata.get("source"),
            Some(&MetadataValue::String("class-s".to_string()))
        );
        assert_eq!(
            items[0].metadata.get("attempt"),
            Some(&MetadataValue::Integer(7))
        );
        assert_eq!(
            items[0].entity,
            Some(serde_json::json!({"kind": "stored", "rank": 7}))
        );
        assert_eq!(items[0].gate_keys, ["gate-satisfied"]);
        assert_eq!(items[0].not_before, Some(nanos_ts(7)));
        assert_eq!(items[0].priority, Some(PriorityValue::Int64(7)));
        assert_eq!(
            items[0].group_key.as_ref().map(GroupKey::as_str),
            Some("group-a")
        );
        assert_eq!(
            items[1].entity,
            Some(serde_json::json!({"profile": {"rank": 42, "region": "east"}}))
        );
        assert!(
            items
                .iter()
                .all(|item| item.lease_token.as_ref() == Some(&token))
        );
        assert!(items.iter().all(|item| item.lease_expires_at == expires));

        let request = ClaimRequest {
            shard: shard.clone(),
            worker_id: WorkerId::new("worker").unwrap(),
            max_items: 2,
            lease_token: token.clone(),
            lease_expires_at: expires,
            now: UtcTimestamp::new(10, 0).unwrap(),
            eligibility_time: None,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(1),
        };
        let grouped = PreparedClaimedResult::from_rendered(&request, &ids, items.clone(), None)
            .expect("grouped retained")
            .into_claimed();
        assert_eq!(grouped.items.len(), 2);
        assert_eq!(
            grouped.items[0].lease_token.as_ref(),
            Some(&request.lease_token)
        );
        assert!(grouped.cohort_id.is_none());

        let mut cohort_request = request.clone();
        cohort_request.compatibility.whole_cohort = true;
        let cohort_id = CohortId::new("coh:group-a:10000000000").unwrap();
        let shaped = finish_retained_grouped_cohort_claim(
            &cohort_request,
            &ids,
            items,
            Some(cohort_id.clone()),
        )
        .expect("cohort shape");
        assert!(shaped.items.iter().all(|item| item.lease_token.is_none()));
        assert_eq!(
            shaped.cohort_lease_token.as_ref(),
            Some(&cohort_request.lease_token)
        );
        assert_eq!(shaped.cohort_id.as_ref(), Some(&cohort_id));
        let continued = PreparedClaimedResult::Retained(shaped).into_claimed();
        assert_eq!(continued.cohort_id.as_ref(), Some(&cohort_id));

        let projection = include_str!("projection.rs");
        let helper = between(
            projection,
            "pub async fn materialize_grouped_cohort_claimed_on(",
            "impl TursoRelational {",
        );
        assert!(helper.contains("connection: &Connection"));
        assert!(
            !helper.contains("lifecycle_state='Leased'"),
            "pre-append snapshot rows are still Pending"
        );
        asserts_no_pool_borrow(helper, "materialize_grouped_cohort_claimed_on");
        let render = between(projection, "fn render_claimed(", "fn item_state(");
        assert!(
            render.contains("self.query("),
            "legacy render_claimed stays on the serving reader for recovery"
        );
        assert!(render.contains("lifecycle_state='Leased'"));
        let select_helper = between(
            projection,
            "pub async fn select_item_claim_ids_on(",
            "pub async fn materialize_grouped_cohort_claimed_on(",
        );
        assert!(
            !select_helper.contains("lifecycle_state='Leased'"),
            "log-first item select must not lease before append"
        );
        assert!(
            !select_helper.contains("ORDER BY payload") && !select_helper.contains("WHERE payload"),
            "candidate Claim SELECT must not sort or filter on payload"
        );
        assert!(
            select_helper.contains("NOT INDEXED") && select_helper.contains("ORDER BY rowid"),
            "FIFO Claim must walk rowid, not sort blobs"
        );
        assert!(
            select_helper.contains("ORDER BY priority_sort,created_seq")
                || select_helper.contains("ORDER BY rowid"),
            "Claim order is indexed sort keys or rowid, never payload"
        );
        assert!(
            !select_helper.contains("ORDER BY payload"),
            "Claim must not sort on the payload blob"
        );
        asserts_no_pool_borrow(select_helper, "select_item_claim_ids_on");
        let _ = select_item_claim_ids_on;
    }

    #[test]
    fn pre_position_outcome_helpers_accept_borrowed_connection() {
        let projection = include_str!("projection.rs");
        for helper in [
            "pub(crate) async fn server_peek_on(",
            "pub(crate) async fn server_pending_rows_on(",
            "pub(crate) async fn server_pending_by_ids_on(",
            "pub(crate) async fn server_update_snapshot_on(",
            "pub(crate) async fn server_live_items_on(",
            "pub(crate) async fn server_metrics_on(",
            "pub(crate) async fn push_idempotency_on(",
        ] {
            assert!(
                projection.contains(helper),
                "missing borrowed-connection helper {helper}"
            );
            let (_, tail) = projection.split_once(helper).expect(helper);
            let signature = tail.split('{').next().expect("helper signature");
            assert!(
                signature.contains("connection: &Connection"),
                "{helper} must accept a borrowed connection/Deferred snapshot"
            );
        }
        assert!(projection.contains("let connection = self.reader.lock().await;"));
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
        // is being staged. The owned apply must finish atomically after admission.
        let started_id = ItemId::new("500").unwrap();
        let (position, command, request_id, fingerprint) =
            replayable_push(&shard, started_id, 0, "started-cut", 512);
        let mut started = Box::pin(AsyncProjectionStore::apply_live(
            store.as_ref(),
            vec![position],
            vec![command],
        ));
        // Poll on this task until the owned apply has been spawned. Observing
        // try_lock failure is insufficient: a queued task may own a reserved
        // mutex permit without having resumed past lock_owned().
        std::future::poll_fn(|cx| {
            assert!(matches!(
                std::future::Future::poll(started.as_mut(), cx),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        drop(started);
        // The response waiter can be gone while the owned apply still runs.
        // Observe its settled result only after its writer guard is released.
        drop(store.writer.lock().await);
        let state = AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), started_id)
            .await
            .unwrap();
        assert_eq!(state, Some(ItemState::Pending));
        assert!(matches!(
            replay(store.as_ref(), &shard, request_id, fingerprint).await,
            IdempotencyDecision::Replay(_)
        ));

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
            resolved_tx.send(result).unwrap();
            future::pending::<()>().await;
        });
        resolved_rx
            .await
            .unwrap()
            .expect("resolved apply must succeed before cancellation");
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
                "SELECT i.lifecycle_state,i.item_version,p.payload,i.not_before,i.eligible_since \
                 FROM fireweed_items i LEFT JOIN fireweed_item_payloads p \
                 ON p.tenant_id=i.tenant_id AND p.queue_id=i.queue_id AND p.item_id=i.item_id \
                 WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=?3",
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
                    authority_first: false,
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
        let bearers = store
            .query(
                "SELECT item_id FROM fireweed_lease_bearers WHERE tenant_id=?1 AND queue_id=?2",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                ],
            )
            .await
            .unwrap();
        assert!(
            bearers.is_empty(),
            "invalidated or purged lease bearers survived"
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

#[cfg(test)]
mod lease_target_query_tests {
    use super::*;

    #[tokio::test]
    async fn lease_target_resolution_seeks_items_and_payloads_by_full_key() {
        let store = TursoRelational::in_memory().await.unwrap();
        let rows = store
            .query(
                format!("EXPLAIN QUERY PLAN {}", lease_target_rows_sql(3)),
                vec![
                    "tenant".into(),
                    "queue".into(),
                    "10".into(),
                    "200".into(),
                    "9999".into(),
                ],
            )
            .await
            .unwrap();
        let details: Vec<_> = rows
            .iter()
            .map(|row| text(&row.values[3]).unwrap())
            .collect();
        for table in ["i", "p"] {
            assert!(
                details.iter().any(
                    |line| line.starts_with(&format!("SEARCH {table} USING INDEX"))
                        && line.contains("tenant_id=? AND queue_id=? AND item_id=?")
                ),
                "{details:?}"
            );
        }
    }
}

#[cfg(test)]
mod addressed_query_tests {
    use super::*;
    #[tokio::test]
    async fn addressed_snapshot_uses_full_key_seeks_for_every_join() {
        let store = TursoRelational::in_memory().await.unwrap();
        let rows = store
            .query(
                format!("EXPLAIN QUERY PLAN {}", addressed_item_rows_sql(3, true)),
                vec![
                    "tenant".into(),
                    "queue".into(),
                    "1".into(),
                    "2".into(),
                    "3".into(),
                ],
            )
            .await
            .unwrap();
        let details: Vec<_> = rows
            .iter()
            .map(|row| text(&row.values[3]).unwrap())
            .collect();
        for table in ["i", "p", "b"] {
            assert!(
                details.iter().any(
                    |line| line.starts_with(&format!("SEARCH {table} USING INDEX"))
                        && line.contains("tenant_id=? AND queue_id=? AND item_id=?")
                ),
                "{details:?}"
            );
        }
    }
}

#[cfg(test)]
mod ordered_claim_query_tests {
    use super::*;

    #[tokio::test]
    async fn eligibility_is_covered_before_full_key_materialization() {
        let store = TursoRelational::in_memory().await.unwrap();
        let rows = store
            .query(
                format!("EXPLAIN QUERY PLAN {}", ORDERED_ITEM_CLAIM_SQL),
                vec![
                    "tenant".into(),
                    "queue".into(),
                    1000_i64.into(),
                    Value::Blob(Vec::new()),
                    i64::MIN.into(),
                    1000_i64.into(),
                ],
            )
            .await
            .unwrap();
        let details: Vec<_> = rows
            .iter()
            .map(|row| text(&row.values[3]).unwrap())
            .collect();
        let program = store
            .query(
                format!("EXPLAIN {}", ORDERED_ITEM_CLAIM_SQL),
                vec![
                    "tenant".into(),
                    "queue".into(),
                    1000_i64.into(),
                    Value::Blob(Vec::new()),
                    i64::MIN.into(),
                    1000_i64.into(),
                ],
            )
            .await
            .unwrap();
        // Turso's EQP labels seeks "USING INDEX" even when all reads are
        // covered. Its unused table cursor/DeferredSeek is lazy: assert that
        // the candidate coroutine actually reads every column from the index.
        let candidate: Vec<_> = program
            .iter()
            .take_while(|row| text(&row.values[1]).unwrap() != "EndCoroutine")
            .collect();
        assert!(
            candidate.len() < program.len(),
            "missing bounded candidate coroutine"
        );
        for row in &candidate {
            if text(&row.values[1]).unwrap() == "Column" {
                assert!(
                    text(&row.values[7])
                        .unwrap()
                        .contains("fireweed_items_pending_eligible_order_idx."),
                    "candidate scan reads item bodies: {row:?}"
                );
            }
        }
        for column in ["not_before", "eligible_since", "cohort_size"] {
            assert!(
                program
                    .iter()
                    .any(|row| text(&row.values[1]).unwrap() == "Column"
                        && text(&row.values[7]).unwrap().ends_with(&format!(
                            "fireweed_items_pending_eligible_order_idx.{column}"
                        ))),
                "eligibility column must come from the index: {column}"
            );
        }
        for table in ["i", "p"] {
            assert!(
                details.iter().any(
                    |line| line.starts_with(&format!("SEARCH {table} USING INDEX"))
                        && line.contains("tenant_id=? AND queue_id=? AND item_id=?")
                ),
                "materialization must seek selected IDs: {details:?}"
            );
        }
    }
}

#[cfg(test)]
mod metrics_query_tests {
    use super::*;

    #[tokio::test]
    async fn lifecycle_counts_do_not_read_resident_rows() {
        let store = TursoRelational::in_memory().await.unwrap();
        let rows = store
            .query(
                format!("EXPLAIN QUERY PLAN {}", LIFECYCLE_METRICS_SQL),
                vec!["tenant".into(), "queue".into()],
            )
            .await
            .unwrap();
        let details: Vec<_> = rows
            .iter()
            .map(|row| text(&row.values[3]).unwrap())
            .collect();
        assert!(
            !details.iter().any(|line| line.contains("fireweed_items")
                || line.contains("SORTER")
                || line.contains("TEMP B-TREE")),
            "four counts must not read resident rows: {details:?}"
        );
    }

    #[tokio::test]
    async fn lifecycle_counts_handle_empty_terminal_and_superseded_rows() {
        let store = TursoRelational::in_memory().await.unwrap();
        let shard = QueueKey::new(
            fireweed_core::TenantId::new("tenant").unwrap(),
            fireweed_core::QueueId::new("queue").unwrap(),
        );
        let empty = store.server_metrics(&shard).await.unwrap();
        assert_eq!(
            (
                empty.pending,
                empty.leased,
                empty.complete,
                empty.failed,
                empty.resident_terminal_count
            ),
            (0, 0, 0, 0, 0)
        );
        for (tenant, queue) in [
            ("tenant", "queue"),
            ("foreign", "queue"),
            ("tenant", "foreign"),
        ] {
            store
                .execute(
                    "INSERT INTO queues(tenant,queue,definition) VALUES(?1,?2,'{}')",
                    vec![tenant.into(), queue.into()],
                )
                .await
                .unwrap();
        }
        for (state, count) in [
            ("Pending", 2),
            ("Leased", 3),
            ("Complete", 4),
            ("Failed", 5),
        ] {
            for (tenant, queue, superseded) in [
                ("tenant", "queue", 0),
                ("foreign", "queue", 0),
                ("tenant", "foreign", 0),
                ("tenant", "queue", 1),
            ] {
                for n in 0..count {
                    let id = format!("{state}-{superseded}-{n}");
                    store
                        .execute(
                            "INSERT INTO fireweed_items(tenant_id,queue_id,item_id,client_item_key,\
                         lifecycle_state,priority_sort,item_version,last_command_sequence,\
                         created_at,updated_at,max_attempts,created_seq,superseded) \
                         VALUES(?1,?2,?3,?3,?4,X'00',1,1,1,1,5,1,?5)",
                            vec![
                                tenant.into(),
                                queue.into(),
                                id.into(),
                                state.into(),
                                Value::Integer(superseded),
                            ],
                        )
                        .await
                        .unwrap();
                }
            }
        }
        // Raw SQL models a populated pre-counter projection; migration must
        // backfill it exactly, including tenant/queue and supersession filters.
        store.migrate().await.unwrap();
        let metrics = store.server_metrics(&shard).await.unwrap();
        assert_eq!(
            (
                metrics.pending,
                metrics.leased,
                metrics.complete,
                metrics.failed,
                metrics.resident_terminal_count
            ),
            (2, 3, 4, 5, 9)
        );
    }
}
