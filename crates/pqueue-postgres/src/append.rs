use std::sync::Arc;

use pqueue_core::{
    DecimalValue, PriorityModel, PriorityModelKind, PriorityValue, UtcTimestamp,
    priority_sort as compute_priority_sort,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::convert::json_priority_model;
use crate::schema::DDL;

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

fn utc_to_odt(ts: &UtcTimestamp) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(ts.seconds).unwrap()
        + time::Duration::nanoseconds(ts.nanoseconds as i64)
}

fn priority_value_to_json(v: &PriorityValue) -> Value {
    match v {
        PriorityValue::Timestamp(ts) => {
            json!({"kind": "timestamp", "seconds": ts.seconds, "nanoseconds": ts.nanoseconds})
        }
        PriorityValue::Int64(n) => json!({"kind": "int64", "value": n}),
        PriorityValue::Decimal(d) => {
            json!({"kind": "decimal", "mantissa": d.mantissa.to_string(), "scale": d.scale})
        }
        PriorityValue::Text(s) => json!({"kind": "text", "value": s}),
    }
}

fn effective_priority(
    explicit: Option<&PriorityValue>,
    model: &PriorityModel,
    now: &UtcTimestamp,
) -> PriorityValue {
    match explicit {
        Some(v) => v.clone(),
        None => match model.kind {
            PriorityModelKind::Timestamp => PriorityValue::Timestamp(*now),
            PriorityModelKind::Int64 => PriorityValue::Int64(0),
            PriorityModelKind::Decimal => PriorityValue::Decimal(DecimalValue {
                mantissa: 0,
                scale: 0,
            }),
            PriorityModelKind::Text => PriorityValue::Text(String::new()),
        },
    }
}

// Determine eligible_since for a newly pushed item:
// - None not_before OR not_before already in the past → eligible now (use now)
// - not_before in the future → not yet eligible (None = NULL in DB)
fn eligible_since_odt(
    not_before: Option<&UtcTimestamp>,
    now: &UtcTimestamp,
) -> Option<OffsetDateTime> {
    match not_before {
        None => Some(utc_to_odt(now)),
        Some(nb) if nb <= now => Some(utc_to_odt(nb)),
        Some(_) => None,
    }
}

// ---------------------------------------------------------------------------
// BatchPush request / response types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PgPushItem {
    pub item_id: String,
    pub client_item_key: String,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<String>,
    pub gate_keys: Vec<String>,
    pub payload: Option<Value>,
}

#[derive(Debug)]
pub struct PgBatchPushRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub items: Vec<PgPushItem>,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub enum PgPushOutcome {
    New { item_version: u64 },
    Duplicate { existing_item_id: String },
}

#[derive(Debug)]
pub struct PgPushItemResult {
    pub client_item_key: String,
    pub item_id: String,
    pub outcome: PgPushOutcome,
}

#[derive(Debug)]
pub struct PgBatchPushResult {
    pub command_sequence: u64,
    pub items: Vec<PgPushItemResult>,
}

// ---------------------------------------------------------------------------
// BatchUpdate request / response types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PgUpdateItem {
    pub item_id: String,
    pub expected_item_version: Option<u64>,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
}

#[derive(Debug)]
pub struct PgBatchUpdateRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub items: Vec<PgUpdateItem>,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub enum PgUpdateOutcome {
    Updated { item_version: u64 },
    NotFound,
    Terminal,
    Conflict { message: String },
}

#[derive(Debug)]
pub struct PgUpdateItemResult {
    pub item_id: String,
    pub outcome: PgUpdateOutcome,
}

#[derive(Debug)]
pub struct PgBatchUpdateResult {
    pub command_sequence: u64,
    pub items: Vec<PgUpdateItemResult>,
}

// ---------------------------------------------------------------------------
// BatchClaim request / response types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PgBatchClaimRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub max_items: usize,
    pub now: UtcTimestamp,
    pub lease_token: String,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgBatchClaimResult {
    pub command_sequence: u64,
    pub claimed_item_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Lease renewal / finalize / expiry request and response types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PgRenewLeaseItem {
    pub item_id: String,
    pub lease_token: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PgRenewLeaseOutcome {
    Renewed { item_version: u64 },
    StaleLease,
    NotFound,
    Terminal,
}

#[derive(Debug)]
pub struct PgRenewLeaseItemResult {
    pub item_id: String,
    pub outcome: PgRenewLeaseOutcome,
}

#[derive(Debug)]
pub struct PgBatchRenewLeasesRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub items: Vec<PgRenewLeaseItem>,
    pub now: UtcTimestamp,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgBatchRenewLeasesResult {
    pub command_sequence: u64,
    pub items: Vec<PgRenewLeaseItemResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgFinalizeKind {
    Complete,
    Fail,
    Retry,
    Release,
}

#[derive(Debug)]
pub struct PgFinalizeItem {
    pub item_id: String,
    pub lease_token: String,
    pub kind: PgFinalizeKind,
    pub retry_not_before: Option<UtcTimestamp>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PgFinalizeOutcome {
    Completed { item_version: u64 },
    Failed { item_version: u64 },
    Retried { item_version: u64 },
    Released { item_version: u64 },
    StaleLease,
    NotFound,
    Terminal,
    Invalid { message: String },
}

#[derive(Debug)]
pub struct PgFinalizeItemResult {
    pub item_id: String,
    pub outcome: PgFinalizeOutcome,
}

#[derive(Debug)]
pub struct PgBatchFinalizeRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub items: Vec<PgFinalizeItem>,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgBatchFinalizeResult {
    pub command_sequence: u64,
    pub items: Vec<PgFinalizeItemResult>,
}

#[derive(Debug)]
pub struct PgLeaseExpiredRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub max_items: usize,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgLeaseExpiredResult {
    pub command_sequence: u64,
    pub expired_item_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgGateState {
    Open,
    Blocked,
}

#[derive(Debug)]
pub struct PgSetGate {
    pub gate_key: String,
    pub state: PgGateState,
}

#[derive(Debug)]
pub struct PgSetGatesRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub expected_epoch: u64,
    pub command_id: String,
    pub request_id: Option<String>,
    pub gates: Vec<PgSetGate>,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgSetGatesResult {
    pub command_sequence: u64,
    pub gates: Vec<PgSetGate>,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AppendError {
    EpochMismatch { expected: u64, current: u64 },
    ShardNotFound,
    QueueNotFound,
    RequestConflict,
    StorageFailure(String),
}

fn to_append_err(e: tokio_postgres::Error) -> AppendError {
    AppendError::StorageFailure(e.to_string())
}

fn lease_token_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

fn json_fingerprint(value: &Value) -> Vec<u8> {
    Sha256::digest(value.to_string().as_bytes()).to_vec()
}

fn push_request_fingerprint(req: &PgBatchPushRequest) -> Vec<u8> {
    let items: Vec<Value> = req
        .items
        .iter()
        .map(|item| {
            json!({
                "item_id": item.item_id,
                "client_item_key": item.client_item_key,
                "priority": item.priority.as_ref().map(priority_value_to_json),
                "not_before": item.not_before.map(|ts| {
                    json!({"seconds": ts.seconds, "nanoseconds": ts.nanoseconds})
                }),
                "group_key": item.group_key,
                "gate_keys": item.gate_keys,
                "payload": item.payload,
            })
        })
        .collect();
    json_fingerprint(&json!({
        "operation": "batch_push",
        "tenant_id": req.tenant_id,
        "queue_id": req.queue_id,
        "shard_id": req.shard_id,
        "expected_epoch": req.expected_epoch,
        "items": items,
    }))
}

fn push_result_to_json(result: &PgBatchPushResult) -> Value {
    let items: Vec<Value> = result
        .items
        .iter()
        .map(|item| {
            let outcome = match &item.outcome {
                PgPushOutcome::New { item_version } => {
                    json!({"kind": "new", "item_version": item_version})
                }
                PgPushOutcome::Duplicate { existing_item_id } => {
                    json!({"kind": "duplicate", "existing_item_id": existing_item_id})
                }
            };
            json!({
                "client_item_key": item.client_item_key,
                "item_id": item.item_id,
                "outcome": outcome,
            })
        })
        .collect();
    json!({"command_sequence": result.command_sequence, "items": items})
}

fn json_to_push_result(value: &Value) -> Result<PgBatchPushResult, AppendError> {
    let command_sequence = value["command_sequence"]
        .as_u64()
        .ok_or_else(|| AppendError::StorageFailure("missing command_sequence".to_string()))?;
    let items = value["items"]
        .as_array()
        .ok_or_else(|| AppendError::StorageFailure("missing push response items".to_string()))?
        .iter()
        .map(|item| {
            let outcome = match item["outcome"]["kind"].as_str().unwrap_or("") {
                "new" => PgPushOutcome::New {
                    item_version: item["outcome"]["item_version"].as_u64().ok_or_else(|| {
                        AppendError::StorageFailure("missing item_version".to_string())
                    })?,
                },
                "duplicate" => PgPushOutcome::Duplicate {
                    existing_item_id: item["outcome"]["existing_item_id"]
                        .as_str()
                        .ok_or_else(|| {
                            AppendError::StorageFailure("missing existing_item_id".to_string())
                        })?
                        .to_string(),
                },
                other => {
                    return Err(AppendError::StorageFailure(format!(
                        "unknown push outcome: {other}"
                    )));
                }
            };
            Ok(PgPushItemResult {
                client_item_key: item["client_item_key"]
                    .as_str()
                    .ok_or_else(|| {
                        AppendError::StorageFailure("missing client_item_key".to_string())
                    })?
                    .to_string(),
                item_id: item["item_id"]
                    .as_str()
                    .ok_or_else(|| AppendError::StorageFailure("missing item_id".to_string()))?
                    .to_string(),
                outcome,
            })
        })
        .collect::<Result<Vec<_>, AppendError>>()?;
    Ok(PgBatchPushResult {
        command_sequence,
        items,
    })
}

async fn refresh_group_summary(
    tx: &tokio_postgres::Transaction<'_>,
    tenant_id: &str,
    queue_id: &str,
    shard_id: u32,
    group_key: Option<&str>,
    now_odt: &OffsetDateTime,
) -> Result<(), AppendError> {
    tx.execute(
        "DELETE FROM pqueue_group_summary s
         WHERE s.tenant_id = $1
           AND s.queue_id = $2
           AND s.shard_id = $3
           AND s.group_key = COALESCE($4::text, '')
           AND NOT EXISTS (
             SELECT 1
             FROM pqueue_items i
             WHERE i.tenant_id = s.tenant_id
               AND i.queue_id = s.queue_id
               AND i.shard_id = s.shard_id
               AND COALESCE(i.group_key, '') = s.group_key
           )",
        &[&tenant_id, &queue_id, &(shard_id as i32), &group_key],
    )
    .await
    .map_err(to_append_err)?;

    tx.execute(
        "WITH group_items AS (
           SELECT lifecycle_state, eligible_since, not_before, priority_sort, created_at, item_id,
                  gate_keys
           FROM pqueue_items
           WHERE tenant_id = $1
             AND queue_id = $2
             AND shard_id = $3
             AND COALESCE(group_key, '') = COALESCE($4::text, '')
         ),
         stats AS (
           SELECT
             COUNT(*)::bigint AS total_count,
             COUNT(*) FILTER (
               WHERE lifecycle_state = 'pending'
                 AND eligible_since IS NOT NULL
                 AND (not_before IS NULL OR not_before <= $5)
                 AND NOT EXISTS (
                   SELECT 1
                   FROM unnest(group_items.gate_keys) AS g(gate_key)
                   JOIN pqueue_gate_state gs
                     ON gs.tenant_id = $1
                    AND gs.queue_id = $2
                    AND gs.shard_id = $3
                    AND gs.gate_key = g.gate_key
                    AND gs.state = 'blocked'
                 )
             )::bigint AS eligible_count,
             COUNT(*) FILTER (WHERE lifecycle_state = 'pending')::bigint AS pending_count,
             COUNT(*) FILTER (WHERE lifecycle_state = 'leased')::bigint AS leased_count,
             COUNT(*) FILTER (WHERE lifecycle_state IN ('complete', 'failed'))::bigint
               AS terminal_count
           FROM group_items
         ),
         rep AS (
           SELECT eligible_since, priority_sort, created_at, item_id
           FROM group_items
           WHERE lifecycle_state = 'pending'
             AND eligible_since IS NOT NULL
             AND (not_before IS NULL OR not_before <= $5)
             AND NOT EXISTS (
               SELECT 1
               FROM unnest(group_items.gate_keys) AS g(gate_key)
               JOIN pqueue_gate_state gs
                 ON gs.tenant_id = $1
                AND gs.queue_id = $2
                AND gs.shard_id = $3
                AND gs.gate_key = g.gate_key
                AND gs.state = 'blocked'
             )
           ORDER BY eligible_since ASC, priority_sort ASC, created_at ASC, item_id ASC
           LIMIT 1
         )
         INSERT INTO pqueue_group_summary (
           tenant_id, queue_id, shard_id, group_key,
           oldest_eligible_at, rep_progress_guard_sort, rep_priority_sort,
           rep_created_at, rep_item_id,
           eligible_count, pending_count, leased_count, terminal_count, updated_at
         )
         SELECT
           $1, $2, $3, COALESCE($4::text, ''),
           rep.eligible_since, rep.eligible_since, rep.priority_sort,
           rep.created_at, rep.item_id,
           stats.eligible_count, stats.pending_count, stats.leased_count,
           stats.terminal_count, $5
         FROM stats
         LEFT JOIN rep ON true
         WHERE stats.total_count > 0
         ON CONFLICT (tenant_id, queue_id, shard_id, group_key) DO UPDATE SET
           oldest_eligible_at = EXCLUDED.oldest_eligible_at,
           rep_progress_guard_sort = EXCLUDED.rep_progress_guard_sort,
           rep_priority_sort = EXCLUDED.rep_priority_sort,
           rep_created_at = EXCLUDED.rep_created_at,
           rep_item_id = EXCLUDED.rep_item_id,
           eligible_count = EXCLUDED.eligible_count,
           pending_count = EXCLUDED.pending_count,
           leased_count = EXCLUDED.leased_count,
           terminal_count = EXCLUDED.terminal_count,
           updated_at = EXCLUDED.updated_at",
        &[
            &tenant_id,
            &queue_id,
            &(shard_id as i32),
            &group_key,
            now_odt,
        ],
    )
    .await
    .map_err(to_append_err)?;

    Ok(())
}

async fn refresh_group_summaries(
    tx: &tokio_postgres::Transaction<'_>,
    tenant_id: &str,
    queue_id: &str,
    shard_id: u32,
    groups: &[Option<String>],
    now_odt: &OffsetDateTime,
) -> Result<(), AppendError> {
    let mut groups = groups.to_vec();
    groups.sort();
    groups.dedup();

    for group in groups {
        refresh_group_summary(tx, tenant_id, queue_id, shard_id, group.as_deref(), now_odt).await?;
    }

    Ok(())
}

async fn lock_shard_sequence(
    tx: &tokio_postgres::Transaction<'_>,
    tenant_id: &str,
    queue_id: &str,
    shard_id: u32,
    expected_epoch: u64,
) -> Result<(u64, u64), AppendError> {
    let s_row = tx
        .query_opt(
            "SELECT assignment_epoch, next_command_sequence
             FROM pqueue_shards
             WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3
             FOR UPDATE",
            &[&tenant_id, &queue_id, &(shard_id as i32)],
        )
        .await
        .map_err(to_append_err)?
        .ok_or(AppendError::ShardNotFound)?;

    let current_epoch = s_row.get::<_, i64>("assignment_epoch") as u64;
    if current_epoch != expected_epoch {
        return Err(AppendError::EpochMismatch {
            expected: expected_epoch,
            current: current_epoch,
        });
    }

    Ok((
        current_epoch,
        s_row.get::<_, i64>("next_command_sequence") as u64,
    ))
}

// ---------------------------------------------------------------------------
// PostgresAppendStore
// ---------------------------------------------------------------------------

pub struct PostgresAppendStore {
    client: Arc<Mutex<tokio_postgres::Client>>,
}

impl PostgresAppendStore {
    pub async fn new(
        client: Arc<Mutex<tokio_postgres::Client>>,
    ) -> Result<Self, tokio_postgres::Error> {
        {
            let c = client.lock().await;
            c.batch_execute(DDL).await?;
        }
        Ok(Self { client })
    }

    // -----------------------------------------------------------------------
    // BatchPush transaction flow (TD-002 §BatchPush)
    // -----------------------------------------------------------------------
    //
    // 1. Load queue definition (priority model, retention config).
    // 2. Lock shard row FOR UPDATE; validate assignment_epoch.
    // 3. For each item: INSERT ON CONFLICT (client_item_key) DO NOTHING.
    //    - New row  → outcome = New.
    //    - Conflict → fetch existing item_id, outcome = Duplicate (no mutation).
    // 4. Insert key-retention record for each new item.
    // 5. Allocate command sequence and write pqueue_commands row.
    // 6. Advance shard next_command_sequence.
    // 7. COMMIT.

    pub async fn batch_push(
        &self,
        req: PgBatchPushRequest,
    ) -> Result<PgBatchPushResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        // Step 1: load queue definition
        let q_row = tx
            .query_opt(
                "SELECT priority_model, client_item_key_retention_ms, request_id_retention_ms
                 FROM pqueue_queues
                 WHERE tenant_id = $1 AND queue_id = $2",
                &[&req.tenant_id, &req.queue_id],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::QueueNotFound)?;

        let pm_json: Value = q_row.get("priority_model");
        let retention_ms: i64 = q_row.get("client_item_key_retention_ms");
        let request_retention_ms: i64 = q_row.get("request_id_retention_ms");
        let pm = json_priority_model(&pm_json).map_err(|e| AppendError::StorageFailure(e.0))?;

        let request_fingerprint = req
            .request_id
            .as_ref()
            .map(|_| push_request_fingerprint(&req));
        if let (Some(request_id), Some(fingerprint)) =
            (req.request_id.as_ref(), request_fingerprint.as_ref())
        {
            if let Some(row) = tx
                .query_opt(
                    "SELECT request_fingerprint, response_payload
                     FROM pqueue_request_idempotency
                     WHERE tenant_id = $1
                       AND queue_id = $2
                       AND operation = 'batch_push'
                       AND request_id = $3
                       AND expires_at > $4
                     FOR UPDATE",
                    &[
                        &req.tenant_id,
                        &req.queue_id,
                        request_id,
                        &utc_to_odt(&req.now),
                    ],
                )
                .await
                .map_err(to_append_err)?
            {
                let stored_fingerprint: Vec<u8> = row.get("request_fingerprint");
                if stored_fingerprint != *fingerprint {
                    return Err(AppendError::RequestConflict);
                }
                let response_payload: Value = row.get("response_payload");
                return json_to_push_result(&response_payload);
            }
        }

        // Step 2: lock shard, validate epoch, read sequence
        let s_row = tx
            .query_opt(
                "SELECT assignment_epoch, next_command_sequence
                 FROM pqueue_shards
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3
                 FOR UPDATE",
                &[&req.tenant_id, &req.queue_id, &(req.shard_id as i32)],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::ShardNotFound)?;

        let current_epoch = s_row.get::<_, i64>("assignment_epoch") as u64;
        if current_epoch != req.expected_epoch {
            return Err(AppendError::EpochMismatch {
                expected: req.expected_epoch,
                current: current_epoch,
            });
        }
        let sequence = s_row.get::<_, i64>("next_command_sequence") as u64;

        let now_odt = utc_to_odt(&req.now);
        let expires_odt = now_odt + time::Duration::milliseconds(retention_ms);

        let mut item_results: Vec<PgPushItemResult> = Vec::new();
        let mut new_item_ids: Vec<String> = Vec::new();
        let mut affected_group_keys: Vec<Option<String>> = Vec::new();

        // Steps 3 + 4: per-item insert
        for item in &req.items {
            let pv = effective_priority(item.priority.as_ref(), &pm, &req.now);
            let pv_json = priority_value_to_json(&pv);
            let ps: Vec<u8> = compute_priority_sort(&pv, &pm);
            let not_before_odt: Option<OffsetDateTime> = item.not_before.as_ref().map(utc_to_odt);
            let elig_odt = eligible_since_odt(item.not_before.as_ref(), &req.now);

            let inserted = tx
                .execute(
                    "INSERT INTO pqueue_items (
                         tenant_id, queue_id, shard_id, item_id, client_item_key,
                         lifecycle_state, priority, priority_sort, not_before,
                         eligible_since, group_key, gate_keys, payload, metadata,
                         retry_count, item_version, last_command_sequence,
                         created_at, updated_at
                     ) VALUES (
                         $1, $2, $3, $4, $5,
                         'pending', $6, $7, $8,
                         $9, $10, $11, $12, '{}'::jsonb,
                         0, 1, $13,
                         $14, $14
                     ) ON CONFLICT (tenant_id, queue_id, client_item_key) DO NOTHING",
                    &[
                        &req.tenant_id,
                        &req.queue_id,
                        &(req.shard_id as i32),
                        &item.item_id,
                        &item.client_item_key,
                        &pv_json,
                        &ps,
                        &not_before_odt,
                        &elig_odt,
                        &item.group_key,
                        &item.gate_keys,
                        &item.payload,
                        &(sequence as i64),
                        &now_odt,
                    ],
                )
                .await
                .map_err(to_append_err)?;

            if inserted == 1 {
                new_item_ids.push(item.item_id.clone());
                affected_group_keys.push(item.group_key.clone());

                tx.execute(
                    "INSERT INTO pqueue_item_key_retention
                         (tenant_id, queue_id, client_item_key, item_id, expires_at)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT DO NOTHING",
                    &[
                        &req.tenant_id,
                        &req.queue_id,
                        &item.client_item_key,
                        &item.item_id,
                        &expires_odt,
                    ],
                )
                .await
                .map_err(to_append_err)?;

                item_results.push(PgPushItemResult {
                    client_item_key: item.client_item_key.clone(),
                    item_id: item.item_id.clone(),
                    outcome: PgPushOutcome::New { item_version: 1 },
                });
            } else {
                // AC-CORE-3: duplicate client_item_key → converge without mutation
                let dup = tx
                    .query_one(
                        "SELECT item_id FROM pqueue_items
                         WHERE tenant_id = $1 AND queue_id = $2 AND client_item_key = $3",
                        &[&req.tenant_id, &req.queue_id, &item.client_item_key],
                    )
                    .await
                    .map_err(to_append_err)?;
                let existing_id: String = dup.get("item_id");
                item_results.push(PgPushItemResult {
                    client_item_key: item.client_item_key.clone(),
                    item_id: item.item_id.clone(),
                    outcome: PgPushOutcome::Duplicate {
                        existing_item_id: existing_id,
                    },
                });
            }
        }

        refresh_group_summaries(
            &tx,
            &req.tenant_id,
            &req.queue_id,
            req.shard_id,
            &affected_group_keys,
            &now_odt,
        )
        .await?;

        // Step 5: write command record
        let checksum = vec![0u8; 4];
        let cmd_payload = json!({
            "kind": "batch_push",
            "item_count": req.items.len(),
            "new_count": new_item_ids.len(),
        });
        tx.execute(
            "INSERT INTO pqueue_commands (
                 tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                 command_id, request_id, command_type, item_ids,
                 command_payload, checksum, created_at
             ) VALUES (
                 $1, $2, $3, $4, $5,
                 $6, $7, 'batch_push', $8,
                 $9, $10, $11
             )",
            &[
                &req.tenant_id,
                &req.queue_id,
                &(req.shard_id as i32),
                &(sequence as i64),
                &(current_epoch as i64),
                &req.command_id,
                &req.request_id,
                &new_item_ids,
                &cmd_payload,
                &checksum,
                &now_odt,
            ],
        )
        .await
        .map_err(to_append_err)?;

        // Step 6: advance shard sequence
        tx.execute(
            "UPDATE pqueue_shards
             SET next_command_sequence = next_command_sequence + 1, updated_at = $4
             WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
            &[
                &req.tenant_id,
                &req.queue_id,
                &(req.shard_id as i32),
                &now_odt,
            ],
        )
        .await
        .map_err(to_append_err)?;

        let result = PgBatchPushResult {
            command_sequence: sequence,
            items: item_results,
        };

        if let (Some(request_id), Some(fingerprint)) =
            (req.request_id.as_ref(), request_fingerprint.as_ref())
        {
            let response_payload = push_result_to_json(&result);
            let command_positions = json!({
                "shard_id": req.shard_id,
                "sequence": sequence,
                "assignment_epoch": current_epoch,
            });
            let expires_at = now_odt + time::Duration::milliseconds(request_retention_ms);
            tx.execute(
                "INSERT INTO pqueue_request_idempotency (
                     tenant_id, queue_id, operation, request_id,
                     request_fingerprint, response_payload, command_positions,
                     expires_at, created_at
                 ) VALUES (
                     $1, $2, 'batch_push', $3,
                     $4, $5, $6,
                     $7, $8
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    request_id,
                    fingerprint,
                    &response_payload,
                    &command_positions,
                    &expires_at,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // BatchUpdate transaction flow (TD-002 §BatchUpdate)
    // -----------------------------------------------------------------------
    //
    // 1. Load queue definition (priority model).
    // 2. Lock shard row FOR UPDATE; validate assignment_epoch.
    // 3. For each item_id:
    //    a. SELECT item; if missing → NotFound.
    //    b. If terminal → Terminal outcome.
    //    c. If expected_item_version mismatch → Conflict.
    //    d. UPDATE priority/not_before, recompute priority_sort + eligible_since,
    //       increment item_version, only when lifecycle_state='pending' and no lease.
    // 4. Write command record for successfully updated items.
    // 5. Advance shard sequence.
    // 6. COMMIT.

    pub async fn batch_update(
        &self,
        req: PgBatchUpdateRequest,
    ) -> Result<PgBatchUpdateResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        // Step 1: load queue definition
        let q_row = tx
            .query_opt(
                "SELECT priority_model
                 FROM pqueue_queues
                 WHERE tenant_id = $1 AND queue_id = $2",
                &[&req.tenant_id, &req.queue_id],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::QueueNotFound)?;

        let pm_json: Value = q_row.get("priority_model");
        let pm = json_priority_model(&pm_json).map_err(|e| AppendError::StorageFailure(e.0))?;

        // Step 2: lock shard, validate epoch
        let s_row = tx
            .query_opt(
                "SELECT assignment_epoch, next_command_sequence
                 FROM pqueue_shards
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3
                 FOR UPDATE",
                &[&req.tenant_id, &req.queue_id, &(req.shard_id as i32)],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::ShardNotFound)?;

        let current_epoch = s_row.get::<_, i64>("assignment_epoch") as u64;
        if current_epoch != req.expected_epoch {
            return Err(AppendError::EpochMismatch {
                expected: req.expected_epoch,
                current: current_epoch,
            });
        }
        let sequence = s_row.get::<_, i64>("next_command_sequence") as u64;

        let now_odt = utc_to_odt(&req.now);
        let mut item_results: Vec<PgUpdateItemResult> = Vec::new();
        let mut updated_item_ids: Vec<String> = Vec::new();
        let mut affected_group_keys: Vec<Option<String>> = Vec::new();

        // Step 3: per-item update
        for item in &req.items {
            let existing = tx
                .query_opt(
                    "SELECT lifecycle_state, item_version, lease_expires_at, group_key
                     FROM pqueue_items
                     WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
                    &[&req.tenant_id, &req.queue_id, &item.item_id],
                )
                .await
                .map_err(to_append_err)?;

            let Some(row) = existing else {
                item_results.push(PgUpdateItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgUpdateOutcome::NotFound,
                });
                continue;
            };

            let state: String = row.get("lifecycle_state");
            let group_key: Option<String> = row.get("group_key");
            if state == "complete" || state == "failed" {
                item_results.push(PgUpdateItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgUpdateOutcome::Terminal,
                });
                continue;
            }

            let current_version = row.get::<_, i64>("item_version") as u64;
            if let Some(expected) = item.expected_item_version {
                if current_version != expected {
                    item_results.push(PgUpdateItemResult {
                        item_id: item.item_id.clone(),
                        outcome: PgUpdateOutcome::Conflict {
                            message: format!(
                                "expected item_version {expected}, found {current_version}"
                            ),
                        },
                    });
                    continue;
                }
            }

            let lease_expires_at: Option<OffsetDateTime> = row.get("lease_expires_at");
            if lease_expires_at.is_some() {
                item_results.push(PgUpdateItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgUpdateOutcome::Conflict {
                        message: "item has an active lease".to_string(),
                    },
                });
                continue;
            }

            // Compute updated priority and eligibility
            let pv = effective_priority(item.priority.as_ref(), &pm, &req.now);
            let pv_json = priority_value_to_json(&pv);
            let ps: Vec<u8> = compute_priority_sort(&pv, &pm);
            let not_before_odt: Option<OffsetDateTime> = item.not_before.as_ref().map(utc_to_odt);
            let elig_odt = eligible_since_odt(item.not_before.as_ref(), &req.now);

            let updated = tx
                .execute(
                    "UPDATE pqueue_items
                     SET priority = $4,
                         priority_sort = $5,
                         not_before = $6,
                         eligible_since = $7,
                         item_version = item_version + 1,
                         last_command_sequence = $8,
                         updated_at = $9
                     WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3
                       AND lifecycle_state = 'pending'
                       AND lease_expires_at IS NULL",
                    &[
                        &req.tenant_id,
                        &req.queue_id,
                        &item.item_id,
                        &pv_json,
                        &ps,
                        &not_before_odt,
                        &elig_odt,
                        &(sequence as i64),
                        &now_odt,
                    ],
                )
                .await
                .map_err(to_append_err)?;

            if updated == 1 {
                let new_version = current_version + 1;
                updated_item_ids.push(item.item_id.clone());
                affected_group_keys.push(group_key);
                item_results.push(PgUpdateItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgUpdateOutcome::Updated {
                        item_version: new_version,
                    },
                });
            } else {
                item_results.push(PgUpdateItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgUpdateOutcome::Conflict {
                        message: "item state changed before update".to_string(),
                    },
                });
            }
        }

        // Step 4: write command record (only if any items were processed)
        if !req.items.is_empty() {
            refresh_group_summaries(
                &tx,
                &req.tenant_id,
                &req.queue_id,
                req.shard_id,
                &affected_group_keys,
                &now_odt,
            )
            .await?;

            let checksum = vec![0u8; 4];
            let cmd_payload = json!({
                "kind": "batch_update",
                "item_count": req.items.len(),
                "updated_count": updated_item_ids.len(),
            });
            tx.execute(
                "INSERT INTO pqueue_commands (
                     tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                     command_id, request_id, command_type, item_ids,
                     command_payload, checksum, created_at
                 ) VALUES (
                     $1, $2, $3, $4, $5,
                     $6, $7, 'batch_update', $8,
                     $9, $10, $11
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(sequence as i64),
                    &(current_epoch as i64),
                    &req.command_id,
                    &req.request_id,
                    &updated_item_ids,
                    &cmd_payload,
                    &checksum,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;

            // Step 5: advance shard sequence
            tx.execute(
                "UPDATE pqueue_shards
                 SET next_command_sequence = next_command_sequence + 1, updated_at = $4
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgBatchUpdateResult {
            command_sequence: sequence,
            items: item_results,
        })
    }

    // -----------------------------------------------------------------------
    // BatchClaim transaction flow (TD-002 §BatchClaim)
    // -----------------------------------------------------------------------
    //
    // This single-shard reference path leases eligible pending items under one
    // transaction using FOR UPDATE SKIP LOCKED. Strict queues use the canonical
    // priority order. Bounded-relaxed queues currently use the same exact order,
    // which is a valid zero-rank-error implementation for the reference mode.

    pub async fn batch_claim(
        &self,
        req: PgBatchClaimRequest,
    ) -> Result<PgBatchClaimResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        let q_exists = tx
            .query_opt(
                "SELECT ordering_mode
                 FROM pqueue_queues
                 WHERE tenant_id = $1 AND queue_id = $2",
                &[&req.tenant_id, &req.queue_id],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::QueueNotFound)?;
        let _ordering_mode: String = q_exists.get("ordering_mode");

        let s_row = tx
            .query_opt(
                "SELECT assignment_epoch, next_command_sequence
                 FROM pqueue_shards
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3
                 FOR UPDATE",
                &[&req.tenant_id, &req.queue_id, &(req.shard_id as i32)],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::ShardNotFound)?;

        let current_epoch = s_row.get::<_, i64>("assignment_epoch") as u64;
        if current_epoch != req.expected_epoch {
            return Err(AppendError::EpochMismatch {
                expected: req.expected_epoch,
                current: current_epoch,
            });
        }
        let sequence = s_row.get::<_, i64>("next_command_sequence") as u64;

        let now_odt = utc_to_odt(&req.now);
        let lease_expires_odt = utc_to_odt(&req.lease_expires_at);
        let max_items = req.max_items as i64;

        let rows = tx
            .query(
                "WITH candidates AS (
                     SELECT item_id
                          , group_key
                          , priority_sort
                          , created_at
                     FROM pqueue_items
                     WHERE tenant_id = $1
                       AND queue_id = $2
                       AND shard_id = $3
                       AND lifecycle_state = 'pending'
                       AND eligible_since IS NOT NULL
                       AND (not_before IS NULL OR not_before <= $4)
                       AND NOT EXISTS (
                         SELECT 1
                         FROM unnest(gate_keys) AS g(gate_key)
                         JOIN pqueue_gate_state gs
                           ON gs.tenant_id = pqueue_items.tenant_id
                          AND gs.queue_id = pqueue_items.queue_id
                          AND gs.shard_id = pqueue_items.shard_id
                          AND gs.gate_key = g.gate_key
                          AND gs.state = 'blocked'
                       )
                     ORDER BY priority_sort ASC, created_at ASC, item_id ASC
                     LIMIT $5
                     FOR UPDATE SKIP LOCKED
                 ),
                 updated AS (
                     UPDATE pqueue_items i
                     SET lifecycle_state = 'leased',
                         lease_token_hash = $6,
                         lease_expires_at = $7,
                         item_version = item_version + 1,
                         last_command_sequence = $8,
                         updated_at = $4
                     FROM candidates c
                     WHERE i.tenant_id = $1
                       AND i.queue_id = $2
                       AND i.shard_id = $3
                       AND i.item_id = c.item_id
                     RETURNING i.item_id
                 )
                 SELECT u.item_id, c.group_key
                 FROM updated u
                 JOIN candidates c ON c.item_id = u.item_id
                 ORDER BY c.priority_sort ASC, c.created_at ASC, c.item_id ASC",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                    &max_items,
                    &lease_token_hash(&req.lease_token),
                    &lease_expires_odt,
                    &(sequence as i64),
                ],
            )
            .await
            .map_err(to_append_err)?;

        let claimed_item_ids: Vec<String> = rows.iter().map(|row| row.get("item_id")).collect();
        let affected_group_keys: Vec<Option<String>> =
            rows.iter().map(|row| row.get("group_key")).collect();

        if !claimed_item_ids.is_empty() {
            refresh_group_summaries(
                &tx,
                &req.tenant_id,
                &req.queue_id,
                req.shard_id,
                &affected_group_keys,
                &now_odt,
            )
            .await?;

            let checksum = vec![0u8; 4];
            let cmd_payload = json!({
                "kind": "batch_claim",
                "claimed_count": claimed_item_ids.len(),
                "lease_expires_at": req.lease_expires_at.seconds,
            });
            tx.execute(
                "INSERT INTO pqueue_commands (
                     tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                     command_id, request_id, command_type, item_ids,
                     command_payload, checksum, created_at
                 ) VALUES (
                     $1, $2, $3, $4, $5,
                     $6, $7, 'batch_claim', $8,
                     $9, $10, $11
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(sequence as i64),
                    &(current_epoch as i64),
                    &req.command_id,
                    &req.request_id,
                    &claimed_item_ids,
                    &cmd_payload,
                    &checksum,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;

            tx.execute(
                "UPDATE pqueue_shards
                 SET next_command_sequence = next_command_sequence + 1, updated_at = $4
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgBatchClaimResult {
            command_sequence: sequence,
            claimed_item_ids,
        })
    }

    pub async fn batch_renew_leases(
        &self,
        req: PgBatchRenewLeasesRequest,
    ) -> Result<PgBatchRenewLeasesResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        tx.query_opt(
            "SELECT 1 FROM pqueue_queues WHERE tenant_id = $1 AND queue_id = $2",
            &[&req.tenant_id, &req.queue_id],
        )
        .await
        .map_err(to_append_err)?
        .ok_or(AppendError::QueueNotFound)?;

        let (current_epoch, sequence) = lock_shard_sequence(
            &tx,
            &req.tenant_id,
            &req.queue_id,
            req.shard_id,
            req.expected_epoch,
        )
        .await?;

        let now_odt = utc_to_odt(&req.now);
        let lease_expires_odt = utc_to_odt(&req.lease_expires_at);
        let mut item_results = Vec::new();
        let mut renewed_item_ids = Vec::new();

        for item in &req.items {
            let row = tx
                .query_opt(
                    "SELECT lifecycle_state, lease_expires_at, item_version
                     FROM pqueue_items
                     WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3
                     FOR UPDATE",
                    &[&req.tenant_id, &req.queue_id, &item.item_id],
                )
                .await
                .map_err(to_append_err)?;

            let Some(row) = row else {
                item_results.push(PgRenewLeaseItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgRenewLeaseOutcome::NotFound,
                });
                continue;
            };

            let state: String = row.get("lifecycle_state");
            if state == "complete" || state == "failed" {
                item_results.push(PgRenewLeaseItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgRenewLeaseOutcome::Terminal,
                });
                continue;
            }

            let updated = tx
                .execute(
                    "UPDATE pqueue_items
                     SET lease_expires_at = $5,
                         item_version = item_version + 1,
                         last_command_sequence = $6,
                         updated_at = $7
                     WHERE tenant_id = $1
                       AND queue_id = $2
                       AND item_id = $3
                       AND lifecycle_state = 'leased'
                       AND lease_token_hash = $4
                       AND lease_expires_at > $7",
                    &[
                        &req.tenant_id,
                        &req.queue_id,
                        &item.item_id,
                        &lease_token_hash(&item.lease_token),
                        &lease_expires_odt,
                        &(sequence as i64),
                        &now_odt,
                    ],
                )
                .await
                .map_err(to_append_err)?;

            if updated == 1 {
                let item_version = row.get::<_, i64>("item_version") as u64 + 1;
                renewed_item_ids.push(item.item_id.clone());
                item_results.push(PgRenewLeaseItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgRenewLeaseOutcome::Renewed { item_version },
                });
            } else {
                item_results.push(PgRenewLeaseItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgRenewLeaseOutcome::StaleLease,
                });
            }
        }

        if !renewed_item_ids.is_empty() {
            let checksum = vec![0u8; 4];
            let cmd_payload = json!({
                "kind": "batch_renew_leases",
                "renewed_count": renewed_item_ids.len(),
                "lease_expires_at": req.lease_expires_at.seconds,
            });
            tx.execute(
                "INSERT INTO pqueue_commands (
                     tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                     command_id, request_id, command_type, item_ids,
                     command_payload, checksum, created_at
                 ) VALUES (
                     $1, $2, $3, $4, $5,
                     $6, $7, 'batch_renew_leases', $8,
                     $9, $10, $11
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(sequence as i64),
                    &(current_epoch as i64),
                    &req.command_id,
                    &req.request_id,
                    &renewed_item_ids,
                    &cmd_payload,
                    &checksum,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;

            tx.execute(
                "UPDATE pqueue_shards
                 SET next_command_sequence = next_command_sequence + 1, updated_at = $4
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgBatchRenewLeasesResult {
            command_sequence: sequence,
            items: item_results,
        })
    }

    pub async fn batch_finalize(
        &self,
        req: PgBatchFinalizeRequest,
    ) -> Result<PgBatchFinalizeResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        let q_row = tx
            .query_opt(
                "SELECT retry_policy
                 FROM pqueue_queues
                 WHERE tenant_id = $1 AND queue_id = $2",
                &[&req.tenant_id, &req.queue_id],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::QueueNotFound)?;
        let retry_policy: Value = q_row.get("retry_policy");
        let max_attempts = retry_policy["max_attempts"].as_u64().unwrap_or(1) as i32;

        let (current_epoch, sequence) = lock_shard_sequence(
            &tx,
            &req.tenant_id,
            &req.queue_id,
            req.shard_id,
            req.expected_epoch,
        )
        .await?;

        let now_odt = utc_to_odt(&req.now);
        let mut item_results = Vec::new();
        let mut finalized_item_ids = Vec::new();
        let mut affected_group_keys = Vec::new();

        for item in &req.items {
            let row = tx
                .query_opt(
                    "SELECT lifecycle_state, lease_expires_at, item_version, retry_count, group_key
                     FROM pqueue_items
                     WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3
                     FOR UPDATE",
                    &[&req.tenant_id, &req.queue_id, &item.item_id],
                )
                .await
                .map_err(to_append_err)?;

            let Some(row) = row else {
                item_results.push(PgFinalizeItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgFinalizeOutcome::NotFound,
                });
                continue;
            };

            let state: String = row.get("lifecycle_state");
            if state == "complete" || state == "failed" {
                item_results.push(PgFinalizeItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgFinalizeOutcome::Terminal,
                });
                continue;
            }

            let lease_valid = tx
                .query_opt(
                    "SELECT 1
                     FROM pqueue_items
                     WHERE tenant_id = $1
                       AND queue_id = $2
                       AND item_id = $3
                       AND lifecycle_state = 'leased'
                       AND lease_token_hash = $4
                       AND lease_expires_at > $5",
                    &[
                        &req.tenant_id,
                        &req.queue_id,
                        &item.item_id,
                        &lease_token_hash(&item.lease_token),
                        &now_odt,
                    ],
                )
                .await
                .map_err(to_append_err)?
                .is_some();

            if !lease_valid {
                item_results.push(PgFinalizeItemResult {
                    item_id: item.item_id.clone(),
                    outcome: PgFinalizeOutcome::StaleLease,
                });
                continue;
            }

            let current_version = row.get::<_, i64>("item_version") as u64;
            let retry_count = row.get::<_, i32>("retry_count");
            let group_key: Option<String> = row.get("group_key");
            let item_version = current_version + 1;

            let outcome = match item.kind {
                PgFinalizeKind::Complete => {
                    tx.execute(
                        "UPDATE pqueue_items
                         SET lifecycle_state = 'complete',
                             lease_token_hash = NULL,
                             lease_expires_at = NULL,
                             eligible_since = NULL,
                             item_version = item_version + 1,
                             last_command_sequence = $4,
                             terminal_at = $5,
                             updated_at = $5
                         WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
                        &[
                            &req.tenant_id,
                            &req.queue_id,
                            &item.item_id,
                            &(sequence as i64),
                            &now_odt,
                        ],
                    )
                    .await
                    .map_err(to_append_err)?;
                    PgFinalizeOutcome::Completed { item_version }
                }
                PgFinalizeKind::Fail => {
                    tx.execute(
                        "UPDATE pqueue_items
                         SET lifecycle_state = 'failed',
                             lease_token_hash = NULL,
                             lease_expires_at = NULL,
                             eligible_since = NULL,
                             item_version = item_version + 1,
                             last_command_sequence = $4,
                             terminal_at = $5,
                             updated_at = $5
                         WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
                        &[
                            &req.tenant_id,
                            &req.queue_id,
                            &item.item_id,
                            &(sequence as i64),
                            &now_odt,
                        ],
                    )
                    .await
                    .map_err(to_append_err)?;
                    PgFinalizeOutcome::Failed { item_version }
                }
                PgFinalizeKind::Retry => {
                    let Some(not_before) = item.retry_not_before.as_ref() else {
                        item_results.push(PgFinalizeItemResult {
                            item_id: item.item_id.clone(),
                            outcome: PgFinalizeOutcome::Invalid {
                                message: "retry_not_before is required for retry".to_string(),
                            },
                        });
                        continue;
                    };
                    let next_retry_count = retry_count + 1;
                    if next_retry_count >= max_attempts {
                        tx.execute(
                            "UPDATE pqueue_items
                             SET lifecycle_state = 'failed',
                                 retry_count = $4,
                                 lease_token_hash = NULL,
                                 lease_expires_at = NULL,
                                 eligible_since = NULL,
                                 item_version = item_version + 1,
                                 last_command_sequence = $5,
                                 terminal_at = $6,
                                 updated_at = $6
                             WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
                            &[
                                &req.tenant_id,
                                &req.queue_id,
                                &item.item_id,
                                &next_retry_count,
                                &(sequence as i64),
                                &now_odt,
                            ],
                        )
                        .await
                        .map_err(to_append_err)?;
                        PgFinalizeOutcome::Failed { item_version }
                    } else {
                        let not_before_odt = utc_to_odt(not_before);
                        tx.execute(
                            "UPDATE pqueue_items
                             SET lifecycle_state = 'pending',
                                 retry_count = $4,
                                 not_before = $5,
                                 eligible_since = $6,
                                 lease_token_hash = NULL,
                                 lease_expires_at = NULL,
                                 item_version = item_version + 1,
                                 last_command_sequence = $7,
                                 updated_at = $8
                             WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
                            &[
                                &req.tenant_id,
                                &req.queue_id,
                                &item.item_id,
                                &next_retry_count,
                                &not_before_odt,
                                &not_before_odt,
                                &(sequence as i64),
                                &now_odt,
                            ],
                        )
                        .await
                        .map_err(to_append_err)?;
                        PgFinalizeOutcome::Retried { item_version }
                    }
                }
                PgFinalizeKind::Release => {
                    tx.execute(
                        "UPDATE pqueue_items
                         SET lifecycle_state = 'pending',
                             eligible_since = COALESCE(eligible_since, $4),
                             lease_token_hash = NULL,
                             lease_expires_at = NULL,
                             item_version = item_version + 1,
                             last_command_sequence = $5,
                             updated_at = $4
                         WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
                        &[
                            &req.tenant_id,
                            &req.queue_id,
                            &item.item_id,
                            &now_odt,
                            &(sequence as i64),
                        ],
                    )
                    .await
                    .map_err(to_append_err)?;
                    PgFinalizeOutcome::Released { item_version }
                }
            };

            finalized_item_ids.push(item.item_id.clone());
            affected_group_keys.push(group_key);
            item_results.push(PgFinalizeItemResult {
                item_id: item.item_id.clone(),
                outcome,
            });
        }

        if !finalized_item_ids.is_empty() {
            refresh_group_summaries(
                &tx,
                &req.tenant_id,
                &req.queue_id,
                req.shard_id,
                &affected_group_keys,
                &now_odt,
            )
            .await?;

            let checksum = vec![0u8; 4];
            let cmd_payload = json!({
                "kind": "batch_finalize",
                "finalized_count": finalized_item_ids.len(),
            });
            tx.execute(
                "INSERT INTO pqueue_commands (
                     tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                     command_id, request_id, command_type, item_ids,
                     command_payload, checksum, created_at
                 ) VALUES (
                     $1, $2, $3, $4, $5,
                     $6, $7, 'batch_finalize', $8,
                     $9, $10, $11
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(sequence as i64),
                    &(current_epoch as i64),
                    &req.command_id,
                    &req.request_id,
                    &finalized_item_ids,
                    &cmd_payload,
                    &checksum,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;

            tx.execute(
                "UPDATE pqueue_shards
                 SET next_command_sequence = next_command_sequence + 1, updated_at = $4
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgBatchFinalizeResult {
            command_sequence: sequence,
            items: item_results,
        })
    }

    pub async fn materialize_expired_leases(
        &self,
        req: PgLeaseExpiredRequest,
    ) -> Result<PgLeaseExpiredResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        tx.query_opt(
            "SELECT 1 FROM pqueue_queues WHERE tenant_id = $1 AND queue_id = $2",
            &[&req.tenant_id, &req.queue_id],
        )
        .await
        .map_err(to_append_err)?
        .ok_or(AppendError::QueueNotFound)?;

        let (current_epoch, sequence) = lock_shard_sequence(
            &tx,
            &req.tenant_id,
            &req.queue_id,
            req.shard_id,
            req.expected_epoch,
        )
        .await?;

        let now_odt = utc_to_odt(&req.now);
        let max_items = req.max_items as i64;
        let rows = tx
            .query(
                "WITH candidates AS (
                     SELECT item_id, group_key
                     FROM pqueue_items
                     WHERE tenant_id = $1
                       AND queue_id = $2
                       AND shard_id = $3
                       AND lifecycle_state = 'leased'
                       AND lease_expires_at <= $4
                     ORDER BY lease_expires_at ASC, item_id ASC
                     LIMIT $5
                     FOR UPDATE SKIP LOCKED
                 ),
                 updated AS (
                     UPDATE pqueue_items i
                     SET lifecycle_state = 'pending',
                         lease_token_hash = NULL,
                         lease_expires_at = NULL,
                         eligible_since = COALESCE(eligible_since, $4),
                         item_version = item_version + 1,
                         last_command_sequence = $6,
                         updated_at = $4
                     FROM candidates c
                     WHERE i.tenant_id = $1
                       AND i.queue_id = $2
                       AND i.shard_id = $3
                       AND i.item_id = c.item_id
                     RETURNING i.item_id
                 )
                 SELECT u.item_id, c.group_key
                 FROM updated u
                 JOIN candidates c ON c.item_id = u.item_id
                 ORDER BY c.item_id ASC",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                    &max_items,
                    &(sequence as i64),
                ],
            )
            .await
            .map_err(to_append_err)?;

        let expired_item_ids: Vec<String> = rows.iter().map(|row| row.get("item_id")).collect();
        let affected_group_keys: Vec<Option<String>> =
            rows.iter().map(|row| row.get("group_key")).collect();

        if !expired_item_ids.is_empty() {
            refresh_group_summaries(
                &tx,
                &req.tenant_id,
                &req.queue_id,
                req.shard_id,
                &affected_group_keys,
                &now_odt,
            )
            .await?;

            let checksum = vec![0u8; 4];
            let cmd_payload = json!({
                "kind": "lease_expired",
                "expired_count": expired_item_ids.len(),
            });
            tx.execute(
                "INSERT INTO pqueue_commands (
                     tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                     command_id, request_id, command_type, item_ids,
                     command_payload, checksum, created_at
                 ) VALUES (
                     $1, $2, $3, $4, $5,
                     $6, $7, 'lease_expired', $8,
                     $9, $10, $11
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(sequence as i64),
                    &(current_epoch as i64),
                    &req.command_id,
                    &req.request_id,
                    &expired_item_ids,
                    &cmd_payload,
                    &checksum,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;

            tx.execute(
                "UPDATE pqueue_shards
                 SET next_command_sequence = next_command_sequence + 1, updated_at = $4
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgLeaseExpiredResult {
            command_sequence: sequence,
            expired_item_ids,
        })
    }

    pub async fn set_gates(&self, req: PgSetGatesRequest) -> Result<PgSetGatesResult, AppendError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_append_err)?;

        tx.query_opt(
            "SELECT 1 FROM pqueue_queues WHERE tenant_id = $1 AND queue_id = $2",
            &[&req.tenant_id, &req.queue_id],
        )
        .await
        .map_err(to_append_err)?
        .ok_or(AppendError::QueueNotFound)?;

        let (current_epoch, sequence) = lock_shard_sequence(
            &tx,
            &req.tenant_id,
            &req.queue_id,
            req.shard_id,
            req.expected_epoch,
        )
        .await?;

        let now_odt = utc_to_odt(&req.now);
        let mut canonical_gates = req.gates;
        canonical_gates.sort_by(|a, b| a.gate_key.cmp(&b.gate_key));
        canonical_gates.dedup_by(|a, b| a.gate_key == b.gate_key);

        for gate in &canonical_gates {
            let state = match gate.state {
                PgGateState::Open => "open",
                PgGateState::Blocked => "blocked",
            };
            tx.execute(
                "INSERT INTO pqueue_gate_state (
                     tenant_id, queue_id, shard_id, gate_key, state, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (tenant_id, queue_id, shard_id, gate_key) DO UPDATE SET
                   state = EXCLUDED.state,
                   updated_at = EXCLUDED.updated_at",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &gate.gate_key,
                    &state,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        if !canonical_gates.is_empty() {
            let gate_payload: Vec<Value> = canonical_gates
                .iter()
                .map(|gate| {
                    json!({
                        "gate_key": gate.gate_key,
                        "state": match gate.state {
                            PgGateState::Open => "open",
                            PgGateState::Blocked => "blocked",
                        },
                    })
                })
                .collect();
            let checksum = vec![0u8; 4];
            let cmd_payload = json!({
                "kind": "set_gates",
                "gates": gate_payload,
            });
            let item_ids: Vec<String> = Vec::new();
            tx.execute(
                "INSERT INTO pqueue_commands (
                     tenant_id, queue_id, shard_id, sequence, assignment_epoch,
                     command_id, request_id, command_type, item_ids,
                     command_payload, checksum, created_at
                 ) VALUES (
                     $1, $2, $3, $4, $5,
                     $6, $7, 'set_gates', $8,
                     $9, $10, $11
                 )",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(sequence as i64),
                    &(current_epoch as i64),
                    &req.command_id,
                    &req.request_id,
                    &item_ids,
                    &cmd_payload,
                    &checksum,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;

            tx.execute(
                "UPDATE pqueue_shards
                 SET next_command_sequence = next_command_sequence + 1, updated_at = $4
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &now_odt,
                ],
            )
            .await
            .map_err(to_append_err)?;
        }

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgSetGatesResult {
            command_sequence: sequence,
            gates: canonical_gates,
        })
    }
}
