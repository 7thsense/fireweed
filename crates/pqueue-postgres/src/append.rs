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
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AppendError {
    EpochMismatch { expected: u64, current: u64 },
    ShardNotFound,
    QueueNotFound,
    StorageFailure(String),
}

fn to_append_err(e: tokio_postgres::Error) -> AppendError {
    AppendError::StorageFailure(e.to_string())
}

fn lease_token_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
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
           SELECT lifecycle_state, eligible_since, not_before, priority_sort, created_at, item_id
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
                "SELECT priority_model, client_item_key_retention_ms
                 FROM pqueue_queues
                 WHERE tenant_id = $1 AND queue_id = $2",
                &[&req.tenant_id, &req.queue_id],
            )
            .await
            .map_err(to_append_err)?
            .ok_or(AppendError::QueueNotFound)?;

        let pm_json: Value = q_row.get("priority_model");
        let retention_ms: i64 = q_row.get("client_item_key_retention_ms");
        let pm = json_priority_model(&pm_json).map_err(|e| AppendError::StorageFailure(e.0))?;

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
                         eligible_since, group_key, payload, metadata,
                         retry_count, item_version, last_command_sequence,
                         created_at, updated_at
                     ) VALUES (
                         $1, $2, $3, $4, $5,
                         'pending', $6, $7, $8,
                         $9, $10, $11, '{}'::jsonb,
                         0, 1, $12,
                         $13, $13
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

        tx.commit().await.map_err(to_append_err)?;
        Ok(PgBatchPushResult {
            command_sequence: sequence,
            items: item_results,
        })
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
}
