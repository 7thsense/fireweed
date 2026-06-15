use std::sync::Arc;

use pqueue_core::{QueueDefinition, QueueId, TenantId, UtcTimestamp};
use pqueue_storage::{
    traits::{ControlPlaneError, ControlPlaneStore, CreateQueueResult, ShardAssignment},
    types::{QueueKey, ShardId, ShardKey},
};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;

use crate::{
    convert::{
        cohort_policy_to_json, eligibility_policy_to_json, ordering_mode_str,
        priority_model_to_json, recurrence_to_json, retry_policy_to_json, row_to_definition,
    },
    schema::DDL,
};

pub struct PostgresControlPlaneStore {
    client: Arc<Mutex<tokio_postgres::Client>>,
}

#[derive(Debug)]
pub struct PgRegisterOwnerRequest {
    pub owner_id: String,
    pub heartbeat_ttl_ms: u64,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgShardLeaseRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub owner_id: String,
    pub lease_ttl_ms: u64,
    pub now: UtcTimestamp,
}

#[derive(Debug)]
pub struct PgEpochShardLeaseRequest {
    pub tenant_id: String,
    pub queue_id: String,
    pub shard_id: u32,
    pub owner_id: String,
    pub expected_epoch: u64,
    pub lease_ttl_ms: u64,
    pub now: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgShardLeaseResult {
    pub acquired: bool,
    pub assignment_epoch: u64,
    pub active_owner_id: Option<String>,
    pub lease_expires_at: Option<UtcTimestamp>,
}

fn utc_to_odt(ts: &UtcTimestamp) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(ts.seconds).unwrap()
        + time::Duration::nanoseconds(ts.nanoseconds as i64)
}

fn odt_to_utc(ts: OffsetDateTime) -> UtcTimestamp {
    UtcTimestamp::new(ts.unix_timestamp(), ts.nanosecond()).unwrap()
}

impl PostgresControlPlaneStore {
    pub async fn new(
        client: Arc<Mutex<tokio_postgres::Client>>,
    ) -> Result<Self, tokio_postgres::Error> {
        {
            let c = client.lock().await;
            c.batch_execute(DDL).await?;
        }
        Ok(Self { client })
    }

    pub async fn register_owner(
        &self,
        req: PgRegisterOwnerRequest,
    ) -> Result<(), ControlPlaneError> {
        let client = self.client.lock().await;
        let now_odt = utc_to_odt(&req.now);
        client
            .execute(
                "INSERT INTO pqueue_workers
                     (owner_id, heartbeat_at, heartbeat_ttl_ms, updated_at)
                 VALUES ($1, $2, $3, $2)
                 ON CONFLICT (owner_id)
                 DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at,
                               heartbeat_ttl_ms = EXCLUDED.heartbeat_ttl_ms,
                               updated_at = EXCLUDED.updated_at",
                &[&req.owner_id, &now_odt, &(req.heartbeat_ttl_ms as i64)],
            )
            .await
            .map_err(to_storage_err)?;
        Ok(())
    }

    pub async fn acquire_shard_lease(
        &self,
        req: PgShardLeaseRequest,
    ) -> Result<PgShardLeaseResult, ControlPlaneError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_storage_err)?;
        let now_odt = utc_to_odt(&req.now);
        let lease_expires_at = now_odt + time::Duration::milliseconds(req.lease_ttl_ms as i64);

        let row = tx
            .query_opt(
                "SELECT assignment_epoch, active_owner_id, lease_expires_at
                 FROM pqueue_shards
                 WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3
                 FOR UPDATE",
                &[&req.tenant_id, &req.queue_id, &(req.shard_id as i32)],
            )
            .await
            .map_err(to_storage_err)?
            .ok_or(ControlPlaneError::QueueNotFound)?;

        let current_epoch = row.get::<_, i64>("assignment_epoch") as u64;
        let active_owner_id: Option<String> = row.get("active_owner_id");
        let current_expires: Option<OffsetDateTime> = row.get("lease_expires_at");
        if active_owner_id.as_deref() != Some(req.owner_id.as_str())
            && current_expires.is_some_and(|expires| expires > now_odt)
        {
            tx.commit().await.map_err(to_storage_err)?;
            return Ok(PgShardLeaseResult {
                acquired: false,
                assignment_epoch: current_epoch,
                active_owner_id,
                lease_expires_at: current_expires.map(odt_to_utc),
            });
        }

        let new_epoch = if active_owner_id.as_deref() == Some(req.owner_id.as_str())
            && current_expires.is_some_and(|expires| expires > now_odt)
        {
            current_epoch
        } else {
            current_epoch + 1
        };
        tx.execute(
            "UPDATE pqueue_shards
             SET assignment_epoch = $4,
                 active_owner_id = $5,
                 target_owner_id = $5,
                 lease_expires_at = $6,
                 state = 'assigned',
                 updated_at = $7
             WHERE tenant_id = $1 AND queue_id = $2 AND shard_id = $3",
            &[
                &req.tenant_id,
                &req.queue_id,
                &(req.shard_id as i32),
                &(new_epoch as i64),
                &req.owner_id,
                &lease_expires_at,
                &now_odt,
            ],
        )
        .await
        .map_err(to_storage_err)?;
        tx.commit().await.map_err(to_storage_err)?;
        Ok(PgShardLeaseResult {
            acquired: true,
            assignment_epoch: new_epoch,
            active_owner_id: Some(req.owner_id),
            lease_expires_at: Some(odt_to_utc(lease_expires_at)),
        })
    }

    pub async fn renew_shard_lease(
        &self,
        req: PgEpochShardLeaseRequest,
    ) -> Result<PgShardLeaseResult, ControlPlaneError> {
        let client = self.client.lock().await;
        let now_odt = utc_to_odt(&req.now);
        let lease_expires_at = now_odt + time::Duration::milliseconds(req.lease_ttl_ms as i64);
        let updated = client
            .execute(
                "UPDATE pqueue_shards
                 SET lease_expires_at = $6,
                     updated_at = $7
                 WHERE tenant_id = $1
                   AND queue_id = $2
                   AND shard_id = $3
                   AND assignment_epoch = $4
                   AND active_owner_id = $5
                   AND lease_expires_at > $7",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(req.expected_epoch as i64),
                    &req.owner_id,
                    &lease_expires_at,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_storage_err)?;
        Ok(PgShardLeaseResult {
            acquired: updated == 1,
            assignment_epoch: req.expected_epoch,
            active_owner_id: (updated == 1).then_some(req.owner_id),
            lease_expires_at: (updated == 1).then_some(odt_to_utc(lease_expires_at)),
        })
    }

    pub async fn release_shard_lease(
        &self,
        req: PgEpochShardLeaseRequest,
    ) -> Result<PgShardLeaseResult, ControlPlaneError> {
        let client = self.client.lock().await;
        let now_odt = utc_to_odt(&req.now);
        let updated = client
            .execute(
                "UPDATE pqueue_shards
                 SET active_owner_id = NULL,
                     lease_expires_at = NULL,
                     state = 'unassigned',
                     updated_at = $6
                 WHERE tenant_id = $1
                   AND queue_id = $2
                   AND shard_id = $3
                   AND assignment_epoch = $4
                   AND active_owner_id = $5",
                &[
                    &req.tenant_id,
                    &req.queue_id,
                    &(req.shard_id as i32),
                    &(req.expected_epoch as i64),
                    &req.owner_id,
                    &now_odt,
                ],
            )
            .await
            .map_err(to_storage_err)?;
        Ok(PgShardLeaseResult {
            acquired: updated == 1,
            assignment_epoch: req.expected_epoch,
            active_owner_id: None,
            lease_expires_at: None,
        })
    }
}

fn to_storage_err(e: tokio_postgres::Error) -> ControlPlaneError {
    ControlPlaneError::StorageFailure(e.to_string())
}

fn is_unique_violation(e: &tokio_postgres::Error) -> bool {
    e.code() == Some(&SqlState::UNIQUE_VIOLATION)
}

impl ControlPlaneStore for PostgresControlPlaneStore {
    async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> Result<CreateQueueResult, ControlPlaneError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(to_storage_err)?;

        let pm = priority_model_to_json(&definition.priority_model);
        let ep = eligibility_policy_to_json(&definition.eligibility_policy);
        let rp = retry_policy_to_json(&definition.retry_policy);
        let cp = cohort_policy_to_json(definition.cohort_policy.as_ref());
        let rec = recurrence_to_json(&definition.recurrence);
        let om = ordering_mode_str(definition.ordering_mode).to_owned();
        let recurring = definition.recurrence.mode == pqueue_core::RecurrenceMode::Recurring;

        let insert = tx
            .execute(
                "INSERT INTO pqueue_queues (
                    tenant_id, queue_id, priority_model, ordering_mode,
                    group_co_residency, recurring, progress_bound_ms,
                    eligibility_policy, request_id_retention_ms,
                    client_item_key_retention_ms, max_lease_duration_ms,
                    retry_policy, max_push_batch_size, max_claim_batch_size,
                    max_eligible_group_size, cohort_policy, recurrence_policy,
                    shard_count
                ) VALUES (
                    $1, $2, $3::jsonb, $4,
                    $5, $6, $7,
                    $8::jsonb, $9,
                    $10, $11,
                    $12::jsonb, $13, $14,
                    $15, $16::jsonb, $17::jsonb,
                    $18
                )",
                &[
                    &definition.tenant_id.as_str(),
                    &definition.queue_id.as_str(),
                    &pm,
                    &om,
                    &definition.group_co_residency,
                    &recurring,
                    &(definition.progress_bound_ms as i64),
                    &ep,
                    &(definition.request_id_retention_ms as i64),
                    &(definition.client_item_key_retention_ms as i64),
                    &(definition.max_lease_duration_ms as i64),
                    &rp,
                    &(definition.max_push_batch_size as i64),
                    &(definition.max_claim_batch_size as i64),
                    &definition.max_eligible_group_size.map(|v| v as i64),
                    &cp,
                    &rec,
                    &(definition.shard_count as i32),
                ],
            )
            .await;

        match insert {
            Ok(_) => {}
            Err(ref e) if is_unique_violation(e) => {
                return Err(ControlPlaneError::QueueAlreadyExists);
            }
            Err(e) => return Err(to_storage_err(e)),
        }

        for shard_idx in 0..definition.shard_count {
            tx.execute(
                "INSERT INTO pqueue_shards (
                    tenant_id, queue_id, shard_id,
                    assignment_epoch, state
                ) VALUES ($1, $2, $3, 1, 'unassigned')",
                &[
                    &definition.tenant_id.as_str(),
                    &definition.queue_id.as_str(),
                    &(shard_idx as i32),
                ],
            )
            .await
            .map_err(to_storage_err)?;
        }

        tx.commit().await.map_err(to_storage_err)?;
        Ok(CreateQueueResult {
            created: true,
            definition,
        })
    }

    async fn queue_definition(&self, key: &QueueKey) -> Result<QueueDefinition, ControlPlaneError> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT tenant_id, queue_id, priority_model, ordering_mode,
                        group_co_residency, progress_bound_ms, eligibility_policy,
                        request_id_retention_ms, client_item_key_retention_ms,
                        max_lease_duration_ms, retry_policy, max_push_batch_size,
                        max_claim_batch_size, max_eligible_group_size,
                        cohort_policy, recurrence_policy, shard_count
                 FROM pqueue_queues
                 WHERE tenant_id = $1 AND queue_id = $2",
                &[&key.tenant_id.as_str(), &key.queue_id.as_str()],
            )
            .await
            .map_err(to_storage_err)?;

        match row {
            None => Err(ControlPlaneError::QueueNotFound),
            Some(r) => row_to_definition(&r).map_err(|e| ControlPlaneError::StorageFailure(e.0)),
        }
    }

    async fn shard_assignments(
        &self,
        key: &QueueKey,
    ) -> Result<Vec<ShardAssignment>, ControlPlaneError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT shard_id, assignment_epoch, active_owner_id
                 FROM pqueue_shards
                 WHERE tenant_id = $1 AND queue_id = $2
                 ORDER BY shard_id",
                &[&key.tenant_id.as_str(), &key.queue_id.as_str()],
            )
            .await
            .map_err(to_storage_err)?;

        if rows.is_empty() {
            return Err(ControlPlaneError::QueueNotFound);
        }

        let assignments = rows
            .iter()
            .map(|row| {
                let shard_id: i32 = row.get("shard_id");
                let epoch: i64 = row.get("assignment_epoch");
                let worker_id: Option<String> = row.get("active_owner_id");
                ShardAssignment {
                    shard_key: ShardKey {
                        tenant_id: key.tenant_id.clone(),
                        queue_id: key.queue_id.clone(),
                        shard_id: ShardId::new(shard_id as u32),
                    },
                    epoch: epoch as u64,
                    worker_id,
                }
            })
            .collect();

        Ok(assignments)
    }

    async fn list_queues(&self, tenant_id: &TenantId) -> Result<Vec<QueueId>, ControlPlaneError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT queue_id FROM pqueue_queues WHERE tenant_id = $1 ORDER BY queue_id",
                &[&tenant_id.as_str()],
            )
            .await
            .map_err(to_storage_err)?;

        let ids = rows
            .iter()
            .map(|row| {
                let id: String = row.get("queue_id");
                QueueId::new(id).expect("stored queue_id must be valid")
            })
            .collect();

        Ok(ids)
    }
}
