//! Memory family factory for the generic async log-replay product.
//!
//! Product ports live once on [`fireweed_engine::AsyncLogReplayBackend`]. This module only
//! opens in-process axes and assembles them. Call sites should type against
//! [`AsyncLogReplayBackend`] (or inference / port traits), not a family product alias.

use fireweed_engine::{AsyncLogReplayBackend, assemble_async_log_replay};
use fireweed_projection::{InMemoryProjection, MemoryLog};

/// Assemble a fresh async-only memory backend (program B migration target).
pub fn async_composed_memory_backend() -> AsyncLogReplayBackend<MemoryLog, InMemoryProjection> {
    async_composed_memory_backend_with_node_id(0)
}

/// Assemble a memory backend that mints item ids with the given node id.
pub fn async_composed_memory_backend_with_node_id(
    node_id: u8,
) -> AsyncLogReplayBackend<MemoryLog, InMemoryProjection> {
    assemble_async_log_replay(MemoryLog::new(), InMemoryProjection::new(), node_id)
        .expect("atomic memory log-replay assemble")
}

#[cfg(test)]
mod tests {
    use fireweed_core::{
        EligibilityPolicy, LeaseToken, OrderingMode, PriorityModel, QueueDefinition, QueueId,
        RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };
    use fireweed_engine::{
        ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, LogRead, ProjectionRead,
        PushPort, PushSpec,
    };

    use super::async_composed_memory_backend;

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    #[tokio::test]
    async fn async_memory_push_and_claim_round_trip() {
        let backend = async_composed_memory_backend();
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        let ids = backend
            .push(
                &shard,
                vec![PushSpec {
                    payload: Some(bytes::Bytes::from_static(b"hello")),
                    ..PushSpec::default()
                }],
                now,
                None,
            )
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);

        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: UtcTimestamp::new(100, 0).unwrap(),
                now: UtcTimestamp::new(2, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(claimed.items[0].item_id, ids[0]);

        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.leased, 1);
        let page = backend.read_from(&shard, None, 16).await.unwrap();
        assert_eq!(page.entries.len(), 2); // push + claim
    }
}
