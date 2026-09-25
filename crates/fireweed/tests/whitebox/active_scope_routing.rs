#![allow(dead_code, unused_imports)]

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::ManualClock;
use fireweed::{
    ActiveScope, ActiveScopeDiscovery, DiscoveryGranularity, EngineError, GroupKey, LogConfig,
    NewItem, OldestFirstScopePrefix, ProjectionStoreConfig, QueueDefinition, QueueId, QueueKey,
    RuntimeCore, StorageConfig, TenantId, UtcTimestamp, open, select_active_scope_from_prefix,
};
use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, RecurrencePolicy, RetryPolicy,
};

fn queue(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn definition(queue: &QueueKey, progress_bound_ms: u64) -> QueueDefinition {
    QueueDefinition {
        tenant_id: queue.tenant_id.clone(),
        queue_id: queue.queue_id.clone(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms,
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
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn scope(queue_id: &str, group: Option<&str>, age: u64) -> ActiveScope {
    ActiveScope {
        queue_id: queue_id.to_string(),
        group_key: group.map(str::to_string),
        oldest_eligible_age_ms: age,
        eligible_count: Some(1),
        progress_bound_risk_count: Some(0),
    }
}

fn stamped(queue: &QueueKey, scopes: Vec<ActiveScope>) -> ActiveScopeDiscovery {
    ActiveScopeDiscovery {
        queue: queue.clone(),
        granularity: DiscoveryGranularity::Group,
        scopes,
    }
}
#[tokio::test]
async fn queue_definition_accessor_reads_memory_and_durable_policy() {
    let q = queue("tenant", "queue");
    let memory = RuntimeCore::new(
        Arc::new(crate::turso_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    memory.create_queue(definition(&q, 12_345)).await.unwrap();
    assert_eq!(
        memory.queue_definition(&q).await.unwrap().progress_bound_ms,
        12_345
    );

    let root = std::env::temp_dir().join(format!(
        "fireweed-active-scope-policy-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let s3 = fireweed_objectlog::shared_s3_test_env();
    let mut cfg = StorageConfig::s3_turso(
        s3.endpoint.clone(),
        s3.bucket.clone(),
        s3.region.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.allow_insecure_http(),
        root.join("projection.turso"),
    );
    cfg.namespace = "active-scope".into();
    let durable = fireweed::open_async(cfg, Arc::new(ManualClock::at(0)))
        .await
        .unwrap();
    durable.create_queue(definition(&q, 54_321)).await.unwrap();
    assert_eq!(
        durable
            .queue_definition(&q)
            .await
            .unwrap()
            .progress_bound_ms,
        54_321
    );
    drop(durable);
    let _ = std::fs::remove_dir_all(root);
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}
