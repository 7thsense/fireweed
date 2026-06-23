//! Tests for the SQLite `ControlPlaneStore` (TD-005), mirroring the in-memory
//! reference behavior and proving `QueueDefinition` JSON round-trips durably.

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityModel, QueueCreationPolicy,
    QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_sqlite::control_plane::SqliteControlPlaneStore;
use pqueue_storage::traits::{ControlPlaneError, ControlPlaneStore};
use pqueue_storage::types::QueueKey;

fn queue_def(tenant: &str, queue: &str, shards: u32) -> QueueDefinition {
    CreateQueue {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: None,
        shard_count: Some(shards),
    }
    .validate(&QueueCreationPolicy {
        deployment_max_shard_count: 16,
        ..QueueCreationPolicy::default()
    })
    .unwrap()
}

fn key(tenant: &str, queue: &str) -> QueueKey {
    QueueKey {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
    }
}

#[tokio::test]
async fn create_queue_persists_definition_and_shards() {
    let store = SqliteControlPlaneStore::open_in_memory().unwrap();
    let def = queue_def("t", "q", 3);
    let result = store.create_queue(def.clone()).await.unwrap();
    assert!(result.created);
    assert_eq!(result.definition, def);

    // The full definition round-trips through JSON (all nested config + ids).
    let fetched = store.queue_definition(&key("t", "q")).await.unwrap();
    assert_eq!(fetched, def);

    let shards = store.shard_assignments(&key("t", "q")).await.unwrap();
    assert_eq!(shards.len(), 3);
    assert!(shards.iter().all(|s| s.epoch == 1 && s.worker_id.is_none()));
    assert_eq!(shards[0].shard_key.shard_id.as_u32(), 0);
    assert_eq!(shards[2].shard_key.shard_id.as_u32(), 2);
}

#[tokio::test]
async fn duplicate_create_is_rejected() {
    let store = SqliteControlPlaneStore::open_in_memory().unwrap();
    store.create_queue(queue_def("t", "q", 1)).await.unwrap();
    let err = store
        .create_queue(queue_def("t", "q", 1))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlPlaneError::QueueAlreadyExists));
}

#[tokio::test]
async fn unknown_queue_reports_not_found() {
    let store = SqliteControlPlaneStore::open_in_memory().unwrap();
    assert!(matches!(
        store
            .queue_definition(&key("t", "missing"))
            .await
            .unwrap_err(),
        ControlPlaneError::QueueNotFound
    ));
    assert!(matches!(
        store
            .shard_assignments(&key("t", "missing"))
            .await
            .unwrap_err(),
        ControlPlaneError::QueueNotFound
    ));
}

#[tokio::test]
async fn list_queues_filters_by_tenant() {
    let store = SqliteControlPlaneStore::open_in_memory().unwrap();
    store.create_queue(queue_def("t1", "a", 1)).await.unwrap();
    store.create_queue(queue_def("t1", "b", 1)).await.unwrap();
    store.create_queue(queue_def("t2", "c", 1)).await.unwrap();
    let mut t1 = store
        .list_queues(&TenantId::new("t1").unwrap())
        .await
        .unwrap();
    t1.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(
        t1.iter().map(|q| q.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let t2 = store
        .list_queues(&TenantId::new("t2").unwrap())
        .await
        .unwrap();
    assert_eq!(t2.len(), 1);
}

#[tokio::test]
async fn definitions_survive_reopen() {
    let path = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("cp-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let def = queue_def("t", "q", 2);
    {
        let store = SqliteControlPlaneStore::open(&path).unwrap();
        store.create_queue(def.clone()).await.unwrap();
    }
    let reopened = SqliteControlPlaneStore::open(&path).unwrap();
    assert_eq!(
        reopened.queue_definition(&key("t", "q")).await.unwrap(),
        def
    );
    assert_eq!(
        reopened
            .shard_assignments(&key("t", "q"))
            .await
            .unwrap()
            .len(),
        2
    );
    let _ = std::fs::remove_file(&path);
}
