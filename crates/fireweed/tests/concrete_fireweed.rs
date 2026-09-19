#[path = "support/storage.rs"]
mod storage;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateRequest, BatchUpdateValue, ClientItemKey,
    EligibilityPolicy, Fireweed, LogConfig, NewItem, ObjectLogAuthority, ObjectLogRuntimeConfig,
    ObjectLogStorage, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, ProjectionConfig, ProjectionStoreConfig, QueueDefinition,
    QueueId, QueueKey, RecoveryPolicy, RecurrencePolicy, RequestId, ResponseBarrier, RetryPolicy,
    SegmentConfig, StorageConfig, SystemClock, TenantId, WorkerId, open, open_product,
};

fn queue_definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("downstream").unwrap(),
        queue_id: QueueId::new("work").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
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
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn accepts_concrete_handle(_: &Fireweed) {}

async fn exercise_operation_families(fireweed: &Fireweed, queue_name: &str) {
    let mut definition = queue_definition();
    definition.queue_id = QueueId::new(queue_name).unwrap();
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();

    let client_key = ClientItemKey::new(format!("{queue_name}-item")).unwrap();
    let item_id = fireweed
        .push(
            &key,
            NewItem {
                client_item_key: Some(client_key.clone()),
                priority: Some(PriorityValue::Int64(10)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fireweed
            .live_item(&key, client_key)
            .await
            .unwrap()
            .unwrap()
            .item_id,
        item_id
    );
    fireweed
        .batch_update(
            &key,
            BatchUpdateRequest {
                request_id: RequestId::new("cf-reschedule").unwrap(),
                updates: vec![BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ItemId(item_id),
                    expected_item_version: None,
                    priority: BatchUpdateValue::Replace(PriorityValue::Int64(5)),
                    not_before: BatchUpdateValue::Keep,
                    payload: BatchUpdateValue::Keep,
                    metadata: BatchUpdateValue::Keep,
                    gate_keys: BatchUpdateValue::Keep,
                    fields: BatchUpdateValue::Keep,
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&key).await.unwrap().pending, 1);
    let _ = fireweed.hot_projection_capabilities(&key);

    let claimed = fireweed.claim(&key, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    fireweed
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&key).await.unwrap().complete, 1);
}

#[test]
fn role_named_object_log_configuration_rejects_retired_sqlite() {
    let config = ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: "object-log".into(),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Sqlite {
            path: "projection.sqlite".into(),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(1024, 5).unwrap(),
        namespace: "downstream".to_string(),
        recovery: RecoveryPolicy::default(),
    };
    let err = config.validate().expect_err("sqlite projection is retired");
    assert!(
        format!("{err:?}").contains("s3 log") || format!("{err:?}").contains("sqlite"),
        "{err:?}"
    );
}

#[tokio::test]
async fn root_crate_is_sufficient_for_a_concrete_memory_handle() {
    let fireweed = open_product(Arc::new(SystemClock));
    accepts_concrete_handle(&fireweed);
    let _: WorkerId = WorkerId::new("snorri").unwrap();

    assert!(fireweed.projection_control().is_some());

    let definition = queue_definition();
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();
    assert_eq!(
        fireweed.queue_definition(&key).await.unwrap().queue_id,
        key.queue_id
    );
    exercise_operation_families(&fireweed, "operation-families-memory").await;
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_uses_the_same_concrete_handle_and_operation_families() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "fireweed-concrete-fs-turso-{}-{nonce}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let fireweed = fireweed::open_async(
        storage::product_config(&root, &storage::unique_namespace("concrete-fs-turso")),
        Arc::new(SystemClock),
    )
    .await
    .expect("open s3--turso");
    accepts_concrete_handle(&fireweed);
    exercise_operation_families(&fireweed, "operation-families-filesystem-turso").await;
    drop(fireweed);
    let _ = std::fs::remove_dir_all(root);
}
