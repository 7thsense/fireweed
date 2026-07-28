use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    ClientItemKey, EligibilityPolicy, Fireweed, NewItem, ObjectLogAuthority,
    ObjectLogRuntimeConfig, ObjectLogStorage, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionConfig, QueueDefinition,
    QueueId, QueueKey, RecoveryPolicy, RecurrencePolicy, ResponseBarrier, RetryPolicy,
    ScheduleUpdate, SegmentConfig, SystemClock, TenantId, WorkerId, open_memory, open_sqlite,
    open_sqlite_sqlite_projection,
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
        .update(
            &key,
            item_id,
            ScheduleUpdate::Set(Some(PriorityValue::Int64(5))),
            ScheduleUpdate::Keep,
            None,
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
fn role_named_object_log_configuration_validates() {
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
    config.validate().unwrap();
}

#[tokio::test]
async fn root_crate_is_sufficient_for_a_concrete_memory_handle() {
    let fireweed = open_memory(Arc::new(SystemClock));
    accepts_concrete_handle(&fireweed);
    let _: WorkerId = WorkerId::new("snorri").unwrap();

    assert!(fireweed.projection_control().is_none());

    let definition = queue_definition();
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();
    assert_eq!(
        fireweed.queue_definition(&key).await.unwrap().queue_id,
        key.queue_id
    );
    exercise_operation_families(&fireweed, "operation-families-memory").await;
}

#[tokio::test]
async fn sqlite_uses_the_same_concrete_handle_and_operation_families() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "fireweed-concrete-facade-{}-{nonce}.sqlite",
        std::process::id()
    ));
    let fireweed = open_sqlite(path.to_str().unwrap(), Arc::new(SystemClock)).unwrap();
    accepts_concrete_handle(&fireweed);
    exercise_operation_families(&fireweed, "operation-families-sqlite").await;
    drop(fireweed);
    let _ = std::fs::remove_file(path);
}

/// Class A matrix cell: durable sqlite log × durable sqlite projection (distinct paths).
#[tokio::test]
async fn sqlite_sqlite_projection_uses_the_same_concrete_handle() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let log = std::env::temp_dir().join(format!(
        "fireweed-concrete-sqlite-log-{}-{nonce}.db",
        std::process::id()
    ));
    let proj = std::env::temp_dir().join(format!(
        "fireweed-concrete-sqlite-proj-{}-{nonce}.db",
        std::process::id()
    ));
    let fireweed = open_sqlite_sqlite_projection(
        log.to_str().unwrap(),
        proj.to_str().unwrap(),
        Arc::new(SystemClock),
    )
    .expect("open sqlite×sqlite facade cell");
    accepts_concrete_handle(&fireweed);
    exercise_operation_families(&fireweed, "operation-families-sqlite-sqlite").await;
    drop(fireweed);
    let _ = std::fs::remove_file(log);
    let _ = std::fs::remove_file(proj);
}

#[test]
fn open_sqlite_sqlite_projection_rejects_identical_paths() {
    let err = open_sqlite_sqlite_projection(
        "/tmp/fireweed-same-path.db",
        "/tmp/fireweed-same-path.db",
        Arc::new(SystemClock),
    )
    .expect_err("identical paths must fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("distinct"),
        "error should mention distinct paths: {msg}"
    );
}
