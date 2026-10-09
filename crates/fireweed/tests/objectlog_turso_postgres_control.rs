use fireweed::*;
use fireweed_memory::ManualClock;
use std::sync::Arc;

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicit limited-role PostgreSQL and RustFS fixture configuration"]
async fn postgres_ownership_with_native_s3_turso_persistence() {
    let url = std::env::var("FIREWEED_CONTROL_PLANE_TEST_URL").expect("explicit PG fixture URL");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-pg-control-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Arc::new(ManualClock::at(1));
    let ownership_clock = Arc::new(ManualClock::at(100));
    let make_storage = |file: &str| {
        let mut config = StorageConfig::s3_turso(
            std::env::var("FIREWEED_TEST_S3_ENDPOINT").unwrap(),
            std::env::var("FIREWEED_TEST_S3_BUCKET").unwrap(),
            "us-east-1",
            std::env::var("FIREWEED_TEST_S3_ACCESS_KEY").unwrap(),
            std::env::var("FIREWEED_TEST_S3_SECRET_KEY").unwrap(),
            true,
            root.join(file),
        );
        config.namespace = format!("snorri-pg-control-{nonce}");
        config
    };
    let make_control = |owner: &str| ObjectLogPostgresControlPlaneConfig {
        url: ConfigSecret::new(url.clone()),
        schema: format!("snorri_control_{nonce}"),
        ownership_clock: Some(ownership_clock.clone()),
        coordination: PostgresCoordinationConfig {
            instance_id: OwnerId::new(format!("{owner}_{nonce}")).unwrap(),
            control_plane: ControlPlaneConfig::default(),
        },
    };
    let mut definition = qdef();
    definition.queue_id = QueueId::new(format!("q_{nonce}")).unwrap();
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let first = open_objectlog_turso_coordinated_async(
        make_storage("first.turso"),
        make_control("first"),
        clock.clone(),
    )
    .await
    .unwrap();
    first.create_queue(definition).await.unwrap();
    let item = first
        .push(
            &key,
            NewItem {
                priority: Some(PriorityValue::Int64(1)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    // Workflow time can jump beyond both lease TTLs without expiring ownership.
    clock.set(3_600);
    let other = open_objectlog_turso_coordinated_async(
        make_storage("other.turso"),
        make_control("other"),
        clock.clone(),
    )
    .await
    .unwrap();
    let rejected = other
        .push(
            &key,
            NewItem {
                priority: Some(PriorityValue::Int64(2)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            rejected,
            EngineError::Forbidden("queue owned by another live owner")
        ),
        "ownership rejection: {rejected:?}"
    );
    other.shutdown().await.unwrap();
    drop(other);
    ownership_clock.set(102);
    first.renew_owned().await.unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let reopened = open_objectlog_turso_coordinated_async(
        make_storage("first.turso"),
        make_control("first"),
        clock,
    )
    .await
    .unwrap();
    let items = reopened.peek(&key, 10).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].item_id, item);
    assert!(
        reopened
            .projection_control()
            .unwrap()
            .verify()
            .await
            .unwrap()
            .compatible
    );
    reopened.shutdown().await.unwrap();
}
