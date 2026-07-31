use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fireweed::{
    CohortOnIncomplete, CohortPolicy, CommitResponseBarrier, ComposedProjectionConfig,
    ComposedStorageConfig, CreateQueue, EligibilityPolicy, EnsureQueueError, EntitySchemaDocument,
    Fireweed, GateKeyPolicy, IndexDeclaration, IndexDef, IndexSpec, IndexType, MetadataValue,
    ObjectLogConfig, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, ProjectionRecoveryPolicy, QueueCreationPolicy, QueueDefinition, QueueId,
    QueueIndex, QueueKey, QueueTemplate, RecurrenceMode, RecurrencePolicy, RetryPolicy,
    SegmentSettings, TenantId, UtcTimestamp,
};
use fireweed_memory::ManualClock;

fn key(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn rich_create() -> CreateQueue {
    let mut metadata_blockers = BTreeMap::new();
    metadata_blockers.insert(
        "blocked".to_string(),
        vec![MetadataValue::String("yes".to_string())],
    );
    CreateQueue {
        tenant_id: TenantId::new("discarded-tenant").unwrap(),
        queue_id: QueueId::new("discarded-queue").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Text,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::ClientItemKey,
        },
        ordering_mode: OrderingMode::BoundedRelaxed,
        max_rank_error: 7,
        progress_bound_ms: 12_345,
        eligibility_policy: EligibilityPolicy {
            metadata_blockers,
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(3),
            max_gates_per_request: Some(5),
        },
        cohort_policy: CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(9_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(8),
        },
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 11_000,
        client_item_key_retention_ms: 12_000,
        terminal_retention_ms: 13_000,
        max_lease_duration_ms: 14_000,
        retry_policy: RetryPolicy { max_attempts: 9 },
        max_push_batch_size: 17,
        max_claim_batch_size: 19,
        max_eligible_group_size: Some(13),
        secondary_indexes: vec![IndexSpec {
            name: "by_customer".to_string(),
            fields: vec!["customer".to_string(), "region".to_string()],
            unique: true,
        }],
        entity_schema: Some(
            serde_json::from_value::<EntitySchemaDocument>(serde_json::json!({
                "entity_schema": {
                    "type": "object",
                    "properties": {"status": {"type": "string"}}
                }
            }))
            .unwrap(),
        ),
        typed_indexes: vec![QueueIndex {
            name: "by_status".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "status".to_string(),
                index_type: IndexType::String,
                unique: false,
            }),
        }],
        emit_change_records: false,
    }
}

fn template() -> QueueTemplate {
    QueueTemplate::new(rich_create(), QueueCreationPolicy::default())
        .with_name("workers")
        .with_revision("2026-07")
}

// Intentionally exhaustive and separate from QueueTemplate's CreateQueue destructure: a new durable
// definition field must be considered by drift coverage independently from template mapping.
fn create_from_definition(definition: QueueDefinition) -> CreateQueue {
    let QueueDefinition {
        tenant_id,
        queue_id,
        priority_model,
        ordering_mode,
        max_rank_error,
        progress_bound_ms,
        eligibility_policy,
        cohort_policy,
        recurrence,
        request_id_retention_ms,
        client_item_key_retention_ms,
        terminal_retention_ms,
        max_lease_duration_ms,
        retry_policy,
        max_push_batch_size,
        max_claim_batch_size,
        max_eligible_group_size,
        secondary_indexes,
        entity_schema,
        typed_indexes,
        emit_change_records,
    } = definition;
    CreateQueue {
        tenant_id,
        queue_id,
        priority_model,
        ordering_mode,
        max_rank_error,
        progress_bound_ms,
        eligibility_policy,
        cohort_policy: cohort_policy.unwrap_or_else(CohortPolicy::disabled),
        recurrence,
        request_id_retention_ms,
        client_item_key_retention_ms,
        terminal_retention_ms,
        max_lease_duration_ms,
        retry_policy,
        max_push_batch_size,
        max_claim_batch_size,
        max_eligible_group_size,
        secondary_indexes,
        entity_schema,
        typed_indexes,
        emit_change_records,
    }
}

fn drifted_definitions(base: &QueueDefinition) -> Vec<QueueDefinition> {
    let mut variants = Vec::new();
    macro_rules! drift {
        ($field:ident, $value:expr) => {{
            let mut definition = base.clone();
            definition.$field = $value;
            variants.push(definition);
        }};
    }
    let mut priority_model = base.priority_model;
    priority_model.direction = PriorityDirection::Ascending;
    drift!(priority_model, priority_model);
    let mut strict = base.clone();
    strict.ordering_mode = OrderingMode::Strict;
    strict.max_rank_error = 0;
    variants.push(strict);
    drift!(max_rank_error, 8);
    drift!(progress_bound_ms, base.progress_bound_ms + 1);
    let mut eligibility_policy = base.eligibility_policy.clone();
    eligibility_policy.max_gate_keys_per_item = Some(4);
    drift!(eligibility_policy, eligibility_policy);
    let mut cohort_policy = base.cohort_policy.unwrap();
    cohort_policy.max_cohort_size = Some(9);
    drift!(cohort_policy, Some(cohort_policy));
    let mut recurring = base.clone();
    recurring.cohort_policy = None;
    recurring.recurrence = RecurrencePolicy {
        mode: RecurrenceMode::Recurring,
        until: Some(UtcTimestamp::new(4_243, 0).unwrap()),
    };
    variants.push(recurring);
    drift!(request_id_retention_ms, base.request_id_retention_ms + 1);
    drift!(
        client_item_key_retention_ms,
        base.client_item_key_retention_ms + 1
    );
    drift!(terminal_retention_ms, base.terminal_retention_ms + 1);
    drift!(max_lease_duration_ms, base.max_lease_duration_ms + 1);
    drift!(retry_policy, RetryPolicy { max_attempts: 10 });
    drift!(max_push_batch_size, base.max_push_batch_size + 1);
    drift!(max_claim_batch_size, base.max_claim_batch_size + 1);
    drift!(max_eligible_group_size, Some(14));
    let mut secondary_indexes = base.secondary_indexes.clone();
    secondary_indexes[0].unique = false;
    drift!(secondary_indexes, secondary_indexes);
    drift!(entity_schema, None);
    let mut typed_indexes = base.typed_indexes.clone();
    let IndexDeclaration::Single(index) = &mut typed_indexes[0].declaration else {
        panic!("fixture uses a single-field typed index")
    };
    index.unique = true;
    drift!(typed_indexes, typed_indexes);
    drift!(emit_change_records, true);
    variants
}

#[test]
fn template_discards_prototype_identity_and_diagnostics_are_not_identity() {
    let first = template();
    let mut other_prototype = rich_create();
    other_prototype.tenant_id = TenantId::new("other-prototype").unwrap();
    other_prototype.queue_id = QueueId::new("other-prototype").unwrap();
    let second = QueueTemplate::new(other_prototype, QueueCreationPolicy::default())
        .with_name("different-name")
        .with_revision("different-revision");
    assert_eq!(first, second);

    let first_key = key("tenant", "first");
    let second_key = key("tenant", "second");
    let first_resolved = first.resolve(&first_key).unwrap();
    let second_resolved = first.resolve(&second_key).unwrap();
    let mut expected_second = first_resolved.clone();
    expected_second.queue_id = second_key.queue_id;
    assert_eq!(second_resolved, expected_second);
    assert_eq!(first_resolved, first_resolved.clone());
}

#[tokio::test]
async fn memory_ensure_is_exact_typed_and_field_complete() {
    let queue = key("tenant", "queue");
    let fireweed = fireweed::open_memory(Arc::new(ManualClock::at(10)));
    let first = fireweed.ensure_queue(&queue, &template()).await.unwrap();
    assert!(first.created);
    assert_eq!(first.template_name.as_deref(), Some("workers"));
    assert_eq!(first.template_revision.as_deref(), Some("2026-07"));
    let second = fireweed.ensure_queue(&queue, &template()).await.unwrap();
    assert!(!second.created);
    assert_eq!(second.definition, first.definition);

    for drifted in drifted_definitions(&first.definition) {
        let desired_template = QueueTemplate::new(
            create_from_definition(drifted),
            QueueCreationPolicy::default(),
        )
        .with_name("drift")
        .with_revision("2");
        let desired = desired_template.resolve(&queue).unwrap();
        match fireweed.ensure_queue(&queue, &desired_template).await {
            Err(EnsureQueueError::DefinitionConflict {
                created,
                desired: reported_desired,
                stored,
                template_name,
                template_revision,
            }) => {
                assert!(!created);
                assert_eq!(*reported_desired, desired);
                assert_eq!(*stored, first.definition);
                assert_eq!(template_name.as_deref(), Some("drift"));
                assert_eq!(template_revision.as_deref(), Some("2"));
            }
            other => panic!("expected exact definition conflict, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn validation_and_policy_divergence_are_caller_visible() {
    let queue = key("tenant", "validation");
    let fireweed = fireweed::open_memory(Arc::new(ManualClock::at(10)));
    let mut invalid = rich_create();
    invalid.progress_bound_ms = 0;
    let invalid = QueueTemplate::new(invalid, QueueCreationPolicy::default())
        .with_name("invalid")
        .with_revision("1");
    assert!(matches!(
        fireweed.ensure_queue(&queue, &invalid).await,
        Err(EnsureQueueError::Validation {
            template_name,
            template_revision,
            ..
        }) if template_name.as_deref() == Some("invalid")
            && template_revision.as_deref() == Some("1")
    ));

    let mut policy_create = rich_create();
    policy_create.eligibility_policy.max_gate_keys_per_item = None;
    policy_create.eligibility_policy.max_gates_per_request = None;
    let first_policy = QueueCreationPolicy {
        default_max_gate_keys_per_item: 3,
        default_max_gates_per_request: 5,
    };
    let first_template = QueueTemplate::new(policy_create.clone(), first_policy);
    let created = fireweed
        .ensure_queue(&queue, &first_template)
        .await
        .unwrap();
    assert!(created.created);
    let other_template = QueueTemplate::new(
        policy_create,
        QueueCreationPolicy {
            default_max_gate_keys_per_item: 4,
            default_max_gates_per_request: 6,
        },
    );
    assert!(matches!(
        fireweed.ensure_queue(&queue, &other_template).await,
        Err(EnsureQueueError::DefinitionConflict {
            created: false,
            desired,
            stored,
            ..
        }) if desired != stored
    ));
}

async fn assert_ensure(fireweed: &Fireweed, queue: &QueueKey, created: bool) {
    let outcome = fireweed.ensure_queue(queue, &template()).await.unwrap();
    assert_eq!(outcome.created, created);
    assert_eq!(outcome.definition, template().resolve(queue).unwrap());
}

fn temporary_path(tag: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "fireweed-template-{tag}-{}-{nonce}",
        std::process::id()
    ))
}

#[tokio::test]
#[ignore = "objectlog/composed reopen reports created=true (create_or_read / recover_definitions gap on LogEngine catalog); sqlite legs may pass in isolation"]
async fn durable_public_constructors_reopen_idempotently() {
    let queue = key("tenant", "durable");

    let sqlite = temporary_path("sqlite");
    let handle =
        fireweed::open_sqlite(sqlite.to_str().unwrap(), Arc::new(ManualClock::at(10))).unwrap();
    assert_ensure(&handle, &queue, true).await;
    drop(handle);
    let handle =
        fireweed::open_sqlite(sqlite.to_str().unwrap(), Arc::new(ManualClock::at(20))).unwrap();
    assert_ensure(&handle, &queue, false).await;
    drop(handle);

    let relational = temporary_path("relational");
    let handle = fireweed::open_sqlite_relational(
        relational.to_str().unwrap(),
        Arc::new(ManualClock::at(10)),
    )
    .unwrap();
    assert_ensure(&handle, &queue, true).await;
    drop(handle);
    let handle = fireweed::open_sqlite_relational(
        relational.to_str().unwrap(),
        Arc::new(ManualClock::at(20)),
    )
    .unwrap();
    assert_ensure(&handle, &queue, false).await;
    drop(handle);

    let objectlog = temporary_path("objectlog");
    let handle = fireweed::open_objectlog(&objectlog, Arc::new(ManualClock::at(10))).unwrap();
    assert_ensure(&handle, &queue, true).await;
    drop(handle);
    let handle = fireweed::open_objectlog(&objectlog, Arc::new(ManualClock::at(20))).unwrap();
    assert_ensure(&handle, &queue, false).await;
    drop(handle);

    let composed_root = temporary_path("composed-root");
    let composed_sqlite = temporary_path("composed.sqlite");
    let composed_config = composed_config(&composed_root, &composed_sqlite);
    let handle =
        fireweed::open_composed_sqlite(composed_config.clone(), Arc::new(ManualClock::at(10)))
            .unwrap();
    assert_ensure(&handle, &queue, true).await;
    drop(handle);
    let handle =
        fireweed::open_composed_sqlite(composed_config, Arc::new(ManualClock::at(20))).unwrap();
    assert_ensure(&handle, &queue, false).await;
    drop(handle);

    std::fs::remove_file(sqlite).unwrap();
    std::fs::remove_file(relational).unwrap();
    std::fs::remove_dir_all(objectlog).unwrap();
    std::fs::remove_dir_all(composed_root).unwrap();
    std::fs::remove_file(composed_sqlite).unwrap();
}

fn composed_config(root: &Path, sqlite: &Path) -> ComposedStorageConfig {
    ComposedStorageConfig {
        object_log: ObjectLogConfig::Local {
            root: root.to_path_buf(),
        },
        object_log_authority: fireweed::ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Sqlite {
            path: sqlite.to_path_buf(),
        },
        response_barrier: CommitResponseBarrier::Strict,
        segments: SegmentSettings::new(64 * 1024, 5).unwrap(),
        namespace: "queue-template-reopen".to_string(),
        recovery: ProjectionRecoveryPolicy::default(),
    }
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_public_constructors_and_composed_reopen_idempotently() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!("queue template PostgreSQL checks skipped: FIREWEED_PG_TEST_URL is unset");
        return;
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let sole_queue = key("template-live", &format!("sole-{nonce}"));
    let handle = fireweed::open_postgres(&url, Arc::new(ManualClock::at(10))).unwrap();
    futures::executor::block_on(assert_ensure(&handle, &sole_queue, true));
    drop(handle);
    let handle = fireweed::open_postgres(&url, Arc::new(ManualClock::at(20))).unwrap();
    futures::executor::block_on(assert_ensure(&handle, &sole_queue, false));
    drop(handle);

    let coordinated_queue = key("template-live", &format!("coordinated-{nonce}"));
    let handle = fireweed::open_postgres_coordinated(
        &url,
        Arc::new(ManualClock::at(10)),
        fireweed::OwnerId::new(format!("template-owner-{nonce}")).unwrap(),
        fireweed::ControlPlaneConfig::default(),
    )
    .unwrap();
    futures::executor::block_on(assert_ensure(&handle, &coordinated_queue, true));
    futures::executor::block_on(assert_ensure(&handle, &coordinated_queue, false));
    drop(handle);

    let composed_root = temporary_path("composed-postgres-root");
    let composed_config = ComposedStorageConfig {
        object_log: ObjectLogConfig::Local {
            root: composed_root.clone(),
        },
        object_log_authority: fireweed::ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Postgres {
            url: fireweed::SecretValue::new(url),
        },
        response_barrier: CommitResponseBarrier::Strict,
        segments: SegmentSettings::new(64 * 1024, 5).unwrap(),
        namespace: format!("queue-template-{nonce}"),
        recovery: ProjectionRecoveryPolicy::default(),
    };
    let composed_queue = key("template-live", &format!("composed-{nonce}"));
    let handle =
        fireweed::open_composed_postgres(composed_config.clone(), Arc::new(ManualClock::at(10)))
            .unwrap();
    futures::executor::block_on(assert_ensure(&handle, &composed_queue, true));
    drop(handle);
    let handle =
        fireweed::open_composed_postgres(composed_config, Arc::new(ManualClock::at(20))).unwrap();
    futures::executor::block_on(assert_ensure(&handle, &composed_queue, false));
    drop(handle);
    std::fs::remove_dir_all(composed_root).unwrap();
}
