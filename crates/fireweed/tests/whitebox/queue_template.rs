#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(all(feature = "postgres", feature = "objectlog"))]
use crate::{
    CommitResponseBarrier, ComposedProjectionConfig, ComposedStorageConfig, ObjectLogConfig,
    ProjectionRecoveryPolicy, SegmentSettings,
};
use fireweed::{
    CohortOnIncomplete, CohortPolicy, CreateQueue, EligibilityPolicy, EnsureQueueError,
    EntitySchemaDocument, Fireweed, GateKeyPolicy, IndexDeclaration, IndexDef, IndexSpec,
    IndexType, LogConfig, MetadataValue, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, ProjectionStoreConfig, QueueCreationPolicy,
    QueueDefinition, QueueId, QueueIndex, QueueKey, QueueTemplate, RecurrenceMode,
    RecurrencePolicy, RetryPolicy, StorageConfig, TenantId, UtcTimestamp, open,
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

#[cfg(feature = "objectlog")]
#[tokio::test]
async fn durable_public_constructors_reopen_idempotently() {
    let queue = key("tenant", "durable");

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let turso_root = temporary_path("s3-turso");
    let s3 = fireweed_objectlog::shared_s3_test_env();
    let mut cfg = StorageConfig::s3_turso(
        s3.endpoint.clone(),
        s3.bucket.clone(),
        s3.region.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.allow_insecure_http(),
        turso_root.join("projection.turso"),
    );
    cfg.namespace = format!("queue-template-durable-{}-{}", std::process::id(), nonce);
    let handle = fireweed::open_async(cfg.clone(), Arc::new(ManualClock::at(10)))
        .await
        .unwrap();
    assert_ensure(&handle, &queue, true).await;
    let _ = handle.metrics(&queue).await;
    drop(handle);
    let handle = fireweed::open_async(cfg, Arc::new(ManualClock::at(20)))
        .await
        .unwrap();
    assert_ensure(&handle, &queue, false).await;
    drop(handle);
    std::fs::remove_dir_all(turso_root).unwrap();
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_public_constructors_and_composed_reopen_idempotently() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");
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

    #[cfg(feature = "objectlog")]
    {
        let composed_root = temporary_path("composed-postgres-root");
        let composed_config = ComposedStorageConfig {
            object_log: ObjectLogConfig::Local {
                root: composed_root.clone(),
            },
            object_log_authority: fireweed::ObjectLogAuthorityConfig::NativeConditionalWrite,
            projection: ComposedProjectionConfig::Postgres {
                url: fireweed::SecretValue::new(url),
            },
            response_barrier: CommitResponseBarrier::AsyncProjection,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
            segments: SegmentSettings::new(64 * 1024, 5).unwrap(),
            namespace: format!("queue-template-{nonce}"),
            recovery: ProjectionRecoveryPolicy::default(),
        };
        let composed_queue = key("template-live", &format!("composed-{nonce}"));
        let composed_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build object-log PostgreSQL operation runtime");
        let handle = fireweed::open_composed_postgres(
            composed_config.clone(),
            Arc::new(ManualClock::at(10)),
        )
        .unwrap();
        composed_runtime.block_on(assert_ensure(&handle, &composed_queue, true));
        drop(handle);
        let handle =
            fireweed::open_composed_postgres(composed_config, Arc::new(ManualClock::at(20)))
                .unwrap();
        composed_runtime.block_on(assert_ensure(&handle, &composed_queue, false));
        drop(handle);
        std::fs::remove_dir_all(composed_root).unwrap();
    }
}
