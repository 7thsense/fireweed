#![cfg(all(feature = "objectlog", feature = "postgres"))]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    Bytes, ClaimRef, CommitEntry, CommitRequest, CommitResponseBarrier, ComposedProjectionConfig,
    ComposedStorageConfig, CompoundIndexDef, CompoundIndexField, ConfigSecret, EligibilityPolicy,
    EngineError, EntityEdit, EntityEditOperation, EntityPredicateValue, EntryOutcome, FilterOp,
    FinalizeKind, GateChange, IndexDeclaration, IndexType, InstanceFence, ItemMutationOperation,
    ItemMutationRequest, ItemMutationReturning, ItemPatch, ItemPredicate, ItemSelector,
    ItemSelectorScope, LeaseGuard, NewItem, ObjectLogAuthority, ObjectLogAuthorityConfig,
    ObjectLogConfig, ObjectLogRuntimeConfig, ObjectLogStorage, OrderField, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    ProjectionConfig, ProjectionRecoveryPolicy, QueryFilter, QueueDefinition, QueueId, QueueIndex,
    QueueKey, RangeScanRequest, RecoveryPolicy, RecurrencePolicy, RequestId, ResponseBarrier,
    RetryPolicy, SecretValue, SegmentConfig, SegmentSettings, SelectedMutation, SideRecord,
    SortDirection, TenantId, TypedValue, UtcTimestamp,
};
use fireweed_engine::DurabilityClass;
use fireweed_memory::ManualClock;
use fireweed_objectlog::segmented::S3BlobStore;
use futures::executor::block_on;
use postgres::{Client, NoTls};

fn runtime_env(suffix: &str) -> Result<String, std::env::VarError> {
    std::env::var(format!("FIREWEED_{suffix}"))
}

fn unique_fixture(name: &str) -> (PathBuf, String) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    (
        std::env::temp_dir().join(format!("fireweed-{name}-{nonce}")),
        format!("fireweed_{name}_{nonce}"),
    )
}

fn unique_bucket(tag: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("fireweed-{tag}-{}", nonce % 1_000_000_000)
}

fn config(root: &Path, schema: &str, url: &str) -> ComposedStorageConfig {
    ComposedStorageConfig {
        object_log: ObjectLogConfig::Local {
            root: root.to_path_buf(),
        },
        object_log_authority: ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Postgres {
            url: SecretValue::new(url),
        },
        response_barrier: CommitResponseBarrier::Strict,
        segments: SegmentSettings::new(64 * 1024, 5).unwrap(),
        namespace: schema.to_owned(),
        recovery: ProjectionRecoveryPolicy::default(),
    }
}

fn public_config(root: &Path, schema: &str, url: &str) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: root.to_path_buf(),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Postgres {
            url: ConfigSecret::new(url),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: schema.to_owned(),
        recovery: RecoveryPolicy::default(),
    }
}

fn definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("composed-tenant").unwrap(),
        queue_id: QueueId::new("durable-queue").unwrap(),
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
        typed_indexes: vec![QueueIndex {
            name: "by_kind_priority".to_owned(),
            declaration: IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    CompoundIndexField {
                        field: "kind".to_owned(),
                        index_type: IndexType::String,
                    },
                    CompoundIndexField {
                        field: "priority".to_owned(),
                        index_type: IndexType::Integer,
                    },
                ],
                unique: false,
            }),
        }],
        emit_change_records: true,
    }
}

fn queue() -> QueueKey {
    QueueKey::new(
        TenantId::new("composed-tenant").unwrap(),
        QueueId::new("durable-queue").unwrap(),
    )
}

fn item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(format!("payload-{priority}").into()),
        entity: Some(serde_json::json!({
            "kind": "composed-lifecycle",
            "priority": priority,
        })),
        ..NewItem::default()
    }
}

fn assert_authoritative_commit_capabilities(caps: &fireweed::CommitCapabilities) {
    assert!(caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.non_work_side_records);
    assert!(caps.authoritative_recovery_reads);
    assert!(caps.delayed_awaits_timers);
    assert_eq!(caps.durability_class, DurabilityClass::Atomic);
}

fn object_count(root: &Path) -> usize {
    fn visit(path: &Path, count: &mut usize) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, count);
            } else {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    visit(root, &mut count);
    count
}

fn postgres_in_schema(url: &str, namespace: &str) -> Client {
    let schema = crate::derived_postgres_schema_name(namespace);
    let mut client = Client::connect(url, NoTls).unwrap();
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .unwrap();
    client
}

fn drop_schema(url: &str, namespace: &str) {
    let schema = crate::derived_postgres_schema_name(namespace);
    let mut client = Client::connect(url, NoTls).unwrap();
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .unwrap();
}

#[test]
fn objectlog_postgres_item_mutation_reopens_and_replays_resolved_response() {
    let Ok(url) = runtime_env("PG_TEST_URL") else {
        eprintln!(
            "SKIP objectlog_postgres_item_mutation_reopens_and_replays_resolved_response: \
             FIREWEED_PG_TEST_URL is unset"
        );
        return;
    };
    let (root, namespace) = unique_fixture("objectlog_postgres_mutation");
    let durability = config(&root, &namespace, &url);
    let clock = Arc::new(ManualClock::at(1_000));
    let queue = queue();
    let fireweed = fireweed::open_composed_postgres(durability.clone(), clock.clone()).unwrap();
    block_on(fireweed.create_queue(definition())).unwrap();
    let item_id = block_on(fireweed.push(&queue, item(10))).unwrap();
    let request = ItemMutationRequest {
        request_id: RequestId::new("objectlog-postgres-mutation-replay").unwrap(),
        evaluated_at: UtcTimestamp::new(1, 0).unwrap(),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![SelectedMutation {
                selector_id: "unindexed-kind".into(),
                selector: ItemSelector {
                    scope: ItemSelectorScope::Live,
                    predicates: vec![ItemPredicate::EntityEq {
                        pointer: "/kind".into(),
                        value: EntityPredicateValue::Value(serde_json::json!("composed-lifecycle")),
                    }],
                },
                predicates: vec![],
                lease_guard: LeaseGuard::RejectActive,
                patch: ItemPatch {
                    entity_edits: vec![EntityEdit {
                        pointer: "/kind".into(),
                        operation: EntityEditOperation::Set(serde_json::json!("already-mutated")),
                    }],
                    ..Default::default()
                },
            }],
        },
    };
    let mut rejected = request.clone();
    rejected.request_id = RequestId::new("objectlog-postgres-mutation-rollback").unwrap();
    rejected.gate_changes = vec![GateChange {
        gate_keys: vec!["not-permitted-for-this-queue".into()],
        blocked: true,
    }];
    assert!(matches!(
        block_on(fireweed.mutate_items(&queue, rejected)),
        Err(EngineError::Invalid(_))
    ));
    assert_eq!(
        block_on(fireweed.peek(&queue, 1)).unwrap()[0].item_version,
        1,
        "a rejected request must not partially apply its accepted item patch"
    );
    let committed = block_on(fireweed.mutate_items(&queue, request.clone())).unwrap();
    assert_eq!(committed.summary.changed, 1);
    assert_eq!(committed.results[0].item_id, item_id);
    let reindexed = block_on(fireweed.range_scan(
        &queue,
        RangeScanRequest {
            index: Some("by_kind_priority".into()),
            filters: vec![QueryFilter {
                field: "kind".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("already-mutated".into()),
            }],
            order_by: vec![OrderField {
                field: "priority".into(),
                direction: SortDirection::Ascending,
            }],
            page_size: 10,
            cursor: None,
        },
    ))
    .unwrap();
    assert_eq!(reindexed.rows.len(), 1);
    assert_eq!(reindexed.rows[0].item_id, item_id);
    drop(fireweed);

    let reopened = fireweed::open_composed_postgres(durability, clock).unwrap();
    let replayed = block_on(reopened.mutate_items(&queue, request.clone())).unwrap();
    assert_eq!(
        replayed, committed,
        "replay must not re-evaluate the selector"
    );
    let mut changed_body = request;
    changed_body.returning = ItemMutationReturning::Identity;
    assert_eq!(
        block_on(reopened.mutate_items(&queue, changed_body)).unwrap_err(),
        EngineError::RequestIdConflict
    );
    drop(reopened);
    drop_schema(&url, &namespace);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn public_objectlog_postgres_delete_and_rebuild() {
    let Ok(url) = runtime_env("PG_TEST_URL") else {
        eprintln!(
            "SKIP public_objectlog_postgres_delete_and_rebuild: FIREWEED_PG_TEST_URL is unset"
        );
        return;
    };
    let (root, schema) = unique_fixture("public_objectlog_postgres");
    let durability = config(&root, &schema, &url);
    let clock = Arc::new(ManualClock::at(1_000));
    let fireweed = fireweed::open_composed_postgres(durability.clone(), clock.clone()).unwrap();
    let key = queue();

    block_on(fireweed.create_queue(definition())).unwrap();
    let query_caps = fireweed.hot_projection_capabilities(&key);
    assert!(query_caps.range_scan);
    assert!(query_caps.grouped_aggregate);
    assert!(query_caps.declared_bucket_segment);
    assert!(query_caps.bounded_mutation);
    assert!(query_caps.claim_by_query);
    assert!(!query_caps.side_record_query);
    assert!(query_caps.paired_capabilities_consistent());
    assert!(
        block_on(fireweed.range_scan(
            &key,
            RangeScanRequest {
                index: Some("by_kind_priority".to_owned()),
                filters: vec![QueryFilter {
                    field: "kind".to_owned(),
                    op: FilterOp::Eq,
                    value: TypedValue::String("composed-lifecycle".to_owned()),
                }],
                order_by: vec![OrderField {
                    field: "priority".to_owned(),
                    direction: SortDirection::Ascending,
                }],
                page_size: 1,
                cursor: None,
            }
        ))
        .unwrap()
        .rows
        .is_empty()
    );
    let first_request = RequestId::new("composed-request-1").unwrap();
    let (first, _) =
        block_on(fireweed.push_with_request_id(&key, first_request.clone(), item(10))).unwrap();
    let second = block_on(fireweed.push(&key, item(20))).unwrap();

    // Strict visibility: acknowledgement means the durable PostgreSQL image is queryable immediately.
    let expected = block_on(fireweed.metrics(&key)).unwrap();
    assert_eq!(expected.pending, 2);
    assert_eq!(block_on(fireweed.peek(&key, 10)).unwrap().len(), 2);
    let caps = fireweed.commit_capabilities(&key).unwrap();
    assert_authoritative_commit_capabilities(&caps);
    assert_eq!(
        block_on(
            fireweed
                .projection_control()
                .expect("projection control")
                .verify()
        )
        .unwrap()
        .projection_sequence,
        block_on(
            fireweed
                .projection_control()
                .expect("projection control")
                .verify()
        )
        .unwrap()
        .authoritative_sequence
    );

    let claimed = block_on(fireweed.claim(&key, 1, 30_000)).unwrap();
    let claim = &claimed[0];
    block_on(fireweed.ack(&key, [claim.item_id])).unwrap();
    let lifecycle_id = block_on(fireweed.push(&key, item(30))).unwrap();
    let expected = block_on(fireweed.metrics(&key)).unwrap();
    assert_eq!(
        (expected.pending, expected.leased, expected.complete),
        (2, 0, 1)
    );
    assert_eq!(
        block_on(fireweed.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );

    // A deliberately behind image replays only the one retained command after its durable cursor.
    let mut postgres = postgres_in_schema(&url, &schema);
    let last_sequence: i64 = postgres
        .query_one(
            "SELECT MAX(last_command_sequence) FROM fireweed_items WHERE tenant_id=$1 AND queue_id=$2",
            &[&"composed-tenant", &"durable-queue"],
        )
        .unwrap()
        .get::<_, Option<i64>>(0)
        .unwrap();
    postgres
        .execute(
            "DELETE FROM fireweed_items WHERE tenant_id=$1 AND queue_id=$2 AND last_command_sequence=$3",
            &[&"composed-tenant", &"durable-queue", &last_sequence],
        )
        .unwrap();
    postgres
        .execute(
            "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
            &[&"composed-tenant", &"durable-queue", &last_sequence],
        )
        .unwrap();
    let tail = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert_eq!(tail.tail_commands_replayed, 1);
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap(), expected);

    // An ahead image fails closed, then restoring its cursor makes it valid again.
    let authoritative_next = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap()
    .authoritative_sequence
        + 1;
    postgres
        .execute(
            "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
            &[
                &"composed-tenant",
                &"durable-queue",
                &((authoritative_next + 2) as i64),
            ],
        )
        .unwrap();
    assert!(
        block_on(
            fireweed
                .projection_control()
                .expect("projection control")
                .verify()
        )
        .is_err()
    );
    postgres
        .execute(
            "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
            &[
                &"composed-tenant",
                &"durable-queue",
                &(authoritative_next as i64),
            ],
        )
        .unwrap();
    drop(postgres);

    // Deleting the disposable image does not touch authoritative objects or durable request outcomes.
    let objects_before_delete = object_count(&root);
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    assert_eq!(object_count(&root), objects_before_delete);
    let mut deleted_postgres = postgres_in_schema(&url, &schema);
    let stale_component_count: i64 = deleted_postgres
        .query_one("SELECT COUNT(*) FROM fireweed_item_index_component", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        stale_component_count, 0,
        "projection deletion must clear decomposed typed-index rows before replay"
    );
    drop(deleted_postgres);
    let deleted = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    assert!(!deleted.compatible);
    assert_eq!(deleted.projection_sequence, 0);
    assert!(deleted.authoritative_sequence > 0);
    let rebuilt = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert!(
        rebuilt.tail_commands_replayed >= 2,
        "projection rebuild must replay the authoritative tail"
    );
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(fireweed.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );

    let (replayed, replay_disp) =
        block_on(fireweed.push_with_request_id(&key, first_request.clone(), item(10))).unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed, first);
    assert_eq!(object_count(&root), objects_before_delete);

    // A fresh public facade reconstructs exact normalized state and the durable request-id outcome.
    drop(fireweed);
    let reopened = fireweed::open_composed_postgres(durability, clock).unwrap();
    assert_eq!(block_on(reopened.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(reopened.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );
    let (reopened_replay, reopened_disp) =
        block_on(reopened.push_with_request_id(&key, first_request, item(10))).unwrap();
    assert_eq!(reopened_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(reopened_replay, first);
    assert_eq!(object_count(&root), objects_before_delete);
    drop(reopened);

    drop_schema(&url, &schema);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn public_s3_objectlog_postgres_open_and_reopen_with_disposable_projection() {
    let endpoint = match runtime_env("S3_TEST_URL").or_else(|_| runtime_env("S3_TEST_ENDPOINT")) {
        Ok(value) => value,
        Err(_) => {
            if std::env::var_os("CI").is_some() {
                panic!(
                    "CI must set FIREWEED_S3_TEST_URL or FIREWEED_S3_TEST_ENDPOINT; S3+Postgres coverage cannot be skipped"
                );
            }
            eprintln!(
                "SKIP public_s3_objectlog_postgres_open_and_reopen_with_disposable_projection: FIREWEED_S3_TEST_URL and FIREWEED_S3_TEST_ENDPOINT are unset"
            );
            return;
        }
    };
    let bucket = runtime_env("S3_TEST_BUCKET").unwrap_or_else(|_| unique_bucket("pg"));
    let access = runtime_env("S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = runtime_env("S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let region = runtime_env("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let pg_url = runtime_env("PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL must be set when exercising the postgres projection");
    let allow_insecure_http = endpoint.starts_with("http://");

    S3BlobStore::new(&endpoint, &bucket, &access, &secret, &region)
        .unwrap()
        .create_bucket()
        .unwrap();

    let (_, run_nonce) = unique_fixture("public_s3_objectlog_postgres");
    let namespace = format!(
        "snorri-s3-v1:prefix_len:32:object-log/{}:{}:{}:{run_nonce}",
        "illegal-namespace".repeat(3),
        "with punctuation:-/",
        "with unicode snowman ☃ and more text to exceed sixty-three bytes"
    );
    let durability = ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::S3Compatible {
            endpoint,
            bucket,
            region,
            access_key_id: ConfigSecret::new(access),
            secret_access_key: ConfigSecret::new(secret),
            allow_insecure_http,
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Postgres {
            url: ConfigSecret::new(pg_url.clone()),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: namespace.clone(),
        recovery: RecoveryPolicy::default(),
    };
    let clock = Arc::new(ManualClock::at(1_000));

    let postgres_caps = {
        let fireweed =
            fireweed::open_objectlog_postgres(durability.clone(), clock.clone()).unwrap();
        let key = queue();
        block_on(fireweed.create_queue(definition())).unwrap();
        block_on(fireweed.push(&key, item(10))).unwrap();
        block_on(fireweed.push(&key, item(20))).unwrap();
        assert_eq!(block_on(fireweed.metrics(&key)).unwrap().pending, 2);
        let control = fireweed
            .projection_control()
            .expect("object-log/Postgres owns a disposable projection");
        assert!(block_on(control.verify()).unwrap().compatible);
        let caps = fireweed.commit_capabilities(&key).unwrap();
        assert_authoritative_commit_capabilities(&caps);
        caps
    };

    {
        let fireweed =
            fireweed::open_objectlog_postgres(durability.clone(), clock.clone()).unwrap();
        let key = queue();
        assert_eq!(block_on(fireweed.metrics(&key)).unwrap().pending, 2);
        assert_eq!(block_on(fireweed.peek(&key, 10)).unwrap().len(), 2);
        let control = fireweed
            .projection_control()
            .expect("reopened object-log/Postgres exposes projection maintenance");
        block_on(control.delete()).unwrap();
        let rebuilt = block_on(control.rebuild()).unwrap();
        assert!(rebuilt.projection_sequence > 0);
        assert!(block_on(control.verify()).unwrap().compatible);
    }

    #[cfg(feature = "sqlite")]
    {
        let (_, sqlite_projection) = unique_fixture("s3_sqlite_capability_parity");
        let mut sqlite_durability = durability;
        sqlite_durability.projection = ProjectionConfig::Sqlite {
            path: std::env::temp_dir().join(format!("{sqlite_projection}.sqlite")),
        };
        let fireweed = fireweed::open_objectlog_sqlite(sqlite_durability, clock).unwrap();
        let sqlite_caps = fireweed.commit_capabilities(&queue()).unwrap();
        assert_eq!(postgres_caps, sqlite_caps);
    }

    drop_schema(&pg_url, &namespace);
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_open_inside_tokio_returns_typed_error() {
    let (root, schema) = unique_fixture("tokio_sync_open");
    let error = match fireweed::open_objectlog_postgres(
        public_config(&root, &schema, "postgres://127.0.0.1:1/postgres"),
        Arc::new(ManualClock::at(1_000)),
    ) {
        Ok(_) => panic!("the synchronous constructor must reject an ambient Tokio runtime"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        EngineError::Invalid(
            "open_objectlog_postgres cannot run inside a Tokio runtime; use open_objectlog_postgres_async"
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_open_is_safe_inside_tokio() {
    let Ok(url) = runtime_env("PG_TEST_URL") else {
        eprintln!("SKIP asynchronous_open_is_safe_inside_tokio: FIREWEED_PG_TEST_URL is unset");
        return;
    };
    let (root, schema) = unique_fixture("tokio_async_open");
    let fireweed = fireweed::open_objectlog_postgres_async(
        public_config(&root, &schema, &url),
        Arc::new(ManualClock::at(1_000)),
    )
    .await
    .unwrap();
    drop(fireweed);
    tokio::task::spawn_blocking(move || drop_schema(&url, &schema))
        .await
        .unwrap();
    let _ = fs::remove_dir_all(root);
}

/// Full recovery proof for the authoritative rich-transition protocol. It remains ignored by default because
/// it requires a live PostgreSQL instance; CI exercises the shorter S3/Postgres capability and reopen proof.
#[test]
#[ignore = "US-009 recovery proof requires FIREWEED_PG_TEST_URL"]
fn us009_objectlog_postgres_rich_commit_recovery_promotion() {
    let url =
        runtime_env("PG_TEST_URL").expect("US-009 promotion proof requires FIREWEED_PG_TEST_URL");
    let (root, schema) = unique_fixture("us009_objectlog_postgres_commit");
    let durability = config(&root, &schema, &url);
    let clock = Arc::new(ManualClock::at(1_000));
    let fireweed = fireweed::open_composed_postgres(durability.clone(), clock.clone()).unwrap();
    let key = queue();
    block_on(fireweed.create_queue(definition())).unwrap();
    assert_authoritative_commit_capabilities(&fireweed.commit_capabilities(&key).unwrap());

    block_on(fireweed.push(&key, item(10))).unwrap();
    let claim = block_on(fireweed.claim(&key, 1, 30_000)).unwrap().remove(0);
    let claim_ref = ClaimRef {
        item_id: claim.item_id,
        lease_token: claim.lease_token.clone().expect("claim token"),
        lease_expires_at: claim.lease_expires_at,
        item_version: claim.item_version,
    };
    let request_id = RequestId::new("us009-objectlog-postgres-transition").unwrap();
    let transition = || CommitRequest {
        request_id: Some(request_id.clone()),
        entries: vec![CommitEntry {
            claim_ref: claim_ref.clone(),
            finalize: FinalizeKind::Complete,
            side_records: vec![SideRecord {
                key: b"state/run-1".to_vec(),
                payload: Bytes::copy_from_slice(b"audit-bytes"),
            }],
            lifecycle_items: vec![item(30)],
            instance_fence: Some(InstanceFence {
                instance_key: b"wf-1".to_vec(),
                expected: 0,
                next: 1,
            }),
        }],
    };
    let outcomes = block_on(fireweed.commit(&key, transition())).unwrap();
    let lifecycle_id = match outcomes.as_slice() {
        [EntryOutcome::Committed { lifecycle_item_ids }] => lifecycle_item_ids[0],
        other => panic!("expected committed rich transition, got {other:?}"),
    };
    assert_eq!(
        block_on(fireweed.side_record(&key, b"state/run-1"))
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert_eq!(
        block_on(fireweed.commit(&key, transition())).unwrap(),
        outcomes
    );
    let recovery = block_on(fireweed.explain_commit(&key, request_id))
        .unwrap()
        .expect("rich transition survives projection rebuild");
    assert_eq!(recovery.entries[0].consumed_input_id, claim.item_id);
    assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(
        recovery.entries[0].side_record_keys,
        vec![b"state/run-1".to_vec()]
    );

    drop(fireweed);
    drop_schema(&url, &schema);
    let _ = fs::remove_dir_all(root);
}
