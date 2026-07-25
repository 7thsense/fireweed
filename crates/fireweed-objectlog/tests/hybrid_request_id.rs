use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_core::{
    BoundedMutationRequest, CompoundIndexDef, CompoundIndexField, EligibilityPolicy, FilterOp,
    IndexDeclaration, IndexType, OrderField, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueryFilter, QueueDefinition, QueueId,
    QueueIndex, RangeScanRequest, RecurrencePolicy, RequestId, RetryPolicy, SortDirection,
    TenantId, TypedValue, UtcTimestamp,
};
use fireweed_engine::{
    ComposedBackend, ControlPlaneStore, EngineError, HotProjectionQueryPort, InProcessControlPlane,
    LogStore, ProjectionRead, PushPort, PushSpec, QueueKey, RequestOutcome,
};
use fireweed_objectlog::{ObjectLog, SegmentConfig};
use fireweed_sqlite::HybridProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "fireweed-objlog-hybrid-request-id-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn shard() -> QueueKey {
    QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    )
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tenant").unwrap(),
        queue_id: QueueId::new("queue").unwrap(),
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
        retry_policy: RetryPolicy { max_attempts: 10 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn mutation_qdef() -> QueueDefinition {
    QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: "by_kind_suppressed".into(),
            declaration: IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    CompoundIndexField {
                        field: "kind".into(),
                        index_type: IndexType::String,
                    },
                    CompoundIndexField {
                        field: "suppressed".into(),
                        index_type: IndexType::Boolean,
                    },
                ],
                unique: false,
            }),
        }],
        ..qdef()
    }
}

fn ts(secs: i64) -> UtcTimestamp {
    UtcTimestamp::new(secs, 0).unwrap()
}

fn gc_config() -> SegmentConfig {
    SegmentConfig::new(1 << 20, 20).unwrap()
}

fn open_hybrid(root: &std::path::Path, sqlite_path: &std::path::Path) -> HybridBackend {
    ComposedBackend::new(
        ObjectLog::open_group_commit(root, gc_config()).expect("open object log"),
        HybridProjectionStore::open(sqlite_path.to_str().expect("utf8 sqlite path"))
            .expect("open hybrid projection"),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover hybrid backend")
}

fn open_immediate_hybrid(root: &std::path::Path, sqlite_path: &std::path::Path) -> HybridBackend {
    ComposedBackend::new(
        ObjectLog::open(root).expect("open object log"),
        HybridProjectionStore::open(sqlite_path.to_str().expect("utf8 sqlite path"))
            .expect("open hybrid projection"),
        InProcessControlPlane::new(),
    )
    .recover()
    .expect("recover hybrid backend")
}

#[tokio::test]
async fn hybrid_request_id_push_replays_after_restart_and_conflicts_on_body_change() {
    let root = tmp_root("replay");
    let sqlite_path = root.join("projection.sqlite");
    let queue = shard();
    let request_id = RequestId::new("push-request-1").unwrap();
    let body = vec![PushSpec::default()];

    let first_ids = {
        let backend = open_hybrid(&root, &sqlite_path);
        backend.create_queue(qdef()).await.unwrap();
        let ids = backend
            .push_with_request_id(&queue, request_id.clone(), body.clone(), ts(1), None)
            .await
            .unwrap();
        assert_eq!(backend.metrics(&queue).await.unwrap().pending, 1);
        let page = backend
            .with_log(|log| log.read_from(&queue, None, 10))
            .expect("read committed log");
        let env = &page.entries[0].1;
        assert_eq!(env.request_id.as_ref(), Some(&request_id));
        assert!(env.request_fingerprint.is_some());
        assert_eq!(
            env.request_outcome,
            Some(RequestOutcome::Push {
                item_ids: ids.clone()
            })
        );
        ids
    };

    let reopened = open_hybrid(&root, &sqlite_path);

    let replayed = reopened
        .push_with_request_id(&queue, request_id.clone(), body, ts(2), None)
        .await
        .unwrap();
    assert_eq!(replayed, first_ids, "same request/body replays ids");
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "replay does not append a second item"
    );

    let err = reopened
        .push_with_request_id(
            &queue,
            request_id,
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(1)),
                ..PushSpec::default()
            }],
            ts(3),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::RequestIdConflict);
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "conflict does not append"
    );

    let fresh_ids = reopened
        .push_with_request_id(
            &queue,
            RequestId::new("push-request-fresh").unwrap(),
            vec![PushSpec::default()],
            ts(4),
            None,
        )
        .await
        .unwrap();
    assert_ne!(
        fresh_ids, first_ids,
        "a request id never committed before restart is fresh"
    );
    assert_eq!(reopened.metrics(&queue).await.unwrap().pending, 2);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bounded_mutation_is_replayed_from_object_log_after_projection_reopen() {
    let root = tmp_root("bounded-mutation");
    let sqlite_path = root.join("projection.sqlite");
    let queue = shard();
    let item_id = {
        let backend = open_immediate_hybrid(&root, &sqlite_path);
        backend.create_queue(mutation_qdef()).await.unwrap();
        let item_id = backend
            .push(
                &queue,
                vec![PushSpec {
                    entity: Some(serde_json::json!({ "kind": "effect", "suppressed": false })),
                    ..PushSpec::default()
                }],
                ts(1),
                None,
            )
            .await
            .unwrap()[0];
        let mut set_fields = BTreeMap::new();
        set_fields.insert("suppressed".into(), TypedValue::Bool(true));
        let result = backend
            .bounded_mutation(
                &queue,
                BoundedMutationRequest {
                    index: Some("by_kind_suppressed".into()),
                    filters: vec![QueryFilter {
                        field: "kind".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("effect".into()),
                    }],
                    set_fields,
                    max_scan_rows: 100,
                },
                fireweed_engine::BoundedMutationContext {
                    now: ts(2),
                    expected_epoch: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.results[0].item_id, item_id);
        item_id
    };

    std::fs::remove_file(&sqlite_path).expect("delete rebuildable projection");
    let reopened = open_immediate_hybrid(&root, &sqlite_path);
    let rows = reopened
        .range_scan(
            &queue,
            RangeScanRequest {
                index: Some("by_kind_suppressed".into()),
                filters: vec![QueryFilter {
                    field: "suppressed".into(),
                    op: FilterOp::Eq,
                    value: TypedValue::Bool(true),
                }],
                order_by: vec![OrderField {
                    field: "suppressed".into(),
                    direction: SortDirection::Ascending,
                }],
                page_size: 10,
                cursor: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].item_id, item_id);

    let _ = std::fs::remove_dir_all(&root);
}
