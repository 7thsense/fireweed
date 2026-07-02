//! API-004 hot projection query surface: every backend advertises the v1 capability set as
//! unavailable and every operation rejects with a structured [`EngineError::Unavailable`] rather
//! than silently degrading to a full scan. No backend bead in epic pqueue-45e13e4d implements the
//! substrate yet (this bead ships only the typed request/response shapes + compile-tested stubs).

use std::collections::BTreeMap;
use std::sync::Arc;

use pqueue::{
    BoundedMutationRequest, ClaimByQueryRequest, DeclaredBucketSegmentRequest, EligibilityPolicy,
    EngineError, FilterOp, GroupByField, GroupedAggregateRequest, OrderField, OrderingMode, Pqueue,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueryCapabilityFlags,
    QueryFilter, QueueDefinition, QueueId, RangeScanRequest, RecurrencePolicy, RetryPolicy,
    SortDirection, TenantId, TypedValue,
};
use pqueue_core::WorkerId;
use pqueue_memory::{ManualClock, composed_memory_backend};
use pqueue_objectlog::ObjectLogBackend;
use pqueue_sqlite::SqliteRelationalBackend;

fn qkey() -> pqueue::QueueKey {
    pqueue::QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn queue_definition() -> QueueDefinition {
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
    }
}

fn range_scan_request() -> RangeScanRequest {
    RangeScanRequest {
        index: Some("by_scheduled_at".to_string()),
        filters: vec![QueryFilter {
            field: "scheduled_at".to_string(),
            op: FilterOp::Gte,
            value: TypedValue::Integer(0),
        }],
        order_by: vec![OrderField {
            field: "scheduled_at".to_string(),
            direction: SortDirection::Ascending,
        }],
        page_size: 50,
        cursor: None,
    }
}

fn grouped_aggregate_request() -> GroupedAggregateRequest {
    GroupedAggregateRequest {
        index: Some("by_status".to_string()),
        filters: vec![],
        group_by: vec![GroupByField {
            field: "status".to_string(),
            time_bucket: None,
        }],
        max_groups: 100,
    }
}

fn declared_bucket_segment_request() -> DeclaredBucketSegmentRequest {
    DeclaredBucketSegmentRequest {
        index: Some("by_engagement_probability".to_string()),
        filters: vec![],
        field: "engagement_probability".to_string(),
        buckets: vec![],
        null_bucket_label: "no-activity".to_string(),
    }
}

fn bounded_mutation_request() -> BoundedMutationRequest {
    let mut set_fields = BTreeMap::new();
    set_fields.insert(
        "suppressed_by_recycling".to_string(),
        TypedValue::Bool(true),
    );
    BoundedMutationRequest {
        index: Some("by_status".to_string()),
        filters: vec![],
        set_fields,
        max_scan_rows: 500,
    }
}

fn claim_by_query_request() -> ClaimByQueryRequest {
    ClaimByQueryRequest {
        index: Some("by_scheduled_at".to_string()),
        filters: vec![],
        order_by: OrderField {
            field: "scheduled_at".to_string(),
            direction: SortDirection::Ascending,
        },
        max_items: 10,
        lease_duration_ms: 30_000,
        worker_id: WorkerId::new("worker-1").unwrap(),
        request_id: None,
    }
}

#[tokio::test]
async fn capability_defaults_are_explicitly_unavailable() {
    let q = qkey();

    // Log-replay / atomic-class family (memory).
    let memory_backend = Arc::new(composed_memory_backend());
    let memory_pq = Pqueue::new(memory_backend, Arc::new(ManualClock::at(0)));
    memory_pq.create_queue(queue_definition()).await.unwrap();
    assert_eq!(
        memory_pq.hot_projection_capabilities(&q),
        QueryCapabilityFlags::default()
    );
    assert_eq!(
        memory_pq.range_scan(&q, range_scan_request()).await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        memory_pq
            .grouped_aggregate(&q, grouped_aggregate_request())
            .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        memory_pq
            .declared_bucket_segment(&q, declared_bucket_segment_request())
            .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        memory_pq
            .bounded_mutation(&q, bounded_mutation_request())
            .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        memory_pq
            .claim_by_query(&q, claim_by_query_request())
            .await
            .unwrap_err(),
        EngineError::Unavailable
    );

    // Sqlite-relational family.
    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "pqueue-hot-projection-queries-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_backend = Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap());
    let sqlite_pq = Pqueue::new(sqlite_backend, Arc::new(ManualClock::at(0)));
    sqlite_pq.create_queue(queue_definition()).await.unwrap();
    assert_eq!(
        sqlite_pq.hot_projection_capabilities(&q),
        QueryCapabilityFlags::default()
    );
    assert_eq!(
        sqlite_pq.range_scan(&q, range_scan_request()).await,
        Err(EngineError::Unavailable)
    );

    // Eventual-apply object-log family.
    let root = std::env::temp_dir().join(format!(
        "pqueue-hot-projection-queries-objlog-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let objectlog_backend = Arc::new(ObjectLogBackend::open(&root).unwrap());
    let objectlog_pq = Pqueue::new(objectlog_backend, Arc::new(ManualClock::at(0)));
    objectlog_pq.create_queue(queue_definition()).await.unwrap();
    assert_eq!(
        objectlog_pq.hot_projection_capabilities(&q),
        QueryCapabilityFlags::default()
    );
    assert_eq!(
        objectlog_pq.range_scan(&q, range_scan_request()).await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        objectlog_pq
            .claim_by_query(&q, claim_by_query_request())
            .await
            .unwrap_err(),
        EngineError::Unavailable
    );

    // `side_record_query` is independently gated and MUST remain unavailable everywhere in this
    // epic — asserted once, not per-backend, since it is a fixed constant on the shared flags type.
    assert!(!QueryCapabilityFlags::default().side_record_query);
}
