//! API-004 hot projection query surface: typed range/group/bucket queries are exercised over the
//! scheduled-action fixture, and bounded mutation is verified against the same typed index surface.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use fireweed::{
    AggregateGroup, BoundedMutationRequest, ClaimByQueryRequest, ClaimRef, CommitEntry,
    CommitRequest, DeclaredBucketSegmentRequest, EligibilityPolicy, FilterOp, FinalizeKind,
    GroupByField, GroupedAggregateRequest, ItemId, LibBackend, MutationOutcome, NewItem,
    OrderField, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueryFilter, QueueDefinition, QueueId, RangeScanRequest, RangeScanRow,
    RecurrencePolicy, RetryPolicy, RuntimeCore, SortDirection, TenantId, TypedValue, UtcTimestamp,
};
use fireweed_core::{
    CompoundIndexDef, CompoundIndexField, IndexDeclaration, IndexType, QueueIndex,
};
use fireweed_engine::{EngineError, HotProjectionQueryPort};
use fireweed_memory::{ManualClock, composed_memory_backend};
use fireweed_sqlite::SqliteRelationalBackend;
use serde_json::{Value, json};

fn qkey() -> fireweed::QueueKey {
    fireweed::QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

#[derive(Default)]
struct UnsupportedHotProjectionBackend;

impl HotProjectionQueryPort for UnsupportedHotProjectionBackend {}

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
        emit_change_records: true,
    }
}

fn bounded_mutation_request() -> BoundedMutationRequest {
    let mut set_fields = BTreeMap::new();
    set_fields.insert(
        "suppressed_by_recycling".to_string(),
        TypedValue::Bool(true),
    );
    BoundedMutationRequest {
        index: Some("by_status_enrollment".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "status".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("scheduled".to_string()),
            },
            QueryFilter {
                field: "is_enrolled_using_open_rate_filter".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::Bool(false),
            },
        ],
        set_fields,
        max_scan_rows: 500,
    }
}

fn claim_conflict_bounded_mutation_request() -> BoundedMutationRequest {
    let mut set_fields = BTreeMap::new();
    set_fields.insert(
        "suppressed_by_recycling".to_string(),
        TypedValue::Bool(true),
    );
    BoundedMutationRequest {
        index: Some("by_target_key".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "target_key".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("contact:001".to_string()),
            },
        ],
        set_fields,
        max_scan_rows: 500,
    }
}

fn claim_due_scheduled_actions_request() -> ClaimByQueryRequest {
    ClaimByQueryRequest {
        index: Some("by_action_type".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "action_type".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("message.send".to_string()),
            },
            QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Lte,
                value: TypedValue::DateTime(UtcTimestamp::new(1_783_004_400, 0).expect("valid ts")),
            },
        ],
        order_by: OrderField {
            field: "scheduled_at".to_string(),
            direction: SortDirection::Ascending,
        },
        max_items: 2,
        lease_duration_ms: 30_000,
        worker_id: fireweed_core::WorkerId::new("query-worker").expect("worker id"),
        request_id: Some(fireweed_core::RequestId::new("query-request").expect("request id")),
    }
}

#[tokio::test]
async fn safe_recycling_rule_update_marks_only_act_001() {
    let memory_fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_safe_recycling_rule_update_on_backend(&memory_fireweed).await;

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-bounded-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_fireweed = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_safe_recycling_rule_update_on_backend(&sqlite_fireweed).await;
}

#[tokio::test]
async fn bounded_mutation_rejects_claimed_records_without_losing_the_claim() {
    let memory_fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_bounded_mutation_rejects_claimed_records_without_losing_the_claim(&memory_fireweed)
        .await;

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-bounded-claim-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_fireweed = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_bounded_mutation_rejects_claimed_records_without_losing_the_claim(&sqlite_fireweed)
        .await;
}

#[tokio::test]
async fn claim_due_scheduled_actions_by_query() {
    let memory_fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_claim_due_scheduled_actions_by_query_on_backend(&memory_fireweed).await;

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-claim-by-query-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_fireweed = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_claim_due_scheduled_actions_by_query_on_backend(&sqlite_fireweed).await;
}

#[tokio::test]
async fn hourly_distribution_by_status() {
    let memory_fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_hourly_distribution_by_status_on_backend(&memory_fireweed).await;

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-hourly-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_fireweed = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_hourly_distribution_by_status_on_backend(&sqlite_fireweed).await;
}

#[tokio::test]
async fn recycling_preview_by_hour() {
    let memory_fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_recycling_preview_by_hour_on_backend(&memory_fireweed).await;

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-recycling-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_fireweed = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_recycling_preview_by_hour_on_backend(&sqlite_fireweed).await;
}

#[tokio::test]
async fn engagement_probability_segments() {
    let memory_fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_engagement_probability_segments_on_backend(&memory_fireweed).await;

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-engagement-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite_fireweed = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_engagement_probability_segments_on_backend(&sqlite_fireweed).await;
}

// ---------------------------------------------------------------------------
// Snorri-shaped hot projection conformance fixture (pqueue-4529ede9)
//
// Domain-neutral scheduled-action-shaped records, seeded as ordinary CLAIMABLE queue items over a
// typed indexed queue (ADR-011). This fixture does not embed Snorri semantics into fireweed: the field
// names are the caller's entity document, not a fireweed schema. See API-004 "Example Fixture".
// ---------------------------------------------------------------------------

fn typed_index(name: &str, declaration: IndexDeclaration) -> QueueIndex {
    QueueIndex {
        name: name.to_string(),
        declaration,
    }
}

fn compound_field(field: &str, index_type: IndexType) -> CompoundIndexField {
    CompoundIndexField {
        field: field.to_string(),
        index_type,
    }
}

/// The seven canonical typed compound indexes declared over the fixture queue (API-004 "Canonical
/// Typed Compound Indexes").
fn scheduled_action_typed_indexes() -> Vec<QueueIndex> {
    vec![
        typed_index(
            "by_scheduled_at",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("scheduled_at", IndexType::Datetime),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_status",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("status", IndexType::String),
                    compound_field("scheduled_at", IndexType::Datetime),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_status_enrollment",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("status", IndexType::String),
                    compound_field("is_enrolled_using_open_rate_filter", IndexType::Boolean),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_action_type",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("action_type", IndexType::String),
                    compound_field("scheduled_at", IndexType::Datetime),
                ],
                unique: false,
            }),
        ),
        // Dense base population for sparse bucket NULL complements.  Every field is constrained by
        // engagement_probability_request; unlike the scheduled ordering index, it has no nullable
        // unconstrained suffix.
        typed_index(
            "by_action_type_base",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("action_type", IndexType::String),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_recycling",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("suppressed_by_recycling", IndexType::Boolean),
                    compound_field("scheduled_at", IndexType::Datetime),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_algorithm",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("scheduler_algorithm", IndexType::String),
                    compound_field("scheduled_at", IndexType::Datetime),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_engagement_probability",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("engagement_probability", IndexType::Float),
                ],
                unique: false,
            }),
        ),
        typed_index(
            "by_target_key",
            IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    compound_field("tenant_id", IndexType::String),
                    compound_field("run_id", IndexType::String),
                    compound_field("target_key", IndexType::String),
                ],
                unique: true,
            }),
        ),
    ]
}

fn scheduled_action_queue_definition() -> QueueDefinition {
    QueueDefinition {
        typed_indexes: scheduled_action_typed_indexes(),
        ..queue_definition()
    }
}

/// The six canonical scheduled-action fixture records (API-004 "Example Fixture", originally drafted
/// in the superseded task pqueue-630dbeaa). Every record shares `tenant_id`, `account_id`,
/// `workflow_id`, `run_id`, and `engagement_threshold`; `instance_id`/`target_key` follow the action
/// number. `act_004.engagement_probability` is `null` and MUST be absent from the numeric index.
fn scheduled_action_fixture_records() -> Vec<Value> {
    let shared = json!({
        "tenant_id": "tenant_7s",
        "account_id": "acct_42",
        "workflow_id": "wf_nurture",
        "run_id": "job_9001",
        "engagement_threshold": 0.10
    });

    let overrides = vec![
        json!({
            "action_id": "act_001", "instance_id": "inst_contact_001", "target_key": "contact:001",
            "scheduled_at": "2026-07-02T14:05:00Z", "status": "scheduled", "action_type": "message.send",
            "scheduler_algorithm": "personalized", "engagement_probability": 0.0825,
            "suppressed_by_recycling": true, "is_enrolled_using_open_rate_filter": false
        }),
        json!({
            "action_id": "act_002", "instance_id": "inst_contact_002", "target_key": "contact:002",
            "scheduled_at": "2026-07-02T14:37:00Z", "status": "scheduled", "action_type": "message.send",
            "scheduler_algorithm": "personalized", "engagement_probability": 0.1280,
            "suppressed_by_recycling": false, "is_enrolled_using_open_rate_filter": true
        }),
        json!({
            "action_id": "act_003", "instance_id": "inst_contact_003", "target_key": "contact:003",
            "scheduled_at": "2026-07-02T15:02:00Z", "status": "suppressed", "action_type": "message.send",
            "scheduler_algorithm": "randomized", "engagement_probability": 0.0000,
            "suppressed_by_recycling": true, "is_enrolled_using_open_rate_filter": false
        }),
        json!({
            "action_id": "act_004", "instance_id": "inst_contact_004", "target_key": "contact:004",
            "scheduled_at": "2026-07-02T15:45:00Z", "status": "scheduled", "action_type": "message.send",
            "scheduler_algorithm": "randomized", "engagement_probability": Value::Null,
            "suppressed_by_recycling": false, "is_enrolled_using_open_rate_filter": true
        }),
        json!({
            "action_id": "act_005", "instance_id": "inst_contact_005", "target_key": "contact:005",
            "scheduled_at": "2026-07-03T09:15:00Z", "status": "failed", "action_type": "message.send",
            "scheduler_algorithm": "personalized", "engagement_probability": 0.4510,
            "suppressed_by_recycling": false, "is_enrolled_using_open_rate_filter": true
        }),
        json!({
            "action_id": "act_006", "instance_id": "inst_contact_006", "target_key": "contact:006",
            "scheduled_at": "2026-07-03T09:50:00Z", "status": "scheduled", "action_type": "subject.mutation",
            "scheduler_algorithm": "personalized", "engagement_probability": 0.9100,
            "suppressed_by_recycling": false, "is_enrolled_using_open_rate_filter": true
        }),
    ];

    overrides
        .into_iter()
        .map(|mut record| {
            let record_obj = record.as_object_mut().unwrap();
            for (k, v) in shared.as_object().unwrap() {
                record_obj.insert(k.clone(), v.clone());
            }
            record
        })
        .collect()
}

fn scheduled_action_item(record: &Value) -> NewItem {
    NewItem {
        payload: Some(bytes::Bytes::from(record.to_string())),
        entity: Some(record.clone()),
        ..Default::default()
    }
}

fn group_datetime(group: &AggregateGroup, field: &str) -> UtcTimestamp {
    match group.key.get(field).expect("group key field") {
        TypedValue::DateTime(ts) => *ts,
        other => panic!("expected datetime for {field}, got {other:?}"),
    }
}

fn group_string(group: &AggregateGroup, field: &str) -> String {
    match group.key.get(field).expect("group key field") {
        TypedValue::String(value) => value.clone(),
        other => panic!("expected string for {field}, got {other:?}"),
    }
}

fn group_bool(group: &AggregateGroup, field: &str) -> bool {
    match group.key.get(field).expect("group key field") {
        TypedValue::Bool(value) => *value,
        other => panic!("expected bool for {field}, got {other:?}"),
    }
}

fn assert_grouped_status_counts(groups: &[AggregateGroup], expected: &[(UtcTimestamp, &str, u64)]) {
    let mut actual = groups
        .iter()
        .map(|group| {
            (
                group_datetime(group, "scheduled_at"),
                group_string(group, "status"),
                group.count,
            )
        })
        .collect::<Vec<_>>();
    actual.sort_by_key(|entry| (entry.0, entry.1.clone()));
    let mut expected = expected
        .iter()
        .map(|(ts, status, count)| (*ts, status.to_string(), *count))
        .collect::<Vec<_>>();
    expected.sort_by_key(|entry| (entry.0, entry.1.clone()));
    assert_eq!(actual, expected);
}

fn assert_grouped_recycling_counts(
    groups: &[AggregateGroup],
    expected: &[(UtcTimestamp, bool, u64)],
) {
    let mut actual = groups
        .iter()
        .map(|group| {
            (
                group_datetime(group, "scheduled_at"),
                group_bool(group, "suppressed_by_recycling"),
                group.count,
            )
        })
        .collect::<Vec<_>>();
    actual.sort_by_key(|entry| (entry.0, entry.1));
    let mut expected = expected
        .iter()
        .map(|(ts, value, count)| (*ts, *value, *count))
        .collect::<Vec<_>>();
    expected.sort_by_key(|entry| (entry.0, entry.1));
    assert_eq!(actual, expected);
}

async fn seed_scheduled_action_fixture<B: LibBackend>(
    fireweed: &RuntimeCore<B>,
) -> Vec<(ItemId, String)> {
    let q = qkey();
    fireweed
        .create_queue(scheduled_action_queue_definition())
        .await
        .unwrap();
    let mut ids = Vec::new();
    for record in scheduled_action_fixture_records() {
        let action_id = record["action_id"].as_str().unwrap().to_string();
        let minted = fireweed
            .push(&q, scheduled_action_item(&record))
            .await
            .unwrap();
        ids.push((minted, action_id));
    }
    ids
}

async fn assert_hourly_distribution_by_status_on_backend<B: LibBackend>(fireweed: &RuntimeCore<B>) {
    let q = qkey();
    let _ = seed_scheduled_action_fixture(fireweed).await;
    let response = fireweed
        .grouped_aggregate(&q, hourly_distribution_request())
        .await
        .unwrap();
    assert_grouped_status_counts(
        &response.groups,
        &[
            (
                UtcTimestamp::new(1_783_000_800, 0).expect("valid ts"),
                "scheduled",
                2,
            ),
            (
                UtcTimestamp::new(1_783_004_400, 0).expect("valid ts"),
                "scheduled",
                1,
            ),
            (
                UtcTimestamp::new(1_783_004_400, 0).expect("valid ts"),
                "suppressed",
                1,
            ),
            (
                UtcTimestamp::new(1_783_069_200, 0).expect("valid ts"),
                "failed",
                1,
            ),
        ],
    );
    assert_eq!(
        response.groups.iter().map(|group| group.count).sum::<u64>(),
        5
    );
}

async fn assert_recycling_preview_by_hour_on_backend<B: LibBackend>(fireweed: &RuntimeCore<B>) {
    let q = qkey();
    let _ = seed_scheduled_action_fixture(fireweed).await;
    let response = fireweed
        .grouped_aggregate(&q, recycling_preview_request())
        .await
        .unwrap();
    assert_grouped_recycling_counts(
        &response.groups,
        &[
            (
                UtcTimestamp::new(1_783_000_800, 0).expect("valid ts"),
                true,
                1,
            ),
            (
                UtcTimestamp::new(1_783_000_800, 0).expect("valid ts"),
                false,
                1,
            ),
            (
                UtcTimestamp::new(1_783_004_400, 0).expect("valid ts"),
                true,
                1,
            ),
            (
                UtcTimestamp::new(1_783_004_400, 0).expect("valid ts"),
                false,
                1,
            ),
            (
                UtcTimestamp::new(1_783_069_200, 0).expect("valid ts"),
                false,
                1,
            ),
        ],
    );
    assert_eq!(
        response.groups.iter().map(|group| group.count).sum::<u64>(),
        5
    );
}

async fn assert_engagement_probability_segments_on_backend<B: LibBackend>(
    fireweed: &RuntimeCore<B>,
) {
    let q = qkey();
    let _ = seed_scheduled_action_fixture(fireweed).await;
    let response = fireweed
        .declared_bucket_segment(&q, engagement_probability_request())
        .await
        .unwrap();
    let labels = response
        .buckets
        .iter()
        .map(|bucket| (bucket.label.clone(), bucket.count))
        .collect::<Vec<_>>();
    assert_eq!(
        labels,
        vec![
            ("0%".to_string(), 1),
            ("8.01-10%".to_string(), 1),
            ("10.01-15%".to_string(), 1),
            ("45.01-50%".to_string(), 1),
            ("no-activity".to_string(), 1),
        ]
    );
    assert_eq!(
        response
            .buckets
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        5
    );
}

async fn assert_safe_recycling_rule_update_on_backend<B: LibBackend>(fireweed: &RuntimeCore<B>) {
    let q = qkey();
    seed_scheduled_action_fixture(fireweed).await;
    let before = fireweed
        .query_index_unique_typed(
            &q,
            "by_target_key",
            &[json!("tenant_7s"), json!("job_9001"), json!("contact:001")],
        )
        .await
        .unwrap()
        .expect("contact:001 should be indexed");

    let response = fireweed
        .bounded_mutation(&q, bounded_mutation_request())
        .await
        .unwrap();
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].item_id, before.item_id);
    assert_eq!(response.results[0].outcome, MutationOutcome::Updated);

    let after = fireweed
        .query_index_unique_typed(
            &q,
            "by_target_key",
            &[json!("tenant_7s"), json!("job_9001"), json!("contact:001")],
        )
        .await
        .unwrap()
        .expect("contact:001 should stay indexed");
    assert_eq!(
        after.item_version,
        before.item_version + 1,
        "bounded mutation must bump item_version for the touched record"
    );
    let touched = response
        .results
        .iter()
        .map(|result| result.item_id)
        .collect::<Vec<_>>();
    assert_eq!(touched, vec![before.item_id]);
}

async fn assert_bounded_mutation_rejects_claimed_records_without_losing_the_claim<B: LibBackend>(
    fireweed: &RuntimeCore<B>,
) {
    let q = qkey();
    seed_scheduled_action_fixture(fireweed).await;
    let target = fireweed
        .query_index_unique_typed(
            &q,
            "by_target_key",
            &[json!("tenant_7s"), json!("job_9001"), json!("contact:001")],
        )
        .await
        .unwrap()
        .expect("contact:001 should be indexed");
    let claimed = fireweed.claim(&q, 6, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 6);
    let claimed_item = claimed
        .iter()
        .find(|item| item.item_id == target.item_id)
        .expect("contact:001 should be claimed");
    let claimed_version = claimed_item.item_version;

    let conflict = fireweed
        .bounded_mutation(&q, claim_conflict_bounded_mutation_request())
        .await
        .unwrap();
    assert_eq!(conflict.results.len(), 1);
    assert_eq!(conflict.results[0].item_id, target.item_id);
    assert_eq!(conflict.results[0].outcome, MutationOutcome::Conflict);

    let still_claimed = fireweed.claimed(&q, &[target.item_id]).await.unwrap();
    assert_eq!(still_claimed.len(), 1);
    assert_eq!(still_claimed[0].item_version, claimed_version);
}

async fn assert_claim_due_scheduled_actions_by_query_on_backend<B: LibBackend>(
    fireweed: &RuntimeCore<B>,
) {
    let q = qkey();
    let ids = seed_scheduled_action_fixture(fireweed).await;
    let ordinarily_claimed = fireweed.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(ordinarily_claimed.len(), 1);

    let claimed = fireweed
        .claim_by_query(&q, claim_due_scheduled_actions_request())
        .await
        .unwrap();
    let mut invalid_changed_body = claim_due_scheduled_actions_request();
    invalid_changed_body.index = Some("missing-index".to_string());
    assert_eq!(
        fireweed
            .claim_by_query(&q, invalid_changed_body)
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "retained request ids use conflict-first precedence before structural query validation"
    );
    assert!(
        claimed
            .items
            .iter()
            .all(|item| item.lease_expires_at == UtcTimestamp::new(30, 0).unwrap()),
        "public claim-by-query leases must be based on the injected clock"
    );
    let by_id: HashMap<ItemId, String> = ids.into_iter().collect();
    assert_eq!(
        by_id
            .get(&ordinarily_claimed[0].item_id)
            .map(String::as_str),
        Some("act_001")
    );
    let action_ids = claimed
        .items
        .iter()
        .map(|item| {
            by_id
                .get(&item.item_id)
                .cloned()
                .expect("claimed row should map to a seeded action_id")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        action_ids,
        vec!["act_002"],
        "query claim must exclude the active ordinary lease on act_001"
    );
    let token = claimed.items[0].lease_token.as_ref().unwrap().as_str();
    assert_eq!(token.len(), 68);
    assert!(token.starts_with("cbq-"));
    assert!(token[4..].bytes().all(|byte| byte.is_ascii_hexdigit()));

    let entries = claimed
        .items
        .iter()
        .map(|item| CommitEntry {
            claim_ref: ClaimRef {
                item_id: item.item_id,
                lease_token: item
                    .lease_token
                    .clone()
                    .expect("claim-by-query must return a lease token"),
                lease_expires_at: item.lease_expires_at,
                item_version: item.item_version,
            },
            finalize: FinalizeKind::Complete,
            side_records: vec![],
            lifecycle_items: vec![],
            instance_fence: None,
        })
        .collect::<Vec<_>>();
    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: None,
                entries,
            },
        )
        .await
        .unwrap();
    assert!(
        outcomes
            .iter()
            .all(|outcome| matches!(outcome, fireweed::EntryOutcome::Committed { .. }))
    );

    let metrics = fireweed.metrics(&q).await.unwrap();
    assert_eq!(metrics.complete, 1);
    assert_eq!(metrics.leased, 1);
    assert_eq!(metrics.pending, 4);
}

#[tokio::test]
async fn backend_capability_advertising_is_explicit() {
    let q = qkey();
    let memory_expected = fireweed::QueryCapabilityFlags {
        range_scan: true,
        grouped_aggregate: true,
        declared_bucket_segment: true,
        bounded_mutation: true,
        claim_by_query: true,
        side_record_query: false,
        // Memory/compose product implements API-001 BatchClaimByItemIds.
        claim_by_item_ids: true,
    };
    // Relational sqlite still advertises the five paired hot-query flags; claim_by_item_ids lands
    // with the relational implement path (compose memory is the product path for this epic).
    let sqlite_expected = fireweed::QueryCapabilityFlags {
        claim_by_item_ids: false,
        ..memory_expected
    };

    let memory = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    assert_eq!(memory.hot_projection_capabilities(&q), memory_expected);
    assert!(
        memory
            .hot_projection_capabilities(&q)
            .paired_capabilities_consistent()
    );
    assert!(!memory.hot_projection_capabilities(&q).side_record_query);

    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-capabilities-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let sqlite = RuntimeCore::new(
        Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    assert_eq!(sqlite.hot_projection_capabilities(&q), sqlite_expected);
    assert!(
        sqlite
            .hot_projection_capabilities(&q)
            .paired_capabilities_consistent()
    );
    assert!(!sqlite.hot_projection_capabilities(&q).side_record_query);

    let unsupported = UnsupportedHotProjectionBackend;
    let unsupported_flags = unsupported.hot_projection_capabilities(&q);
    assert_eq!(unsupported_flags, fireweed::QueryCapabilityFlags::default());
    assert!(!unsupported_flags.side_record_query);
    assert_eq!(
        unsupported
            .range_scan(
                &q,
                RangeScanRequest {
                    index: None,
                    filters: vec![],
                    order_by: vec![OrderField {
                        field: "scheduled_at".to_string(),
                        direction: SortDirection::Ascending,
                    }],
                    page_size: 1,
                    cursor: None,
                },
            )
            .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        unsupported
            .grouped_aggregate(&q, hourly_distribution_request())
            .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        unsupported
            .declared_bucket_segment(&q, engagement_probability_request())
            .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        unsupported
            .bounded_mutation(
                &q,
                bounded_mutation_request(),
                fireweed_engine::BoundedMutationContext {
                    now: UtcTimestamp::new(0, 0).unwrap(),
                    expected_epoch: None,
                },
            )
            .await,
        Err(EngineError::Unavailable)
    );
    assert!(matches!(
        unsupported
            .claim_by_query(
                &q,
                claim_due_scheduled_actions_request(),
                fireweed_engine::ClaimByQueryContext {
                    now: UtcTimestamp::new(0, 0).unwrap(),
                    eligibility_time: None,
                    expected_epoch: None,
                },
            )
            .await,
        Err(EngineError::Unavailable)
    ));
}

fn hourly_distribution_request() -> GroupedAggregateRequest {
    GroupedAggregateRequest {
        index: Some("by_status".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "action_type".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("message.send".to_string()),
            },
            QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Gte,
                value: TypedValue::DateTime(UtcTimestamp::new(1_782_950_400, 0).expect("valid ts")),
            },
            QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Lt,
                value: TypedValue::DateTime(UtcTimestamp::new(1_783_123_200, 0).expect("valid ts")),
            },
        ],
        group_by: vec![
            GroupByField {
                field: "scheduled_at".to_string(),
                time_bucket: Some(fireweed::TimeBucket::Hour),
            },
            GroupByField {
                field: "status".to_string(),
                time_bucket: None,
            },
        ],
        max_groups: 10,
    }
}

fn recycling_preview_request() -> GroupedAggregateRequest {
    GroupedAggregateRequest {
        index: Some("by_recycling".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "action_type".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("message.send".to_string()),
            },
            QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Gte,
                value: TypedValue::DateTime(UtcTimestamp::new(1_782_950_400, 0).expect("valid ts")),
            },
            QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Lt,
                value: TypedValue::DateTime(UtcTimestamp::new(1_783_123_200, 0).expect("valid ts")),
            },
        ],
        group_by: vec![
            GroupByField {
                field: "scheduled_at".to_string(),
                time_bucket: Some(fireweed::TimeBucket::Hour),
            },
            GroupByField {
                field: "suppressed_by_recycling".to_string(),
                time_bucket: None,
            },
        ],
        max_groups: 10,
    }
}

fn engagement_probability_request() -> DeclaredBucketSegmentRequest {
    DeclaredBucketSegmentRequest {
        index: Some("by_engagement_probability".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "action_type".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("message.send".to_string()),
            },
        ],
        field: "engagement_probability".to_string(),
        buckets: vec![
            fireweed::BucketRule {
                label: "0%".to_string(),
                exact: Some(0.0),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            },
            fireweed::BucketRule {
                label: "8.01-10%".to_string(),
                exact: None,
                gt: Some(0.08),
                gte: None,
                lt: None,
                lte: Some(0.10),
            },
            fireweed::BucketRule {
                label: "10.01-15%".to_string(),
                exact: None,
                gt: Some(0.10),
                gte: None,
                lt: None,
                lte: Some(0.15),
            },
            fireweed::BucketRule {
                label: "45.01-50%".to_string(),
                exact: None,
                gt: Some(0.45),
                gte: None,
                lt: None,
                lte: Some(0.50),
            },
        ],
        null_bucket_label: "no-activity".to_string(),
    }
}

fn action_id_map(ids: &[(ItemId, String)], rows: &[RangeScanRow]) -> Vec<String> {
    let by_id: HashMap<ItemId, String> = ids.iter().cloned().collect();
    rows.iter()
        .map(|row| {
            by_id
                .get(&row.item_id)
                .cloned()
                .expect("row item_id should map to a seeded action_id")
        })
        .collect()
}

#[tokio::test]
async fn ordered_cursor_pagination_is_stable() {
    let q = qkey();
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    fireweed
        .create_queue(scheduled_action_queue_definition())
        .await
        .unwrap();

    let mut ids = Vec::new();
    for record in scheduled_action_fixture_records() {
        let action_id = record["action_id"].as_str().unwrap().to_string();
        let minted = fireweed
            .push(&q, scheduled_action_item(&record))
            .await
            .unwrap();
        ids.push((minted, action_id));
    }

    let request = RangeScanRequest {
        index: Some("by_action_type".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "action_type".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("message.send".to_string()),
            },
        ],
        order_by: vec![OrderField {
            field: "scheduled_at".to_string(),
            direction: SortDirection::Ascending,
        }],
        page_size: 2,
        cursor: None,
    };

    let page1 = fireweed.range_scan(&q, request.clone()).await.unwrap();
    assert_eq!(action_id_map(&ids, &page1.rows), vec!["act_001", "act_002"]);
    let page2 = fireweed
        .range_scan(
            &q,
            RangeScanRequest {
                cursor: page1.next_cursor.clone(),
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(action_id_map(&ids, &page2.rows), vec!["act_003", "act_004"]);
    let page3 = fireweed
        .range_scan(
            &q,
            RangeScanRequest {
                cursor: page2.next_cursor.clone(),
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(action_id_map(&ids, &page3.rows), vec!["act_005"]);
    assert!(page3.next_cursor.is_none());

    let late = json!({
        "tenant_id": "tenant_7s",
        "account_id": "acct_42",
        "workflow_id": "wf_nurture",
        "run_id": "job_9001",
        "action_id": "act_999",
        "instance_id": "inst_contact_999",
        "target_key": "contact:999",
        "scheduled_at": "2026-07-03T12:15:00Z",
        "status": "scheduled",
        "action_type": "message.send",
        "scheduler_algorithm": "personalized",
        "engagement_probability": 0.7777,
        "engagement_threshold": 0.10,
        "suppressed_by_recycling": false,
        "is_enrolled_using_open_rate_filter": true
    });
    let minted = fireweed
        .push(&q, scheduled_action_item(&late))
        .await
        .unwrap();
    ids.push((minted, "act_999".to_string()));

    let page1 = fireweed.range_scan(&q, request.clone()).await.unwrap();
    assert_eq!(action_id_map(&ids, &page1.rows), vec!["act_001", "act_002"]);
    let page2 = fireweed
        .range_scan(
            &q,
            RangeScanRequest {
                cursor: page1.next_cursor.clone(),
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(action_id_map(&ids, &page2.rows), vec!["act_003", "act_004"]);
    let page3 = fireweed
        .range_scan(
            &q,
            RangeScanRequest {
                cursor: page2.next_cursor.clone(),
                ..request.clone()
            },
        )
        .await
        .unwrap();
    let seen = [
        action_id_map(&ids, &page1.rows),
        action_id_map(&ids, &page2.rows),
        action_id_map(&ids, &page3.rows),
    ]
    .concat();
    assert_eq!(
        seen[..5],
        ["act_001", "act_002", "act_003", "act_004", "act_005"]
    );
    assert_eq!(seen.last().cloned(), Some("act_999".to_string()));
}

#[tokio::test]
async fn detail_range_filter_by_run_status_and_schedule() {
    let q = qkey();
    let sqlite_path = std::env::temp_dir()
        .join(format!(
            "fireweed-hot-projection-queries-range-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&sqlite_path);
    let backend = Arc::new(SqliteRelationalBackend::open(&sqlite_path).unwrap());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    fireweed
        .create_queue(scheduled_action_queue_definition())
        .await
        .unwrap();

    let mut ids = Vec::new();
    for record in scheduled_action_fixture_records() {
        let action_id = record["action_id"].as_str().unwrap().to_string();
        let minted = fireweed
            .push(&q, scheduled_action_item(&record))
            .await
            .unwrap();
        ids.push((minted, action_id));
    }

    let request = RangeScanRequest {
        index: Some("by_status".to_string()),
        filters: vec![
            QueryFilter {
                field: "tenant_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("tenant_7s".to_string()),
            },
            QueryFilter {
                field: "run_id".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("job_9001".to_string()),
            },
            QueryFilter {
                field: "status".to_string(),
                op: FilterOp::Eq,
                value: TypedValue::String("scheduled".to_string()),
            },
            QueryFilter {
                field: "scheduled_at".to_string(),
                op: FilterOp::Lte,
                value: TypedValue::DateTime(UtcTimestamp::new(1_783_008_000, 0).expect("valid ts")),
            },
        ],
        order_by: vec![OrderField {
            field: "scheduled_at".to_string(),
            direction: SortDirection::Ascending,
        }],
        page_size: 2,
        cursor: None,
    };

    let page1 = fireweed.range_scan(&q, request.clone()).await.unwrap();
    assert_eq!(action_id_map(&ids, &page1.rows), vec!["act_001", "act_002"]);
    let page2 = fireweed
        .range_scan(
            &q,
            RangeScanRequest {
                cursor: page1.next_cursor.clone(),
                ..request
            },
        )
        .await
        .unwrap();
    assert_eq!(action_id_map(&ids, &page2.rows), vec!["act_004"]);
    assert!(page2.next_cursor.is_none());
}

/// Seeds the six fixture records as ordinary CLAIMABLE items over the typed indexed queue, proves
/// exact typed lookup by `(tenant_id, run_id, target_key)` for `contact:001`, and validates that the
/// claimed row materializes `action_id`, `status`, `scheduled_at`, `action_type`, and
/// `engagement_probability` stably. Also proves `act_004`'s null `engagement_probability` keeps it
/// out of the numeric typed index (API-004 null semantics), rather than minting a synthetic null key.
#[tokio::test]
async fn hot_projection_fixture_seeds_six_claimable_records_and_resolves_target_key_lookup() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed
        .create_queue(scheduled_action_queue_definition())
        .await
        .unwrap();

    let records = scheduled_action_fixture_records();
    for record in &records {
        fireweed
            .push(&q, scheduled_action_item(record))
            .await
            .unwrap();
    }

    // All six records are ordinary CLAIMABLE (pending) queue items — not side/projection records.
    let metrics = fireweed.metrics(&q).await.unwrap();
    assert_eq!(metrics.pending, 6);

    // Exact typed lookup by (tenant_id, run_id, target_key) for contact:001.
    let hit = fireweed
        .query_index_unique_typed(
            &q,
            "by_target_key",
            &[json!("tenant_7s"), json!("job_9001"), json!("contact:001")],
        )
        .await
        .unwrap()
        .expect("contact:001 is indexed by (tenant_id, run_id, target_key)");

    // Stable row materialization: claim the resolved item and check the fixture fields survive.
    let claimed = fireweed.claim(&q, 6, 30_000).await.unwrap();
    let claimed_contact_001 = claimed
        .iter()
        .find(|item| item.item_id == hit.item_id)
        .expect("the target_key hit resolves to a claimed item");
    let materialized: Value = serde_json::from_slice(
        claimed_contact_001
            .payload
            .as_deref()
            .expect("payload carries the entity document"),
    )
    .unwrap();
    assert_eq!(materialized["action_id"], json!("act_001"));
    assert_eq!(materialized["status"], json!("scheduled"));
    assert_eq!(materialized["scheduled_at"], json!("2026-07-02T14:05:00Z"));
    assert_eq!(materialized["action_type"], json!("message.send"));
    assert_eq!(materialized["engagement_probability"], json!(0.0825));

    // act_004's null engagement_probability makes it sparse (absent) in the numeric compound index —
    // a compound key requires every field present, so a null/missing field yields no index entry
    // rather than a synthetic null key (API-004 "Declared Numeric Buckets" null semantics).
    let engagement_probability_index = match &scheduled_action_typed_indexes()
        .into_iter()
        .find(|index| index.name == "by_engagement_probability")
        .unwrap()
        .declaration
    {
        IndexDeclaration::Compound(def) => def.clone(),
        IndexDeclaration::Single(_) => unreachable!(),
    };
    let act_004 = records
        .iter()
        .find(|r| r["action_id"] == json!("act_004"))
        .unwrap();
    assert_eq!(
        engagement_probability_index.index_key(act_004).unwrap(),
        None,
        "a null engagement_probability must be absent from the numeric typed index"
    );
}
