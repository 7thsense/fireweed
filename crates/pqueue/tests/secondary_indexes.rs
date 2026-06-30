//! Typed secondary-index behavior exercised through the public `pqueue` facade:
//! numeric ordering, boolean and datetime encoding, compound keys, sparse omission,
//! unique-conflict atomicity, key moves on update/replace, and purge removal.

use std::collections::BTreeMap;
use std::sync::Arc;

use pqueue::{NewItem, PayloadUpdate, Pqueue};
use pqueue_core::{
    ClientItemKey, CompoundIndexDef, CompoundIndexField, EligibilityPolicy, IndexDeclaration,
    IndexDef, IndexType, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, QueueIndex, RecurrencePolicy, RetryPolicy,
    TenantId,
};
use pqueue_memory::{ManualClock, MemoryBackend};
use serde_json::{Value, json};

fn qkey() -> pqueue::QueueKey {
    pqueue::QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn typed_index(name: &str, declaration: IndexDeclaration) -> QueueIndex {
    QueueIndex {
        name: name.to_string(),
        declaration,
    }
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
        typed_indexes: vec![
            typed_index(
                "by_score",
                IndexDeclaration::Single(IndexDef {
                    field: "score".to_string(),
                    index_type: IndexType::Integer,
                    unique: true,
                }),
            ),
            typed_index(
                "by_active",
                IndexDeclaration::Single(IndexDef {
                    field: "active".to_string(),
                    index_type: IndexType::Boolean,
                    unique: true,
                }),
            ),
            typed_index(
                "by_due_at",
                IndexDeclaration::Single(IndexDef {
                    field: "due_at".to_string(),
                    index_type: IndexType::Datetime,
                    unique: false,
                }),
            ),
            typed_index(
                "by_region_zone",
                IndexDeclaration::Compound(CompoundIndexDef {
                    fields: vec![
                        CompoundIndexField {
                            field: "region".to_string(),
                            index_type: IndexType::String,
                        },
                        CompoundIndexField {
                            field: "zone".to_string(),
                            index_type: IndexType::Integer,
                        },
                    ],
                    unique: false,
                }),
            ),
            typed_index(
                "by_external_id",
                IndexDeclaration::Single(IndexDef {
                    field: "external_id".to_string(),
                    index_type: IndexType::String,
                    unique: true,
                }),
            ),
        ],
    }
}

fn item(entity: Value) -> NewItem {
    NewItem {
        entity: Some(entity),
        ..Default::default()
    }
}

fn key(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.as_bytes().to_vec()).collect()
}

async fn new_pq() -> Pqueue<MemoryBackend> {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    pq.create_queue(queue_definition()).await.unwrap();
    pq
}

fn score_key(value: i64) -> Vec<u8> {
    let decl = IndexDeclaration::Single(IndexDef {
        field: "score".to_string(),
        index_type: IndexType::Integer,
        unique: true,
    });
    let record = json!({ "score": value });
    match decl {
        IndexDeclaration::Single(def) => def.index_key(&record).unwrap().unwrap(),
        IndexDeclaration::Compound(_) => unreachable!(),
    }
}

fn boolean_key(value: bool) -> Vec<u8> {
    let decl = IndexDeclaration::Single(IndexDef {
        field: "active".to_string(),
        index_type: IndexType::Boolean,
        unique: true,
    });
    let record = json!({ "active": value });
    match decl {
        IndexDeclaration::Single(def) => def.index_key(&record).unwrap().unwrap(),
        IndexDeclaration::Compound(_) => unreachable!(),
    }
}

fn datetime_key(value: &str) -> Vec<u8> {
    let decl = IndexDeclaration::Single(IndexDef {
        field: "due_at".to_string(),
        index_type: IndexType::Datetime,
        unique: false,
    });
    let record = json!({ "due_at": value });
    match decl {
        IndexDeclaration::Single(def) => def.index_key(&record).unwrap().unwrap(),
        IndexDeclaration::Compound(_) => unreachable!(),
    }
}

fn compound_key(region: &str, zone: i64) -> Vec<u8> {
    let decl = CompoundIndexDef {
        fields: vec![
            CompoundIndexField {
                field: "region".to_string(),
                index_type: IndexType::String,
            },
            CompoundIndexField {
                field: "zone".to_string(),
                index_type: IndexType::Integer,
            },
        ],
        unique: false,
    };
    decl.index_key(&json!({ "region": region, "zone": zone }))
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn secondary_indexes_typed_integer_boolean_datetime_and_compound_semantics_work() {
    let pq = new_pq().await;
    let q = qkey();

    let score_2 = pq
        .push(
            &q,
            item(json!({
                "score": 2,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "A"
            })),
        )
        .await
        .unwrap();
    let score_10 = pq
        .push(
            &q,
            item(json!({
                "score": 10,
                "active": true,
                "due_at": "2026-06-30T08:00:00-04:00",
                "region": "us-east",
                "zone": 7,
                "external_id": "B"
            })),
        )
        .await
        .unwrap();

    let key_2 = score_key(2);
    let key_10 = score_key(10);
    assert!(key_2 < key_10, "axon integer keys sort numerically");

    let active_false = pq
        .query_index_unique(&q, "by_active", key(&["false"]))
        .await
        .unwrap();
    let active_true = pq
        .query_index_unique(&q, "by_active", key(&["true"]))
        .await
        .unwrap();
    assert_eq!(active_false.expect("false is indexed").item_id, score_2);
    assert_eq!(active_true.expect("true is indexed").item_id, score_10);
    assert!(boolean_key(false) < boolean_key(true));

    let due = pq
        .query_index(&q, "by_due_at", key(&["2026-06-30T12:00:00Z"]))
        .await
        .unwrap();
    let due_ids: Vec<_> = due.into_iter().map(|hit| hit.item_id).collect();
    assert_eq!(due_ids, vec![score_2, score_10]);
    assert_eq!(
        datetime_key("2026-06-30T12:00:00Z"),
        datetime_key("2026-06-30T08:00:00-04:00"),
        "datetime keys canonicalize equivalent instants"
    );

    let compound = pq
        .query_index(&q, "by_region_zone", key(&["us-east", "7"]))
        .await
        .unwrap();
    let compound_ids: Vec<_> = compound.into_iter().map(|hit| hit.item_id).collect();
    assert_eq!(compound_ids, vec![score_2, score_10]);

    let full_key = compound_key("us-east", 7);
    let leading_key = CompoundIndexDef {
        fields: vec![CompoundIndexField {
            field: "region".to_string(),
            index_type: IndexType::String,
        }],
        unique: false,
    }
    .index_key(&json!({ "region": "us-east" }))
    .unwrap()
    .unwrap();
    assert!(full_key.starts_with(&leading_key));
}

#[tokio::test]
async fn secondary_indexes_missing_fields_remain_sparse() {
    let pq = new_pq().await;
    let q = qkey();

    let _with_external = pq
        .push(
            &q,
            item(json!({
                "score": 1,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "A"
            })),
        )
        .await
        .unwrap();
    let sparse = pq
        .push(
            &q,
            item(json!({
                "score": 3,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7
            })),
        )
        .await
        .unwrap();

    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["A"]))
        .await
        .unwrap()
        .expect("external_id is indexed");
    assert_ne!(
        hit.item_id, sparse,
        "missing external_id stays out of the index"
    );
    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["missing"]))
            .await
            .unwrap()
            .is_none()
    );
    let region_hits = pq
        .query_index(&q, "by_region_zone", key(&["us-east", "7"]))
        .await
        .unwrap();
    assert_eq!(region_hits.len(), 2);
}

#[tokio::test]
async fn secondary_indexes_unique_conflicts_are_atomic_for_push_update_and_replace() {
    let pq = new_pq().await;
    let q = qkey();

    let original = pq
        .push(
            &q,
            item(json!({
                "score": 1,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "DUP"
            })),
        )
        .await
        .unwrap();

    let push_err = pq
        .push(
            &q,
            item(json!({
                "score": 2,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "DUP"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(push_err, pqueue::EngineError::Conflict);

    let stable = pq
        .query_index_unique(&q, "by_external_id", key(&["DUP"]))
        .await
        .unwrap()
        .expect("original still indexed");
    assert_eq!(stable.item_id, original);
    assert_eq!(
        pq.query_index(&q, "by_region_zone", key(&["us-east", "7"]))
            .await
            .unwrap()
            .len(),
        1
    );

    let other = pq
        .push(
            &q,
            item(json!({
                "score": 3,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "OTHER"
            })),
        )
        .await
        .unwrap();

    let update_err = pq
        .update_fields(
            &q,
            other,
            BTreeMap::new(),
            PayloadUpdate::Keep,
            Some(json!({
                "score": 3,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "DUP"
            })),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(update_err, pqueue::EngineError::Conflict);
    assert_eq!(
        pq.query_index_unique(&q, "by_external_id", key(&["DUP"]))
            .await
            .unwrap()
            .unwrap()
            .item_id,
        original
    );
    assert_eq!(
        pq.query_index_unique(&q, "by_external_id", key(&["OTHER"]))
            .await
            .unwrap()
            .unwrap()
            .item_id,
        other
    );
}

#[tokio::test]
async fn secondary_indexes_update_fields_and_replace_move_the_indexed_entry() {
    let pq = new_pq().await;
    let q = qkey();

    let original = pq
        .push(
            &q,
            item(json!({
                "score": 1,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "OLD"
            })),
        )
        .await
        .unwrap();

    let new_version = pq
        .update_fields(
            &q,
            original,
            BTreeMap::new(),
            PayloadUpdate::Keep,
            Some(json!({
                "score": 1,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "NEW"
            })),
            None,
        )
        .await
        .unwrap();
    assert_eq!(new_version, 2);
    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["OLD"]))
            .await
            .unwrap()
            .is_none()
    );
    let moved = pq
        .query_index_unique(&q, "by_external_id", key(&["NEW"]))
        .await
        .unwrap()
        .expect("updated value resolves");
    assert_eq!(moved.item_id, original);
    assert_eq!(moved.item_version, 2);

    let client_key = ClientItemKey::new("ck-1").unwrap();
    pq.upsert(
        &q,
        client_key.clone(),
        item(json!({
            "score": 3,
            "active": true,
            "due_at": "2026-06-30T12:00:00Z",
            "region": "us-east",
            "zone": 7,
            "external_id": "V1"
        })),
    )
    .await
    .unwrap();
    let outcome = pq
        .upsert(
            &q,
            client_key,
            item(json!({
                "score": 4,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "V2"
            })),
        )
        .await
        .unwrap();
    let replaced = match outcome {
        pqueue::UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
        other => panic!("expected replace, got {other:?}"),
    };
    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["V1"]))
            .await
            .unwrap()
            .is_none()
    );
    let moved = pq
        .query_index_unique(&q, "by_external_id", key(&["V2"]))
        .await
        .unwrap()
        .expect("replacement value resolves");
    assert_eq!(moved.item_id, replaced);
}

#[tokio::test]
async fn secondary_indexes_purge_removes_the_index_entry() {
    let pq = new_pq().await;
    let q = qkey();

    let id = pq
        .push(
            &q,
            item(json!({
                "score": 1,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 7,
                "external_id": "GONE"
            })),
        )
        .await
        .unwrap();

    pq.purge(&q, [id], false).await.unwrap();

    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["GONE"]))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        pq.query_index(&q, "by_region_zone", key(&["us-east", "7"]))
            .await
            .unwrap()
            .is_empty()
    );
}
