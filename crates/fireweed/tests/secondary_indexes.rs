//! Typed secondary-index behavior exercised through the public `pqueue` facade:
//! numeric ordering, boolean and datetime encoding, compound keys, sparse omission,
//! unique-conflict atomicity, key moves on update/replace, and purge removal.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use fireweed::{NewItem, PayloadUpdate, Pqueue};
use fireweed_core::{
    ClientItemKey, CompoundIndexDef, CompoundIndexField, EligibilityPolicy, IndexDeclaration,
    IndexDef, IndexSpec, IndexType, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, QueueIndex, RecurrencePolicy,
    RetryPolicy, TenantId,
};
use fireweed_memory::{ComposedMemoryBackend, ManualClock, composed_memory_backend};
use fireweed_sqlite::SqliteRelationalBackend;
use serde_json::{Value, json};

fn qkey() -> fireweed::QueueKey {
    fireweed::QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
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
        emit_change_records: true,
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
                "by_ratio",
                IndexDeclaration::Single(IndexDef {
                    field: "ratio".to_string(),
                    index_type: IndexType::Float,
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

async fn new_pq() -> Pqueue<ComposedMemoryBackend> {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    pq.create_queue(queue_definition()).await.unwrap();
    pq
}

async fn new_sqlite_relational_pq() -> Pqueue<SqliteRelationalBackend> {
    let backend = Arc::new(SqliteRelationalBackend::in_memory().unwrap());
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
    assert_eq!(push_err, fireweed::EngineError::Conflict);

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
    assert_eq!(update_err, fireweed::EngineError::Conflict);
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
        fireweed::UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
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

/// Bug #1: `update_fields` with `entity: None` must leave the entity document and typed index
/// memberships unchanged. Previously `None` incorrectly cleared the entity, ejecting the item
/// from every typed index it belonged to.
#[tokio::test]
async fn secondary_indexes_update_fields_none_entity_preserves_typed_index() {
    let pq = new_pq().await;
    let q = qkey();

    let id = pq
        .push(
            &q,
            item(json!({
                "score": 42,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-west",
                "zone": 5,
                "external_id": "keep-me"
            })),
        )
        .await
        .unwrap();

    // entity: None means "leave unchanged"; item must stay in all typed indexes
    let new_version = pq
        .update_fields(&q, id, BTreeMap::new(), PayloadUpdate::Keep, None, None)
        .await
        .unwrap();
    assert_eq!(new_version, 2, "version bumps even on no-op entity");

    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["keep-me"]))
        .await
        .unwrap()
        .expect("item must remain in typed index after update_fields with entity:None");
    assert_eq!(hit.item_id, id);
    assert_eq!(hit.item_version, 2);

    // A second entity-None update must also keep the index intact
    let v3 = pq
        .update_fields(&q, id, BTreeMap::new(), PayloadUpdate::Keep, None, None)
        .await
        .unwrap();
    assert_eq!(v3, 3);
    assert_eq!(
        pq.query_index_unique(&q, "by_external_id", key(&["keep-me"]))
            .await
            .unwrap()
            .unwrap()
            .item_id,
        id
    );
}

/// Bug #2: a String-typed field whose stored value looks like a JSON token (e.g. "123") must be
/// queryable by passing the raw UTF-8 bytes. Previously the lookup bytes were JSON-parsed first
/// (`b"123"` → `Number(123)`), causing a type mismatch against the stored `String("123")` key.
#[tokio::test]
async fn secondary_indexes_string_typed_field_with_numeric_looking_value_is_queryable() {
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
                "zone": 1,
                "external_id": "123"
            })),
        )
        .await
        .unwrap();

    // b"123" as lookup bytes for a String-typed field must match the stored string "123"
    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["123"]))
        .await
        .unwrap()
        .expect("String-typed field \"123\" must be found by byte slice b\"123\"");
    assert_eq!(hit.item_id, id);
}

/// Bug #3: legacy `IndexSpec` (byte-field) indexes must round-trip arbitrary bytes without
/// loss. Previously the encoding used `from_utf8_lossy`, so byte sequences that are invalid
/// UTF-8 were silently mangled; the new length-prefix encoding is byte-exact.
#[tokio::test]
async fn secondary_indexes_legacy_index_is_byte_exact_for_invalid_utf8() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);

    let def = QueueDefinition {
        secondary_indexes: vec![IndexSpec {
            name: "by_raw".to_string(),
            fields: vec!["raw_field".to_string()],
            unique: false,
        }],
        typed_indexes: vec![],
        emit_change_records: true,
        ..queue_definition()
    };
    pq.create_queue(def).await.unwrap();
    let q = qkey();

    // 0xff 0x00 0xfe is not valid UTF-8
    let raw: Bytes = Bytes::from_static(&[0xff, 0x00, 0xfe]);
    let id = pq
        .push(
            &q,
            NewItem {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("raw_field".to_string(), raw.clone());
                    m
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Lookup with the exact same bytes must find the item
    let hits = pq
        .query_index(&q, "by_raw", vec![raw.to_vec()])
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "byte-exact index must match on raw bytes");
    assert_eq!(hits[0].item_id, id);

    // The lossy replacement encoding (U+FFFD sequences) must NOT match the byte-exact entry
    let lossy_string = String::from_utf8_lossy(&raw).into_owned();
    let lossy_hits = pq
        .query_index(&q, "by_raw", vec![lossy_string.into_bytes()])
        .await
        .unwrap();
    assert!(
        lossy_hits.is_empty(),
        "lossy UTF-8 replacement bytes must not collide with the byte-exact index entry"
    );
}

/// Bug fix: upsert INSERT path was calling legacy `index_validate` (entity = None), so a fresh
/// upsert with a colliding typed-unique index key was silently accepted. Now it calls
/// `index_validate_push` which passes `entity_document` to the typed-index check.
#[tokio::test]
async fn secondary_indexes_upsert_insert_typed_unique_conflict_is_rejected() {
    let pq = new_pq().await;
    let q = qkey();

    // Push an item that occupies external_id "TAKEN" in the typed unique index.
    pq.push(
        &q,
        item(json!({
            "score": 1,
            "active": false,
            "due_at": "2026-06-30T12:00:00Z",
            "region": "us-east",
            "zone": 1,
            "external_id": "TAKEN"
        })),
    )
    .await
    .unwrap();

    // A fresh upsert (no prior entry for this client_item_key) that tries to claim the same
    // typed-unique key must be rejected with Conflict.
    let fresh_key = ClientItemKey::new("new-key-1").unwrap();
    let err = pq
        .upsert(
            &q,
            fresh_key,
            item(json!({
                "score": 2,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 2,
                "external_id": "TAKEN"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Conflict,
        "upsert insert must reject typed-unique collision"
    );

    // The original holder must still be indexed.
    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["TAKEN"]))
        .await
        .unwrap()
        .expect("original item must still hold the typed-unique key");
    assert_eq!(
        hit.item_version, 1,
        "original item is unchanged after rejected insert"
    );
}

/// Same typed-unique upsert-insert rejection exercised against the SQLite log-replay backend
/// (no environment variable required — uses an ephemeral in-memory SQLite store). Verifies that
/// `rebuild_all` and `create_queue` call `.with_typed_indexes` and that the upsert insert path
/// routes through typed-aware validation.
#[tokio::test]
async fn secondary_indexes_sqlite_log_replay_upsert_insert_and_update_typed_unique_conflict() {
    use fireweed_sqlite::composed_sqlite_backend_in_memory;

    let backend = Arc::new(composed_sqlite_backend_in_memory().expect("sqlite in-memory"));
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    pq.create_queue(queue_definition()).await.unwrap();
    let q = qkey();

    // Push an item occupying external_id "SLOT".
    let original = pq
        .push(
            &q,
            item(json!({
                "score": 1,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 1,
                "external_id": "SLOT"
            })),
        )
        .await
        .unwrap();

    // Fresh upsert-insert with a colliding typed-unique key must be Conflict.
    let fresh_key = ClientItemKey::new("new-sqlite-key").unwrap();
    let insert_err = pq
        .upsert(
            &q,
            fresh_key,
            item(json!({
                "score": 2,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 2,
                "external_id": "SLOT"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(
        insert_err,
        fireweed::EngineError::Conflict,
        "sqlite upsert insert must reject typed-unique collision"
    );

    // Push another item for the update_fields conflict check.
    let other = pq
        .push(
            &q,
            item(json!({
                "score": 3,
                "active": true,
                "due_at": "2026-06-30T12:00:00Z",
                "region": "us-east",
                "zone": 3,
                "external_id": "OTHER"
            })),
        )
        .await
        .unwrap();

    // update_fields that would move `other` into the typed-unique slot held by `original`.
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
                "zone": 3,
                "external_id": "SLOT"
            })),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        update_err,
        fireweed::EngineError::Conflict,
        "sqlite update_fields must reject typed-unique collision"
    );

    // Both items remain in their original indexed positions.
    assert_eq!(
        pq.query_index_unique(&q, "by_external_id", key(&["SLOT"]))
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

// ---------------------------------------------------------------------------
// Typed query API (ADR-011) — raw-byte bypass prevention + name resolution
// ---------------------------------------------------------------------------

/// Raw bytes that do not decode to the declared field type for a typed index must be rejected with
/// `EngineError::Invalid`, not silently accepted or panicked. This proves the typed index validation
/// cannot be bypassed by passing arbitrary bytes through the raw-byte `query_index*` overloads.
#[tokio::test]
async fn secondary_indexes_raw_bytes_invalid_for_typed_index_are_rejected() {
    let pq = new_pq().await;
    let q = qkey();

    // Push one item so the index is populated and non-trivially exercised.
    pq.push(
        &q,
        item(json!({
            "score": 42,
            "active": true,
            "due_at": "2026-06-30T12:00:00Z",
            "region": "us-east",
            "zone": 1,
            "external_id": "guard"
        })),
    )
    .await
    .unwrap();

    // b"not-a-number" is not valid JSON — the Integer-typed "by_score" index rejects it.
    let err = pq
        .query_index_unique(&q, "by_score", vec![b"not-a-number".to_vec()])
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Invalid("lookup key is not a valid JSON number"),
        "non-JSON bytes for Integer index must return EngineError::Invalid"
    );

    // b"maybe" is not valid JSON boolean — the Boolean-typed "by_active" index rejects it.
    let err = pq
        .query_index_unique(&q, "by_active", vec![b"maybe".to_vec()])
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Invalid("lookup key is not a valid JSON boolean"),
        "non-JSON-boolean bytes for Boolean index must return EngineError::Invalid"
    );

    // Valid bytes for the same fields succeed — the validation is type-specific, not blanket rejection.
    let hit = pq
        .query_index_unique(&q, "by_score", vec![serde_json::to_vec(&42i64).unwrap()])
        .await
        .unwrap();
    assert!(hit.is_some(), "valid JSON number bytes resolve correctly");

    let hit = pq
        .query_index_unique(&q, "by_active", vec![b"true".to_vec()])
        .await
        .unwrap();
    assert!(hit.is_some(), "valid JSON boolean bytes resolve correctly");
}

/// The `query_index_unique_typed` and `query_index_typed` methods accept `serde_json::Value`
/// directly and resolve the index by the configured `QueueIndex.name`. This proves that:
/// 1. The typed API encodes values correctly (the same result as the raw-byte path with valid bytes).
/// 2. The index name used in the API must match `QueueIndex.name` exactly.
/// 3. An unknown name returns `EngineError::Invalid`.
#[tokio::test]
async fn secondary_indexes_typed_value_query_and_name_based_resolution() {
    let pq = new_pq().await;
    let q = qkey();

    let id = pq
        .push(
            &q,
            item(json!({
                "score": 99,
                "active": false,
                "due_at": "2026-06-30T12:00:00Z",
                "ratio": 1.5,
                "region": "eu-west",
                "zone": 3,
                "external_id": "typed-query-test"
            })),
        )
        .await
        .unwrap();

    // query_index_unique_typed resolves by QueueIndex.name ("by_score") with a serde_json::Value.
    let hit = pq
        .query_index_unique_typed(&q, "by_score", &[serde_json::json!(99i64)])
        .await
        .unwrap()
        .expect("by_score typed query must find the item");
    assert_eq!(hit.item_id, id, "typed query resolves the correct item");

    // query_index_typed (non-unique) works the same way.
    let hits = pq
        .query_index_typed(
            &q,
            "by_due_at",
            &[serde_json::json!("2026-06-30T12:00:00Z")],
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].item_id, id);

    let hits = pq
        .query_index_typed(
            &q,
            "by_due_at",
            &[serde_json::json!(1_782_820_800_000_000_000i64)],
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].item_id, id,
        "numeric epoch-nanos datetime values accepted by axon-esf must query the same instant"
    );

    let hits = pq
        .query_index_typed(&q, "by_ratio", &[serde_json::json!(1.5)])
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].item_id, id);

    // Compound typed index: two-value key.
    let hits = pq
        .query_index_typed(
            &q,
            "by_region_zone",
            &[serde_json::json!("eu-west"), serde_json::json!(3i64)],
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].item_id, id);

    // An unknown index name must return EngineError::Invalid regardless of the query method used.
    let err = pq
        .query_index_unique_typed(&q, "no_such_index", &[serde_json::json!(99i64)])
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Invalid("unknown secondary index"),
        "unknown index name must return EngineError::Invalid"
    );
    let err = pq
        .query_index_typed(&q, "no_such_index", &[serde_json::json!("x")])
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Invalid("unknown secondary index"),
        "unknown index name on non-unique path must also return EngineError::Invalid"
    );

    let err = pq
        .query_index_unique_typed(&q, "by_score", &[serde_json::json!("99")])
        .await
        .unwrap_err();
    assert!(
        matches!(err, fireweed::EngineError::Invalid(_)),
        "string JSON must be rejected for Integer typed indexes"
    );
    let err = pq
        .query_index_typed(&q, "by_due_at", &[serde_json::json!(false)])
        .await
        .unwrap_err();
    assert!(
        matches!(err, fireweed::EngineError::Invalid(_)),
        "boolean JSON must be rejected for Datetime typed indexes"
    );
    let err = pq
        .query_index_typed(&q, "by_ratio", &[serde_json::json!("1.5")])
        .await
        .unwrap_err();
    assert!(
        matches!(err, fireweed::EngineError::Invalid(_)),
        "string JSON must be rejected for Float typed indexes"
    );

    let err = pq
        .query_index_unique_typed(
            &q,
            "by_due_at",
            &[serde_json::json!("2026-06-30T12:00:00Z")],
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Invalid("secondary index is not unique"),
        "typed unique lookup must reject non-unique indexes before backend lookup"
    );

    // The typed query result is byte-for-byte equivalent to the raw-byte path with correct bytes.
    let raw_hit = pq
        .query_index_unique(&q, "by_external_id", key(&["typed-query-test"]))
        .await
        .unwrap()
        .expect("raw-byte query finds the same item");
    let typed_hit = pq
        .query_index_unique_typed(
            &q,
            "by_external_id",
            &[serde_json::json!("typed-query-test")],
        )
        .await
        .unwrap()
        .expect("typed query finds the same item");
    assert_eq!(
        raw_hit.item_id, typed_hit.item_id,
        "raw-byte and typed-value queries produce identical results for the same key"
    );
}

#[tokio::test]
async fn secondary_indexes_typed_query_relational_error_precedence_is_explicit() {
    let pq = new_sqlite_relational_pq().await;
    let q = qkey();

    let err = pq
        .query_index_unique_typed(
            &q,
            "by_due_at",
            &[serde_json::json!("2026-06-30T12:00:00Z")],
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        fireweed::EngineError::Invalid("secondary index is not unique"),
        "facade uniqueness validation should run before relational backend availability"
    );

    let hits = pq
        .query_index_typed(&q, "by_score", &[serde_json::json!(99i64)])
        .await
        .unwrap();
    assert!(
        hits.is_empty(),
        "sqlite relational typed indexes are implemented and a valid missing key returns an empty result"
    );
}
