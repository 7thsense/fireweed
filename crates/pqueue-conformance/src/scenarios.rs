//! The conformance scenarios. Each is generic over a [`ConformanceBackend`](crate::ConformanceBackend)
//! and takes a `make: impl Fn() -> B` factory (some build a second backend for replay reconstruction).
//! Each fails if the port under test returns a default/no-op — the behavioral no-stub proof (plan §6).

use std::collections::BTreeMap;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, CohortPolicy, CompoundIndexDef, CompoundIndexField, EntitySchemaDocument,
    GateKeyPolicy, GroupKey, IndexDeclaration, IndexDef, IndexType, ItemId, ItemState, LeaseToken,
    Metadata, MetadataValue, PriorityValue, QueueDefinition, QueueIndex, RequestId,
};
use pqueue_engine::{
    ClaimCommand, ClaimCompatibility, ClaimRef, ClaimRequest, CommandPosition, EngineError,
    EngineResult, FenceLeaseCommand, FinalizeCommand, FinalizeKind, FinalizeOutcome, GroupBatching,
    InstanceFence, PauseQueueCommand, PayloadUpdate, ProjectionSnapshot, PushCommand, PushSpec,
    QueueCommand, ReplacePendingCommand, SideRecord, UnfenceLeaseCommand, UpsertOutcome,
};

// Re-exported so callers that address every scenario through the `scenarios::` path uniformly (e.g. the
// `pg_conformance!` macro in crates/pqueue-postgres/tests/conformance.rs) can reach the capability-gated
// wrapper the same way they reach every other scenario, instead of needing a special case for this one.
pub use crate::claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported;

// Method calls resolve through the `ConformanceBackend` bound's supertraits, so the individual port
// traits need not be imported here.
use crate::{
    Adr011ConformanceBackend, ConformanceBackend, ConformanceCommitTransition, ConformanceCore,
    claim_req, commit, envelope, item, item_max, qdef, qkey, shard, ts,
};

fn adr011_qdef_with_entity_schema() -> QueueDefinition {
    QueueDefinition {
        entity_schema: Some(
            serde_json::from_value::<EntitySchemaDocument>(serde_json::json!({
                "entity_schema": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {"type": "string"}
                    }
                }
            }))
            .unwrap(),
        ),
        ..qdef()
    }
}

fn adr011_qdef_with_indexes(indexes: Vec<QueueIndex>) -> QueueDefinition {
    QueueDefinition {
        typed_indexes: indexes,
        ..qdef()
    }
}

fn adr011_single_index(name: &str, field: &str, index_type: IndexType, unique: bool) -> QueueIndex {
    QueueIndex {
        name: name.to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: field.to_string(),
            index_type,
            unique,
        }),
    }
}

fn adr011_compound_index(
    name: &str,
    fields: impl IntoIterator<Item = (&'static str, IndexType)>,
    unique: bool,
) -> QueueIndex {
    QueueIndex {
        name: name.to_string(),
        declaration: IndexDeclaration::Compound(CompoundIndexDef {
            fields: fields
                .into_iter()
                .map(|(field, index_type)| CompoundIndexField {
                    field: field.to_string(),
                    index_type,
                })
                .collect(),
            unique,
        }),
    }
}

fn adr011_entity(field: &str, value: &str) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert(
        field.to_string(),
        serde_json::Value::String(value.to_string()),
    );
    serde_json::Value::Object(m)
}

fn adr011_full_entity(
    score: i64,
    ratio: f64,
    active: bool,
    due_at: &str,
    region: &str,
    zone: i64,
    external_id: Option<&str>,
) -> serde_json::Value {
    let mut entity = serde_json::json!({
        "score": score,
        "ratio": ratio,
        "active": active,
        "due_at": due_at,
        "region": region,
        "zone": zone,
    });
    if let Some(external_id) = external_id {
        entity["external_id"] = serde_json::Value::String(external_id.to_string());
    }
    entity
}

fn adr011_key(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|part| part.as_bytes().to_vec()).collect()
}

fn adr011_typed_index_qdef() -> QueueDefinition {
    adr011_qdef_with_indexes(vec![
        adr011_single_index("by_score", "score", IndexType::Integer, false),
        adr011_single_index("by_ratio", "ratio", IndexType::Float, false),
        adr011_single_index("by_active", "active", IndexType::Boolean, true),
        adr011_single_index("by_due_at", "due_at", IndexType::Datetime, false),
        adr011_single_index("by_external_id", "external_id", IndexType::String, true),
        adr011_compound_index(
            "by_region_zone",
            [("region", IndexType::String), ("zone", IndexType::Integer)],
            false,
        ),
    ])
}

fn adr011_typed_push(entity: serde_json::Value) -> PushSpec {
    PushSpec {
        entity: Some(entity),
        ..Default::default()
    }
}

fn commit_transition_capable(caps: &pqueue_engine::CommitCapabilities) -> bool {
    caps.atomic_transition_commit
        && caps.vectorized_commit
        && caps.lease_validation
        && caps.retained_commit_idempotency
        && caps.non_work_side_records
        && caps.authoritative_recovery_reads
        && matches!(
            caps.durability_class,
            pqueue_engine::DurabilityClass::Atomic
        )
}

async fn commit_transition_declines_consistently<B: ConformanceCommitTransition>(
    make: impl Fn() -> B,
) {
    let b = make();
    assert_eq!(
        b.commit_capabilities(),
        pqueue_engine::CommitCapabilities::default()
    );

    let err = b
        .commit_transition(
            &shard(),
            pqueue_engine::CommitTransition {
                request_id: None,
                entries: vec![],
            },
            ts(0),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let err = b
        .explain_commit(&shard(), RequestId::new("decline").unwrap())
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let err = b.side_record(&shard(), b"state/run").await.unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
}

async fn commit_transition_claim_ref<B: ConformanceCore>(backend: &B, now: i64) -> ClaimRef {
    backend
        .push(&shard(), vec![PushSpec::default()], ts(now), None)
        .await
        .unwrap();
    let claimed = backend.claim(claim_req(1, now + 60, now)).await.unwrap();
    let c = &claimed.items[0];
    ClaimRef {
        item_id: c.item_id,
        lease_token: c.lease_token.clone().expect("claimed item carries a token"),
        lease_expires_at: c.lease_expires_at,
        item_version: c.item_version,
    }
}

fn commit_transition_entry(
    claim_ref: ClaimRef,
    finalize: FinalizeKind,
    side_key: &str,
    side_value: &str,
    lifecycle_priority: i64,
    instance_fence: Option<InstanceFence>,
) -> pqueue_engine::CommitTransitionEntry {
    pqueue_engine::CommitTransitionEntry {
        claim_ref,
        finalize,
        side_records: vec![SideRecord {
            key: side_key.as_bytes().to_vec(),
            payload: bytes::Bytes::copy_from_slice(side_value.as_bytes()),
        }],
        lifecycle_items: vec![PushSpec {
            priority: Some(pqueue_core::PriorityValue::Int64(lifecycle_priority)),
            ..Default::default()
        }],
        instance_fence,
    }
}

/// Commit-transition positive path: a single entry writes side records, enqueues lifecycle work, finalizes
/// the input, and survives reopen through the recovery/read ports.
pub async fn commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen<
    B: ConformanceCommitTransition,
>(
    make: impl Fn() -> B,
) {
    let b = make();
    if !commit_transition_capable(&b.commit_capabilities()) {
        commit_transition_declines_consistently(make).await;
        return;
    }

    b.create_queue(qdef()).await.unwrap();
    let claim_ref = commit_transition_claim_ref(&b, 0).await;
    let input_id = claim_ref.item_id;
    let lifecycle_key = "state/run-1";
    let rid = RequestId::new("txn-commit-transition-1").unwrap();
    let outcomes = b
        .commit_transition(
            &shard(),
            pqueue_engine::CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![commit_transition_entry(
                    claim_ref.clone(),
                    FinalizeKind::Complete,
                    lifecycle_key,
                    "audit-bytes",
                    20,
                    Some(InstanceFence {
                        instance_key: b"wf-1".to_vec(),
                        expected: 0,
                        next: 1,
                    }),
                )],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    let lifecycle_id = match &outcomes[0] {
        pqueue_engine::CommitEntryOutcome::Committed { lifecycle_item_ids } => {
            assert_eq!(lifecycle_item_ids.len(), 1);
            lifecycle_item_ids[0]
        }
        other => panic!("expected Committed, got {other:?}"),
    };
    assert_ne!(lifecycle_id, input_id);

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.complete),
        (1, 0, 1),
        "input completes and only the lifecycle item remains pending"
    );
    let peeked = b.peek(&qkey(), 10).await.unwrap();
    assert_eq!(peeked.len(), 1);
    assert_eq!(peeked[0].item_id, lifecycle_id);
    assert_eq!(
        b.side_record(&qkey(), lifecycle_key.as_bytes())
            .await
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );
    let claimed = b.claim(claim_req(10, 600, 2)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, lifecycle_id);

    drop(b);
    let b = make();
    assert_eq!(
        b.side_record(&qkey(), lifecycle_key.as_bytes())
            .await
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );
    let recovery = b
        .explain_commit(&qkey(), rid.clone())
        .await
        .unwrap()
        .expect("committed transition survives reopen");
    assert_eq!(recovery.request_id, rid);
    assert_eq!(recovery.entries.len(), 1);
    assert_eq!(recovery.entries[0].consumed_input_id, input_id);
    assert_eq!(recovery.entries[0].instance, Some((b"wf-1".to_vec(), 1)));
    assert_eq!(
        recovery.entries[0].side_record_keys,
        vec![lifecycle_key.as_bytes().to_vec()]
    );
    assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().complete,
        1,
        "finalized input survives reopen"
    );
}

/// Rejection path: a bad lease token rejects atomically without writing side records or enqueuing
/// lifecycle work.
pub async fn commit_transition_rejects_bad_token_without_writing<B: ConformanceCommitTransition>(
    make: impl Fn() -> B,
) {
    let b = make();
    if !commit_transition_capable(&b.commit_capabilities()) {
        commit_transition_declines_consistently(make).await;
        return;
    }

    b.create_queue(qdef()).await.unwrap();
    let mut claim_ref = commit_transition_claim_ref(&b, 0).await;
    claim_ref.lease_token = LeaseToken::new("not-the-real-token").unwrap();
    let outcomes = b
        .commit_transition(
            &shard(),
            pqueue_engine::CommitTransition {
                request_id: None,
                entries: vec![commit_transition_entry(
                    claim_ref,
                    FinalizeKind::Complete,
                    "state/x",
                    "v",
                    20,
                    None,
                )],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        outcomes,
        vec![pqueue_engine::CommitEntryOutcome::Rejected(
            EngineError::StaleLease
        )]
    );
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!((m.pending, m.leased, m.complete), (0, 1, 0));
    assert_eq!(
        b.side_record(&qkey(), b"state/x").await.unwrap(),
        None,
        "bad token writes no side record"
    );
}

/// Rejection path: a bad item version rejects atomically without writing side records or enqueuing
/// lifecycle work.
pub async fn commit_transition_rejects_bad_version_without_writing<
    B: ConformanceCommitTransition,
>(
    make: impl Fn() -> B,
) {
    let b = make();
    if !commit_transition_capable(&b.commit_capabilities()) {
        commit_transition_declines_consistently(make).await;
        return;
    }

    b.create_queue(qdef()).await.unwrap();
    let mut claim_ref = commit_transition_claim_ref(&b, 0).await;
    claim_ref.item_version += 99;
    let outcomes = b
        .commit_transition(
            &shard(),
            pqueue_engine::CommitTransition {
                request_id: None,
                entries: vec![commit_transition_entry(
                    claim_ref,
                    FinalizeKind::Complete,
                    "state/y",
                    "v",
                    20,
                    None,
                )],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        outcomes,
        vec![pqueue_engine::CommitEntryOutcome::Rejected(
            EngineError::Conflict
        )]
    );
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!((m.pending, m.leased, m.complete), (0, 1, 0));
    assert_eq!(
        b.side_record(&qkey(), b"state/y").await.unwrap(),
        None,
        "bad version writes no side record"
    );
}

/// Request-id replay path: identical bodies return prior outcomes without duplicating side records or
/// lifecycle items; a body change under the same id conflicts.
pub async fn commit_transition_request_id_replays_without_double_write<
    B: ConformanceCommitTransition,
>(
    make: impl Fn() -> B,
) {
    let b = make();
    if !commit_transition_capable(&b.commit_capabilities()) {
        commit_transition_declines_consistently(make).await;
        return;
    }

    b.create_queue(qdef()).await.unwrap();
    let claim_ref = commit_transition_claim_ref(&b, 0).await;
    let rid = RequestId::new("txn-replay-1").unwrap();
    let body = |claim_ref: ClaimRef| pqueue_engine::CommitTransition {
        request_id: Some(rid.clone()),
        entries: vec![commit_transition_entry(
            claim_ref,
            FinalizeKind::Complete,
            "state/run-1",
            "v1",
            20,
            None,
        )],
    };

    let first = b
        .commit_transition(&shard(), body(claim_ref.clone()), ts(1), None)
        .await
        .unwrap();
    let lifecycle_id = match &first[0] {
        pqueue_engine::CommitEntryOutcome::Committed { lifecycle_item_ids } => {
            lifecycle_item_ids[0]
        }
        other => panic!("expected Committed, got {other:?}"),
    };
    let replay = b
        .commit_transition(&shard(), body(claim_ref.clone()), ts(1), None)
        .await
        .unwrap();
    assert_eq!(first, replay);
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!((m.pending, m.leased, m.complete), (1, 0, 1));
    assert_eq!(
        b.side_record(&qkey(), b"state/run-1")
            .await
            .unwrap()
            .as_deref(),
        Some(b"v1".as_slice())
    );
    assert_eq!(b.peek(&qkey(), 10).await.unwrap()[0].item_id, lifecycle_id);

    let conflict = b
        .commit_transition(
            &shard(),
            pqueue_engine::CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![pqueue_engine::CommitTransitionEntry {
                    claim_ref,
                    finalize: FinalizeKind::Fail,
                    side_records: vec![SideRecord {
                        key: b"state/run-1".to_vec(),
                        payload: Bytes::copy_from_slice(b"v1"),
                    }],
                    lifecycle_items: vec![PushSpec::default()],
                    instance_fence: None,
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(conflict, EngineError::RequestIdConflict);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    assert_eq!(
        b.side_record(&qkey(), b"state/run-1")
            .await
            .unwrap()
            .as_deref(),
        Some(b"v1".as_slice())
    );
}

/// Recovery path: explain_commit reconstructs the committed transition, and the retained recovery state
/// survives reopen.
pub async fn commit_transition_explain_commit_recovers_transition_and_survives_reopen<
    B: ConformanceCommitTransition,
>(
    make: impl Fn() -> B,
) {
    let b = make();
    if !commit_transition_capable(&b.commit_capabilities()) {
        commit_transition_declines_consistently(make).await;
        return;
    }

    b.create_queue(qdef()).await.unwrap();
    let claim_ref = commit_transition_claim_ref(&b, 0).await;
    let input_id = claim_ref.item_id;
    let rid = RequestId::new("recover-1").unwrap();
    let instance_key = b"instance/run-1".to_vec();
    let outcomes = b
        .commit_transition(
            &shard(),
            pqueue_engine::CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![pqueue_engine::CommitTransitionEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![SideRecord {
                        key: b"audit/run-1".to_vec(),
                        payload: Bytes::copy_from_slice(b"audit-bytes"),
                    }],
                    lifecycle_items: vec![PushSpec::default()],
                    instance_fence: Some(InstanceFence {
                        instance_key: instance_key.clone(),
                        expected: 0,
                        next: 5,
                    }),
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    let lifecycle_id = match &outcomes[0] {
        pqueue_engine::CommitEntryOutcome::Committed { lifecycle_item_ids } => {
            lifecycle_item_ids[0]
        }
        other => panic!("expected Committed, got {other:?}"),
    };

    drop(b);
    let b = make();
    let recovery = b
        .explain_commit(&shard(), rid.clone())
        .await
        .unwrap()
        .expect("record survives reopen");
    assert_eq!(recovery.request_id, rid);
    assert_eq!(recovery.entries.len(), 1);
    let entry = &recovery.entries[0];
    assert_eq!(entry.consumed_input_id, input_id);
    assert_eq!(entry.instance, Some((instance_key.clone(), 5)));
    assert_eq!(entry.side_record_keys, vec![b"audit/run-1".to_vec()]);
    assert_eq!(entry.lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(entry.status, pqueue_engine::CommitEntryStatus::Committed);
    assert_eq!(
        b.side_record(&shard(), b"audit/run-1")
            .await
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
    let claimed = b.claim(claim_req(10, 600, 2)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, lifecycle_id);
}

/// ADR-011 entity schemas reject invalid documents before visible state or request-id replay records.
pub async fn adr011_schema_validation_rejects_before_visible_state<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_qdef_with_entity_schema())
        .await
        .unwrap();
    let invalid = || PushSpec {
        entity: Some(serde_json::json!({"count": 1})),
        ..Default::default()
    };
    let valid = || PushSpec {
        entity: Some(serde_json::json!({"name": "ok"})),
        ..Default::default()
    };

    let err = b
        .push(&shard(), vec![invalid()], ts(0), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 0);

    let rid = RequestId::new("adr011-schema-req").unwrap();
    let err = b
        .push_with_request_id(&shard(), rid.clone(), vec![invalid()], ts(1), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 0);

    let first = b
        .push_with_request_id(&shard(), rid.clone(), vec![valid()], ts(2), None)
        .await
        .unwrap();
    let replay = b
        .push_with_request_id(&shard(), rid, vec![valid()], ts(3), None)
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
}

/// ADR-011 typed secondary indexes preserve scalar semantics, datetime canonicalization, and compound
/// name-based lookup through `IndexQueryPort`.
pub async fn adr011_typed_scalar_and_compound_indexes_work<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let score_2 = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                2,
                1.5,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("A"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap()[0];
    let score_10 = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                10,
                2.5,
                true,
                "2026-06-30T08:00:00-04:00",
                "us-east",
                7,
                Some("B"),
            ))],
            ts(1),
            None,
        )
        .await
        .unwrap()[0];

    assert_eq!(
        b.index_lookup(&shard(), "by_score", &adr011_key(&["2"]))
            .await
            .unwrap()[0]
            .item_id,
        score_2
    );
    assert_eq!(
        b.index_lookup(&shard(), "by_score", &adr011_key(&["10"]))
            .await
            .unwrap()[0]
            .item_id,
        score_10
    );
    assert_eq!(
        b.index_lookup(&shard(), "by_ratio", &adr011_key(&["1.5"]))
            .await
            .unwrap()[0]
            .item_id,
        score_2
    );
    assert_eq!(
        b.index_get_unique(&shard(), "by_active", &adr011_key(&["false"]))
            .await
            .unwrap()
            .expect("false is indexed")
            .item_id,
        score_2
    );
    assert_eq!(
        b.index_get_unique(&shard(), "by_active", &adr011_key(&["true"]))
            .await
            .unwrap()
            .expect("true is indexed")
            .item_id,
        score_10
    );
    let due_ids: Vec<_> = b
        .index_lookup(
            &shard(),
            "by_due_at",
            &adr011_key(&["2026-06-30T12:00:00Z"]),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.item_id)
        .collect();
    assert_eq!(
        due_ids,
        vec![score_2, score_10],
        "equivalent datetimes canonicalize to the same key"
    );
    let compound_ids: Vec<_> = b
        .index_lookup(&shard(), "by_region_zone", &adr011_key(&["us-east", "7"]))
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.item_id)
        .collect();
    assert_eq!(compound_ids, vec![score_2, score_10]);
}

pub async fn adr011_typed_missing_fields_remain_sparse<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let with_external = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                1,
                1.0,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("A"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap()[0];
    let sparse = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                3,
                3.0,
                true,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                None,
            ))],
            ts(1),
            None,
        )
        .await
        .unwrap()[0];

    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["A"]))
            .await
            .unwrap()
            .expect("external_id is indexed")
            .item_id,
        with_external
    );
    assert!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["missing"]))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        b.index_lookup(&shard(), "by_score", &adr011_key(&["3"]))
            .await
            .unwrap()[0]
            .item_id,
        sparse
    );
}

pub async fn adr011_typed_unique_conflicts_are_atomic<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let original = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                1,
                1.0,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("DUP"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap()[0];

    let push_err = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                2,
                2.0,
                true,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("DUP"),
            ))],
            ts(1),
            None,
        )
        .await;
    assert!(matches!(push_err, Err(EngineError::Conflict)));
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["DUP"]))
            .await
            .unwrap()
            .expect("original remains indexed")
            .item_id,
        original
    );

    let batch_err = b
        .push(
            &shard(),
            vec![
                adr011_typed_push(adr011_full_entity(
                    3,
                    3.0,
                    false,
                    "2026-06-30T12:00:00Z",
                    "us-east",
                    7,
                    Some("BATCH"),
                )),
                adr011_typed_push(adr011_full_entity(
                    4,
                    4.0,
                    true,
                    "2026-06-30T12:00:00Z",
                    "us-east",
                    7,
                    Some("BATCH"),
                )),
            ],
            ts(2),
            None,
        )
        .await;
    assert!(matches!(batch_err, Err(EngineError::Conflict)));
    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["BATCH"]))
            .await
            .unwrap(),
        None
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
}

pub async fn adr011_typed_update_fields_and_replace_rekey<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let id = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                1,
                1.0,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("OLD"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap()[0];
    b.update_fields(
        &shard(),
        id,
        BTreeMap::new(),
        PayloadUpdate::Keep,
        Some(adr011_full_entity(
            1,
            1.0,
            false,
            "2026-06-30T12:00:00Z",
            "us-west",
            8,
            Some("NEW"),
        )),
        None,
        ts(1),
        None,
    )
    .await
    .unwrap();
    assert!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["OLD"]))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["NEW"]))
            .await
            .unwrap()
            .expect("new key indexed")
            .item_id,
        id
    );
    assert!(
        b.index_lookup(&shard(), "by_region_zone", &adr011_key(&["us-east", "7"]))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        b.index_lookup(&shard(), "by_region_zone", &adr011_key(&["us-west", "8"]))
            .await
            .unwrap()[0]
            .item_id,
        id
    );

    let key = ClientItemKey::new("replace-key").unwrap();
    b.replace_if_pending(
        &shard(),
        &key,
        None,
        None,
        None,
        None,
        BTreeMap::new(),
        Metadata::default(),
        Some(adr011_entity("external_id", "SAME")),
        ts(2),
        None,
    )
    .await
    .unwrap();
    let result = b
        .replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            Some(adr011_entity("external_id", "SAME")),
            ts(3),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "replacement can reclaim its superseded unique key: {:?}",
        result
    );
}

pub async fn adr011_typed_update_fields_unique_conflict_is_atomic<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![
                adr011_typed_push(adr011_full_entity(
                    1,
                    1.0,
                    false,
                    "2026-06-30T12:00:00Z",
                    "us-east",
                    7,
                    Some("A"),
                )),
                adr011_typed_push(adr011_full_entity(
                    2,
                    2.0,
                    true,
                    "2026-06-30T12:00:00Z",
                    "us-east",
                    8,
                    Some("B"),
                )),
            ],
            ts(0),
            None,
        )
        .await
        .unwrap();

    let result = b
        .update_fields(
            &shard(),
            ids[1],
            BTreeMap::new(),
            PayloadUpdate::Keep,
            Some(adr011_full_entity(
                2,
                2.0,
                true,
                "2026-06-30T12:00:00Z",
                "us-east",
                8,
                Some("A"),
            )),
            None,
            ts(1),
            None,
        )
        .await;
    assert!(matches!(result, Err(EngineError::Conflict)));
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 2);
    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["A"]))
            .await
            .unwrap()
            .expect("occupied unique key remains indexed")
            .item_id,
        ids[0]
    );
    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["B"]))
            .await
            .unwrap()
            .expect("failed update keeps original key")
            .item_id,
        ids[1]
    );
}

pub async fn adr011_typed_purge_frees_unique_key<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let id = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                1,
                1.0,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("PURGE"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap()[0];
    b.purge(&shard(), vec![id], false, ts(1), None)
        .await
        .unwrap();
    assert!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["PURGE"]))
            .await
            .unwrap()
            .is_none()
    );
    let new_id = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                2,
                2.0,
                true,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("PURGE"),
            ))],
            ts(2),
            None,
        )
        .await
        .unwrap()[0];
    assert_ne!(id, new_id);
}

pub async fn adr011_typed_upsert_insert_unique_conflict_is_atomic<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let existing = b
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                1,
                1.0,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("UPSERT-DUP"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap()[0];
    let err = b
        .replace_if_pending(
            &shard(),
            &ClientItemKey::new("fresh-upsert").unwrap(),
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            Some(adr011_full_entity(
                2,
                2.0,
                true,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("UPSERT-DUP"),
            )),
            ts(1),
            None,
        )
        .await;
    assert!(matches!(err, Err(EngineError::Conflict)));
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["UPSERT-DUP"]))
            .await
            .unwrap()
            .expect("existing key remains indexed")
            .item_id,
        existing
    );
}

pub async fn adr011_typed_log_replay_reconstructs_index_rows<
    B: Adr011ConformanceBackend + ConformanceBackend,
>(
    make: impl Fn() -> B,
) {
    let a = make();
    a.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let ids = a
        .push(
            &shard(),
            vec![adr011_typed_push(adr011_full_entity(
                1,
                1.0,
                false,
                "2026-06-30T12:00:00Z",
                "us-east",
                7,
                Some("REPLAY"),
            ))],
            ts(0),
            None,
        )
        .await
        .unwrap();

    let page = a.read_from(&shard(), None, 1000).await.unwrap();
    let b = make();
    b.create_queue(adr011_typed_index_qdef()).await.unwrap();
    let b_epoch = b.current_epoch(&shard()).await.unwrap();
    for (_pos, env) in &page.entries {
        let env = env.clone();
        b.write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env), b_epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .unwrap();
    }

    assert_eq!(
        b.index_get_unique(&shard(), "by_external_id", &adr011_key(&["REPLAY"]))
            .await
            .unwrap()
            .expect("typed index row reconstructed from log")
            .item_id,
        ids[0]
    );
    assert_eq!(
        b.index_lookup(&shard(), "by_region_zone", &adr011_key(&["us-east", "7"]))
            .await
            .unwrap()[0]
            .item_id,
        ids[0]
    );
}

pub async fn adr011_typed_schema_less_queue_unaffected<B: Adr011ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    assert!(
        b.index_get_unique(&shard(), "nonexistent", &adr011_key(&["x"]))
            .await
            .is_err()
    );
}

/// Eventual-apply backends MUST refuse upsert (Invariant 2 / TD-007 §2.3: the atomic XDEL+XADD
/// `replace_if_pending` is offered only on the atomic durability class). The refusal is the structured
/// `Unavailable` (RESP `-ERR pqueue unavailable`). Used by the eventual-apply conformance variant in
/// place of the three atomic-class upsert scenarios.
pub async fn upsert_is_unavailable<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let err = b
        .replace_if_pending(
            &shard(),
            &ClientItemKey::new("dup").unwrap(),
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::Unavailable,
        "eventual-apply backends must refuse upsert with Unavailable (Invariant 2)"
    );
}

/// FAC-1 (atomic class): `update_fields` merges a LIVE item's hot-storage fields/payload in place
/// (set + remove), bumps `item_version`, honors the `expected_item_version` CAS (`Conflict` on mismatch),
/// and rejects unknown/terminal ids — the write half of the `live_items` read.
pub async fn update_fields_merges_and_cas<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "1" leased
    let id = ItemId::new("1").unwrap();
    let key = ClientItemKey::new("ka").unwrap();

    // Set two fields + payload on the leased item; version bumps off the genesis 1.
    let v = b
        .update_fields(
            &shard(),
            id,
            BTreeMap::from([
                ("state".to_string(), Some(Bytes::from_static(b"sent"))),
                ("n".to_string(), Some(Bytes::from_static(b"1"))),
            ]),
            PayloadUpdate::Set(Some(Bytes::from_static(b"body"))),
            None,
            None,
            ts(20),
            None,
        )
        .await
        .unwrap();
    let live = b
        .live_items(&shard(), std::slice::from_ref(&key))
        .await
        .unwrap()
        .into_iter()
        .next()
        .flatten()
        .expect("live");
    assert_eq!(
        live.fields.get("state").map(|x| x.as_ref()),
        Some(&b"sent"[..])
    );
    assert_eq!(live.payload.as_deref(), Some(&b"body"[..]));
    assert_eq!(live.item_version, v);

    // Merge: remove a key, add another, KEEP payload; CAS on the current version.
    let v2 = b
        .update_fields(
            &shard(),
            id,
            BTreeMap::from([
                ("n".to_string(), None),
                ("attempts".to_string(), Some(Bytes::from_static(b"2"))),
            ]),
            PayloadUpdate::Keep,
            None,
            Some(v),
            ts(21),
            None,
        )
        .await
        .unwrap();
    assert!(v2 > v, "version advances on each update");
    let live = b
        .live_items(&shard(), &[key])
        .await
        .unwrap()
        .into_iter()
        .next()
        .flatten()
        .expect("live");
    assert!(!live.fields.contains_key("n"), "removed key is gone");
    assert_eq!(
        live.fields.get("attempts").map(|x| x.as_ref()),
        Some(&b"2"[..])
    );
    assert_eq!(
        live.fields.get("state").map(|x| x.as_ref()),
        Some(&b"sent"[..]),
        "untouched key survives the merge"
    );
    assert_eq!(
        live.payload.as_deref(),
        Some(&b"body"[..]),
        "Keep left the payload"
    );

    // Stale CAS -> Conflict, nothing changes.
    assert_eq!(
        b.update_fields(
            &shard(),
            id,
            BTreeMap::from([("state".to_string(), Some(Bytes::from_static(b"x")))]),
            PayloadUpdate::Keep,
            None,
            Some(v),
            ts(22),
            None,
        )
        .await,
        Err(EngineError::Conflict)
    );
    // Unknown id -> NotFound.
    assert_eq!(
        b.update_fields(
            &shard(),
            ItemId::new("90").unwrap(),
            BTreeMap::new(),
            PayloadUpdate::Keep,
            None,
            None,
            ts(23),
            None,
        )
        .await,
        Err(EngineError::NotFound)
    );

    // After completion the item is Terminal and rejects further updates.
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
        ts(30),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        b.update_fields(
            &shard(),
            id,
            BTreeMap::new(),
            PayloadUpdate::Keep,
            None,
            None,
            ts(31),
            None
        )
        .await,
        Err(EngineError::Terminal)
    );
}

/// FAC-1 (eventual-apply class): the read-your-write field mutation is refused with `Unavailable`
/// (parity with `upsert_is_unavailable`).
pub async fn update_fields_is_unavailable<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    assert_eq!(
        b.update_fields(
            &shard(),
            ItemId::new("1").unwrap(),
            BTreeMap::new(),
            PayloadUpdate::Keep,
            None,
            None,
            ts(20),
            None
        )
        .await,
        Err(EngineError::Unavailable)
    );
}

/// FAC-2 (every class): `reclaim_expired` is the per-queue, host-driven lease sweep — expired leases
/// return to Pending (claimable again), the reclaimed ids are returned, and it is idempotent + half-open.
pub async fn reclaim_expired_sweeps_per_queue<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // leased, expires ts(500)
    let id = ItemId::new("1").unwrap();

    // Half-open: at exactly the expiry the lease is still valid — nothing reclaimed.
    assert!(
        b.reclaim_expired(&shard(), None, ts(500), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Past expiry: the id is returned and the item is Pending again.
    assert_eq!(
        b.reclaim_expired(&shard(), None, ts(600), None)
            .await
            .unwrap(),
        vec![id]
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    // Idempotent: a second sweep finds nothing.
    assert!(
        b.reclaim_expired(&shard(), None, ts(700), None)
            .await
            .unwrap()
            .is_empty()
    );
    // Claimable again.
    assert_eq!(
        b.claim(claim_req(1, 1000, 800)).await.unwrap().items.len(),
        1
    );
}

/// `ProjectionRead::peek` — non-destructive, priority-ordered eligible view (fails if it returns a
/// default/empty no-op).
pub async fn peek_is_priority_ordered_and_nondestructive<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;
    let views = b.peek(&shard(), 10).await.unwrap();
    let peeked: Vec<String> = views.iter().map(|v| v.item_id.to_string()).collect();
    assert_eq!(
        peeked,
        vec!["2", "3", "1"],
        "peek is priority-ordered (10,20,30)"
    );
    // Non-destructive: peeking again returns the same items (peek must not consume/claim).
    assert_eq!(
        b.peek(&shard(), 10).await.unwrap().len(),
        3,
        "peek does not consume"
    );
    assert_eq!(
        b.peek(&shard(), 1).await.unwrap().len(),
        1,
        "peek honors the limit"
    );
}

/// `ProjectionRead::pending` — lists in-flight (leased) items (fails on a default/empty no-op).
pub async fn pending_lists_leased_items<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "nothing leased yet"
    );
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let pending = b.pending(&shard()).await.unwrap();
    assert_eq!(pending.len(), 1, "the leased item appears in pending");
    assert_eq!(pending[0].item_id.to_string(), "1");
    assert_eq!(pending[0].lease_token.as_str(), "lease-1");
}

/// `SnapshotStore::write_snapshot`/`read_snapshot`/`latest_snapshot` — durable round-trip (fails on a
/// no-op store: latest must reflect the most-recent write and read must return the exact payload).
pub async fn snapshots_write_read_latest<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let sk = shard();
    assert!(
        b.latest_snapshot(&sk).await.unwrap().is_none(),
        "no snapshot yet"
    );
    let pos = CommandPosition::new(sk.clone(), 0, 0);
    let r1 = b
        .write_snapshot(
            &sk,
            pos.clone(),
            ProjectionSnapshot {
                payload: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
    let r2 = b
        .write_snapshot(
            &sk,
            pos,
            ProjectionSnapshot {
                payload: vec![4, 5, 6],
            },
        )
        .await
        .unwrap();
    assert_ne!(r1.ref_id, r2.ref_id, "each snapshot gets a distinct ref");
    assert_eq!(
        b.latest_snapshot(&sk)
            .await
            .unwrap()
            .expect("a snapshot exists")
            .ref_id,
        r2.ref_id,
        "latest is the most-recent write"
    );
    assert_eq!(b.read_snapshot(&r1).await.unwrap().payload, vec![1, 2, 3]);
    assert_eq!(b.read_snapshot(&r2).await.unwrap().payload, vec![4, 5, 6]);
}

pub async fn push_then_select_eligible_in_priority_order<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();

    // Push out of priority order: 30, 10, 20.
    let push = QueueCommand::Push(PushCommand {
        items: vec![
            item("1", "ka", 30),
            item("2", "kb", 10),
            item("3", "kc", 20),
        ],
    });
    commit(&b, envelope(push, vec![])).await;

    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    let ids: Vec<String> = eligible.iter().map(|i| i.to_string()).collect();
    // Ascending Int64 priority => 10(b), 20(c), 30(a). Fails if select_eligible is a no-op.
    assert_eq!(
        ids,
        vec!["2", "3", "1"],
        "must be priority-ordered, not insertion order"
    );
}

pub async fn claim_then_complete_lifecycle<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Claim it.
    let claim = QueueCommand::Claim(ClaimCommand {
        item_ids: vec![ItemId::new("1").unwrap()],
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(200),
    });
    commit(&b, envelope(claim, vec![ItemId::new("1").unwrap()])).await;

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.leased, 1, "claim must move item to leased");
    assert_eq!(m.pending, 0);
    // Claimed item is no longer eligible.
    assert!(
        b.select_eligible(&shard(), ts(300), 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Complete it.
    let fin = QueueCommand::Finalize(FinalizeCommand {
        outcomes: vec![FinalizeOutcome::new(
            ItemId::new("1").unwrap(),
            FinalizeKind::Complete,
        )],
    });
    commit(&b, envelope(fin, vec![ItemId::new("1").unwrap()])).await;

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        m.complete, 1,
        "finalize-complete must move item to complete"
    );
    assert_eq!(m.leased, 0);
}

pub async fn replace_pending_supersedes_old<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("5", "dup", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Upsert: same client_item_key replaces the pending item with a new id.
    let replace = QueueCommand::ReplacePending(ReplacePendingCommand {
        client_item_key: ClientItemKey::new("dup").unwrap(),
        superseded_item_id: ItemId::new("5").unwrap(),
        replacement: item("6", "dup", 5),
    });
    commit(&b, envelope(replace, vec![])).await;

    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    let ids: Vec<String> = eligible.iter().map(|i| i.to_string()).collect();
    assert_eq!(
        ids,
        vec!["6"],
        "superseded old id must not be eligible; new id is"
    );

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.pending, 1, "superseded item excluded from counts");
}

pub async fn high_water_is_monotonic<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let p1 = CommandPosition::new(shard(), 0, 1);
    let p2 = CommandPosition::new(shard(), 0, 2);

    b.set_high_water(&shard(), p2.clone()).await.unwrap();
    assert_eq!(b.high_water(&shard()).await.unwrap(), Some(p2.clone()));
    // Regression must be rejected (TD-007 §4).
    assert_eq!(
        b.set_high_water(&shard(), p1).await,
        Err(EngineError::Invalid("high-water regression"))
    );
    assert_eq!(b.high_water(&shard()).await.unwrap(), Some(p2));
}

pub async fn claim_returns_priority_ordered_rich_items<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(2, 500, 100)).await.unwrap();
    let ids: Vec<String> = claimed
        .items
        .iter()
        .map(|i| i.item_id.to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["2", "3"],
        "claim must deliver highest priority first"
    );
    // Rich shape populated (would fail if claim returned a stub).
    let first = &claimed.items[0];
    assert_eq!(
        first.lease_token.as_ref().map(|token| token.as_str()),
        Some("lease-1")
    );
    assert_eq!(first.item_version, 2, "claim bumps item_version");
    assert_eq!(first.attempt_count, 1, "first delivery");
    assert_eq!(first.lease_expires_at, ts(500));

    // The unclaimed lowest-priority item remains eligible.
    let remaining = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        remaining.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
        vec!["1"]
    );
}

pub async fn claim_empty_when_nothing_eligible<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert!(claimed.items.is_empty());
}

pub async fn claimed_item_shape_includes_payload_fields_and_gate_keys<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    let mut def = qdef();
    def.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    def.eligibility_policy.max_gate_keys_per_item = Some(8);
    b.create_queue(def).await.unwrap();
    let mut item = item("1", "ka", 5);
    item.not_before = Some(ts(50));
    item.group_key = Some(GroupKey::new("group-a").unwrap());
    item.payload = Some(Bytes::from_static(b"opaque-payload"));
    item.fields = BTreeMap::from([("field-a".to_string(), Bytes::from_static(b"value-a"))]);
    item.metadata
        .insert("tenant_segment", MetadataValue::String("vip".to_string()));
    item.gate_keys = vec!["gate-a".to_string(), "gate-b".to_string()];
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand { items: vec![item] }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let got = &claimed.items[0];
    assert_eq!(got.item_id, ItemId::new("1").unwrap());
    assert_eq!(got.client_item_key, ClientItemKey::new("ka").unwrap());
    assert_eq!(got.item_version, 2, "claim bumps item_version");
    assert_eq!(got.priority, Some(PriorityValue::Int64(5)));
    assert_eq!(got.not_before, Some(ts(50)));
    assert_eq!(got.group_key, Some(GroupKey::new("group-a").unwrap()));
    assert_eq!(got.lease_token, Some(LeaseToken::new("lease-1").unwrap()));
    assert_eq!(got.lease_expires_at, ts(500));
    assert_eq!(got.attempt_count, 1);
    assert_eq!(got.payload.as_deref(), Some(&b"opaque-payload"[..]));
    assert_eq!(
        got.fields.get("field-a").map(|bytes| bytes.as_ref()),
        Some(&b"value-a"[..])
    );
    assert_eq!(
        got.metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string()))
    );
    assert_eq!(got.gate_keys, vec!["gate-a", "gate-b"]);

    let view = b
        .claimed_view(&shard(), &[ItemId::new("1").unwrap()])
        .await
        .unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(
        view[0].metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string())),
        "claimed_view must render the same claimed-item metadata shape"
    );
    assert_eq!(
        view[0].gate_keys,
        vec!["gate-a", "gate-b"],
        "claimed_view must render the same claimed-item gate-key shape"
    );
}

pub async fn claimed_item_shape_reflects_update_fields_after_reclaim<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    let mut def = qdef();
    def.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    def.eligibility_policy.max_gate_keys_per_item = Some(8);
    b.create_queue(def).await.unwrap();

    let mut item = item("1", "ka", 5);
    item.not_before = Some(ts(50));
    item.group_key = Some(GroupKey::new("group-a").unwrap());
    item.payload = Some(Bytes::from_static(b"opaque-payload"));
    item.fields = BTreeMap::from([
        ("field-a".to_string(), Bytes::from_static(b"value-a")),
        ("field-b".to_string(), Bytes::from_static(b"value-b")),
    ]);
    item.metadata
        .insert("tenant_segment", MetadataValue::String("vip".to_string()));
    item.gate_keys = vec!["gate-a".to_string()];
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand { items: vec![item] }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let first = &claimed.items[0];
    let item_id = first.item_id;
    let client_item_key = first.client_item_key.clone();
    let claim_version = first.item_version;
    assert_eq!(first.lease_token, Some(LeaseToken::new("lease-1").unwrap()));
    assert_eq!(first.payload.as_deref(), Some(&b"opaque-payload"[..]));
    assert_eq!(first.not_before, Some(ts(50)));
    assert_eq!(first.group_key, Some(GroupKey::new("group-a").unwrap()));
    assert_eq!(
        first.metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string()))
    );
    assert_eq!(first.gate_keys, vec!["gate-a"]);
    assert_eq!(
        first.fields.get("field-a").map(|bytes| bytes.as_ref()),
        Some(&b"value-a"[..])
    );
    assert_eq!(
        first.fields.get("field-b").map(|bytes| bytes.as_ref()),
        Some(&b"value-b"[..])
    );

    let updated_version = b
        .update_fields(
            &shard(),
            item_id,
            BTreeMap::from([
                (
                    "field-a".to_string(),
                    Some(Bytes::from_static(b"value-a-2")),
                ),
                ("field-b".to_string(), None),
                ("field-c".to_string(), Some(Bytes::from_static(b"value-c"))),
            ]),
            PayloadUpdate::Set(Some(Bytes::from_static(b"opaque-payload-v2"))),
            None,
            Some(claim_version),
            ts(120),
            None,
        )
        .await
        .unwrap();
    assert!(updated_version > claim_version);

    let live_claim = b.claimed_view(&shard(), &[item_id]).await.unwrap();
    assert_eq!(live_claim.len(), 1);
    let live_claim = &live_claim[0];
    assert_eq!(live_claim.item_id, item_id);
    assert_eq!(live_claim.client_item_key, client_item_key);
    assert_eq!(live_claim.item_version, updated_version);
    assert_eq!(
        live_claim.lease_token,
        Some(LeaseToken::new("lease-1").unwrap())
    );
    assert_eq!(
        live_claim.payload.as_deref(),
        Some(&b"opaque-payload-v2"[..])
    );
    assert_eq!(live_claim.not_before, Some(ts(50)));
    assert_eq!(
        live_claim.group_key,
        Some(GroupKey::new("group-a").unwrap())
    );
    assert_eq!(
        live_claim.metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string()))
    );
    assert_eq!(live_claim.gate_keys, vec!["gate-a"]);
    assert_eq!(
        live_claim.fields.get("field-a").map(|bytes| bytes.as_ref()),
        Some(&b"value-a-2"[..])
    );
    assert_eq!(
        live_claim.fields.get("field-c").map(|bytes| bytes.as_ref()),
        Some(&b"value-c"[..])
    );
    assert!(
        !live_claim.fields.contains_key("field-b"),
        "removed field must stay absent from the claimed shape"
    );

    assert_eq!(
        b.reclaim_expired(&shard(), None, ts(600), None)
            .await
            .unwrap(),
        vec![item_id]
    );

    let mut reclaim_req = claim_req(1, 1100, 700);
    reclaim_req.lease_token = LeaseToken::new("lease-2").unwrap();
    let reclaimed = b.claim(reclaim_req).await.unwrap();
    assert_eq!(reclaimed.items.len(), 1);
    let got = &reclaimed.items[0];
    assert_eq!(got.item_id, item_id);
    assert_eq!(got.client_item_key, client_item_key);
    assert!(got.item_version > updated_version);
    assert_eq!(got.lease_token, Some(LeaseToken::new("lease-2").unwrap()));
    assert_eq!(got.attempt_count, 2);
    assert_eq!(got.payload.as_deref(), Some(&b"opaque-payload-v2"[..]));
    assert_eq!(got.not_before, Some(ts(50)));
    assert_eq!(got.group_key, Some(GroupKey::new("group-a").unwrap()));
    assert_eq!(
        got.metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string()))
    );
    assert_eq!(got.gate_keys, vec!["gate-a"]);
    assert_eq!(
        got.fields.get("field-a").map(|bytes| bytes.as_ref()),
        Some(&b"value-a-2"[..])
    );
    assert_eq!(
        got.fields.get("field-c").map(|bytes| bytes.as_ref()),
        Some(&b"value-c"[..])
    );
    assert!(
        !got.fields.contains_key("field-b"),
        "re-claimed shape must keep the current field map"
    );
}

pub async fn claimed_item_shape_omits_empty_conditionals<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let got = &claimed.items[0];
    assert_eq!(got.item_id, ItemId::new("1").unwrap());
    assert_eq!(got.client_item_key, ClientItemKey::new("ka").unwrap());
    assert_eq!(got.item_version, 2);
    assert_eq!(got.lease_token, Some(LeaseToken::new("lease-1").unwrap()));
    assert_eq!(got.lease_expires_at, ts(500));
    assert_eq!(got.priority, Some(PriorityValue::Int64(5)));
    assert_eq!(got.not_before, None, "absent not_before stays absent");
    assert_eq!(got.group_key, None, "absent group_key stays absent");
    assert_eq!(got.payload, None, "absent payload stays absent");
    assert!(
        got.metadata.is_empty(),
        "absent metadata stays empty/omitted"
    );
    assert!(
        got.gate_keys.is_empty(),
        "gate_keys are absent for gate_keys=none queues"
    );
}

pub async fn claimed_item_shape_whole_cohort_omits_per_item_lease_token<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    let mut def = qdef();
    def.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(30_000),
        on_incomplete: None,
        max_cohort_size: Some(10),
    });
    b.create_queue(def).await.unwrap();

    let members: Vec<_> = [
        ("1", "ka", "field-a", b"value-a".as_slice()),
        ("2", "kb", "field-b", b"value-b".as_slice()),
        ("3", "kc", "field-c", b"value-c".as_slice()),
    ]
    .into_iter()
    .map(|(id, key, field, value)| {
        let mut member = item(id, key, 5);
        member.group_key = Some(GroupKey::new("cohort-a").unwrap());
        member.cohort_size = Some(3);
        member.fields = BTreeMap::from([(field.to_string(), Bytes::from_static(value))]);
        member
    })
    .collect();
    commit(
        &b,
        envelope(QueueCommand::Push(PushCommand { items: members }), vec![]),
    )
    .await;

    let claimed = b
        .claim(ClaimRequest {
            compatibility: ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
            ..claim_req(10, 500, 100)
        })
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 3);
    assert_eq!(
        claimed.cohort_lease_token,
        Some(LeaseToken::new("lease-1").unwrap()),
        "whole_cohort carries the shared lease token at the response top level"
    );
    assert!(
        claimed.cohort_id.is_some(),
        "whole_cohort identifies the claimed cohort at the response top level"
    );
    assert!(
        claimed.items.iter().all(|item| item.lease_token.is_none()),
        "whole_cohort item rows omit per-item lease_token"
    );
    let expected_fields = BTreeMap::from([
        (
            ItemId::new("1").unwrap(),
            BTreeMap::from([("field-a".to_string(), Bytes::from_static(b"value-a"))]),
        ),
        (
            ItemId::new("2").unwrap(),
            BTreeMap::from([("field-b".to_string(), Bytes::from_static(b"value-b"))]),
        ),
        (
            ItemId::new("3").unwrap(),
            BTreeMap::from([("field-c".to_string(), Bytes::from_static(b"value-c"))]),
        ),
    ]);
    for item in &claimed.items {
        assert_eq!(
            item.fields,
            expected_fields.get(&item.item_id).cloned().unwrap(),
            "whole_cohort item rows retain the current fields map"
        );
    }
}

pub async fn structured_live_items_are_ordered_and_only_live<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let mut fields = BTreeMap::new();
    fields.insert("recipient_ref".to_string(), Bytes::from_static(b"r-1"));
    fields.insert("payload_ref".to_string(), Bytes::from_static(b"work-1"));
    let mut pushed = item("7", "hot-key", 5);
    pushed.payload = Some(Bytes::from_static(b"opaque"));
    pushed.fields = fields.clone();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![pushed],
            }),
            vec![],
        ),
    )
    .await;

    let keys = vec![
        ClientItemKey::new("missing").unwrap(),
        ClientItemKey::new("hot-key").unwrap(),
    ];
    let live = b.live_items(&shard(), &keys).await.unwrap();
    assert!(live[0].is_none(), "missing keys render as absent");
    let Some(item) = &live[1] else {
        panic!("hot-key should render while pending");
    };
    assert_eq!(item.lifecycle_state, ItemState::Pending);
    assert_eq!(item.payload.as_deref(), Some(&b"opaque"[..]));
    assert_eq!(item.fields, fields);

    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].fields, fields);
    let live = b
        .live_items(&shard(), &[ClientItemKey::new("hot-key").unwrap()])
        .await
        .unwrap();
    assert_eq!(
        live[0].as_ref().map(|i| i.lifecycle_state),
        Some(ItemState::Leased),
        "leased items are still live hot-storage records"
    );

    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(
            claimed.items[0].item_id,
            FinalizeKind::Complete,
        )],
        ts(20),
        None,
    )
    .await
    .unwrap();
    let live = b
        .live_items(&shard(), &[ClientItemKey::new("hot-key").unwrap()])
        .await
        .unwrap();
    assert!(live[0].is_none(), "terminal items are no longer live");
}

pub async fn upsert_inserts_then_replaces_pending<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();

    // First upsert → Inserted with a BACKEND-ASSIGNED id (capture it).
    let id1 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(1),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    // Second upsert (same key) → Replaced; the new id is backend-assigned and supersedes id1.
    let id2 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(2),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Replaced {
            new_item_id,
            superseded_item_id,
        } => {
            assert_eq!(superseded_item_id, id1, "the first id is superseded");
            assert_ne!(
                new_item_id, id1,
                "the replacement got a fresh backend-assigned id"
            );
            new_item_id
        }
        other => panic!("expected Replaced, got {other:?}"),
    };
    // Only the replacement is eligible; the superseded id is gone.
    let elig = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(elig, vec![id2], "only the replacement is eligible");
}

pub async fn upsert_rejects_claimed_and_terminal<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();
    let id1 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(1),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    // Claim it → leased. Upsert must be rejected with Invalid (no transition on in-flight work).
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let err = b
        .replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(20),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Invalid("collision with claimed item"));

    // Finalize-complete the leased item → terminal. Upsert must then be rejected with Terminal.
    commit(
        &b,
        envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(id1, FinalizeKind::Complete)],
            }),
            vec![id1],
        ),
    )
    .await;
    let err = b
        .replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(30),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Terminal);
}

pub async fn upsert_preserves_group_delay_and_payload_in_claim_shape<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("grouped").unwrap();
    let group = GroupKey::new("group-a").unwrap();

    let assigned = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            Some(group.clone()),
            Some(ts(250)),
            Some(Bytes::from_static(b"payload")),
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(1),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    assert!(
        b.claim(claim_req(10, 500, 100))
            .await
            .unwrap()
            .items
            .is_empty(),
        "not_before must keep the upserted item out of early claims"
    );

    let claimed = b.claim(claim_req(10, 500, 300)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let item = &claimed.items[0];
    assert_eq!(
        item.item_id, assigned,
        "the claimed item is the backend-assigned upsert id"
    );
    assert_eq!(item.group_key.as_ref(), Some(&group));
    assert_eq!(item.not_before, Some(ts(250)));
    assert_eq!(item.payload.as_deref(), Some(&b"payload"[..]));
}

pub async fn tick_reclaims_expired_lease_with_no_client_traffic<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim with a lease expiring at t=100.
    b.claim(claim_req(10, 100, 10)).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Before expiry: tick is a no-op.
    let r = b.tick(ts(50)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // After expiry: tick reclaims — WITH ZERO intervening client commands (DoD, TD-007 §3).
    let r = b.tick(ts(200)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 1);
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.pending, 1);
    assert_eq!(m.leased, 0);

    // Idempotent: re-ticking at the same time reclaims nothing (item already pending).
    let r = b.tick(ts(200)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 0);

    // The reclaimed item is back to pending/eligible. (The reclaim itself does NOT charge an attempt —
    // attempt_count = number of deliveries; a fresh claim of this item would charge the next one.)
    let pending = b.select_eligible(&shard(), ts(300), 10).await.unwrap();
    assert_eq!(
        pending.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
        vec!["1"]
    );
}

pub async fn tick_lease_boundary_is_half_open<B: ConformanceCore>(make: impl Fn() -> B) {
    // Convention: a lease is valid THROUGH `lease_expires_at`; reclaim fires only at now > exp (B1).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(10, 100, 10)).await.unwrap(); // lease_expires_at = ts(100)

    // At exactly the expiry instant: lease still held, nothing reclaimed.
    assert_eq!(b.tick(ts(100)).await.unwrap().leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // One unit past expiry: reclaimed.
    assert_eq!(b.tick(ts(101)).await.unwrap().leases_reclaimed, 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 0);
}

pub async fn paused_queue_yields_no_claims<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    // Pause: nothing eligible/claimable.
    commit(
        &b,
        envelope(
            QueueCommand::PauseQueue(PauseQueueCommand::default()),
            vec![],
        ),
    )
    .await;
    assert!(
        b.claim(claim_req(10, 500, 10))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        b.select_eligible(&shard(), ts(10), 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Resume: claimable again.
    commit(&b, envelope(QueueCommand::ResumeQueue, vec![])).await;
    assert_eq!(
        b.claim(claim_req(10, 500, 10)).await.unwrap().items.len(),
        1
    );
}

pub async fn fenced_lease_finalize_is_stale<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let id = ItemId::new("1").unwrap();

    // Operator fences the lease.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    // The holder's finalize is rejected StaleLease, and nothing is committed (still leased).
    let outcomes = vec![FinalizeOutcome::new(id, FinalizeKind::Complete)];
    assert_eq!(
        b.finalize(&shard(), outcomes.clone(), ts(20), None).await,
        Err(EngineError::StaleLease)
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Operator unfences: finalize now succeeds.
    commit(
        &b,
        envelope(
            QueueCommand::UnfenceLease(UnfenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    b.finalize(&shard(), outcomes, ts(30), None).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
}

pub async fn renew_extends_lease_and_rejects<B: ConformanceCore>(make: impl Fn() -> B) {
    // renew_validate MIRRORS finalize_validate: only a live, non-fenced, non-terminal, non-superseded
    // leased item may be renewed; a renew extends the lease WITHOUT charging an attempt (TD-006:129).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "1" leased, expires at ts(500)
    let id = ItemId::new("1").unwrap();

    // Unknown id -> NotFound, and NOTHING appended (reject before commit, B1).
    assert_eq!(
        b.renew(
            &shard(),
            vec![ItemId::new("90").unwrap()],
            ts(2000),
            ts(20),
            None
        )
        .await,
        Err(EngineError::NotFound)
    );

    // Happy path: extend the lease to ts(2000). Ticking PAST the old expiry (500) reclaims nothing,
    // and the attempt_count is unchanged (renew does not charge a delivery).
    b.renew(&shard(), vec![id], ts(2000), ts(20), None)
        .await
        .unwrap();
    assert_eq!(
        b.tick(ts(600)).await.unwrap().leases_reclaimed,
        0,
        "extended lease must not be reclaimed past the OLD expiry"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);
    let lease = b
        .pending(&shard())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.item_id == id)
        .expect("item still in-flight");
    assert_eq!(lease.attempt_count, 1, "renew does not charge a delivery");
    assert_eq!(
        lease.lease_expires_at,
        ts(2000),
        "renew extended the lease deadline"
    );

    // A never-leased (Pending) item -> Invalid, same as finalize_validate's catch-all.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    assert_eq!(
        b.renew(
            &shard(),
            vec![ItemId::new("4").unwrap()],
            ts(2000),
            ts(21),
            None
        )
        .await,
        Err(EngineError::Invalid("item is not leased"))
    );

    // Fenced lease -> StaleLease, exactly as finalize_validate rejects it.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    assert_eq!(
        b.renew(&shard(), vec![id], ts(3000), ts(30), None).await,
        Err(EngineError::StaleLease)
    );
}

pub async fn reassign_swaps_token_and_charges_attempt<B: ConformanceCore>(make: impl Fn() -> B) {
    // Cross-consumer XCLAIM: ReassignLease swaps the lease token to a new consumer AND charges exactly one
    // delivery (TD-006:129). Rejection semantics mirror renew/finalize (validate_leased), appending
    // nothing on reject.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "1" leased by "lease-1", attempt_count = 1
    let id = ItemId::new("1").unwrap();
    let new_token = LeaseToken::new("lease-2").unwrap();

    // Unknown id -> NotFound, and NOTHING appended.
    assert_eq!(
        b.reassign(
            &shard(),
            vec![ItemId::new("90").unwrap()],
            new_token.clone(),
            ts(2000),
            ts(20),
            None
        )
        .await,
        Err(EngineError::NotFound)
    );

    // Happy path: transfer the lease to "lease-2", extend to ts(2000), charge exactly one delivery.
    b.reassign(
        &shard(),
        vec![id],
        new_token.clone(),
        ts(2000),
        ts(20),
        None,
    )
    .await
    .unwrap();
    let lease = b
        .pending(&shard())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.item_id == id)
        .expect("still in-flight under the new consumer");
    assert_eq!(
        lease.lease_token, new_token,
        "lease transferred to the new consumer"
    );
    assert_eq!(
        lease.attempt_count, 2,
        "reassign charges one delivery (claim=1 + reassign=1)"
    );
    assert_eq!(
        lease.lease_expires_at,
        ts(2000),
        "reassign extended the deadline"
    );
    // The new lease is live: ticking past the OLD expiry (500) reclaims nothing.
    assert_eq!(b.tick(ts(600)).await.unwrap().leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Fenced lease -> StaleLease, exactly as renew/finalize reject it.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    assert_eq!(
        b.reassign(&shard(), vec![id], new_token, ts(3000), ts(30), None)
            .await,
        Err(EngineError::StaleLease)
    );
}

pub async fn claimed_view_renders_leased_items<B: ConformanceCore>(make: impl Fn() -> B) {
    // `claimed_view` renders the rich claim shape for currently-leased ids; pending + unknown ids are
    // omitted (the RESP `XCLAIM` reply source).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim only the top-priority item "1" (5 < 9, ascending); "4" stays pending.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let a = ItemId::new("1").unwrap();
    let p = ItemId::new("4").unwrap();

    let view = b
        .claimed_view(&shard(), &[a, p, ItemId::new("90").unwrap()])
        .await
        .unwrap();
    assert_eq!(
        view.len(),
        1,
        "only the leased item renders; the pending + unknown ids are omitted"
    );
    assert_eq!(view[0].item_id, a);
    assert_eq!(
        view[0].lease_token,
        Some(LeaseToken::new("lease-1").unwrap())
    );
    assert_eq!(view[0].attempt_count, 1);
}

pub async fn purge_removes_present_items_and_gates_leased<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    // PurgePort (RESP XDEL / operator purge): removes present items, returns the count actually removed,
    // no-ops on absent ids, and gates a LEASED purge behind `force` (API-001) — appending nothing on the
    // gate rejection.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("2", "kb", 9)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim "1" (top priority) → leased; "2" stays pending.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let a = ItemId::new("1").unwrap();
    let b_id = ItemId::new("2").unwrap();

    // Purging a LEASED item without force is gated (Conflict), appending nothing.
    assert_eq!(
        b.purge(&shard(), vec![a], false, ts(20), None).await,
        Err(EngineError::Conflict)
    );
    // Mixed batch [pending, leased] without force: the gate rejects ALL-OR-NOTHING regardless of order —
    // the pending id is NOT purged even though it precedes the leased one in the batch.
    assert_eq!(
        b.purge(&shard(), vec![b_id, a], false, ts(20), None).await,
        Err(EngineError::Conflict)
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "the pending id in a gate-rejected mixed batch is NOT purged"
    );

    // Purge a PENDING item, REPEATED, plus an ABSENT id: the repeat counts once (de-dup), the absent id
    // is a no-op → count 1.
    let removed = b
        .purge(
            &shard(),
            vec![b_id, b_id, ItemId::new("90").unwrap()],
            false,
            ts(21),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        removed, 1,
        "a repeated present id removes/counts once; absent is a no-op"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 0, "b is gone");

    // Force-purge the leased item "1": removed, count 1, no longer leased.
    let removed_a = b
        .purge(&shard(), vec![a], true, ts(22), None)
        .await
        .unwrap();
    assert_eq!(removed_a, 1);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        0,
        "a force-purged"
    );
}

pub async fn retry_beyond_max_attempts_goes_terminal<B: ConformanceCore>(make: impl Fn() -> B) {
    // Retry-exhaustion (B'): `attempt_count` = deliveries. A `Finalize{Retry}` UNDER `max_attempts` returns
    // the item to pending (claimable again); the retry once it has used all `max_attempts` deliveries drives
    // it TERMINAL (Failed). With max_attempts = 2: delivery 1 → retry → pending; delivery 2 → retry → failed.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item_max("1", "ka", 5, 2)],
            }),
            vec![],
        ),
    )
    .await;
    let id = ItemId::new("1").unwrap();
    let retry_outcome = || {
        vec![FinalizeOutcome::new(
            ItemId::new("1").unwrap(),
            FinalizeKind::Retry,
        )]
    };

    // Delivery 1: claim → attempt_count = 1.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(
        b.pending(&shard()).await.unwrap()[0].attempt_count,
        1,
        "first delivery"
    );
    // Retry UNDER the bound (1 < 2) → back to pending, still claimable.
    b.finalize(&shard(), retry_outcome(), ts(20), None)
        .await
        .unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.failed),
        (1, 0, 0),
        "retry under max → pending"
    );
    assert!(
        !b.select_eligible(&shard(), ts(30), 10)
            .await
            .unwrap()
            .is_empty(),
        "the retried item is claimable again"
    );

    // Delivery 2: claim again → attempt_count = 2 (now AT the bound).
    b.claim(claim_req(1, 500, 30)).await.unwrap();
    assert_eq!(
        b.pending(&shard()).await.unwrap()[0].attempt_count,
        2,
        "second delivery"
    );
    // Retry AT the bound (2 >= 2) → TERMINAL (Failed), NOT back to pending.
    b.finalize(&shard(), retry_outcome(), ts(40), None)
        .await
        .unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.failed),
        (0, 0, 1),
        "retry at/beyond max_attempts → terminal Failed"
    );
    assert!(
        b.select_eligible(&shard(), ts(50), 10)
            .await
            .unwrap()
            .is_empty(),
        "the exhausted item is terminal — not claimable"
    );
    // It is now terminal: a further finalize is rejected (Terminal), not a silent re-queue.
    assert_eq!(
        b.finalize(
            &shard(),
            vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
            ts(60),
            None
        )
        .await,
        Err(EngineError::Terminal)
    );

    // Boundary: max_attempts = 1 means ONE delivery, no retries — the first retry exhausts immediately
    // (pins `>=`, not `>`). Push a second item "2" with max_attempts = 1.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item_max("2", "kb", 9, 1)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 70)).await.unwrap(); // delivery 1 (attempt_count = 1 == max)
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(
            ItemId::new("2").unwrap(),
            FinalizeKind::Retry,
        )],
        ts(80),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().failed,
        2,
        "max_attempts=1: the very first retry exhausts → Failed (b joins the earlier a)"
    );
}

pub async fn retry_with_backoff_defers_eligibility<B: ConformanceCore>(make: impl Fn() -> B) {
    // Queue-native retry backoff: a `Finalize{Retry}` carrying `not_before` returns the item to Pending
    // (still under the attempt bound) but DEFERS its re-eligibility until that timestamp. The item shows
    // up Pending in metrics, yet `select_eligible` skips it until `now >= not_before` (half-open `<= now`).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Delivery 1: claim (lease to ts(500)), then Retry under the bound with a backoff to ts(100).
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: ItemId::new("1").unwrap(),
            kind: FinalizeKind::Retry,
            not_before: Some(ts(100)),
        }],
        ts(20),
        None,
    )
    .await
    .unwrap();

    // Back to Pending (not terminal).
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased),
        (1, 0),
        "retry under max → pending, not terminal"
    );

    // Still deferred: 50 < 100, so nothing is eligible yet.
    assert!(
        b.select_eligible(&shard(), ts(50), 10)
            .await
            .unwrap()
            .is_empty(),
        "backed off before not_before — not eligible"
    );
    // Eligible AT the boundary (half-open `<= now` convention).
    assert!(
        !b.select_eligible(&shard(), ts(100), 10)
            .await
            .unwrap()
            .is_empty(),
        "eligible at the not_before boundary"
    );
    // And actually claimable once the backoff elapses.
    assert_eq!(
        b.claim(claim_req(1, 600, 100)).await.unwrap().items.len(),
        1,
        "claimable at the not_before boundary"
    );
}

pub async fn finalize_of_nonleased_item_is_rejected_without_appending<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    let id = ItemId::new("1").unwrap();
    // Item is Pending (never claimed) -> finalize rejected, and NOTHING is appended (no divergence, B1).
    let outcomes = vec![FinalizeOutcome::new(id, FinalizeKind::Complete)];
    assert_eq!(
        b.finalize(&shard(), outcomes, ts(10), None).await,
        Err(EngineError::Invalid("item is not leased"))
    );
}

pub async fn pause_and_fence_reconstruct_from_log<B: ConformanceBackend>(make: impl Fn() -> B) {
    // Backend A: push two items, claim+fence one, leave one pending, pause the queue.
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    a.claim(claim_req(1, 500, 10)).await.unwrap(); // claims "1" (priority 5 < 9)
    let aid = ItemId::new("1").unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![aid],
            }),
            vec![aid],
        ),
    )
    .await;
    commit(
        &a,
        envelope(
            QueueCommand::PauseQueue(PauseQueueCommand::default()),
            vec![],
        ),
    )
    .await;

    // Replay A's full log into a fresh backend B (TD-007 §4 replay reconstruction).
    let page = a.read_from(&shard(), None, 1000).await.unwrap();
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let b_epoch = b.current_epoch(&shard()).await.unwrap();
    for (_pos, env) in &page.entries {
        let env = env.clone();
        b.write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env), b_epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .unwrap();
    }

    // B reconstructed the durable state: pause withholds the pending item, and the fence holds.
    assert!(
        b.claim(claim_req(10, 500, 50))
            .await
            .unwrap()
            .items
            .is_empty(),
        "pause reconstructed"
    );
    let outcomes = vec![FinalizeOutcome::new(aid, FinalizeKind::Complete)];
    assert_eq!(
        b.finalize(&shard(), outcomes, ts(60), None).await,
        Err(EngineError::StaleLease),
        "fence reconstructed"
    );
}

pub async fn high_water_advances_on_each_commit<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let sk = shard();
    assert!(b.high_water(&sk).await.unwrap().is_none(), "no commits yet");
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    let h1 = b.high_water(&sk).await.unwrap().expect("after push");
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let h2 = b.high_water(&sk).await.unwrap().expect("after claim");
    assert!(
        h1.precedes(&h2),
        "command_position high-water must advance on each commit (push -> claim)"
    );
}

/// Relational-reconnect class (ADR-008 §2 / TD-001 conformance capability classes): committed state
/// **survives a process restart via reopen-the-store**, with no test-driven manual log replay. Commit
/// items, drop the backend handle (simulated crash), build a fresh backend from the **same durable
/// store** (the `make` factory MUST reopen it, and the queue definition MUST persist — the second handle
/// does NOT re-`create_queue`), and assert the committed state is present.
///
/// This is a black-box durability assertion: it does not (and as a `ConformanceCore`-bounded scenario
/// CANNOT) assert *how* the state is restored. A log-bearing backend may satisfy it via log replay on
/// open; the **transactional-authoritative relational backend (`postgres_native`, BQ-12) is the intended
/// exemplar** — TD-001: "only a transactional-authoritative relational projection runs the
/// reconnect-after-crash class." The sqlite smoke (BQ-10) only proves this scenario + the
/// `relational_reconnect_suite!` macro compile and run.
///
/// Only durable backends whose `make` reopens shared state belong to this class; an in-memory backend
/// sees a fresh empty store on the second `make()` and is not in this class (it does not run this suite).
pub async fn reconnect_after_crash_preserves_committed_state<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;

    // Simulated crash: drop the handle, then reopen the SAME durable store.
    drop(a);
    let b = make();

    // No manual log replay: the DB-resident projection is authoritative, so the committed items are
    // present (in ascending Int64 priority order: 10(b), 20(c), 30(a)). Fails if reopen lost state.
    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        eligible,
        vec![
            ItemId::new("2").unwrap(),
            ItemId::new("3").unwrap(),
            ItemId::new("1").unwrap(),
        ],
        "committed items present in priority order after reconnect (no log replay)"
    );
}

/// Reconnect preserves NON-pending lifecycle state too: a completed item stays terminal and the
/// untouched items stay pending across a reopen. (Relational: read from the DB-resident projection;
/// log-bearing: reconstructed by replay — the scenario asserts only the recovered *state*, so both
/// recovery models satisfy it.)
pub async fn reconnect_preserves_terminal_and_pending_state<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;
    // Claim the priority-10 item ("2") and complete it -> terminal; "1"/"3" stay pending.
    let claimed = a.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(claimed.items[0].item_id.to_string(), "2");
    a.finalize(
        &shard(),
        vec![FinalizeOutcome::new(
            ItemId::new("2").unwrap(),
            FinalizeKind::Complete,
        )],
        ts(20),
        None,
    )
    .await
    .unwrap();

    drop(a);
    let b = make();

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.complete),
        (2, 0, 1),
        "terminal + pending counts survive reconnect"
    );
    assert_eq!(
        b.select_eligible(&shard(), ts(100), 10).await.unwrap(),
        vec![ItemId::new("3").unwrap(), ItemId::new("1").unwrap()],
        "the two untouched items remain pending in priority order; the completed one does not reappear"
    );
}

/// Reconnect preserves a LEASED item as leased (token-contract-safe: asserts the recovered lifecycle
/// *state* via metrics, NOT the lease token — the relational family deliberately loses the cleartext token
/// on reopen, while a log-bearing family reconstructs it; both keep the item `Leased`). The tokenless
/// in-flight lease is still reclaimable by the owner: a tick past the deadline returns it to pending.
pub async fn reconnect_preserves_leased_item_state<B: ConformanceCore>(make: impl Fn() -> B) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    a.claim(claim_req(1, 500, 10)).await.unwrap(); // leased through ts(500)

    drop(a);
    let b = make();

    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        1,
        "the in-flight lease survives reconnect as Leased"
    );
    // The lease deadline survived too, so the reclaim tick can return the tokenless lease to pending.
    b.tick(ts(501)).await.unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.leased, m.pending),
        (0, 1),
        "the reclaim tick recovers the tokenless in-flight lease"
    );
}

/// Reconnect preserves transaction abort semantics: a rejected mutation must not become a durable command
/// that later replays into visible state. This is the black-box restart form of the log-class
/// `rejected_mutations_do_not_append_commands` check, and applies to every durable external profile.
pub async fn reconnect_after_rejected_mutation_has_no_phantom_commit<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    let id = ItemId::new("1").unwrap();
    assert_eq!(
        a.finalize(
            &shard(),
            vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
            ts(10),
            None,
        )
        .await,
        Err(EngineError::Invalid("item is not leased")),
        "finalize of a pending item is rejected"
    );

    drop(a);
    let b = make();

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.complete),
        (1, 0, 0),
        "the rejected finalize did not become visible after reconnect"
    );
    let claimed = b.claim(claim_req(1, 500, 20)).await.unwrap();
    assert_eq!(
        claimed.items.iter().map(|i| i.item_id).collect::<Vec<_>>(),
        vec![id],
        "the original pending item remains claimable after reconnect"
    );
}

/// **Log-class** durability guarantee (B1, no-divergence): a REJECTED mutation must not append any
/// command — the durable log length is unchanged. The behavioral rejection itself (the structured
/// `NotFound`/`Conflict`/`Invalid` error) is asserted in the CORE class (the renew/reassign/purge/
/// finalize scenarios, which every family runs); this scenario adds the log-tail guarantee that the
/// reject happens BEFORE any append. Bounded by [`ConformanceBackend`] (needs `LogRead`).
pub async fn rejected_mutations_do_not_append_commands<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    // Push two items, claim the higher-priority one ("1", priority 5) → leased; "4" stays pending.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // leases "1"

    let before = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();

    let unknown = ItemId::new("91").unwrap();
    let _ = b
        .renew(&shard(), vec![unknown], ts(2000), ts(20), None)
        .await; // unknown id → NotFound
    let _ = b
        .reassign(
            &shard(),
            vec![unknown],
            LeaseToken::new("l2").unwrap(),
            ts(2000),
            ts(20),
            None,
        )
        .await; // unknown id → NotFound
    let _ = b
        .purge(
            &shard(),
            vec![ItemId::new("1").unwrap()],
            false,
            ts(20),
            None,
        )
        .await; // leased without force → Conflict
    let _ = b
        .finalize(
            &shard(),
            vec![FinalizeOutcome::new(
                ItemId::new("4").unwrap(),
                FinalizeKind::Complete,
            )],
            ts(20),
            None,
        )
        .await; // pending, not leased → Invalid

    let after = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(
        before, after,
        "rejected renew/reassign/purge/finalize must NOT append any command (B1 no-divergence)"
    );
}

/// API-001 external transaction contract: once `push` returns success, the accepted item is visible to
/// reads and claims on the authoritative owner. This catches log-then-apply backends that acknowledge at
/// durable append time but return before their serving projection / response barrier is satisfied.
pub async fn successful_push_is_visible_before_response_returns<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("txn-visible").unwrap();
    let ids = b
        .push(
            &shard(),
            vec![PushSpec {
                client_item_key: Some(key.clone()),
                priority: Some(PriorityValue::Int64(7)),
                payload: Some(Bytes::from_static(b"payload")),
                fields: BTreeMap::from([("state".to_string(), Bytes::from_static(b"new"))]),
                ..Default::default()
            }],
            ts(10),
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "successful push must be visible in queue metrics before response returns"
    );
    let live = b
        .live_items(&shard(), std::slice::from_ref(&key))
        .await
        .unwrap()
        .into_iter()
        .next()
        .flatten()
        .expect("successful push is live by client_item_key");
    assert_eq!(live.item_id, ids[0]);
    assert_eq!(live.payload.as_deref(), Some(&b"payload"[..]));
    assert_eq!(
        live.fields.get("state").map(|v| v.as_ref()),
        Some(&b"new"[..])
    );
    let claimed = b.claim(claim_req(1, 500, 20)).await.unwrap();
    assert_eq!(
        claimed.items.first().map(|item| item.item_id),
        Some(ids[0]),
        "successful push must be claimable immediately when eligible"
    );
}

/// API-001 external transaction contract: a structured rejection has no durable or visible effect for
/// the rejected mutation. The log-class suite separately checks append count; this core check compares
/// observable state and therefore runs for every projection family.
pub async fn rejected_finalize_leaves_visible_state_unchanged<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![PushSpec {
                client_item_key: Some(ClientItemKey::new("pending").unwrap()),
                priority: Some(PriorityValue::Int64(1)),
                ..Default::default()
            }],
            ts(0),
            None,
        )
        .await
        .unwrap();
    let before = b.metrics(&qkey()).await.unwrap();
    assert!(
        matches!(
            b.finalize(
                &shard(),
                vec![FinalizeOutcome::new(ids[0], FinalizeKind::Complete)],
                ts(10),
                None,
            )
            .await,
            Err(EngineError::Invalid(_))
        ),
        "finalizing a pending item must be a structured rejection"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap(),
        before,
        "rejected finalize must not change visible queue state"
    );
    let claimed = b.claim(claim_req(1, 500, 20)).await.unwrap();
    assert_eq!(
        claimed.items.first().map(|item| item.item_id),
        Some(ids[0]),
        "the rejected finalize must not consume or terminate the pending item"
    );
}

/// API-001 external transaction contract: retrying the same `request_id` and same body replays the
/// original committed response, while reusing the id with a different body is a structural conflict.
/// Backends must return `Unavailable` rather than silently accepting a request id without replay
/// semantics; selectable backends must pass this scenario.
pub async fn request_id_push_replays_once_and_conflicts_on_body_change<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let request_id = RequestId::new("txn-push-1").unwrap();
    let body = vec![PushSpec {
        client_item_key: Some(ClientItemKey::new("request-key").unwrap()),
        priority: Some(PriorityValue::Int64(11)),
        ..Default::default()
    }];

    let first = b
        .push_with_request_id(&shard(), request_id.clone(), body.clone(), ts(0), None)
        .await
        .unwrap();
    let second = b
        .push_with_request_id(&shard(), request_id.clone(), body, ts(1), None)
        .await
        .unwrap();
    assert_eq!(second, first, "same request_id/body replays ids");
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "request replay must not append a second item"
    );

    let conflict_body = vec![PushSpec {
        client_item_key: Some(ClientItemKey::new("request-key-2").unwrap()),
        priority: Some(PriorityValue::Int64(12)),
        ..Default::default()
    }];
    assert_eq!(
        b.push_with_request_id(&shard(), request_id, conflict_body, ts(2), None)
            .await,
        Err(EngineError::RequestIdConflict)
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "request-id conflict must not append"
    );
}

/// **Two-family CORE parity** (BQ-13, ADR-008 §2): drive the SAME arbitrary command sequence against two
/// backends from DIFFERENT projection families and assert their observable read-state is identical at
/// every step — a head-to-head differential proof of "behaviorally identical on the core class", stronger
/// than each family passing fixed scenarios separately. Pushes use the write-UoW with EXPLICIT item ids
/// (server-minted ids differ per backend by construction), so the compared state — metrics, eligibility
/// order, peek, and the (token-bearing) pending set — is family-independent.
pub async fn cross_family_core_parity<A: ConformanceCore, B: ConformanceCore>(
    make_a: impl Fn() -> A,
    make_b: impl Fn() -> B,
) {
    let a = make_a();
    let b = make_b();
    a.create_queue(qdef()).await.unwrap();
    b.create_queue(qdef()).await.unwrap();

    async fn commit_both<A: ConformanceCore, B: ConformanceCore>(a: &A, b: &B, cmd: QueueCommand) {
        commit(a, envelope(cmd.clone(), vec![])).await;
        commit(b, envelope(cmd, vec![])).await;
    }

    /// Assert the two backends present identical observable state. `select_eligible`/`peek` are compared in
    /// order (both families order by the strict-claim key); `pending` is sorted first (it is unordered).
    async fn parity<A: ConformanceCore, B: ConformanceCore>(a: &A, b: &B, now: i64, label: &str) {
        assert_eq!(
            a.metrics(&qkey()).await.unwrap(),
            b.metrics(&qkey()).await.unwrap(),
            "metrics diverge @ {label}"
        );
        assert_eq!(
            a.select_eligible(&shard(), ts(now), 100).await.unwrap(),
            b.select_eligible(&shard(), ts(now), 100).await.unwrap(),
            "select_eligible diverge @ {label}"
        );
        let pa: Vec<(String, Option<PriorityValue>, u64)> = a
            .peek(&shard(), 100)
            .await
            .unwrap()
            .into_iter()
            .map(|v| (v.item_id.to_string(), v.priority, v.item_version))
            .collect();
        let pb: Vec<(String, Option<PriorityValue>, u64)> = b
            .peek(&shard(), 100)
            .await
            .unwrap()
            .into_iter()
            .map(|v| (v.item_id.to_string(), v.priority, v.item_version))
            .collect();
        assert_eq!(pa, pb, "peek diverge @ {label}");
        let sort_pending = |v: Vec<pqueue_engine::LeaseView>| {
            let mut s: Vec<(String, String, i64, u32)> = v
                .into_iter()
                .map(|l| {
                    (
                        l.item_id.to_string(),
                        l.lease_token.as_str().to_string(),
                        l.lease_expires_at.seconds,
                        l.attempt_count,
                    )
                })
                .collect();
            s.sort();
            s
        };
        assert_eq!(
            sort_pending(a.pending(&shard()).await.unwrap()),
            sort_pending(b.pending(&shard()).await.unwrap()),
            "pending diverge @ {label}"
        );
    }

    parity(&a, &b, 100, "empty").await;

    // Push out of priority order (explicit ids so the families agree on identity).
    commit_both(
        &a,
        &b,
        QueueCommand::Push(PushCommand {
            items: vec![
                item("1", "ka", 30),
                item("2", "kb", 10),
                item("3", "kc", 20),
            ],
        }),
    )
    .await;
    parity(&a, &b, 100, "after push").await;

    // Claim the priority-10 head ("2") on both — identical request, identical selection + lease.
    a.claim(claim_req(1, 500, 10)).await.unwrap();
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    parity(&a, &b, 100, "after claim b").await;

    // Renew, then complete "2".
    a.renew(
        &shard(),
        vec![ItemId::new("2").unwrap()],
        ts(900),
        ts(20),
        None,
    )
    .await
    .unwrap();
    b.renew(
        &shard(),
        vec![ItemId::new("2").unwrap()],
        ts(900),
        ts(20),
        None,
    )
    .await
    .unwrap();
    parity(&a, &b, 100, "after renew b").await;
    let fin_b = vec![FinalizeOutcome::new(
        ItemId::new("2").unwrap(),
        FinalizeKind::Complete,
    )];
    a.finalize(&shard(), fin_b.clone(), ts(30), None)
        .await
        .unwrap();
    b.finalize(&shard(), fin_b, ts(30), None).await.unwrap();
    parity(&a, &b, 100, "after complete b").await;

    // Claim "3" (now the head), reassign it to a new consumer, then retry it back to pending.
    a.claim(claim_req(1, 500, 40)).await.unwrap();
    b.claim(claim_req(1, 500, 40)).await.unwrap();
    parity(&a, &b, 100, "after claim c").await;
    let l2 = LeaseToken::new("lease-2").unwrap();
    a.reassign(
        &shard(),
        vec![ItemId::new("3").unwrap()],
        l2.clone(),
        ts(800),
        ts(50),
        None,
    )
    .await
    .unwrap();
    b.reassign(
        &shard(),
        vec![ItemId::new("3").unwrap()],
        l2,
        ts(800),
        ts(50),
        None,
    )
    .await
    .unwrap();
    parity(&a, &b, 100, "after reassign c").await;
    let retry_c = vec![FinalizeOutcome::new(
        ItemId::new("3").unwrap(),
        FinalizeKind::Retry,
    )];
    a.finalize(&shard(), retry_c.clone(), ts(60), None)
        .await
        .unwrap();
    b.finalize(&shard(), retry_c, ts(60), None).await.unwrap();
    parity(&a, &b, 100, "after retry c").await;

    // Fence-then-finalize "3" after a re-claim: both families reject the fenced finalize identically.
    a.claim(claim_req(1, 500, 70)).await.unwrap();
    b.claim(claim_req(1, 500, 70)).await.unwrap();
    commit_both(
        &a,
        &b,
        QueueCommand::FenceLease(FenceLeaseCommand {
            item_ids: vec![ItemId::new("3").unwrap()],
        }),
    )
    .await;
    let fin_c = vec![FinalizeOutcome::new(
        ItemId::new("3").unwrap(),
        FinalizeKind::Complete,
    )];
    assert!(
        a.finalize(&shard(), fin_c.clone(), ts(80), None)
            .await
            .is_err()
    );
    assert!(b.finalize(&shard(), fin_c, ts(80), None).await.is_err());
    parity(&a, &b, 100, "after fenced-finalize reject").await;

    // Lease-expiry reclaim tick: "3" was leased through ts(500); tick past it returns it to pending.
    a.tick(ts(501)).await.unwrap();
    b.tick(ts(501)).await.unwrap();
    parity(&a, &b, 600, "after reclaim tick").await;

    // Purge the still-pending "1".
    a.purge(
        &shard(),
        vec![ItemId::new("1").unwrap()],
        false,
        ts(90),
        None,
    )
    .await
    .unwrap();
    b.purge(
        &shard(),
        vec![ItemId::new("1").unwrap()],
        false,
        ts(90),
        None,
    )
    .await
    .unwrap();
    parity(&a, &b, 600, "after purge a").await;

    // Pause hides eligibility on both; resume restores it.
    commit_both(
        &a,
        &b,
        QueueCommand::PauseQueue(PauseQueueCommand::default()),
    )
    .await;
    parity(&a, &b, 600, "after pause").await;
    commit_both(&a, &b, QueueCommand::ResumeQueue).await;
    parity(&a, &b, 600, "after resume").await;

    // ReplacePending (upsert via the write-UoW with explicit ids): supersede the pending "3" with "8".
    commit_both(
        &a,
        &b,
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("kc").unwrap(),
            superseded_item_id: ItemId::new("3").unwrap(),
            replacement: item("8", "kc", 20),
        }),
    )
    .await;
    parity(&a, &b, 600, "after replace c->c2").await;
}

/// **BQ-14a — claim compatibility is resolved and gated.** The claim resolves its `ClaimUnit` from the
/// request's compatibility options (API-001 Batch Claim) and gates non-item units. Item-level (the
/// default) is byte-identical to the existing claim; a valid group/cohort/same-group unit is refused with
/// the structured `Unavailable` (its selection lands in BQ-14b/c — an honest not-yet-implemented, not a
/// silent item-claim); an invalid combination is rejected with the structured validation error. Every
/// backend resolves identically (the shared `require_item_level_claim`).
pub async fn claim_compatibility_is_resolved_and_gated<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // An invalid combination (group_batching + whole_cohort) is rejected with the structured validation
    // error on EVERY backend — family-agnostic (no def fields read, no projection family difference).
    let mut bad = claim_req(1, 500, 10);
    bad.compatibility = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        whole_cohort: true,
        ..Default::default()
    };
    assert!(
        matches!(b.claim(bad).await, Err(EngineError::Invalid(_))),
        "an invalid compatibility combination is rejected with the structured error"
    );

    // The rejected claim changed nothing — an item-level (default) claim still leases "1", proving the
    // compatibility gate rejects BEFORE any selection/commit (no partial state).
    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "item-level claim is unchanged by the compatibility gate"
    );

    // NOTE: the behavior of a VALID compatibility unit (same_group_key / group_batching / whole_cohort) is
    // family-specific — the relational family implements group/cohort selection (BQ-14b/c), the in-memory
    // family does not maintain `group_summary` and refuses with `Unavailable`. That is RELATIONAL-class,
    // deliberately NOT asserted here (it would diverge across families); see the relational backends'
    // own `group_batching_*` / `same_group_key_*` tests.
}

// ---------------------------------------------------------------------------
// BQ-20 — the Single Authoritative Fencing Rule (TD-003). The durable `assignment_epoch` is the one
// fencing authority: `acquire_epoch` advances it strictly + durably (step 1, "durable fence before
// use"), and `LogWriter::append` rejects any non-current `expected_epoch` (step 2). Both projection
// families run these (a CORE guarantee; TD-001 lease/epoch fencing is the core class).
// ---------------------------------------------------------------------------

/// Append `command` to the queue under `expected_epoch` through the atomic write UoW, returning the
/// fence outcome (`EpochFenced` when stale). Apply only runs if the append is admitted.
async fn append_at_epoch<B: ConformanceCore>(
    b: &B,
    expected_epoch: u64,
    command: QueueCommand,
) -> EngineResult<()> {
    let env = envelope(command, vec![]);
    b.write(move |lw, pw| {
        let pos = lw.append(&shard(), std::slice::from_ref(&env), expected_epoch)?;
        pw.apply(&pos, std::slice::from_ref(&env))?;
        Ok(())
    })
    .await
}

/// A stale (non-current) epoch is fenced at append; the current epoch is admitted; `acquire_epoch`
/// advances the durable epoch strictly. Rejection is on "not equal to current", not "<= current" — a
/// future epoch is rejected too.
pub async fn stale_epoch_append_is_fenced<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let e0 = b.current_epoch(&shard()).await.unwrap();

    // An append at the current epoch is admitted.
    append_at_epoch(
        &b,
        e0,
        QueueCommand::PauseQueue(PauseQueueCommand::default()),
    )
    .await
    .expect("append at the current epoch is admitted");

    // Acquire allocates a strictly-greater, durably-recorded epoch (TD-003 monotonicity).
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(
        e1 > e0,
        "acquire_epoch must allocate a strictly-greater epoch"
    );
    assert_eq!(
        b.current_epoch(&shard()).await.unwrap(),
        e1,
        "acquire durably advances the recorded current epoch"
    );

    // The superseded owner's old epoch is fenced...
    assert_eq!(
        append_at_epoch(&b, e0, QueueCommand::ResumeQueue).await,
        Err(EngineError::EpochFenced),
        "a stale (old) epoch is fenced"
    );
    // ...and a NON-current FUTURE epoch is also rejected (the rule is "not current", not "<= current").
    assert_eq!(
        append_at_epoch(&b, e1 + 1, QueueCommand::ResumeQueue).await,
        Err(EngineError::EpochFenced),
        "a future (non-current) epoch is fenced too"
    );
    // The current owner appends fine.
    append_at_epoch(&b, e1, QueueCommand::ResumeQueue)
        .await
        .expect("the current-epoch owner is admitted");
}

/// The post-advance / pre-segment window is closed: the instant `acquire_epoch` advances to E+1 — before
/// the new owner writes ANY E+1 segment — a stale epoch-E writer is already fenced (TD-003 step 2: reject
/// against the recorded current epoch, which step 1 advanced at acquire, not lazily on first data write).
pub async fn epoch_fence_closes_pre_segment_window<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let e0 = b.current_epoch(&shard()).await.unwrap();
    // An epoch-E segment exists (the previous owner wrote data at E).
    append_at_epoch(
        &b,
        e0,
        QueueCommand::PauseQueue(PauseQueueCommand::default()),
    )
    .await
    .unwrap();

    // The new owner acquires E+1 — durably fenced — but has NOT written any E+1 segment yet.
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(e1 > e0);

    // The stale E writer's VERY NEXT append — in the window before any E+1 segment exists — is fenced.
    assert_eq!(
        append_at_epoch(&b, e0, QueueCommand::ResumeQueue).await,
        Err(EngineError::EpochFenced),
        "the pre-segment window is closed: a stale writer is fenced at handoff, not at first conflict"
    );

    // Only now does the new owner write the first E+1 segment.
    append_at_epoch(&b, e1, QueueCommand::ResumeQueue)
        .await
        .expect("the new owner writes the first new-epoch segment");
}

#[cfg(test)]
mod commit_transition_scenario_tests {
    use super::*;
    use pqueue_sqlite::SqliteRelationalBackend;
    use std::path::PathBuf;

    fn temp_db_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "pqueue-conformance-commit-transition-{tag}-{}.db",
            std::process::id()
        ))
    }

    fn sqlite_relational_make(path: &str) -> impl Fn() -> SqliteRelationalBackend + '_ {
        move || SqliteRelationalBackend::open(path).unwrap()
    }

    #[tokio::test]
    async fn sqlite_relational_commit_transition_writes_and_recovers() {
        let path = temp_db_path("writes");
        let _ = std::fs::remove_file(&path);
        let make = sqlite_relational_make(path.to_str().unwrap());
        commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(
            make,
        )
        .await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_relational_commit_transition_rejects_bad_token() {
        let path = temp_db_path("bad-token");
        let _ = std::fs::remove_file(&path);
        let make = sqlite_relational_make(path.to_str().unwrap());
        commit_transition_rejects_bad_token_without_writing(make).await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_relational_commit_transition_rejects_bad_version() {
        let path = temp_db_path("bad-version");
        let _ = std::fs::remove_file(&path);
        let make = sqlite_relational_make(path.to_str().unwrap());
        commit_transition_rejects_bad_version_without_writing(make).await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_relational_commit_transition_request_id_replays() {
        let path = temp_db_path("replay");
        let _ = std::fs::remove_file(&path);
        let make = sqlite_relational_make(path.to_str().unwrap());
        commit_transition_request_id_replays_without_double_write(make).await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_relational_commit_transition_explain_commit_recovers() {
        let path = temp_db_path("recover");
        let _ = std::fs::remove_file(&path);
        let make = sqlite_relational_make(path.to_str().unwrap());
        commit_transition_explain_commit_recovers_transition_and_survives_reopen(make).await;
        let _ = std::fs::remove_file(&path);
    }
}
