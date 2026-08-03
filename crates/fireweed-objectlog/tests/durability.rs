//! Durability checks for the supported composed object-log profile.
//!
//! Native LogEngine append/read/reopen coverage lives with `log_engine_store`.

use fireweed_conformance::{claim_req, qdef, qkey};
use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, EntitySchemaDocument, GateKeyPolicy, IndexDeclaration,
    IndexDef, IndexSpec, IndexType, MetadataValue, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueId, QueueIndex, RecurrenceMode, RecurrencePolicy,
    TenantId, UtcTimestamp,
};
use fireweed_engine::{
    ClaimPort, ControlPlaneStore, EngineError, ProjectionRead, PushPort, PushSpec,
};
use fireweed_objectlog::{block_on_objectlog_future, composed_objectlog_backend};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("fireweed-objlog-dur-{tag}-{}", std::process::id()))
}

fn non_default_qdef() -> fireweed_core::QueueDefinition {
    let mut blockers = BTreeMap::new();
    blockers.insert(
        "blocked".to_string(),
        vec![MetadataValue::String("yes".to_string())],
    );

    fireweed_core::QueueDefinition {
        tenant_id: TenantId::new("tenant-rich").unwrap(),
        queue_id: QueueId::new("queue-rich").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Text,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::ClientItemKey,
        },
        ordering_mode: OrderingMode::BoundedRelaxed,
        max_rank_error: 7,
        progress_bound_ms: 12_345,
        eligibility_policy: fireweed_core::EligibilityPolicy {
            metadata_blockers: blockers,
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(3),
            max_gates_per_request: Some(5),
        },
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(9_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(8),
        }),
        recurrence: RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(UtcTimestamp::new(4_242, 123_000_000).unwrap()),
        },
        request_id_retention_ms: 11_000,
        client_item_key_retention_ms: 12_000,
        terminal_retention_ms: 13_000,
        max_lease_duration_ms: 14_000,
        retry_policy: fireweed_core::RetryPolicy { max_attempts: 9 },
        max_push_batch_size: 17,
        max_claim_batch_size: 19,
        max_eligible_group_size: Some(23),
        secondary_indexes: vec![IndexSpec {
            name: "by_customer".to_string(),
            fields: vec!["customer".to_string(), "region".to_string()],
            unique: true,
        }],
        entity_schema: Some(
            serde_json::from_value::<EntitySchemaDocument>(json!({
                "entity_schema": {
                    "type": "object",
                    "required": ["status"],
                    "properties": {
                        "status": {"type": "string"},
                        "attempt": {"type": "integer"}
                    }
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

#[tokio::test]
async fn composed_object_log_compatible_loser_can_use_queue_immediately() {
    let root = tmp_root("composed-compatible-loser-immediate-use");
    let _ = std::fs::remove_dir_all(&root);
    let winner = composed_objectlog_backend(&root).expect("open winner");
    let loser = composed_objectlog_backend(&root).expect("open loser");
    assert!(winner.create_queue(qdef()).await.unwrap().created);
    winner
        .push(
            &qkey(),
            vec![PushSpec::default()],
            UtcTimestamp::new(1, 0).unwrap(),
            None,
        )
        .await
        .expect("winner durable push before handoff");
    let loser_outcome = loser.create_queue(qdef()).await.unwrap();
    assert!(!loser_outcome.created);
    assert_eq!(loser_outcome.definition, qdef());
    assert_eq!(
        loser.peek(&qkey(), 10).await.expect("replayed read").len(),
        1
    );

    let claimed = loser
        .claim(claim_req(10, 30, 10))
        .await
        .expect("loser claims replayed authoritative item");
    assert_eq!(claimed.items.len(), 1);
    loser
        .push(
            &qkey(),
            vec![PushSpec::default()],
            UtcTimestamp::new(2, 0).unwrap(),
            None,
        )
        .await
        .expect("loser appends after handoff");

    let reopened = composed_objectlog_backend(&root).expect("reopen");
    assert_eq!(reopened.queue_definition(&qkey()).await.unwrap(), qdef());
    assert_eq!(
        reopened
            .peek(&qkey(), 10)
            .await
            .expect("winner and loser history replay")
            .len(),
        1
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn composed_loser_replays_commands_committed_before_create_handoff() {
    let root = tmp_root("composed-loser-replays-before-create");
    let _ = std::fs::remove_dir_all(&root);
    let winner = composed_objectlog_backend(&root).expect("open winner");
    let loser = composed_objectlog_backend(&root).expect("open loser before create");
    assert!(winner.create_queue(qdef()).await.unwrap().created);
    winner
        .push(
            &qkey(),
            vec![PushSpec::default()],
            UtcTimestamp::new(1, 0).unwrap(),
            None,
        )
        .await
        .expect("winner durable push");

    let outcome = loser
        .create_queue(qdef())
        .await
        .expect("compatible handoff");
    assert!(!outcome.created);
    assert_eq!(
        loser.peek(&qkey(), 10).await.expect("replayed read").len(),
        1
    );
    let claimed = loser
        .claim(claim_req(10, 30, 10))
        .await
        .expect("loser claims replayed authoritative item");
    assert_eq!(claimed.items.len(), 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn composed_incompatible_loser_caches_durable_winner_for_read() {
    let root = tmp_root("composed-incompatible-loser-readable");
    let _ = std::fs::remove_dir_all(&root);
    let definition = non_default_qdef();
    let mut incompatible = definition.clone();
    incompatible.request_id_retention_ms += 1;
    let winner = composed_objectlog_backend(&root).expect("open winner");
    let loser = composed_objectlog_backend(&root).expect("open loser before create");
    assert!(
        winner
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );

    assert!(matches!(
        loser.create_queue(incompatible).await,
        Err(EngineError::QueueDefinitionConflict)
    ));
    let key =
        fireweed_engine::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert_eq!(
        loser.queue_definition(&key).await.expect("cached winner"),
        definition
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn composed_object_log_rejects_incompatible_non_placement_definition() {
    let root = tmp_root("composed-incompatible-non-placement");
    let _ = std::fs::remove_dir_all(&root);
    let definition = non_default_qdef();
    let mut incompatible = definition.clone();
    incompatible.request_id_retention_ms += 1;

    let winner = composed_objectlog_backend(&root).expect("open winner");
    assert!(
        block_on_objectlog_future(winner.create_queue(definition.clone()))
            .expect("create")
            .created
    );

    let loser = composed_objectlog_backend(&root).expect("open loser");
    assert!(matches!(
        block_on_objectlog_future(loser.create_queue(incompatible)),
        Err(EngineError::QueueDefinitionConflict)
    ));
    let key =
        fireweed_engine::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert_eq!(
        block_on_objectlog_future(loser.queue_definition(&key)).expect("durable winner readable"),
        definition
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn composed_object_log_concurrent_create_returns_durable_winner() {
    let root = tmp_root("composed-create-race");
    let _ = std::fs::remove_dir_all(&root);
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let backend = composed_objectlog_backend(&root).expect("open contender");
                barrier.wait();
                block_on_objectlog_future(backend.create_queue(non_default_qdef()))
            })
        })
        .collect::<Vec<_>>();

    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().expect("thread"))
        .collect::<Result<Vec<_>, _>>()
        .expect("race outcomes");
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.definition == non_default_qdef())
    );

    let reopened = composed_objectlog_backend(&root).expect("reopen");
    assert_eq!(
        block_on_objectlog_future(reopened.queue_definition(&fireweed_engine::QueueKey::new(
            TenantId::new("tenant-rich").unwrap(),
            QueueId::new("queue-rich").unwrap()
        )))
        .unwrap(),
        non_default_qdef()
    );

    let _ = std::fs::remove_dir_all(&root);
}
