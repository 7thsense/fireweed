use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, EntitySchemaDocument, GateKeyPolicy, IndexDeclaration,
    IndexDef, IndexSpec, IndexType, MetadataValue, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, QueueIndex, RecurrenceMode,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use fireweed_engine::{ClaimPort, ControlPlaneStore, EngineError, LogStore, PushPort, PushSpec};
use fireweed_sqlite::{SqliteLog, composed_sqlite_backend};

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fireweed-sqlite-definition-{tag}-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn rich_definition() -> QueueDefinition {
    let mut blockers = BTreeMap::new();
    blockers.insert(
        "blocked".to_string(),
        vec![MetadataValue::String("yes".to_string())],
    );
    QueueDefinition {
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
        retry_policy: RetryPolicy { max_attempts: 9 },
        max_push_batch_size: 17,
        max_claim_batch_size: 19,
        max_eligible_group_size: Some(23),
        secondary_indexes: vec![IndexSpec {
            name: "by_customer".to_string(),
            fields: vec!["customer".to_string(), "region".to_string()],
            unique: true,
        }],
        entity_schema: Some(
            serde_json::from_value::<EntitySchemaDocument>(serde_json::json!({
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

#[test]
fn sqlite_log_definition_create_or_read_rich_round_trip() {
    let path = temp_db("rich-round-trip");
    let _ = std::fs::remove_file(&path);
    let definition = rich_definition();
    let outcome = LogStore::create_or_read_definition(
        &mut SqliteLog::open(path.to_str().unwrap()).unwrap(),
        &definition,
    )
    .unwrap()
    .expect("SQLite owns the durable definition catalog");
    assert!(outcome.created);
    assert_eq!(outcome.definition, definition);

    let recovered = SqliteLog::open(path.to_str().unwrap())
        .unwrap()
        .recover_definitions()
        .unwrap();
    assert_eq!(recovered, vec![definition]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sqlite_log_compatible_definition_race_has_one_winner_no_overwrite() {
    let path = temp_db("compatible-race");
    let _ = std::fs::remove_file(&path);
    let definition = rich_definition();
    let barrier = Arc::new(Barrier::new(4));
    let handles = (0..4)
        .map(|_| {
            let path = path.clone();
            let definition = definition.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut log = SqliteLog::open(path.to_str().unwrap()).unwrap();
                barrier.wait();
                LogStore::create_or_read_definition(&mut log, &definition)
                    .unwrap()
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.definition == definition)
    );

    let later = LogStore::create_or_read_definition(
        &mut SqliteLog::open(path.to_str().unwrap()).unwrap(),
        &definition,
    )
    .unwrap()
    .unwrap();
    assert!(!later.created);
    assert_eq!(later.definition, definition);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sqlite_log_incompatible_definition_race_conflicts_and_caches_winner() {
    let path = temp_db("incompatible-race");
    let _ = std::fs::remove_file(&path);
    let first = rich_definition();
    let mut second = first.clone();
    second.request_id_retention_ms += 1;
    let barrier = Arc::new(Barrier::new(2));
    let handles = [first.clone(), second.clone()].map(|definition| {
        let path = path.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let mut log = SqliteLog::open(path.to_str().unwrap()).unwrap();
            barrier.wait();
            log.persist_definition(&definition)
        })
    });
    let outcomes = handles.map(|handle| handle.join().unwrap());
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(EngineError::QueueDefinitionConflict)))
            .count(),
        1
    );

    let recovered = SqliteLog::open(path.to_str().unwrap())
        .unwrap()
        .recover_definitions()
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert!(recovered[0] == first || recovered[0] == second);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn composed_sqlite_compatible_loser_can_push_claim_and_reopen() {
    let path = temp_db("compatible-loser-use");
    let _ = std::fs::remove_file(&path);
    let definition = fireweed_conformance::qdef();
    let backends = [
        Arc::new(composed_sqlite_backend(path.to_str().unwrap()).unwrap()),
        Arc::new(composed_sqlite_backend(path.to_str().unwrap()).unwrap()),
    ];
    let barrier = Arc::new(Barrier::new(2));
    let handles = backends.each_ref().map(|backend| {
        let backend = Arc::clone(backend);
        let barrier = barrier.clone();
        let definition = definition.clone();
        std::thread::spawn(move || {
            barrier.wait();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(backend.create_queue(definition))
        })
    });
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.definition == definition)
    );
    let loser_index = outcomes
        .iter()
        .position(|outcome| !outcome.created)
        .expect("one durable create loser");
    let loser = &backends[loser_index];
    loser
        .push(
            &fireweed_conformance::qkey(),
            vec![PushSpec::default()],
            UtcTimestamp::new(1, 0).unwrap(),
            None,
        )
        .await
        .unwrap();
    let claimed = loser
        .claim(fireweed_conformance::claim_req(1, 30, 10))
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 1);
    drop(backends);

    let reopened = composed_sqlite_backend(path.to_str().unwrap()).unwrap();
    assert_eq!(
        reopened
            .queue_definition(&fireweed_conformance::qkey())
            .await
            .unwrap(),
        definition
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn composed_sqlite_loser_replays_commands_committed_before_create_handoff() {
    let path = temp_db("loser-replay-before-handoff");
    let _ = std::fs::remove_file(&path);
    let definition = fireweed_conformance::qdef();
    let winner = composed_sqlite_backend(path.to_str().unwrap()).unwrap();
    let loser = composed_sqlite_backend(path.to_str().unwrap()).unwrap();
    assert!(
        winner
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );
    winner
        .push(
            &fireweed_conformance::qkey(),
            vec![PushSpec::default()],
            UtcTimestamp::new(1, 0).unwrap(),
            None,
        )
        .await
        .unwrap();

    let outcome = loser.create_queue(definition).await.unwrap();
    assert!(!outcome.created);
    let claimed = loser
        .claim(fireweed_conformance::claim_req(1, 30, 10))
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 1);
    let _ = std::fs::remove_file(path);
}
