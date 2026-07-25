//! Durability checks for the filesystem log-only substrate and supported composed object-log profile.

use fireweed_conformance::{claim_req, envelope, item, qdef, qkey, shard};
use fireweed_core::{
    ClientItemKey, CohortOnIncomplete, CohortPolicy, EntitySchemaDocument, GateKeyPolicy,
    IndexDeclaration, IndexDef, IndexSpec, IndexType, ItemId, MetadataValue, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueId, QueueIndex,
    RecurrenceMode, RecurrencePolicy, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    ClaimPort, ControlPlaneStore, EngineError, LogRead, ProjectionRead, PushCommand, PushPort,
    PushSpec, QueueCommand, ReplacePendingCommand, SetGatesCommand,
};
use fireweed_objectlog::{LocalObjectLog, ObjectLogSegmentConfig, composed_objectlog_backend};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("fireweed-objlog-dur-{tag}-{}", std::process::id()))
}

fn queue_dir(root: &std::path::Path, key: &fireweed_engine::QueueKey) -> std::path::PathBuf {
    let raw = format!("{}\0{}", key.tenant_id.as_str(), key.queue_id.as_str());
    let encoded = raw
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join(encoded)
}

fn corrupt_last_log_object(root: &std::path::Path, key: &fireweed_engine::QueueKey) {
    let log_dir = queue_dir(root, key).join("log");
    let mut files = std::fs::read_dir(log_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    files.sort();
    std::fs::write(files.last().unwrap(), b"{ truncated not valid json").unwrap();
}

fn push_env(id: &str) -> fireweed_engine::CommandEnvelope {
    envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item(id, &format!("k{id}"), 1)],
        }),
        vec![ItemId::new(id).unwrap()],
    )
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
async fn local_object_log_appends_reads_and_reopens_without_projection() {
    let root = tmp_root("local-store");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    let envs = [
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![ItemId::new("1").unwrap()],
        ),
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("2", "kb", 7)],
            }),
            vec![ItemId::new("2").unwrap()],
        ),
    ];

    let positions = store.append(&shard(), &envs, 0).unwrap();
    assert_eq!(
        positions
            .iter()
            .map(|position| position.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );

    let reopened = LocalObjectLog::open(&root).expect("reopen");
    let page = reopened.read_from(&shard(), None, 10).await.unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].0.sequence, 0);
    assert_eq!(page.entries[1].0.sequence, 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn local_object_log_create_returns_durable_decoded_definition() {
    let root = tmp_root("local-create-rich-definition");
    let _ = std::fs::remove_dir_all(&root);
    let definition = non_default_qdef();
    let store = LocalObjectLog::open(&root).expect("open");

    let created = store
        .create_queue(definition.clone())
        .expect("create rich definition");
    assert!(created.created);
    assert_eq!(created.definition, definition);

    let reopened = LocalObjectLog::open(&root).expect("reopen");
    let existing = reopened
        .create_queue(definition.clone())
        .expect("read existing rich definition");
    assert!(!existing.created);
    assert_eq!(existing.definition, definition);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn local_object_log_concurrent_compatible_create_has_one_durable_winner() {
    let root = tmp_root("local-compatible-create-race");
    let _ = std::fs::remove_dir_all(&root);
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let store = LocalObjectLog::open(&root).expect("open contender");
                barrier.wait();
                store.create_queue(non_default_qdef())
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

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn local_object_log_stale_temp_name_cannot_wedge_create_retry() {
    let root = tmp_root("local-stale-create-temp");
    let _ = std::fs::remove_dir_all(&root);
    let dir = queue_dir(&root, &qkey());
    std::fs::create_dir_all(&dir).unwrap();
    // Simulate a crashed prior process whose PID and a broad range of process-local attempt suffixes are
    // reused. The create loop must skip occupied names instead of treating the first collision as fatal.
    for attempt in 0..256 {
        let stale = dir.join(format!("queue.json.tmp.{}.{attempt}", std::process::id()));
        std::fs::write(stale, b"stale incomplete metadata").unwrap();
    }

    let store = LocalObjectLog::open(&root).expect("open with stale temp");
    assert!(
        store
            .create_queue(qdef())
            .expect("retry skips stale name")
            .created
    );
    assert!(dir.join("queue.json").is_file());

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn local_object_log_create_error_cleans_its_unique_temp_file() {
    let root = tmp_root("local-create-error-temp-cleanup");
    let _ = std::fs::remove_dir_all(&root);
    let dir = queue_dir(&root, &qkey());
    let store = LocalObjectLog::open(&root).expect("open before invalid queue target");
    std::fs::create_dir_all(dir.join("queue.json")).unwrap();
    assert!(matches!(
        store.create_queue(qdef()),
        Err(EngineError::Storage(_))
    ));
    let leaked = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("queue.json.tmp."))
        .collect::<Vec<_>>();
    assert!(
        leaked.is_empty(),
        "failed create leaked temp files: {leaked:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn local_object_log_concurrent_incompatible_loser_conflicts() {
    let root = tmp_root("local-incompatible-create-race");
    let _ = std::fs::remove_dir_all(&root);
    let barrier = Arc::new(Barrier::new(2));
    let definitions = {
        let first = non_default_qdef();
        let mut second = first.clone();
        second.priority_model.direction = PriorityDirection::Ascending;
        vec![first, second]
    };
    let handles = definitions
        .into_iter()
        .map(|definition| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let store = LocalObjectLog::open(&root).expect("open contender");
                barrier.wait();
                store.create_queue(definition)
            })
        })
        .collect::<Vec<_>>();

    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().expect("thread"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(EngineError::QueueDefinitionConflict)))
            .count(),
        1
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn local_object_log_rejects_incompatible_non_placement_definition() {
    let root = tmp_root("local-incompatible-non-placement");
    let _ = std::fs::remove_dir_all(&root);
    let definition = non_default_qdef();
    let mut incompatible = definition.clone();
    incompatible.request_id_retention_ms += 1;

    let store = LocalObjectLog::open(&root).expect("open");
    assert!(store.create_queue(definition).expect("create").created);
    assert!(matches!(
        store.create_queue(incompatible.clone()),
        Err(EngineError::QueueDefinitionConflict)
    ));

    let reopened = LocalObjectLog::open(&root).expect("reopen");
    assert!(matches!(
        reopened.create_queue(incompatible),
        Err(EngineError::QueueDefinitionConflict)
    ));

    let _ = std::fs::remove_dir_all(&root);
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

    let reopened = composed_objectlog_backend(&root).expect("reopen");
    assert_eq!(reopened.queue_definition(&qkey()).await.unwrap(), qdef());

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
        futures::executor::block_on(winner.create_queue(definition.clone()))
            .expect("create")
            .created
    );

    let loser = composed_objectlog_backend(&root).expect("open loser");
    assert!(matches!(
        futures::executor::block_on(loser.create_queue(incompatible)),
        Err(EngineError::QueueDefinitionConflict)
    ));
    let key =
        fireweed_engine::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert_eq!(
        futures::executor::block_on(loser.queue_definition(&key)).expect("durable winner readable"),
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
                futures::executor::block_on(backend.create_queue(non_default_qdef()))
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
        futures::executor::block_on(reopened.queue_definition(&fireweed_engine::QueueKey::new(
            TenantId::new("tenant-rich").unwrap(),
            QueueId::new("queue-rich").unwrap()
        )))
        .unwrap(),
        non_default_qdef()
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn local_object_log_rejects_stale_expected_epoch_before_append() {
    let root = tmp_root("local-epoch");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    assert_eq!(store.acquire_epoch(&shard()).unwrap(), 1);
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "ka", 5)],
        }),
        vec![ItemId::new("1").unwrap()],
    );

    let err = store.append(&shard(), &[env], 0).unwrap_err();
    assert_eq!(err, EngineError::EpochFenced);
    let page = store.read_from(&shard(), None, 10).await.unwrap();
    assert!(page.entries.is_empty(), "stale append wrote no objects");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn local_object_log_rejects_replace_pending_before_append() {
    let root = tmp_root("local-rp");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    let valid = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "ka", 5)],
        }),
        vec![ItemId::new("1").unwrap()],
    );
    let unsupported = envelope(
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("ka").unwrap(),
            superseded_item_id: ItemId::new("1").unwrap(),
            replacement: item("2", "ka", 5),
        }),
        vec![],
    );

    let err = store
        .append(&shard(), &[valid, unsupported], 0)
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
    let page = store.read_from(&shard(), None, 10).await.unwrap();
    assert!(
        page.entries.is_empty(),
        "unsupported command wrote no objects"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn local_object_log_rejects_gate_commands_before_append() {
    let root = tmp_root("local-set-gates");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    let valid = push_env("1");
    let unsupported = envelope(
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["hold".to_string()],
            blocked: true,
        }),
        vec![],
    );

    assert_eq!(
        store
            .append(&shard(), &[valid, unsupported], 0)
            .unwrap_err(),
        EngineError::Unavailable
    );
    assert!(
        store
            .read_from(&shard(), None, 10)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn local_object_log_skips_a_torn_trailing_segment_on_reopen() {
    let root = tmp_root("local-torn-tail");
    let _ = std::fs::remove_dir_all(&root);
    let key = shard();
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    store.append(&key, &[push_env("1")], 0).unwrap();
    store.append(&key, &[push_env("2")], 0).unwrap();
    corrupt_last_log_object(&root, &key);

    let reopened = LocalObjectLog::open(&root).expect("reopen");
    let page = reopened.read_from(&key, None, 10).await.unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].0.sequence, 0);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn objectlog_segment_configuration_is_respected() {
    let root = tmp_root("segment-config");
    let _ = std::fs::remove_dir_all(&root);
    let shard = shard();
    let store = LocalObjectLog::open_with_config(
        &root,
        ObjectLogSegmentConfig {
            segment_max_commands: 10,
            segment_max_bytes: 1,
            segment_max_latency_ms: 5,
        },
    )
    .expect("open");
    store.create_queue(qdef()).unwrap();
    store
        .append(&shard, &[push_env("1"), push_env("2"), push_env("3")], 0)
        .expect("append");

    let stats = store.segment_stats(&shard).expect("segment stats");
    assert_eq!(
        stats.segment_objects, 3,
        "segment_max_bytes=1 should cap the batch size at one command per segment"
    );
    assert_eq!(stats.command_objects, 3);

    let _ = std::fs::remove_dir_all(&root);
}
