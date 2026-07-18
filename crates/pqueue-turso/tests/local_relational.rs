use std::time::Duration;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, ItemId, LeaseToken, Metadata, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    AsyncProjectionStore, ClaimCommand, CommandChecksum, CommandEnvelope, CommandId,
    CommandPosition, ProjectionStore, PushCommand, PushItem, QueueCommand, QueueKey,
};
use pqueue_relational::OWNED_PROJECTION_TABLES;
use pqueue_sqlite::SqliteProjectionStore;
use pqueue_turso::{
    JournalMode, RelationalStatement, TursoConfig, TursoRelational, TursoRelationalError,
};
use tempfile::tempdir;
use turso::Value;

fn timestamp(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).expect("timestamp")
}

fn definition() -> QueueDefinition {
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
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: Vec::new(),
        entity_schema: None,
        typed_indexes: Vec::new(),
        emit_change_records: false,
    }
}

fn envelope(id: &str, command: QueueCommand, item_ids: Vec<ItemId>, now: i64) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: timestamp(now),
    }
}

#[tokio::test]
async fn configures_and_verifies_the_exact_shared_schema() {
    let store = TursoRelational::in_memory().await.expect("open Turso");
    let settings = store.connection_settings().await.expect("settings");
    assert_eq!(settings.journal_mode, "wal");
    assert_eq!(settings.synchronous, 1);
    assert_eq!(settings.busy_timeout_ms, 5_000);

    let report = store.schema_report().await.expect("schema report");
    for table in OWNED_PROJECTION_TABLES {
        assert!(report.tables.iter().any(|actual| actual == table));
    }
    for index in [
        "pqueue_items_active_key",
        "pqueue_items_group_due_idx",
        "pqueue_item_index_key_idx",
    ] {
        assert!(report.indexes.iter().any(|actual| actual == index));
    }
}

#[tokio::test]
async fn migration_is_idempotent_and_state_survives_reopen() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("projection.db");
    let config = TursoConfig::local(&path).with_busy_timeout(Duration::from_millis(2_500));
    let store = TursoRelational::open(config.clone()).await.expect("open");
    store.migrate().await.expect("second migration");

    store
        .execute_immediate(&[
            RelationalStatement::new(
                "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)",
                vec!["t".into(), "q".into(), "{}".into()],
            ),
            RelationalStatement::new(
                "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq,assignment_epoch) \
                 VALUES(?1,?2,?3,?4,?5)",
                vec!["t".into(), "q".into(), 7_i64.into(), 3_i64.into(), 2_i64.into()],
            ),
        ])
        .await
        .expect("atomic seed");
    drop(store);

    let reopened = TursoRelational::open(config).await.expect("reopen");
    let settings = reopened.connection_settings().await.expect("settings");
    assert_eq!(settings.journal_mode, "wal");
    assert_eq!(settings.busy_timeout_ms, 2_500);
    let rows = reopened
        .query(
            "SELECT next_seq,next_item_seq,assignment_epoch FROM relational_cursor \
             WHERE tenant=?1 AND queue=?2",
            vec!["t".into(), "q".into()],
        )
        .await
        .expect("cursor");
    assert_eq!(
        rows[0].values,
        vec![Value::Integer(7), Value::Integer(3), Value::Integer(2)]
    );
}

#[tokio::test]
async fn immediate_batch_rolls_back_every_statement_on_error() {
    let store = TursoRelational::in_memory().await.expect("open");
    let result = store
        .execute_immediate(&[
            RelationalStatement::new(
                "INSERT INTO pqueue_side_records(tenant_id,queue_id,key,payload) \
                 VALUES(?1,?2,?3,?4)",
                vec!["t".into(), "q".into(), vec![1_u8].into(), vec![2_u8].into()],
            ),
            RelationalStatement::new(
                "INSERT INTO pqueue_side_records(tenant_id,queue_id,key,payload) \
                 VALUES(?1,?2,?3,?4)",
                vec!["t".into(), "q".into(), vec![1_u8].into(), vec![3_u8].into()],
            ),
        ])
        .await;
    assert!(matches!(result, Err(TursoRelationalError::Database(_))));

    let rows = store
        .query("SELECT COUNT(*) FROM pqueue_side_records", vec![])
        .await
        .expect("count");
    assert_eq!(rows[0].values, vec![Value::Integer(0)]);
}

#[tokio::test]
async fn rejects_invalid_config_before_opening() {
    let result = TursoRelational::open(
        TursoConfig::in_memory()
            .with_busy_timeout(Duration::ZERO)
            .with_journal_mode(JournalMode::Mvcc),
    )
    .await;
    assert!(matches!(
        result,
        Err(TursoRelationalError::Configuration(_))
    ));
}

#[tokio::test]
async fn async_projection_matches_sqlite_for_push_claim_reads_and_frontier() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let mut sqlite = SqliteProjectionStore::in_memory().expect("sqlite");
    ProjectionStore::ensure_shard(&mut sqlite, &definition).expect("sqlite ensure");
    let turso = TursoRelational::in_memory().await.expect("Turso");
    AsyncProjectionStore::ensure_shard(&turso, definition.clone())
        .await
        .expect("Turso ensure");

    let first = ItemId::mint(1, 0, 0);
    let second = ItemId::mint(1, 0, 1);
    let items = vec![
        PushItem {
            client_item_key: ClientItemKey::new("first").unwrap(),
            item_id: first,
            priority: Some(PriorityValue::Int64(20)),
            not_before: None,
            group_key: None,
            max_attempts: 3,
            payload: Some(Bytes::from_static(b"first-payload")),
            fields: std::collections::BTreeMap::from([(
                "color".to_string(),
                Bytes::from_static(b"red"),
            )]),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        },
        PushItem {
            client_item_key: ClientItemKey::new("second").unwrap(),
            item_id: second,
            priority: Some(PriorityValue::Int64(10)),
            not_before: None,
            group_key: None,
            max_attempts: 3,
            payload: None,
            fields: Default::default(),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        },
    ];
    let push = envelope(
        "push",
        QueueCommand::Push(PushCommand { items }),
        vec![first, second],
        10,
    );
    let push_position = CommandPosition::new(shard.clone(), 1, 0);
    ProjectionStore::apply(
        &mut sqlite,
        std::slice::from_ref(&push_position),
        std::slice::from_ref(&push),
    )
    .expect("sqlite push");
    AsyncProjectionStore::apply_live(&turso, vec![push_position.clone()], vec![push.clone()])
        .await
        .expect("Turso push");

    let sqlite_eligible =
        ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(10), 10).unwrap();
    let turso_eligible =
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(10), 10)
            .await
            .unwrap();
    assert_eq!(turso_eligible, sqlite_eligible);
    assert_eq!(turso_eligible, vec![second, first]);
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), first)
            .await
            .unwrap(),
        ProjectionStore::item_state(&sqlite, &shard, &first).unwrap()
    );
    assert_eq!(
        AsyncProjectionStore::item_version(&turso, shard.clone(), first)
            .await
            .unwrap(),
        ProjectionStore::item_version(&sqlite, &shard, &first).unwrap()
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap(),
        ProjectionStore::recovery_high_water(&sqlite, &shard).unwrap()
    );

    let lease = LeaseToken::new("lease-1").unwrap();
    let claim = envelope(
        "claim",
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![second],
            lease_token: lease.clone(),
            lease_expires_at: timestamp(100),
            worker_id: Some(WorkerId::new("worker").unwrap()),
        }),
        vec![second],
        11,
    );
    let claim_position = CommandPosition::new(shard.clone(), 1, 1);
    ProjectionStore::apply(
        &mut sqlite,
        std::slice::from_ref(&claim_position),
        std::slice::from_ref(&claim),
    )
    .expect("sqlite claim");
    AsyncProjectionStore::apply_live(&turso, vec![claim_position.clone()], vec![claim])
        .await
        .expect("Turso claim");

    let sqlite_claimed = ProjectionStore::render_claimed(&sqlite, &shard, &[second]).unwrap();
    let turso_claimed = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![second])
        .await
        .unwrap();
    assert_eq!(turso_claimed.len(), 1);
    assert_eq!(turso_claimed[0].item_id, sqlite_claimed[0].item_id);
    assert_eq!(
        turso_claimed[0].item_version,
        sqlite_claimed[0].item_version
    );
    assert_eq!(turso_claimed[0].lease_token, Some(lease));
    assert_eq!(
        turso_claimed[0].attempt_count,
        sqlite_claimed[0].attempt_count
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap(),
        Some(claim_position)
    );
    assert_eq!(
        AsyncProjectionStore::recover_definitions(&turso)
            .await
            .unwrap(),
        vec![definition]
    );

    // An overlapping recovery prefix is an idempotent no-op and cannot move the frontier backward.
    AsyncProjectionStore::apply_recovery(&turso, vec![push_position], vec![push])
        .await
        .expect("overlap replay");
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard)
            .await
            .unwrap(),
        Some(CommandPosition::new(
            QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap()
            ),
            1,
            1,
        ))
    );

    // This is deliberately the initial CreateQueue/Push/Claim projection slice. Other command arms fail
    // before a transaction mutates rows or advances the frontier; full command parity is later work.
    let unsupported = envelope("resume", QueueCommand::ResumeQueue, Vec::new(), 12);
    let unsupported_position = CommandPosition::new(
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        ),
        1,
        2,
    );
    assert!(matches!(
        AsyncProjectionStore::apply_live(&turso, vec![unsupported_position], vec![unsupported])
            .await,
        Err(pqueue_engine::EngineError::Unavailable)
    ));
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(
            &turso,
            QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .sequence,
        1
    );
}
