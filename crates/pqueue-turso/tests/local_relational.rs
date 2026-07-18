use std::time::Duration;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, ItemId, LeaseToken, Metadata, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    AsyncProjectionStore, ClaimCommand, CommandChecksum, CommandEnvelope, CommandId,
    CommandPosition, FenceLeaseCommand, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, ProjectionStore, PushCommand, PushItem, QueueCommand, QueueKey,
    ReassignLeaseCommand, RenewLeaseCommand, UnfenceLeaseCommand,
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

async fn apply_both(
    sqlite: &mut SqliteProjectionStore,
    turso: &TursoRelational,
    shard: &QueueKey,
    sequence: u64,
    command: CommandEnvelope,
) {
    let position = CommandPosition::new(shard.clone(), 1, sequence);
    ProjectionStore::apply(
        sqlite,
        std::slice::from_ref(&position),
        std::slice::from_ref(&command),
    )
    .expect("SQLite apply");
    AsyncProjectionStore::apply_live(turso, vec![position], vec![command])
        .await
        .expect("Turso apply");
}

fn push_item(item_id: ItemId, key: &str, max_attempts: u32) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id,
        priority: None,
        not_before: None,
        group_key: None,
        max_attempts,
        payload: None,
        fields: Default::default(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity_document: None,
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

    // An unsupported arm in an already-applied overlap is skipped before capability validation.
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(
            QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            1,
            0,
        )],
        vec![envelope(
            "overlap-unsupported",
            QueueCommand::ResumeQueue,
            Vec::new(),
            12,
        )],
    )
    .await
    .expect("unsupported overlap is idempotently skipped");

    // Unsupported live-frontier arms fail before mutation or cursor advancement.
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

    let gap = envelope(
        "gap-claim",
        QueueCommand::Claim(ClaimCommand {
            item_ids: Vec::new(),
            lease_token: LeaseToken::new("gap-token").unwrap(),
            lease_expires_at: timestamp(200),
            worker_id: None,
        }),
        Vec::new(),
        13,
    );
    assert!(matches!(
        AsyncProjectionStore::apply_recovery(
            &turso,
            vec![CommandPosition::new(
                QueueKey::new(
                    TenantId::new("tenant").unwrap(),
                    QueueId::new("queue").unwrap(),
                ),
                1,
                3,
            )],
            vec![gap],
        )
        .await,
        Err(pqueue_engine::EngineError::Storage(_))
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

    // Even an empty supported command advances both sequence and assignment epoch.
    let empty_claim = envelope(
        "empty-claim",
        QueueCommand::Claim(ClaimCommand {
            item_ids: Vec::new(),
            lease_token: LeaseToken::new("empty-token").unwrap(),
            lease_expires_at: timestamp(201),
            worker_id: None,
        }),
        Vec::new(),
        14,
    );
    let empty_position = CommandPosition::new(
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        ),
        7,
        2,
    );
    AsyncProjectionStore::apply_live(&turso, vec![empty_position.clone()], vec![empty_claim])
        .await
        .expect("empty claim");
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(
            &turso,
            QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
        )
        .await
        .unwrap(),
        Some(empty_position)
    );
}

#[tokio::test]
async fn lease_lifecycle_matches_sqlite_and_failed_batch_rolls_back_cursor_and_rows() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let mut sqlite = SqliteProjectionStore::in_memory().expect("sqlite");
    ProjectionStore::ensure_shard(&mut sqlite, &definition).expect("sqlite ensure");
    let turso = TursoRelational::in_memory().await.expect("Turso");
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .expect("Turso ensure");

    let item = ItemId::mint(2, 0, 0);
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        0,
        envelope(
            "push-lifecycle",
            QueueCommand::Push(PushCommand {
                items: vec![push_item(item, "lease-item", 3)],
            }),
            vec![item],
            10,
        ),
    )
    .await;
    turso
        .execute(
            "INSERT INTO pqueue_request_idempotency \
             (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
              command_positions,expires_at,created_at) VALUES(?1,?2,'claim_by_query',?3,?4,?5,?6,?7,?8)",
            vec![
                "tenant".into(),
                "queue".into(),
                "renewed-request".into(),
                vec![1_u8].into(),
                serde_json::json!({
                    "item_ids": [item],
                    "lease_token": "lease-original",
                    "worker_id": "worker-a",
                })
                .to_string()
                .into(),
                "[]".into(),
                15_000_000_000_i64.into(),
                10_000_000_000_i64.into(),
            ],
        )
        .await
        .expect("seed claim-by-query replay");
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        1,
        envelope(
            "claim-lifecycle",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item],
                lease_token: LeaseToken::new("lease-original").unwrap(),
                lease_expires_at: timestamp(20),
                worker_id: Some(WorkerId::new("worker-a").unwrap()),
            }),
            vec![item],
            11,
        ),
    )
    .await;
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        2,
        envelope(
            "renew-lifecycle",
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![item],
                lease_expires_at: timestamp(30),
            }),
            vec![item],
            12,
        ),
    )
    .await;
    let renewed = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![item])
        .await
        .unwrap();
    assert_eq!(renewed[0].lease_expires_at, timestamp(30));
    assert_eq!(renewed[0].item_version, 3);
    let replay = turso
        .query(
            "SELECT expires_at FROM pqueue_request_idempotency WHERE request_id=?1",
            vec!["renewed-request".into()],
        )
        .await
        .expect("renewed replay");
    assert_eq!(replay[0].values, vec![Value::Integer(30_000_000_000)]);

    for (sequence, command) in [
        (
            3,
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![item],
            }),
        ),
        (
            4,
            QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                item_ids: vec![item],
            }),
        ),
    ] {
        apply_both(
            &mut sqlite,
            &turso,
            &shard,
            sequence,
            envelope("fence-cycle", command, vec![item], 13),
        )
        .await;
    }
    assert_eq!(
        AsyncProjectionStore::item_version(&turso, shard.clone(), item)
            .await
            .unwrap(),
        Some(3),
        "operator fencing does not bump the item version"
    );

    let reassigned_token = LeaseToken::new("lease-reassigned").unwrap();
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        5,
        envelope(
            "reassign-lifecycle",
            QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: vec![item],
                lease_token: reassigned_token.clone(),
                lease_expires_at: timestamp(40),
            }),
            vec![item],
            14,
        ),
    )
    .await;
    let reassigned = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![item])
        .await
        .unwrap();
    assert_eq!(reassigned[0].lease_token, Some(reassigned_token));
    assert_eq!(reassigned[0].lease_expires_at, timestamp(40));
    assert_eq!(reassigned[0].attempt_count, 2);
    assert_eq!(reassigned[0].item_version, 4);

    // A later Retry references no row, so the preceding renewal and both cursor advances roll back.
    let missing = ItemId::mint(2, 0, 99);
    let renew = envelope(
        "rollback-renew",
        QueueCommand::RenewLease(RenewLeaseCommand {
            item_ids: vec![item],
            lease_expires_at: timestamp(90),
        }),
        vec![item],
        15,
    );
    let bad_finalize = envelope(
        "rollback-finalize",
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome::new(missing, FinalizeKind::Retry)],
        }),
        vec![missing],
        15,
    );
    let rollback_positions = vec![
        CommandPosition::new(shard.clone(), 1, 6),
        CommandPosition::new(shard.clone(), 1, 7),
    ];
    let rollback_commands = vec![renew, bad_finalize];
    let sqlite_failed =
        ProjectionStore::apply(&mut sqlite, &rollback_positions, &rollback_commands);
    assert!(matches!(
        sqlite_failed,
        Err(pqueue_engine::EngineError::NotFound)
    ));
    let failed =
        AsyncProjectionStore::apply_live(&turso, rollback_positions, rollback_commands).await;
    assert!(matches!(failed, Err(pqueue_engine::EngineError::NotFound)));
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap()
            .unwrap()
            .sequence,
        5
    );
    let after_rollback = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![item])
        .await
        .unwrap();
    assert_eq!(after_rollback[0].lease_expires_at, timestamp(40));
    assert_eq!(after_rollback[0].item_version, 4);
    assert_eq!(
        AsyncProjectionStore::item_version(&turso, shard.clone(), item)
            .await
            .unwrap(),
        ProjectionStore::item_version(&sqlite, &shard, &item).unwrap()
    );

    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        6,
        envelope(
            "expire-lifecycle",
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![item],
            }),
            vec![item],
            16,
        ),
    )
    .await;
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), item)
            .await
            .unwrap(),
        ProjectionStore::item_state(&sqlite, &shard, &item).unwrap()
    );
    assert_eq!(
        AsyncProjectionStore::item_version(&turso, shard.clone(), item)
            .await
            .unwrap(),
        ProjectionStore::item_version(&sqlite, &shard, &item).unwrap()
    );
    assert!(
        AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![item])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(16), 10)
            .await
            .unwrap(),
        ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(16), 10).unwrap()
    );
}

#[tokio::test]
async fn finalize_dispositions_match_sqlite_for_terminal_retry_release_and_rearm() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let mut sqlite = SqliteProjectionStore::in_memory().expect("sqlite");
    ProjectionStore::ensure_shard(&mut sqlite, &definition).expect("sqlite ensure");
    let turso = TursoRelational::in_memory().await.expect("Turso");
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .expect("Turso ensure");

    let ids: Vec<ItemId> = (0..6).map(|counter| ItemId::mint(3, 0, counter)).collect();
    let items = ids
        .iter()
        .enumerate()
        .map(|(index, item)| {
            push_item(
                *item,
                &format!("finalize-{index}"),
                if index == 2 { 1 } else { 3 },
            )
        })
        .collect();
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        0,
        envelope(
            "push-finalize",
            QueueCommand::Push(PushCommand { items }),
            ids.clone(),
            10,
        ),
    )
    .await;
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        1,
        envelope(
            "claim-finalize",
            QueueCommand::Claim(ClaimCommand {
                item_ids: ids.clone(),
                lease_token: LeaseToken::new("lease-finalize").unwrap(),
                lease_expires_at: timestamp(30),
                worker_id: None,
            }),
            ids.clone(),
            11,
        ),
    )
    .await;

    let mut retry = FinalizeOutcome::new(ids[3], FinalizeKind::Retry);
    retry.not_before = Some(timestamp(100));
    let outcomes = vec![
        FinalizeOutcome::new(ids[0], FinalizeKind::Complete),
        FinalizeOutcome::new(ids[1], FinalizeKind::Fail),
        FinalizeOutcome::new(ids[2], FinalizeKind::Retry),
        retry,
        FinalizeOutcome::new(ids[4], FinalizeKind::Release),
        FinalizeOutcome::new(ids[5], FinalizeKind::Rearm),
    ];
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        2,
        envelope(
            "finalize-all",
            QueueCommand::Finalize(FinalizeCommand { outcomes }),
            ids.clone(),
            12,
        ),
    )
    .await;

    for item in &ids {
        assert_eq!(
            AsyncProjectionStore::item_state(&turso, shard.clone(), *item)
                .await
                .unwrap(),
            ProjectionStore::item_state(&sqlite, &shard, item).unwrap()
        );
        assert_eq!(
            AsyncProjectionStore::item_version(&turso, shard.clone(), *item)
                .await
                .unwrap(),
            ProjectionStore::item_version(&sqlite, &shard, item).unwrap()
        );
    }
    assert!(
        AsyncProjectionStore::render_claimed(&turso, shard.clone(), ids.clone())
            .await
            .unwrap()
            .is_empty()
    );
    for now in [12, 99, 100] {
        assert_eq!(
            AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(now), 10)
                .await
                .unwrap(),
            ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(now), 10).unwrap()
        );
    }

    // Release preserves the delivery count while Rearm resets it before the next claim.
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        3,
        envelope(
            "claim-release-rearm",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![ids[4], ids[5]],
                lease_token: LeaseToken::new("lease-second-delivery").unwrap(),
                lease_expires_at: timestamp(40),
                worker_id: None,
            }),
            vec![ids[4], ids[5]],
            13,
        ),
    )
    .await;
    let claimed = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![ids[4], ids[5]])
        .await
        .unwrap();
    let sqlite_claimed = ProjectionStore::render_claimed(&sqlite, &shard, &[ids[4], ids[5]])
        .expect("SQLite claimed");
    assert_eq!(
        claimed
            .iter()
            .map(|item| (item.item_id, item.attempt_count))
            .collect::<Vec<_>>(),
        sqlite_claimed
            .iter()
            .map(|item| (item.item_id, item.attempt_count))
            .collect::<Vec<_>>()
    );
    assert_eq!(claimed[0].attempt_count, 2);
    assert_eq!(claimed[1].attempt_count, 1);
}

#[tokio::test]
async fn lifecycle_state_frontier_and_eligibility_survive_reopen() {
    let dir = tempdir().expect("tempdir");
    let config = TursoConfig::local(dir.path().join("lifecycle.db"));
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::open(config.clone()).await.expect("open");
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .expect("ensure");
    let item = ItemId::mint(4, 0, 0);
    let push = envelope(
        "reopen-push",
        QueueCommand::Push(PushCommand {
            items: vec![push_item(item, "reopen-item", 3)],
        }),
        vec![item],
        10,
    );
    let claim = envelope(
        "reopen-claim",
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![item],
            lease_token: LeaseToken::new("reopen-token").unwrap(),
            lease_expires_at: timestamp(20),
            worker_id: None,
        }),
        vec![item],
        11,
    );
    let expire = envelope(
        "reopen-expire",
        QueueCommand::LeaseExpired(LeaseExpiredCommand {
            item_ids: vec![item],
        }),
        vec![item],
        12,
    );
    AsyncProjectionStore::apply_live(
        &turso,
        vec![
            CommandPosition::new(shard.clone(), 2, 0),
            CommandPosition::new(shard.clone(), 2, 1),
            CommandPosition::new(shard.clone(), 2, 2),
        ],
        vec![push, claim, expire],
    )
    .await
    .expect("apply lifecycle");
    drop(turso);

    let reopened = TursoRelational::open(config).await.expect("reopen");
    assert_eq!(
        AsyncProjectionStore::item_state(&reopened, shard.clone(), item)
            .await
            .unwrap(),
        Some(pqueue_core::ItemState::Pending)
    );
    assert_eq!(
        AsyncProjectionStore::item_version(&reopened, shard.clone(), item)
            .await
            .unwrap(),
        Some(3)
    );
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(&reopened, shard.clone(), timestamp(12), 10)
            .await
            .unwrap(),
        vec![item]
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&reopened, shard)
            .await
            .unwrap()
            .unwrap(),
        CommandPosition::new(
            QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            2,
            2,
        )
    );
}

#[tokio::test]
async fn cleartext_lease_tokens_are_scoped_by_queue_for_identical_item_ids() {
    let first_definition = definition();
    let mut second_definition = first_definition.clone();
    second_definition.queue_id = QueueId::new("other-queue").unwrap();
    let first_shard = QueueKey::new(
        first_definition.tenant_id.clone(),
        first_definition.queue_id.clone(),
    );
    let second_shard = QueueKey::new(
        second_definition.tenant_id.clone(),
        second_definition.queue_id.clone(),
    );
    let turso = TursoRelational::in_memory().await.expect("Turso");
    AsyncProjectionStore::ensure_shard(&turso, first_definition)
        .await
        .expect("first ensure");
    AsyncProjectionStore::ensure_shard(&turso, second_definition)
        .await
        .expect("second ensure");

    let shared_id = ItemId::mint(5, 0, 0);
    AsyncProjectionStore::apply_live(
        &turso,
        vec![
            CommandPosition::new(first_shard.clone(), 1, 0),
            CommandPosition::new(second_shard.clone(), 1, 0),
        ],
        vec![
            envelope(
                "first-push",
                QueueCommand::Push(PushCommand {
                    items: vec![push_item(shared_id, "first-shard-item", 3)],
                }),
                vec![shared_id],
                10,
            ),
            envelope(
                "second-push",
                QueueCommand::Push(PushCommand {
                    items: vec![push_item(shared_id, "second-shard-item", 3)],
                }),
                vec![shared_id],
                10,
            ),
        ],
    )
    .await
    .expect("push both shards");
    let first_token = LeaseToken::new("first-shard-token").unwrap();
    let second_token = LeaseToken::new("second-shard-token").unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![
            CommandPosition::new(first_shard.clone(), 1, 1),
            CommandPosition::new(second_shard.clone(), 1, 1),
        ],
        vec![
            envelope(
                "first-claim",
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![shared_id],
                    lease_token: first_token.clone(),
                    lease_expires_at: timestamp(20),
                    worker_id: None,
                }),
                vec![shared_id],
                11,
            ),
            envelope(
                "second-claim",
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![shared_id],
                    lease_token: second_token.clone(),
                    lease_expires_at: timestamp(20),
                    worker_id: None,
                }),
                vec![shared_id],
                11,
            ),
        ],
    )
    .await
    .expect("claim both shards");

    let first = AsyncProjectionStore::render_claimed(&turso, first_shard, vec![shared_id])
        .await
        .unwrap();
    let second = AsyncProjectionStore::render_claimed(&turso, second_shard, vec![shared_id])
        .await
        .unwrap();
    assert_eq!(first[0].lease_token, Some(first_token));
    assert_eq!(second[0].lease_token, Some(second_token));
}

#[tokio::test]
async fn ensure_shard_rejects_missing_or_negative_cursor_state() {
    let definition = definition();
    let store = TursoRelational::in_memory().await.expect("Turso");
    AsyncProjectionStore::ensure_shard(&store, definition.clone())
        .await
        .expect("ensure");
    store
        .execute(
            "DELETE FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            vec!["tenant".into(), "queue".into()],
        )
        .await
        .expect("delete cursor");
    assert!(matches!(
        AsyncProjectionStore::ensure_shard(&store, definition.clone()).await,
        Err(pqueue_engine::EngineError::Storage(_))
    ));

    store
        .execute(
            "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq,assignment_epoch) \
             VALUES(?1,?2,-1,0,0)",
            vec!["tenant".into(), "queue".into()],
        )
        .await
        .expect("insert corrupt cursor");
    assert!(matches!(
        AsyncProjectionStore::ensure_shard(&store, definition).await,
        Err(pqueue_engine::EngineError::Storage(_))
    ));
}

#[tokio::test]
async fn active_lease_reopen_is_explicitly_hash_only_until_response_recovery_exists() {
    let dir = tempdir().expect("tempdir");
    let config = TursoConfig::local(dir.path().join("active-lease.db"));
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::open(config.clone()).await.expect("open");
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .expect("ensure");
    let item = ItemId::mint(6, 0, 0);
    AsyncProjectionStore::apply_live(
        &store,
        vec![
            CommandPosition::new(shard.clone(), 1, 0),
            CommandPosition::new(shard.clone(), 1, 1),
        ],
        vec![
            envelope(
                "active-reopen-push",
                QueueCommand::Push(PushCommand {
                    items: vec![push_item(item, "active-reopen-item", 3)],
                }),
                vec![item],
                10,
            ),
            envelope(
                "active-reopen-claim",
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![item],
                    lease_token: LeaseToken::new("active-reopen-token").unwrap(),
                    lease_expires_at: timestamp(30),
                    worker_id: None,
                }),
                vec![item],
                11,
            ),
        ],
    )
    .await
    .expect("claim");
    drop(store);

    let reopened = TursoRelational::open(config).await.expect("reopen");
    assert_eq!(
        AsyncProjectionStore::item_state(&reopened, shard.clone(), item)
            .await
            .unwrap(),
        Some(pqueue_core::ItemState::Leased),
        "durable lease state and token hash survive"
    );
    assert!(
        AsyncProjectionStore::render_claimed(&reopened, shard, vec![item])
            .await
            .unwrap()
            .is_empty(),
        "cleartext token recovery is intentionally not claimed by this adapter slice"
    );
}
