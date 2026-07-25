use std::time::Duration;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, ClientItemKey, CohortId, CohortPolicy, EligibilityPolicy, GroupKey, IndexDeclaration,
    IndexDef, IndexType, ItemId, LeaseToken, Metadata, MetadataValue, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueDefinition, QueueId, QueueIndex, RecurrencePolicy, RequestId, RetryPolicy, TenantId,
    UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    AsyncProjectionStore, ClaimCommand, ClaimCompatibility, ClaimUnit, CohortClaimCommand,
    CohortExpiredCommand, CohortFinalizeCommand, CohortRenewLeaseCommand, CommandChecksum,
    CommandEnvelope, CommandId, CommandPosition, EngineError, FenceLeaseCommand, FinalizeCommand,
    FinalizeKind, FinalizeOutcome, FinalizeTarget, GroupBatching, IdempotencyDecision,
    LeaseExpiredCommand, PauseQueueCommand, PayloadUpdate, ProjectionStore, PurgeItemsCommand,
    PushCommand, PushFingerprint, PushItem, QueueCommand, QueueKey, ReassignLeaseCommand,
    RenewLeaseCommand, ReplacePendingCommand, RequestOutcome, ScheduleUpdate, SetGatesCommand,
    UnfenceLeaseCommand, UpdateFieldsCommand, WriteSideRecordsCommand,
};

fn indexed_item(item_id: ItemId, key: &str, email: &str) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id,
        priority: None,
        not_before: None,
        group_key: None,
        max_attempts: 3,
        payload: None,
        fields: Default::default(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity_document: Some(serde_json::json!({ "email": email })),
    }
}

#[tokio::test]
async fn push_preappend_and_durable_idempotency_are_native_async() {
    let mut def = definition();
    def.typed_indexes = vec![QueueIndex {
        name: "by_email".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "email".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, def)
        .await
        .unwrap();
    let id = ItemId::mint(1, 1, 1);
    let item = indexed_item(id, "key-one", "one@example.com");
    AsyncProjectionStore::validate_push(&turso, shard.clone(), vec![item.clone()], timestamp(1))
        .await
        .unwrap();
    let request_id = RequestId::new("push-request").unwrap();
    let mut push = envelope(
        "push-idempotent",
        QueueCommand::Push(PushCommand { items: vec![item] }),
        vec![id],
        1,
    );
    push.request_id = Some(request_id.clone());
    push.request_fingerprint = Some(42);
    push.request_outcome = Some(RequestOutcome::Push { item_ids: vec![id] });
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 0)],
        vec![push],
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::push_idempotency(
            &turso,
            shard.clone(),
            request_id.clone(),
            PushFingerprint {
                canonical_sha256: fireweed_engine::push_items_fingerprint_sha256(&[indexed_item(
                    id,
                    "key-one",
                    "one@example.com",
                )])
                .unwrap(),
                legacy_body_hash: BodyHash(42),
            },
            timestamp(2),
        )
        .await
        .unwrap(),
        IdempotencyDecision::Replay(vec![id])
    );
    assert_eq!(
        AsyncProjectionStore::push_idempotency(
            &turso,
            shard.clone(),
            request_id,
            PushFingerprint {
                canonical_sha256: [43; 32],
                legacy_body_hash: BodyHash(43),
            },
            timestamp(2),
        )
        .await
        .unwrap(),
        IdempotencyDecision::Conflict
    );
    let conflicting = indexed_item(ItemId::mint(1, 1, 2), "key-two", "one@example.com");
    assert!(matches!(
        AsyncProjectionStore::validate_push(&turso, shard.clone(), vec![conflicting], timestamp(2))
            .await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    let unkeyed_id = ItemId::mint(1, 1, 3);
    let unkeyed = indexed_item(unkeyed_id, "key-three", "three@example.com");
    AsyncProjectionStore::validate_push(&turso, shard.clone(), vec![unkeyed.clone()], timestamp(2))
        .await
        .unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 1)],
        vec![envelope(
            "push-without-request-id",
            QueueCommand::Push(PushCommand {
                items: vec![unkeyed],
            }),
            vec![unkeyed_id],
            2,
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), unkeyed_id)
            .await
            .unwrap(),
        Some(fireweed_core::ItemState::Pending)
    );
    let pause = envelope(
        "pause",
        QueueCommand::PauseQueue(PauseQueueCommand { drain_intake: true }),
        Vec::new(),
        3,
    );
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 2)],
        vec![pause],
    )
    .await
    .unwrap();
    assert!(
        AsyncProjectionStore::pause_blocks_intake(&turso, shard.clone())
            .await
            .unwrap()
    );
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 3)],
        vec![envelope("resume", QueueCommand::ResumeQueue, Vec::new(), 4)],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 4)],
        vec![envelope(
            "pause-without-drain",
            QueueCommand::PauseQueue(PauseQueueCommand {
                drain_intake: false,
            }),
            Vec::new(),
            5,
        )],
    )
    .await
    .unwrap();
    assert!(
        !AsyncProjectionStore::pause_blocks_intake(&turso, shard.clone())
            .await
            .unwrap()
    );

    let historical_id = ItemId::mint(1, 1, 4);
    let historical = envelope(
        "historical-push",
        QueueCommand::Push(PushCommand {
            items: vec![indexed_item(
                historical_id,
                "historical-key",
                "historical@example.com",
            )],
        }),
        vec![historical_id],
        6,
    );
    let historical_position = CommandPosition::new(shard.clone(), 0, 5);
    assert!(matches!(
        AsyncProjectionStore::apply_live(
            &turso,
            vec![historical_position.clone()],
            vec![historical.clone()],
        )
        .await,
        Err(fireweed_engine::EngineError::EpochFenced)
    ));
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), historical_id)
            .await
            .unwrap(),
        None
    );

    AsyncProjectionStore::apply_recovery(&turso, vec![historical_position], vec![historical])
        .await
        .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), historical_id)
            .await
            .unwrap(),
        Some(fireweed_core::ItemState::Pending)
    );

    let rolled_back_id = ItemId::mint(1, 7, 5);
    let descending_positions = vec![
        CommandPosition::new(shard.clone(), 7, 6),
        CommandPosition::new(shard.clone(), 6, 7),
    ];
    let descending_commands = vec![
        envelope(
            "descending-epoch-push",
            QueueCommand::Push(PushCommand {
                items: vec![indexed_item(
                    rolled_back_id,
                    "descending-key",
                    "descending@example.com",
                )],
            }),
            vec![rolled_back_id],
            7,
        ),
        envelope(
            "descending-epoch-pause",
            QueueCommand::PauseQueue(PauseQueueCommand { drain_intake: true }),
            Vec::new(),
            8,
        ),
    ];
    assert!(matches!(
        AsyncProjectionStore::apply_live(&turso, descending_positions, descending_commands).await,
        Err(fireweed_engine::EngineError::EpochFenced)
    ));
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), rolled_back_id)
            .await
            .unwrap(),
        None
    );
    assert!(
        !AsyncProjectionStore::pause_blocks_intake(&turso, shard)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn accepted_push_full_chunk_materializes_items_gates_and_indexes() {
    let mut definition = definition();
    definition.max_push_batch_size = 100;
    definition.typed_indexes = vec![QueueIndex {
        name: "by_email".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "email".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let items: Vec<_> = (0..47)
        .map(|offset| {
            let id = ItemId::mint(1, 0, offset);
            let mut item = indexed_item(
                id,
                &format!("batch-key-{offset}"),
                &format!("batch-{offset}@example.com"),
            );
            item.gate_keys = vec![format!("gate-{offset}")];
            item
        })
        .collect();
    let ids = items.iter().map(|item| item.item_id).collect::<Vec<_>>();
    apply_turso(
        &turso,
        &shard,
        0,
        envelope(
            "full-chunk-push",
            QueueCommand::Push(PushCommand { items }),
            ids,
            1,
        ),
    )
    .await
    .unwrap();

    for (table, expected) in [
        ("fireweed_items", 47),
        ("fireweed_item_gates", 47),
        ("fireweed_item_index", 47),
    ] {
        assert_eq!(
            turso
                .query(format!("SELECT COUNT(*) FROM {table}"), vec![])
                .await
                .unwrap()[0]
                .values,
            vec![Value::Integer(expected)]
        );
    }
    assert_eq!(
        turso
            .query(
                "SELECT MIN(created_seq),MAX(created_seq) FROM fireweed_items",
                vec![],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(0), Value::Integer(46)]
    );
}

#[tokio::test]
async fn group_cap_counts_existing_and_incoming_cohort_members() {
    let mut def = definition();
    def.max_eligible_group_size = Some(2);
    def.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(60_000),
        on_incomplete: None,
        max_cohort_size: Some(3),
    });
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, def)
        .await
        .unwrap();
    let group = GroupKey::new("capped-cohort").unwrap();
    let first = ItemId::mint(2, 1, 1);
    let second = ItemId::mint(2, 1, 2);
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 0)],
        vec![envelope(
            "capped-cohort-initial",
            QueueCommand::Push(PushCommand {
                items: vec![
                    cohort_item(first, "capped-one", &group, "one@cap.test"),
                    cohort_item(second, "capped-two", &group, "two@cap.test"),
                ],
            }),
            vec![first, second],
            1,
        )],
    )
    .await
    .unwrap();

    let third = cohort_item(
        ItemId::mint(2, 1, 3),
        "capped-three",
        &group,
        "three@cap.test",
    );
    let result =
        AsyncProjectionStore::validate_push(&turso, shard, vec![third], timestamp(2)).await;
    assert!(
        matches!(result, Err(fireweed_engine::EngineError::Conflict)),
        "unexpected group-cap validation result: {result:?}"
    );
}
use fireweed_relational::OWNED_PROJECTION_TABLES;
use fireweed_sqlite::{AsyncSqliteProjectionStore, SqliteProjectionStore, SqliteRelational};
use fireweed_turso::{
    JournalMode, RelationalStatement, TursoConfig, TursoRelational, TursoRelationalError,
};
use tempfile::tempdir;
use turso::Value;

fn timestamp(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).expect("timestamp")
}

fn cohort_item(item_id: ItemId, key: &str, group: &GroupKey, email: &str) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id,
        priority: None,
        not_before: None,
        group_key: Some(group.clone()),
        max_attempts: 3,
        payload: None,
        fields: Default::default(),
        metadata: Metadata::default(),
        cohort_size: Some(2),
        gate_keys: vec!["capacity".to_string()],
        entity_document: Some(serde_json::json!({ "email": email })),
    }
}

#[tokio::test]
async fn grouped_typed_cohort_lifecycle_is_atomic_and_refreshes_summary() {
    let mut definition = definition();
    definition.typed_indexes = vec![QueueIndex {
        name: "by_email".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "email".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let group = GroupKey::new("cohort-a").unwrap();
    let first = ItemId::mint(20, 0, 0);
    let second = ItemId::mint(20, 0, 1);
    let push = envelope(
        "cohort-push",
        QueueCommand::Push(PushCommand {
            items: vec![
                cohort_item(first, "first", &group, "a@example.com"),
                cohort_item(second, "second", &group, "b@example.com"),
            ],
        }),
        vec![first, second],
        10,
    );
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 2, 0)],
        vec![push],
    )
    .await
    .unwrap();

    let cohort_id = CohortId::new("coh:cohort-a:10000000000").unwrap();
    let cohort = turso
        .query(
            "SELECT cohort_id,cohort_size,member_count,state FROM fireweed_cohorts",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(
        cohort[0].values,
        vec![
            Value::Text(cohort_id.as_str().to_string()),
            Value::Integer(2),
            Value::Integer(2),
            Value::Text("complete".to_string()),
        ]
    );
    assert_eq!(
        turso
            .query("SELECT COUNT(*) FROM fireweed_item_index", vec![])
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(2)]
    );
    assert_eq!(
        turso
            .query(
                "SELECT eligible_item_count FROM fireweed_group_summary WHERE group_key=?1",
                vec![Value::Text(group.as_str().to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(2)]
    );

    let invalid_claim = envelope(
        "bad-cohort-claim",
        QueueCommand::CohortClaim(CohortClaimCommand {
            cohort_id: cohort_id.clone(),
            item_ids: vec![first],
            lease_token: LeaseToken::new("bad-token").unwrap(),
            lease_expires_at: timestamp(30),
        }),
        vec![first],
        11,
    );
    assert!(matches!(
        AsyncProjectionStore::apply_live(
            &turso,
            vec![CommandPosition::new(shard.clone(), 2, 1)],
            vec![invalid_claim],
        )
        .await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap()
            .unwrap()
            .sequence,
        0
    );

    let lease = LeaseToken::new("cohort-token").unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 2, 1)],
        vec![envelope(
            "cohort-claim",
            QueueCommand::CohortClaim(CohortClaimCommand {
                cohort_id: cohort_id.clone(),
                item_ids: vec![first, second],
                lease_token: lease,
                lease_expires_at: timestamp(30),
            }),
            vec![first, second],
            11,
        )],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 2, 2)],
        vec![envelope(
            "cohort-renew",
            QueueCommand::CohortRenewLease(CohortRenewLeaseCommand {
                cohort_id: cohort_id.clone(),
                lease_expires_at: timestamp(40),
            }),
            vec![first, second],
            12,
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        turso
            .query(
                "SELECT DISTINCT lease_expires_at FROM fireweed_items",
                vec![],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(40_000_000_000)]
    );
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 2, 3)],
        vec![envelope(
            "cohort-finalize",
            QueueCommand::CohortFinalize(CohortFinalizeCommand {
                cohort_id,
                kind: FinalizeKind::Complete,
                not_before: None,
            }),
            vec![first, second],
            13,
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        turso
            .query("SELECT state,retention_until FROM fireweed_cohorts", vec![],)
            .await
            .unwrap()[0]
            .values,
        vec![
            Value::Text("terminal".to_string()),
            Value::Integer(73_000_000_000),
        ]
    );
    assert_eq!(
        turso
            .query(
                "SELECT eligible_item_count FROM fireweed_group_summary",
                vec![],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(0)]
    );
}

#[tokio::test]
async fn grouped_push_unique_conflict_and_cohort_expiry_roll_back_or_converge() {
    let mut definition = definition();
    definition.typed_indexes = vec![QueueIndex {
        name: "by_email".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "email".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let group = GroupKey::new("cohort-b").unwrap();
    let first = ItemId::mint(21, 0, 0);
    let second = ItemId::mint(21, 0, 1);
    let unrelated = ItemId::mint(21, 0, 2);
    let conflicting = envelope(
        "conflicting-push",
        QueueCommand::Push(PushCommand {
            items: vec![
                cohort_item(first, "first", &group, "same@example.com"),
                cohort_item(second, "second", &group, "same@example.com"),
            ],
        }),
        vec![first, second],
        20,
    );
    assert!(matches!(
        AsyncProjectionStore::apply_recovery(
            &turso,
            vec![CommandPosition::new(shard.clone(), 3, 0)],
            vec![conflicting],
        )
        .await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    assert_eq!(
        turso
            .query("SELECT COUNT(*) FROM fireweed_items", vec![])
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(0)]
    );
    let valid = envelope(
        "valid-push",
        QueueCommand::Push(PushCommand {
            items: vec![
                cohort_item(first, "first", &group, "a@example.com"),
                cohort_item(second, "second", &group, "b@example.com"),
                PushItem {
                    group_key: Some(group.clone()),
                    entity_document: Some(serde_json::json!({ "email": "other@example.com" })),
                    ..push_item(unrelated, "unrelated", 3)
                },
            ],
        }),
        vec![first, second, unrelated],
        20,
    );
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 3, 0)],
        vec![valid],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 3, 1)],
        vec![envelope(
            "cohort-expired",
            QueueCommand::CohortExpired(CohortExpiredCommand {
                group_key: group.clone(),
            }),
            vec![first, second],
            21,
        )],
    )
    .await
    .unwrap();
    let states = turso
        .query(
            "SELECT item_id,lifecycle_state FROM fireweed_items ORDER BY item_id",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(states[0].values[1], Value::Text("Failed".to_string()));
    assert_eq!(states[1].values[1], Value::Text("Failed".to_string()));
    assert_eq!(states[2].values[1], Value::Text("Pending".to_string()));
    assert_eq!(
        turso
            .query(
                "SELECT eligible_item_count FROM fireweed_group_summary WHERE group_key=?1",
                vec![Value::Text(group.as_str().to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(1)]
    );
    assert_eq!(
        turso
            .query(
                "SELECT state,expire_command_pos,retention_until FROM fireweed_cohorts",
                vec![],
            )
            .await
            .unwrap()[0]
            .values,
        vec![
            Value::Text("terminal".to_string()),
            Value::Integer(1),
            Value::Integer(81_000_000_000),
        ]
    );
}

async fn apply_turso(
    turso: &TursoRelational,
    shard: &QueueKey,
    sequence: u64,
    command: CommandEnvelope,
) -> Result<(), fireweed_engine::EngineError> {
    AsyncProjectionStore::apply_live(
        turso,
        vec![CommandPosition::new(shard.clone(), 4, sequence)],
        vec![command],
    )
    .await
}

async fn group_summary_count(turso: &TursoRelational, group: &GroupKey) -> i64 {
    match &turso
        .query(
            "SELECT eligible_item_count FROM fireweed_group_summary WHERE group_key=?1",
            vec![Value::Text(group.as_str().to_string())],
        )
        .await
        .unwrap()[0]
        .values[0]
    {
        Value::Integer(count) => *count,
        value => panic!("unexpected summary count: {value:?}"),
    }
}

#[tokio::test]
async fn noncohort_group_summary_tracks_ordinary_item_lifecycle() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let group = GroupKey::new("ordinary-group").unwrap();
    let first = ItemId::mint(22, 0, 0);
    let second = ItemId::mint(22, 0, 1);
    let grouped = |item, key| PushItem {
        group_key: Some(group.clone()),
        ..push_item(item, key, 3)
    };
    apply_turso(
        &turso,
        &shard,
        0,
        envelope(
            "grouped-push",
            QueueCommand::Push(PushCommand {
                items: vec![grouped(first, "first"), grouped(second, "second")],
            }),
            vec![first, second],
            30,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 2);

    let token = LeaseToken::new("ordinary-token").unwrap();
    apply_turso(
        &turso,
        &shard,
        1,
        envelope(
            "ordinary-claim",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![first],
                lease_token: token.clone(),
                lease_expires_at: timestamp(60),
                worker_id: None,
            }),
            vec![first],
            31,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 1);
    assert_eq!(
        AsyncProjectionStore::purge_validate(&turso, shard.clone(), vec![first], false).await,
        Err(fireweed_engine::EngineError::Conflict)
    );
    assert_eq!(
        AsyncProjectionStore::purge_validate(&turso, shard.clone(), vec![second, second], false)
            .await
            .unwrap(),
        vec![second]
    );
    AsyncProjectionStore::renew_validate(
        &turso,
        shard.clone(),
        vec![fireweed_engine::RenewTarget {
            item_id: first,
            lease_token: token.clone(),
        }],
        timestamp(32),
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::renew_validate(
            &turso,
            shard.clone(),
            vec![fireweed_engine::RenewTarget {
                item_id: first,
                lease_token: LeaseToken::new("wrong-token").unwrap(),
            }],
            timestamp(32),
        )
        .await,
        Err(fireweed_engine::EngineError::StaleLease)
    );
    AsyncProjectionStore::renew_validate(
        &turso,
        shard.clone(),
        vec![fireweed_engine::RenewTarget {
            item_id: first,
            lease_token: token.clone(),
        }],
        timestamp(60),
    )
    .await
    .unwrap();
    assert!(matches!(
        AsyncProjectionStore::renew_validate(
            &turso,
            shard.clone(),
            vec![fireweed_engine::RenewTarget {
                item_id: second,
                lease_token: token.clone(),
            }],
            timestamp(32),
        )
        .await,
        Err(fireweed_engine::EngineError::Invalid("item is not leased"))
    ));
    let version_before = AsyncProjectionStore::item_version(&turso, shard.clone(), first)
        .await
        .unwrap()
        .unwrap();
    apply_turso(
        &turso,
        &shard,
        2,
        envelope(
            "ordinary-renew",
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![first],
                lease_expires_at: timestamp(90),
            }),
            vec![first],
            32,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_version(&turso, shard.clone(), first)
            .await
            .unwrap(),
        Some(version_before + 1)
    );
    let renewed = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![first])
        .await
        .unwrap();
    assert_eq!(renewed[0].lease_expires_at, timestamp(90));
    apply_turso(
        &turso,
        &shard,
        3,
        envelope(
            "ordinary-release",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(first, FinalizeKind::Release)],
            }),
            vec![first],
            33,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 2);

    apply_turso(
        &turso,
        &shard,
        4,
        envelope(
            "ordinary-reclaim",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![first],
                lease_token: token.clone(),
                lease_expires_at: timestamp(60),
                worker_id: None,
            }),
            vec![first],
            34,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 1);
    apply_turso(
        &turso,
        &shard,
        5,
        envelope(
            "ordinary-expiry",
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![first],
            }),
            vec![first],
            35,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 2);

    apply_turso(
        &turso,
        &shard,
        6,
        envelope(
            "ordinary-final-claim",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![first],
                lease_token: token,
                lease_expires_at: timestamp(60),
                worker_id: None,
            }),
            vec![first],
            36,
        ),
    )
    .await
    .unwrap();
    apply_turso(
        &turso,
        &shard,
        7,
        envelope(
            "ordinary-complete",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(first, FinalizeKind::Complete)],
            }),
            vec![first],
            37,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 1);
    apply_turso(
        &turso,
        &shard,
        8,
        envelope(
            "ordinary-purge",
            QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![first],
                force: false,
            }),
            vec![first],
            38,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 1);

    apply_turso(
        &turso,
        &shard,
        9,
        envelope(
            "ordinary-pending-purge",
            QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![second],
                force: true,
            }),
            vec![second],
            39,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 0);
}

#[tokio::test]
async fn grouped_replace_is_rejected_before_projection_mutation() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let group = GroupKey::new("replace-group").unwrap();
    let original = ItemId::mint(23, 0, 0);
    let replacement = ItemId::mint(23, 0, 1);
    apply_turso(
        &turso,
        &shard,
        0,
        envelope(
            "replace-source",
            QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    group_key: Some(group.clone()),
                    ..push_item(original, "replace-key", 3)
                }],
            }),
            vec![original],
            40,
        ),
    )
    .await
    .unwrap();
    let result = apply_turso(
        &turso,
        &shard,
        1,
        envelope(
            "grouped-replace",
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("replace-key").unwrap(),
                superseded_item_id: original,
                replacement: push_item(replacement, "replace-key", 3),
            }),
            vec![replacement],
            41,
        ),
    )
    .await;
    assert!(matches!(
        result,
        Err(fireweed_engine::EngineError::Unavailable)
    ));
    assert_eq!(group_summary_count(&turso, &group).await, 1);
    assert_eq!(
        turso
            .query("SELECT item_id,superseded FROM fireweed_items", vec![],)
            .await
            .unwrap()[0]
            .values,
        vec![Value::Text(original.to_string()), Value::Integer(0)]
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard)
            .await
            .unwrap()
            .unwrap()
            .sequence,
        0
    );

    let ungrouped = ItemId::mint(23, 0, 2);
    let grouped_replacement = ItemId::mint(23, 0, 3);
    let shard = QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    );
    apply_turso(
        &turso,
        &shard,
        1,
        envelope(
            "ungrouped-replace-source",
            QueueCommand::Push(PushCommand {
                items: vec![push_item(ungrouped, "ungrouped-key", 3)],
            }),
            vec![ungrouped],
            42,
        ),
    )
    .await
    .unwrap();
    let result = apply_turso(
        &turso,
        &shard,
        2,
        envelope(
            "grouped-replacement-target",
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("ungrouped-key").unwrap(),
                superseded_item_id: ungrouped,
                replacement: PushItem {
                    group_key: Some(group),
                    ..push_item(grouped_replacement, "ungrouped-key", 3)
                },
            }),
            vec![ungrouped, grouped_replacement],
            43,
        ),
    )
    .await;
    assert!(matches!(
        result,
        Err(fireweed_engine::EngineError::Unavailable)
    ));
    assert_eq!(
        turso
            .query(
                "SELECT superseded FROM fireweed_items WHERE item_id=?1",
                vec![Value::Text(ungrouped.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(0)]
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard)
            .await
            .unwrap()
            .unwrap()
            .sequence,
        1
    );
}

#[tokio::test]
async fn typed_update_and_replace_preserve_unique_index_atomicity_and_replay() {
    let mut definition = definition();
    definition.typed_indexes = vec![QueueIndex {
        name: "by_email".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "email".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let first = ItemId::mint(24, 0, 0);
    let second = ItemId::mint(24, 0, 1);
    let replacement = ItemId::mint(24, 0, 2);
    let conflict_replacement = ItemId::mint(24, 0, 3);
    let typed_item = |item_id, key: &str, email: &str| PushItem {
        entity_document: Some(serde_json::json!({ "email": email })),
        ..push_item(item_id, key, 3)
    };
    apply_turso(
        &turso,
        &shard,
        0,
        envelope(
            "typed-mutation-push",
            QueueCommand::Push(PushCommand {
                items: vec![
                    typed_item(first, "first", "a@example.com"),
                    typed_item(second, "second", "b@example.com"),
                ],
            }),
            vec![first, second],
            50,
        ),
    )
    .await
    .unwrap();
    let before_item = turso
        .query(
            "SELECT fields,payload,entity_document FROM fireweed_items WHERE item_id=?1",
            vec![Value::Text(first.to_string())],
        )
        .await
        .unwrap()[0]
        .values
        .clone();
    let before_index = turso
        .query(
            "SELECT index_key FROM fireweed_item_index WHERE item_id=?1",
            vec![Value::Text(first.to_string())],
        )
        .await
        .unwrap()[0]
        .values
        .clone();

    let conflicting_update = envelope(
        "typed-update-conflict",
        QueueCommand::UpdateFields(UpdateFieldsCommand {
            item_id: first,
            field_ops: std::collections::BTreeMap::from([(
                "must_rollback".to_string(),
                Some(Bytes::from_static(b"yes")),
            )]),
            payload: PayloadUpdate::Set(Some(Bytes::from_static(b"must-rollback"))),
            set_priority: ScheduleUpdate::Keep,
            set_not_before: ScheduleUpdate::Keep,
            set_entity_document: Some(serde_json::json!({ "email": "b@example.com" })),
            set_fields: None,
            set_metadata: None,
            set_gate_keys: None,
            api001_batch: false,
        }),
        vec![first],
        51,
    );
    assert!(matches!(
        apply_turso(&turso, &shard, 1, conflicting_update).await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    assert_eq!(
        turso
            .query(
                "SELECT fields,payload,entity_document FROM fireweed_items WHERE item_id=?1",
                vec![Value::Text(first.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        before_item
    );
    assert_eq!(
        turso
            .query(
                "SELECT index_key FROM fireweed_item_index WHERE item_id=?1",
                vec![Value::Text(first.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        before_index
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap()
            .unwrap()
            .sequence,
        0
    );

    let successful_update = envelope(
        "typed-update-success",
        QueueCommand::UpdateFields(UpdateFieldsCommand {
            item_id: first,
            field_ops: std::collections::BTreeMap::from([(
                "changed".to_string(),
                Some(Bytes::from_static(b"yes")),
            )]),
            payload: PayloadUpdate::Set(Some(Bytes::from_static(b"changed"))),
            set_priority: ScheduleUpdate::Keep,
            set_not_before: ScheduleUpdate::Keep,
            set_entity_document: Some(serde_json::json!({ "email": "c@example.com" })),
            set_fields: None,
            set_metadata: None,
            set_gate_keys: None,
            api001_batch: false,
        }),
        vec![first],
        52,
    );
    apply_turso(&turso, &shard, 1, successful_update.clone())
        .await
        .unwrap();
    let changed = turso
        .query(
            "SELECT fields,payload,entity_document FROM fireweed_items WHERE item_id=?1",
            vec![Value::Text(first.to_string())],
        )
        .await
        .unwrap()[0]
        .values
        .clone();
    assert_ne!(changed, before_item);
    assert_eq!(
        changed[2],
        Value::Text("{\"email\":\"c@example.com\"}".to_string())
    );
    let changed_index = turso
        .query(
            "SELECT index_key FROM fireweed_item_index WHERE item_id=?1",
            vec![Value::Text(first.to_string())],
        )
        .await
        .unwrap()[0]
        .values
        .clone();
    assert_ne!(changed_index, before_index);
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 4, 1)],
        vec![successful_update],
    )
    .await
    .unwrap();

    let successful_replace = envelope(
        "typed-replace-success",
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("first").unwrap(),
            superseded_item_id: first,
            replacement: PushItem {
                gate_keys: vec!["replacement-gate".to_string()],
                ..typed_item(replacement, "first", "c@example.com")
            },
        }),
        vec![first, replacement],
        53,
    );
    apply_turso(&turso, &shard, 2, successful_replace.clone())
        .await
        .unwrap();
    assert_eq!(
        turso
            .query(
                "SELECT item_id FROM fireweed_item_index WHERE index_name='by_email' ORDER BY item_id",
                vec![],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text(second.to_string()),
            Value::Text(replacement.to_string())
        ]
    );
    assert_eq!(
        turso
            .query(
                "SELECT COUNT(*) FROM fireweed_item_gates WHERE item_id=?1 AND gate_key='replacement-gate'",
                vec![Value::Text(replacement.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(1)]
    );
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 4, 2)],
        vec![successful_replace],
    )
    .await
    .unwrap();

    let second_before = turso
        .query(
            "SELECT superseded,entity_document FROM fireweed_items WHERE item_id=?1",
            vec![Value::Text(second.to_string())],
        )
        .await
        .unwrap()[0]
        .values
        .clone();
    let second_index_before = turso
        .query(
            "SELECT index_key FROM fireweed_item_index WHERE item_id=?1",
            vec![Value::Text(second.to_string())],
        )
        .await
        .unwrap()[0]
        .values
        .clone();
    let conflicting_replace = envelope(
        "typed-replace-conflict",
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("second").unwrap(),
            superseded_item_id: second,
            replacement: typed_item(conflict_replacement, "second", "c@example.com"),
        }),
        vec![second, conflict_replacement],
        54,
    );
    assert!(matches!(
        apply_turso(&turso, &shard, 3, conflicting_replace).await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    assert_eq!(
        turso
            .query(
                "SELECT superseded,entity_document FROM fireweed_items WHERE item_id=?1",
                vec![Value::Text(second.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        second_before
    );
    assert_eq!(
        turso
            .query(
                "SELECT index_key FROM fireweed_item_index WHERE item_id=?1",
                vec![Value::Text(second.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        second_index_before
    );
    assert!(
        turso
            .query(
                "SELECT item_id FROM fireweed_items WHERE item_id=?1",
                vec![Value::Text(conflict_replacement.to_string())],
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard)
            .await
            .unwrap()
            .unwrap()
            .sequence,
        2
    );
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

fn rich_create_definition() -> QueueDefinition {
    let mut definition = definition();
    definition.priority_model = PriorityModel {
        kind: PriorityModelKind::Text,
        direction: PriorityDirection::Descending,
        tie_breaker: PriorityTieBreaker::ClientItemKey,
    };
    definition.ordering_mode = OrderingMode::BoundedRelaxed;
    definition.max_rank_error = 7;
    definition.progress_bound_ms = 12_345;
    definition.eligibility_policy.gate_keys = fireweed_core::GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(4);
    definition.eligibility_policy.max_gates_per_request = Some(9);
    definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(30_000),
        on_incomplete: Some(fireweed_core::CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(12),
    });
    definition.recurrence = fireweed_core::RecurrencePolicy {
        mode: fireweed_core::RecurrenceMode::Recurring,
        until: Some(timestamp(8_000)),
    };
    definition.request_id_retention_ms = 71_000;
    definition.client_item_key_retention_ms = 72_000;
    definition.terminal_retention_ms = 73_000;
    definition.max_lease_duration_ms = 74_000;
    definition.retry_policy = RetryPolicy { max_attempts: 11 };
    definition.max_push_batch_size = 17;
    definition.max_claim_batch_size = 13;
    definition.max_eligible_group_size = Some(8);
    definition.emit_change_records = true;
    definition
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

async fn apply_async_pair(
    sqlite: &AsyncSqliteProjectionStore,
    turso: &TursoRelational,
    shard: &QueueKey,
    sequence: u64,
    command: CommandEnvelope,
) {
    let position = CommandPosition::new(shard.clone(), 1, sequence);
    AsyncProjectionStore::apply_live(sqlite, vec![position.clone()], vec![command.clone()])
        .await
        .expect("async SQLite apply");
    AsyncProjectionStore::apply_live(turso, vec![position], vec![command])
        .await
        .expect("Turso apply");
}

async fn assert_rich_selection_matches(
    sqlite: &SqliteRelational,
    turso: &TursoRelational,
    shard: &QueueKey,
    unit: ClaimUnit,
    compatibility: ClaimCompatibility,
    now: UtcTimestamp,
    max_items: usize,
) -> fireweed_engine::RichClaimSelection {
    let expected =
        ProjectionStore::select_rich_claim(sqlite, shard, unit, &compatibility, now, max_items)
            .unwrap();
    let actual = AsyncProjectionStore::select_rich_claim(
        turso,
        shard.clone(),
        unit,
        compatibility,
        now,
        max_items,
    )
    .await
    .unwrap();
    assert_eq!(actual.item_ids, expected.item_ids);
    assert_eq!(actual.cohort_id, expected.cohort_id);
    actual
}

async fn apply_both_rich(
    sqlite: &mut SqliteRelational,
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
    .expect("SQLite rich apply");
    AsyncProjectionStore::apply_live(turso, vec![position], vec![command])
        .await
        .expect("Turso rich apply");
}

#[tokio::test]
async fn filtered_item_selection_matches_sqlite_and_applies_limit_after_filters() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let mut sqlite = SqliteRelational::in_memory().unwrap();
    ProjectionStore::ensure_shard(&mut sqlite, &definition).unwrap();
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();

    let group_a = GroupKey::new("group-a").unwrap();
    let group_b = GroupKey::new("group-b").unwrap();
    let mut west = Metadata::new();
    west.insert("region", MetadataValue::String("west".to_string()));
    let mut east = Metadata::new();
    east.insert("region", MetadataValue::String("east".to_string()));
    let ids = [
        ItemId::mint(30, 0, 0),
        ItemId::mint(30, 0, 1),
        ItemId::mint(30, 0, 2),
    ];
    let items = vec![
        PushItem {
            client_item_key: ClientItemKey::new("wrong-metadata").unwrap(),
            item_id: ids[0],
            priority: Some(PriorityValue::Int64(1)),
            not_before: None,
            group_key: Some(group_a.clone()),
            max_attempts: 3,
            payload: None,
            fields: Default::default(),
            metadata: west,
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        },
        PushItem {
            client_item_key: ClientItemKey::new("matching").unwrap(),
            item_id: ids[1],
            priority: Some(PriorityValue::Int64(2)),
            not_before: None,
            group_key: Some(group_a.clone()),
            max_attempts: 3,
            payload: None,
            fields: Default::default(),
            metadata: east.clone(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        },
        PushItem {
            client_item_key: ClientItemKey::new("wrong-group").unwrap(),
            item_id: ids[2],
            priority: Some(PriorityValue::Int64(3)),
            not_before: None,
            group_key: Some(group_b),
            max_attempts: 3,
            payload: None,
            fields: Default::default(),
            metadata: east,
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        },
    ];
    apply_both_rich(
        &mut sqlite,
        &turso,
        &shard,
        0,
        envelope(
            "filtered-push",
            QueueCommand::Push(PushCommand { items }),
            ids.to_vec(),
            10,
        ),
    )
    .await;
    let compatibility = ClaimCompatibility {
        group_key: Some(group_a),
        metadata_equals: std::collections::BTreeMap::from([(
            "region".to_string(),
            MetadataValue::String("east".to_string()),
        )]),
        ..ClaimCompatibility::default()
    };

    let expected =
        ProjectionStore::select_item_claim(&sqlite, &shard, &compatibility, timestamp(10), 1)
            .unwrap();
    let actual =
        AsyncProjectionStore::select_item_claim(&turso, shard, compatibility, timestamp(10), 1)
            .await
            .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual, vec![ids[1]]);
}

#[tokio::test]
async fn filtered_item_selection_crosses_page_boundary_and_matches_nested_values_exactly() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let mut sqlite = SqliteRelational::in_memory().unwrap();
    ProjectionStore::ensure_shard(&mut sqlite, &definition).unwrap();
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    let group = GroupKey::new("paged").unwrap();
    let exact_nested =
        MetadataValue::Object(Metadata::from_entries(std::collections::BTreeMap::from([
            (
                "zone".to_string(),
                MetadataValue::String("east".to_string()),
            ),
        ])));
    let superset_nested =
        MetadataValue::Object(Metadata::from_entries(std::collections::BTreeMap::from([
            (
                "zone".to_string(),
                MetadataValue::String("east".to_string()),
            ),
            ("extra".to_string(), MetadataValue::Bool(true)),
        ])));
    let mut items = Vec::new();
    let mut ids = Vec::new();
    for index in 0..129_u32 {
        let id = ItemId::mint(40, 0, index);
        let mut metadata = Metadata::new();
        metadata.insert("location", superset_nested.clone());
        ids.push(id);
        items.push(PushItem {
            client_item_key: ClientItemKey::new(format!("superset-{index}")).unwrap(),
            item_id: id,
            priority: Some(PriorityValue::Int64(index as i64)),
            not_before: None,
            group_key: Some(group.clone()),
            max_attempts: 3,
            payload: None,
            fields: Default::default(),
            metadata,
            cohort_size: None,
            gate_keys: Vec::new(),
            entity_document: None,
        });
    }
    let matching = ItemId::mint(40, 0, 129);
    let mut metadata = Metadata::new();
    metadata.insert("location", exact_nested.clone());
    ids.push(matching);
    items.push(PushItem {
        client_item_key: ClientItemKey::new("exact").unwrap(),
        item_id: matching,
        priority: Some(PriorityValue::Int64(129)),
        not_before: None,
        group_key: Some(group.clone()),
        max_attempts: 3,
        payload: None,
        fields: Default::default(),
        metadata,
        cohort_size: None,
        gate_keys: Vec::new(),
        entity_document: None,
    });
    apply_both_rich(
        &mut sqlite,
        &turso,
        &shard,
        0,
        envelope(
            "paged-filter-push",
            QueueCommand::Push(PushCommand { items }),
            ids,
            10,
        ),
    )
    .await;
    let compatibility = ClaimCompatibility {
        group_key: Some(group),
        metadata_equals: std::collections::BTreeMap::from([("location".to_string(), exact_nested)]),
        ..ClaimCompatibility::default()
    };
    let expected =
        ProjectionStore::select_item_claim(&sqlite, &shard, &compatibility, timestamp(10), 1)
            .unwrap();
    let actual =
        AsyncProjectionStore::select_item_claim(&turso, shard, compatibility, timestamp(10), 1)
            .await
            .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual, vec![matching]);
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
        "fireweed_items_active_key",
        "fireweed_items_group_due_idx",
        "fireweed_item_index_key_idx",
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
                "INSERT INTO fireweed_side_records(tenant_id,queue_id,key,payload) \
                 VALUES(?1,?2,?3,?4)",
                vec!["t".into(), "q".into(), vec![1_u8].into(), vec![2_u8].into()],
            ),
            RelationalStatement::new(
                "INSERT INTO fireweed_side_records(tenant_id,queue_id,key,payload) \
                 VALUES(?1,?2,?3,?4)",
                vec!["t".into(), "q".into(), vec![1_u8].into(), vec![3_u8].into()],
            ),
        ])
        .await;
    assert!(matches!(result, Err(TursoRelationalError::Database(_))));

    let rows = store
        .query("SELECT COUNT(*) FROM fireweed_side_records", vec![])
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
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand::default()),
            Vec::new(),
            12,
        )],
    )
    .await
    .expect("unsupported overlap is idempotently skipped");

    // Side-record commands are part of the complete relational projection corpus and advance the frontier.
    let side_records = envelope(
        "side-records",
        QueueCommand::WriteSideRecords(WriteSideRecordsCommand::default()),
        Vec::new(),
        12,
    );
    let side_records_position = CommandPosition::new(
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        ),
        1,
        2,
    );
    AsyncProjectionStore::apply_live(&turso, vec![side_records_position], vec![side_records])
        .await
        .expect("side-record apply");
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
        2
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
                4,
            )],
            vec![gap],
        )
        .await,
        Err(fireweed_engine::EngineError::Storage(_))
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
        2
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
        3,
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
            "INSERT INTO fireweed_request_idempotency \
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
            "SELECT expires_at FROM fireweed_request_idempotency WHERE request_id=?1",
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
        Err(fireweed_engine::EngineError::NotFound)
    ));
    let failed =
        AsyncProjectionStore::apply_live(&turso, rollback_positions, rollback_commands).await;
    assert!(matches!(
        failed,
        Err(fireweed_engine::EngineError::NotFound)
    ));
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
    let finalize_token = LeaseToken::new("lease-finalize").unwrap();
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
                lease_token: finalize_token.clone(),
                lease_expires_at: timestamp(30),
                worker_id: None,
            }),
            ids.clone(),
            11,
        ),
    )
    .await;

    let claimed = AsyncProjectionStore::render_claimed(&turso, shard.clone(), ids.clone())
        .await
        .unwrap();
    let targets = claimed
        .iter()
        .map(|item| FinalizeTarget {
            item_id: item.item_id,
            lease_token: finalize_token.clone(),
            item_version: item.item_version,
            kind: FinalizeKind::Complete,
            not_before: None,
        })
        .collect::<Vec<_>>();
    let mut stale_targets = targets.clone();
    stale_targets[0].item_version = stale_targets[0].item_version.saturating_add(1);
    assert_eq!(
        AsyncProjectionStore::finalize_validate(
            &turso,
            shard.clone(),
            stale_targets,
            timestamp(30),
            3,
        )
        .await,
        Err(fireweed_engine::EngineError::Conflict)
    );
    assert_eq!(
        AsyncProjectionStore::finalize_validate(&turso, shard.clone(), targets, timestamp(30), 3,)
            .await
            .unwrap(),
        ids.iter()
            .enumerate()
            .map(|(index, item_id)| fireweed_engine::FinalizeLeaseMember {
                item_id: *item_id,
                attempt_count: 1,
                max_attempts: if index == 2 { 1 } else { 3 },
            })
            .collect::<Vec<_>>()
    );

    let mut retry = FinalizeOutcome::new(ids[3], FinalizeKind::Retry);
    retry.not_before = Some(timestamp(100));
    let outcomes = vec![
        FinalizeOutcome::new(ids[0], FinalizeKind::Complete),
        FinalizeOutcome::new(ids[1], FinalizeKind::Fail),
        FinalizeOutcome::new(ids[2], FinalizeKind::Retry),
        retry,
        FinalizeOutcome::new(ids[4], FinalizeKind::Release),
        FinalizeOutcome {
            not_before: Some(timestamp(50)),
            ..FinalizeOutcome::new(ids[5], FinalizeKind::Rearm)
        },
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
    for now in [12, 49, 50, 99, 100] {
        assert_eq!(
            AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(now), 10)
                .await
                .unwrap(),
            ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(now), 10).unwrap()
        );
    }
    let before_rearm =
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(49), 10)
            .await
            .unwrap();
    assert!(!before_rearm.contains(&ids[5]));
    let at_rearm =
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(50), 10)
            .await
            .unwrap();
    assert!(at_rearm.contains(&ids[5]));

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
                lease_expires_at: timestamp(140),
                worker_id: None,
            }),
            vec![ids[4], ids[5]],
            101,
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
        Some(fireweed_core::ItemState::Pending)
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
        Err(fireweed_engine::EngineError::Storage(_))
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
        Err(fireweed_engine::EngineError::Storage(_))
    ));
}

#[tokio::test]
async fn turso_create_returns_authoritative_rich_definition() {
    let store = TursoRelational::in_memory().await.expect("Turso");
    let definition = rich_create_definition();

    let outcome = store
        .create_or_read_queue(definition.clone())
        .await
        .unwrap();

    assert!(outcome.created);
    assert_eq!(outcome.definition, definition);
    let rows = store
        .query(
            "SELECT definition FROM queues WHERE tenant=?1 AND queue=?2",
            vec!["tenant".into(), "queue".into()],
        )
        .await
        .unwrap();
    let encoded = match &rows[0].values[0] {
        Value::Text(encoded) => encoded,
        value => panic!("unexpected durable definition value: {value:?}"),
    };
    let durable: QueueDefinition = serde_json::from_str(encoded).unwrap();
    assert_eq!(durable, definition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turso_concurrent_compatible_create_has_one_winner_no_overwrite() {
    let dir = tempdir().unwrap();
    let config = TursoConfig::local(dir.path().join("compatible-create.db"));
    let first = TursoRelational::open(config.clone()).await.unwrap();
    let second = TursoRelational::open(config).await.unwrap();
    let definition = rich_create_definition();

    let (first_outcome, second_outcome) = tokio::join!(
        first.create_or_read_queue(definition.clone()),
        second.create_or_read_queue(definition.clone())
    );
    let outcomes = [first_outcome.unwrap(), second_outcome.unwrap()];
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.definition == definition)
    );
    let durable = first
        .create_or_read_queue(definition.clone())
        .await
        .unwrap();
    assert!(!durable.created);
    assert_eq!(durable.definition, definition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turso_concurrent_incompatible_create_conflicts_and_preserves_winner() {
    let dir = tempdir().unwrap();
    let config = TursoConfig::local(dir.path().join("incompatible-create.db"));
    let first = TursoRelational::open(config.clone()).await.unwrap();
    let second = TursoRelational::open(config).await.unwrap();
    let first_definition = rich_create_definition();
    let mut second_definition = first_definition.clone();
    second_definition.progress_bound_ms += 1;
    second_definition.max_claim_batch_size += 1;

    let (first_outcome, second_outcome) = tokio::join!(
        first.create_or_read_queue(first_definition.clone()),
        second.create_or_read_queue(second_definition.clone())
    );
    let (winner, loser_store) = match (first_outcome, second_outcome) {
        (Ok(outcome), Err(EngineError::QueueDefinitionConflict)) => (outcome, &second),
        (Err(EngineError::QueueDefinitionConflict), Ok(outcome)) => (outcome, &first),
        (first, second) => panic!("expected one winner and one conflict: {first:?}, {second:?}"),
    };
    assert!(winner.created);
    assert!(winner.definition == first_definition || winner.definition == second_definition);
    assert_eq!(
        loser_store
            .create_or_read_queue(winner.definition.clone())
            .await
            .unwrap()
            .definition,
        winner.definition
    );
}

#[tokio::test]
async fn active_lease_reopen_uses_durable_hash_for_renew_validation() {
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
        Some(fireweed_core::ItemState::Leased),
        "durable lease state and token hash survive"
    );
    AsyncProjectionStore::renew_validate(
        &reopened,
        shard.clone(),
        vec![fireweed_engine::RenewTarget {
            item_id: item,
            lease_token: LeaseToken::new("active-reopen-token").unwrap(),
        }],
        timestamp(20),
    )
    .await
    .expect("durable token hash validates after reopen");
    assert!(
        AsyncProjectionStore::render_claimed(&reopened, shard, vec![item])
            .await
            .unwrap()
            .is_empty(),
        "cleartext token recovery is intentionally not claimed by this adapter slice"
    );
}

#[tokio::test]
async fn purge_retains_pending_leased_and_terminal_keys_like_async_sqlite() {
    for (case, lifecycle) in [("pending", 0_u8), ("leased-force", 1), ("terminal", 2)] {
        let definition = definition();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let sqlite = AsyncSqliteProjectionStore::in_memory()
            .await
            .expect("async SQLite");
        AsyncProjectionStore::ensure_shard(&sqlite, definition.clone())
            .await
            .expect("SQLite ensure");
        let turso = TursoRelational::in_memory().await.expect("Turso");
        AsyncProjectionStore::ensure_shard(&turso, definition)
            .await
            .expect("Turso ensure");

        let item_id = ItemId::mint(11, lifecycle, 1);
        let key = format!("purge-{case}");
        let mut sequence = 0;
        apply_async_pair(
            &sqlite,
            &turso,
            &shard,
            sequence,
            envelope(
                &format!("push-{case}"),
                QueueCommand::Push(PushCommand {
                    items: vec![push_item(item_id, &key, 3)],
                }),
                vec![item_id],
                1,
            ),
        )
        .await;

        if lifecycle >= 1 {
            sequence += 1;
            apply_async_pair(
                &sqlite,
                &turso,
                &shard,
                sequence,
                envelope(
                    &format!("claim-{case}"),
                    QueueCommand::Claim(ClaimCommand {
                        item_ids: vec![item_id],
                        lease_token: LeaseToken::new(format!("token-{case}")).unwrap(),
                        lease_expires_at: timestamp(30),
                        worker_id: None,
                    }),
                    vec![item_id],
                    2,
                ),
            )
            .await;
        }
        if lifecycle == 2 {
            sequence += 1;
            apply_async_pair(
                &sqlite,
                &turso,
                &shard,
                sequence,
                envelope(
                    &format!("finalize-{case}"),
                    QueueCommand::Finalize(FinalizeCommand {
                        outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
                    }),
                    vec![item_id],
                    3,
                ),
            )
            .await;
        }

        sequence += 1;
        let purge_at = 4;
        apply_async_pair(
            &sqlite,
            &turso,
            &shard,
            sequence,
            envelope(
                &format!("purge-{case}"),
                QueueCommand::PurgeItems(PurgeItemsCommand {
                    item_ids: vec![item_id],
                    force: lifecycle == 1,
                }),
                vec![item_id],
                purge_at,
            ),
        )
        .await;

        let replacement = push_item(ItemId::mint(11, lifecycle, 2), &key, 3);
        let sqlite_repush = AsyncProjectionStore::validate_push(
            &sqlite,
            shard.clone(),
            vec![replacement.clone()],
            timestamp(purge_at + 1),
        )
        .await;
        let turso_repush = AsyncProjectionStore::validate_push(
            &turso,
            shard.clone(),
            vec![replacement],
            timestamp(purge_at + 1),
        )
        .await;
        assert_eq!(
            sqlite_repush,
            Err(fireweed_engine::EngineError::Conflict),
            "SQLite must reject the within-retention {case} re-push"
        );
        assert_eq!(
            turso_repush, sqlite_repush,
            "Turso must match SQLite for the within-retention {case} re-push"
        );

        let tombstone = turso
            .query(
                "SELECT item_id,expires_at FROM fireweed_item_key_retention \
                 WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
                vec!["tenant".into(), "queue".into(), key.into()],
            )
            .await
            .expect("retention tombstone");
        assert_eq!(tombstone.len(), 1, "{case} purge tombstone count");
        assert_eq!(tombstone[0].values[0], Value::Text(item_id.to_string()));
        assert_eq!(
            tombstone[0].values[1],
            Value::Integer((purge_at + 60) * 1_000_000_000),
            "{case} purge tombstone expiry"
        );
    }
}

#[tokio::test]
async fn control_gate_update_replace_and_purge_match_sqlite() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let mut sqlite = SqliteProjectionStore::in_memory().expect("sqlite");
    ProjectionStore::ensure_shard(&mut sqlite, &definition).expect("sqlite ensure");
    let turso = TursoRelational::in_memory().await.expect("Turso");
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .expect("Turso ensure");
    let gated = ItemId::mint(7, 0, 0);
    let plain = ItemId::mint(7, 0, 1);
    let old = ItemId::mint(7, 0, 2);
    let field_item = ItemId::mint(7, 0, 3);
    let purged = ItemId::mint(7, 0, 4);
    let replacement = ItemId::mint(7, 0, 5);
    let paused_push = ItemId::mint(7, 0, 6);
    let mut gated_item = push_item(gated, "gated", 3);
    gated_item.gate_keys = vec!["deploy".to_string()];
    let mut initial_field = push_item(field_item, "fields", 3);
    initial_field.payload = Some(Bytes::from_static(b"old"));
    initial_field
        .fields
        .insert("remove".into(), Bytes::from_static(b"x"));
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        0,
        envelope(
            "shape-push",
            QueueCommand::Push(PushCommand {
                items: vec![
                    gated_item,
                    push_item(plain, "plain", 3),
                    push_item(old, "replace-key", 3),
                    initial_field,
                    push_item(purged, "purge-key", 3),
                ],
            }),
            vec![gated, plain, old, field_item, purged],
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
            "block",
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["deploy".into()],
                blocked: true,
            }),
            Vec::new(),
            11,
        ),
    )
    .await;
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(11), 20)
            .await
            .unwrap(),
        ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(11), 20).unwrap()
    );
    assert!(
        !AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(11), 20)
            .await
            .unwrap()
            .contains(&gated)
    );
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        2,
        envelope(
            "pause",
            QueueCommand::PauseQueue(PauseQueueCommand { drain_intake: true }),
            Vec::new(),
            12,
        ),
    )
    .await;
    assert!(
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(12), 20)
            .await
            .unwrap()
            .is_empty()
    );
    // Projection replay still materializes already-authorized intake while paused; eligibility remains stopped.
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        3,
        envelope(
            "paused-push",
            QueueCommand::Push(PushCommand {
                items: vec![push_item(paused_push, "paused-push", 3)],
            }),
            vec![paused_push],
            13,
        ),
    )
    .await;
    assert!(
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(13), 20)
            .await
            .unwrap()
            .is_empty()
    );
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        4,
        envelope("resume", QueueCommand::ResumeQueue, Vec::new(), 14),
    )
    .await;
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        5,
        envelope(
            "unblock",
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["deploy".into()],
                blocked: false,
            }),
            Vec::new(),
            15,
        ),
    )
    .await;
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(15), 20)
            .await
            .unwrap(),
        ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(15), 20).unwrap()
    );

    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        6,
        envelope(
            "update",
            QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id: field_item,
                field_ops: std::collections::BTreeMap::from([
                    ("remove".into(), None),
                    ("added".into(), Some(Bytes::from_static(b"yes"))),
                ]),
                payload: PayloadUpdate::Set(Some(Bytes::from_static(b"new"))),
                set_priority: ScheduleUpdate::Keep,
                set_not_before: ScheduleUpdate::Keep,
                set_entity_document: None,
                set_fields: None,
                set_metadata: None,
                set_gate_keys: None,
                api001_batch: false,
            }),
            vec![field_item],
            16,
        ),
    )
    .await;
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        7,
        envelope(
            "replace",
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("replace-key").unwrap(),
                superseded_item_id: old,
                replacement: push_item(replacement, "replace-key", 3),
            }),
            vec![old, replacement],
            17,
        ),
    )
    .await;
    let eligible =
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(17), 20)
            .await
            .unwrap();
    assert!(!eligible.contains(&old));
    assert!(eligible.contains(&replacement));
    assert_eq!(
        eligible,
        ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(17), 20).unwrap()
    );

    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        8,
        envelope(
            "claim-updated",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![field_item, purged],
                lease_token: LeaseToken::new("control-token").unwrap(),
                lease_expires_at: timestamp(30),
                worker_id: None,
            }),
            vec![field_item, purged],
            18,
        ),
    )
    .await;
    let updated = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![field_item])
        .await
        .unwrap();
    let sqlite_updated = ProjectionStore::render_claimed(&sqlite, &shard, &[field_item]).unwrap();
    assert_eq!(updated[0].fields, sqlite_updated[0].fields);
    assert_eq!(updated[0].payload, sqlite_updated[0].payload);
    assert_eq!(updated[0].item_version, sqlite_updated[0].item_version);
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        9,
        envelope(
            "terminal",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(purged, FinalizeKind::Complete)],
            }),
            vec![purged],
            19,
        ),
    )
    .await;
    apply_both(
        &mut sqlite,
        &turso,
        &shard,
        10,
        envelope(
            "purge",
            QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![purged],
                force: false,
            }),
            vec![purged],
            20,
        ),
    )
    .await;
    assert_eq!(
        AsyncProjectionStore::item_state(&turso, shard.clone(), purged)
            .await
            .unwrap(),
        ProjectionStore::item_state(&sqlite, &shard, &purged).unwrap()
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap()
            .unwrap()
            .sequence,
        10
    );
    let tombstone = turso.query("SELECT item_id,expires_at FROM fireweed_item_key_retention WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3", vec!["tenant".into(), "queue".into(), "purge-key".into()]).await.unwrap();
    assert_eq!(tombstone[0].values[0], Value::Text(purged.to_string()));
    assert_eq!(tombstone[0].values[1], Value::Integer(80_000_000_000));

    let rollback_positions = vec![
        CommandPosition::new(shard.clone(), 1, 11),
        CommandPosition::new(shard.clone(), 1, 12),
    ];
    let rollback_commands = vec![
        envelope(
            "rollback-pause",
            QueueCommand::PauseQueue(PauseQueueCommand {
                drain_intake: false,
            }),
            Vec::new(),
            21,
        ),
        envelope(
            "duplicate-replacement",
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("plain").unwrap(),
                superseded_item_id: ItemId::mint(7, 0, 99),
                replacement: push_item(ItemId::mint(7, 0, 100), "plain", 3),
            }),
            Vec::new(),
            21,
        ),
    ];
    assert!(ProjectionStore::apply(&mut sqlite, &rollback_positions, &rollback_commands).is_err());
    assert!(
        AsyncProjectionStore::apply_live(&turso, rollback_positions, rollback_commands)
            .await
            .is_err()
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap()
            .unwrap()
            .sequence,
        10
    );
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(&turso, shard.clone(), timestamp(21), 20)
            .await
            .unwrap(),
        ProjectionStore::eligible_candidates(&sqlite, &shard, timestamp(21), 20).unwrap()
    );
}

#[tokio::test]
async fn async_rich_claim_selection_matches_sqlite_without_durable_mutation() {
    let mut rich_definition = definition();
    rich_definition.max_eligible_group_size = Some(3);
    rich_definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(30_000),
        on_incomplete: None,
        max_cohort_size: Some(4),
    });
    let shard = QueueKey::new(
        rich_definition.tenant_id.clone(),
        rich_definition.queue_id.clone(),
    );
    let mut sqlite = SqliteRelational::in_memory().unwrap();
    ProjectionStore::ensure_shard(&mut sqlite, &rich_definition).unwrap();
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, rich_definition.clone())
        .await
        .unwrap();
    let oldest = GroupKey::new("oldest").unwrap();
    let later = GroupKey::new("later").unwrap();
    let first = ItemId::mint(30, 0, 0);
    let second = ItemId::mint(30, 0, 1);
    let third = ItemId::mint(30, 0, 2);
    let scheduled = ItemId::mint(30, 0, 3);
    let grouped = |id, key: &str, group: &GroupKey, not_before, gates| {
        let mut metadata = Metadata::new();
        metadata.insert(
            "region",
            MetadataValue::String(if key == "first" { "west" } else { "east" }.to_string()),
        );
        PushItem {
            priority: Some(PriorityValue::Int64(match key {
                "first" => 1,
                "third" => 50,
                "second" => 100,
                _ => 200,
            })),
            group_key: Some(group.clone()),
            not_before,
            gate_keys: gates,
            metadata,
            ..push_item(id, key, 3)
        }
    };
    apply_both_rich(
        &mut sqlite,
        &turso,
        &shard,
        0,
        envelope(
            "rich-groups",
            QueueCommand::Push(PushCommand {
                items: vec![
                    grouped(
                        first,
                        "first",
                        &oldest,
                        Some(timestamp(100)),
                        vec!["blocked".to_string()],
                    ),
                    grouped(
                        second,
                        "second",
                        &oldest,
                        Some(timestamp(100)),
                        vec!["blocked".to_string()],
                    ),
                    grouped(third, "third", &later, None, Vec::new()),
                    grouped(
                        scheduled,
                        "scheduled",
                        &oldest,
                        Some(timestamp(1_000)),
                        Vec::new(),
                    ),
                ],
            }),
            vec![first, second, third, scheduled],
            10,
        ),
    )
    .await;
    let frontier = AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
        .await
        .unwrap();
    assert_eq!(group_summary_count(&turso, &oldest).await, 0);
    let batching = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    let selected = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::WholeGroup,
        batching.clone(),
        timestamp(100),
        3,
    )
    .await;
    assert_eq!(selected.item_ids, vec![first, second, third]);
    assert_eq!(group_summary_count(&turso, &oldest).await, 0);
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&turso, shard.clone())
            .await
            .unwrap(),
        frontier
    );
    let same = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::SameGroupKey,
        ClaimCompatibility {
            same_group_key: true,
            ..Default::default()
        },
        timestamp(100),
        1,
    )
    .await;
    assert_eq!(same.item_ids, vec![first]);
    let east_filter = std::collections::BTreeMap::from([(
        "region".to_string(),
        MetadataValue::String("east".to_string()),
    )]);
    let filtered_whole = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::WholeGroup,
        ClaimCompatibility {
            group_batching: Some(GroupBatching { max_groups: 2 }),
            metadata_equals: east_filter.clone(),
            ..Default::default()
        },
        timestamp(100),
        3,
    )
    .await;
    assert_eq!(filtered_whole.item_ids, vec![third, second]);
    let explicit_same = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::SameGroupKey,
        ClaimCompatibility {
            group_key: Some(later.clone()),
            same_group_key: true,
            metadata_equals: std::collections::BTreeMap::from([(
                "region".to_string(),
                MetadataValue::String("east".to_string()),
            )]),
            ..Default::default()
        },
        timestamp(100),
        3,
    )
    .await;
    assert_eq!(explicit_same.item_ids, vec![third]);
    assert!(matches!(
        ProjectionStore::select_rich_claim(
            &sqlite,
            &shard,
            ClaimUnit::WholeGroup,
            &batching,
            timestamp(100),
            1,
        ),
        Err(fireweed_engine::EngineError::BatchTooLarge)
    ));
    assert!(matches!(
        AsyncProjectionStore::select_rich_claim(
            &turso,
            shard.clone(),
            ClaimUnit::WholeGroup,
            batching.clone(),
            timestamp(100),
            1,
        )
        .await,
        Err(fireweed_engine::EngineError::BatchTooLarge)
    ));
    apply_both_rich(
        &mut sqlite,
        &turso,
        &shard,
        1,
        envelope(
            "lease-group-sibling",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![second],
                lease_token: LeaseToken::new("sibling-lease").unwrap(),
                lease_expires_at: timestamp(200),
                worker_id: None,
            }),
            vec![second],
            100,
        ),
    )
    .await;
    let skip_partially_leased_whole = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::WholeGroup,
        batching.clone(),
        timestamp(100),
        3,
    )
    .await;
    assert_eq!(skip_partially_leased_whole.item_ids, vec![third]);
    let skip_partially_leased_same = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::SameGroupKey,
        ClaimCompatibility {
            same_group_key: true,
            ..Default::default()
        },
        timestamp(100),
        3,
    )
    .await;
    assert_eq!(skip_partially_leased_same.item_ids, vec![first]);
    apply_both_rich(
        &mut sqlite,
        &turso,
        &shard,
        2,
        envelope(
            "block-rich-group",
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["blocked".to_string()],
                blocked: true,
            }),
            Vec::new(),
            101,
        ),
    )
    .await;
    let selected = assert_rich_selection_matches(
        &sqlite,
        &turso,
        &shard,
        ClaimUnit::WholeGroup,
        batching.clone(),
        timestamp(101),
        3,
    )
    .await;
    assert_eq!(selected.item_ids, vec![third]);
    apply_both_rich(
        &mut sqlite,
        &turso,
        &shard,
        3,
        envelope(
            "pause-rich",
            QueueCommand::PauseQueue(PauseQueueCommand {
                drain_intake: false,
            }),
            Vec::new(),
            102,
        ),
    )
    .await;
    assert!(
        assert_rich_selection_matches(
            &sqlite,
            &turso,
            &shard,
            ClaimUnit::SameGroupKey,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
            timestamp(102),
            3,
        )
        .await
        .item_ids
        .is_empty()
    );

    let cohort_shard = QueueKey::new(
        TenantId::new("cohort-tenant").unwrap(),
        QueueId::new("cohort-queue").unwrap(),
    );
    let mut cohort_definition = rich_definition;
    cohort_definition.tenant_id = cohort_shard.tenant_id.clone();
    cohort_definition.queue_id = cohort_shard.queue_id.clone();
    let mut cohort_sqlite = SqliteRelational::in_memory().unwrap();
    ProjectionStore::ensure_shard(&mut cohort_sqlite, &cohort_definition).unwrap();
    let cohort_turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&cohort_turso, cohort_definition)
        .await
        .unwrap();
    let incomplete_group = GroupKey::new("incomplete").unwrap();
    let complete_group = GroupKey::new("complete").unwrap();
    let incomplete = ItemId::mint(31, 0, 0);
    let cohort_first = ItemId::mint(31, 0, 1);
    let cohort_second = ItemId::mint(31, 0, 2);
    let cohort_member = |id, key: &str, group: &GroupKey, gates| {
        let mut metadata = Metadata::new();
        metadata.insert("region", MetadataValue::String("east".to_string()));
        PushItem {
            group_key: Some(group.clone()),
            cohort_size: Some(2),
            gate_keys: gates,
            metadata,
            ..push_item(id, key, 3)
        }
    };
    apply_both_rich(
        &mut cohort_sqlite,
        &cohort_turso,
        &cohort_shard,
        0,
        envelope(
            "rich-cohorts",
            QueueCommand::Push(PushCommand {
                items: vec![
                    cohort_member(incomplete, "incomplete", &incomplete_group, Vec::new()),
                    cohort_member(
                        cohort_first,
                        "cohort-first",
                        &complete_group,
                        vec!["cohort-blocked".to_string()],
                    ),
                    cohort_member(cohort_second, "cohort-second", &complete_group, Vec::new()),
                ],
            }),
            vec![incomplete, cohort_first, cohort_second],
            200,
        ),
    )
    .await;
    let cohort_compatibility = ClaimCompatibility {
        whole_cohort: true,
        ..Default::default()
    };
    let cohort = assert_rich_selection_matches(
        &cohort_sqlite,
        &cohort_turso,
        &cohort_shard,
        ClaimUnit::WholeCohort,
        cohort_compatibility.clone(),
        timestamp(200),
        2,
    )
    .await;
    assert_eq!(cohort.item_ids, vec![cohort_first, cohort_second]);
    assert!(cohort.cohort_id.is_some());
    let filtered_cohort = assert_rich_selection_matches(
        &cohort_sqlite,
        &cohort_turso,
        &cohort_shard,
        ClaimUnit::WholeCohort,
        ClaimCompatibility {
            whole_cohort: true,
            metadata_equals: east_filter,
            ..Default::default()
        },
        timestamp(200),
        2,
    )
    .await;
    assert_eq!(filtered_cohort.item_ids, vec![cohort_first, cohort_second]);
    assert!(matches!(
        ProjectionStore::select_rich_claim(
            &cohort_sqlite,
            &cohort_shard,
            ClaimUnit::WholeCohort,
            &cohort_compatibility,
            timestamp(200),
            1,
        ),
        Err(fireweed_engine::EngineError::BatchTooLarge)
    ));
    assert!(matches!(
        AsyncProjectionStore::select_rich_claim(
            &cohort_turso,
            cohort_shard.clone(),
            ClaimUnit::WholeCohort,
            cohort_compatibility.clone(),
            timestamp(200),
            1,
        )
        .await,
        Err(fireweed_engine::EngineError::BatchTooLarge)
    ));
    apply_both_rich(
        &mut cohort_sqlite,
        &cohort_turso,
        &cohort_shard,
        1,
        envelope(
            "block-rich-cohort",
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["cohort-blocked".to_string()],
                blocked: true,
            }),
            Vec::new(),
            201,
        ),
    )
    .await;
    assert!(
        assert_rich_selection_matches(
            &cohort_sqlite,
            &cohort_turso,
            &cohort_shard,
            ClaimUnit::WholeCohort,
            cohort_compatibility,
            timestamp(201),
            2,
        )
        .await
        .item_ids
        .is_empty()
    );
}
