use std::time::Duration;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, ClientItemKey, CohortId, CohortOnIncomplete, CohortPolicy, EligibilityPolicy,
    GateKeyPolicy, GroupKey, IndexDeclaration, IndexDef, IndexType, ItemId, LeaseToken, Metadata,
    MetadataValue, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, QueueIndex, RecurrencePolicy,
    RequestId, RetryPolicy, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncProjectionStore, ClaimCommand, ClaimCompatibility, CohortClaimCommand,
    CohortExpiredCommand, CohortFinalizeCommand, CohortRenewLeaseCommand, CommandChecksum,
    CommandEnvelope, CommandId, CommandPosition, EngineError, FinalizeCommand, FinalizeKind,
    FinalizeOutcome, IdempotencyDecision, LeaseExpiredCommand, PauseQueueCommand, PayloadUpdate,
    PurgeItemsCommand, PushCommand, PushFingerprint, PushItem, QueueCommand, QueueKey,
    RenewLeaseCommand, ReplacePendingCommand, RequestOutcome, ScheduleUpdate, UpdateFieldsCommand,
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
        index_fields: Default::default(),
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
    let mut definition = gated_definition();
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
    let mut def = cohort_definition(3);
    def.max_eligible_group_size = Some(2);
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
        index_fields: Default::default(),
        entity_document: Some(serde_json::json!({ "email": email })),
    }
}

#[tokio::test]
async fn grouped_typed_cohort_lifecycle_is_atomic_and_refreshes_summary() {
    let mut definition = cohort_definition(10);
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

    let lease = LeaseToken::new("cohort-token").unwrap();
    AsyncProjectionStore::apply_live(
        &turso,
        vec![CommandPosition::new(shard.clone(), 2, 1)],
        vec![envelope(
            "cohort-claim",
            QueueCommand::CohortClaim(CohortClaimCommand {
                cohort_id: cohort_id.clone(),
                item_ids: vec![first, second],
                lease_token: lease.clone(),
                lease_expires_at: timestamp(30),
            }),
            vec![first, second],
            11,
        )],
    )
    .await
    .unwrap();
    let rendered = AsyncProjectionStore::render_claimed(&turso, shard.clone(), vec![first, second])
        .await
        .unwrap();
    assert_eq!(
        rendered.len(),
        2,
        "both committed cohort members must render"
    );
    assert_eq!(
        rendered.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![first, second]
    );
    assert!(
        rendered
            .iter()
            .all(|item| item.lease_token.as_ref() == Some(&lease)
                && item.lease_expires_at == timestamp(30))
    );
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
    let mut cohort_definition = cohort_definition(10);
    cohort_definition.typed_indexes = vec![QueueIndex {
        name: "by_email".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "email".to_string(),
            index_type: IndexType::String,
            unique: true,
        }),
    }];
    let shard = QueueKey::new(
        cohort_definition.tenant_id.clone(),
        cohort_definition.queue_id.clone(),
    );
    let mut ordinary_definition = definition();
    ordinary_definition.queue_id = QueueId::new("ordinary-cohort-neighbor").unwrap();
    let ordinary_shard = QueueKey::new(
        ordinary_definition.tenant_id.clone(),
        ordinary_definition.queue_id.clone(),
    );
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, cohort_definition)
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&turso, ordinary_definition)
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
            ],
        }),
        vec![first, second],
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
        vec![CommandPosition::new(ordinary_shard.clone(), 3, 0)],
        vec![envelope(
            "unrelated-grouped-push",
            QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    group_key: Some(group.clone()),
                    entity_document: Some(serde_json::json!({ "email": "other@example.com" })),
                    ..push_item(unrelated, "unrelated", 3)
                }],
            }),
            vec![unrelated],
            20,
        )],
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
                "SELECT eligible_item_count FROM fireweed_group_summary \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                vec![
                    Value::Text(ordinary_shard.tenant_id.as_str().to_string()),
                    Value::Text(ordinary_shard.queue_id.as_str().to_string()),
                    Value::Text(group.as_str().to_string()),
                ],
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
    let result = AsyncProjectionStore::apply_live(
        turso,
        vec![CommandPosition::new(shard.clone(), 4, sequence)],
        vec![command],
    )
    .await;
    // Independent full-row oracle checks both successful transitions and
    // rejected transactions across cohorts, supersession, retries and replay.
    let rows = turso.query(
        "SELECT lifecycle_state,COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND superseded=0 GROUP BY lifecycle_state",
        vec![shard.tenant_id.as_str().into(), shard.queue_id.as_str().into()],
    ).await.unwrap();
    let metrics = turso.server_metrics(shard).await.unwrap();
    let mut expected = [0u64; 4];
    for row in rows {
        let Value::Text(state) = &row.values[0] else {
            panic!("state")
        };
        let Value::Integer(count) = row.values[1] else {
            panic!("count")
        };
        let index = ["Pending", "Leased", "Complete", "Failed"]
            .iter()
            .position(|s| *s == state.as_str())
            .unwrap();
        expected[index] = count as u64;
    }
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        expected
    );
    result
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
                authority_first: false,
            }),
            vec![first],
            31,
        ),
    )
    .await
    .unwrap();
    // Item Claim leaves the group summary lagged; grouped Claim relects (BQ-14).
    assert_eq!(group_summary_count(&turso, &group).await, 2);
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
                authority_first: false,
            }),
            vec![first],
            34,
        ),
    )
    .await
    .unwrap();
    assert_eq!(group_summary_count(&turso, &group).await, 2);
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
            61,
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
                authority_first: false,
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
    // Complete also lags; Purge relects the remaining pending member.
    assert_eq!(group_summary_count(&turso, &group).await, 2);
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
    result.expect("grouped ReplacePending uses the shared relational apply");
    assert_eq!(
        turso
            .query(
                "SELECT superseded FROM fireweed_items WHERE item_id=?1",
                vec![Value::Text(original.to_string())],
            )
            .await
            .unwrap()[0]
            .values,
        vec![Value::Integer(1)]
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
            "SELECT fields,(SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) AS payload,entity_document FROM fireweed_items WHERE item_id=?1",
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
            client_item_key: None,
            expected_item_version: None,
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
                "SELECT fields,(SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) AS payload,entity_document FROM fireweed_items WHERE item_id=?1",
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
            client_item_key: None,
            expected_item_version: None,
        }),
        vec![first],
        52,
    );
    apply_turso(&turso, &shard, 1, successful_update.clone())
        .await
        .unwrap();
    let changed = turso
        .query(
            "SELECT fields,(SELECT p.payload FROM fireweed_item_payloads p WHERE p.tenant_id=fireweed_items.tenant_id AND p.queue_id=fireweed_items.queue_id AND p.item_id=fireweed_items.item_id) AS payload,entity_document FROM fireweed_items WHERE item_id=?1",
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

fn gated_definition() -> QueueDefinition {
    let mut definition = definition();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(64);
    definition.eligibility_policy.max_gates_per_request = Some(64);
    definition
}

fn cohort_definition(max_cohort_size: u64) -> QueueDefinition {
    let mut definition = gated_definition();
    definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(30_000),
        on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(max_cohort_size),
    });
    definition
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

async fn apply_command(
    turso: &TursoRelational,
    shard: &QueueKey,
    sequence: u64,
    command: CommandEnvelope,
) {
    let position = CommandPosition::new(shard.clone(), 1, sequence);
    AsyncProjectionStore::apply_live(turso, vec![position], vec![command])
        .await
        .expect("Turso apply");
}

#[tokio::test]
async fn filtered_item_selection_applies_limit_after_filters() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
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
            index_fields: Default::default(),
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
            index_fields: Default::default(),
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
            index_fields: Default::default(),
            entity_document: None,
        },
    ];
    apply_command(
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

    let actual =
        AsyncProjectionStore::select_item_claim(&turso, shard, compatibility, timestamp(10), 1)
            .await
            .unwrap();
    assert_eq!(actual, vec![ids[1]]);
}

#[tokio::test]
async fn filtered_item_selection_crosses_page_boundary_and_matches_nested_values_exactly() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
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
            index_fields: Default::default(),
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
        index_fields: Default::default(),
        entity_document: None,
    });
    apply_command(
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
    let actual =
        AsyncProjectionStore::select_item_claim(&turso, shard, compatibility, timestamp(10), 1)
            .await
            .unwrap();
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
        index_fields: Default::default(),
        entity_document: None,
    }
}

#[tokio::test]
async fn configures_and_verifies_the_exact_shared_schema() {
    let store = TursoRelational::in_memory().await.expect("open Turso");
    let settings = store.connection_settings().await.expect("settings");
    assert_eq!(settings.journal_mode, "wal");
    assert_eq!(settings.synchronous, 0);
    assert_eq!(settings.busy_timeout_ms, 5_000);

    let report = store.schema_report().await.expect("schema report");
    for table in OWNED_PROJECTION_TABLES {
        assert!(report.tables.iter().any(|actual| actual == table));
    }
    for index in [
        "fireweed_items_active_key",
        "fireweed_items_pending_eligible_order_idx",
        "fireweed_items_pending_group_nonnull_idx",
        "fireweed_item_index_key_numeric_asc_idx",
        "fireweed_items_retained_numeric_idx",
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
            authority_first: false,
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
        21,
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
                    authority_first: false,
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
                    authority_first: false,
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
                    authority_first: false,
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
    let claimed = AsyncProjectionStore::render_claimed(&reopened, shard, vec![item])
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(
        claimed[0].lease_token.as_ref().map(LeaseToken::as_str),
        Some("active-reopen-token"),
        "lease cleartext is recovered from fireweed_lease_bearers"
    );
}

#[tokio::test]
async fn new_requests_collect_expired_unique_receipts_with_bounded_queue_scope() {
    let def = definition();
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let turso = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&turso, def)
        .await
        .unwrap();
    for i in 0..72 {
        let queue = if i == 71 {
            "foreign"
        } else {
            shard.queue_id.as_str()
        };
        let expiry = if i == 70 {
            200_000_000_000i64
        } else {
            100_000_000_000i64
        };
        turso.execute("INSERT INTO fireweed_request_idempotency \
            (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,command_positions,expires_at,created_at) \
            VALUES (?1,?2,'claim_by_query',?3,?4,'{}','[]',?5,0)",
            vec![shard.tenant_id.as_str().into(),queue.into(),format!("old-{i:03}").into(),Value::Blob(vec![1]),expiry.into()]).await.unwrap();
        turso.execute("INSERT INTO fireweed_claim_replay_items (tenant_id,queue_id,request_id,item_id) VALUES (?1,?2,?3,?4)",
            vec![shard.tenant_id.as_str().into(),queue.into(),format!("old-{i:03}").into(),format!("item-{i}").into()]).await.unwrap();
    }
    for sequence in 0..2u64 {
        let id = ItemId::mint(1, 1, (sequence + 1) as u32);
        let mut push = envelope(
            &format!("new-{sequence}"),
            QueueCommand::Push(PushCommand {
                items: vec![indexed_item(
                    id,
                    &format!("new-{sequence}"),
                    "x@example.com",
                )],
            }),
            vec![id],
            100,
        );
        push.request_id = Some(RequestId::new(format!("fresh-{sequence}")).unwrap());
        push.request_fingerprint = Some(sequence + 1);
        push.request_outcome = Some(RequestOutcome::Push { item_ids: vec![id] });
        AsyncProjectionStore::apply_live(
            &turso,
            vec![CommandPosition::new(shard.clone(), 1, sequence)],
            vec![push],
        )
        .await
        .unwrap();
        let count = turso
            .query("SELECT COUNT(*) FROM fireweed_request_idempotency", vec![])
            .await
            .unwrap();
        assert_eq!(
            count[0].values[0],
            Value::Integer(if sequence == 0 { 9 } else { 4 }),
            "expired unique receipts must be collected, at most 64 per new receipt"
        );
        let edges = turso
            .query("SELECT COUNT(*) FROM fireweed_claim_replay_items", vec![])
            .await
            .unwrap();
        assert_eq!(
            edges[0].values[0],
            Value::Integer(if sequence == 0 { 8 } else { 2 }),
            "expired reverse edges must not leak"
        );
    }
    let retained=turso.query("SELECT queue_id,request_id FROM fireweed_request_idempotency WHERE operation='claim_by_query' ORDER BY request_id",vec![]).await.unwrap();
    assert_eq!(retained.len(), 2);
    assert_eq!(
        retained[0].values[1],
        Value::Text("old-070".into()),
        "unexpired/lease-extended receipt survives"
    );
    assert_eq!(
        retained[1].values[0],
        Value::Text("foreign".into()),
        "other queue is untouched"
    );
}

#[tokio::test]
async fn delete_projection_clears_writer_cursor_and_allows_log_replay() {
    let def = definition();
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let dir = tempdir().unwrap();
    let turso = TursoRelational::open(TursoConfig::local(dir.path().join("projection.db")))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&turso, def.clone())
        .await
        .unwrap();
    let id = ItemId::mint(1, 1, 1);
    let item = indexed_item(id, "rebuild-key", "rebuild@example.com");
    let push = envelope(
        "rebuild-push",
        QueueCommand::Push(PushCommand {
            items: vec![item.clone()],
        }),
        vec![id],
        1,
    );
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 0)],
        vec![push.clone()],
    )
    .await
    .unwrap();
    assert!(
        turso
            .writer_recovery_high_water(&shard)
            .await
            .unwrap()
            .is_some()
    );
    turso.delete_projection().await.unwrap();
    assert!(
        turso
            .writer_recovery_high_water(&shard)
            .await
            .unwrap()
            .is_none(),
        "writer cursor must be empty after delete_projection"
    );
    let items = turso
        .query("SELECT COUNT(*) FROM fireweed_items", vec![])
        .await
        .unwrap();
    assert_eq!(
        items[0].values[0],
        Value::Integer(0),
        "serving reader must observe an empty item table after delete"
    );
    AsyncProjectionStore::ensure_shard(&turso, def)
        .await
        .unwrap();
    AsyncProjectionStore::apply_recovery(
        &turso,
        vec![CommandPosition::new(shard.clone(), 1, 0)],
        vec![push],
    )
    .await
    .expect("wiped projection must accept the original log replay");
    assert_eq!(
        turso
            .writer_recovery_high_water(&shard)
            .await
            .unwrap()
            .map(|position| position.sequence),
        Some(0)
    );
}
