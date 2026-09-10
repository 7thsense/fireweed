mod support;

use std::collections::BTreeMap;

use bytes::Bytes;
use fireweed_conformance::{envelope, item, qdef};
use fireweed_core::{ClientItemKey, GateKeyPolicy, ItemId, ItemState, LeaseToken};
use fireweed_engine::{
    AdvanceInstanceFenceCommand, AsyncProjectionStore, ClaimCommand, CommandPosition,
    FenceLeaseCommand, LeaseExpiredCommand, PauseQueueCommand, PayloadUpdate, PushCommand,
    QueueCommand, ReassignLeaseCommand, ReplacePendingCommand, ScheduleUpdate, SetGatesCommand,
    SideRecord, UnfenceLeaseCommand, UpdateFieldsCommand, WriteSideRecordsCommand,
};

use support::{Pair, lifecycle};

async fn gated_pair() -> Pair {
    let mut definition = qdef();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(4);
    definition.eligibility_policy.max_gates_per_request = Some(4);
    let shard =
        fireweed_engine::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let reference = fireweed_turso::TursoRelational::in_memory().await.unwrap();
    let turso = fireweed_turso::TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&reference, definition.clone())
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    Pair {
        reference,
        turso,
        shard,
    }
}

#[tokio::test]
async fn sqlite_and_turso_lifecycle_have_zero_observable_mismatch() {
    let pair = Pair::memory().await;
    let id = ItemId::new("101").unwrap();
    let expected = [
        ItemState::Pending,
        ItemState::Leased,
        ItemState::Leased,
        ItemState::Complete,
    ];
    for (sequence, (command, state)) in lifecycle(id).into_iter().zip(expected).enumerate() {
        pair.apply(sequence as u64, command).await;
        pair.assert_projection_image_and_reads_equal(&[id]).await;
        assert_eq!(
            AsyncProjectionStore::item_state(&pair.turso, pair.shard.clone(), id)
                .await
                .unwrap(),
            Some(state)
        );
    }
}

#[tokio::test]
async fn generated_rich_history_has_exact_projection_image_and_read_parity() {
    let pair = gated_pair().await;
    let original = ItemId::new("121").unwrap();
    let leased = ItemId::new("122").unwrap();
    let replacement = ItemId::new("123").unwrap();
    let mut original_item = item("121", "replace-key", 1);
    original_item.gate_keys = vec!["capacity".to_string()];
    pair.apply(
        0,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![original_item, item("122", "lease-key", 2)],
            }),
            vec![original, leased],
        ),
    )
    .await;
    pair.apply(
        1,
        envelope(
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["capacity".to_string()],
                blocked: true,
            }),
            vec![],
        ),
    )
    .await;
    pair.apply(
        2,
        envelope(
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("replace-key").unwrap(),
                superseded_item_id: original,
                replacement: item("123", "replace-key", 3),
            }),
            vec![original, replacement],
        ),
    )
    .await;
    pair.apply(
        3,
        envelope(
            QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id: replacement,
                field_ops: BTreeMap::from([(
                    "status".to_string(),
                    Some(Bytes::from_static(b"ready")),
                )]),
                payload: PayloadUpdate::Set(Some(Bytes::from_static(b"payload"))),
                set_priority: ScheduleUpdate::Keep,
                set_not_before: ScheduleUpdate::Keep,
                set_entity_document: None,
                set_fields: None,
                set_metadata: None,
                set_gate_keys: None,
                api001_batch: false,
                client_item_key: None,
                expected_item_version: None,
            }),
            vec![replacement],
        ),
    )
    .await;
    pair.apply(
        4,
        envelope(
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![leased],
                lease_token: LeaseToken::new("generated-lease").unwrap(),
                lease_expires_at: fireweed_conformance::ts(20),
                worker_id: None,
                authority_first: false,
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        5,
        envelope(
            QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: vec![leased],
                lease_token: LeaseToken::new("generated-reassign").unwrap(),
                lease_expires_at: fireweed_conformance::ts(30),
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        6,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![leased],
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        7,
        envelope(
            QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                item_ids: vec![leased],
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        8,
        envelope(
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![leased],
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        9,
        envelope(
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![SideRecord {
                    key: b"side".to_vec(),
                    payload: Bytes::from_static(b"value"),
                }],
            }),
            vec![],
        ),
    )
    .await;
    pair.apply(
        10,
        envelope(
            QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                instance_key: b"instance".to_vec(),
                expected: 0,
                next: 1,
            }),
            vec![],
        ),
    )
    .await;
    pair.apply(
        11,
        envelope(
            QueueCommand::PauseQueue(PauseQueueCommand { drain_intake: true }),
            vec![],
        ),
    )
    .await;
    pair.apply(12, envelope(QueueCommand::ResumeQueue, vec![]))
        .await;
    pair.assert_projection_image_and_reads_equal(&[original, leased, replacement])
        .await;
}

#[tokio::test]
async fn sqlite_and_turso_rollback_the_same_conflicting_batch_without_cursor_drift() {
    let pair = Pair::memory().await;
    let ids = [ItemId::new("111").unwrap(), ItemId::new("112").unwrap()];
    let command = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![
                item("111", "duplicate-active-key", 0),
                item("112", "duplicate-active-key", 1),
            ],
        }),
        ids.to_vec(),
    );
    let position = CommandPosition::new(pair.shard.clone(), 0, 0);
    let sqlite = AsyncProjectionStore::apply_live(
        &pair.reference,
        vec![position.clone()],
        vec![command.clone()],
    )
    .await
    .unwrap_err();
    let turso = AsyncProjectionStore::apply_live(&pair.turso, vec![position], vec![command])
        .await
        .unwrap_err();
    assert_eq!(
        std::mem::discriminant(&turso),
        std::mem::discriminant(&sqlite),
        "SQLite and Turso must return the same structured error class"
    );
    pair.assert_items_equal(&ids).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&pair.turso, pair.shard.clone())
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn fused_claim_mutation_matches_individual_replay_with_partial_claims() {
    use fireweed_conformance::ts;
    use fireweed_engine::{
        AddressedMutation, ItemMutationOperation, ItemMutationRequest, ItemMutationReturning,
        ItemPatch, LeaseGuard, LifecyclePatch,
    };
    let pair = Pair::memory().await;
    let ids: Vec<_> = ["901", "902", "903"]
        .into_iter()
        .map(|id| ItemId::new(id).unwrap())
        .collect();
    pair.apply(
        0,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("901", "a", 1),
                    item("902", "b", 2),
                    item("903", "c", 3),
                ],
            }),
            ids.clone(),
        ),
    )
    .await;
    let claims = vec![
        envelope(
            QueueCommand::Claim(
                ClaimCommand::new(
                    ids[..2].to_vec(),
                    LeaseToken::new("first").unwrap(),
                    ts(30),
                    None,
                )
                .with_authority_first(),
            ),
            ids[..2].to_vec(),
        ),
        envelope(
            QueueCommand::Claim(
                ClaimCommand::new(
                    vec![ids[2]],
                    LeaseToken::new("second").unwrap(),
                    ts(30),
                    None,
                )
                .with_authority_first(),
            ),
            vec![ids[2]],
        ),
    ];
    for (i, command) in claims.iter().enumerate() {
        AsyncProjectionStore::apply_live(
            &pair.reference,
            vec![CommandPosition::new(pair.shard.clone(), 0, i as u64 + 1)],
            vec![command.clone()],
        )
        .await
        .unwrap();
    }
    let request = ItemMutationRequest {
        request_id: fireweed_core::RequestId::new("fused").unwrap(),
        evaluated_at: ts(20),
        dry_run: false,
        returning: ItemMutationReturning::Identity,
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed {
            entries: vec![
                AddressedMutation {
                    item_id: ids[0],
                    expected_item_version: None,
                    predicates: vec![],
                    lease_guard: LeaseGuard::Match(LeaseToken::new("first").unwrap()),
                    patch: ItemPatch {
                        lifecycle: LifecyclePatch::SetPending,
                        payload: fireweed_engine::BatchUpdateValue::Replace(Some(
                            Bytes::from_static(b"enriched"),
                        )),
                        ..Default::default()
                    },
                },
                AddressedMutation {
                    item_id: ids[2],
                    expected_item_version: None,
                    predicates: vec![],
                    lease_guard: LeaseGuard::Match(LeaseToken::new("second").unwrap()),
                    patch: ItemPatch {
                        lifecycle: LifecyclePatch::SetFailed,
                        ..Default::default()
                    },
                },
            ],
        },
    };
    let plan = pair
        .reference
        .plan_addressed_item_mutation(&pair.shard, &qdef(), &request, &[])
        .await
        .unwrap();
    assert_eq!(plan.response.summary.changed, 2);
    let mutation = envelope(
        QueueCommand::MutateItems(plan.command),
        vec![ids[0], ids[2]],
    );
    AsyncProjectionStore::apply_live(
        &pair.reference,
        vec![CommandPosition::new(pair.shard.clone(), 0, 3)],
        vec![mutation.clone()],
    )
    .await
    .unwrap();
    let mut commands = claims;
    commands.push(mutation);
    let positions: Vec<_> = (1..=3)
        .map(|sequence| CommandPosition::new(pair.shard.clone(), 0, sequence))
        .collect();
    AsyncProjectionStore::apply_live(&pair.turso, positions.clone(), commands.clone())
        .await
        .unwrap();
    pair.assert_projection_image_and_reads_equal(&ids).await;
    // Replay of the already-covered combined prefix must not double-charge claims.
    AsyncProjectionStore::apply_live(&pair.turso, positions, commands.clone())
        .await
        .unwrap();
    pair.assert_projection_image_and_reads_equal(&ids).await;
    // A valid base version is insufficient: an authoritative claim must still
    // start Pending. Reject the whole transaction if a paired row is already leased.
    let QueueCommand::MutateItems(mut invalid_mutation) = commands[2].command.clone() else {
        unreachable!()
    };
    invalid_mutation.items.truncate(1);
    invalid_mutation.items[0].item_id = ids[1];
    let fireweed_engine::ResolvedItemMutationAction::Replace(values) =
        &mut invalid_mutation.items[0].action
    else {
        unreachable!()
    };
    values.item_version =
        AsyncProjectionStore::item_version(&pair.turso, pair.shard.clone(), ids[1])
            .await
            .unwrap()
            .unwrap()
            + 2;
    let invalid_claim = envelope(
        QueueCommand::Claim(
            ClaimCommand::new(
                vec![ids[1]],
                LeaseToken::new("illegal-reclaim").unwrap(),
                ts(40),
                None,
            )
            .with_authority_first(),
        ),
        vec![ids[1]],
    );
    assert!(
        AsyncProjectionStore::apply_live(
            &pair.turso,
            vec![
                CommandPosition::new(pair.shard.clone(), 0, 4),
                CommandPosition::new(pair.shard.clone(), 0, 5)
            ],
            vec![
                invalid_claim,
                envelope(QueueCommand::MutateItems(invalid_mutation), vec![ids[1]])
            ]
        )
        .await
        .is_err()
    );
    pair.assert_projection_image_and_reads_equal(&ids).await;
}

#[tokio::test]
async fn named_rowid_reads_use_full_keys_and_integer_ranges() {
    use fireweed_relational::{
        NAMED_ROWID_ENDPOINTS_SQL, NAMED_ROWID_SLICE_SQL, named_rowid_bounds_sql,
    };
    use turso::Value;
    let pair = Pair::memory().await;
    let keys = vec![
        Value::Text("t".into()),
        Value::Text("q".into()),
        Value::Text("1".into()),
        Value::Text("1000".into()),
    ];
    for (sql, parameters, expected) in [
        (
            NAMED_ROWID_ENDPOINTS_SQL.to_string(),
            keys.clone(),
            "item_id=?",
        ),
        (named_rowid_bounds_sql(2), keys, "item_id=?"),
        (
            NAMED_ROWID_SLICE_SQL.to_string(),
            vec![
                Value::Text("t".into()),
                Value::Text("q".into()),
                Value::Integer(1),
                Value::Integer(1000),
            ],
            "INTEGER PRIMARY KEY",
        ),
    ] {
        let rows = pair
            .turso
            .query(format!("EXPLAIN QUERY PLAN {sql}"), parameters)
            .await
            .unwrap();
        let plans: Vec<_> = rows
            .iter()
            .map(|row| match &row.values[3] {
                Value::Text(text) => text.as_str(),
                other => panic!("unexpected plan: {other:?}"),
            })
            .collect();
        assert!(
            plans.iter().any(|plan| plan.contains(expected)),
            "bounded lookup regressed: {plans:?}"
        );
        assert!(
            !plans
                .iter()
                .any(|plan| plan.contains("sqlite_autoindex_fireweed_items_1")
                    && !plan.contains("item_id=?")),
            "queue-prefix scan: {plans:?}"
        );
    }
}

#[tokio::test]
async fn addressed_payload_and_gate_writes_are_batched_with_exact_replacements() {
    use fireweed_conformance::ts;
    use fireweed_engine::{
        AddressedMutation, BatchUpdateValue, ItemMutationOperation, ItemMutationRequest,
        ItemMutationReturning, ItemPatch, LeaseGuard,
    };
    let pair = gated_pair().await;
    let mut definition = qdef();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(4);
    definition.eligibility_policy.max_gates_per_request = Some(4);
    let rows: Vec<_> = (1000..1100)
        .map(|i| {
            let mut row = item(&i.to_string(), &i.to_string(), i);
            row.gate_keys = vec!["old".into()];
            row.payload = Some(Bytes::from_static(b"old"));
            row
        })
        .collect();
    let ids: Vec<_> = rows.iter().map(|row| row.item_id).collect();
    pair.apply(
        0,
        envelope(QueueCommand::Push(PushCommand { items: rows }), ids.clone()),
    )
    .await;
    let request = ItemMutationRequest {
        request_id: fireweed_core::RequestId::new("batched-aux").unwrap(),
        evaluated_at: ts(20),
        dry_run: false,
        returning: ItemMutationReturning::Identity,
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed {
            entries: ids
                .iter()
                .enumerate()
                .map(|(i, id)| AddressedMutation {
                    item_id: *id,
                    expected_item_version: None,
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        payload: BatchUpdateValue::Replace(
                            (i % 2 == 0).then(|| Bytes::from_static(b"new")),
                        ),
                        gate_keys: fireweed_engine::GateKeyDelta {
                            add: if i % 2 == 0 {
                                vec!["new".into()]
                            } else {
                                vec![]
                            },
                            remove: vec!["old".into()],
                            remove_prefixes: vec![],
                        },
                        ..Default::default()
                    },
                })
                .collect(),
        },
    };
    let plan = pair
        .turso
        .plan_addressed_item_mutation(&pair.shard, &definition, &request, &[])
        .await
        .unwrap();
    assert_eq!(plan.response.summary.changed, 100);
    let command = plan.command;
    pair.apply(
        1,
        envelope(QueueCommand::MutateItems(command.clone()), ids.clone()),
    )
    .await;
    let shape = pair.turso.last_apply_statement_shape().unwrap();
    assert!(
        shape.write_statement_count < 2 * ids.len(),
        "payload/gate writes regressed to per-row calls: {shape:?}"
    );
    let payloads = pair
        .turso
        .query(
            "SELECT payload FROM fireweed_item_payloads ORDER BY item_id",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(payloads.len(), 100);
    for (i, row) in payloads.iter().enumerate() {
        assert_eq!(
            row.values[0],
            if i % 2 == 0 {
                turso::Value::Blob(b"new".to_vec())
            } else {
                turso::Value::Null
            }
        );
    }
    let gates = pair
        .turso
        .query("SELECT gate_key FROM fireweed_item_gates", vec![])
        .await
        .unwrap();
    assert_eq!(gates.len(), 50);
    assert!(
        gates
            .iter()
            .all(|row| row.values[0] == turso::Value::Text("new".into()))
    );
    let mut first = command.items[0].clone();
    let fireweed_engine::ResolvedItemMutationAction::Replace(values) = &mut first.action else {
        unreachable!()
    };
    values.item_version += 1;
    values.payload = Some(Bytes::from_static(b"middle"));
    let mut last = first.clone();
    let fireweed_engine::ResolvedItemMutationAction::Replace(values) = &mut last.action else {
        unreachable!()
    };
    values.item_version += 1;
    values.payload = Some(Bytes::from_static(b"last"));
    values.gate_keys = vec!["last".into()];
    let mut repeated = command;
    repeated.items = vec![first, last];
    pair.apply(
        2,
        envelope(QueueCommand::MutateItems(repeated), vec![ids[0], ids[0]]),
    )
    .await;
    let row = pair
        .turso
        .query(
            "SELECT payload FROM fireweed_item_payloads WHERE item_id='1000'",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(row[0].values[0], turso::Value::Blob(b"last".to_vec()));
    let row = pair
        .turso
        .query(
            "SELECT gate_key FROM fireweed_item_gates WHERE item_id='1000'",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].values[0], turso::Value::Text("last".into()));
    pair.assert_projection_image_and_reads_equal(&ids).await;
}

#[tokio::test]
async fn addressed_metadata_changes_omit_unchanged_payload_but_clear_and_replace_replay() {
    use fireweed_conformance::ts;
    use fireweed_engine::{
        AddressedMutation, BatchUpdateValue, ItemMutationOperation, ItemMutationRequest,
        ItemMutationReturning, ItemPatch, LeaseGuard, ResolvedItemMutationAction,
    };
    const READ_PAYLOAD: &str = "SELECT CASE WHEN p.item_id IS NULL THEN i.payload ELSE p.payload END \
        FROM fireweed_items i LEFT JOIN fireweed_item_payloads p \
        ON p.tenant_id=i.tenant_id AND p.queue_id=i.queue_id AND p.item_id=i.item_id";
    for legacy_inline_payload in [false, true] {
        let pair = Pair::memory().await;
        let definition = qdef();
        let mut pushed = item("1701", "preserve-payload", 1);
        let id = pushed.item_id;
        let body = Bytes::from(vec![0x5a; 64 * 1024]);
        pushed.payload = Some(body.clone());
        pair.apply(
            0,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![pushed],
                }),
                vec![id],
            ),
        )
        .await;
        if legacy_inline_payload {
            // Legacy projections kept the body on the main row. A keep action must
            // retain that body without requiring migration to the separate table.
            for store in [&pair.reference, &pair.turso] {
                store
                    .execute(
                        "UPDATE fireweed_items SET payload=?1",
                        vec![turso::Value::Blob(body.to_vec())],
                    )
                    .await
                    .unwrap();
                store
                    .execute("DELETE FROM fireweed_item_payloads", vec![])
                    .await
                    .unwrap();
            }
        }
        let mut last_command = None;
        for step in 0..5 {
            let payload = match step {
                1 => BatchUpdateValue::Replace(None),
                3 | 4 => BatchUpdateValue::Replace(Some(body.clone())),
                _ => BatchUpdateValue::Keep,
            };
            let mut metadata = fireweed_core::Metadata::default();
            metadata.insert(
                "step",
                fireweed_core::MetadataValue::String(step.to_string()),
            );
            let request = ItemMutationRequest {
                request_id: fireweed_core::RequestId::new(format!("preserve-{step}")).unwrap(),
                evaluated_at: ts(20 + step),
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed {
                    entries: vec![AddressedMutation {
                        item_id: id,
                        expected_item_version: None,
                        predicates: vec![],
                        lease_guard: LeaseGuard::RejectActive,
                        patch: ItemPatch {
                            payload,
                            metadata: BatchUpdateValue::Replace(metadata),
                            ..Default::default()
                        },
                    }],
                },
            };
            let plan = pair
                .turso
                .plan_addressed_item_mutation(&pair.shard, &definition, &request, &[])
                .await
                .unwrap();
            assert_eq!(plan.response.summary.changed, 1);
            assert_eq!(
                plan.command.items[0].action.keeps_payload(),
                matches!(step, 0 | 2 | 4)
            );
            let command = envelope(QueueCommand::MutateItems(plan.command.clone()), vec![id]);
            let encoded = fireweed_engine::command_codec::encode_log_batch(0, &[command]).unwrap();
            if step != 3 {
                assert!(
                    encoded.len() < 4096,
                    "unchanged/cleared body leaked into log: {}",
                    encoded.len()
                );
            }
            let (_, decoded) = fireweed_engine::command_codec::decode_log_batch(&encoded).unwrap();
            pair.apply(step as u64 + 1, decoded.into_iter().next().unwrap())
                .await;
            pair.assert_projection_image_and_reads_equal(&[id]).await;
            let rows = pair.turso.query(READ_PAYLOAD, vec![]).await.unwrap();
            let expected = if matches!(step, 1 | 2) {
                turso::Value::Null
            } else {
                turso::Value::Blob(body.to_vec())
            };
            assert_eq!(rows[0].values[0], expected);
            last_command = Some(plan.command);
        }
        // Repeated IDs require sequential semantics: the second action retains the
        // first action's new body, not the body from before the command.
        let mut command = last_command.unwrap();
        let mut first_values = command.items[0]
            .action
            .replacement_values()
            .unwrap()
            .clone();
        first_values.item_version += 1;
        first_values.payload = Some(Bytes::from_static(b"middle"));
        let mut last_values = first_values.clone();
        last_values.item_version += 1;
        last_values.payload = None;
        command.items = vec![
            fireweed_engine::ResolvedItemMutation {
                item_id: id,
                action: ResolvedItemMutationAction::Replace(Box::new(first_values)),
            },
            fireweed_engine::ResolvedItemMutation {
                item_id: id,
                action: ResolvedItemMutationAction::ReplaceKeepingPayload(Box::new(last_values)),
            },
        ];
        pair.apply(6, envelope(QueueCommand::MutateItems(command), vec![id]))
            .await;
        pair.assert_projection_image_and_reads_equal(&[id]).await;
        let rows = pair.turso.query(READ_PAYLOAD, vec![]).await.unwrap();
        assert_eq!(rows[0].values[0], turso::Value::Blob(b"middle".to_vec()));
    }
}
