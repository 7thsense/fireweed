use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fireweed_conformance::{envelope, item, qdef, ts};
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, Metadata, MetadataValue, PriorityValue, RequestId,
};
use fireweed_engine::{
    AsyncProjectionStore, BatchUpdateOutcome, BatchUpdateResponse, CommandEnvelope,
    CommandPosition, PayloadUpdate, PushCommand, QueueCommand, QueueKey, RequestOutcome,
    ScheduleUpdate, UpdateFieldsBatchCommand, UpdateFieldsCommand,
};
use fireweed_relational::{fields_from_json, metadata_from_json, parse_priority};
use fireweed_turso::{
    JournalMode, TURSO_SUPPORTED_BOUNDARY, TURSO_SUPPORTED_VERSION, TursoConfig, TursoRelational,
    verify_local_wal_benchmark_evidence,
};

#[tokio::test]
async fn turso_projection_full_shared_conformance() {
    let store = TursoRelational::in_memory().await.unwrap();
    fireweed_conformance::async_projection::run_full_async_projection_conformance(&store).await;
}

fn batch_fixture(
    count: usize,
) -> (
    Vec<fireweed_engine::PushItem>,
    Vec<ItemId>,
    Vec<UpdateFieldsCommand>,
) {
    let mut pushed = Vec::with_capacity(count);
    let mut ids = Vec::with_capacity(count);
    let mut updates = Vec::with_capacity(count);
    for index in 0..count {
        let id = ItemId::new((count as u64 * 10_000 + index as u64 + 1).to_string()).unwrap();
        let key = format!("batch-key-{count}-{index}");
        pushed.push(item(&id.to_string(), &key, index as i64));
        ids.push(id);
        updates.push(UpdateFieldsCommand {
            item_id: id,
            field_ops: BTreeMap::new(),
            payload: PayloadUpdate::Set(Some(Bytes::from(format!("payload-{index}")))),
            set_priority: ScheduleUpdate::Set(Some(PriorityValue::Int64(index as i64 + 1))),
            set_not_before: ScheduleUpdate::Set(Some(ts(100))),
            set_entity_document: None,
            set_fields: Some(BTreeMap::from([(
                "field".to_string(),
                Bytes::from(format!("value-{index}")),
            )])),
            set_metadata: Some(Metadata::default()),
            set_gate_keys: Some(vec![format!("gate-{index}")]),
            api001_batch: true,
            client_item_key: None,
            expected_item_version: None,
        });
    }
    (pushed, ids, updates)
}

async fn apply_measured_batch(count: usize) -> fireweed_turso::TursoBatchUpdateStatementShape {
    let mut definition = qdef();
    definition.max_push_batch_size = 1_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let (pushed, ids, updates) = batch_fixture(count);
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids.clone(),
        )],
    )
    .await
    .unwrap();

    let request_id = RequestId::new(format!("batch-request-{count}")).unwrap();
    let response = BatchUpdateResponse {
        request_id: request_id.clone(),
        results: ids
            .iter()
            .enumerate()
            .map(|(index, item_id)| BatchUpdateOutcome::Updated {
                item_id: *item_id,
                client_item_key: ClientItemKey::new(format!("batch-key-{count}-{index}")).unwrap(),
                item_version: 2,
            })
            .collect(),
    };
    let mut commands = updates
        .into_iter()
        .map(|update| {
            let item_id = update.item_id;
            envelope(QueueCommand::UpdateFields(update), vec![item_id])
        })
        .collect::<Vec<CommandEnvelope>>();
    commands[0].request_id = Some(request_id);
    commands[0].request_fingerprint = Some(42);
    commands[0].request_outcome = Some(RequestOutcome::BatchUpdate {
        response_payload: serde_json::to_string(&response).unwrap(),
    });
    let positions = (1..=count)
        .map(|sequence| CommandPosition::new(shard.clone(), 0, sequence as u64))
        .collect::<Vec<_>>();
    let replay_positions = positions.clone();
    let replay_commands = commands.clone();
    AsyncProjectionStore::apply_live(&store, positions, commands)
        .await
        .unwrap();
    let shape = store.last_batch_update_statement_shape().unwrap();
    for index in [0, count - 1] {
        let item = store
            .query(
                "SELECT item_version,fields,payload,metadata,priority,not_before,eligible_since \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    ids[index].to_string().into(),
                ],
            )
            .await
            .unwrap();
        let values = &item[0].values;
        assert_eq!(values[0], turso::Value::Integer(2));
        let turso::Value::Text(fields) = &values[1] else {
            panic!("fields were not text")
        };
        assert_eq!(
            fields_from_json(fields.clone()).unwrap().get("field"),
            Some(&Bytes::from(format!("value-{index}")))
        );
        let turso::Value::Blob(payload) = &values[2] else {
            panic!("payload was not a blob")
        };
        assert_eq!(payload, &Bytes::from(format!("payload-{index}")).to_vec());
        let turso::Value::Text(metadata) = &values[3] else {
            panic!("metadata was not text")
        };
        assert_eq!(
            metadata_from_json(metadata.clone()).unwrap(),
            Metadata::default()
        );
        let turso::Value::Text(priority) = &values[4] else {
            panic!("priority was not text")
        };
        assert_eq!(
            parse_priority(Some(priority.clone())).unwrap(),
            Some(PriorityValue::Int64(index as i64 + 1))
        );
        assert_eq!(values[5], turso::Value::Integer(100_000_000_000));
        assert_eq!(values[6], turso::Value::Integer(0));
        let gates = store
            .query(
                "SELECT gate_key FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 \
                 AND item_id=?3 ORDER BY gate_key",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    ids[index].to_string().into(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            gates[0].values,
            [turso::Value::Text(format!("gate-{index}"))]
        );
    }
    AsyncProjectionStore::apply_recovery(&store, replay_positions, replay_commands)
        .await
        .unwrap();
    let replayed_versions = store
        .query(
            "SELECT COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND item_version<>2",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(replayed_versions[0].values, [turso::Value::Integer(0)]);
    assert_eq!(store.last_batch_update_statement_shape(), None);
    shape
}

async fn apply_operation_shaped_batch(
    count: usize,
    schedule: bool,
    uniform: bool,
) -> (
    fireweed_turso::TursoBatchUpdateStatementShape,
    fireweed_turso::TursoApplyPhaseObservation,
) {
    let mut definition = qdef();
    definition.max_push_batch_size = 1_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let (pushed, ids, _) = batch_fixture(count);
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids.clone(),
        )],
    )
    .await
    .unwrap();

    let due = ts(100);
    let updates = (0..count)
        .map(|index| UpdateFieldsCommand {
            item_id: ItemId::from_u64(0),
            field_ops: BTreeMap::new(),
            payload: if schedule {
                PayloadUpdate::Keep
            } else {
                PayloadUpdate::Set(Some(Bytes::from(if uniform {
                    "profile-uniform".to_string()
                } else {
                    format!("profile-{index}")
                })))
            },
            set_priority: if schedule {
                ScheduleUpdate::Set(Some(PriorityValue::Int64(if uniform {
                    7
                } else {
                    index as i64 + 7
                })))
            } else {
                ScheduleUpdate::Keep
            },
            set_not_before: if schedule {
                ScheduleUpdate::Set(Some(due))
            } else {
                ScheduleUpdate::Keep
            },
            set_entity_document: None,
            set_fields: None,
            set_metadata: Some(Metadata::default()),
            set_gate_keys: None,
            api001_batch: true,
            client_item_key: Some(
                ClientItemKey::new(format!("batch-key-{count}-{index}")).unwrap(),
            ),
            expected_item_version: None,
        })
        .collect::<Vec<_>>();
    let request_id = RequestId::new(format!(
        "operation-shaped-{}-{}-{count}",
        if schedule { "schedule" } else { "enrich" },
        if uniform { "uniform" } else { "varying" }
    ))
    .unwrap();
    let response = BatchUpdateResponse {
        request_id: request_id.clone(),
        results: ids
            .iter()
            .enumerate()
            .map(|(index, _)| BatchUpdateOutcome::Updated {
                item_id: ItemId::from_u64(0),
                client_item_key: ClientItemKey::new(format!("batch-key-{count}-{index}")).unwrap(),
                item_version: 0,
            })
            .collect(),
    };
    let response_payload = serde_json::to_string(&response).unwrap();
    let mut command = envelope(
        QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
        ids.clone(),
    );
    command.request_id = Some(request_id.clone());
    command.request_fingerprint = Some(71);
    command.request_outcome = Some(RequestOutcome::BatchUpdate {
        response_payload: response_payload.clone(),
    });
    let position = CommandPosition::new(shard.clone(), 0, 1);
    AsyncProjectionStore::apply_live(&store, vec![position.clone()], vec![command.clone()])
        .await
        .unwrap();
    let shape = store.last_batch_update_statement_shape().unwrap();
    let phase = store.last_apply_phase_observation().unwrap();

    for index in [0, count - 1] {
        let rows = store
            .query(
                "SELECT item_version,lifecycle_state,payload,metadata,priority,not_before,eligible_since \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    ids[index].to_string().into(),
                ],
            )
            .await
            .unwrap();
        let values = &rows[0].values;
        assert_eq!(values[0], turso::Value::Integer(2));
        assert_eq!(values[1], turso::Value::Text("Pending".into()));
        if schedule {
            assert_eq!(values[2], turso::Value::Null);
            let turso::Value::Text(priority) = &values[4] else {
                panic!("priority was not text")
            };
            assert_eq!(
                parse_priority(Some(priority.clone())).unwrap(),
                Some(PriorityValue::Int64(if uniform {
                    7
                } else {
                    index as i64 + 7
                }))
            );
            assert_eq!(values[5], turso::Value::Integer(100_000_000_000));
            assert_eq!(values[6], turso::Value::Integer(0));
        } else {
            assert_eq!(
                values[2],
                turso::Value::Blob(
                    Bytes::from(if uniform {
                        "profile-uniform".to_string()
                    } else {
                        format!("profile-{index}")
                    })
                    .to_vec()
                )
            );
        }
    }
    let replay = store
        .query(
            "SELECT response_payload FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation='batch_update' AND request_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                request_id.as_str().to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(replay[0].values, [turso::Value::Text(response_payload)]);
    AsyncProjectionStore::apply_recovery(&store, vec![position], vec![command])
        .await
        .unwrap();
    let versions = store
        .query(
            "SELECT COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND item_version<>2",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(versions[0].values, [turso::Value::Integer(0)]);
    assert_eq!(store.last_batch_update_statement_shape(), None);
    (shape, phase)
}

#[tokio::test]
async fn turso_batch_update_apply_is_operation_shaped() {
    for count in [1, 100, 800] {
        for schedule in [false, true] {
            for uniform in [false, true] {
                let (shape, phase) = apply_operation_shaped_batch(count, schedule, uniform).await;
                let chunks = count.div_ceil(100);
                assert_eq!(shape.item_count, count);
                assert_eq!(
                    shape.broad_current_row_read_count, 0,
                    "operation-shaped apply materialized broad current rows"
                );
                assert!(shape.read_statement_count <= 2, "shape={shape:?}");
                assert!(
                    shape.statement_count <= chunks + 6,
                    "statement growth exceeded fixed overhead plus chunks: {shape:?}"
                );
                assert!(shape.max_bind_count <= 900, "shape={shape:?}");
                assert_eq!(phase.row_read_us, 0, "live cursor seed was not reused");
                assert!(phase.total_us >= phase.commit_us, "phase={phase:?}");
                eprintln!(
                    "count={count} schedule={schedule} uniform={uniform} shape={shape:?} phase={phase:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn turso_grouped_schedule_fast_path_preserves_summary_order_and_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("grouped-schedule.db");
    let mut definition = qdef();
    definition.max_push_batch_size = 10;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();

    let ids = (1..=4).map(ItemId::from_u64).collect::<Vec<_>>();
    let group = GroupKey::new("grouped-fast-path").unwrap();
    let mut pushed = [10_i64, 20, 30, 5]
        .into_iter()
        .enumerate()
        .map(|(index, priority)| {
            item(
                &ids[index].to_string(),
                &format!("grouped-{index}"),
                priority,
            )
        })
        .collect::<Vec<_>>();
    for item in &mut pushed[..3] {
        item.group_key = Some(group.clone());
    }
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids.clone(),
        )],
    )
    .await
    .unwrap();

    let update = |index: usize, priority: i64, not_before: i64, phase: &str| {
        let mut metadata = Metadata::default();
        metadata.insert("phase", MetadataValue::String(phase.to_string()));
        UpdateFieldsCommand {
            item_id: ItemId::from_u64(0),
            field_ops: BTreeMap::new(),
            payload: PayloadUpdate::Keep,
            set_priority: ScheduleUpdate::Set(Some(PriorityValue::Int64(priority))),
            set_not_before: ScheduleUpdate::Set(Some(ts(not_before))),
            set_entity_document: None,
            set_fields: None,
            set_metadata: Some(metadata),
            set_gate_keys: None,
            api001_batch: true,
            client_item_key: Some(ClientItemKey::new(format!("grouped-{index}")).unwrap()),
            expected_item_version: None,
        }
    };
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 1)],
        vec![envelope(
            QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
                updates: vec![
                    update(0, 40, 0, "reranked"),
                    update(2, 1, 0, "reranked"),
                    update(3, 2, 0, "reranked"),
                ],
            }),
            vec![ids[0], ids[2], ids[3]],
        )],
    )
    .await
    .unwrap();

    let summary = store
        .query(
            "SELECT eligible_item_count,rep_item_id,rep_created_seq \
             FROM fireweed_group_summary WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                group.as_str().to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        summary[0].values,
        [
            turso::Value::Integer(3),
            turso::Value::Text(ids[2].to_string()),
            turso::Value::Integer(2),
        ]
    );

    let fallback_update = envelope(
        QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
            updates: vec![update(2, 50, 0, "fallback")],
        }),
        vec![ids[2]],
    );
    let fallback_position = CommandPosition::new(shard.clone(), 0, 2);
    AsyncProjectionStore::apply_live(
        &store,
        vec![fallback_position.clone()],
        vec![fallback_update.clone()],
    )
    .await
    .unwrap();
    let summary = store
        .query(
            "SELECT eligible_item_count,rep_item_id,rep_created_seq \
             FROM fireweed_group_summary WHERE group_key=?1",
            vec![group.as_str().to_string().into()],
        )
        .await
        .unwrap();
    assert_eq!(
        summary[0].values,
        [
            turso::Value::Integer(3),
            turso::Value::Text(ids[1].to_string()),
            turso::Value::Integer(1),
        ]
    );
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(&store, shard.clone(), ts(0), 10)
            .await
            .unwrap(),
        vec![ids[3], ids[1], ids[0], ids[2]]
    );
    let changed = store
        .query(
            "SELECT item_version,priority,not_before,metadata FROM fireweed_items WHERE item_id=?1",
            vec![ids[2].to_string().into()],
        )
        .await
        .unwrap();
    assert_eq!(changed[0].values[0], turso::Value::Integer(3));
    assert_eq!(
        parse_priority(match &changed[0].values[1] {
            turso::Value::Text(value) => Some(value.clone()),
            value => panic!("unexpected priority: {value:?}"),
        })
        .unwrap(),
        Some(PriorityValue::Int64(50))
    );
    assert_eq!(changed[0].values[2], turso::Value::Integer(0));
    let turso::Value::Text(metadata) = &changed[0].values[3] else {
        panic!("metadata was not text")
    };
    let mut expected_metadata = Metadata::default();
    expected_metadata.insert("phase", MetadataValue::String("fallback".to_string()));
    assert_eq!(
        metadata_from_json(metadata.clone()).unwrap(),
        expected_metadata
    );

    for (sequence, priority, not_before, expected_count, expected_rep) in [
        (3_u64, 40, 100, 2_i64, ids[1]),
        (4, 40, 200, 2, ids[1]),
        (5, 0, 0, 3, ids[0]),
    ] {
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, sequence)],
            vec![envelope(
                QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
                    updates: vec![update(0, priority, not_before, "transition")],
                }),
                vec![ids[0]],
            )],
        )
        .await
        .unwrap();
        let transition_summary = store
            .query(
                "SELECT eligible_item_count,rep_item_id FROM fireweed_group_summary WHERE group_key=?1",
                vec![group.as_str().to_string().into()],
            )
            .await
            .unwrap();
        assert_eq!(
            transition_summary[0].values,
            [
                turso::Value::Integer(expected_count),
                turso::Value::Text(expected_rep.to_string()),
            ]
        );
    }

    // Recovery overlap remains a no-op after reopening an older summary row whose new sequence
    // column needs backfilling.
    store
        .execute(
            "UPDATE fireweed_group_summary SET rep_created_seq=NULL WHERE group_key=?1",
            vec![group.as_str().to_string().into()],
        )
        .await
        .unwrap();
    drop(store);
    let reopened = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::apply_recovery(&reopened, vec![fallback_position], vec![fallback_update])
        .await
        .unwrap();
    let reopened_summary = reopened
        .query(
            "SELECT rep_item_id,rep_created_seq FROM fireweed_group_summary WHERE group_key=?1",
            vec![group.as_str().to_string().into()],
        )
        .await
        .unwrap();
    assert_eq!(
        reopened_summary[0].values,
        [
            turso::Value::Text(ids[0].to_string()),
            turso::Value::Integer(0),
        ]
    );
    assert_eq!(
        AsyncProjectionStore::item_version(&reopened, shard, ids[2])
            .await
            .unwrap(),
        Some(3)
    );
}

#[tokio::test]
#[ignore = "file-backed 10k-row schedule index attribution"]
async fn turso_indexed_schedule_rewrite_profile() {
    const ROWS: usize = 10_000;
    const CHUNK: usize = 800;
    const PRE_FIX_ALL_INDEX_US: u64 = 4_052_016;
    const P3_RECOVERY_BUDGET_US: u64 = 1_261_830;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schedule-profile.db");
    let mut definition = qdef();
    definition.max_push_batch_size = ROWS as u64;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();

    let mut pushed = Vec::with_capacity(ROWS);
    let mut ids = Vec::with_capacity(ROWS);
    for index in 0..ROWS {
        let id = ItemId::from_u64(index as u64 + 1);
        let mut pushed_item = item(
            &id.to_string(),
            &format!("schedule-key-{index:05}"),
            index as i64,
        );
        pushed_item.payload = Some(Bytes::from(vec![b'x'; 1_024]));
        pushed_item.group_key = Some(GroupKey::new(format!("job-{}", index % 100)).unwrap());
        pushed.push(pushed_item);
        ids.push(id);
    }
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids.clone(),
        )],
    )
    .await
    .unwrap();

    async fn apply_schedule(
        store: &TursoRelational,
        shard: &QueueKey,
        ids: &[ItemId],
        start: usize,
        sequence: u64,
        label: &str,
    ) -> fireweed_turso::TursoApplyPhaseObservation {
        let updates = (start..start + CHUNK)
            .map(|index| UpdateFieldsCommand {
                item_id: ItemId::from_u64(0),
                field_ops: BTreeMap::new(),
                payload: PayloadUpdate::Keep,
                set_priority: ScheduleUpdate::Set(Some(PriorityValue::Int64(sequence as i64))),
                set_not_before: ScheduleUpdate::Set(Some(ts(0))),
                set_entity_document: None,
                set_fields: None,
                set_metadata: Some(Metadata::default()),
                set_gate_keys: None,
                api001_batch: true,
                client_item_key: Some(
                    ClientItemKey::new(format!("schedule-key-{index:05}")).unwrap(),
                ),
                expected_item_version: None,
            })
            .collect();
        AsyncProjectionStore::apply_live(
            store,
            vec![CommandPosition::new(shard.clone(), 0, sequence)],
            vec![envelope(
                QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
                ids[start..start + CHUNK].to_vec(),
            )],
        )
        .await
        .unwrap();
        let phase = store.last_apply_phase_observation().unwrap();
        eprintln!("schedule-index-profile label={label} phase={phase:?}");
        phase
    }

    let all = apply_schedule(&store, &shard, &ids, 0, 1, "all-indexes").await;
    store
        .execute("DROP INDEX fireweed_items_group_due_idx", vec![])
        .await
        .unwrap();
    let active_pending = apply_schedule(&store, &shard, &ids, CHUNK, 2, "active+pending").await;
    store
        .execute("DROP INDEX fireweed_items_active_scope_idx", vec![])
        .await
        .unwrap();
    let pending = apply_schedule(&store, &shard, &ids, CHUNK * 2, 3, "pending-only").await;
    store
        .execute("DROP INDEX fireweed_items_pending_order_idx", vec![])
        .await
        .unwrap();
    let base = apply_schedule(&store, &shard, &ids, CHUNK * 3, 4, "base-row").await;

    eprintln!(
        "schedule-index-attribution-us all={} group_due={} active_scope={} pending_order={} base={}",
        all.update_side_us,
        all.update_side_us
            .saturating_sub(active_pending.update_side_us),
        active_pending
            .update_side_us
            .saturating_sub(pending.update_side_us),
        pending.update_side_us.saturating_sub(base.update_side_us),
        base.update_side_us,
    );
    assert!(PRE_FIX_ALL_INDEX_US > P3_RECOVERY_BUDGET_US);
    assert!(
        all.total_us <= P3_RECOVERY_BUDGET_US,
        "all-index schedule rewrite exceeded the 634 item/s recovery budget: {all:?}"
    );
    assert!(
        all.cursor_definition_us > 0,
        "cursor phase was not recorded"
    );
    assert!(all.update_side_us > 0, "update phase was not recorded");
    assert!(all.commit_us > 0, "commit phase was not recorded");
    assert_eq!(
        store
            .query(
                "SELECT COUNT(*) FROM fireweed_items WHERE item_version=2",
                vec![],
            )
            .await
            .unwrap()[0]
            .values,
        [turso::Value::Integer((CHUNK * 4) as i64)]
    );
}

#[tokio::test]
async fn turso_batch_update_apply_preserves_conditional_fallbacks() {
    let mut definition = qdef();
    definition.max_push_batch_size = 100;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let (pushed, ids, _) = batch_fixture(4);
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids.clone(),
        )],
    )
    .await
    .unwrap();
    store
        .execute(
            "UPDATE fireweed_items SET lifecycle_state='Leased' \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                ids[2].to_string().into(),
            ],
        )
        .await
        .unwrap();
    store
        .execute(
            "UPDATE fireweed_items SET lifecycle_state='Complete' \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                ids[3].to_string().into(),
            ],
        )
        .await
        .unwrap();

    let replacement = UpdateFieldsCommand {
        item_id: ids[0],
        field_ops: BTreeMap::new(),
        payload: PayloadUpdate::Set(Some(Bytes::from_static(b"conditional-payload"))),
        set_priority: ScheduleUpdate::Set(Some(PriorityValue::Int64(44))),
        set_not_before: ScheduleUpdate::Set(Some(ts(200))),
        set_entity_document: None,
        set_fields: Some(BTreeMap::from([(
            "conditional".into(),
            Bytes::from_static(b"updated"),
        )])),
        set_metadata: Some(Metadata::default()),
        set_gate_keys: Some(vec!["conditional-gate".into()]),
        api001_batch: true,
        client_item_key: None,
        expected_item_version: Some(1),
    };
    let skipped = (1..4)
        .map(|index| UpdateFieldsCommand {
            item_id: ids[index],
            field_ops: BTreeMap::new(),
            payload: PayloadUpdate::Set(Some(Bytes::from_static(b"must-not-apply"))),
            set_priority: ScheduleUpdate::Keep,
            set_not_before: ScheduleUpdate::Keep,
            set_entity_document: None,
            set_fields: None,
            set_metadata: None,
            set_gate_keys: Some(Vec::new()),
            api001_batch: true,
            client_item_key: None,
            expected_item_version: (index == 1).then_some(99),
        })
        .collect::<Vec<_>>();
    let request_id = RequestId::new("conditional-fallbacks").unwrap();
    let response = BatchUpdateResponse {
        request_id: request_id.clone(),
        results: vec![
            BatchUpdateOutcome::Updated {
                item_id: ids[0],
                client_item_key: ClientItemKey::new("batch-key-4-0").unwrap(),
                item_version: 2,
            },
            BatchUpdateOutcome::Conflict,
            BatchUpdateOutcome::Invalid,
            BatchUpdateOutcome::Terminal,
        ],
    };
    let response_payload = serde_json::to_string(&response).unwrap();
    let mut updates = vec![replacement];
    updates.extend(skipped);
    let mut command = envelope(
        QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
        ids.clone(),
    );
    command.request_id = Some(request_id.clone());
    command.request_fingerprint = Some(72);
    command.request_outcome = Some(RequestOutcome::BatchUpdate {
        response_payload: response_payload.clone(),
    });
    let position = CommandPosition::new(shard.clone(), 0, 1);
    AsyncProjectionStore::apply_live(&store, vec![position.clone()], vec![command.clone()])
        .await
        .unwrap();

    let changed = store
        .query(
            "SELECT item_version,fields,payload,priority,not_before,eligible_since \
             FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                ids[0].to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(changed[0].values[0], turso::Value::Integer(2));
    let turso::Value::Text(fields) = &changed[0].values[1] else {
        panic!("fields were not text")
    };
    assert_eq!(
        fields_from_json(fields.clone()).unwrap().get("conditional"),
        Some(&Bytes::from_static(b"updated"))
    );
    assert_eq!(
        changed[0].values[2],
        turso::Value::Blob(b"conditional-payload".to_vec())
    );
    assert_eq!(changed[0].values[4], turso::Value::Integer(200_000_000_000));
    assert_eq!(changed[0].values[5], turso::Value::Integer(0));
    let gates = store
        .query(
            "SELECT gate_key FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 \
             AND item_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                ids[0].to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        gates[0].values,
        [turso::Value::Text("conditional-gate".into())]
    );
    let untouched = store
        .query(
            "SELECT item_id,item_version,lifecycle_state,payload FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id IN (?3,?4,?5) ORDER BY item_id",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                ids[1].to_string().into(),
                ids[2].to_string().into(),
                ids[3].to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(untouched.len(), 3);
    for row in &untouched {
        assert_eq!(row.values[1], turso::Value::Integer(1));
        assert_eq!(row.values[3], turso::Value::Null);
    }
    assert_eq!(untouched[0].values[2], turso::Value::Text("Pending".into()));
    assert_eq!(untouched[1].values[2], turso::Value::Text("Leased".into()));
    assert_eq!(
        untouched[2].values[2],
        turso::Value::Text("Complete".into())
    );
    let replay = store
        .query(
            "SELECT response_payload FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation='batch_update' AND request_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                request_id.as_str().to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(replay[0].values, [turso::Value::Text(response_payload)]);
    AsyncProjectionStore::apply_recovery(&store, vec![position], vec![command])
        .await
        .unwrap();
    let version = store
        .query(
            "SELECT item_version FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
                ids[0].to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(version[0].values, [turso::Value::Integer(2)]);
}

#[tokio::test]
async fn turso_batch_update_statement_shape_is_bind_bounded() {
    for count in [1, 100, 1_000] {
        let shape = apply_measured_batch(count).await;
        assert_eq!(shape.item_count, count);
        assert!(
            shape.max_bind_count <= 900,
            "{} binds exceeded the explicit 900-bind boundary at {count} items",
            shape.max_bind_count
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn turso_projection_keeps_single_thread_heartbeat_live() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>(_: T) {}
    assert_send_sync::<TursoRelational>();

    let mut definition = qdef();
    definition.max_push_batch_size = 1_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = Arc::new(TursoRelational::in_memory().await.unwrap());
    AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
        .await
        .unwrap();
    let (pushed, ids, _) = batch_fixture(1_000);
    let future = AsyncProjectionStore::apply_live(
        store.as_ref(),
        vec![CommandPosition::new(shard, 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids,
        )],
    );
    assert_send(future);

    let finished = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicUsize::new(0));
    let heartbeat_finished = finished.clone();
    let heartbeat_ticks = ticks.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        while !heartbeat_finished.load(Ordering::Acquire) {
            interval.tick().await;
            heartbeat_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    while ticks.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let ticks_before_apply = ticks.load(Ordering::Relaxed);
    let apply_store = store.clone();
    let apply_finished = finished.clone();
    let apply = tokio::spawn(async move {
        let (pushed, ids, _) = batch_fixture(1_000);
        let result = AsyncProjectionStore::apply_live(
            apply_store.as_ref(),
            vec![CommandPosition::new(
                QueueKey::new(qdef().tenant_id, qdef().queue_id),
                0,
                0,
            )],
            vec![envelope(
                QueueCommand::Push(PushCommand { items: pushed }),
                ids,
            )],
        )
        .await;
        apply_finished.store(true, Ordering::Release);
        result
    });
    tokio::time::timeout(Duration::from_secs(15), apply)
        .await
        .expect("Turso apply exceeded heartbeat deadline")
        .unwrap()
        .unwrap();
    heartbeat.await.unwrap();
    assert!(ticks.load(Ordering::Relaxed) > ticks_before_apply);
}

fn benchmark_evidence() -> serde_json::Value {
    serde_json::json!({
        "turso_version": TURSO_SUPPORTED_VERSION,
        "turso_features": ["local"],
        "boundary": TURSO_SUPPORTED_BOUNDARY,
        "batch_sizes": [1, 100, 1000],
        "operations_per_second": 1.0,
        "p50_us": 1.0,
        "p95_us": 2.0,
        "p99_us": 3.0,
        "database_bytes": 1.0,
        "cpu_time_ms": 1.0,
        "peak_rss_bytes": 1.0,
        "excluded_time": {"cold_open": true, "fixture_generation": true},
        "regression_limits": {
            "min_operations_per_second_ratio": 0.8,
            "max_p99_ratio": 1.25
        }
    })
}

#[test]
fn turso_local_wal_benchmark_evidence_verifier() {
    let valid = benchmark_evidence();
    verify_local_wal_benchmark_evidence(&valid).unwrap();
    let baseline: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/turso-local-wal-baseline.json")).unwrap();
    verify_local_wal_benchmark_evidence(&baseline).unwrap();
    for field in [
        "batch_sizes",
        "operations_per_second",
        "p50_us",
        "p95_us",
        "p99_us",
        "database_bytes",
        "cpu_time_ms",
        "peak_rss_bytes",
        "turso_version",
        "turso_features",
        "boundary",
        "regression_limits",
    ] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            verify_local_wal_benchmark_evidence(&missing).is_err(),
            "missing {field} was accepted"
        );
    }
    for field in [
        "operations_per_second",
        "p50_us",
        "p95_us",
        "p99_us",
        "database_bytes",
        "cpu_time_ms",
        "peak_rss_bytes",
    ] {
        let mut nonpositive = valid.clone();
        nonpositive[field] = serde_json::json!(0);
        assert!(
            verify_local_wal_benchmark_evidence(&nonpositive).is_err(),
            "nonpositive {field} was accepted"
        );
    }
    let mut zero_batch = valid.clone();
    zero_batch["batch_sizes"] = serde_json::json!([1, 0, 1000]);
    assert!(verify_local_wal_benchmark_evidence(&zero_batch).is_err());
    for (field, invalid) in [
        ("turso_version", serde_json::json!("0.8.0")),
        ("turso_features", serde_json::json!(["local", "sync"])),
        ("boundary", serde_json::json!("embedded_replica")),
    ] {
        let mut evidence = valid.clone();
        evidence[field] = invalid;
        assert!(
            verify_local_wal_benchmark_evidence(&evidence).is_err(),
            "invalid {field} was accepted"
        );
    }
    for field in ["min_operations_per_second_ratio", "max_p99_ratio"] {
        let mut evidence = valid.clone();
        evidence["regression_limits"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            verify_local_wal_benchmark_evidence(&evidence).is_err(),
            "missing regression limit {field} was accepted"
        );
    }
}

fn process_cpu_ms() -> f64 {
    let stat = fs::read_to_string("/proc/self/stat").expect("Linux /proc is required by this cut");
    let fields = stat
        .rsplit_once(')')
        .expect("valid /proc/self/stat command field")
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let ticks = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    let ticks_per_second = Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(100.0);
    ticks as f64 * 1_000.0 / ticks_per_second
}

fn peak_rss_bytes() -> u64 {
    fs::read_to_string("/proc/self/status")
        .expect("Linux /proc is required by this cut")
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .expect("VmHWM is present")
        * 1_024
}

fn local_database_bytes(directory: &Path, stem: &str) -> u64 {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(stem))
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

#[tokio::test]
async fn turso_local_wal_benchmark_smoke_produces_verifiable_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("projection.db");
    let mut definition = qdef();
    definition.max_push_batch_size = 2_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());

    // Cold open and fixture construction are intentionally complete before either clock starts.
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let mut pushed = Vec::with_capacity(1_101);
    let mut batches = Vec::new();
    for count in [1, 100, 1_000] {
        let (items, _, updates) = batch_fixture(count);
        pushed.extend(items);
        batches.push(updates);
    }
    let pushed_ids = pushed.iter().map(|item| item.item_id).collect::<Vec<_>>();
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            pushed_ids,
        )],
    )
    .await
    .unwrap();

    let cpu_start = process_cpu_ms();
    let total_start = Instant::now();
    let mut next_sequence = 1_u64;
    let mut per_operation_us = Vec::new();
    for updates in batches {
        let count = updates.len();
        let commands = updates
            .into_iter()
            .map(|update| {
                let item_id = update.item_id;
                envelope(QueueCommand::UpdateFields(update), vec![item_id])
            })
            .collect::<Vec<_>>();
        let positions = (0..count)
            .map(|offset| CommandPosition::new(shard.clone(), 0, next_sequence + offset as u64))
            .collect();
        let batch_start = Instant::now();
        AsyncProjectionStore::apply_live(&store, positions, commands)
            .await
            .unwrap();
        let elapsed_us = batch_start.elapsed().as_secs_f64() * 1_000_000.0;
        per_operation_us.push(elapsed_us / count as f64);
        next_sequence += count as u64;
    }
    let total = total_start.elapsed();
    let cpu_time_ms = process_cpu_ms() - cpu_start;
    per_operation_us.sort_by(f64::total_cmp);
    let evidence = serde_json::json!({
        "turso_version": TURSO_SUPPORTED_VERSION,
        "turso_features": ["local"],
        "boundary": TURSO_SUPPORTED_BOUNDARY,
        "batch_sizes": [1, 100, 1000],
        "operations_per_second": 1101.0 / total.as_secs_f64(),
        "p50_us": per_operation_us[1],
        "p95_us": per_operation_us[2],
        "p99_us": per_operation_us[2],
        "database_bytes": local_database_bytes(directory.path(), "projection.db"),
        "cpu_time_ms": cpu_time_ms,
        "peak_rss_bytes": peak_rss_bytes(),
        "excluded_time": {"cold_open": true, "fixture_generation": true},
        "regression_limits": {
            "min_operations_per_second_ratio": 0.8,
            "max_p99_ratio": 1.25
        },
        "measurement": {
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "latency_unit": "microseconds_per_updated_item",
            "database_bytes_include": ["main", "wal", "shm"],
            "cpu_source": "/proc/self/stat",
            "rss_source": "/proc/self/status VmHWM"
        }
    });
    verify_local_wal_benchmark_evidence(&evidence).unwrap();
    println!("{}", serde_json::to_string_pretty(&evidence).unwrap());
}

#[tokio::test]
async fn unqualified_mvcc_mode_fails_closed() {
    let error =
        TursoRelational::open(TursoConfig::in_memory().with_journal_mode(JournalMode::Mvcc))
            .await
            .err()
            .expect("MVCC must remain outside the qualified boundary");
    assert!(error.to_string().contains("MVCC is unsupported"));
}
