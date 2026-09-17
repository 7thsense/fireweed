#![cfg(feature = "local")]

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use fireweed_conformance::{envelope, item, qdef, ts};
use fireweed_core::{
    CohortId, CohortOnIncomplete, CohortPolicy, GateKeyPolicy, GroupKey, IndexDeclaration,
    IndexDef, IndexType, ItemId, ItemState, LeaseToken, Metadata, MetadataValue, QueueDefinition,
    QueueId, QueueIndex, RequestId, TypedValue,
};
use fireweed_engine::{
    AddressedMutation, AsyncProjectionStore, BatchUpdateValue, ClaimCommand, CohortClaimCommand,
    CommandPosition, CommitOutcomeEntry, EntityEdit, EntityEditOperation, EntityPredicateValue,
    FenceLeaseCommand, GateChange, ItemMutationOperation, ItemMutationOutcome,
    ItemMutationPrecondition, ItemMutationRequest, ItemMutationReturning, ItemPatch, ItemPredicate,
    ItemSelector, ItemSelectorScope, LeaseGuard, PauseQueueCommand, PushCommand, QueueCommand,
    QueueKey, RequestOutcome, SelectedMutation, SetGatesCommand, SideRecord,
    UpdateFieldsBatchCommand, UpdateFieldsCommand, WriteSideRecordsCommand,
};
use fireweed_turso::{TursoConfig, TursoRelational};
use serde_json::json;

struct Fixture {
    store: TursoRelational,
    definition: QueueDefinition,
    shard: QueueKey,
    sequence: u64,
}

impl Fixture {
    async fn new(mut definition: QueueDefinition) -> Self {
        definition.emit_change_records = false;
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let store = TursoRelational::in_memory().await.unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition.clone())
            .await
            .unwrap();
        Self {
            store,
            definition,
            shard,
            sequence: 0,
        }
    }

    async fn apply(&mut self, command: QueueCommand) {
        AsyncProjectionStore::apply_live(
            &self.store,
            vec![CommandPosition::new(self.shard.clone(), 4, self.sequence)],
            vec![envelope(command, vec![])],
        )
        .await
        .unwrap();
        self.sequence += 1;
    }
}

fn id(value: u64) -> ItemId {
    ItemId::new(value.to_string()).unwrap()
}

fn indexed_definition(field: &str, unique: bool) -> QueueDefinition {
    let mut definition = qdef();
    definition.typed_indexes = vec![QueueIndex {
        name: format!("by_{field}"),
        declaration: IndexDeclaration::Single(IndexDef {
            field: field.into(),
            index_type: IndexType::String,
            unique,
        }),
    }];
    definition
}

fn request(operation: ItemMutationOperation) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new("mutation-parity").unwrap(),
        evaluated_at: ts(10),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation,
    }
}

fn addressed(item_id: ItemId, patch: ItemPatch) -> ItemMutationRequest {
    request(ItemMutationOperation::Addressed {
        entries: vec![AddressedMutation {
            item_id,
            expected_item_version: None,
            predicates: vec![],
            lease_guard: LeaseGuard::RejectActive,
            patch,
        }],
    })
}

fn entity_patch(pointer: &str, value: serde_json::Value) -> ItemPatch {
    ItemPatch {
        entity_edits: vec![EntityEdit {
            pointer: pointer.into(),
            operation: EntityEditOperation::Set(value),
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn selectors_preserve_first_match_fifo_snapshots_and_compact_entities() {
    let mut fixture = Fixture::new(indexed_definition("kind", false)).await;
    let items = [(1, "red"), (2, "blue"), (3, "red")]
        .into_iter()
        .map(|(n, kind)| {
            let mut row = item(&n.to_string(), &format!("key-{n}"), 10 - n);
            row.index_fields
                .insert("kind".into(), TypedValue::String(kind.into()));
            row.payload = Some(Bytes::from(vec![n as u8; 5000]));
            row
        })
        .collect();
    fixture
        .apply(QueueCommand::Push(PushCommand { items }))
        .await;
    let mut metadata = Metadata::new();
    metadata.insert("stage", MetadataValue::String("scheduled".into()));
    let select = request(ItemMutationOperation::SelectFirst {
        clauses: vec![
            SelectedMutation {
                selector_id: "red-owner".into(),
                selector: ItemSelector {
                    scope: ItemSelectorScope::Live,
                    predicates: vec![ItemPredicate::EntityEq {
                        pointer: "/kind".into(),
                        value: EntityPredicateValue::Value(json!("red")),
                    }],
                },
                predicates: vec![ItemPredicate::MetadataEq {
                    name: "stage".into(),
                    value: Some(MetadataValue::String("ready".into())),
                }],
                lease_guard: LeaseGuard::RejectActive,
                patch: ItemPatch {
                    metadata: BatchUpdateValue::Replace(metadata.clone()),
                    ..Default::default()
                },
            },
            SelectedMutation {
                selector_id: "fallback".into(),
                selector: ItemSelector {
                    scope: ItemSelectorScope::Live,
                    predicates: vec![],
                },
                predicates: vec![],
                lease_guard: LeaseGuard::RejectActive,
                patch: ItemPatch {
                    metadata: BatchUpdateValue::Replace(metadata),
                    ..Default::default()
                },
            },
        ],
    });
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &select, &[])
        .await
        .unwrap();
    assert_eq!(
        plan.response
            .results
            .iter()
            .map(|row| row.item_id)
            .collect::<Vec<_>>(),
        vec![id(1), id(2), id(3)]
    );
    assert_eq!(plan.response.summary.changed, 1);
    assert_eq!(plan.response.summary.rejected, 2);
    assert_eq!(plan.response.selectors[0].matched, 2);
    assert_eq!(
        plan.response.results[0].outcome,
        ItemMutationOutcome::PreconditionFailed(ItemMutationPrecondition::Predicate)
    );
    assert_eq!(
        plan.response.results[0].before.as_ref().unwrap().entity,
        Some(json!({"kind":"red"}))
    );
    assert_eq!(
        plan.response.results[1]
            .before
            .as_ref()
            .unwrap()
            .payload
            .as_ref()
            .unwrap()
            .len(),
        5000
    );
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    let repeat = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &select, &[])
        .await
        .unwrap();
    assert_eq!(repeat.response.summary.changed, 0);
    assert_eq!(
        repeat.response.results[1].outcome,
        ItemMutationOutcome::NoChange
    );
}

#[tokio::test]
async fn addressed_unique_validation_sees_unaddressed_rows_and_root_removal_releases_key() {
    let mut fixture = Fixture::new(indexed_definition("email", true)).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: (1..=2)
                .map(|n| {
                    let mut row = item(&n.to_string(), &format!("key-{n}"), n);
                    row.index_fields.insert(
                        "email".into(),
                        TypedValue::String(format!("{n}@example.com")),
                    );
                    row
                })
                .collect(),
        }))
        .await;
    let conflict = addressed(id(1), entity_patch("/email", json!("2@example.com")));
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &conflict, &[])
        .await
        .unwrap();
    assert_eq!(
        plan.response.results[0].outcome,
        ItemMutationOutcome::Invalid
    );
    assert!(plan.command.items.is_empty());
    let remove = addressed(
        id(2),
        ItemPatch {
            entity_edits: vec![EntityEdit {
                pointer: String::new(),
                operation: EntityEditOperation::Remove,
            }],
            ..Default::default()
        },
    );
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &remove, &[])
        .await
        .unwrap();
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    let absent = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &remove, &[])
        .await
        .unwrap();
    assert!(
        absent.response.results[0]
            .before
            .as_ref()
            .unwrap()
            .entity
            .is_none()
    );
    assert_eq!(
        absent.response.results[0].outcome,
        ItemMutationOutcome::NoChange
    );
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &conflict, &[])
        .await
        .unwrap();
    assert_eq!(plan.response.summary.changed, 1);
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    let rows = fixture
        .store
        .query(
            "SELECT COUNT(*) FROM fireweed_item_index WHERE tenant_id=?1 AND queue_id=?2",
            vec![
                fixture.shard.tenant_id.as_str().into(),
                fixture.shard.queue_id.as_str().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].values, vec![turso::Value::Integer(1)]);
}

#[tokio::test]
async fn competing_sibling_unique_edits_fail_without_changing_projection() {
    let mut fixture = Fixture::new(indexed_definition("email", true)).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: (1..=2)
                .map(|n| {
                    let mut row = item(&n.to_string(), &format!("key-{n}"), n);
                    row.entity_document = Some(json!({"email":format!("{n}@example.com")}));
                    row
                })
                .collect(),
        }))
        .await;
    let mutation = request(ItemMutationOperation::Addressed {
        entries: (1..=2)
            .map(|n| AddressedMutation {
                item_id: id(n),
                expected_item_version: Some(1),
                predicates: vec![],
                lease_guard: LeaseGuard::RejectActive,
                patch: entity_patch("/email", json!("shared@example.com")),
            })
            .collect(),
    });
    assert!(matches!(
        fixture
            .store
            .plan_item_mutation(&fixture.shard, &fixture.definition, &mutation, &[])
            .await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    let updates = (1..=2)
        .map(|n| fireweed_engine::BoundedMutationUpdate {
            expected_item_version: 1,
            command: UpdateFieldsCommand {
                item_id: id(n),
                set_entity_document: Some(json!({"email":"shared@example.com"})),
                ..Default::default()
            },
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        fixture
            .store
            .server_validate_bounded_updates(&fixture.shard, &fixture.definition, &updates)
            .await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    for n in 1..=2 {
        let unchanged = addressed(id(n), ItemPatch::default());
        let observed = fixture
            .store
            .plan_item_mutation(&fixture.shard, &fixture.definition, &unchanged, &[])
            .await
            .unwrap();
        let before = observed.response.results[0].before.as_ref().unwrap();
        assert_eq!(before.item_version, 1);
        assert_eq!(
            before.entity,
            Some(json!({"email":format!("{n}@example.com")}))
        );
    }
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&fixture.store, fixture.shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(fixture.shard.clone(), 4, 0))
    );
}

#[tokio::test]
async fn schema_validation_covers_real_and_dry_run_mutations_without_sql_writes() {
    let mut definition = qdef();
    definition.entity_schema = Some(
        serde_json::from_value(json!({"entity_schema": {
            "type":"object", "required":["score"], "properties":{"score":{"type":"integer"}}
        }}))
        .unwrap(),
    );
    let mut fixture = Fixture::new(definition).await;
    let mut row = item("1", "schema", 1);
    row.entity_document = Some(json!({"score":1}));
    fixture
        .apply(QueueCommand::Push(PushCommand { items: vec![row] }))
        .await;
    for dry_run in [false, true] {
        let mut invalid = addressed(id(1), entity_patch("/score", json!("invalid")));
        invalid.dry_run = dry_run;
        assert!(matches!(
            fixture
                .store
                .plan_item_mutation(&fixture.shard, &fixture.definition, &invalid, &[])
                .await,
            Err(fireweed_engine::EngineError::EntitySchemaViolation(_))
        ));
    }
    let mut valid = addressed(id(1), entity_patch("/score", json!(2)));
    valid.dry_run = true;
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &valid, &[])
        .await
        .unwrap();
    assert!(plan.response.dry_run);
    assert_eq!(
        plan.response.results[0].outcome,
        ItemMutationOutcome::WouldUpdate {
            item_version: 2,
            state: ItemState::Pending
        }
    );
    assert!(plan.command.items.is_empty());
    valid.dry_run = false;
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &valid, &[])
        .await
        .unwrap();
    assert_eq!(
        plan.response.results[0].before.as_ref().unwrap().entity,
        Some(json!({"score":1}))
    );
}

#[tokio::test]
async fn pending_claim_tail_is_visible_to_selectors_without_double_version_increment() {
    let mut fixture = Fixture::new(indexed_definition("kind", false)).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: vec![item("1", "claimed", 1)],
        }))
        .await;
    let claim = ClaimCommand {
        item_ids: vec![id(1)],
        lease_token: LeaseToken::new("tail").unwrap(),
        lease_expires_at: ts(100),
        worker_id: None,
        authority_first: true,
    };
    let mut mutation = addressed(id(1), ItemPatch::default());
    if let ItemMutationOperation::Addressed { entries } = &mut mutation.operation {
        entries[0].lease_guard = LeaseGuard::Match(claim.lease_token.clone());
        entries[0].expected_item_version = Some(2);
    }
    let before_apply = fixture
        .store
        .plan_item_mutation(
            &fixture.shard,
            &fixture.definition,
            &mutation,
            std::slice::from_ref(&claim),
        )
        .await
        .unwrap();
    assert_eq!(
        before_apply.response.results[0].outcome,
        ItemMutationOutcome::NoChange
    );
    assert_eq!(
        before_apply.response.results[0]
            .before
            .as_ref()
            .unwrap()
            .attempt_count,
        1
    );
    fixture.apply(QueueCommand::Claim(claim.clone())).await;
    let after_apply = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &mutation, &[claim])
        .await
        .unwrap();
    assert_eq!(after_apply.response, before_apply.response);
}

#[tokio::test]
async fn reopened_cohort_mutation_preserves_bearer_and_invalidates_only_addressed_lease() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("projection.db");
    let mut definition = indexed_definition("kind", false);
    definition.cohort_policy = Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(30000),
        on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(2),
    });
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition.clone())
        .await
        .unwrap();
    let group = GroupKey::new("cohort").unwrap();
    let rows = (1..=2)
        .map(|n| {
            let mut row = item(&n.to_string(), &format!("key-{n}"), n);
            row.group_key = Some(group.clone());
            row.cohort_size = Some(2);
            row.gate_keys = vec!["capacity".into()];
            row
        })
        .collect();
    let cohort = CohortId::new("coh:cohort:0").unwrap();
    let token = LeaseToken::new("cohort-token").unwrap();
    let commands = vec![
        envelope(QueueCommand::Push(PushCommand { items: rows }), vec![]),
        envelope(
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["capacity".into()],
                blocked: true,
            }),
            vec![],
        ),
        envelope(
            QueueCommand::CohortClaim(CohortClaimCommand {
                cohort_id: cohort,
                item_ids: vec![id(1), id(2)],
                lease_token: token.clone(),
                lease_expires_at: ts(100),
            }),
            vec![id(1), id(2)],
        ),
    ];
    AsyncProjectionStore::apply_live(
        &store,
        (0..3)
            .map(|sequence| CommandPosition::new(shard.clone(), 4, sequence))
            .collect(),
        commands,
    )
    .await
    .unwrap();
    drop(store);
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    let mut mutation = addressed(id(1), ItemPatch::default());
    if let ItemMutationOperation::Addressed { entries } = &mut mutation.operation {
        entries[0].lease_guard = LeaseGuard::InvalidateActive;
    }
    mutation.gate_changes = vec![GateChange {
        gate_keys: vec!["capacity".into()],
        blocked: false,
    }];
    let plan = store
        .plan_item_mutation(&shard, &definition, &mutation, &[])
        .await
        .unwrap();
    let before = plan.response.results[0].before.as_ref().unwrap();
    assert!(before.lease_is_cohort);
    assert_eq!(before.lease_token, Some(token.clone()));
    assert_eq!(before.gate_keys, vec!["capacity"]);
    assert_eq!(
        plan.response.results[0].outcome,
        ItemMutationOutcome::Updated {
            item_version: 3,
            state: ItemState::Pending
        }
    );
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 4, 3)],
        vec![envelope(
            QueueCommand::MutateItems(plan.command),
            vec![id(1)],
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(&store, shard.clone(), id(1))
            .await
            .unwrap(),
        Some(ItemState::Pending)
    );
    let peer = AsyncProjectionStore::render_claimed(&store, shard, vec![id(2)])
        .await
        .unwrap();
    assert_eq!(peer[0].lease_token, Some(token));
}

#[tokio::test]
async fn scalar_validator_preserves_leased_semantics_and_cross_row_uniqueness() {
    let mut fixture = Fixture::new(indexed_definition("email", true)).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: (1..=2)
                .map(|n| {
                    let mut row = item(&n.to_string(), &format!("key-{n}"), n);
                    row.entity_document = Some(json!({"email":format!("{n}@example.com")}));
                    row
                })
                .collect(),
        }))
        .await;
    let unchanged = UpdateFieldsCommand {
        item_id: id(1),
        ..Default::default()
    };
    assert_eq!(
        fixture
            .store
            .server_validate_update_fields(&fixture.shard, &fixture.definition, &unchanged, Some(1))
            .await
            .unwrap(),
        1
    );
    fixture
        .apply(QueueCommand::UpdateFields(unchanged.clone()))
        .await;
    assert_eq!(
        fixture
            .store
            .server_validate_update_fields(&fixture.shard, &fixture.definition, &unchanged, Some(2))
            .await
            .unwrap(),
        2
    );
    fixture
        .apply(QueueCommand::Claim(ClaimCommand {
            item_ids: vec![id(1)],
            lease_token: LeaseToken::new("scalar").unwrap(),
            lease_expires_at: ts(100),
            worker_id: None,
            authority_first: false,
        }))
        .await;
    assert_eq!(
        fixture
            .store
            .server_validate_update_fields(&fixture.shard, &fixture.definition, &unchanged, Some(3))
            .await
            .unwrap(),
        3
    );
    let conflict = UpdateFieldsCommand {
        set_entity_document: Some(json!({"email":"2@example.com"})),
        ..unchanged.clone()
    };
    assert!(matches!(
        fixture
            .store
            .server_validate_update_fields(&fixture.shard, &fixture.definition, &conflict, None)
            .await,
        Err(fireweed_engine::EngineError::Conflict)
    ));
    fixture
        .apply(QueueCommand::FenceLease(FenceLeaseCommand {
            item_ids: vec![id(1)],
        }))
        .await;
    assert!(matches!(
        fixture
            .store
            .server_validate_update_fields(&fixture.shard, &fixture.definition, &unchanged, None)
            .await,
        Err(fireweed_engine::EngineError::StaleLease)
    ));
}

#[tokio::test]
async fn scalar_reschedule_preserves_batch_replaced_payload_and_reports_next_version() {
    let mut fixture = Fixture::new(indexed_definition("email", false)).await;
    // Zero is a valid first minted ID, distinct from a by-key unresolved target.
    let mut row = item("0", "payload-row", 1);
    row.payload = Some(bytes::Bytes::from_static(b"original"));
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: vec![row.clone()],
        }))
        .await;
    fixture
        .apply(QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
            updates: vec![UpdateFieldsCommand {
                item_id: row.item_id,
                payload: fireweed_engine::PayloadUpdate::Set(Some(bytes::Bytes::from_static(
                    b"batch-updated",
                ))),
                api001_batch: true,
                expected_item_version: Some(1),
                ..Default::default()
            }],
        }))
        .await;
    let before = fixture
        .store
        .server_live_items(&fixture.shard, std::slice::from_ref(&row.client_item_key))
        .await
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(before.payload.as_deref(), Some(b"batch-updated".as_slice()));
    let update = UpdateFieldsCommand {
        item_id: row.item_id,
        set_priority: fireweed_engine::ScheduleUpdate::Set(Some(
            fireweed_core::PriorityValue::Int64(99),
        )),
        ..Default::default()
    };
    let prior = fixture
        .store
        .server_validate_update_fields(&fixture.shard, &fixture.definition, &update, None)
        .await
        .unwrap();
    fixture.apply(QueueCommand::UpdateFields(update)).await;
    let after = fixture
        .store
        .server_live_items(&fixture.shard, std::slice::from_ref(&row.client_item_key))
        .await
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(
        after.payload, before.payload,
        "scalar schedule change must preserve the payload"
    );
    assert_eq!(
        after.priority,
        Some(fireweed_core::PriorityValue::Int64(99))
    );
    assert_eq!(after.item_version, prior + 1);
    // The unversioned API001 fast path must also treat explicit ID 0 as a row.
    fixture
        .apply(QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
            updates: vec![UpdateFieldsCommand {
                item_id: row.item_id,
                payload: fireweed_engine::PayloadUpdate::Set(Some(bytes::Bytes::from_static(
                    b"fast-zero",
                ))),
                api001_batch: true,
                ..Default::default()
            }],
        }))
        .await;
    let fast = fixture
        .store
        .server_live_items(&fixture.shard, &[row.client_item_key])
        .await
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(fast.payload.as_deref(), Some(b"fast-zero".as_slice()));
    assert_eq!(fast.item_version, after.item_version + 1);
}

#[tokio::test]
async fn retained_commit_receipts_and_binary_prefix_pages_are_queue_scoped() {
    let mut fixture = Fixture::new(qdef()).await;
    let mut other = fixture.definition.clone();
    other.queue_id = QueueId::new("neighbor").unwrap();
    let other_shard = QueueKey::new(other.tenant_id.clone(), other.queue_id.clone());
    AsyncProjectionStore::ensure_shard(&fixture.store, other)
        .await
        .unwrap();
    let mut records = (0..1002)
        .map(|n| SideRecord {
            key: format!("bulk/{n:04}").into_bytes(),
            payload: Bytes::from(n.to_string()),
        })
        .collect::<Vec<_>>();
    records.extend(
        [
            vec![255],
            vec![255, 0],
            vec![255, 255],
            b"adjacent".to_vec(),
        ]
        .into_iter()
        .map(|key| SideRecord {
            key,
            payload: Bytes::from_static(b"binary"),
        }),
    );
    fixture
        .apply(QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
            records,
        }))
        .await;
    let first = AsyncProjectionStore::side_records_by_prefix(
        &fixture.store,
        fixture.shard.clone(),
        b"bulk/".to_vec(),
        usize::MAX,
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.entries.len(), 1000);
    assert_eq!(first.next_cursor, Some(b"bulk/1000".to_vec()));
    let last = AsyncProjectionStore::side_records_by_prefix(
        &fixture.store,
        fixture.shard.clone(),
        b"bulk/".to_vec(),
        1000,
        first.next_cursor,
    )
    .await
    .unwrap();
    assert_eq!(last.entries.len(), 2);
    assert!(last.next_cursor.is_none());
    let zero = AsyncProjectionStore::side_records_by_prefix(
        &fixture.store,
        fixture.shard.clone(),
        vec![255],
        0,
        None,
    )
    .await
    .unwrap();
    assert!(zero.entries.is_empty());
    assert_eq!(zero.next_cursor, Some(vec![255]));
    let binary = AsyncProjectionStore::side_records_by_prefix(
        &fixture.store,
        fixture.shard.clone(),
        vec![255],
        10,
        zero.next_cursor,
    )
    .await
    .unwrap();
    assert_eq!(
        binary
            .entries
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>(),
        vec![vec![255], vec![255, 0], vec![255, 255]]
    );
    assert!(
        AsyncProjectionStore::side_records_by_prefix(
            &fixture.store,
            other_shard.clone(),
            vec![],
            100,
            None
        )
        .await
        .unwrap()
        .entries
        .is_empty()
    );
    let entries = vec![CommitOutcomeEntry {
        consumed_input_id: id(1),
        additional_consumed_input_ids: vec![id(2)],
        instance: Some((b"instance".to_vec(), 4)),
        side_record_keys: vec![],
        lifecycle_item_ids: vec![id(3)],
        rejection: None,
    }];
    let mut marker = envelope(
        QueueCommand::PauseQueue(PauseQueueCommand::default()),
        vec![],
    );
    let request_id = RequestId::new("retained").unwrap();
    marker.request_id = Some(request_id.clone());
    marker.request_fingerprint = Some(123);
    marker.request_outcome = Some(RequestOutcome::CommitTransition {
        entries: entries.clone(),
    });
    AsyncProjectionStore::apply_live(
        &fixture.store,
        vec![CommandPosition::new(
            fixture.shard.clone(),
            4,
            fixture.sequence,
        )],
        vec![marker],
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::read_durable_commit(
            &fixture.store,
            fixture.shard.clone(),
            request_id.clone()
        )
        .await
        .unwrap(),
        Some(entries)
    );
    assert!(
        AsyncProjectionStore::read_durable_commit(&fixture.store, other_shard, request_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selector_snapshot_keeps_gate_membership_and_item_metadata_in_one_commit() {
    let mut definition = indexed_definition("kind", false);
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    let mut fixture = Fixture::new(definition).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: (1..=2)
                .map(|n| {
                    let mut row = item(&n.to_string(), &format!("key-{n}"), n);
                    row.metadata
                        .insert("epoch", MetadataValue::String("0".into()));
                    row.gate_keys = vec!["epoch-0".into()];
                    row
                })
                .collect(),
        }))
        .await;
    let store = Arc::new(fixture.store);
    let writer = Arc::clone(&store);
    let shard = fixture.shard.clone();
    let writing = tokio::spawn(async move {
        for sequence in 1..=32 {
            let updates = (1..=2)
                .map(|n| {
                    let mut metadata = Metadata::new();
                    metadata.insert("epoch", MetadataValue::String(sequence.to_string()));
                    UpdateFieldsCommand {
                        item_id: id(n),
                        set_metadata: Some(metadata),
                        set_gate_keys: Some(vec![format!("epoch-{sequence}")]),
                        ..Default::default()
                    }
                })
                .collect();
            AsyncProjectionStore::apply_live(
                writer.as_ref(),
                vec![CommandPosition::new(shard.clone(), 4, sequence)],
                vec![envelope(
                    QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
                    vec![],
                )],
            )
            .await
            .unwrap();
            tokio::task::yield_now().await;
        }
    });
    let select = request(ItemMutationOperation::SelectFirst {
        clauses: vec![SelectedMutation {
            selector_id: "all".into(),
            selector: ItemSelector {
                scope: ItemSelectorScope::Live,
                predicates: vec![],
            },
            predicates: vec![],
            lease_guard: LeaseGuard::RejectActive,
            patch: ItemPatch::default(),
        }],
    });
    for _ in 0..64 {
        let plan = store
            .plan_item_mutation(&fixture.shard, &fixture.definition, &select, &[])
            .await
            .unwrap();
        assert_eq!(plan.response.results.len(), 2);
        let mut epochs = Vec::new();
        for result in &plan.response.results {
            let before = result.before.as_ref().unwrap();
            let Some(MetadataValue::String(epoch)) = before.metadata.get("epoch") else {
                panic!("missing epoch");
            };
            assert_eq!(before.gate_keys, vec![format!("epoch-{epoch}")]);
            epochs.push(epoch.clone());
        }
        assert_eq!(epochs[0], epochs[1]);
        tokio::task::yield_now().await;
    }
    writing.await.unwrap();
}

#[tokio::test]
async fn legacy_and_typed_indexes_follow_scalar_batch_mutation_replacement_and_purge() {
    let mut definition = indexed_definition("kind", false);
    definition.secondary_indexes = vec![
        fireweed_core::IndexSpec {
            name: "by_external".into(),
            fields: vec!["external".into()],
            unique: true,
        },
        fireweed_core::IndexSpec {
            name: "by_bucket".into(),
            fields: vec!["bucket".into()],
            unique: false,
        },
    ];
    let mut fixture = Fixture::new(definition).await;
    let rows = (1..=2)
        .map(|n| {
            let mut row = item(&n.to_string(), &format!("key-{n}"), n);
            row.fields = BTreeMap::from([
                ("external".into(), Bytes::from(vec![n as u8, 255])),
                ("bucket".into(), Bytes::from_static(b"A")),
            ]);
            row.index_fields
                .insert("kind".into(), TypedValue::String("red".into()));
            row
        })
        .collect();
    fixture
        .apply(QueueCommand::Push(PushCommand { items: rows }))
        .await;
    let update = UpdateFieldsCommand {
        item_id: id(1),
        field_ops: BTreeMap::from([
            ("external".into(), Some(Bytes::from_static(&[2, 0]))),
            ("bucket".into(), None),
        ]),
        set_entity_document: Some(json!({"kind":"blue"})),
        ..Default::default()
    };
    fixture
        .store
        .server_validate_update_fields(&fixture.shard, &fixture.definition, &update, Some(1))
        .await
        .unwrap();
    fixture.apply(QueueCommand::UpdateFields(update)).await;
    assert!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_external", &[vec![1, 255]])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[vec![2, 0]])
            .await
            .unwrap()
            .unwrap()
            .item_id,
        id(1)
    );
    assert_eq!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_bucket", &[b"A".to_vec()])
            .await
            .unwrap()
            .iter()
            .map(|hit| hit.item_id)
            .collect::<Vec<_>>(),
        vec![id(2)]
    );
    assert_eq!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_kind", &[b"blue".to_vec()])
            .await
            .unwrap()[0]
            .item_id,
        id(1)
    );
    let batch = UpdateFieldsCommand {
        item_id: id(2),
        set_fields: Some(BTreeMap::from([
            ("external".into(), Bytes::from_static(b"3")),
            ("bucket".into(), Bytes::from_static(b"B")),
        ])),
        api001_batch: true,
        expected_item_version: Some(1),
        ..Default::default()
    };
    fixture
        .apply(QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
            updates: vec![batch],
        }))
        .await;
    assert!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_external", &[vec![2, 255]])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"3".to_vec()])
            .await
            .unwrap()
            .unwrap()
            .item_id,
        id(2)
    );
    assert_eq!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_kind", &[b"red".to_vec()])
            .await
            .unwrap()[0]
            .item_id,
        id(2)
    );
    let mutation = addressed(
        id(1),
        ItemPatch {
            field_edits: BTreeMap::from([("external".into(), Some(Bytes::from_static(b"4")))]),
            ..Default::default()
        },
    );
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &mutation, &[])
        .await
        .unwrap();
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    assert!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_external", &[vec![2, 0]])
            .await
            .unwrap()
            .is_empty()
    );
    let mut replacement = item("3", "key-2", 3);
    replacement.fields = BTreeMap::from([
        ("external".into(), Bytes::from_static(b"3")),
        ("bucket".into(), Bytes::from_static(b"B")),
    ]);
    replacement
        .index_fields
        .insert("kind".into(), TypedValue::String("green".into()));
    fixture
        .apply(QueueCommand::ReplacePending(
            fireweed_engine::ReplacePendingCommand {
                client_item_key: replacement.client_item_key.clone(),
                superseded_item_id: id(2),
                replacement,
            },
        ))
        .await;
    assert_eq!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"3".to_vec()])
            .await
            .unwrap()
            .unwrap()
            .item_id,
        id(3)
    );
    assert!(
        fixture
            .store
            .server_index_lookup(&fixture.shard, "by_kind", &[b"red".to_vec()])
            .await
            .unwrap()
            .is_empty()
    );
    let complete = addressed(
        id(1),
        ItemPatch {
            lifecycle: fireweed_engine::LifecyclePatch::SetComplete,
            ..Default::default()
        },
    );
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &complete, &[])
        .await
        .unwrap();
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    assert_eq!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"4".to_vec()])
            .await
            .unwrap()
            .unwrap()
            .item_id,
        id(1)
    );
    fixture
        .apply(QueueCommand::PurgeItems(
            fireweed_engine::PurgeItemsCommand {
                item_ids: vec![id(1), id(3)],
                force: true,
            },
        ))
        .await;
    let rows = fixture
        .store
        .query("SELECT COUNT(*) FROM fireweed_item_index", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0].values, vec![turso::Value::Integer(0)]);
}

#[tokio::test]
async fn batch_update_preflight_preserves_entry_outcomes_and_rejects_both_unique_contenders() {
    use fireweed_engine::{BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateOutcome};

    let mut definition = indexed_definition("kind", false);
    definition.secondary_indexes = vec![fireweed_core::IndexSpec {
        name: "by_external".into(),
        fields: vec!["external".into()],
        unique: true,
    }];
    let mut fixture = Fixture::new(definition).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: (1..=9)
                .map(|n| {
                    let mut row = item(&n.to_string(), &format!("key-{n}"), n);
                    row.fields
                        .insert("external".into(), Bytes::from(n.to_string()));
                    row.index_fields
                        .insert("kind".into(), TypedValue::String("red".into()));
                    row
                })
                .collect(),
        }))
        .await;
    let complete = addressed(
        id(7),
        ItemPatch {
            lifecycle: fireweed_engine::LifecyclePatch::SetComplete,
            ..Default::default()
        },
    );
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &complete, &[])
        .await
        .unwrap();
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    fixture
        .apply(QueueCommand::Claim(ClaimCommand {
            item_ids: vec![id(8)],
            lease_token: LeaseToken::new("batch-update-leased").unwrap(),
            lease_expires_at: ts(100),
            worker_id: None,
            authority_first: false,
        }))
        .await;

    let entry = |n, replacement: Option<&'static [u8]>| BatchUpdateEntry {
        item_ref: BatchUpdateItemRef::ItemId(id(n)),
        expected_item_version: Some(1),
        priority: BatchUpdateValue::Keep,
        not_before: BatchUpdateValue::Keep,
        payload: BatchUpdateValue::Keep,
        metadata: BatchUpdateValue::Keep,
        gate_keys: BatchUpdateValue::Keep,
        fields: replacement
            .map(|value| {
                BatchUpdateValue::Replace(BTreeMap::from([(
                    "external".into(),
                    Bytes::from_static(value),
                )]))
            })
            .unwrap_or(BatchUpdateValue::Keep),
    };
    let mut stale = entry(5, None);
    stale.expected_item_version = Some(0);
    let mut mismatch = entry(6, None);
    mismatch.item_ref = BatchUpdateItemRef::Both {
        item_id: id(6),
        client_item_key: fireweed_core::ClientItemKey::new("key-5").unwrap(),
    };
    let mut clear = entry(6, None);
    clear.fields = BatchUpdateValue::Replace(BTreeMap::new());
    let updates = vec![
        entry(1, Some(b"shared")),
        entry(2, Some(b"shared")),
        entry(3, Some(b"4")),
        entry(4, None),
        stale,
        mismatch,
        entry(7, None),
        entry(8, None),
        entry(999, None),
        clear,
        entry(6, None),
    ];
    let before = AsyncProjectionStore::recovery_high_water(&fixture.store, fixture.shard.clone())
        .await
        .unwrap();
    let plan = fixture
        .store
        .server_plan_batch_update(&fixture.shard, &fixture.definition, updates)
        .await
        .unwrap();
    assert_eq!(
        plan.outcomes,
        vec![
            BatchUpdateOutcome::Invalid,
            BatchUpdateOutcome::Invalid,
            BatchUpdateOutcome::Invalid,
            BatchUpdateOutcome::Updated {
                item_id: id(4),
                client_item_key: fireweed_core::ClientItemKey::new("key-4").unwrap(),
                item_version: 2,
            },
            BatchUpdateOutcome::Conflict,
            BatchUpdateOutcome::Invalid,
            BatchUpdateOutcome::Terminal,
            BatchUpdateOutcome::Conflict,
            BatchUpdateOutcome::NotFound,
            BatchUpdateOutcome::Updated {
                item_id: id(6),
                client_item_key: fireweed_core::ClientItemKey::new("key-6").unwrap(),
                item_version: 2,
            },
            BatchUpdateOutcome::Conflict,
        ]
    );
    assert_eq!(
        plan.commands
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>(),
        vec![3, 9]
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&fixture.store, fixture.shard.clone())
            .await
            .unwrap(),
        before
    );
    assert!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"shared".to_vec()])
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"6".to_vec()])
            .await
            .unwrap()
            .unwrap()
            .item_version,
        1
    );
    fixture
        .apply(QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
            updates: plan
                .commands
                .into_iter()
                .map(|(_, command)| command)
                .collect(),
        }))
        .await;
    assert!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"6".to_vec()])
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .server_index_get_unique(&fixture.shard, "by_external", &[b"4".to_vec()])
            .await
            .unwrap()
            .unwrap()
            .item_version,
        2
    );
    let kinds = fixture
        .store
        .server_index_lookup(&fixture.shard, "by_kind", &[b"red".to_vec()])
        .await
        .unwrap();
    assert_eq!(kinds.len(), 9);
    assert!(
        kinds
            .iter()
            .any(|hit| hit.item_id == id(6) && hit.item_version == 2)
    );
}

#[tokio::test]
async fn batch_apply_changes_gates_only_for_accepted_rows_with_resolved_ids() {
    let mut definition = qdef();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    let mut fixture = Fixture::new(definition).await;
    fixture
        .apply(QueueCommand::Push(PushCommand {
            items: (1..=4)
                .map(|n| {
                    let mut row = item(&n.to_string(), &format!("key-{n}"), n);
                    row.gate_keys = vec!["old".into()];
                    row
                })
                .collect(),
        }))
        .await;
    fixture
        .apply(QueueCommand::Claim(ClaimCommand {
            item_ids: vec![id(3)],
            lease_token: LeaseToken::new("gates-leased").unwrap(),
            lease_expires_at: ts(100),
            worker_id: None,
            authority_first: false,
        }))
        .await;
    let complete = addressed(
        id(4),
        ItemPatch {
            lifecycle: fireweed_engine::LifecyclePatch::SetComplete,
            ..Default::default()
        },
    );
    let plan = fixture
        .store
        .plan_item_mutation(&fixture.shard, &fixture.definition, &complete, &[])
        .await
        .unwrap();
    fixture.apply(QueueCommand::MutateItems(plan.command)).await;
    let update = |n| UpdateFieldsCommand {
        item_id: id(n),
        api001_batch: true,
        expected_item_version: Some(1),
        set_gate_keys: Some(vec!["new".into()]),
        ..Default::default()
    };
    let mut stale = update(1);
    stale.expected_item_version = Some(0);
    let mut unresolved = update(0);
    unresolved.client_item_key = Some(fireweed_core::ClientItemKey::new("key-2").unwrap());
    fixture
        .apply(QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
            updates: vec![stale, unresolved, update(3), update(4), update(5)],
        }))
        .await;
    let rows = fixture.store.query(
        "SELECT i.item_id,i.item_version,g.gate_key FROM fireweed_items i \
         JOIN fireweed_item_gates g ON g.tenant_id=i.tenant_id AND g.queue_id=i.queue_id AND g.item_id=i.item_id \
         ORDER BY i.item_id", vec![],
    ).await.unwrap();
    assert_eq!(
        rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
        vec![
            vec![
                turso::Value::Text("1".into()),
                turso::Value::Integer(1),
                turso::Value::Text("old".into())
            ],
            vec![
                turso::Value::Text("2".into()),
                turso::Value::Integer(2),
                turso::Value::Text("new".into())
            ],
            vec![
                turso::Value::Text("3".into()),
                turso::Value::Integer(2),
                turso::Value::Text("old".into())
            ],
            vec![
                turso::Value::Text("4".into()),
                turso::Value::Integer(2),
                turso::Value::Text("old".into())
            ],
        ]
    );
    let gates = fixture
        .store
        .query("SELECT COUNT(*) FROM fireweed_item_gates", vec![])
        .await
        .unwrap();
    assert_eq!(gates[0].values, vec![turso::Value::Integer(4)]);
}
