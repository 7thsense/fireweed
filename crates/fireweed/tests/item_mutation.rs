use std::collections::BTreeMap;
use std::sync::Arc;

use fireweed::{
    AddressedMutation, BatchUpdateValue, ClientItemKey, EligibilityPolicy, EntityEdit,
    EntityEditOperation, EntityPredicateValue, Fireweed, GateChange, GateKeyDelta, GateKeyPolicy,
    ItemMutationOperation, ItemMutationOutcome, ItemMutationRequest, ItemMutationReturning,
    ItemPatch, ItemPredicate, ItemSelector, ItemSelectorScope, LeaseGuard, LifecyclePatch, NewItem,
    OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker,
    PriorityValue, QueueDefinition, QueueId, QueueKey, RecurrencePolicy, RequestId, RetryPolicy,
    SelectedMutation, SystemClock, TenantId, UtcTimestamp,
};
#[cfg(feature = "objectlog")]
use fireweed::{
    ObjectLogRuntimeConfig, ObjectLogStorage, ProjectionConfig, RecoveryAction, RecoveryPolicy,
    ResponseBarrier, SegmentConfig,
};

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn definition(queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("mutation-tests").unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(8),
            ..EligibilityPolicy::default()
        },
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
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

async fn create(fireweed: &Fireweed, name: &str) -> QueueKey {
    let definition = definition(name);
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();
    queue
}

fn addressed_request(
    request_id: &str,
    item_id: fireweed::ItemId,
    expected_item_version: Option<u64>,
) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new(request_id).unwrap(),
        evaluated_at: ts(10),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![GateChange {
            gate_keys: vec!["queue-hold".into()],
            blocked: true,
        }],
        operation: ItemMutationOperation::Addressed {
            entries: vec![AddressedMutation {
                item_id,
                expected_item_version,
                predicates: vec![ItemPredicate::EntityEq {
                    pointer: "/workflow/kind".into(),
                    value: EntityPredicateValue::Value(serde_json::json!("job")),
                }],
                lease_guard: LeaseGuard::RejectActive,
                patch: ItemPatch {
                    priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(5))),
                    gate_keys: GateKeyDelta {
                        add: vec!["queue-hold".into()],
                        remove: vec![],
                    },
                    field_edits: BTreeMap::from([(
                        "owner".into(),
                        Some(bytes::Bytes::from_static(b"snorri")),
                    )]),
                    entity_edits: vec![EntityEdit {
                        pointer: "/workflow/revision".into(),
                        operation: EntityEditOperation::Set(serde_json::json!(2)),
                    }],
                    ..Default::default()
                },
            }],
        },
    }
}

#[tokio::test]
async fn memory_facade_mutates_atomically_and_replays_exact_response() {
    let fireweed = fireweed::open_memory(Arc::new(SystemClock));
    let queue = create(&fireweed, "memory").await;
    let key = ClientItemKey::new("item-a").unwrap();
    let item_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(key.clone()),
                priority: Some(PriorityValue::Int64(10)),
                entity: Some(serde_json::json!({
                    "workflow": {"kind": "job", "revision": 1}
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut dry_run = addressed_request("mutation-dry-run", item_id, Some(1));
    dry_run.dry_run = true;
    let preview = fireweed.mutate_items(&queue, dry_run).await.unwrap();
    assert!(preview.position.is_none());
    assert!(matches!(
        preview.results[0].outcome,
        ItemMutationOutcome::WouldUpdate {
            item_version: 2,
            ..
        }
    ));
    assert_eq!(
        fireweed
            .live_item(&queue, key.clone())
            .await
            .unwrap()
            .unwrap()
            .item_version,
        1
    );

    let request = addressed_request("mutation-commit", item_id, Some(1));
    let committed = fireweed
        .mutate_items(&queue, request.clone())
        .await
        .unwrap();
    assert!(committed.position.is_some());
    assert_eq!(committed.summary.changed, 1);
    assert_eq!(
        committed.results[0].before.as_ref().unwrap().item_version,
        1
    );
    let replayed = fireweed.mutate_items(&queue, request).await.unwrap();
    assert_eq!(
        replayed, committed,
        "replay returns the retained response verbatim"
    );

    let live = fireweed.live_item(&queue, key).await.unwrap().unwrap();
    assert_eq!(live.item_version, 2);
    assert_eq!(live.priority, Some(PriorityValue::Int64(5)));
    assert_eq!(live.fields.get("owner").unwrap().as_ref(), b"snorri");
    assert!(fireweed.claim(&queue, 1, 1_000).await.unwrap().is_empty());

    let mut conflict = addressed_request("mutation-commit", item_id, Some(2));
    conflict.gate_changes.clear();
    assert_eq!(
        fireweed.mutate_items(&queue, conflict).await.unwrap_err(),
        fireweed::EngineError::RequestIdConflict
    );
}

#[tokio::test]
async fn selector_first_match_invalidates_lease_and_retained_terminal_can_be_purged() {
    let fireweed = fireweed::open_memory(Arc::new(SystemClock));
    let queue = create(&fireweed, "selector").await;
    let item_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("leased").unwrap()),
                entity: Some(serde_json::json!({"kind": "job"})),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claimed = fireweed.claim(&queue, 1, 10_000).await.unwrap().remove(0);
    let response = fireweed
        .mutate_items(
            &queue,
            ItemMutationRequest {
                request_id: RequestId::new("selector-invalidate").unwrap(),
                evaluated_at: ts(10),
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::SelectFirst {
                    clauses: vec![
                        SelectedMutation {
                            selector_id: "first".into(),
                            selector: ItemSelector {
                                scope: ItemSelectorScope::Live,
                                predicates: vec![ItemPredicate::EntityEq {
                                    pointer: "/kind".into(),
                                    value: EntityPredicateValue::Value(serde_json::json!("job")),
                                }],
                            },
                            lease_guard: LeaseGuard::Match(
                                claimed.lease_token.clone().expect("item lease token"),
                            ),
                            patch: ItemPatch {
                                lifecycle: LifecyclePatch::SetComplete,
                                ..Default::default()
                            },
                        },
                        SelectedMutation {
                            selector_id: "never".into(),
                            selector: ItemSelector {
                                scope: ItemSelectorScope::Live,
                                predicates: vec![],
                            },
                            lease_guard: LeaseGuard::InvalidateActive,
                            patch: ItemPatch {
                                lifecycle: LifecyclePatch::SetFailed,
                                ..Default::default()
                            },
                        },
                    ],
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(response.results[0].selector_id.as_deref(), Some("first"));
    assert!(matches!(
        response.results[0].outcome,
        ItemMutationOutcome::Updated {
            state: fireweed::ItemState::Complete,
            ..
        }
    ));

    let terminal_overlay = fireweed
        .mutate_items(
            &queue,
            ItemMutationRequest {
                request_id: RequestId::new("terminal-overlay").unwrap(),
                evaluated_at: ts(11),
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed {
                    entries: vec![AddressedMutation {
                        item_id,
                        expected_item_version: Some(3),
                        predicates: vec![],
                        lease_guard: LeaseGuard::RejectActive,
                        patch: ItemPatch {
                            field_edits: BTreeMap::from([(
                                "terminal-overlay".into(),
                                Some(bytes::Bytes::from_static(b"cleared")),
                            )]),
                            ..Default::default()
                        },
                    }],
                },
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        terminal_overlay.results[0].outcome,
        ItemMutationOutcome::Updated {
            state: fireweed::ItemState::Complete,
            ..
        }
    ));

    let purge = fireweed
        .mutate_items(
            &queue,
            ItemMutationRequest {
                request_id: RequestId::new("selector-purge").unwrap(),
                evaluated_at: ts(11),
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::SelectFirst {
                    clauses: vec![SelectedMutation {
                        selector_id: "terminal".into(),
                        selector: ItemSelector {
                            scope: ItemSelectorScope::Retained,
                            predicates: vec![ItemPredicate::StateIn(vec![
                                fireweed::ItemState::Complete,
                            ])],
                        },
                        lease_guard: LeaseGuard::RejectActive,
                        patch: ItemPatch {
                            lifecycle: LifecyclePatch::Purge,
                            ..Default::default()
                        },
                    }],
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(purge.summary.purged, 1);
    assert_eq!(purge.results[0].item_id, item_id);
}

#[tokio::test]
async fn active_lease_boundary_and_composed_predicates_use_evaluated_at() {
    let fireweed = fireweed::open_memory(Arc::new(SystemClock));
    let queue = create(&fireweed, "lease-boundary").await;
    let item_id = fireweed.push(&queue, NewItem::default()).await.unwrap();
    let claimed = fireweed.claim(&queue, 1, 10_000).await.unwrap().remove(0);

    let at_expiry = claimed.lease_expires_at;
    let rejected = fireweed
        .mutate_items(
            &queue,
            ItemMutationRequest {
                request_id: RequestId::new("lease-equality-rejected").unwrap(),
                evaluated_at: at_expiry,
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed {
                    entries: vec![AddressedMutation {
                        item_id,
                        expected_item_version: None,
                        predicates: vec![],
                        lease_guard: LeaseGuard::RequireActive,
                        patch: ItemPatch {
                            field_edits: BTreeMap::from([(
                                "should-not-land".into(),
                                Some(bytes::Bytes::from_static(b"x")),
                            )]),
                            ..Default::default()
                        },
                    }],
                },
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        rejected.results[0].outcome,
        ItemMutationOutcome::PreconditionFailed(fireweed::ItemMutationPrecondition::ActiveLease)
    ));

    let released = fireweed
        .mutate_items(
            &queue,
            ItemMutationRequest {
                request_id: RequestId::new("lease-equality-release").unwrap(),
                evaluated_at: at_expiry,
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed {
                    entries: vec![AddressedMutation {
                        item_id,
                        expected_item_version: None,
                        predicates: vec![ItemPredicate::All(vec![
                            ItemPredicate::AttemptCountEq(1),
                            ItemPredicate::LeaseActive(false),
                            ItemPredicate::Any(vec![
                                ItemPredicate::StateIn(vec![fireweed::ItemState::Leased]),
                                ItemPredicate::Not(Box::new(ItemPredicate::StateIn(vec![
                                    fireweed::ItemState::Pending,
                                ]))),
                            ]),
                        ])],
                        lease_guard: LeaseGuard::InvalidateActive,
                        patch: ItemPatch {
                            lifecycle: LifecyclePatch::SetPending,
                            ..Default::default()
                        },
                    }],
                },
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        released.results[0].outcome,
        ItemMutationOutcome::Updated {
            state: fireweed::ItemState::Pending,
            ..
        }
    ));
    assert_eq!(
        fireweed.claim(&queue, 1, 1_000).await.unwrap()[0].item_id,
        item_id
    );
}

#[cfg(feature = "objectlog")]
#[tokio::test]
async fn objectlog_inmemory_reopen_replays_without_selector_evaluation() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-item-mutation-objectlog-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let fireweed = fireweed::open_objectlog(&root, Arc::new(SystemClock)).unwrap();
    let queue = create(&fireweed, "objectlog").await;
    let item_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("object-item").unwrap()),
                entity: Some(serde_json::json!({"workflow": {"kind": "job"}})),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let request = addressed_request("objectlog-replay", item_id, Some(1));
    let committed = fireweed
        .mutate_items(&queue, request.clone())
        .await
        .unwrap();
    drop(fireweed);

    let reopened = fireweed::open_objectlog(&root, Arc::new(SystemClock)).unwrap();
    let replayed = reopened.mutate_items(&queue, request).await.unwrap();
    assert_eq!(replayed, committed);
    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
#[tokio::test]
async fn objectlog_sqlite_reopen_replays_without_selector_evaluation() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-item-mutation-objectlog-sqlite-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let runtime = ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: root.join("object-log"),
        },
        projection: ProjectionConfig::Sqlite {
            path: root.join("projection.sqlite"),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(262_144, 20).unwrap(),
        namespace: "mutation-objectlog-sqlite".into(),
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
    };
    let fireweed = fireweed::open_objectlog_sqlite(runtime.clone(), Arc::new(SystemClock)).unwrap();
    let queue = create(&fireweed, "objectlog-sqlite").await;
    let item_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("object-sqlite-item").unwrap()),
                entity: Some(serde_json::json!({"workflow": {"kind": "job"}})),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let request = addressed_request("objectlog-sqlite-replay", item_id, Some(1));
    let committed = fireweed
        .mutate_items(&queue, request.clone())
        .await
        .unwrap();
    drop(fireweed);

    let reopened = fireweed::open_objectlog_sqlite(runtime, Arc::new(SystemClock)).unwrap();
    assert_eq!(reopened.mutate_items(&queue, request).await.unwrap(), committed);
    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
}
