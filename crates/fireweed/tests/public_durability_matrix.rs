use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateRequest, BatchUpdateValue, Bytes, ClaimRef,
    ClientItemKey, CommitEntry, CommitRequest, CompoundIndexDef, CompoundIndexField,
    DiscoveryGranularity, EligibilityPolicy, EngineError, EntryOutcome, FilterOp, FinalizeKind,
    Fireweed, GateKeyPolicy, IndexDeclaration, IndexSpec, IndexType, ItemMutationOperation,
    ItemMutationOutcome, ItemMutationRequest, ItemMutationResponse, ItemMutationReturning,
    ItemPatch, ItemPredicate, ItemSelector, ItemSelectorScope, LeaseGuard, LifecyclePatch, NewItem,
    ObjectLogAuthority, ObjectLogRuntimeConfig, ObjectLogStorage, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionConfig,
    QueryFilter, QueueDefinition, QueueId, QueueIndex, QueueKey, RecoveryAction, RecoveryPolicy,
    RecurrencePolicy, RequestId, ResponseBarrier, RetryPolicy, ScheduleUpdate, SegmentConfig,
    SelectedMutation, SideRecord, SystemClock, TenantId, TypedValue, UtcTimestamp,
};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(cell: &str) -> Self {
        let ordinal = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-durability-{cell}-{}-{ordinal}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn queue_definition(cell: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("durability").unwrap(),
        queue_id: QueueId::new(cell).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(4),
        },
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 3_600_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![IndexSpec {
            name: "by_customer_region".into(),
            fields: vec!["customer".into(), "region".into()],
            unique: true,
        }],
        entity_schema: None,
        typed_indexes: vec![QueueIndex {
            name: "by_kind_suppressed".into(),
            declaration: IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    CompoundIndexField {
                        field: "kind".into(),
                        index_type: IndexType::String,
                    },
                    CompoundIndexField {
                        field: "suppressed".into(),
                        index_type: IndexType::Boolean,
                    },
                ],
                unique: false,
            }),
        }],
        emit_change_records: true,
    }
}

fn queue_key(cell: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("durability").unwrap(),
        QueueId::new(cell).unwrap(),
    )
}

fn primary_item() -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new("primary").unwrap()),
        priority: Some(PriorityValue::Int64(10)),
        payload: Some(Bytes::from_static(b"original")),
        fields: BTreeMap::from([
            ("customer".into(), Bytes::from_static(b"old")),
            ("region".into(), Bytes::from_static(b"west")),
        ]),
        gate_keys: vec!["hold".into()],
        entity: Some(serde_json::json!({"kind": "effect", "suppressed": false})),
        ..NewItem::default()
    }
}

fn batch_request(item_id: fireweed::ItemId) -> BatchUpdateRequest {
    BatchUpdateRequest {
        request_id: RequestId::new("batch-primary-v1").unwrap(),
        updates: vec![BatchUpdateEntry {
            item_ref: BatchUpdateItemRef::Both {
                item_id,
                client_item_key: ClientItemKey::new("primary").unwrap(),
            },
            expected_item_version: None,
            priority: BatchUpdateValue::Keep,
            not_before: BatchUpdateValue::Keep,
            payload: BatchUpdateValue::Replace(Some(Bytes::from_static(b"batched"))),
            metadata: BatchUpdateValue::Keep,
            gate_keys: BatchUpdateValue::Keep,
            fields: BatchUpdateValue::Replace(BTreeMap::from([
                ("customer".into(), Bytes::from_static(b"acme")),
                ("region".into(), Bytes::from_static(b"east")),
            ])),
        }],
    }
}

fn selector_mutation(
    request_id: &str,
    key: &str,
    field: &str,
    value: &'static [u8],
    priority: i64,
) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new(request_id).unwrap(),
        evaluated_at: UtcTimestamp::new(1_800_000_000, 0).unwrap(),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![
                SelectedMutation {
                    selector_id: "eligible-before-patch".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![
                            ItemPredicate::ClientItemKeyEq(ClientItemKey::new(key).unwrap()),
                            ItemPredicate::FieldEq {
                                name: field.into(),
                                value: None,
                            },
                        ],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(priority))),
                        field_edits: BTreeMap::from([(
                            field.into(),
                            Some(Bytes::from_static(value)),
                        )]),
                        ..ItemPatch::default()
                    },
                },
                SelectedMutation {
                    selector_id: "must-not-run-on-replay".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![ItemPredicate::ClientItemKeyEq(
                            ClientItemKey::new(key).unwrap(),
                        )],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(99))),
                        ..ItemPatch::default()
                    },
                },
            ],
        },
    }
}

fn lease_invalidation_mutation(item_key: &str, evaluated_at: UtcTimestamp) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new("mutation-lease-invalidation-v1").unwrap(),
        evaluated_at,
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![
                SelectedMutation {
                    selector_id: "active-lease".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![
                            ItemPredicate::ClientItemKeyEq(ClientItemKey::new(item_key).unwrap()),
                            ItemPredicate::LeaseActive(true),
                        ],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::InvalidateActive,
                    patch: ItemPatch {
                        lifecycle: LifecyclePatch::SetPending,
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(-90))),
                        field_edits: BTreeMap::from([(
                            "lease-invalidated".into(),
                            Some(Bytes::from_static(b"yes")),
                        )]),
                        ..ItemPatch::default()
                    },
                },
                SelectedMutation {
                    selector_id: "must-not-run-on-replay".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![ItemPredicate::ClientItemKeyEq(
                            ClientItemKey::new(item_key).unwrap(),
                        )],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::InvalidateActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(99))),
                        ..ItemPatch::default()
                    },
                },
            ],
        },
    }
}

fn objectlog_sqlite(root: &Path, barrier: ResponseBarrier, cell: &str) -> Fireweed {
    fireweed::open_objectlog_sqlite(
        ObjectLogRuntimeConfig {
            object_log: ObjectLogStorage::Local {
                root: root.join("object-log"),
            },
            authority: ObjectLogAuthority::NativeConditionalWrite,
            projection: ProjectionConfig::Sqlite {
                path: root.join("projection.sqlite"),
            },
            response_barrier: barrier,
            segments: SegmentConfig::new(262_144, 20).unwrap(),
            namespace: format!("durability-{cell}"),
            recovery: RecoveryPolicy {
                incompatible_projection: RecoveryAction::RebuildProjection,
                verify_checksums: true,
                max_tail_commands: 1_000_000,
            },
        },
        Arc::new(SystemClock),
    )
    .unwrap()
}

async fn seed(cell: &str, fireweed: &Fireweed) -> SeededState {
    let queue = queue_key(cell);
    let definition = queue_definition(cell);
    assert!(
        fireweed
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );

    let push_request = RequestId::new("push-primary-v1").unwrap();
    let (primary_id, primary_disp) = fireweed
        .push_with_request_id(&queue, push_request.clone(), primary_item())
        .await
        .unwrap();
    assert_eq!(primary_disp, fireweed::PushDisposition::Fresh);
    let (replayed_id, replay_disp) = fireweed
        .push_with_request_id(&queue, push_request, primary_item())
        .await
        .unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed_id, primary_id);
    fireweed
        .batch_update(
            &queue,
            BatchUpdateRequest {
                request_id: RequestId::new("durability-reschedule-primary").unwrap(),
                updates: vec![BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ItemId(primary_id),
                    expected_item_version: None,
                    priority: BatchUpdateValue::Replace(PriorityValue::Int64(7)),
                    not_before: BatchUpdateValue::Keep,
                    payload: BatchUpdateValue::Keep,
                    metadata: BatchUpdateValue::Keep,
                    gate_keys: BatchUpdateValue::Keep,
                    fields: BatchUpdateValue::Keep,
                }],
            },
        )
        .await
        .unwrap();
    let batch = batch_request(primary_id);
    let batch_response = fireweed.batch_update(&queue, batch.clone()).await.unwrap();
    assert_eq!(
        fireweed.batch_update(&queue, batch.clone()).await.unwrap(),
        batch_response
    );

    let mutation = fireweed
        .bounded_mutation(
            &queue,
            fireweed::BoundedMutationRequest {
                index: Some("by_kind_suppressed".into()),
                filters: vec![QueryFilter {
                    field: "kind".into(),
                    op: FilterOp::Eq,
                    value: TypedValue::String("effect".into()),
                }],
                set_fields: BTreeMap::from([("suppressed".into(), TypedValue::Bool(true))]),
                max_scan_rows: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(mutation.results.len(), 1);
    assert_eq!(mutation.results[0].item_id, primary_id);

    let before_selector = fireweed
        .live_item(&queue, ClientItemKey::new("primary").unwrap())
        .await
        .unwrap()
        .expect("primary exists before selector mutation");
    let selector_request = selector_mutation(
        "mutation-selector-v1",
        "primary",
        "selector-durable",
        b"yes",
        2,
    );
    let mut selector_preview = selector_request.clone();
    selector_preview.dry_run = true;
    let preview = fireweed
        .mutate_items(&queue, selector_preview)
        .await
        .unwrap();
    assert!(preview.position.is_none());
    assert_eq!(preview.summary.changed, 1);
    assert_eq!(
        preview.results[0].selector_id.as_deref(),
        Some("eligible-before-patch")
    );
    assert!(matches!(
        preview.results[0].outcome,
        ItemMutationOutcome::WouldUpdate { .. }
    ));
    let after_preview = fireweed
        .live_item(&queue, ClientItemKey::new("primary").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_preview.item_version, before_selector.item_version);
    assert_eq!(after_preview.priority, before_selector.priority);
    assert!(!after_preview.fields.contains_key("selector-durable"));
    let selector_response = fireweed
        .mutate_items(&queue, selector_request.clone())
        .await
        .unwrap();
    assert!(selector_response.position.is_some());
    assert_eq!(selector_response.summary.changed, 1);
    assert_eq!(
        selector_response.results[0].selector_id.as_deref(),
        Some("eligible-before-patch")
    );
    assert_eq!(
        fireweed
            .mutate_items(&queue, selector_request.clone())
            .await
            .unwrap(),
        selector_response,
        "selector replay must return the retained outcome without reevaluating the now-divergent selector"
    );
    let after_selector = fireweed
        .live_item(&queue, ClientItemKey::new("primary").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after_selector.item_version,
        before_selector.item_version + 1
    );
    assert_eq!(after_selector.priority, Some(PriorityValue::Int64(2)));
    assert_eq!(after_selector.fields["selector-durable"].as_ref(), b"yes");

    let dry_reopen_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("dry-run-reopen").unwrap()),
                priority: Some(PriorityValue::Int64(30)),
                gate_keys: vec!["hold".into()],
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let dry_reopen_before = fireweed
        .live_item(&queue, ClientItemKey::new("dry-run-reopen").unwrap())
        .await
        .unwrap()
        .unwrap();
    let dry_reopen_request = selector_mutation(
        "mutation-dry-reopen-v1",
        "dry-run-reopen",
        "dry-run-cross-reopen",
        b"committed-after-reopen",
        25,
    );
    let mut dry_reopen_preview = dry_reopen_request.clone();
    dry_reopen_preview.dry_run = true;
    let dry_preview = fireweed
        .mutate_items(&queue, dry_reopen_preview)
        .await
        .unwrap();
    assert!(dry_preview.position.is_none());
    assert_eq!(dry_preview.summary.changed, 1);
    let dry_after_preview = fireweed
        .live_item(&queue, ClientItemKey::new("dry-run-reopen").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dry_after_preview.item_version,
        dry_reopen_before.item_version
    );
    assert_eq!(dry_after_preview.priority, dry_reopen_before.priority);
    assert!(
        !dry_after_preview
            .fields
            .contains_key("dry-run-cross-reopen")
    );

    let lease_item_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("lease-invalidation").unwrap()),
                priority: Some(PriorityValue::Int64(-100)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let leased = fireweed.claim(&queue, 1, 30_000).await.unwrap().remove(0);
    assert_eq!(leased.item_id, lease_item_id);
    let old_claim_ref = ClaimRef {
        item_id: leased.item_id,
        lease_token: leased.lease_token.clone().unwrap(),
        lease_expires_at: leased.lease_expires_at,
        item_version: leased.item_version,
    };
    let old_token = old_claim_ref.lease_token.clone();
    let lease_request = lease_invalidation_mutation(
        "lease-invalidation",
        UtcTimestamp::new(
            leased.lease_expires_at.seconds.saturating_sub(1),
            leased.lease_expires_at.nanoseconds,
        )
        .unwrap(),
    );
    let lease_response = fireweed
        .mutate_items(&queue, lease_request.clone())
        .await
        .unwrap();
    assert_eq!(lease_response.summary.changed, 1);
    assert_eq!(
        lease_response.results[0].selector_id.as_deref(),
        Some("active-lease")
    );
    assert!(
        fireweed
            .claimed(&queue, &[lease_item_id])
            .await
            .unwrap()
            .is_empty()
    );
    let replacement = fireweed.claim(&queue, 1, 30_000).await.unwrap().remove(0);
    assert_eq!(replacement.item_id, lease_item_id);
    assert_ne!(replacement.lease_token.as_ref(), Some(&old_token));
    assert!(replacement.item_version > leased.item_version);
    assert!(matches!(
        fireweed
            .commit(
                &queue,
                CommitRequest {
                    request_id: Some(RequestId::new("mutation-stale-lease-v1").unwrap()),
                    entries: vec![CommitEntry {
                        claim_ref: old_claim_ref.clone(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![],
                        instance_fence: None,
                    }],
                },
            )
            .await
            .unwrap()
            .as_slice(),
        [EntryOutcome::Rejected(EngineError::StaleLease)]
    ));
    fireweed.release(&queue, [lease_item_id]).await.unwrap();
    fireweed
        .batch_update(
            &queue,
            BatchUpdateRequest {
                request_id: RequestId::new("durability-defer-released").unwrap(),
                updates: vec![BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ItemId(lease_item_id),
                    expected_item_version: None,
                    priority: BatchUpdateValue::Keep,
                    not_before: BatchUpdateValue::Replace(Some(
                        UtcTimestamp::new(1_900_000_000, 0).unwrap(),
                    )),
                    payload: BatchUpdateValue::Keep,
                    metadata: BatchUpdateValue::Keep,
                    gate_keys: BatchUpdateValue::Keep,
                    fields: BatchUpdateValue::Keep,
                }],
            },
        )
        .await
        .unwrap();

    let source_id = fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("commit-source").unwrap()),
                priority: Some(PriorityValue::Int64(1)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let claimed = fireweed.claim(&queue, 1, 30_000).await.unwrap();
    assert_eq!(claimed[0].item_id, source_id);
    let commit_request_id = RequestId::new("commit-transition-v1").unwrap();
    let commit_request = CommitRequest {
        request_id: Some(commit_request_id.clone()),
        entries: vec![CommitEntry {
            claim_ref: ClaimRef {
                item_id: claimed[0].item_id,
                lease_token: claimed[0]
                    .lease_token
                    .clone()
                    .expect("claimed item carries a lease token"),
                lease_expires_at: claimed[0].lease_expires_at,
                item_version: claimed[0].item_version,
            },
            finalize: FinalizeKind::Complete,
            side_records: vec![SideRecord {
                key: b"transition/state".to_vec(),
                payload: Bytes::from_static(b"committed"),
            }],
            lifecycle_items: vec![NewItem {
                client_item_key: Some(ClientItemKey::new("continuation").unwrap()),
                priority: Some(PriorityValue::Int64(20)),
                ..NewItem::default()
            }],
            instance_fence: None,
        }],
    };
    let commit_response = fireweed
        .commit(&queue, commit_request.clone())
        .await
        .unwrap();
    assert!(matches!(
        commit_response.as_slice(),
        [EntryOutcome::Committed { .. }]
    ));
    assert_eq!(
        fireweed
            .commit(&queue, commit_request.clone())
            .await
            .unwrap(),
        commit_response
    );
    fireweed
        .set_gates(&queue, vec!["hold".into()], true)
        .await
        .unwrap();
    assert!(
        fireweed
            .peek(&queue, 10)
            .await
            .unwrap()
            .iter()
            .all(|item| item.item_id != primary_id),
        "blocked gate must exclude the primary from peek before close"
    );
    let while_blocked = fireweed.claim(&queue, 10, 30_000).await.unwrap();
    assert!(
        while_blocked.iter().all(|item| item.item_id != primary_id),
        "blocked gate must exclude the primary before close"
    );
    fireweed
        .release(&queue, while_blocked.iter().map(|item| item.item_id))
        .await
        .unwrap();

    SeededState {
        definition,
        primary_id,
        batch,
        batch_response,
        selector_request,
        selector_response,
        dry_reopen_id,
        dry_reopen_before_version: dry_reopen_before.item_version,
        dry_reopen_request,
        lease_item_id,
        lease_request,
        lease_response,
        old_claim_ref,
        commit_request_id,
        commit_request,
        commit_response,
    }
}

struct SeededState {
    definition: QueueDefinition,
    primary_id: fireweed::ItemId,
    batch: BatchUpdateRequest,
    batch_response: fireweed::BatchUpdateResponse,
    selector_request: ItemMutationRequest,
    selector_response: ItemMutationResponse,
    dry_reopen_id: fireweed::ItemId,
    dry_reopen_before_version: u64,
    dry_reopen_request: ItemMutationRequest,
    lease_item_id: fireweed::ItemId,
    lease_request: ItemMutationRequest,
    lease_response: ItemMutationResponse,
    old_claim_ref: ClaimRef,
    commit_request_id: RequestId,
    commit_request: CommitRequest,
    commit_response: Vec<EntryOutcome>,
}

async fn verify_reopen(cell: &str, fireweed: &Fireweed, state: SeededState) {
    let queue = queue_key(cell);
    assert_eq!(
        fireweed.queue_definition(&queue).await.unwrap(),
        state.definition
    );
    let (replayed_id, replay_disp) = fireweed
        .push_with_request_id(
            &queue,
            RequestId::new("push-primary-v1").unwrap(),
            primary_item(),
        )
        .await
        .unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed_id, state.primary_id);
    let mut conflicting_push = primary_item();
    conflicting_push.payload = Some(Bytes::from_static(b"different-body"));
    assert_eq!(
        fireweed
            .push_with_request_id(
                &queue,
                RequestId::new("push-primary-v1").unwrap(),
                conflicting_push,
            )
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict
    );
    assert_eq!(
        fireweed.batch_update(&queue, state.batch).await.unwrap(),
        state.batch_response
    );
    assert_eq!(
        fireweed
            .mutate_items(&queue, state.selector_request.clone())
            .await
            .unwrap(),
        state.selector_response,
        "selector mutation must replay its exact retained response after reopen without reevaluation"
    );

    let dry_before_reopen = fireweed
        .live_item(&queue, ClientItemKey::new("dry-run-reopen").unwrap())
        .await
        .unwrap()
        .expect("dry-run witness survives reopen");
    assert_eq!(dry_before_reopen.item_id, state.dry_reopen_id);
    assert_eq!(
        dry_before_reopen.item_version,
        state.dry_reopen_before_version
    );
    assert_eq!(dry_before_reopen.priority, Some(PriorityValue::Int64(30)));
    assert!(
        !dry_before_reopen
            .fields
            .contains_key("dry-run-cross-reopen")
    );
    let dry_committed = fireweed
        .mutate_items(&queue, state.dry_reopen_request.clone())
        .await
        .unwrap();
    assert!(dry_committed.position.is_some());
    assert_eq!(dry_committed.summary.changed, 1);
    assert_eq!(
        dry_committed.results[0].selector_id.as_deref(),
        Some("eligible-before-patch")
    );
    assert_eq!(
        fireweed
            .mutate_items(&queue, state.dry_reopen_request.clone())
            .await
            .unwrap(),
        dry_committed,
        "the request id reused after a pre-close dry-run must become exactly replayable after its real commit"
    );
    let dry_after_commit = fireweed
        .live_item(&queue, ClientItemKey::new("dry-run-reopen").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dry_after_commit.item_version,
        state.dry_reopen_before_version + 1
    );
    assert_eq!(dry_after_commit.priority, Some(PriorityValue::Int64(25)));
    assert_eq!(
        dry_after_commit.fields["dry-run-cross-reopen"].as_ref(),
        b"committed-after-reopen"
    );

    assert_eq!(
        fireweed
            .mutate_items(&queue, state.lease_request.clone())
            .await
            .unwrap(),
        state.lease_response,
        "lease-invalidation mutation must replay after reopen without selecting the fallback clause"
    );
    assert!(
        fireweed
            .claimed(&queue, &[state.lease_item_id])
            .await
            .unwrap()
            .is_empty(),
        "the invalidated claim selection must stay absent after reopen"
    );
    assert!(matches!(
        fireweed
            .commit(
                &queue,
                CommitRequest {
                    request_id: Some(RequestId::new("mutation-stale-lease-after-reopen").unwrap(),),
                    entries: vec![CommitEntry {
                        claim_ref: state.old_claim_ref.clone(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![],
                        instance_fence: None,
                    }],
                },
            )
            .await
            .unwrap()
            .as_slice(),
        [EntryOutcome::Rejected(EngineError::Invalid(
            "item is not leased"
        ))]
    ));
    assert_eq!(
        fireweed.commit(&queue, state.commit_request).await.unwrap(),
        state.commit_response
    );

    let primary = fireweed
        .live_item(&queue, ClientItemKey::new("primary").unwrap())
        .await
        .unwrap()
        .expect("primary item survives reopen");
    assert_eq!(primary.item_id, state.primary_id);
    assert_eq!(primary.priority, Some(PriorityValue::Int64(2)));
    assert_eq!(primary.payload.as_deref(), Some(b"batched".as_slice()));
    assert_eq!(primary.fields["customer"].as_ref(), b"acme");
    assert_eq!(primary.fields["region"].as_ref(), b"east");
    assert_eq!(primary.fields["selector-durable"].as_ref(), b"yes");

    let secondary = fireweed
        .query_index_unique(
            &queue,
            "by_customer_region",
            vec![b"acme".to_vec(), b"east".to_vec()],
        )
        .await
        .unwrap()
        .expect("secondary index survives reopen");
    assert_eq!(secondary.item_id, state.primary_id);
    let typed = fireweed
        .query_index_typed(
            &queue,
            "by_kind_suppressed",
            &[serde_json::json!("effect"), serde_json::json!(true)],
        )
        .await
        .unwrap();
    assert_eq!(typed.len(), 1);
    assert_eq!(typed[0].item_id, state.primary_id);

    let recovery = fireweed
        .explain_commit(&queue, state.commit_request_id)
        .await
        .unwrap()
        .expect("commit recovery survives reopen");
    assert_eq!(recovery.entries.len(), 1);
    assert_eq!(
        fireweed
            .side_record(&queue, b"transition/state")
            .await
            .unwrap()
            .as_deref(),
        Some(b"committed".as_slice())
    );
    assert!(
        fireweed
            .peek(&queue, 10)
            .await
            .unwrap()
            .iter()
            .all(|item| item.item_id != state.primary_id),
        "blocked gate must remain excluded from peek after reopen"
    );
    let while_blocked = fireweed.claim(&queue, 10, 30_000).await.unwrap();
    assert!(
        while_blocked
            .iter()
            .all(|item| item.item_id != state.primary_id),
        "blocked gate state must survive reopen"
    );
    fireweed
        .release(&queue, while_blocked.iter().map(|item| item.item_id))
        .await
        .unwrap();
    let scopes = fireweed
        .discover_active_scopes(&queue, DiscoveryGranularity::Queue)
        .await
        .unwrap();
    assert!(scopes.iter().any(|scope| scope.queue_id == cell));

    fireweed
        .set_gates(&queue, vec!["hold".into()], false)
        .await
        .unwrap();
    assert!(
        fireweed
            .peek(&queue, 10)
            .await
            .unwrap()
            .iter()
            .any(|item| item.item_id == state.primary_id),
        "unblocked primary must return to peek after reopen"
    );
    let after_unblock = fireweed.claim(&queue, 10, 30_000).await.unwrap();
    assert!(
        after_unblock
            .iter()
            .any(|item| item.item_id == state.primary_id),
        "durably blocked items must become claimable after reopen and unblock"
    );
}

async fn assert_durable(cell: &str, open: impl Fn(&Path) -> Fireweed) {
    let fixture = FixtureRoot::new(cell);
    let fireweed = open(fixture.path());
    let state = seed(cell, &fireweed).await;
    drop(fireweed);
    let reopened = open(fixture.path());
    verify_reopen(cell, &reopened, state).await;
    drop(reopened);
}

#[tokio::test]
async fn sqlite_log_close_reopen() {
    assert_durable("sqlite-log", |root| {
        fireweed::open_sqlite(
            root.join("log.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn sqlite_relational_close_reopen() {
    assert_durable("sqlite-relational", |root| {
        fireweed::open_sqlite_relational(
            root.join("relational.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn objectlog_local_direct_close_reopen() {
    assert_durable("objectlog-local", |root| {
        fireweed::open_objectlog(root.join("object-log"), Arc::new(SystemClock)).unwrap()
    })
    .await;
}

#[tokio::test]
async fn objectlog_sqlite_strict_close_reopen() {
    assert_durable("objectlog-sqlite-strict", |root| {
        objectlog_sqlite(root, ResponseBarrier::Strict, "strict")
    })
    .await;
}

#[tokio::test]
async fn objectlog_sqlite_async_close_reopen() {
    assert_durable("objectlog-sqlite-async", |root| {
        objectlog_sqlite(root, ResponseBarrier::AsyncProjection, "async")
    })
    .await;
}
