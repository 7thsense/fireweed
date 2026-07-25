use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateRequest, BatchUpdateValue, Bytes, ClaimRef,
    ClientItemKey, CommitEntry, CommitRequest, CompoundIndexDef, CompoundIndexField,
    DiscoveryGranularity, EligibilityPolicy, EngineError, EntryOutcome, FilterOp, FinalizeKind,
    Fireweed, GateKeyPolicy, IndexDeclaration, IndexSpec, IndexType, NewItem,
    ObjectLogRuntimeConfig, ObjectLogStorage, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionConfig, QueryFilter,
    QueueDefinition, QueueId, QueueIndex, QueueKey, RecoveryAction, RecoveryPolicy,
    RecurrencePolicy, RequestId, ResponseBarrier, RetryPolicy, ScheduleUpdate, SegmentConfig,
    SideRecord, SystemClock, TenantId, TypedValue,
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

fn objectlog_sqlite(root: &Path, barrier: ResponseBarrier, cell: &str) -> Fireweed {
    fireweed::open_objectlog_sqlite(
        ObjectLogRuntimeConfig {
            object_log: ObjectLogStorage::Local {
                root: root.join("object-log"),
            },
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
    let primary_id = fireweed
        .push_with_request_id(&queue, push_request.clone(), primary_item())
        .await
        .unwrap();
    assert_eq!(
        fireweed
            .push_with_request_id(&queue, push_request, primary_item())
            .await
            .unwrap(),
        primary_id
    );
    fireweed
        .update(
            &queue,
            primary_id,
            ScheduleUpdate::Set(Some(PriorityValue::Int64(7))),
            ScheduleUpdate::Keep,
            None,
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
    assert_eq!(
        fireweed
            .push_with_request_id(
                &queue,
                RequestId::new("push-primary-v1").unwrap(),
                primary_item(),
            )
            .await
            .unwrap(),
        state.primary_id
    );
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
        fireweed.commit(&queue, state.commit_request).await.unwrap(),
        state.commit_response
    );

    let primary = fireweed
        .live_item(&queue, ClientItemKey::new("primary").unwrap())
        .await
        .unwrap()
        .expect("primary item survives reopen");
    assert_eq!(primary.item_id, state.primary_id);
    assert_eq!(primary.priority, Some(PriorityValue::Int64(7)));
    assert_eq!(primary.payload.as_deref(), Some(b"batched".as_slice()));
    assert_eq!(primary.fields["customer"].as_ref(), b"acme");
    assert_eq!(primary.fields["region"].as_ref(), b"east");

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
