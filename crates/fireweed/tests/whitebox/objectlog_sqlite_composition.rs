#![allow(dead_code, unused_imports)]
#![cfg(all(feature = "objectlog", feature = "sqlite"))]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    AsyncProjectionSpec, BoundedMutationRequest, Bytes, ClaimByQueryAt, ClaimByQueryRequest,
    ClaimRef, Clock, CommitEntry, CommitRequest, CommitResponseBarrier, ComposedProjectionConfig,
    ComposedStorageConfig, CompoundIndexDef, CompoundIndexField, EligibilityPolicy, EngineError,
    EntryOutcome, FilterOp, FinalizeKind, IndexDeclaration, IndexType, InstanceFence,
    MetricsByQueryRequest, MultiClaimCommitEntry, MultiClaimCommitRequest, NewItem,
    ObjectLogConfig, OrderField, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, ProjectionRecoveryPolicy, QueryFilter, QueueDefinition,
    QueueId, QueueIndex, QueueKey, RangeScanRequest, RecurrencePolicy, RequestId, RetryPolicy,
    SecretValue, SegmentSettings, SideRecord, SortDirection, TenantId, TypedValue, UtcTimestamp,
};
use fireweed_engine::DurabilityClass;
use fireweed_memory::ManualClock;
use fireweed_objectlog::segmented::S3BlobStore;
use futures::executor::block_on;
use rusqlite::{Connection, params};

fn runtime_env(suffix: &str) -> Result<String, std::env::VarError> {
    std::env::var(format!("FIREWEED_{suffix}"))
}

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn definition(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("composed-tenant").unwrap(),
        queue_id: QueueId::new(queue_id).unwrap(),
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
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn queue(queue_id: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("composed-tenant").unwrap(),
        QueueId::new(queue_id).unwrap(),
    )
}

fn item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(format!("payload-{priority}").into()),
        ..NewItem::default()
    }
}

struct PanicClock(AtomicBool);

impl PanicClock {
    fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    fn arm(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Clock for PanicClock {
    fn now(&self) -> UtcTimestamp {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "explicit query-claim times must not consult the injected clock"
        );
        UtcTimestamp::new(0, 0).unwrap()
    }
}

fn local_config(root: &Path, sqlite: &Path) -> ComposedStorageConfig {
    ComposedStorageConfig {
        object_log: ObjectLogConfig::Local {
            root: root.to_path_buf(),
        },
        object_log_authority: fireweed::ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Sqlite {
            path: sqlite.to_path_buf(),
        },
        response_barrier: CommitResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentSettings::new(64 * 1024, 5).unwrap(),
        namespace: format!("sqlite-local-{}", nonce()),
        recovery: ProjectionRecoveryPolicy::default(),
    }
}

fn open_sqlite_cell(config: ComposedStorageConfig, clock: Arc<dyn Clock>) -> fireweed::Fireweed {
    match &config.object_log {
        ObjectLogConfig::Local { .. } => fireweed::open_composed_sqlite(config, clock).unwrap(),
        ObjectLogConfig::S3Compatible { .. } => {
            // Whitebox path into crate-private S3 open (same dispatch as public open_objectlog_sqlite).
            crate::open_s3_composed_sqlite(config, clock).unwrap()
        }
    }
}

fn assert_delete_rebuild(
    config: ComposedStorageConfig,
    queue_id: &str,
) -> (fireweed::QueueMetrics, fireweed::ItemId, fireweed::ItemId) {
    let clock = Arc::new(ManualClock::at(1_000));
    let fireweed = open_sqlite_cell(config, clock);
    let key = queue(queue_id);
    block_on(fireweed.create_queue(definition(queue_id))).unwrap();
    let request = RequestId::new(format!("request-{queue_id}")).unwrap();
    let (first, first_disp) =
        block_on(fireweed.push_with_request_id(&key, request.clone(), item(10))).unwrap();
    assert_eq!(first_disp, fireweed::PushDisposition::Fresh);
    let second = block_on(fireweed.push(&key, item(20))).unwrap();

    let expected = block_on(fireweed.metrics(&key)).unwrap();
    assert_eq!(expected.pending, 2);
    let verification = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    assert_eq!(
        verification.projection_sequence, verification.authoritative_sequence,
        "strict success is immediately visible in the durable SQLite image"
    );

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    assert_eq!(
        block_on(fireweed.metrics(&key)).unwrap(),
        expected,
        "deleting SQLite leaves the hot projection untouched"
    );
    let rebuilt = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 2);
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(fireweed.peek(&key, 10))
            .unwrap()
            .into_iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![first, second],
        "rebuild reconstructs the exact normalized resident set"
    );
    let (replayed, replay_disp) =
        block_on(fireweed.push_with_request_id(&key, request, item(10))).unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(
        replayed, first,
        "same-body replay returns the original item without a duplicate transition"
    );
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap(), expected);
    (expected, first, second)
}

fn assert_strict_commit_transition_round_trip(config: ComposedStorageConfig, queue_id: &str) {
    let clock = Arc::new(ManualClock::at(1_000));
    let fireweed = fireweed::open_composed_sqlite(config, clock).unwrap();
    let key = queue(queue_id);
    block_on(fireweed.create_queue(definition(queue_id))).unwrap();

    let caps = fireweed.commit_capabilities(&key).unwrap();
    assert!(caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.authoritative_recovery_reads);
    assert_eq!(caps.durability_class, DurabilityClass::Atomic);

    let request = RequestId::new(format!("request-{queue_id}")).unwrap();
    let (first, _) =
        block_on(fireweed.push_with_request_id(&key, request.clone(), item(10))).unwrap();
    let second = block_on(fireweed.push(&key, item(20))).unwrap();
    let claimed = block_on(fireweed.claim(&key, 1, 30_000)).unwrap();
    let claim = &claimed[0];
    let claim_ref = ClaimRef {
        item_id: claim.item_id,
        lease_token: claim
            .lease_token
            .clone()
            .expect("claimed item carries a lease token"),
        lease_expires_at: claim.lease_expires_at,
        item_version: claim.item_version,
    };
    let transition_request_id = RequestId::new(format!("txn-{queue_id}")).unwrap();
    let transition = CommitRequest {
        request_id: Some(transition_request_id.clone()),
        entries: vec![CommitEntry {
            claim_ref,
            finalize: FinalizeKind::Complete,
            side_records: vec![SideRecord {
                key: b"state/run-1".to_vec(),
                payload: Bytes::copy_from_slice(b"audit-bytes"),
            }],
            lifecycle_items: vec![item(30)],
            instance_fence: Some(InstanceFence {
                instance_key: b"wf-1".to_vec(),
                expected: 0,
                next: 1,
            }),
        }],
    };
    let outcomes = block_on(fireweed.commit(&key, transition)).unwrap();
    assert_eq!(outcomes.len(), 1);
    let lifecycle_id = match &outcomes[0] {
        EntryOutcome::Committed { lifecycle_item_ids } => {
            assert_eq!(lifecycle_item_ids.len(), 1);
            lifecycle_item_ids[0]
        }
        other => panic!("expected committed outcome, got {other:?}"),
    };

    let expected = block_on(fireweed.metrics(&key)).unwrap();
    assert_eq!(
        (expected.pending, expected.leased, expected.complete),
        (2, 0, 1)
    );
    assert_eq!(
        block_on(fireweed.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );
    assert_eq!(
        block_on(fireweed.side_record(&key, b"state/run-1"))
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    assert!(
        block_on(
            fireweed
                .projection_control()
                .expect("projection control")
                .verify()
        )
        .is_err()
    );
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();

    assert_eq!(block_on(fireweed.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(fireweed.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );
    assert_eq!(
        block_on(fireweed.side_record(&key, b"state/run-1"))
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );
    let replay = block_on(fireweed.commit(
        &key,
        CommitRequest {
            request_id: Some(transition_request_id.clone()),
            entries: vec![CommitEntry {
                claim_ref: ClaimRef {
                    item_id: claim.item_id,
                    lease_token: claim
                        .lease_token
                        .clone()
                        .expect("claimed item carries a lease token"),
                    lease_expires_at: claim.lease_expires_at,
                    item_version: claim.item_version,
                },
                finalize: FinalizeKind::Complete,
                side_records: vec![SideRecord {
                    key: b"state/run-1".to_vec(),
                    payload: Bytes::copy_from_slice(b"audit-bytes"),
                }],
                lifecycle_items: vec![item(30)],
                instance_fence: Some(InstanceFence {
                    instance_key: b"wf-1".to_vec(),
                    expected: 0,
                    next: 1,
                }),
            }],
        },
    ))
    .unwrap();
    assert_eq!(replay, outcomes);
    let recovery = block_on(fireweed.explain_commit(&key, transition_request_id))
        .unwrap()
        .expect("committed transition survives delete + rebuild");
    assert_eq!(recovery.entries.len(), 1);
    assert_eq!(recovery.entries[0].consumed_input_id, claim.item_id);
    // fireweed-bf03cbf5: no longer retained in the durable outcome — see
    // `fireweed_engine::EntryRecovery::side_record_keys`.
    assert_eq!(recovery.entries[0].side_record_keys, Vec::<Vec<u8>>::new());
    assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(recovery.entries[0].instance, Some((b"wf-1".to_vec(), 1)));
    let (replayed, replay_disp) =
        block_on(fireweed.push_with_request_id(&key, request, item(10))).unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed, first);
}

#[test]
fn public_objectlog_sqlite_delete_and_rebuild() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-sqlite-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    let queue_id = "durable-local";
    let (expected, first, second) = assert_delete_rebuild(config.clone(), queue_id);

    let reopened =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(2_000))).unwrap();
    let key = queue(queue_id);
    assert_eq!(block_on(reopened.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(reopened.peek(&key, 10))
            .unwrap()
            .into_iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    drop(reopened);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_bounded_mutation_replays_from_authoritative_log() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-mutation-{}", nonce()));
    let config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("durable-mutation");
    let mut queue_definition = definition("durable-mutation");
    queue_definition.typed_indexes = vec![QueueIndex {
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
    }];
    block_on(fireweed.create_queue(queue_definition)).unwrap();
    let item_id = block_on(fireweed.push(
        &key,
        NewItem {
            entity: Some(serde_json::json!({ "kind": "effect", "suppressed": false })),
            ..NewItem::default()
        },
    ))
    .unwrap();
    let mut set_fields = BTreeMap::new();
    set_fields.insert("suppressed".into(), TypedValue::Bool(true));
    let result = block_on(fireweed.bounded_mutation(
        &key,
        BoundedMutationRequest {
            index: Some("by_kind_suppressed".into()),
            filters: vec![QueryFilter {
                field: "kind".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("effect".into()),
            }],
            set_fields,
            max_scan_rows: 100,
        },
    ))
    .unwrap();
    assert_eq!(result.results[0].item_id, item_id);

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    let rows = block_on(fireweed.range_scan(
        &key,
        RangeScanRequest {
            index: Some("by_kind_suppressed".into()),
            filters: vec![QueryFilter {
                field: "suppressed".into(),
                op: FilterOp::Eq,
                value: TypedValue::Bool(true),
            }],
            order_by: vec![OrderField {
                field: "suppressed".into(),
                direction: SortDirection::Ascending,
            }],
            page_size: 10,
            cursor: None,
        },
    ))
    .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].item_id, item_id);
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_strict_commit_transition_round_trip() {
    let fixture =
        std::env::temp_dir().join(format!("fireweed-public-strict-transition-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    assert_strict_commit_transition_round_trip(config, "strict-transition-queue");
    let _ = fs::remove_dir_all(fixture);
}

/// Bead fireweed-6072ff52: `side_records_by_prefix` reads back one instance's audit chain in key
/// order, stays isolated from a sibling instance's records under a different prefix, and pages via
/// `next_cursor` on the objectlog+sqlite composition (the coverage gap the plain composed sqlite
/// backend already closed under fireweed-e47e9287).
#[test]
fn public_objectlog_sqlite_side_records_by_prefix_pages_ordered() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-side-prefix-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    let queue_id = "side-prefix-queue";
    let key = queue(queue_id);
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    block_on(fireweed.create_queue(definition(queue_id))).unwrap();

    block_on(fireweed.push(&key, item(10))).unwrap();
    let claimed = block_on(fireweed.claim(&key, 1, 30_000)).unwrap();
    let claim = &claimed[0];
    let claim_ref = ClaimRef {
        item_id: claim.item_id,
        lease_token: claim
            .lease_token
            .clone()
            .expect("claimed item carries a lease token"),
        lease_expires_at: claim.lease_expires_at,
        item_version: claim.item_version,
    };
    let side = |key: &str, payload: &str| SideRecord {
        key: key.as_bytes().to_vec(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    };
    let transition = CommitRequest {
        request_id: Some(RequestId::new(format!("txn-{queue_id}")).unwrap()),
        entries: vec![CommitEntry {
            claim_ref,
            finalize: FinalizeKind::Complete,
            side_records: vec![
                side("audit:instance-1:001", "a1"),
                side("audit:instance-1:003", "a3"),
                side("audit:instance-1:002", "a2"),
                side("audit:instance-2:001", "other-instance"),
            ],
            lifecycle_items: vec![],
            instance_fence: None,
        }],
    };
    block_on(fireweed.commit(&key, transition)).unwrap();

    let first_page =
        block_on(fireweed.side_records_by_prefix(&key, b"audit:instance-1:", 2, None)).unwrap();
    assert_eq!(
        first_page.entries,
        vec![
            (b"audit:instance-1:001".to_vec(), Bytes::from_static(b"a1")),
            (b"audit:instance-1:002".to_vec(), Bytes::from_static(b"a2")),
        ]
    );
    let cursor = first_page
        .next_cursor
        .clone()
        .expect("a third matching entry remains");
    assert_eq!(cursor, b"audit:instance-1:003".to_vec());

    let second_page =
        block_on(fireweed.side_records_by_prefix(&key, b"audit:instance-1:", 2, Some(cursor)))
            .unwrap();
    assert_eq!(
        second_page.entries,
        vec![(b"audit:instance-1:003".to_vec(), Bytes::from_static(b"a3"))]
    );
    assert_eq!(
        second_page.next_cursor, None,
        "the prefix's key range is exhausted"
    );

    let other =
        block_on(fireweed.side_records_by_prefix(&key, b"audit:instance-2:", 10, None)).unwrap();
    assert_eq!(
        other.entries,
        vec![(
            b"audit:instance-2:001".to_vec(),
            Bytes::from_static(b"other-instance")
        )]
    );

    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_multi_claim_continuation_rebuilds_exactly_once() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-multi-claim-{}", nonce()));
    let config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    let queue_id = "multi-claim-transition";
    let key = queue(queue_id);
    let fireweed =
        fireweed::open_composed_sqlite(config.clone(), Arc::new(ManualClock::at(1_000))).unwrap();
    block_on(fireweed.create_queue(definition(queue_id))).unwrap();
    block_on(fireweed.push_batch(&key, vec![item(10), item(11)])).unwrap();
    let claimed = block_on(fireweed.claim(&key, 2, 30_000)).unwrap();
    assert_eq!(claimed.len(), 2);
    let to_ref = |item: &fireweed::ClaimedItem| ClaimRef {
        item_id: item.item_id,
        lease_token: item
            .lease_token
            .clone()
            .expect("claimed item carries token"),
        lease_expires_at: item.lease_expires_at,
        item_version: item.item_version,
    };
    let primary = to_ref(&claimed[0]);
    let additional = to_ref(&claimed[1]);
    let request_id = RequestId::new("strict-result-await-continuation").unwrap();
    let request = MultiClaimCommitRequest {
        request_id: Some(request_id.clone()),
        entries: vec![MultiClaimCommitEntry {
            claim_ref: primary.clone(),
            additional_claim_refs: vec![additional.clone()],
            finalize: FinalizeKind::Complete,
            side_records: vec![SideRecord {
                key: b"instance/result-await".to_vec(),
                payload: Bytes::copy_from_slice(b"revision-2"),
            }],
            lifecycle_items: vec![item(20)],
            instance_fence: Some(InstanceFence {
                instance_key: b"result-await".to_vec(),
                expected: 0,
                next: 2,
            }),
        }],
    };
    let first = block_on(fireweed.commit_multi_claim(&key, request.clone())).unwrap();
    let continuation_id = match &first[0] {
        EntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
        other => panic!("expected committed multi-claim entry, got {other:?}"),
    };
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert_eq!(
        block_on(fireweed.commit_multi_claim(&key, request)).unwrap(),
        first
    );
    drop(fireweed);

    let reopened =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(2_000))).unwrap();
    let recovery = block_on(reopened.explain_commit(&key, request_id))
        .unwrap()
        .expect("multi-claim commit survives strict reopen");
    assert_eq!(recovery.entries[0].consumed_input_id, primary.item_id);
    assert_eq!(
        recovery.entries[0].additional_consumed_input_ids,
        vec![additional.item_id]
    );
    assert_eq!(
        block_on(reopened.side_record(&key, b"instance/result-await"))
            .unwrap()
            .as_deref(),
        Some(b"revision-2".as_slice())
    );
    let continuation = block_on(reopened.claim(&key, 10, 30_000)).unwrap();
    assert_eq!(continuation.len(), 1);
    assert_eq!(continuation[0].item_id, continuation_id);
    assert!(
        block_on(reopened.claim(&key, 10, 30_000))
            .unwrap()
            .is_empty()
    );
    drop(reopened);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_async_supports_authoritative_log_commit() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-async-{}", nonce()));
    let mut config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    config.response_barrier = CommitResponseBarrier::AsyncProjection;
    config.async_projection = Some(AsyncProjectionSpec::default());
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("async-queue");
    block_on(fireweed.create_queue(definition("async-queue"))).unwrap();

    let caps = fireweed.commit_capabilities(&key).unwrap();
    assert!(!caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.non_work_side_records);
    assert!(caps.authoritative_recovery_reads);
    assert_eq!(caps.durability_class, DurabilityClass::EventualApply);

    block_on(fireweed.push(&key, item(1))).unwrap();
    let claimed = block_on(fireweed.claim(&key, 1, 30_000)).unwrap();
    let claimed = &claimed[0];
    let outcomes = block_on(fireweed.commit(
        &key,
        CommitRequest {
            request_id: None,
            entries: vec![CommitEntry {
                claim_ref: ClaimRef {
                    item_id: claimed.item_id,
                    lease_token: claimed.lease_token.clone().unwrap(),
                    lease_expires_at: claimed.lease_expires_at,
                    item_version: claimed.item_version,
                },
                finalize: FinalizeKind::Complete,
                side_records: vec![],
                lifecycle_items: vec![],
                instance_fence: None,
            }],
        },
    ))
    .unwrap();
    assert!(matches!(
        outcomes.as_slice(),
        [EntryOutcome::Committed { .. }]
    ));
    let metrics = block_on(fireweed.metrics(&key)).unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (0, 0, 1)
    );

    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_verification_is_exact_per_queue() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-verify-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let dominant = queue("dominant-queue");
    let behind = queue("behind-queue");
    block_on(fireweed.create_queue(definition("dominant-queue"))).unwrap();
    block_on(fireweed.create_queue(definition("behind-queue"))).unwrap();
    block_on(fireweed.push(&dominant, item(1))).unwrap();
    block_on(fireweed.push(&dominant, item(2))).unwrap();
    block_on(fireweed.push(&behind, item(3))).unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();

    let connection = Connection::open(&sqlite).unwrap();
    connection
        .execute(
            "UPDATE relational_cursor SET next_seq=0 WHERE tenant=?1 AND queue=?2",
            params!["composed-tenant", "behind-queue"],
        )
        .unwrap();
    drop(connection);
    assert!(
        block_on(
            fireweed
                .projection_control()
                .expect("projection control")
                .verify()
        )
        .is_err(),
        "a caught-up higher-sequence queue must not mask a behind queue"
    );
    let rebuilt = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 3);
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_lifecycle_interleaves_without_replay_gaps() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-interleave-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let mut config = local_config(&root, &sqlite);
    config.response_barrier = CommitResponseBarrier::AsyncProjection;
    config.async_projection = Some(AsyncProjectionSpec::default());
    let fireweed =
        Arc::new(fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap());
    let key = queue("interleaved-queue");
    block_on(fireweed.create_queue(definition("interleaved-queue"))).unwrap();
    block_on(fireweed.push(&key, item(0))).unwrap();

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    assert!(matches!(
        block_on(fireweed.push(&key, item(1))),
        Err(fireweed::EngineError::Unavailable)
    ));
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap().pending, 1);
    assert!(
        block_on(
            fireweed
                .projection_control()
                .expect("projection control")
                .verify()
        )
        .is_err()
    );
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    block_on(fireweed.push(&key, item(1))).unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();

    let writer = Arc::clone(&fireweed);
    let writer_key = key.clone();
    let thread = std::thread::spawn(move || {
        for priority in 2..22 {
            loop {
                match block_on(writer.push(&writer_key, item(priority))) {
                    Ok(_) => break,
                    Err(EngineError::Unavailable) => std::thread::yield_now(),
                    Err(error) => panic!("concurrent lifecycle writer failed: {error}"),
                }
            }
        }
    });
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    thread.join().unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap().pending, 22);
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_async_verify_drains_deferred_checkpoint() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-verify-drain-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let mut config = local_config(&root, &sqlite);
    config.response_barrier = CommitResponseBarrier::AsyncProjection;
    config.async_projection = Some(AsyncProjectionSpec::default());
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("verify-drain-queue");
    block_on(fireweed.create_queue(definition("verify-drain-queue"))).unwrap();
    block_on(fireweed.push(&key, item(1))).unwrap();

    let verification = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    assert_eq!(
        verification.projection_sequence, verification.authoritative_sequence,
        "verification must synchronously drain an already-admitted async SQLite checkpoint"
    );
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_strict_writes_fail_closed_while_projection_is_deleted() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-strict-offline-{}", nonce()));
    let config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("strict-offline-queue");
    block_on(fireweed.create_queue(definition("strict-offline-queue"))).unwrap();
    block_on(fireweed.push(&key, item(0))).unwrap();
    let claimed = block_on(fireweed.claim(&key, 1, 30_000)).unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    assert!(matches!(
        block_on(fireweed.push(&key, item(1))),
        Err(fireweed::EngineError::Unavailable)
    ));
    assert!(matches!(
        block_on(fireweed.ack(&key, [claimed[0].item_id])),
        Err(fireweed::EngineError::Unavailable)
    ));
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    block_on(fireweed.ack(&key, [claimed[0].item_id])).unwrap();
    block_on(fireweed.push(&key, item(2))).unwrap();
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap().pending, 1);
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_filtered_metrics_survive_delete_and_rebuild() {
    let fixture =
        std::env::temp_dir().join(format!("fireweed-public-filtered-metrics-{}", nonce()));
    let config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    let fireweed =
        fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("filtered-metrics-queue");
    let mut queue_definition = definition("filtered-metrics-queue");
    queue_definition.typed_indexes = vec![QueueIndex {
        name: "by_record_kind_scheduled_at".to_string(),
        declaration: IndexDeclaration::Compound(CompoundIndexDef {
            fields: vec![
                CompoundIndexField {
                    field: "record_kind".to_string(),
                    index_type: IndexType::String,
                },
                CompoundIndexField {
                    field: "scheduled_at".to_string(),
                    index_type: IndexType::Datetime,
                },
            ],
            unique: false,
        }),
    }];
    block_on(fireweed.create_queue(queue_definition)).unwrap();

    let mut items = Vec::new();
    for (priority_base, state) in [
        (0, "complete"),
        (10, "failed"),
        (20, "leased"),
        (30, "pending"),
    ] {
        for (offset, record_kind) in ["transition", "effect", "await", "result"]
            .into_iter()
            .enumerate()
        {
            items.push(NewItem {
                priority: Some(PriorityValue::Int64(priority_base + offset as i64)),
                entity: Some(serde_json::json!({
                    "record_kind": record_kind,
                    "scheduled_at": "2026-07-19T12:00:00Z",
                    "expected_state": state
                })),
                ..NewItem::default()
            });
        }
    }
    let ids = block_on(fireweed.push_batch(&key, items)).unwrap();
    let complete = block_on(fireweed.claim(&key, 4, 30_000)).unwrap();
    assert_eq!(
        complete.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        ids[0..4]
    );
    block_on(fireweed.ack(&key, complete.iter().map(|item| item.item_id))).unwrap();
    let failed = block_on(fireweed.claim(&key, 4, 30_000)).unwrap();
    assert_eq!(
        failed.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        ids[4..8]
    );
    block_on(fireweed.fail(&key, failed.iter().map(|item| item.item_id))).unwrap();
    let leased = block_on(fireweed.claim(&key, 4, 30_000)).unwrap();
    assert_eq!(
        leased.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        ids[8..12]
    );

    let request = MetricsByQueryRequest {
        index: Some("by_record_kind_scheduled_at".to_string()),
        filters: vec![QueryFilter {
            field: "record_kind".to_string(),
            op: FilterOp::Eq,
            value: TypedValue::String("transition".to_string()),
        }],
    };
    let ordinary_before = block_on(fireweed.metrics(&key)).unwrap();
    assert_eq!(
        (
            ordinary_before.pending,
            ordinary_before.leased,
            ordinary_before.complete,
            ordinary_before.failed,
        ),
        (4, 4, 4, 4)
    );
    let expected = block_on(fireweed.metrics_by_query(&key, request.clone())).unwrap();
    assert_eq!(
        (
            expected.pending,
            expected.leased,
            expected.complete,
            expected.failed
        ),
        (1, 1, 1, 1)
    );
    let invalid = MetricsByQueryRequest {
        index: Some("by_record_kind_scheduled_at".to_string()),
        filters: vec![QueryFilter {
            field: "private_payload".to_string(),
            op: FilterOp::Eq,
            value: TypedValue::String("x".to_string()),
        }],
    };
    assert!(matches!(
        block_on(fireweed.metrics_by_query(&key, invalid)),
        Err(EngineError::Invalid(_))
    ));

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    assert_eq!(
        block_on(fireweed.metrics_by_query(&key, request)).unwrap(),
        expected
    );
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap(), ordinary_before);
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_filtered_claim_survives_delete_and_rebuild() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-filtered-claim-{}", nonce()));
    let config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    let clock = Arc::new(PanicClock::new());
    let fireweed = fireweed::open_composed_sqlite(config, clock.clone()).unwrap();
    let key = queue("filtered-claim-queue");
    let mut queue_definition = definition("filtered-claim-queue");
    queue_definition.typed_indexes = vec![QueueIndex {
        name: "by_record_kind_scheduled_at".to_string(),
        declaration: IndexDeclaration::Compound(CompoundIndexDef {
            fields: vec![
                CompoundIndexField {
                    field: "record_kind".to_string(),
                    index_type: IndexType::String,
                },
                CompoundIndexField {
                    field: "scheduled_at".to_string(),
                    index_type: IndexType::Datetime,
                },
            ],
            unique: false,
        }),
    }];
    block_on(fireweed.create_queue(queue_definition)).unwrap();

    let caps = fireweed.hot_projection_capabilities(&key);
    assert!(caps.paired_capabilities_consistent());
    assert!(caps.range_scan);
    assert!(caps.claim_by_query);

    let records = [
        ("transition", "1970-01-01T00:01:40Z", 100),
        ("effect", "1970-01-01T00:01:41Z", 100),
        ("await", "1970-01-01T00:01:42Z", 100),
        ("result", "1970-01-01T00:01:43Z", 100),
        ("transition", "1970-01-01T00:03:20Z", 200),
    ];
    let ids = block_on(
        fireweed.push_batch(
            &key,
            records
                .into_iter()
                .enumerate()
                .map(
                    |(offset, (record_kind, scheduled_at, not_before))| NewItem {
                        priority: Some(PriorityValue::Int64(offset as i64)),
                        not_before: Some(UtcTimestamp::new(not_before, 0).unwrap()),
                        entity: Some(serde_json::json!({
                            "record_kind": record_kind,
                            "scheduled_at": scheduled_at,
                        })),
                        ..NewItem::default()
                    },
                )
                .collect(),
        ),
    )
    .unwrap();
    clock.arm();

    let request = ClaimByQueryRequest {
        index: Some("by_record_kind_scheduled_at".to_string()),
        filters: vec![QueryFilter {
            field: "record_kind".to_string(),
            op: FilterOp::Eq,
            value: TypedValue::String("transition".to_string()),
        }],
        order_by: OrderField {
            field: "scheduled_at".to_string(),
            direction: SortDirection::Ascending,
        },
        max_items: 1,
        lease_duration_ms: 30_000,
        worker_id: fireweed_core::WorkerId::new("transition-worker").unwrap(),
        request_id: Some(RequestId::new("transition-query-1").unwrap()),
    };
    let first = block_on(
        fireweed.claim_by_query_at(
            &key,
            request.clone(),
            ClaimByQueryAt::new()
                .eligibility_time(UtcTimestamp::new(150, 0).unwrap())
                .lease_time(UtcTimestamp::new(1_000, 0).unwrap()),
        ),
    )
    .unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].item_id, ids[0]);
    assert_eq!(
        first.items[0].lease_expires_at,
        UtcTimestamp::new(1_030, 0).unwrap()
    );
    assert!(
        block_on(fireweed.claimed(&key, &ids[1..4]))
            .unwrap()
            .is_empty()
    );

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();

    let second = block_on(
        fireweed.claim_by_query_at(
            &key,
            ClaimByQueryRequest {
                request_id: Some(RequestId::new("transition-query-2").unwrap()),
                ..request
            },
            ClaimByQueryAt::new()
                .eligibility_time(UtcTimestamp::new(250, 0).unwrap())
                .lease_time(UtcTimestamp::new(1_001, 0).unwrap()),
        ),
    )
    .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].item_id, ids[4]);
    assert!(
        block_on(fireweed.claimed(&key, &ids[1..4]))
            .unwrap()
            .is_empty()
    );

    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_lifecycle_seals_already_buffered_writes_before_reset() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-buffered-{}", nonce()));
    let mut config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    config.segments = SegmentSettings::new(16 * 1024 * 1024, 1_000).unwrap();
    let fireweed =
        Arc::new(fireweed::open_composed_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap());
    let key = queue("buffered-lifecycle-queue");
    block_on(fireweed.create_queue(definition("buffered-lifecycle-queue"))).unwrap();

    assert_eq!(
        fireweed.test_buffered_group_commit_commands(),
        Some(0),
        "LogEngine owns group commit; the facade exposes no dual-stack buffer"
    );
    block_on(fireweed.push(&key, item(7))).unwrap();

    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .delete(),
    )
    .unwrap();
    let rebuilt = block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .rebuild(),
    )
    .unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 1);
    block_on(
        fireweed
            .projection_control()
            .expect("projection control")
            .verify(),
    )
    .unwrap();
    assert_eq!(block_on(fireweed.metrics(&key)).unwrap().pending, 1);
    drop(fireweed);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_namespaces_isolate_shared_object_root() {
    let fixture = std::env::temp_dir().join(format!("fireweed-public-namespace-{}", nonce()));
    let root = fixture.join("shared-objects");
    let mut first_config = local_config(&root, &fixture.join("first.sqlite"));
    first_config.namespace = "first namespace".into();
    let mut second_config = local_config(&root, &fixture.join("second.sqlite"));
    second_config.namespace = "second namespace".into();
    let key = queue("shared-queue-name");

    let first =
        fireweed::open_composed_sqlite(first_config.clone(), Arc::new(ManualClock::at(1_000)))
            .unwrap();
    block_on(first.create_queue(definition("shared-queue-name"))).unwrap();
    block_on(first.push(&key, item(11))).unwrap();
    drop(first);

    let second =
        fireweed::open_composed_sqlite(second_config.clone(), Arc::new(ManualClock::at(1_000)))
            .unwrap();
    assert!(
        block_on(second.create_queue(definition("shared-queue-name")))
            .unwrap()
            .created,
        "the second namespace must not recover the first namespace's queue catalog"
    );
    block_on(second.push(&key, item(22))).unwrap();
    drop(second);

    let first =
        fireweed::open_composed_sqlite(first_config, Arc::new(ManualClock::at(2_000))).unwrap();
    let second =
        fireweed::open_composed_sqlite(second_config, Arc::new(ManualClock::at(2_000))).unwrap();
    assert_eq!(
        block_on(first.peek(&key, 10)).unwrap()[0].priority,
        Some(PriorityValue::Int64(11))
    );
    assert_eq!(
        block_on(second.peek(&key, 10)).unwrap()[0].priority,
        Some(PriorityValue::Int64(22))
    );
    drop((first, second));
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_s3_sqlite_delete_and_rebuild() {
    let endpoint = match runtime_env("S3_TEST_URL").or_else(|_| runtime_env("S3_TEST_ENDPOINT")) {
        Ok(value) => value,
        Err(_) => {
            panic!(
                "FIREWEED_S3_TEST_URL or FIREWEED_S3_TEST_ENDPOINT required (fail-closed live S3; no LOUD skip)"
            );
        }
    };
    let bucket = runtime_env("S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed-test".into());
    let access = runtime_env("S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = runtime_env("S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let region = runtime_env("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    // crates.io object-log S3BlobStore has no create_bucket helper; the test bucket is operator-owned.
    let _ = S3BlobStore::new(&endpoint, &region, &bucket, &access, &secret);

    let fixture = std::env::temp_dir().join(format!("fireweed-public-s3-sqlite-{}", nonce()));
    let queue_id = format!("durable-s3-{}", nonce());
    let config = ComposedStorageConfig {
        object_log: ObjectLogConfig::S3Compatible {
            endpoint,
            bucket,
            region,
            access_key_id: SecretValue::new(access),
            secret_access_key: SecretValue::new(secret),
            allow_insecure_http: true,
        },
        object_log_authority: fireweed::ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Sqlite {
            path: fixture.join("projection.sqlite"),
        },
        response_barrier: CommitResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentSettings::new(64 * 1024, 5).unwrap(),
        namespace: format!("sqlite-s3-{}", nonce()),
        recovery: ProjectionRecoveryPolicy::default(),
    };
    let _ = assert_delete_rebuild(config, &queue_id);
    let _ = fs::remove_dir_all(fixture);
}
