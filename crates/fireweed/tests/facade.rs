#![allow(dead_code, unused_imports)]

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use fireweed::*;
use fireweed_memory::ManualClock;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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

fn at(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

fn metadata_fence_item(priority: i64, group: &str, region: &str) -> NewItem {
    let mut metadata = Metadata::new();
    metadata.insert("region", MetadataValue::String(region.to_string()));
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        group_key: Some(GroupKey::new(group).unwrap()),
        metadata,
        ..Default::default()
    }
}

fn metadata_equals_compatibility(group: &str, region: &str) -> ClaimCompatibility {
    ClaimCompatibility {
        group_key: Some(GroupKey::new(group).unwrap()),
        metadata_equals: BTreeMap::from([(
            "region".to_string(),
            MetadataValue::String(region.to_string()),
        )]),
        ..ClaimCompatibility::default()
    }
}

/// Compatibility-constrained item claims must not fail closed as Unavailable on memory projections
/// (AsyncLogReplayBackend / InMemoryProjection path).
#[tokio::test]
async fn claim_with_metadata_equals_filters_over_memory() {
    let fireweed = fireweed::open_memory(Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let match_id = fireweed
        .push(&q, metadata_fence_item(10, "group-a", "east"))
        .await
        .unwrap();
    fireweed
        .push(&q, metadata_fence_item(5, "group-a", "west"))
        .await
        .unwrap();
    fireweed
        .push(&q, metadata_fence_item(1, "group-b", "east"))
        .await
        .unwrap();

    let claimed = fireweed
        .claim_with(
            &q,
            10,
            30_000,
            metadata_equals_compatibility("group-a", "east"),
        )
        .await
        .expect("metadata_equals claim must not be Unavailable on memory");
    assert_eq!(
        claimed.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![match_id]
    );
    assert_eq!(
        claimed[0].group_key.as_ref().map(|g| g.as_str()),
        Some("group-a")
    );
}

/// Same fence over sqlite (log × memory projection via open_sqlite, and relational projection).
#[tokio::test]
async fn claim_with_metadata_equals_filters_over_sqlite() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let log_path = std::env::temp_dir().join(format!(
        "fireweed-facade-compat-claim-log-{}-{nonce}.db",
        std::process::id()
    ));
    let rel_path = std::env::temp_dir().join(format!(
        "fireweed-facade-compat-claim-rel-{}-{nonce}.db",
        std::process::id()
    ));

    async fn exercise(label: &str, fireweed: fireweed::Fireweed) {
        let q = qkey();
        fireweed.create_queue(qdef()).await.unwrap();
        let match_id = fireweed
            .push(&q, metadata_fence_item(10, "group-a", "east"))
            .await
            .unwrap();
        fireweed
            .push(&q, metadata_fence_item(5, "group-a", "west"))
            .await
            .unwrap();
        fireweed
            .push(&q, metadata_fence_item(1, "group-b", "east"))
            .await
            .unwrap();

        let claimed = fireweed
            .claim_with(
                &q,
                10,
                30_000,
                metadata_equals_compatibility("group-a", "east"),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{label}: metadata_equals claim must not fail: {error:?}")
            });
        assert_eq!(
            claimed.iter().map(|item| item.item_id).collect::<Vec<_>>(),
            vec![match_id],
            "{label}: only the group-a/east fence match is claimed"
        );
    }

    exercise(
        "open_sqlite",
        fireweed::open_sqlite(log_path.to_str().unwrap(), Arc::new(ManualClock::at(0))).unwrap(),
    )
    .await;
    exercise(
        "open_sqlite_relational",
        fireweed::open_sqlite_relational(rel_path.to_str().unwrap(), Arc::new(ManualClock::at(0)))
            .unwrap(),
    )
    .await;

    let _ = std::fs::remove_file(log_path);
    let _ = std::fs::remove_file(rel_path);
}

#[tokio::test]
async fn stamped_discovery_preserves_ungrouped_order_through_relational_constructor() {
    let clock = Arc::new(ManualClock::at(10));
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "fireweed-facade-stamped-discovery-{}-{nonce}.db",
        std::process::id()
    ));
    let fireweed = fireweed::open_sqlite_relational(path.to_str().unwrap(), clock).unwrap();
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let ungrouped = at(10);
    let mut keyed = at(20);
    keyed.group_key = Some(GroupKey::new("keyed").unwrap());
    fireweed
        .push_batch(&q, vec![ungrouped, keyed])
        .await
        .unwrap();

    let existing = fireweed
        .discover_active_scopes(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    let stamped = fireweed
        .discover_active_scopes_stamped(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    assert_eq!(stamped.queue, q);
    assert_eq!(stamped.granularity, DiscoveryGranularity::Group);
    assert_eq!(stamped.scopes, existing);
    assert_eq!(
        stamped
            .scopes
            .iter()
            .map(|scope| scope.group_key.as_deref())
            .collect::<Vec<_>>(),
        vec![None, Some("keyed")]
    );

    drop(fireweed);
    std::fs::remove_file(path).unwrap();
}

/// fireweed-6486ed63 / fireweed-01802c42: facade-level request-id retention across reopen on
/// the public `open_sqlite` async log-replay product (Fresh / Replayed / RequestIdConflict).
///
/// Specifically asserts **changed-body-across-reopen** returns `RequestIdConflict` and same body
/// returns `Replayed` after a cold open (recovery rebuilds the push ledger fingerprints).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn request_id_push_retention_survives_open_sqlite_reopen() {
    use fireweed::open_sqlite;
    let path = std::env::temp_dir()
        .join(format!(
            "fireweed-facade-request-id-reopen-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);
    let q = qkey();
    let rid = RequestId::new("facade-reopen-req").unwrap();
    let first_ids = {
        let fireweed = open_sqlite(&path, Arc::new(ManualClock::at(0))).unwrap();
        fireweed.create_queue(qdef()).await.unwrap();
        assert!(
            fireweed
                .commit_capabilities(&q)
                .unwrap()
                .retained_commit_idempotency
        );
        let first = fireweed
            .push_batch_with_request_id(&q, rid.clone(), vec![at(10), at(20)])
            .await
            .unwrap();
        assert!(first.is_fresh());
        let replay = fireweed
            .push_batch_with_request_id(&q, rid.clone(), vec![at(10), at(20)])
            .await
            .unwrap();
        assert!(replay.is_replayed());
        assert_eq!(replay.item_ids, first.item_ids);
        assert_eq!(
            fireweed
                .push_batch_with_request_id(&q, rid.clone(), vec![at(11), at(22)])
                .await
                .unwrap_err(),
            EngineError::RequestIdConflict
        );
        first.item_ids.clone()
    };
    let reopened = open_sqlite(&path, Arc::new(ManualClock::at(0))).unwrap();
    reopened.create_queue(qdef()).await.unwrap();
    let replay = reopened
        .push_batch_with_request_id(&q, rid.clone(), vec![at(10), at(20)])
        .await
        .unwrap();
    assert!(
        replay.is_replayed(),
        "facade reopen must Replayed same body under retained request id"
    );
    assert_eq!(replay.item_ids, first_ids);
    assert_eq!(
        reopened
            .push_batch_with_request_id(&q, rid, vec![at(11), at(22)])
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "facade reopen must RequestIdConflict on changed body"
    );
    drop(reopened);
    let _ = std::fs::remove_file(&path);
}

/// AC fireweed-dd6cbcde: upsert → claim → commit_transition on objectlog × sqlite under Strict.
#[tokio::test]
#[cfg(all(feature = "objectlog", feature = "sqlite"))]
async fn objectlog_sqlite_strict_upsert_claim_commit_transition() {
    use fireweed::{
        EntryOutcome, ObjectLogAuthority, ObjectLogRuntimeConfig, ObjectLogStorage,
        ProjectionConfig, RecoveryAction, RecoveryPolicy, ResponseBarrier, SegmentConfig,
        SideRecord,
    };

    let root = std::env::temp_dir().join(format!(
        "fireweed-facade-objlog-sqlite-strict-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let config = ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: root.join("object-log"),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Sqlite {
            path: root.join("projection.sqlite"),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(262_144, 20).unwrap(),
        namespace: "upsert-claim-commit".into(),
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
    };
    let fireweed =
        fireweed::open_objectlog_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let caps = fireweed.commit_capabilities(&q).unwrap();
    assert!(
        caps.atomic_transition_commit,
        "Strict objectlog×sqlite must advertise atomic commit"
    );

    let key = ClientItemKey::new("work-1").unwrap();
    let upserted = fireweed.upsert(&q, key, at(10)).await.unwrap();
    let item_id = match upserted {
        fireweed_engine::UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected insert, got {other:?}"),
    };

    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].item_id, item_id);
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

    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: Some(RequestId::new("txn-1").unwrap()),
                entries: vec![CommitEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![SideRecord {
                        key: b"state/run-1".to_vec(),
                        payload: Bytes::copy_from_slice(b"done"),
                    }],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(&outcomes[0], EntryOutcome::Committed { .. }));
    assert_eq!(
        fireweed
            .side_record(&q, b"state/run-1")
            .await
            .unwrap()
            .as_deref(),
        Some(b"done".as_slice())
    );
    let metrics = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (0, 0, 1)
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// Bead fireweed-e47e9287: `Fireweed::side_records_by_prefix` is reachable through the public
/// trait-object facade (not just a concrete sqlite backend type), over the `sqlite`-log ×
/// `in-memory`-projection composition (`open_sqlite`). Proves ordered, prefix-isolated hydration of an
/// instance's audit chain — the read snorri needs to key audit records as
/// `audit:{workflow_instance_id}:{transition_request_id}` and enumerate one instance's chain from a single
/// prefix instead of tracking key lists in the checkpoint head.
#[tokio::test]
async fn side_records_by_prefix_reads_through_facade_over_sqlite_log() {
    use fireweed::open_sqlite;
    let path = std::env::temp_dir()
        .join(format!(
            "fireweed-facade-prefix-scan-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);
    let q = qkey();

    let fireweed = open_sqlite(&path, Arc::new(ManualClock::at(0))).unwrap();
    fireweed.create_queue(qdef()).await.unwrap();

    let key = ClientItemKey::new("work-1").unwrap();
    let upserted = fireweed.upsert(&q, key, at(10)).await.unwrap();
    let item_id = match upserted {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected insert, got {other:?}"),
    };
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].item_id, item_id);
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

    fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: Some(RequestId::new("txn-prefix").unwrap()),
                entries: vec![CommitEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![
                        SideRecord {
                            key: b"audit:instance-1:001".to_vec(),
                            payload: Bytes::from_static(b"one"),
                        },
                        SideRecord {
                            key: b"audit:instance-1:002".to_vec(),
                            payload: Bytes::from_static(b"two"),
                        },
                        SideRecord {
                            key: b"audit:instance-2:001".to_vec(),
                            payload: Bytes::from_static(b"other-instance"),
                        },
                    ],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();

    let page = fireweed
        .side_records_by_prefix(&q, b"audit:instance-1:", 1, None)
        .await
        .unwrap();
    assert_eq!(
        page.entries,
        vec![(b"audit:instance-1:001".to_vec(), Bytes::from_static(b"one"))]
    );
    let cursor = page.next_cursor.clone().expect("a second entry remains");

    let page2 = fireweed
        .side_records_by_prefix(&q, b"audit:instance-1:", 1, Some(cursor))
        .await
        .unwrap();
    assert_eq!(
        page2.entries,
        vec![(b"audit:instance-1:002".to_vec(), Bytes::from_static(b"two"))]
    );
    assert_eq!(page2.next_cursor, None);

    let _ = std::fs::remove_file(&path);
}
