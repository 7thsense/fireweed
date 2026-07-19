#![cfg(all(feature = "objectlog", feature = "sqlite"))]

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::executor::block_on;
use pqueue::{
    Bytes, ClaimRef, CommitCapabilities, CommitEntry, CommitRequest, EligibilityPolicy,
    EmbeddedDurabilityConfig, EmbeddedObjectLogConfig, EmbeddedProjectionConfig,
    EmbeddedRecoveryPolicy, EmbeddedResponseBarrier, EmbeddedSecret, EmbeddedSegmentConfig,
    EngineError, EntryOutcome, FinalizeKind, InstanceFence, LeaseToken, NewItem, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueDefinition, QueueId, QueueKey, RecurrencePolicy, RequestId, RetryPolicy, SideRecord,
    TenantId, UtcTimestamp,
};
use pqueue_engine::DurabilityClass;
use pqueue_memory::ManualClock;
use pqueue_objectlog::segmented::S3BlobStore;
use rusqlite::{Connection, params};

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn definition(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("embedded-tenant").unwrap(),
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
        TenantId::new("embedded-tenant").unwrap(),
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

fn local_config(root: &Path, sqlite: &Path) -> EmbeddedDurabilityConfig {
    EmbeddedDurabilityConfig {
        object_log: EmbeddedObjectLogConfig::Local {
            root: root.to_path_buf(),
        },
        projection: EmbeddedProjectionConfig::Sqlite {
            path: sqlite.to_path_buf(),
        },
        response_barrier: EmbeddedResponseBarrier::Strict,
        segments: EmbeddedSegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: format!("sqlite-local-{}", nonce()),
        recovery: EmbeddedRecoveryPolicy::default(),
    }
}

fn assert_delete_rehydrate(
    config: EmbeddedDurabilityConfig,
    queue_id: &str,
) -> (pqueue::QueueMetrics, pqueue::ItemId, pqueue::ItemId) {
    let clock = Arc::new(ManualClock::at(1_000));
    let pq = pqueue::open_embedded_sqlite(config, clock).unwrap();
    let key = queue(queue_id);
    block_on(pq.create_queue(definition(queue_id))).unwrap();
    let request = RequestId::new(format!("request-{queue_id}")).unwrap();
    let first = block_on(pq.push_with_request_id(&key, request.clone(), item(10))).unwrap();
    let second = block_on(pq.push(&key, item(20))).unwrap();

    let expected = block_on(pq.metrics(&key)).unwrap();
    assert_eq!(expected.pending, 2);
    let verification = block_on(pq.verify_projection()).unwrap();
    assert_eq!(
        verification.projection_sequence, verification.authoritative_sequence,
        "strict success is immediately visible in the durable SQLite image"
    );

    block_on(pq.delete_projection()).unwrap();
    assert_eq!(
        block_on(pq.metrics(&key)).unwrap(),
        expected,
        "deleting SQLite leaves the hot projection untouched"
    );
    let rebuilt = block_on(pq.rehydrate_projection()).unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 2);
    assert_eq!(block_on(pq.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(pq.peek(&key, 10))
            .unwrap()
            .into_iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![first, second],
        "rehydration reconstructs the exact normalized resident set"
    );
    assert_eq!(
        block_on(pq.push_with_request_id(&key, request, item(10))).unwrap(),
        first,
        "same-body replay returns the original item without a duplicate transition"
    );
    assert_eq!(block_on(pq.metrics(&key)).unwrap(), expected);
    (expected, first, second)
}

fn assert_strict_commit_transition_round_trip(config: EmbeddedDurabilityConfig, queue_id: &str) {
    let clock = Arc::new(ManualClock::at(1_000));
    let pq = pqueue::open_embedded_sqlite(config, clock).unwrap();
    let key = queue(queue_id);
    block_on(pq.create_queue(definition(queue_id))).unwrap();

    let caps = pq.commit_capabilities(&key).unwrap();
    assert!(caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.authoritative_recovery_reads);
    assert_eq!(caps.durability_class, DurabilityClass::Atomic);

    let request = RequestId::new(format!("request-{queue_id}")).unwrap();
    let first = block_on(pq.push_with_request_id(&key, request.clone(), item(10))).unwrap();
    let second = block_on(pq.push(&key, item(20))).unwrap();
    let claimed = block_on(pq.claim(&key, 1, 30_000)).unwrap();
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
    let outcomes = block_on(pq.commit(&key, transition)).unwrap();
    assert_eq!(outcomes.len(), 1);
    let lifecycle_id = match &outcomes[0] {
        EntryOutcome::Committed { lifecycle_item_ids } => {
            assert_eq!(lifecycle_item_ids.len(), 1);
            lifecycle_item_ids[0]
        }
        other => panic!("expected committed outcome, got {other:?}"),
    };

    let expected = block_on(pq.metrics(&key)).unwrap();
    assert_eq!(
        (expected.pending, expected.leased, expected.complete),
        (2, 0, 1)
    );
    assert_eq!(
        block_on(pq.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );
    assert_eq!(
        block_on(pq.side_record(&key, b"state/run-1"))
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );

    block_on(pq.delete_projection()).unwrap();
    assert!(block_on(pq.verify_projection()).is_err());
    block_on(pq.rehydrate_projection()).unwrap();
    block_on(pq.verify_projection()).unwrap();

    assert_eq!(block_on(pq.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(pq.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );
    assert_eq!(
        block_on(pq.side_record(&key, b"state/run-1"))
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );
    let replay = block_on(pq.commit(
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
    let recovery = block_on(pq.explain_commit(&key, transition_request_id))
        .unwrap()
        .expect("committed transition survives delete + rehydrate");
    assert_eq!(recovery.entries.len(), 1);
    assert_eq!(recovery.entries[0].consumed_input_id, claim.item_id);
    assert_eq!(
        recovery.entries[0].side_record_keys,
        vec![b"state/run-1".to_vec()]
    );
    assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(recovery.entries[0].instance, Some((b"wf-1".to_vec(), 1)));
    assert_eq!(
        block_on(pq.push_with_request_id(&key, request, item(10))).unwrap(),
        first
    );
}

#[test]
fn public_objectlog_sqlite_delete_and_rehydrate() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-sqlite-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    let queue_id = "durable-local";
    let (expected, first, second) = assert_delete_rehydrate(config.clone(), queue_id);

    let reopened = pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(2_000))).unwrap();
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
fn public_objectlog_sqlite_strict_commit_transition_round_trip() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-strict-transition-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    assert_strict_commit_transition_round_trip(config, "strict-transition-queue");
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_async_projection_remains_non_authoritative() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-async-{}", nonce()));
    let mut config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    config.response_barrier = EmbeddedResponseBarrier::AsyncProjection;
    let pq = pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("async-queue");
    block_on(pq.create_queue(definition("async-queue"))).unwrap();

    let caps = pq.commit_capabilities(&key).unwrap();
    assert_eq!(caps, CommitCapabilities::default());
    assert!(matches!(
        block_on(pq.commit(
            &key,
            CommitRequest {
                request_id: None,
                entries: vec![CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: pqueue::ItemId::new("1").unwrap(),
                        lease_token: LeaseToken::new("lease-1").unwrap(),
                        lease_expires_at: UtcTimestamp::new(1, 0).unwrap(),
                        item_version: 0,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            }
        )),
        Err(EngineError::Unavailable)
    ));

    drop(pq);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_verification_is_exact_per_queue() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-verify-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let config = local_config(&root, &sqlite);
    let pq = pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let dominant = queue("dominant-queue");
    let behind = queue("behind-queue");
    block_on(pq.create_queue(definition("dominant-queue"))).unwrap();
    block_on(pq.create_queue(definition("behind-queue"))).unwrap();
    block_on(pq.push(&dominant, item(1))).unwrap();
    block_on(pq.push(&dominant, item(2))).unwrap();
    block_on(pq.push(&behind, item(3))).unwrap();
    block_on(pq.verify_projection()).unwrap();

    let connection = Connection::open(&sqlite).unwrap();
    connection
        .execute(
            "UPDATE relational_cursor SET next_seq=0 WHERE tenant=?1 AND queue=?2",
            params!["embedded-tenant", "behind-queue"],
        )
        .unwrap();
    drop(connection);
    assert!(
        block_on(pq.verify_projection()).is_err(),
        "a caught-up higher-sequence queue must not mask a behind queue"
    );
    let rebuilt = block_on(pq.rehydrate_projection()).unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 3);
    block_on(pq.verify_projection()).unwrap();
    drop(pq);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_lifecycle_interleaves_without_replay_gaps() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-interleave-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let mut config = local_config(&root, &sqlite);
    config.response_barrier = EmbeddedResponseBarrier::AsyncProjection;
    let pq =
        Arc::new(pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap());
    let key = queue("interleaved-queue");
    block_on(pq.create_queue(definition("interleaved-queue"))).unwrap();
    block_on(pq.push(&key, item(0))).unwrap();

    block_on(pq.delete_projection()).unwrap();
    assert!(matches!(
        block_on(pq.push(&key, item(1))),
        Err(pqueue::EngineError::Unavailable)
    ));
    assert_eq!(block_on(pq.metrics(&key)).unwrap().pending, 1);
    assert!(block_on(pq.verify_projection()).is_err());
    block_on(pq.rehydrate_projection()).unwrap();
    block_on(pq.push(&key, item(1))).unwrap();
    block_on(pq.verify_projection()).unwrap();

    let writer = Arc::clone(&pq);
    let writer_key = key.clone();
    let thread = std::thread::spawn(move || {
        for priority in 2..22 {
            block_on(writer.push(&writer_key, item(priority))).unwrap();
        }
    });
    block_on(pq.rehydrate_projection()).unwrap();
    thread.join().unwrap();
    block_on(pq.rehydrate_projection()).unwrap();
    block_on(pq.verify_projection()).unwrap();
    assert_eq!(block_on(pq.metrics(&key)).unwrap().pending, 22);
    drop(pq);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_async_verify_drains_deferred_checkpoint() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-verify-drain-{}", nonce()));
    let root = fixture.join("objects");
    let sqlite = fixture.join("projection.sqlite");
    let mut config = local_config(&root, &sqlite);
    config.response_barrier = EmbeddedResponseBarrier::AsyncProjection;
    config.segments = EmbeddedSegmentConfig::new(1, 60_000).unwrap();
    let pq = pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("verify-drain-queue");
    block_on(pq.create_queue(definition("verify-drain-queue"))).unwrap();
    block_on(pq.push(&key, item(1))).unwrap();

    let verification = block_on(pq.verify_projection()).unwrap();
    assert_eq!(
        verification.projection_sequence, verification.authoritative_sequence,
        "verification must synchronously drain an already-admitted async SQLite checkpoint"
    );
    drop(pq);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_strict_writes_fail_closed_while_projection_is_deleted() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-strict-offline-{}", nonce()));
    let config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    let pq = pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap();
    let key = queue("strict-offline-queue");
    block_on(pq.create_queue(definition("strict-offline-queue"))).unwrap();
    block_on(pq.push(&key, item(0))).unwrap();
    let claimed = block_on(pq.claim(&key, 1, 30_000)).unwrap();
    block_on(pq.delete_projection()).unwrap();
    assert!(matches!(
        block_on(pq.push(&key, item(1))),
        Err(pqueue::EngineError::Unavailable)
    ));
    assert!(matches!(
        block_on(pq.ack(&key, [claimed[0].item_id])),
        Err(pqueue::EngineError::Unavailable)
    ));
    block_on(pq.rehydrate_projection()).unwrap();
    block_on(pq.ack(&key, [claimed[0].item_id])).unwrap();
    block_on(pq.push(&key, item(2))).unwrap();
    assert_eq!(block_on(pq.metrics(&key)).unwrap().pending, 1);
    drop(pq);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_lifecycle_seals_already_buffered_writes_before_reset() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-buffered-{}", nonce()));
    let mut config = local_config(&fixture.join("objects"), &fixture.join("projection.sqlite"));
    config.segments = EmbeddedSegmentConfig::new(16 * 1024 * 1024, 1_000).unwrap();
    let pq =
        Arc::new(pqueue::open_embedded_sqlite(config, Arc::new(ManualClock::at(1_000))).unwrap());
    let key = queue("buffered-lifecycle-queue");
    block_on(pq.create_queue(definition("buffered-lifecycle-queue"))).unwrap();

    let writer = Arc::clone(&pq);
    let writer_key = key.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        block_on(writer.push(&writer_key, item(7)))
    });
    started_rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(25));

    block_on(pq.delete_projection()).unwrap();
    assert!(
        thread.join().unwrap().is_ok(),
        "quiescence must seal the waiting push"
    );
    let rebuilt = block_on(pq.rehydrate_projection()).unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 1);
    block_on(pq.verify_projection()).unwrap();
    assert_eq!(block_on(pq.metrics(&key)).unwrap().pending, 1);
    drop(pq);
    let _ = fs::remove_dir_all(fixture);
}

#[test]
fn public_objectlog_sqlite_namespaces_isolate_shared_object_root() {
    let fixture = std::env::temp_dir().join(format!("pqueue-public-namespace-{}", nonce()));
    let root = fixture.join("shared-objects");
    let mut first_config = local_config(&root, &fixture.join("first.sqlite"));
    first_config.namespace = "first namespace".into();
    let mut second_config = local_config(&root, &fixture.join("second.sqlite"));
    second_config.namespace = "second namespace".into();
    let key = queue("shared-queue-name");

    let first =
        pqueue::open_embedded_sqlite(first_config.clone(), Arc::new(ManualClock::at(1_000)))
            .unwrap();
    block_on(first.create_queue(definition("shared-queue-name"))).unwrap();
    block_on(first.push(&key, item(11))).unwrap();
    drop(first);

    let second =
        pqueue::open_embedded_sqlite(second_config.clone(), Arc::new(ManualClock::at(1_000)))
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
        pqueue::open_embedded_sqlite(first_config, Arc::new(ManualClock::at(2_000))).unwrap();
    let second =
        pqueue::open_embedded_sqlite(second_config, Arc::new(ManualClock::at(2_000))).unwrap();
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
fn public_s3_sqlite_delete_and_rehydrate() {
    let endpoint = match std::env::var("PQUEUE_S3_TEST_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("SKIP public_s3_sqlite_delete_and_rehydrate: PQUEUE_S3_TEST_URL is unset");
            return;
        }
    };
    let bucket = std::env::var("PQUEUE_S3_TEST_BUCKET").unwrap_or_else(|_| "pqueue-test".into());
    let access = std::env::var("PQUEUE_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = std::env::var("PQUEUE_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let region = std::env::var("PQUEUE_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    S3BlobStore::new(&endpoint, &bucket, &access, &secret, &region)
        .unwrap()
        .create_bucket()
        .unwrap();

    let fixture = std::env::temp_dir().join(format!("pqueue-public-s3-sqlite-{}", nonce()));
    let queue_id = format!("durable-s3-{}", nonce());
    let config = EmbeddedDurabilityConfig {
        object_log: EmbeddedObjectLogConfig::S3Compatible {
            endpoint,
            bucket,
            region,
            access_key_id: EmbeddedSecret::new(access),
            secret_access_key: EmbeddedSecret::new(secret),
            allow_insecure_http: true,
        },
        projection: EmbeddedProjectionConfig::Sqlite {
            path: fixture.join("projection.sqlite"),
        },
        response_barrier: EmbeddedResponseBarrier::Strict,
        segments: EmbeddedSegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: format!("sqlite-s3-{}", nonce()),
        recovery: EmbeddedRecoveryPolicy::default(),
    };
    let _ = assert_delete_rehydrate(config, &queue_id);
    let _ = fs::remove_dir_all(fixture);
}
