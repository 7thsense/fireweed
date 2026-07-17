#![cfg(all(feature = "objectlog", feature = "sqlite"))]

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::executor::block_on;
use pqueue::{
    EligibilityPolicy, EmbeddedDurabilityConfig, EmbeddedObjectLogConfig, EmbeddedProjectionConfig,
    EmbeddedRecoveryPolicy, EmbeddedResponseBarrier, EmbeddedSecret, EmbeddedSegmentConfig,
    NewItem, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker,
    PriorityValue, QueueDefinition, QueueId, QueueKey, RecurrencePolicy, RequestId, RetryPolicy,
    TenantId,
};
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
