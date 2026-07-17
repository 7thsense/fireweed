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
