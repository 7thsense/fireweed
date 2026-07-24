#![cfg(all(feature = "objectlog", feature = "postgres"))]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::executor::block_on;
use postgres::{Client, NoTls};
use pqueue::{
    Bytes, ClaimRef, CommitEntry, CommitRequest, EligibilityPolicy, EmbeddedDurabilityConfig,
    EmbeddedObjectLogConfig, EmbeddedProjectionConfig, EmbeddedRecoveryPolicy,
    EmbeddedResponseBarrier, EmbeddedSecret, EmbeddedSegmentConfig, EngineError, EntryOutcome,
    FinalizeKind, InstanceFence, NewItem, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, QueueKey,
    RangeScanRequest, RecurrencePolicy, RequestId, RetryPolicy, SideRecord, TenantId,
};
use pqueue_engine::DurabilityClass;
use pqueue_memory::ManualClock;
use pqueue_objectlog::segmented::S3BlobStore;

fn unique_fixture(name: &str) -> (PathBuf, String) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    (
        std::env::temp_dir().join(format!("pqueue-{name}-{nonce}")),
        format!("pqueue_{name}_{nonce}"),
    )
}

fn unique_bucket(tag: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("pqueue-{tag}-{}", nonce % 1_000_000_000)
}

fn config(root: &Path, schema: &str, url: &str) -> EmbeddedDurabilityConfig {
    EmbeddedDurabilityConfig {
        object_log: EmbeddedObjectLogConfig::Local {
            root: root.to_path_buf(),
        },
        projection: EmbeddedProjectionConfig::Postgres {
            url: EmbeddedSecret::new(url),
        },
        response_barrier: EmbeddedResponseBarrier::Strict,
        segments: EmbeddedSegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: schema.to_owned(),
        recovery: EmbeddedRecoveryPolicy::default(),
    }
}

fn definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("embedded-tenant").unwrap(),
        queue_id: QueueId::new("durable-queue").unwrap(),
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

fn queue() -> QueueKey {
    QueueKey::new(
        TenantId::new("embedded-tenant").unwrap(),
        QueueId::new("durable-queue").unwrap(),
    )
}

fn item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(format!("payload-{priority}").into()),
        ..NewItem::default()
    }
}

fn assert_authoritative_commit_capabilities(caps: &pqueue::CommitCapabilities) {
    assert!(caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.non_work_side_records);
    assert!(caps.authoritative_recovery_reads);
    assert!(caps.delayed_awaits_timers);
    assert_eq!(caps.durability_class, DurabilityClass::Atomic);
}

fn object_count(root: &Path) -> usize {
    fn visit(path: &Path, count: &mut usize) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, count);
            } else {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    visit(root, &mut count);
    count
}

fn postgres_in_schema(url: &str, schema: &str) -> Client {
    let mut client = Client::connect(url, NoTls).unwrap();
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .unwrap();
    client
}

fn drop_schema(url: &str, schema: &str) {
    let mut client = Client::connect(url, NoTls).unwrap();
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .unwrap();
}

#[test]
fn public_objectlog_postgres_delete_and_rehydrate() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "SKIP public_objectlog_postgres_delete_and_rehydrate: PQUEUE_PG_TEST_URL is unset"
        );
        return;
    };
    let (root, schema) = unique_fixture("public_objectlog_postgres");
    let durability = config(&root, &schema, &url);
    let clock = Arc::new(ManualClock::at(1_000));
    let pq = pqueue::open_embedded(durability.clone(), clock.clone()).unwrap();
    let key = queue();

    block_on(pq.create_queue(definition())).unwrap();
    let query_caps = pq.hot_projection_capabilities(&key);
    assert_eq!(query_caps, Default::default());
    assert!(query_caps.paired_capabilities_consistent());
    assert!(matches!(
        block_on(pq.range_scan(
            &key,
            RangeScanRequest {
                index: None,
                filters: vec![],
                order_by: vec![],
                page_size: 1,
                cursor: None,
            }
        )),
        Err(EngineError::Unavailable)
    ));
    let first_request = RequestId::new("embedded-request-1").unwrap();
    let first = block_on(pq.push_with_request_id(&key, first_request.clone(), item(10))).unwrap();
    let second = block_on(pq.push(&key, item(20))).unwrap();

    // Strict visibility: acknowledgement means the durable PostgreSQL image is queryable immediately.
    let expected = block_on(pq.metrics(&key)).unwrap();
    assert_eq!(expected.pending, 2);
    assert_eq!(block_on(pq.peek(&key, 10)).unwrap().len(), 2);
    let caps = pq.commit_capabilities(&key).unwrap();
    assert_authoritative_commit_capabilities(&caps);
    assert_eq!(
        block_on(pq.verify_projection())
            .unwrap()
            .projection_sequence,
        block_on(pq.verify_projection())
            .unwrap()
            .authoritative_sequence
    );

    let claimed = block_on(pq.claim(&key, 1, 30_000)).unwrap();
    let claim = &claimed[0];
    block_on(pq.ack(&key, [claim.item_id])).unwrap();
    let lifecycle_id = block_on(pq.push(&key, item(30))).unwrap();
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

    // A deliberately behind image replays only the one retained command after its durable cursor.
    let mut postgres = postgres_in_schema(&url, &schema);
    let last_sequence: i64 = postgres
        .query_one(
            "SELECT MAX(last_command_sequence) FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2",
            &[&"embedded-tenant", &"durable-queue"],
        )
        .unwrap()
        .get::<_, Option<i64>>(0)
        .unwrap();
    postgres
        .execute(
            "DELETE FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND last_command_sequence=$3",
            &[&"embedded-tenant", &"durable-queue", &last_sequence],
        )
        .unwrap();
    postgres
        .execute(
            "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
            &[&"embedded-tenant", &"durable-queue", &last_sequence],
        )
        .unwrap();
    let tail = block_on(pq.rehydrate_projection()).unwrap();
    assert_eq!(tail.tail_commands_replayed, 1);
    assert_eq!(block_on(pq.metrics(&key)).unwrap(), expected);

    // An ahead image fails closed, then restoring its cursor makes it valid again.
    let authoritative_next = block_on(pq.verify_projection())
        .unwrap()
        .authoritative_sequence
        + 1;
    postgres
        .execute(
            "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
            &[
                &"embedded-tenant",
                &"durable-queue",
                &((authoritative_next + 2) as i64),
            ],
        )
        .unwrap();
    assert!(block_on(pq.verify_projection()).is_err());
    postgres
        .execute(
            "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
            &[
                &"embedded-tenant",
                &"durable-queue",
                &(authoritative_next as i64),
            ],
        )
        .unwrap();
    drop(postgres);

    // Deleting the disposable image does not touch authoritative objects or durable request outcomes.
    let objects_before_delete = object_count(&root);
    block_on(pq.delete_projection()).unwrap();
    assert_eq!(object_count(&root), objects_before_delete);
    let rebuilt = block_on(pq.rehydrate_projection()).unwrap();
    assert!(
        rebuilt.tail_commands_replayed >= 2,
        "projection rebuild must replay the authoritative tail"
    );
    assert_eq!(block_on(pq.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(pq.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );

    let replayed =
        block_on(pq.push_with_request_id(&key, first_request.clone(), item(10))).unwrap();
    assert_eq!(replayed, first);
    assert_eq!(object_count(&root), objects_before_delete);

    // A fresh public facade reconstructs exact normalized state and the durable request-id outcome.
    drop(pq);
    let reopened = pqueue::open_embedded(durability, clock).unwrap();
    assert_eq!(block_on(reopened.metrics(&key)).unwrap(), expected);
    assert_eq!(
        block_on(reopened.peek(&key, 10))
            .unwrap()
            .iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![second, lifecycle_id]
    );
    assert_eq!(
        block_on(reopened.push_with_request_id(&key, first_request, item(10))).unwrap(),
        first
    );
    assert_eq!(object_count(&root), objects_before_delete);
    drop(reopened);

    drop_schema(&url, &schema);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn public_s3_objectlog_postgres_open_and_reopen_with_disposable_projection() {
    let endpoint = match std::env::var("PQUEUE_S3_TEST_URL")
        .or_else(|_| std::env::var("PQUEUE_S3_TEST_ENDPOINT"))
    {
        Ok(value) => value,
        Err(_) => {
            if std::env::var_os("CI").is_some() {
                panic!(
                    "CI must set PQUEUE_S3_TEST_URL or PQUEUE_S3_TEST_ENDPOINT; S3+Postgres coverage cannot be skipped"
                );
            }
            eprintln!(
                "SKIP public_s3_objectlog_postgres_open_and_reopen_with_disposable_projection: PQUEUE_S3_TEST_URL and PQUEUE_S3_TEST_ENDPOINT are unset"
            );
            return;
        }
    };
    let bucket = std::env::var("PQUEUE_S3_TEST_BUCKET").unwrap_or_else(|_| unique_bucket("pg"));
    let access = std::env::var("PQUEUE_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = std::env::var("PQUEUE_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let region = std::env::var("PQUEUE_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let pg_url = std::env::var("PQUEUE_PG_TEST_URL")
        .expect("PQUEUE_PG_TEST_URL must be set when exercising the postgres projection");
    let allow_insecure_http = endpoint.starts_with("http://");

    S3BlobStore::new(&endpoint, &bucket, &access, &secret, &region)
        .unwrap()
        .create_bucket()
        .unwrap();

    let namespace = format!(
        "snorri-s3-v1:prefix_len:32:object-log/{}:{}:{}",
        "illegal-namespace".repeat(3),
        "with punctuation:-/",
        "with unicode snowman ☃ and more text to exceed sixty-three bytes"
    );
    let durability = EmbeddedDurabilityConfig {
        object_log: EmbeddedObjectLogConfig::S3Compatible {
            endpoint,
            bucket,
            region,
            access_key_id: EmbeddedSecret::new(access),
            secret_access_key: EmbeddedSecret::new(secret),
            allow_insecure_http,
        },
        projection: EmbeddedProjectionConfig::Postgres {
            url: EmbeddedSecret::new(pg_url),
        },
        response_barrier: EmbeddedResponseBarrier::Strict,
        segments: EmbeddedSegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace,
        recovery: EmbeddedRecoveryPolicy::default(),
    };
    let clock = Arc::new(ManualClock::at(1_000));

    let postgres_caps = {
        let pq = pqueue::open_embedded(durability.clone(), clock.clone()).unwrap();
        let key = queue();
        block_on(pq.create_queue(definition())).unwrap();
        block_on(pq.push(&key, item(10))).unwrap();
        block_on(pq.push(&key, item(20))).unwrap();
        assert_eq!(block_on(pq.metrics(&key)).unwrap().pending, 2);
        let caps = pq.commit_capabilities(&key).unwrap();
        assert_authoritative_commit_capabilities(&caps);
        caps
    };

    {
        let pq = pqueue::open_embedded(durability.clone(), clock.clone()).unwrap();
        let key = queue();
        assert_eq!(block_on(pq.metrics(&key)).unwrap().pending, 2);
        assert_eq!(block_on(pq.peek(&key, 10)).unwrap().len(), 2);
    }

    #[cfg(feature = "sqlite")]
    {
        let (_, sqlite_projection) = unique_fixture("s3_sqlite_capability_parity");
        let mut sqlite_durability = durability;
        sqlite_durability.projection = EmbeddedProjectionConfig::Sqlite {
            path: std::env::temp_dir().join(format!("{sqlite_projection}.sqlite")),
        };
        let pq = pqueue::open_embedded_sqlite(sqlite_durability, clock).unwrap();
        let sqlite_caps = pq.commit_capabilities(&queue()).unwrap();
        assert_eq!(postgres_caps, sqlite_caps);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_open_inside_tokio_returns_typed_error() {
    let (root, schema) = unique_fixture("tokio_sync_open");
    let error = match pqueue::open_embedded(
        config(&root, &schema, "postgres://127.0.0.1:1/postgres"),
        Arc::new(ManualClock::at(1_000)),
    ) {
        Ok(_) => panic!("the synchronous constructor must reject an ambient Tokio runtime"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        EngineError::Invalid(
            "open_embedded cannot run inside a Tokio runtime; use open_embedded_async"
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_open_is_safe_inside_tokio() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("SKIP asynchronous_open_is_safe_inside_tokio: PQUEUE_PG_TEST_URL is unset");
        return;
    };
    let (root, schema) = unique_fixture("tokio_async_open");
    let pq = pqueue::open_embedded_async(
        config(&root, &schema, &url),
        Arc::new(ManualClock::at(1_000)),
    )
    .await
    .unwrap();
    drop(pq);
    drop_schema(&url, &schema);
    let _ = fs::remove_dir_all(root);
}

/// Full recovery proof for the authoritative rich-transition protocol. It remains ignored by default because
/// it requires a live PostgreSQL instance; CI exercises the shorter S3/Postgres capability and reopen proof.
#[test]
#[ignore = "US-009 recovery proof requires PQUEUE_PG_TEST_URL"]
fn us009_objectlog_postgres_rich_commit_recovery_promotion() {
    let url = std::env::var("PQUEUE_PG_TEST_URL")
        .expect("US-009 promotion proof requires PQUEUE_PG_TEST_URL");
    let (root, schema) = unique_fixture("us009_objectlog_postgres_commit");
    let durability = config(&root, &schema, &url);
    let clock = Arc::new(ManualClock::at(1_000));
    let pq = pqueue::open_embedded(durability.clone(), clock.clone()).unwrap();
    let key = queue();
    block_on(pq.create_queue(definition())).unwrap();
    assert_authoritative_commit_capabilities(&pq.commit_capabilities(&key).unwrap());

    block_on(pq.push(&key, item(10))).unwrap();
    let claim = block_on(pq.claim(&key, 1, 30_000)).unwrap().remove(0);
    let claim_ref = ClaimRef {
        item_id: claim.item_id,
        lease_token: claim.lease_token.clone().expect("claim token"),
        lease_expires_at: claim.lease_expires_at,
        item_version: claim.item_version,
    };
    let request_id = RequestId::new("us009-objectlog-postgres-transition").unwrap();
    let transition = || CommitRequest {
        request_id: Some(request_id.clone()),
        entries: vec![CommitEntry {
            claim_ref: claim_ref.clone(),
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
    let outcomes = block_on(pq.commit(&key, transition())).unwrap();
    let lifecycle_id = match outcomes.as_slice() {
        [EntryOutcome::Committed { lifecycle_item_ids }] => lifecycle_item_ids[0],
        other => panic!("expected committed rich transition, got {other:?}"),
    };
    assert_eq!(
        block_on(pq.side_record(&key, b"state/run-1"))
            .unwrap()
            .as_deref(),
        Some(b"audit-bytes".as_slice())
    );

    block_on(pq.delete_projection()).unwrap();
    block_on(pq.rehydrate_projection()).unwrap();
    assert_eq!(block_on(pq.commit(&key, transition())).unwrap(), outcomes);
    let recovery = block_on(pq.explain_commit(&key, request_id))
        .unwrap()
        .expect("rich transition survives projection rebuild");
    assert_eq!(recovery.entries[0].consumed_input_id, claim.item_id);
    assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(
        recovery.entries[0].side_record_keys,
        vec![b"state/run-1".to_vec()]
    );

    drop(pq);
    drop_schema(&url, &schema);
    let _ = fs::remove_dir_all(root);
}
