#![cfg(all(feature = "objectlog", feature = "postgres"))]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::executor::block_on;
use postgres::{Client, NoTls};
use pqueue::{
    EligibilityPolicy, EmbeddedDurabilityConfig, EmbeddedObjectLogConfig, EmbeddedProjectionConfig,
    EmbeddedRecoveryPolicy, EmbeddedResponseBarrier, EmbeddedSecret, EmbeddedSegmentConfig,
    EngineError, NewItem, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, QueueKey, RangeScanRequest,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId,
};
use pqueue_engine::DurabilityClass;
use pqueue_memory::ManualClock;

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
    assert_eq!(
        caps,
        Default::default(),
        "an object log and an independent Postgres projection cannot honestly advertise one atomic append+apply boundary"
    );
    assert_eq!(caps.durability_class, DurabilityClass::EventualApply);
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
