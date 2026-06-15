// Integration tests: ControlPlaneStore transaction flows + BatchPush/BatchUpdate.
// Verifies: tenant-scoped create/read, static single-shard assignment/epoch,
// INV-8 (cross-tenant isolation), BatchPush/BatchUpdate write paths, command
// records, required partial indexes, and AC-CORE-3 duplicate-key convergence.
//
// In OrbStack Linux, port forwarding doesn't expose mapped ports on 127.0.0.1,
// so we connect directly to the container's bridge IP on port 5432.

use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityModel, PriorityValue,
    QueueCreationPolicy, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        AppendError, PgBatchPushRequest, PgBatchUpdateRequest, PgPushItem, PgPushOutcome,
        PgUpdateItem, PgUpdateOutcome,
    },
};
use pqueue_storage::{
    traits::{ControlPlaneError, ControlPlaneStore},
    types::QueueKey,
};

async fn start_pg() -> (Arc<Mutex<tokio_postgres::Client>>, impl std::fmt::Debug) {
    let pg = Postgres::default().start().await.unwrap();

    let container_ip = {
        let id = pg.id();
        let out = std::process::Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                id,
            ])
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };

    let url =
        format!("host={container_ip} port=5432 user=postgres password=postgres dbname=postgres");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    (Arc::new(Mutex::new(client)), pg)
}

async fn control_store(
    client_arc: Arc<Mutex<tokio_postgres::Client>>,
) -> PostgresControlPlaneStore {
    PostgresControlPlaneStore::new(client_arc).await.unwrap()
}

async fn append_store(client_arc: Arc<Mutex<tokio_postgres::Client>>) -> PostgresAppendStore {
    PostgresAppendStore::new(client_arc).await.unwrap()
}

fn tid(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn simple_def(tenant: TenantId, queue: QueueId) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tenant,
        queue_id: queue,
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: None,
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn now_ts() -> UtcTimestamp {
    UtcTimestamp::new(1_718_000_000, 0).unwrap()
}

// ---------------------------------------------------------------------------
// Create and read (ControlPlaneStore)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_queue_and_read_back() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let def = simple_def(tid("tenant-a"), qid("orders"));
    let result = s.create_queue(def.clone()).await.unwrap();

    assert!(result.created);
    assert_eq!(result.definition.queue_id, qid("orders"));
    assert_eq!(result.definition.tenant_id, tid("tenant-a"));

    let key = QueueKey {
        tenant_id: tid("tenant-a"),
        queue_id: qid("orders"),
    };
    let fetched = s.queue_definition(&key).await.unwrap();
    assert_eq!(fetched.queue_id, qid("orders"));
    assert_eq!(fetched.tenant_id, tid("tenant-a"));
    assert_eq!(fetched.progress_bound_ms, 30_000);
    assert_eq!(fetched.shard_count, 1);
    assert_eq!(fetched.retry_policy.max_attempts, 3);
}

#[tokio::test]
async fn create_queue_roundtrips_priority_model() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let def = simple_def(tid("t"), qid("q-pm"));
    let pm = def.priority_model;
    s.create_queue(def).await.unwrap();

    let key = QueueKey {
        tenant_id: tid("t"),
        queue_id: qid("q-pm"),
    };
    let fetched = s.queue_definition(&key).await.unwrap();
    assert_eq!(fetched.priority_model, pm);
}

#[tokio::test]
async fn duplicate_create_returns_queue_already_exists() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let def = simple_def(tid("t"), qid("dup"));
    s.create_queue(def.clone()).await.unwrap();
    let err = s.create_queue(def).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueAlreadyExists);
}

#[tokio::test]
async fn read_missing_queue_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let key = QueueKey {
        tenant_id: tid("t"),
        queue_id: qid("ghost"),
    };
    let err = s.queue_definition(&key).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueNotFound);
}

// ---------------------------------------------------------------------------
// Shard assignment and epoch (AC: static single-shard assignment/epoch)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shard_assignments_single_shard_epoch_one() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let def = simple_def(tid("t"), qid("sharded"));
    s.create_queue(def).await.unwrap();

    let key = QueueKey {
        tenant_id: tid("t"),
        queue_id: qid("sharded"),
    };
    let shards = s.shard_assignments(&key).await.unwrap();

    assert_eq!(
        shards.len(),
        1,
        "single-shard queue must have exactly one assignment"
    );
    assert_eq!(shards[0].epoch, 1, "initial epoch must be 1");
    assert!(shards[0].worker_id.is_none(), "initial shard has no owner");
    assert_eq!(shards[0].shard_key.shard_id.as_u32(), 0);
}

#[tokio::test]
async fn shard_assignments_missing_queue_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let key = QueueKey {
        tenant_id: tid("t"),
        queue_id: qid("ghost"),
    };
    let err = s.shard_assignments(&key).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueNotFound);
}

// ---------------------------------------------------------------------------
// List queues
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_queues_returns_own_queues() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    s.create_queue(simple_def(tid("t"), qid("q1")))
        .await
        .unwrap();
    s.create_queue(simple_def(tid("t"), qid("q2")))
        .await
        .unwrap();

    let mut listed = s.list_queues(&tid("t")).await.unwrap();
    listed.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let names: Vec<&str> = listed.iter().map(|q| q.as_str()).collect();
    assert_eq!(names, vec!["q1", "q2"]);
}

#[tokio::test]
async fn list_queues_empty_tenant_returns_empty() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    let listed = s.list_queues(&tid("nobody")).await.unwrap();
    assert!(listed.is_empty());
}

// ---------------------------------------------------------------------------
// INV-8: cross-tenant isolation
// Tenant B must not see queues belonging to Tenant A.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inv8_cross_tenant_read_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    s.create_queue(simple_def(tid("tenant-a"), qid("secret-queue")))
        .await
        .unwrap();

    let key = QueueKey {
        tenant_id: tid("tenant-b"),
        queue_id: qid("secret-queue"),
    };
    let err = s.queue_definition(&key).await.unwrap_err();
    assert_eq!(
        err,
        ControlPlaneError::QueueNotFound,
        "INV-8: cross-tenant queue_definition must return QueueNotFound"
    );
}

#[tokio::test]
async fn inv8_cross_tenant_shard_read_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    s.create_queue(simple_def(tid("tenant-a"), qid("q")))
        .await
        .unwrap();

    let key = QueueKey {
        tenant_id: tid("tenant-b"),
        queue_id: qid("q"),
    };
    let err = s.shard_assignments(&key).await.unwrap_err();
    assert_eq!(
        err,
        ControlPlaneError::QueueNotFound,
        "INV-8: cross-tenant shard_assignments must return QueueNotFound"
    );
}

#[tokio::test]
async fn inv8_list_queues_does_not_leak_across_tenants() {
    let (c, _pg) = start_pg().await;
    let s = control_store(c).await;

    s.create_queue(simple_def(tid("tenant-a"), qid("a-queue")))
        .await
        .unwrap();
    s.create_queue(simple_def(tid("tenant-b"), qid("b-queue")))
        .await
        .unwrap();

    let a_list = s.list_queues(&tid("tenant-a")).await.unwrap();
    let b_list = s.list_queues(&tid("tenant-b")).await.unwrap();

    assert_eq!(a_list.len(), 1);
    assert_eq!(a_list[0], qid("a-queue"));
    assert_eq!(b_list.len(), 1);
    assert_eq!(
        b_list[0],
        qid("b-queue"),
        "INV-8: list_queues must not leak across tenants"
    );
}

// ---------------------------------------------------------------------------
// BatchPush transaction flow
// ---------------------------------------------------------------------------

fn make_push_request(
    tenant_id: &str,
    queue_id: &str,
    items: Vec<(&str, &str)>, // (item_id, client_item_key)
) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: tenant_id.to_string(),
        queue_id: queue_id.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-{}", uuid_str()),
        request_id: None,
        items: items
            .into_iter()
            .map(|(id, key)| PgPushItem {
                item_id: id.to_string(),
                client_item_key: key.to_string(),
                priority: None,
                not_before: None,
                group_key: None,
                cohort_size: None,
                recurrence_until: None,
                gate_keys: vec![],
                payload: None,
            })
            .collect(),
        now: now_ts(),
    }
}

fn uuid_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}-{}", t.as_secs(), t.subsec_nanos())
}

/// Verify that BatchPush inserts items into pqueue_items as 'pending' and
/// returns New outcomes with item_version=1.
#[tokio::test]
async fn batch_push_inserts_pending_items() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-push")))
        .await
        .unwrap();

    let req = make_push_request(
        "t",
        "q-push",
        vec![("item-001", "key-001"), ("item-002", "key-002")],
    );
    let result = ap.batch_push(req).await.unwrap();

    assert_eq!(result.items.len(), 2);
    assert!(matches!(
        result.items[0].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));
    assert!(matches!(
        result.items[1].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));

    // Verify rows in pqueue_items
    let client = c.lock().await;
    let rows = client
        .query(
            "SELECT item_id, lifecycle_state, item_version
             FROM pqueue_items
             WHERE tenant_id = 't' AND queue_id = 'q-push'
             ORDER BY item_id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let state: String = row.get("lifecycle_state");
        let version: i64 = row.get("item_version");
        assert_eq!(state, "pending");
        assert_eq!(version, 1);
    }
}

/// AC-CORE-3: pushing the same client_item_key a second time returns Duplicate
/// without mutating the existing item (item_version unchanged).
#[tokio::test]
async fn batch_push_duplicate_client_item_key_is_noop() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-dup")))
        .await
        .unwrap();

    // First push: new item
    let req1 = make_push_request("t", "q-dup", vec![("item-a", "unique-key")]);
    let r1 = ap.batch_push(req1).await.unwrap();
    assert!(matches!(
        r1.items[0].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));
    let cmd_seq_1 = r1.command_sequence;

    // Second push with same client_item_key: must return Duplicate, no mutation
    let req2 = PgBatchPushRequest {
        tenant_id: "t".to_string(),
        queue_id: "q-dup".to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-dup-{}", uuid_str()),
        request_id: None,
        items: vec![PgPushItem {
            item_id: "item-a-v2".to_string(), // different item_id, same key
            client_item_key: "unique-key".to_string(),
            priority: None,
            not_before: None,
            group_key: None,
            cohort_size: None,
            recurrence_until: None,
            gate_keys: vec![],
            payload: None,
        }],
        now: now_ts(),
    };
    let r2 = ap.batch_push(req2).await.unwrap();
    let cmd_seq_2 = r2.command_sequence;

    assert_eq!(r2.items.len(), 1);
    match &r2.items[0].outcome {
        PgPushOutcome::Duplicate { existing_item_id } => {
            assert_eq!(
                existing_item_id, "item-a",
                "duplicate must point at original item_id"
            );
        }
        PgPushOutcome::New { .. } => panic!("expected Duplicate, got New"),
    }

    // Verify sequences advanced despite duplicate (command record written for each call)
    assert_eq!(
        cmd_seq_2,
        cmd_seq_1 + 1,
        "command sequence must advance even for duplicate-only batches"
    );

    // Verify item_version unchanged at 1 (no mutation)
    let client = c.lock().await;
    let row = client
        .query_one(
            "SELECT item_id, item_version FROM pqueue_items WHERE tenant_id = 't' AND queue_id = 'q-dup' AND client_item_key = 'unique-key'",
            &[],
        )
        .await
        .unwrap();
    let version: i64 = row.get("item_version");
    assert_eq!(
        version, 1,
        "AC-CORE-3: duplicate push must not increment item_version"
    );

    // Verify only ONE row in pqueue_items (duplicate did not insert a second row)
    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items WHERE tenant_id = 't' AND queue_id = 'q-dup'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(
        count, 1,
        "AC-CORE-3: duplicate push must not create a second item row"
    );
}

/// Epoch fencing: pushing with a wrong assignment_epoch must be rejected before
/// any state is mutated.
#[tokio::test]
async fn batch_push_rejects_stale_epoch() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-epoch")))
        .await
        .unwrap();

    let req = PgBatchPushRequest {
        tenant_id: "t".to_string(),
        queue_id: "q-epoch".to_string(),
        shard_id: 0,
        expected_epoch: 99, // wrong: actual epoch is 1
        command_id: format!("cmd-{}", uuid_str()),
        request_id: None,
        items: vec![PgPushItem {
            item_id: "item-epoch".to_string(),
            client_item_key: "key-epoch".to_string(),
            priority: None,
            not_before: None,
            group_key: None,
            cohort_size: None,
            recurrence_until: None,
            gate_keys: vec![],
            payload: None,
        }],
        now: now_ts(),
    };

    let err = ap.batch_push(req).await.unwrap_err();
    assert!(
        matches!(
            err,
            AppendError::EpochMismatch {
                expected: 99,
                current: 1
            }
        ),
        "stale epoch must produce EpochMismatch; got {:?}",
        err
    );

    // Verify no items were inserted
    let client = c.lock().await;
    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items WHERE tenant_id = 't' AND queue_id = 'q-epoch'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(
        count, 0,
        "epoch rejection must leave pqueue_items untouched"
    );
}

/// Command records: BatchPush must write exactly one row to pqueue_commands per call.
/// The row must have command_type='batch_push' and the correct shard sequence.
#[tokio::test]
async fn batch_push_writes_command_record() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-cmd")))
        .await
        .unwrap();

    let req = make_push_request("t", "q-cmd", vec![("item-1", "key-1")]);
    let result = ap.batch_push(req).await.unwrap();

    let client = c.lock().await;
    let row = client
        .query_one(
            "SELECT command_type, sequence, assignment_epoch, item_ids
             FROM pqueue_commands
             WHERE tenant_id = 't' AND queue_id = 'q-cmd' AND shard_id = 0",
            &[],
        )
        .await
        .unwrap();

    let cmd_type: String = row.get("command_type");
    let sequence: i64 = row.get("sequence");
    let epoch: i64 = row.get("assignment_epoch");
    let item_ids: Vec<String> = row.get("item_ids");

    assert_eq!(cmd_type, "batch_push");
    assert_eq!(sequence as u64, result.command_sequence);
    assert_eq!(epoch, 1);
    assert_eq!(item_ids, vec!["item-1"]);
}

/// Required partial indexes: all TD-002 indexes must be present in pg_indexes.
#[tokio::test]
async fn required_partial_indexes_exist() {
    let (c, _pg) = start_pg().await;
    // Initialise schema via append store
    let _ap = append_store(c.clone()).await;

    let client = c.lock().await;
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes
             WHERE tablename IN ('pqueue_items', 'pqueue_commands')
               AND indexname IN (
                 'pqueue_items_claim_strict_idx',
                 'pqueue_items_eligible_age_idx',
                 'pqueue_items_lease_expiry_idx',
                 'pqueue_items_group_claim_idx',
                 'pqueue_commands_replay_idx'
               )",
            &[],
        )
        .await
        .unwrap();

    let found: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        found.contains(&"pqueue_items_claim_strict_idx".to_string()),
        "missing pqueue_items_claim_strict_idx"
    );
    assert!(
        found.contains(&"pqueue_items_eligible_age_idx".to_string()),
        "missing pqueue_items_eligible_age_idx"
    );
    assert!(
        found.contains(&"pqueue_items_lease_expiry_idx".to_string()),
        "missing pqueue_items_lease_expiry_idx"
    );
    assert!(
        found.contains(&"pqueue_items_group_claim_idx".to_string()),
        "missing pqueue_items_group_claim_idx"
    );
    assert!(
        found.contains(&"pqueue_commands_replay_idx".to_string()),
        "missing pqueue_commands_replay_idx"
    );
}

/// Two sequential BatchPush calls advance the command sequence monotonically.
/// Evidence that pqueue_commands replay position tracks correctly.
#[tokio::test]
async fn sequential_pushes_advance_command_sequence() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-seq")))
        .await
        .unwrap();

    let r1 = ap
        .batch_push(make_push_request("t", "q-seq", vec![("i1", "k1")]))
        .await
        .unwrap();
    let r2 = ap
        .batch_push(make_push_request("t", "q-seq", vec![("i2", "k2")]))
        .await
        .unwrap();

    assert_eq!(r1.command_sequence, 0, "first push must use sequence 0");
    assert_eq!(r2.command_sequence, 1, "second push must use sequence 1");

    // Verify both command rows exist and are replay-ordered
    let client = c.lock().await;
    let rows = client
        .query(
            "SELECT sequence FROM pqueue_commands
             WHERE tenant_id = 't' AND queue_id = 'q-seq' AND shard_id = 0
             ORDER BY sequence",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let seqs: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(seqs, vec![0, 1]);
}

// ---------------------------------------------------------------------------
// BatchUpdate transaction flow
// ---------------------------------------------------------------------------

fn push_one_item(tenant: &str, queue: &str, item_id: &str, key: &str) -> PgBatchPushRequest {
    make_push_request(tenant, queue, vec![(item_id, key)])
}

/// BatchUpdate updates the priority and bumps item_version for a pending item.
#[tokio::test]
async fn batch_update_updates_pending_item() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-upd")))
        .await
        .unwrap();
    ap.batch_push(push_one_item("t", "q-upd", "item-u1", "key-u1"))
        .await
        .unwrap();

    let upd_req = PgBatchUpdateRequest {
        tenant_id: "t".to_string(),
        queue_id: "q-upd".to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-upd-{}", uuid_str()),
        request_id: None,
        items: vec![PgUpdateItem {
            item_id: "item-u1".to_string(),
            expected_item_version: Some(1),
            priority: Some(PriorityValue::Timestamp(
                UtcTimestamp::new(1_718_000_100, 0).unwrap(),
            )),
            not_before: None,
        }],
        now: UtcTimestamp::new(1_718_000_050, 0).unwrap(),
    };

    let result = ap.batch_update(upd_req).await.unwrap();
    assert_eq!(result.items.len(), 1);
    assert!(
        matches!(
            result.items[0].outcome,
            PgUpdateOutcome::Updated { item_version: 2 }
        ),
        "update of pending item must return Updated with item_version=2"
    );

    // Verify item_version incremented in DB
    let client = c.lock().await;
    let row = client
        .query_one(
            "SELECT item_version FROM pqueue_items WHERE tenant_id = 't' AND queue_id = 'q-upd' AND item_id = 'item-u1'",
            &[],
        )
        .await
        .unwrap();
    let version: i64 = row.get("item_version");
    assert_eq!(
        version, 2,
        "item_version must be incremented after BatchUpdate"
    );
}

/// BatchUpdate with wrong expected_item_version returns Conflict per item.
#[tokio::test]
async fn batch_update_version_conflict() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-ver")))
        .await
        .unwrap();
    ap.batch_push(push_one_item("t", "q-ver", "item-v1", "key-v1"))
        .await
        .unwrap();

    let upd_req = PgBatchUpdateRequest {
        tenant_id: "t".to_string(),
        queue_id: "q-ver".to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-{}", uuid_str()),
        request_id: None,
        items: vec![PgUpdateItem {
            item_id: "item-v1".to_string(),
            expected_item_version: Some(99), // wrong: actual is 1
            priority: None,
            not_before: None,
        }],
        now: now_ts(),
    };

    let result = ap.batch_update(upd_req).await.unwrap();
    assert_eq!(result.items.len(), 1);
    assert!(
        matches!(result.items[0].outcome, PgUpdateOutcome::Conflict { .. }),
        "wrong expected_item_version must yield Conflict"
    );

    // item_version must not have changed
    let client = c.lock().await;
    let row = client
        .query_one(
            "SELECT item_version FROM pqueue_items WHERE tenant_id = 't' AND queue_id = 'q-ver' AND item_id = 'item-v1'",
            &[],
        )
        .await
        .unwrap();
    let version: i64 = row.get("item_version");
    assert_eq!(version, 1, "Conflict must leave item_version unchanged");
}

/// BatchUpdate for an unknown item_id returns NotFound per item.
#[tokio::test]
async fn batch_update_not_found_for_missing_item() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-nf")))
        .await
        .unwrap();

    let upd_req = PgBatchUpdateRequest {
        tenant_id: "t".to_string(),
        queue_id: "q-nf".to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-{}", uuid_str()),
        request_id: None,
        items: vec![PgUpdateItem {
            item_id: "nonexistent-item".to_string(),
            expected_item_version: None,
            priority: None,
            not_before: None,
        }],
        now: now_ts(),
    };

    let result = ap.batch_update(upd_req).await.unwrap();
    assert_eq!(result.items.len(), 1);
    assert!(
        matches!(result.items[0].outcome, PgUpdateOutcome::NotFound),
        "update of missing item must return NotFound"
    );
}

/// BatchUpdate also writes a command record (command_type = 'batch_update').
#[tokio::test]
async fn batch_update_writes_command_record() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-ucmd")))
        .await
        .unwrap();
    ap.batch_push(push_one_item("t", "q-ucmd", "item-uc", "key-uc"))
        .await
        .unwrap();

    let upd_req = PgBatchUpdateRequest {
        tenant_id: "t".to_string(),
        queue_id: "q-ucmd".to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-upd-{}", uuid_str()),
        request_id: None,
        items: vec![PgUpdateItem {
            item_id: "item-uc".to_string(),
            expected_item_version: None,
            priority: None,
            not_before: None,
        }],
        now: now_ts(),
    };
    ap.batch_update(upd_req).await.unwrap();

    let client = c.lock().await;
    let row = client
        .query_one(
            "SELECT command_type FROM pqueue_commands
             WHERE tenant_id = 't' AND queue_id = 'q-ucmd' AND command_type = 'batch_update'",
            &[],
        )
        .await
        .unwrap();
    let cmd_type: String = row.get("command_type");
    assert_eq!(cmd_type, "batch_update");
}

/// AC-CORE-3 evidence: key-retention records are written for new items and
/// persist after the push transaction commits.
#[tokio::test]
async fn batch_push_writes_key_retention_records() {
    let (c, _pg) = start_pg().await;
    let cs = control_store(c.clone()).await;
    let ap = append_store(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-ret")))
        .await
        .unwrap();
    ap.batch_push(make_push_request(
        "t",
        "q-ret",
        vec![("i-r1", "key-r1"), ("i-r2", "key-r2")],
    ))
    .await
    .unwrap();

    let client = c.lock().await;
    let rows = client
        .query(
            "SELECT client_item_key, expires_at
             FROM pqueue_item_key_retention
             WHERE tenant_id = 't' AND queue_id = 'q-ret'
             ORDER BY client_item_key",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2, "one retention record per new item");
    let keys: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(keys, vec!["key-r1", "key-r2"]);
    // expires_at must be in the future relative to push time
    let expires: time::OffsetDateTime = rows[0].get("expires_at");
    let push_odt = time::OffsetDateTime::from_unix_timestamp(1_718_000_000).unwrap();
    assert!(expires > push_odt, "expires_at must be after push time");
}
