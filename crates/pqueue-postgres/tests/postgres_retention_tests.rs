// B-044: Postgres retention and compaction groundwork — INV-5.
//
// INV-5 (TP-003): "Replay of any mutating request_id (or async operation_id)
// yields byte-identical committed state and an equivalent response; divergences = 0."
//
// GC functions must not delete any record whose replay or audit window has not
// yet expired. Tests here verify:
//
//   1. request_id retention — expired records GC'd; active records preserved.
//   2. item_key retention  — expired records GC'd; active records preserved.
//   3. terminal retention  — expired terminal items GC'd; recent ones preserved.
//   4. No-delete-before-window — expire_terminal_items never touches
//      pqueue_commands; command-log rows outlive terminal item rows (INV-5).
//
// Requires Docker (testcontainers). Connects via container bridge IP (OrbStack).

use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityModel,
    QueueCreationPolicy, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{PgBatchPushRequest, PgPushItem},
    retention,
};
use pqueue_storage::traits::ControlPlaneStore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

async fn setup(c: Arc<Mutex<tokio_postgres::Client>>) -> PostgresAppendStore {
    PostgresAppendStore::new(c).await.unwrap()
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

fn push_req(tenant: &str, queue: &str, item_id: &str, key: &str) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: tenant.to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cmd-{}-{}", item_id, key),
        request_id: None,
        items: vec![PgPushItem {
            item_id: item_id.to_string(),
            client_item_key: key.to_string(),
            priority: None,
            not_before: None,
            group_key: None,
            payload: None,
        }],
        now: UtcTimestamp::new(1_718_000_000, 0).unwrap(),
    }
}

// OffsetDateTime helpers
fn past(secs_ago: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_718_000_000 - secs_ago).unwrap()
}

fn future(secs_from_now: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_718_000_000 + secs_from_now).unwrap()
}

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_718_000_000).unwrap()
}

// ---------------------------------------------------------------------------
// 1. request_id retention
// ---------------------------------------------------------------------------

/// Expired request_id records (expires_at in the past) must be deleted by GC.
#[tokio::test]
async fn postgres_retention_tests_request_id_expired_record_deleted() {
    let (c, _pg) = start_pg().await;
    let _ap = setup(c.clone()).await; // runs DDL

    // Insert an already-expired idempotency record directly.
    {
        let client = c.lock().await;
        client
            .execute(
                "INSERT INTO pqueue_request_idempotency
                 (tenant_id, queue_id, operation, request_id,
                  request_fingerprint, command_positions, expires_at, created_at)
                 VALUES ('t', 'q', 'batch_push', 'req-expired',
                         '\\xdeadbeef', '{}'::jsonb, $1, $2)",
                &[&past(3600), &past(7200)],
            )
            .await
            .unwrap();
    }

    let client = c.lock().await;
    let deleted = retention::expire_request_idempotency(&client, now()).await.unwrap();
    assert_eq!(deleted, 1, "one expired request_id record must be deleted");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_request_idempotency
             WHERE tenant_id = 't' AND request_id = 'req-expired'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(count, 0, "expired request_id record must be gone after GC");
}

/// Active request_id records (expires_at in the future) must NOT be deleted.
/// INV-5: replay window is still open; GC must not delete the record.
#[tokio::test]
async fn postgres_retention_tests_request_id_active_record_preserved() {
    let (c, _pg) = start_pg().await;
    let _ap = setup(c.clone()).await;

    {
        let client = c.lock().await;
        client
            .execute(
                "INSERT INTO pqueue_request_idempotency
                 (tenant_id, queue_id, operation, request_id,
                  request_fingerprint, command_positions, expires_at, created_at)
                 VALUES ('t', 'q', 'batch_push', 'req-active',
                         '\\xdeadbeef', '{}'::jsonb, $1, $2)",
                &[&future(3600), &past(10)],
            )
            .await
            .unwrap();
    }

    let client = c.lock().await;
    let deleted = retention::expire_request_idempotency(&client, now()).await.unwrap();
    assert_eq!(deleted, 0, "no active request_id records must be deleted");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_request_idempotency
             WHERE tenant_id = 't' AND request_id = 'req-active'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(count, 1, "active request_id record must survive GC (INV-5: replay window open)");
}

// ---------------------------------------------------------------------------
// 2. item_key retention
// ---------------------------------------------------------------------------

/// Expired item-key convergence records must be deleted by GC.
#[tokio::test]
async fn postgres_retention_tests_item_key_expired_record_deleted() {
    let (c, _pg) = start_pg().await;
    let cs = PostgresControlPlaneStore::new(c.clone()).await.unwrap();
    let ap = setup(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-ikr-exp"))).await.unwrap();
    ap.batch_push(push_req("t", "q-ikr-exp", "item-1", "key-ikr-1")).await.unwrap();

    // Back-date the retention record's expires_at to the past.
    {
        let client = c.lock().await;
        client
            .execute(
                "UPDATE pqueue_item_key_retention SET expires_at = $1
                 WHERE tenant_id = 't' AND queue_id = 'q-ikr-exp'",
                &[&past(10)],
            )
            .await
            .unwrap();
    }

    let client = c.lock().await;
    let deleted = retention::expire_item_key_retention(&client, now()).await.unwrap();
    assert_eq!(deleted, 1, "one expired item-key record must be deleted");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_item_key_retention
             WHERE tenant_id = 't' AND queue_id = 'q-ikr-exp'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(count, 0, "expired item-key retention record must be gone");
}

/// Active item-key convergence records (within retention window) must NOT be
/// deleted. INV-5: duplicate-push convergence must stay available for replay.
#[tokio::test]
async fn postgres_retention_tests_item_key_active_record_preserved() {
    let (c, _pg) = start_pg().await;
    let cs = PostgresControlPlaneStore::new(c.clone()).await.unwrap();
    let ap = setup(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-ikr-act"))).await.unwrap();
    ap.batch_push(push_req("t", "q-ikr-act", "item-2", "key-ikr-2")).await.unwrap();
    // expires_at is now + 86400s by default (client_item_key_retention_ms = 86_400_000)

    let client = c.lock().await;
    let deleted = retention::expire_item_key_retention(&client, now()).await.unwrap();
    assert_eq!(deleted, 0, "no active item-key record must be deleted");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_item_key_retention
             WHERE tenant_id = 't' AND queue_id = 'q-ikr-act'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(count, 1, "active item-key record must survive GC");
}

// ---------------------------------------------------------------------------
// 3. terminal item retention
// ---------------------------------------------------------------------------

/// Terminal items past their retention window must be deleted.
#[tokio::test]
async fn postgres_retention_tests_terminal_expired_item_deleted() {
    let (c, _pg) = start_pg().await;
    let cs = PostgresControlPlaneStore::new(c.clone()).await.unwrap();
    let ap = setup(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-term-exp"))).await.unwrap();
    ap.batch_push(push_req("t", "q-term-exp", "item-t1", "key-t1")).await.unwrap();

    // Mark item as terminal with terminal_at well in the past.
    {
        let client = c.lock().await;
        client
            .execute(
                "UPDATE pqueue_items
                 SET lifecycle_state = 'complete', terminal_at = $1, updated_at = $1
                 WHERE tenant_id = 't' AND queue_id = 'q-term-exp' AND item_id = 'item-t1'",
                &[&past(90_000)], // 25 hours ago — past any reasonable terminal_retention
            )
            .await
            .unwrap();
    }

    // GC with cutoff = 1 second ago (item_t1's terminal_at is 90000s ago, well past cutoff).
    let client = c.lock().await;
    let cutoff = past(1);
    let deleted = retention::expire_terminal_items(&client, "t", "q-term-exp", cutoff)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "one expired terminal item must be deleted");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items
             WHERE tenant_id = 't' AND queue_id = 'q-term-exp' AND item_id = 'item-t1'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(count, 0, "expired terminal item row must be gone");
}

/// Terminal items within their retention window must NOT be deleted.
#[tokio::test]
async fn postgres_retention_tests_terminal_recent_item_preserved() {
    let (c, _pg) = start_pg().await;
    let cs = PostgresControlPlaneStore::new(c.clone()).await.unwrap();
    let ap = setup(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-term-act"))).await.unwrap();
    ap.batch_push(push_req("t", "q-term-act", "item-t2", "key-t2")).await.unwrap();

    // Mark item as terminal just 5 seconds ago.
    {
        let client = c.lock().await;
        client
            .execute(
                "UPDATE pqueue_items
                 SET lifecycle_state = 'complete', terminal_at = $1, updated_at = $1
                 WHERE tenant_id = 't' AND queue_id = 'q-term-act' AND item_id = 'item-t2'",
                &[&past(5)],
            )
            .await
            .unwrap();
    }

    // GC with cutoff = 1 hour ago — item terminated only 5s ago, well within window.
    let client = c.lock().await;
    let cutoff = past(3600);
    let deleted = retention::expire_terminal_items(&client, "t", "q-term-act", cutoff)
        .await
        .unwrap();
    assert_eq!(deleted, 0, "recently terminal item must not be deleted (still within window)");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items
             WHERE tenant_id = 't' AND queue_id = 'q-term-act' AND item_id = 'item-t2'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert_eq!(count, 1, "recent terminal item must still exist");
}

/// Pending items must never be touched by terminal GC.
#[tokio::test]
async fn postgres_retention_tests_terminal_pending_items_untouched() {
    let (c, _pg) = start_pg().await;
    let cs = PostgresControlPlaneStore::new(c.clone()).await.unwrap();
    let ap = setup(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-pend"))).await.unwrap();
    ap.batch_push(push_req("t", "q-pend", "item-p1", "key-p1")).await.unwrap();

    // Try to GC with a very permissive cutoff — item is pending, not terminal.
    let client = c.lock().await;
    let deleted =
        retention::expire_terminal_items(&client, "t", "q-pend", future(9999)).await.unwrap();
    assert_eq!(deleted, 0, "pending items must never be deleted by terminal retention GC");
}

// ---------------------------------------------------------------------------
// 4. No-delete-before-window: command log survives terminal item GC (INV-5)
// ---------------------------------------------------------------------------

/// expire_terminal_items must not touch pqueue_commands.
///
/// After a terminal item is GC'd, the command log row that recorded the
/// original push must remain — replay and audit require it. This is the core
/// INV-5 groundwork invariant: command-log rows outlive the terminal item rows
/// they correspond to.
#[tokio::test]
async fn postgres_retention_tests_inv5_terminal_gc_does_not_delete_command_log() {
    let (c, _pg) = start_pg().await;
    let cs = PostgresControlPlaneStore::new(c.clone()).await.unwrap();
    let ap = setup(c.clone()).await;

    cs.create_queue(simple_def(tid("t"), qid("q-inv5"))).await.unwrap();
    let push_result =
        ap.batch_push(push_req("t", "q-inv5", "item-inv5", "key-inv5")).await.unwrap();
    let cmd_seq = push_result.command_sequence;

    // Mark item terminal with terminal_at well in the past.
    {
        let client = c.lock().await;
        client
            .execute(
                "UPDATE pqueue_items
                 SET lifecycle_state = 'complete', terminal_at = $1, updated_at = $1
                 WHERE tenant_id = 't' AND queue_id = 'q-inv5' AND item_id = 'item-inv5'",
                &[&past(90_000)],
            )
            .await
            .unwrap();
    }

    // GC terminal items — cutoff is 1 second ago, so the item (90000s past) is GC'd.
    let client = c.lock().await;
    let deleted = retention::expire_terminal_items(&client, "t", "q-inv5", past(1))
        .await
        .unwrap();
    assert_eq!(deleted, 1, "terminal item must be deleted by GC");

    // Verify: item row is gone.
    let item_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items
             WHERE tenant_id = 't' AND queue_id = 'q-inv5' AND item_id = 'item-inv5'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(item_count, 0, "terminal item row must be deleted");

    // INV-5: command log row must survive terminal item GC.
    let cmd_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_commands
             WHERE tenant_id = 't' AND queue_id = 'q-inv5'
               AND shard_id = 0 AND sequence = $1",
            &[&(cmd_seq as i64)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        cmd_count, 1,
        "INV-5: pqueue_commands row must survive terminal item GC; \
         command log outlives item retention"
    );
}

/// Active request_id idempotency record is not deleted by expire_request_idempotency.
/// INV-5: the replay window for this request_id is still open.
#[tokio::test]
async fn postgres_retention_tests_inv5_active_idempotency_record_not_deleted_by_gc() {
    let (c, _pg) = start_pg().await;
    let _ap = setup(c.clone()).await;

    // Insert an idempotency record that expires 1 hour from now.
    {
        let client = c.lock().await;
        client
            .execute(
                "INSERT INTO pqueue_request_idempotency
                 (tenant_id, queue_id, operation, request_id,
                  request_fingerprint, command_positions, expires_at, created_at)
                 VALUES ('t', 'q', 'batch_push', 'req-inv5',
                         '\\xcafe', '{\"shard_0\": 0}'::jsonb, $1, $2)",
                &[&future(3600), &past(60)],
            )
            .await
            .unwrap();
    }

    let client = c.lock().await;
    // GC cutoff is now — this record expires in the future, so it must survive.
    let deleted = retention::expire_request_idempotency(&client, now()).await.unwrap();
    assert_eq!(
        deleted, 0,
        "INV-5: active request_id idempotency record must not be GC'd (replay window open)"
    );

    let count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_request_idempotency
             WHERE tenant_id = 't' AND request_id = 'req-inv5'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "active idempotency record must still exist");
}
