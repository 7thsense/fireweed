// Integration tests for B-053 Postgres recurring rearm and native purge.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrenceMode, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        PgBatchClaimRequest, PgBatchFinalizeRequest, PgBatchPushRequest, PgFinalizeItem,
        PgFinalizeKind, PgFinalizeOutcome, PgPurgeItem, PgPurgeItemsRequest, PgPurgeOutcome,
        PgPushItem,
    },
};
use pqueue_storage::traits::ControlPlaneStore;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

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

fn tid(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn recurring_queue(queue: &str) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tid("tenant"),
        queue_id: qid(queue),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        group_co_residency: true,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(ts(1_718_100_000)),
        },
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: Some(50),
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn push_item(id: &str, group: &str, priority: i64) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: format!("key-{id}"),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: Some(group.to_string()),
        cohort_size: None,
        recurrence_until: None,
        gate_keys: vec![],
        payload: None,
    }
}

fn push_request(queue: &str, item: PgPushItem) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("push-{queue}-{}", item.item_id),
        request_id: None,
        items: vec![item],
        now: ts(1_718_000_000),
    }
}

fn claim_request(queue: &str, token: &str, now: i64) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("claim-{queue}-{token}"),
        request_id: None,
        max_items: 10,
        now: ts(now),
        lease_token: token.to_string(),
        lease_expires_at: ts(now + 100),
    }
}

fn finalize_rearm_request(
    queue: &str,
    item_id: &str,
    token: &str,
    not_before: i64,
) -> PgBatchFinalizeRequest {
    PgBatchFinalizeRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("rearm-{queue}-{item_id}"),
        request_id: None,
        items: vec![PgFinalizeItem {
            item_id: item_id.to_string(),
            lease_token: token.to_string(),
            kind: PgFinalizeKind::Rearm,
            retry_not_before: Some(ts(not_before)),
        }],
        now: ts(1_718_000_010),
    }
}

fn purge_request(
    queue: &str,
    request_id: Option<&str>,
    force: bool,
    item: PgPurgeItem,
) -> PgPurgeItemsRequest {
    PgPurgeItemsRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("purge-{queue}-{}", request_id.unwrap_or("none")),
        request_id: request_id.map(str::to_string),
        force,
        items: vec![item],
        now: ts(1_718_000_020),
    }
}

async fn create_queue(
    queue: &str,
) -> (
    Arc<Mutex<tokio_postgres::Client>>,
    PostgresAppendStore,
    impl std::fmt::Debug,
) {
    let (client, pg) = start_pg().await;
    let control = PostgresControlPlaneStore::new(client.clone())
        .await
        .unwrap();
    let append = PostgresAppendStore::new(client.clone()).await.unwrap();
    control.create_queue(recurring_queue(queue)).await.unwrap();
    (client, append, pg)
}

#[tokio::test]
async fn postgres_recurrence_purge_tests_rearm_resets_retry_and_recomputes_summary() {
    let (client, append, _pg) = create_queue("rearm").await;
    append
        .batch_push(push_request("rearm", push_item("tick-1", "job-1", 100)))
        .await
        .unwrap();

    let claimed = append
        .batch_claim(claim_request("rearm", "lease-1", 1_718_000_001))
        .await
        .unwrap();
    assert_eq!(claimed.claimed_item_ids, vec!["tick-1"]);

    let rearmed = append
        .batch_finalize(finalize_rearm_request(
            "rearm",
            "tick-1",
            "lease-1",
            1_718_000_100,
        ))
        .await
        .unwrap();
    assert!(matches!(
        rearmed.items[0].outcome,
        PgFinalizeOutcome::Rearmed { item_version: 3 }
    ));

    let c = client.lock().await;
    let item = c
        .query_one(
            "SELECT lifecycle_state, retry_count, lease_token_hash IS NULL AS no_lease,
                    not_before, eligible_since, item_version
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='rearm' AND item_id='tick-1'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(item.get::<_, String>("lifecycle_state"), "pending");
    assert_eq!(item.get::<_, i32>("retry_count"), 0);
    assert!(item.get::<_, bool>("no_lease"));
    assert_eq!(item.get::<_, i64>("item_version"), 3);

    let summary = c
        .query_one(
            "SELECT oldest_eligible_at IS NULL AS no_eligible
             FROM pqueue_group_summary
             WHERE tenant_id='tenant' AND queue_id='rearm' AND group_key='job-1'",
            &[],
        )
        .await
        .unwrap();
    assert!(summary.get::<_, bool>("no_eligible"));
}

#[tokio::test]
async fn postgres_recurrence_purge_tests_force_purge_invalidates_lease_and_replays_result() {
    let (client, append, _pg) = create_queue("purge").await;
    append
        .batch_push(push_request("purge", push_item("tick-1", "job-1", 100)))
        .await
        .unwrap();
    append
        .batch_claim(claim_request("purge", "lease-1", 1_718_000_001))
        .await
        .unwrap();

    let conflict = append
        .purge_items(purge_request(
            "purge",
            None,
            false,
            PgPurgeItem {
                item_id: Some("tick-1".to_string()),
                client_item_key: None,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        conflict.items[0].outcome,
        PgPurgeOutcome::Conflict { .. }
    ));

    let purge = append
        .purge_items(purge_request(
            "purge",
            Some("purge-req-1"),
            true,
            PgPurgeItem {
                item_id: None,
                client_item_key: Some("key-tick-1".to_string()),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        purge.items[0].outcome,
        PgPurgeOutcome::Purged { .. }
    ));

    let replay = append
        .purge_items(purge_request(
            "purge",
            Some("purge-req-1"),
            true,
            PgPurgeItem {
                item_id: None,
                client_item_key: Some("key-tick-1".to_string()),
            },
        ))
        .await
        .unwrap();
    assert_eq!(replay.command_sequence, purge.command_sequence);
    assert!(matches!(
        replay.items[0].outcome,
        PgPurgeOutcome::Purged { .. }
    ));

    let c = client.lock().await;
    let items = c
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='purge'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(items, 0);

    let tombstone = c
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_item_key_retention
             WHERE tenant_id='tenant' AND queue_id='purge' AND client_item_key='key-tick-1'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(tombstone, 1);

    let commands = c
        .query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_commands
             WHERE tenant_id='tenant' AND queue_id='purge' AND command_type='purge_items'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(commands, 1);
}
