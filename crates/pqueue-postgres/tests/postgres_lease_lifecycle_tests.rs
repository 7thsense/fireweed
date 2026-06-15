// Integration tests for B-041 renew/finalize/expiry lifecycle paths.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        PgBatchClaimRequest, PgBatchFinalizeRequest, PgBatchPushRequest, PgBatchRenewLeasesRequest,
        PgFinalizeItem, PgFinalizeKind, PgFinalizeOutcome, PgLeaseExpiredRequest, PgPushItem,
        PgRenewLeaseItem, PgRenewLeaseOutcome,
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

fn queue_def(queue: &str) -> pqueue_core::QueueDefinition {
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
        recurrence: RecurrencePolicy::default(),
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
        gate_keys: vec![],
        payload: None,
    }
}

fn push_request(queue: &str, item_id: &str) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("push-{queue}-{item_id}"),
        request_id: None,
        items: vec![push_item(item_id, "group", 10)],
        now: ts(1_718_000_000),
    }
}

fn claim_request(queue: &str, token: &str, now: i64, expires: i64) -> PgBatchClaimRequest {
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
        lease_expires_at: ts(expires),
    }
}

fn renew_request(queue: &str, item_id: &str, token: &str, now: i64) -> PgBatchRenewLeasesRequest {
    PgBatchRenewLeasesRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("renew-{queue}-{item_id}-{token}"),
        request_id: None,
        items: vec![PgRenewLeaseItem {
            item_id: item_id.to_string(),
            lease_token: token.to_string(),
        }],
        now: ts(now),
        lease_expires_at: ts(now + 120),
    }
}

fn finalize_request(
    queue: &str,
    item_id: &str,
    token: &str,
    kind: PgFinalizeKind,
    retry_not_before: Option<UtcTimestamp>,
    now: i64,
) -> PgBatchFinalizeRequest {
    PgBatchFinalizeRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("finalize-{queue}-{item_id}-{now}-{kind:?}"),
        request_id: None,
        items: vec![PgFinalizeItem {
            item_id: item_id.to_string(),
            lease_token: token.to_string(),
            kind,
            retry_not_before,
        }],
        now: ts(now),
    }
}

fn expire_request(queue: &str, now: i64) -> PgLeaseExpiredRequest {
    PgLeaseExpiredRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("expire-{queue}-{now}"),
        request_id: None,
        max_items: 10,
        now: ts(now),
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
    control.create_queue(queue_def(queue)).await.unwrap();
    (client, append, pg)
}

#[tokio::test]
async fn postgres_lease_lifecycle_tests_renew_and_finalize_complete_are_token_fenced() {
    let (client, append, _pg) = create_queue("renew-finalize").await;
    append
        .batch_push(push_request("renew-finalize", "item"))
        .await
        .unwrap();
    append
        .batch_claim(claim_request(
            "renew-finalize",
            "tok",
            1_718_000_010,
            1_718_000_050,
        ))
        .await
        .unwrap();

    let stale = append
        .batch_renew_leases(renew_request(
            "renew-finalize",
            "item",
            "wrong-token",
            1_718_000_020,
        ))
        .await
        .unwrap();
    assert_eq!(stale.items[0].outcome, PgRenewLeaseOutcome::StaleLease);

    let renewed = append
        .batch_renew_leases(renew_request(
            "renew-finalize",
            "item",
            "tok",
            1_718_000_020,
        ))
        .await
        .unwrap();
    assert_eq!(
        renewed.items[0].outcome,
        PgRenewLeaseOutcome::Renewed { item_version: 3 }
    );

    let finalized = append
        .batch_finalize(finalize_request(
            "renew-finalize",
            "item",
            "tok",
            PgFinalizeKind::Complete,
            None,
            1_718_000_030,
        ))
        .await
        .unwrap();
    assert_eq!(
        finalized.items[0].outcome,
        PgFinalizeOutcome::Completed { item_version: 4 }
    );

    let c = client.lock().await;
    let item = c
        .query_one(
            "SELECT lifecycle_state, lease_token_hash IS NULL AS token_cleared,
                    terminal_at IS NOT NULL AS terminal_set
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='renew-finalize' AND item_id='item'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(item.get::<_, String>("lifecycle_state"), "complete");
    assert!(item.get::<_, bool>("token_cleared"));
    assert!(item.get::<_, bool>("terminal_set"));
}

#[tokio::test]
async fn postgres_lease_lifecycle_tests_release_and_retry_redeliver() {
    let (_client, append, _pg) = create_queue("release-retry").await;
    append
        .batch_push(push_request("release-retry", "item"))
        .await
        .unwrap();
    append
        .batch_claim(claim_request(
            "release-retry",
            "tok-1",
            1_718_000_010,
            1_718_000_100,
        ))
        .await
        .unwrap();
    let released = append
        .batch_finalize(finalize_request(
            "release-retry",
            "item",
            "tok-1",
            PgFinalizeKind::Release,
            None,
            1_718_000_020,
        ))
        .await
        .unwrap();
    assert_eq!(
        released.items[0].outcome,
        PgFinalizeOutcome::Released { item_version: 3 }
    );
    let reclaimed = append
        .batch_claim(claim_request(
            "release-retry",
            "tok-2",
            1_718_000_030,
            1_718_000_100,
        ))
        .await
        .unwrap();
    assert_eq!(reclaimed.claimed_item_ids, vec!["item"]);

    let retried = append
        .batch_finalize(finalize_request(
            "release-retry",
            "item",
            "tok-2",
            PgFinalizeKind::Retry,
            Some(ts(1_718_000_200)),
            1_718_000_040,
        ))
        .await
        .unwrap();
    assert_eq!(
        retried.items[0].outcome,
        PgFinalizeOutcome::Retried { item_version: 5 }
    );

    let early = append
        .batch_claim(claim_request(
            "release-retry",
            "tok-early",
            1_718_000_100,
            1_718_000_300,
        ))
        .await
        .unwrap();
    assert!(early.claimed_item_ids.is_empty());

    let later = append
        .batch_claim(claim_request(
            "release-retry",
            "tok-later",
            1_718_000_210,
            1_718_000_300,
        ))
        .await
        .unwrap();
    assert_eq!(later.claimed_item_ids, vec!["item"]);
}

#[tokio::test]
async fn postgres_lease_lifecycle_tests_expiry_materializes_before_redelivery() {
    let (client, append, _pg) = create_queue("expiry").await;
    append
        .batch_push(push_request("expiry", "item"))
        .await
        .unwrap();
    append
        .batch_claim(claim_request(
            "expiry",
            "tok-exp",
            1_718_000_010,
            1_718_000_020,
        ))
        .await
        .unwrap();

    let before_expiry_command = append
        .batch_claim(claim_request(
            "expiry",
            "tok-before-expire",
            1_718_000_030,
            1_718_000_100,
        ))
        .await
        .unwrap();
    assert!(before_expiry_command.claimed_item_ids.is_empty());

    let expired = append
        .materialize_expired_leases(expire_request("expiry", 1_718_000_030))
        .await
        .unwrap();
    assert_eq!(expired.expired_item_ids, vec!["item"]);

    let redelivered = append
        .batch_claim(claim_request(
            "expiry",
            "tok-redeliver",
            1_718_000_040,
            1_718_000_100,
        ))
        .await
        .unwrap();
    assert_eq!(redelivered.claimed_item_ids, vec!["item"]);

    let c = client.lock().await;
    let command = c
        .query_one(
            "SELECT command_type
             FROM pqueue_commands
             WHERE tenant_id='tenant' AND queue_id='expiry' AND command_type='lease_expired'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(command.get::<_, String>("command_type"), "lease_expired");
}
