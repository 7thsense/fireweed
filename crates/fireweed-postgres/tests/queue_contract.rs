use fireweed_core::queue_contract::*;
use fireweed_engine::{Clock, queue_contract::QueueStore};
use fireweed_postgres::PostgresQueueStore;
use std::sync::Arc;
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}
#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn mandatory_shared_queue_contract_and_reopen() {
    let url =
        std::env::var("FIREWEED_API007_PG_TEST_URL").expect("isolated PostgreSQL fixture URL");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let queue = QueueAddress {
        tenant: "api007-conformance".into(),
        queue: nonce.to_string(),
    };
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
    fireweed_conformance::queue_contract::run(&store, queue.clone()).await;
    store.shutdown().await.unwrap();
    let reopened =
        PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
            .await
            .unwrap();
    let state = reopened
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["instance".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    let receipt = reopened
        .read_commit(ReceiptReadRequest {
            queue,
            request_ids: vec!["transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert!(matches!(receipt[0], ReceiptLookup::Found(_)));
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn mandatory_competition_expiry_and_long_ordering() {
    use fireweed_conformance::queue_contract::{self as suite, TestClock};
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
    let clock = Arc::new(TestClock::default());
    let a = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 2)
        .await
        .unwrap();
    let b = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 2)
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let q = |name: &str| QueueAddress {
        tenant: "api007-conformance".into(),
        queue: format!("{nonce}-{name}"),
    };
    suite::competing_claims(&a, &b, q("competition")).await;
    suite::expiry_and_administration(&a, &clock, q("expiry")).await;
    suite::ordered_long_values_and_cursor_scope(&a, q("long")).await;
    suite::predicate_and_order_oracles(&a, q("predicate-oracles")).await;
    suite::ordered_groups_sparse_stats_and_replay_precedence(&a, &clock, q("groups-stats-replay"))
        .await;
    suite::ordered_insert_uniqueness(&a, q("insert-uniqueness")).await;
    suite::shrinking_upsert_index_amplification(&a, q("shrinking-index-upsert")).await;
    suite::unrestricted_names_obey_response_bounds(&a, q("names-bounds")).await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
