//! Public-only native binding: consumers name no core/engine/backend types.
use fireweed::queue::*;
use fireweed_conformance::queue_contract::{self as suite, TestClock};
use std::sync::Arc;
#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn concrete_queue_runs_identical_contract_and_reopens_exact_receipt() {
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").expect("isolated public facade fixture");
    let options = PostgresOptions::new(ConfigSecret::new(url));
    assert!(!format!("{options:?}").contains("postgresql://"));
    let clock = Arc::new(TestClock::default());
    let queue = Queue::postgres(options.clone(), clock.clone())
        .await
        .unwrap();
    let other = Queue::postgres(options.clone(), clock.clone())
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let address = |name: &str| QueueAddress {
        tenant: "api007-public-facade".into(),
        queue: format!("{nonce}-{name}"),
    };
    let primary = address("contract");
    suite::run(&queue, primary.clone()).await;
    let expected = queue
        .read_commit(ReceiptReadRequest {
            queue: primary.clone(),
            request_ids: vec!["transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert!(matches!(expected[0], ReceiptLookup::Found(_)));
    suite::competing_claims(&queue, &other, address("competition")).await;
    suite::expiry_and_administration(&queue, &clock, address("expiry")).await;
    suite::ordered_long_values_and_cursor_scope(&queue, address("long")).await;
    suite::predicate_and_order_oracles(&queue, address("predicates")).await;
    suite::ordered_groups_sparse_stats_and_replay_precedence(&queue, &clock, address("groups"))
        .await;
    suite::ordered_insert_uniqueness(&queue, address("insert-uniqueness")).await;
    suite::shrinking_upsert_index_amplification(&queue, address("shrinking-upsert")).await;
    suite::indexed_additional_attributes(&queue, address("additional-attributes")).await;
    suite::numeric_bucket_stats(&queue, &clock, address("numeric-buckets")).await;
    suite::unrestricted_names_obey_response_bounds(&queue, address("names-bounds")).await;
    let acknowledged = suite::prepare_process_loss(&queue, address("acknowledged-reopen")).await;
    let clone = queue.clone();
    queue.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    assert_eq!(
        clone
            .read_commit(ReceiptReadRequest {
                queue: primary.clone(),
                request_ids: vec!["transition".into()],
                max_bytes: 1 << 20
            })
            .await,
        Err(QueueError::Busy)
    );
    let reopened = Queue::postgres(options, clock).await.unwrap();
    assert_eq!(
        reopened
            .read_commit(ReceiptReadRequest {
                queue: primary.clone(),
                request_ids: vec!["transition".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        expected
    );
    suite::verify_process_loss(&reopened, acknowledged).await;
    let state = reopened
        .read_state(StateReadRequest {
            queue: primary,
            collection: "instances".into(),
            keys: vec!["instance".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    reopened.shutdown().await.unwrap();
}
