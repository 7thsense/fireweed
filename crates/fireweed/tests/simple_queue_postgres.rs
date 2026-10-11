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
    suite::atomic_administration_preview_and_replay(&queue, address("administration-preview"))
        .await;
    suite::administrative_size_siblings_and_preflight(&queue, address("administration-sizes"))
        .await;
    suite::broad_administration_exceeds_input_chunk(&queue, address("administration-broad")).await;
    let acknowledged = suite::prepare_process_loss(&queue, address("acknowledged-reopen")).await;
    let administrative_expected = queue
        .read_commit(ReceiptReadRequest {
            queue: address("administration-preview"),
            request_ids: vec!["admin-transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    let wide_expected = queue
        .read_commit(ReceiptReadRequest {
            queue: address("administration-sizes"),
            request_ids: vec!["wide-accepted".into(), "middle".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
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
    assert_eq!(
        reopened
            .read_commit(ReceiptReadRequest {
                queue: address("administration-preview"),
                request_ids: vec!["admin-transition".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        administrative_expected
    );
    assert_eq!(
        reopened
            .read_commit(ReceiptReadRequest {
                queue: address("administration-sizes"),
                request_ids: vec!["wide-accepted".into(), "middle".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        wide_expected
    );

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

#[tokio::test]
async fn invalid_construction_never_contacts_server_and_redacts_secrets() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let secret = "CONSTRUCTION-SECRET-MUST-NOT-APPEAR";
    let url = format!("postgresql://user:{secret}@{address}/postgres");
    let clock = Arc::new(TestClock::default());
    let mut cases = Vec::new();
    let mut invalid = PostgresOptions::new(ConfigSecret::new(url.clone()));
    invalid.limits.max_records = 0;
    cases.push((invalid, false));
    for pool in [0, 33] {
        let mut invalid = PostgresOptions::new(ConfigSecret::new(url.clone()));
        invalid.pool_size = pool;
        cases.push((invalid, false));
    }
    for runtime in [
        QueueRuntimeOptions {
            operations: 0,
            ..Default::default()
        },
        QueueRuntimeOptions {
            operations: usize::MAX,
            ..Default::default()
        },
        QueueRuntimeOptions {
            retained_bytes: Some(0),
            ..Default::default()
        },
        QueueRuntimeOptions {
            scratch_bytes: Some(0),
            ..Default::default()
        },
        QueueRuntimeOptions {
            scratch_workers: Some(0),
            ..Default::default()
        },
        QueueRuntimeOptions {
            operations: 1,
            scratch_workers: Some(2),
            ..Default::default()
        },
    ] {
        let mut invalid = PostgresOptions::new(ConfigSecret::new(url.clone()));
        invalid.runtime = runtime;
        cases.push((invalid, false));
    }
    let mut overflow = PostgresOptions::new(ConfigSecret::new(url.clone()));
    overflow.limits.max_request_bytes = u64::MAX / 8 + 1;
    cases.push((overflow, true));
    let mut overflow = PostgresOptions::new(ConfigSecret::new(url));
    overflow.limits.max_request_bytes = u64::MAX / 16 + 1;
    overflow.limits.max_group_bytes = u64::MAX / 16 + 1;
    cases.push((overflow, true));
    for invalid_url in ["   ".to_owned(), format!("host='{secret}")] {
        cases.push((PostgresOptions::new(ConfigSecret::new(invalid_url)), false));
    }
    for (options, overflow) in cases {
        let error = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::select! {
                result = Queue::postgres(options, clock.clone()) => match result {
                    Err(error) => error,
                    Ok(_) => panic!("invalid configuration constructed a queue"),
                },
                _ = listener.accept() => panic!("invalid configuration contacted PostgreSQL"),
            }
        })
        .await
        .expect("configuration must fail without waiting for I/O");
        assert!(!format!("{error:?}").contains(secret));
        if overflow {
            assert_eq!(error, QueueError::TooLarge);
        } else {
            assert!(matches!(error, QueueError::Invalid(_)));
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn explicit_retained_capacity_is_enforced_through_public_clones() {
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
    let mut options = PostgresOptions::new(ConfigSecret::new(url));
    options.runtime = QueueRuntimeOptions {
        operations: 1,
        retained_bytes: Some(1),
        scratch_bytes: Some(1),
        scratch_workers: Some(1),
    };
    let queue = Queue::postgres(options, Arc::new(TestClock::default()))
        .await
        .unwrap();
    let clone = queue.clone();
    let request = ReceiptReadRequest {
        queue: QueueAddress {
            tenant: "capacity-test".into(),
            queue: "missing".into(),
        },
        request_ids: vec!["original".into()],
        max_bytes: 1024,
    };
    // This valid request would reach QueueNotFound if construction ignored the
    // explicit retained-byte setting. Both clones reject before any queue read.
    assert_eq!(
        queue.read_commit(request.clone()).await,
        Err(QueueError::Busy)
    );
    assert_eq!(clone.read_commit(request).await, Err(QueueError::Busy));
    queue.shutdown().await.unwrap();
    clone.shutdown().await.unwrap();
}
