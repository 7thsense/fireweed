//! Receipt lookup source authority, including positional aliases and empty reads.
use super::*;
use fireweed_core::queue_administration::*;
use fireweed_engine::{queue_contract::QueueStore, queue_storage_layout::discovery_digest};
fn ordinary_receipt_group() -> CommitGroup {
    CommitGroup {
        request_id: "ordinary".into(),
        administration: None,
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![UnclaimedUpdate {
            key: "missing".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(3),
                ..Default::default()
            }),
        }],
    }
}
fn administrative_receipt_group() -> CommitGroup {
    CommitGroup {
        request_id: "admin".into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![],
        administration: Some(AdministrativeCommit {
            max_result_bytes: 65536,
            intent: AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                    key: "missing".into(),
                    expected_version: None,
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                }]),
            },
        }),
    }
}
async fn seed_receipts(
    queue: &NativeQueueStore,
    address: &QueueAddress,
) -> (CommitReceipt, CommitReceipt) {
    queue
        .provision(ProvisionRequest {
            queue: address.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    queue
        .insert(InsertRequest {
            queue: address.clone(),
            request_id: "seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![NewWork {
                key: "a".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: vec![1].into(),
            }],
        })
        .await
        .unwrap();
    let mut result = queue
        .commit(CommitBatch {
            queue: address.clone(),
            groups: vec![ordinary_receipt_group(), administrative_receipt_group()],
        })
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    let admin = result.pop().unwrap();
    let ordinary = result.pop().unwrap();
    (ordinary, admin)
}
async fn mixed_receipt_lookup_contract(s3: bool) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-receipt-vector-{s3}-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let prefix = s3.then(|| format!("api007-receipt-vector-{nonce}"));
    let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
    let address = QueueAddress {
        tenant: "receipt-vector".into(),
        queue: "queue".into(),
    };
    let queue = native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
    let (ordinary, admin) = seed_receipts(&queue, &address).await;
    let expected = vec![
        ReceiptLookup::Found(admin.clone()),
        ReceiptLookup::Absent,
        ReceiptLookup::Found(ordinary),
        ReceiptLookup::Found(admin),
        ReceiptLookup::Absent,
    ];
    assert_eq!(expected[0], expected[3]);
    let ids = vec![
        "admin".into(),
        "absent".into(),
        "ordinary".into(),
        "admin".into(),
        "absent".into(),
    ];
    let exact = encoded_len(&expected).unwrap();
    let read = ReceiptReadRequest {
        queue: address.clone(),
        request_ids: ids,
        max_bytes: exact,
    };
    assert_eq!(queue.read_commit(read.clone()).await.unwrap(), expected);
    assert!(matches!(
        queue
            .read_commit(ReceiptReadRequest {
                max_bytes: exact - 1,
                ..read.clone()
            })
            .await,
        Err(QueueError::TooLarge)
    ));
    assert_eq!(
        queue
            .read_commit(ReceiptReadRequest {
                queue: address.clone(),
                request_ids: vec![],
                max_bytes: 1
            })
            .await
            .unwrap(),
        vec![]
    );
    queue.shutdown().await.unwrap();
    drop(queue);
    let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
    assert_eq!(rebuilt.read_commit(read).await.unwrap(), expected);
    rebuilt.shutdown().await.unwrap();
    drop(rebuilt);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn native_lookup_mixed_formats_aliases_absence_and_exact_budget_local() {
    mixed_receipt_lookup_contract(false).await;
}
#[tokio::test]
async fn native_lookup_mixed_formats_aliases_absence_and_exact_budget_real_s3() {
    mixed_receipt_lookup_contract(true).await;
}

async fn corrupt_lookup_receipts_are_source_checked(s3: bool) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for case in [
        "missing",
        "phantom",
        "fingerprint",
        "format",
        "same-length-outcome",
        "oversized",
        "malformed",
        "expiry",
    ] {
        let root = std::env::temp_dir().join(format!("fireweed-receipt-cache-{s3}-{case}-{nonce}"));
        std::fs::create_dir_all(&root).unwrap();
        let prefix = s3.then(|| format!("api007-receipt-cache-{case}-{nonce}"));
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let address = QueueAddress {
            tenant: "receipt-cache".into(),
            queue: case.into(),
        };
        let queue =
            native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
        let (ordinary, admin) = seed_receipts(&queue, &address).await;
        queue.shutdown().await.unwrap();
        drop(queue);
        let path = root.join("primary.db");
        let db = turso::Builder::new_local(path.to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = db.connect().unwrap();
        let params = || vec![turso::Value::Blob(b"ordinary".to_vec())];
        match case {
            "missing" => {
                connection
                    .execute(
                        "DELETE FROM q7_requests WHERE domain=2 AND identity=?1",
                        params(),
                    )
                    .await
                    .unwrap();
            }
            "phantom" => {
                connection.execute("INSERT INTO q7_requests(queue_id,domain,identity_hash,identity,fingerprint_format,fingerprint,outcome,expired) SELECT queue_id,domain,?2,?3,fingerprint_format,fingerprint,outcome,expired FROM q7_requests WHERE domain=2 AND identity=?1",
                vec![turso::Value::Blob(b"ordinary".to_vec()),turso::Value::Blob(discovery_digest(b"phantom").to_vec()),turso::Value::Blob(b"phantom".to_vec())]).await.unwrap();
            }
            "fingerprint" => {
                connection.execute("UPDATE q7_requests SET fingerprint=zeroblob(32) WHERE domain=2 AND identity=?1",params()).await.unwrap();
            }
            "format" => {
                connection.execute("UPDATE q7_requests SET fingerprint_format=4 WHERE domain=2 AND identity=?1",params()).await.unwrap();
            }
            "same-length-outcome" => {
                assert!(matches!(
                    ordinary.outcome,
                    GroupOutcome::Rejected(Rejection::NotFound)
                ));
                connection
                    .execute(
                        "UPDATE q7_requests SET outcome=x'0101' WHERE domain=2 AND identity=?1",
                        params(),
                    )
                    .await
                    .unwrap();
                fireweed_engine::queue_publication::view_receipt_outcome(2, 3, &[1, 1]).unwrap();
            }
            "oversized" => {
                connection.execute("UPDATE q7_requests SET outcome=zeroblob(8388608) WHERE domain=2 AND identity=?1",params()).await.unwrap();
            }
            "malformed" => {
                connection
                    .execute(
                        "UPDATE q7_requests SET outcome=x'ff' WHERE domain=2 AND identity=?1",
                        params(),
                    )
                    .await
                    .unwrap();
            }
            "expiry" => {
                connection
                    .execute(
                        "UPDATE q7_requests SET expired=1 WHERE domain=2 AND identity=?1",
                        params(),
                    )
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        drop(connection);
        drop(db);
        let reopened =
            native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
        let target = if case == "phantom" {
            "phantom"
        } else {
            "ordinary"
        };
        let outcome = reopened
            .read_commit(ReceiptReadRequest {
                queue: address.clone(),
                request_ids: vec!["admin".into(), target.into(), target.into()],
                max_bytes: 1 << 20,
            })
            .await;
        assert!(
            matches!(outcome, Err(QueueError::OutcomeUnknown(_))),
            "lookup corruption: s3={s3} case={case} outcome={outcome:?}"
        );
        reopened.shutdown().await.unwrap();
        drop(reopened);
        let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
        assert_eq!(
            rebuilt
                .read_commit(ReceiptReadRequest {
                    queue: address,
                    request_ids: vec![
                        "admin".into(),
                        "ordinary".into(),
                        "ordinary".into(),
                        "phantom".into()
                    ],
                    max_bytes: 1 << 20
                })
                .await
                .unwrap(),
            vec![
                ReceiptLookup::Found(admin),
                ReceiptLookup::Found(ordinary.clone()),
                ReceiptLookup::Found(ordinary),
                ReceiptLookup::Absent
            ]
        );
        rebuilt.shutdown().await.unwrap();
        drop(rebuilt);
        let path = root.join("rebuilt.db");
        let db = turso::Builder::new_local(path.to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = db.connect().unwrap();
        let mut rows = connection
            .query("SELECT applied_next_offset FROM q7_queues", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            turso::Value::Integer(4),
            "cold source head unchanged by failed read: {case}"
        );
        drop(rows);
        drop(connection);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[tokio::test]
async fn native_lookup_original_authority_rejects_corrupt_cache_local() {
    corrupt_lookup_receipts_are_source_checked(false).await;
}
#[tokio::test]
async fn native_lookup_original_authority_rejects_corrupt_cache_real_s3() {
    corrupt_lookup_receipts_are_source_checked(true).await;
}
