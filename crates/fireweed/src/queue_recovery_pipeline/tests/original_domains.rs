//! Paired immutable source-original integrity controls.
use super::*;
#[cfg(target_os = "linux")]
async fn corrupt_single_originals_are_source_checked(s3: bool, only_domain: Option<i64>) {
    use fireweed_engine::{queue_contract::QueueStore, queue_storage_layout::discovery_digest};
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for domain in [0_i64, 1] {
        if only_domain.is_some_and(|selected| selected != domain) {
            continue;
        }
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
            if only_domain.is_some() && case != "same-length-outcome" {
                continue;
            }
            let root = std::env::temp_dir().join(format!(
                "fireweed-single-authority-{s3}-{domain}-{case}-{nonce}"
            ));
            std::fs::create_dir_all(&root).unwrap();
            let prefix = s3.then(|| format!("api007-single-authority-{domain}-{case}-{nonce}"));
            let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
            let queue =
                native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
            let address = QueueAddress {
                tenant: "single-authority".into(),
                queue: case.into(),
            };
            queue
                .provision(ProvisionRequest {
                    queue: address.clone(),
                    schema: schema(),
                })
                .await
                .unwrap();
            let insert = InsertRequest {
                queue: address.clone(),
                request_id: "known".into(),
                mode: InsertMode::InsertOnly,
                items: ["a", "b"]
                    .into_iter()
                    .map(|key| NewWork {
                        key: key.into(),
                        priority: 0,
                        available_at: 0,
                        fields: Default::default(),
                        body: vec![1].into(),
                    })
                    .collect(),
            };
            let inserted = queue.insert(insert.clone()).await.unwrap();
            let claim = ClaimRequest {
                queue: address.clone(),
                request_id: "known".into(),
                filter: Filter::default(),
                order: ClaimOrder::PriorityAscending,
                eligible_at: 0,
                lease_nanos: 1_000_000_000,
                max_records: 2,
                max_bytes: 1 << 20,
                fields: None,
                include_body: true,
            };
            let claimed = queue.claim(claim.clone()).await.unwrap();
            assert_eq!(claimed.len(), 2);
            // The exact same literal identity is valid independently in all three domains.
            queue
                .commit(CommitBatch {
                    queue: address.clone(),
                    groups: vec![CommitGroup {
                        request_id: "known".into(),
                        administration: None,
                        state_updates: vec![],
                        work_updates: vec![],
                        work_inserts: vec![],
                        claimed_updates: claimed
                            .iter()
                            .map(|work| ClaimedUpdate {
                                key: work.record.work.key.clone(),
                                lease: work.lease.clone(),
                                expected_version: work.record.version,
                                patch: WorkPatch::default(),
                                disposition: Disposition::Check,
                            })
                            .collect(),
                    }],
                })
                .await
                .unwrap();
            queue.shutdown().await.unwrap();
            drop(queue);
            let path = root.join("primary.db");
            let db = turso::Builder::new_local(path.to_str().unwrap())
                .build()
                .await
                .unwrap();
            let connection = db.connect().unwrap();
            let params = || {
                vec![
                    turso::Value::Integer(domain),
                    turso::Value::Blob(b"known".to_vec()),
                ]
            };
            let mut rows = connection
                .query(
                    "SELECT outcome FROM q7_requests WHERE domain=?1 AND identity=?2",
                    params(),
                )
                .await
                .unwrap();
            let raw = match rows.next().await.unwrap().unwrap().get_value(0).unwrap() {
                turso::Value::Blob(bytes) => bytes,
                _ => panic!("receipt bytes"),
            };
            drop(rows);
            match case {
                "missing" => {
                    connection
                        .execute(
                            "DELETE FROM q7_requests WHERE domain=?1 AND identity=?2",
                            params(),
                        )
                        .await
                        .unwrap();
                }
                "phantom" => {
                    connection.execute("INSERT INTO q7_requests(queue_id,domain,identity_hash,identity,fingerprint_format,fingerprint,outcome,expired) SELECT queue_id,domain,?3,?4,fingerprint_format,fingerprint,outcome,expired FROM q7_requests WHERE domain=?1 AND identity=?2",
                    vec![turso::Value::Integer(domain),turso::Value::Blob(b"known".to_vec()),turso::Value::Blob(discovery_digest(b"phantom").to_vec()),turso::Value::Blob(b"phantom".to_vec())]).await.unwrap();
                }
                "fingerprint" => {
                    connection.execute("UPDATE q7_requests SET fingerprint=zeroblob(32) WHERE domain=?1 AND identity=?2",params()).await.unwrap();
                }
                "format" => {
                    connection.execute("UPDATE q7_requests SET fingerprint_format=4 WHERE domain=?1 AND identity=?2",params()).await.unwrap();
                }
                "same-length-outcome" => {
                    let mut changed = raw.clone();
                    if domain == 0 {
                        // FWQP insert result: count, tag, key length, one-byte key, version.
                        let offset = 8 + 1 + 8 + 1;
                        assert_eq!(&changed[offset..offset + 8], &1_u64.to_le_bytes());
                        changed[offset..offset + 8].copy_from_slice(&2_u64.to_le_bytes());
                        let view = fireweed_engine::queue_publication::view_receipt_outcome(
                            0, 3, &changed,
                        )
                        .unwrap();
                        let fireweed_engine::queue_publication::ReceiptOutcomeRef::Insert(outcomes) =
                            view
                        else {
                            panic!("insert result")
                        };
                        fireweed_engine::queue_publication::validate_insert_receipt(
                            &insert, outcomes,
                        )
                        .unwrap();
                    } else {
                        // Valid original lease membership with an older creation version:
                        // current ownership/token and current >= creation still pass.
                        let offset = 13 + 8 + 1 + 8 + claimed[0].lease.token.len();
                        assert_eq!(&changed[offset..offset + 8], &2_u64.to_le_bytes());
                        changed[offset..offset + 8].copy_from_slice(&1_u64.to_le_bytes());
                        fireweed_engine::queue_claim_receipt::view_claim_receipt(&changed).unwrap();
                    }
                    assert_eq!(changed.len(), raw.len());
                    assert_ne!(changed, raw);
                    connection
                        .execute(
                            "UPDATE q7_requests SET outcome=?3 WHERE domain=?1 AND identity=?2",
                            vec![
                                turso::Value::Integer(domain),
                                turso::Value::Blob(b"known".to_vec()),
                                turso::Value::Blob(changed),
                            ],
                        )
                        .await
                        .unwrap();
                }
                "oversized" => {
                    connection.execute("UPDATE q7_requests SET outcome=zeroblob(8388608) WHERE domain=?1 AND identity=?2",params()).await.unwrap();
                }
                "malformed" => {
                    connection
                        .execute(
                            "UPDATE q7_requests SET outcome=x'ff' WHERE domain=?1 AND identity=?2",
                            params(),
                        )
                        .await
                        .unwrap();
                }
                "expiry" => {
                    connection
                        .execute(
                            "UPDATE q7_requests SET expired=1 WHERE domain=?1 AND identity=?2",
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
            let mut caller_insert = insert.clone();
            let mut caller_claim = claim.clone();
            if case == "phantom" {
                caller_insert.request_id = "phantom".into();
                caller_claim.request_id = "phantom".into();
            }
            let outcome = if domain == 0 {
                reopened.insert(caller_insert).await.map(|_| ())
            } else {
                reopened.claim(caller_claim).await.map(|_| ())
            };
            assert!(
                matches!(outcome, Err(QueueError::OutcomeUnknown(_))),
                "domain={domain} case={case} s3={s3} outcome={outcome:?}"
            );
            if matches!(
                case,
                "same-length-outcome" | "oversized" | "malformed" | "expiry"
            ) {
                let mut changed_insert = insert.clone();
                changed_insert.items[0].body = vec![2].into();
                let mut changed_claim = claim.clone();
                changed_claim.max_records = 1;
                let changed = if domain == 0 {
                    reopened.insert(changed_insert).await.map(|_| ())
                } else {
                    reopened.claim(changed_claim).await.map(|_| ())
                };
                assert!(
                    matches!(changed, Err(QueueError::RequestConflict)),
                    "independently proved conflict precedes opaque outcome/expiry: domain={domain} case={case} changed={changed:?}"
                );
            }
            reopened.shutdown().await.unwrap();
            drop(reopened);
            let db = turso::Builder::new_local(path.to_str().unwrap())
                .build()
                .await
                .unwrap();
            let connection = db.connect().unwrap();
            let mut rows=connection.query("SELECT applied_next_offset,(SELECT count(*) FROM q7_records WHERE collection=0) FROM q7_queues",()).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(
                row.get_value(0).unwrap(),
                turso::Value::Integer(4),
                "no extra publication: domain={domain} case={case}"
            );
            assert_eq!(row.get_value(1).unwrap(), turso::Value::Integer(2));
            drop(rows);
            drop(connection);
            drop(db);
            let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
            assert_eq!(
                rebuilt.insert(insert).await.unwrap(),
                inserted,
                "cold insert source original: {case}"
            );
            assert_eq!(
                rebuilt.claim(claim).await.unwrap(),
                claimed,
                "cold claim source original: {case}"
            );
            rebuilt.shutdown().await.unwrap();
            drop(rebuilt);
            let rebuilt_path = root.join("rebuilt.db");
            let db = turso::Builder::new_local(rebuilt_path.to_str().unwrap())
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
                "cold source cut proves no hidden append: domain={domain} case={case}"
            );
            drop(rows);
            drop(connection);
            drop(db);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_single_original_authority_rejects_corrupt_cache_local() {
    corrupt_single_originals_are_source_checked(false, None).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_single_original_authority_rejects_corrupt_cache_real_s3() {
    corrupt_single_originals_are_source_checked(true, None).await;
}

#[tokio::test]
async fn native_insert_original_hash_rejects_same_length_valid_result() {
    corrupt_single_originals_are_source_checked(false, Some(0)).await;
}
#[tokio::test]
async fn native_claim_original_hash_rejects_same_length_valid_membership() {
    corrupt_single_originals_are_source_checked(false, Some(1)).await;
}
