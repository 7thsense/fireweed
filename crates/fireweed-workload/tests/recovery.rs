use fireweed::*;
use fireweed_workload::*;
use std::path::Path;
use std::time::Duration;

fn durable_commit(claim: ClaimRef) -> CommitRequest {
    CommitRequest {
        request_id: Some(RequestId::new("durable-commit").unwrap()),
        entries: vec![CommitEntry {
            claim_ref: claim,
            finalize: FinalizeKind::Complete,
            side_records: vec![SideRecord {
                key: b"receipt/0".to_vec(),
                payload: Bytes::from_static(b"delivered"),
            }],
            lifecycle_items: vec![],
            instance_fence: Some(InstanceFence {
                instance_key: b"instance/0".to_vec(),
                expected: 0,
                next: 1,
            }),
        }],
    }
}

// A separate process exits immediately after acknowledged public API calls,
// bypassing Fireweed drop/shutdown. The parent rebuilds using only its log.
#[test]
fn acknowledged_child() {
    let Some(root) = std::env::var_os("FIREWEED_WORKLOAD_RECOVERY_CHILD") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let fw = open_store(&root, false, TestClock::at(200)).unwrap();
            let q = create_queue(&fw, "recovery").await.unwrap();
            let rows: Vec<_> = (0..12).map(|id| item(id, 0, 1024)).collect();
            let pushed = fw
                .push_batch_with_request_id(&q, RequestId::new("durable-load").unwrap(), rows)
                .await
                .unwrap();
            let claimed = fw.claim(&q, 4, 1000).await.unwrap();
            assert_eq!(claimed.len(), 4);
            let claim = claim_ref(&claimed[0]);
            let committed = fw.commit(&q, durable_commit(claim.clone())).await.unwrap();
            assert!(
                matches!(committed.as_slice(), [EntryOutcome::Committed { .. }]),
                "{committed:?}"
            );
            // Oracle data is outside the log and is never supplied back to workers.
            std::fs::write(
                root.join("oracle.json"),
                serde_json::to_vec(&serde_json::json!({"ids": pushed.item_ids, "claim": claim}))
                    .unwrap(),
            )
            .unwrap();
            std::process::exit(0);
        })
        .await
        .unwrap();
    });
}

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let dst = target.join(entry.file_name());
        if path.is_dir() {
            copy_tree(&path, &dst);
        } else {
            std::fs::copy(path, dst).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_log_rebuild_preserves_ids_payloads_leases_and_receipts() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let original = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "acknowledged_child", "--nocapture"])
            .env("FIREWEED_WORKLOAD_RECOVERY_CHILD", original.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let oracle: serde_json::Value =
            serde_json::from_slice(&std::fs::read(original.path().join("oracle.json")).unwrap())
                .unwrap();
        let expected: Vec<ItemId> = serde_json::from_value(oracle["ids"].clone()).unwrap();
        let original_claim: ClaimRef = serde_json::from_value(oracle["claim"].clone()).unwrap();
        // Crash-loss: the child exited without dropping Fireweed, and only the
        // log is copied. This is RecoveryAction-on-open, not a live
        // ProjectionLifecycle substitute.
        let rebuilt = tempfile::tempdir().unwrap();
        copy_tree(&original.path().join("log"), &rebuilt.path().join("log"));
        let clock = TestClock::at(200);
        let fw = open_store(rebuilt.path(), false, clock.clone()).unwrap();
        let q = create_queue(&fw, "recovery").await.unwrap();
        let replay = fw
            .push_batch_with_request_id(
                &q,
                RequestId::new("durable-load").unwrap(),
                (0..12).map(|id| item(id, 0, 1024)).collect(),
            )
            .await
            .unwrap();
        assert_eq!(replay.item_ids, expected);
        assert_eq!(replay.disposition, PushDisposition::Replayed);
        assert!(matches!(
            fw.push_batch_with_request_id(
                &q,
                RequestId::new("durable-load").unwrap(),
                vec![item(99, 0, 1024)]
            )
            .await,
            Err(EngineError::RequestIdConflict)
        ));
        assert_eq!(
            fw.side_record(&q, b"receipt/0").await.unwrap().as_deref(),
            Some(b"delivered".as_slice())
        );
        let committed = fw
            .commit(&q, durable_commit(original_claim.clone()))
            .await
            .unwrap();
        assert!(
            matches!(committed.as_slice(), [EntryOutcome::Committed { .. }]),
            "commit replay after rebuild: {committed:?}"
        );
        let mut changed = durable_commit(original_claim);
        changed.entries[0].side_records[0].payload = Bytes::from_static(b"different");
        assert!(matches!(
            fw.commit(&q, changed).await,
            Err(EngineError::RequestIdConflict)
        ));
        let metrics = fw.metrics(&q).await.unwrap();
        assert_eq!(
            (metrics.pending, metrics.leased, metrics.complete),
            (8, 3, 1)
        );
        clock.set(202);
        assert_eq!(
            fw.reclaim_expired_at(&q, Some(12), ts(202))
                .await
                .unwrap()
                .len(),
            3
        );
        let mut claimed = Vec::new();
        while claimed.len() < 11 {
            claimed.extend(fw.claim(&q, 11 - claimed.len(), 1000).await.unwrap());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(claimed.len(), 11);
        claimed.sort_by_key(recipient);
        assert_eq!(
            claimed.iter().map(recipient).collect::<Vec<_>>(),
            (1..12).collect::<Vec<_>>()
        );
        for c in &claimed {
            assert_eq!(c.payload, Some(body(recipient(c), 0, 1024)));
        }
    })
    .await
    .expect("log-only recovery timed out");
}

fn basic_mutation(rows: &[ClaimedItem], stage: usize) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new(format!("basic-enrich-{stage}")).unwrap(),
        evaluated_at: ts(200),
        dry_run: false,
        returning: ItemMutationReturning::Identity,
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed {
            entries: rows
                .iter()
                .map(|row| {
                    let id = recipient(row);
                    AddressedMutation {
                        item_id: row.item_id,
                        expected_item_version: Some(row.item_version),
                        predicates: vec![],
                        lease_guard: LeaseGuard::Match(row.lease_token.clone().unwrap()),
                        patch: ItemPatch {
                            lifecycle: LifecyclePatch::SetPending,
                            payload: BatchUpdateValue::Replace(Some(body(id, stage, 1024))),
                            metadata: BatchUpdateValue::Replace(metadata(stage, id)),
                            priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(
                                if stage == 2 { due(id) } else { id as i64 },
                            ))),
                            not_before: BatchUpdateValue::Replace(Some(ts(if stage == 2 {
                                due(id)
                            } else {
                                1
                            }))),
                            ..Default::default()
                        },
                    }
                })
                .collect(),
        },
    }
}

#[test]
fn acknowledged_basic_child() {
    let Some(root) = std::env::var_os("FIREWEED_WORKLOAD_RECOVERY_CHILD") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let fw = open_store_with_projection_root(
                &root,
                false,
                TestClock::at(200),
                &root.join("derived"),
            )
            .unwrap();
            let q = create_queue(&fw, "basic-recovery").await.unwrap();
            let ids = fw
                .push_batch(&q, (0..64).map(|id| item(id, 0, 1024)).collect())
                .await
                .unwrap();
            let mut last_request = None;
            for stage in 1..=2 {
                let claimed = fw.claim(&q, 64, 3_600_000).await.unwrap();
                assert_eq!(claimed.len(), 64);
                for row in &claimed {
                    assert_eq!(row.payload, Some(body(recipient(row), stage - 1, 1024)));
                }
                let request = basic_mutation(&claimed, stage);
                let response = fw.mutate_items(&q, request.clone()).await.unwrap();
                assert!(response.results.iter().all(|r| matches!(r.outcome, ItemMutationOutcome::Updated { .. })));
                last_request = Some(request);
            }
            let delivery = fw.claim(&q, 64, 3_600_000).await.unwrap();
            assert_eq!(delivery.len(), 64);
            fw.complete(&q, delivery[..16].iter().map(|row| row.item_id))
                .await
                .unwrap();
            fw.fail(&q, delivery[16..24].iter().map(|row| row.item_id))
                .await
                .unwrap();
            let remaining: Vec<_> = delivery[24..].iter().map(|row| row.item_id).collect();
            std::fs::write(
                root.join("oracle.json"),
                serde_json::to_vec(&serde_json::json!({"ids":ids,"remaining":remaining,"last_request":last_request})).unwrap(),
            )
            .unwrap();
            std::process::exit(0);
        })
        .await
        .unwrap();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_row_enrichments_and_outcomes_rebuild_from_log_alone() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let original = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "acknowledged_basic_child", "--nocapture"])
            .env("FIREWEED_WORKLOAD_RECOVERY_CHILD", original.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let oracle: serde_json::Value =
            serde_json::from_slice(&std::fs::read(original.path().join("oracle.json")).unwrap())
                .unwrap();
        let ids: Vec<ItemId> = serde_json::from_value(oracle["ids"].clone()).unwrap();
        let remaining: Vec<ItemId> = serde_json::from_value(oracle["remaining"].clone()).unwrap();
        let rebuilt = tempfile::tempdir().unwrap();
        copy_tree(&original.path().join("log"), &rebuilt.path().join("log"));
        let clock = TestClock::at(200);
        let fw = open_store(rebuilt.path(), false, clock.clone()).unwrap();
        let q = create_queue(&fw, "basic-recovery").await.unwrap();
        let m = fw.metrics(&q).await.unwrap();
        assert_eq!((m.pending, m.leased, m.complete, m.failed), (0, 40, 16, 8));
        let request: ItemMutationRequest =
            serde_json::from_value(oracle["last_request"].clone()).unwrap();
        let replay = fw.mutate_items(&q, request).await.unwrap();
        assert_eq!(replay.results.len(), 64);
        assert!(
            replay
                .results
                .iter()
                .all(|r| matches!(r.outcome, ItemMutationOutcome::Updated { .. }))
        );
        fw.release(&q, remaining.iter().copied()).await.unwrap();
        // A release acknowledgement covers the log; the worker polls until the
        // derived queue makes the released rows eligible, without an extra read barrier.
        let delivery = loop {
            let rows = fw.claim(&q, 64, 3_600_000).await.unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        assert_eq!(
            delivery.iter().map(|row| row.item_id).collect::<Vec<_>>(),
            remaining
        );
        for row in &delivery {
            assert_eq!(row.payload, Some(body(recipient(row), 2, 1024)));
        }
        fw.complete(&q, remaining).await.unwrap();
        clock.set(1000);
        assert_eq!(fw.purge(&q, ids, false).await.unwrap(), 64);
        let m = fw.metrics(&q).await.unwrap();
        assert_eq!(m.pending + m.leased + m.complete + m.failed, 0);
    })
    .await
    .expect("basic log-only recovery timed out");
}
