//! Black-box acceptance: no projection SQL, engine internals, or direct log writes.
use fireweed::*;
use fireweed_workload::*;
use std::time::Duration;

fn update(id: ItemId, payload: BatchUpdateValue<Option<Bytes>>) -> BatchUpdateEntry {
    BatchUpdateEntry {
        item_ref: BatchUpdateItemRef::ItemId(id),
        expected_item_version: None,
        payload,
        priority: BatchUpdateValue::Keep,
        not_before: BatchUpdateValue::Keep,
        metadata: BatchUpdateValue::Keep,
        gate_keys: BatchUpdateValue::Keep,
        fields: BatchUpdateValue::Keep,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_replacement_keep_clear_and_purge() {
    tokio::time::timeout(Duration::from_secs(30), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let fw = open_store(root.path(), memory, TestClock::at(200)).unwrap();
            let q = create_queue(&fw, "payload").await.unwrap();
            let ids = fw
                .push_batch(&q, vec![item(0, 0, 1024), item(1, 0, 1024)])
                .await
                .unwrap();
            let request = BatchUpdateRequest {
                request_id: RequestId::new("replace").unwrap(),
                updates: vec![
                    update(
                        ids[0],
                        BatchUpdateValue::Replace(Some(Bytes::from_static(b"enriched"))),
                    ),
                    update(ids[1], BatchUpdateValue::Keep),
                ],
            };
            let first = fw.batch_update(&q, request.clone()).await.unwrap();
            assert!(
                first
                    .results
                    .iter()
                    .all(|r| matches!(r, BatchUpdateOutcome::Updated { .. })),
                "{first:?}"
            );
            assert_eq!(fw.batch_update(&q, request).await.unwrap(), first);
            let claimed = fw.claim_with(&q, 2, 1000, compatibility(0)).await.unwrap();
            assert_eq!(claimed.len(), 2);
            assert_eq!(claimed[0].payload.as_deref(), Some(b"enriched".as_slice()));
            assert_eq!(claimed[1].payload, Some(body(1, 0, 1024)));
            fw.release(&q, ids.clone()).await.unwrap();
            let cleared = fw
                .batch_update(
                    &q,
                    BatchUpdateRequest {
                        request_id: RequestId::new("clear").unwrap(),
                        updates: ids
                            .iter()
                            .map(|id| update(*id, BatchUpdateValue::Replace(None)))
                            .collect(),
                    },
                )
                .await
                .unwrap();
            assert!(
                cleared
                    .results
                    .iter()
                    .all(|r| matches!(r, BatchUpdateOutcome::Updated { .. })),
                "{cleared:?}"
            );
            let claimed = fw.claim(&q, 2, 1000).await.unwrap();
            assert_eq!(claimed.len(), 2);
            assert!(claimed.iter().all(|c| c.payload.is_none()));
            fw.complete(&q, ids.clone()).await.unwrap();
            assert_eq!(fw.purge(&q, ids.clone(), false).await.unwrap(), 2);
            assert_eq!(fw.purge(&q, ids, false).await.unwrap(), 0);
            let m = fw.metrics(&q).await.unwrap();
            assert_eq!((m.pending, m.leased, m.complete, m.failed), (0, 0, 0, 0));
            let fresh = fw
                .push_batch(&q, vec![item(0, 0, 1024), item(1, 0, 1024)])
                .await
                .unwrap();
            assert_eq!(fresh.len(), 2);
            assert!(fresh.iter().all(|id| *id > claimed[1].item_id));
        }
    })
    .await
    .expect("payload contract timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn due_order_fifo_and_expired_lease_fencing() {
    tokio::time::timeout(Duration::from_secs(30), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let clock = TestClock::at(100);
            let fw = open_store(root.path(), memory, clock.clone()).unwrap();
            let q = create_queue(&fw, "ordering").await.unwrap();
            let mut rows: Vec<_> = (0..4).map(|id| item(id, 0, 256)).collect();
            for (row, (priority, due)) in
                rows.iter_mut()
                    .zip([(1, 300), (4, 100), (2, 100), (2, 100)])
            {
                row.priority = Some(PriorityValue::Int64(priority));
                row.not_before = Some(ts(due));
            }
            let ids = fw.push_batch(&q, rows).await.unwrap();
            let claimed = fw.claim(&q, 3, 1000).await.unwrap();
            assert_eq!(
                claimed.iter().map(|c| c.item_id).collect::<Vec<_>>(),
                vec![ids[2], ids[3], ids[1]]
            );
            assert!(fw.claim(&q, 3, 1000).await.unwrap().is_empty());
            clock.set(102);
            let reclaimed = fw.reclaim_expired_at(&q, Some(3), ts(102)).await.unwrap();
            assert_eq!(reclaimed.len(), 3);
            let mut fresh = Vec::new();
            while fresh.len() < 3 {
                fresh.extend(fw.claim(&q, 3 - fresh.len(), 1000).await.unwrap());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(fresh.len(), 3);
            let stale = fw
                .commit(
                    &q,
                    CommitRequest {
                        request_id: Some(RequestId::new("stale").unwrap()),
                        entries: vec![CommitEntry {
                            claim_ref: claim_ref(&claimed[0]),
                            finalize: FinalizeKind::Complete,
                            side_records: vec![SideRecord {
                                key: b"must-not-exist".to_vec(),
                                payload: Bytes::from_static(b"bad"),
                            }],
                            lifecycle_items: vec![item(99, 0, 256)],
                            instance_fence: None,
                        }],
                    },
                )
                .await
                .unwrap();
            assert!(
                matches!(stale.as_slice(), [EntryOutcome::Rejected(_)]),
                "{stale:?}"
            );
            assert!(
                fw.side_record(&q, b"must-not-exist")
                    .await
                    .unwrap()
                    .is_none()
            );
            fw.complete(&q, fresh.iter().map(|c| c.item_id))
                .await
                .unwrap();
            clock.set(300);
            let future = fw.claim(&q, 3, 1000).await.unwrap();
            assert_eq!(future.len(), 1);
            assert_eq!(future[0].item_id, ids[0]);
        }
    })
    .await
    .expect("ordering contract timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separate_uniform_batches_keep_global_priority_and_future_eligibility() {
    tokio::time::timeout(Duration::from_secs(20), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let clock = TestClock::at(100);
            let fw = open_store(root.path(), memory, clock.clone()).unwrap();
            let q = create_queue(&fw, "separate-batches").await.unwrap();
            let mut future = item(0, 0, 100);
            future.priority = Some(PriorityValue::Int64(0));
            future.not_before = Some(ts(200));
            let future_id = fw.push_batch(&q, vec![future]).await.unwrap()[0];
            let mut high = item(1, 0, 100);
            high.priority = Some(PriorityValue::Int64(20));
            let high_id = fw.push_batch(&q, vec![high]).await.unwrap()[0];
            let mut low = item(2, 0, 100);
            low.priority = Some(PriorityValue::Int64(10));
            let low_id = fw.push_batch(&q, vec![low]).await.unwrap()[0];
            let first = fw.claim(&q, 1, 1000).await.unwrap();
            assert_eq!(
                first[0].item_id, low_id,
                "{memory}: later lower priority must win"
            );
            fw.complete(&q, [low_id]).await.unwrap();
            let second = fw.claim(&q, 1, 1000).await.unwrap();
            assert_eq!(second[0].item_id, high_id);
            fw.complete(&q, [high_id]).await.unwrap();
            clock.set(200);
            let due = fw.claim(&q, 1, 1000).await.unwrap();
            assert_eq!(due.len(), 1);
            assert_eq!(due[0].item_id, future_id);
        }
    })
    .await
    .expect("mixed-batch ordering timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transitions_cannot_both_advance_the_same_instance_fence() {
    tokio::time::timeout(Duration::from_secs(20), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let fw = open_store(root.path(), memory, TestClock::at(200)).unwrap();
            let q = create_queue(&fw, "fence-race").await.unwrap();
            fw.push_batch(&q, (0..8).map(|id| item(id, 0, 256)).collect())
                .await
                .unwrap();
            let claimed = fw.claim(&q, 8, 1000).await.unwrap();
            assert_eq!(claimed.len(), 8);
            let requests = claimed
                .iter()
                .enumerate()
                .map(|(id, c)| CommitRequest {
                    request_id: Some(RequestId::new(format!("race-{id}")).unwrap()),
                    entries: vec![CommitEntry {
                        claim_ref: claim_ref(c),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![SideRecord {
                            key: b"winner".to_vec(),
                            payload: Bytes::from(id.to_string()),
                        }],
                        lifecycle_items: vec![],
                        instance_fence: Some(InstanceFence {
                            instance_key: b"shared-instance".to_vec(),
                            expected: 0,
                            next: 1,
                        }),
                    }],
                })
                .collect::<Vec<_>>();
            let results =
                futures::future::join_all(requests.into_iter().map(|r| fw.commit(&q, r))).await;
            let mut winners = vec![];
            for (id, result) in results.into_iter().enumerate() {
                match result.unwrap().as_slice() {
                    [EntryOutcome::Committed { .. }] => winners.push(id),
                    [EntryOutcome::Rejected(EngineError::Conflict)] => {}
                    other => panic!("unexpected fence result: {other:?}"),
                }
            }
            assert_eq!(winners.len(), 1);
            assert_eq!(
                fw.side_record(&q, b"winner").await.unwrap(),
                Some(Bytes::from(winners[0].to_string()))
            );
            let metrics = fw.metrics(&q).await.unwrap();
            assert_eq!((metrics.complete, metrics.leased), (1, 7));
        }
    })
    .await
    .expect("concurrent fence test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_thousand_row_batches_are_admitted() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let root = tempfile::tempdir().unwrap();
        let fw = open_store(root.path(), false, TestClock::at(200)).unwrap();
        let q = create_queue(&fw, "thousand").await.unwrap();
        let ids = fw
            .push_batch(&q, (0..1000).map(|id| item(id, 0, 128)).collect())
            .await
            .unwrap();
        let claimed = fw.claim(&q, 1000, 1000).await.unwrap();
        assert_eq!(claimed.len(), 1000);
        fw.complete(&q, ids).await.unwrap();
        assert_eq!(fw.metrics(&q).await.unwrap().complete, 1000);
    })
    .await
    .expect("a legal batch must not receive endless backpressure");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_cycles_allow_expired_request_and_item_keys_to_be_reused() {
    tokio::time::timeout(Duration::from_secs(45), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let clock = TestClock::at(200);
            let fw = open_store(root.path(), memory, clock.clone()).unwrap();
            let mut d = definition("retention-cycles");
            d.request_id_retention_ms = 1000;
            d.client_item_key_retention_ms = 1000;
            let q = QueueKey::new(d.tenant_id.clone(), d.queue_id.clone());
            fw.create_queue(d).await.unwrap();
            let mut previous = vec![];
            for cycle in 0..8 {
                clock.set(200 + cycle * 120);
                let rows = (0..32).map(|id| item(id, 0, 8192)).collect::<Vec<_>>();
                let ids = fw
                    .push_batch_with_request_id(&q, RequestId::new("load").unwrap(), rows.clone())
                    .await
                    .unwrap()
                    .item_ids;
                assert_eq!(
                    fw.push_batch_with_request_id(&q, RequestId::new("load").unwrap(), rows)
                        .await
                        .unwrap()
                        .item_ids,
                    ids
                );
                assert!(ids.iter().all(|id| !previous.contains(id)));
                let request = BatchUpdateRequest {
                    request_id: RequestId::new("enrich").unwrap(),
                    updates: ids
                        .iter()
                        .map(|id| {
                            update(
                                *id,
                                BatchUpdateValue::Replace(Some(Bytes::from(vec![
                                    cycle as u8;
                                    8192
                                ]))),
                            )
                        })
                        .collect(),
                };
                let response = fw.batch_update(&q, request.clone()).await.unwrap();
                assert_eq!(fw.batch_update(&q, request).await.unwrap(), response);
                assert!(
                    response
                        .results
                        .iter()
                        .all(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
                );
                let claimed = fw.claim(&q, 32, 1000).await.unwrap();
                assert_eq!(claimed.len(), 32);
                assert!(
                    claimed
                        .iter()
                        .all(|row| row.payload.as_deref()
                            == Some(vec![cycle as u8; 8192].as_slice()))
                );
                fw.complete(&q, ids.clone()).await.unwrap();
                clock.set(270 + cycle * 120);
                assert_eq!(fw.purge(&q, ids.clone(), false).await.unwrap(), 32);
                let m = fw.metrics(&q).await.unwrap();
                assert_eq!((m.pending, m.leased, m.complete, m.failed), (0, 0, 0, 0));
                previous = ids;
            }
        }
    })
    .await
    .expect("retention cycles timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ordinary_claims_reserve_disjoint_rows_before_projection() {
    tokio::time::timeout(Duration::from_secs(30), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let fw = open_store(root.path(), memory, TestClock::at(200)).unwrap();
            let q = create_queue(&fw, "concurrent-claims").await.unwrap();
            let inserted = fw
                .push_batch(&q, (0..240).map(|id| item(id, 0, 1024)).collect())
                .await
                .unwrap();
            let batches =
                futures::future::try_join_all((0..8).map(|_| fw.claim(&q, 30, 3_600_000)))
                    .await
                    .unwrap();
            let claimed: Vec<_> = batches
                .into_iter()
                .flatten()
                .map(|row| row.item_id)
                .collect();
            let unique: std::collections::HashSet<_> = claimed.iter().copied().collect();
            assert_eq!(claimed.len(), 240);
            assert_eq!(
                unique.len(),
                240,
                "concurrent claims returned duplicate rows"
            );
            assert!(inserted.iter().all(|id| unique.contains(id)));
            fw.complete(&q, claimed).await.unwrap();
            let metrics = fw.metrics(&q).await.unwrap();
            assert_eq!(metrics.complete, 240);
            assert_eq!(metrics.pending + metrics.leased, 0);
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_retry_uses_the_acknowledged_claim_attempt_and_version() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let fw = open_store(root.path(), false, TestClock::at(200)).unwrap();
        let q = create_queue(&fw, "immediate-retry").await.unwrap();
        fw.push_batch(&q, (0..240).map(|id| item(id, 2, 1024)).collect())
            .await
            .unwrap();
        for attempt in 1..=5 {
            assert_eq!(fw.metrics(&q).await.unwrap().pending, 240);
            let rows = fw.claim(&q, 240, 3_600_000).await.unwrap();
            assert_eq!(rows.len(), 240);
            assert!(rows.iter().all(|row| row.attempt_count == attempt));
            // No coverage read between claim acknowledgement and retry.
            fw.retry(&q, rows.iter().map(|row| row.item_id), Some(ts(100)))
                .await
                .unwrap();
        }
        let metrics = fw.metrics(&q).await.unwrap();
        assert_eq!(metrics.failed, 240);
        assert_eq!(metrics.pending + metrics.leased, 0);
    })
    .await
    .unwrap();
}

fn addressed_request(name: &str, entries: Vec<AddressedMutation>) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new(name).unwrap(),
        evaluated_at: ts(200),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed { entries },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leased_rows_are_atomically_enriched_and_mixed_outcomes_replay() {
    tokio::time::timeout(Duration::from_secs(45), async {
        // The memory store is retired (3a8d8270); only the public s3 × turso store remains.
        {
            let memory = false;
            let root = tempfile::tempdir().unwrap();
            let fw = open_store(root.path(), memory, TestClock::at(200)).unwrap();
            let q = create_queue(&fw, "atomic-enrichment").await.unwrap();
            let ids = fw
                .push_batch(&q, (0..3).map(|id| item(id, 0, 32)).collect())
                .await
                .unwrap();
            let claimed = fw.claim(&q, 3, 1000).await.unwrap();
            assert_eq!(claimed.len(), 3);
            let entries: Vec<_> = claimed
                .iter()
                .enumerate()
                .map(|(i, row)| AddressedMutation {
                    item_id: row.item_id,
                    expected_item_version: Some(row.item_version),
                    predicates: vec![],
                    lease_guard: LeaseGuard::Match(row.lease_token.clone().unwrap()),
                    patch: ItemPatch {
                        lifecycle: [
                            LifecyclePatch::SetPending,
                            LifecyclePatch::SetComplete,
                            LifecyclePatch::SetFailed,
                        ][i],
                        payload: BatchUpdateValue::Replace(Some(Bytes::from_static(b"enriched"))),
                        metadata: BatchUpdateValue::Replace(metadata(1, i)),
                        ..Default::default()
                    },
                })
                .collect();
            let request = addressed_request("atomic-1", entries.clone());
            let result = fw.mutate_items(&q, request.clone()).await.unwrap();
            assert_eq!(result.summary.changed, 3, "{result:?}");
            assert!(result.position.is_some());
            assert_eq!(fw.mutate_items(&q, request.clone()).await.unwrap(), result);
            let mut conflicting = request;
            conflicting.returning = ItemMutationReturning::Identity;
            assert_eq!(
                fw.mutate_items(&q, conflicting).await.unwrap_err(),
                EngineError::RequestIdConflict
            );
            let next = fw.claim(&q, 3, 1000).await.unwrap();
            assert_eq!(next.len(), 1);
            assert_eq!(next[0].item_id, ids[0]);
            assert_eq!(next[0].payload.as_deref(), Some(b"enriched".as_slice()));
            let mut stale = entries[0].clone();
            stale.expected_item_version = None;
            let rejected = fw
                .mutate_items(&q, addressed_request("stale-token", vec![stale]))
                .await
                .unwrap();
            assert!(matches!(
                rejected.results[0].outcome,
                ItemMutationOutcome::StaleLease
            ));
            let m = fw.metrics(&q).await.unwrap();
            assert_eq!((m.pending, m.leased, m.complete, m.failed), (0, 1, 1, 1));
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_addressed_updates_have_one_version_winner() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let root = tempfile::tempdir().unwrap();
        let fw = std::sync::Arc::new(open_store(root.path(), false, TestClock::at(200)).unwrap());
        let q = create_queue(&fw, "atomic-cas").await.unwrap();
        fw.push_batch(&q, vec![item(0, 0, 32)]).await.unwrap();
        let row = fw.claim(&q, 1, 1000).await.unwrap().remove(0);
        let entry = AddressedMutation {
            item_id: row.item_id,
            expected_item_version: Some(row.item_version),
            predicates: vec![],
            lease_guard: LeaseGuard::Match(row.lease_token.unwrap()),
            patch: ItemPatch {
                lifecycle: LifecyclePatch::SetPending,
                ..Default::default()
            },
        };
        let (a, b) = tokio::join!(
            fw.mutate_items(&q, addressed_request("writer-a", vec![entry.clone()])),
            fw.mutate_items(&q, addressed_request("writer-b", vec![entry]))
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.summary.changed + b.summary.changed, 1);
        assert_eq!(a.summary.rejected + b.summary.rejected, 1);
        assert_eq!(fw.claim(&q, 1, 1000).await.unwrap().len(), 1);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_full_delivery_batches_remain_disjoint() {
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    tokio::time::timeout(Duration::from_secs(45), async {
        let root = tempfile::tempdir().unwrap();
        let fw = open_store(root.path(), false, TestClock::at(200)).unwrap();
        let q = create_queue(&fw, "full-concurrent-batches").await.unwrap();
        let mut ids = std::collections::BTreeSet::new();
        for batch in 0..8 {
            ids.extend(
                fw.push_batch(
                    &q,
                    (batch * 1000..(batch + 1) * 1000)
                        .map(|id| item(id, 0, 1024))
                        .collect(),
                )
                .await
                .unwrap(),
            );
        }
        // Async pushes acknowledge the log before projection coverage. Delivery
        // retries transient backpressure within the same overall test deadline.
        let claimed = futures::future::try_join_all(
            (0..8).map(|_| retry(deadline, || fw.claim(&q, 1000, 1000))),
        )
        .await
        .unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for rows in claimed {
            assert_eq!(rows.len(), 1000);
            for row in rows {
                assert!(seen.insert(row.item_id), "duplicate lease");
            }
        }
        assert_eq!(ids, seen);
        assert_eq!(fw.metrics(&q).await.unwrap().leased, 8000);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_mutation_caller_does_not_strand_batch_peers() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let root = tempfile::tempdir().unwrap();
        let fw = std::sync::Arc::new(open_store(root.path(), false, TestClock::at(200)).unwrap());
        let q = create_queue(&fw, "cancelled-mutation").await.unwrap();
        for batch in 0..2 {
            fw.push_batch(
                &q,
                (batch * 1000..(batch + 1) * 1000)
                    .map(|id| item(id, 0, 1024))
                    .collect(),
            )
            .await
            .unwrap();
        }
        let mut requests = Vec::new();
        for batch in 0..2 {
            let rows = fw.claim(&q, 1000, 1000).await.unwrap();
            assert_eq!(rows.len(), 1000);
            requests.push(addressed_request(
                &format!("cancel-batch-{batch}"),
                rows.into_iter()
                    .map(|row| AddressedMutation {
                        item_id: row.item_id,
                        expected_item_version: Some(row.item_version),
                        predicates: vec![],
                        lease_guard: LeaseGuard::Match(row.lease_token.unwrap()),
                        patch: ItemPatch {
                            lifecycle: LifecyclePatch::SetPending,
                            payload: BatchUpdateValue::Replace(Some(Bytes::from_static(
                                b"committed",
                            ))),
                            ..Default::default()
                        },
                    })
                    .collect(),
            ));
        }
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let mut calls = Vec::new();
        for request in &requests {
            let (fw, q, barrier, request) =
                (fw.clone(), q.clone(), barrier.clone(), request.clone());
            calls.push(tokio::spawn(async move {
                barrier.wait().await;
                fw.mutate_items(&q, request).await
            }));
        }
        barrier.wait().await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        calls.remove(0).abort();
        let peer = calls.remove(0).await.unwrap().unwrap();
        assert_eq!(peer.summary.changed, 1000);
        // The aborted caller may have been queued or already accepted. Either
        // way, retry must return one exact successful result, never a second CAS.
        let recovered = fw.mutate_items(&q, requests[0].clone()).await.unwrap();
        assert_eq!(recovered.summary.changed, 1000);
        assert_eq!(
            fw.mutate_items(&q, requests[1].clone()).await.unwrap(),
            peer
        );
        assert_eq!(fw.metrics(&q).await.unwrap().pending, 2000);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..2 {
            let rows = fw.claim(&q, 1000, 1000).await.unwrap();
            assert_eq!(rows.len(), 1000);
            for row in rows {
                assert!(seen.insert(row.item_id));
                assert_eq!(row.payload.as_deref(), Some(b"committed".as_slice()));
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_anonymous_pushes_return_their_own_rows() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = tempfile::tempdir().unwrap();
        let fw = std::sync::Arc::new(open_store(root.path(), false, TestClock::at(200)).unwrap());
        let q = create_queue(&fw, "anonymous-push").await.unwrap();
        let mut workers = Vec::new();
        for worker in 0..8 {
            let fw = fw.clone();
            let q = q.clone();
            workers.push(tokio::spawn(async move {
                let mut row = item(worker, 0, 32);
                row.client_item_key = None;
                let ids = fw.push_batch(&q, vec![row]).await.unwrap();
                assert_eq!(ids.len(), 1);
                (ids[0], body(worker, 0, 32))
            }));
        }
        let mut expected = std::collections::HashMap::new();
        for worker in workers {
            let (id, payload) = worker.await.unwrap();
            assert!(
                expected.insert(id, payload).is_none(),
                "two callers received the same row"
            );
        }
        let mut seen = 0;
        while seen < 8 {
            for claimed in fw.claim(&q, 8, 1000).await.unwrap() {
                assert_eq!(claimed.payload, expected.remove(&claimed.item_id));
                seen += 1;
            }
        }
        assert!(expected.is_empty());
    })
    .await
    .expect("anonymous pushes must retain distinct responses");
}
