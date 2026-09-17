use fireweed::*;
use fireweed_workload::{
    Config, TestClock, campaign, create_queue, definition, item, open_store, ts,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_pages_include_terminal_metadata_and_isolate_queues() {
    let root = tempfile::tempdir().unwrap();
    let clock = TestClock::at(200);
    let fw = open_store(root.path(), false, clock.clone()).unwrap();
    let q = create_queue(&fw, "retained").await.unwrap();
    let other = create_queue(&fw, "other").await.unwrap();
    let ids = fw
        .push_batch(&q, (0..7).map(|i| item(i, 0, 256)).collect())
        .await
        .unwrap();
    fw.push_batch(&other, vec![item(100, 0, 256)])
        .await
        .unwrap();
    let claims = fw.claim(&q, 3, 10_000).await.unwrap();
    fw.complete(&q, [claims[0].item_id]).await.unwrap();
    fw.fail(&q, [claims[1].item_id]).await.unwrap();
    fw.metrics(&q).await.unwrap();
    assert!(fw.retained_items(&q, None, 0).await.is_err());
    assert!(fw.retained_items(&q, None, 1001).await.is_err());
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = fw.retained_items(&q, cursor, 2).await.unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 2);
        for row in &page {
            if let Some(previous) = cursor {
                assert!(row.item_id > previous);
            }
            assert!(row.payload.is_some());
            assert!(row.metadata.get("recipient").is_some());
            seen.push(row.clone());
        }
        cursor = page.last().map(|r| r.item_id);
    }
    assert_eq!(seen.len(), 7);
    assert_eq!(
        seen.iter()
            .filter(|r| r.lifecycle_state == ItemState::Complete)
            .count(),
        1
    );
    assert_eq!(
        seen.iter()
            .filter(|r| r.lifecycle_state == ItemState::Failed)
            .count(),
        1
    );
    assert_eq!(
        seen.iter()
            .filter(|r| r.lifecycle_state == ItemState::Leased)
            .count(),
        1
    );
    assert!(
        fw.live_item(
            &q,
            seen.iter()
                .find(|r| r.lifecycle_state == ItemState::Complete)
                .unwrap()
                .client_item_key
                .clone()
        )
        .await
        .unwrap()
        .is_none()
    );
    fw.release(&q, [claims[2].item_id]).await.unwrap();
    let active = fw.claim(&q, 7, 10_000).await.unwrap();
    fw.complete(&q, active.iter().map(|r| r.item_id))
        .await
        .unwrap();
    clock.set(1000);
    assert_eq!(fw.purge(&q, ids, false).await.unwrap(), 7);
    fw.metrics(&q).await.unwrap();
    assert!(fw.retained_items(&q, None, 1000).await.unwrap().is_empty());
    assert_eq!(
        fw.retained_items(&other, None, 1000).await.unwrap().len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn campaign_windows_reporting_and_discovered_retention() {
    for (campaign_metadata_only, campaign_timestamp_priority) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let root = tempfile::tempdir().unwrap();
        let report = campaign::run(
            Config {
                apply_debt_bytes: Some(96 * 1024),
                items: 448,
                campaign_metadata_only,
                campaign_timestamp_priority,
                shards: 2,
                workers: 2,
                load_workers: 2,
                batch: 37,
                cycles: 2,
                recycle: true,
                deadline: std::time::Duration::from_secs(120),
                ..Default::default()
            },
            root.path(),
        )
        .await
        .unwrap();
        assert_eq!(report["apply_debt_max_bytes"], 96 * 1024);
        assert_eq!(
            report["priority_workload"],
            if campaign_timestamp_priority {
                "availability_timestamp"
            } else {
                "mixed_sequence_stress"
            }
        );

        let mut verified = 0;
        for shard in report["shards"].as_array().unwrap() {
            for campaign in shard["campaigns"].as_array().unwrap() {
                for cycle in campaign["cycles"].as_array().unwrap() {
                    let n = cycle["items"].as_u64().unwrap();
                    assert_eq!(cycle["verified"], n);
                    assert_eq!(cycle["purged"], n);
                    assert_eq!(
                        cycle["payload_replacements"],
                        if campaign_metadata_only { 0 } else { 2 * n }
                    );
                    assert!(cycle["initial_payload_bytes"].as_u64().unwrap() >= n * 896);
                    let progress_reads = cycle["progress_reads"].as_u64().unwrap();
                    assert!(progress_reads > 0);
                    let phases = cycle["progress_by_phase"].as_object().unwrap();
                    assert_eq!(
                        phases
                            .values()
                            .map(|v| v["reads"].as_u64().unwrap())
                            .sum::<u64>(),
                        progress_reads
                    );
                    for (phase, values) in phases {
                        assert!(phase.split("->").all(|p| {
                            ["load", "prepare", "delivery", "verify", "purge"].contains(&p)
                        }));
                        assert!(
                            values["attempts"].as_u64().unwrap()
                                >= values["reads"].as_u64().unwrap()
                        );
                        assert!(
                            values["max_s"].as_f64().unwrap() >= values["p95_s"].as_f64().unwrap()
                        );
                    }
                    verified += n;
                }
            }
        }
        assert_eq!(verified, 896);
    }
}

#[test]
fn campaign_acknowledged_child() {
    let Some(root) = std::env::var_os("FIREWEED_CAMPAIGN_CHILD") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        campaign::run(
            Config {
                items: 112,
                campaign_timestamp_priority: std::env::var("FIREWEED_CAMPAIGN_TIMESTAMP")
                    .as_deref()
                    == Ok("1"),
                campaign_metadata_only: std::env::var("FIREWEED_CAMPAIGN_METADATA").as_deref()
                    == Ok("1"),
                shards: 1,
                workers: 2,
                batch: 37,
                cycles: 1,
                deadline: std::time::Duration::from_secs(90),
                ..Default::default()
            },
            std::path::Path::new(&root),
        )
        .await
        .unwrap();
        std::process::exit(0);
    });
}
fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_campaign_reports_rebuild_from_log_only() {
    for (campaign_metadata_only, campaign_timestamp_priority) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let original = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "campaign_acknowledged_child", "--nocapture"])
            .env("FIREWEED_CAMPAIGN_CHILD", original.path())
            .env(
                "FIREWEED_CAMPAIGN_METADATA",
                if campaign_metadata_only { "1" } else { "0" },
            )
            .env(
                "FIREWEED_CAMPAIGN_TIMESTAMP",
                if campaign_timestamp_priority {
                    "1"
                } else {
                    "0"
                },
            )
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "{}",
            String::from_utf8_lossy(&status.stderr)
        );
        let rebuilt = tempfile::tempdir().unwrap();
        copy_tree(
            &original.path().join("shard-0/log"),
            &rebuilt.path().join("log"),
        );
        let fw = open_store(rebuilt.path(), false, TestClock::at(2000)).unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for campaign in 0..2 {
            let mut d = definition(&format!("campaign-{campaign}"));
            if campaign_timestamp_priority {
                d.priority_model = PriorityModel::timestamp_ascending();
            }
            let q = QueueKey::new(d.tenant_id.clone(), d.queue_id.clone());
            fw.create_queue(d).await.unwrap();
            let m = fw.metrics(&q).await.unwrap();
            assert_eq!(m.complete + m.failed, 56);
            let rows = fw.retained_items(&q, None, 1000).await.unwrap();
            assert_eq!(rows.len(), 56);
            for row in rows {
                let doc: serde_json::Value =
                    serde_json::from_slice(row.payload.as_ref().unwrap()).unwrap();
                let id = doc["id"].as_u64().unwrap();
                assert!(seen.insert(id));
                assert_eq!(doc["campaign"], campaign);
                let enriched = if campaign_metadata_only {
                    serde_json::to_value(&row.metadata).unwrap()
                } else {
                    doc.clone()
                };
                let first = 1000 + 60 * ((id / 7) % 4);
                assert_eq!(enriched["scheduled_at"], first);
                assert_eq!(
                    enriched["top_times"],
                    serde_json::json!([first + 600, first, first + 300])
                );
                assert_eq!(enriched["color"], ["red", "blue", "green"][id as usize % 3]);
                assert_eq!(enriched["score"], id % 101);
                if campaign_metadata_only {
                    assert_eq!(doc.as_object().unwrap().len(), 3);
                }
                assert_eq!(
                    row.lifecycle_state,
                    if id.is_multiple_of(31) {
                        ItemState::Failed
                    } else {
                        ItemState::Complete
                    }
                );
                assert_eq!(
                    row.metadata.get("outcome"),
                    Some(&MetadataValue::String(
                        if id.is_multiple_of(31) {
                            "failed"
                        } else {
                            "accepted"
                        }
                        .into()
                    ))
                );
                assert_eq!(
                    row.metadata.get("provider_id"),
                    Some(&MetadataValue::String(format!("provider-{id}")))
                );
                assert_eq!(row.attempt_count, 3 + u32::from(id.is_multiple_of(19)));
                assert_eq!(
                    row.priority,
                    Some(if campaign_timestamp_priority {
                        PriorityValue::Timestamp(ts(first as i64))
                    } else {
                        PriorityValue::Int64(first as i64)
                    })
                );
                assert_eq!(row.not_before, Some(ts(1000 + 60 * ((id / 7) % 4) as i64)));
            }
        }
        assert_eq!(seen.len(), 112);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn campaign_chunks_obey_distinct_handler_limits() {
    let root = tempfile::tempdir().unwrap();
    let report = campaign::run(
        Config {
            items: 2240,
            campaign_metadata_only: true,
            purge_batch: Some(1025),
            shards: 1,
            workers: 2,
            load_workers: 2,
            batch: 1000,
            cycles: 1,
            recycle: true,
            deadline: std::time::Duration::from_secs(120),
            ..Default::default()
        },
        root.path(),
    )
    .await
    .unwrap();
    for campaign in report["shards"][0]["campaigns"].as_array().unwrap() {
        let cycle = &campaign["cycles"][0];
        assert_eq!(cycle["handler_rows"][0], 1120);
        assert_eq!(cycle["handler_rows"][1], 1120);
        for (stage, limit) in [500, 200, 500].into_iter().enumerate() {
            assert!(cycle["max_handler_batch"][stage].as_u64().unwrap() <= limit);
        }
        assert!(cycle["handler_batches"][1].as_u64().unwrap() >= 6);
        assert_eq!(cycle["purge_batches"], 2);
        assert_eq!(cycle["max_purge_batch"], 1025);
        assert_eq!(cycle["claim_batches"], cycle["mutation_batches"]);
        assert!(cycle["max_claim_batch"].as_u64().unwrap() <= 1000);
        assert!(cycle["max_claim_batch"].as_u64().unwrap() > 500);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kept_payload_planning_preserves_no_change_and_before_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let fw = open_store(root.path(), false, TestClock::at(100)).unwrap();
    let q = create_queue(&fw, "kept-body").await.unwrap();
    let input = item(0, 0, 128 * 1024);
    let body = input.payload.clone().unwrap();
    let mut metadata = input.metadata.clone();
    metadata.insert("color", MetadataValue::String("red".into()));
    let id = fw.push_batch(&q, vec![input]).await.unwrap()[0];
    let request = |name: &str, patch: ItemPatch, returning| ItemMutationRequest {
        request_id: RequestId::new(name).unwrap(),
        evaluated_at: ts(100),
        dry_run: false,
        returning,
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed {
            entries: vec![AddressedMutation {
                item_id: id,
                expected_item_version: None,
                predicates: vec![],
                lease_guard: LeaseGuard::RejectActive,
                patch,
            }],
        },
    };
    let changed = fw
        .mutate_items(
            &q,
            request(
                "metadata",
                ItemPatch {
                    metadata: BatchUpdateValue::Replace(metadata),
                    ..Default::default()
                },
                ItemMutationReturning::Identity,
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        changed.results[0].outcome,
        ItemMutationOutcome::Updated { .. }
    ));
    assert_eq!(
        fw.retained_items(&q, None, 1).await.unwrap()[0]
            .payload
            .as_ref(),
        Some(&body)
    );
    let no_change = fw
        .mutate_items(
            &q,
            request(
                "keep",
                ItemPatch::default(),
                ItemMutationReturning::Identity,
            ),
        )
        .await
        .unwrap();
    assert_eq!(no_change.results[0].outcome, ItemMutationOutcome::NoChange);
    let equal = fw
        .mutate_items(
            &q,
            request(
                "equal",
                ItemPatch {
                    payload: BatchUpdateValue::Replace(Some(body.clone())),
                    ..Default::default()
                },
                ItemMutationReturning::Identity,
            ),
        )
        .await
        .unwrap();
    assert_eq!(equal.results[0].outcome, ItemMutationOutcome::NoChange);
    let snapshot = fw
        .mutate_items(
            &q,
            request(
                "snapshot",
                ItemPatch::default(),
                ItemMutationReturning::BeforeSnapshot,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        snapshot.results[0]
            .before
            .as_ref()
            .unwrap()
            .payload
            .as_ref(),
        Some(&body)
    );
    let removed = fw
        .mutate_items(
            &q,
            request(
                "remove",
                ItemPatch {
                    payload: BatchUpdateValue::Replace(None),
                    ..Default::default()
                },
                ItemMutationReturning::Identity,
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        removed.results[0].outcome,
        ItemMutationOutcome::Updated { .. }
    ));
    assert_eq!(
        fw.retained_items(&q, None, 1).await.unwrap()[0].payload,
        None
    );
}
