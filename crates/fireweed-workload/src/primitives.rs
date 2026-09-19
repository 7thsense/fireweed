//! Component capacity through public APIs. This complements the autonomous
//! workflow test; its update addresses deliberately come from the load response.
use crate::*;

fn component_body(id: usize, stage: usize, cfg: &Config) -> Bytes {
    if !cfg.primitive_varied_payload {
        return body(id, stage, cfg.payload_bytes);
    }
    let original = crate::campaign::initial_body(id, 0, cfg.payload_bytes);
    if stage == 0 {
        return original;
    }
    let mut document: serde_json::Value =
        serde_json::from_slice(&original).expect("generated JSON body");
    document["primitive_enrichment"] = serde_json::json!(stage);
    Bytes::from(serde_json::to_vec(&document).expect("generated enriched body"))
}

pub async fn run(cfg: Config, root: &Path) -> Result<serde_json::Value> {
    tokio::time::timeout(cfg.deadline, run_inner(cfg, root))
        .await
        .map_err(|_| "primitive capacity run timed out")?
}

async fn run_inner(cfg: Config, root: &Path) -> Result<serde_json::Value> {
    if cfg.items == 0 || cfg.shards == 0 || !(1..=1000).contains(&cfg.batch) {
        return Err("items/shards must be positive and batch in 1..=1000".into());
    }
    let started = Instant::now();
    let deadline = started + cfg.deadline;
    let mut tasks = tokio::task::JoinSet::new();
    for shard in 0..cfg.shards {
        let cfg = cfg.clone();
        let root = root.join(format!("shard-{shard}"));
        tasks.spawn(async move {
            let clock = TestClock::at(200);
            let projection_root = cfg.projection_root.as_ref().map(|path| path.join(format!("shard-{shard}"))).unwrap_or_else(|| root.clone());
            let fw = open_store_with_projection_root(&root, cfg.memory, clock.clone(), &projection_root)?;
            let q = create_queue(&fw, "primitives").await?;
            let recipients: Vec<_> = (shard..cfg.items).step_by(cfg.shards).collect();
            let mut ids = vec![]; let mut phases = vec![];
            let mut initial_payload_bytes = 0u64;
            let mut payload_replacement_bytes = 0u64;
            let phase = Instant::now();
            for chunk in recipients.chunks(cfg.batch) {
                let rows = chunk.iter().map(|id| {
                    item_with_payload(*id, 0, component_body(*id, 0, &cfg))
                }).collect::<Vec<_>>();
                initial_payload_bytes += rows.iter().map(|row| row.payload.as_ref().map_or(0, |body| body.len()) as u64).sum::<u64>();
                ids.extend(retry(deadline, || fw.push_batch(&q, rows.clone())).await?);
            }
            // Include projection catch-up in phase timing even when metrics can
            // derive exact counts from a durable, not-yet-applied membership tail.
            if !cfg.memory {
                let resident = retry(deadline, || fw.retained_items(&q,None,1)).await?;
                if resident.is_empty() != ids.is_empty() { return Err("load projection presence mismatch".into()); }
            }
            let m = retry(deadline, || fw.metrics(&q)).await?;
            if m.pending != recipients.len() as u64 { return Err("load count mismatch".into()); }
            phases.push(phase_report("insert", recipients.len(), phase, started));
            for stage in 1..=2 {
                let phase = Instant::now();
                // Deliberately vary every replacement. Reverse the scheduling pass
                // to avoid granting the implementation an insertion-order shortcut.
                let order: Vec<_> = if stage == 1 { (0..ids.len()).collect() } else { (0..ids.len()).rev().collect() };
                for (batch, chunk) in order.chunks(cfg.batch).enumerate() {
                    let updates: Vec<BatchUpdateEntry> = chunk.iter().map(|offset| {
                        let recipient = recipients[*offset];
                        BatchUpdateEntry {
                            item_ref: if stage == 1 { BatchUpdateItemRef::ClientItemKey(ClientItemKey::new(format!("r-{recipient:09}-s0")).unwrap()) }
                                else { BatchUpdateItemRef::ItemId(ids[*offset]) },
                            expected_item_version: None,
                            payload: if stage == 1 { BatchUpdateValue::Replace(Some(component_body(recipient, 2, &cfg))) } else { BatchUpdateValue::Keep },
                            metadata: BatchUpdateValue::Replace(metadata(stage, recipient)),
                            priority: if stage == 2 { BatchUpdateValue::Replace(PriorityValue::Int64(due(recipient))) } else { BatchUpdateValue::Keep },
                            not_before: if stage == 2 { BatchUpdateValue::Replace(Some(ts(due(recipient)))) } else { BatchUpdateValue::Keep },
                            fields: BatchUpdateValue::Keep, gate_keys: BatchUpdateValue::Keep,
                        }
                    }).collect();
                    if stage == 1 {
                        payload_replacement_bytes += updates.iter().map(|entry: &BatchUpdateEntry| match &entry.payload {
                            BatchUpdateValue::Replace(Some(body)) => body.len() as u64,
                            _ => 0,
                        }).sum::<u64>();
                    }
                    let request = BatchUpdateRequest { request_id: RequestId::new(format!("stage-{stage}-{batch}")).unwrap(), updates };
                    let response = retry(deadline, || fw.batch_update(&q, request.clone())).await?;
                    if response.results.len() != chunk.len() || response.results.iter().any(|r| !matches!(r, BatchUpdateOutcome::Updated { .. })) {
                        return Err(format!("update rejected: {response:?}").into());
                    }
                }
                retry(deadline, || fw.metrics(&q)).await?;
                phases.push(phase_report(if stage == 1 { "enrich_by_key" } else { "schedule_by_id" }, recipients.len(), phase, started));
            }
            let phase = Instant::now();
            let mut consumed = std::collections::BTreeSet::new(); let mut last_priority = i64::MIN;
            while consumed.len() < recipients.len() {
                let rows = retry(deadline, || fw.claim(&q, cfg.batch, 3_600_000)).await?;
                if rows.is_empty() { tokio::time::sleep(Duration::from_millis(1)).await; continue; }
                for row in &rows {
                    let id = recipient(row);
                    if !consumed.insert(id) { return Err("duplicate delivery".into()); }
                    if row.payload != Some(component_body(id, 2, &cfg)) { return Err(format!("payload mismatch for {id}").into()); }
                    if due(id) < last_priority { return Err(format!("delivery priority inversion at recipient {id}: due={}, returned={:?}, preceding={last_priority}, delivered={}, batch={:?}", due(id), row.priority, consumed.len(), rows.iter().map(|r| (recipient(r), r.priority.clone())).collect::<Vec<_>>()).into()); }
                    last_priority = due(id);
                }
                retry(deadline, || fw.complete(&q, rows.iter().map(|r| r.item_id))).await?;
            }
            let m = retry(deadline, || fw.metrics(&q)).await?;
            if m.complete != recipients.len() as u64 || m.pending != 0 || m.leased != 0 { return Err("completion count mismatch".into()); }
            phases.push(phase_report("claim_and_complete", recipients.len(), phase, started));
            clock.set(600);
            let phase = Instant::now();
            for chunk in ids.chunks(cfg.batch) {
                let removed = retry(deadline, || fw.purge(&q, chunk.iter().copied(), false)).await?;
                if removed != chunk.len() as u64 { return Err("purge count mismatch".into()); }
            }
            // The memory backend applies synchronously and has no retained-row API.
            if !cfg.memory && !retry(deadline, || fw.retained_items(&q,None,1)).await?.is_empty() {
                return Err("purge left projected rows".into());
            }
            let m = retry(deadline, || fw.metrics(&q)).await?;
            if m.complete + m.pending + m.leased + m.failed != 0 { return Err("retention left queue rows".into()); }
            phases.push(phase_report("purge", recipients.len(), phase, started));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(serde_json::json!({"shard": shard, "items": recipients.len(), "phases": phases, "initial_payload_bytes": initial_payload_bytes, "payload_replacement_bytes": payload_replacement_bytes}))
        });
    }
    let mut reports = vec![];
    while let Some(result) = tasks.join_next().await {
        reports.push(result??);
    }
    reports.sort_by_key(|r| r["shard"].as_u64());
    let mut aggregate_phases = vec![];
    for index in 0..5 {
        let first = reports
            .iter()
            .map(|r| r["phases"][index]["start_s"].as_f64().unwrap())
            .fold(f64::INFINITY, f64::min);
        let last = reports
            .iter()
            .map(|r| r["phases"][index]["end_s"].as_f64().unwrap())
            .fold(0.0, f64::max);
        aggregate_phases.push(serde_json::json!({"phase": reports[0]["phases"][index]["phase"],
            "records": cfg.items, "wall_window_s": last - first, "records_per_s": cfg.items as f64 / (last - first)}));
    }
    Ok(
        serde_json::json!({"schema": "primitive-capacity/v1", "items": cfg.items, "physical_shards": cfg.shards,
        "batch": cfg.batch, "payload_bytes": cfg.payload_bytes, "phase_concurrency_per_store": 1,
        "payload_workload": if cfg.primitive_varied_payload { "campaign_varied" } else { "repeated_padding" },
        "initial_payload_bytes": reports.iter().map(|r| r["initial_payload_bytes"].as_u64().unwrap()).sum::<u64>(),
        "payload_replacement_bytes": reports.iter().map(|r| r["payload_replacement_bytes"].as_u64().unwrap()).sum::<u64>(),
        "cell": if cfg.memory { "memory--memory" } else { "s3--turso" },
        "settled_wall_s": started.elapsed().as_secs_f64(), "aggregate_phases": aggregate_phases, "shards": reports}),
    )
}

fn phase_report(name: &str, records: usize, phase: Instant, started: Instant) -> serde_json::Value {
    let end = Instant::now();
    let wall = end.duration_since(phase);
    let report = serde_json::json!({"phase": name, "records": records, "settled_wall_s": wall.as_secs_f64(),
        "start_s": phase.duration_since(started).as_secs_f64(), "end_s": end.duration_since(started).as_secs_f64(),
        "records_per_s": records as f64 / wall.as_secs_f64()});
    eprintln!("phase_complete {report}");
    report
}

#[cfg(test)]
mod payload_tests {
    use super::*;

    #[test]
    fn varied_replacement_preserves_identity_and_varied_content() {
        let cfg = Config {
            primitive_varied_payload: true,
            ..Config::default()
        };
        let mut previous = None;
        for id in [0, 17, 999_999] {
            let initial: serde_json::Value =
                serde_json::from_slice(&component_body(id, 0, &cfg)).unwrap();
            assert_eq!(initial["id"], id);
            assert_eq!(initial["campaign"], 0);
            let padding = initial["padding"].as_str().unwrap();
            assert_eq!(padding.len(), 896);
            assert!(
                padding
                    .bytes()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    >= 20
            );
            assert_ne!(previous.as_deref(), Some(padding));
            previous = Some(padding.to_owned());
            let mut enriched: serde_json::Value =
                serde_json::from_slice(&component_body(id, 2, &cfg)).unwrap();
            assert_eq!(
                enriched
                    .as_object_mut()
                    .unwrap()
                    .remove("primitive_enrichment"),
                Some(serde_json::json!(2))
            );
            assert_eq!(enriched, initial);
        }
        let legacy = Config::default();
        assert_eq!(
            component_body(17, 2, &legacy),
            body(17, 2, legacy.payload_bytes)
        );
    }
}
