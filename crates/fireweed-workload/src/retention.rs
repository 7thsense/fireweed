//! Reuse one public store across retention cycles; sample process/storage size.
use crate::*;

pub async fn run(cfg: Config, root: &Path) -> Result<serde_json::Value> {
    tokio::time::timeout(cfg.deadline, run_inner(cfg, root))
        .await
        .map_err(|_| "retention workload timed out")?
}

async fn run_inner(cfg: Config, root: &Path) -> Result<serde_json::Value> {
    if cfg.items == 0 || cfg.cycles == 0 || cfg.shards != 1 || !(1..=1000).contains(&cfg.batch) {
        return Err(
            "retention requires items/cycles positive, one shard, batch in 1..=1000".into(),
        );
    }
    let clock = TestClock::at(200);
    let projection_root = cfg.projection_root.as_deref().unwrap_or(root);
    let fw = open_store_with_projection_root(root, cfg.memory, clock.clone(), projection_root)?;
    let mut d = definition("retention");
    d.request_id_retention_ms = 1000;
    let q = QueueKey::new(d.tenant_id.clone(), d.queue_id.clone());
    fw.create_queue(d).await?;
    let started = Instant::now();
    let deadline = started + cfg.deadline;
    let mut reports = vec![];
    for cycle in 0..cfg.cycles {
        clock.set(200 + cycle as u64 * 120);
        let phase = Instant::now();
        let mut ids = vec![];
        for first in (0..cfg.items).step_by(cfg.batch) {
            let rows = (first..(first + cfg.batch).min(cfg.items))
                .map(|id| item(id, 0, cfg.payload_bytes))
                .collect::<Vec<_>>();
            let request_id = RequestId::new(format!("load-{first}")).unwrap();
            ids.extend(
                retry(deadline, || {
                    fw.push_batch_with_request_id(&q, request_id.clone(), rows.clone())
                })
                .await?
                .item_ids,
            );
        }
        for (batch, chunk) in ids.chunks(cfg.batch).enumerate() {
            let request = BatchUpdateRequest {
                request_id: RequestId::new(format!("enrich-{batch}")).unwrap(),
                updates: chunk
                    .iter()
                    .enumerate()
                    .map(|(offset, id)| BatchUpdateEntry {
                        item_ref: BatchUpdateItemRef::ItemId(*id),
                        expected_item_version: None,
                        payload: BatchUpdateValue::Replace(Some(body(
                            batch * cfg.batch + offset,
                            1,
                            cfg.payload_bytes,
                        ))),
                        metadata: BatchUpdateValue::Keep,
                        priority: BatchUpdateValue::Keep,
                        not_before: BatchUpdateValue::Keep,
                        fields: BatchUpdateValue::Keep,
                        gate_keys: BatchUpdateValue::Keep,
                    })
                    .collect(),
            };
            let response = retry(deadline, || fw.batch_update(&q, request.clone())).await?;
            if response.results.len() != chunk.len()
                || response
                    .results
                    .iter()
                    .any(|r| !matches!(r, BatchUpdateOutcome::Updated { .. }))
            {
                return Err(format!("cycle {cycle} enrichment failed: {response:?}").into());
            }
        }
        let mut consumed = std::collections::BTreeSet::new();
        while consumed.len() < cfg.items {
            let rows = retry(deadline, || fw.claim(&q, cfg.batch, 60_000)).await?;
            if rows.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            for row in &rows {
                let id = recipient(row);
                if !consumed.insert(id) || row.payload != Some(body(id, 1, cfg.payload_bytes)) {
                    return Err(format!("cycle {cycle} duplicate or corrupt item {id}").into());
                }
            }
            retry(deadline, || fw.complete(&q, rows.iter().map(|r| r.item_id))).await?;
        }
        clock.set(270 + cycle as u64 * 120);
        for chunk in ids.chunks(cfg.batch) {
            if retry(deadline, || fw.purge(&q, chunk.iter().copied(), false)).await?
                != chunk.len() as u64
            {
                return Err("retention purge count mismatch".into());
            }
        }
        let m = fw.metrics(&q).await?;
        if m.pending + m.leased + m.complete + m.failed != 0 {
            return Err("cycle left queue rows".into());
        }
        drop(ids);
        drop(consumed);
        let rss_kib = process_rss_kib();
        let projection_bytes = std::fs::metadata(projection_root.join("projection.db"))
            .ok()
            .map(|m| m.len());
        let wall = phase.elapsed().as_secs_f64();
        let report = serde_json::json!({"cycle": cycle, "items": cfg.items, "wall_s": wall, "lifecycles_per_s": cfg.items as f64 / wall, "rss_kib": rss_kib, "projection_bytes": projection_bytes});
        eprintln!("cycle_complete {report}");
        reports.push(report);
    }
    Ok(
        serde_json::json!({"schema": "retention-capacity/v1", "items_per_cycle": cfg.items, "batch": cfg.batch, "cycles": reports, "settled_wall_s": started.elapsed().as_secs_f64(), "cell": if cfg.memory { "memory--memory" } else { "s3--turso" }}),
    )
}
