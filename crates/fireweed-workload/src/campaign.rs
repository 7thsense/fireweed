//! Campaign-shaped acceptance through the public API. No projection or log internals.
use crate::*;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicUsize};

const WINDOWS: usize = 4;
const CAMPAIGNS: usize = 2;
const ENRICH_LIMIT: usize = 500;
const SCHEDULE_LIMIT: usize = 200;
const DELIVERY_LIMIT: usize = 500;

const PROGRESS_PHASES: [&str; 5] = ["load", "prepare", "delivery", "verify", "purge"];

fn progress_by_phase(samples: Vec<(usize, usize, f64, usize)>) -> Value {
    let mut phases = std::collections::BTreeMap::<String, Vec<(f64, usize)>>::new();
    for (from, to, latency, attempts) in samples {
        let label = if from == to {
            PROGRESS_PHASES[from].to_string()
        } else {
            format!("{}->{}", PROGRESS_PHASES[from], PROGRESS_PHASES[to])
        };
        phases.entry(label).or_default().push((latency, attempts));
    }
    let mut report = serde_json::Map::new();
    for (phase, mut reads) in phases {
        reads.sort_by(|a, b| a.0.total_cmp(&b.0));
        report.insert(
            phase,
            json!({
                "reads": reads.len(), "attempts": reads.iter().map(|r| r.1).sum::<usize>(),
                "max_attempts": reads.iter().map(|r| r.1).max().unwrap(),
                "total_s": reads.iter().map(|r| r.0).sum::<f64>(),
                "p95_s": reads[(reads.len()-1)*95/100].0, "max_s": reads.last().unwrap().0,
            }),
        );
    }
    Value::Object(report)
}

// Snorri uses availability timestamps for both ordering and eligibility.
// Unscheduled work sorts first, with a monotonic nanosecond FIFO tie-breaker.
// The original ordinal/seconds mixture remains an explicitly labeled stress case.
fn fifo_priority(id: usize, cfg: &Config) -> PriorityValue {
    if cfg.campaign_timestamp_priority {
        PriorityValue::Timestamp(
            UtcTimestamp::new(1 + (id / 1_000_000_000) as i64, (id % 1_000_000_000) as u32)
                .unwrap(),
        )
    } else {
        PriorityValue::Int64(id as i64)
    }
}
fn scheduled_priority(seconds: i64, cfg: &Config) -> PriorityValue {
    if cfg.campaign_timestamp_priority {
        PriorityValue::Timestamp(ts(seconds))
    } else {
        PriorityValue::Int64(seconds)
    }
}

async fn create_campaign_queue(fw: &Fireweed, name: &str, cfg: &Config) -> Result<QueueKey> {
    let mut d = definition(name);
    if cfg.campaign_timestamp_priority {
        d.priority_model = PriorityModel::timestamp_ascending();
    }
    let q = QueueKey::new(d.tenant_id.clone(), d.queue_id.clone());
    fw.create_queue(d).await?;
    Ok(q)
}

fn key(id: usize) -> ClientItemKey {
    ClientItemKey::new(format!("recipient-{id:012}")).unwrap()
}
fn meta(stage: usize, id: usize, campaign: usize) -> Metadata {
    let mut m = metadata(stage, id);
    m.insert("campaign", MetadataValue::String(campaign.to_string()));
    m
}
pub(crate) fn initial_body(id: usize, campaign: usize, size: usize) -> Bytes {
    // Deterministic varied bytes, rather than a repeated-byte compression shortcut.
    let mut seed = id as u64 + 1;
    let padding: String = (0..size.saturating_sub(128))
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (b'a' + (seed % 26) as u8) as char
        })
        .collect();
    Bytes::from(
        serde_json::to_vec(&json!({"id":id,"campaign":campaign,"padding":padding})).unwrap(),
    )
}
fn prepare_payload(row: &ClaimedItem, stage: usize, base: i64) -> Result<(Bytes, i64)> {
    let mut document: Value = serde_json::from_slice(row.payload.as_ref().ok_or("missing body")?)?;
    let id = document["id"].as_u64().ok_or("missing recipient")? as usize;
    if id != recipient(row) {
        return Err("payload/metadata recipient mismatch".into());
    }
    let priority;
    if stage == 0 {
        let first = base + 60 * ((id / 7) % WINDOWS) as i64;
        document["top_times"] = json!([first + 600, first, first + 300]);
        document["color"] = json!(["red", "blue", "green"][id % 3]);
        document["score"] = json!(id % 101);
        priority = id as i64;
    } else {
        // Scheduling consumes stored enrichment, never a producer's in-memory list.
        let times = document["top_times"]
            .as_array()
            .ok_or("missing persisted top times")?;
        priority = times
            .iter()
            .map(|v| v.as_i64().ok_or("invalid top time"))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .ok_or("empty top times")?;
        document["scheduled_at"] = json!(priority);
    }
    Ok((Bytes::from(serde_json::to_vec(&document)?), priority))
}

fn prepare_metadata(row: &ClaimedItem, stage: usize, base: i64) -> Result<(Metadata, i64)> {
    let id = recipient(row);
    let mut metadata = row.metadata.clone();
    metadata.insert("stage", MetadataValue::String((stage + 1).to_string()));
    let priority;
    if stage == 0 {
        let document: Value = serde_json::from_slice(row.payload.as_ref().ok_or("missing body")?)?;
        if document["id"].as_u64() != Some(id as u64) {
            return Err("payload/metadata recipient mismatch".into());
        }
        let first = base + 60 * ((id / 7) % WINDOWS) as i64;
        metadata.insert(
            "top_times",
            MetadataValue::Array(
                [first + 600, first, first + 300]
                    .into_iter()
                    .map(MetadataValue::Integer)
                    .collect(),
            ),
        );
        metadata.insert(
            "color",
            MetadataValue::String(["red", "blue", "green"][id % 3].into()),
        );
        metadata.insert("score", MetadataValue::Integer((id % 101) as i64));
        priority = id as i64;
    } else {
        let Some(MetadataValue::Array(times)) = metadata.get("top_times") else {
            return Err("missing persisted top times".into());
        };
        priority = times
            .iter()
            .map(|value| match value {
                MetadataValue::Integer(value) => Ok(*value),
                _ => Err("invalid top time"),
            })
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .ok_or("empty top times")?;
        metadata.insert("scheduled_at", MetadataValue::Integer(priority));
    }
    Ok((metadata, priority))
}

#[derive(Default)]
struct Counts {
    prepared: AtomicUsize,
    initial_payload_bytes: AtomicU64,
    payload_replacements: AtomicUsize,
    payload_replacement_bytes: AtomicU64,
    terminal: AtomicUsize,
    retries: AtomicUsize,
    claims: AtomicUsize,
    claim_batches: AtomicUsize,
    mutation_batches: AtomicUsize,
    max_claim_batch: AtomicUsize,
    empty_claims: AtomicUsize,
    handler_rows: [AtomicUsize; 3],
    handler_batches: [AtomicUsize; 3],
    max_handler_batch: [AtomicUsize; 3],
    due_to_claim_max_us: AtomicU64,
}

#[allow(
    clippy::too_many_arguments,
    reason = "Campaign workers explicitly receive shared observations, stage bounds, and timing inputs"
)]
async fn workers(
    fw: &Fireweed,
    q: &QueueKey,
    cfg: &Config,
    counts: &Counts,
    campaign: usize,
    cycle: usize,
    target: usize,
    preparing: bool,
    base: i64,
    now: i64,
    deadline: Instant,
    window_started: Instant,
) -> Result<()> {
    futures::future::try_join_all((0..cfg.workers).map(|worker| async move {
        let mut generation = 0;
        while if preparing {
            counts.prepared.load(Ordering::SeqCst) < target * 2
        } else {
            counts.terminal.load(Ordering::SeqCst) < target
        } {
            let rows = retry(deadline, || fw.claim(q, cfg.batch, 3_600_000)).await?;
            if rows.is_empty() {
                counts.empty_claims.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
            counts.claims.fetch_add(rows.len(), Ordering::SeqCst);
            counts.claim_batches.fetch_add(1, Ordering::Relaxed);
            counts
                .max_claim_batch
                .fetch_max(rows.len(), Ordering::Relaxed);
            // Claim order, not handler completion order, is the queue's guarantee.
            let mut previous = (i64::MIN, 0);
            for row in &rows {
                let order = match (&row.priority, cfg.campaign_timestamp_priority) {
                    (Some(PriorityValue::Timestamp(t)), true) => (t.seconds, t.nanoseconds),
                    (Some(PriorityValue::Int64(p)), false) => (*p, 0),
                    _ => return Err("missing or wrong campaign priority type".into()),
                };
                if order < previous {
                    return Err("priority inversion within claim".into());
                }
                previous = order;
                if row.not_before.is_some_and(|t| t > ts(now)) {
                    return Err("early claim".into());
                }
            }
            // Handler transport limits are independent of the bounded storage batch.
            // Finish all chunks from this claim, then publish their per-row guarded results together.
            let mut entries = Vec::with_capacity(rows.len());
            let mut terminal = 0;
            let mut retried = 0;
            let mut prepared = 0;
            for stage in 0..3 {
                let selected: Vec<_> = rows
                    .iter()
                    .filter(|r| {
                        r.metadata.get("stage") == Some(&MetadataValue::String(stage.to_string()))
                    })
                    .collect();
                let limit = [ENRICH_LIMIT, SCHEDULE_LIMIT, DELIVERY_LIMIT][stage].min(cfg.batch);
                for chunk in selected.chunks(limit) {
                    if preparing != (stage < 2) {
                        return Err("stage crossed scheduling barrier".into());
                    }
                    counts.max_handler_batch[stage].fetch_max(chunk.len(), Ordering::SeqCst);
                    if stage == 2 {
                        counts.due_to_claim_max_us.fetch_max(
                            window_started.elapsed().as_micros() as u64,
                            Ordering::SeqCst,
                        );
                    }
                    for row in chunk {
                        let id = recipient(row);
                        let mut patch = ItemPatch::default();
                        if stage < 2 {
                            let priority = if cfg.campaign_metadata_only {
                                let (metadata, priority) = prepare_metadata(row, stage, base)?;
                                patch.metadata = BatchUpdateValue::Replace(metadata);
                                priority
                            } else {
                                let (body, priority) = prepare_payload(row, stage, base)?;
                                counts.payload_replacements.fetch_add(1, Ordering::Relaxed);
                                counts
                                    .payload_replacement_bytes
                                    .fetch_add(body.len() as u64, Ordering::Relaxed);
                                patch.payload = BatchUpdateValue::Replace(Some(body));
                                patch.metadata =
                                    BatchUpdateValue::Replace(meta(stage + 1, id, campaign));
                                priority
                            };
                            patch.priority = BatchUpdateValue::Replace(Some(if stage == 0 {
                                fifo_priority(id, cfg)
                            } else {
                                scheduled_priority(priority, cfg)
                            }));
                            patch.not_before = BatchUpdateValue::Replace(Some(ts(if stage == 0 {
                                1
                            } else {
                                priority
                            })));
                            patch.lifecycle = LifecyclePatch::SetPending;
                        } else {
                            let scheduled = if cfg.campaign_metadata_only {
                                match row.metadata.get("scheduled_at") {
                                    Some(MetadataValue::Integer(value)) => *value,
                                    _ => return Err("missing stored schedule".into()),
                                }
                            } else {
                                let doc: Value = serde_json::from_slice(
                                    row.payload.as_ref().ok_or("missing scheduled payload")?,
                                )?;
                                doc["scheduled_at"].as_i64().ok_or("missing schedule")?
                            };
                            if scheduled > now {
                                return Err("early provider delivery".into());
                            }
                            let mut tracking = row.metadata.clone();
                            if cfg.faults
                                && id.is_multiple_of(19)
                                && tracking.get("retry").is_none()
                            {
                                tracking.insert("retry", MetadataValue::String("1".into()));
                                patch.lifecycle = LifecyclePatch::SetPending;
                                retried += 1;
                            } else {
                                let failed = cfg.faults && id.is_multiple_of(31);
                                tracking.insert(
                                    "outcome",
                                    MetadataValue::String(
                                        if failed { "failed" } else { "accepted" }.into(),
                                    ),
                                );
                                tracking.insert(
                                    "provider_id",
                                    MetadataValue::String(format!("provider-{id}")),
                                );
                                tracking
                                    .insert("completed_at", MetadataValue::String(now.to_string()));
                                patch.lifecycle = if failed {
                                    LifecyclePatch::SetFailed
                                } else {
                                    LifecyclePatch::SetComplete
                                };
                                terminal += 1;
                            }
                            patch.metadata = BatchUpdateValue::Replace(tracking);
                        }
                        entries.push(AddressedMutation {
                            item_id: row.item_id,
                            expected_item_version: Some(row.item_version),
                            predicates: vec![],
                            lease_guard: LeaseGuard::Match(
                                row.lease_token.clone().ok_or("missing lease")?,
                            ),
                            patch,
                        });
                    }
                    counts.handler_rows[stage].fetch_add(chunk.len(), Ordering::SeqCst);
                    counts.handler_batches[stage].fetch_add(1, Ordering::SeqCst);
                    if stage < 2 {
                        prepared += chunk.len();
                    }
                }
            }
            let request = ItemMutationRequest {
                request_id: RequestId::new(format!(
                    "c{cycle}-t{now}-p{preparing}-w{worker}-g{generation}"
                ))
                .unwrap(),
                evaluated_at: ts(now),
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed { entries },
            };
            generation += 1;
            let response = retry(deadline, || fw.mutate_items(q, request.clone())).await?;
            if response.results.len() != rows.len()
                || response
                    .results
                    .iter()
                    .any(|r| !matches!(r.outcome, ItemMutationOutcome::Updated { .. }))
            {
                return Err("missing/rejected campaign mutation result".into());
            }
            counts.prepared.fetch_add(prepared, Ordering::SeqCst);
            counts.mutation_batches.fetch_add(1, Ordering::Relaxed);
            counts.terminal.fetch_add(terminal, Ordering::SeqCst);
            counts.retries.fetch_add(retried, Ordering::SeqCst);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }))
    .await?;
    Ok(())
}

// Separate read oracle: checks persisted fields, identities and dispositions, not worker counters.
#[allow(
    clippy::too_many_arguments,
    reason = "The independent persisted-row oracle keeps population, campaign, phase, and deadline explicit"
)]
async fn verify_rows(
    fw: &Fireweed,
    q: &QueueKey,
    ids: &[usize],
    campaign: usize,
    base: i64,
    terminal: bool,
    cfg: &Config,
    deadline: Instant,
) -> Result<(usize, usize)> {
    let mut seen = std::collections::BTreeSet::new();
    let expected: std::collections::BTreeSet<_> = ids.iter().copied().collect();
    let mut cursor = None;
    let mut failed_count = 0;
    loop {
        let rows = retry(deadline, || fw.retained_items(q, cursor, 1000)).await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let doc: Value =
                serde_json::from_slice(row.payload.as_ref().ok_or("missing retained body")?)?;
            let id = doc["id"].as_u64().ok_or("missing retained id")? as usize;
            if !expected.contains(&id) || !seen.insert(id) || row.client_item_key != key(id) {
                return Err("missing/duplicate/foreign campaign identity".into());
            }
            let first = base + 60 * ((id / 7) % 4) as i64;
            let enriched = if cfg.campaign_metadata_only {
                serde_json::to_value(&row.metadata)?
            } else {
                doc.clone()
            };
            if doc["campaign"] != campaign
                || enriched["color"] != ["red", "blue", "green"][id % 3]
                || enriched["score"] != id % 101
                || enriched["top_times"] != json!([first + 600, first, first + 300])
                || enriched["scheduled_at"] != first
                || row.priority != Some(scheduled_priority(first, cfg))
                || row.not_before != Some(ts(first))
            {
                return Err("persisted enrichment/scheduling mismatch".into());
            }
            let original: Value =
                serde_json::from_slice(&initial_body(id, campaign, cfg.payload_bytes))?;
            if doc["padding"] != original["padding"]
                || (cfg.campaign_metadata_only && doc != original)
            {
                return Err("original attributes lost".into());
            }
            let failed = cfg.faults && id.is_multiple_of(31);
            let retries = usize::from(cfg.faults && id.is_multiple_of(19));
            if terminal {
                let state = if failed {
                    ItemState::Failed
                } else {
                    ItemState::Complete
                };
                if row.lifecycle_state != state
                    || row.metadata.get("outcome")
                        != Some(&MetadataValue::String(
                            if failed { "failed" } else { "accepted" }.into(),
                        ))
                    || row.metadata.get("provider_id")
                        != Some(&MetadataValue::String(format!("provider-{id}")))
                    || row.metadata.get("completed_at")
                        != Some(&MetadataValue::String(first.to_string()))
                    || row.attempt_count != (3 + retries) as u32
                {
                    return Err(format!("persisted disposition mismatch for {id}: {row:?}").into());
                }
                failed_count += usize::from(failed);
            } else if row.lifecycle_state != ItemState::Pending || row.attempt_count != 2 {
                return Err("scheduled backlog contains non-pending or wrong-attempt row".into());
            }
        }
        cursor = rows.last().map(|r| r.item_id);
    }
    if seen != expected {
        return Err("retained export omitted recipients".into());
    }
    Ok((seen.len(), failed_count))
}

pub async fn run(cfg: Config, root: &Path) -> Result<Value> {
    if cfg.memory
        || cfg.items < cfg.shards * CAMPAIGNS
        || cfg.workers == 0
        || cfg.shards == 0
        || cfg.cycles == 0
        || (!cfg.recycle && cfg.cycles != 1)
        || !(1..=1000).contains(&cfg.batch)
        || cfg.load_workers == 0
        || cfg.apply_debt_bytes == Some(0)
        || !(1..=8192).contains(&cfg.purge_batch.unwrap_or(8000))
    {
        return Err(
            "campaign requires disk Turso, positive dimensions and at least two rows per shard"
                .into(),
        );
    }
    tokio::time::timeout(cfg.deadline, run_inner(cfg, root))
        .await
        .map_err(|_| "campaign deadline exceeded")?
}
async fn run_inner(cfg: Config, root: &Path) -> Result<Value> {
    let started = Instant::now();
    let deadline = started + cfg.deadline;
    let barrier = Arc::new(tokio::sync::Barrier::new(cfg.shards * CAMPAIGNS));
    let mut shards = tokio::task::JoinSet::new();
    for shard in 0..cfg.shards {
        let cfg = cfg.clone();
        let barrier = barrier.clone();
        let root = root.join(format!("shard-{shard}"));
        shards.spawn(async move {
            let clock = TestClock::at(100);
            let projection_root = cfg.projection_root.as_ref().map(|p| p.join(format!("shard-{shard}"))).unwrap_or(root.clone());
            let mut policy = AsyncProjectionSpec::default();
            if let Some(bytes) = cfg.apply_debt_bytes { policy.apply_debt_max_bytes = bytes; }
            let fw = Arc::new(open_store_with_async_policy(&root, false, clock.clone(), &projection_root, policy)?);
            let campaign_results = futures::future::try_join_all((0..CAMPAIGNS).map(|campaign| {
                let fw = fw.clone(); let clock = clock.clone(); let cfg = cfg.clone(); let barrier = barrier.clone(); let projection_root = projection_root.clone();
                async move {
                    let q = create_campaign_queue(&fw, &format!("campaign-{campaign}"), &cfg).await?;
                    let ids: Vec<_> = (shard..cfg.items).step_by(cfg.shards).enumerate().filter_map(|(offset,id)| (offset % CAMPAIGNS == campaign).then_some(id)).collect();
                    let mut cycles = Vec::new();
                    for cycle in 0..cfg.cycles {
                        let base = 1000 + cycle as i64 * 7200;
                        // Both campaigns use the same time windows; only campaign zero advances the shared clock.
                        barrier.wait().await;
                        if campaign == 0 { clock.set((base - 60) as u64); }
                        barrier.wait().await;
                        let cycle_start = Instant::now();
                        let counts = Counts::default();
                        let stop = AtomicBool::new(false);
                        let progress_phase = AtomicUsize::new(0);
                        let observe = async {
                            let mut latencies = Vec::new();
                            let mut samples = Vec::new();
                            while !stop.load(Ordering::SeqCst) {
                                let from = progress_phase.load(Ordering::Relaxed);
                                let acknowledged_terminal = counts.terminal.load(Ordering::SeqCst) as u64;
                                let mut attempts = 0;
                                let t = Instant::now();
                                let m = retry(deadline, || { attempts += 1; fw.metrics(&q) }).await?;
                                let elapsed = t.elapsed().as_secs_f64();
                                let to = progress_phase.load(Ordering::Relaxed);
                                samples.push((from, to, elapsed, attempts));
                                let resident = m.pending + m.leased + m.complete + m.failed;
                                if resident > ids.len() as u64 { return Err("progress exceeds list size".into()); }
                                // Between completed ingestion and the start of purge,
                                // every recipient remains resident. A strong read must
                                // also include terminal mutations acknowledged before
                                // that read began. Crossing into purge can legitimately
                                // reduce either count, so exclude those intervals.
                                if (1..=3).contains(&from) && (1..=3).contains(&to) {
                                    if resident != ids.len() as u64 { return Err("progress omitted resident recipients".into()); }
                                    if m.complete + m.failed < acknowledged_terminal { return Err("progress omitted acknowledged terminal outcomes".into()); }
                                }
                                latencies.push(elapsed);
                                tokio::time::sleep(Duration::from_millis(1000)).await;
                            }
                            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((latencies, samples))
                        };
                        let execute = async {
                            let phase = Instant::now();
                            futures::stream::iter(0..ids.len().div_ceil(cfg.batch)).map(|batch_index| {
                                let ids = &ids; let cfg = &cfg; let fw = &fw; let q = &q; let counts = &counts;
                                async move {
                                let chunk = &ids[batch_index*cfg.batch..((batch_index+1)*cfg.batch).min(ids.len())];
                                let rows: Vec<_> = chunk.iter().map(|id| NewItem { client_item_key: Some(key(*id)),
                                    priority: Some(fifo_priority(*id, cfg)), not_before: Some(ts(1)),
                                    metadata: meta(0, *id, campaign), payload: Some(initial_body(*id, campaign, cfg.payload_bytes)), ..Default::default() }).collect();
                                counts.initial_payload_bytes.fetch_add(rows.iter().map(|row| row.payload.as_ref().map_or(0, |body| body.len() as u64)).sum::<u64>(), Ordering::Relaxed);
                                let accepted = retry(deadline, || fw.push_batch(q, rows.clone())).await?;
                                if accepted.len() != chunk.len() { return Err("missing push identities".into()); }
                                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                            }}).buffer_unordered(cfg.load_workers).try_collect::<Vec<_>>().await?;
                            let load_s = phase.elapsed().as_secs_f64();
                            // Metrics may fold an unapplied durable tail. A public
                            // retained read explicitly settles projection work.
                            let resident = retry(deadline, || fw.retained_items(&q,None,1)).await?;
                            if resident.is_empty() != ids.is_empty() { return Err("load projection presence mismatch".into()); }
                            let m = retry(deadline, || fw.metrics(&q)).await?;
                            if m.pending != ids.len() as u64 { return Err("resident load mismatch".into()); }
                            barrier.wait().await; // entire list is resident before any preparation starts
                            let phase = Instant::now();
                            progress_phase.store(1, Ordering::Relaxed);
                            workers(&fw,&q,&cfg,&counts,campaign,cycle,ids.len(),true,base,base-60,deadline,Instant::now()).await?;
                            retry(deadline, || fw.metrics(&q)).await?;
                            verify_rows(&fw,&q,&ids,campaign,base,false,&cfg,deadline).await?;
                            if !retry(deadline, || fw.claim(&q,500,1000)).await?.is_empty() { return Err("future backlog claimed early".into()); }
                            let prepare_s = phase.elapsed().as_secs_f64();
                            barrier.wait().await; // million scheduled rows, not cumulative completions
                            let phase = Instant::now();
                            progress_phase.store(2, Ordering::Relaxed);
                            for window in 0..WINDOWS {
                                if campaign == 0 { clock.set((base + window as i64 * 60) as u64); }
                                barrier.wait().await;
                                let target = ids.iter().filter(|id| (**id / 7) % WINDOWS <= window).count();
                                workers(&fw,&q,&cfg,&counts,campaign,cycle,target,false,base,base+window as i64*60,deadline,Instant::now()).await?;
                                let m = retry(deadline, || fw.metrics(&q)).await?;
                                if m.complete + m.failed != target as u64 || m.leased != 0 { return Err("window disposition mismatch".into()); }
                                if !retry(deadline, || fw.claim(&q,500,1000)).await?.is_empty() { return Err("next window delivered early".into()); }
                                barrier.wait().await;
                            }
                            let delivery_s = phase.elapsed().as_secs_f64();
                            let phase = Instant::now();
                            progress_phase.store(3, Ordering::Relaxed);
                            let (verified, failed) = verify_rows(&fw,&q,&ids,campaign,base,true,&cfg,deadline).await?;
                            let verify_s = phase.elapsed().as_secs_f64();
                            barrier.wait().await;
                            if campaign == 0 { clock.set((base + 600) as u64); }
                            barrier.wait().await;
                            let phase = Instant::now();
                            progress_phase.store(4, Ordering::Relaxed);
                            // Retention discovers stored identities; no producer ID list drives purge.
                            let mut cursor = None; let mut purged = 0;
                            let purge_batch = cfg.purge_batch.unwrap_or(8000);
                            let mut purge_ids = Vec::with_capacity(purge_batch);
                            let mut purge_batches = 0; let mut max_purge_batch = 0;
                            if cfg.recycle { loop {
                                let page_size = (purge_batch - purge_ids.len()).min(1000);
                                let rows = retry(deadline, || fw.retained_items(&q,cursor,page_size)).await?;
                                let ended = rows.is_empty();
                                if let Some(row) = rows.last() { cursor = Some(row.item_id); }
                                purge_ids.extend(rows.iter().map(|row| row.item_id));
                                if !purge_ids.is_empty() && (ended || purge_ids.len() == purge_batch) {
                                    purged += retry(deadline, || fw.purge(&q,purge_ids.iter().copied(),false)).await?;
                                    purge_batches += 1; max_purge_batch = max_purge_batch.max(purge_ids.len());
                                    purge_ids.clear();
                                }
                                if ended { break; }
                            }}
                            if cfg.recycle && !retry(deadline, || fw.retained_items(&q,None,1)).await?.is_empty() {
                                return Err("purge left projected rows".into());
                            }
                            let m = retry(deadline, || fw.metrics(&q)).await?;
                            if cfg.recycle && (purged != ids.len() as u64 || m.pending+m.leased+m.complete+m.failed != 0) { return Err("retention mismatch".into()); }
                            stop.store(true,Ordering::SeqCst);
                            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({"cycle":cycle,"items":ids.len(),"verified":verified,"delivered":verified-failed,"failed":failed,"purged":purged,
                                "purge_batches":purge_batches,"max_purge_batch":max_purge_batch,
                                "pending":0,"leased":0,"load_s":load_s,"prepare_s":prepare_s,"delivery_s":delivery_s,"verify_s":verify_s,"purge_s":phase.elapsed().as_secs_f64(),
                                "wall_s":cycle_start.elapsed().as_secs_f64(),"retries":counts.retries.load(Ordering::SeqCst),"claims":counts.claims.load(Ordering::SeqCst),
                                "initial_payload_bytes":counts.initial_payload_bytes.load(Ordering::Relaxed),"payload_replacements":counts.payload_replacements.load(Ordering::Relaxed),"payload_replacement_bytes":counts.payload_replacement_bytes.load(Ordering::Relaxed),
                                "claim_batches":counts.claim_batches.load(Ordering::Relaxed),"mutation_batches":counts.mutation_batches.load(Ordering::Relaxed),
                                "max_claim_batch":counts.max_claim_batch.load(Ordering::Relaxed),"empty_claims":counts.empty_claims.load(Ordering::Relaxed),
                                "due_to_claim_max_us":counts.due_to_claim_max_us.load(Ordering::SeqCst),"max_handler_batch":counts.max_handler_batch.each_ref().map(|v|v.load(Ordering::SeqCst)),
                                "handler_rows":counts.handler_rows.each_ref().map(|v|v.load(Ordering::SeqCst)),"handler_batches":counts.handler_batches.each_ref().map(|v|v.load(Ordering::SeqCst)),
                                "process_rss_kib":process_rss_kib(),"projection_bytes":std::fs::metadata(projection_root.join("projection.db")).ok().map(|s|s.len()),
                                "projection_wal_bytes":std::fs::metadata(projection_root.join("projection.db-wal")).ok().map(|s|s.len())}))
                        };
                        let (mut report, (mut latencies, samples)) = tokio::try_join!(execute,observe)?;
                        latencies.sort_by(f64::total_cmp);
                        report["progress_reads"] = json!(latencies.len());
                        report["progress_p95_s"] = json!(latencies[(latencies.len()-1)*95/100]);
                        report["progress_by_phase"] = progress_by_phase(samples);
                        eprintln!("campaign_cycle_complete shard={shard} campaign={campaign} {report}");
                        cycles.push(report);
                    }
                    Ok::<_,Box<dyn std::error::Error+Send+Sync>>(json!({"campaign":campaign,"cycles":cycles}))
                }
            })).await?;
            Ok::<_,Box<dyn std::error::Error+Send+Sync>>(json!({"shard":shard,"campaigns":campaign_results}))
        });
    }
    let mut reports = Vec::new();
    while let Some(result) = shards.join_next().await {
        reports.push(result??);
    }
    reports.sort_by_key(|s| s["shard"].as_u64());
    Ok(
        json!({"schema":"campaign-capacity/v3","enrichment_storage":if cfg.campaign_metadata_only {"row_metadata"} else {"payload"},"cell":"filesystem--turso","items":cfg.items,"cycles":cfg.cycles,
        "priority_workload":if cfg.campaign_timestamp_priority {"availability_timestamp"} else {"mixed_sequence_stress"},"physical_shards":cfg.shards,"workers_per_campaign":cfg.workers,"load_workers_per_campaign":cfg.load_workers,"campaigns":CAMPAIGNS,"batch":cfg.batch,"stage_limits":[500,200,500],"faults":cfg.faults,"includes_purge":cfg.recycle,
        "purge_batch":cfg.purge_batch.unwrap_or(8000),"lease_ms":3600000,"request_id_retention_ms":3600000,"cycle_clock_step_s":7200,
        "payload_bytes":cfg.payload_bytes,"scheduled_windows":WINDOWS,"resident_backlog":cfg.items,"progress_interval_ms":1000,"apply_debt_max_bytes":cfg.apply_debt_bytes.unwrap_or(AsyncProjectionSpec::default().apply_debt_max_bytes),
        "completed_lifecycles_per_s":(cfg.items*cfg.cycles) as f64/started.elapsed().as_secs_f64(),"settled_wall_s":started.elapsed().as_secs_f64(),"shards":reports}),
    )
}

#[cfg(test)]
mod priority_model_tests {
    use super::*;

    #[test]
    fn fifo_ordinals_remain_before_future_timestamps_at_large_list_sizes() {
        let cfg = Config {
            campaign_timestamp_priority: true,
            ..Config::default()
        };
        let mut previous = (i64::MIN, 0);
        for id in [0, 999, 1000, 1_000_000, 1_000_000_000] {
            let PriorityValue::Timestamp(value) = fifo_priority(id, &cfg) else {
                panic!("timestamp FIFO")
            };
            let order = (value.seconds, value.nanoseconds);
            assert!(order > previous);
            assert!(
                order < (1000, 0),
                "unscheduled FIFO work must precede the first scheduled window"
            );
            previous = order;
        }
        assert_eq!(
            scheduled_priority(1000, &cfg),
            PriorityValue::Timestamp(ts(1000))
        );
        let stress = Config::default();
        assert_eq!(
            fifo_priority(1_000_000, &stress),
            PriorityValue::Int64(1_000_000)
        );
        assert_eq!(
            scheduled_priority(1000, &stress),
            PriorityValue::Int64(1000)
        );
    }
}
