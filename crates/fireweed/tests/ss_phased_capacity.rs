//! Seventh Sense phased capacity harness.
//!
//! Default cell is the production pair: filesystem object log × Turso
//! (`filesystem--turso`). No environment variables are required.
//!
//! ```text
//! cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
//! ```
//!
//! Optional overrides (calibration only): `SS_N`, `SS_CELL=objectlog` (memory
//! projection control), `SS_CELL=sqlite` (sqlite command-log, not production).
//!
//! Phase barriers (`join_all` waves) are intentional here so each phase can be
//! attributed. The streaming evidence lane in `ss_streaming.rs` must not reuse
//! them as its admission mechanism.

#![cfg(feature = "objectlog")]

#[path = "support/ss_capacity.rs"]
mod ss_capacity;
use ss_capacity::*;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fireweed::*;
use serde_json::{Value, json};

const DEFAULT_N: usize = 10_000;
const WARMUP_N: usize = 10_000;

struct PhaseRow {
    name: &'static str,
    items: usize,
    mutations: usize,
    ack_wall: Duration,
    settled_wall: Duration,
    residual: ResidualSnapshot,
    calls: Vec<(&'static str, CallStats)>,
}

impl PhaseRow {
    fn items_per_s(&self) -> f64 {
        self.items as f64 / self.settled_wall.as_secs_f64().max(1e-9)
    }
    fn mutations_per_s(&self) -> f64 {
        (self.items * self.mutations) as f64 / self.settled_wall.as_secs_f64().max(1e-9)
    }
    fn ack_items_per_s(&self) -> f64 {
        self.items as f64 / self.ack_wall.as_secs_f64().max(1e-9)
    }
    fn evidence(&self) -> Value {
        json!({
            "name": self.name,
            "items": self.items,
            "mutations": self.mutations,
            "ack_wall_s": self.ack_wall.as_secs_f64(),
            "settled_wall_s": self.settled_wall.as_secs_f64(),
            "settlement_lag_s": self.settled_wall.saturating_sub(self.ack_wall).as_secs_f64(),
            "ack_items_per_s": self.ack_items_per_s(),
            "settled_items_per_s": self.items_per_s(),
            "settled_mutations_per_s": self.mutations_per_s(),
            "residual": self.residual.evidence(),
            "calls": self.calls.iter().map(|(op, stats)| stats.evidence(op)).collect::<Vec<_>>(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ss_phased_capacity_smoke() {
    let process_started = Instant::now();
    let n = env_usize("SS_N", DEFAULT_N);
    let push_batch = env_usize("SS_PUSH_BATCH", 100);
    let claim_batch = env_usize("SS_CLAIM_BATCH", 100);
    assert!(
        n > 0 && n.is_multiple_of(claim_batch),
        "SS_N must be >0 and divisible by SS_CLAIM_BATCH"
    );

    let cell = Cell::parse();
    let clock = Arc::new(SystemClock);
    let inflight = cell.inflight();
    eprintln!("{} inflight={inflight}", cell.describe());
    let mem_before_open = read_mem();
    let fw = Arc::new(cell.open(clock));

    let warmup_def = qdef("t-ss-phased", "q-warmup", push_batch, claim_batch);
    fw.create_queue(warmup_def).await.expect("warmup queue");
    let warmup_q = QueueKey::new(
        TenantId::new("t-ss-phased").unwrap(),
        QueueId::new("q-warmup").unwrap(),
    );
    let warm_payload = Bytes::from(vec![b'w'; 64]);
    let warm_now = UtcTimestamp::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        0,
    )
    .unwrap();
    for chunk in (0..WARMUP_N).step_by(push_batch) {
        let end = (chunk + push_batch).min(WARMUP_N);
        let items: Vec<NewItem> = (chunk..end)
            .map(|i| NewItem {
                client_item_key: Some(ClientItemKey::new(format!("warm-{i}")).unwrap()),
                payload: Some(warm_payload.clone()),
                not_before: Some(warm_now),
                priority: Some(PriorityValue::Timestamp(warm_now)),
                ..Default::default()
            })
            .collect();
        fw.push_batch(&warmup_q, items).await.expect("warmup push");
    }
    fw.metrics(&warmup_q)
        .await
        .expect("settle warmup projection debt");

    let def = qdef("t-ss-phased", "q-ss", push_batch, claim_batch);
    fw.create_queue(def).await.expect("create queue");
    let queue = QueueKey::new(
        TenantId::new("t-ss-phased").unwrap(),
        QueueId::new("q-ss").unwrap(),
    );

    let stub = Bytes::from(vec![b's'; STUB_BYTES]);
    let profile = Bytes::from(vec![b'p'; PROFILE_BYTES]);
    let now = UtcTimestamp::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        0,
    )
    .unwrap();

    // --- P1 ingest ---
    let mut p1_calls = CallStats::new();
    let t0 = Instant::now();
    let keys: Vec<ClientItemKey> = (0..n).map(key).collect();
    let starts: Vec<usize> = (0..n).step_by(push_batch).collect();
    for window in starts.chunks(inflight) {
        let futs = window.iter().map(|&chunk| {
            let fw = Arc::clone(&fw);
            let queue = queue.clone();
            let stub = stub.clone();
            let end = (chunk + push_batch).min(n);
            let items: Vec<NewItem> = (chunk..end)
                .map(|i| NewItem {
                    client_item_key: Some(key(i)),
                    group_key: Some(job_key(i, n)),
                    payload: Some(stub.clone()),
                    metadata: phase_meta("needs_profile"),
                    not_before: Some(now),
                    priority: Some(PriorityValue::Timestamp(now)),
                    ..Default::default()
                })
                .collect();
            async move {
                let c0 = Instant::now();
                let ids =
                    retry_backpressure("P1 push", || fw.push_batch(&queue, items.clone())).await;
                (c0.elapsed(), ids.len(), end - chunk)
            }
        });
        for (elapsed, got, expect) in futures::future::join_all(futs).await {
            p1_calls.record(elapsed);
            assert_eq!(got, expect);
        }
    }
    let p1_ack = t0.elapsed();
    let (p1_settled, p1_residual) = settle_phase(fw.as_ref(), &queue, t0, n + 1).await;
    let p1 = PhaseRow {
        name: "P1_ingest",
        items: n,
        mutations: 1,
        ack_wall: p1_ack,
        settled_wall: p1_settled,
        residual: p1_residual,
        calls: vec![("BatchPush", p1_calls)],
    };

    // --- P2 enrich (pending BatchUpdate) ---
    let mut p2_calls = CallStats::new();
    let t0 = Instant::now();
    let mut updated = 0usize;
    for (window_i, window) in keys.chunks(claim_batch * inflight).enumerate() {
        let futs = window.chunks(claim_batch).enumerate().map(|(j, chunk)| {
            let fw = Arc::clone(&fw);
            let queue = queue.clone();
            let profile = profile.clone();
            let chunk = chunk.to_vec();
            let req_i = window_i * inflight + j;
            async move {
                let updates: Vec<BatchUpdateEntry> = chunk
                    .iter()
                    .map(|k| BatchUpdateEntry {
                        item_ref: BatchUpdateItemRef::ClientItemKey(k.clone()),
                        expected_item_version: None,
                        priority: BatchUpdateValue::Keep,
                        not_before: BatchUpdateValue::Keep,
                        payload: BatchUpdateValue::Replace(Some(profile.clone())),
                        metadata: BatchUpdateValue::Replace(phase_meta("needs_schedule")),
                        gate_keys: BatchUpdateValue::Keep,
                        fields: BatchUpdateValue::Keep,
                    })
                    .collect();
                let req = BatchUpdateRequest {
                    request_id: RequestId::new(format!("p2-{req_i}")).unwrap(),
                    updates,
                };
                let c0 = Instant::now();
                let resp =
                    retry_backpressure("P2 update", || fw.batch_update(&queue, req.clone())).await;
                let ok = resp
                    .results
                    .iter()
                    .filter(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
                    .count();
                (c0.elapsed(), ok, chunk.len())
            }
        });
        for (elapsed, ok, expect) in futures::future::join_all(futs).await {
            p2_calls.record(elapsed);
            assert_eq!(ok, expect, "P2 every entry Updated");
            updated += ok;
        }
    }
    assert_eq!(updated, n);
    let p2_ack = t0.elapsed();
    let (p2_settled, p2_residual) = settle_phase(fw.as_ref(), &queue, t0, n + 1).await;
    let p2 = PhaseRow {
        name: "P2_enrich",
        items: n,
        mutations: 1,
        ack_wall: p2_ack,
        settled_wall: p2_settled,
        residual: p2_residual,
        calls: vec![("BatchUpdate", p2_calls)],
    };

    let sample_idx = [0usize, n / 2, n.saturating_sub(1)];
    for i in sample_idx {
        let view = fw
            .live_item(&queue, key(i))
            .await
            .expect("live")
            .expect("present after P2");
        assert_eq!(
            view.payload.as_ref().map(|b| b.len()),
            Some(PROFILE_BYTES),
            "P2 sampled profile blob"
        );
    }

    // --- P3 schedule ---
    let mut p3_calls = CallStats::new();
    let t0 = Instant::now();
    let due = UtcTimestamp::new(now.seconds.saturating_sub(1), 0).unwrap();
    updated = 0;
    for (window_i, window) in keys.chunks(claim_batch * inflight).enumerate() {
        let futs = window.chunks(claim_batch).enumerate().map(|(j, chunk)| {
            let fw = Arc::clone(&fw);
            let queue = queue.clone();
            let chunk = chunk.to_vec();
            let req_i = window_i * inflight + j;
            async move {
                let updates: Vec<BatchUpdateEntry> = chunk
                    .iter()
                    .map(|k| BatchUpdateEntry {
                        item_ref: BatchUpdateItemRef::ClientItemKey(k.clone()),
                        expected_item_version: None,
                        priority: BatchUpdateValue::Replace(PriorityValue::Timestamp(due)),
                        not_before: BatchUpdateValue::Replace(Some(due)),
                        payload: BatchUpdateValue::Keep,
                        metadata: BatchUpdateValue::Replace(phase_meta("ready")),
                        gate_keys: BatchUpdateValue::Keep,
                        fields: BatchUpdateValue::Keep,
                    })
                    .collect();
                let req = BatchUpdateRequest {
                    request_id: RequestId::new(format!("p3-{req_i}")).unwrap(),
                    updates,
                };
                let c0 = Instant::now();
                let resp =
                    retry_backpressure("P3 update", || fw.batch_update(&queue, req.clone())).await;
                let ok = resp
                    .results
                    .iter()
                    .filter(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
                    .count();
                (c0.elapsed(), ok, chunk.len())
            }
        });
        for (elapsed, ok, expect) in futures::future::join_all(futs).await {
            p3_calls.record(elapsed);
            assert_eq!(ok, expect, "P3 every entry Updated");
            updated += ok;
        }
    }
    assert_eq!(updated, n);
    let p3_ack = t0.elapsed();
    let (p3_settled, p3_residual) = settle_phase(fw.as_ref(), &queue, t0, n + 1).await;
    let p3 = PhaseRow {
        name: "P3_schedule",
        items: n,
        mutations: 1,
        ack_wall: p3_ack,
        settled_wall: p3_settled,
        residual: p3_residual,
        calls: vec![("BatchUpdate", p3_calls)],
    };

    for i in sample_idx {
        let view = fw
            .live_item(&queue, key(i))
            .await
            .expect("live")
            .expect("present after P3");
        assert_eq!(view.not_before, Some(due), "P3 sampled delivery timestamp");
        assert_eq!(
            view.priority,
            Some(PriorityValue::Timestamp(due)),
            "P3 sampled priority"
        );
    }

    // --- P4 deliver: unfiltered claim + complete ---
    // Waves of `inflight` claims. Writer serializes the lease txn; appends pack.
    // Overlap complete of wave N with claim wave N+1. One empty claim is not "done".
    let mut p4_claim = CallStats::new();
    let mut p4_fin = CallStats::new();
    let t0 = Instant::now();
    let mut completed = 0usize;
    let claim_wave = inflight;
    let mut prev = {
        let futs = (0..claim_wave).map(|_| {
            let fw = Arc::clone(&fw);
            let queue = queue.clone();
            async move {
                let c0 = Instant::now();
                let items =
                    retry_backpressure("P4 claim", || fw.claim(&queue, claim_batch, 30_000)).await;
                (c0.elapsed(), items)
            }
        });
        futures::future::join_all(futs).await
    };
    for (elapsed, _) in &prev {
        p4_claim.record(*elapsed);
    }
    loop {
        let batches: Vec<Vec<_>> = prev
            .into_iter()
            .map(|(_, items)| items)
            .filter(|items| !items.is_empty())
            .collect();
        if batches.is_empty() {
            if completed >= n {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(180),
                "P4 did not drain remaining items: completed={completed} n={n}"
            );
            // One empty wave is not done: apply debt can still catch up.
            tokio::time::sleep(RETRY_CADENCE).await;
            prev = {
                let futs = (0..claim_wave).map(|_| {
                    let fw = Arc::clone(&fw);
                    let queue = queue.clone();
                    async move {
                        let c0 = Instant::now();
                        let items = retry_backpressure("P4 claim", || {
                            fw.claim(&queue, claim_batch, 30_000)
                        })
                        .await;
                        (c0.elapsed(), items)
                    }
                });
                futures::future::join_all(futs).await
            };
            for (elapsed, _) in &prev {
                p4_claim.record(*elapsed);
            }
            continue;
        }
        completed += batches.iter().map(|batch| batch.len()).sum::<usize>();
        let finishing = completed >= n;
        let fw_fin = Arc::clone(&fw);
        let fw_claim = Arc::clone(&fw);
        let queue_fin = queue.clone();
        let queue_claim = queue.clone();
        let (fin_times, next) = tokio::join!(
            async move {
                let futs = batches.into_iter().map(|batch| {
                    let fw = Arc::clone(&fw_fin);
                    let queue = queue_fin.clone();
                    async move {
                        let ids: Vec<_> = batch.iter().map(|item| item.item_id).collect();
                        let c1 = Instant::now();
                        retry_backpressure("P4 complete", || fw.complete(&queue, ids.clone()))
                            .await;
                        c1.elapsed()
                    }
                });
                futures::future::join_all(futs).await
            },
            async move {
                if finishing {
                    return Vec::new();
                }
                let futs = (0..claim_wave).map(|_| {
                    let fw = Arc::clone(&fw_claim);
                    let queue = queue_claim.clone();
                    async move {
                        let c0 = Instant::now();
                        let items = retry_backpressure("P4 claim", || {
                            fw.claim(&queue, claim_batch, 30_000)
                        })
                        .await;
                        (c0.elapsed(), items)
                    }
                });
                futures::future::join_all(futs).await
            }
        );
        for elapsed in fin_times {
            p4_fin.record(elapsed);
        }
        for (elapsed, _) in &next {
            p4_claim.record(*elapsed);
        }
        prev = next;
    }
    assert_eq!(completed, n, "P4 completed all items");
    let p4_ack = t0.elapsed();
    let (p4_settled, p4_residual) = settle_phase(fw.as_ref(), &queue, t0, n + 1).await;
    let p4 = PhaseRow {
        name: "P4_deliver",
        items: n,
        mutations: 2,
        ack_wall: p4_ack,
        settled_wall: p4_settled,
        residual: p4_residual,
        calls: vec![("BatchClaim", p4_claim), ("BatchFinalize", p4_fin)],
    };

    assert_eq!(p4.residual.pending, 0, "residual pending");
    // leased must be 0; complete == n (plus nothing from warmup — different queue)
    assert_eq!(p4.residual.leased, 0, "residual leased");
    assert_eq!(p4.residual.complete, n as u64, "complete count");
    assert_eq!(p4.residual.eligible, 0, "residual eligible");

    let phases = [p1, p2, p3, p4];
    let mem_end = read_mem();
    if let Some(root) = cell.log_root() {
        let (objects, bytes) = count_tree(root);
        eprintln!(
            "object_log_tree objects={objects} bytes={bytes} ({:.1} MiB) bytes/object={:.0}",
            bytes as f64 / (1024.0 * 1024.0),
            if objects == 0 {
                0.0
            } else {
                bytes as f64 / objects as f64
            }
        );
    }
    let proj_bytes = cell.projection_bytes();
    if proj_bytes > 0 {
        eprintln!(
            "turso_projection_bytes={} ({:.1} MiB)",
            proj_bytes,
            proj_bytes as f64 / (1024.0 * 1024.0)
        );
    }
    eprintln!(
        "memory before_open rss={:.1} MiB hwm={:.1} MiB | after_run rss={:.1} MiB hwm={:.1} MiB | delta_rss={:.1} MiB ({:.0} B/item)",
        mem_before_open.rss_bytes as f64 / (1024.0 * 1024.0),
        mem_before_open.hwm_bytes as f64 / (1024.0 * 1024.0),
        mem_end.rss_bytes as f64 / (1024.0 * 1024.0),
        mem_end.hwm_bytes as f64 / (1024.0 * 1024.0),
        mem_end.rss_bytes.saturating_sub(mem_before_open.rss_bytes) as f64 / (1024.0 * 1024.0),
        mem_end.rss_bytes.saturating_sub(mem_before_open.rss_bytes) as f64 / n.max(1) as f64
    );
    eprintln!(
        "=== ss_phased_capacity cell={} log_axis={} projection={} inflight={inflight} N={n} push={push_batch} claim={claim_batch} ===",
        cell.cell_name(),
        cell.log_axis(),
        cell.projection_axis()
    );
    eprintln!(
        "phase\titems\tack_wall_s\tsettled_wall_s\tack_items_per_s\tsettled_items_per_s\tsettled_mutations_per_s"
    );
    for p in &phases {
        eprintln!(
            "{}\t{}\t{:.3}\t{:.3}\t{:.0}\t{:.0}\t{:.0}",
            p.name,
            p.items,
            p.ack_wall.as_secs_f64(),
            p.settled_wall.as_secs_f64(),
            p.ack_items_per_s(),
            p.items_per_s(),
            p.mutations_per_s()
        );
        for (op, st) in &p.calls {
            eprintln!(
                "  {op} p50={:.2} p95={:.2} p99={:.2} ms (n={})",
                st.percentile_ms(50.0),
                st.percentile_ms(95.0),
                st.percentile_ms(99.0),
                st.samples.len()
            );
        }
    }

    write_evidence(
        &cell,
        &phases,
        n,
        push_batch,
        claim_batch,
        mem_before_open,
        mem_end,
        process_started.elapsed(),
    );

    drop(fw);
    cell.cleanup();
}

fn write_evidence(
    cell: &Cell,
    phases: &[PhaseRow],
    n: usize,
    push_batch: usize,
    claim_batch: usize,
    mem_before_open: MemSample,
    mem_end: MemSample,
    process_wall: Duration,
) {
    let utc = chrono_like_utc();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/perf/evidence/ss-phased")
        .join(&utc);
    let _ = std::fs::create_dir_all(&dir);
    let command = format!(
        "SS_CELL={} SS_N={n} SS_PUSH_BATCH={push_batch} SS_CLAIM_BATCH={claim_batch} SS_INFLIGHT={} cargo test -p fireweed --test ss_phased_capacity --release ss_phased_capacity_smoke -- --exact --nocapture",
        cell.cell_name(),
        cell.inflight()
    );
    let evidence = evidence_v4(
        json!({
            "utc": utc,
            "source_sha": source_sha(),
            "host": host_name(),
            "command": command,
            "cell": cell.cell_name(),
            "log_axis": cell.log_axis(),
            "projection_axis": cell.projection_axis(),
            "workers": 1,
            "inflight": cell.inflight(),
            "n": n,
            "push_batch": push_batch,
            "claim_batch": claim_batch,
            "sampling": {
                "sample_indices": [0, n / 2, n.saturating_sub(1)],
                "payload_and_schedule_samples_passed": true,
            },
            "memory": {
                "rss_before_open_bytes": mem_before_open.rss_bytes,
                "hwm_before_open_bytes": mem_before_open.hwm_bytes,
                "rss_after_run_bytes": mem_end.rss_bytes,
                "hwm_after_run_bytes": mem_end.hwm_bytes,
                "rss_delta_bytes": mem_end.rss_bytes.saturating_sub(mem_before_open.rss_bytes),
                "projection_bytes": cell.projection_bytes(),
            },
        }),
        phases,
        process_wall,
    );
    let path = dir.join("summary.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&evidence).expect("serialize phased evidence"),
    )
    .expect("write phased evidence");
    eprintln!("wrote {}", path.display());
}

fn evidence_v4(mut run: Value, phases: &[PhaseRow], process_wall: Duration) -> Value {
    let final_residual = phases
        .last()
        .map(|phase| phase.residual.evidence())
        .unwrap_or_else(|| ResidualSnapshot::default().evidence());
    let object = run.as_object_mut().expect("run evidence object");
    object.insert("schema".into(), json!("ss-phased-summary/v4"));
    object.insert("process_wall_s".into(), json!(process_wall.as_secs_f64()));
    object.insert("final_residual".into(), final_residual);
    object.insert(
        "phases".into(),
        Value::Array(phases.iter().map(PhaseRow::evidence).collect()),
    );
    run
}

#[test]
fn ss_evidence_v4_measures_settlement_and_residual() {
    let phase = PhaseRow {
        name: "fixture",
        items: 32,
        mutations: 2,
        ack_wall: Duration::from_millis(10),
        settled_wall: Duration::from_millis(25),
        residual: ResidualSnapshot {
            pending: 0,
            leased: 0,
            complete: 32,
            failed: 0,
            eligible: 0,
        },
        calls: vec![("fixture_call", CallStats::default())],
    };
    let evidence = evidence_v4(
        json!({
            "sampling": { "payload_and_schedule_samples_passed": true },
            "source_sha": "fixture",
            "host": "fixture",
            "command": "fixture",
        }),
        &[phase],
        Duration::from_millis(40),
    );

    assert_eq!(evidence["schema"], "ss-phased-summary/v4");
    assert_eq!(evidence["phases"][0]["ack_wall_s"], 0.01);
    assert_eq!(evidence["phases"][0]["settled_wall_s"], 0.025);
    assert_eq!(evidence["process_wall_s"], 0.04);
    assert_eq!(evidence["final_residual"]["eligible"], 0);
    assert_eq!(evidence["final_residual"]["complete"], 32);
    assert_eq!(
        evidence["sampling"]["payload_and_schedule_samples_passed"],
        true
    );
}
