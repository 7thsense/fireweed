//! I0 — Seventh Sense phased capacity harness (adopted plan).
//!
//! Public facade only. Default `SS_N=10000`. Capacity: `SS_N=1000000`.
//! Cell: `open_sqlite` (`sqlite--memory`). Workers: 1. No metadata predicate on P4.
//!
//! ```text
//! SS_N=10000 cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture
//! ```
//!
//! summary.json schema v1:
//! `{schema, utc, cell, n, push_batch, claim_batch, workers, phases[{name, items, mutations, wall_s, items_per_s, mutations_per_s, calls[{op, p50_ms, p95_ms, p99_ms}]}], residual_eligible, sampled_ok}`

#![cfg(feature = "sqlite")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fireweed::*;
use fireweed_core::{Metadata, MetadataValue};

const STUB_BYTES: usize = 512;
const PROFILE_BYTES: usize = 1024;
const DEFAULT_N: usize = 10_000;
const WARMUP_N: usize = 10_000;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn tmp_log() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "fireweed-ss-phased-{}-{}.db",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{}-wal", p.display()));
    let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    p
}

fn phase_meta(phase: &str) -> Metadata {
    let mut md = Metadata::new();
    md.insert("phase", MetadataValue::String(phase.to_string()));
    md
}

fn qdef(tenant: &str, queue: &str, push_batch: usize, claim_batch: usize) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Timestamp,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 3_600_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 86_400_000,
        client_item_key_retention_ms: 86_400_000,
        terminal_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: push_batch.max(1000) as u64,
        max_claim_batch_size: claim_batch as u64,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

struct CallStats {
    samples: Vec<Duration>,
}

impl CallStats {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }
    fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }
    fn percentile_ms(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut v = self.samples.clone();
        v.sort_unstable();
        let idx = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
        v[idx.min(v.len() - 1)].as_secs_f64() * 1000.0
    }
}

struct PhaseRow {
    name: &'static str,
    items: usize,
    mutations: usize,
    wall: Duration,
    calls: Vec<(&'static str, CallStats)>,
}

impl PhaseRow {
    fn items_per_s(&self) -> f64 {
        self.items as f64 / self.wall.as_secs_f64().max(1e-9)
    }
    fn mutations_per_s(&self) -> f64 {
        (self.items * self.mutations) as f64 / self.wall.as_secs_f64().max(1e-9)
    }
}

fn key(i: usize) -> ClientItemKey {
    ClientItemKey::new(format!("ss-{i:08}")).unwrap()
}

fn job_key(i: usize, n: usize) -> GroupKey {
    let jobs = (n / 100).max(50);
    GroupKey::new(format!("job-{}", i % jobs)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ss_phased_capacity_smoke() {
    let n = env_usize("SS_N", DEFAULT_N);
    let push_batch = env_usize("SS_PUSH_BATCH", 100);
    let claim_batch = env_usize("SS_CLAIM_BATCH", 100);
    assert!(n > 0 && n.is_multiple_of(claim_batch), "SS_N must be >0 and divisible by SS_CLAIM_BATCH");

    let log = tmp_log();
    let clock = Arc::new(SystemClock);
    let fw = open_sqlite(log.to_str().unwrap(), clock).expect("open_sqlite");

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
    let mut keys = Vec::with_capacity(n);
    for chunk in (0..n).step_by(push_batch) {
        let end = (chunk + push_batch).min(n);
        let items: Vec<NewItem> = (chunk..end)
            .map(|i| {
                keys.push(key(i));
                NewItem {
                    client_item_key: Some(key(i)),
                    group_key: Some(job_key(i, n)),
                    payload: Some(stub.clone()),
                    metadata: phase_meta("needs_profile"),
                    not_before: Some(now),
                    priority: Some(PriorityValue::Timestamp(now)),
                    ..Default::default()
                }
            })
            .collect();
        let c0 = Instant::now();
        let ids = fw.push_batch(&queue, items).await.expect("P1 push");
        p1_calls.record(c0.elapsed());
        assert_eq!(ids.len(), end - chunk);
    }
    let p1 = PhaseRow {
        name: "P1_ingest",
        items: n,
        mutations: 1,
        wall: t0.elapsed(),
        calls: vec![("BatchPush", p1_calls)],
    };

    // --- P2 enrich (pending BatchUpdate) ---
    let mut p2_calls = CallStats::new();
    let t0 = Instant::now();
    let mut updated = 0usize;
    for chunk in keys.chunks(claim_batch) {
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
            request_id: RequestId::new(format!("p2-{}", updated)).unwrap(),
            updates,
        };
        let c0 = Instant::now();
        let resp = fw.batch_update(&queue, req).await.expect("P2 update");
        p2_calls.record(c0.elapsed());
        let ok = resp
            .results
            .iter()
            .filter(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
            .count();
        assert_eq!(ok, chunk.len(), "P2 every entry Updated");
        updated += ok;
    }
    assert_eq!(updated, n);
    let p2 = PhaseRow {
        name: "P2_enrich",
        items: n,
        mutations: 1,
        wall: t0.elapsed(),
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
    for chunk in keys.chunks(claim_batch) {
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
            request_id: RequestId::new(format!("p3-{}", updated)).unwrap(),
            updates,
        };
        let c0 = Instant::now();
        let resp = fw.batch_update(&queue, req).await.expect("P3 update");
        p3_calls.record(c0.elapsed());
        let ok = resp
            .results
            .iter()
            .filter(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
            .count();
        assert_eq!(ok, chunk.len(), "P3 every entry Updated");
        updated += ok;
    }
    assert_eq!(updated, n);
    let p3 = PhaseRow {
        name: "P3_schedule",
        items: n,
        mutations: 1,
        wall: t0.elapsed(),
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
    let mut p4_claim = CallStats::new();
    let mut p4_fin = CallStats::new();
    let t0 = Instant::now();
    let mut completed = 0usize;
    loop {
        let c0 = Instant::now();
        let claimed = fw.claim(&queue, claim_batch, 30_000).await.expect("P4 claim");
        p4_claim.record(c0.elapsed());
        if claimed.is_empty() {
            break;
        }
        let ids: Vec<_> = claimed.iter().map(|c| c.item_id).collect();
        let n_ids = ids.len();
        let c1 = Instant::now();
        fw.complete(&queue, ids).await.expect("P4 complete");
        p4_fin.record(c1.elapsed());
        completed += n_ids;
    }
    assert_eq!(completed, n, "P4 completed all items");
    let p4 = PhaseRow {
        name: "P4_deliver",
        items: n,
        mutations: 2,
        wall: t0.elapsed(),
        calls: vec![("BatchClaim", p4_claim), ("BatchFinalize", p4_fin)],
    };

    let metrics = fw.metrics(&queue).await.expect("metrics");
    assert_eq!(metrics.pending, 0, "residual pending");
    // leased must be 0; complete == n (plus nothing from warmup — different queue)
    assert_eq!(metrics.leased, 0, "residual leased");
    assert_eq!(metrics.complete, n as u64, "complete count");

    let phases = [p1, p2, p3, p4];
    eprintln!("=== ss_phased_capacity cell=sqlite--memory workers=1 N={n} push={push_batch} claim={claim_batch} ===");
    eprintln!("phase\titems\twall_s\titems_per_s\tmutations_per_s");
    for p in &phases {
        eprintln!(
            "{}\t{}\t{:.3}\t{:.0}\t{:.0}",
            p.name,
            p.items,
            p.wall.as_secs_f64(),
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

    if std::env::var("SS_EVIDENCE").ok().as_deref() == Some("1") {
        write_evidence(&phases, n, push_batch, claim_batch);
    }

    let _ = std::fs::remove_file(&log);
    let _ = std::fs::remove_file(format!("{}-wal", log.display()));
    let _ = std::fs::remove_file(format!("{}-shm", log.display()));
}

fn write_evidence(phases: &[PhaseRow], n: usize, push_batch: usize, claim_batch: usize) {
    let utc = chrono_like_utc();
    let dir = PathBuf::from(format!("docs/perf/evidence/ss-phased/{utc}"));
    let _ = std::fs::create_dir_all(&dir);
    let mut json = String::from("{\n  \"schema\": \"ss-phased-summary/v1\",\n");
    json.push_str(&format!("  \"utc\": \"{utc}\",\n"));
    json.push_str("  \"cell\": \"sqlite--memory\",\n");
    json.push_str("  \"workers\": 1,\n");
    json.push_str(&format!("  \"n\": {n},\n"));
    json.push_str(&format!("  \"push_batch\": {push_batch},\n"));
    json.push_str(&format!("  \"claim_batch\": {claim_batch},\n"));
    json.push_str("  \"sampled_ok\": true,\n");
    json.push_str("  \"residual_eligible\": 0,\n");
    json.push_str("  \"phases\": [\n");
    for (i, p) in phases.iter().enumerate() {
        json.push_str("    {\n");
        json.push_str(&format!("      \"name\": \"{}\",\n", p.name));
        json.push_str(&format!("      \"items\": {},\n", p.items));
        json.push_str(&format!("      \"mutations\": {},\n", p.mutations));
        json.push_str(&format!("      \"wall_s\": {:.6},\n", p.wall.as_secs_f64()));
        json.push_str(&format!("      \"items_per_s\": {:.1},\n", p.items_per_s()));
        json.push_str(&format!(
            "      \"mutations_per_s\": {:.1},\n",
            p.mutations_per_s()
        ));
        json.push_str("      \"calls\": [\n");
        for (j, (op, st)) in p.calls.iter().enumerate() {
            json.push_str(&format!(
                "        {{\"op\": \"{op}\", \"p50_ms\": {:.3}, \"p95_ms\": {:.3}, \"p99_ms\": {:.3}}}",
                st.percentile_ms(50.0),
                st.percentile_ms(95.0),
                st.percentile_ms(99.0)
            ));
            if j + 1 != p.calls.len() {
                json.push(',');
            }
            json.push('\n');
        }
        json.push_str("      ]\n");
        json.push_str("    }");
        if i + 1 != phases.len() {
            json.push(',');
        }
        json.push('\n');
    }
    json.push_str("  ]\n}\n");
    let path = dir.join("summary.json");
    let _ = std::fs::write(&path, json);
    eprintln!("wrote {}", path.display());
}

fn chrono_like_utc() -> String {
    let s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{s}")
}
