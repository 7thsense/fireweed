//! TP-002 **E3 (live/S3-compatible `object_log_sqlite_projection`)** release-tier evidence harness.
//!
//! This is the live counterpart to the in-process segment-counter smoke row in
//! `pqueue-objectlog/tests/segmented_s3_substrate_tests.rs::counters_surface_emits_a_release_ledger_row`.
//! It drives the REAL production `SegmentedObjectLogSqliteBackend` (group-commit ack-after-seal +
//! snapshot-tail recovery, bead pqueue-8a76daad) over a real S3-compatible endpoint (MinIO) by injecting an
//! `S3BlobStore` through `open_with_blob_store`, and measures the three E3 bars:
//!
//!   1. **>=2 segment sizes** — the profile runs at two `SegmentConfig`s (a latency-dominant config and a
//!      size-dominant config); per config it reports the measured group-commit counters (segments sealed,
//!      objects PUT, mean/max commands per sealed segment).
//!   2. **Group-commit ack latency p95/p99 vs `segment_max_latency_ms`** — concurrent pushes co-buffer; each
//!      push's wall-clock ack latency (returns only after seal+projection-apply) is recorded, and p95/p99 are
//!      asserted bounded by the config's `segment_max_latency_ms` plus a stated tolerance (the flusher poll
//!      interval `max_latency_ms/4` + a fixed S3/SQLite seal-cost slack). The ack lands near the latency cap,
//!      not wildly over.
//!   3. **Snapshot-tail recovery within the recovery-window budget** — a resident backlog is loaded (env-
//!      scaled; 10,000,000 items in the release shape) and materialized into a durable SQLite projection,
//!      then the backend is reopened and recovery is measured via the `RecoveryStats` seam: it MUST resume at
//!      the persisted high-water (`start_seq > 0`, NOT a full-genesis replay) and replay a bounded tail
//!      (`<<` total commands, within `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS`), and the recovered state (pending
//!      item count) MUST equal the pre-restart state.
//!
//! ## ENV-GATING (mirrors the postgres E0/E1 baseline + the MinIO substrate test)
//!
//! Gated on `PQUEUE_S3_TEST_ENDPOINT`; absent it, a LOUD skip prints and the test returns green (the E3
//! evidence is DEFERRED, never a hidden/fabricated pass). The two perf lanes:
//!   - SMOKE (default, any reachable MinIO): MEASURES + reports + emits a SMOKE-tier E3 row. Bars are NOT
//!     hard-failed (a small resident over a casual endpoint is not a valid release perf environment).
//!   - PERF (`PQUEUE_PERF_ENV=1` AND the release resident shape `PQUEUE_E3_RESIDENT=10000000`): hard-asserts
//!     the bars and emits a RELEASE-tier E3 row only when they are met.
//!
//! ## Running it (orbstack networking — this host cannot reach docker PUBLISHED ports; use the container IP)
//!
//! ```text
//! docker run -d --name pqe3-minio -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! IP=$(docker inspect pqe3-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
//! # routine live smoke (small resident, fast):
//! PQUEUE_S3_TEST_ENDPOINT="http://$IP:9000" \
//!     cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests -- --nocapture
//! # the full TP-002 E3 RELEASE shape (10M-item snapshot-tail recovery; hard-fails the bars):
//! PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000 PQUEUE_S3_TEST_ENDPOINT="http://$IP:9000" \
//!     cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests -- --nocapture
//! ```
//!
//! Optional overrides: `PQUEUE_S3_TEST_BUCKET` (default `pqueue-test`), `PQUEUE_S3_TEST_ACCESS_KEY` /
//! `PQUEUE_S3_TEST_SECRET_KEY` (default `minioadmin`), `PQUEUE_E3_LOAD_BATCH` (items per push command during
//! the recovery-load phase, default 1000), `PQUEUE_E3_ACK_PUSHES` (pushes per ack-latency config, default
//! 2000), `PQUEUE_E3_ACK_CONCURRENCY` (concurrent push tasks, default 64), `PQUEUE_E3_LOAD_CONCURRENCY`
//! (concurrent recovery-load tasks, default 8).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use pqueue_core::{
    EligibilityPolicy, Metadata, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushPort, PushSpec, QueueKey};
use pqueue_objectlog::segmented::{BlobStore, S3BlobStore, SegmentConfig};
use pqueue_server::SegmentedObjectLogSqliteBackend;

/// Fixed seal-cost slack (ms) added to the latency-cap-derived ack bar: covers one segment-object PUT + one
/// create-only manifest PUT + the recover-manifest LIST/GET round-trips over the hand-rolled SigV4 S3 client,
/// plus the per-batch SQLite projection apply and async scheduling jitter. The ack of a group-commit seal is
/// expected to land near the latency cap + this bounded seal cost, never wildly over.
const ACK_SEAL_SLACK_MS: f64 = 750.0;

/// The release resident shape: the full TP-002 E3 10M-item snapshot-tail recovery measurement.
const RELEASE_RESIDENT: u64 = 10_000_000;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
        terminal_retention_ms: 600_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    }
}

fn spec(payload: &str) -> PushSpec {
    PushSpec {
        client_item_key: None,
        priority: None,
        not_before: None,
        group_key: None,
        payload: Some(Bytes::from(payload.to_string())),
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
    }
}

fn ts() -> UtcTimestamp {
    UtcTimestamp::new(1_700_000_000, 0).unwrap()
}

fn pct(latencies_ms: &mut [f64], p: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (((latencies_ms.len() as f64) * p).ceil() as usize)
        .saturating_sub(1)
        .min(latencies_ms.len() - 1);
    latencies_ms[idx]
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

struct S3Env {
    endpoint: String,
    bucket: String,
    access: String,
    secret: String,
}

impl S3Env {
    fn store(&self) -> Arc<dyn BlobStore> {
        let s3 = S3BlobStore::new(
            &self.endpoint,
            &self.bucket,
            &self.access,
            &self.secret,
            "us-east-1",
        )
        .expect("build S3 client");
        Arc::new(s3)
    }
}

/// A unique scratch SQLite projection path under the system temp dir (removed at the end of the run).
fn projection_path(label: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir()
        .join(format!(
            "pqueue-e3-{label}-{}-{n}-{nanos}.db",
            std::process::id()
        ))
        .to_str()
        .expect("utf8 temp path")
        .to_string()
}

/// One named segment-size profile + its measured ack-latency/counters results.
struct AckResult {
    name: &'static str,
    target_bytes: usize,
    max_latency_ms: u64,
    segments_sealed: u64,
    objects_put: u64,
    commands_committed: u64,
    mean_batch: f64,
    max_batch: usize,
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    ack_bar_ms: f64,
    bar_met: bool,
}

/// Drive `pushes` single-item pushes through the segmented backend over MinIO at `concurrency`, with the
/// flusher running, recording each push's ack latency. Reports the group-commit counters + ack percentiles.
async fn run_ack_config(
    s3: &S3Env,
    name: &'static str,
    cfg: SegmentConfig,
    pushes: u64,
    concurrency: u64,
) -> AckResult {
    let qid = format!("e3ack-{name}-{}", std::process::id());
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let proj = projection_path(&format!("ack-{name}"));

    let backend = Arc::new(
        SegmentedObjectLogSqliteBackend::open_with_blob_store(s3.store(), &proj, cfg)
            .expect("open segmented backend over S3"),
    );
    backend.create_queue(def).await.expect("create queue");
    let flusher = backend.spawn_flusher();

    let per_task = pushes.div_ceil(concurrency);
    let mut handles = Vec::new();
    for t in 0..concurrency {
        let backend = backend.clone();
        let shard = shard.clone();
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(per_task as usize);
            for i in 0..per_task {
                let start = Instant::now();
                backend
                    .push(&shard, vec![spec(&format!("t{t}-{i}"))], ts(), None)
                    .await
                    .expect("push acked after seal");
                lat.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            lat
        }));
    }
    let mut latencies = Vec::with_capacity(pushes as usize);
    for h in handles {
        latencies.extend(h.await.expect("ack task joined"));
    }
    flusher.abort();

    let c = backend.segment_counters();
    let ack_p95 = pct(&mut latencies, 0.95);
    let ack_p99 = pct(&mut latencies, 0.99);
    let ack_bar_ms =
        cfg.max_latency_ms as f64 + (cfg.max_latency_ms as f64 / 4.0) + ACK_SEAL_SLACK_MS;
    let bar_met = ack_p95 <= ack_bar_ms && ack_p99 <= ack_bar_ms;

    let _ = std::fs::remove_file(&proj);

    AckResult {
        name,
        target_bytes: cfg.target_bytes,
        max_latency_ms: cfg.max_latency_ms,
        segments_sealed: c.segments_sealed,
        objects_put: c.objects_put,
        commands_committed: c.commands_committed,
        mean_batch: round3(c.mean_batch_size()),
        max_batch: c.max_batch_size(),
        ack_p95_ms: round3(ack_p95),
        ack_p99_ms: round3(ack_p99),
        ack_bar_ms: round3(ack_bar_ms),
        bar_met,
    }
}

struct RecoveryResult {
    resident: u64,
    load_batch: u64,
    command_count: u64,
    total_commands: u64,
    start_seq: u64,
    tail_replayed: u64,
    snapshot_used: bool,
    recovery_max_tail: u64,
    recovery_wall_ms: f64,
    pending_after: u64,
    bar_met: bool,
}

/// Push with a bounded retry on the substrate's documented same-epoch manifest-CAS `Conflict` (the seal doc
/// says such a transient race is "surfaced as a conflict so it is not mistaken for an ack" and the caller
/// retries). After the S3 `list` pagination fix this is rare, but a bounded retry keeps a long load robust.
async fn push_with_retry(
    backend: &SegmentedObjectLogSqliteBackend,
    shard: &QueueKey,
    items: Vec<PushSpec>,
) {
    let mut attempt = 0u64;
    loop {
        match backend.push(shard, items.clone(), ts(), None).await {
            Ok(_) => return,
            Err(EngineError::Conflict) if attempt < 16 => {
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_millis(20 * attempt)).await;
            }
            Err(e) => panic!("push failed after {attempt} retries: {e:?}"),
        }
    }
}

/// Load `resident` items (pushes of `load_batch` items each) into a SQLite projection over MinIO, then reopen
/// and measure snapshot-tail recovery via the `RecoveryStats` seam (bead pqueue-8a76daad).
async fn run_recovery(s3: &S3Env, resident: u64, load_batch: u64) -> RecoveryResult {
    let qid = format!("e3rec-{}", std::process::id());
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let proj = projection_path("recovery");
    // A large byte target + a generous latency cap so the bulk load seals FEW, LARGE segments: concurrent
    // loaders fill the 8 MiB buffer fast (size-triggered seals), and the 10 s cap means even a load stall
    // produces only a handful of latency-sealed segments. This keeps the per-queue manifest small (the seal
    // cost amortizes over a big group-commit batch — the whole point of the segmented substrate) rather than
    // one tiny segment per push.
    let cfg = SegmentConfig::new(8_388_608, 10_000).unwrap();
    let load_concurrency = env_u64("PQUEUE_E3_LOAD_CONCURRENCY", 8).max(1);

    let (command_count, total_commands, pending_loaded) = {
        let backend = Arc::new(
            SegmentedObjectLogSqliteBackend::open_with_blob_store(s3.store(), &proj, cfg)
                .expect("open backend for load"),
        );
        backend
            .create_queue(def.clone())
            .await
            .expect("create queue");
        let flusher = backend.spawn_flusher();

        // Concurrent loaders, each owning a disjoint id range, co-buffer into shared group-commit segments.
        let share = resident.div_ceil(load_concurrency);
        let mut handles = Vec::new();
        for w in 0..load_concurrency {
            let start = w * share;
            if start >= resident {
                break;
            }
            let end = (start + share).min(resident);
            let backend = backend.clone();
            let shard = shard.clone();
            handles.push(tokio::spawn(async move {
                let mut commands = 0u64;
                let mut id = start;
                while id < end {
                    let n = (end - id).min(load_batch);
                    let items: Vec<PushSpec> =
                        (0..n).map(|k| spec(&format!("i{}", id + k))).collect();
                    push_with_retry(&backend, &shard, items).await;
                    id += n;
                    commands += 1;
                }
                commands
            }));
        }
        let mut command_count = 0u64;
        for h in handles {
            command_count += h.await.expect("load task joined");
        }

        // Let the flusher seal any trailing buffered command so the projection is fully caught up before we
        // snapshot (a clean shutdown). Poll until pending == resident or a generous deadline (> the cap).
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let pending = backend.metrics(&shard).await.unwrap().pending;
            if pending >= resident || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        flusher.abort();
        let total_commands = backend.segment_counters().commands_committed;
        let pending = backend.metrics(&shard).await.unwrap().pending;
        (command_count, total_commands, pending)
    };
    assert_eq!(
        pending_loaded, resident,
        "every loaded item must be materialized + durable before the snapshot"
    );

    // Reopen on the SAME bucket + SAME SQLite projection: create_queue triggers snapshot-tail recovery.
    let backend2 = Arc::new(
        SegmentedObjectLogSqliteBackend::open_with_blob_store(s3.store(), &proj, cfg)
            .expect("reopen backend"),
    );
    let t = Instant::now();
    backend2
        .create_queue(def.clone())
        .await
        .expect("recover queue");
    let recovery_wall_ms = t.elapsed().as_secs_f64() * 1000.0;

    let stats = backend2.recovery_stats(&shard).expect("recovery ran");
    let pending_after = backend2.metrics(&shard).await.unwrap().pending;
    let recovery_max_tail = env_u64("PQUEUE_RECOVERY_MAX_TAIL_COMMANDS", 1_000_000);

    // The recovery bar: resumed from the persisted snapshot high-water (NOT genesis), replayed a tail within
    // the documented recovery-window budget AND strictly less than the total committed log (proving it did
    // not re-replay genesis), and the recovered pending state equals the pre-restart state.
    let bar_met = stats.snapshot_used
        && stats.start_seq > 0
        && stats.tail_replayed <= recovery_max_tail
        && (stats.tail_replayed as u128) < (total_commands as u128)
        && pending_after == resident;

    let _ = std::fs::remove_file(&proj);

    RecoveryResult {
        resident,
        load_batch,
        command_count,
        total_commands,
        start_seq: stats.start_seq,
        tail_replayed: stats.tail_replayed,
        snapshot_used: stats.snapshot_used,
        recovery_max_tail,
        recovery_wall_ms: round3(recovery_wall_ms),
        pending_after,
        bar_met,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_object_log_e3_live_tests() {
    let Ok(endpoint) = std::env::var("PQUEUE_S3_TEST_ENDPOINT") else {
        eprintln!(
            "\n================================================================\n\
             TP-002 E3 LIVE OBJECT-LOG HARNESS SKIPPED (performance_object_log_e3_live_tests)\n\
             set PQUEUE_S3_TEST_ENDPOINT=http://<container-ip>:9000 to run it.\n\
             (this host cannot reach docker PUBLISHED ports; use the MinIO container IP)\n\
             The E3 ack-latency + snapshot-tail-recovery evidence is DEFERRED, not a hidden pass.\n\
             ================================================================\n"
        );
        return;
    };
    let s3 = S3Env {
        endpoint,
        bucket: std::env::var("PQUEUE_S3_TEST_BUCKET").unwrap_or_else(|_| "pqueue-test".into()),
        access: std::env::var("PQUEUE_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into()),
        secret: std::env::var("PQUEUE_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into()),
    };
    // Ensure the bucket exists once up front (a signing failure surfaces here, loudly).
    S3BlobStore::new(
        &s3.endpoint,
        &s3.bucket,
        &s3.access,
        &s3.secret,
        "us-east-1",
    )
    .expect("build S3 client")
    .create_bucket()
    .expect("create/ensure bucket");

    let perf_env = std::env::var("PQUEUE_PERF_ENV").is_ok();
    let resident = env_u64("PQUEUE_E3_RESIDENT", 4_000);
    let load_batch = env_u64("PQUEUE_E3_LOAD_BATCH", 1_000).max(1);
    let ack_pushes = env_u64("PQUEUE_E3_ACK_PUSHES", 2_000).max(1);
    let ack_concurrency = env_u64("PQUEUE_E3_ACK_CONCURRENCY", 64).max(1);

    // ---- Bar 1 + 2: >=2 segment sizes, ack latency p95/p99 vs the latency cap, per config ----
    // Config A: LATENCY-dominant (a huge byte target → only the flusher's latency cap seals; ack should land
    // near the cap). Config B: SIZE-dominant (a tiny byte target → a size seal fires inside enqueue under
    // concurrency, so ack is bounded WELL under its (generous) latency cap).
    let cfg_latency = SegmentConfig::new(8_388_608, 50).unwrap();
    let cfg_size = SegmentConfig::new(4_096, 1_000).unwrap();
    let ack_a = run_ack_config(&s3, "latency", cfg_latency, ack_pushes, ack_concurrency).await;
    let ack_b = run_ack_config(&s3, "size", cfg_size, ack_pushes, ack_concurrency).await;

    // ---- Bar 3: snapshot-tail recovery within the recovery-window budget ----
    let rec = run_recovery(&s3, resident, load_batch).await;

    let all_bars_met = ack_a.bar_met && ack_b.bar_met && rec.bar_met;
    let release_shape = resident >= RELEASE_RESIDENT;

    // ----- Report -----
    println!(
        "\nTP-002 E3 live object_log_sqlite_projection over MinIO ({}) — perf_env={perf_env}, resident={resident}:",
        s3.endpoint
    );
    for a in [&ack_a, &ack_b] {
        println!(
            "  [{:7}] target_bytes={:>9} max_latency_ms={:>5}  segments_sealed={:>6} objects_put={:>6} \
             commands={:>6} mean_batch={:>7.1} max_batch={:>5}  ack_p95={:>8.2}ms ack_p99={:>8.2}ms \
             (bar<={:.2}ms) -> {}",
            a.name,
            a.target_bytes,
            a.max_latency_ms,
            a.segments_sealed,
            a.objects_put,
            a.commands_committed,
            a.mean_batch,
            a.max_batch,
            a.ack_p95_ms,
            a.ack_p99_ms,
            a.ack_bar_ms,
            if a.bar_met { "PASS" } else { "OVER" }
        );
    }
    println!(
        "  [recover] resident={} load_batch={} commands_loaded={} total_committed={} \
         start_seq={} tail_replayed={} snapshot_used={} (budget {}) wall={:.1}ms pending_after={} -> {}",
        rec.resident,
        rec.load_batch,
        rec.command_count,
        rec.total_commands,
        rec.start_seq,
        rec.tail_replayed,
        rec.snapshot_used,
        rec.recovery_max_tail,
        rec.recovery_wall_ms,
        rec.pending_after,
        if rec.bar_met { "PASS" } else { "FAIL" }
    );
    if !release_shape {
        println!(
            "  NOTE: resident {resident} < release shape {RELEASE_RESIDENT}; the full TP-002 E3 release \
             measurement is PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000 (this run is a smaller resident)."
        );
    }
    if !perf_env && !all_bars_met {
        eprintln!(
            "NOTE: an E3 bar was not met in this (non-perf) environment — recorded as SMOKE evidence. The \
             bars are hard-enforced only under PQUEUE_PERF_ENV + the release resident shape."
        );
    }

    // In a designated perf env at the release shape, the bars are REQUIRED (hard fail).
    if perf_env && release_shape {
        assert!(
            ack_a.bar_met,
            "E3 ack latency bar (latency config) not met: p95={}ms p99={}ms (bar {}ms)",
            ack_a.ack_p95_ms, ack_a.ack_p99_ms, ack_a.ack_bar_ms
        );
        assert!(
            ack_b.bar_met,
            "E3 ack latency bar (size config) not met: p95={}ms p99={}ms (bar {}ms)",
            ack_b.ack_p95_ms, ack_b.ack_p99_ms, ack_b.ack_bar_ms
        );
        assert!(
            rec.bar_met,
            "E3 snapshot-tail recovery bar not met: start_seq={} tail_replayed={} snapshot_used={} \
             pending_after={} (resident {})",
            rec.start_seq, rec.tail_replayed, rec.snapshot_used, rec.pending_after, rec.resident
        );
    }

    // ----- Emit ONE E3 ledger row from the REAL measured values -----
    // RELEASE-tier only when a perf env at the release resident shape actually met every bar; otherwise
    // SMOKE (recorded, gate-visible, but never satisfies the release E3 requirement).
    let tier = if perf_env && release_shape && all_bars_met {
        "release"
    } else {
        "smoke"
    }
    .to_string();
    let scale = if release_shape {
        "release".to_string()
    } else {
        format!("resident={resident}")
    };
    let environment = format!(
        "live object_log_sqlite_projection (SegmentedObjectLogSqliteBackend) over S3-compatible MinIO at {}, \
         single deployment, resident={resident}, perf_env={perf_env}; the full TP-002 E3 shape is \
         PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000",
        s3.endpoint
    );

    let mut values: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for a in [&ack_a, &ack_b] {
        let p = a.name;
        values.insert(
            format!("cfg_{p}_target_bytes"),
            serde_json::json!(a.target_bytes),
        );
        values.insert(
            format!("cfg_{p}_max_latency_ms"),
            serde_json::json!(a.max_latency_ms),
        );
        values.insert(
            format!("cfg_{p}_segments_sealed"),
            serde_json::json!(a.segments_sealed),
        );
        values.insert(
            format!("cfg_{p}_objects_put"),
            serde_json::json!(a.objects_put),
        );
        values.insert(
            format!("cfg_{p}_commands_committed"),
            serde_json::json!(a.commands_committed),
        );
        values.insert(
            format!("cfg_{p}_mean_commands_per_segment"),
            serde_json::json!(a.mean_batch),
        );
        values.insert(
            format!("cfg_{p}_max_group_commit_batch"),
            serde_json::json!(a.max_batch),
        );
        values.insert(
            format!("cfg_{p}_ack_p95_ms"),
            serde_json::json!(a.ack_p95_ms),
        );
        values.insert(
            format!("cfg_{p}_ack_p99_ms"),
            serde_json::json!(a.ack_p99_ms),
        );
        values.insert(
            format!("cfg_{p}_ack_bar_ms"),
            serde_json::json!(a.ack_bar_ms),
        );
        values.insert(format!("cfg_{p}_ack_bar_met"), serde_json::json!(a.bar_met));
    }
    values.insert("segment_configs".into(), serde_json::json!(2));
    values.insert("recovery_resident".into(), serde_json::json!(rec.resident));
    values.insert(
        "recovery_command_count".into(),
        serde_json::json!(rec.command_count),
    );
    values.insert(
        "recovery_total_commands".into(),
        serde_json::json!(rec.total_commands),
    );
    values.insert(
        "recovery_start_seq".into(),
        serde_json::json!(rec.start_seq),
    );
    values.insert(
        "recovery_tail_replayed".into(),
        serde_json::json!(rec.tail_replayed),
    );
    values.insert(
        "recovery_snapshot_used".into(),
        serde_json::json!(rec.snapshot_used),
    );
    values.insert(
        "recovery_max_tail_budget".into(),
        serde_json::json!(rec.recovery_max_tail),
    );
    values.insert(
        "recovery_wall_ms".into(),
        serde_json::json!(rec.recovery_wall_ms),
    );
    values.insert(
        "recovery_pending_after".into(),
        serde_json::json!(rec.pending_after),
    );
    values.insert("recovery_bar_met".into(), serde_json::json!(rec.bar_met));
    values.insert("bars_met".into(), serde_json::json!(all_bars_met));

    let row = pqueue_release::LedgerRow {
        suite: "performance_object_log_e3_live_tests".into(),
        command: "PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000 PQUEUE_S3_TEST_ENDPOINT=http://<minio-ip>:9000 cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale,
        seed: 0,
        environment,
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "E3: >=2 segment sizes; group-commit ack p95&p99 <= segment_max_latency_ms + max_latency_ms/4 + 750ms seal slack; 10M-item SQLite projection rebuilt via snapshot+bounded-tail (start_seq>0, tail<=PQUEUE_RECOVERY_MAX_TAIL_COMMANDS, tail<total) with recovered pending == resident".into(),
        evidence_tier: tier.clone(),
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["E3".into()],
            values,
        },
    };
    let path = pqueue_release::ledger_path(
        env!("CARGO_MANIFEST_DIR"),
        "performance_object_log_e3_live_tests",
    );
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, &row).expect("emit E3 ledger row");
    let summary =
        pqueue_release::verify_ledger(&path, true).expect("emitted E3 row validates strict");
    let seen = if tier == "smoke" {
        summary.smoke_evidence_ids.contains("E3")
    } else {
        summary.evidence_ids.contains("E3")
    };
    assert!(
        seen,
        "emitted row must carry the E3 evidence id ({tier} tier)"
    );
    println!("  emitted {tier}-tier E3 ledger row -> {}", path.display());
}
