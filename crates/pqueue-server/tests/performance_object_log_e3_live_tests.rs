//! TP-002 **E3 (live/S3-compatible object-log projection matrix)** release-tier evidence harness.
//!
//! This is the live counterpart to the in-process segment-counter smoke row in
//! `pqueue-objectlog/tests/segmented_s3_substrate_tests.rs::counters_surface_emits_a_release_ledger_row`.
//! It drives the REAL production segmented object-log backends over a real S3-compatible endpoint (MinIO)
//! by injecting an `S3BlobStore` through `open_with_blob_store`, and measures the E3 bars:
//!
//!   1. **>=4 commit-latency bounds** — each profile runs at `1ms`, `5ms`, `20ms`, and `100ms`
//!      `SegmentConfig`s; per bound it reports the measured group-commit counters (segments sealed,
//!      objects PUT, mean/max commands per sealed segment) plus throughput.
//!   2. **Group-commit ack latency p50/p95/p99 vs the configured budget** — concurrent pushes co-buffer; each
//!      push's wall-clock ack latency (returns only after seal+projection-apply) is recorded, and p50/p95/p99
//!      are asserted bounded by the config's `segment_max_latency_ms` plus a stated tolerance (the flusher poll
//!      interval `max_latency_ms/4` + a fixed seal-cost slack). The ack lands near the latency cap, not
//!      wildly over.
//!   3. **Snapshot-tail recovery within the recovery-window budget** — the SQLite projection variant loads a
//!      resident backlog (env-scaled; 10,000,000 items in the release shape), materializes it, then reopens and
//!      measures recovery via the `recovery_stats` seam: it MUST resume at the persisted high-water
//!      (`start_seq > 0`, NOT a full-genesis replay) and replay a bounded tail (`<<` total commands, within
//!      `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS`), and the recovered state (pending item count) MUST equal the
//!      pre-restart state. The in-memory projection variant records `recovery_excluded=true` because it does
//!      not expose the SQLite reopen telemetry seam.
//!
//! ## ENV-GATING (mirrors the postgres E0/E1 baseline + the MinIO substrate test)
//!
//! Gated on `PQUEUE_S3_TEST_ENDPOINT`; absent it, a LOUD skip prints and the test returns green (the E3
//! evidence is DEFERRED, never a hidden/fabricated pass). The two perf lanes:
//!   - SMOKE (default, any reachable MinIO): MEASURES + reports + emits SMOKE-tier rows. Bars are NOT
//!     hard-failed (a small resident over a casual endpoint is not a valid release perf environment).
//!   - PERF (`PQUEUE_PERF_ENV=1` AND the release resident shape `PQUEUE_E3_RESIDENT=10000000`): hard-asserts
//!     the bars and emits RELEASE-tier rows only when they are met.
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
//! 100000), `PQUEUE_E3_ACK_CONCURRENCY` (concurrent push tasks, default 384), `PQUEUE_E3_LOAD_CONCURRENCY`
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
use pqueue_objectlog::segmented::{BlobStore, S3BlobStore, SegmentConfig, SegmentCounters};
use pqueue_server::{SegmentedObjectLogInMemoryBackend, SegmentedObjectLogSqliteBackend};

/// Fixed seal-cost slack (ms) added to the latency-cap-derived ack bar: covers one segment-object PUT + one
/// create-only manifest PUT + the recover-manifest LIST/GET round-trips over the hand-rolled SigV4 S3 client,
/// plus the per-batch SQLite projection apply and async scheduling jitter. The ack of a group-commit seal is
/// expected to land near the latency cap + this bounded seal cost, never wildly over.
const ACK_SEAL_SLACK_MS: f64 = 750.0;

/// The release resident shape: the full TP-002 E3 10M-item snapshot-tail recovery measurement.
const RELEASE_RESIDENT: u64 = 10_000_000;

const E3_THROUGHPUT_FLOOR_PER_SEC: f64 = 2777.78;

const E3_BOUND_CONFIGS: [BoundConfig; 4] = [
    BoundConfig {
        label: "1ms",
        target_bytes: 8_388_608,
        max_latency_ms: 1,
    },
    BoundConfig {
        label: "5ms",
        target_bytes: 8_388_608,
        max_latency_ms: 5,
    },
    BoundConfig {
        label: "20ms",
        target_bytes: 8_388_608,
        max_latency_ms: 20,
    },
    BoundConfig {
        label: "100ms",
        target_bytes: 8_388_608,
        max_latency_ms: 100,
    },
];

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
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
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
        entity: None,
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

#[derive(Clone, Copy)]
struct BoundConfig {
    label: &'static str,
    target_bytes: usize,
    max_latency_ms: u64,
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

trait E3Flusher {
    fn spawn_background_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()>;
}

impl E3Flusher for SegmentedObjectLogSqliteBackend {
    fn spawn_background_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        SegmentedObjectLogSqliteBackend::spawn_flusher(self)
    }
}

impl E3Flusher for SegmentedObjectLogInMemoryBackend {
    fn spawn_background_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        SegmentedObjectLogInMemoryBackend::spawn_flusher(self)
    }
}

struct E3ProfileSpec {
    backend_profile: &'static str,
}

const E3_PROFILE_SPECS: [E3ProfileSpec; 2] = [
    E3ProfileSpec {
        backend_profile: "object_log_inmemory_projection",
    },
    E3ProfileSpec {
        backend_profile: "object_log_sqlite_projection",
    },
];

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

/// One bound measurement inside one backend-profile run.
struct AckResult {
    label: &'static str,
    target_bytes: usize,
    max_latency_ms: u64,
    segments_sealed: u64,
    objects_put: u64,
    commands_committed: u64,
    mean_batch: f64,
    max_batch: usize,
    throughput_per_s: f64,
    throughput_floor_met: bool,
    ack_p50_ms: f64,
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    ack_bar_ms: f64,
    latency_bar_met: bool,
    bar_met: bool,
}

struct ProfileRun {
    backend_profile: &'static str,
    projection_label: &'static str,
    ack_results: Vec<AckResult>,
    recovery: Option<RecoveryResult>,
    wall_ms: f64,
    bars_met: bool,
}

trait E3Backend:
    ControlPlaneStore + PushPort + ProjectionRead + E3Flusher + Send + Sync + 'static
{
    fn snapshot_segment_counters(&self) -> SegmentCounters;
}

impl E3Backend for SegmentedObjectLogSqliteBackend {
    fn snapshot_segment_counters(&self) -> SegmentCounters {
        SegmentedObjectLogSqliteBackend::segment_counters(self)
    }
}

impl E3Backend for SegmentedObjectLogInMemoryBackend {
    fn snapshot_segment_counters(&self) -> SegmentCounters {
        SegmentedObjectLogInMemoryBackend::segment_counters(self)
    }
}

trait E3RecoveryProbe {
    fn recovery_probe(&self, shard: &QueueKey) -> Option<(u64, u64, bool)>;
}

impl E3RecoveryProbe for SegmentedObjectLogSqliteBackend {
    fn recovery_probe(&self, shard: &QueueKey) -> Option<(u64, u64, bool)> {
        self.recovery_stats(shard)
            .map(|stats| (stats.start_seq, stats.tail_replayed, stats.snapshot_used))
    }
}

impl E3RecoveryProbe for SegmentedObjectLogInMemoryBackend {
    fn recovery_probe(&self, _shard: &QueueKey) -> Option<(u64, u64, bool)> {
        None
    }
}

/// Drive `pushes` single-item pushes through one backend/profile over MinIO at `concurrency`, with the
/// flusher running, recording each push's ack latency and end-to-end throughput.
async fn run_ack_config<B, F>(
    s3: &S3Env,
    profile: &'static str,
    bound: BoundConfig,
    pushes: u64,
    concurrency: u64,
    open: F,
) -> AckResult
where
    B: E3Backend,
    F: Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> pqueue_engine::EngineResult<B>,
{
    let qid = format!("e3ack-{profile}-{}-{}", bound.label, std::process::id());
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let proj = projection_path(&format!("ack-{profile}-{}", bound.label));
    let cfg = SegmentConfig::new(bound.target_bytes, bound.max_latency_ms).unwrap();

    let backend = Arc::new(open(s3.store(), &proj, cfg).expect("open segmented backend over S3"));
    backend.create_queue(def).await.expect("create queue");
    let flusher = backend.spawn_background_flusher();
    let started = Instant::now();

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
    let wall_s = started.elapsed().as_secs_f64();
    let _wall_ms = wall_s * 1000.0;

    let c = backend.snapshot_segment_counters();
    let throughput_per_s = pushes as f64 / wall_s.max(f64::MIN_POSITIVE);
    let ack_p50 = pct(&mut latencies, 0.50);
    let ack_p95 = pct(&mut latencies, 0.95);
    let ack_p99 = pct(&mut latencies, 0.99);
    let ack_bar_ms =
        cfg.max_latency_ms as f64 + (cfg.max_latency_ms as f64 / 4.0) + ACK_SEAL_SLACK_MS;
    let throughput_floor_met = throughput_per_s >= E3_THROUGHPUT_FLOOR_PER_SEC;
    let latency_bar_met = ack_p50 <= ack_bar_ms && ack_p95 <= ack_bar_ms && ack_p99 <= ack_bar_ms;
    let bar_met = throughput_floor_met && latency_bar_met;

    let _ = std::fs::remove_file(&proj);

    AckResult {
        label: bound.label,
        target_bytes: cfg.target_bytes,
        max_latency_ms: cfg.max_latency_ms,
        segments_sealed: c.segments_sealed,
        objects_put: c.objects_put,
        commands_committed: c.commands_committed,
        mean_batch: round3(c.mean_batch_size()),
        max_batch: c.max_batch_size(),
        throughput_per_s: round3(throughput_per_s),
        throughput_floor_met,
        ack_p50_ms: round3(ack_p50),
        ack_p95_ms: round3(ack_p95),
        ack_p99_ms: round3(ack_p99),
        ack_bar_ms: round3(ack_bar_ms),
        latency_bar_met,
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
async fn push_with_retry<B: PushPort>(backend: &B, shard: &QueueKey, items: Vec<PushSpec>) {
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
async fn run_recovery<B, F>(
    s3: &S3Env,
    profile: &'static str,
    resident: u64,
    load_batch: u64,
    open: F,
) -> RecoveryResult
where
    B: E3Backend + E3RecoveryProbe,
    F: Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> pqueue_engine::EngineResult<B>,
{
    let qid = format!("e3rec-{profile}-{}", std::process::id());
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let proj = projection_path(&format!("recovery-{profile}"));
    // A large byte target + a generous latency cap so the bulk load seals FEW, LARGE segments: concurrent
    // loaders fill the 8 MiB buffer fast (size-triggered seals), and the 10 s cap means even a load stall
    // produces only a handful of latency-sealed segments. This keeps the per-queue manifest small (the seal
    // cost amortizes over a big group-commit batch — the whole point of the segmented substrate) rather than
    // one tiny segment per push.
    let cfg = SegmentConfig::new(8_388_608, 10_000).unwrap();
    let load_concurrency = env_u64("PQUEUE_E3_LOAD_CONCURRENCY", 8).max(1);

    let (command_count, total_commands, pending_loaded) = {
        let backend = Arc::new(open(s3.store(), &proj, cfg).expect("open backend for load"));
        backend
            .create_queue(def.clone())
            .await
            .expect("create queue");
        let flusher = backend.spawn_background_flusher();

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
                    push_with_retry(backend.as_ref(), &shard, items).await;
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
        let total_commands = backend.snapshot_segment_counters().commands_committed;
        let pending = backend.metrics(&shard).await.unwrap().pending;
        (command_count, total_commands, pending)
    };
    assert_eq!(
        pending_loaded, resident,
        "every loaded item must be materialized + durable before the snapshot"
    );

    // Reopen on the SAME bucket + SAME SQLite projection: create_queue triggers snapshot-tail recovery.
    let backend2 = Arc::new(open(s3.store(), &proj, cfg).expect("reopen backend"));
    let t = Instant::now();
    backend2
        .create_queue(def.clone())
        .await
        .expect("recover queue");
    let recovery_wall_ms = t.elapsed().as_secs_f64() * 1000.0;

    let pending_after = backend2.metrics(&shard).await.unwrap().pending;
    let recovery_max_tail = env_u64("PQUEUE_RECOVERY_MAX_TAIL_COMMANDS", 1_000_000);
    let Some((start_seq, tail_replayed, snapshot_used)) = backend2.recovery_probe(&shard) else {
        let _ = std::fs::remove_file(&proj);
        return RecoveryResult {
            resident,
            load_batch,
            command_count,
            total_commands,
            start_seq: 0,
            tail_replayed: 0,
            snapshot_used: false,
            recovery_max_tail,
            recovery_wall_ms: round3(recovery_wall_ms),
            pending_after,
            bar_met: false,
        };
    };

    // The recovery bar: resumed from the persisted snapshot high-water (NOT genesis), replayed a tail within
    // the documented recovery-window budget AND strictly less than the total committed log (proving it did
    // not re-replay genesis), and the recovered pending state equals the pre-restart state.
    let bar_met = snapshot_used
        && start_seq > 0
        && tail_replayed <= recovery_max_tail
        && (tail_replayed as u128) < (total_commands as u128)
        && pending_after == resident;

    let _ = std::fs::remove_file(&proj);

    RecoveryResult {
        resident,
        load_batch,
        command_count,
        total_commands,
        start_seq,
        tail_replayed,
        snapshot_used,
        recovery_max_tail,
        recovery_wall_ms: round3(recovery_wall_ms),
        pending_after,
        bar_met,
    }
}

async fn run_profile_run<B, F>(
    s3: &S3Env,
    profile: &'static str,
    projection_label: &'static str,
    resident: u64,
    load_batch: u64,
    ack_pushes: u64,
    ack_concurrency: u64,
    include_recovery: bool,
    open: F,
) -> ProfileRun
where
    B: E3Backend + E3RecoveryProbe,
    F: Copy + Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> pqueue_engine::EngineResult<B>,
{
    let started = Instant::now();
    let mut ack_results = Vec::with_capacity(E3_BOUND_CONFIGS.len());
    for bound in E3_BOUND_CONFIGS {
        ack_results.push(
            run_ack_config::<B, _>(s3, profile, bound, ack_pushes, ack_concurrency, open).await,
        );
    }
    let recovery = if include_recovery {
        Some(run_recovery::<B, _>(s3, profile, resident, load_batch, open).await)
    } else {
        None
    };
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    let bars_met = ack_results.iter().all(|result| result.bar_met)
        && recovery.as_ref().is_none_or(|r| r.bar_met);
    ProfileRun {
        backend_profile: profile,
        projection_label,
        ack_results,
        recovery,
        wall_ms: round3(wall_ms),
        bars_met,
    }
}

fn validate_e3_profile_matrix(runs: &[ProfileRun], require_bars: bool) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let mut seen_profiles = std::collections::BTreeSet::new();
    for run in runs {
        if !seen_profiles.insert(run.backend_profile) {
            errors.push(format!("duplicate profile {}", run.backend_profile));
        }
        if run.ack_results.len() != E3_BOUND_CONFIGS.len() {
            errors.push(format!(
                "profile {} has {} bounds; expected {}",
                run.backend_profile,
                run.ack_results.len(),
                E3_BOUND_CONFIGS.len()
            ));
        }
        let mut seen_bounds = std::collections::BTreeSet::new();
        for result in &run.ack_results {
            if !seen_bounds.insert(result.label) {
                errors.push(format!(
                    "profile {} has duplicate bound {}",
                    run.backend_profile, result.label
                ));
            }
            if result.throughput_per_s < E3_THROUGHPUT_FLOOR_PER_SEC {
                errors.push(format!(
                    "profile {} bound {} throughput {:.3} < floor {:.2}",
                    run.backend_profile,
                    result.label,
                    result.throughput_per_s,
                    E3_THROUGHPUT_FLOOR_PER_SEC
                ));
            }
            if !result.throughput_floor_met {
                errors.push(format!(
                    "profile {} bound {} did not record throughput_floor_met=true",
                    run.backend_profile, result.label
                ));
            }
            if result.ack_p50_ms > result.ack_bar_ms
                || result.ack_p95_ms > result.ack_bar_ms
                || result.ack_p99_ms > result.ack_bar_ms
            {
                errors.push(format!(
                    "profile {} bound {} latency over budget (p50/p95/p99={} / {} / {} > {})",
                    run.backend_profile,
                    result.label,
                    result.ack_p50_ms,
                    result.ack_p95_ms,
                    result.ack_p99_ms,
                    result.ack_bar_ms
                ));
            }
            if !result.latency_bar_met {
                errors.push(format!(
                    "profile {} bound {} did not record latency_bar_met=true",
                    run.backend_profile, result.label
                ));
            }
        }
        for bound in E3_BOUND_CONFIGS {
            if !seen_bounds.contains(bound.label) {
                errors.push(format!(
                    "profile {} missing bound {}",
                    run.backend_profile, bound.label
                ));
            }
        }
        if require_bars {
            if let Some(recovery) = &run.recovery {
                if !recovery.bar_met {
                    errors.push(format!(
                        "profile {} recovery bar not met",
                        run.backend_profile
                    ));
                }
            } else if run.backend_profile == "object_log_sqlite_projection" {
                errors.push(format!(
                    "profile {} is missing required recovery evidence",
                    run.backend_profile
                ));
            }
            if !run.bars_met {
                errors.push(format!("profile {} bars_met=false", run.backend_profile));
            }
        }
    }
    for spec in E3_PROFILE_SPECS.iter() {
        if !seen_profiles.contains(spec.backend_profile) {
            errors.push(format!("missing profile {}", spec.backend_profile));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn profile_row(
    s3_endpoint: &str,
    perf_env: bool,
    resident: u64,
    load_batch: u64,
    profile_run: &ProfileRun,
) -> pqueue_release::LedgerRow {
    let scale = if resident >= RELEASE_RESIDENT {
        "release".to_string()
    } else {
        format!("resident={resident}")
    };
    let tier = if perf_env && resident >= RELEASE_RESIDENT && profile_run.bars_met {
        "release"
    } else {
        "smoke"
    }
    .to_string();
    let mut values: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    values.insert(
        "bound_count".into(),
        serde_json::json!(profile_run.ack_results.len()),
    );
    values.insert("duration_ms".into(), serde_json::json!(profile_run.wall_ms));
    values.insert("resident".into(), serde_json::json!(resident));
    values.insert("load_batch".into(), serde_json::json!(load_batch));
    values.insert("bars_met".into(), serde_json::json!(profile_run.bars_met));
    match &profile_run.recovery {
        Some(recovery) => {
            values.insert("recovery_excluded".into(), serde_json::json!(false));
            values.insert(
                "recovery_resident".into(),
                serde_json::json!(recovery.resident),
            );
            values.insert(
                "recovery_command_count".into(),
                serde_json::json!(recovery.command_count),
            );
            values.insert(
                "recovery_total_commands".into(),
                serde_json::json!(recovery.total_commands),
            );
            values.insert(
                "recovery_start_seq".into(),
                serde_json::json!(recovery.start_seq),
            );
            values.insert(
                "recovery_tail_replayed".into(),
                serde_json::json!(recovery.tail_replayed),
            );
            values.insert(
                "recovery_snapshot_used".into(),
                serde_json::json!(recovery.snapshot_used),
            );
            values.insert(
                "recovery_max_tail_budget".into(),
                serde_json::json!(recovery.recovery_max_tail),
            );
            values.insert(
                "recovery_wall_ms".into(),
                serde_json::json!(recovery.recovery_wall_ms),
            );
            values.insert(
                "recovery_pending_after".into(),
                serde_json::json!(recovery.pending_after),
            );
            values.insert(
                "recovery_bar_met".into(),
                serde_json::json!(recovery.bar_met),
            );
        }
        None => {
            values.insert("recovery_excluded".into(), serde_json::json!(true));
            values.insert(
                "recovery_exclusion_reason".into(),
                serde_json::json!(
                    "in-memory projection variant does not expose the SQLite reopen telemetry seam"
                ),
            );
        }
    }
    for result in &profile_run.ack_results {
        let prefix = format!("bound_{}", result.label);
        values.insert(
            format!("{prefix}_target_bytes"),
            serde_json::json!(result.target_bytes),
        );
        values.insert(
            format!("{prefix}_max_latency_ms"),
            serde_json::json!(result.max_latency_ms),
        );
        values.insert(
            format!("{prefix}_segments_sealed"),
            serde_json::json!(result.segments_sealed),
        );
        values.insert(
            format!("{prefix}_objects_put"),
            serde_json::json!(result.objects_put),
        );
        values.insert(
            format!("{prefix}_commands_committed"),
            serde_json::json!(result.commands_committed),
        );
        values.insert(
            format!("{prefix}_mean_commands_per_segment"),
            serde_json::json!(result.mean_batch),
        );
        values.insert(
            format!("{prefix}_max_group_commit_batch"),
            serde_json::json!(result.max_batch),
        );
        values.insert(
            format!("{prefix}_throughput_per_s"),
            serde_json::json!(result.throughput_per_s),
        );
        values.insert(
            format!("{prefix}_throughput_floor_met"),
            serde_json::json!(result.throughput_floor_met),
        );
        values.insert(
            format!("{prefix}_ack_p50_ms"),
            serde_json::json!(result.ack_p50_ms),
        );
        values.insert(
            format!("{prefix}_ack_p95_ms"),
            serde_json::json!(result.ack_p95_ms),
        );
        values.insert(
            format!("{prefix}_ack_p99_ms"),
            serde_json::json!(result.ack_p99_ms),
        );
        values.insert(
            format!("{prefix}_ack_bar_ms"),
            serde_json::json!(result.ack_bar_ms),
        );
        values.insert(
            format!("{prefix}_latency_bar_met"),
            serde_json::json!(result.latency_bar_met),
        );
        values.insert(
            format!("{prefix}_bar_met"),
            serde_json::json!(result.bar_met),
        );
    }
    values.insert(
        "throughput_floor_per_s".into(),
        serde_json::json!(E3_THROUGHPUT_FLOOR_PER_SEC),
    );

    pqueue_release::LedgerRow {
        suite: "performance_object_log_e3_live_tests".into(),
        command: "PQUEUE_S3_TEST_ENDPOINT=http://<minio-ip>:9000 scripts/perf/tp002-e3-minio.sh".into(),
        backend_profile: profile_run.backend_profile.into(),
        scale,
        seed: 0,
        environment: format!(
            "live {} over S3-compatible MinIO at {}, single deployment, resident={resident}, load_batch={load_batch}, perf_env={perf_env}; both committed object-log projection variants are exercised at 1/5/20/100ms bounds",
            profile_run.projection_label,
            s3_endpoint
        ),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "E3: 1/5/20/100ms bounds; group-commit ack p50/p95/p99 within bound; throughput >= E0 floor; 10M-item projection rebuilt via snapshot+bounded-tail with recovered pending == resident".into(),
        evidence_tier: tier,
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["E3".into()],
            values,
        },
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
             The E3 matrix evidence is DEFERRED, not a hidden pass.\n\
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
    let ack_pushes = env_u64("PQUEUE_E3_ACK_PUSHES", 100_000).max(1);
    let ack_concurrency = env_u64("PQUEUE_E3_ACK_CONCURRENCY", 384).max(1);
    let release_shape = resident >= RELEASE_RESIDENT;
    let require_bars = perf_env && release_shape;

    let runs = [
        run_profile_run::<SegmentedObjectLogInMemoryBackend, _>(
            &s3,
            "object_log_inmemory_projection",
            "inmemory",
            resident,
            load_batch,
            ack_pushes,
            ack_concurrency,
            false,
            |store, _projection_path, cfg| {
                SegmentedObjectLogInMemoryBackend::open_with_blob_store(store, cfg)
            },
        )
        .await,
        run_profile_run::<SegmentedObjectLogSqliteBackend, _>(
            &s3,
            "object_log_sqlite_projection",
            "sqlite",
            resident,
            load_batch,
            ack_pushes,
            ack_concurrency,
            true,
            |store, projection_path, cfg| {
                SegmentedObjectLogSqliteBackend::open_with_blob_store(store, projection_path, cfg)
            },
        )
        .await,
    ];

    validate_e3_profile_matrix(&runs, require_bars).expect("E3 profile matrix shape and bars");

    println!(
        "\nTP-002 E3 live object-log projection matrix over MinIO ({}) — perf_env={perf_env}, resident={resident}:",
        s3.endpoint
    );
    for run in &runs {
        println!(
            "  [{}] profile={} wall={:.1}ms bars_met={} recovery={} (projection={})",
            run.backend_profile,
            run.backend_profile,
            run.wall_ms,
            run.bars_met,
            match &run.recovery {
                Some(recovery) if recovery.bar_met => "PASS",
                Some(_) => "FAIL",
                None => "EXCLUDED",
            },
            run.projection_label
        );
        for a in &run.ack_results {
            println!(
                "    [{:>4}] target_bytes={:>9} max_latency_ms={:>5} throughput={:>9.1}/s \
                 segments_sealed={:>6} objects_put={:>6} commands={:>6} mean_batch={:>7.1} max_batch={:>5} \
                 ack_p50={:>8.2}ms ack_p95={:>8.2}ms ack_p99={:>8.2}ms bar<={:.2}ms -> {}",
                a.label,
                a.target_bytes,
                a.max_latency_ms,
                a.throughput_per_s,
                a.segments_sealed,
                a.objects_put,
                a.commands_committed,
                a.mean_batch,
                a.max_batch,
                a.ack_p50_ms,
                a.ack_p95_ms,
                a.ack_p99_ms,
                a.ack_bar_ms,
                if a.bar_met { "PASS" } else { "OVER" }
            );
        }
        println!(
            "    [recover] resident={} load_batch={} commands_loaded={} total_committed={} \
             start_seq={} tail_replayed={} snapshot_used={} (budget {}) wall={:.1}ms pending_after={} -> {}",
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.resident),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.load_batch),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.command_count),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.total_commands),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.start_seq),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.tail_replayed),
            run.recovery
                .as_ref()
                .is_some_and(|recovery| recovery.snapshot_used),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.recovery_max_tail),
            run.recovery
                .as_ref()
                .map_or(0.0, |recovery| recovery.recovery_wall_ms),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.pending_after),
            match &run.recovery {
                Some(recovery) if recovery.bar_met => "PASS",
                Some(_) => "FAIL",
                None => "EXCLUDED",
            }
        );
    }
    if !release_shape {
        println!(
            "  NOTE: resident {resident} < release shape {RELEASE_RESIDENT}; the full TP-002 E3 release \
             measurement is PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000 (this run is a smaller resident)."
        );
    }
    if !perf_env && !runs.iter().all(|run| run.bars_met) {
        eprintln!(
            "NOTE: an E3 bar was not met in this (non-perf) environment — recorded as SMOKE evidence. The \
             bars are hard-enforced only under PQUEUE_PERF_ENV + the release resident shape."
        );
    }

    let path = pqueue_release::ledger_path(
        env!("CARGO_MANIFEST_DIR"),
        "performance_object_log_e3_live_tests",
    );
    let _ = std::fs::remove_file(&path);
    for run in &runs {
        let row = profile_row(&s3.endpoint, perf_env, resident, load_batch, run);
        pqueue_release::append_row(&path, &row).expect("emit E3 ledger row");
    }
    let summary =
        pqueue_release::verify_ledger(&path, true).expect("emitted E3 rows validate strict");
    let seen = if perf_env && release_shape && runs.iter().all(|run| run.bars_met) {
        summary.evidence_ids.contains("E3")
    } else {
        summary.smoke_evidence_ids.contains("E3")
    };
    assert!(seen, "emitted rows must carry the E3 evidence id");
    println!(
        "  emitted {} E3 ledger row(s) -> {}",
        runs.len(),
        path.display()
    );
}

fn synthetic_ack(label: &'static str, throughput_per_s: f64, ack_bar_ms: f64) -> AckResult {
    let pass = throughput_per_s >= E3_THROUGHPUT_FLOOR_PER_SEC;
    AckResult {
        label,
        target_bytes: 8_388_608,
        max_latency_ms: 100,
        segments_sealed: 1,
        objects_put: 1,
        commands_committed: 1,
        mean_batch: 1.0,
        max_batch: 1,
        throughput_per_s,
        throughput_floor_met: pass,
        ack_p50_ms: ack_bar_ms - 1.0,
        ack_p95_ms: ack_bar_ms - 0.5,
        ack_p99_ms: ack_bar_ms - 0.25,
        ack_bar_ms,
        latency_bar_met: true,
        bar_met: pass,
    }
}

fn synthetic_recovery(bar_met: bool) -> RecoveryResult {
    RecoveryResult {
        resident: 10_000_000,
        load_batch: 1_000,
        command_count: 10,
        total_commands: 100,
        start_seq: 10,
        tail_replayed: 0,
        snapshot_used: true,
        recovery_max_tail: 1_000_000,
        recovery_wall_ms: 5.0,
        pending_after: 10_000_000,
        bar_met,
    }
}

fn synthetic_profile_run(
    backend_profile: &'static str,
    projection_label: &'static str,
    include_recovery: bool,
) -> ProfileRun {
    let ack_results = E3_BOUND_CONFIGS
        .iter()
        .map(|bound| synthetic_ack(bound.label, 3_000.0, 10.0))
        .collect::<Vec<_>>();
    let recovery = if include_recovery {
        Some(synthetic_recovery(true))
    } else {
        None
    };
    let bars_met = ack_results.iter().all(|result| result.bar_met)
        && recovery.as_ref().is_none_or(|r| r.bar_met);
    ProfileRun {
        backend_profile,
        projection_label,
        ack_results,
        recovery,
        wall_ms: 10.0,
        bars_met,
    }
}

#[test]
fn e3_matrix_rejects_missing_profile() {
    let runs = vec![synthetic_profile_run(
        "object_log_inmemory_projection",
        "inmemory",
        false,
    )];
    let errors = validate_e3_profile_matrix(&runs, true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("missing profile object_log_sqlite_projection")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_missing_bound() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results.pop();
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("missing bound 100ms")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_below_floor_throughput() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[0].throughput_per_s = 2_000.0;
    run.ack_results[0].throughput_floor_met = false;
    run.ack_results[0].bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("throughput 2000.000 < floor")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_over_budget_latency() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[1].ack_p99_ms = run.ack_results[1].ack_bar_ms + 1.0;
    run.ack_results[1].latency_bar_met = false;
    run.ack_results[1].bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("latency over budget")),
        "{errors:?}"
    );
}
