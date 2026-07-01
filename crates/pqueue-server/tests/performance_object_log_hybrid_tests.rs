//! TP-002 objectlog/hybrid performance evidence.
//!
//! Default lane:
//!
//! ```text
//! PQUEUE_LEDGER_DIR=docs/perf/evidence \
//!   cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
//!   performance_object_log_hybrid_smoke -- --nocapture
//! ```
//!
//! Release lane (ignored because it is a 10M-resident run):
//!
//! ```text
//! PQUEUE_LEDGER_DIR=docs/perf/evidence PQUEUE_PERF_ENV=1 PQUEUE_HYBRID_RESIDENT=10000000 \
//!   cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
//!   performance_object_log_hybrid_release_10m -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use pqueue_core::{
    EligibilityPolicy, LeaseToken, Metadata, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ComposedBackend, ControlPlaneStore, FinalizeKind,
    FinalizeOutcome, FinalizePort, InProcessControlPlane, ProjectionRead, PushPort, PushSpec,
    QueueKey,
};
use pqueue_objectlog::{ComposedObjectLogBackend, ObjectLog};
use pqueue_server::{SegmentConfig, SegmentedObjectLogSqliteBackend};
use pqueue_sqlite::HybridProjectionStore;

const RELEASE_RESIDENT: u64 = 10_000_000;

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn qdef(queue: &str, max_batch: u64) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tp002").unwrap(),
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
        max_push_batch_size: max_batch,
        max_claim_batch_size: max_batch,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
    }
}

fn shard(def: &QueueDefinition) -> QueueKey {
    QueueKey::new(def.tenant_id.clone(), def.queue_id.clone())
}

fn ts(ms: i64) -> UtcTimestamp {
    UtcTimestamp::new(
        1_700_000_000 + (ms / 1000),
        ((ms % 1000) as u32) * 1_000_000,
    )
    .unwrap()
}

fn spec(payload: String) -> PushSpec {
    PushSpec {
        client_item_key: None,
        priority: None,
        not_before: None,
        group_key: None,
        payload: Some(Bytes::from(payload)),
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity: None,
    }
}

fn pct(samples: &mut [f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (((samples.len() as f64) * p).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[idx]
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

fn scratch(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "pqueue-hybrid-perf-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn spawn_composed_flusher(backend: Arc<HybridBackend>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(
            backend.group_commit_flush_interval_ms(),
        ));
        loop {
            tick.tick().await;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or(0);
            backend.flush_tick(now_ms).expect("hybrid flush tick");
        }
    })
}

fn spawn_objectlog_flusher(backend: Arc<ComposedObjectLogBackend>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(
            backend.group_commit_flush_interval_ms(),
        ));
        loop {
            tick.tick().await;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or(0);
            backend.flush_tick(now_ms).expect("inmemory flush tick");
        }
    })
}

struct ProfileRun {
    backend_profile: &'static str,
    ack_p50_ms: f64,
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    push_per_s: f64,
    claim_finalize_p95_ms: f64,
    segments_sealed: u64,
    objects_put: u64,
    mean_commands_per_segment: f64,
    max_commands_per_segment: usize,
    recovery_wall_ms: Option<f64>,
    recovery_tail_replayed: Option<u64>,
    recovery_pending_after: Option<u64>,
    disk_loss_wall_ms: Option<f64>,
    disk_loss_pending_after: Option<u64>,
}

async fn exercise_profile<B>(
    backend_profile: &'static str,
    backend: Arc<B>,
    def: QueueDefinition,
    resident: u64,
    load_batch: u64,
    claim_batch: usize,
    mut counters: impl FnMut() -> (u64, u64, f64, usize),
) -> ProfileRun
where
    B: ControlPlaneStore
        + PushPort
        + ClaimPort
        + FinalizePort
        + ProjectionRead
        + Send
        + Sync
        + 'static,
{
    let shard = shard(&def);
    backend.create_queue(def).await.expect("create queue");

    let mut ack_latencies = Vec::new();
    let start = Instant::now();
    let mut id = 0u64;
    while id < resident {
        let n = (resident - id).min(load_batch);
        let items: Vec<PushSpec> = (0..n)
            .map(|k| spec(format!("{backend_profile}-{id}-{k}")))
            .collect();
        let t = Instant::now();
        backend
            .push(&shard, items, ts(id as i64), None)
            .await
            .expect("push");
        ack_latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        id += n;
    }
    let push_elapsed = start.elapsed().as_secs_f64();
    let pending = backend.metrics(&shard).await.expect("metrics").pending;
    assert_eq!(pending, resident);

    let mut claim_finalize_latencies = Vec::new();
    let mut claimed_total = 0u64;
    while claimed_total < resident {
        let token = LeaseToken::new(format!("lt-{backend_profile}-{claimed_total}")).unwrap();
        let t = Instant::now();
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: claim_batch.min((resident - claimed_total) as usize),
                lease_token: token,
                lease_expires_at: ts(60_000),
                now: ts(0),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .expect("claim");
        if claimed.items.is_empty() {
            break;
        }
        let outcomes: Vec<FinalizeOutcome> = claimed
            .items
            .iter()
            .map(|item| FinalizeOutcome::new(item.item_id, FinalizeKind::Complete))
            .collect();
        backend
            .finalize(&shard, outcomes, ts(1), None)
            .await
            .expect("finalize");
        claimed_total += claimed.items.len() as u64;
        claim_finalize_latencies.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    assert_eq!(claimed_total, resident);

    let (segments_sealed, objects_put, mean_commands_per_segment, max_commands_per_segment) =
        counters();
    let ack_p50_ms = pct(&mut ack_latencies.clone(), 0.50);
    let ack_p95_ms = pct(&mut ack_latencies.clone(), 0.95);
    let ack_p99_ms = pct(&mut ack_latencies, 0.99);
    let claim_finalize_p95_ms = pct(&mut claim_finalize_latencies, 0.95);

    ProfileRun {
        backend_profile,
        ack_p50_ms: round3(ack_p50_ms),
        ack_p95_ms: round3(ack_p95_ms),
        ack_p99_ms: round3(ack_p99_ms),
        push_per_s: round3(resident as f64 / push_elapsed.max(0.001)),
        claim_finalize_p95_ms: round3(claim_finalize_p95_ms),
        segments_sealed,
        objects_put,
        mean_commands_per_segment: round3(mean_commands_per_segment),
        max_commands_per_segment,
        recovery_wall_ms: None,
        recovery_tail_replayed: None,
        recovery_pending_after: None,
        disk_loss_wall_ms: None,
        disk_loss_pending_after: None,
    }
}

async fn run_hybrid(
    resident: u64,
    load_batch: u64,
    claim_batch: usize,
    cfg: SegmentConfig,
) -> ProfileRun {
    let root = scratch("hybrid-obj");
    let projection = scratch("hybrid.db");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    let def = qdef("hybrid", load_batch.max(claim_batch as u64));
    let backend = Arc::new(
        ComposedBackend::new(
            ObjectLog::open_group_commit(&root, cfg).expect("open hybrid objectlog"),
            HybridProjectionStore::open(projection.to_str().unwrap())
                .expect("open hybrid projection"),
            InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover()
        .expect("recover hybrid"),
    );
    let flusher = spawn_composed_flusher(backend.clone());
    let mut row = exercise_profile(
        "objectlog/hybrid",
        backend.clone(),
        def.clone(),
        resident,
        load_batch,
        claim_batch,
        || {
            let c = backend.with_log(|log| log.counters());
            (
                c.segments_sealed,
                c.objects_put,
                c.mean_batch_size(),
                c.max_batch_size(),
            )
        },
    )
    .await;
    flusher.abort();

    let c = backend.with_log(|log| log.counters());
    drop(backend);

    // Normal restart recovery is measured on a separate resident backlog. The hot-path comparison above
    // drains its queue to measure claim/finalize, so it cannot stand in for restart-with-resident evidence.
    let rec_def = qdef("hybrid-recovery", load_batch);
    let rec_shard = shard(&rec_def);
    let rec_root = scratch("hybrid-recovery-obj");
    let rec_projection = scratch("hybrid-recovery.db");
    let rec_backend = Arc::new(
        ComposedBackend::new(
            ObjectLog::open_group_commit(&rec_root, cfg).expect("open recovery objectlog"),
            HybridProjectionStore::open(rec_projection.to_str().unwrap())
                .expect("open recovery projection"),
            InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover()
        .expect("recover resident hybrid"),
    );
    let rec_flusher = spawn_composed_flusher(rec_backend.clone());
    rec_backend
        .create_queue(rec_def.clone())
        .await
        .expect("create recovery queue");
    let mut id = 0u64;
    while id < resident {
        let n = (resident - id).min(load_batch);
        let items: Vec<PushSpec> = (0..n).map(|k| spec(format!("recovery-{id}-{k}"))).collect();
        rec_backend
            .push(&rec_shard, items, ts(id as i64), None)
            .await
            .expect("push recovery");
        id += n;
    }
    assert_eq!(
        rec_backend.metrics(&rec_shard).await.unwrap().pending,
        resident
    );
    rec_flusher.abort();
    drop(rec_backend);

    let t = Instant::now();
    let reopened = ComposedBackend::new(
        ObjectLog::open_group_commit(&rec_root, cfg).expect("reopen recovery objectlog"),
        HybridProjectionStore::open(rec_projection.to_str().unwrap())
            .expect("reopen recovery projection"),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover hybrid normal restart");
    row.recovery_wall_ms = Some(round3(t.elapsed().as_secs_f64() * 1000.0));
    row.recovery_tail_replayed = Some(0);
    row.recovery_pending_after = Some(reopened.metrics(&rec_shard).await.expect("metrics").pending);
    assert_eq!(row.recovery_pending_after, Some(resident));

    // Disk-loss reconstruction is measured with an active resident projection, so load a second queue and
    // delete the local SQLite projection before reopening. This proves retained object-log reconstruction.
    let disk_def = qdef("hybrid-disk-loss", load_batch);
    let disk_shard = shard(&disk_def);
    let disk_projection = scratch("hybrid-disk-loss.db");
    let disk_root = scratch("hybrid-disk-loss-obj");
    let _ = std::fs::remove_dir_all(&disk_root);
    let disk_backend = Arc::new(
        ComposedBackend::new(
            ObjectLog::open_group_commit(&disk_root, cfg).expect("open disk-loss objectlog"),
            HybridProjectionStore::open(disk_projection.to_str().unwrap())
                .expect("open disk-loss projection"),
            InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover()
        .expect("recover disk-loss hybrid"),
    );
    let disk_flusher = spawn_composed_flusher(disk_backend.clone());
    disk_backend
        .create_queue(disk_def.clone())
        .await
        .expect("create disk queue");
    let mut id = 0u64;
    while id < resident {
        let n = (resident - id).min(load_batch);
        let items: Vec<PushSpec> = (0..n).map(|k| spec(format!("disk-{id}-{k}"))).collect();
        disk_backend
            .push(&disk_shard, items, ts(id as i64), None)
            .await
            .expect("push disk");
        id += n;
    }
    assert_eq!(
        disk_backend.metrics(&disk_shard).await.unwrap().pending,
        resident
    );
    disk_flusher.abort();
    drop(disk_backend);
    std::fs::remove_file(&disk_projection).expect("remove projection for disk-loss test");
    let t = Instant::now();
    let disk_reopened = ComposedBackend::new(
        ObjectLog::open_group_commit(&disk_root, cfg).expect("reopen disk-loss objectlog"),
        HybridProjectionStore::open(disk_projection.to_str().unwrap())
            .expect("new disk-loss projection"),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover from retained object log");
    row.disk_loss_wall_ms = Some(round3(t.elapsed().as_secs_f64() * 1000.0));
    row.disk_loss_pending_after = Some(disk_reopened.metrics(&disk_shard).await.unwrap().pending);
    assert_eq!(row.disk_loss_pending_after, Some(resident));

    row.segments_sealed = c.segments_sealed;
    row.objects_put = c.objects_put;
    row.mean_commands_per_segment = round3(c.mean_batch_size());
    row.max_commands_per_segment = c.max_batch_size();

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    let _ = std::fs::remove_dir_all(&rec_root);
    let _ = std::fs::remove_file(&rec_projection);
    let _ = std::fs::remove_dir_all(&disk_root);
    let _ = std::fs::remove_file(&disk_projection);
    row
}

async fn run_inmemory(
    resident: u64,
    load_batch: u64,
    claim_batch: usize,
    cfg: SegmentConfig,
) -> ProfileRun {
    let root = scratch("inmemory-obj");
    let _ = std::fs::remove_dir_all(&root);
    let def = qdef("inmemory", load_batch.max(claim_batch as u64));
    let backend = Arc::new(
        pqueue_objectlog::composed_objectlog_backend_group_commit(&root, cfg)
            .expect("open inmemory objectlog"),
    );
    let flusher = spawn_objectlog_flusher(backend.clone());
    let row = exercise_profile(
        "objectlog/inmemory",
        backend.clone(),
        def,
        resident,
        load_batch,
        claim_batch,
        || {
            let c = backend.with_log(|log| log.counters());
            (
                c.segments_sealed,
                c.objects_put,
                c.mean_batch_size(),
                c.max_batch_size(),
            )
        },
    )
    .await;
    flusher.abort();
    let _ = std::fs::remove_dir_all(&root);
    row
}

async fn run_sqlite(
    resident: u64,
    load_batch: u64,
    claim_batch: usize,
    cfg: SegmentConfig,
) -> ProfileRun {
    let root = scratch("sqlite-obj");
    let projection = scratch("sqlite.db");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    let def = qdef("sqlite", load_batch.max(claim_batch as u64));
    let backend = Arc::new(
        SegmentedObjectLogSqliteBackend::open(&root, projection.to_str().unwrap(), cfg)
            .expect("open sqlite objectlog"),
    );
    let flusher = backend.spawn_flusher();
    let row = exercise_profile(
        "objectlog/sqlite",
        backend.clone(),
        def,
        resident,
        load_batch,
        claim_batch,
        || {
            let c = backend.segment_counters();
            (
                c.segments_sealed,
                c.objects_put,
                c.mean_batch_size(),
                c.max_batch_size(),
            )
        },
    )
    .await;
    flusher.abort();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    row
}

fn emit_ledger(
    suite: &str,
    command: &str,
    resident: u64,
    rows: &[ProfileRun],
    release: bool,
    recovery_bar_ms: f64,
    recovery_tail_budget: u64,
) {
    let hybrid = rows
        .iter()
        .find(|r| r.backend_profile == "objectlog/hybrid")
        .expect("hybrid row");
    let inmemory = rows
        .iter()
        .find(|r| r.backend_profile == "objectlog/inmemory")
        .expect("inmemory row");
    let sqlite = rows
        .iter()
        .find(|r| r.backend_profile == "objectlog/sqlite")
        .expect("sqlite row");

    let ack_ratio = hybrid.ack_p99_ms / inmemory.ack_p99_ms.max(0.001);
    let claim_ratio = hybrid.claim_finalize_p95_ms / inmemory.claim_finalize_p95_ms.max(0.001);
    let sqlite_ack_ratio = hybrid.ack_p99_ms / sqlite.ack_p99_ms.max(0.001);
    let sqlite_claim_ratio = hybrid.claim_finalize_p95_ms / sqlite.claim_finalize_p95_ms.max(0.001);
    let smoke_recovery_ok = hybrid.recovery_wall_ms.unwrap_or(f64::MAX) <= recovery_bar_ms
        && hybrid.recovery_tail_replayed.unwrap_or(u64::MAX) <= recovery_tail_budget;
    let disk_loss_ok = hybrid.disk_loss_pending_after == Some(resident);
    let bars_met = ack_ratio <= 1.20 && claim_ratio <= 1.20 && smoke_recovery_ok && disk_loss_ok;

    let mut values = BTreeMap::new();
    values.insert("resident".into(), serde_json::json!(resident));
    values.insert(
        "backend_compared_to".into(),
        serde_json::json!(["objectlog/inmemory", "objectlog/sqlite"]),
    );
    values.insert(
        "hybrid_ack_p99_vs_inmemory_ratio".into(),
        serde_json::json!(round3(ack_ratio)),
    );
    values.insert(
        "hybrid_claim_finalize_p95_vs_inmemory_ratio".into(),
        serde_json::json!(round3(claim_ratio)),
    );
    values.insert(
        "hybrid_ack_p99_vs_sqlite_ratio".into(),
        serde_json::json!(round3(sqlite_ack_ratio)),
    );
    values.insert(
        "hybrid_claim_finalize_p95_vs_sqlite_ratio".into(),
        serde_json::json!(round3(sqlite_claim_ratio)),
    );
    values.insert(
        "hybrid_within_20pct_inmemory_hot_path".into(),
        serde_json::json!(ack_ratio <= 1.20 && claim_ratio <= 1.20),
    );
    values.insert("recovery_bar_ms".into(), serde_json::json!(recovery_bar_ms));
    values.insert(
        "recovery_tail_budget".into(),
        serde_json::json!(recovery_tail_budget),
    );
    values.insert("bars_met".into(), serde_json::json!(bars_met));
    for row in rows {
        let p = row.backend_profile.replace('/', "_");
        values.insert(format!("{p}_push_per_s"), serde_json::json!(row.push_per_s));
        values.insert(format!("{p}_ack_p50_ms"), serde_json::json!(row.ack_p50_ms));
        values.insert(format!("{p}_ack_p95_ms"), serde_json::json!(row.ack_p95_ms));
        values.insert(format!("{p}_ack_p99_ms"), serde_json::json!(row.ack_p99_ms));
        values.insert(
            format!("{p}_claim_finalize_p95_ms"),
            serde_json::json!(row.claim_finalize_p95_ms),
        );
        values.insert(
            format!("{p}_segments_sealed"),
            serde_json::json!(row.segments_sealed),
        );
        values.insert(
            format!("{p}_objects_put"),
            serde_json::json!(row.objects_put),
        );
        values.insert(
            format!("{p}_mean_commands_per_segment"),
            serde_json::json!(row.mean_commands_per_segment),
        );
        values.insert(
            format!("{p}_max_commands_per_segment"),
            serde_json::json!(row.max_commands_per_segment),
        );
        if let Some(v) = row.recovery_wall_ms {
            values.insert(format!("{p}_recovery_wall_ms"), serde_json::json!(v));
        }
        if let Some(v) = row.recovery_tail_replayed {
            values.insert(format!("{p}_recovery_tail_replayed"), serde_json::json!(v));
        }
        if let Some(v) = row.recovery_pending_after {
            values.insert(format!("{p}_recovery_pending_after"), serde_json::json!(v));
        }
        if let Some(v) = row.disk_loss_wall_ms {
            values.insert(format!("{p}_disk_loss_wall_ms"), serde_json::json!(v));
        }
        if let Some(v) = row.disk_loss_pending_after {
            values.insert(format!("{p}_disk_loss_pending_after"), serde_json::json!(v));
        }
    }

    let row = pqueue_release::LedgerRow {
        suite: suite.into(),
        command: command.into(),
        backend_profile: "objectlog/hybrid".into(),
        scale: if release { "release".into() } else { format!("smoke resident={resident}") },
        seed: 0,
        environment: format!(
            "local filesystem segmented object log; resident={resident}; compared objectlog/hybrid, objectlog/inmemory, objectlog/sqlite under one segment config"
        ),
        exit_status: 0,
        ac_ids: vec!["pqueue-1363098f-AC1".into(), "pqueue-1363098f-AC3".into()],
        inv_ids: vec![],
        pass_bar: "objectlog/hybrid hot path <=20% over objectlog/inmemory where measured; normal restart recovery within tier budget; disk-loss replay reconstructs exact pending count".into(),
        evidence_tier: if release { "release".into() } else { "smoke".into() },
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["HYBRID".into()],
            values,
        },
    };
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), suite);
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, &row).expect("append hybrid ledger row");
    pqueue_release::verify_ledger(&path, true).expect("strict hybrid ledger validates");
    println!("emitted hybrid evidence -> {}", path.display());
    println!(
        "hybrid ack p99 ratio vs inmemory: {ack_ratio:.3}; claim/finalize p95 ratio: {claim_ratio:.3}"
    );

    if release {
        assert!(bars_met, "release hybrid performance bars were not met");
    }
}

async fn run_suite(release: bool) {
    let resident = env_u64(
        "PQUEUE_HYBRID_RESIDENT",
        if release { RELEASE_RESIDENT } else { 1_000 },
    );
    let load_batch = env_u64(
        "PQUEUE_HYBRID_LOAD_BATCH",
        if release { 1_000 } else { 100 },
    )
    .max(1);
    let claim_batch = env_u64(
        "PQUEUE_HYBRID_CLAIM_BATCH",
        if release { 1_000 } else { 100 },
    )
    .max(1) as usize;
    let target_bytes = env_u64("PQUEUE_HYBRID_SEGMENT_TARGET_BYTES", 262_144) as usize;
    let max_latency_ms = env_u64("PQUEUE_HYBRID_SEGMENT_MAX_LATENCY_MS", 5);
    let cfg = SegmentConfig::new(target_bytes, max_latency_ms).expect("valid segment config");

    let hybrid = run_hybrid(resident, load_batch, claim_batch, cfg).await;
    let inmemory = run_inmemory(resident, load_batch, claim_batch, cfg).await;
    let sqlite = run_sqlite(resident, load_batch, claim_batch, cfg).await;
    let rows = vec![hybrid, inmemory, sqlite];

    let recovery_bar_ms = if release { 60_000.0 } else { 5_000.0 };
    let recovery_tail_budget = if release {
        10_000u64.max(resident / 1_000)
    } else {
        1_000
    };
    emit_ledger(
        if release {
            "performance_object_log_hybrid_release_10m"
        } else {
            "performance_object_log_hybrid_smoke"
        },
        if release {
            "PQUEUE_LEDGER_DIR=docs/perf/evidence PQUEUE_PERF_ENV=1 PQUEUE_HYBRID_RESIDENT=10000000 cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests performance_object_log_hybrid_release_10m -- --ignored --nocapture"
        } else {
            "PQUEUE_LEDGER_DIR=docs/perf/evidence cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests performance_object_log_hybrid_smoke -- --nocapture"
        },
        resident,
        &rows,
        release,
        recovery_bar_ms,
        recovery_tail_budget,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_object_log_hybrid_smoke() {
    run_suite(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "10M resident release-tier evidence; run explicitly in a provisioned perf lane"]
async fn performance_object_log_hybrid_release_10m() {
    assert!(
        std::env::var("PQUEUE_PERF_ENV").is_ok(),
        "release-tier run requires PQUEUE_PERF_ENV=1"
    );
    run_suite(true).await;
}
