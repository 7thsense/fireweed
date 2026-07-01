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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, LeaseToken, Metadata, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
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

// ---------------------------------------------------------------------------
// Release-grade workload harness (bead pqueue-3d5bb3df).
//
// The legacy `run_suite` lane above pushes one uniform payload through a single
// sequential producer/consumer at one resident count and one repetition. That is
// not release-grade evidence across scale, cache state, concurrency, or workload
// shape. The additions below close that gap:
//
//   * a deterministic seeded workload generator (`WorkloadGen`, seed=0) with
//     pinned payload-size, client_item_key-cardinality, retry-injection, and
//     error-injection distributions — documented in
//     `docs/perf/tp002-hybrid-async-workload.md`;
//   * warm-cache (projection pre-touched) vs cold-cache (fresh open) variants;
//   * a concurrency/batch sweep driven by N tokio producer/consumer tasks,
//     emitting one ledger cell per (cache, concurrency, batch);
//   * a resident scale matrix over {10k, 100k, 1M, 10M} gated by an RSS capacity
//     guard that skips-with-log any scale over the machine budget;
//   * >=5 repetitions per release cell recording median + coefficient-of-variation
//     under a documented outlier-trim policy.
// ---------------------------------------------------------------------------

/// Deterministic `splitmix64` PRNG. Same seed => identical `u64` stream, so a
/// workload pinned at `seed = 0` replays byte-for-byte across runs. This is the
/// single source of randomness for the seeded generator (no `rand` dependency,
/// no wall-clock, no OS entropy) — that is what makes the distribution pinnable.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform draw in `[0, n)` (`n == 0` treated as `1`).
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    /// Bernoulli trial: true with probability `num/den`.
    fn bernoulli(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

/// One generated unit of work. These four fields are the pinned "shape" the
/// distribution-pin test asserts are identical across two seed=0 runs.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkItem {
    payload_size: usize,
    client_item_key: u64,
    inject_retry: bool,
    inject_error: bool,
}

/// Non-uniform payload-size distribution (bytes): mostly small, a long tail of
/// large records. The cumulative weights are out of 100.
const PAYLOAD_SIZE_CDF: &[(u64, usize)] = &[
    (50, 64),      // 50% : 64 B
    (80, 256),     // 30% : 256 B
    (95, 1_024),   // 15% : 1 KiB
    (99, 4_096),   // 4%  : 4 KiB
    (100, 16_384), // 1%  : 16 KiB
];

/// Pinned distribution parameters. `seed = 0` is the documented release seed.
#[derive(Clone, Copy)]
struct WorkloadSpec {
    seed: u64,
    /// `client_item_key` is drawn from `[0, key_cardinality)` — a bounded key
    /// space so the workload exercises key reuse pressure, not one-key-per-item.
    key_cardinality: u64,
    /// Retry-injection probability, `retry_num / retry_den`.
    retry_num: u64,
    retry_den: u64,
    /// Error-injection probability, `error_num / error_den`.
    error_num: u64,
    error_den: u64,
}

impl WorkloadSpec {
    /// The pinned release workload (seed=0). Documented in
    /// `docs/perf/tp002-hybrid-async-workload.md`.
    fn pinned() -> Self {
        Self {
            seed: 0,
            key_cardinality: 64,
            retry_num: 1,
            retry_den: 20, // ~5% retry-injected
            error_num: 1,
            error_den: 50, // ~2% error-injected
        }
    }
}

/// Deterministic workload generator. Draws four independent variates per item in
/// a fixed order so the stream is stable and reproducible for a given seed.
struct WorkloadGen {
    rng: SplitMix64,
    spec: WorkloadSpec,
}

impl WorkloadGen {
    fn new(spec: WorkloadSpec) -> Self {
        Self {
            rng: SplitMix64::new(spec.seed),
            spec,
        }
    }

    fn next(&mut self) -> WorkItem {
        // Draw order is fixed: payload-size, then key, then retry, then error.
        let roll = self.rng.below(100);
        let payload_size = PAYLOAD_SIZE_CDF
            .iter()
            .find(|(cum, _)| roll < *cum)
            .map(|(_, sz)| *sz)
            .unwrap_or(64);
        let client_item_key = self.rng.below(self.spec.key_cardinality);
        let inject_retry = self.rng.bernoulli(self.spec.retry_num, self.spec.retry_den);
        let inject_error = self.rng.bernoulli(self.spec.error_num, self.spec.error_den);
        WorkItem {
            payload_size,
            client_item_key,
            inject_retry,
            inject_error,
        }
    }

    /// Materialize the first `n` items as a pinned workload vector.
    fn take(mut self, n: u64) -> Vec<WorkItem> {
        (0..n).map(|_| self.next()).collect()
    }
}

/// Build a real push spec from a generated work item. The payload is padded to
/// the drawn size so segment/object-log IO reflects the non-uniform distribution;
/// the client item key carries the drawn cardinality bucket but is made unique per
/// item (suffixed with the global index) so key-dedup never suppresses a resident.
fn work_spec(profile: &str, global_index: u64, w: &WorkItem) -> PushSpec {
    let mut payload = format!("{profile}-{global_index}-");
    if payload.len() < w.payload_size {
        payload.extend(std::iter::repeat('x').take(w.payload_size - payload.len()));
    }
    let key = ClientItemKey::new(format!("k{}-{global_index}", w.client_item_key))
        .expect("non-empty client item key");
    PushSpec {
        client_item_key: Some(key),
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

/// Injected-distribution tallies for a workload, recorded into the ledger so a
/// cell proves which pinned distribution actually drove it.
struct WorkloadTally {
    injected_retry: u64,
    injected_error: u64,
    distinct_payload_sizes: usize,
}

fn tally_workload(workload: &[WorkItem]) -> WorkloadTally {
    let injected_retry = workload.iter().filter(|w| w.inject_retry).count() as u64;
    let injected_error = workload.iter().filter(|w| w.inject_error).count() as u64;
    let distinct_payload_sizes = workload
        .iter()
        .map(|w| w.payload_size)
        .collect::<HashSet<_>>()
        .len();
    WorkloadTally {
        injected_retry,
        injected_error,
        distinct_payload_sizes,
    }
}

/// Drive one measured (cache, concurrency, batch) cell: fan out `concurrency`
/// producer tasks over disjoint slices of the pinned workload, then fan out
/// `concurrency` consumer tasks that claim+finalize until every resident is
/// terminal. Returns the measured throughput/latency for the cell.
struct CellMeasurement {
    push_per_s: f64,
    ack_p50_ms: f64,
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    claim_finalize_p95_ms: f64,
    drain_wall_ms: f64,
}

async fn measure_workload<B>(
    backend: Arc<B>,
    shard: QueueKey,
    profile: &'static str,
    workload: Arc<Vec<WorkItem>>,
    concurrency: usize,
    load_batch: u64,
    claim_batch: usize,
) -> CellMeasurement
where
    B: PushPort + ClaimPort + FinalizePort + ProjectionRead + Send + Sync + 'static,
{
    let resident = workload.len() as u64;
    let concurrency = concurrency.max(1);

    // --- producers: concurrent push over disjoint contiguous slices ---
    let slice = resident.div_ceil(concurrency as u64);
    let push_start = Instant::now();
    let mut producers = Vec::new();
    for task in 0..concurrency as u64 {
        let start = task * slice;
        if start >= resident {
            break;
        }
        let end = (start + slice).min(resident);
        let backend = backend.clone();
        let shard = shard.clone();
        let workload = workload.clone();
        producers.push(tokio::spawn(async move {
            let mut ack = Vec::new();
            let mut id = start;
            while id < end {
                let n = (end - id).min(load_batch);
                let items: Vec<PushSpec> = (0..n)
                    .map(|k| {
                        let gi = id + k;
                        work_spec(profile, gi, &workload[gi as usize])
                    })
                    .collect();
                let t = Instant::now();
                backend
                    .push(&shard, items, ts(id as i64), None)
                    .await
                    .expect("push");
                ack.push(t.elapsed().as_secs_f64() * 1000.0);
                id += n;
            }
            ack
        }));
    }
    let mut ack_latencies = Vec::new();
    for p in producers {
        ack_latencies.extend(p.await.expect("producer task"));
    }
    let push_elapsed = push_start.elapsed().as_secs_f64();
    assert_eq!(
        backend.metrics(&shard).await.expect("metrics").pending,
        resident,
        "every resident item must land (no dedup suppression)"
    );

    // --- consumers: concurrent claim+finalize until all residents terminal ---
    let completed = Arc::new(AtomicU64::new(0));
    let drain_start = Instant::now();
    let mut consumers = Vec::new();
    for task in 0..concurrency as u64 {
        let backend = backend.clone();
        let shard = shard.clone();
        let completed = completed.clone();
        consumers.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut round = 0u64;
            loop {
                if completed.load(Ordering::Acquire) >= resident {
                    break;
                }
                let token =
                    LeaseToken::new(format!("lt-{profile}-{task}-{round}")).expect("lease token");
                round += 1;
                let t = Instant::now();
                let claimed = backend
                    .claim(ClaimRequest {
                        shard: shard.clone(),
                        worker_id: WorkerId::new(format!("w{task}")).expect("worker id"),
                        max_items: claim_batch,
                        lease_token: token,
                        lease_expires_at: ts(60_000),
                        now: ts(0),
                        compatibility: ClaimCompatibility::default(),
                        expected_epoch: None,
                    })
                    .await
                    .expect("claim");
                if claimed.items.is_empty() {
                    tokio::task::yield_now().await;
                    continue;
                }
                let outcomes: Vec<FinalizeOutcome> = claimed
                    .items
                    .iter()
                    .map(|item| FinalizeOutcome::new(item.item_id, FinalizeKind::Complete))
                    .collect();
                let n = outcomes.len() as u64;
                backend
                    .finalize(&shard, outcomes, ts(1), None)
                    .await
                    .expect("finalize");
                completed.fetch_add(n, Ordering::AcqRel);
                latencies.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            latencies
        }));
    }
    let mut claim_latencies = Vec::new();
    for c in consumers {
        claim_latencies.extend(c.await.expect("consumer task"));
    }
    let drain_elapsed = drain_start.elapsed().as_secs_f64();
    assert_eq!(completed.load(Ordering::Acquire), resident);
    assert_eq!(
        backend.metrics(&shard).await.expect("metrics").pending,
        0,
        "queue must fully drain"
    );

    CellMeasurement {
        push_per_s: round3(resident as f64 / push_elapsed.max(0.001)),
        ack_p50_ms: round3(pct(&mut ack_latencies.clone(), 0.50)),
        ack_p95_ms: round3(pct(&mut ack_latencies.clone(), 0.95)),
        ack_p99_ms: round3(pct(&mut ack_latencies, 0.99)),
        claim_finalize_p95_ms: round3(pct(&mut claim_latencies, 0.95)),
        drain_wall_ms: round3(drain_elapsed * 1000.0),
    }
}

/// Open a fresh hybrid backend at scratch paths. Returns the backend plus the
/// object-log root and projection path so the caller can clean them up.
fn open_hybrid_backend(
    label: &str,
    load_batch: u64,
    claim_batch: usize,
    cfg: SegmentConfig,
) -> (Arc<HybridBackend>, std::path::PathBuf, std::path::PathBuf) {
    let root = scratch(&format!("{label}-obj"));
    let projection = scratch(&format!("{label}.db"));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    let _ = load_batch;
    let _ = claim_batch;
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
    (backend, root, projection)
}

/// Run one (cache, concurrency, load_batch, claim_batch) cell against a fresh
/// hybrid backend. Warm cells pre-touch the projection by draining a warmup
/// queue before the measured queue; cold cells measure on a freshly opened
/// backend with nothing cached.
async fn run_matrix_cell(
    warm: bool,
    concurrency: usize,
    load_batch: u64,
    claim_batch: usize,
    resident: u64,
    spec: WorkloadSpec,
    cfg: SegmentConfig,
) -> (CellMeasurement, WorkloadTally) {
    let label = format!(
        "matrix-{}-c{concurrency}-lb{load_batch}-cb{claim_batch}",
        if warm { "warm" } else { "cold" }
    );
    let (backend, root, projection) = open_hybrid_backend(&label, load_batch, claim_batch, cfg);
    let flusher = spawn_composed_flusher(backend.clone());

    if warm {
        // Pre-touch the projection: fully load+drain a warmup queue so SQLite
        // page cache and projection structures are hot before we measure.
        let warm_def = qdef("matrix-warmup", load_batch.max(claim_batch as u64));
        let warm_shard = shard(&warm_def);
        backend
            .create_queue(warm_def)
            .await
            .expect("create warmup queue");
        let warm_workload = Arc::new(WorkloadGen::new(spec).take(resident));
        measure_workload(
            backend.clone(),
            warm_shard,
            "objectlog/hybrid",
            warm_workload,
            concurrency,
            load_batch,
            claim_batch,
        )
        .await;
    }

    let def = qdef("matrix-measured", load_batch.max(claim_batch as u64));
    let measured_shard = shard(&def);
    backend
        .create_queue(def)
        .await
        .expect("create measured queue");
    let workload = Arc::new(WorkloadGen::new(spec).take(resident));
    let tally = tally_workload(&workload);
    let measurement = measure_workload(
        backend.clone(),
        measured_shard,
        "objectlog/hybrid",
        workload,
        concurrency,
        load_batch,
        claim_batch,
    )
    .await;

    flusher.abort();
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    (measurement, tally)
}

/// Emit one ledger cell (row) for a cache/concurrency/batch matrix run.
#[allow(clippy::too_many_arguments)]
fn emit_matrix_cell(
    suite: &str,
    command: &str,
    resident: u64,
    warm: bool,
    concurrency: usize,
    load_batch: u64,
    claim_batch: usize,
    m: &CellMeasurement,
    tally: &WorkloadTally,
) {
    let cache = if warm { "warm" } else { "cold" };
    let mut values = BTreeMap::new();
    values.insert("resident".into(), serde_json::json!(resident));
    values.insert("cache_state".into(), serde_json::json!(cache));
    values.insert("concurrency".into(), serde_json::json!(concurrency));
    values.insert("load_batch".into(), serde_json::json!(load_batch));
    values.insert("claim_batch".into(), serde_json::json!(claim_batch));
    values.insert("push_per_s".into(), serde_json::json!(m.push_per_s));
    values.insert("ack_p50_ms".into(), serde_json::json!(m.ack_p50_ms));
    values.insert("ack_p95_ms".into(), serde_json::json!(m.ack_p95_ms));
    values.insert("ack_p99_ms".into(), serde_json::json!(m.ack_p99_ms));
    values.insert(
        "claim_finalize_p95_ms".into(),
        serde_json::json!(m.claim_finalize_p95_ms),
    );
    values.insert("drain_wall_ms".into(), serde_json::json!(m.drain_wall_ms));
    values.insert(
        "workload_seed".into(),
        serde_json::json!(WorkloadSpec::pinned().seed),
    );
    values.insert(
        "injected_retry".into(),
        serde_json::json!(tally.injected_retry),
    );
    values.insert(
        "injected_error".into(),
        serde_json::json!(tally.injected_error),
    );
    values.insert(
        "distinct_payload_sizes".into(),
        serde_json::json!(tally.distinct_payload_sizes),
    );

    let row = pqueue_release::LedgerRow {
        suite: suite.into(),
        command: command.into(),
        backend_profile: "objectlog/hybrid".into(),
        scale: format!("smoke resident={resident} cache={cache} concurrency={concurrency} load_batch={load_batch} claim_batch={claim_batch}"),
        seed: 0,
        environment: format!(
            "local filesystem segmented object log; seeded workload (seed=0, non-uniform payload); cache={cache}; concurrency={concurrency}; load_batch={load_batch}; claim_batch={claim_batch}"
        ),
        exit_status: 0,
        ac_ids: vec!["pqueue-3d5bb3df-AC2".into()],
        inv_ids: vec![],
        pass_bar: "one ledger cell emitted per (cache, concurrency, batch); queue drains fully under concurrent producers/consumers".into(),
        evidence_tier: "smoke".into(),
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["HYBRID-CACHE-MATRIX".into()],
            values,
        },
    };
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), suite);
    pqueue_release::append_row(&path, &row).expect("append matrix ledger cell");
}

/// Population statistics over a sample set, plus the documented outlier-trim.
struct RepStats {
    reps: usize,
    trimmed_reps: usize,
    median: f64,
    cov: f64,
}

/// Outlier-trim policy (documented in `docs/perf/tp002-hybrid-async-workload.md`):
/// with >= 5 reps, drop the single lowest and single highest sample ("trimmed
/// extremes"), then compute the median and coefficient-of-variation
/// (stddev / mean, sample stddev) over the retained samples. With < 5 reps no
/// trim is applied.
fn rep_stats(samples: &[f64]) -> RepStats {
    let reps = samples.len();
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let trimmed: Vec<f64> = if reps >= 5 {
        sorted[1..reps - 1].to_vec()
    } else {
        sorted.clone()
    };
    let trimmed_reps = trimmed.len();
    let median = if trimmed_reps == 0 {
        0.0
    } else {
        trimmed[trimmed_reps / 2]
    };
    let mean = if trimmed_reps == 0 {
        0.0
    } else {
        trimmed.iter().sum::<f64>() / trimmed_reps as f64
    };
    let variance = if trimmed_reps > 1 {
        trimmed.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (trimmed_reps as f64 - 1.0)
    } else {
        0.0
    };
    let cov = if mean.abs() > f64::EPSILON {
        variance.sqrt() / mean
    } else {
        0.0
    };
    RepStats {
        reps,
        trimmed_reps,
        median: round3(median),
        cov: round3(cov),
    }
}

/// The release scale ladder (AC3).
const SCALE_LADDER: &[u64] = &[10_000, 100_000, 1_000_000, 10_000_000];

/// Estimated resident RSS per item used by the capacity guard: padded payload
/// (mean ~0.6 KiB) plus projection row, object-log copy, and index overhead.
const EST_BYTES_PER_ITEM: u64 = 4_096;

/// Read `MemAvailable` (bytes) from `/proc/meminfo`, if present.
fn detected_mem_available_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// RSS capacity budget for the scale guard. `PQUEUE_HYBRID_RSS_BUDGET_BYTES`
/// overrides everything. In the release lane the budget is 3/4 of detected
/// available memory (a provisioned box runs the big scales); otherwise a
/// conservative fixed budget keeps the default (non-provisioned) lane fast by
/// admitting only the 10k scale.
fn rss_budget_bytes(release: bool) -> u64 {
    if let Ok(v) = std::env::var("PQUEUE_HYBRID_RSS_BUDGET_BYTES") {
        if let Ok(n) = v.parse::<u64>() {
            return n;
        }
    }
    if release {
        detected_mem_available_bytes()
            .map(|m| m / 4 * 3)
            .unwrap_or(128 * 1024 * 1024)
    } else {
        128 * 1024 * 1024
    }
}

/// True if a resident count fits the capacity budget.
fn scale_fits_budget(resident: u64, budget: u64) -> bool {
    resident.saturating_mul(EST_BYTES_PER_ITEM) <= budget
}

/// Run `reps` repetitions of the hybrid hot path at one resident scale and
/// summarize with median + CoV under the trim policy.
async fn run_scale_reps(
    resident: u64,
    reps: usize,
    load_batch: u64,
    claim_batch: usize,
    spec: WorkloadSpec,
    cfg: SegmentConfig,
) -> (RepStats, RepStats, WorkloadTally) {
    let mut push_samples = Vec::new();
    let mut claim_samples = Vec::new();
    let mut last_tally = None;
    for rep in 0..reps {
        let label = format!("scale-{resident}-r{rep}");
        let (backend, root, projection) = open_hybrid_backend(&label, load_batch, claim_batch, cfg);
        let flusher = spawn_composed_flusher(backend.clone());
        let def = qdef("scale-measured", load_batch.max(claim_batch as u64));
        let shard = shard(&def);
        backend.create_queue(def).await.expect("create scale queue");
        let workload = Arc::new(WorkloadGen::new(spec).take(resident));
        last_tally = Some(tally_workload(&workload));
        let m = measure_workload(
            backend.clone(),
            shard,
            "objectlog/hybrid",
            workload,
            1,
            load_batch,
            claim_batch,
        )
        .await;
        push_samples.push(m.push_per_s);
        claim_samples.push(m.claim_finalize_p95_ms);
        flusher.abort();
        drop(backend);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(&projection);
    }
    (
        rep_stats(&push_samples),
        rep_stats(&claim_samples),
        last_tally.expect("at least one rep"),
    )
}

/// Emit one ledger cell for a scale-matrix cell (median + CoV + trim fields).
#[allow(clippy::too_many_arguments)]
fn emit_scale_cell(
    suite: &str,
    command: &str,
    resident: u64,
    reps: usize,
    push: &RepStats,
    claim: &RepStats,
    tally: &WorkloadTally,
    release: bool,
) {
    let mut values = BTreeMap::new();
    values.insert("resident".into(), serde_json::json!(resident));
    values.insert("reps".into(), serde_json::json!(reps));
    values.insert("trimmed_reps".into(), serde_json::json!(push.trimmed_reps));
    values.insert(
        "outlier_trim_policy".into(),
        serde_json::json!("trimmed-extremes: drop 1 lowest + 1 highest of >=5 reps, then median + CoV over the remainder"),
    );
    values.insert("push_per_s_median".into(), serde_json::json!(push.median));
    values.insert("push_per_s_cov".into(), serde_json::json!(push.cov));
    values.insert(
        "claim_finalize_p95_ms_median".into(),
        serde_json::json!(claim.median),
    );
    values.insert(
        "claim_finalize_p95_ms_cov".into(),
        serde_json::json!(claim.cov),
    );
    values.insert(
        "distinct_payload_sizes".into(),
        serde_json::json!(tally.distinct_payload_sizes),
    );
    values.insert(
        "injected_retry".into(),
        serde_json::json!(tally.injected_retry),
    );
    values.insert(
        "injected_error".into(),
        serde_json::json!(tally.injected_error),
    );

    let row = pqueue_release::LedgerRow {
        suite: suite.into(),
        command: command.into(),
        backend_profile: "objectlog/hybrid".into(),
        scale: format!("resident={resident} reps={reps}"),
        seed: 0,
        environment: format!(
            "local filesystem segmented object log; seeded workload (seed=0); resident={resident}; {reps} reps; median+CoV under trimmed-extremes outlier policy"
        ),
        exit_status: 0,
        ac_ids: vec!["pqueue-3d5bb3df-AC3".into()],
        inv_ids: vec![],
        pass_bar: ">=5 reps per release cell; median + coefficient-of-variation recorded under the documented outlier-trim policy".into(),
        evidence_tier: if release { "release".into() } else { "smoke".into() },
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["HYBRID-SCALE-MATRIX".into()],
            values,
        },
    };
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), suite);
    pqueue_release::append_row(&path, &row).expect("append scale ledger cell");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_object_log_hybrid_smoke() {
    run_suite(false).await;
}

/// AC1: the seeded generator is pinned — two seed=0 runs produce identical
/// payload-size/key/retry/error sequences, and payload sizes are non-uniform.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn performance_object_log_hybrid_distribution_pins() {
    let spec = WorkloadSpec::pinned();
    let n = 5_000;
    let run_a = WorkloadGen::new(spec).take(n);
    let run_b = WorkloadGen::new(spec).take(n);

    assert_eq!(run_a.len(), n as usize);
    assert_eq!(
        run_a, run_b,
        "seed=0 must replay identical payload-size/key/retry/error sequences"
    );

    let distinct_sizes: HashSet<usize> = run_a.iter().map(|w| w.payload_size).collect();
    assert!(
        distinct_sizes.len() > 1,
        "payload sizes must be non-uniform, saw {distinct_sizes:?}"
    );
    let distinct_keys: HashSet<u64> = run_a.iter().map(|w| w.client_item_key).collect();
    assert!(
        distinct_keys.len() > 1 && distinct_keys.len() <= spec.key_cardinality as usize,
        "client_item_key must span a bounded cardinality, saw {}",
        distinct_keys.len()
    );
    assert!(
        run_a.iter().any(|w| w.inject_retry),
        "retry-injection distribution must fire at least once"
    );
    assert!(
        run_a.iter().any(|w| w.inject_error),
        "error-injection distribution must fire at least once"
    );

    // A differing seed must produce a different stream (guards against a
    // constant/degenerate generator masquerading as deterministic).
    let mut other = spec;
    other.seed = 1;
    let run_c = WorkloadGen::new(other).take(n);
    assert_ne!(run_a, run_c, "a different seed must change the stream");

    println!(
        "distribution pins ok: n={n} distinct_sizes={} distinct_keys={} retry={} error={}",
        distinct_sizes.len(),
        distinct_keys.len(),
        run_a.iter().filter(|w| w.inject_retry).count(),
        run_a.iter().filter(|w| w.inject_error).count(),
    );
}

/// AC2: warm + cold cache variants across a >=2-way concurrency/batch sweep at
/// resident<=1000, emitting one ledger cell per (cache, concurrency, batch).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_object_log_hybrid_cache_matrix_smoke() {
    let suite = "performance_object_log_hybrid_cache_matrix_smoke";
    let command = "cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests performance_object_log_hybrid_cache_matrix_smoke -- --nocapture";
    let resident = env_u64("PQUEUE_HYBRID_RESIDENT", 1_000).min(1_000);
    let spec = WorkloadSpec::pinned();
    let cfg = SegmentConfig::new(262_144, 5).expect("valid segment config");

    // Fresh ledger for this run.
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), suite);
    let _ = std::fs::remove_file(&path);

    // >=2-way concurrency and >=2-way batch sweep, both cache states.
    let concurrencies = [1usize, 4];
    let batches: [(u64, usize); 2] = [(50, 50), (200, 200)];

    let mut cells = 0usize;
    for warm in [false, true] {
        for &concurrency in &concurrencies {
            for &(load_batch, claim_batch) in &batches {
                let (m, tally) = run_matrix_cell(
                    warm,
                    concurrency,
                    load_batch,
                    claim_batch,
                    resident,
                    spec,
                    cfg,
                )
                .await;
                emit_matrix_cell(
                    suite,
                    command,
                    resident,
                    warm,
                    concurrency,
                    load_batch,
                    claim_batch,
                    &m,
                    &tally,
                );
                cells += 1;
                println!(
                    "cell cache={} concurrency={concurrency} load_batch={load_batch} claim_batch={claim_batch} push/s={} claim_p95={}ms retry={} error={} distinct_sizes={}",
                    if warm { "warm" } else { "cold" },
                    m.push_per_s,
                    m.claim_finalize_p95_ms,
                    tally.injected_retry,
                    tally.injected_error,
                    tally.distinct_payload_sizes,
                );
            }
        }
    }

    let expected = 2 * concurrencies.len() * batches.len();
    assert_eq!(cells, expected, "one cell per (cache, concurrency, batch)");
    pqueue_release::verify_ledger(&path, true).expect("strict cache-matrix ledger validates");
    println!("emitted {cells} cache-matrix cells -> {}", path.display());
}

/// AC3: resident scale matrix over {10k,100k,1M,10M}, capacity-guard skip-with-log
/// over budget, >=5 reps per release cell with median + CoV + outlier-trim fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_object_log_hybrid_scale_matrix() {
    let suite = "performance_object_log_hybrid_scale_matrix";
    let command = "cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests performance_object_log_hybrid_scale_matrix -- --nocapture";
    let release = std::env::var("PQUEUE_PERF_ENV").is_ok();
    let reps = env_u64("PQUEUE_HYBRID_SCALE_REPS", 5).max(5) as usize;
    let load_batch = env_u64("PQUEUE_HYBRID_LOAD_BATCH", 1_000).max(1);
    let claim_batch = env_u64("PQUEUE_HYBRID_CLAIM_BATCH", 1_000).max(1) as usize;
    let spec = WorkloadSpec::pinned();
    let cfg = SegmentConfig::new(262_144, 5).expect("valid segment config");
    let budget = rss_budget_bytes(release);

    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), suite);
    let _ = std::fs::remove_file(&path);

    let mut ran = 0usize;
    let mut skipped = 0usize;
    for &resident in SCALE_LADDER {
        if !scale_fits_budget(resident, budget) {
            skipped += 1;
            println!(
                "SKIP scale resident={resident}: estimated {} bytes exceeds capacity budget {} bytes (set PQUEUE_HYBRID_RSS_BUDGET_BYTES to raise)",
                resident.saturating_mul(EST_BYTES_PER_ITEM),
                budget,
            );
            continue;
        }
        let (push, claim, tally) =
            run_scale_reps(resident, reps, load_batch, claim_batch, spec, cfg).await;
        assert_eq!(push.reps, reps, "must run >=5 reps per release cell");
        emit_scale_cell(
            suite, command, resident, reps, &push, &claim, &tally, release,
        );
        ran += 1;
        println!(
            "scale resident={resident} reps={reps} trimmed={} push/s median={} cov={} claim_p95 median={} cov={}",
            push.trimmed_reps, push.median, push.cov, claim.median, claim.cov,
        );
    }

    assert!(
        ran >= 1,
        "at least one scale cell must fit the capacity budget (budget={budget} bytes)"
    );
    println!("scale matrix: ran={ran} skipped={skipped} budget={budget} bytes reps={reps}");
    if ran > 0 {
        pqueue_release::verify_ledger(&path, true).expect("strict scale-matrix ledger validates");
        println!("emitted {ran} scale cells -> {}", path.display());
    }
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
