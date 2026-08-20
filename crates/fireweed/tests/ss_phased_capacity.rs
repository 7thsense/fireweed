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

#![cfg(feature = "objectlog")]

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

#[derive(Clone, Copy, Debug, Default)]
struct MemSample {
    rss_bytes: u64,
    hwm_bytes: u64,
}

fn read_mem() -> MemSample {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return MemSample::default();
    };
    let mut rss = 0u64;
    let mut hwm = 0u64;
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else { continue };
        let Some(value) = parts.next().and_then(|v| v.parse::<u64>().ok()) else {
            continue;
        };
        // /proc values are KiB.
        match key {
            "VmRSS:" => rss = value.saturating_mul(1024),
            "VmHWM:" => hwm = value.saturating_mul(1024),
            _ => {}
        }
    }
    MemSample {
        rss_bytes: rss,
        hwm_bytes: hwm,
    }
}

fn count_tree(root: &std::path::Path) -> (u64, u64) {
    let mut objects = 0u64;
    let mut bytes = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata() {
                objects += 1;
                bytes += meta.len();
            }
        }
    }
    (objects, bytes)
}

fn parent_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("SS_LOG_DIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned()),
    )
}

fn unique_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

/// Production-shaped cells vs sqlite-command-log calibration. These are not the same log.
enum Cell {
    /// Filesystem object log (S3 protocol) × memory projection. Log-axis calibration.
    ObjectLogFilesystemMemory { root: PathBuf },
    /// Filesystem object log × Turso projection. Production pair.
    #[cfg(feature = "turso")]
    ObjectLogFilesystemTurso {
        log_root: PathBuf,
        projection_path: PathBuf,
    },
    /// SQLite command log × memory projection. Not production.
    #[cfg(feature = "sqlite")]
    SqliteCommandLogMemory { path: PathBuf, sync: SqliteLogSync },
}

impl Cell {
    fn parse() -> Self {
        let raw = std::env::var("SS_CELL").unwrap_or_else(|_| {
            #[cfg(feature = "turso")]
            {
                "objectlog-turso".into()
            }
            #[cfg(not(feature = "turso"))]
            {
                "objectlog".into()
            }
        });
        let sync_set = std::env::var("SS_SQLITE_SYNC").ok();
        match raw.to_ascii_lowercase().as_str() {
            "objectlog" | "filesystem" | "filesystem--memory" => {
                if let Some(sync) = sync_set {
                    panic!(
                        "SS_SQLITE_SYNC={sync} is a sqlite-command-log knob; it does not apply to \
                         the object-log cell. Unset it or use SS_CELL=sqlite (calibration only)."
                    );
                }
                let root = parent_dir().join(format!("fireweed-ss-phased-ol-{}", unique_suffix()));
                let _ = std::fs::remove_dir_all(&root);
                std::fs::create_dir_all(&root).expect("object-log root");
                Self::ObjectLogFilesystemMemory { root }
            }
            "objectlog-turso" | "filesystem--turso" | "turso" => {
                #[cfg(feature = "turso")]
                {
                    if let Some(sync) = sync_set {
                        panic!(
                            "SS_SQLITE_SYNC={sync} is a sqlite-command-log knob; it does not apply \
                             to filesystem--turso. Unset it."
                        );
                    }
                    let root =
                        parent_dir().join(format!("fireweed-ss-phased-olt-{}", unique_suffix()));
                    let _ = std::fs::remove_dir_all(&root);
                    std::fs::create_dir_all(&root).expect("object-log+turso root");
                    return Self::ObjectLogFilesystemTurso {
                        log_root: root.join("log"),
                        projection_path: root.join("projection.db"),
                    };
                }
                #[cfg(not(feature = "turso"))]
                panic!("SS_CELL=objectlog-turso requires the turso cargo feature (default-on)");
            }
            "sqlite" | "sqlite--memory" | "sqlite-log" => {
                #[cfg(feature = "sqlite")]
                {
                    let sync = match sync_set
                        .unwrap_or_else(|| "full".into())
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "normal" => SqliteLogSync::Normal,
                        "off" => SqliteLogSync::Off,
                        _ => SqliteLogSync::Full,
                    };
                    let path =
                        parent_dir().join(format!("fireweed-ss-phased-sl-{}.db", unique_suffix()));
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
                    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
                    return Self::SqliteCommandLogMemory { path, sync };
                }
                #[cfg(not(feature = "sqlite"))]
                panic!(
                    "SS_CELL=sqlite requires the sqlite cargo feature; it is not the production log"
                );
            }
            other => panic!(
                "SS_CELL must be objectlog (filesystem--memory), objectlog-turso \
                 (filesystem--turso, production pair), or sqlite (command-log calibration), \
                 got {other:?}"
            ),
        }
    }

    fn cell_name(&self) -> &'static str {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => "filesystem--memory",
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { .. } => "filesystem--turso",
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => "sqlite--memory",
        }
    }

    fn log_axis(&self) -> &'static str {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => "filesystem",
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { .. } => "filesystem",
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => "sqlite",
        }
    }

    fn projection_axis(&self) -> &'static str {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => "memory",
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { .. } => "turso",
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => "memory",
        }
    }

    fn inflight(&self) -> usize {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => env_usize("SS_INFLIGHT", 8).max(1),
            #[cfg(feature = "turso")]
            // Gather concurrent produces into one packed PUT. Apply is one transaction
            // per object; ack is log-durable (AsyncProjection).
            Self::ObjectLogFilesystemTurso { .. } => env_usize("SS_INFLIGHT", 8).max(1),
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => env_usize("SS_INFLIGHT", 1).max(1),
        }
    }

    fn open(&self, clock: Arc<dyn Clock>) -> Fireweed {
        match self {
            Self::ObjectLogFilesystemMemory { root } => {
                open_objectlog(root, clock).expect("open_objectlog")
            }
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso {
                log_root,
                projection_path,
            } => {
                std::fs::create_dir_all(log_root).expect("turso log root");
                open(
                    StorageConfig {
                        log: LogConfig::Filesystem {
                            root: log_root.clone(),
                        },
                        projection: ProjectionStoreConfig::Turso {
                            path: projection_path.clone(),
                        },
                        control_plane: None,
                        authority: None,
                        response_barrier: ResponseBarrier::AsyncProjection,
                        async_projection: Some(AsyncProjectionSpec::default()),
                        sqlite_projection_deferred_flush_chunk: None,
                        segments: SegmentConfig {
                            target_bytes: 256 * 1024,
                            max_latency_ms: 50,
                        },
                        namespace: "ss-phased".to_owned(),
                        recovery: RecoveryPolicy::default(),
                    },
                    clock,
                )
                .expect("open filesystem--turso")
            }
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { path, sync } => {
                open_sqlite_with_sync(path.to_str().unwrap(), clock, *sync).expect("open_sqlite")
            }
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::ObjectLogFilesystemMemory { root } => {
                format!(
                    "cell=filesystem--memory log_axis=filesystem (object-log) \
                     projection=memory (O(N) resident) root={}",
                    root.display()
                )
            }
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso {
                log_root,
                projection_path,
            } => {
                format!(
                    "cell=filesystem--turso log_axis=filesystem (object-log, production) \
                     projection=turso (cache-bound, swappable) log={} proj={}",
                    log_root.display(),
                    projection_path.display()
                )
            }
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { path, sync } => {
                format!(
                    "cell=sqlite--memory log_axis=sqlite (command-log calibration, NOT production) \
                     projection=memory sqlite_sync={sync:?} path={}",
                    path.display()
                )
            }
        }
    }

    fn cleanup(&self) {
        match self {
            Self::ObjectLogFilesystemMemory { root } => {
                let _ = std::fs::remove_dir_all(root);
            }
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso {
                log_root,
                projection_path,
            } => {
                let _ = std::fs::remove_dir_all(log_root);
                let _ = std::fs::remove_file(projection_path);
                let _ = std::fs::remove_file(format!("{}-wal", projection_path.display()));
                let _ = std::fs::remove_file(format!("{}-shm", projection_path.display()));
            }
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { path, .. } => {
                let _ = std::fs::remove_file(path);
                let _ = std::fs::remove_file(format!("{}-wal", path.display()));
                let _ = std::fs::remove_file(format!("{}-shm", path.display()));
            }
        }
    }

    fn log_root(&self) -> Option<&std::path::Path> {
        match self {
            Self::ObjectLogFilesystemMemory { root } => Some(root),
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { log_root, .. } => Some(log_root),
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => None,
        }
    }

    fn projection_bytes(&self) -> u64 {
        match self {
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso {
                projection_path, ..
            } => {
                let mut n = 0u64;
                for p in [
                    projection_path.clone(),
                    PathBuf::from(format!("{}-wal", projection_path.display())),
                    PathBuf::from(format!("{}-shm", projection_path.display())),
                ] {
                    if let Ok(meta) = std::fs::metadata(&p) {
                        n = n.saturating_add(meta.len());
                    }
                }
                n
            }
            _ => 0,
        }
    }
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
                let ids = fw.push_batch(&queue, items).await.expect("P1 push");
                (c0.elapsed(), ids.len(), end - chunk)
            }
        });
        for (elapsed, got, expect) in futures::future::join_all(futs).await {
            p1_calls.record(elapsed);
            assert_eq!(got, expect);
        }
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
                let resp = fw.batch_update(&queue, req).await.expect("P2 update");
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
                let resp = fw.batch_update(&queue, req).await.expect("P3 update");
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
    // Overlap complete of batch N with claim N+1. Eight concurrent claims still
    // hang on packer/reservation; keep one claim in flight until that is fixed.
    let mut p4_claim = CallStats::new();
    let mut p4_fin = CallStats::new();
    let t0 = Instant::now();
    let mut completed = 0usize;
    let c0 = Instant::now();
    let mut prev = fw
        .claim(&queue, claim_batch, 30_000)
        .await
        .expect("P4 claim");
    p4_claim.record(c0.elapsed());
    loop {
        if prev.is_empty() {
            break;
        }
        let ids: Vec<_> = prev.iter().map(|c| c.item_id).collect();
        let n_ids = ids.len();
        let fw_fin = Arc::clone(&fw);
        let fw_claim = Arc::clone(&fw);
        let queue_fin = queue.clone();
        let queue_claim = queue.clone();
        let (fin_elapsed, (claim_elapsed, next)) = tokio::join!(
            async move {
                let c1 = Instant::now();
                fw_fin.complete(&queue_fin, ids).await.expect("P4 complete");
                c1.elapsed()
            },
            async move {
                let c0 = Instant::now();
                let claimed = fw_claim
                    .claim(&queue_claim, claim_batch, 30_000)
                    .await
                    .expect("P4 claim");
                (c0.elapsed(), claimed)
            }
        );
        p4_fin.record(fin_elapsed);
        p4_claim.record(claim_elapsed);
        completed += n_ids;
        prev = next;
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

    write_evidence(
        &cell,
        &phases,
        n,
        push_batch,
        claim_batch,
        mem_before_open,
        mem_end,
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
) {
    let utc = chrono_like_utc();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/perf/evidence/ss-phased")
        .join(&utc);
    let _ = std::fs::create_dir_all(&dir);
    let mut json = String::from("{\n  \"schema\": \"ss-phased-summary/v3\",\n");
    json.push_str(&format!("  \"utc\": \"{utc}\",\n"));
    json.push_str(&format!("  \"cell\": \"{}\",\n", cell.cell_name()));
    json.push_str(&format!("  \"log_axis\": \"{}\",\n", cell.log_axis()));
    json.push_str(&format!(
        "  \"projection_axis\": \"{}\",\n",
        cell.projection_axis()
    ));
    json.push_str("  \"workers\": 1,\n");
    json.push_str(&format!("  \"inflight\": {},\n", cell.inflight()));
    json.push_str(&format!(
        "  \"rss_before_open_bytes\": {},\n",
        mem_before_open.rss_bytes
    ));
    json.push_str(&format!(
        "  \"hwm_before_open_bytes\": {},\n",
        mem_before_open.hwm_bytes
    ));
    json.push_str(&format!(
        "  \"rss_after_run_bytes\": {},\n",
        mem_end.rss_bytes
    ));
    json.push_str(&format!(
        "  \"hwm_after_run_bytes\": {},\n",
        mem_end.hwm_bytes
    ));
    json.push_str(&format!(
        "  \"rss_delta_bytes\": {},\n",
        mem_end.rss_bytes.saturating_sub(mem_before_open.rss_bytes)
    ));
    json.push_str(&format!(
        "  \"projection_bytes\": {},\n",
        cell.projection_bytes()
    ));
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
