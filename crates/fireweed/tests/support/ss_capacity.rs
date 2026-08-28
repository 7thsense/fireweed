// Shared by the Seventh Sense phased and streaming evidence harnesses.
// Unused-item lints are not meaningful at this layer: each [[test]] binary
// only calls a subset of the helpers.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fireweed::*;
use fireweed_core::{Metadata, MetadataValue};
use serde_json::{Value, json};

pub const STUB_BYTES: usize = 512;
pub const PROFILE_BYTES: usize = 1024;

pub fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MemSample {
    pub rss_bytes: u64,
    pub hwm_bytes: u64,
}

pub fn read_mem() -> MemSample {
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

pub fn count_tree(root: &std::path::Path) -> (u64, u64) {
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

pub fn parent_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("SS_LOG_DIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned()),
    )
}

pub fn unique_suffix() -> String {
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
pub enum Cell {
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
    pub fn parse() -> Self {
        Self::parse_for("phased")
    }

    pub fn parse_for(kind: &str) -> Self {
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
                let root = parent_dir().join(format!("fireweed-ss-{kind}-ol-{}", unique_suffix()));
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
                        parent_dir().join(format!("fireweed-ss-{kind}-olt-{}", unique_suffix()));
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
                        parent_dir().join(format!("fireweed-ss-{kind}-sl-{}.db", unique_suffix()));
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

    pub fn cell_name(&self) -> &'static str {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => "filesystem--memory",
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { .. } => "filesystem--turso",
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => "sqlite--memory",
        }
    }

    pub fn log_axis(&self) -> &'static str {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => "filesystem",
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { .. } => "filesystem",
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => "sqlite",
        }
    }

    pub fn projection_axis(&self) -> &'static str {
        match self {
            Self::ObjectLogFilesystemMemory { .. } => "memory",
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { .. } => "turso",
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => "memory",
        }
    }

    pub fn inflight(&self) -> usize {
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

    pub fn open(&self, clock: Arc<dyn Clock>) -> Fireweed {
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

    pub fn describe(&self) -> String {
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

    pub fn cleanup(&self) {
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

    pub fn log_root(&self) -> Option<&std::path::Path> {
        match self {
            Self::ObjectLogFilesystemMemory { root } => Some(root),
            #[cfg(feature = "turso")]
            Self::ObjectLogFilesystemTurso { log_root, .. } => Some(log_root),
            #[cfg(feature = "sqlite")]
            Self::SqliteCommandLogMemory { .. } => None,
        }
    }

    pub fn projection_bytes(&self) -> u64 {
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

pub fn phase_meta(phase: &str) -> Metadata {
    let mut md = Metadata::new();
    md.insert("phase", MetadataValue::String(phase.to_string()));
    md
}

pub fn qdef(tenant: &str, queue: &str, push_batch: usize, claim_batch: usize) -> QueueDefinition {
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

#[derive(Default)]
pub struct CallStats {
    pub samples: Vec<Duration>,
}

impl CallStats {
    pub fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }
    pub fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }
    pub fn percentile_ms(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut v = self.samples.clone();
        v.sort_unstable();
        let idx = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
        v[idx.min(v.len() - 1)].as_secs_f64() * 1000.0
    }

    pub fn evidence(&self, op: &str) -> Value {
        json!({
            "op": op,
            "samples": self.samples.len(),
            "p50_ms": self.percentile_ms(50.0),
            "p95_ms": self.percentile_ms(95.0),
            "p99_ms": self.percentile_ms(99.0),
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct ResidualSnapshot {
    pub pending: u64,
    pub leased: u64,
    pub complete: u64,
    pub failed: u64,
    pub eligible: usize,
}

impl ResidualSnapshot {
    pub fn evidence(&self) -> Value {
        json!({
            "pending": self.pending,
            "leased": self.leased,
            "complete": self.complete,
            "failed": self.failed,
            "eligible": self.eligible,
        })
    }
}

pub async fn settle_phase(
    fw: &Fireweed,
    queue: &QueueKey,
    phase_started: Instant,
    eligible_limit: usize,
) -> (Duration, ResidualSnapshot) {
    // `metrics` on the derived object-log × Turso composition catches the
    // projection up to the durable log frontier before reading counts. `peek`
    // then records residual eligible work from that same settled projection.
    let metrics = fw.metrics(queue).await.expect("phase settlement metrics");
    let eligible = fw
        .peek(queue, eligible_limit)
        .await
        .expect("phase settlement eligible residual")
        .len();
    (
        phase_started.elapsed(),
        ResidualSnapshot {
            pending: metrics.pending,
            leased: metrics.leased,
            complete: metrics.complete,
            failed: metrics.failed,
            eligible,
        },
    )
}

pub fn key(i: usize) -> ClientItemKey {
    ClientItemKey::new(format!("ss-{i:08}")).unwrap()
}

pub fn job_key(i: usize, n: usize) -> GroupKey {
    let jobs = (n / 100).max(50);
    GroupKey::new(format!("job-{}", i % jobs)).unwrap()
}

pub fn source_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

pub fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".into())
}

pub fn chrono_like_utc() -> String {
    let s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{s}")
}
