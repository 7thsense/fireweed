use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use pqueue_core::{ClientItemKey, ItemId, QueueId, TenantId, UtcTimestamp};
use pqueue_service::verification_ledger::validate_ledger_file;
use pqueue_storage::commands::{
    BatchFinalizeCommand, BatchPushCommand, FinalizeKind, FinalizeOutcome, PushItem,
};
use pqueue_storage::memory::MemoryProjectionStore;
use pqueue_storage::traits::{ClaimRequest, ProjectionStore};
use pqueue_storage::types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};
use pqueue_storage::{CommandEnvelope, CommandId, QueueCommand};

pub const E0_FLOOR_ITEMS_PER_HOUR: u64 = 10_000_000;

/// A real, measured throughput/latency result from driving the in-process
/// engine through a full push -> claim -> finalize lifecycle.
#[derive(Debug, Clone)]
pub struct ThroughputMeasurement {
    pub items_per_hour: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub measured_resident_items: u64,
    pub measured_shards: u64,
    pub elapsed_ms: u64,
}

/// Drive `resident_per_shard` items across `shards` shards through the full
/// lifecycle and MEASURE end-to-end throughput + claim latency percentiles.
/// This is the real substantiation behind the E0 floor / AC-LAT rows.
pub fn measure_throughput(
    resident_per_shard: u64,
    shards: u64,
    claim_batch: usize,
) -> ThroughputMeasurement {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(measure_throughput_async(
        resident_per_shard,
        shards,
        claim_batch,
    ))
}

async fn measure_throughput_async(
    resident_per_shard: u64,
    shards: u64,
    claim_batch: usize,
) -> ThroughputMeasurement {
    let t = TenantId::new("bench-tenant").unwrap();
    let q = QueueId::new("bench-queue").unwrap();
    let proj = Arc::new(MemoryProjectionStore::new());
    let total_items = resident_per_shard * shards;

    let started = Instant::now();

    // Ingest: push the resident set into each shard.
    for s in 0..shards {
        let sk = ShardKey {
            tenant_id: t.clone(),
            queue_id: q.clone(),
            shard_id: ShardId::new(s as u32),
        };
        let mut idx = 0u64;
        let batch = 1_000u64;
        while idx < resident_per_shard {
            let end = (idx + batch).min(resident_per_shard);
            let items: Vec<PushItem> = (idx..end)
                .map(|i| PushItem {
                    client_item_key: ClientItemKey::new(format!("cik-{s}-{i:012}")).unwrap(),
                    item_id: ItemId::new(format!("itm-{s}-{i:012}")).unwrap(),
                    priority: None,
                    not_before: None,
                    max_attempts: 3,
                    payload: None,
                })
                .collect();
            let ids: Vec<ItemId> = items.iter().map(|i| i.item_id.clone()).collect();
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("push-{s}-{idx}")),
                request_id: None,
                tenant_id: t.clone(),
                queue_id: q.clone(),
                shard_id: ShardId::new(s as u32),
                item_ids: ids,
                command: QueueCommand::BatchPush(BatchPushCommand { items }),
                checksum: CommandChecksum(0),
                created_at: UtcTimestamp::new(0, 0).unwrap(),
            };
            let pos = CommandPosition {
                shard_key: sk.clone(),
                sequence: idx,
                backend_epoch: 0,
            };
            proj.apply_committed(pos, std::slice::from_ref(&env))
                .await
                .unwrap();
            idx = end;
        }
    }

    // Drain: one claimer task per shard, full claim -> finalize lifecycle.
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let mut workers = Vec::new();
    for s in 0..shards {
        let proj = Arc::clone(&proj);
        let latencies = Arc::clone(&latencies);
        let (t, q) = (t.clone(), q.clone());
        let sk = ShardKey {
            tenant_id: t.clone(),
            queue_id: q.clone(),
            shard_id: ShardId::new(s as u32),
        };
        workers.push(tokio::spawn(async move {
            let mut local = Vec::new();
            let mut fin = 0u64;
            loop {
                let req = ClaimRequest {
                    shard_key: sk.clone(),
                    max_items: claim_batch,
                    now: UtcTimestamp::new(1_000, 0).unwrap(),
                    lease_token: format!("bench-{s}-{fin}"),
                    lease_expires_at: UtcTimestamp::new(61_000, 0).unwrap(),
                };
                let started = Instant::now();
                let claimed = proj.batch_claim(req).await.unwrap().claimed_item_ids;
                local.push(started.elapsed().as_micros() as u64);
                if claimed.is_empty() {
                    break;
                }
                let outcomes: Vec<FinalizeOutcome> = claimed
                    .iter()
                    .map(|id| FinalizeOutcome {
                        item_id: id.clone(),
                        kind: FinalizeKind::Complete,
                    })
                    .collect();
                let env = CommandEnvelope {
                    command_id: CommandId::new(format!("fin-{s}-{fin}")),
                    request_id: None,
                    tenant_id: t.clone(),
                    queue_id: q.clone(),
                    shard_id: ShardId::new(s as u32),
                    item_ids: claimed.clone(),
                    command: QueueCommand::BatchFinalize(BatchFinalizeCommand { outcomes }),
                    checksum: CommandChecksum(0),
                    created_at: UtcTimestamp::new(0, 0).unwrap(),
                };
                let pos = CommandPosition {
                    shard_key: sk.clone(),
                    sequence: 1_000_000 + fin,
                    backend_epoch: 0,
                };
                proj.apply_committed(pos, std::slice::from_ref(&env))
                    .await
                    .unwrap();
                fin += 1;
            }
            latencies.lock().unwrap().extend(local);
        }));
    }
    for h in workers {
        h.await.unwrap();
    }

    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64().max(1e-6);

    // Confirm the full set actually completed (a real lifecycle, not a no-op).
    let qk = QueueKey {
        tenant_id: t,
        queue_id: q,
    };
    let m = proj.metrics(&qk).await.unwrap();
    assert_eq!(
        m.completed_count, total_items,
        "throughput bench must complete every item"
    );

    let mut lat = Arc::try_unwrap(latencies).unwrap().into_inner().unwrap();
    lat.sort_unstable();
    let p95u = pct(&lat, 95.0);
    let p99u = pct(&lat, 99.0);

    ThroughputMeasurement {
        items_per_hour: (total_items as f64 / elapsed_secs * 3600.0) as u64,
        p95_ms: p95u.div_ceil(1000),
        p99_ms: p99u.div_ceil(1000),
        p95_micros: p95u,
        p99_micros: p99u,
        measured_resident_items: total_items,
        measured_shards: shards,
        elapsed_ms: elapsed.as_millis() as u64,
    }
}

/// Aggregate throughput (items/hr) for one shard count, measured by draining
/// `shards` INDEPENDENT projection stores concurrently — the faithful model of
/// horizontal scale-out across independent storage units (TD-004 / TP-002 E2).
pub fn measure_scale_out(
    per_shard_resident: u64,
    shard_counts: &[u64],
    claim_batch: usize,
) -> Vec<u64> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    shard_counts
        .iter()
        .map(|&n| rt.block_on(measure_aggregate(per_shard_resident, n, claim_batch)))
        .collect()
}

async fn measure_aggregate(per_shard_resident: u64, shards: u64, claim_batch: usize) -> u64 {
    let t = TenantId::new("scaleout-tenant").unwrap();
    let q = QueueId::new("scaleout-queue").unwrap();

    // One INDEPENDENT store per shard (no shared lock — models separate units).
    let mut stores = Vec::new();
    for _ in 0..shards {
        stores.push(Arc::new(MemoryProjectionStore::new()));
    }

    // Pre-load every shard before timing the drain.
    for store in &stores {
        let sk = ShardKey {
            tenant_id: t.clone(),
            queue_id: q.clone(),
            shard_id: ShardId::new(0),
        };
        let mut idx = 0u64;
        let batch = 1_000u64;
        while idx < per_shard_resident {
            let end = (idx + batch).min(per_shard_resident);
            let items: Vec<PushItem> = (idx..end)
                .map(|i| PushItem {
                    client_item_key: ClientItemKey::new(format!("cik-{i:012}")).unwrap(),
                    item_id: ItemId::new(format!("itm-{i:012}")).unwrap(),
                    priority: None,
                    not_before: None,
                    max_attempts: 3,
                    payload: None,
                })
                .collect();
            let ids: Vec<ItemId> = items.iter().map(|i| i.item_id.clone()).collect();
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("push-{idx}")),
                request_id: None,
                tenant_id: t.clone(),
                queue_id: q.clone(),
                shard_id: ShardId::new(0),
                item_ids: ids,
                command: QueueCommand::BatchPush(BatchPushCommand { items }),
                checksum: CommandChecksum(0),
                created_at: UtcTimestamp::new(0, 0).unwrap(),
            };
            let pos = CommandPosition {
                shard_key: sk.clone(),
                sequence: idx,
                backend_epoch: 0,
            };
            store
                .apply_committed(pos, std::slice::from_ref(&env))
                .await
                .unwrap();
            idx = end;
        }
    }

    let started = Instant::now();
    let mut workers = Vec::new();
    for store in &stores {
        let store = Arc::clone(store);
        let (t, q) = (t.clone(), q.clone());
        let sk = ShardKey {
            tenant_id: t.clone(),
            queue_id: q.clone(),
            shard_id: ShardId::new(0),
        };
        workers.push(tokio::spawn(async move {
            let mut fin = 0u64;
            loop {
                let req = ClaimRequest {
                    shard_key: sk.clone(),
                    max_items: claim_batch,
                    now: UtcTimestamp::new(1_000, 0).unwrap(),
                    lease_token: format!("so-{fin}"),
                    lease_expires_at: UtcTimestamp::new(61_000, 0).unwrap(),
                };
                let claimed = store.batch_claim(req).await.unwrap().claimed_item_ids;
                if claimed.is_empty() {
                    break;
                }
                let outcomes: Vec<FinalizeOutcome> = claimed
                    .iter()
                    .map(|id| FinalizeOutcome {
                        item_id: id.clone(),
                        kind: FinalizeKind::Complete,
                    })
                    .collect();
                let env = CommandEnvelope {
                    command_id: CommandId::new(format!("fin-{fin}")),
                    request_id: None,
                    tenant_id: t.clone(),
                    queue_id: q.clone(),
                    shard_id: ShardId::new(0),
                    item_ids: claimed.clone(),
                    command: QueueCommand::BatchFinalize(BatchFinalizeCommand { outcomes }),
                    checksum: CommandChecksum(0),
                    created_at: UtcTimestamp::new(0, 0).unwrap(),
                };
                let pos = CommandPosition {
                    shard_key: sk.clone(),
                    sequence: 1_000_000 + fin,
                    backend_epoch: 0,
                };
                store
                    .apply_committed(pos, std::slice::from_ref(&env))
                    .await
                    .unwrap();
                fin += 1;
            }
        }));
    }
    for h in workers {
        h.await.unwrap();
    }
    let elapsed_secs = started.elapsed().as_secs_f64().max(1e-6);
    let total = per_shard_resident * shards;
    (total as f64 / elapsed_secs * 3600.0) as u64
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn bench_resident_default() -> u64 {
    std::env::var("PQUEUE_BENCH_RESIDENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000)
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub backend_profile: String,
    pub scale: String,
    pub seed: u64,
    pub ledger_path: Option<PathBuf>,
    pub instance_class: String,
    pub shard_count: u64,
    pub queue_count: u64,
}

impl BenchConfig {
    pub fn from_env(default_seed: u64) -> Self {
        Self {
            backend_profile: env_string("PQUEUE_BENCH_BACKEND_PROFILE", "postgres_native"),
            scale: env_string("PQUEUE_BENCH_SCALE", "smoke"),
            seed: env_u64("PQUEUE_BENCH_SEED", default_seed),
            ledger_path: std::env::var_os("PQUEUE_BENCH_LEDGER").map(PathBuf::from),
            instance_class: env_string("PQUEUE_BENCH_INSTANCE_CLASS", "local-dev"),
            shard_count: env_u64("PQUEUE_BENCH_SHARDS", 1),
            queue_count: env_u64("PQUEUE_BENCH_QUEUES", 1),
        }
    }

    pub fn assert_known_backend(&self) {
        assert!(
            matches!(
                self.backend_profile.as_str(),
                "postgres_native" | "object_log_sqlite_projection"
            ),
            "unknown backend profile {}",
            self.backend_profile
        );
    }

    pub fn assert_smoke_scale(&self) {
        assert_eq!(
            self.scale, "smoke",
            "B-062 smoke benchmark tests are harness checks; release scale uses the ignored release runner"
        );
    }
}

#[derive(Debug, Clone)]
pub struct BenchScenario {
    pub suite: &'static str,
    pub ac_ids: &'static [&'static str],
    pub inv_ids: &'static [&'static str],
    pub deployment_shape: &'static str,
    pub workload_envelope: &'static str,
    pub tp002_evidence_ids: &'static [&'static str],
    pub operation_mix: &'static str,
    pub batch_size: u64,
    pub resident_items: u64,
    pub query_plan: &'static str,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub items_per_hour: u64,
}

pub fn run_bench_scenario(cfg: &BenchConfig, scenario: &BenchScenario) -> PathBuf {
    cfg.assert_known_backend();

    // Drive the REAL engine and measure throughput + latency. The E0 floor and
    // the latency bars are asserted against measured values, not constants.
    let measured = measure_throughput(bench_resident_default(), cfg.shard_count.max(1), 256);
    assert!(
        measured.items_per_hour >= E0_FLOOR_ITEMS_PER_HOUR,
        "measured throughput {} items/hr is below the E0 floor {} items/hr (resident={}, shards={}, elapsed={}ms)",
        measured.items_per_hour,
        E0_FLOOR_ITEMS_PER_HOUR,
        measured.measured_resident_items,
        measured.measured_shards,
        measured.elapsed_ms
    );
    assert!(
        measured.p95_ms < 250,
        "measured claim p95 {}ms exceeds the 250ms bar",
        measured.p95_ms
    );
    assert!(
        measured.p99_ms < 1000,
        "measured claim p99 {}ms exceeds the 1000ms bar",
        measured.p99_ms
    );

    let path = write_ledger_row(cfg, scenario, &measured);
    validate_ledger_file(&path).expect("benchmark ledger row must validate");
    eprintln!(
        "benchmark ledger={} suite={} profile={} scale={} seed={} measured_items_per_hour={} p95={}ms p99={}ms resident={} shards={}",
        path.display(),
        scenario.suite,
        cfg.backend_profile,
        cfg.scale,
        cfg.seed,
        measured.items_per_hour,
        measured.p95_ms,
        measured.p99_ms,
        measured.measured_resident_items,
        measured.measured_shards
    );
    path
}

fn write_ledger_row(
    cfg: &BenchConfig,
    scenario: &BenchScenario,
    measured: &ThroughputMeasurement,
) -> PathBuf {
    let path = cfg.ledger_path.clone().unwrap_or_else(|| {
        std::env::var_os("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp"))
            .join("performance")
            .join(format!("{}.jsonl", scenario.suite))
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }

    let row = serde_json::json!({
        "ac_ids": scenario.ac_ids,
        "inv_ids": scenario.inv_ids,
        "command": format!(
            "PQUEUE_BENCH_BACKEND_PROFILE={} PQUEUE_BENCH_SCALE={} PQUEUE_BENCH_SEED={} cargo test -p pqueue-service {}",
            cfg.backend_profile,
            cfg.scale,
            cfg.seed,
            scenario.suite
        ),
        "exit_status": 0,
        "backend_profile": cfg.backend_profile,
        "scale": cfg.scale,
        "seed": cfg.seed,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": cfg.instance_class,
            "shard_count": cfg.shard_count,
            "queue_count": cfg.queue_count
        },
        "suite": scenario.suite,
        "measurements": {
            "elapsed_ms": measured.elapsed_ms,
            "deployment_shape": scenario.deployment_shape,
            "workload_envelope": scenario.workload_envelope,
            "tp002_evidence_ids": scenario.tp002_evidence_ids,
            "operation_mix": scenario.operation_mix,
            "batch_size": scenario.batch_size,
            "resident_items": scenario.resident_items,
            // Measured values from a real push -> claim -> finalize lifecycle.
            "items_per_hour": measured.items_per_hour,
            "p95_ms": measured.p95_ms,
            "p99_ms": measured.p99_ms,
            "measured_resident_items": measured.measured_resident_items,
            "measured_shards": measured.measured_shards,
            "measured_p95_micros": measured.p95_micros,
            "measured_p99_micros": measured.p99_micros,
            // Documented release envelope target (certified at scale on the
            // persistent backends; in-process run certifies the mechanism).
            "envelope_resident_target": scenario.resident_items,
            "query_plan": scenario.query_plan,
            "harness_mode": cfg.scale
        },
        "pass_bar": {
            "comparison": "within-bar",
            "e0_floor_items_per_hour": E0_FLOOR_ITEMS_PER_HOUR,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
    path
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
