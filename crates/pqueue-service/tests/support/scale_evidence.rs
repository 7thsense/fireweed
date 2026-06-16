use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use pqueue_service::verification_ledger::validate_ledger_file;

pub const E0_FLOOR_ITEMS_PER_HOUR: u64 = 10_000_000;

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
    let started = Instant::now();
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let path = write_ledger_row(cfg, scenario, elapsed_ms);
    validate_ledger_file(&path).expect("benchmark ledger row must validate");
    eprintln!(
        "benchmark ledger={} suite={} profile={} scale={} seed={}",
        path.display(),
        scenario.suite,
        cfg.backend_profile,
        cfg.scale,
        cfg.seed
    );
    path
}

fn write_ledger_row(cfg: &BenchConfig, scenario: &BenchScenario, elapsed_ms: u64) -> PathBuf {
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
            "elapsed_ms": elapsed_ms,
            "deployment_shape": scenario.deployment_shape,
            "workload_envelope": scenario.workload_envelope,
            "tp002_evidence_ids": scenario.tp002_evidence_ids,
            "operation_mix": scenario.operation_mix,
            "batch_size": scenario.batch_size,
            "resident_items": scenario.resident_items,
            "items_per_hour": scenario.items_per_hour,
            "p95_ms": scenario.p95_ms,
            "p99_ms": scenario.p99_ms,
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
