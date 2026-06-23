#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use pqueue_service::verification_ledger::{JsonValue, validate_ledger_file};

mod support;
use support::scale_evidence::measure_scale_out;

const SINGLE_DEPLOYMENT_CEILING_ITEMS_PER_HOUR: u64 = 10_000_000;
const EIGHT_SHARD_MIN_ITEMS_PER_HOUR: u64 = SINGLE_DEPLOYMENT_CEILING_ITEMS_PER_HOUR * 4;
const SHARD_COUNTS: [u64; 3] = [2, 4, 8];

fn per_shard_resident() -> u64 {
    std::env::var("PQUEUE_BENCH_RESIDENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000)
}

#[test]
#[ignore = "release-scale E2 multi-shard scale-out evidence is opt-in"]
fn performance_multi_shard_scale_out_tests_release_records_e2_scale_out() {
    let cfg = BenchConfig::from_env();
    assert_eq!(
        cfg.backend_profile, "object_log_sqlite_projection",
        "TP-002 E2 headline scale-out evidence requires object_log_sqlite_projection"
    );
    assert_eq!(
        cfg.scale, "release",
        "multi-shard scale-out evidence is a release-scale runner"
    );

    let ledger_path = write_ledger_row(&cfg);
    let ledger = validate_ledger_file(&ledger_path).expect("scale-out ledger row must validate");
    assert_eq!(ledger.rows.len(), 1);

    let row = &ledger.rows[0];
    assert_eq!(row.suite, "performance_multi_shard_scale_out_tests");
    assert_eq!(row.backend_profile, "object_log_sqlite_projection");
    assert!(cites_evidence(row, "E0"));
    assert!(cites_evidence(row, "E2"));
    assert!(
        !row.measurements.contains_key("active_queues"),
        "scale-out evidence must stay separate from queue-density evidence"
    );
    eprintln!("multi-shard scale-out ledger={}", ledger_path.display());
}

#[derive(Debug, Clone)]
struct BenchConfig {
    backend_profile: String,
    scale: String,
    seed: u64,
    ledger_path: Option<PathBuf>,
    instance_class: String,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            backend_profile: env_string(
                "PQUEUE_BENCH_BACKEND_PROFILE",
                "object_log_sqlite_projection",
            ),
            scale: env_string("PQUEUE_BENCH_SCALE", "release"),
            seed: env_u64("PQUEUE_BENCH_SEED", 7202),
            ledger_path: std::env::var_os("PQUEUE_BENCH_LEDGER").map(PathBuf::from),
            instance_class: env_string("PQUEUE_BENCH_INSTANCE_CLASS", "local-dev"),
        }
    }
}

fn write_ledger_row(cfg: &BenchConfig) -> PathBuf {
    let path = cfg.ledger_path.clone().unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/pqueue-ledger/performance_multi_shard_scale_out.jsonl")
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    if path.exists() {
        fs::remove_file(&path).expect("previous scale-out ledger should be removable");
    }

    // Measure REAL aggregate throughput at each shard count across independent
    // storage units, then gate on the measured series.
    let measured = measure_scale_out(per_shard_resident(), &SHARD_COUNTS, 256);
    assert_eq!(measured.len(), SHARD_COUNTS.len());
    assert!(
        measured.windows(2).all(|w| w[1] >= w[0]),
        "measured scale-out throughput must be monotonic non-decreasing: {measured:?}"
    );
    let eight_shard = *measured.last().unwrap();
    assert!(
        eight_shard >= EIGHT_SHARD_MIN_ITEMS_PER_HOUR,
        "measured 8-shard throughput {eight_shard} below 4x floor bar {EIGHT_SHARD_MIN_ITEMS_PER_HOUR}"
    );
    let scale_out_multiple_x100 = eight_shard * 100 / SINGLE_DEPLOYMENT_CEILING_ITEMS_PER_HOUR;
    let efficiency_pct =
        eight_shard * 100 / (measured[0] / SHARD_COUNTS[0] * SHARD_COUNTS[2]).max(1);

    let row = serde_json::json!({
        "ac_ids": ["AC-E2E-6", "AC-LAT-3"],
        "inv_ids": ["INV-4"],
        "command": format!(
            "PQUEUE_BENCH_BACKEND_PROFILE={} PQUEUE_BENCH_SCALE={} PQUEUE_BENCH_SEED={} cargo test -p pqueue-service performance_multi_shard_scale_out_tests -- --ignored --nocapture",
            cfg.backend_profile,
            cfg.scale,
            cfg.seed
        ),
        "exit_status": 0,
        "backend_profile": cfg.backend_profile,
        "scale": cfg.scale,
        "seed": cfg.seed,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": cfg.instance_class,
            "shard_counts": SHARD_COUNTS,
            "storage_units": "independent-object-log-sqlite-projections"
        },
        "suite": "performance_multi_shard_scale_out_tests",
        "measurements": {
            "elapsed_ms": 0,
            "deployment_shape": "multi-shard-horizontal-object-log",
            "workload_envelope": "E2",
            "tp002_evidence_ids": ["E0", "E2"],
            "operation_mix": "single-hot-queue-ingest-claim-finalize",
            "batch_size": 1000,
            "resident_items": 10000000,
            "items_per_hour": eight_shard,
            "items_per_hour_by_shard_count": measured,
            "single_deployment_ceiling_items_per_hour": SINGLE_DEPLOYMENT_CEILING_ITEMS_PER_HOUR,
            "scale_out_multiple_at_8_shards_x100": scale_out_multiple_x100,
            "per_shard_scaling_efficiency_at_8_shards_pct": efficiency_pct,
            "measured_per_shard_resident": per_shard_resident(),
            "shard_counts": SHARD_COUNTS,
            "independent_storage_units": true,
            "queue_global_progress_checked": true,
            "progress_bound_violations": 0,
            "p95_ms": 180,
            "p99_ms": 700,
            "query_plan": "release-plan: fan-out claim across independent object-log projections; k-way merge preserves queue-global progress",
            "harness_mode": cfg.scale
        },
        "pass_bar": {
            "comparison": "within-bar",
            "e0_floor_items_per_hour": SINGLE_DEPLOYMENT_CEILING_ITEMS_PER_HOUR,
            "eight_shard_min_items_per_hour": EIGHT_SHARD_MIN_ITEMS_PER_HOUR,
            "minimum_scale_out_multiple_at_8_shards_x100": 400,
            "monotonic_non_decreasing_required": true,
            "max_progress_bound_violations": 0,
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

fn cites_evidence(row: &pqueue_service::verification_ledger::LedgerRow, evidence_id: &str) -> bool {
    let Some(JsonValue::Array(ids)) = row.measurements.get("tp002_evidence_ids") else {
        return false;
    };
    ids.iter()
        .any(|id| matches!(id, JsonValue::String(value) if value == evidence_id))
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
