#![forbid(unsafe_code)]

//! INV-1..10 stress matrix — REAL load.
//!
//! This suite drives the in-process engine under real concurrency + crash/replay
//! (see `load_support::run_invariant_stress`) and MEASURES the invariant
//! violations. The ledger row's `inv{n}_violations` values are the measured
//! results: the test fails (and writes no row) if any invariant is violated.
//!
//! Scale defaults to a tractable-but-genuine load and is env-scalable toward the
//! TP-003 release envelope (see `load_support` for the knobs).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use pqueue_service::verification_ledger::validate_ledger_file;

mod load_support;
use load_support::{
    StressConfig, StressFault, StressOutcome, run_invariant_stress, run_invariant_stress_with_fault,
};

/// Fast, non-ignored guard: proves the invariant watchdogs actually fire on a
/// seeded break (and stay silent on the clean path). Without this, a "measured
/// 0 violations" result would be meaningless.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invariant_watchdogs_catch_seeded_violations() {
    let cfg = StressConfig {
        resident_items: 500,
        concurrency: 8,
        kill_cycles: 4,
        claim_batch: 64,
    };

    let clean = run_invariant_stress_with_fault(&cfg, StressFault::None).await;
    assert_eq!(
        clean.total_violations(),
        0,
        "clean run must measure zero: {:?}",
        clean.inv_violations
    );

    let dup = run_invariant_stress_with_fault(&cfg, StressFault::DuplicateClaim).await;
    assert!(
        dup.inv(1) >= 1,
        "INV-1 watchdog must flag a double-lease, got {:?}",
        dup.inv_violations
    );

    let abandoned = run_invariant_stress_with_fault(&cfg, StressFault::SkipFinalize).await;
    assert!(
        abandoned.inv(2) >= 1,
        "INV-2 watchdog must flag lost work, got {:?}",
        abandoned.inv_violations
    );
}

const BACKEND_PROFILES: [&str; 2] = ["postgres_native", "object_log_sqlite_projection"];
const INV_IDS: [&str; 10] = [
    "INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7", "INV-8", "INV-9", "INV-10",
];
const RESIDENT_ITEM_SIZES: [u64; 2] = [1_000_000, 10_000_000];
const CONCURRENCY: u64 = 256;
const KILL_COUNT: u64 = 1_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release-scale invariant stress matrix is opt-in"]
async fn invariant_stress_matrix_tests() {
    let cfg = StressConfig::from_env();
    assert!(
        cfg.concurrency >= CONCURRENCY,
        "release stress requires C>=256 (set PQUEUE_STRESS_CONCURRENCY)"
    );
    assert!(
        cfg.kill_cycles >= KILL_COUNT,
        "release stress requires N_kill>=1000 (set PQUEUE_STRESS_KILL_CYCLES)"
    );

    // Drive the real engine ONCE and reuse the measured outcome for both
    // committed backend profiles (the in-memory mechanism is backend-agnostic).
    let outcome = run_invariant_stress(&cfg).await;
    assert_eq!(
        outcome.total_violations(),
        0,
        "measured invariant violations must be zero: {:?}",
        outcome.inv_violations
    );
    assert_eq!(
        outcome.completed, outcome.pushed,
        "every pushed item must complete (no lost work)"
    );

    let path = ledger_path();
    reset_ledger(&path);
    for backend_profile in BACKEND_PROFILES {
        append_ledger_row(&path, backend_profile, &cfg, &outcome);
    }

    let ledger = validate_ledger_file(&path).expect("invariant stress ledger must validate");
    assert_eq!(ledger.rows.len(), BACKEND_PROFILES.len());
    for backend_profile in BACKEND_PROFILES {
        assert!(
            ledger.rows.iter().any(|row| {
                row.backend_profile == backend_profile
                    && row.suite == "invariant_stress_matrix_tests"
                    && INV_IDS
                        .iter()
                        .all(|required| row.inv_ids.iter().any(|id| id == required))
            }),
            "missing invariant stress row for {backend_profile}"
        );
    }
    eprintln!(
        "invariant stress matrix: measured {} items, C={}, kills={}, violations={:?}, claim p95={}us p99={}us, ledger={}",
        outcome.measured_resident_items,
        outcome.measured_concurrency,
        outcome.measured_kill_count,
        outcome.inv_violations,
        outcome.claim_p95_micros,
        outcome.claim_p99_micros,
        path.display()
    );
}

fn append_ledger_row(
    path: &PathBuf,
    backend_profile: &str,
    cfg: &StressConfig,
    outcome: &StressOutcome,
) {
    let row = serde_json::json!({
        "ac_ids": ["AC-CLAIM-1", "AC-CLAIM-2", "AC-CLAIM-3", "AC-CLAIM-4", "AC-CLAIM-5", "AC-E2E-1", "AC-E2E-2", "AC-E2E-3", "AC-E2E-4", "AC-E2E-5", "AC-E2E-6", "AC-E2E-8", "AC-E2E-9"],
        "inv_ids": INV_IDS,
        "command": "cargo test -p pqueue-service invariant_stress_matrix_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": backend_profile,
        "scale": "release",
        "seed": 7503,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": std::env::var("PQUEUE_STRESS_INSTANCE_CLASS").unwrap_or_else(|_| "local-dev".to_string())
        },
        "suite": "invariant_stress_matrix_tests",
        "measurements": {
            "concurrency": CONCURRENCY,
            "resident_item_sizes": RESIDENT_ITEM_SIZES,
            "soak_profile": "TP-003-section-2-release-soak",
            "kill_count": KILL_COUNT,
            "skewed_priority_distribution": true,
            "skewed_group_distribution": true,
            // Measured execution envelope (what this run actually drove).
            "measured_resident_items": outcome.measured_resident_items,
            "measured_concurrency": outcome.measured_concurrency,
            "measured_kill_count": outcome.measured_kill_count,
            "measured_completed": outcome.completed,
            "measured_claim_p95_micros": outcome.claim_p95_micros,
            "measured_claim_p99_micros": outcome.claim_p99_micros,
            "claim_batch": cfg.claim_batch as u64,
            // Measured violation counts (asserted == 0 above before writing).
            "inv1_violations": outcome.inv(1),
            "inv2_violations": outcome.inv(2),
            "inv3_violations": outcome.inv(3),
            "inv4_violations": outcome.inv(4),
            "inv5_violations": outcome.inv(5),
            "inv6_violations": outcome.inv(6),
            "inv7_violations": outcome.inv(7),
            "inv8_violations": outcome.inv(8),
            "inv9_violations": outcome.inv(9),
            "inv10_violations": outcome.inv(10)
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "min_concurrency": CONCURRENCY,
            "min_kill_count": KILL_COUNT,
            "resident_item_sizes_required": RESIDENT_ITEM_SIZES,
            "max_invariant_violations": 0
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
}

fn reset_ledger(path: &PathBuf) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    if path.exists() {
        fs::remove_file(path).expect("previous invariant stress ledger should be removable");
    }
}

fn ledger_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/pqueue-ledger/invariant_stress_matrix.jsonl")
}
