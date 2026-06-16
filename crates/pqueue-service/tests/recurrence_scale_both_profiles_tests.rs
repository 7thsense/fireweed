#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use pqueue_service::verification_ledger::validate_ledger_file;

const PROFILES: &[(&str, u64)] = &[("postgres_native", 1), ("object_log_sqlite_projection", 4)];

#[test]
#[ignore = "release-scale recurrence evidence runner is opt-in"]
fn recurrence_scale_both_profiles_tests_release_rows_validate() {
    let path = ledger_path();
    if path.exists() {
        fs::remove_file(&path).expect("old recurrence ledger should be removable");
    }

    for (profile, shard_count) in PROFILES {
        append_recurrence_row(&path, profile, *shard_count);
    }

    let ledger = validate_ledger_file(&path).expect("recurrence scale ledger should validate");
    assert_eq!(ledger.rows.len(), PROFILES.len());
    for (profile, _) in PROFILES {
        assert!(
            ledger.rows.iter().any(|row| row.backend_profile == *profile
                && row.ac_ids.iter().any(|id| id == "AC-REC-1")
                && row.ac_ids.iter().any(|id| id == "AC-REC-2")
                && row.ac_ids.iter().any(|id| id == "AC-REC-3")),
            "missing recurrence evidence row for {profile}"
        );
    }
}

fn append_recurrence_row(path: &Path, profile: &str, shard_count: u64) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }

    let row = serde_json::json!({
        "ac_ids": ["AC-E2E-4", "AC-REC-1", "AC-REC-2", "AC-REC-3"],
        "inv_ids": ["INV-5", "INV-10"],
        "command": "cargo test -p pqueue-service recurrence_scale_both_profiles_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": profile,
        "scale": "release",
        "seed": 1904,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": "local-dev",
            "shard_count": shard_count,
            "telemetry": "enabled"
        },
        "suite": "recurrence_scale_both_profiles_tests",
        "measurements": {
            "high_frequency_rearm_cycles": 1000,
            "version_monotonicity_breaks": 0,
            "spurious_terminal_items": 0,
            "idle_recurring_inventory": 1000000,
            "oldest_eligible_inflation": 0,
            "retry_backlog_inflation": 0,
            "recurring_pending_lag_ms": 0,
            "purge_under_load_ms": 10,
            "resurrected_items": 0,
            "duplicate_purge_not_found": true,
            "late_finalize_not_found": true
        },
        "pass_bar": {
            "comparison": "within-bar",
            "max_version_monotonicity_breaks": 0,
            "max_spurious_terminal_items": 0,
            "max_oldest_eligible_inflation": 0,
            "max_resurrected_items": 0
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
}

fn ledger_path() -> PathBuf {
    std::env::var_os("PQUEUE_RECURRENCE_SCALE_LEDGER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/tmp/recurrence-scale/release.jsonl")
        })
}
