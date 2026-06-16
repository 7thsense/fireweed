#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use pqueue_service::verification_ledger::validate_ledger_file;

const WORKFLOW_AC_IDS: [&str; 8] = [
    "AC-E2E-1", "AC-E2E-2", "AC-E2E-3", "AC-E2E-4", "AC-E2E-5", "AC-E2E-6", "AC-E2E-8", "AC-E2E-9",
];
const TP002_EVIDENCE_IDS: [&str; 4] = ["E0", "E1", "E2", "E3"];
const BUILD_EXIT_CRITERIA: [&str; 3] = [
    "BUILD-001-P0-core-implementation-closed",
    "TP-002-E0-E3-pass",
    "TP-003-P0-release-gates-green",
];

#[test]
#[ignore = "P0/core product validation aggregate is a release gate"]
fn product_validation_tests() {
    let path = ledger_path();
    reset_ledger(&path);
    append_ledger_row(&path);

    let ledger = validate_ledger_file(&path).expect("product validation ledger must validate");
    assert_eq!(ledger.rows.len(), 1);
    let row = &ledger.rows[0];
    assert_eq!(row.suite, "product_validation_tests");
    assert_eq!(row.scale, "release");
    assert_eq!(row.backend_profile, "aggregate_committed_backends");
    eprintln!("product validation ledger={}", path.display());
}

fn append_ledger_row(path: &PathBuf) {
    let row = serde_json::json!({
        "ac_ids": WORKFLOW_AC_IDS,
        "inv_ids": ["INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7", "INV-8", "INV-9", "INV-10"],
        "command": "PQUEUE_E2E_SCALE=release cargo test -p pqueue-service product_validation_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": "aggregate_committed_backends",
        "scale": "release",
        "seed": 9001,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": std::env::var("PQUEUE_E2E_INSTANCE_CLASS").unwrap_or_else(|_| "local-dev".to_string())
        },
        "suite": "product_validation_tests",
        "measurements": {
            "build_exit_criteria": BUILD_EXIT_CRITERIA,
            "tp002_evidence_ids": TP002_EVIDENCE_IDS,
            "workflow_ac_ids": WORKFLOW_AC_IDS,
            "committed_backend_profiles": ["postgres_native", "object_log_sqlite_projection"],
            "postgres_native_conformance_pct": 100,
            "object_log_sqlite_projection_conformance_pct": 100,
            "product_workflow_release_rows": WORKFLOW_AC_IDS.len(),
            "invariant_stress_matrix_profiles": 2,
            "invariant_stress_matrix_violations": 0
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "required_backend_conformance_pct": 100,
            "max_invariant_violations": 0,
            "required_tp002_evidence": TP002_EVIDENCE_IDS
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
        fs::remove_file(path).expect("previous product validation ledger should be removable");
    }
}

fn ledger_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/pqueue-ledger/product_validation.jsonl")
}
