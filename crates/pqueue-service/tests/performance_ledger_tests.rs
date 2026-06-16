#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use pqueue_service::verification_ledger::{JsonValue, validate_ledger_file};

#[test]
fn performance_ledger_tests_emit_validator_compatible_rows() {
    let target = ledger_path("performance.jsonl");
    copy_fixture("performance_ledger_expected.jsonl", &target);

    let ledger = validate_ledger_file(&target).expect("performance ledger should validate");
    assert_eq!(ledger.rows.len(), 2);
    assert!(ledger.rows.iter().any(|row| cites_evidence(row, "E1")));
    assert!(ledger.rows.iter().any(|row| cites_evidence(row, "E2")));
}

fn cites_evidence(row: &pqueue_service::verification_ledger::LedgerRow, evidence_id: &str) -> bool {
    let Some(JsonValue::Array(ids)) = row.measurements.get("tp002_evidence_ids") else {
        return false;
    };
    ids.iter()
        .any(|id| matches!(id, JsonValue::String(value) if value == evidence_id))
}

fn copy_fixture(name: &str, target: &Path) {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    fs::copy(fixture_path(name), target).expect("fixture should copy to canonical ledger path");
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn ledger_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/pqueue-ledger")
        .join(name)
}
