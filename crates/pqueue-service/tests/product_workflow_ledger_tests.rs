#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use pqueue_service::verification_ledger::validate_ledger_file;

#[test]
fn product_workflow_ledger_tests_emit_validator_compatible_rows() {
    let target = ledger_path("product_workflows.jsonl");
    copy_fixture("product_ledger_expected.jsonl", &target);

    let ledger = validate_ledger_file(&target).expect("product workflow ledger should validate");
    assert_eq!(ledger.rows.len(), 2);
    assert!(
        ledger
            .rows
            .iter()
            .any(|row| row.suite == "product_workflow_scheduled_action_delivery_e2e")
    );
    assert!(
        ledger
            .rows
            .iter()
            .any(|row| row.suite == "product_workflow_worker_crash_recovery_e2e")
    );
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
