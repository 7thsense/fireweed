//! End-to-end: an evidence suite appends rows, the verifier validates them and asserts required evidence.

use std::collections::BTreeMap;

use pqueue_release::{LedgerRow, Measurements, append_row, missing_evidence, verify_ledger};

fn tmp_ledger(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "pqueue-release-ledger-{tag}-{}.jsonl",
        std::process::id()
    ))
}

fn row(suite: &str, exit: i32, evidence: &[&str]) -> LedgerRow {
    LedgerRow {
        suite: suite.into(),
        command: format!("cargo test {suite}"),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "release".into(),
        seed: 42,
        environment: "ci".into(),
        exit_status: exit,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "floor held".into(),
        measurements: Measurements {
            tp002_evidence_ids: evidence.iter().map(|s| s.to_string()).collect(),
            values: BTreeMap::from([("items_per_sec".into(), serde_json::json!(154598))]),
        },
    }
}

#[test]
fn appended_rows_validate_and_evidence_is_asserted() {
    let path = tmp_ledger("ok");
    let _ = std::fs::remove_file(&path);

    append_row(&path, &row("object_log_commit_recovery_tests", 0, &["E3"])).unwrap();
    append_row(
        &path,
        &row("performance_cross_queue_scale_out_tests", 0, &["E2"]),
    )
    .unwrap();

    let summary = verify_ledger(&path, true).expect("strict validation passes");
    assert_eq!(summary.rows, 2);
    assert!(summary.evidence_ids.contains("E2") && summary.evidence_ids.contains("E3"));

    // E2+E3 present, but E0/E1 are not — the gate's require-evidence must report the gap.
    let missing = missing_evidence(&summary, &["E0", "E1", "E2", "E3"].map(String::from));
    assert_eq!(missing, vec!["E0".to_string(), "E1".to_string()]);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn strict_validation_fails_a_failed_run_row() {
    let path = tmp_ledger("failed");
    let _ = std::fs::remove_file(&path);
    append_row(
        &path,
        &row("performance_cross_queue_scale_out_tests", 1, &["E2"]),
    )
    .unwrap();
    let errors = verify_ledger(&path, true).expect_err("a failed-run row must be rejected");
    assert!(errors.iter().any(|e| e.0.contains("exit_status")));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn malformed_line_is_rejected() {
    let path = tmp_ledger("malformed");
    std::fs::write(&path, b"{not valid json}\n").unwrap();
    let errors = verify_ledger(&path, true).expect_err("malformed row rejected");
    assert!(errors.iter().any(|e| e.0.contains("malformed")));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn empty_ledger_fails_strict() {
    let path = tmp_ledger("empty");
    std::fs::write(&path, b"\n  \n").unwrap(); // only blank lines
    let errors = verify_ledger(&path, true).expect_err("an empty ledger is not evidence");
    assert!(errors.iter().any(|e| e.0.contains("empty")));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn missing_file_is_an_error() {
    let path = tmp_ledger("does-not-exist");
    let _ = std::fs::remove_file(&path);
    let errors = verify_ledger(&path, true).expect_err("a missing ledger is an error");
    assert!(errors.iter().any(|e| e.0.contains("cannot open")));
}
