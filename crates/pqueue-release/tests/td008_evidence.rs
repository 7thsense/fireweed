//! TD-008 evidence-bundle regression: capture the frontier evidence row from an observed terminal-reap
//! run and reject the legacy hand-authored attestation.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use pqueue_release::{LedgerRow, Measurements, append_row, ledger_path, verify_ledger};

const ARTIFACT_PATH: &str = "docs/perf/evidence/td008-terminal-reap-frontier.jsonl";
const OBSERVED_RUN_COMMAND: &str = "cargo test -p pqueue-projection reap_waits_for_emission -- --nocapture";

fn observed_marker() -> String {
    let output = Command::new("cargo")
        .args([
            "test",
            "-p",
            "pqueue-projection",
            "reap_waits_for_emission",
            "--",
            "--nocapture",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("CARGO_TERM_COLOR", "never")
        .output()
        .expect("run observed terminal-reap suite");

    assert!(
        output.status.success(),
        "observed terminal-reap suite failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find(|line| line.starts_with("TD008_OBSERVED "))
        .unwrap_or_else(|| {
            panic!(
                "observed suite did not print the TD008 marker:\n{}",
                stdout
            )
        })
        .to_string()
}

fn observed_row(observed_marker: &str) -> LedgerRow {
    LedgerRow {
        suite: "td008_terminal_reap_frontier".into(),
        command: OBSERVED_RUN_COMMAND.into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "smoke".into(),
        seed: 0,
        environment: "docs/perf/evidence".into(),
        exit_status: 0,
        ac_ids: vec!["TestTD008ObservedEvidenceRowRecorded".into()],
        inv_ids: vec![],
        pass_bar: "TD-008 terminal reap evidence bundle recorded from an observed run".into(),
        evidence_tier: "smoke".into(),
        measurements: Measurements {
            tp002_evidence_ids: vec![],
            values: BTreeMap::from([
                (
                    "artifact_path".into(),
                    serde_json::json!(ARTIFACT_PATH),
                ),
                ("frontier_rule".into(), serde_json::json!(true)),
                ("observed_run_stdout".into(), serde_json::json!(observed_marker)),
                ("retain_only_opt_out".into(), serde_json::json!(true)),
            ]),
        },
    }
}

fn static_attestation_row() -> LedgerRow {
    LedgerRow {
        suite: "td008_terminal_reap_frontier".into(),
        command: "cargo test -p pqueue-release --test td008_evidence td008_evidence_bundle_recorded -- --nocapture".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "smoke".into(),
        seed: 0,
        environment: "docs/perf/evidence".into(),
        exit_status: 0,
        ac_ids: vec!["TestTD008EvidenceLedgerRejectsStaticAttestation".into()],
        inv_ids: vec![],
        pass_bar: "TD-008 terminal reap evidence bundle recorded".into(),
        evidence_tier: "smoke".into(),
        measurements: Measurements {
            tp002_evidence_ids: vec![],
            values: BTreeMap::from([
                (
                    "artifact_path".into(),
                    serde_json::json!(ARTIFACT_PATH),
                ),
                ("frontier_rule".into(), serde_json::json!(true)),
                ("retain_only_opt_out".into(), serde_json::json!(true)),
            ]),
        },
    }
}

fn tmp_ledger(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "pqueue-release-td008-{tag}-{}.jsonl",
        std::process::id()
    ))
}

fn verify_td008_harness(path: &Path, observed_marker: &str) -> Result<(), String> {
    verify_ledger(path, true)
        .map_err(|errors| {
            errors
                .into_iter()
                .map(|e| e.0)
                .collect::<Vec<_>>()
                .join("; ")
        })?;

    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read td008 evidence bundle at {}: {e}", path.display()))?;
    if !content.contains(observed_marker) {
        return Err(format!(
            "td008 evidence bundle must include the observed run marker; missing {observed_marker:?}"
        ));
    }
    Ok(())
}

#[test]
fn td008_observed_evidence_row_recorded() {
    let path = ledger_path(env!("CARGO_MANIFEST_DIR"), "td008-terminal-reap-frontier");
    let _ = std::fs::remove_file(&path);

    let marker = observed_marker();
    append_row(&path, &observed_row(&marker)).expect("append td008 observed evidence row");
    verify_td008_harness(&path, &marker).expect("observed td008 evidence ledger validates");

    let content = std::fs::read_to_string(&path).expect("read td008 evidence bundle");
    assert!(content.contains("td008_terminal_reap_frontier"));
    assert!(content.contains(ARTIFACT_PATH));
    assert!(content.contains("observed_run_stdout"));
    assert!(content.contains(&marker));
}

#[test]
fn td008_evidence_ledger_rejects_static_attestation() {
    let path = tmp_ledger("static");
    let _ = std::fs::remove_file(&path);

    let marker = observed_marker();
    append_row(&path, &static_attestation_row()).expect("append static td008 evidence row");
    let err = verify_td008_harness(&path, &marker).expect_err("static attestation must be rejected");
    assert!(
        err.contains("observed run marker"),
        "rejection should explain the missing observed run marker: {err}"
    );
}
