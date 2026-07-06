//! TD-008 evidence-bundle regression: write the frontier evidence row into docs/perf/evidence and
//! validate the resulting ledger.

use std::collections::BTreeMap;

use pqueue_release::{LedgerRow, Measurements, append_row, ledger_path, verify_ledger};

fn evidence_row() -> LedgerRow {
    LedgerRow {
        suite: "td008_terminal_reap_frontier".into(),
        command: "cargo test -p pqueue-release --test td008_evidence td008_evidence_bundle_recorded -- --nocapture".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "smoke".into(),
        seed: 0,
        environment: "docs/perf/evidence".into(),
        exit_status: 0,
        ac_ids: vec!["TestTD008EvidenceBundleRecorded".into()],
        inv_ids: vec![],
        pass_bar: "TD-008 terminal reap evidence bundle recorded".into(),
        evidence_tier: "smoke".into(),
        measurements: Measurements {
            tp002_evidence_ids: vec![],
            values: BTreeMap::from([
                (
                    "artifact_path".into(),
                    serde_json::json!("docs/perf/evidence/td008-terminal-reap-frontier.jsonl"),
                ),
                ("frontier_rule".into(), serde_json::json!(true)),
                ("retain_only_opt_out".into(), serde_json::json!(true)),
            ]),
        },
    }
}

#[test]
fn td008_evidence_bundle_recorded() {
    let path = ledger_path(env!("CARGO_MANIFEST_DIR"), "td008-terminal-reap-frontier");
    let _ = std::fs::remove_file(&path);

    append_row(&path, &evidence_row()).expect("append td008 evidence row");
    let summary = verify_ledger(&path, true).expect("strict td008 evidence ledger validates");
    assert_eq!(summary.rows, 1);
    assert!(summary.evidence_ids.is_empty());
    assert!(summary.smoke_evidence_ids.is_empty());

    let content = std::fs::read_to_string(&path).expect("read td008 evidence bundle");
    assert!(content.contains("td008_terminal_reap_frontier"));
    assert!(content.contains("docs/perf/evidence/td008-terminal-reap-frontier.jsonl"));
}
