//! TD-008 evidence-bundle regression: capture the frontier evidence row from an observed terminal-reap
//! run and reject the legacy hand-authored attestation.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use fireweed_release::{LedgerRow, Measurements, append_row, ledger_path, verify_ledger};

const ARTIFACT_PATH: &str = "docs/perf/evidence/td008-terminal-reap-frontier.jsonl";
const OBSERVED_RUN_COMMAND: &str =
    "cargo test -p fireweed-projection reap_waits_for_emission -- --nocapture";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedTerminalReapRun {
    marker: String,
    reaped: u32,
    lag_before: u32,
    lag_after: u32,
    oldest_unemitted_age_ms: u64,
}

fn observed_run() -> ObservedTerminalReapRun {
    let output = Command::new("cargo")
        .args([
            "test",
            "-p",
            "fireweed-projection",
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
    let marker = stdout
        .lines()
        .find(|line| line.starts_with("TD008_OBSERVED "))
        .unwrap_or_else(|| panic!("observed suite did not print the TD008 marker:\n{}", stdout))
        .to_string();

    parse_observed_run(&marker).unwrap_or_else(|err| panic!("{err}"))
}

fn parse_observed_run(marker: &str) -> Result<ObservedTerminalReapRun, String> {
    let mut parts = marker.split_whitespace();
    let prefix = parts
        .next()
        .ok_or_else(|| "missing TD008 marker prefix".to_string())?;
    let suite = parts
        .next()
        .ok_or_else(|| "missing TD008 marker suite".to_string())?;
    if prefix != "TD008_OBSERVED" || suite != "reap_waits_for_emission" {
        return Err(format!("unexpected TD008 marker header: {marker}"));
    }

    let mut fields = BTreeMap::new();
    for part in parts {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("malformed TD008 marker field: {part}"))?;
        fields.insert(key, value);
    }

    let parse_u32 = |key: &str| -> Result<u32, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("missing TD008 marker field: {key}"))?
            .parse::<u32>()
            .map_err(|e| format!("invalid TD008 marker field {key}: {e}"))
    };
    let parse_u64 = |key: &str| -> Result<u64, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("missing TD008 marker field: {key}"))?
            .parse::<u64>()
            .map_err(|e| format!("invalid TD008 marker field {key}: {e}"))
    };

    Ok(ObservedTerminalReapRun {
        marker: marker.to_string(),
        reaped: parse_u32("reaped")?,
        lag_before: parse_u32("lag_before")?,
        lag_after: parse_u32("lag_after")?,
        oldest_unemitted_age_ms: parse_u64("oldest_unemitted_age_ms")?,
    })
}

fn observed_row(run: &ObservedTerminalReapRun) -> LedgerRow {
    LedgerRow {
        suite: "td008_terminal_reap_frontier".into(),
        command: OBSERVED_RUN_COMMAND.into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "smoke".into(),
        seed: 0,
        environment: "docs/perf/evidence".into(),
        exit_status: 0,
        ac_ids: vec![
            "TestTD008EvidenceBundleRecorded".into(),
            "TestTD008ObservedEvidenceRowMatchesRun".into(),
        ],
        inv_ids: vec![],
        pass_bar: "TD-008 terminal reap evidence bundle recorded from an observed run".into(),
        evidence_tier: "smoke".into(),
        measurements: Measurements {
            tp002_evidence_ids: vec![],
            values: BTreeMap::from([
                ("artifact_path".into(), serde_json::json!(ARTIFACT_PATH)),
                ("frontier_rule".into(), serde_json::json!(true)),
                (
                    "observed_run_stdout".into(),
                    serde_json::json!(run.marker.clone()),
                ),
                ("reaped".into(), serde_json::json!(run.reaped)),
                ("lag_before".into(), serde_json::json!(run.lag_before)),
                ("lag_after".into(), serde_json::json!(run.lag_after)),
                (
                    "oldest_unemitted_age_ms".into(),
                    serde_json::json!(run.oldest_unemitted_age_ms),
                ),
                ("retain_only_opt_out".into(), serde_json::json!(true)),
            ]),
        },
    }
}

fn tmp_ledger(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fireweed-release-td008-{tag}-{}.jsonl",
        std::process::id()
    ))
}

fn verify_td008_harness(path: &Path, observed_marker: &str) -> Result<(), String> {
    verify_ledger(path, true).map_err(|errors| {
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
fn td008_evidence_bundle_recorded() {
    let path = ledger_path(env!("CARGO_MANIFEST_DIR"), "td008-terminal-reap-frontier");
    let _ = std::fs::remove_file(&path);

    let run = observed_run();
    let row = observed_row(&run);
    append_row(&path, &row).expect("append td008 observed evidence row");
    verify_td008_harness(&path, &run.marker).expect("observed td008 evidence ledger validates");

    let content = std::fs::read_to_string(&path).expect("read td008 evidence bundle");
    let serialized = row.to_jsonl();
    assert_eq!(content, format!("{serialized}\n"));

    let parsed: LedgerRow = serde_json::from_str(serialized.as_str()).expect("parse td008 row");
    assert_eq!(parsed, row);
    assert_eq!(
        parsed.measurements.values["artifact_path"],
        serde_json::json!(ARTIFACT_PATH)
    );
    assert_eq!(
        parsed.measurements.values["observed_run_stdout"],
        serde_json::json!(run.marker)
    );
    assert_eq!(
        parsed.measurements.values["reaped"],
        serde_json::json!(run.reaped)
    );
    assert_eq!(
        parsed.measurements.values["lag_before"],
        serde_json::json!(run.lag_before)
    );
    assert_eq!(
        parsed.measurements.values["lag_after"],
        serde_json::json!(run.lag_after)
    );
    assert_eq!(
        parsed.measurements.values["oldest_unemitted_age_ms"],
        serde_json::json!(run.oldest_unemitted_age_ms)
    );
}

#[test]
fn td008_observed_evidence_row_matches_run() {
    let run = observed_run();
    let row = observed_row(&run);

    assert_eq!(row.command, OBSERVED_RUN_COMMAND);
    assert_eq!(
        row.measurements.values["artifact_path"],
        serde_json::json!(ARTIFACT_PATH)
    );
    assert_eq!(
        row.measurements.values["frontier_rule"],
        serde_json::json!(true)
    );
    assert_eq!(
        row.measurements.values["observed_run_stdout"],
        serde_json::json!(run.marker.clone())
    );
    assert_eq!(row.measurements.values["reaped"], serde_json::json!(1u32));
    assert_eq!(
        row.measurements.values["lag_before"],
        serde_json::json!(1u32)
    );
    assert_eq!(
        row.measurements.values["lag_after"],
        serde_json::json!(0u32)
    );
    assert_eq!(
        row.measurements.values["oldest_unemitted_age_ms"],
        serde_json::json!(90_000u64)
    );
    assert_eq!(
        row.measurements.values["retain_only_opt_out"],
        serde_json::json!(true)
    );

    let serialized = row.to_jsonl();
    let parsed: LedgerRow = serde_json::from_str(&serialized).expect("parse td008 row");
    assert_eq!(parsed, row);
}

#[test]
fn td008_evidence_ledger_rejects_static_attestation() {
    let path = tmp_ledger("static");
    let _ = std::fs::remove_file(&path);

    let run = observed_run();
    let row = LedgerRow {
        suite: "td008_terminal_reap_frontier".into(),
        command: "cargo test -p fireweed-release --test td008_evidence td008_evidence_ledger_rejects_static_attestation -- --nocapture".into(),
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
    };
    append_row(&path, &row).expect("append static td008 evidence row");
    let err =
        verify_td008_harness(&path, &run.marker).expect_err("static attestation must be rejected");
    assert!(
        err.contains("observed run marker"),
        "rejection should explain the missing observed run marker: {err}"
    );
}
