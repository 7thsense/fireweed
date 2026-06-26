//! Release-evidence verification ledger (TP-001/TP-002/TP-003).
//!
//! A *verification ledger* is an append-only JSONL file under `target/pqueue-ledger/`. Each line is one
//! [`LedgerRow`] recording a measured release-evidence run: which suite/command produced it, the
//! backend profile + scale + seed + environment it ran under, the acceptance/invariant ids and TP-002
//! evidence ids it substantiates, the pass bar, the exit status, and the measured values. Evidence suites
//! append rows via [`append_row`]; the CI gate runs the `pqueue-verify-ledger` binary to strict-validate a
//! ledger and assert that required evidence ids (E0–E3) are present.
//!
//! This is the hexagonal-era rebuild of the ledger schema + verifier that lived in the removed
//! `pqueue-service` crate. The required fields match what `scripts/ci/release-gate.sh` validates.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One verification-ledger row: a single measured release-evidence run.
///
/// Every field is required (a row that fails to deserialize is rejected by the verifier). `measurements`
/// carries the TP-002 evidence ids plus the measured values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerRow {
    /// The named test suite that produced this row (e.g. `performance_cross_queue_scale_out_tests`).
    pub suite: String,
    /// The exact command that produced it (for reproduction).
    pub command: String,
    /// Backend profile under test (`postgres_native` | `object_log_sqlite_projection` | `memory` | ...).
    pub backend_profile: String,
    /// Scale shape (`smoke` | `release` | a specific `S=…` descriptor).
    pub scale: String,
    /// Deterministic seed for the run.
    pub seed: u64,
    /// Where it ran (host class / CI lane / `in-process`).
    pub environment: String,
    /// Process exit status of the producing command. A non-zero status is NOT evidence (the run failed).
    pub exit_status: i32,
    /// Acceptance-criterion ids this row substantiates (e.g. `AC-E2E-1`). May be empty for pure scale rows.
    #[serde(default)]
    pub ac_ids: Vec<String>,
    /// Invariant ids held during the run (e.g. `INV-1`). May be empty.
    #[serde(default)]
    pub inv_ids: Vec<String>,
    /// The pass bar this row was judged against (human-readable).
    pub pass_bar: String,
    /// Measured values + the TP-002 evidence ids substantiated.
    pub measurements: Measurements,
}

/// Measured values for a row. [`tp002_evidence_ids`](Self::tp002_evidence_ids) names the E0–E3 records this
/// row substantiates; arbitrary additional measured key/values are kept in [`values`](Self::values).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Measurements {
    /// TP-002 evidence ids this row substantiates (`E0` | `E1` | `E2` | `E3`).
    #[serde(default)]
    pub tp002_evidence_ids: Vec<String>,
    /// Any additional measured values (throughput, latency percentiles, recovery time, …).
    #[serde(flatten, default)]
    pub values: BTreeMap<String, serde_json::Value>,
}

impl LedgerRow {
    /// Serialize this row to a single JSONL line (no trailing newline).
    pub fn to_jsonl(&self) -> String {
        serde_json::to_string(self).expect("LedgerRow serializes")
    }
}

/// Append one row to the ledger at `path`, creating the file (and parent dirs) if needed. The whole line —
/// JSON body AND trailing newline — is written in a SINGLE `write_all`, so under the OS append flag
/// concurrent appenders stay line-atomic for lines below the platform atomic-append size (PIPE_BUF). (A
/// `writeln!` would emit the body and `"\n"` as two separate writes, which O_APPEND could interleave.)
pub fn append_row(path: &Path, row: &LedgerRow) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(format!("{}\n", row.to_jsonl()).as_bytes())
}

/// A validation finding (a reason a ledger row or the ledger as a whole is not acceptable evidence).
#[derive(Debug, Clone, PartialEq)]
pub struct LedgerError(pub String);

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Outcome of validating a ledger: the rows seen and the union of evidence ids they substantiate.
#[derive(Debug, Clone, Default)]
pub struct LedgerSummary {
    pub rows: usize,
    pub evidence_ids: std::collections::BTreeSet<String>,
}

/// Validate a ledger file. In `strict` mode each row must be well-formed AND acceptable evidence (exit 0,
/// non-empty identifying fields, and traceable to at least one acceptance or evidence id). Returns the
/// [`LedgerSummary`] on success, or every [`LedgerError`] found.
pub fn verify_ledger(path: &Path, strict: bool) -> Result<LedgerSummary, Vec<LedgerError>> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return Err(vec![LedgerError(format!(
                "cannot open ledger {path:?}: {e}"
            ))]);
        }
    };
    let mut errors = Vec::new();
    let mut summary = LedgerSummary::default();
    for (i, line) in io::BufReader::new(file).lines().enumerate() {
        let lineno = i + 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: read error: {e}")));
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let row: LedgerRow = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: malformed row: {e}")));
                continue;
            }
        };
        summary.rows += 1;
        summary
            .evidence_ids
            .extend(row.measurements.tp002_evidence_ids.iter().cloned());
        if strict {
            for e in strict_row_errors(&row) {
                errors.push(LedgerError(format!("line {lineno} ({}): {e}", row.suite)));
            }
        }
    }
    if strict && summary.rows == 0 {
        errors.push(LedgerError("ledger is empty".into()));
    }
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(errors)
    }
}

/// Strict-mode acceptability checks for a single row.
fn strict_row_errors(row: &LedgerRow) -> Vec<String> {
    let mut e = Vec::new();
    if row.exit_status != 0 {
        e.push(format!(
            "exit_status {} != 0 (a failed run is not evidence)",
            row.exit_status
        ));
    }
    if row.suite.trim().is_empty() {
        e.push("empty suite".into());
    }
    if row.command.trim().is_empty() {
        e.push("empty command".into());
    }
    if row.backend_profile.trim().is_empty() {
        e.push("empty backend_profile".into());
    }
    if row.scale.trim().is_empty() {
        e.push("empty scale".into());
    }
    if row.environment.trim().is_empty() {
        e.push("empty environment".into());
    }
    if row.pass_bar.trim().is_empty() {
        e.push("empty pass_bar".into());
    }
    if row.ac_ids.is_empty() && row.measurements.tp002_evidence_ids.is_empty() {
        e.push("row cites no ac_ids and no tp002_evidence_ids (untraceable)".into());
    }
    e
}

/// Assert every id in `required` (e.g. `["E0","E1","E2","E3"]`) appears in some row's
/// `measurements.tp002_evidence_ids`. Returns the missing ids (empty = satisfied).
pub fn missing_evidence(summary: &LedgerSummary, required: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|id| !summary.evidence_ids.contains(*id))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(suite: &str, exit: i32, evidence: &[&str]) -> LedgerRow {
        LedgerRow {
            suite: suite.into(),
            command: format!("cargo test {suite}"),
            backend_profile: "memory".into(),
            scale: "smoke".into(),
            seed: 7,
            environment: "in-process".into(),
            exit_status: exit,
            ac_ids: vec!["AC-E2E-1".into()],
            inv_ids: vec!["INV-1".into()],
            pass_bar: "floor held".into(),
            measurements: Measurements {
                tp002_evidence_ids: evidence.iter().map(|s| s.to_string()).collect(),
                values: BTreeMap::from([("items_per_sec".into(), serde_json::json!(123456))]),
            },
        }
    }

    #[test]
    fn row_round_trips_jsonl() {
        let r = row("s", 0, &["E2"]);
        let parsed: LedgerRow = serde_json::from_str(&r.to_jsonl()).unwrap();
        assert_eq!(r, parsed);
        // The flattened measurement value survives the round-trip.
        assert_eq!(
            parsed.measurements.values["items_per_sec"],
            serde_json::json!(123456)
        );
    }

    #[test]
    fn strict_rejects_failed_and_untraceable_rows() {
        // A well-formed, traceable, exit-0 row has no strict errors.
        assert!(strict_row_errors(&row("ok", 0, &["E0"])).is_empty());
        // exit_status != 0.
        assert!(
            strict_row_errors(&row("bad", 1, &["E0"]))
                .iter()
                .any(|e| e.contains("exit_status"))
        );
        // no ac_ids and no evidence ids.
        let mut untraceable = row("u", 0, &[]);
        untraceable.ac_ids.clear();
        assert!(
            strict_row_errors(&untraceable)
                .iter()
                .any(|e| e.contains("untraceable"))
        );
    }

    #[test]
    fn missing_evidence_reports_the_gap() {
        let mut s = LedgerSummary::default();
        s.evidence_ids.extend(["E2".to_string(), "E3".to_string()]);
        let missing = missing_evidence(&s, &["E0", "E1", "E2", "E3"].map(String::from));
        assert_eq!(missing, vec!["E0".to_string(), "E1".to_string()]);
    }
}
