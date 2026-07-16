//! Schema-aware TP-003 transaction evidence verification for shipped Postgres storage pairs.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub const REQUIRED_PROFILES: [&str; 2] = ["postgres/sqlite", "postgres/postgres"];
pub const REQUIRED_ACS: [&str; 4] = ["AC-TXN-1", "AC-TXN-2", "AC-TXN-3", "AC-TXN-6"];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionEvidenceRow {
    pub suite: String,
    pub spec: String,
    pub ac: String,
    pub backend: String,
    pub result: String,
    pub detail: String,
    pub assertions: Vec<String>,
    pub recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionEvidenceError(pub String);

impl std::fmt::Display for TransactionEvidenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionEvidenceSummary {
    pub rows: usize,
    pub satisfied: BTreeSet<(String, String)>,
}

/// Verify AC-TXN-1/2/3/6 for both exact Postgres storage pairs.
///
/// Every required row must pass. Capability omissions may be documented inside
/// a passing row's assertions, but no governing specification currently
/// authorizes a whole-row N/A substitute for an AC-TXN requirement.
/// Coverage gaps, partial/fail/N/A results, duplicate rows, unknown profiles/ACs,
/// and missing pairs fail closed.
pub fn verify_transaction_evidence(
    paths: &[PathBuf],
) -> Result<TransactionEvidenceSummary, Vec<TransactionEvidenceError>> {
    let mut errors = Vec::new();
    if paths.is_empty() {
        return Err(vec![TransactionEvidenceError(
            "at least one transaction evidence file is required".into(),
        )]);
    }

    let mut summary = TransactionEvidenceSummary::default();
    for path in paths {
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) => {
                errors.push(TransactionEvidenceError(format!(
                    "cannot read transaction evidence {}: {error}",
                    path.display()
                )));
                continue;
            }
        };
        let mut file_rows = 0;
        for (index, line) in contents.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            file_rows += 1;
            let row: TransactionEvidenceRow = match serde_json::from_str(line) {
                Ok(row) => row,
                Err(error) => {
                    errors.push(TransactionEvidenceError(format!(
                        "{} line {}: malformed TP-003 row: {error}",
                        path.display(),
                        index + 1
                    )));
                    continue;
                }
            };
            summary.rows += 1;
            let context = format!(
                "{} line {} [{} {}]",
                path.display(),
                index + 1,
                row.backend,
                row.ac
            );
            if row.suite != "external_transaction_contract_matrix_tests" {
                errors.push(TransactionEvidenceError(format!(
                    "{context}: unexpected suite {:?}",
                    row.suite
                )));
            }
            if row.spec != "TP-003 §3.10" {
                errors.push(TransactionEvidenceError(format!(
                    "{context}: unexpected spec {:?}",
                    row.spec
                )));
            }
            if !REQUIRED_PROFILES.contains(&row.backend.as_str()) {
                errors.push(TransactionEvidenceError(format!(
                    "{context}: profile is not an exact shipped Postgres storage pair"
                )));
                continue;
            }
            if !REQUIRED_ACS.contains(&row.ac.as_str()) {
                errors.push(TransactionEvidenceError(format!(
                    "{context}: AC is outside the required exact-pair contract"
                )));
                continue;
            }
            if row.recorded_at.trim().is_empty() {
                errors.push(TransactionEvidenceError(format!(
                    "{context}: recorded_at is empty"
                )));
            }

            let key = (row.backend.clone(), row.ac.clone());
            if !summary.satisfied.insert(key) {
                errors.push(TransactionEvidenceError(format!(
                    "{context}: duplicate authority row"
                )));
                continue;
            }
            match row.result.as_str() {
                "pass" => {
                    if row.assertions.is_empty() {
                        errors.push(TransactionEvidenceError(format!(
                            "{context}: pass row has no assertions"
                        )));
                    }
                    if row
                        .assertions
                        .iter()
                        .any(|assertion| assertion.contains("GAP"))
                    {
                        errors.push(TransactionEvidenceError(format!(
                            "{context}: pass row contains a coverage GAP"
                        )));
                    }
                }
                "n/a" => errors.push(TransactionEvidenceError(format!(
                    "{context}: row-level n/a is not authorized; exact-pair AC evidence must pass"
                ))),
                other => errors.push(TransactionEvidenceError(format!(
                    "{context}: result must be pass, got {other:?}"
                ))),
            }
        }
        if file_rows == 0 {
            errors.push(TransactionEvidenceError(format!(
                "transaction evidence file {} is empty",
                path.display()
            )));
        }
    }

    for profile in REQUIRED_PROFILES {
        for ac in REQUIRED_ACS {
            if !summary
                .satisfied
                .contains(&(profile.to_string(), ac.to_string()))
            {
                errors.push(TransactionEvidenceError(format!(
                    "missing required transaction evidence: profile={profile} ac={ac}"
                )));
            }
        }
    }

    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(errors)
    }
}

pub fn evidence_paths(paths: impl IntoIterator<Item = impl AsRef<Path>>) -> Vec<PathBuf> {
    paths
        .into_iter()
        .map(|path| path.as_ref().to_path_buf())
        .collect()
}
