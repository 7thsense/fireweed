//! Host-independent verification for the E3 object-log projection contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::transaction::TransactionEvidenceRow;
use crate::{LedgerRow, verify_ledger};

pub const REQUIRED_E3_PROFILES: [&str; 2] = [
    "object_log_inmemory_projection",
    "object_log_sqlite_projection",
];
pub const REQUIRED_BOUNDS_MS: [u64; 4] = [1, 5, 20, 100];
pub const REQUIRED_TXN_ACS: [&str; 6] = [
    "AC-TXN-1", "AC-TXN-2", "AC-TXN-3", "AC-TXN-4", "AC-TXN-6", "AC-TXN-7",
];
pub const FENCE_SUITE: &str = "segmented_object_log_commits_through_minio";
pub const FENCE_PROFILE: &str = "minio_create_only_cas";
pub const FENCE_MODE: &str = "create_only_put_if_absent";
pub const NO_CAS_REASON: &str = "release_profile_requires_create_only_cas";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3ContractManifest {
    pub schema_version: u32,
    pub source_revision: String,
    pub e3_ledger: String,
    pub transaction_evidence: String,
    pub fencing_evidence: String,
    pub entries: Vec<E3ContractEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3ContractEntry {
    pub profile: String,
    pub bound_ms: u64,
    pub request_id_timing: RequestIdTiming,
    pub transaction_authorities: Vec<E3TransactionAuthority>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestIdTiming {
    ForceSealedConfigIndependent,
    LatencyWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3TransactionAuthority {
    pub ac: String,
    pub backend: String,
    pub applicability: Applicability,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Applicability {
    Pass,
    CapabilityNa { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoCasStatus {
    Proven,
    Excluded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoCasDisposition {
    pub status: NoCasStatus,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3FenceEvidenceRow {
    pub schema_version: u32,
    pub suite: String,
    pub source_revision: String,
    pub store_profile: String,
    pub result: String,
    pub stale_epoch_rejected: bool,
    pub current_epoch_committed: bool,
    pub cas_mode: String,
    pub no_cas: NoCasDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E3FenceObservation {
    pub source_revision: String,
    pub stale_epoch_rejected: bool,
    pub current_epoch_committed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E3ContractError(pub String);

impl std::fmt::Display for E3ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E3ContractSummary {
    pub entries: usize,
    pub transaction_rows: usize,
}

pub fn build_e3_fence_evidence(
    observation: E3FenceObservation,
) -> Result<E3FenceEvidenceRow, E3ContractError> {
    if !valid_revision(&observation.source_revision) {
        return Err(E3ContractError(
            "fence source_revision must be a 40-character lowercase hex revision".into(),
        ));
    }
    Ok(E3FenceEvidenceRow {
        schema_version: 1,
        suite: FENCE_SUITE.into(),
        source_revision: observation.source_revision,
        store_profile: FENCE_PROFILE.into(),
        result: if observation.stale_epoch_rejected && observation.current_epoch_committed {
            "pass"
        } else {
            "fail"
        }
        .into(),
        stale_epoch_rejected: observation.stale_epoch_rejected,
        current_epoch_committed: observation.current_epoch_committed,
        cas_mode: FENCE_MODE.into(),
        no_cas: NoCasDisposition {
            status: NoCasStatus::Excluded,
            reason: NO_CAS_REASON.into(),
        },
    })
}

pub fn write_e3_fence_evidence(path: &Path, row: &E3FenceEvidenceRow) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("tmp");
    let body = serde_json::to_vec_pretty(row).expect("E3FenceEvidenceRow serializes");
    fs::write(&temp, body)?;
    fs::rename(temp, path)
}

pub fn verify_e3_contract(manifest_path: &Path) -> Result<E3ContractSummary, Vec<E3ContractError>> {
    let body = match fs::read_to_string(manifest_path) {
        Ok(body) => body,
        Err(error) => {
            return Err(vec![E3ContractError(format!(
                "cannot read E3 contract {}: {error}",
                manifest_path.display()
            ))]);
        }
    };
    let manifest: E3ContractManifest = match serde_json::from_str(&body) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Err(vec![E3ContractError(format!(
                "malformed E3 contract {}: {error}",
                manifest_path.display()
            ))]);
        }
    };
    let mut errors = Vec::new();
    if manifest.schema_version != 1 {
        errors.push(E3ContractError(format!(
            "unsupported schema_version {}",
            manifest.schema_version
        )));
    }
    if !valid_revision(&manifest.source_revision) {
        errors.push(E3ContractError(
            "source_revision must be a 40-character lowercase hex revision".into(),
        ));
    }
    let Some(base) = manifest_path.parent() else {
        return Err(vec![E3ContractError(
            "manifest has no parent directory".into(),
        )]);
    };
    let ledger_path = resolve_authority(base, &manifest.e3_ledger, "e3_ledger", &mut errors);
    let txn_path = resolve_authority(
        base,
        &manifest.transaction_evidence,
        "transaction_evidence",
        &mut errors,
    );
    let fence_path = resolve_authority(
        base,
        &manifest.fencing_evidence,
        "fencing_evidence",
        &mut errors,
    );

    if let Some(path) = ledger_path {
        verify_e3_ledger(&path, &manifest.source_revision, &mut errors);
    }
    let txn_rows = txn_path
        .as_ref()
        .map(|path| read_transaction_rows(path, &mut errors))
        .unwrap_or_default();
    if let Some(path) = fence_path {
        verify_fence(&path, &manifest.source_revision, &mut errors);
    }
    verify_entries(&manifest.entries, &txn_rows, &mut errors);

    if errors.is_empty() {
        Ok(E3ContractSummary {
            entries: manifest.entries.len(),
            transaction_rows: txn_rows.len(),
        })
    } else {
        Err(errors)
    }
}

fn valid_revision(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn resolve_authority(
    base: &Path,
    relative: &str,
    field: &str,
    errors: &mut Vec<E3ContractError>,
) -> Option<PathBuf> {
    let path = Path::new(relative);
    if relative.trim().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        errors.push(E3ContractError(format!(
            "{field} must be a non-empty safe relative path"
        )));
        None
    } else {
        Some(base.join(path))
    }
}

fn verify_e3_ledger(path: &Path, revision: &str, errors: &mut Vec<E3ContractError>) {
    if let Err(findings) = verify_ledger(path, true) {
        errors.extend(
            findings
                .into_iter()
                .map(|error| E3ContractError(format!("E3 ledger {}: {error}", path.display()))),
        );
        return;
    }
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "cannot read E3 ledger {}: {error}",
                path.display()
            )));
            return;
        }
    };
    let mut rows = BTreeMap::new();
    for (index, line) in body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let Ok(row) = serde_json::from_str::<LedgerRow>(line) else {
            continue;
        };
        if rows
            .insert(row.backend_profile.clone(), row.clone())
            .is_some()
        {
            errors.push(E3ContractError(format!(
                "E3 ledger line {} duplicates profile {}",
                index + 1,
                row.backend_profile
            )));
        }
    }
    for profile in REQUIRED_E3_PROFILES {
        let Some(row) = rows.remove(profile) else {
            errors.push(E3ContractError(format!(
                "missing E3 ledger profile {profile}"
            )));
            continue;
        };
        if row.evidence_tier != "release" || row.scale != "release" {
            errors.push(E3ContractError(format!(
                "E3 ledger profile {profile} must be release tier and scale"
            )));
        }
        if !row
            .measurements
            .tp002_evidence_ids
            .iter()
            .any(|id| id == "E3")
        {
            errors.push(E3ContractError(format!(
                "E3 ledger profile {profile} does not substantiate E3"
            )));
        }
        require_value(&row, "source_revision", serde_json::json!(revision), errors);
        require_value(&row, "bound_count", serde_json::json!(4), errors);
        require_value(&row, "bars_met", serde_json::json!(true), errors);
        for bound in REQUIRED_BOUNDS_MS {
            require_value(
                &row,
                &format!("bound_{bound}ms_bar_met"),
                serde_json::json!(true),
                errors,
            );
        }
    }
    for profile in rows.keys() {
        errors.push(E3ContractError(format!(
            "unexpected E3 ledger profile {profile}"
        )));
    }
}

fn require_value(
    row: &LedgerRow,
    key: &str,
    expected: serde_json::Value,
    errors: &mut Vec<E3ContractError>,
) {
    if row.measurements.values.get(key) != Some(&expected) {
        errors.push(E3ContractError(format!(
            "E3 ledger profile {} requires {key}={expected}",
            row.backend_profile
        )));
    }
}

fn read_transaction_rows(
    path: &Path,
    errors: &mut Vec<E3ContractError>,
) -> Vec<TransactionEvidenceRow> {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "cannot read transaction evidence {}: {error}",
                path.display()
            )));
            return Vec::new();
        }
    };
    let mut rows = Vec::new();
    for (index, line) in body.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(line) {
            Ok(row) => rows.push(row),
            Err(error) => errors.push(E3ContractError(format!(
                "{} line {}: malformed TP-003 row: {error}",
                path.display(),
                index + 1
            ))),
        }
    }
    if rows.is_empty() {
        errors.push(E3ContractError(format!(
            "transaction evidence {} is empty",
            path.display()
        )));
    }
    rows
}

fn verify_entries(
    entries: &[E3ContractEntry],
    rows: &[TransactionEvidenceRow],
    errors: &mut Vec<E3ContractError>,
) {
    let mut pairs = BTreeSet::new();
    for entry in entries {
        let context = format!("profile={} bound={}ms", entry.profile, entry.bound_ms);
        if !REQUIRED_E3_PROFILES.contains(&entry.profile.as_str()) {
            errors.push(E3ContractError(format!("{context}: unexpected profile")));
        }
        if !REQUIRED_BOUNDS_MS.contains(&entry.bound_ms) {
            errors.push(E3ContractError(format!("{context}: unexpected bound")));
        }
        if !pairs.insert((entry.profile.clone(), entry.bound_ms)) {
            errors.push(E3ContractError(format!("{context}: duplicate entry")));
        }
        if entry.request_id_timing != RequestIdTiming::ForceSealedConfigIndependent {
            errors.push(E3ContractError(format!(
                "{context}: force-sealed request_id evidence must not be labeled latency-window timed"
            )));
        }
        let mut acs = BTreeSet::new();
        for authority in &entry.transaction_authorities {
            if !acs.insert(authority.ac.clone()) {
                errors.push(E3ContractError(format!(
                    "{context}: duplicate transaction authority {}",
                    authority.ac
                )));
                continue;
            }
            if !REQUIRED_TXN_ACS.contains(&authority.ac.as_str()) {
                errors.push(E3ContractError(format!(
                    "{context}: unexpected transaction AC {}",
                    authority.ac
                )));
                continue;
            }
            if let Applicability::CapabilityNa { reason } = &authority.applicability {
                errors.push(E3ContractError(format!(
                    "{context}: {} capability n/a is not authorized ({reason})",
                    authority.ac
                )));
                continue;
            }
            let candidates: Vec<_> = rows
                .iter()
                .filter(|row| row.ac == authority.ac && row.backend == authority.backend)
                .collect();
            if candidates.len() != 1 {
                errors.push(E3ContractError(format!(
                    "{context}: {} requires exactly one TP-003 row for backend {:?}, found {}",
                    authority.ac,
                    authority.backend,
                    candidates.len()
                )));
                continue;
            }
            let row = candidates[0];
            if row.suite.trim().is_empty()
                || row.spec != "TP-003 §3.10"
                || row.result != "pass"
                || row.assertions.is_empty()
                || row
                    .assertions
                    .iter()
                    .any(|assertion| assertion.contains("GAP"))
            {
                errors.push(E3ContractError(format!(
                    "{context}: {} TP-003 authority is not a complete passing row",
                    authority.ac
                )));
            }
            if authority.ac == "AC-TXN-7"
                && !row
                    .assertions
                    .iter()
                    .any(|assertion| assertion.contains(&format!("bound={}ms", entry.bound_ms)))
            {
                errors.push(E3ContractError(format!(
                    "{context}: AC-TXN-7 row lacks the selected bound assertion"
                )));
            }
        }
        for ac in REQUIRED_TXN_ACS {
            if !acs.contains(ac) {
                errors.push(E3ContractError(format!(
                    "{context}: missing transaction authority {ac}"
                )));
            }
        }
    }
    for profile in REQUIRED_E3_PROFILES {
        for bound in REQUIRED_BOUNDS_MS {
            if !pairs.contains(&(profile.to_string(), bound)) {
                errors.push(E3ContractError(format!(
                    "missing E3 contract entry: profile={profile} bound={bound}ms"
                )));
            }
        }
    }
}

fn verify_fence(path: &Path, revision: &str, errors: &mut Vec<E3ContractError>) {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "cannot read fencing evidence {}: {error}",
                path.display()
            )));
            return;
        }
    };
    let row: E3FenceEvidenceRow = match serde_json::from_str(&body) {
        Ok(row) => row,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "malformed fencing evidence {}: {error}",
                path.display()
            )));
            return;
        }
    };
    if row.schema_version != 1
        || row.suite != FENCE_SUITE
        || row.source_revision != revision
        || row.store_profile != FENCE_PROFILE
        || row.result != "pass"
        || !row.stale_epoch_rejected
        || !row.current_epoch_committed
        || row.cas_mode != FENCE_MODE
        || row.no_cas.status != NoCasStatus::Excluded
        || row.no_cas.reason != NO_CAS_REASON
    {
        errors.push(E3ContractError(format!(
            "fencing evidence {} does not prove stale rejection/current commit under the release CAS profile with the authorized no-CAS exclusion",
            path.display()
        )));
    }
}
