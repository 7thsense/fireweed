//! Host-independent verification for the E3 object-log projection contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::cost::{
    PriceInputs, WorkloadAssumptions, build_release_cost_rows, release_cost_inputs,
    validate_release_cost_rows,
};
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
pub const E3_CONTRACT_SCHEMA_VERSION: u32 = 2;
pub const FENCE_SUITE: &str = "segmented_object_log_commits_through_minio";
pub const FENCE_PROFILE: &str = "minio_create_only_cas";
pub const FENCE_MODE: &str = "create_only_put_if_absent";
pub const NO_CAS_REASON: &str = "release_profile_requires_create_only_cas";
pub const TRANSACTION_SUITE: &str = "external_transaction_contract_matrix_tests";
pub const E3_PRODUCER_SUITE: &str = "performance_object_log_e3_live_tests";
pub const E3_PRODUCER_COMMAND: &str = "scripts/perf/tp002-e3-minio.sh";
static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3ContractManifest {
    pub schema_version: u32,
    pub source_revision: String,
    pub e3_ledger: String,
    pub transaction_evidence: String,
    pub fencing_evidence: String,
    pub ac7_binding: Ac7Binding,
    pub entries: Vec<E3ContractEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ac7Binding {
    pub suite: String,
    pub backend: String,
    pub bounds_ms: Vec<u64>,
    pub latency_window_timing: RequestIdTiming,
    pub request_id_timing: RequestIdTiming,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3ContractEntry {
    pub profile: String,
    pub bound_ms: u64,
    pub request_id_timing: RequestIdTiming,
    pub manifest_fence: E3FenceAuthority,
    pub transaction_authorities: Vec<E3TransactionAuthority>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3FenceAuthority {
    pub suite: String,
    pub store_profile: String,
    pub applicability: Applicability,
    pub no_cas: NoCasDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestIdTiming {
    ForceSealedConfigIndependent,
    LatencyWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E3TransactionAuthority {
    pub ac: String,
    pub backend: String,
    pub applicability: Applicability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub cost_rows: usize,
}

pub fn build_e3_contract_manifest(
    source_revision: String,
    e3_ledger: String,
    transaction_evidence: String,
    fencing_evidence: String,
) -> Result<E3ContractManifest, E3ContractError> {
    if !valid_revision(&source_revision) {
        return Err(E3ContractError(
            "contract source_revision must be a 40-character lowercase hex revision".into(),
        ));
    }
    let no_cas = NoCasDisposition {
        status: NoCasStatus::Excluded,
        reason: NO_CAS_REASON.into(),
    };
    let entries = REQUIRED_E3_PROFILES
        .into_iter()
        .flat_map(|profile| {
            let no_cas = no_cas.clone();
            REQUIRED_BOUNDS_MS.into_iter().map(move |bound_ms| {
                let transaction_authorities = REQUIRED_TXN_ACS
                    .into_iter()
                    .map(|ac| E3TransactionAuthority {
                        ac: ac.into(),
                        backend: governed_backend(profile, ac)
                            .expect("governed profile/AC matrix is complete")
                            .into(),
                        applicability: Applicability::Pass,
                    })
                    .collect();
                E3ContractEntry {
                    profile: profile.into(),
                    bound_ms,
                    request_id_timing: RequestIdTiming::ForceSealedConfigIndependent,
                    manifest_fence: E3FenceAuthority {
                        suite: FENCE_SUITE.into(),
                        store_profile: FENCE_PROFILE.into(),
                        applicability: Applicability::Pass,
                        no_cas: no_cas.clone(),
                    },
                    transaction_authorities,
                }
            })
        })
        .collect();
    Ok(E3ContractManifest {
        schema_version: E3_CONTRACT_SCHEMA_VERSION,
        source_revision,
        e3_ledger,
        transaction_evidence,
        fencing_evidence,
        ac7_binding: Ac7Binding {
            suite: TRANSACTION_SUITE.into(),
            backend: "objectlog(force-seal|group-commit)".into(),
            bounds_ms: REQUIRED_BOUNDS_MS.to_vec(),
            latency_window_timing: RequestIdTiming::LatencyWindow,
            request_id_timing: RequestIdTiming::ForceSealedConfigIndependent,
        },
        entries,
    })
}

pub fn write_e3_contract(path: &Path, manifest: &E3ContractManifest) -> std::io::Result<()> {
    let body = serde_json::to_vec_pretty(manifest).expect("E3ContractManifest serializes");
    atomic_write_evidence(path, &body, "E3 contract")
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
    let body = serde_json::to_vec_pretty(row).expect("E3FenceEvidenceRow serializes");
    atomic_write_evidence(path, &body, "fence evidence")
}

fn atomic_write_evidence(path: &Path, body: &[u8], label: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        let mut cursor = PathBuf::new();
        for component in parent.components() {
            cursor.push(component.as_os_str());
            if fs::symlink_metadata(&cursor).is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return Err(std::io::Error::other(format!(
                    "refusing a symlinked {label} parent path"
                )));
            }
        }
    }
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(std::io::Error::other(format!(
            "refusing to replace a symlink {label} target"
        )));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| std::io::Error::other("fence-evidence path requires a UTF-8 file name"))?;
    let (temp, mut file) = loop {
        let nonce = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    if let Err(error) = file.write_all(body).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    Ok(())
}

pub fn verify_e3_contract(
    manifest_path: &Path,
    expected_revision: &str,
) -> Result<E3ContractSummary, Vec<E3ContractError>> {
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
    if manifest.schema_version != E3_CONTRACT_SCHEMA_VERSION {
        errors.push(E3ContractError(format!(
            "unsupported schema_version {}; expected {E3_CONTRACT_SCHEMA_VERSION}",
            manifest.schema_version,
        )));
    }
    if !valid_revision(&manifest.source_revision) {
        errors.push(E3ContractError(
            "source_revision must be a 40-character lowercase hex revision".into(),
        ));
    }
    if !valid_revision(expected_revision) {
        errors.push(E3ContractError(
            "expected revision must be a 40-character lowercase hex revision".into(),
        ));
    } else if manifest.source_revision != expected_revision {
        errors.push(E3ContractError(format!(
            "contract source_revision {} does not match expected revision {expected_revision}",
            manifest.source_revision
        )));
    }
    let Some(base) = manifest_path.parent() else {
        return Err(vec![E3ContractError(
            "manifest has no parent directory".into(),
        )]);
    };
    let canonical_base = match base.canonicalize() {
        Ok(base) => base,
        Err(error) => {
            return Err(vec![E3ContractError(format!(
                "cannot canonicalize manifest directory {}: {error}",
                base.display()
            ))]);
        }
    };
    let ledger_path = resolve_authority(
        base,
        &canonical_base,
        &manifest.e3_ledger,
        "e3_ledger",
        &mut errors,
    );
    let txn_path = resolve_authority(
        base,
        &canonical_base,
        &manifest.transaction_evidence,
        "transaction_evidence",
        &mut errors,
    );
    let fence_path = resolve_authority(
        base,
        &canonical_base,
        &manifest.fencing_evidence,
        "fencing_evidence",
        &mut errors,
    );

    let e3_rows = ledger_path
        .as_ref()
        .map(|path| verify_e3_ledger(path, &manifest.source_revision, &mut errors))
        .unwrap_or_default();
    let cost_rows = verify_cost_contract(&e3_rows, &mut errors);
    let txn_rows = txn_path
        .as_ref()
        .map(|path| read_transaction_rows(path, &mut errors))
        .unwrap_or_default();
    let fence = fence_path
        .as_ref()
        .and_then(|path| verify_fence(path, &manifest.source_revision, &mut errors));
    verify_entries(
        &manifest.entries,
        &manifest.ac7_binding,
        &txn_rows,
        fence.as_ref(),
        &mut errors,
    );

    if errors.is_empty() {
        Ok(E3ContractSummary {
            entries: manifest.entries.len(),
            transaction_rows: txn_rows.len(),
            cost_rows,
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
    canonical_base: &Path,
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
        let candidate = base.join(path);
        let mut cursor = base.to_path_buf();
        for component in path.components() {
            cursor.push(component.as_os_str());
            match fs::symlink_metadata(&cursor) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    errors.push(E3ContractError(format!(
                        "{field} path {relative:?} contains a symlink"
                    )));
                    return None;
                }
                Ok(_) => {}
                Err(error) => {
                    errors.push(E3ContractError(format!(
                        "cannot inspect {field} path {}: {error}",
                        cursor.display()
                    )));
                    return None;
                }
            }
        }
        match candidate.canonicalize() {
            Ok(canonical) if canonical.starts_with(canonical_base) => Some(canonical),
            Ok(_) => {
                errors.push(E3ContractError(format!(
                    "{field} path {relative:?} escapes the manifest directory"
                )));
                None
            }
            Err(error) => {
                errors.push(E3ContractError(format!(
                    "cannot canonicalize {field} path {}: {error}",
                    candidate.display()
                )));
                None
            }
        }
    }
}

fn verify_e3_ledger(
    path: &Path,
    revision: &str,
    errors: &mut Vec<E3ContractError>,
) -> Vec<LedgerRow> {
    if let Err(findings) = verify_ledger(path, true) {
        errors.extend(
            findings
                .into_iter()
                .map(|error| E3ContractError(format!("E3 ledger {}: {error}", path.display()))),
        );
        return Vec::new();
    }
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "cannot read E3 ledger {}: {error}",
                path.display()
            )));
            return Vec::new();
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
    let source_rows = rows.values().cloned().collect::<Vec<_>>();
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
        if row.suite != E3_PRODUCER_SUITE || !row.command.contains(E3_PRODUCER_COMMAND) {
            errors.push(E3ContractError(format!(
                "E3 ledger profile {profile} must come from governed producer suite {E3_PRODUCER_SUITE} via {E3_PRODUCER_COMMAND}"
            )));
        }
        if row.measurements.tp002_evidence_ids.as_slice() != ["E3"] {
            errors.push(E3ContractError(format!(
                "E3 ledger profile {profile} must substantiate exactly E3"
            )));
        }
        require_value(&row, "source_revision", serde_json::json!(revision), errors);
        require_value(&row, "bound_count", serde_json::json!(4), errors);
        require_value(&row, "bars_met", serde_json::json!(true), errors);
        require_value(&row, "portable_gate", serde_json::json!(true), errors);
        require_value(
            &row,
            "quiet_host_required",
            serde_json::json!(false),
            errors,
        );
        require_value(&row, "host_speed_gate", serde_json::json!(false), errors);
        require_value(
            &row,
            "wall_clock_capacity_only",
            serde_json::json!(true),
            errors,
        );
        if contains_quiet_host_gate(&row.environment) || contains_quiet_host_gate(&row.pass_bar) {
            errors.push(E3ContractError(format!(
                "E3 ledger profile {profile} contains a non-portable quiet-host gate"
            )));
        }
        for bound in REQUIRED_BOUNDS_MS {
            require_value(
                &row,
                &format!("bound_{bound}ms_bar_met"),
                serde_json::json!(true),
                errors,
            );
            require_value(
                &row,
                &format!("bound_{bound}ms_recorder_control_logical_match"),
                serde_json::json!(true),
                errors,
            );
            require_bounded_resources(&row, &format!("bound_{bound}ms"), errors);
            require_u64(
                &row,
                &format!("bound_{bound}ms_store_request_bytes"),
                errors,
            );
            require_u64(
                &row,
                &format!("bound_{bound}ms_store_response_bytes"),
                errors,
            );
        }
        require_exact_recovery(&row, errors);
    }
    for profile in rows.keys() {
        errors.push(E3ContractError(format!(
            "unexpected E3 ledger profile {profile}"
        )));
    }
    source_rows
}

fn require_u64(row: &LedgerRow, key: &str, errors: &mut Vec<E3ContractError>) -> Option<u64> {
    let value = row
        .measurements
        .values
        .get(key)
        .and_then(serde_json::Value::as_u64);
    if value.is_none() {
        errors.push(E3ContractError(format!(
            "E3 ledger profile {} requires numeric {key}",
            row.backend_profile
        )));
    }
    value
}

fn require_bounded_resources(row: &LedgerRow, prefix: &str, errors: &mut Vec<E3ContractError>) {
    let configured = require_u64(row, &format!("{prefix}_buffer_configured_bytes"), errors);
    let current = require_u64(row, &format!("{prefix}_buffer_current_bytes"), errors);
    let peak = require_u64(row, &format!("{prefix}_buffer_peak_bytes"), errors);
    let waiters = require_u64(row, &format!("{prefix}_pending_waiters"), errors);
    if current != Some(0)
        || waiters != Some(0)
        || peak
            .zip(configured)
            .is_none_or(|(peak, configured)| peak > configured)
    {
        errors.push(E3ContractError(format!(
            "E3 ledger profile {} {prefix} violates bounded-resource accounting",
            row.backend_profile
        )));
    }
}

fn require_exact_recovery(row: &LedgerRow, errors: &mut Vec<E3ContractError>) {
    let values = &row.measurements.values;
    let before = values
        .get("recovery_state_digest_before")
        .and_then(serde_json::Value::as_str);
    let after = values
        .get("recovery_state_digest_after")
        .and_then(serde_json::Value::as_str);
    let samples = values
        .get("recovery_replay_progress_samples")
        .and_then(serde_json::Value::as_array);
    let start = require_u64(row, "recovery_start_seq", errors);
    let tail = require_u64(row, "recovery_tail_replayed", errors);
    let progress_monotonic = samples.is_some_and(|samples| {
        samples.len() >= 2
            && samples.windows(2).all(|pair| {
                pair[0]
                    .as_u64()
                    .zip(pair[1].as_u64())
                    .is_some_and(|(a, b)| a <= b)
            })
            && samples.first().and_then(serde_json::Value::as_u64) == start
            && samples.last().and_then(serde_json::Value::as_u64)
                == start
                    .zip(tail)
                    .map(|(start, tail)| start.saturating_add(tail))
    });
    if before.is_none()
        || before != after
        || require_u64(row, "recovery_verified_items", errors) != Some(10_000_000)
        || require_u64(row, "recovery_missing_items", errors) != Some(0)
        || require_u64(row, "recovery_duplicate_items", errors) != Some(0)
        || require_u64(row, "recovery_queue_count", errors) != Some(1)
        || require_u64(row, "recovery_verification_chunk_items", errors)
            .is_none_or(|chunk| chunk == 0 || chunk > 512)
        || !progress_monotonic
    {
        errors.push(E3ContractError(format!(
            "E3 ledger profile {} does not prove exact streaming 10M recovery with monotonic replay progress",
            row.backend_profile
        )));
    }
    require_bounded_resources(row, "recovery", errors);
    require_u64(row, "recovery_store_request_bytes", errors);
    require_u64(row, "recovery_store_response_bytes", errors);
}

fn verify_cost_contract(rows: &[LedgerRow], errors: &mut Vec<E3ContractError>) -> usize {
    let inputs = match release_cost_inputs(rows) {
        Ok(inputs) => inputs,
        Err(findings) => {
            errors.extend(
                findings
                    .into_iter()
                    .map(|finding| E3ContractError(format!("E3 cost/recovery source: {finding}"))),
            );
            return 0;
        }
    };
    let prices = PriceInputs::adr_001_us_east_1();
    let workload = WorkloadAssumptions::tp002_e3_push_baseline();
    let cost_rows = match build_release_cost_rows(
        &inputs,
        &workload,
        &prices,
        "pqueue-build-e3-contract (semantic recomputation)",
    ) {
        Ok(rows) => rows,
        Err(findings) => {
            errors.extend(findings.into_iter().map(E3ContractError));
            return 0;
        }
    };
    if let Err(findings) = validate_release_cost_rows(&cost_rows) {
        errors.extend(findings.into_iter().map(E3ContractError));
        0
    } else {
        cost_rows.len()
    }
}

fn contains_quiet_host_gate(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    ["quiet host", "quiet-host", "quiet window", "idle host"]
        .iter()
        .any(|needle| value.contains(needle))
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
    ac7_binding: &Ac7Binding,
    rows: &[TransactionEvidenceRow],
    fence: Option<&E3FenceEvidenceRow>,
    errors: &mut Vec<E3ContractError>,
) {
    if ac7_binding.suite != TRANSACTION_SUITE
        || ac7_binding.backend != "objectlog(force-seal|group-commit)"
        || ac7_binding.bounds_ms.as_slice() != REQUIRED_BOUNDS_MS
        || ac7_binding.latency_window_timing != RequestIdTiming::LatencyWindow
        || ac7_binding.request_id_timing != RequestIdTiming::ForceSealedConfigIndependent
    {
        errors.push(E3ContractError("AC-TXN-7 binding must name the governed suite/backend, exact [1,5,20,100]ms bounds, genuine latency-window timing, and force-sealed config-independent request-id timing".into()));
    }
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
        verify_entry_fence(&context, &entry.manifest_fence, fence, errors);
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
            let expected_backend = governed_backend(&entry.profile, &authority.ac);
            if expected_backend != Some(authority.backend.as_str()) {
                errors.push(E3ContractError(format!(
                    "{context}: {} backend {:?} is not the governed authority {:?}",
                    authority.ac, authority.backend, expected_backend
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
            if row.suite != TRANSACTION_SUITE
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

fn verify_entry_fence(
    context: &str,
    authority: &E3FenceAuthority,
    fence: Option<&E3FenceEvidenceRow>,
    errors: &mut Vec<E3ContractError>,
) {
    let Some(fence) = fence else {
        errors.push(E3ContractError(format!(
            "{context}: manifest fence authority has no valid evidence row"
        )));
        return;
    };
    if authority.suite != fence.suite
        || authority.store_profile != fence.store_profile
        || authority.no_cas != fence.no_cas
        || !matches!(authority.applicability, Applicability::Pass)
    {
        errors.push(E3ContractError(format!(
            "{context}: manifest fence authority does not link the passing stale-epoch fence and no-CAS disposition"
        )));
    }
}

fn governed_backend<'a>(profile: &str, ac: &str) -> Option<&'a str> {
    match (profile, ac) {
        ("object_log_inmemory_projection", "AC-TXN-1" | "AC-TXN-2" | "AC-TXN-3" | "AC-TXN-4") => {
            Some("objectlog")
        }
        ("object_log_sqlite_projection", "AC-TXN-1" | "AC-TXN-2" | "AC-TXN-3") => {
            Some("object_log_sqlite")
        }
        ("object_log_sqlite_projection", "AC-TXN-4") => Some("objectlog"),
        (_, "AC-TXN-6") => Some("sqlite_log|object_log_sqlite"),
        (_, "AC-TXN-7") => Some("objectlog(force-seal|group-commit)"),
        _ => None,
    }
}

fn verify_fence(
    path: &Path,
    revision: &str,
    errors: &mut Vec<E3ContractError>,
) -> Option<E3FenceEvidenceRow> {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "cannot read fencing evidence {}: {error}",
                path.display()
            )));
            return None;
        }
    };
    let row: E3FenceEvidenceRow = match serde_json::from_str(&body) {
        Ok(row) => row,
        Err(error) => {
            errors.push(E3ContractError(format!(
                "malformed fencing evidence {}: {error}",
                path.display()
            )));
            return None;
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
        None
    } else {
        Some(row)
    }
}
