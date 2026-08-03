//! Fail-closed source and freshness binding for governed release evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA_VERSION: u32 = 1;
pub const POLICY: &str = "exact-tag-rerun";
pub const SCOPE: &str = "tp002-release-v1";
pub const PROMOTED_EVIDENCE_PREFIX: &str = "target/tp002-release";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceAttestation {
    pub schema_version: u32,
    pub policy: String,
    pub scope: String,
    pub source: SourceBinding,
    pub producing_command: String,
    pub produced_at: String,
    pub reviewed_at: String,
    pub evidence: Vec<DigestBinding>,
    pub inputs: Vec<InputBinding>,
    #[serde(default)]
    pub exception: Option<ManualException>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBinding {
    pub tag: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigestBinding {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputKind {
    ProductCode,
    Harness,
    Config,
    DependencyLock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBinding {
    pub kind: InputKind,
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualException {
    pub approval_id: String,
    pub approved_by: String,
    pub reason: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationError(pub String);

impl std::fmt::Display for AttestationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn load_attestation(path: &Path) -> Result<EvidenceAttestation, Vec<AttestationError>> {
    let bytes = fs::read(path).map_err(|error| {
        vec![AttestationError(format!(
            "cannot read attestation {}: {error}",
            path.display()
        ))]
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        vec![AttestationError(format!(
            "malformed attestation {}: {error}",
            path.display()
        ))]
    })
}

/// Verify an attestation against the tag/commit being released and the current checkout.
///
/// All paths are repo-relative and may name a file or directory. Directory digests bind a sorted,
/// recursive snapshot, including relative names and file contents. Symlinks are rejected.
pub fn verify_attestation(
    manifest: &EvidenceAttestation,
    repo_root: &Path,
    expected_tag: &str,
    expected_commit: &str,
) -> Result<(), Vec<AttestationError>> {
    verify_attestation_impl(manifest, repo_root, None, expected_tag, expected_commit)
}

/// Verify a not-yet-promoted bundle while keeping v1 evidence bindings pinned to their eventual
/// `target/tp002-release` locations. Source inputs are still verified against `repo_root`.
pub fn verify_attestation_with_evidence_root(
    manifest: &EvidenceAttestation,
    repo_root: &Path,
    evidence_root: &Path,
    expected_tag: &str,
    expected_commit: &str,
) -> Result<(), Vec<AttestationError>> {
    verify_attestation_impl(
        manifest,
        repo_root,
        Some(evidence_root),
        expected_tag,
        expected_commit,
    )
}

fn verify_attestation_impl(
    manifest: &EvidenceAttestation,
    repo_root: &Path,
    evidence_root: Option<&Path>,
    expected_tag: &str,
    expected_commit: &str,
) -> Result<(), Vec<AttestationError>> {
    let mut errors = Vec::new();
    if manifest.schema_version != SCHEMA_VERSION {
        errors.push(format!(
            "schema_version {} is unsupported; expected {SCHEMA_VERSION}",
            manifest.schema_version
        ));
    }
    if manifest.policy != POLICY {
        errors.push(format!(
            "policy {:?} is unsupported; expected {POLICY:?}",
            manifest.policy
        ));
    }
    if manifest.scope != SCOPE {
        errors.push(format!(
            "scope {:?} is unsupported; expected {SCOPE:?}",
            manifest.scope
        ));
    }
    if manifest.source.tag != expected_tag {
        errors.push(format!(
            "source tag {:?} does not match release tag {expected_tag:?}",
            manifest.source.tag
        ));
    }
    if manifest.source.commit != expected_commit {
        errors.push(format!(
            "source commit {:?} does not match release commit {expected_commit:?}",
            manifest.source.commit
        ));
    }
    if !is_full_lower_hex_commit(&manifest.source.commit) {
        errors.push("source commit must be a full 40-character lowercase Git SHA".into());
    }
    if manifest.producing_command.trim().is_empty() {
        errors.push("producing_command must not be empty".into());
    }
    for (field, value) in [
        ("produced_at", manifest.produced_at.as_str()),
        ("reviewed_at", manifest.reviewed_at.as_str()),
    ] {
        if !looks_like_utc_timestamp(value) {
            errors.push(format!("{field} must be a UTC RFC3339 timestamp"));
        }
    }
    if manifest.evidence.is_empty() {
        errors.push("evidence must contain at least one digest binding".into());
    }
    if manifest.exception.is_some() {
        errors.push(
            "manual exception is declared: automated release evidence must remain red and requires the documented emergency approval path"
                .into(),
        );
    }

    let mut kinds = BTreeSet::new();
    let mut seen_paths = BTreeSet::new();
    for binding in &manifest.evidence {
        let (root, path) = match evidence_root {
            Some(root) => {
                let prefix = Path::new(PROMOTED_EVIDENCE_PREFIX);
                let bound = Path::new(&binding.path);
                let relative = match bound.strip_prefix(prefix) {
                    Ok(relative) if relative.components().next().is_some() => relative,
                    _ => {
                        errors.push(format!(
                            "evidence path {:?} is outside promoted prefix {PROMOTED_EVIDENCE_PREFIX:?}",
                            binding.path
                        ));
                        continue;
                    }
                };
                (root, relative.to_string_lossy().into_owned())
            }
            None => (repo_root, binding.path.clone()),
        };
        verify_digest_binding(
            root,
            "evidence",
            &path,
            &binding.sha256,
            &mut seen_paths,
            &mut errors,
        );
    }
    for binding in &manifest.inputs {
        kinds.insert(binding.kind);
        verify_digest_binding(
            repo_root,
            "input",
            &binding.path,
            &binding.sha256,
            &mut seen_paths,
            &mut errors,
        );
    }
    for required in [
        InputKind::ProductCode,
        InputKind::Harness,
        InputKind::Config,
        InputKind::DependencyLock,
    ] {
        if !kinds.contains(&required) {
            errors.push(format!("inputs are missing required kind {required:?}"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.into_iter().map(AttestationError).collect())
    }
}

fn verify_digest_binding(
    repo_root: &Path,
    label: &str,
    path: &str,
    expected: &str,
    seen_paths: &mut BTreeSet<String>,
    errors: &mut Vec<String>,
) {
    if !is_safe_relative_path(path) {
        errors.push(format!(
            "{label} path {path:?} is not a safe repo-relative path"
        ));
        return;
    }
    if !seen_paths.insert(path.to_string()) {
        errors.push(format!("duplicate digest binding for path {path:?}"));
        return;
    }
    if !is_sha256(expected) {
        errors.push(format!("{label} path {path:?} has an invalid SHA-256"));
        return;
    }
    let canonical_root = match repo_root.canonicalize() {
        Ok(root) => root,
        Err(error) => {
            errors.push(format!("cannot canonicalize repo root: {error}"));
            return;
        }
    };
    let candidate = repo_root.join(path);
    let mut cursor = repo_root.to_path_buf();
    for component in Path::new(path).components() {
        cursor.push(component.as_os_str());
        if fs::symlink_metadata(&cursor).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            errors.push(format!("{label} path {path:?} contains a symlink"));
            return;
        }
    }
    let canonical = match candidate.canonicalize() {
        Ok(candidate) if candidate.starts_with(&canonical_root) => candidate,
        Ok(_) => {
            errors.push(format!("{label} path {path:?} escapes the repo root"));
            return;
        }
        Err(error) => {
            errors.push(format!(
                "cannot canonicalize {label} path {path:?}: {error}"
            ));
            return;
        }
    };
    match digest_path(&canonical) {
        Ok(actual) if actual != expected => errors.push(format!(
            "{label} digest mismatch for {path:?}: expected {expected}, actual {actual}; refresh the evidence attestation"
        )),
        Ok(_) => {}
        Err(error) => errors.push(format!("cannot hash {label} path {path:?}: {error}")),
    }
}

fn is_safe_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_full_lower_hex_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn looks_like_utc_timestamp(value: &str) -> bool {
    value.len() >= 20 && value.ends_with('Z') && value.as_bytes().get(10) == Some(&b'T')
}

/// Compute the canonical SHA-256 used by attestation bindings.
pub fn digest_path(path: &Path) -> Result<String, String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    if metadata.file_type().is_symlink() {
        return Err("symlinks are not accepted in attested inputs".into());
    }
    if metadata.is_file() {
        hash_file(&mut hasher, Path::new("."), path)?;
    } else if metadata.is_dir() {
        let mut files = Vec::new();
        collect_files(path, path, &mut files)?;
        files.sort_by(|left, right| left.0.cmp(&right.0));
        for (relative, absolute) in files {
            hash_file(&mut hasher, &relative, &absolute)?;
        }
    } else {
        return Err("path is neither a regular file nor a directory".into());
    }
    let digest = hasher.finalize();
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<(), String> {
    let mut entries = fs::read_dir(current)
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() {
            return Err(format!("symlink {} is not accepted", path.display()));
        }
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| error.to_string())?
                .to_path_buf();
            files.push((relative, path));
        } else {
            return Err(format!("{} is not a regular file", path.display()));
        }
    }
    Ok(())
}

fn hash_file(hasher: &mut Sha256, relative: &Path, absolute: &Path) -> Result<(), String> {
    let name = relative.to_string_lossy();
    let contents = fs::read(absolute).map_err(|error| error.to_string())?;
    hasher.update(b"file\0");
    hasher.update((name.len() as u64).to_le_bytes());
    hasher.update(name.as_bytes());
    hasher.update((contents.len() as u64).to_le_bytes());
    hasher.update(contents);
    Ok(())
}

/// Helper used by evidence producers to construct digest bindings without duplicating the hash format.
pub fn bind_paths(
    repo_root: &Path,
    paths: impl IntoIterator<Item = String>,
) -> Result<BTreeMap<String, String>, String> {
    paths
        .into_iter()
        .map(|path| digest_path(&repo_root.join(&path)).map(|digest| (path, digest)))
        .collect()
}
