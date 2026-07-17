use std::fs;
use std::path::{Path, PathBuf};

use pqueue_release::attestation::{
    DigestBinding, EvidenceAttestation, InputBinding, InputKind, ManualException, POLICY,
    SCHEMA_VERSION, SCOPE, SourceBinding, digest_path, verify_attestation,
};

const TAG: &str = "v9.8.7";
const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

fn temp_repo(test: &str) -> PathBuf {
    let root =
        std::env::temp_dir().join(format!("pqueue-attestation-{test}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    for directory in ["crates/app/src", "scripts/perf", "config", "evidence"] {
        fs::create_dir_all(root.join(directory)).unwrap();
    }
    fs::write(
        root.join("crates/app/src/lib.rs"),
        "pub fn value() -> u8 { 1 }\n",
    )
    .unwrap();
    fs::write(root.join("scripts/perf/run.sh"), "cargo test --release\n").unwrap();
    fs::write(root.join("config/release.toml"), "scale = 'release'\n").unwrap();
    fs::write(root.join("Cargo.lock"), "version = 4\n").unwrap();
    fs::write(root.join("evidence/e0-e3.jsonl"), "{\"bars_met\":true}\n").unwrap();
    root
}

fn binding(root: &Path, path: &str) -> DigestBinding {
    DigestBinding {
        path: path.into(),
        sha256: digest_path(&root.join(path)).unwrap(),
    }
}

fn input(root: &Path, kind: InputKind, path: &str) -> InputBinding {
    InputBinding {
        kind,
        path: path.into(),
        sha256: digest_path(&root.join(path)).unwrap(),
    }
}

fn manifest(root: &Path) -> EvidenceAttestation {
    EvidenceAttestation {
        schema_version: SCHEMA_VERSION,
        policy: POLICY.into(),
        scope: SCOPE.into(),
        source: SourceBinding {
            tag: TAG.into(),
            commit: COMMIT.into(),
        },
        producing_command: "bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3"
            .into(),
        produced_at: "2026-07-16T12:00:00Z".into(),
        reviewed_at: "2026-07-16T13:00:00Z".into(),
        evidence: vec![binding(root, "evidence/e0-e3.jsonl")],
        inputs: vec![
            input(root, InputKind::ProductCode, "crates"),
            input(root, InputKind::Harness, "scripts/perf"),
            input(root, InputKind::Config, "config/release.toml"),
            input(root, InputKind::DependencyLock, "Cargo.lock"),
        ],
        exception: None,
    }
}

#[test]
fn exact_tag_attestation_accepts_matching_source_and_digests() {
    let root = temp_repo("valid");
    verify_attestation(&manifest(&root), &root, TAG, COMMIT).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn product_code_change_without_refreshed_attestation_fails_closed() {
    let root = temp_repo("code-drift");
    let attestation = manifest(&root);
    fs::write(
        root.join("crates/app/src/lib.rs"),
        "pub fn value() -> u8 { 2 }\n",
    )
    .unwrap();

    let errors = verify_attestation(&attestation, &root, TAG, COMMIT).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("input digest mismatch for \"crates\""))
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn harness_change_without_refreshed_attestation_fails_closed() {
    let root = temp_repo("harness-drift");
    let attestation = manifest(&root);
    fs::write(
        root.join("scripts/perf/run.sh"),
        "cargo test --release --all\n",
    )
    .unwrap();

    let errors = verify_attestation(&attestation, &root, TAG, COMMIT).unwrap_err();
    assert!(errors.iter().any(|error| {
        error
            .0
            .contains("input digest mismatch for \"scripts/perf\"")
    }));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn different_release_commit_requires_a_fresh_exact_tag_run() {
    let root = temp_repo("source-drift");
    let errors = verify_attestation(
        &manifest(&root),
        &root,
        TAG,
        "ffffffffffffffffffffffffffffffffffffffff",
    )
    .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("does not match release commit"))
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn manual_exception_is_explicitly_red_to_automation() {
    let root = temp_repo("exception");
    let mut attestation = manifest(&root);
    attestation.exception = Some(ManualException {
        approval_id: "INC-42".into(),
        approved_by: "release-manager@example.test".into(),
        reason: "production security repair".into(),
        expires_at: "2026-07-17T12:00:00Z".into(),
    });
    let errors = verify_attestation(&attestation, &root, TAG, COMMIT).unwrap_err();
    assert!(errors.iter().any(|error| {
        error
            .0
            .contains("automated release evidence must remain red")
    }));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn traversal_and_missing_input_class_fail_closed() {
    let root = temp_repo("shape");
    let mut attestation = manifest(&root);
    attestation
        .inputs
        .retain(|input| input.kind != InputKind::Config);
    attestation.evidence[0].path = "../outside.jsonl".into();
    let errors = verify_attestation(&attestation, &root, TAG, COMMIT).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("safe repo-relative"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("missing required kind Config"))
    );
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn symlinked_digest_ancestor_cannot_escape_repo_root() {
    use std::os::unix::fs::symlink;

    let root = temp_repo("symlink-ancestor");
    let outside =
        std::env::temp_dir().join(format!("pqueue-attestation-outside-{}", std::process::id()));
    let _ = fs::remove_dir_all(&outside);
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("evidence.json"), "{}\n").unwrap();
    symlink(&outside, root.join("linked-outside")).unwrap();

    let mut attestation = manifest(&root);
    attestation.evidence[0] = DigestBinding {
        path: "linked-outside/evidence.json".into(),
        sha256: digest_path(&outside.join("evidence.json")).unwrap(),
    };
    let errors = verify_attestation(&attestation, &root, TAG, COMMIT).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("contains a symlink"))
    );
    fs::remove_dir_all(root).unwrap();
    fs::remove_dir_all(outside).unwrap();
}
