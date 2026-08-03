use std::path::{Path, PathBuf};

use fireweed_release::RunOwned;
use fireweed_release::attestation::{
    DigestBinding, EvidenceAttestation, InputBinding, InputKind, POLICY, PROMOTED_EVIDENCE_PREFIX,
    SCHEMA_VERSION, SCOPE, SourceBinding, digest_path,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut value = |wanted: &str| {
        let flag = args.next().unwrap_or_else(|| usage());
        if flag != wanted {
            usage()
        }
        args.next().unwrap_or_else(|| usage())
    };
    let repo = PathBuf::from(value("--repo-root"));
    let bundle_root = PathBuf::from(value("--bundle-root"));
    let tag = value("--tag");
    let commit = value("--commit");
    let produced_at = value("--produced-at");
    let reviewed_at = value("--reviewed-at");
    let out = PathBuf::from(value("--out"));
    if args.next().is_some() {
        usage()
    }

    let evidence_paths = [
        "composite-contract.json",
        "e0.jsonl",
        "e1.jsonl",
        "e2-scale.jsonl",
        "e2-density.jsonl",
        "e2-failover.json",
        "e3",
    ]
    .map(str::to_string);
    let evidence = evidence_paths
        .into_iter()
        .map(|relative| DigestBinding {
            sha256: digest_path(&bundle_root.join(&relative))
                .unwrap_or_else(|error| panic!("hash bundle input {relative}: {error}")),
            path: format!("{PROMOTED_EVIDENCE_PREFIX}/{relative}"),
        })
        .collect();
    let inputs = [
        (InputKind::ProductCode, "crates"),
        (InputKind::Harness, "scripts"),
        (InputKind::Config, ".github/workflows/release.yml"),
        (InputKind::DependencyLock, "Cargo.lock"),
    ]
    .into_iter()
    .map(|(kind, path)| InputBinding {
        kind,
        path: path.into(),
        sha256: digest(&repo, path),
    })
    .collect();
    let attestation = EvidenceAttestation {
        schema_version: SCHEMA_VERSION,
        policy: POLICY.into(),
        scope: SCOPE.into(),
        source: SourceBinding { tag, commit },
        producing_command:
            "scripts/release/build-governed-evidence-bundle.sh + fireweed-build-evidence-attestation"
                .into(),
        produced_at,
        reviewed_at,
        evidence,
        inputs,
        exception: None,
    };
    let body = serde_json::to_vec_pretty(&attestation).expect("attestation serializes");
    let run_root = out
        .parent()
        .expect("attestation output requires a parent directory");
    let out = RunOwned::new(&repo, run_root, &out)
        .unwrap_or_else(|error| panic!("authorize run-owned attestation output: {error}"));
    out.write(body)
        .unwrap_or_else(|error| panic!("write {}: {error}", out.path().display()));
    println!("wrote {}", out.path().display());
}

fn digest(repo: &Path, relative: &str) -> String {
    digest_path(&repo.join(relative)).unwrap_or_else(|error| panic!("hash {relative}: {error}"))
}

fn usage() -> ! {
    eprintln!(
        "usage: fireweed-build-evidence-attestation --repo-root <dir> --bundle-root <external-dir> --tag <tag> --commit <sha> --produced-at <UTC> --reviewed-at <UTC> --out <external-path>"
    );
    std::process::exit(2)
}
