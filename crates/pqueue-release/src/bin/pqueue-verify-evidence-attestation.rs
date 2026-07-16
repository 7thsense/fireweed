use std::path::PathBuf;

use pqueue_release::attestation::{load_attestation, verify_attestation};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut manifest = None;
    let mut repo_root = None;
    let mut tag = None;
    let mut commit = None;
    while let Some(arg) = args.next() {
        let target = match arg.as_str() {
            "--manifest" => &mut manifest,
            "--repo-root" => &mut repo_root,
            "--tag" => &mut tag,
            "--commit" => &mut commit,
            _ => usage(&format!("unknown argument {arg:?}")),
        };
        *target = Some(
            args.next()
                .unwrap_or_else(|| usage("missing argument value")),
        );
    }
    let manifest = PathBuf::from(manifest.unwrap_or_else(|| usage("--manifest is required")));
    let repo_root = PathBuf::from(repo_root.unwrap_or_else(|| usage("--repo-root is required")));
    let tag = tag.unwrap_or_else(|| usage("--tag is required"));
    let commit = commit.unwrap_or_else(|| usage("--commit is required"));

    let attestation = load_attestation(&manifest).unwrap_or_else(|errors| fail(errors));
    verify_attestation(&attestation, &repo_root, &tag, &commit)
        .unwrap_or_else(|errors| fail(errors));
    println!("validated release-evidence attestation for {tag} at {commit}");
}

fn fail(errors: Vec<pqueue_release::attestation::AttestationError>) -> ! {
    for error in errors {
        eprintln!("error: {error}");
    }
    std::process::exit(1)
}

fn usage(message: &str) -> ! {
    eprintln!("error: {message}");
    eprintln!(
        "usage: pqueue-verify-evidence-attestation --manifest <json> --repo-root <dir> --tag <vX.Y.Z> --commit <40-char-sha>"
    );
    std::process::exit(2)
}
