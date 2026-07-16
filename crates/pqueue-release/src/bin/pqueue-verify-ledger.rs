//! `pqueue-verify-ledger` — strict-validate a verification ledger and assert required evidence is present.
//!
//! Usage:
//!   pqueue-verify-ledger (--ledger <path> | --ledger-dir <dir> | --manifest <path>) [--strict]
//!       [--require-evidence E0,E1,E2,E3]        # RELEASE-tier evidence ids (the headline)
//!       [--require-smoke-evidence E2,E3]        # SMOKE-tier evidence ids (the in-process lane)
//!
//! `--manifest` is the governed TP-002 release path and validates only its exact authority files.
//! `--ledger-dir` remains available for generated smoke/gate output, not governed repository evidence.
//! `--require-evidence` counts only RELEASE-tier rows; `--require-smoke-evidence` counts SMOKE-tier rows.
//! Exit 0 if the ledger validates and every required id is present; non-zero with diagnostics otherwise.
//! This is the CI gate's evidence check; it rebuilds the binary removed with pqueue-service.

use std::path::PathBuf;
use std::process::ExitCode;

use pqueue_release::{
    LedgerSummary, missing_evidence, missing_smoke_evidence, verify_ledger, verify_ledger_dir,
    verify_release_manifest,
};

fn main() -> ExitCode {
    let mut ledger: Option<PathBuf> = None;
    let mut ledger_dir: Option<PathBuf> = None;
    let mut manifest: Option<PathBuf> = None;
    let mut strict = false;
    let mut require: Vec<String> = Vec::new();
    let mut require_smoke: Vec<String> = Vec::new();

    let parse_list = |list: String| -> Vec<String> {
        list.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ledger" => match args.next() {
                Some(p) => ledger = Some(PathBuf::from(p)),
                None => return fail("--ledger requires a path"),
            },
            "--ledger-dir" => match args.next() {
                Some(p) => ledger_dir = Some(PathBuf::from(p)),
                None => return fail("--ledger-dir requires a path"),
            },
            "--manifest" => match args.next() {
                Some(p) => manifest = Some(PathBuf::from(p)),
                None => return fail("--manifest requires a path"),
            },
            "--strict" => strict = true,
            "--require-evidence" => match args.next() {
                Some(list) => require = parse_list(list),
                None => return fail("--require-evidence requires a comma-separated list"),
            },
            "--require-smoke-evidence" => match args.next() {
                Some(list) => require_smoke = parse_list(list),
                None => return fail("--require-smoke-evidence requires a comma-separated list"),
            },
            other => return fail(&format!("unknown argument: {other}")),
        }
    }

    // Asserting evidence is only sound under strict validation: otherwise a failed-run row (exit_status != 0)
    // would still have its evidence ids counted as "present". So a require flag implies --strict.
    if (!require.is_empty() || !require_smoke.is_empty()) && !strict {
        strict = true;
        eprintln!(
            "note: --require-evidence/--require-smoke-evidence implies --strict (failed/malformed rows are not evidence)"
        );
    }

    let result: Result<LedgerSummary, Vec<pqueue_release::LedgerError>> =
        match (&ledger, &ledger_dir, &manifest) {
            (Some(p), None, None) => verify_ledger(p, strict),
            (None, Some(d), None) => verify_ledger_dir(d, strict),
            (None, None, Some(m)) => verify_release_manifest(m),
            (None, None, None) => {
                return fail(
                    "one of --ledger <path>, --ledger-dir <dir>, or --manifest <path> is required",
                );
            }
            _ => return fail("--ledger, --ledger-dir, and --manifest are mutually exclusive"),
        };

    match result {
        Ok(summary) => {
            let missing = missing_evidence(&summary, &require);
            let missing_smoke = missing_smoke_evidence(&summary, &require_smoke);
            if !missing.is_empty() {
                eprintln!(
                    "missing required RELEASE-tier evidence ids: {}",
                    missing.join(", ")
                );
                return ExitCode::FAILURE;
            }
            if !missing_smoke.is_empty() {
                eprintln!(
                    "missing required SMOKE-tier evidence ids: {}",
                    missing_smoke.join(", ")
                );
                return ExitCode::FAILURE;
            }
            println!(
                "validated {} ledger row(s); release-tier evidence: [{}]; smoke-tier evidence: [{}]",
                summary.rows,
                set_str(&summary.evidence_ids),
                set_str(&summary.smoke_evidence_ids),
            );
            ExitCode::SUCCESS
        }
        Err(errors) => {
            for e in &errors {
                eprintln!("{e}");
            }
            eprintln!("ledger validation FAILED with {} error(s)", errors.len());
            ExitCode::FAILURE
        }
    }
}

fn set_str(s: &std::collections::BTreeSet<String>) -> String {
    s.iter().cloned().collect::<Vec<_>>().join(", ")
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("pqueue-verify-ledger: {msg}");
    eprintln!(
        "usage: pqueue-verify-ledger (--ledger <path> | --ledger-dir <dir> | --manifest <path>) [--strict] [--require-evidence E0,E1,E2,E3] [--require-smoke-evidence E2,E3]"
    );
    ExitCode::FAILURE
}
