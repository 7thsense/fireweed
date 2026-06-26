//! `pqueue-verify-ledger` — strict-validate a verification ledger and assert required evidence is present.
//!
//! Usage:
//!   pqueue-verify-ledger --ledger <path> [--strict] [--require-evidence E0,E1,E2,E3]
//!
//! Exit 0 if the ledger validates (and, when `--require-evidence` is given, every required evidence id is
//! present in some row); non-zero with diagnostics on stderr otherwise. This is the CI gate's evidence check;
//! it rebuilds the binary removed with pqueue-service.

use std::path::PathBuf;
use std::process::ExitCode;

use pqueue_release::{missing_evidence, verify_ledger};

fn main() -> ExitCode {
    let mut ledger: Option<PathBuf> = None;
    let mut strict = false;
    let mut require: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ledger" => match args.next() {
                Some(p) => ledger = Some(PathBuf::from(p)),
                None => return fail("--ledger requires a path"),
            },
            "--strict" => strict = true,
            "--require-evidence" => match args.next() {
                Some(list) => {
                    require = list
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                }
                None => return fail("--require-evidence requires a comma-separated list"),
            },
            other => return fail(&format!("unknown argument: {other}")),
        }
    }

    let Some(ledger) = ledger else {
        return fail("--ledger <path> is required");
    };

    // Asserting evidence is only sound under strict validation: otherwise a failed-run row (exit_status != 0)
    // would still have its evidence ids counted as "present". So --require-evidence implies --strict.
    if !require.is_empty() && !strict {
        strict = true;
        eprintln!(
            "note: --require-evidence implies --strict (failed/malformed rows are not evidence)"
        );
    }

    match verify_ledger(&ledger, strict) {
        Ok(summary) => {
            let missing = missing_evidence(&summary, &require);
            if !missing.is_empty() {
                eprintln!("missing required evidence ids: {}", missing.join(", "));
                return ExitCode::FAILURE;
            }
            println!(
                "validated {} ledger row(s); evidence present: [{}]",
                summary.rows,
                summary
                    .evidence_ids
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
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

fn fail(msg: &str) -> ExitCode {
    eprintln!("pqueue-verify-ledger: {msg}");
    eprintln!(
        "usage: pqueue-verify-ledger --ledger <path> [--strict] [--require-evidence E0,E1,E2,E3]"
    );
    ExitCode::FAILURE
}
