use std::path::PathBuf;
use std::process::ExitCode;

use fireweed_release::e3_contract::{
    build_e3_contract_manifest, verify_e3_contract, write_e3_contract,
};

fn main() -> ExitCode {
    let mut output = None;
    let mut revision = None;
    let mut ledger = None;
    let mut transactions = None;
    let mut fencing = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let target = match arg.as_str() {
            "--out" => &mut output,
            "--source-revision" => &mut revision,
            "--e3-ledger" => &mut ledger,
            "--transaction-evidence" => &mut transactions,
            "--fencing-evidence" => &mut fencing,
            _ => return fail(&format!("unknown argument: {arg}")),
        };
        let Some(value) = args.next() else {
            return fail(&format!("{arg} requires a value"));
        };
        if target.replace(value).is_some() {
            return fail(&format!("duplicate argument: {arg}"));
        }
    }
    let (Some(output), Some(revision), Some(ledger), Some(transactions), Some(fencing)) =
        (output, revision, ledger, transactions, fencing)
    else {
        return fail(
            "required: --out --source-revision --e3-ledger --transaction-evidence --fencing-evidence",
        );
    };
    let output = PathBuf::from(output);
    let manifest = match build_e3_contract_manifest(revision.clone(), ledger, transactions, fencing)
    {
        Ok(manifest) => manifest,
        Err(error) => return fail(&error.0),
    };
    if let Err(error) = write_e3_contract(&output, &manifest) {
        return fail(&format!("cannot write {}: {error}", output.display()));
    }
    match verify_e3_contract(&output, &revision) {
        Ok(summary) => {
            eprintln!(
                "wrote verified E3 contract: {} entries, {} TP-003 rows, {} recomputed cost rows -> {}",
                summary.entries,
                summary.transaction_rows,
                summary.cost_rows,
                output.display()
            );
            ExitCode::SUCCESS
        }
        Err(errors) => {
            let _ = std::fs::remove_file(&output);
            fail(&format!(
                "generated E3 contract failed semantic validation: {}",
                errors
                    .into_iter()
                    .map(|error| error.0)
                    .collect::<Vec<_>>()
                    .join("; ")
            ))
        }
    }
}

fn fail(message: &str) -> ExitCode {
    eprintln!("fireweed-build-e3-contract: {message}");
    ExitCode::FAILURE
}
