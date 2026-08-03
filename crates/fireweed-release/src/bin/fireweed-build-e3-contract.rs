use std::path::PathBuf;
use std::process::ExitCode;

use fireweed_release::e3_contract::{
    E3_EVIDENCE_LINK_SCHEMA_VERSION, E3AuthorityMode, E3EvidenceLink, build_e3_contract_manifest,
    verify_e3_contract, write_e3_contract,
};
use fireweed_release::{Promoted, RunOwned};

fn main() -> ExitCode {
    let mut output = None;
    let mut revision = None;
    let mut ledger = None;
    let mut transactions = None;
    let mut fencing = None;
    let mut run_id = None;
    let mut composition_fingerprint = None;
    let mut authority_mode = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let target = match arg.as_str() {
            "--out" => &mut output,
            "--source-revision" => &mut revision,
            "--e3-ledger" => &mut ledger,
            "--transaction-evidence" => &mut transactions,
            "--fencing-evidence" => &mut fencing,
            "--run-id" => &mut run_id,
            "--composition-fingerprint" => &mut composition_fingerprint,
            "--authority-mode" => &mut authority_mode,
            _ => return fail(&format!("unknown argument: {arg}")),
        };
        let Some(value) = args.next() else {
            return fail(&format!("{arg} requires a value"));
        };
        if target.replace(value).is_some() {
            return fail(&format!("duplicate argument: {arg}"));
        }
    }
    let (
        Some(output),
        Some(revision),
        Some(ledger),
        Some(transactions),
        Some(fencing),
        Some(run_id),
        Some(composition_fingerprint),
        Some(authority_mode),
    ) = (
        output,
        revision,
        ledger,
        transactions,
        fencing,
        run_id,
        composition_fingerprint,
        authority_mode,
    )
    else {
        return fail(
            "required: --out --source-revision --e3-ledger --transaction-evidence --fencing-evidence --run-id --composition-fingerprint --authority-mode",
        );
    };
    let authority_mode = match authority_mode.as_str() {
        "native-create-only" => E3AuthorityMode::NativeCreateOnly,
        _ => return fail("--authority-mode must be native-create-only"),
    };
    let output = PathBuf::from(output);
    let repository_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("resolve repository root");
    let Some(run_root) = output.parent() else {
        return fail("--out must have an existing external parent directory");
    };
    let output = match RunOwned::new(repository_root, run_root, &output) {
        Ok(output) => output,
        Err(error) => return fail(&format!("invalid run-owned --out: {error}")),
    };
    let promoted_path = |label: &str, value: String| -> Result<String, ExitCode> {
        let input = Promoted::new(&value)
            .map_err(|error| fail(&format!("invalid promoted {label}: {error}")))?;
        Ok(input.path().to_string_lossy().into_owned())
    };
    let ledger = match promoted_path("--e3-ledger", ledger) {
        Ok(path) => path,
        Err(exit) => return exit,
    };
    let transactions = match promoted_path("--transaction-evidence", transactions) {
        Ok(path) => path,
        Err(exit) => return exit,
    };
    let fencing = match promoted_path("--fencing-evidence", fencing) {
        Ok(path) => path,
        Err(exit) => return exit,
    };
    let evidence_link = E3EvidenceLink {
        schema_version: E3_EVIDENCE_LINK_SCHEMA_VERSION,
        run_id,
        composition_fingerprint,
        authority_mode,
    };
    let manifest = match build_e3_contract_manifest(
        revision.clone(),
        evidence_link,
        ledger,
        transactions,
        fencing,
    ) {
        Ok(manifest) => manifest,
        Err(error) => return fail(&error.0),
    };
    if let Err(error) = write_e3_contract(&output, &manifest) {
        return fail(&format!(
            "cannot write {}: {error}",
            output.path().display()
        ));
    }
    match verify_e3_contract(output.path(), &revision) {
        Ok(summary) => {
            eprintln!(
                "wrote verified E3 contract: {} entries, {} TP-003 rows, {} recomputed cost rows -> {}",
                summary.entries,
                summary.transaction_rows,
                summary.cost_rows,
                output.path().display()
            );
            ExitCode::SUCCESS
        }
        Err(errors) => {
            let _ = output.delete();
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
