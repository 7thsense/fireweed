use std::path::PathBuf;

use fireweed_release::Promoted;
use fireweed_release::transaction::{REQUIRED_ACS, REQUIRED_PROFILES, verify_transaction_evidence};

fn main() {
    let mut paths = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--evidence" => paths.push(PathBuf::from(
                args.next()
                    .unwrap_or_else(|| usage("--evidence requires a path")),
            )),
            _ => usage(&format!("unknown argument {arg:?}")),
        }
    }
    let inputs = paths
        .into_iter()
        .map(|path| {
            Promoted::new(&path).unwrap_or_else(|error| {
                usage(&format!(
                    "cannot authorize promoted evidence {}: {error}",
                    path.display()
                ))
            })
        })
        .collect::<Vec<_>>();
    let summary = verify_transaction_evidence(&inputs).unwrap_or_else(|errors| {
        for error in errors {
            eprintln!("error: {error}");
        }
        std::process::exit(1);
    });
    println!(
        "validated {} TP-003 row(s): {} profiles x {} ACs",
        summary.rows,
        REQUIRED_PROFILES.len(),
        REQUIRED_ACS.len()
    );
}

fn usage(message: &str) -> ! {
    eprintln!("error: {message}");
    eprintln!(
        "usage: fireweed-verify-transaction-evidence --evidence <matrix.jsonl> [--evidence <parity.jsonl> ...]"
    );
    std::process::exit(2)
}
