use std::path::PathBuf;

use pqueue_release::transaction::{REQUIRED_ACS, REQUIRED_PROFILES, verify_transaction_evidence};

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
    let summary = verify_transaction_evidence(&paths).unwrap_or_else(|errors| {
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
        "usage: pqueue-verify-transaction-evidence --evidence <matrix.jsonl> [--evidence <parity.jsonl> ...]"
    );
    std::process::exit(2)
}
