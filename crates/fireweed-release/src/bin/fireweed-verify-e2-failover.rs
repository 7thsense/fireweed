use std::process::ExitCode;

use fireweed_release::{Promoted, ReadableEvidence};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: fireweed-verify-e2-failover <evidence.json>");
        return ExitCode::FAILURE;
    };
    let input = match Promoted::new(&path) {
        Ok(input) => input,
        Err(error) => {
            eprintln!("cannot authorize promoted E2 failover input: {error}");
            return ExitCode::FAILURE;
        }
    };
    match fireweed_release::e2_failover::verify_file(
        input.readable_path().expect("Promoted authorizes reads"),
    ) {
        Ok(row) => {
            println!(
                "validated E2 failover evidence: owner {} epoch {} -> owner {} epoch {}",
                row.old_owner_id, row.old_epoch, row.new_owner_id, row.new_epoch
            );
            ExitCode::SUCCESS
        }
        Err(errors) => {
            for error in errors {
                eprintln!("{error}");
            }
            ExitCode::FAILURE
        }
    }
}
