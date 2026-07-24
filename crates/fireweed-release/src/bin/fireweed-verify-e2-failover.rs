use std::path::Path;
use std::process::ExitCode;

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: fireweed-verify-e2-failover <evidence.json>");
        return ExitCode::FAILURE;
    };
    match fireweed_release::e2_failover::verify_file(Path::new(&path)) {
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
