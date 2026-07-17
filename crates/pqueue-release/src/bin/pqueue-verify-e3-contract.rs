use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage: pqueue-verify-e3-contract --manifest <path> --expected-revision <40-char-sha>"
    );
    std::process::exit(2);
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(manifest_flag) = args.next() else {
        usage()
    };
    let Some(path) = args.next() else { usage() };
    let Some(revision_flag) = args.next() else {
        usage()
    };
    let Some(expected_revision) = args.next() else {
        usage()
    };
    if manifest_flag != "--manifest"
        || revision_flag != "--expected-revision"
        || args.next().is_some()
    {
        usage();
    }
    match pqueue_release::e3_contract::verify_e3_contract(
        &PathBuf::from(path),
        &expected_revision.to_string_lossy(),
    ) {
        Ok(summary) => println!(
            "E3 contract verified: {} entries, {} transaction rows",
            summary.entries, summary.transaction_rows
        ),
        Err(errors) => {
            for error in errors {
                eprintln!("E3 contract verification failed: {error}");
            }
            std::process::exit(1);
        }
    }
}
