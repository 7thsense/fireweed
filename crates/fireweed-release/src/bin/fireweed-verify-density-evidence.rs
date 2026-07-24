use std::path::PathBuf;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("usage: fireweed-verify-density-evidence <ledger.jsonl>");
            std::process::exit(2);
        });
    let contents = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        eprintln!("{}: {error}", path.display());
        std::process::exit(1);
    });
    let mut rows = 0usize;
    let mut errors = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        rows += 1;
        match serde_json::from_str::<fireweed_release::LedgerRow>(line) {
            Ok(row) => {
                if let Err(row_errors) = fireweed_release::density::validate_release_row(&row) {
                    errors.extend(
                        row_errors
                            .into_iter()
                            .map(|error| format!("line {}: {error}", index + 1)),
                    );
                }
            }
            Err(error) => errors.push(format!("line {}: malformed row: {error}", index + 1)),
        }
    }
    if rows == 0 {
        errors.push("density ledger is empty".into());
    }
    if !errors.is_empty() {
        for error in errors {
            eprintln!("{error}");
        }
        std::process::exit(1);
    }
    println!("density release evidence valid: {rows} row(s)");
}
