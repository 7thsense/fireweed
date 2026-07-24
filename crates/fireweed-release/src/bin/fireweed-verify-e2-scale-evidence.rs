use std::path::Path;

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else { usage() };
    let Some(flag) = args.next() else { usage() };
    let Some(revision) = args.next() else { usage() };
    if args.next().is_some() || flag != "--expected-revision" {
        usage()
    }
    let body =
        std::fs::read_to_string(Path::new(&path)).unwrap_or_else(|e| fail(vec![e.to_string()]));
    let rows = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line).unwrap_or_else(|error| {
                fail(vec![format!("row {} is malformed: {error}", index + 1)])
            })
        })
        .collect::<Vec<_>>();
    match fireweed_release::e2::validate_release_rows(&rows, &revision) {
        Ok(()) => println!(
            "portable E2 cross-owner evidence valid for {revision} (three canonical sweeps)"
        ),
        Err(errors) => fail(errors),
    }
}

fn fail(errors: Vec<String>) -> ! {
    for e in errors {
        eprintln!("{e}");
    }
    std::process::exit(1)
}
fn usage() -> ! {
    eprintln!("usage: fireweed-verify-e2-scale-evidence <jsonl> --expected-revision <sha>");
    std::process::exit(2)
}
