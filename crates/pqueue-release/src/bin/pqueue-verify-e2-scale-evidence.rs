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
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    let row = lines
        .next()
        .and_then(|line| serde_json::from_str(line).ok())
        .unwrap_or_else(|| fail(vec!["expected one valid ledger row".into()]));
    if lines.next().is_some() {
        fail(vec!["expected exactly one ledger row".into()]);
    }
    match pqueue_release::e2::validate_release_row(&row, &revision) {
        Ok(()) => println!("portable E2 cross-owner evidence valid for {revision}"),
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
    eprintln!("usage: pqueue-verify-e2-scale-evidence <jsonl> --expected-revision <sha>");
    std::process::exit(2)
}
