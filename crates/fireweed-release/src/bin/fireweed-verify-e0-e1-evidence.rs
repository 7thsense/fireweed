use fireweed_release::{Promoted, ReadableEvidence};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut e0 = None;
    let mut e1 = None;
    let mut revision = None;
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--e0" => e0 = Some(value),
            "--e1" => e1 = Some(value),
            "--expected-revision" => revision = Some(value),
            _ => usage(),
        }
    }
    let (Some(e0), Some(e1), Some(revision)) = (e0, e1, revision) else {
        usage()
    };
    if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
        usage()
    }
    let mut errors = Vec::new();
    for (path, id) in [(&e0, "E0"), (&e1, "E1")] {
        let input = Promoted::new(path).unwrap_or_else(|error| {
            eprintln!("{id}: cannot authorize promoted input: {error}");
            std::process::exit(1);
        });
        if let Err(found) = fireweed_release::single_deployment::verify_file(
            input.readable_path().expect("Promoted authorizes reads"),
            id,
            &revision,
        ) {
            errors.extend(found.into_iter().map(|e| format!("{id}: {e}")));
        }
    }
    if !errors.is_empty() {
        for error in errors {
            eprintln!("{error}");
        }
        std::process::exit(1);
    }
    println!("portable E0/E1 evidence valid for {revision}");
}

fn usage() -> ! {
    eprintln!(
        "usage: fireweed-verify-e0-e1-evidence --e0 <jsonl> --e1 <jsonl> --expected-revision <sha>"
    );
    std::process::exit(2)
}
