fn main() {
    match pqueue_service::verification_ledger::run_from_args(std::env::args()) {
        Ok(rows) => {
            println!("validated {rows} ledger row(s)");
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
