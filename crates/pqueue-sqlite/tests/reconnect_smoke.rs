//! Proves the new `relational_reconnect_suite!` macro compiles AND runs (BQ-10): the reconnect scenario
//! (commit → drop handle → reopen same store → committed state present, no manual log replay) executed
//! against a file-backed sqlite backend whose `make` reopens the SAME database. (Today's sqlite restores
//! via log replay on open; the relational backend in BQ-11 will satisfy the same scenario DB-natively.)

use pqueue_sqlite::SqliteBackend;
use std::sync::Once;

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!("pqueue-sqlite-reconnect-{}.db", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

/// Reopen the SAME database file on every call; clean it exactly once, before the first `make()`, so the
/// first session starts empty and the second session reopens its committed state.
fn make() -> SqliteBackend {
    static CLEAN: Once = Once::new();
    let p = db_path();
    CLEAN.call_once(|| {
        let _ = std::fs::remove_file(&p);
    });
    SqliteBackend::open(&p).expect("open sqlite reconnect db")
}

pqueue_conformance::relational_reconnect_suite!(make);
