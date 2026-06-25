//! `relational_reconnect_suite!` against the log-backed `SqliteBackend` (BQ-10): a reopen of the same
//! durable store recovers committed state (here via log replay; the relational backend recovers the same
//! scenarios DB-natively — see `relational_reconnect.rs`).
//!
//! Each scenario calls `make()` twice (open → drop → reopen the SAME store). The db path is keyed by the
//! test's THREAD id, so the two opens within one scenario share a file while distinct scenarios (each on
//! its own never-reused libtest thread) get isolated files — no cross-scenario pollution under the default
//! parallel test runner.

use pqueue_sqlite::SqliteBackend;
use std::cell::Cell;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-sqlite-reconnect-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

/// Reopen the SAME database file across a scenario's calls; clean it once (the first `make()` on this
/// thread) so the first session starts empty and the second reopens its committed state.
fn make() -> SqliteBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    SqliteBackend::open(&p).expect("open sqlite reconnect db")
}

pqueue_conformance::relational_reconnect_suite!(make);
