//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed sqlite-LOG +
//! in-memory-projection backend (`ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>`).
//!
//! Each scenario opens the SAME durable sqlite log, commits, drops the handle (simulated crash), then
//! REOPENS a fresh `ComposedBackend` over the same file. The composition's `recover()` enumerates the
//! durable queue catalog (the log's `queue_defs` table) and rebuilds the fresh in-memory projection by
//! replaying the durable command log — recovering identically to the monolithic `SqliteBackend`, which
//! `reconnect_smoke.rs` runs the same suite against. Proves the bare composition no longer loses state on
//! restart (the gap ADR-012 P2 closes). The db path is keyed by the test's thread id (see reconnect_smoke).

use pqueue_sqlite::composed_sqlite_backend;
use std::cell::Cell;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-composed-log-reconnect-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn make() -> pqueue_sqlite::ComposedSqliteBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    composed_sqlite_backend(&p).expect("open composed sqlite-log reconnect db")
}

pqueue_conformance::durable_reconnect_suite!(make);
