//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed UNIFIED sqlite-relational
//! backend (`ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>`).
//!
//! The DB-authoritative projection needs no log replay (its `apply` wrote durably in the same transaction),
//! so the composition's `recover()` only repopulates the in-process control plane from the durable `queues`
//! catalog and re-seeds the id-mint counters from `pqueue_items`. Mirrors the monolith's
//! `relational_reconnect.rs`. The db path is keyed by the test's thread id.

use pqueue_sqlite::composed_sqlite_relational;
use std::cell::Cell;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-composed-relational-reconnect-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn make() -> pqueue_sqlite::ComposedSqliteRelationalBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    composed_sqlite_relational(&p).expect("open composed unified sqlite-relational reconnect db")
}

pqueue_conformance::durable_reconnect_suite!(make);
