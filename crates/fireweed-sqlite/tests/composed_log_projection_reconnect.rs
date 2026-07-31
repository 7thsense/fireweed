//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed sqlite-LOG +
//! sqlite-PROJECTION backend (`ComposedBackend<SqliteLog, SqliteProjectionStore, InProcessControlPlane>`).
//!
//! The DERIVED sqlite projection persists its high-water inside the same transaction that applies each
//! batch, so on reopen the composition's `recover()` replays ONLY the durable log tail beyond the persisted
//! projection snapshot (snapshot-tail recovery, bead pqueue-8a76daad), recovering committed/terminal/
//! pending/leased state + the id-mint counters. Two durable files (log + projection), both keyed by the
//! test's thread id so a scenario's two opens share them while distinct scenarios stay isolated.

use fireweed_sqlite::composed_sqlite_log_sqlite_projection;
use std::cell::Cell;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn paths() -> (String, String) {
    let tid = format!("{:?}", std::thread::current().id());
    let log = std::env::temp_dir()
        .join(format!("fireweed-composed-logproj-reconnect-{tid}.log.db"))
        .to_str()
        .unwrap()
        .to_string();
    let proj = std::env::temp_dir()
        .join(format!("fireweed-composed-logproj-reconnect-{tid}.proj.db"))
        .to_str()
        .unwrap()
        .to_string();
    (log, proj)
}

fn make() -> fireweed_engine::AsyncLogReplayBackend<
    fireweed_sqlite::SqliteLog,
    fireweed_sqlite::SqliteProjectionStore,
> {
    let (log, proj) = paths();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&log);
            let _ = std::fs::remove_file(&proj);
            c.set(true);
        }
    });
    composed_sqlite_log_sqlite_projection(&log, &proj)
        .expect("open composed sqlite-log + sqlite-projection reconnect db")
}

fireweed_conformance::durable_reconnect_suite!(make);
