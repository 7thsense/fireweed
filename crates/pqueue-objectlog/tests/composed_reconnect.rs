//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the two DURABLE object-log
//! compositions, proving each recovers identically to its monolith after a reopen.
//!
//! - `objectlog_inmemory` — `ComposedBackend<ObjectLog, InMemoryProjection, ObjectLog>`: a
//!   reopen enumerates the durable `queue.json` catalog and rebuilds the fresh in-memory projection by
//!   replaying the segmented object log from genesis (mirrors the monolithic `ObjectLogBackend`, which
//!   `reconnect_smoke.rs` runs the same suite against).
//! - `objectlog_sqlite_projection` — `ComposedBackend<ObjectLog, SqliteProjectionStore, ...>`: the DERIVED
//!   sqlite projection persists its high-water, so a reopen replays ONLY the object-log tail beyond the
//!   snapshot (snapshot-tail recovery, bead pqueue-8a76daad). This is the composition the prior subagent
//!   showed FAILS without recovery; it now passes, matching the `segmented_objectlog_sqlite_*` server tests.
//!
//! Paths are keyed by the test's thread id so a scenario's two opens share durable state while distinct
//! scenarios stay isolated (the established reconnect-suite pattern).

mod objectlog_inmemory {
    use pqueue_objectlog::composed_objectlog_backend;
    use std::cell::Cell;
    use std::path::PathBuf;

    thread_local! {
        static CLEANED: Cell<bool> = const { Cell::new(false) };
    }

    fn root_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "pqueue-composed-objectlog-reconnect-{:?}",
            std::thread::current().id()
        ))
    }

    fn make() -> pqueue_objectlog::ComposedObjectLogBackend {
        let root = root_path();
        CLEANED.with(|c| {
            if !c.get() {
                let _ = std::fs::remove_dir_all(&root);
                c.set(true);
            }
        });
        composed_objectlog_backend(root).expect("open composed object-log reconnect store")
    }

    pqueue_conformance::durable_reconnect_suite!(make);
}

mod objectlog_sqlite_projection {
    use pqueue_engine::{ComposedBackend, InProcessControlPlane};
    use pqueue_objectlog::ObjectLog;
    use pqueue_sqlite::SqliteProjectionStore;
    use std::cell::Cell;

    thread_local! {
        static CLEANED: Cell<bool> = const { Cell::new(false) };
    }

    fn paths() -> (std::path::PathBuf, String) {
        let tid = format!("{:?}", std::thread::current().id());
        let root = std::env::temp_dir().join(format!("pqueue-composed-ol-sqliteproj-{tid}"));
        let proj = std::env::temp_dir()
            .join(format!("pqueue-composed-ol-sqliteproj-{tid}.proj.db"))
            .to_str()
            .unwrap()
            .to_string();
        (root, proj)
    }

    fn make() -> ComposedBackend<ObjectLog, SqliteProjectionStore, InProcessControlPlane> {
        let (root, proj) = paths();
        CLEANED.with(|c| {
            if !c.get() {
                let _ = std::fs::remove_dir_all(&root);
                let _ = std::fs::remove_file(&proj);
                c.set(true);
            }
        });
        ComposedBackend::new(
            ObjectLog::open(root).expect("open object log"),
            SqliteProjectionStore::open(&proj).expect("open sqlite projection"),
            InProcessControlPlane::new(),
        )
        .recover()
        .expect("recover composed object-log + sqlite-projection backend")
    }

    pqueue_conformance::durable_reconnect_suite!(make);
}
