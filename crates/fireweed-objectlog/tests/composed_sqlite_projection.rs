//! ADR-012 P1b-ii Part B: the EVENTUAL-APPLY object-log LOG axis driving the DERIVED sqlite relational
//! PROJECTION, assembled by the one generic `ComposedBackend`:
//! `ComposedBackend<ObjectLog, SqliteProjectionStore, InProcessControlPlane>`.
//!
//! This re-expresses the hand-written `ObjectLogSqliteBackend` monolith (the `object_log_sqlite_projection`
//! runtime) as an orthogonal composition: the production segmented group-commit object log is the durable
//! command-log authority, and the sqlite `pqueue_items` projection is the materialized read model fed by
//! `apply_committed_batch`. Because the object log is `DurabilityClass::EventualApply`, the composition runs
//! the **eventual** core conformance class (upsert / update_fields are refused, exactly the monolith's
//! capability set). The projection family stubs secondary indexes, so the index/log-replay scenarios of the
//! full `conformance_suite!` do not apply — the core class is the projection family's suite.

use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_engine::{ComposedBackend, InProcessControlPlane};
use fireweed_objectlog::ObjectLog;
use fireweed_sqlite::SqliteProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "pqueue-objlog-sqliteproj-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn make() -> ComposedBackend<ObjectLog, SqliteProjectionStore, InProcessControlPlane> {
    ComposedBackend::new(
        ObjectLog::open(tmp_root()).expect("open object log"),
        SqliteProjectionStore::in_memory().expect("open sqlite projection"),
        InProcessControlPlane::new(),
    )
}

fireweed_conformance::core_suite!(@eventual make);
