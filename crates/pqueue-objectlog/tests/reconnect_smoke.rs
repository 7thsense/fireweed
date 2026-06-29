//! Durable reconnect conformance for the object-log backend. The external pqueue contract is the same
//! across implementations: accepted mutations survive restart, rejected mutations leave no durable effect,
//! and recovered state is visible through the same ports.

use std::cell::Cell;
use std::path::PathBuf;

use pqueue_objectlog::ObjectLogBackend;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn root_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "pqueue-objectlog-reconnect-{:?}",
        std::thread::current().id()
    ))
}

fn make() -> ObjectLogBackend {
    let root = root_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_dir_all(&root);
            c.set(true);
        }
    });
    ObjectLogBackend::open(root).expect("open object-log reconnect store")
}

pqueue_conformance::durable_reconnect_suite!(make);
