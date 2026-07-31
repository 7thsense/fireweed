//! Recovery-on-open reconnect suite against the LogEngine object-log × memory product.
//!
//! The retired dual-stack `ComposedBackend<ObjectLog, …>` reconnect cells were removed with the
//! in-tree segmented ObjectLog substrate (program A). Product reopen is
//! [`fireweed_objectlog::composed_objectlog_backend`].

mod objectlog_memory {
    use fireweed_objectlog::composed_objectlog_backend;
    use std::cell::Cell;
    use std::path::PathBuf;

    thread_local! {
        static CLEANED: Cell<bool> = const { Cell::new(false) };
    }

    fn root_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "fireweed-composed-objectlog-reconnect-{:?}",
            std::thread::current().id()
        ))
    }

    fn make() -> fireweed_objectlog::ComposedObjectLogBackend {
        let root = root_path();
        CLEANED.with(|c| {
            if !c.get() {
                let _ = std::fs::remove_dir_all(&root);
                c.set(true);
            }
        });
        composed_objectlog_backend(root).expect("open composed object-log reconnect store")
    }

    fireweed_conformance::durable_reconnect_suite!(make);
}
