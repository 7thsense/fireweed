//! Complete shared conformance suite for the supported composed object-log profile.
//!
//! Each scenario uses a fresh segmented object log paired with the in-memory projection through the
//! backend-opaque composition layer.

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "fireweed-objectlog-conformance-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fireweed_conformance::conformance_suite!(|| fireweed_objectlog::composed_objectlog_backend(
    tmp_root()
)
.expect("compose object log"));
