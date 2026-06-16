#![forbid(unsafe_code)]

use std::path::PathBuf;

use pqueue_storage::concurrency_registry::load_registry;

#[test]
fn loom_registered_concurrency_tests_no_custom_structures_registered() {
    let registry = load_registry(registry_path()).expect("registry should validate");
    for audit in registry.audits {
        assert!(
            audit.no_custom_concurrency,
            "{} must have explicit loom/equivalent tests for custom structures",
            audit.crate_name
        );
        assert!(
            audit.custom_structures.is_empty(),
            "{} unexpectedly registered custom concurrency structures",
            audit.crate_name
        );
    }
}

fn registry_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("concurrency_registry.toml")
}
