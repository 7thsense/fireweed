#![forbid(unsafe_code)]

use std::path::PathBuf;

use pqueue_storage::concurrency_registry::load_registry;

#[test]
fn concurrency_verification_registry_tests_loads_checked_in_registry() {
    let registry = load_registry(registry_path()).expect("checked-in registry should validate");
    assert_eq!(registry.schema_version, 1);
    assert!(
        registry
            .workspace_scope
            .contains("pqueue-storage and pqueue-service")
    );
    assert_eq!(registry.audits.len(), 2);
    assert!(registry.audits.iter().any(|audit| {
        audit.crate_name == "pqueue-storage"
            && audit.no_custom_concurrency
            && audit
                .source_globs_checked
                .iter()
                .any(|glob| glob.contains("crates/pqueue-storage/src"))
    }),);
    assert!(registry.audits.iter().any(|audit| {
        audit.crate_name == "pqueue-service"
            && audit.no_custom_concurrency
            && audit
                .source_globs_checked
                .iter()
                .any(|glob| glob.contains("crates/pqueue-service/src"))
    }),);
}

#[test]
fn concurrency_verification_registry_tests_rejects_missing_loom_record() {
    let err = load_registry(fixture_path("missing_test.toml"))
        .expect_err("custom structures must have loom/equivalent records");
    assert!(err.to_string().contains("loom_tests"));
}

fn registry_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("concurrency_registry.toml")
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}
