use super::*;

#[test]
fn legacy_hybrid_profiles_preserve_relocated_tuning_fingerprints() {
    let plain = legacy_hybrid_product_config(Some(7), false, None).unwrap();
    assert_eq!(plain.deferred_flush_chunk, 7);
    assert!(!plain.strict);
    assert!(plain.async_monitor.is_none());

    let strict = legacy_hybrid_product_config(Some(11), true, None).unwrap();
    assert_eq!(strict.deferred_flush_chunk, 11);
    assert!(strict.strict);
    assert!(strict.async_monitor.is_none());

    let async_spec = AsyncProjectionSpec::new(41, 42, 43, 44, 45).unwrap();
    let asynchronous = legacy_hybrid_product_config(Some(13), false, Some(async_spec)).unwrap();
    assert_eq!(asynchronous.deferred_flush_chunk, 13);
    assert!(!asynchronous.strict);
    let monitor = asynchronous.async_monitor.expect("async monitor");
    assert_eq!(monitor.apply_lag_max_commands, 41);
    assert_eq!(monitor.apply_debt_max_bytes, 42);
    assert_eq!(monitor.apply_queue_depth_max, 43);
    assert_eq!(monitor.oldest_unapplied_max_ms, 44);
    assert_eq!(monitor.apply_poison_retry_threshold, 45);
}

#[test]
fn direct_config_construction_owns_tuning_on_backend_spec() {
    let async_spec = AsyncProjectionSpec::new(101, 102, 103, 104, 105).unwrap();
    let config = Config::new(
        BackendSpec {
            log: LogSpec::Memory,
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            async_projection: Some(async_spec),
            sqlite_projection_deferred_flush_chunk: Some(17),
        },
        0,
        "127.0.0.1:0".to_owned(),
        Duration::from_secs(1),
        Vec::new(),
    );
    assert_eq!(config.backend.async_projection, Some(async_spec));
    assert_eq!(
        config.backend.sqlite_projection_deferred_flush_chunk,
        Some(17)
    );
}

#[test]
fn removed_top_level_fields_and_public_sqlite_type_do_not_return() {
    let source = include_str!("lib.rs");
    for forbidden in [
        ["pub hybrid_", "async:"].concat(),
        ["pub deferred_", "flush_chunk:"].concat(),
        ["pub use fireweed_sqlite::", "HybridAsyncThresholds"].concat(),
    ] {
        assert!(
            !source.contains(&forbidden),
            "removed public surface returned: {forbidden}"
        );
    }
}
