use super::*;

#[test]
fn direct_config_construction_owns_tuning_on_backend_spec() {
    let async_spec = AsyncProjectionSpec::new(101, 102, 103, 104, 105).unwrap();
    let config = Config::new(
        BackendSpec {
            log: LogSpec::ObjectLog(ObjectLogSpec::local(
                std::env::temp_dir().join("fireweed-p3m-tuning"),
                SegmentConfig::new(262_144, 20).expect("valid segment"),
            )),
            projection: ProjectionSpec::Turso {
                path: std::env::temp_dir().join("fireweed-p3m-tuning.db"),
            },
            control_plane: ControlPlaneSpec::InProcess,
            // Field ownership under a coherent AsyncProjection pairing (P3v validates Option/barrier).
            response_barrier: ResponseBarrierSpec::AsyncProjection,
            async_projection: Some(async_spec),
            sqlite_projection_deferred_flush_chunk: None,
        },
        0,
        "127.0.0.1:0".to_owned(),
        Duration::from_secs(1),
        Vec::new(),
    );
    assert_eq!(
        config.backend.response_barrier,
        ResponseBarrierSpec::AsyncProjection
    );
    assert_eq!(config.backend.async_projection, Some(async_spec));
    assert_eq!(config.backend.sqlite_projection_deferred_flush_chunk, None);
    assert_eq!(config.validate_for_start(), Ok(()));
}
