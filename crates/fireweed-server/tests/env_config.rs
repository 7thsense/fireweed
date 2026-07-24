use std::collections::BTreeMap;

use fireweed_server::{Config, ControlPlaneSpec, LogSpec, ObjectLogSpec, S3CredentialSource};

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn postgres_control_plane_env_selects_typed_shared_authority() {
    let config = Config::from_env(&env(&[
        ("PQUEUE_CONTROL_PLANE", "postgres"),
        ("PQUEUE_REPLICA_COUNT", "3"),
        ("PQUEUE_OWNER_ID", "pqueue-7f4c9d8b-owner-a"),
        ("PQUEUE_ADVERTISE_ADDR", "10.0.0.12:8080"),
        (
            "PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL",
            "postgres://pqueue:secret@postgres.internal/pqueue",
        ),
        ("PQUEUE_CONTROL_PLANE_HEARTBEAT_TTL_MS", "7000"),
        ("PQUEUE_CONTROL_PLANE_LEASE_TTL_MS", "21000"),
    ]))
    .expect("shared Postgres control-plane config must parse");

    assert_eq!(config.advertise_addr.as_deref(), Some("10.0.0.12:8080"));
    assert_eq!(config.owner_id.as_str(), "pqueue-7f4c9d8b-owner-a");

    match config.backend.control_plane {
        ControlPlaneSpec::Postgres { url, config } => {
            assert_eq!(url, "postgres://pqueue:secret@postgres.internal/pqueue");
            assert_eq!(config.heartbeat_ttl_ms, 7_000);
            assert_eq!(config.lease_ttl_ms, 21_000);
        }
        ControlPlaneSpec::InProcess => panic!("expected shared Postgres control plane"),
    }
}

#[test]
fn postgres_control_plane_missing_dsn_fails_closed() {
    for dsn in [None, Some("")] {
        let mut values = env(&[("PQUEUE_CONTROL_PLANE", "postgres")]);
        if let Some(dsn) = dsn {
            values.insert(
                "PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL".into(),
                dsn.into(),
            );
        }
        let Err(error) = Config::from_env(&values) else {
            panic!("missing DSN must fail closed");
        };
        assert!(
            error
                .0
                .contains("PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL"),
            "{}",
            error.0
        );
    }
}

#[test]
fn postgres_control_plane_boundary_rejects_inprocess_for_multiple_replicas() {
    let Err(error) = Config::from_env(&env(&[
        ("PQUEUE_CONTROL_PLANE", "inprocess"),
        ("PQUEUE_REPLICA_COUNT", "2"),
        ("PQUEUE_OWNER_ID", "replica-a"),
        ("PQUEUE_ADVERTISE_ADDR", "10.0.0.12:8080"),
    ])) else {
        panic!("the development-only plane cannot coordinate replicas");
    };
    assert!(
        error.0.contains("development-only single-process"),
        "{}",
        error.0
    );
}

#[test]
fn postgres_control_plane_invalid_ttls_fail_closed() {
    for (key, value) in [
        ("PQUEUE_CONTROL_PLANE_HEARTBEAT_TTL_MS", "invalid"),
        ("PQUEUE_CONTROL_PLANE_LEASE_TTL_MS", "0"),
    ] {
        let mut values = env(&[
            ("PQUEUE_CONTROL_PLANE", "postgres"),
            (
                "PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL",
                "postgres://localhost/pqueue",
            ),
        ]);
        values.insert(key.into(), value.into());
        let Err(error) = Config::from_env(&values) else {
            panic!("invalid control-plane TTL must fail closed");
        };
        assert!(error.0.contains(key), "{}", error.0);
    }
}
#[test]
fn objectlog_s3_env_builds_typed_shared_profile() {
    let config = Config::from_env(&env(&[
        ("PQUEUE_LOG_BACKEND", "objectlog"),
        ("PQUEUE_OBJECT_LOG_STORE", "s3"),
        ("PQUEUE_OBJECT_LOG_S3_ENDPOINT", "https://s3.example.com"),
        ("PQUEUE_OBJECT_LOG_S3_BUCKET", "pqueue-prod"),
        ("PQUEUE_OBJECT_LOG_S3_REGION", "us-west-2"),
        ("PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID", "production-access"),
        (
            "PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
            "production-secret",
        ),
        ("PQUEUE_SEGMENT_TARGET_BYTES", "524288"),
        ("PQUEUE_SEGMENT_MAX_LATENCY_MS", "17"),
    ]))
    .expect("complete secure S3 config");

    let LogSpec::ObjectLog(spec) = config.backend.log else {
        panic!("expected object-log spec");
    };
    assert!(spec.is_shared());
    match spec {
        ObjectLogSpec::S3 {
            endpoint,
            bucket,
            region,
            credentials,
            segment_config,
            allow_insecure_http,
        } => {
            assert_eq!(endpoint, "https://s3.example.com");
            assert_eq!(bucket, "pqueue-prod");
            assert_eq!(region, "us-west-2");
            assert!(!allow_insecure_http);
            assert_eq!(segment_config.target_bytes, 524_288);
            assert_eq!(segment_config.max_latency_ms, 17);
            assert_eq!(
                credentials,
                S3CredentialSource::Static {
                    access_key_id: "production-access".to_string(),
                    secret_access_key: "production-secret".to_string(),
                }
            );
        }
        ObjectLogSpec::LocalFilesystem { .. } => panic!("S3 must not fall back to local"),
    }
}

#[test]
fn objectlog_s3_env_rejects_plaintext_without_explicit_local_opt_in() {
    let result = Config::from_env(&env(&[
        ("PQUEUE_OBJECT_LOG_STORE", "s3"),
        ("PQUEUE_OBJECT_LOG_S3_ENDPOINT", "http://minio:9000"),
        ("PQUEUE_OBJECT_LOG_S3_BUCKET", "pqueue"),
        ("PQUEUE_OBJECT_LOG_S3_REGION", "us-east-1"),
        ("PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID", "access"),
        ("PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY", "secret"),
    ]));
    let Err(error) = result else {
        panic!("plaintext must fail closed by default");
    };
    assert!(error.0.contains("ALLOW_INSECURE") || error.0.contains("plaintext"));
}

#[test]
fn objectlog_s3_env_rejects_s3_variables_under_local_profile() {
    let result = Config::from_env(&env(&[
        ("PQUEUE_OBJECT_LOG_STORE", "local"),
        ("PQUEUE_OBJECT_LOG_S3_BUCKET", "must-not-be-ignored"),
    ]));
    let Err(error) = result else {
        panic!("shared-store settings must not silently fall back to local");
    };
    assert!(error.0.contains("fall back to node-local storage"));
}

#[test]
fn objectlog_s3_local_profile_is_explicitly_single_replica() {
    let config = Config::from_env(&env(&[
        ("PQUEUE_OBJECT_LOG_STORE", "local"),
        ("PQUEUE_OBJECT_LOG_ROOT", "/tmp/pqueue-local-only"),
    ]))
    .expect("explicit local profile");
    let LogSpec::ObjectLog(spec) = config.backend.log else {
        panic!("expected object-log spec");
    };
    assert!(!spec.is_shared());
    assert!(matches!(spec, ObjectLogSpec::LocalFilesystem { .. }));
}

#[test]
fn postgres_control_plane_multireplica_advertise_address_fails_closed() {
    for advertise in [None, Some(""), Some("0.0.0.0:8080"), Some("not-an-address")] {
        let mut values = env(&[
            ("PQUEUE_CONTROL_PLANE", "postgres"),
            ("PQUEUE_REPLICA_COUNT", "2"),
            ("PQUEUE_OWNER_ID", "replica-a"),
            (
                "PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL",
                "postgres://localhost/pqueue",
            ),
        ]);
        if let Some(advertise) = advertise {
            values.insert("PQUEUE_ADVERTISE_ADDR".into(), advertise.into());
        }
        let Err(error) = Config::from_env(&values) else {
            panic!("missing or malformed advertise address must fail closed");
        };
        assert!(error.0.contains("PQUEUE_ADVERTISE_ADDR"), "{}", error.0);
    }
}

#[test]
fn owner_identity_multireplica_requires_and_preserves_full_width_id() {
    let mut values = env(&[
        ("PQUEUE_CONTROL_PLANE", "postgres"),
        ("PQUEUE_REPLICA_COUNT", "2"),
        ("PQUEUE_ADVERTISE_ADDR", "10.0.0.12:8080"),
        (
            "PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL",
            "postgres://localhost/pqueue",
        ),
        ("PQUEUE_NODE_ID", "7"),
    ]);
    let Err(error) = Config::from_env(&values) else {
        panic!("multi-replica owner id is mandatory");
    };
    assert!(error.0.contains("PQUEUE_OWNER_ID"), "{}", error.0);

    values.insert("PQUEUE_OWNER_ID".into(), "".into());
    let Err(error) = Config::from_env(&values) else {
        panic!("empty multi-replica owner id is invalid");
    };
    assert!(error.0.contains("PQUEUE_OWNER_ID"), "{}", error.0);

    values.insert(
        "PQUEUE_OWNER_ID".into(),
        "pod-7f4c9d8b-6b8d9f7c5-x2k9m".into(),
    );
    let config = Config::from_env(&values).expect("full-width owner identity is valid");
    assert_eq!(config.node_id, 7);
    assert_eq!(config.owner_id.as_str(), "pod-7f4c9d8b-6b8d9f7c5-x2k9m");

    values.insert(
        "PQUEUE_OWNER_ID".into(),
        "pod-7f4c9d8b-6b8d9f7c5-r8v4q".into(),
    );
    let peer = Config::from_env(&values).expect("second full-width owner identity is valid");
    assert_eq!(peer.node_id, config.node_id, "8-bit item IDs may match");
    assert_ne!(
        peer.owner_id, config.owner_id,
        "control-plane owners must not"
    );
    assert_eq!(peer.owner_id.as_str(), "pod-7f4c9d8b-6b8d9f7c5-r8v4q");
}

#[test]
fn owner_identity_single_replica_derives_legacy_node_owner() {
    let config = Config::from_env(&env(&[("PQUEUE_NODE_ID", "23")]))
        .expect("single-replica legacy configuration remains valid");
    assert_eq!(config.node_id, 23);
    assert_eq!(config.owner_id.as_str(), "node-23");
}
