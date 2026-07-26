use std::collections::BTreeMap;

use fireweed_server::{Config, ControlPlaneSpec, LogSpec, ObjectLogSpec, S3CredentialSource};

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn fireweed_environment_is_authoritative() {
    let config = Config::from_env(&env(&[
        ("FIREWEED_LOG_BACKEND", "memory"),
        ("FIREWEED_PROJECTION_BACKEND", "inmemory"),
        ("FIREWEED_LISTEN_ADDR", "127.0.0.1:7001"),
    ]))
    .expect("Fireweed environment names must parse");
    assert_eq!(config.listen, "127.0.0.1:7001");
    assert!(matches!(config.backend.log, LogSpec::Memory));
}

#[test]
fn public_config_errors_name_the_fireweed_namespace() {
    let Err(error) = Config::from_env(&env(&[("FIREWEED_BOOTSTRAP_QUEUES", "missing-colon")]))
    else {
        panic!("invalid Fireweed configuration must fail closed");
    };
    assert!(error.to_string().contains("FIREWEED_BOOTSTRAP_QUEUES"));
}

#[test]
fn service_help_advertises_only_fireweed_runtime_names() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fireweed-service"))
        .arg("--help")
        .output()
        .expect("run fireweed-service --help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(stdout.starts_with("fireweed-service\n"));
    assert!(stdout.contains("FIREWEED_LISTEN_ADDR"));
}

#[test]
fn fireweed_service_binary_runs_the_fireweed_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fireweed-service"))
        .arg("--help")
        .output()
        .expect("run fireweed-service help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(stdout.starts_with("fireweed-service\n"));
    assert!(stdout.contains("FIREWEED_LISTEN_ADDR"));
}

#[test]
fn postgres_control_plane_env_selects_typed_shared_authority() {
    let config = Config::from_env(&env(&[
        ("FIREWEED_CONTROL_PLANE", "postgres"),
        ("FIREWEED_REPLICA_COUNT", "3"),
        ("FIREWEED_OWNER_ID", "fireweed-7f4c9d8b-owner-a"),
        ("FIREWEED_ADVERTISE_ADDR", "10.0.0.12:8080"),
        (
            "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL",
            "postgres://fireweed:secret@postgres.internal/fireweed",
        ),
        ("FIREWEED_CONTROL_PLANE_HEARTBEAT_TTL_MS", "7000"),
        ("FIREWEED_CONTROL_PLANE_LEASE_TTL_MS", "21000"),
    ]))
    .expect("shared Postgres control-plane config must parse");

    assert_eq!(config.advertise_addr.as_deref(), Some("10.0.0.12:8080"));
    assert_eq!(config.owner_id.as_str(), "fireweed-7f4c9d8b-owner-a");

    match config.backend.control_plane {
        ControlPlaneSpec::Postgres { url, config } => {
            assert_eq!(url, "postgres://fireweed:secret@postgres.internal/fireweed");
            assert_eq!(config.heartbeat_ttl_ms, 7_000);
            assert_eq!(config.lease_ttl_ms, 21_000);
        }
        ControlPlaneSpec::InProcess => panic!("expected shared Postgres control plane"),
    }
}

#[test]
fn postgres_control_plane_missing_dsn_fails_closed() {
    for dsn in [None, Some("")] {
        let mut values = env(&[("FIREWEED_CONTROL_PLANE", "postgres")]);
        if let Some(dsn) = dsn {
            values.insert(
                "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL".into(),
                dsn.into(),
            );
        }
        let Err(error) = Config::from_env(&values) else {
            panic!("missing DSN must fail closed");
        };
        assert!(
            error
                .0
                .contains("FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL"),
            "{}",
            error.0
        );
    }
}

#[test]
fn postgres_control_plane_boundary_rejects_inprocess_for_multiple_replicas() {
    let Err(error) = Config::from_env(&env(&[
        ("FIREWEED_CONTROL_PLANE", "inprocess"),
        ("FIREWEED_REPLICA_COUNT", "2"),
        ("FIREWEED_OWNER_ID", "replica-a"),
        ("FIREWEED_ADVERTISE_ADDR", "10.0.0.12:8080"),
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
        ("FIREWEED_CONTROL_PLANE_HEARTBEAT_TTL_MS", "invalid"),
        ("FIREWEED_CONTROL_PLANE_LEASE_TTL_MS", "0"),
    ] {
        let mut values = env(&[
            ("FIREWEED_CONTROL_PLANE", "postgres"),
            (
                "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL",
                "postgres://localhost/fireweed",
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
        ("FIREWEED_LOG_BACKEND", "objectlog"),
        ("FIREWEED_OBJECT_LOG_STORE", "s3"),
        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "https://s3.example.com"),
        ("FIREWEED_OBJECT_LOG_S3_BUCKET", "fireweed-prod"),
        ("FIREWEED_OBJECT_LOG_S3_REGION", "us-west-2"),
        ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", "production-access"),
        (
            "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
            "production-secret",
        ),
        ("FIREWEED_SEGMENT_TARGET_BYTES", "524288"),
        ("FIREWEED_SEGMENT_MAX_LATENCY_MS", "17"),
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
            assert_eq!(bucket, "fireweed-prod");
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
fn every_objectlog_s3_projection_accepts_postgres_publication_authority() {
    for projection in [
        "inmemory",
        "sqlite",
        "hybrid",
        "hybrid-strict",
        "hybrid-async",
    ] {
        let config = Config::from_env(&env(&[
            ("FIREWEED_LOG_BACKEND", "objectlog"),
            ("FIREWEED_PROJECTION_BACKEND", projection),
            ("FIREWEED_OBJECT_LOG_STORE", "s3"),
            ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "https://s3.example.com"),
            ("FIREWEED_OBJECT_LOG_S3_BUCKET", "fireweed-prod"),
            ("FIREWEED_OBJECT_LOG_S3_REGION", "us-west-2"),
            ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
            ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", "production-access"),
            (
                "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
                "production-secret",
            ),
            ("FIREWEED_CONTROL_PLANE", "postgres"),
            (
                "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL",
                "postgres://fireweed:secret@postgres.internal/fireweed",
            ),
        ]))
        .unwrap_or_else(|error| panic!("objectlog/{projection} must parse: {error}"));

        assert!(matches!(
            config.backend.log,
            LogSpec::ObjectLog(ObjectLogSpec::S3 { .. })
        ));
        assert!(matches!(
            config.backend.control_plane,
            ControlPlaneSpec::Postgres { .. }
        ));
    }
}

#[test]
fn objectlog_s3_env_rejects_plaintext_without_explicit_local_opt_in() {
    let result = Config::from_env(&env(&[
        ("FIREWEED_OBJECT_LOG_STORE", "s3"),
        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "http://minio:9000"),
        ("FIREWEED_OBJECT_LOG_S3_BUCKET", "fireweed"),
        ("FIREWEED_OBJECT_LOG_S3_REGION", "us-east-1"),
        ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", "access"),
        ("FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY", "secret"),
    ]));
    let Err(error) = result else {
        panic!("plaintext must fail closed by default");
    };
    assert!(error.0.contains("ALLOW_INSECURE") || error.0.contains("plaintext"));
}

#[test]
fn objectlog_s3_env_rejects_s3_variables_under_local_profile() {
    let result = Config::from_env(&env(&[
        ("FIREWEED_OBJECT_LOG_STORE", "local"),
        ("FIREWEED_OBJECT_LOG_S3_BUCKET", "must-not-be-ignored"),
    ]));
    let Err(error) = result else {
        panic!("shared-store settings must not silently fall back to local");
    };
    assert!(error.0.contains("fall back to node-local storage"));
}

#[test]
fn objectlog_s3_local_profile_is_explicitly_single_replica() {
    let config = Config::from_env(&env(&[
        ("FIREWEED_OBJECT_LOG_STORE", "local"),
        ("FIREWEED_OBJECT_LOG_ROOT", "/tmp/fireweed-local-only"),
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
            ("FIREWEED_CONTROL_PLANE", "postgres"),
            ("FIREWEED_REPLICA_COUNT", "2"),
            ("FIREWEED_OWNER_ID", "replica-a"),
            (
                "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL",
                "postgres://localhost/fireweed",
            ),
        ]);
        if let Some(advertise) = advertise {
            values.insert("FIREWEED_ADVERTISE_ADDR".into(), advertise.into());
        }
        let Err(error) = Config::from_env(&values) else {
            panic!("missing or malformed advertise address must fail closed");
        };
        assert!(error.0.contains("FIREWEED_ADVERTISE_ADDR"), "{}", error.0);
    }
}

#[test]
fn owner_identity_multireplica_requires_and_preserves_full_width_id() {
    let mut values = env(&[
        ("FIREWEED_CONTROL_PLANE", "postgres"),
        ("FIREWEED_REPLICA_COUNT", "2"),
        ("FIREWEED_ADVERTISE_ADDR", "10.0.0.12:8080"),
        (
            "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL",
            "postgres://localhost/fireweed",
        ),
        ("FIREWEED_NODE_ID", "7"),
    ]);
    let Err(error) = Config::from_env(&values) else {
        panic!("multi-replica owner id is mandatory");
    };
    assert!(error.0.contains("FIREWEED_OWNER_ID"), "{}", error.0);

    values.insert("FIREWEED_OWNER_ID".into(), "".into());
    let Err(error) = Config::from_env(&values) else {
        panic!("empty multi-replica owner id is invalid");
    };
    assert!(error.0.contains("FIREWEED_OWNER_ID"), "{}", error.0);

    values.insert(
        "FIREWEED_OWNER_ID".into(),
        "pod-7f4c9d8b-6b8d9f7c5-x2k9m".into(),
    );
    let config = Config::from_env(&values).expect("full-width owner identity is valid");
    assert_eq!(config.node_id, 7);
    assert_eq!(config.owner_id.as_str(), "pod-7f4c9d8b-6b8d9f7c5-x2k9m");

    values.insert(
        "FIREWEED_OWNER_ID".into(),
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
fn owner_identity_single_replica_derives_node_owner() {
    let config = Config::from_env(&env(&[("FIREWEED_NODE_ID", "23")]))
        .expect("single-replica configuration remains valid");
    assert_eq!(config.node_id, 23);
    assert_eq!(config.owner_id.as_str(), "node-23");
}
