use std::collections::BTreeMap;

use pqueue_server::{Config, ControlPlaneSpec};

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
        (
            "PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL",
            "postgres://pqueue:secret@postgres.internal/pqueue",
        ),
        ("PQUEUE_CONTROL_PLANE_HEARTBEAT_TTL_MS", "7000"),
        ("PQUEUE_CONTROL_PLANE_LEASE_TTL_MS", "21000"),
    ]))
    .expect("shared Postgres control-plane config must parse");

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
