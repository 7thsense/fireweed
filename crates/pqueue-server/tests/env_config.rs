use std::collections::BTreeMap;

use pqueue_server::{Config, LogSpec, ObjectLogSpec, S3CredentialSource};

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn objectlog_s3_env_builds_typed_shared_profile() {
    let config = Config::from_env(&map(&[
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
    let result = Config::from_env(&map(&[
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
    let result = Config::from_env(&map(&[
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
    let config = Config::from_env(&map(&[
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
