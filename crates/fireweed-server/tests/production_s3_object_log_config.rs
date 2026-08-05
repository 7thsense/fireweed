//! P4s — live S3 production-config + API-005 provenance (fireweed-1b97d9b7).
//!
//! Consumes the P1s attested endpoint (`FIREWEED_S3_TEST_*` / secret file outside
//! the repository). Replaces retired Garage-positive production-config coverage
//! with provider-neutral cell IDs and attested MinIO provenance. Disposable
//! in-process docker MinIO and `.env.garage-e3` are not used.
//!
//! Focused run:
//! ```text
//! set -a; source /tmp/fireweed-s3-secrets/credentials.env; set +a
//! export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed
//! cargo test -p fireweed-server --test production_s3_object_log_config -- --nocapture
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use fireweed_server::{Config, LogSpec, ProjectionSpec, start};
use serde_json::Value;

const ATTESTATION_PATH: &str = "/tmp/fireweed-s3-secrets/s3-native-cas-capability-attestation.json";

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// Load P1s secret material from the standard test env vars (sourced outside the repo).
fn require_p1s_s3() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT is required for P4s live S3 production-config");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET")
        .expect("FIREWEED_S3_TEST_BUCKET is required for P4s live S3 production-config");
    let region =
        std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
        .expect("FIREWEED_S3_TEST_ACCESS_KEY is required for P4s live S3 production-config");
    let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
        .expect("FIREWEED_S3_TEST_SECRET_KEY is required for P4s live S3 production-config");
    (endpoint, bucket, region, access, secret)
}

fn load_attestation() -> Value {
    let text = std::fs::read_to_string(ATTESTATION_PATH).unwrap_or_else(|error| {
        panic!("P4s requires P1s attestation at {ATTESTATION_PATH}: {error}")
    });
    serde_json::from_str(&text).unwrap_or_else(|error| {
        panic!("P1s attestation must be valid JSON at {ATTESTATION_PATH}: {error}")
    })
}

/// Current public env surface for S3 log × projection (manifest cell `s3--{projection}`).
fn production_s3_env(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access: &str,
    secret: &str,
    projection: &str,
    bootstrap_queue: &str,
) -> BTreeMap<String, String> {
    let mut env = map(&[
        ("FIREWEED_LOG_BACKEND", "s3"),
        ("FIREWEED_PROJECTION_BACKEND", projection),
        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", endpoint),
        ("FIREWEED_OBJECT_LOG_S3_BUCKET", bucket),
        ("FIREWEED_OBJECT_LOG_S3_REGION", region),
        ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", access),
        ("FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY", secret),
        ("FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP", "true"),
        ("FIREWEED_SEGMENT_TARGET_BYTES", "1048576"),
        ("FIREWEED_SEGMENT_MAX_LATENCY_MS", "5"),
        ("FIREWEED_LISTEN_ADDR", "127.0.0.1:0"),
    ]);
    // Provider-neutral bootstrap queue id (never garage-*). Empty omits bootstrap.
    if !bootstrap_queue.is_empty() {
        env.insert(
            "FIREWEED_BOOTSTRAP_QUEUES".into(),
            bootstrap_queue.to_string(),
        );
    }
    if projection == "sqlite" {
        let path = std::env::temp_dir().join(format!(
            "fireweed-p4s-proj-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        env.insert(
            "FIREWEED_SQLITE_PROJECTION_PATH".into(),
            path.display().to_string(),
        );
    }
    if projection == "turso" {
        let path = std::env::temp_dir().join(format!(
            "fireweed-p4s-proj-{}-{}.turso",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        env.insert(
            "FIREWEED_TURSO_PROJECTION_PATH".into(),
            path.display().to_string(),
        );
    }
    env
}

#[test]
fn production_s3_config_rejects_incomplete_credentials_and_local_fallback() {
    let complete = production_s3_env(
        "http://127.0.0.1:9000",
        "fireweed",
        "us-east-1",
        "ak",
        "sk",
        "memory",
        "t1:s3--memory--config-negative",
    );
    for missing in [
        "FIREWEED_OBJECT_LOG_S3_ENDPOINT",
        "FIREWEED_OBJECT_LOG_S3_BUCKET",
        "FIREWEED_OBJECT_LOG_S3_REGION",
        "FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE",
        "FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID",
        "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
    ] {
        let mut env = complete.clone();
        env.remove(missing);
        let Err(error) = Config::from_env(&env) else {
            panic!("incomplete S3 config must fail closed: missing {missing}");
        };
        assert!(
            error.0.contains(missing) || error.0.contains("S3") || error.0.contains("s3"),
            "{missing}: {}",
            error.0
        );
    }

    // S3-shaped fields while selecting filesystem must refuse silent ignore.
    let mixed = map(&[
        ("FIREWEED_LOG_BACKEND", "filesystem"),
        ("FIREWEED_OBJECT_LOG_ROOT", "/tmp/would-silently-fallback"),
        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "http://minio:9000"),
        ("FIREWEED_PROJECTION_BACKEND", "memory"),
    ]);
    let Err(error) = Config::from_env(&mixed) else {
        panic!("filesystem backend must not ignore shared S3 configuration fields");
    };
    assert!(
        error.0.contains("refusing to ignore")
            || error.0.contains("S3")
            || error.0.contains("FIREWEED_OBJECT_LOG_S3"),
        "unexpected error: {}",
        error.0
    );
}

#[test]
fn production_s3_config_parses_public_s3_memory_and_sqlite_cells() {
    let env = production_s3_env(
        "https://s3.example.com",
        "fireweed-qual",
        "us-east-1",
        "ak",
        "sk",
        "memory",
        "t1:s3--memory",
    );
    let config = Config::from_env(&env).expect("s3×memory production env");
    assert!(matches!(config.backend.log, LogSpec::ObjectLog(_)));
    assert!(matches!(
        config.backend.projection,
        ProjectionSpec::InMemory
    ));

    let env = production_s3_env(
        "https://s3.example.com",
        "fireweed-qual",
        "us-east-1",
        "ak",
        "sk",
        "sqlite",
        "t1:s3--sqlite",
    );
    let config = Config::from_env(&env).expect("s3×sqlite production env");
    assert!(matches!(
        config.backend.projection,
        ProjectionSpec::Sqlite { .. }
    ));
}

/// P1s attestation is consumed for positive live identity (not Garage).
#[test]
fn p1s_attestation_is_minio_native_cas_not_garage() {
    let doc = load_attestation();

    assert_eq!(
        doc.get("bead_id").and_then(|v| v.as_str()),
        Some("fireweed-f5fa7380"),
        "attestation must bind to P1s bead fireweed-f5fa7380"
    );
    assert_eq!(
        doc.get("capability_id").and_then(|v| v.as_str()),
        Some("S3-NATIVE-CAS-CAPABILITY-ATTESTATION"),
        "attestation must carry the manifest capability id"
    );
    assert!(
        doc.pointer("/credential_path_isolation/env_garage_e3_absent")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        "attestation must prove .env.garage-e3 is absent"
    );
    assert!(
        doc.pointer("/credential_path_isolation/secret_file_outside_repository")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        "attestation must keep secrets outside the repository"
    );

    let selected = doc
        .pointer("/results/selected_provider")
        .or_else(|| doc.pointer("/s3/provider"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    assert_eq!(
        selected, "minio",
        "P1s positive selection must be minio, not garage: selected={selected}"
    );
    assert!(
        doc.pointer("/results/selected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        "attestation results.selected must be true"
    );
    assert!(
        doc.pointer("/s3/native_atomic_conditional_create")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            && doc
                .pointer("/s3/native_atomic_conditional_update")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        "attestation must prove native CAS create+update"
    );

    // Garage may appear only as a rejected/unsupported candidate residual.
    let rejected = doc
        .pointer("/results/rejected_candidates")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        rejected.iter().any(|candidate| {
            candidate
                .get("provider")
                .and_then(|v| v.as_str())
                .map(|p| p.eq_ignore_ascii_case("garage"))
                .unwrap_or(false)
                && candidate.get("selectable").and_then(|v| v.as_bool()) == Some(false)
        }),
        "attestation must retain Garage as a non-selectable rejected candidate"
    );
}

/// P1s env builds a typed Config whose endpoint/bucket match the attestation.
///
/// Full `start()` applies a bootstrap queue inventory. On S3, Fireweed publishes
/// queue definitions with create-only PutObject (`If-None-Match: *`); a P1s-qualified
/// endpoint must allow bootstrap. This test proves the production config path is
/// wired to the attested endpoint and never pins Garage cell ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_s3_object_log_config_uses_p1s_attested_endpoint() {
    let doc = load_attestation();
    let (endpoint, bucket, region, access, secret) = require_p1s_s3();

    let attested_endpoint = doc
        .pointer("/s3/endpoint")
        .or_else(|| doc.pointer("/preflight/endpoint"))
        .and_then(|v| v.as_str())
        .expect("attestation must record s3.endpoint");
    assert_eq!(
        endpoint.trim_end_matches('/'),
        attested_endpoint.trim_end_matches('/'),
        "FIREWEED_S3_TEST_ENDPOINT must match P1s attestation endpoint"
    );
    let attested_bucket = doc
        .pointer("/preflight/bucket")
        .or_else(|| doc.pointer("/s3/bucket_ownership_acknowledgement"))
        .and_then(|v| v.as_str())
        .expect("attestation must record bucket");
    assert_eq!(
        bucket, attested_bucket,
        "FIREWEED_S3_TEST_BUCKET must match P1s attestation bucket"
    );

    // Provider-neutral bootstrap cell id (manifest form s3--memory--…, never garage-*).
    let queue = "t1:s3--memory--p4s-live";
    let env = production_s3_env(
        &endpoint, &bucket, &region, &access, &secret, "memory", queue,
    );
    let config = Config::from_env(&env).expect("P1s S3 env builds typed Config");
    assert!(
        matches!(config.backend.log, LogSpec::ObjectLog(_)),
        "must select S3 object-log backend"
    );

    let server = start(config)
        .await
        .expect("P1s-qualified S3 start must succeed with create-only definition authority");
    server.shutdown_and_drain(Duration::from_secs(10)).await;
}

/// Unsupported / unreachable endpoint fails closed (not via retired Garage pins).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_s3_endpoint_fails_closed_without_garage_identity() {
    let env = production_s3_env(
        "http://127.0.0.1:1",
        "fireweed-unreachable",
        "us-east-1",
        "ak",
        "sk",
        "memory",
        "t1:s3--memory--unsupported-endpoint",
    );
    let config = Config::from_env(&env).expect("parse succeeds pre-I/O");
    let result = start(config).await;
    assert!(
        result.is_err(),
        "unreachable S3 endpoint must fail at open/start"
    );
    let err = format!("{:?}", result.err().unwrap());
    // Failure text may document Garage as unsupported residual; it must not claim a garage cell id.
    assert!(
        !err.to_ascii_lowercase().contains("garage-s3"),
        "error must not name garage-s3 positive cell ids: {err}"
    );
}

/// S3 subset of the API-005 ownership map must match the shared method_contracts set exactly.
#[test]
fn s3_api005_ownership_matches_provider_neutral_method_set() {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop();
    root.pop();
    let map_path = root.join("docs/helix/04-build/api005-suite-ownership-map.json");
    let text = std::fs::read_to_string(&map_path)
        .unwrap_or_else(|e| panic!("ownership map missing at {}: {e}", map_path.display()));
    let doc: Value = serde_json::from_str(&text).expect("ownership map json");

    let contracts = doc
        .get("method_contracts")
        .and_then(|v| v.as_array())
        .expect("method_contracts array");
    let neutral: BTreeSet<&str> = contracts
        .iter()
        .filter_map(|m| m.get("method").and_then(|v| v.as_str()))
        .collect();
    assert!(
        !neutral.is_empty(),
        "provider-neutral method set must be non-empty"
    );

    let entries = doc
        .get("entries")
        .and_then(|v| v.as_array())
        .expect("entries array");

    let mut methods_by_cell: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for entry in entries {
        let cell = entry.get("cell_id").and_then(|v| v.as_str()).unwrap_or("");
        if !cell.starts_with("s3--") {
            continue;
        }
        // Positive cell ids use manifest separators; never garage-* brands.
        assert!(
            !cell.to_ascii_lowercase().contains("garage"),
            "S3 ownership cell id must not contain garage: {cell}"
        );
        if entry.get("kind").and_then(|v| v.as_str()) != Some("method") {
            continue;
        }
        let method = entry
            .get("method")
            .and_then(|v| v.as_str())
            .expect("method entry must name method");
        methods_by_cell.entry(cell).or_default().insert(method);
    }

    assert!(
        !methods_by_cell.is_empty(),
        "ownership map must register S3 cell method ownership entries"
    );
    for (cell, methods) in &methods_by_cell {
        assert_eq!(
            methods,
            &neutral,
            "S3 cell {cell} method set must equal provider-neutral method_contracts \
             (missing={:?} extra={:?})",
            neutral.difference(methods).collect::<Vec<_>>(),
            methods.difference(&neutral).collect::<Vec<_>>()
        );
    }

    // Garage must not appear as a positive functional suite identity in the ownership map.
    // (Historical prose is out of scope; ownership entries use cell_id / test_id only.)
    for entry in entries {
        let cell = entry
            .get("cell_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let test_id = entry
            .get("test_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        assert!(
            !cell.contains("garage") && !test_id.contains("garage"),
            "ownership entry must not use garage identity: cell={cell} test_id={test_id}"
        );
    }
}
