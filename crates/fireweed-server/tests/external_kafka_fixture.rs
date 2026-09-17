//! P8k: hermetic ExternalKafka qualification fixture contracts.
//!
//! Proves the reusable fixture starts a digest-pinned single-node Kafka-compatible broker, creates a
//! run-owned topic, completes an rskafka produce/fetch preflight, and tears down on success and drop.
//! Feature-off compile/negative routes for the ExternalKafka sink live in
//! `change_record_sink` unit tests (default-off `external-kafka` feature).

#[path = "support/external_kafka_fixture.rs"]
mod external_kafka_fixture;

use external_kafka_fixture::{
    ExternalKafkaFixture, REDPANDA_IMAGE_DIGEST, REDPANDA_IMAGE_PINNED, REDPANDA_IMAGE_TAG,
    SENTINEL_KEY, SENTINEL_VALUE,
};

#[test]
fn pinned_image_constants_document_immutable_broker() {
    assert!(
        REDPANDA_IMAGE_PINNED.contains("sha256:"),
        "pinned image must use digest form"
    );
    assert_eq!(
        &REDPANDA_IMAGE_DIGEST[..7],
        "sha256:",
        "digest constant must be a sha256 digest"
    );
    assert!(
        REDPANDA_IMAGE_TAG.starts_with("redpandadata/redpanda:"),
        "tag constant names the Redpanda distribution"
    );
}

#[test]
fn fixture_starts_preflights_and_cleans_up() {
    let mut fixture = match ExternalKafkaFixture::start() {
        Ok(f) => f,
        Err(err) => {
            // Fail closed when docker/image are present but the fixture is broken. When the host
            // lacks docker or the pinned image, surface a LOUD skip-style failure with the reason —
            // ExternalKafka qualification cannot record results without the fixture.
            panic!("ExternalKafka fixture failed to start (required for P8k): {err}");
        }
    };

    assert!(
        fixture.host_port() > 0,
        "ephemeral loopback port must be assigned"
    );
    assert!(
        fixture.endpoint().starts_with("kafka://127.0.0.1:"),
        "endpoint must be kafka:// on loopback: {}",
        fixture.endpoint()
    );
    assert!(
        fixture.topic().starts_with("fireweed-ext-kafka-topic-"),
        "topic must be run-owned: {}",
        fixture.topic()
    );
    assert!(
        !fixture.log_capture().is_empty() || fixture.log_dir().exists(),
        "log capture or log dir must be populated"
    );

    // Second preflight on the live fixture must remain green (idempotent readiness).
    fixture
        .preflight_rskafka(external_kafka_fixture::DEFAULT_RSKAFKA_TIMEOUT)
        .expect("repeat rskafka preflight");

    let name = fixture.container_name().to_string();
    let process_id = fixture.process_id();
    let address = fixture.bootstrap().parse().unwrap();
    fixture.cleanup();
    assert_broker_stopped(&name, process_id, address);
}

#[test]
fn fixture_drop_tears_down_broker() {
    let fixture = ExternalKafkaFixture::start().expect("fixture start for drop teardown");
    let name = fixture.container_name().to_string();
    let process_id = fixture.process_id();
    let address = fixture.bootstrap().parse().unwrap();
    drop(fixture);
    assert_broker_stopped(&name, process_id, address);
}

#[test]
fn sentinel_constants_are_stable_for_cross_harness_correlation() {
    assert_eq!(
        SENTINEL_KEY,
        b"fireweed-external-kafka-fixture-sentinel-key"
    );
    assert_eq!(
        SENTINEL_VALUE,
        b"fireweed-external-kafka-fixture-sentinel-value"
    );
}

/// Default-off feature compile/negative routes for ExternalKafka live in
/// `change_record_sink::tests::change_record_sink_external_kafka_mode_uses_rskafka` (feature-off
/// build path). This test only locks mode classification for a `kafka://` endpoint so fixture
/// routes never collide with feature-off negatives.
#[test]
fn kafka_endpoint_classifies_as_external_kafka_mode() {
    use fireweed_server::{ChangeRecordSinkConfig, ChangeRecordSinkMode};

    let config = ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some("kafka://127.0.0.1:9".into()),
        ..Default::default()
    };
    assert_eq!(config.mode(), ChangeRecordSinkMode::ExternalKafka);
}

fn assert_broker_stopped(name: &str, process_id: Option<u32>, address: std::net::SocketAddr) {
    assert!(
        std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(200))
            .is_err(),
        "broker still accepts connections after cleanup"
    );
    if let Some(pid) = process_id {
        #[cfg(target_os = "linux")]
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "Kafka child was not reaped"
        );
    } else {
        let status = std::process::Command::new("docker")
            .args(["inspect", "-f", "{{.State.Running}}", name])
            .output()
            .expect("docker inspect after cleanup");
        assert!(
            !status.status.success() || String::from_utf8_lossy(&status.stdout).trim() != "true",
            "container {name} still runs after cleanup"
        );
    }
}
