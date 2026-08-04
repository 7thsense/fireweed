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
    fixture.cleanup();
    // Container must be gone after explicit cleanup.
    let status = std::process::Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", &name])
        .output()
        .expect("docker inspect after cleanup");
    let running = String::from_utf8_lossy(&status.stdout);
    assert!(
        !status.status.success() || running.trim() != "true",
        "container {name} must not remain running after cleanup"
    );
}

#[test]
fn fixture_drop_tears_down_container() {
    let fixture = ExternalKafkaFixture::start().expect("fixture start for drop teardown");
    let name = fixture.container_name().to_string();
    drop(fixture);
    let status = std::process::Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", &name])
        .output()
        .expect("docker inspect after drop");
    let running = String::from_utf8_lossy(&status.stdout);
    assert!(
        !status.status.success() || running.trim() != "true",
        "Drop must remove container {name}"
    );
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

    let mut config = ChangeRecordSinkConfig::default();
    config.enabled = true;
    config.endpoint = Some("kafka://127.0.0.1:9".into());
    assert_eq!(config.mode(), ChangeRecordSinkMode::ExternalKafka);
}
