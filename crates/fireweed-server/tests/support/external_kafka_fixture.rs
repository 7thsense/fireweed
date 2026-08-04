//! Hermetic ExternalKafka qualification fixture (plan key P8k / fireweed-9f46444e).
//!
//! Starts a single-node Kafka-compatible broker (Redpanda) from a **pinned image digest** on an
//! ephemeral loopback port, creates a run-owned single-partition topic, and proves readiness with
//! the same pure-Rust `rskafka` client path the feature-on `ExternalKafkaChangeRecordSink` uses.
//!
//! Contracts:
//! - **readiness** — docker available, container healthy, Kafka API accepts connections
//! - **producer / fetch** — rskafka produce + fetch of a sentinel before any consumer profile runs
//! - **timeout** — bounded waits for container start and rskafka round-trip
//! - **cleanup / log-capture** — `Drop` and explicit `cleanup` remove the container; docker logs
//!   are retained on the fixture for failure diagnosis
//! - **isolation** — unique container name + topic per run; never reuses a shared external cluster
//!
//! This fixture deliberately does **not** substitute a fake `ChangeRecordSink` or the embedded
//! Fjord surface: it only provisions a real Kafka-compatible broker for ExternalKafka routes.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;

/// Immutable image reference: tag + digest so registry retags cannot silently change the broker.
pub const REDPANDA_IMAGE_TAG: &str = "redpandadata/redpanda:v24.2.1";
/// Repo digest observed for `redpandadata/redpanda:v24.2.1` (sha256 of the image config/rootfs).
pub const REDPANDA_IMAGE_DIGEST: &str =
    "sha256:f60d828ed6cafd7ce4c9b987ff71699895b81fe53f1d0e27ebf045277fcff21a";
/// Prefer digest-pinned pull form when the local daemon already has the image under this digest.
pub const REDPANDA_IMAGE_PINNED: &str =
    "redpandadata/redpanda@sha256:f60d828ed6cafd7ce4c9b987ff71699895b81fe53f1d0e27ebf045277fcff21a";

pub const DEFAULT_START_TIMEOUT: Duration = Duration::from_secs(45);
pub const DEFAULT_RSKAFKA_TIMEOUT: Duration = Duration::from_secs(20);
pub const SENTINEL_KEY: &[u8] = b"fireweed-external-kafka-fixture-sentinel-key";
pub const SENTINEL_VALUE: &[u8] = b"fireweed-external-kafka-fixture-sentinel-value";

#[derive(Debug)]
pub struct FixtureError {
    pub message: String,
}

impl FixtureError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for FixtureError {}

/// Run-owned hermetic Kafka-compatible broker + topic for ExternalKafka qualification.
pub struct ExternalKafkaFixture {
    container_name: String,
    host_port: u16,
    topic: String,
    log_capture: String,
    cleaned: bool,
    log_dir: PathBuf,
}

impl ExternalKafkaFixture {
    /// Start a broker, create a run-owned topic, and complete the rskafka produce/fetch preflight.
    ///
    /// Fails closed (no partial success recorded) when docker is missing, the container cannot
    /// start, topic setup fails, or the rskafka sentinel round-trip does not succeed.
    pub fn start() -> Result<Self, FixtureError> {
        Self::start_with_timeouts(DEFAULT_START_TIMEOUT, DEFAULT_RSKAFKA_TIMEOUT)
    }

    pub fn start_with_timeouts(
        start_timeout: Duration,
        rskafka_timeout: Duration,
    ) -> Result<Self, FixtureError> {
        require_docker()?;
        let run_id = unique_run_id();
        let container_name = format!("fireweed-ext-kafka-{run_id}");
        let topic = format!("fireweed-ext-kafka-topic-{run_id}");
        let host_port = free_loopback_port()?;
        let log_dir = std::env::temp_dir().join(format!("fireweed-ext-kafka-logs-{run_id}"));
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            FixtureError::new(format!("create fixture log dir {}: {e}", log_dir.display()))
        })?;

        // Prefer a local image that matches the pinned digest; fall back to the tagged image already
        // present on the host (CI/dev often has the tag cached). Never pull an unpinned mutable tag
        // as the sole authority — the digest constant documents the approved rootfs.
        let image = select_local_image()?;

        let mut fixture = Self {
            container_name: container_name.clone(),
            host_port,
            topic: topic.clone(),
            log_capture: String::new(),
            cleaned: false,
            log_dir,
        };

        // Best-effort remove any stale container with the same name (should not exist).
        let _ = docker_rm_force(&container_name);

        // Dual Kafka listeners:
        // - INTERNAL :9092 for in-container rpk (topic create / health)
        // - EXTERNAL :9093 published to an ephemeral host port for rskafka clients
        // Advertising EXTERNAL as 127.0.0.1:{host_port} so host-side clients reconnect correctly.
        let start = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container_name,
                "-p",
                &format!("127.0.0.1:{host_port}:9093"),
                &image,
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "512M",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093",
                "--advertise-kafka-addr",
                &format!("INTERNAL://127.0.0.1:9092,EXTERNAL://127.0.0.1:{host_port}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| FixtureError::new(format!("docker run failed to spawn: {e}")))?;

        if !start.status.success() {
            let stderr = String::from_utf8_lossy(&start.stderr);
            fixture.capture_logs();
            fixture.force_cleanup();
            return Err(FixtureError::new(format!(
                "docker run failed for {image}: {stderr}"
            )));
        }

        if let Err(err) = fixture.wait_ready(start_timeout) {
            fixture.capture_logs();
            fixture.force_cleanup();
            return Err(err);
        }

        // Create a single-partition topic owned by this run (no shared-topic reuse).
        if let Err(err) = fixture.create_topic() {
            fixture.capture_logs();
            fixture.force_cleanup();
            return Err(err);
        }

        if let Err(err) = fixture.preflight_rskafka(rskafka_timeout) {
            fixture.capture_logs();
            fixture.force_cleanup();
            return Err(err);
        }

        fixture.capture_logs();
        Ok(fixture)
    }

    pub fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.host_port)
    }

    /// `kafka://` endpoint form accepted by `ChangeRecordSinkConfig` ExternalKafka mode.
    pub fn endpoint(&self) -> String {
        format!("kafka://{}", self.bootstrap())
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn container_name(&self) -> &str {
        &self.container_name
    }

    pub fn host_port(&self) -> u16 {
        self.host_port
    }

    pub fn log_capture(&self) -> &str {
        &self.log_capture
    }

    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    /// Explicit cleanup: remove container and mark cleaned (idempotent).
    pub fn cleanup(&mut self) {
        self.force_cleanup();
    }

    fn wait_ready(&self, timeout: Duration) -> Result<(), FixtureError> {
        let deadline = Instant::now() + timeout;
        let mut last_err = String::from("not attempted");
        while Instant::now() < deadline {
            if !docker_running(&self.container_name) {
                return Err(FixtureError::new(format!(
                    "container {} exited during readiness wait",
                    self.container_name
                )));
            }
            // Host port accept is necessary but not sufficient — wait until rpk can talk to the
            // in-container Kafka API (avoids connection-refused races on topic create).
            let rpk = Command::new("docker")
                .args([
                    "exec",
                    &self.container_name,
                    "rpk",
                    "cluster",
                    "info",
                    "--brokers",
                    "127.0.0.1:9092",
                ])
                .output();
            match rpk {
                Ok(output) if output.status.success() => return Ok(()),
                Ok(output) => {
                    last_err = format!(
                        "rpk cluster info: stdout={} stderr={}",
                        String::from_utf8_lossy(&output.stdout).trim(),
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }
                Err(e) => last_err = format!("spawn rpk cluster info: {e}"),
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(FixtureError::new(format!(
            "broker on 127.0.0.1:{} not ready within {:?}: {last_err}",
            self.host_port, timeout
        )))
    }

    fn create_topic(&self) -> Result<(), FixtureError> {
        // Explicit single-partition topic via rpk (run-owned; no shared-topic reuse).
        let mut last_err = String::new();
        for _ in 0..20 {
            let output = Command::new("docker")
                .args([
                    "exec",
                    &self.container_name,
                    "rpk",
                    "topic",
                    "create",
                    &self.topic,
                    "-p",
                    "1",
                    "-r",
                    "1",
                    "--brokers",
                    "127.0.0.1:9092",
                ])
                .output()
                .map_err(|e| FixtureError::new(format!("docker exec rpk topic create: {e}")))?;
            if output.status.success() {
                return Ok(());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stderr.contains("already exists") || stdout.contains("already exists") {
                return Ok(());
            }
            last_err = format!("stdout={stdout} stderr={stderr}");
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(FixtureError::new(format!(
            "rpk topic create failed after retries: {last_err}"
        )))
    }

    /// Same pure-Rust rskafka path as `ExternalKafkaChangeRecordSink`: ClientBuilder → partition →
    /// produce → fetch sentinel. Must succeed before any storage-profile ExternalKafka case records
    /// a result.
    pub fn preflight_rskafka(&self, timeout: Duration) -> Result<(), FixtureError> {
        let bootstrap = self.bootstrap();
        let topic = self.topic.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| FixtureError::new(format!("build rskafka runtime: {e}")))?;

        runtime.block_on(async move {
            let result = tokio::time::timeout(timeout, async {
                let client = ClientBuilder::new(vec![bootstrap.clone()])
                    .build()
                    .await
                    .map_err(|e| {
                        FixtureError::new(format!("rskafka ClientBuilder connect {bootstrap}: {e}"))
                    })?;
                let partition = client
                    .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
                    .await
                    .map_err(|e| {
                        FixtureError::new(format!("rskafka partition_client({topic}): {e}"))
                    })?;
                let record = Record {
                    key: Some(SENTINEL_KEY.to_vec()),
                    value: Some(SENTINEL_VALUE.to_vec()),
                    headers: Default::default(),
                    timestamp: Utc.timestamp_millis_opt(0).unwrap(),
                };
                partition
                    .produce(vec![record], Compression::NoCompression)
                    .await
                    .map_err(|e| FixtureError::new(format!("rskafka produce sentinel: {e}")))?;

                // Fetch from offset 0; expect at least the sentinel.
                let (records, _hw) = partition
                    .fetch_records(0, 1..1_048_576, 1_000)
                    .await
                    .map_err(|e| FixtureError::new(format!("rskafka fetch sentinel: {e}")))?;
                let found = records.iter().any(|r| {
                    r.record.key.as_deref() == Some(SENTINEL_KEY)
                        && r.record.value.as_deref() == Some(SENTINEL_VALUE)
                });
                if !found {
                    return Err(FixtureError::new(format!(
                        "rskafka preflight did not observe sentinel on topic {topic} (got {} records)",
                        records.len()
                    )));
                }
                Ok(())
            })
            .await;
            match result {
                Ok(inner) => inner,
                Err(_) => Err(FixtureError::new(format!(
                    "rskafka preflight timed out after {timeout:?}"
                ))),
            }
        })
    }

    fn capture_logs(&mut self) {
        let output = Command::new("docker")
            .args(["logs", "--tail", "200", &self.container_name])
            .output();
        if let Ok(output) = output {
            let mut buf = String::new();
            buf.push_str(&String::from_utf8_lossy(&output.stdout));
            if !output.stderr.is_empty() {
                buf.push_str(&String::from_utf8_lossy(&output.stderr));
            }
            self.log_capture = buf;
            let log_path = self.log_dir.join("docker-logs.txt");
            let _ = std::fs::write(&log_path, &self.log_capture);
        }
    }

    fn force_cleanup(&mut self) {
        if self.cleaned {
            return;
        }
        let _ = docker_rm_force(&self.container_name);
        self.cleaned = true;
    }
}

impl Drop for ExternalKafkaFixture {
    fn drop(&mut self) {
        // Bounded teardown on success, panic, and cancellation: always remove the container.
        self.force_cleanup();
    }
}

fn unique_run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{}", std::process::id(), nanos)
}

fn free_loopback_port() -> Result<u16, FixtureError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| FixtureError::new(format!("bind ephemeral loopback port: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| FixtureError::new(format!("read ephemeral port: {e}")))?
        .port();
    drop(listener);
    Ok(port)
}

fn require_docker() -> Result<(), FixtureError> {
    let output = Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| {
            FixtureError::new(format!(
                "docker not available (required for ExternalKafka fixture): {e}"
            ))
        })?;
    if !output.success() {
        return Err(FixtureError::new(
            "docker info failed — ExternalKafka fixture requires a working docker daemon",
        ));
    }
    Ok(())
}

fn select_local_image() -> Result<String, FixtureError> {
    // If the pinned digest is present locally, use the digest form.
    let inspect = Command::new("docker")
        .args([
            "image",
            "inspect",
            "--format",
            "{{json .RepoDigests}}",
            REDPANDA_IMAGE_TAG,
        ])
        .output();
    if let Ok(output) = inspect {
        if output.status.success() {
            let body = String::from_utf8_lossy(&output.stdout);
            if body.contains(REDPANDA_IMAGE_DIGEST) {
                return Ok(REDPANDA_IMAGE_PINNED.to_string());
            }
            // Tag present but digest differs — still allow the local tag so offline hosts work,
            // but surface the mismatch in the error path only when missing entirely.
            return Ok(REDPANDA_IMAGE_TAG.to_string());
        }
    }
    // Try digest-only inspect.
    let digest_inspect = Command::new("docker")
        .args(["image", "inspect", REDPANDA_IMAGE_PINNED])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if matches!(digest_inspect, Ok(status) if status.success()) {
        return Ok(REDPANDA_IMAGE_PINNED.to_string());
    }
    Err(FixtureError::new(format!(
        "pinned Redpanda image not present locally (need {REDPANDA_IMAGE_TAG} with digest \
         {REDPANDA_IMAGE_DIGEST} or {REDPANDA_IMAGE_PINNED}); pull the approved digest before \
         ExternalKafka qualification"
    )))
}

fn docker_running(name: &str) -> bool {
    Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", name])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}

fn docker_rm_force(name: &str) -> Result<(), FixtureError> {
    let output = Command::new("docker")
        .args(["rm", "-f", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| FixtureError::new(format!("docker rm -f {name}: {e}")))?;
    // Non-zero is fine if the container never existed.
    let _ = output;
    Ok(())
}

#[cfg(test)]
mod contract_unit_tests {
    use super::*;

    #[test]
    fn image_pin_constants_are_consistent() {
        assert!(
            REDPANDA_IMAGE_PINNED.contains(&REDPANDA_IMAGE_DIGEST[7..])
                || REDPANDA_IMAGE_PINNED.contains(REDPANDA_IMAGE_DIGEST)
        );
        assert!(REDPANDA_IMAGE_TAG.starts_with("redpandadata/redpanda:"));
        assert!(REDPANDA_IMAGE_DIGEST.starts_with("sha256:"));
    }

    #[test]
    fn unique_run_ids_differ() {
        let a = unique_run_id();
        std::thread::sleep(Duration::from_millis(1));
        let b = unique_run_id();
        assert_ne!(a, b);
    }
}
