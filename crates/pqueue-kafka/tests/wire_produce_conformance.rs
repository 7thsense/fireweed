// Wire conformance tests for pqueue-kafka producer path.
//
// Uses a Go franz-go oracle to verify that pqueue-kafka speaks correct Kafka
// producer wire protocol (ApiVersions, Metadata, Produce) to an independent
// client implementation.
//
// Tests are skipped when `go` is not in PATH.

use pqueue_kafka::test_support::TestProducerServer;
use std::path::PathBuf;
use std::process::Command;

fn go_available() -> bool {
    Command::new("go")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn oracle_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("compat")
        .join("producer_oracle")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_produce_conformance_franz_go_produce() {
    if !go_available() {
        eprintln!("SKIP: go not in PATH");
        return;
    }

    let topic = "test-queue";
    let server = TestProducerServer::start(vec![topic.to_string()]).await;
    let bootstrap = server.bootstrap_servers();
    let dir = oracle_dir();

    // Run the blocking Go subprocess on a blocking thread so the tokio runtime
    // can continue processing server tasks concurrently.
    let out = tokio::task::spawn_blocking(move || {
        Command::new("go")
            .args(["run", "."])
            .arg(&bootstrap)
            .arg(topic)
            .current_dir(&dir)
            .output()
            .expect("failed to spawn go run")
    })
    .await
    .expect("spawn_blocking panicked");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    print!("{stdout}");
    if !stderr.is_empty() {
        eprint!("{stderr}");
    }

    assert!(
        out.status.success(),
        "franz-go producer oracle failed (exit {:?})\nstdout: {stdout}\nstderr: {stderr}",
        out.status.code()
    );
}
