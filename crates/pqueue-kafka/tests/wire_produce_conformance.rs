// Wire conformance tests for pqueue-kafka producer path.
//
// Uses a Go franz-go oracle to verify that pqueue-kafka speaks correct Kafka
// producer wire protocol (ApiVersions, Metadata, Produce) to an independent
// client implementation.
//
// Tests are skipped when `go` is not in PATH.

use pqueue_kafka::test_support::TestProducerServer;
use pqueue_core::{QueueId, TenantId};
use pqueue_storage::traits::LogStore;
use pqueue_storage::types::{ShardId, ShardKey};
use pqueue_storage::QueueCommand;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

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

/// Same oracle, but now with pqueue storage — verifies 5 records land in the log with
/// correct value payloads (e.g. "val-0" through "val-4").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_produce_conformance_franz_go_payload_stored() {
    if !go_available() {
        eprintln!("SKIP: go not in PATH");
        return;
    }

    let topic = "test-queue";
    let server = TestProducerServer::start_with_store(vec![topic.to_string()]).await;
    let store = server.store().unwrap().clone();
    let bootstrap = server.bootstrap_servers();
    let dir = oracle_dir();

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
    assert!(
        out.status.success(),
        "franz-go oracle failed\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Give the async writer a moment to flush persisted records.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let shard_key = ShardKey {
        tenant_id: TenantId::new("default").unwrap(),
        queue_id: QueueId::new(topic).unwrap(),
        shard_id: ShardId::new(0),
    };
    let page = store
        .log
        .read_from(&shard_key, None, 20)
        .await
        .expect("shard should exist after franz-go produce");

    // Collect all items from all BatchPush commands.
    let mut all_payloads: Vec<Option<bytes::Bytes>> = vec![];
    for (_, envelope) in &page.commands {
        if let QueueCommand::BatchPush(cmd) = &envelope.command {
            for item in &cmd.items {
                all_payloads.push(item.payload.clone());
            }
        }
    }

    assert_eq!(all_payloads.len(), 5, "expected 5 items from 5 produced records, got {}", all_payloads.len());
    for (i, payload) in all_payloads.iter().enumerate() {
        let expected = format!("val-{}", i);
        assert_eq!(
            payload.as_deref(),
            Some(expected.as_bytes()),
            "item {} payload mismatch: got {:?}",
            i,
            payload.as_ref().and_then(|b| std::str::from_utf8(b).ok())
        );
    }
}
