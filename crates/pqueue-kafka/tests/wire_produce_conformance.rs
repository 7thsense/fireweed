// Wire conformance tests for pqueue-kafka producer path.
//
// Uses a Go franz-go oracle to verify that pqueue-kafka speaks correct Kafka
// producer wire protocol (ApiVersions, Metadata, Produce) to an independent
// client implementation.
//
// Tests are skipped when `go` is not in PATH.

use pqueue_kafka::test_support::TestProducerServer;
use pqueue_core::{QueueId, TenantId, UtcTimestamp};
use pqueue_storage::traits::{ClaimRequest, LogStore, ProjectionStore};
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

/// Full end-to-end pipeline: franz-go produces records → pqueue workers claim them.
///
/// Verifies the "Kafka producer → pqueue enqueue → worker claim" contract
/// that makes pqueue-kafka useful as an ingestion bridge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_produce_conformance_produced_records_claimable_as_pqueue_items() {
    if !go_available() {
        eprintln!("SKIP: go not in PATH");
        return;
    }

    let topic = "claim-test-queue";
    let server = TestProducerServer::start_with_store(vec![topic.to_string()]).await;
    let store = server.store().unwrap().clone();
    let bootstrap = server.bootstrap_servers();
    let dir = oracle_dir();

    // Franz-go produces 5 records.
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
    assert!(out.status.success(), "franz-go oracle failed\nstdout: {stdout}\nstderr: {stderr}");

    // Wait for async persist path.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let shard_key = ShardKey {
        tenant_id: TenantId::new("default").unwrap(),
        queue_id: QueueId::new(topic).unwrap(),
        shard_id: ShardId::new(0),
    };

    // Claim all items as a pqueue worker would.
    let now = UtcTimestamp::new(0, 0).unwrap();
    let expires = UtcTimestamp::new(3600, 0).unwrap();
    let claim_result = store
        .projection
        .batch_claim(ClaimRequest {
            shard_key: shard_key.clone(),
            max_items: 10,
            now: now.clone(),
            lease_token: "worker-1".to_string(),
            lease_expires_at: expires,
        })
        .await
        .expect("batch_claim must succeed after produce");

    assert_eq!(
        claim_result.claimed_item_ids.len(),
        5,
        "expected 5 claimable items from 5 produced records, got {}",
        claim_result.claimed_item_ids.len()
    );

    // Verify the claimed items' payloads via the log (read back by item_id prefix).
    let page = store.log.read_from(&shard_key, None, 20).await.unwrap();
    let mut id_to_payload = std::collections::HashMap::new();
    for (_, envelope) in &page.commands {
        if let QueueCommand::BatchPush(cmd) = &envelope.command {
            for item in &cmd.items {
                id_to_payload.insert(item.item_id.as_str().to_owned(), item.payload.clone());
            }
        }
    }

    for claimed_id in &claim_result.claimed_item_ids {
        let payload = id_to_payload.get(claimed_id.as_str())
            .unwrap_or_else(|| panic!("claimed item_id {} not found in log", claimed_id));
        assert!(payload.is_some(), "claimed item {} has no payload", claimed_id);
    }
}
