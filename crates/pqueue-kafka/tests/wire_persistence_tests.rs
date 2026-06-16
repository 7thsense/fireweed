// Integration tests verifying produce records are persisted to pqueue storage.
// Uses start_with_store() to get a server with MemoryLogStore+MemoryProjectionStore,
// then checks that BatchPush commands landed after a Produce request.

use bytes::{BufMut, BytesMut};
use pqueue_core::{QueueId, TenantId};
use pqueue_kafka::test_support::TestProducerServer;
use pqueue_storage::traits::LogStore;
use pqueue_storage::types::{ShardId, ShardKey};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn framed_produce_v3(queue: &str, correlation_id: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(-1); // null transactional_id
    body.put_i16(1); // acks=1
    body.put_i32(5000); // timeout_ms
    body.put_i32(1); // topics count
    body.put_i16(queue.len() as i16);
    body.extend_from_slice(queue.as_bytes());
    body.put_i32(1); // partitions count
    body.put_i32(0); // partition 0
    body.put_i32(-1); // null records (no records — produces empty push batch)

    let mut hdr = BytesMut::new();
    hdr.put_i16(0); // api_key Produce
    hdr.put_i16(3); // api_version 3 (legacy header)
    hdr.put_i32(correlation_id);
    hdr.put_i16(-1); // null client_id
    hdr.extend_from_slice(&body);

    let mut frame = BytesMut::new();
    frame.put_i32(hdr.len() as i32);
    frame.extend_from_slice(&hdr);
    frame.to_vec()
}

fn framed_produce_v9_with_record(queue: &str, correlation_id: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(0x00); // null transactional_id CNS
    body.put_i16(1); // acks=1
    body.put_i32(5000); // timeout_ms

    // topic_data: compact_array 1 element
    body.put_u8(0x02);
    let qlen = queue.len() as u8 + 1;
    body.put_u8(qlen);
    body.extend_from_slice(queue.as_bytes());

    // partition_data: compact_array 1 element
    body.put_u8(0x02);
    body.put_i32(0); // partition 0

    // Build a minimal RecordBatch with 1 record.
    let batch = build_record_batch(b"k", b"v");
    assert!(
        batch.len() + 1 < 128,
        "batch too large for single-byte varint"
    );
    body.put_u8((batch.len() + 1) as u8); // compact_nullable_bytes length varint
    body.extend_from_slice(&batch);
    body.put_u8(0x00); // partition tagged_fields
    body.put_u8(0x00); // topic tagged_fields
    body.put_u8(0x00); // request tagged_fields

    let mut hdr = BytesMut::new();
    hdr.put_i16(0); // Produce
    hdr.put_i16(9); // v9 flexible
    hdr.put_i32(correlation_id);
    hdr.put_i16(-1); // null client_id NULLABLE_STRING
    hdr.put_u8(0x00); // empty _tagged_fields
    hdr.extend_from_slice(&body);

    let mut frame = BytesMut::new();
    frame.put_i32(hdr.len() as i32);
    frame.extend_from_slice(&hdr);
    frame.to_vec()
}

fn build_record_batch(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut rec_body = BytesMut::new();
    rec_body.put_i8(0); // attributes
    write_zigzag(&mut rec_body, 0); // timestampDelta
    write_zigzag(&mut rec_body, 0); // offsetDelta
    write_zigzag(&mut rec_body, key.len() as i64);
    rec_body.extend_from_slice(key);
    write_zigzag(&mut rec_body, value.len() as i64);
    rec_body.extend_from_slice(value);
    write_zigzag(&mut rec_body, 0); // headers count

    let mut record = BytesMut::new();
    write_zigzag(&mut record, rec_body.len() as i64);
    record.extend_from_slice(&rec_body);

    let records_bytes = record.freeze();
    let batch_length_val = (49 + records_bytes.len()) as i32;

    let mut batch = BytesMut::new();
    batch.put_i64(0); // base_offset
    batch.put_i32(batch_length_val);
    batch.put_i32(-1); // partition_leader_epoch
    batch.put_u8(2); // magic
    batch.put_u32(0); // crc
    batch.put_i16(0); // attributes
    batch.put_i32(0); // last_offset_delta
    batch.put_i64(0); // base_timestamp
    batch.put_i64(0); // max_timestamp
    batch.put_i64(-1); // producer_id
    batch.put_i16(-1); // producer_epoch
    batch.put_i32(-1); // base_sequence
    batch.put_i32(1); // records_count
    batch.extend_from_slice(&records_bytes);
    batch.to_vec()
}

fn write_zigzag(buf: &mut BytesMut, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        if v < 0x80 {
            buf.put_u8(v as u8);
            break;
        }
        buf.put_u8((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
}

async fn round_trip(stream: &mut TcpStream, frame: Vec<u8>) -> Vec<u8> {
    stream.write_all(&frame).await.unwrap();
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut len_buf))
        .await
        .expect("timeout")
        .unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
        .await
        .expect("timeout")
        .unwrap();
    body
}

fn make_shard_key(queue: &str) -> ShardKey {
    ShardKey {
        tenant_id: TenantId::new("default").unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        shard_id: ShardId::new(0),
    }
}

/// Produce v9 (flexible) with one real record — verify the log gets a BatchPush entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_v9_record_persisted_to_log() {
    let server = TestProducerServer::start_with_store(vec!["test-queue".to_string()]).await;
    let store = server.store().unwrap().clone();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    let frame = framed_produce_v9_with_record("test-queue", 1);
    let resp = round_trip(&mut stream, frame).await;

    // Verify response is well-formed (correlation_id present).
    assert!(resp.len() >= 4, "response too short: {:?}", resp);
    assert_eq!(i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]), 1);

    // Give the async persist task a moment to complete (it runs in the writer task).
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify the shard exists in the log with at least one BatchPush command.
    let shard_key = make_shard_key("test-queue");
    let page = store
        .log
        .read_from(&shard_key, None, 10)
        .await
        .expect("shard should exist after produce");

    assert!(
        !page.commands.is_empty(),
        "log should contain at least one command after produce"
    );

    // Verify the command is a BatchPush with the decoded record payload.
    use pqueue_storage::QueueCommand;
    let (_, envelope) = &page.commands[0];
    match &envelope.command {
        QueueCommand::BatchPush(cmd) => {
            assert!(
                !cmd.items.is_empty(),
                "BatchPush should have at least one item"
            );
            let item = &cmd.items[0];
            assert_eq!(
                item.payload.as_deref(),
                Some(b"v".as_ref()),
                "expected value payload b\"v\", got {:?}",
                item.payload
            );
        }
        other => panic!("expected BatchPush, got {:?}", other),
    }
}

/// Produce v3 (legacy, null records) — no push batch, so log must remain empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_v3_null_records_no_log_entry() {
    let server = TestProducerServer::start_with_store(vec!["test-queue".to_string()]).await;
    let store = server.store().unwrap().clone();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    let frame = framed_produce_v3("test-queue", 2);
    let resp = round_trip(&mut stream, frame).await;
    assert!(resp.len() >= 4);
    assert_eq!(i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]), 2);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // No items → no log entry (shard never created).
    let shard_key = make_shard_key("test-queue");
    use pqueue_storage::traits::LogStore;
    let result = store.log.read_from(&shard_key, None, 10).await;
    // Either ShardNotFound or an empty page — both are acceptable.
    match result {
        Err(_) => {} // ShardNotFound — correct
        Ok(page) => assert!(
            page.commands.is_empty(),
            "expected no commands for null-records produce"
        ),
    }
}

/// Multiple sequential produces accumulate in the log, each with a unique item_id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_produces_accumulate_in_log() {
    let server = TestProducerServer::start_with_store(vec!["q".to_string()]).await;
    let store = server.store().unwrap().clone();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    for i in 0..3i32 {
        let frame = framed_produce_v9_with_record("q", i + 10);
        let resp = round_trip(&mut stream, frame).await;
        assert!(resp.len() >= 4);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let shard_key = make_shard_key("q");
    let page = store.log.read_from(&shard_key, None, 10).await.unwrap();
    assert_eq!(
        page.commands.len(),
        3,
        "expected 3 log entries, got {}",
        page.commands.len()
    );

    // Verify all item_ids are unique — each produce must not overwrite prior items.
    use pqueue_storage::QueueCommand;
    let mut ids = std::collections::HashSet::new();
    for (_, envelope) in &page.commands {
        if let QueueCommand::BatchPush(cmd) = &envelope.command {
            for item in &cmd.items {
                assert!(
                    ids.insert(item.item_id.as_str().to_owned()),
                    "duplicate item_id across batches: {}",
                    item.item_id
                );
            }
        }
    }
    assert_eq!(
        ids.len(),
        3,
        "expected 3 unique item_ids, got {}",
        ids.len()
    );
}
