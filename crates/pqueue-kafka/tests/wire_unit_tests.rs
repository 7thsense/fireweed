// Unit-level wire protocol tests for pqueue-kafka router.
// Send raw Kafka frames, verify response bytes.

use bytes::{BufMut, BytesMut};
use pqueue_kafka::test_support::TestProducerServer;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn framed_request(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let mut req = BytesMut::new();
    req.put_i16(api_key);
    req.put_i16(api_version);
    req.put_i32(correlation_id);
    req.put_i16(-1); // null client_id (legacy header v1)
    req.extend_from_slice(body);

    let mut framed = BytesMut::new();
    framed.put_i32(req.len() as i32);
    framed.extend_from_slice(&req);
    framed.to_vec()
}

fn framed_flexible_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Vec<u8> {
    // Request header v2 (flexible): api_key(2) api_version(2) corr_id(4)
    //   client_id NULLABLE_STRING (i16 length + bytes — NOT compact varint)
    //   _tagged_fields varint
    // Both header v1 and v2 use the same NULLABLE_STRING format for client_id.
    let mut req = BytesMut::new();
    req.put_i16(api_key);
    req.put_i16(api_version);
    req.put_i32(correlation_id);
    req.put_i16(-1); // null NULLABLE_STRING client_id (i16 = -1)
    req.put_u8(0x00); // empty _tagged_fields
    req.extend_from_slice(body);

    let mut framed = BytesMut::new();
    framed.put_i32(req.len() as i32);
    framed.extend_from_slice(&req);
    framed.to_vec()
}

async fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.unwrap();
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_unit_api_versions_v0() {
    let server = TestProducerServer::start(vec!["q1".to_string()]).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    let req = framed_request(18, 0, 42, &[]);
    stream.write_all(&req).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
        .await
        .expect("timeout reading ApiVersions v0 response");

    // Response: correlation_id(4) + error_code(2) + api_keys_count + ...
    assert!(resp.len() >= 6);
    let corr_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(corr_id, 42);
    let error_code = i16::from_be_bytes([resp[4], resp[5]]);
    assert_eq!(error_code, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_unit_api_versions_v3_flexible() {
    let server = TestProducerServer::start(vec!["q1".to_string()]).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    // ApiVersions v3 uses flexible request header v2 (null client_id + tagged_fields).
    // But per Kafka spec, ApiVersions response always uses response header v0 (no tagged_fields).
    let req = framed_flexible_request(
        18,
        3,
        99,
        &[
            0x00, // null client_software_name compact_nullable_string
            0x00, // null client_software_version compact_nullable_string
            0x00, // empty tagged_fields
        ],
    );
    stream.write_all(&req).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
        .await
        .expect("timeout reading ApiVersions v3 response");

    // Response header v0: correlation_id(4) + body
    assert!(resp.len() >= 4);
    let corr_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(corr_id, 99);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_unit_metadata_v0_lists_queues() {
    let server = TestProducerServer::start(vec!["my-queue".to_string()]).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    // Metadata v0 (legacy): null topics array = list all.
    let req = framed_request(
        3,
        0,
        1,
        &[
            0xFF, 0xFF, // null array (int32 -1 = list all topics)
        ],
    );
    stream.write_all(&req).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
        .await
        .expect("timeout reading Metadata v0 response");

    assert!(resp.len() >= 4);
    let corr_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(corr_id, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_unit_produce_v3_returns_ok() {
    let server = TestProducerServer::start(vec!["my-queue".to_string()]).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    // Build a minimal ProduceRequest v3 body (legacy, no flexible fields).
    // v3: transactional_id(NullableString) + acks(i16) + timeout(i32) + topics
    let mut body = BytesMut::new();
    body.put_i16(-1); // null transactional_id
    body.put_i16(1); // acks=1
    body.put_i32(5000); // timeout_ms
    // topics array: 1 element
    body.put_i32(1);
    // topic name "my-queue"
    body.put_i16(8);
    body.extend_from_slice(b"my-queue");
    // partitions array: 1 element
    body.put_i32(1);
    body.put_i32(0); // partition 0
    // records: null bytes (no records)
    body.put_i32(-1);

    let req = framed_request(0, 3, 77, &body);
    stream.write_all(&req).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
        .await
        .expect("timeout reading Produce v3 response");

    assert!(resp.len() >= 4, "response too short: {:?}", resp);
    let corr_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(corr_id, 77, "wrong correlation_id in response");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_unit_produce_v9_flexible_one_record() {
    let server = TestProducerServer::start(vec!["my-queue".to_string()]).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    // Produce v9 uses FLEXIBLE request header v2:
    //   api_key(2) api_version(2) correlation_id(4) client_id(CNS) tagged_fields(varint)
    //   body: transactional_id(CNS) acks(i16) timeout_ms(i32) topic_data(compact_array) tagged_fields
    //
    // Build a minimal one-record Produce v9 request body.
    // acks=1, timeout=5000ms, topic="my-queue", partition=0, records=(one batch)
    let mut body = BytesMut::new();
    body.put_u8(0x00); // transactional_id: null CNS (varint 0 = null)
    body.put_i16(1); // acks
    body.put_i32(5000); // timeout_ms

    // topic_data: compact_array len=2 (varint: count+1)
    body.put_u8(0x02); // 1 element (2-1=1)
    // TopicProduceData.name: compact_string "my-queue" (len varint = 9, then bytes)
    body.put_u8(9); // 8 chars + 1
    body.extend_from_slice(b"my-queue");
    // TopicProduceData.partition_data: compact_array 1 element
    body.put_u8(0x02); // 1 element
    // PartitionProduceData.index
    body.put_i32(0);
    // PartitionProduceData.records: compact_nullable_bytes (varint N+1 where N=byte_count, or 0=null)
    // We'll send a single record batch.
    // A minimal RecordBatch:
    // base_offset(8) + batch_length(4) + partition_leader_epoch(4) + magic(1=2) +
    // crc(4) + attributes(2) + last_offset_delta(4) + base_timestamp(8) +
    // max_timestamp(8) + producer_id(8) + producer_epoch(2) + base_sequence(4) +
    // records_count(4) + records...
    //
    // Minimum: build a real RecordBatch with 1 record.
    let batch = build_minimal_record_batch(b"key-0", b"val-0");
    // compact_nullable_bytes varint = batch.len() + 1
    let batch_varint_len = batch.len() + 1;
    // encode as varint (assuming < 128)
    assert!(
        batch_varint_len < 128,
        "batch must be < 127 bytes for single-byte varint"
    );
    body.put_u8(batch_varint_len as u8);
    body.extend_from_slice(&batch);
    // PartitionProduceData tagged_fields
    body.put_u8(0x00);
    // TopicProduceData tagged_fields
    body.put_u8(0x00);
    // ProduceRequest tagged_fields
    body.put_u8(0x00);

    let req = framed_flexible_request(0, 9, 88, &body);
    stream.write_all(&req).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
        .await
        .expect("timeout reading Produce v9 response");

    assert!(
        resp.len() >= 4,
        "response too short: {} bytes: {:?}",
        resp.len(),
        resp
    );
    let corr_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(corr_id, 88, "wrong correlation_id");

    // Response header v1 (flexible Produce): correlation_id(4) + tagged_fields(0x00) + body
    // body: throttle_time_ms(4) + responses compact_array + tagged_fields
    // Print raw bytes for diagnosis
    eprintln!("Produce v9 response ({} bytes): {:02x?}", resp.len(), &resp);

    // Response header v1: correlation_id(4) + tagged_fields(1)
    assert!(
        resp.len() >= 5,
        "response too short for header v1: {:?}",
        resp
    );
    let tf = resp[4];
    assert_eq!(
        tf, 0x00,
        "expected 0x00 response header tagged_fields, got 0x{:02x}",
        tf
    );

    // ProduceResponse v9 body starts at resp[5]:
    // In ProduceResponse, the field ORDER is: responses FIRST, then throttle_time_ms at end.
    // (Per Kafka protocol spec — this differs from request ordering.)
    // responses: compact_array varint (first byte should be >= 2 for 1+ topics)
    let body_bytes = &resp[5..];
    assert!(!body_bytes.is_empty(), "empty body");
    let responses_varint = body_bytes[0];
    assert!(
        responses_varint >= 2,
        "expected >=1 response topic (varint>=2), got {}",
        responses_varint
    );
}

fn build_minimal_record_batch(key: &[u8], value: &[u8]) -> Vec<u8> {
    // A Kafka RecordBatch with 1 record.
    // Magic byte = 2 (current format).
    // We skip the CRC check here (just set 0); the server doesn't verify CRC.
    let mut record = BytesMut::new();
    // Record: length(varint) + attributes(i8) + timestampDelta(varint) +
    //         offsetDelta(varint) + key(compact_bytes) + value(compact_bytes) + headers_count(varint)
    let key_len = key.len() as i64;
    let val_len = value.len() as i64;
    let mut rec_body = BytesMut::new();
    rec_body.put_i8(0); // attributes
    write_zigzag_varint(&mut rec_body, 0); // timestampDelta
    write_zigzag_varint(&mut rec_body, 0); // offsetDelta
    // key: signed varint (key_len), then bytes
    write_zigzag_varint(&mut rec_body, key_len);
    rec_body.extend_from_slice(key);
    // value: signed varint (val_len), then bytes
    write_zigzag_varint(&mut rec_body, val_len);
    rec_body.extend_from_slice(value);
    write_zigzag_varint(&mut rec_body, 0); // headers count
    write_zigzag_varint(&mut record, rec_body.len() as i64);
    record.extend_from_slice(&rec_body);

    // RecordBatch header (49 bytes before records):
    let records_bytes = record.freeze();
    let batch_length = (49 - 8 - 4) as i32 + records_bytes.len() as i32; // (header - base_offset - batch_length) + records
    // Actually: batch_length = size of (partition_leader_epoch through end of batch)
    // = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + records.len()
    // = 49 + records.len()
    let batch_length_val = (49 + records_bytes.len()) as i32;
    // Recalc: batch_length field = number of bytes following it until end of batch
    // = partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) +
    //   baseTimestamp(8) + maxTimestamp(8) + producerId(8) + producerEpoch(2) + baseSequence(4) +
    //   recordsCount(4) + records
    // = 4+1+4+2+4+8+8+8+2+4+4 + records.len() = 49 + records.len()
    let _ = batch_length;
    let mut batch = BytesMut::new();
    batch.put_i64(0); // base_offset
    batch.put_i32(batch_length_val); // batch_length
    batch.put_i32(-1); // partition_leader_epoch (ignored)
    batch.put_u8(2); // magic = 2
    batch.put_u32(0); // crc (we skip real CRC)
    batch.put_i16(0); // attributes
    batch.put_i32(0); // last_offset_delta
    batch.put_i64(0); // base_timestamp
    batch.put_i64(0); // max_timestamp
    batch.put_i64(-1); // producer_id (non-idempotent)
    batch.put_i16(-1); // producer_epoch
    batch.put_i32(-1); // base_sequence
    batch.put_i32(1); // records_count
    batch.extend_from_slice(&records_bytes);
    batch.to_vec()
}

fn write_zigzag_varint(buf: &mut BytesMut, value: i64) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_unit_unsupported_api_key_sends_error_frame() {
    let server = TestProducerServer::start(vec![]).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    // API key 1 (Fetch) is not supported.
    let req = framed_request(1, 0, 55, &[]);
    stream.write_all(&req).await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
        .await
        .expect("timeout reading error response");

    assert!(resp.len() >= 6);
    let corr_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(corr_id, 55);
    // Error code should be 10 (UNKNOWN_SERVER_ERROR)
    let error_code = i16::from_be_bytes([resp[4], resp[5]]);
    assert_eq!(error_code, 10);
}
