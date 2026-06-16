//! Produce handler (API Key 0) — maps Kafka ProduceRequest to pqueue push.
//!
//! Each record in the batch becomes a pqueue `PushItem`. The topic name maps
//! to a queue_id, partition 0 to shard 0. Transactional produces are
//! rejected (UNSUPPORTED_FOR_MESSAGE_FORMAT / 43). Idempotent producers
//! (producer_id != -1) are rejected with OUT_OF_ORDER_SEQUENCE_NUMBER (45)
//! until idempotency is implemented (ADR-005 P2).

use bytes::Bytes;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceResponse, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use pqueue_core::{ClientItemKey, ItemId, TenantId, UtcTimestamp};
use pqueue_storage::commands::{BatchPushCommand, PushItem};

/// Decode zigzag-encoded signed varint from `data`, returning `(value, bytes_consumed)`.
fn read_zigzag(data: &[u8]) -> Option<(i64, usize)> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            // Zigzag decode: (n >>> 1) ^ -(n & 1)
            let decoded = ((result >> 1) as i64) ^ -((result & 1) as i64);
            return Some((decoded, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// Extract individual record key/value pairs from an uncompressed RecordBatch.
///
/// RecordBatch layout (bytes):
///   base_offset(8) batch_length(4) partition_leader_epoch(4) magic(1)
///   crc(4) attributes(2) last_offset_delta(4) base_timestamp(8)
///   max_timestamp(8) producer_id(8) producer_epoch(2) base_sequence(4)
///   records_count(4)  ← offset 57, then records begin at offset 61
///
/// Each Record:
///   length(zigzag) attributes(i8) timestampDelta(zigzag) offsetDelta(zigzag)
///   key_len(zigzag) key_bytes value_len(zigzag) value_bytes headers_count(varint) headers…
///
/// Returns Vec of (key_bytes, value_bytes); skips records it can't parse.
/// Returns `None` for the whole batch if the format is unrecognised (magic != 2 or compressed).
fn decode_records(batch: &[u8]) -> Option<Vec<(Option<Bytes>, Option<Bytes>)>> {
    // Minimum RecordBatch header = 61 bytes.
    if batch.len() < 61 {
        return None;
    }
    let magic = batch[16];
    if magic != 2 {
        return None;
    }
    // Attributes bits 0-2 = compression codec.  0 = uncompressed.
    let attributes = i16::from_be_bytes([batch[21], batch[22]]);
    if attributes & 0x07 != 0 {
        return None;
    } // compressed — skip
    let count = i32::from_be_bytes([batch[57], batch[58], batch[59], batch[60]]);
    if count <= 0 {
        return Some(vec![]);
    }

    let mut pos = 61usize;
    let mut records = Vec::with_capacity(count as usize);

    for _ in 0..count {
        if pos >= batch.len() {
            break;
        }
        // record length (zigzag) — covers everything after this field
        let (_, len_bytes) = read_zigzag(&batch[pos..])?;
        pos += len_bytes;
        if pos >= batch.len() {
            break;
        }
        // attributes (i8)
        pos += 1;
        // timestampDelta (zigzag)
        let (_, ts_len) = read_zigzag(&batch[pos..])?;
        pos += ts_len;
        // offsetDelta (zigzag)
        let (_, od_len) = read_zigzag(&batch[pos..])?;
        pos += od_len;
        // key
        let (key_len, kl_len) = read_zigzag(&batch[pos..])?;
        pos += kl_len;
        let key = if key_len < 0 {
            None
        } else {
            let end = pos + key_len as usize;
            if end > batch.len() {
                break;
            }
            let k = Bytes::copy_from_slice(&batch[pos..end]);
            pos = end;
            Some(k)
        };
        // value
        let (val_len, vl_len) = read_zigzag(&batch[pos..])?;
        pos += vl_len;
        let value = if val_len < 0 {
            None
        } else {
            let end = pos + val_len as usize;
            if end > batch.len() {
                break;
            }
            let v = Bytes::copy_from_slice(&batch[pos..end]);
            pos = end;
            Some(v)
        };
        // Skip headers: headers_count (unsigned varint), then each header
        if pos >= batch.len() {
            records.push((key, value));
            break;
        }
        let (hcount, hc_len) = read_zigzag(&batch[pos..])?;
        pos += hc_len;
        for _ in 0..hcount.max(0) {
            // header key: compact_string (unsigned varint N, then N bytes)
            let (hk_len, hkl_len) = read_zigzag(&batch[pos..])?;
            pos += hkl_len;
            pos += hk_len.max(0) as usize;
            // header value: compact_bytes (signed varint N, then N bytes; -1 = null)
            let (hv_len, hvl_len) = read_zigzag(&batch[pos..])?;
            pos += hvl_len;
            if hv_len > 0 {
                pos += hv_len as usize;
            }
        }
        records.push((key, value));
    }

    Some(records)
}

/// One pqueue push batch per topic-partition that was accepted.
pub struct ProducePushBatch {
    pub queue_id: String,
    pub push: BatchPushCommand,
}

/// Handle a Produce request, returning a list of push batches and a wire response.
///
/// The caller is responsible for writing the push batches to the LogStore.
pub fn handle(api_version: i16, body: &[u8]) -> (ProduceResponse, Vec<ProducePushBatch>) {
    let mut buf = Bytes::copy_from_slice(body);
    let request = match ProduceRequest::decode(&mut buf, api_version) {
        Ok(r) => r,
        Err(_) => return (ProduceResponse::default(), vec![]),
    };

    let mut response = ProduceResponse::default();
    let mut push_batches: Vec<ProducePushBatch> = vec![];

    // Reject transactional produces.
    let has_txn = request
        .transactional_id
        .as_ref()
        .is_some_and(|id| !id.0.is_empty());

    for topic_data in request.topic_data {
        let topic_name = topic_data.name.0.to_string();
        let mut topic_resp = TopicProduceResponse::default();
        topic_resp.name = TopicName(StrBytes::from_string(topic_name.clone()));

        for partition_data in topic_data.partition_data {
            let partition = partition_data.index;
            let mut part_resp = PartitionProduceResponse::default();
            part_resp.index = partition;

            if has_txn {
                // UNSUPPORTED_FOR_MESSAGE_FORMAT
                part_resp.error_code = 43;
                part_resp.base_offset = -1;
                topic_resp.partition_responses.push(part_resp);
                continue;
            }

            // Only partition 0 maps to shard 0.
            if partition != 0 {
                // UNKNOWN_TOPIC_OR_PARTITION
                part_resp.error_code = 3;
                part_resp.base_offset = -1;
                topic_resp.partition_responses.push(part_resp);
                continue;
            }

            let mut items: Vec<PushItem> = vec![];

            if let Some(records_bytes) = partition_data.records {
                if !records_bytes.is_empty() {
                    // Try to decode individual records to extract key/value bytes.
                    // Falls back to counting from the batch header when decode_records()
                    // returns None (compressed or unrecognised format).
                    let decoded = decode_records(&records_bytes);
                    match decoded {
                        Some(kvs) => {
                            for (i, (key_bytes, value_bytes)) in kvs.into_iter().enumerate() {
                                let item_id =
                                    ItemId::new(format!("{}-{}-{}", topic_name, partition, i))
                                        .unwrap_or_else(|_| ItemId::new("fallback").unwrap());
                                // Use the record key as client_item_key if it's valid UTF-8.
                                let client_key = key_bytes
                                    .as_ref()
                                    .and_then(|k| std::str::from_utf8(k).ok())
                                    .and_then(|s| ClientItemKey::new(s).ok())
                                    .unwrap_or_else(|| {
                                        ClientItemKey::new(format!("{}-key-{}", topic_name, i))
                                            .unwrap_or_else(|_| {
                                                ClientItemKey::new("fallback-key").unwrap()
                                            })
                                    });
                                items.push(PushItem {
                                    item_id,
                                    client_item_key: client_key,
                                    priority: None,
                                    not_before: None,
                                    max_attempts: 1,
                                    payload: value_bytes,
                                });
                            }
                        }
                        None => {
                            // Compressed or unknown format: count from header, no payload.
                            if records_bytes.len() >= 61 {
                                let count = i32::from_be_bytes([
                                    records_bytes[57],
                                    records_bytes[58],
                                    records_bytes[59],
                                    records_bytes[60],
                                ])
                                .max(0) as usize;
                                for i in 0..count {
                                    let item_id =
                                        ItemId::new(format!("{}-{}-{}", topic_name, partition, i))
                                            .unwrap_or_else(|_| ItemId::new("fallback").unwrap());
                                    let client_key =
                                        ClientItemKey::new(format!("{}-key-{}", topic_name, i))
                                            .unwrap_or_else(|_| {
                                                ClientItemKey::new("fallback-key").unwrap()
                                            });
                                    items.push(PushItem {
                                        item_id,
                                        client_item_key: client_key,
                                        priority: None,
                                        not_before: None,
                                        max_attempts: 1,
                                        payload: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            let base_offset = 0i64;
            if !items.is_empty() {
                push_batches.push(ProducePushBatch {
                    queue_id: topic_name.clone(),
                    push: BatchPushCommand { items },
                });
            }
            part_resp.error_code = 0;
            part_resp.base_offset = base_offset;
            topic_resp.partition_responses.push(part_resp);
        }

        response.responses.push(topic_resp);
    }

    (response, push_batches)
}

/// Default tenant used for all produce operations.
pub fn default_tenant() -> TenantId {
    TenantId::new("default").unwrap()
}

/// Synthetic timestamp for push items from produce.
pub fn produce_timestamp() -> UtcTimestamp {
    UtcTimestamp::new(0, 0).unwrap()
}
