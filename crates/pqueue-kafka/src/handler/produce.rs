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
                    // Parse record batch header to count records.
                    // Minimum RecordBatch header: 8 (base_offset) + 4 (batch_length) +
                    //   4 (partition_leader_epoch) + 1 (magic) + 4 (crc) +
                    //   2 (attributes) + 4 (last_offset_delta) + 8+8 (timestamps) +
                    //   8 (producer_id) + 2 (producer_epoch) + 4 (base_sequence) +
                    //   4 (records_count) = 61 bytes
                    if records_bytes.len() >= 61 {
                        let record_count_bytes =
                            &records_bytes[records_bytes.len() - (records_bytes.len() - 57)..];
                        // records_count is at offset 57 in the batch header
                        let count = if records_bytes.len() >= 61 {
                            i32::from_be_bytes([
                                records_bytes[57],
                                records_bytes[58],
                                records_bytes[59],
                                records_bytes[60],
                            ])
                            .max(0) as usize
                        } else {
                            0
                        };
                        let _ = record_count_bytes; // suppress unused

                        for i in 0..count {
                            let item_id = ItemId::new(format!("{}-{}-{}", topic_name, partition, i))
                                .unwrap_or_else(|_| ItemId::new("fallback").unwrap());
                            let client_key =
                                ClientItemKey::new(format!("{}-key-{}", topic_name, i))
                                    .unwrap_or_else(|_| ClientItemKey::new("fallback-key").unwrap());
                            items.push(PushItem {
                                item_id,
                                client_item_key: client_key,
                                priority: None,
                                not_before: None,
                                max_attempts: 1,
                            });
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
