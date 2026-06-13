//! Request routing and response serialization for the pqueue-kafka wire server.
//!
//! Handles both legacy (request header v1) and flexible (request header v2)
//! Kafka wire protocol headers. First-flexible-version per API:
//!   Produce (0) v9+, Metadata (3) v9+, ApiVersions (18) v3+.

use crate::handler::{api_versions, metadata, produce};
use crate::handler::metadata::BrokerMeta;
use crate::handler::produce::ProducePushBatch;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::protocol::Encodable;
use pqueue_core::{ItemId, QueueId, TenantId, UtcTimestamp};
use pqueue_storage::memory::{MemoryLogStore, MemoryProjectionStore};
use pqueue_storage::traits::{LogStore, ProjectionStore};
use pqueue_storage::types::{CommandChecksum, ShardId, ShardKey};
use pqueue_storage::{CommandEnvelope, CommandId, QueueCommand};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("unsupported api key: {0}")]
    UnsupportedApi(i16),
    #[error("request too short")]
    RequestTooShort,
    #[error("encoding error: {0}")]
    Encode(String),
    #[error("storage error: {0}")]
    Storage(String),
}

/// In-process pqueue storage backing the Kafka produce path.
///
/// Produce records are enqueued as pqueue items via `LogStore` + `ProjectionStore`.
/// Workers can then claim them through the native pqueue API.
pub struct KafkaStore {
    pub log: MemoryLogStore,
    pub projection: MemoryProjectionStore,
    next_cmd_id: AtomicU64,
}

impl KafkaStore {
    pub fn new() -> Self {
        Self {
            log: MemoryLogStore::new(),
            projection: MemoryProjectionStore::new(),
            next_cmd_id: AtomicU64::new(0),
        }
    }

    /// Persist a produce batch to the log and update the projection.
    ///
    /// Called from `run_writer` after the produce response is built but before
    /// the response bytes are flushed to the client — ensuring ack-after-store.
    pub async fn persist(&self, batches: Vec<ProducePushBatch>) -> Result<(), RouterError> {
        let tenant = TenantId::new("default").map_err(|e| RouterError::Storage(e.to_string()))?;
        let ts = UtcTimestamp::new(0, 0).map_err(|e| RouterError::Storage(e.to_string()))?;

        for mut batch in batches {
            let queue = QueueId::new(&batch.queue_id)
                .map_err(|e| RouterError::Storage(e.to_string()))?;
            let shard_key = ShardKey {
                tenant_id: tenant.clone(),
                queue_id: queue.clone(),
                shard_id: ShardId::new(0),
            };
            let cmd_id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
            // Assign globally-unique item_ids using the cmd_id counter so that
            // items from different produce calls never collide in the projection.
            for (i, item) in batch.push.items.iter_mut().enumerate() {
                item.item_id = ItemId::new(format!("kafka-{cmd_id}-{i}"))
                    .map_err(|e| RouterError::Storage(e.to_string()))?;
            }
            let item_ids = batch.push.items.iter().map(|i| i.item_id.clone()).collect();
            let envelope = CommandEnvelope {
                command_id: CommandId::new(format!("kafka-{cmd_id}")),
                request_id: None,
                tenant_id: tenant.clone(),
                queue_id: queue,
                shard_id: ShardId::new(0),
                item_ids,
                command: QueueCommand::BatchPush(batch.push),
                checksum: CommandChecksum(0),
                created_at: ts,
            };
            let result = self.log
                .append_batch(&shard_key, None, vec![envelope.clone()])
                .await
                .map_err(|e| RouterError::Storage(e.to_string()))?;
            self.projection
                .apply_committed(result.last_position, &[envelope])
                .await
                .map_err(|e| RouterError::Storage(e.to_string()))?;
        }
        Ok(())
    }
}

impl Default for KafkaStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared state accessible from each connection handler.
pub struct RouterState {
    pub queues: Vec<String>,
    pub broker: BrokerMeta,
    /// Optional pqueue storage backing. When present, produce records are
    /// durably enqueued before the Produce response is sent.
    pub store: Option<Arc<KafkaStore>>,
}

pub type SharedRouterState = Arc<RwLock<RouterState>>;

/// Whether an API/version uses the flexible (v2) request header.
fn is_flexible_request(api_key: i16, api_version: i16) -> bool {
    match api_key {
        0 => api_version >= 9,  // Produce
        3 => api_version >= 9,  // Metadata
        18 => api_version >= 3, // ApiVersions
        _ => false,
    }
}

/// Whether an API/version response needs the flexible response header (v1).
///
/// ApiVersions responses ALWAYS use response header v0 (per Kafka spec §2.5)
/// so bootstrap clients can parse them before version negotiation.
fn is_flexible_response(api_key: i16, api_version: i16) -> bool {
    if api_key == 18 {
        return false; // ApiVersions: always response header v0
    }
    match api_key {
        0 => api_version >= 9,  // Produce
        3 => api_version >= 9,  // Metadata
        _ => false,
    }
}

/// Decode a varint from `data`, returning (value, bytes_consumed).
fn parse_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// Skip the request header (client_id) and return the slice starting at the body.
///
/// Both request header v1 (legacy) and v2 (flexible) use NULLABLE_STRING (INT16 length
/// prefix) for client_id. Flexible v2 appends a TAGGED_FIELDS section after client_id.
///
/// On entry, `frame` is the full frame bytes; `header_end` is the offset after
/// the fixed fields (api_key + api_version + correlation_id = 8 bytes).
fn body_slice<'a>(
    frame: &'a [u8],
    header_end: usize,
    flexible: bool,
) -> Result<&'a [u8], RouterError> {
    if frame.len() < header_end {
        return Err(RouterError::RequestTooShort);
    }
    let rest = &frame[header_end..];

    // Both header versions use NULLABLE_STRING (INT16 length) for client_id.
    if rest.len() < 2 {
        return Ok(&[]);
    }
    let cid_len = i16::from_be_bytes([rest[0], rest[1]]);
    let cid_body_len = if cid_len < 0 { 0usize } else { cid_len as usize };
    let after_cid = 2 + cid_body_len;

    if !flexible {
        return Ok(if after_cid <= rest.len() { &rest[after_cid..] } else { &[] });
    }

    // Flexible header v2 adds _tagged_fields after client_id.
    // Each tag: key(varint) + length(varint) + data.  Usually tag_count=0.
    if rest.len() <= after_cid {
        return Ok(&[]);
    }
    let (tag_count, mut pos) = parse_varint(&rest[after_cid..]).ok_or(RouterError::RequestTooShort)?;
    pos += after_cid;
    for _ in 0..tag_count {
        // Skip tag key.
        let (_, key_len) = parse_varint(&rest[pos..]).ok_or(RouterError::RequestTooShort)?;
        pos += key_len;
        // Skip tag value (length-prefixed).
        let (val_len, vl_len) = parse_varint(&rest[pos..]).ok_or(RouterError::RequestTooShort)?;
        pos += vl_len + val_len as usize;
    }
    Ok(if pos <= rest.len() { &rest[pos..] } else { &[] })
}

/// Route one raw Kafka frame (without the 4-byte length prefix).
///
/// Returns `(framed_response, push_batches)`.  `push_batches` is non-empty only
/// for Produce requests; the caller must persist them to the `KafkaStore` before
/// flushing the response to the client (ack-after-store).
pub fn route(
    frame: &[u8],
    state: &RouterState,
) -> Result<(Bytes, Vec<ProducePushBatch>), RouterError> {
    if frame.len() < 8 {
        return Err(RouterError::RequestTooShort);
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);

    let flexible = is_flexible_request(api_key, api_version);
    let body = body_slice(frame, 8, flexible)?;

    let mut push_batches: Vec<ProducePushBatch> = vec![];
    let response_bytes = match api_key {
        18 => encode_msg(api_version, &api_versions::handle(api_version))?,
        3 => encode_msg(
            api_version,
            &metadata::handle(api_version, body, &state.queues, &state.broker),
        )?,
        0 => {
            let (resp, batches) = produce::handle(api_version, body);
            push_batches = batches;
            encode_msg(api_version, &resp)?
        }
        key => return Err(RouterError::UnsupportedApi(key)),
    };

    Ok((
        frame_response(
            correlation_id,
            response_bytes,
            is_flexible_response(api_key, api_version),
        ),
        push_batches,
    ))
}

fn encode_msg<T: Encodable>(api_version: i16, msg: &T) -> Result<Bytes, RouterError> {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf, api_version)
        .map_err(|e| RouterError::Encode(format!("{:?}", e)))?;
    Ok(buf.freeze())
}

/// Build response payload (no 4-byte length prefix).
///
/// Layout: correlation_id(4) [+ tagged_fields(1 byte, flexible only)] + body.
/// The caller (heimq-wire WireServer) prepends the length prefix before writing.
fn frame_response(correlation_id: i32, body: Bytes, flexible: bool) -> Bytes {
    let header_extra = if flexible { 1 } else { 0 };
    let payload_len = 4 + header_extra + body.len();
    let mut out = BytesMut::with_capacity(payload_len);
    out.put_i32(correlation_id);
    if flexible {
        out.put_u8(0x00); // empty tagged_fields
    }
    out.extend_from_slice(&body);
    out.freeze()
}
