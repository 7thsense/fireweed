//! Request routing and response serialization for the pqueue-kafka wire server.
//!
//! Handles both legacy (request header v1) and flexible (request header v2)
//! Kafka wire protocol headers. First-flexible-version per API:
//!   Produce (0) v9+, Metadata (3) v9+, ApiVersions (18) v3+.

use crate::handler::{api_versions, metadata, produce};
use crate::handler::metadata::BrokerMeta;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::protocol::Encodable;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("unsupported api key: {0}")]
    UnsupportedApi(i16),
    #[error("request too short")]
    RequestTooShort,
    #[error("encoding error: {0}")]
    Encode(String),
}

/// Shared state accessible from each connection handler.
pub struct RouterState {
    pub queues: Vec<String>,
    pub broker: BrokerMeta,
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
/// Returns a fully framed response (4-byte length + correlation_id + [tagged_fields] + body).
pub fn route(frame: &[u8], state: &RouterState) -> Result<Bytes, RouterError> {
    if frame.len() < 8 {
        return Err(RouterError::RequestTooShort);
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);

    let flexible = is_flexible_request(api_key, api_version);
    let body = body_slice(frame, 8, flexible)?;


    let response_bytes = match api_key {
        18 => encode_msg(api_version, &api_versions::handle(api_version))?,
        3 => encode_msg(
            api_version,
            &metadata::handle(api_version, body, &state.queues, &state.broker),
        )?,
        0 => {
            let (resp, _) = produce::handle(api_version, body);
            encode_msg(api_version, &resp)?
        }
        key => return Err(RouterError::UnsupportedApi(key)),
    };

    Ok(frame_response(
        correlation_id,
        response_bytes,
        is_flexible_response(api_key, api_version),
    ))
}

fn encode_msg<T: Encodable>(api_version: i16, msg: &T) -> Result<Bytes, RouterError> {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf, api_version)
        .map_err(|e| RouterError::Encode(format!("{:?}", e)))?;
    Ok(buf.freeze())
}

fn frame_response(correlation_id: i32, body: Bytes, flexible: bool) -> Bytes {
    // payload = correlation_id(4) [+ tagged_fields(1)] + body
    let header_extra = if flexible { 1 } else { 0 };
    let payload_len = 4 + header_extra + body.len();
    let mut out = BytesMut::with_capacity(4 + payload_len);
    out.put_i32(payload_len as i32);
    out.put_i32(correlation_id);
    if flexible {
        out.put_u8(0x00); // empty tagged_fields
    }
    out.extend_from_slice(&body);
    out.freeze()
}
