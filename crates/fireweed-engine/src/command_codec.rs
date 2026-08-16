//! Native durable command-envelope codec (product log path).
//!
//! # Design
//!
//! Core queue primitives are packed with a **non-human-readable** serde format
//! ([`postcard`]): `ItemId` is a native `u64`, timestamps are structs of integers,
//! lease tokens / keys are UTF-8 strings, and opaque payloads/fields are **raw
//! byte blobs** (see [`crate::wire_bytes`] — Base64 only for JSON).
//!
//! There is no encode/decode tax beyond layout packing for claim/finalize/push
//! metadata. A payload blob is exactly that blob; the consumer owns its meaning.
//! Client-driven typed indexes (ADR-011) still travel as native field values on
//! the entity document when present — projection keying uses axon_esf native
//! typed encodings, not a second JSON envelope around the log row.
//!
//! # Framing
//!
//! ```text
//! [ magic "FWC1" | postcard(CommandEnvelope) ]
//! ```
//!
//! # No dual-read
//!
//! The durable product log is **FWC1 only**. There is no JSON envelope path on
//! read or write. Client-controllable indexes travel as native [`TypedValue`]s
//! on the push item (`index_fields`); payload is an opaque byte blob.

use crate::command::CommandEnvelope;
use crate::error::{EngineError, EngineResult};

/// Four-byte magic for the native durable command frame (FireWeed Command v1).
pub const NATIVE_ENVELOPE_MAGIC: &[u8; 4] = b"FWC1";

/// Four-byte magic for a native object-log batch frame (epoch + envelopes).
pub const NATIVE_BATCH_MAGIC: &[u8; 4] = b"FWB1";

#[derive(serde::Serialize, serde::Deserialize)]
struct NativeLogBatch {
    backend_epoch: u64,
    commands: Vec<CommandEnvelope>,
}

/// Encode a produce payload: `FWB1` + postcard(`epoch`, envelopes).
///
/// This is the object-log hot path. Do not JSON-encode command envelopes here —
/// payloads would Base64 and the batch would be cloned into a serde Value.
pub fn encode_log_batch(backend_epoch: u64, commands: &[CommandEnvelope]) -> EngineResult<Vec<u8>> {
    let body = postcard::to_allocvec(&NativeLogBatchRef {
        backend_epoch,
        commands,
    })
    .map_err(|e| EngineError::Storage(format!("native log-batch encode failed: {e}")))?;
    let mut out = Vec::with_capacity(NATIVE_BATCH_MAGIC.len() + body.len());
    out.extend_from_slice(NATIVE_BATCH_MAGIC);
    out.extend_from_slice(&body);
    Ok(out)
}

#[derive(serde::Serialize)]
struct NativeLogBatchRef<'a> {
    backend_epoch: u64,
    commands: &'a [CommandEnvelope],
}

/// Decode a produce payload. `FWB1` is native; anything else is legacy JSON
/// `BatchFrame` (`{backend_epoch, commands}`) so existing object-log history
/// still rebuilds.
pub fn decode_log_batch(bytes: &[u8]) -> EngineResult<(u64, Vec<CommandEnvelope>)> {
    if bytes.starts_with(NATIVE_BATCH_MAGIC) {
        let body = &bytes[NATIVE_BATCH_MAGIC.len()..];
        let batch: NativeLogBatch = postcard::from_bytes(body)
            .map_err(|e| EngineError::Storage(format!("native log-batch decode failed: {e}")))?;
        return Ok((batch.backend_epoch, batch.commands));
    }
    #[derive(serde::Deserialize)]
    struct LegacyJsonBatch {
        backend_epoch: u64,
        commands: Vec<CommandEnvelope>,
    }
    let batch: LegacyJsonBatch = serde_json::from_slice(bytes)
        .map_err(|e| EngineError::Storage(format!("legacy json log-batch decode failed: {e}")))?;
    Ok((batch.backend_epoch, batch.commands))
}

/// Encode a command envelope for durable append (native binary frame).
///
/// Payload and field maps are raw bytes; core ids are native integers; typed
/// index fields are [`fireweed_core::TypedValue`]. This is the bytes form
/// retained by [`crate::LogStore::append_serialized`].
pub fn encode_command_envelope(env: &CommandEnvelope) -> EngineResult<Vec<u8>> {
    let body = postcard::to_allocvec(env)
        .map_err(|e| EngineError::Storage(format!("native command encode failed: {e}")))?;
    let mut out = Vec::with_capacity(NATIVE_ENVELOPE_MAGIC.len() + body.len());
    out.extend_from_slice(NATIVE_ENVELOPE_MAGIC);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a durable log row (FWC1 frame only).
pub fn decode_command_envelope(bytes: &[u8]) -> EngineResult<CommandEnvelope> {
    if !bytes.starts_with(NATIVE_ENVELOPE_MAGIC) {
        return Err(EngineError::Storage(
            "command envelope is not FWC1 (legacy JSON log rows are not supported)".into(),
        ));
    }
    let body = &bytes[NATIVE_ENVELOPE_MAGIC.len()..];
    let mut de = postcard::Deserializer::from_bytes(body);
    serde_path_to_error::deserialize(&mut de).map_err(|e| {
        EngineError::Storage(format!(
            "native command decode failed at `{}`: {}",
            e.path(),
            e.inner()
        ))
    })
}

/// True when `bytes` use the native durable frame.
pub fn is_native_envelope(bytes: &[u8]) -> bool {
    bytes.starts_with(NATIVE_ENVELOPE_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{
        ClaimCommand, CommandChecksum, CommandId, FinalizeCommand, FinalizeKind, FinalizeOutcome,
        PushCommand, PushItem, QueueCommand,
    };
    use bytes::Bytes;
    use fireweed_core::{ClientItemKey, ItemId, LeaseToken, Metadata, UtcTimestamp};
    use std::collections::BTreeMap;

    fn push_env(payload_len: usize, with_indexes: bool) -> CommandEnvelope {
        let item_id = ItemId::mint(1, 0, 42);
        let index_fields = if with_indexes {
            (0..19)
                .map(|i| {
                    (
                        format!("f{i}"),
                        fireweed_core::TypedValue::String(if i == 0 {
                            "k-1".into()
                        } else {
                            format!("v{i}-1")
                        }),
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        CommandEnvelope {
            command_id: CommandId::new("cmd-1"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: ClientItemKey::new(item_id.to_string()).unwrap(),
                    item_id,
                    priority: None,
                    not_before: None,
                    group_key: None,
                    max_attempts: 3,
                    payload: Some(Bytes::from(vec![b'x'; payload_len])),
                    fields: BTreeMap::new(),
                    metadata: Metadata::default(),
                    cohort_size: None,
                    gate_keys: vec![],
                    index_fields,
                    entity_document: None,
                }],
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(0, 0).unwrap(),
        }
    }

    fn claim_env(n: usize) -> CommandEnvelope {
        let ids: Vec<ItemId> = (0..n as u32).map(|i| ItemId::mint(1, 0, i)).collect();
        CommandEnvelope {
            command_id: CommandId::new("claim"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: ids.clone(),
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: ids,
                lease_token: LeaseToken::new("lease-abc").unwrap(),
                lease_expires_at: UtcTimestamp::new(60, 0).unwrap(),
                worker_id: None,
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(0, 0).unwrap(),
        }
    }

    fn finalize_env(n: usize) -> CommandEnvelope {
        let outcomes: Vec<FinalizeOutcome> = (0..n as u32)
            .map(|i| FinalizeOutcome::new(ItemId::mint(1, 0, i), FinalizeKind::Complete))
            .collect();
        let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
        CommandEnvelope {
            command_id: CommandId::new("fin"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: ids,
            command: QueueCommand::Finalize(FinalizeCommand { outcomes }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(0, 0).unwrap(),
        }
    }

    #[test]
    fn native_round_trip_push_with_blob_payload() {
        let env = push_env(2300, true);
        let bytes = encode_command_envelope(&env).unwrap();
        assert!(is_native_envelope(&bytes));
        let back = decode_command_envelope(&bytes).unwrap();
        // Re-encode both via JSON for structural compare (no PartialEq on tree).
        let a = serde_json::to_value(&env).unwrap();
        let b = serde_json::to_value(&back).unwrap();
        assert_eq!(a, b);
        // Payload must appear as raw length in binary body, not Base64 expansion.
        let QueueCommand::Push(p) = &back.command else {
            panic!("expected push");
        };
        assert_eq!(p.items[0].payload.as_ref().map(|b| b.len()), Some(2300));
    }

    #[test]
    fn legacy_json_is_rejected() {
        let env = claim_env(3);
        let json = serde_json::to_vec(&env).unwrap();
        assert!(!is_native_envelope(&json));
        assert!(decode_command_envelope(&json).is_err());
    }

    #[test]
    fn claim_and_finalize_are_compact_native() {
        let claim_env = claim_env(500);
        let fin_env = finalize_env(500);
        let claim = encode_command_envelope(&claim_env).unwrap();
        let fin = encode_command_envelope(&fin_env).unwrap();
        let claim_json = serde_json::to_vec(&claim_env).unwrap();
        let fin_json = serde_json::to_vec(&fin_env).unwrap();
        // Native should beat JSON substantially (ItemId u64 vs decimal string, no object keys).
        assert!(
            claim.len() * 2 < claim_json.len(),
            "claim native {} vs json {}",
            claim.len(),
            claim_json.len()
        );
        assert!(
            fin.len() * 2 < fin_json.len(),
            "finalize native {} vs json {}",
            fin.len(),
            fin_json.len()
        );
        // Round-trip: core primitives decode with zero JSON.
        let claim_back = decode_command_envelope(&claim).unwrap();
        let fin_back = decode_command_envelope(&fin).unwrap();
        assert_eq!(
            serde_json::to_value(&claim_env).unwrap(),
            serde_json::to_value(&claim_back).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&fin_env).unwrap(),
            serde_json::to_value(&fin_back).unwrap()
        );
    }

    #[test]
    fn snorri_shaped_push_beats_json_base64_on_payload() {
        let env = push_env(2300, true);
        let native = encode_command_envelope(&env).unwrap();
        let json = serde_json::to_vec(&env).unwrap();
        // ~2.3 KB payload alone is ~3067 Base64 chars in JSON; native keeps 2300 raw.
        assert!(
            native.len() < json.len(),
            "native {} should be < json {}",
            native.len(),
            json.len()
        );
        // Payload contribution: native body must not pay the ~1.33× Base64 tax on 2300 bytes.
        let expansion_saved_floor = 2300 / 4; // ~575 bytes Base64 overhead alone
        assert!(
            (json.len() - native.len()) >= expansion_saved_floor,
            "expected at least payload Base64 savings: json={} native={}",
            json.len(),
            native.len()
        );
        eprintln!(
            "encode size snorri-shaped push: native={} json={} saved={} ({:.0}%)",
            native.len(),
            json.len(),
            json.len() - native.len(),
            100.0 * (1.0 - native.len() as f64 / json.len() as f64)
        );
        let claim = encode_command_envelope(&claim_env(500)).unwrap();
        let claim_json = serde_json::to_vec(&claim_env(500)).unwrap();
        eprintln!(
            "encode size claim×500: native={} json={} saved={} ({:.0}%)",
            claim.len(),
            claim_json.len(),
            claim_json.len() - claim.len(),
            100.0 * (1.0 - claim.len() as f64 / claim_json.len() as f64)
        );
    }

    #[test]
    fn item_id_is_u64_on_native_path() {
        let id = ItemId::mint(7, 3, 99);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{}\"", id.as_u64()));
        let bin = postcard::to_allocvec(&id).unwrap();
        assert_eq!(postcard::from_bytes::<ItemId>(&bin).unwrap(), id);
        // Packed u64 varint is a few bytes, not a decimal string.
        assert!(bin.len() <= 10, "postcard ItemId len {}", bin.len());
    }

    #[test]
    fn log_batch_native_round_trips_and_legacy_json_still_decodes() {
        let commands = vec![claim_env(2), finalize_env(2)];
        let native = encode_log_batch(7, &commands).unwrap();
        assert!(native.starts_with(NATIVE_BATCH_MAGIC));
        let (epoch, back) = decode_log_batch(&native).unwrap();
        assert_eq!(epoch, 7);
        assert_eq!(back.len(), 2);

        let json = serde_json::json!({
            "backend_epoch": 3,
            "commands": commands,
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let (epoch, back) = decode_log_batch(&json_bytes).unwrap();
        assert_eq!(epoch, 3);
        assert_eq!(back.len(), 2);
        assert!(
            native.len() < json_bytes.len(),
            "native batch {} should beat json {}",
            native.len(),
            json_bytes.len()
        );
    }
}
