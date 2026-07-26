//! Manifest-authoritative segment framing and integrity checks.
//!
//! This module deliberately knows nothing about object-store keys, manifests,
//! or replay. Callers translate a manifest entry into [`ManifestIntegrity`]
//! before decode, keeping format and algorithm choices out of read paths.

use fireweed_engine::{CommandEnvelope, DurableIntegrityStage, EngineError, EngineResult};
use sha2::{Digest, Sha256};

pub(crate) const MAGIC: [u8; 4] = *b"FWSG";
pub(crate) const VERSION: u8 = 3;
pub(crate) const HEADER_LEN: usize = 4 + 1 + 8 + 8;
const TRAILER_LEN: usize = 4;

// These are decode safety bounds, not sizing targets. They are intentionally
// much larger than the production defaults while preventing hostile frames
// from driving allocation from unchecked u32 values.
pub(crate) const MAX_SEGMENT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_RECORDS: usize = 1_000_000;

pub(crate) const CRC32C_ALGORITHM: &str = "crc32c-castagnoli";
pub(crate) const SHA256_ALGORITHM: &str = "sha256";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestIntegrity {
    pub(crate) frame_crc32c: u32,
    pub(crate) content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentIntegrityError {
    pub(crate) stage: DurableIntegrityStage,
    pub(crate) manifest_index: u64,
    pub(crate) locator: String,
    detail: &'static str,
}

impl SegmentIntegrityError {
    fn new(
        stage: DurableIntegrityStage,
        manifest_index: u64,
        locator: &str,
        detail: &'static str,
    ) -> Self {
        Self {
            stage,
            manifest_index,
            locator: locator.to_owned(),
            detail,
        }
    }
}

impl std::fmt::Display for SegmentIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "segment integrity failure stage={} manifest_index={} locator={} detail={}",
            self.stage, self.manifest_index, self.locator, self.detail
        )
    }
}

impl From<SegmentIntegrityError> for EngineError {
    fn from(value: SegmentIntegrityError) -> Self {
        Self::DurableDataCorrupt {
            stage: value.stage,
            manifest_index: value.manifest_index,
            locator: value.locator,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedSegment {
    pub(crate) bytes: Vec<u8>,
    pub(crate) frame_crc32c: u32,
    pub(crate) content_sha256: String,
}

pub(crate) fn object_locator(object_key: &str) -> String {
    let digest = hex_lower(&Sha256::digest(object_key.as_bytes()));
    digest[..16].to_owned()
}

pub(crate) fn encoded_len(lengths: impl IntoIterator<Item = usize>) -> Option<usize> {
    let record_overhead = 8;
    let base = HEADER_LEN + 4 + TRAILER_LEN;
    lengths.into_iter().try_fold(base, |size, len| {
        size.checked_add(record_overhead)?.checked_add(len)
    })
}

/// Validate the complete prospective frame before admission or buffering.
/// Empty command batches are permitted here as no-op requests; `encode`
/// separately rejects an empty physical segment.
pub(crate) fn validate_write_lengths(
    lengths: impl IntoIterator<Item = usize>,
) -> EngineResult<usize> {
    let lengths = lengths.into_iter();
    let mut count = 0_usize;
    let mut object_len = HEADER_LEN + 4 + TRAILER_LEN;
    let record_overhead = 8;
    for len in lengths {
        count = count.saturating_add(1);
        if count > MAX_RECORDS || len > MAX_RECORD_BYTES || len > u32::MAX as usize {
            return Err(EngineError::RequestTooLarge {
                requested: len,
                limit: MAX_RECORD_BYTES,
            });
        }
        object_len = object_len
            .checked_add(record_overhead)
            .and_then(|size| size.checked_add(len))
            .ok_or(EngineError::RequestTooLarge {
                requested: usize::MAX,
                limit: MAX_SEGMENT_BYTES,
            })?;
        if object_len > MAX_SEGMENT_BYTES {
            return Err(EngineError::RequestTooLarge {
                requested: object_len,
                limit: MAX_SEGMENT_BYTES,
            });
        }
    }
    Ok(object_len)
}

pub(crate) fn encode(
    epoch: u64,
    first_seq: u64,
    records: &[Vec<u8>],
) -> EngineResult<EncodedSegment> {
    if records.is_empty() {
        return Err(EngineError::RequestTooLarge {
            requested: 0,
            limit: MAX_SEGMENT_BYTES,
        });
    }
    let object_len = validate_write_lengths(records.iter().map(Vec::len))?;
    let mut object = Vec::with_capacity(object_len);
    object.extend_from_slice(&MAGIC);
    object.push(VERSION);
    object.extend_from_slice(&epoch.to_le_bytes());
    object.extend_from_slice(&first_seq.to_le_bytes());
    object.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for record in records {
        object.extend_from_slice(&(record.len() as u32).to_le_bytes());
        object.extend_from_slice(&crc32c::crc32c(record).to_le_bytes());
        object.extend_from_slice(record);
    }
    let frame_crc32c = crc32c::crc32c(&object);
    object.extend_from_slice(&frame_crc32c.to_le_bytes());
    debug_assert_eq!(object.len(), object_len);
    let content_sha256 = hex_lower(&Sha256::digest(&object));
    Ok(EncodedSegment {
        bytes: object,
        frame_crc32c,
        content_sha256,
    })
}

pub(crate) fn decode(
    bytes: &[u8],
    manifest_index: u64,
    locator: &str,
    expected: &ManifestIntegrity,
) -> EngineResult<(u64, u64, Vec<CommandEnvelope>)> {
    let (epoch, first_seq, _, commands) =
        decode_range(bytes, manifest_index, locator, expected, 0, usize::MAX)?;
    Ok((epoch, first_seq, commands))
}

/// Validate the complete segment frame, but deserialize only the requested command range.
/// The object bytes are already bounded by `MAX_SEGMENT_BYTES`; this additionally prevents recovery
/// paging from allocating a resident-sized `Vec<CommandEnvelope>` for a large physical segment.
pub(crate) fn decode_range(
    bytes: &[u8],
    manifest_index: u64,
    locator: &str,
    expected: &ManifestIntegrity,
    skip: usize,
    limit: usize,
) -> EngineResult<(u64, u64, usize, Vec<CommandEnvelope>)> {
    let mut cursor =
        ValidatedSegmentCursor::new(bytes.to_vec(), manifest_index, locator, expected)?;
    cursor.skip_to(skip)?;
    let commands = cursor.take(limit)?;
    Ok((cursor.epoch(), cursor.first_seq(), cursor.count(), commands))
}

/// A fully integrity-validated segment with an incremental record cursor. The object frame is fetched and
/// hashed once; subsequent recovery pages advance through the retained bounded byte frame without rescanning
/// or re-decoding records already returned. Resident memory is bounded by `MAX_SEGMENT_BYTES` plus one page of
/// decoded commands.
#[derive(Clone)]
pub(crate) struct ValidatedSegmentCursor {
    bytes: Vec<u8>,
    manifest_index: u64,
    locator: String,
    epoch: u64,
    first_seq: u64,
    count: usize,
    records_end: usize,
    cursor: usize,
    next_record: usize,
}

impl ValidatedSegmentCursor {
    pub(crate) fn new(
        bytes: Vec<u8>,
        manifest_index: u64,
        locator: &str,
        expected: &ManifestIntegrity,
    ) -> EngineResult<Self> {
        let fail =
            |stage, detail| SegmentIntegrityError::new(stage, manifest_index, locator, detail);
        if bytes.len() > MAX_SEGMENT_BYTES {
            return Err(fail(DurableIntegrityStage::Bounds, "segment_too_large").into());
        }
        if bytes.len() < HEADER_LEN || bytes[..4] != MAGIC {
            return Err(fail(DurableIntegrityStage::Header, "bad_magic_or_short_header").into());
        }
        if bytes[4] != VERSION {
            return Err(fail(DurableIntegrityStage::Manifest, "format_version_mismatch").into());
        }
        let epoch = u64::from_le_bytes(bytes[5..13].try_into().expect("fixed header"));
        let first_seq = u64::from_le_bytes(bytes[13..21].try_into().expect("fixed header"));
        if bytes.len() < HEADER_LEN + 4 + TRAILER_LEN {
            return Err(fail(DurableIntegrityStage::Bounds, "truncated_frame").into());
        }
        let records_end = bytes.len() - TRAILER_LEN;
        let trailer = u32::from_le_bytes(bytes[records_end..].try_into().expect("trailer"));
        if trailer != expected.frame_crc32c || crc32c::crc32c(&bytes[..records_end]) != trailer {
            return Err(fail(DurableIntegrityStage::FrameCrc32c, "crc32c_mismatch").into());
        }
        if hex_lower(&Sha256::digest(&bytes)) != expected.content_sha256 {
            return Err(fail(DurableIntegrityStage::Sha256, "sha256_mismatch").into());
        }

        let mut cursor = HEADER_LEN;
        let count = read_u32(&bytes, &mut cursor, records_end, &fail)? as usize;
        let min_record_bytes = 8;
        if count > MAX_RECORDS || count > (records_end.saturating_sub(cursor) / min_record_bytes) {
            return Err(fail(DurableIntegrityStage::Bounds, "record_count_out_of_bounds").into());
        }
        // Pass one validates every offset, length, record checksum, and exact frame
        // consumption before the potentially large CommandEnvelope vector exists.
        let records_start = cursor;
        for _ in 0..count {
            let len = read_u32(&bytes, &mut cursor, records_end, &fail)? as usize;
            if len > MAX_RECORD_BYTES {
                return Err(fail(DurableIntegrityStage::Bounds, "record_too_large").into());
            }
            let expected_record_crc = read_u32(&bytes, &mut cursor, records_end, &fail)?;
            let end = cursor
                .checked_add(len)
                .filter(|end| *end <= records_end)
                .ok_or_else(|| {
                    EngineError::from(fail(DurableIntegrityStage::Bounds, "truncated_record"))
                })?;
            let record = &bytes[cursor..end];
            if crc32c::crc32c(record) != expected_record_crc {
                return Err(fail(DurableIntegrityStage::RecordCrc32c, "crc32c_mismatch").into());
            }
            cursor = end;
        }
        if cursor != records_end {
            return Err(fail(DurableIntegrityStage::Bounds, "trailing_bytes").into());
        }

        Ok(Self {
            bytes,
            manifest_index,
            locator: locator.to_owned(),
            epoch,
            first_seq,
            count,
            records_end,
            cursor: records_start,
            next_record: 0,
        })
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn first_seq(&self) -> u64 {
        self.first_seq
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn next_record(&self) -> usize {
        self.next_record
    }

    pub(crate) fn skip_to(&mut self, target: usize) -> EngineResult<()> {
        if target < self.next_record {
            return Err(EngineError::Conflict);
        }
        while self.next_record < target.min(self.count) {
            let _ = self.next(false)?;
        }
        Ok(())
    }

    pub(crate) fn take(&mut self, limit: usize) -> EngineResult<Vec<CommandEnvelope>> {
        let mut commands =
            Vec::with_capacity(limit.min(self.count.saturating_sub(self.next_record)));
        while commands.len() < limit && self.next_record < self.count {
            if let Some(command) = self.next(true)? {
                commands.push(command);
            }
        }
        Ok(commands)
    }

    fn next(&mut self, decode: bool) -> EngineResult<Option<CommandEnvelope>> {
        if self.next_record >= self.count {
            return Ok(None);
        }
        let fail = |stage, detail| {
            SegmentIntegrityError::new(stage, self.manifest_index, &self.locator, detail)
        };
        let len = read_u32(&self.bytes, &mut self.cursor, self.records_end, &fail)? as usize;
        let _ = read_u32(&self.bytes, &mut self.cursor, self.records_end, &fail)?;
        let end = self
            .cursor
            .checked_add(len)
            .filter(|end| *end <= self.records_end)
            .ok_or_else(|| {
                EngineError::from(fail(DurableIntegrityStage::Bounds, "truncated_record"))
            })?;
        let command = if decode {
            Some(
                serde_json::from_slice(&self.bytes[self.cursor..end]).map_err(|_| {
                    EngineError::from(fail(DurableIntegrityStage::Payload, "invalid_json"))
                })?,
            )
        } else {
            None
        };
        self.cursor = end;
        self.next_record += 1;
        Ok(command)
    }
}

fn read_u32(
    bytes: &[u8],
    cursor: &mut usize,
    end: usize,
    fail: &impl Fn(DurableIntegrityStage, &'static str) -> SegmentIntegrityError,
) -> EngineResult<u32> {
    let next = cursor
        .checked_add(4)
        .filter(|next| *next <= end)
        .ok_or_else(|| {
            EngineError::from(fail(DurableIntegrityStage::Bounds, "truncated_length"))
        })?;
    let value = u32::from_le_bytes(bytes[*cursor..next].try_into().expect("four bytes"));
    *cursor = next;
    Ok(value)
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn castagnoli_and_sha256_standard_vectors_are_pinned() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(
            hex_lower(&Sha256::digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn current_frame_golden_is_literal() {
        let records = vec![b"{}".to_vec(), b"[]".to_vec()];
        let encoded = encode(7, 11, &records).unwrap();
        assert_eq!(
            hex_lower(&encoded.bytes),
            "465753470307000000000000000b000000000000000200000002000000aad07b297b7d0200000076bd4d765b5de85b0807"
        );
        assert_eq!(encoded.frame_crc32c, 0x0708_5be8);
        assert_eq!(
            encoded.content_sha256,
            "fcd9404e276f954a7d00eda7dfebea2bccacdb6fcd36d55fb71607151b9d0051"
        );
    }

    #[test]
    fn representative_envelope_frames_decode() {
        let mut command =
            fireweed_conformance::envelope(fireweed_engine::QueueCommand::ResumeQueue, vec![]);
        // Keep the deterministic command identity explicit so the current frame golden
        // proves that durable frames decode byte-for-byte.
        command.command_id = fireweed_engine::CommandId::new("c");
        let record = serde_json::to_vec(&command).unwrap();
        let encoded = encode(7, 11, std::slice::from_ref(&record)).unwrap();
        let expected = ManifestIntegrity {
            frame_crc32c: encoded.frame_crc32c,
            content_sha256: encoded.content_sha256.clone(),
        };
        let decoded = decode(&encoded.bytes, 3, "fixture", &expected).unwrap().2;
        assert_eq!(serde_json::to_vec(&decoded[0]).unwrap(), record);
    }

    #[test]
    fn malformed_counts_fail_before_command_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        let trailer = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&trailer.to_le_bytes());
        let expected = ManifestIntegrity {
            frame_crc32c: trailer,
            content_sha256: hex_lower(&Sha256::digest(&bytes)),
        };
        let error = decode(&bytes, 9, "opaque", &expected).unwrap_err();
        assert!(matches!(
            error,
            EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::Bounds,
                manifest_index: 9,
                ..
            }
        ));
    }

    fn rewrite_integrity(bytes: &mut [u8]) -> ManifestIntegrity {
        let trailer_at = bytes.len() - TRAILER_LEN;
        let frame_crc32c = crc32c::crc32c(&bytes[..trailer_at]);
        bytes[trailer_at..].copy_from_slice(&frame_crc32c.to_le_bytes());
        ManifestIntegrity {
            frame_crc32c,
            content_sha256: hex_lower(&Sha256::digest(bytes)),
        }
    }

    #[test]
    fn valid_outer_integrity_still_rejects_malicious_lengths_and_counts() {
        let records = vec![b"{}".to_vec()];
        let original = encode(1, 2, &records).unwrap().bytes;
        for (offset, replacement) in [
            (HEADER_LEN, u32::MAX),
            (HEADER_LEN + 4, (MAX_RECORD_BYTES as u32).saturating_add(1)),
            (HEADER_LEN + 4, 100_u32),
        ] {
            let mut bytes = original.clone();
            bytes[offset..offset + 4].copy_from_slice(&replacement.to_le_bytes());
            let expected = rewrite_integrity(&mut bytes);
            assert!(matches!(
                decode(&bytes, 4, "opaque", &expected),
                Err(EngineError::DurableDataCorrupt {
                    stage: DurableIntegrityStage::Bounds,
                    manifest_index: 4,
                    ..
                })
            ));
        }
    }

    #[test]
    fn writable_frame_bounds_are_exact_without_allocating_payloads() {
        let exact = [
            MAX_RECORD_BYTES,
            MAX_RECORD_BYTES,
            MAX_RECORD_BYTES,
            MAX_SEGMENT_BYTES - (HEADER_LEN + 4 + TRAILER_LEN) - 4 * 8 - 3 * MAX_RECORD_BYTES,
        ];
        assert_eq!(validate_write_lengths(exact).unwrap(), MAX_SEGMENT_BYTES);
        let mut over = exact;
        over[3] += 1;
        assert!(matches!(
            validate_write_lengths(over),
            Err(EngineError::RequestTooLarge {
                limit: MAX_SEGMENT_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn retired_frame_version_is_rejected() {
        let mut bytes = encode(7, 11, &[b"{}".to_vec()]).unwrap().bytes;
        bytes[4] = 2;
        let trailer_at = bytes.len() - TRAILER_LEN;
        let frame_crc32c = crc32c::crc32c(&bytes[..trailer_at]);
        bytes[trailer_at..].copy_from_slice(&frame_crc32c.to_le_bytes());
        let expected = ManifestIntegrity {
            frame_crc32c,
            content_sha256: hex_lower(&Sha256::digest(&bytes)),
        };
        assert!(matches!(
            decode(&bytes, 3, "retired-frame", &expected),
            Err(EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::Manifest,
                ..
            })
        ));
    }
}
