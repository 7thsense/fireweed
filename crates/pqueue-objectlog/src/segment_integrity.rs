//! Versioned, manifest-authoritative segment framing and integrity checks.
//!
//! This module deliberately knows nothing about object-store keys, manifests,
//! or replay. Callers translate a manifest entry into [`ManifestIntegrity`]
//! before decode, keeping format and algorithm choices out of read paths.

use pqueue_engine::{CommandEnvelope, DurableIntegrityStage, EngineError, EngineResult};
use sha2::{Digest, Sha256};

pub(crate) const MAGIC: [u8; 4] = *b"PQSG";
pub(crate) const V2: u8 = 2;
pub(crate) const V3: u8 = 3;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterFormat {
    V2,
    V3,
}

impl WriterFormat {
    pub(crate) const fn version(self) -> u8 {
        match self {
            Self::V2 => V2,
            Self::V3 => V3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManifestIntegrity {
    V2 {
        checksum_fnv1a: u64,
    },
    V3 {
        frame_crc32c: u32,
        content_sha256: String,
    },
}

impl ManifestIntegrity {
    const fn version(&self) -> u8 {
        match self {
            Self::V2 { .. } => V2,
            Self::V3 { .. } => V3,
        }
    }
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
    pub(crate) legacy_checksum: u64,
    pub(crate) frame_crc32c: Option<u32>,
    pub(crate) content_sha256: Option<String>,
}

pub(crate) fn object_locator(object_key: &str) -> String {
    let digest = hex_lower(&Sha256::digest(object_key.as_bytes()));
    digest[..16].to_owned()
}

pub(crate) fn encoded_len(
    format: WriterFormat,
    lengths: impl IntoIterator<Item = usize>,
) -> Option<usize> {
    let record_overhead = match format {
        WriterFormat::V2 => 4,
        WriterFormat::V3 => 8,
    };
    let base = HEADER_LEN + 4 + usize::from(format == WriterFormat::V3) * TRAILER_LEN;
    lengths.into_iter().try_fold(base, |size, len| {
        size.checked_add(record_overhead)?.checked_add(len)
    })
}

/// Validate the complete prospective frame before admission or buffering.
/// Empty command batches are permitted here as no-op requests; `encode`
/// separately rejects an empty physical segment.
pub(crate) fn validate_write_lengths(
    format: WriterFormat,
    lengths: impl IntoIterator<Item = usize>,
) -> EngineResult<usize> {
    let lengths = lengths.into_iter();
    let mut count = 0_usize;
    let mut object_len = HEADER_LEN + 4 + usize::from(format == WriterFormat::V3) * TRAILER_LEN;
    let record_overhead = if format == WriterFormat::V3 { 8 } else { 4 };
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
    format: WriterFormat,
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
    let object_len = validate_write_lengths(format, records.iter().map(Vec::len))?;
    let mut object = Vec::with_capacity(object_len);
    object.extend_from_slice(&MAGIC);
    object.push(format.version());
    object.extend_from_slice(&epoch.to_le_bytes());
    object.extend_from_slice(&first_seq.to_le_bytes());
    let records_start = object.len();
    object.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for record in records {
        object.extend_from_slice(&(record.len() as u32).to_le_bytes());
        if format == WriterFormat::V3 {
            object.extend_from_slice(&crc32c::crc32c(record).to_le_bytes());
        }
        object.extend_from_slice(record);
    }
    let legacy_checksum = if format == WriterFormat::V2 {
        fnv1a(&object[records_start..])
    } else {
        0
    };
    let frame_crc32c = if format == WriterFormat::V3 {
        let checksum = crc32c::crc32c(&object);
        object.extend_from_slice(&checksum.to_le_bytes());
        Some(checksum)
    } else {
        None
    };
    debug_assert_eq!(object.len(), object_len);
    let content_sha256 = (format == WriterFormat::V3).then(|| hex_lower(&Sha256::digest(&object)));
    Ok(EncodedSegment {
        bytes: object,
        legacy_checksum,
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
    let fail = |stage, detail| SegmentIntegrityError::new(stage, manifest_index, locator, detail);
    if bytes.len() > MAX_SEGMENT_BYTES {
        return Err(fail(DurableIntegrityStage::Bounds, "segment_too_large").into());
    }
    if bytes.len() < HEADER_LEN || bytes[..4] != MAGIC {
        return Err(fail(DurableIntegrityStage::Header, "bad_magic_or_short_header").into());
    }
    if bytes[4] != expected.version() {
        return Err(fail(DurableIntegrityStage::Manifest, "format_version_mismatch").into());
    }
    let epoch = u64::from_le_bytes(bytes[5..13].try_into().expect("fixed header"));
    let first_seq = u64::from_le_bytes(bytes[13..21].try_into().expect("fixed header"));
    let records_end = match expected {
        ManifestIntegrity::V2 { checksum_fnv1a } => {
            let blob = &bytes[HEADER_LEN..];
            if fnv1a(blob) != *checksum_fnv1a {
                return Err(fail(DurableIntegrityStage::LegacyFnv1a, "fnv1a_mismatch").into());
            }
            bytes.len()
        }
        ManifestIntegrity::V3 {
            frame_crc32c,
            content_sha256,
        } => {
            if bytes.len() < HEADER_LEN + 4 + TRAILER_LEN {
                return Err(fail(DurableIntegrityStage::Bounds, "truncated_v3_frame").into());
            }
            let trailer_at = bytes.len() - TRAILER_LEN;
            let trailer = u32::from_le_bytes(bytes[trailer_at..].try_into().expect("trailer"));
            if trailer != *frame_crc32c || crc32c::crc32c(&bytes[..trailer_at]) != trailer {
                return Err(fail(DurableIntegrityStage::FrameCrc32c, "crc32c_mismatch").into());
            }
            if hex_lower(&Sha256::digest(bytes)) != *content_sha256 {
                return Err(fail(DurableIntegrityStage::Sha256, "sha256_mismatch").into());
            }
            trailer_at
        }
    };

    let mut cursor = HEADER_LEN;
    let count = read_u32(bytes, &mut cursor, records_end, &fail)? as usize;
    let min_record_bytes = if matches!(expected, ManifestIntegrity::V3 { .. }) {
        8
    } else {
        4
    };
    if count > MAX_RECORDS || count > (records_end.saturating_sub(cursor) / min_record_bytes) {
        return Err(fail(DurableIntegrityStage::Bounds, "record_count_out_of_bounds").into());
    }
    // Pass one validates every offset, length, record checksum, and exact frame
    // consumption before the potentially large CommandEnvelope vector exists.
    let records_start = cursor;
    for _ in 0..count {
        let len = read_u32(bytes, &mut cursor, records_end, &fail)? as usize;
        if len > MAX_RECORD_BYTES {
            return Err(fail(DurableIntegrityStage::Bounds, "record_too_large").into());
        }
        let expected_record_crc = if matches!(expected, ManifestIntegrity::V3 { .. }) {
            Some(read_u32(bytes, &mut cursor, records_end, &fail)?)
        } else {
            None
        };
        let end = cursor
            .checked_add(len)
            .filter(|end| *end <= records_end)
            .ok_or_else(|| {
                EngineError::from(fail(DurableIntegrityStage::Bounds, "truncated_record"))
            })?;
        let record = &bytes[cursor..end];
        if expected_record_crc.is_some_and(|crc| crc32c::crc32c(record) != crc) {
            return Err(fail(DurableIntegrityStage::RecordCrc32c, "crc32c_mismatch").into());
        }
        cursor = end;
    }
    if cursor != records_end {
        return Err(fail(DurableIntegrityStage::Bounds, "trailing_bytes").into());
    }

    // Pass two performs structured decode only after the complete frame is
    // proven bounded. GET has already materialized the object; these bounds
    // prevent secondary allocations from attacker-controlled length fields.
    cursor = records_start;
    let selected = count.saturating_sub(skip).min(limit);
    let mut commands = Vec::with_capacity(selected);
    for index in 0..count {
        let len = read_u32(bytes, &mut cursor, records_end, &fail)? as usize;
        if matches!(expected, ManifestIntegrity::V3 { .. }) {
            let _ = read_u32(bytes, &mut cursor, records_end, &fail)?;
        }
        let end = cursor + len;
        if index >= skip && commands.len() < selected {
            let command = serde_json::from_slice(&bytes[cursor..end]).map_err(|_| {
                EngineError::from(fail(DurableIntegrityStage::Payload, "invalid_json"))
            })?;
            commands.push(command);
        }
        cursor = end;
    }
    Ok((epoch, first_seq, count, commands))
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

pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn castagnoli_and_sha256_standard_vectors_are_pinned() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(
            hex_lower(&Sha256::digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn v2_and_v3_frame_goldens_are_literal() {
        let records = vec![b"{}".to_vec(), b"[]".to_vec()];
        let v2 = encode(WriterFormat::V2, 7, 11, &records).unwrap();
        let v3 = encode(WriterFormat::V3, 7, 11, &records).unwrap();
        assert_eq!(
            hex_lower(&v2.bytes),
            "505153470207000000000000000b0000000000000002000000020000007b7d020000005b5d"
        );
        assert_eq!(
            hex_lower(&v3.bytes),
            "505153470307000000000000000b000000000000000200000002000000aad07b297b7d0200000076bd4d765b5dfa25e5d4"
        );
        assert_eq!(v2.legacy_checksum, 0xb828_4cc9_e212_925b);
        assert_eq!(v3.frame_crc32c, Some(0xd4e5_25fa));
        assert_eq!(
            v3.content_sha256.unwrap(),
            "a6d8f2ac8c9bd6b29959ee6e2b689941695d9405b8e15df83823dafc5a3444f9"
        );
    }

    #[test]
    fn historical_valid_envelope_frame_goldens_decode() {
        let command =
            pqueue_conformance::envelope(pqueue_engine::QueueCommand::ResumeQueue, vec![]);
        let record = serde_json::to_vec(&command).unwrap();
        let v2 = decode_hex(
            "505153470207000000000000000b0000000000000001000000b40000007b22636f6d6d616e645f6964223a2263222c22726571756573745f6964223a6e756c6c2c22726571756573745f66696e6765727072696e74223a6e756c6c2c22726571756573745f6f7574636f6d65223a6e756c6c2c226974656d5f696473223a5b5d2c22636f6d6d616e64223a22526573756d655175657565222c22636865636b73756d223a302c22637265617465645f6174223a7b227365636f6e6473223a302c226e616e6f7365636f6e6473223a307d7d",
        );
        let v3 = decode_hex(
            "505153470307000000000000000b0000000000000001000000b4000000fc2826b17b22636f6d6d616e645f6964223a2263222c22726571756573745f6964223a6e756c6c2c22726571756573745f66696e6765727072696e74223a6e756c6c2c22726571756573745f6f7574636f6d65223a6e756c6c2c226974656d5f696473223a5b5d2c22636f6d6d616e64223a22526573756d655175657565222c22636865636b73756d223a302c22637265617465645f6174223a7b227365636f6e6473223a302c226e616e6f7365636f6e6473223a307d7d5ab57384",
        );
        let v2_expected = ManifestIntegrity::V2 {
            checksum_fnv1a: 0x7713_fa77_8126_97e2,
        };
        let v3_expected = ManifestIntegrity::V3 {
            frame_crc32c: 0x8473_b55a,
            content_sha256: "a2170d1f565d17ae623a7bc3f669a28c9818ab8c478735ab01c97bac2b5e4c63"
                .to_owned(),
        };
        let decoded_v2 = decode(&v2, 3, "fixture", &v2_expected).unwrap().2;
        let decoded_v3 = decode(&v3, 3, "fixture", &v3_expected).unwrap().2;
        assert_eq!(serde_json::to_vec(&decoded_v2[0]).unwrap(), record);
        assert_eq!(serde_json::to_vec(&decoded_v3[0]).unwrap(), record);
    }

    #[test]
    fn malformed_counts_fail_before_command_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.push(V2);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        let expected = ManifestIntegrity::V2 {
            checksum_fnv1a: fnv1a(&bytes[HEADER_LEN..]),
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

    fn rewrite_v3_integrity(bytes: &mut [u8]) -> ManifestIntegrity {
        let trailer_at = bytes.len() - TRAILER_LEN;
        let frame_crc32c = crc32c::crc32c(&bytes[..trailer_at]);
        bytes[trailer_at..].copy_from_slice(&frame_crc32c.to_le_bytes());
        ManifestIntegrity::V3 {
            frame_crc32c,
            content_sha256: hex_lower(&Sha256::digest(bytes)),
        }
    }

    #[test]
    fn valid_outer_integrity_still_rejects_malicious_v3_lengths_and_counts() {
        let records = vec![b"{}".to_vec()];
        let original = encode(WriterFormat::V3, 1, 2, &records).unwrap().bytes;
        for (offset, replacement) in [
            (HEADER_LEN, u32::MAX),
            (HEADER_LEN + 4, (MAX_RECORD_BYTES as u32).saturating_add(1)),
            (HEADER_LEN + 4, 100_u32),
        ] {
            let mut bytes = original.clone();
            bytes[offset..offset + 4].copy_from_slice(&replacement.to_le_bytes());
            let expected = rewrite_v3_integrity(&mut bytes);
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
        assert_eq!(
            validate_write_lengths(WriterFormat::V3, exact).unwrap(),
            MAX_SEGMENT_BYTES
        );
        let mut over = exact;
        over[3] += 1;
        assert!(matches!(
            validate_write_lengths(WriterFormat::V3, over),
            Err(EngineError::RequestTooLarge {
                limit: MAX_SEGMENT_BYTES,
                ..
            })
        ));
    }

    #[test]
    #[ignore = "manual SP-07 v2/v3 encode/decode overhead benchmark; compare interleaved same-run profiles"]
    fn segment_v2_v3_integrity_overhead_manual_benchmark() {
        use std::hint::black_box;
        use std::time::Instant;

        let command =
            pqueue_conformance::envelope(pqueue_engine::QueueCommand::ResumeQueue, vec![]);
        let record = serde_json::to_vec(&command).unwrap();
        let records = vec![record; 256];
        let v2_len = encoded_len(WriterFormat::V2, records.iter().map(Vec::len)).unwrap();
        let v3_len = encoded_len(WriterFormat::V3, records.iter().map(Vec::len)).unwrap();
        assert_eq!(v3_len - v2_len, records.len() * 4 + TRAILER_LEN);

        let iterations = 1_000;
        for format in [WriterFormat::V2, WriterFormat::V3] {
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(encode(format, 7, 11, black_box(&records)).unwrap());
            }
            eprintln!(
                "SP-07 segment integrity manual benchmark format={format:?} records={} iterations={iterations} elapsed={:?}",
                records.len(),
                started.elapsed()
            );
        }

        let mixed = [
            WriterFormat::V2,
            WriterFormat::V3,
            WriterFormat::V2,
            WriterFormat::V3,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, format)| {
            let encoded = encode(
                format,
                7,
                11 + index as u64 * records.len() as u64,
                &records,
            )
            .unwrap();
            let expected = match format {
                WriterFormat::V2 => ManifestIntegrity::V2 {
                    checksum_fnv1a: encoded.legacy_checksum,
                },
                WriterFormat::V3 => ManifestIntegrity::V3 {
                    frame_crc32c: encoded.frame_crc32c.unwrap(),
                    content_sha256: encoded.content_sha256.clone().unwrap(),
                },
            };
            (encoded.bytes, expected)
        })
        .collect::<Vec<_>>();
        let started = Instant::now();
        for _ in 0..iterations {
            for (index, (bytes, expected)) in mixed.iter().enumerate() {
                let (_, first_seq, decoded) =
                    decode(black_box(bytes), index as u64, "benchmark", expected).unwrap();
                assert_eq!(first_seq, 11 + index as u64 * records.len() as u64);
                assert_eq!(decoded.len(), records.len());
                black_box(decoded);
            }
        }
        eprintln!(
            "SP-07 segment integrity manual mixed-replay benchmark segments={} records_per_segment={} iterations={iterations} elapsed={:?}",
            mixed.len(),
            records.len(),
            started.elapsed()
        );
    }
}
