//! SPIKE-006 workload generation: the declared 100-field queue shape the task
//! specifies (~85 small, ~10 medium, ~2 large; 4 of the small fields
//! indexed), deterministic via a SplitMix64 mixer (same approach as
//! spikes/claim-scale and spikes/value-boundary, so distinct (queue, item,
//! field) triples never collide and every run is byte-reproducible).

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

// 85 small + 10 medium + 2 large = 97 declared fields, matching the task's
// "about 100 declared fields... ~85 small, ~10 medium, ~2 large" breakdown
// exactly per-category (97 rounds to "about 100").
pub const SMALL_COUNT: usize = 85; // fields 0..=84
pub const MEDIUM_COUNT: usize = 10; // fields 85..=94
pub const LARGE_COUNT: usize = 2; // fields 95..=96
pub const FIELD_COUNT: usize = SMALL_COUNT + MEDIUM_COUNT + LARGE_COUNT;

/// The 4 declared indexed fields (within 3-5 the task asks for), alternating
/// Int/Str by construction (see `field_kind`) so the index set covers both
/// declared types the contract's `IndexType` distinguishes.
pub const INDEXED_FIELDS: [usize; 4] = [0, 1, 2, 3];
/// Fixed unindexed small fields touched by the "update 1-3 small fields"
/// operation (the common-case update the task names).
pub const UPDATE_SMALL_FIELDS: [usize; 3] = [10, 20, 30];
/// The single indexed field touched by the "update one indexed field" op.
pub const UPDATE_INDEXED_FIELD: usize = 0;
/// The two large ("profile") fields; sizes fixed per field id (not
/// per-row-random) so ingest cost is predictable across reps. Both sit
/// inside the owner's stated 16-64 KiB band; the sum (64 KiB) is the
/// worst-case whole-record large-field load the task's workload describes.
pub const LARGE_FIELD_SIZES: [usize; LARGE_COUNT] = [16 * 1024, 48 * 1024];
pub const UPDATE_LARGE_FIELD: usize = 95;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    Int,
    Str,
    Bytes,
}

/// Small fields alternate Int/Str by parity (even=Int, odd=Str) so the 4
/// declared indexes exercise both types; medium fields are always Str
/// (profile-adjacent text); the 2 large fields are Bytes (opaque blobs).
pub fn field_kind(idx: usize) -> FieldKind {
    if idx < SMALL_COUNT {
        if idx % 2 == 0 {
            FieldKind::Int
        } else {
            FieldKind::Str
        }
    } else if idx < SMALL_COUNT + MEDIUM_COUNT {
        FieldKind::Str
    } else {
        FieldKind::Bytes
    }
}

pub fn is_small(idx: usize) -> bool {
    idx < SMALL_COUNT
}
pub fn is_medium(idx: usize) -> bool {
    idx >= SMALL_COUNT && idx < SMALL_COUNT + MEDIUM_COUNT
}
pub fn is_large(idx: usize) -> bool {
    idx >= SMALL_COUNT + MEDIUM_COUNT
}

/// The value carried by one field, generic over the three declared kinds.
/// Mirrors the contract's `Value` (kernel-contract-draft.md:151) restricted
/// to the three variants this workload actually needs (Int, Str, Bytes) --
/// Float/Bool/Time are not exercised, the same simplification SPIKE-003
/// disclosed for its own single-variant (`Str`) generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FieldValue {
    Int(i64),
    Str(String),
    Bytes(Vec<u8>),
}

// ---------------------------------------------------------------------------
// Deterministic generation (SplitMix64 finalizer; see spikes/claim-scale).
// ---------------------------------------------------------------------------

pub fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn seed_for(queue_no: i64, item_no: i64, field_no: usize, salt: u64) -> u64 {
    mix64(
        (queue_no as u64)
            .wrapping_mul(0x1_0000_0001)
            .wrapping_add((item_no as u64).wrapping_mul(0x9E3779B1))
            .wrapping_add((field_no as u64).wrapping_mul(0xD1B54A33))
            .wrapping_add(salt),
    )
}

fn rand_hex_string(seed: u64, len: usize) -> String {
    let mut out = String::with_capacity(len + 16);
    let mut counter: u64 = 0;
    while out.len() < len {
        let v = mix64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(counter));
        let _ = write!(out, "{v:016x}");
        counter += 1;
    }
    out.truncate(len);
    out
}

fn rand_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut counter: u64 = 0;
    while out.len() < len {
        let v = mix64(seed.wrapping_add(counter));
        out.extend_from_slice(&v.to_le_bytes());
        counter += 1;
    }
    out.truncate(len);
    out
}

/// Small Str field size: 8-32 bytes (short strings, e.g. a status code or a
/// short upstream id).
fn small_str_len(seed: u64) -> usize {
    8 + (mix64(seed) % 25) as usize
}

/// Medium field size: 256 B - 2 KiB, the task's stated band.
fn medium_len(seed: u64) -> usize {
    256 + (mix64(seed) % (2048 - 256 + 1)) as usize
}

/// The field the aggregate op counts/groups by (also `UPDATE_INDEXED_FIELD`).
/// Its value range is bounded (unlike other Int fields, which are
/// near-unique) so a range query at realistic row counts produces
/// meaningfully-populated groups instead of ~1 row/group.
const AGGREGATE_FIELD_RANGE: i64 = 2000;

pub fn gen_field(queue_no: i64, item_no: i64, field_no: usize, round: u64) -> FieldValue {
    let seed = seed_for(queue_no, item_no, field_no, 0xA5A5_0000 ^ round);
    match field_kind(field_no) {
        FieldKind::Int if field_no == UPDATE_INDEXED_FIELD => {
            FieldValue::Int((mix64(seed) % AGGREGATE_FIELD_RANGE as u64) as i64)
        }
        FieldKind::Int => FieldValue::Int((mix64(seed) as i64) & 0x7FFF_FFFF_FFFF),
        FieldKind::Str => {
            let len = if is_small(field_no) {
                small_str_len(seed)
            } else {
                medium_len(seed)
            };
            FieldValue::Str(rand_hex_string(seed, len))
        }
        FieldKind::Bytes => {
            let large_idx = field_no - (SMALL_COUNT + MEDIUM_COUNT);
            let len = LARGE_FIELD_SIZES[large_idx];
            FieldValue::Bytes(rand_bytes(seed, len))
        }
    }
}

/// The full 100-field record for one item, as (field_no, value) pairs in
/// field-id order.
pub fn gen_record(queue_no: i64, item_no: i64) -> Vec<(usize, FieldValue)> {
    (0..FIELD_COUNT).map(|f| (f, gen_field(queue_no, item_no, f, 0))).collect()
}

/// One-row insertion sequence assignment: monotonic per queue, not
/// constant, so the claim-order B-tree holds a realistic key spread (same
/// reasoning as spikes/claim-scale's `rank` assignment).
pub fn rank_for(base_ts: i64, item_no: i64) -> i64 {
    base_ts + item_no
}

/// Index-key bytes for an indexed field's value: big-endian i64 for Int (so
/// byte order matches numeric order, matching axon-esf's own ordering
/// concern for range queries), UTF-8 bytes for Str.
pub fn index_key_bytes(v: &FieldValue) -> Vec<u8> {
    match v {
        FieldValue::Int(i) => {
            // Bias to unsigned so byte-lexicographic order matches numeric
            // order across the sign boundary, standard ESF-style encoding.
            let biased = (*i as i128 - i64::MIN as i128) as u64;
            biased.to_be_bytes().to_vec()
        }
        FieldValue::Str(s) => s.as_bytes().to_vec(),
        FieldValue::Bytes(_) => panic!("large fields are never indexed in this workload"),
    }
}
