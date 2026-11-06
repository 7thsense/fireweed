//! Batch data generation. Kept out of the timed region: a real caller already
//! holds the value it wants to write, so only DB work (encode/patch/SQL) is
//! measured, never this harness's own RNG cost.
use rand::distributions::Alphanumeric;
use rand::Rng;

/// A fixed-size random byte template, unique per (item, field) via a short
/// hex marker spliced into its tail. Cloning + splicing is O(size) memcpy
/// instead of O(size) RNG draws, which is what lets a 4 KiB x 200-field
/// ingest run finish in the time budget instead of being generator-bound.
pub struct ValueTemplate {
    base: String,
    size: usize,
}

impl ValueTemplate {
    pub fn new(rng: &mut impl Rng, size: usize) -> Self {
        let base: String = rng
            .sample_iter(&Alphanumeric)
            .take(size)
            .map(char::from)
            .collect();
        Self { base, size }
    }

    /// `revision` distinguishes repeated writes to the same (item, field) —
    /// the update sweep's churn pool reuses `value_for` every round, and each
    /// round must produce genuinely new bytes to exercise a real overwrite
    /// rather than a no-op content match.
    pub fn value_for(&self, item_no: i64, field_idx: usize, revision: u64) -> String {
        let marker = format!("{item_no:x}-{field_idx:x}-{revision:x}");
        let mut bytes = self.base.as_bytes().to_vec();
        let mlen = marker.len().min(bytes.len());
        let start = bytes.len() - mlen;
        bytes[start..].copy_from_slice(&marker.as_bytes()[..mlen]);
        // SAFETY: base (Alphanumeric) and marker (hex + '-') are both ASCII.
        unsafe { String::from_utf8_unchecked(bytes) }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

/// One record's worth of (field index, value) pairs, all `fields` fields set.
pub fn full_record(tpl: &ValueTemplate, item_no: i64, fields: usize) -> Vec<(usize, String)> {
    (0..fields)
        .map(|f| (f, tpl.value_for(item_no, f, 0)))
        .collect()
}

/// The field indices an update touches: `count` indices, always including
/// field 0 when `include_indexed` is set (field 0 is the spike's one
/// declared index target), otherwise strictly excluding it.
pub fn touched_fields(count: usize, total_fields: usize, include_indexed: bool) -> Vec<usize> {
    let mut out = Vec::with_capacity(count);
    if include_indexed {
        out.push(0);
    }
    let mut next = 1;
    while out.len() < count && next < total_fields {
        out.push(next);
        next += 1;
    }
    out
}
