//! Segment flush knobs shared by env/config and LogEngine [`FlushConfig`] mapping.
//!
//! Historical name kept for product env compatibility (`FIREWEED_SEGMENT_*`). LogEngine owns
//! co-buffering; these fields map onto [`object_log::FlushConfig`] via
//! [`crate::flush_config_from_segment`].

use fireweed_engine::{EngineError, EngineResult};

/// Maximum writable segment / flush target (64 MiB). Bounds env validation.
pub const MAX_SEGMENT_BYTES: usize = 64 * 1024 * 1024;

/// Segment sizing controls: seal by buffered bytes and/or max latency.
///
/// Maps to crates.io `object_log::FlushConfig` (`max_bytes` / `linger`) for LogEngine products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentConfig {
    /// Seal when the buffered serialized byte size reaches this value.
    pub target_bytes: usize,
    /// Seal when the oldest buffered command's age reaches this many ms.
    pub max_latency_ms: u64,
    /// Dev/test escape hatch that allowed sealing one command per segment on the retired
    /// in-tree substrate. Ignored by LogEngine products; retained for config wire compatibility.
    pub dev_unsafe_one_command_segments: bool,
}

impl SegmentConfig {
    /// Validate (`max_latency_ms > 0`, `target_bytes > 0`). Returns the config.
    pub fn new(target_bytes: usize, max_latency_ms: u64) -> EngineResult<Self> {
        if max_latency_ms == 0 {
            return Err(EngineError::Invalid("segment_max_latency_ms must be > 0"));
        }
        if target_bytes == 0 {
            return Err(EngineError::Invalid("segment_target_bytes must be > 0"));
        }
        if target_bytes > MAX_SEGMENT_BYTES {
            return Err(EngineError::Invalid(
                "segment_target_bytes exceeds maximum writable segment size",
            ));
        }
        Ok(Self {
            target_bytes,
            max_latency_ms,
            dev_unsafe_one_command_segments: false,
        })
    }
}
