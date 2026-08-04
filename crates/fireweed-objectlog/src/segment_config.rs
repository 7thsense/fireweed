//! Segment flush knobs shared by env/config and LogEngine [`FlushConfig`] mapping.
//!
//! Historical name kept for product env compatibility (`FIREWEED_SEGMENT_*`). LogEngine owns
//! co-buffering; these fields map onto [`object_log::FlushConfig`] via
//! [`crate::flush_config_from_segment`].

use fireweed_engine::{
    EngineError, EngineResult, PRODUCTION_OBJECT_LOG_MAX_BATCHES,
    PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR, validate_production_object_log_segment_shape,
};

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
    /// Retired-substrate test escape hatch. It is private so programmatic,
    /// environment, Helm, and release profiles cannot opt out of production
    /// group-commit validation.
    dev_unsafe_one_command_segments: bool,
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

    /// Validate the effective LogEngine flush shape used by production
    /// filesystem and S3 composition roots.
    pub fn validate_for_production(self) -> EngineResult<Self> {
        if self.dev_unsafe_one_command_segments {
            return Err(EngineError::Invalid(
                PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR,
            ));
        }
        validate_production_object_log_segment_shape(
            self.target_bytes,
            self.max_latency_ms,
            PRODUCTION_OBJECT_LOG_MAX_BATCHES,
        )?;
        Ok(self)
    }

    #[cfg(test)]
    fn with_dev_unsafe_one_command_segments(mut self) -> Self {
        self.dev_unsafe_one_command_segments = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_validation_rejects_only_forced_single_batch_shapes_and_private_override() {
        assert_eq!(
            SegmentConfig::new(1, 20)
                .expect("structurally valid test shape")
                .validate_for_production(),
            Err(EngineError::Invalid(
                PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR
            ))
        );
        assert_eq!(
            SegmentConfig::new(2, 1)
                .expect("neighboring group-commit shape")
                .validate_for_production(),
            SegmentConfig::new(2, 1)
        );
        assert_eq!(
            SegmentConfig::new(1_048_576, 20)
                .expect("valid production shape")
                .with_dev_unsafe_one_command_segments()
                .validate_for_production(),
            Err(EngineError::Invalid(
                PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR
            ))
        );
    }
}
