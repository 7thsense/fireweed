//! Provider-neutral validation for production object-log group-commit settings.
//!
//! This module deliberately validates only configurations that *force* every
//! non-empty producer batch to seal alone. A positive linger can still produce
//! a one-command object on a sufficiently quiet queue; that is an observed
//! workload/economics property, not a configuration shape that validation can
//! truthfully rule out.

use crate::{EngineError, EngineResult};

/// LogEngine's production ceiling for producer batches co-buffered in one object.
///
/// Keep this explicit at the Fireweed boundary instead of inheriting an
/// upstream default silently. Filesystem and S3 use the same value.
pub const PRODUCTION_OBJECT_LOG_MAX_BATCHES: usize = 10_000;

/// Stable error fingerprint for a configuration that disables group commit.
pub const PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR: &str =
    "production object-log configuration would force one object per command";

/// Reject an effective flush shape that necessarily seals the first non-empty
/// producer batch instead of allowing a second batch to co-buffer.
///
/// The three independent LogEngine triggers are the byte target, linger window,
/// and producer-batch ceiling. Their minimum/no-wait values are the exact
/// provider-neutral unsafe predicate. Larger values are structurally eligible
/// for group commit; achieved commands-per-object remains a measured workload
/// boundary.
pub fn validate_production_object_log_segment_shape(
    target_bytes: usize,
    max_latency_ms: u64,
    max_batches: usize,
) -> EngineResult<()> {
    if target_bytes <= 1 || max_latency_ms == 0 || max_batches <= 1 {
        return Err(EngineError::Invalid(
            PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_forced_single_batch_predicate_and_neighbors() {
        for (target_bytes, max_latency_ms, max_batches) in
            [(1, 20, 10_000), (1_048_576, 0, 10_000), (1_048_576, 20, 1)]
        {
            assert_eq!(
                validate_production_object_log_segment_shape(
                    target_bytes,
                    max_latency_ms,
                    max_batches,
                ),
                Err(EngineError::Invalid(
                    PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR
                ))
            );
        }

        for (target_bytes, max_latency_ms, max_batches) in [
            (2, 1, 2),
            (1_048_576, 20, PRODUCTION_OBJECT_LOG_MAX_BATCHES),
        ] {
            assert_eq!(
                validate_production_object_log_segment_shape(
                    target_bytes,
                    max_latency_ms,
                    max_batches,
                ),
                Ok(())
            );
        }
    }
}
