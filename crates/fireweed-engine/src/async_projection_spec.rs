use crate::{EngineError, EngineResult};

/// Provider-neutral bounds for returning before a selected projection has applied through the
/// committed log position.
///
/// These limits describe the asynchronous response policy shared by every projection adapter.
/// `apply_start_delay_ms` defaults to `0` (SQLite/memory). Turso object-log compose may raise it
/// so apply does not unseal the object-log packer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncProjectionSpec {
    pub apply_lag_max_commands: u64,
    pub apply_debt_max_bytes: u64,
    pub apply_queue_depth_max: usize,
    pub oldest_unapplied_max_ms: u64,
    pub apply_poison_retry_threshold: u32,
    /// Milliseconds to wait after the first enqueue before the apply worker
    /// starts. `0` (default) starts immediately. Turso object-log compose sets
    /// this so apply does not unseal the packer; SQLite/memory leave it `0`.
    pub apply_start_delay_ms: u64,
}

impl Default for AsyncProjectionSpec {
    fn default() -> Self {
        Self {
            apply_lag_max_commands: 100_000,
            apply_debt_max_bytes: 512 * 1024 * 1024,
            apply_queue_depth_max: 1_024,
            oldest_unapplied_max_ms: 60_000,
            apply_poison_retry_threshold: 3,
            apply_start_delay_ms: 0,
        }
    }
}

impl AsyncProjectionSpec {
    /// Construct a validated async-projection policy. Every bound must be positive.
    pub fn new(
        apply_lag_max_commands: u64,
        apply_debt_max_bytes: u64,
        apply_queue_depth_max: usize,
        oldest_unapplied_max_ms: u64,
        apply_poison_retry_threshold: u32,
    ) -> EngineResult<Self> {
        let zero =
            |name: &str| EngineError::Storage(format!("async projection bound {name} must be > 0"));
        if apply_lag_max_commands == 0 {
            return Err(zero("apply_lag_max_commands"));
        }
        if apply_debt_max_bytes == 0 {
            return Err(zero("apply_debt_max_bytes"));
        }
        if apply_queue_depth_max == 0 {
            return Err(zero("apply_queue_depth_max"));
        }
        if oldest_unapplied_max_ms == 0 {
            return Err(zero("oldest_unapplied_max_ms"));
        }
        if apply_poison_retry_threshold == 0 {
            return Err(zero("apply_poison_retry_threshold"));
        }
        Ok(Self {
            apply_lag_max_commands,
            apply_debt_max_bytes,
            apply_queue_depth_max,
            oldest_unapplied_max_ms,
            apply_poison_retry_threshold,
            apply_start_delay_ms: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_projection_spec_preserves_legacy_defaults() {
        let spec = AsyncProjectionSpec::default();
        assert_eq!(spec.apply_lag_max_commands, 100_000);
        assert_eq!(spec.apply_debt_max_bytes, 512 * 1024 * 1024);
        assert_eq!(spec.apply_queue_depth_max, 1_024);
        assert_eq!(spec.oldest_unapplied_max_ms, 60_000);
        assert_eq!(spec.apply_poison_retry_threshold, 3);
        assert_eq!(spec.apply_start_delay_ms, 0);
    }

    #[test]
    fn async_projection_spec_rejects_each_zero_bound() {
        let valid = AsyncProjectionSpec::default();
        for candidate in [
            with(
                0,
                valid.apply_debt_max_bytes,
                valid.apply_queue_depth_max,
                valid.oldest_unapplied_max_ms,
                valid.apply_poison_retry_threshold,
            ),
            with(
                valid.apply_lag_max_commands,
                0,
                valid.apply_queue_depth_max,
                valid.oldest_unapplied_max_ms,
                valid.apply_poison_retry_threshold,
            ),
            with(
                valid.apply_lag_max_commands,
                valid.apply_debt_max_bytes,
                0,
                valid.oldest_unapplied_max_ms,
                valid.apply_poison_retry_threshold,
            ),
            with(
                valid.apply_lag_max_commands,
                valid.apply_debt_max_bytes,
                valid.apply_queue_depth_max,
                0,
                valid.apply_poison_retry_threshold,
            ),
            with(
                valid.apply_lag_max_commands,
                valid.apply_debt_max_bytes,
                valid.apply_queue_depth_max,
                valid.oldest_unapplied_max_ms,
                0,
            ),
        ] {
            assert!(candidate.is_err());
        }
    }

    fn with(
        lag: u64,
        debt: u64,
        depth: usize,
        age: u64,
        poison: u32,
    ) -> EngineResult<AsyncProjectionSpec> {
        AsyncProjectionSpec::new(lag, debt, depth, age, poison)
    }
}
