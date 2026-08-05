//! Bounded assignment-epoch resolution (P7N).
//!
//! Product data-plane writes stamp a positively-acquired assignment epoch. Callers resolve
//! "current" (and optional CAS expected) epochs through one pure fence helper so every log
//! family (memory, sqlite, postgres, filesystem, s3) shares identical EpochFenced /
//! epoch-0 rejection semantics.
//!
//! P14 replaces remaining ad-hoc async block_on epoch lookups with this helper's
//! sync-bounded path; this module only authors the resolver contract and pure fence.

use crate::error::{EngineError, EngineResult};

/// Minimum legal assignment epoch for a data-plane commit stamp.
///
/// Control-plane genesis uses epoch 0 ("never granted"). A write path must not
/// stamp epoch 0: that would defeat fencing against a subsequent positive acquire.
pub const MIN_ASSIGNMENT_EPOCH: u64 = 1;

/// Pure fence: resolve the epoch a write should stamp.
///
/// * When `expected` is `Some(e)`, require `e == current` else [`EngineError::EpochFenced`].
/// * When the resolved epoch is `0`, return [`EngineError::Invalid`] with a stable
///   message so callers never silently commit under genesis epoch-0.
///
/// This function performs no I/O. Callers supply `current` from a prior
/// `LogStore::current_epoch` / `AsyncLogStore::current_epoch` read (sync or async).
pub fn resolve_bounded_epoch(current: u64, expected: Option<u64>) -> EngineResult<u64> {
    if expected.is_some_and(|e| e != current) {
        return Err(EngineError::EpochFenced);
    }
    if current < MIN_ASSIGNMENT_EPOCH {
        return Err(EngineError::Invalid(
            "assignment epoch 0 is not valid for data-plane writes; acquire a positive epoch first",
        ));
    }
    Ok(current)
}

/// Same fence without the epoch-0 rejection — for read-only / diagnostic paths that
/// observe genesis state. Product write planners must use [`resolve_bounded_epoch`].
pub fn resolve_epoch_fence(current: u64, expected: Option<u64>) -> EngineResult<u64> {
    if expected.is_some_and(|e| e != current) {
        return Err(EngineError::EpochFenced);
    }
    Ok(current)
}

/// Sync adapter: read `current` via a fallible closure, then apply
/// [`resolve_bounded_epoch`]. Used by blocking / offloaded product paths so they
/// share the same fence as the async planners without nesting runtimes.
pub fn resolve_bounded_epoch_sync<F>(expected: Option<u64>, read_current: F) -> EngineResult<u64>
where
    F: FnOnce() -> EngineResult<u64>,
{
    let current = read_current()?;
    resolve_bounded_epoch(current, expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_matches_expected_current() {
        assert_eq!(resolve_bounded_epoch(3, Some(3)).unwrap(), 3);
        assert_eq!(resolve_bounded_epoch(3, None).unwrap(), 3);
    }

    #[test]
    fn fence_rejects_mismatched_expected() {
        assert!(matches!(
            resolve_bounded_epoch(3, Some(2)),
            Err(EngineError::EpochFenced)
        ));
    }

    #[test]
    fn write_path_rejects_epoch_zero() {
        assert!(matches!(
            resolve_bounded_epoch(0, None),
            Err(EngineError::Invalid(_))
        ));
        assert!(matches!(
            resolve_bounded_epoch(0, Some(0)),
            Err(EngineError::Invalid(_))
        ));
    }

    #[test]
    fn diagnostic_fence_allows_epoch_zero() {
        assert_eq!(resolve_epoch_fence(0, None).unwrap(), 0);
        assert!(matches!(
            resolve_epoch_fence(0, Some(1)),
            Err(EngineError::EpochFenced)
        ));
    }

    #[test]
    fn sync_adapter_forwards_read_errors_and_fences() {
        let ok = resolve_bounded_epoch_sync(Some(2), || Ok(2)).unwrap();
        assert_eq!(ok, 2);
        assert!(matches!(
            resolve_bounded_epoch_sync(Some(1), || Ok(2)),
            Err(EngineError::EpochFenced)
        ));
        assert!(matches!(
            resolve_bounded_epoch_sync(None, || Err(EngineError::NotFound)),
            Err(EngineError::NotFound)
        ));
    }
}
