//! Bounded assignment-epoch resolution (P7N + P14).
//!
//! Product data-plane writes stamp a positively-acquired assignment epoch when ownership
//! is in play. Callers resolve "current" (and optional CAS expected) epochs through one
//! pure fence helper so every log family (memory, sqlite, postgres, filesystem, s3) shares
//! identical EpochFenced / epoch-0 rejection semantics.
//!
//! **P14 contract**
//! - Async callers pre-resolve via `await current_epoch` then pure [`resolve_write_epoch`]
//!   (never `block_on` an epoch future on a Tokio worker).
//! - Bounded sync bridges use [`resolve_bounded_epoch_sync`] / [`resolve_write_epoch_sync`]
//!   after offloaded reads; they must not nest runtimes on the caller.
//! - Ownership-stamped writes (`expected = Some(_)`) reject genesis epoch-0.
//! - Sole-owner / uncoordinated paths (`expected = None`) may still observe log genesis 0.
//!
//! This module owns the pure fence + sync/async adapters only. Product open paths keep
//! residual whole-op bridges until per-cell runtime-safety exit criteria land.

use std::future::Future;

use crate::error::{EngineError, EngineResult};

/// Minimum legal assignment epoch for a data-plane commit stamp.
///
/// Control-plane genesis uses epoch 0 ("never granted"). A write path that stamps an
/// *ownership* assignment must not use epoch 0: that would defeat fencing against a
/// subsequent positive acquire.
pub const MIN_ASSIGNMENT_EPOCH: u64 = 1;

/// Pure fence for ownership-stamped writes: reject mismatch and genesis epoch-0.
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

/// Same CAS fence without the epoch-0 rejection — sole-owner product paths and diagnostics.
pub fn resolve_epoch_fence(current: u64, expected: Option<u64>) -> EngineResult<u64> {
    if expected.is_some_and(|e| e != current) {
        return Err(EngineError::EpochFenced);
    }
    Ok(current)
}

/// Product write-path fence after async (or sync) pre-resolution of `current`.
pub fn resolve_write_epoch(current: u64, expected: Option<u64>) -> EngineResult<u64> {
    match expected {
        Some(_) => resolve_bounded_epoch(current, expected),
        None => resolve_epoch_fence(current, None),
    }
}

/// Sync adapter for ownership fence after a bounded offloaded current-epoch read.
pub fn resolve_bounded_epoch_sync<F>(expected: Option<u64>, read_current: F) -> EngineResult<u64>
where
    F: FnOnce() -> EngineResult<u64>,
{
    let current = read_current()?;
    resolve_bounded_epoch(current, expected)
}

/// Sync adapter for product write fences after a bounded offloaded current-epoch read.
pub fn resolve_write_epoch_sync<F>(expected: Option<u64>, read_current: F) -> EngineResult<u64>
where
    F: FnOnce() -> EngineResult<u64>,
{
    let current = read_current()?;
    resolve_write_epoch(current, expected)
}

/// Async pre-resolution: await `read_current`, then apply the pure write fence.
pub async fn resolve_write_epoch_async<F, Fut>(
    expected: Option<u64>,
    read_current: F,
) -> EngineResult<u64>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = EngineResult<u64>>,
{
    let current = read_current().await?;
    resolve_write_epoch(current, expected)
}

/// Async pre-resolution with strict ownership fence (always no epoch-0).
pub async fn resolve_bounded_epoch_async<F, Fut>(
    expected: Option<u64>,
    read_current: F,
) -> EngineResult<u64>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = EngineResult<u64>>,
{
    let current = read_current().await?;
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
    fn write_epoch_sole_owner_allows_genesis_zero() {
        assert_eq!(resolve_write_epoch(0, None).unwrap(), 0);
        assert_eq!(resolve_write_epoch(4, None).unwrap(), 4);
    }

    #[test]
    fn write_epoch_ownership_stamp_rejects_genesis() {
        assert!(matches!(
            resolve_write_epoch(0, Some(0)),
            Err(EngineError::Invalid(_))
        ));
        assert_eq!(resolve_write_epoch(2, Some(2)).unwrap(), 2);
        assert!(matches!(
            resolve_write_epoch(2, Some(1)),
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

    #[test]
    fn write_epoch_sync_adapter_sole_owner_and_ownership() {
        assert_eq!(resolve_write_epoch_sync(None, || Ok(0)).unwrap(), 0);
        assert_eq!(resolve_write_epoch_sync(Some(3), || Ok(3)).unwrap(), 3);
        assert!(matches!(
            resolve_write_epoch_sync(Some(0), || Ok(0)),
            Err(EngineError::Invalid(_))
        ));
    }

    #[test]
    fn async_pre_resolution_polls_without_nested_runtime() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        fn once_poll<T>(fut: impl Future<Output = T>) -> T {
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            let mut fut = std::pin::pin!(fut);
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => v,
                Poll::Pending => panic!("epoch async adapter must complete without I/O suspension"),
            }
        }

        let sole = once_poll(resolve_write_epoch_async(None, || async { Ok(0u64) })).unwrap();
        assert_eq!(sole, 0);
        let owned = once_poll(resolve_write_epoch_async(Some(5), || async { Ok(5u64) })).unwrap();
        assert_eq!(owned, 5);
        let fenced = once_poll(resolve_write_epoch_async(Some(4), || async { Ok(5u64) }));
        assert!(matches!(fenced, Err(EngineError::EpochFenced)));
        let bounded =
            once_poll(resolve_bounded_epoch_async(Some(1), || async { Ok(1u64) })).unwrap();
        assert_eq!(bounded, 1);
    }
}
