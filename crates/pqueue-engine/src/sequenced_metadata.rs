//! Typed protocol vocabulary for sequenced object-log metadata.
//!
//! The types in this module deliberately describe protocol differences instead of hiding them behind one
//! generic "metadata write" helper. Fenced manifest/floor publication retains its create-only address;
//! deletion-watermark publication records a prefix only after every physical delete in that prefix succeeds.

use std::marker::PhantomData;

use crate::{EngineError, EngineResult};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct ManifestIndex(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct HeadVersion(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct AssignmentEpoch(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct CommandSequence(pub u64);

/// The create-only address is permanent. Reclamation may replace its value with a tombstone, but must not
/// free the address because stale writers rely on the resulting conditional-write collision.
pub enum RetainedAddress {}

/// The address may be physically deleted once the class-specific eligibility policy proves it unreachable.
pub enum FreeAddress {}

pub enum ManifestHeadClass {}
pub enum RetentionFloorClass {}
pub enum DeletionWatermarkClass {}

/// A typed create-only publication family. `C` identifies the durable metadata class and `A` its address
/// retention policy, preventing call sites from silently substituting a freeable key for a permanent fence.
pub struct CreateOnlyPublication<C, A>(PhantomData<(C, A)>);

impl<C> CreateOnlyPublication<C, RetainedAddress> {
    pub fn publish<P, R>(
        expected_body: &[u8],
        put_if_absent: P,
        reread: R,
    ) -> EngineResult<CreateOnlyResolution>
    where
        P: FnOnce() -> EngineResult<bool>,
        R: FnOnce() -> EngineResult<Option<Vec<u8>>>,
    {
        create_only_with_recovery(expected_body, put_if_absent, reread)
    }
}

/// Resolution of a create-only write. Storage errors are not automatically retryable: the write may have
/// taken effect before its response was lost, so the boundary rereads the exact authoritative address first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOnlyResolution {
    Applied,
    AlreadyApplied,
    AppliedAfterAmbiguity,
    PreconditionLost,
    Ambiguous(EngineError),
}

impl CreateOnlyResolution {
    pub fn applied(&self) -> bool {
        matches!(
            self,
            Self::Applied | Self::AlreadyApplied | Self::AppliedAfterAmbiguity
        )
    }
}

/// Resolve create-only publication without adding a read to its successful hot path. Only a failed response
/// is reread, distinguishing effect-then-error from a definite precondition loss or unresolved ambiguity.
fn create_only_with_recovery<P, R>(
    expected_body: &[u8],
    put_if_absent: P,
    reread: R,
) -> EngineResult<CreateOnlyResolution>
where
    P: FnOnce() -> EngineResult<bool>,
    R: FnOnce() -> EngineResult<Option<Vec<u8>>>,
{
    match put_if_absent() {
        Ok(true) => Ok(CreateOnlyResolution::Applied),
        Ok(false) => match reread() {
            Ok(Some(actual)) if actual == expected_body => Ok(CreateOnlyResolution::AlreadyApplied),
            Ok(_) => Ok(CreateOnlyResolution::PreconditionLost),
            Err(source) => Ok(CreateOnlyResolution::Ambiguous(source)),
        },
        Err(source) => match reread() {
            Ok(Some(actual)) if actual == expected_body => {
                Ok(CreateOnlyResolution::AppliedAfterAmbiguity)
            }
            Ok(Some(_)) => Ok(CreateOnlyResolution::PreconditionLost),
            Ok(None) | Err(_) => Ok(CreateOnlyResolution::Ambiguous(source)),
        },
    }
}

/// Advance-before-delete protocol used only by the retention floor. Publication grants eligibility; callers
/// may begin physical deletion only after the fenced monotonic advance is durably confirmed.
pub struct AdvanceThenDelete<C, A>(PhantomData<(C, A)>);

impl AdvanceThenDelete<RetentionFloorClass, RetainedAddress> {
    pub fn publish_then_delete<S, T, Advance, Delete>(
        state: &mut S,
        advance: Advance,
        delete: Delete,
    ) -> EngineResult<T>
    where
        Advance: FnOnce(&mut S) -> EngineResult<()>,
        Delete: FnOnce(&mut S) -> EngineResult<T>,
    {
        advance(state)?;
        delete(state)
    }
}

/// Delete-before-advance protocol used only by the deletion watermark. A failed delete returns immediately,
/// so the monotone marker can never skip incomplete physical work.
pub struct DeleteThenAdvance<C, A>(PhantomData<(C, A)>);

impl DeleteThenAdvance<DeletionWatermarkClass, FreeAddress> {
    pub fn delete_all_then_advance<T, I, Delete, Advance>(
        targets: I,
        mut delete: Delete,
        advance: Advance,
    ) -> EngineResult<usize>
    where
        I: IntoIterator<Item = T>,
        Delete: FnMut(T) -> EngineResult<()>,
        Advance: FnOnce() -> EngineResult<()>,
    {
        let mut completed = 0;
        for target in targets {
            delete(target)?;
            completed += 1;
        }
        advance()?;
        Ok(completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    #[test]
    fn ambiguous_create_resolves_from_authoritative_reread() {
        let body = b"winner";
        let resolution = create_only_with_recovery(
            body,
            || Err(EngineError::Storage("lost response".into())),
            || Ok(Some(body.to_vec())),
        )
        .unwrap();
        assert_eq!(resolution, CreateOnlyResolution::AppliedAfterAmbiguity);
    }

    #[test]
    fn different_body_is_precondition_lost() {
        let resolution = CreateOnlyPublication::<ManifestHeadClass, RetainedAddress>::publish(
            b"ours",
            || Ok(false),
            || Ok(Some(b"theirs".to_vec())),
        )
        .unwrap();
        assert_eq!(resolution, CreateOnlyResolution::PreconditionLost);
    }

    #[test]
    fn missing_or_failed_ambiguity_reread_stays_typed_ambiguous() {
        for reread in [Ok(None), Err(EngineError::Storage("read failed".into()))] {
            let resolution = CreateOnlyPublication::<ManifestHeadClass, RetainedAddress>::publish(
                b"ours",
                || Err(EngineError::Storage("write response lost".into())),
                || reread,
            )
            .unwrap();
            assert!(matches!(resolution, CreateOnlyResolution::Ambiguous(_)));
        }
    }

    #[test]
    fn successful_create_performs_zero_rereads() {
        let reads = Cell::new(0);
        let resolution = CreateOnlyPublication::<ManifestHeadClass, RetainedAddress>::publish(
            b"ours",
            || Ok(true),
            || {
                reads.set(reads.get() + 1);
                Ok(None)
            },
        )
        .unwrap();
        assert_eq!(resolution, CreateOnlyResolution::Applied);
        assert_eq!(reads.get(), 0);
    }

    #[test]
    fn delete_marker_never_runs_after_incomplete_delete() {
        let events = RefCell::new(Vec::new());
        let result =
            DeleteThenAdvance::<DeletionWatermarkClass, FreeAddress>::delete_all_then_advance(
                [0, 1, 2],
                |index| {
                    events.borrow_mut().push(index);
                    if index == 1 {
                        Err(EngineError::Storage("delete".into()))
                    } else {
                        Ok(())
                    }
                },
                || {
                    events.borrow_mut().push(99);
                    Ok(())
                },
            );
        assert!(matches!(result, Err(EngineError::Storage(_))));
        assert_eq!(*events.borrow(), vec![0, 1]);
    }

    #[test]
    fn floor_publication_precedes_delete() {
        let events = RefCell::new(Vec::new());
        AdvanceThenDelete::<RetentionFloorClass, RetainedAddress>::publish_then_delete(
            &mut (),
            |_| {
                events.borrow_mut().push("advance");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("delete");
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(*events.borrow(), vec!["advance", "delete"]);
    }
}
