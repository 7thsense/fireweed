//! Posting effects derived only from authenticated record transitions.
use super::*;
use crate::{
    queue_index::{ordered_key_ref, ordered_key_ref_len},
    queue_source_index::QueueSourceKey,
};

impl QueueSourceRecordTransition<'_, '_> {
    /// Remove old and add new scalar memberships with full literal identities.
    /// Sort buffers are admitted before allocation and retained through callbacks.
    /// Equal canonical keys (including signed zero) require no posting edit.
    /// The caller must discard all accumulated output if any callback fails.
    pub fn visit_scalar_postings<F>(
        &self,
        job: &Arc<QueueReservation>,
        visitor: &mut F,
    ) -> QueueResult<()>
    where
        F: FnMut(QueueSourceKey<'_>, bool) -> QueueResult<()>,
    {
        if !std::ptr::eq(self._charge.reservation(), job.as_ref()) {
            return Err(unknown("posting operation differs"));
        }
        let identity = self.resolved().identity;
        self.visit_field_changes(&mut |field, before, after| {
            if before == after {
                return Ok(());
            }
            let bytes = before
                .map_or(Ok(0), |value| ordered_key_ref_len(&[Some(value)]))?
                .checked_add(after.map_or(Ok(0), |value| ordered_key_ref_len(&[Some(value)]))?)
                .and_then(|n| n.checked_add(2 * std::mem::size_of::<Vec<u8>>()))
                .ok_or(QueueError::TooLarge)?;
            let _charge =
                job.owned_buffer(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)?;
            let old = before
                .map(|value| ordered_key_ref(&[Some(value)]))
                .transpose()?;
            let new = after
                .map(|value| ordered_key_ref(&[Some(value)]))
                .transpose()?;
            if old == new {
                return Ok(());
            }
            for (sort, present) in [(old.as_deref(), false), (new.as_deref(), true)] {
                if let Some(sort) = sort {
                    visitor(
                        QueueSourceKey::ScalarPosting {
                            collection: identity.collection,
                            field,
                            sort,
                            key: identity.key,
                        },
                        present,
                    )?;
                }
            }
            Ok(())
        })
    }
}
