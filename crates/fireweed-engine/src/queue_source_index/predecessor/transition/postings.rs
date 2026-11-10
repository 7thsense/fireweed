//! Posting effects derived only from authenticated record transitions.
use super::*;
use crate::{
    queue_index::{ordered_key_ref, ordered_key_ref_len},
    queue_source_index::{QUEUE_SOURCE_ABSENT_SORT, QueueSourceKey},
};

impl QueueSourceRecordTransition<'_, '_> {
    /// Remove old and add new scalar memberships with full literal identities.
    /// Every declared field a live record lacks owns one sentinel posting
    /// (`QUEUE_SOURCE_ABSENT_SORT`), so absence probes are index-backed.
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
        })?;
        self.visit_absence_sentinels(job, visitor)
    }
    fn visit_absence_sentinels<F>(
        &self,
        job: &Arc<QueueReservation>,
        visitor: &mut F,
    ) -> QueueResult<()>
    where
        F: FnMut(QueueSourceKey<'_>, bool) -> QueueResult<()>,
    {
        if self.definition.fields.is_empty() {
            return Ok(());
        }
        let identity = self.resolved().identity;
        let before_exists = self
            .predecessor
            .fact
            .is_some_and(|fact| fact.live.is_some());
        let after_exists = self.fact.live.is_some();
        let mut sentinel = |field: &str, present: bool| {
            visitor(
                QueueSourceKey::ScalarPosting {
                    collection: identity.collection,
                    field,
                    sort: &QUEUE_SOURCE_ABSENT_SORT,
                    key: identity.key,
                },
                present,
            )
        };
        match (before_exists, after_exists) {
            (false, false) => Ok(()),
            // Only explicitly changed fields are loaded for an edit, and an
            // unedited field keeps its sentinel state.
            (true, true) => self.visit_field_changes(&mut |field, before, after| {
                if self.definition.fields.contains_key(field) && before.is_none() != after.is_none()
                {
                    sentinel(field, after.is_none())?;
                }
                Ok(())
            }),
            (false, true) => {
                let _charge = self.sentinel_names_charge(job)?;
                let names: Vec<&str> = self.definition.fields.keys().map(String::as_str).collect();
                self.visit_after_fields_borrowed(&names, &mut |field, value| {
                    if value.is_none() {
                        sentinel(field, true)?;
                    }
                    Ok(())
                })
            }
            (true, false) => {
                let before = self.fields.checked()?;
                for field in self.definition.fields.keys() {
                    if before.get(field)?.is_none() {
                        sentinel(field, false)?;
                    }
                }
                Ok(())
            }
        }
    }
    fn sentinel_names_charge(
        &self,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<crate::queue_admission::OwnedQueueBufferCharge> {
        let bytes = std::mem::size_of::<&str>()
            .checked_mul(self.definition.fields.len())
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        job.owned_buffer(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)
    }
}
