//! Provisional record/field/posting edits; never sufficient to publish a record.
use super::*;
use crate::{
    queue_publication::IdentityRef,
    queue_source_index::{QueueSourceCompileLimits, edits::SourceEdits},
};

pub struct QueueSourceRecordEdits<'a> {
    output: SourceEdits<'a>,
}
impl QueueSourceRecordEdits<'_> {
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> {
        self.output.edits()
    }
    pub fn verify(
        &self,
        record: &crate::queue_log_record::OwnedQueueLogRecord,
        cut: &QueueSourceCut<'_>,
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.output.verify(record, cut, job)
    }
}
pub struct QueueSourceRecordEditAccumulator<'c, 'a, R: QueueSourceReader> {
    context: &'c QueueSourcePublicationContext<'a, R>,
    processed: u64,
    last: Option<IdentityRef<'a>>,
    poisoned: bool,
    output: SourceEdits<'a>,
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub fn record_edits<'c>(
        &'c self,
        limits: QueueSourceCompileLimits,
    ) -> QueueResult<QueueSourceRecordEditAccumulator<'c, 'a, R>> {
        Ok(QueueSourceRecordEditAccumulator {
            context: self,
            processed: 0,
            last: None,
            poisoned: false,
            output: SourceEdits::new(self.checked.owner(), self.cut, self.job, limits, "record")?,
        })
    }
}
impl<'a, R: QueueSourceReader> QueueSourceRecordEditAccumulator<'_, 'a, R> {
    pub fn observe(&mut self, transition: &QueueSourceRecordTransition<'_, 'a>) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("record edit accumulator poisoned"));
        }
        self.poisoned = true;
        let identity = transition.publication_identity();
        if !std::ptr::eq(transition.context_owner(), &self.context._charge)
            || self.processed >= self.context.publication().summary().commands
            || self.last.is_some_and(|last| {
                (last.collection, last.key) >= (identity.collection, identity.key)
            })
        {
            return Err(unknown("record edit command coverage differs"));
        }
        self.output.insert(
            QueueSourceKey::Record {
                collection: identity.collection,
                key: identity.key,
            },
            Some(&transition.fact()),
        )?;
        transition.visit_field_changes(&mut |field, before, after| {
            // Field cells retain exact resolved bytes. Ordering codecs may
            // normalize signed zero, but payload storage must not do so.
            let equal = match (before, after) {
                (
                    Some(crate::queue_publication::ValueRef::Float(old)),
                    Some(crate::queue_publication::ValueRef::Float(new)),
                ) => old.to_bits() == new.to_bits(),
                _ => before == after,
            };
            if !equal {
                self.output.insert(
                    QueueSourceKey::Field {
                        collection: identity.collection,
                        key: identity.key,
                        field,
                    },
                    after.as_ref(),
                )?;
            }
            Ok(())
        })?;
        transition.visit_scalar_postings(self.context.job, &mut |key, present| {
            self.output.insert(key, present.then_some(&()))
        })?;
        transition.visit_named_postings(self.context.job, &mut |effect| {
            for (sort, present) in [(effect.before, false), (effect.after, true)] {
                if let Some(sort) = sort {
                    self.output.insert(
                        QueueSourceKey::NamedPosting {
                            collection: identity.collection,
                            index: effect.index,
                            sort,
                            key: identity.key,
                        },
                        present.then_some(&()),
                    )?;
                }
            }
            Ok(())
        })?;
        self.processed = self.processed.checked_add(1).ok_or(QueueError::TooLarge)?;
        self.last = Some(identity);
        self.poisoned = false;
        Ok(())
    }
    pub fn finish(self) -> QueueResult<QueueSourceRecordEdits<'a>> {
        if self.poisoned || self.processed != self.context.publication().summary().commands {
            return Err(unknown("incomplete record edit command coverage"));
        }
        Ok(QueueSourceRecordEdits {
            output: self.output,
        })
    }
}
