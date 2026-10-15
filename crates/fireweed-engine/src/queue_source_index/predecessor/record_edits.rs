//! Provisional record/field/posting edits; never sufficient to publish a record.
use super::*;
use crate::queue_source_index::{QueueSourceCompileLimits, edits::SourceEdits};

pub struct QueueSourceRecordEdits<'a> {
    pub(super) output: SourceEdits<'a>,
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
    coverage: super::coverage::CommandCoverage<'c, 'a>,
    output: SourceEdits<'a>,
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub fn record_edits<'c>(
        &'c self,
        limits: QueueSourceCompileLimits,
    ) -> QueueResult<QueueSourceRecordEditAccumulator<'c, 'a, R>> {
        Ok(QueueSourceRecordEditAccumulator {
            context: self,
            coverage: super::coverage::CommandCoverage::new(
                &self._charge,
                self.publication().summary().commands,
            ),
            output: self.output(limits, "record")?,
        })
    }
}
impl<'a, R: QueueSourceReader> QueueSourceRecordEditAccumulator<'_, 'a, R> {
    pub fn observe(&mut self, transition: &QueueSourceRecordTransition<'_, 'a>) -> QueueResult<()> {
        let identity = self.coverage.begin(transition)?;
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
        self.coverage.complete(identity)?;
        Ok(())
    }
    pub fn finish(self) -> QueueResult<QueueSourceRecordEdits<'a>> {
        self.coverage.verify()?;
        Ok(QueueSourceRecordEdits {
            output: self.output,
        })
    }
}
