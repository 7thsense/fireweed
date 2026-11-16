//! Provisional intrinsic memberships from the same complete command stream.
use super::*;
use crate::queue_source_index::{
    QueueSourceCompileLimits, QueueSourceWorkIndex, edits::SourceEdits,
};

pub struct QueueSourceIntrinsicEdits<'a> {
    pub(super) output: SourceEdits<'a>,
}
impl QueueSourceIntrinsicEdits<'_> {
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
pub struct QueueSourceIntrinsicAccumulator<'c, 'a, R: QueueSourceReader> {
    coverage: super::coverage::CommandCoverage<'c, 'a>,
    output: SourceEdits<'a>,
    _context: &'c QueueSourcePublicationContext<'a, R>,
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub fn intrinsics<'c>(
        &'c self,
        limits: QueueSourceCompileLimits,
    ) -> QueueResult<QueueSourceIntrinsicAccumulator<'c, 'a, R>> {
        Ok(QueueSourceIntrinsicAccumulator {
            coverage: super::coverage::CommandCoverage::new(
                &self._charge,
                self.publication().summary().commands,
            ),
            output: self.output(limits, "intrinsic")?,
            _context: self,
        })
    }
}
impl<'a, R: QueueSourceReader> QueueSourceIntrinsicAccumulator<'_, 'a, R> {
    pub fn observe(&mut self, transition: &QueueSourceRecordTransition<'_, 'a>) -> QueueResult<()> {
        let identity = self.coverage.begin(transition)?;
        let before = transition.before_fact().and_then(|fact| fact.live);
        let after = transition.fact().live;
        if before.is_some() != after.is_some() {
            self.output.insert(
                QueueSourceKey::LiveKey {
                    collection: identity.collection,
                    key: identity.key,
                },
                after.is_some().then_some(&()),
            )?;
        }
        if identity.collection.is_none() {
            let metadata = |live| match live {
                Some(QueueSourceLive::Work { metadata, .. }) => Some(metadata),
                _ => None,
            };
            for index in QueueSourceWorkIndex::ALL {
                let old = metadata(before)
                    .map(|m| index.posting(m))
                    .transpose()?
                    .flatten();
                let new = metadata(after)
                    .map(|m| index.posting(m))
                    .transpose()?
                    .flatten();
                if old == new {
                    continue;
                }
                for (posting, present) in [(old, false), (new, true)] {
                    if let Some(posting) = posting {
                        self.output.insert(
                            QueueSourceKey::WorkIntrinsic {
                                posting,
                                key: identity.key,
                            },
                            present.then_some(&()),
                        )?;
                    }
                }
            }
        }
        self.coverage.complete(identity)?;
        Ok(())
    }
    pub fn finish(self) -> QueueResult<QueueSourceIntrinsicEdits<'a>> {
        self.coverage.verify()?;
        Ok(QueueSourceIntrinsicEdits {
            output: self.output,
        })
    }
}

#[cfg(test)]
pub(super) mod tests;
