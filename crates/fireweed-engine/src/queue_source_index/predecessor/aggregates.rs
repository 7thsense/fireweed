//! Raw lifecycle totals and insertion allocation authority. Clock-dependent
//! expired-lease classification belongs to reads, not these durable counters.
use super::*;
use crate::queue_source_index::{QueueSourceCompileLimits, edits::SourceEdits};
use fireweed_core::queue_contract::Lifecycle;
use std::collections::BTreeSet;

const LIFECYCLES: [Lifecycle; 4] = [
    Lifecycle::Pending,
    Lifecycle::Leased,
    Lifecycle::Completed,
    Lifecycle::Failed,
];
fn lifecycle(fact: Option<QueueSourceRecord>) -> Option<usize> {
    match fact?.live? {
        QueueSourceLive::Work { metadata, .. } => LIFECYCLES
            .iter()
            .position(|value| *value == metadata.lifecycle),
        QueueSourceLive::State { .. } => None,
    }
}
pub struct QueueSourceAggregateEdits<'a> {
    output: SourceEdits<'a>,
}
impl QueueSourceAggregateEdits<'_> {
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
pub struct QueueSourceAggregateAccumulator<'c, 'a, R: QueueSourceReader> {
    context: &'c QueueSourcePublicationContext<'a, R>,
    coverage: super::coverage::CommandCoverage<'c, 'a>,
    removed: [u64; 4],
    added: [u64; 4],
    fresh: BTreeSet<u64>,
    limits: QueueSourceCompileLimits,
    charge: OwnedQueueBufferCharge,
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub fn aggregates<'c>(
        &'c self,
        limits: QueueSourceCompileLimits,
    ) -> QueueResult<QueueSourceAggregateAccumulator<'c, 'a, R>> {
        Ok(QueueSourceAggregateAccumulator {
            context: self,
            coverage: super::coverage::CommandCoverage::new(
                &self._charge,
                self.publication().summary().commands,
            ),
            removed: [0; 4],
            added: [0; 4],
            fresh: BTreeSet::new(),
            limits,
            charge: self.job.owned_buffer(
                1024 + std::mem::size_of::<QueueSourceAggregateAccumulator<'_, '_, R>>() as u64,
            )?,
        })
    }
}
impl<'a, R: QueueSourceReader> QueueSourceAggregateAccumulator<'_, 'a, R> {
    pub fn observe(&mut self, transition: &QueueSourceRecordTransition<'_, 'a>) -> QueueResult<()> {
        let identity = self.coverage.begin(transition)?;
        if let Some(index) = lifecycle(transition.before_fact()) {
            self.removed[index] = self.removed[index]
                .checked_add(1)
                .ok_or(QueueError::TooLarge)?;
        }
        if let Some(index) = lifecycle(Some(transition.fact())) {
            self.added[index] = self.added[index]
                .checked_add(1)
                .ok_or(QueueError::TooLarge)?;
        }
        if let Some(QueueSourceLive::Work { metadata, .. }) = transition.fact().live
            && metadata.insertion_sequence > self.context.publication().summary().initial_sequence
        {
            if self.fresh.contains(&metadata.insertion_sequence) {
                return Err(unknown("duplicate fresh work insertion allocation"));
            }
            self.charge.grow(256)?;
            self.fresh.insert(metadata.insertion_sequence);
        }
        self.coverage.complete(identity)
    }
    pub async fn finish(self) -> QueueResult<QueueSourceAggregateEdits<'a>> {
        self.coverage.verify()?;
        let mut old = [0u64; 4];
        let mut final_counts = [0u64; 4];
        let mut old_total = 0u128;
        let mut final_total = 0u128;
        for (index, lifecycle) in LIFECYCLES.into_iter().enumerate() {
            let count: u64 = self
                .context
                .fixed(QueueSourceKey::LifecycleCount(lifecycle))
                .await?
                .ok_or_else(|| unknown("source lifecycle count missing"))?;
            if count < self.removed[index] {
                return Err(unknown("source lifecycle count contradicts observed work"));
            }
            let final_count =
                i128::from(count) - i128::from(self.removed[index]) + i128::from(self.added[index]);
            final_counts[index] = u64::try_from(final_count)
                .map_err(|_| unknown("source lifecycle count overflow"))?;
            old[index] = count;
            old_total = old_total
                .checked_add(u128::from(count))
                .ok_or(QueueError::TooLarge)?;
            final_total = final_total
                .checked_add(u128::from(final_counts[index]))
                .ok_or(QueueError::TooLarge)?;
        }
        let summary = self.context.publication().summary();
        if old_total > u128::from(summary.initial_sequence)
            || final_total > u128::from(summary.final_sequence)
        {
            return Err(unknown(
                "source lifecycle total exceeds insertion authority",
            ));
        }
        let mut output = SourceEdits::new(
            self.context.checked.owner(),
            self.context.cut,
            self.context.job,
            self.limits,
            "aggregate",
        )?;
        for (index, lifecycle) in LIFECYCLES.into_iter().enumerate() {
            if old[index] != final_counts[index] {
                output.insert(
                    QueueSourceKey::LifecycleCount(lifecycle),
                    Some(&final_counts[index]),
                )?;
            }
        }
        if summary.final_sequence != summary.initial_sequence {
            output.insert(QueueSourceKey::Sequence, Some(&summary.final_sequence))?;
        }
        Ok(QueueSourceAggregateEdits { output })
    }
}

#[cfg(test)]
mod tests;
