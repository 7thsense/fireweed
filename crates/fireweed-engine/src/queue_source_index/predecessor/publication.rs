//! Complete dispatch owns every reducer; only this module constructs the proof
//! consumed by the sealed compiler carrier.
use super::*;
use crate::queue_source_index::{
    QueueSourceCompileLimits, QueueSourceCompiled,
    edits::{OutputQuota, SourceEdits},
};

pub(in crate::queue_source_index) struct CompleteSourcePublication<'a> {
    output: SourceEdits<'a>,
}
impl<'a> CompleteSourcePublication<'a> {
    pub(in crate::queue_source_index) fn into_output(self) -> SourceEdits<'a> {
        self.output
    }
}

pub async fn compile_publication<'a, R: QueueSourceReader>(
    reader: &'a R,
    checked: &'a CheckedQueueLogRecord<'a>,
    definition: &'a AdmittedQueueDefinition,
    limits: QueueSourceCompileLimits,
    job: &'a Arc<QueueReservation>,
) -> QueueResult<QueueSourceCompiled<'a>> {
    let mut context =
        QueueSourcePublicationContext::begin(reader, checked, definition, job).await?;
    context.output_quota = Some(OutputQuota::new(checked.owner(), context.cut, job, limits)?);
    let mut records = context.record_edits(limits)?;
    let mut unique = context.unique_owners()?;
    let mut aggregates = context.aggregates(limits)?;
    let mut intrinsics = context.intrinsics(limits)?;
    for command in context.commands() {
        let command = command?;
        let predecessor = context.predecessor(&command).await?;
        let transition = context.record_transition(&predecessor).await?;
        records.observe(&transition)?;
        unique.observe(&transition)?;
        aggregates.observe(&transition)?;
        intrinsics.observe(&transition)?;
    }
    // Every reducer finishes, including zero-output cases. Unique owners,
    // lifecycle counts and original receipts still require source checks.
    let records = records.finish()?;
    let unique = unique.finish().await?;
    let aggregates = aggregates.finish().await?;
    let intrinsics = intrinsics.finish()?;
    let originals = context.originals(limits).await?;
    if context.output_quota.is_none() {
        return Err(unknown("compiler shared quota missing"));
    }
    let output = records
        .output
        .merge(unique.output)?
        .merge(aggregates.output)?
        .merge(intrinsics.output)?
        .merge(originals.output)?;
    output.verify(checked.owner(), &context.cut, job)?;
    output.verify_complete()?;
    Ok(QueueSourceCompiled::from_publication(
        CompleteSourcePublication { output },
    ))
}

#[cfg(test)]
mod tests;
