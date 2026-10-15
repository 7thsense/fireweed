//! Ordered logical groups share one addressed source image and physical prefix.
use super::*;
use crate::queue_log_record::{OwnedQueueLogRecord, encode_checked_publication_record};
use crate::queue_publication::{PublicationReceipt, encode_publication_pinned};

pub struct QueueSourceBatchPreimage<'a> {
    image: SourceImage<'a>,
    batch: &'a CommitBatch,
    positions: Vec<usize>,
    definition: &'a AdmittedQueueDefinition,
    job: &'a Arc<QueueReservation>,
    _declarations: OwnedQueueBufferCharge,
}
pub struct QueueSourceBatchPlan<'a> {
    deltas: Vec<CommitDelta>,
    before: RecordSnapshot,
    preimage: QueueSourceBatchPreimage<'a>,
    _planning: OwnedQueueBufferCharge,
}
/// The positions are a contiguous caller-order prefix, independent of the
/// identity-sorted wire receipts. No unpublished suffix is retained here.
/// This capability does not establish original freshness or that preceding
/// caller positions are resolved; the coordinator must establish both before
/// planning and must retain submission ownership through original resolution.
pub struct QueueSourceBatchPublication<'a> {
    record: OwnedQueueLogRecord,
    positions: Vec<usize>,
    cut: QueueSourceCut<'a>,
    batch: &'a CommitBatch,
    definition: crate::queue_schema_codec::DefinitionPin,
    job: &'a QueueReservation,
    _declarations: OwnedQueueBufferCharge,
}
pub async fn load_batch_preimage<'a, R: QueueSourceReader>(
    reader: &'a R,
    batch: &'a CommitBatch,
    positions: &[usize],
    definition: &'a AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &'a Arc<QueueReservation>,
) -> QueueResult<QueueSourceBatchPreimage<'a>> {
    // Admit the envelope validation sets, selected positions and cloned read-set
    // before building any of them. Payload copies here are independently funded.
    let bytes = encoded_len(batch)?
        .checked_mul(16)
        .and_then(|n| n.checked_add((positions.len() as u64).checked_mul(1024)?))
        .and_then(|n| n.checked_add(8192))
        .ok_or(QueueError::TooLarge)?;
    let declarations = job.owned_buffer(bytes)?;
    batch.preflight(limits)?;
    crate::queue_memory::administrative_envelope_preflight(batch, limits)?;
    if positions.is_empty()
        || positions.iter().any(|&p| p >= batch.groups.len())
        || positions
            .windows(2)
            .any(|p| p[0].checked_add(1) != Some(p[1]))
    {
        return Err(QueueError::Invalid(
            "source batch requires a contiguous nonempty prefix".into(),
        ));
    }
    // Validate every sibling before reading fresh state. A materialization union
    // can contain repeated keys and must never be validated as an atomic group.
    batch.validate(definition.schema(), limits)?;
    let mut structural = 0u64;
    for &p in positions {
        let group = &batch.groups[p];
        validate_ordinary_group(&batch.queue, group, definition, limits)?;
        structural = structural
            .checked_add(request_scratch(group, definition.schema())?)
            .ok_or(QueueError::TooLarge)?;
    }
    let image_charge = job.owned_buffer(structural)?;
    let envelope = queue_planner::load_envelope(positions.iter().map(|&p| &batch.groups[p]));
    let image = load_image(
        reader,
        &batch.queue,
        &envelope,
        definition,
        job,
        image_charge,
    )
    .await?;
    Ok(QueueSourceBatchPreimage {
        image,
        batch,
        positions: positions.to_vec(),
        definition,
        job,
        _declarations: declarations,
    })
}
impl<'a> QueueSourceBatchPreimage<'a> {
    pub async fn plan<R: QueueSourceReader>(
        mut self,
        reader: &R,
        clock: &dyn crate::Clock,
    ) -> QueueResult<QueueSourceBatchPlan<'a>> {
        if !reader.cut()?.same_cut(&self.image.cut) {
            return Err(unknown("batch planning reader changed cut"));
        }
        let envelope =
            queue_planner::load_envelope(self.positions.iter().map(|&p| &self.batch.groups[p]));
        let bytes = crate::queue_memory::group_expansion_budget(
            &envelope,
            &self.image.snapshot,
            self.definition.schema(),
        )?
        .checked_add(8192)
        .ok_or(QueueError::TooLarge)?;
        let mut planning = self.job.owned_buffer(bytes)?;
        let before = self.image.snapshot.clone();
        let mut deltas = Vec::with_capacity(self.positions.len());
        for &position in &self.positions {
            let group = &self.batch.groups[position];
            // Keep loaded absence after releases. A probe already loaded into the
            // overlay must never restore a source-cut owner over prior effects.
            self.image
                .load_unique(reader, group, self.definition, self.job)
                .await?;
            planning.grow(
                crate::queue_memory::group_expansion_budget(
                    group,
                    &self.image.snapshot,
                    self.definition.schema(),
                )?
                .checked_add(8192)
                .ok_or(QueueError::TooLarge)?,
            )?;
            let time = clock.now();
            if time.nanoseconds >= 1_000_000_000 {
                return Err(unknown("invalid clock nanoseconds"));
            }
            let now =
                timestamp_nanos(time).map_err(|_| unknown("clock outside queue time domain"))?;
            let delta = queue_planner::plan_commit_group(
                group,
                &mut self.image.snapshot,
                now,
                self.definition.schema(),
            )?;
            deltas.push(delta);
        }
        Ok(QueueSourceBatchPlan {
            deltas,
            before,
            preimage: self,
            _planning: planning,
        })
    }
}
impl<'a> QueueSourceBatchPlan<'a> {
    pub fn receipts(&self) -> impl ExactSizeIterator<Item = &CommitReceipt> {
        self.deltas.iter().map(|d| &d.receipt)
    }
    fn encode_count(
        &self,
        count: usize,
        limits: QueueSourceGroupEncodingLimits,
    ) -> QueueResult<OwnedQueueLogRecord> {
        let input = &self.preimage;
        // Serialization sizing is borrowed. Fund canonical maps, cloned commands,
        // receipt containers and sorting before combine_deltas allocates them.
        let bytes = self.deltas[..count].iter().try_fold(8192u64, |n, delta| {
            encoded_len(delta)?
                .checked_mul(16)
                .and_then(|b| b.checked_add((delta.work.len() as u64).checked_mul(4096)?))
                .and_then(|b| b.checked_add((delta.state.len() as u64).checked_mul(4096)?))
                .and_then(|b| b.checked_add(4096))
                .and_then(|b| n.checked_add(b))
                .ok_or(QueueError::TooLarge)
        })?;
        let _canonical = input.job.owned_buffer(bytes)?;
        let canonical =
            queue_planner::combine_deltas(&self.deltas[..count], self.before.next_sequence)?;
        let mut receipts = self.deltas[..count]
            .iter()
            .zip(&input.positions[..count])
            .map(|(delta, &position)| {
                PublicationReceipt::commit(
                    &input.batch.queue,
                    &input.batch.groups[position],
                    &delta.receipt,
                )
                .map_err(publication::publication_error)
            })
            .collect::<QueueResult<Vec<_>>>()?;
        receipts.sort_unstable_by(|left, right| left.identity().cmp(&right.identity()));
        let frame = encode_publication_pinned(
            &input.batch.queue,
            input.definition.pin(),
            &self.before,
            &canonical,
            &receipts,
            limits.frame_bytes,
            input.job,
        )
        .map_err(publication::publication_error)?;
        encode_checked_publication_record(
            input.image.cut.epoch(),
            frame.as_bytes(),
            limits.record_bytes,
            input.job,
        )
    }
    /// Try the complete selected run first. If it overflows, preserve the first
    /// fitting prefix without resampling any clock. Coalescing is nonmonotonic;
    /// first-overflow splitting makes no claim of globally maximal occupancy.
    pub fn encode_prefix(
        mut self,
        limits: QueueSourceGroupEncodingLimits,
    ) -> QueueResult<QueueSourceBatchPublication<'a>> {
        if limits.frame_bytes == 0 || limits.record_bytes == 0 {
            return Err(QueueError::Invalid(
                "batch encoding limits must be positive".into(),
            ));
        }
        let total = self.deltas.len();
        let (record, count) = match self.encode_count(total, limits) {
            Ok(record) => (record, total),
            Err(QueueError::TooLarge) => {
                let mut selected = None;
                for count in 1..total {
                    match self.encode_count(count, limits) {
                        Ok(record) => selected = Some((record, count)),
                        Err(QueueError::TooLarge) => break,
                        Err(error) => return Err(error),
                    }
                }
                selected.ok_or(QueueError::TooLarge)?
            }
            Err(error) => return Err(error),
        };
        self.preimage.positions.truncate(count);
        Ok(QueueSourceBatchPublication {
            record,
            positions: self.preimage.positions,
            cut: self.preimage.image.cut,
            batch: self.preimage.batch,
            definition: self.preimage.definition.pin(),
            job: self.preimage.job,
            _declarations: self.preimage._declarations,
        })
    }
}
impl QueueSourceBatchPublication<'_> {
    pub fn positions(&self) -> &[usize] {
        &self.positions
    }
    pub fn record_for(
        &self,
        cut: &QueueSourceCut<'_>,
        definition: &AdmittedQueueDefinition,
        batch: &CommitBatch,
        job: &QueueReservation,
    ) -> QueueResult<&OwnedQueueLogRecord> {
        if !self.cut.same_cut(cut)
            || !std::ptr::eq(self.job, job)
            || !std::ptr::eq(self.batch, batch)
            || !definition.belongs_to(job)
            || definition.pin() != self.definition
            || !self.record.belongs_to(job)
        {
            return Err(unknown(
                "batch publication declaration, cut, definition or operation differs",
            ));
        }
        let checked = self.record.checked()?;
        cut.validate_publication(&checked, job)?;
        if checked.view().definition_pin_bytes() != *self.definition.as_bytes() {
            return Err(unknown("batch publication definition changed"));
        }
        Ok(&self.record)
    }
}
