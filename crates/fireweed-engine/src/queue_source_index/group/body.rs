//! Materialize only addressed administrative comparisons, grouped by source C.
use super::*;
use crate::queue_source_index::{
    QueueSourceRecordReader, QueueSourceWorkBodyEvidence, verify_source_work_bodies,
};
pub(super) async fn work_fact<'a, R: QueueSourceReader>(
    reader: &'a R,
    cut: QueueSourceCut<'a>,
    key: &str,
    needs_body: bool,
    job: &Arc<QueueReservation>,
) -> QueueResult<(
    Option<QueueSourceRecord>,
    Option<QueueSourceWorkBodyEvidence<'a>>,
)> {
    let declaration = QueueSourceKey::Record {
        collection: None,
        key,
    };
    let charge = job.owned_buffer(declaration.encoded_len()? as u64)?;
    let encoded = declaration.encode()?;
    let owner = reader.lookup(&encoded).await?;
    let point = reader.point(&owner)?;
    let fact = point
        .value_for(&cut, &encoded)?
        .map(crate::queue_storage_layout::decode)
        .transpose()?;
    let body = if needs_body {
        QueueSourceWorkBodyEvidence::from_source(cut, key, point, job)?
    } else {
        None
    };
    drop(owner);
    drop(encoded);
    drop(charge);
    Ok((fact, body))
}
impl SourceImage<'_> {
    pub(super) async fn load_bodies<R: QueueSourceRecordReader>(
        &mut self,
        reader: &R,
        needs: &[QueueSourceWorkBodyEvidence<'_>],
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        if needs.is_empty() {
            return Ok(());
        }
        let _group_charge = job.owned_buffer(
            (needs.len() as u64)
                .checked_mul(1024)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let mut groups = BTreeMap::<u64, Vec<&QueueSourceWorkBodyEvidence<'_>>>::new();
        for body in needs {
            groups.entry(body.source_offset()).or_default().push(body);
        }
        for (offset, members) in groups {
            let owner = reader.read_record(&self.cut, offset).await?;
            let checked = reader.checked_record(&owner, &self.cut, offset)?;
            let verified =
                verify_source_work_bodies(&members, &checked, offset, definition.pin(), job)?;
            for (position, member) in members.iter().enumerate() {
                let raw = verified.body(position)?;
                let charge = job.owned_buffer(raw.len() as u64)?;
                // Transfer independent funding before allocating/copying or
                // asserting body_loaded. The checked C retires after all members.
                self.retained_charges.push(charge);
                let row = self
                    .snapshot
                    .work
                    .get_mut(member.key())
                    .ok_or_else(|| unknown("body target disappeared from source image"))?;
                row.record.work.body = raw.to_vec().into();
                row.body_loaded = true;
            }
        }
        Ok(())
    }
}
