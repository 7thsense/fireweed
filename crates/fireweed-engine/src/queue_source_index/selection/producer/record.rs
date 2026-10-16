//! Same-cut live metadata shared by scalar and universe leaves.
use super::*;
#[allow(clippy::too_many_arguments)]
pub(super) async fn live_record<R: QueueSourceStreamingReader>(
    reader: &R,
    original: QueueSourceCut<'_>,
    header: &QueueSourceHeader,
    collection: Option<&str>,
    record_key: &str,
    job: &Arc<QueueReservation>,
    budget: &QueueSourceSelectionBudget,
) -> QueueResult<QueueSourceRecord> {
    scope(reader, original, job)?;
    let key = QueueSourceKey::Record {
        collection,
        key: record_key,
    };
    let _key_memory = job.owned_buffer(key.encoded_len()? as u64)?;
    budget.grant(
        job,
        QueueSourceSelectionWork {
            rows: 1,
            copied_bytes: key.encoded_len()? as u64,
            scratch_steps: 0,
        },
    )?;
    let key = key.encode()?;
    let owner = reader.lookup(&key).await?;
    let point = reader.point(&owner)?;
    let bytes = point
        .value_for(&original, &key)?
        .ok_or_else(|| unknown("posting has no record fact"))?;
    budget.grant(
        job,
        QueueSourceSelectionWork {
            copied_bytes: std::mem::size_of::<QueueSourceRecord>() as u64,
            ..Default::default()
        },
    )?;
    let record: QueueSourceRecord = queue_storage_layout::decode(bytes)?;
    record.validate(collection)?;
    let span = match record.live {
        Some(QueueSourceLive::Work { metadata, body }) => {
            if metadata.insertion_sequence > header.sequence() {
                return Err(unknown("posting has future insertion sequence"));
            }
            body.span()
        }
        Some(QueueSourceLive::State { value }) => value.span(),
        None => return Err(unknown("posting refers to a tombstone")),
    };
    if span.log_offset() == 0 || span.log_offset() >= original.through() {
        return Err(unknown(
            "posting record field count or history offset differs",
        ));
    }
    drop(owner);
    Ok(record)
}
