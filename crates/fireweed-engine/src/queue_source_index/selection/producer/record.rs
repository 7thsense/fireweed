//! Same-cut metadata points; postings require a live fact, explicit Keys may not.
use super::*;
#[allow(clippy::too_many_arguments)]
pub(super) async fn record_at<R: QueueSourceStreamingReader>(
    reader: &R,
    original: QueueSourceCut<'_>,
    header: &QueueSourceHeader,
    collection: Option<&str>,
    record_key: &str,
    job: &Arc<QueueReservation>,
    budget: &QueueSourceSelectionBudget,
) -> QueueResult<Option<QueueSourceRecord>> {
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
    let Some(bytes) = point.value_for(&original, &key)? else {
        return Ok(None);
    };
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
        // Authenticate and validate the tombstone before classifying it. Its
        // highwater remains meaningful even though there is no live payload.
        None => return Ok(Some(record)),
    };
    if span.log_offset() == 0 || span.log_offset() >= original.through() {
        return Err(unknown(
            "posting record field count or history offset differs",
        ));
    }
    drop(owner);
    Ok(Some(record))
}

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
    let record = record_at(
        reader, original, header, collection, record_key, job, budget,
    )
    .await?
    .ok_or_else(|| unknown("posting has no record fact"))?;
    if record.live.is_none() {
        return Err(unknown("posting refers to a tombstone"));
    }
    Ok(record)
}
