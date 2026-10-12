//! Shared exact unique-tuple ownership, rooted in the matching index row.
use super::*;
use fireweed_engine::queue_planner::{RecordIdentity, RecordSnapshot, UniqueIdentity};
use std::collections::BTreeSet;
pub(super) async fn load_unique_authority(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    probes: &BTreeSet<UniqueIdentity>,
    snapshot: &mut RecordSnapshot,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<()> {
    for probe in probes {
        let collection = collection_number(schema, probe.collection.as_deref())?;
        let definition = collection_definition(schema, collection)?;
        let index = index_number(definition, &probe.index)?;
        let digest = discovery_digest(&probe.full_sort_key);
        let params = vec![
            Value::Integer(queue_id),
            Value::Integer(i64::from(collection)),
            Value::Integer(i64::from(index)),
            blob(&digest),
            blob(&probe.full_sort_key),
        ];
        let metadata=one(connection,"SELECT CASE WHEN typeof(r.key)='blob' THEN length(r.key) ELSE -1 END,CASE WHEN typeof(r.id)='integer' THEN r.id ELSE NULL END,CASE WHEN typeof(r.queue_id)='integer' THEN r.queue_id ELSE NULL END,CASE WHEN typeof(r.collection)='integer' THEN r.collection ELSE NULL END,CASE WHEN typeof(r.active)='integer' THEN r.active ELSE NULL END FROM q7_indexes i LEFT JOIN q7_records r ON r.id=i.record_id WHERE i.queue_id=?1 AND i.collection=?2 AND i.index_no=?3 AND i.hash=?4 AND i.full_key=?5 AND i.is_unique=1",params.clone(),5).await?;
        if let Some(metadata) = metadata {
            if integer(&metadata[1])? <= 0
                || integer(&metadata[2])? != queue_id
                || integer(&metadata[3])? != i64::from(collection)
                || integer(&metadata[4])? != 1
            {
                return Err(unknown(
                    "unique index owner is missing or outside collection authority",
                ));
            }
            let length = u64::try_from(integer(&metadata[0])?).map_err(unknown)?;
            charges.push(
                job.owned_buffer(
                    length
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let row=one(connection,"SELECT r.key FROM q7_indexes i JOIN q7_records r ON r.id=i.record_id WHERE i.queue_id=?1 AND i.collection=?2 AND i.index_no=?3 AND i.hash=?4 AND i.full_key=?5 AND i.is_unique=1",params,1).await?.ok_or_else(||unknown("unique owner disappeared"))?;
            let key = std::str::from_utf8(bytes(&row[0])?)
                .map_err(unknown)?
                .to_owned();
            snapshot.unique_owners.insert(
                probe.clone(),
                RecordIdentity {
                    collection: probe.collection.clone(),
                    key,
                },
            );
        }
    }
    Ok(())
}
