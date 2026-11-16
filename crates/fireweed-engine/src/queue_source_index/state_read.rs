//! Addressed state DTOs from one original source cut, without projection access.
use super::*;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_contract::OwnedQueueValue,
    queue_publication::IdentityRef,
    queue_schema_codec::AdmittedQueueDefinition,
};
use fireweed_core::queue_contract::*;
use std::{collections::BTreeMap, sync::Arc};
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source state read: {reason}"))
}
fn scope<R: QueueSourceReader>(
    reader: &R,
    original: QueueSourceCut<'_>,
    job: &QueueReservation,
) -> QueueResult<()> {
    if !original.belongs_to(job) || !reader.cut()?.same_cut(&original) {
        return Err(unknown("original source or operation differs"));
    }
    Ok(())
}
struct Member<'a> {
    evidence: QueueSourceStateValueEvidence<'a>,
    fields: QueueSourceFields,
}
/// Every full literal identity is looked up once, and every historical physical
/// offset is read once. All values/fields are complete before response sizing.
/// The adapter retains this original capture and confirms freshness afterward.
pub async fn load_source_state_records<R: QueueSourceRecordReader>(
    reader: &R,
    original: QueueSourceCut<'_>,
    request: &StateReadRequest,
    definition: &AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueValue<Vec<Option<StateRecord>>, Vec<OwnedQueueBufferCharge>>> {
    request.validate(definition.schema(), limits)?;
    scope(reader, original, job)?;
    if !definition.belongs_to(job)
        || original.address()
            != (crate::queue_namespace::AddressRef {
                tenant: &request.queue.tenant,
                queue: &request.queue.queue,
            })
    {
        return Err(unknown("queue or definition operation differs"));
    }
    // Admit all request-sized maps, group vectors and output slots before any
    // source dispatch; field and payload allocations receive separate funding.
    let capacity = encoded_len(request)?
        .checked_mul(16)
        .and_then(|n| n.checked_add((request.keys.len() as u64).checked_mul(2048)?))
        .and_then(|n| {
            n.checked_add(
                (request.collection.len() as u64)
                    .checked_mul(request.keys.len() as u64)?
                    .checked_mul(4)?,
            )
        })
        .and_then(|n| n.checked_add(4096))
        .ok_or(QueueError::TooLarge)?;
    let mut charges = vec![job.owned_buffer(capacity)?];
    let header = super::header::load_source_header_at(reader, original, job).await?;
    if header.genesis().definition_pin != definition.pin() {
        return Err(unknown("persisted definition differs"));
    }
    let mut members = BTreeMap::<&str, Option<Member<'_>>>::new();
    let collection = &definition.schema().state[&request.collection];
    for key in &request.keys {
        if members.contains_key(key.as_str()) {
            continue;
        }
        scope(reader, original, job)?;
        let declaration = QueueSourceKey::Record {
            collection: Some(&request.collection),
            key,
        };
        let _key_charge = job.owned_buffer(declaration.encoded_len()? as u64)?;
        let encoded = declaration.encode()?;
        let owner = reader.lookup(&encoded).await?;
        let evidence = QueueSourceStateValueEvidence::from_source(
            original,
            &request.collection,
            key,
            reader.point(&owner)?,
            job,
        )?;
        drop(owner);
        let member = if let Some(evidence) = evidence {
            let fields = super::fields::load_group_fields(
                reader,
                original,
                IdentityRef {
                    collection: Some(&request.collection),
                    key,
                },
                evidence.field_count(),
                collection,
                None,
                job,
            )
            .await?;
            scope(reader, original, job)?;
            Some(Member { evidence, fields })
        } else {
            None
        };
        members.insert(key, member);
    }
    let mut groups = BTreeMap::<u64, Vec<&Member<'_>>>::new();
    for member in members.values().flatten() {
        groups
            .entry(member.evidence.source_offset())
            .or_default()
            .push(member);
    }
    let mut rows = BTreeMap::<&str, StateRecord>::new();
    for (offset, members) in groups {
        let owner = reader.read_record(&original, offset).await?;
        let checked = reader.checked_record(&owner, &original, offset)?;
        let evidence: Vec<_> = members.iter().map(|member| &member.evidence).collect();
        let verified =
            verify_source_state_values(&evidence, &checked, offset, definition.pin(), job)?;
        for (position, member) in members.iter().enumerate() {
            let raw = verified.value(position)?;
            let fields = member.fields.complete_fields()?;
            let capacity = encoded_len(fields)?
                .checked_add(raw.len() as u64)
                .and_then(|n| n.checked_add(request.collection.len() as u64))
                .and_then(|n| n.checked_add(member.evidence.key().len() as u64))
                .and_then(|n| n.checked_mul(16))
                .and_then(|n| {
                    n.checked_add(crate::queue_memory::record_headers(1, fields.len()).ok()?)
                })
                .ok_or(QueueError::TooLarge)?;
            charges.push(job.owned_buffer(capacity)?);
            rows.insert(
                member.evidence.key(),
                StateRecord {
                    collection: request.collection.clone(),
                    key: member.evidence.key().into(),
                    version: member.evidence.version(),
                    fields: fields.clone(),
                    value: raw.to_vec().into(),
                },
            );
        }
    }
    let mut records = Vec::with_capacity(request.keys.len());
    for key in &request.keys {
        let row = rows.get(key.as_str());
        if let Some(row) = row {
            let capacity = encoded_len(row)?
                .checked_mul(16)
                .and_then(|n| {
                    n.checked_add(crate::queue_memory::record_headers(1, row.fields.len()).ok()?)
                })
                .ok_or(QueueError::TooLarge)?;
            charges.push(job.owned_buffer(capacity)?);
        }
        records.push(row.cloned());
    }
    scope(reader, original, job)?;
    if encoded_len(&records)? > request.max_bytes {
        return Err(QueueError::TooLarge);
    }
    Ok(OwnedQueueValue::new(records, charges))
}
