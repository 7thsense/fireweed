//! Exact historical StatePut membership for current authenticated state facts.
use super::*;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{CheckedQueueLogRecord, QueueLogFormat, QueueLogPayload},
    queue_publication::EffectRef,
    queue_schema_codec::DefinitionPin,
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::{collections::BTreeMap, sync::Arc};
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source state value: {reason}"))
}
/// Full literal identity and current version from one authenticated point.
/// Historical membership and freshness are separate checks; this is not a DTO.
pub struct QueueSourceStateValueEvidence<'a> {
    collection: String,
    key: String,
    cut: QueueSourceCut<'a>,
    highwater: u64,
    field_count: u64,
    payload: QueueSourcePayload,
    _charge: OwnedQueueBufferCharge,
}
impl<'a> QueueSourceStateValueEvidence<'a> {
    pub fn from_source(
        cut: QueueSourceCut<'a>,
        collection: &str,
        key: &str,
        point: QueueSourcePoint<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<Self>> {
        if !cut.belongs_to(job) || collection.trim().is_empty() || key.trim().is_empty() {
            return Err(unknown("value declaration or operation differs"));
        }
        let source_key = QueueSourceKey::Record {
            collection: Some(collection),
            key,
        };
        let charge = job.owned_buffer(
            (source_key.encoded_len()? as u64)
                .checked_add(collection.len() as u64)
                .and_then(|n| n.checked_add(key.len() as u64))
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let encoded = source_key.encode()?;
        let Some(raw) = point.value_for(&cut, &encoded)? else {
            return Ok(None);
        };
        let fact: QueueSourceRecord = crate::queue_storage_layout::decode(raw)?;
        fact.validate(Some(collection))?;
        let Some(QueueSourceLive::State { value }) = fact.live else {
            return Ok(None);
        };
        if value.span().log_offset() == 0 || value.span().log_offset() >= cut.through() {
            return Err(unknown("value lies outside captured source"));
        }
        Ok(Some(Self {
            collection: collection.into(),
            key: key.into(),
            cut,
            highwater: fact.highwater,
            field_count: fact.field_count,
            payload: value,
            _charge: charge,
        }))
    }
    pub fn collection(&self) -> &str {
        &self.collection
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn version(&self) -> u64 {
        self.highwater
    }
    pub fn field_count(&self) -> u64 {
        self.field_count
    }
    pub fn source_offset(&self) -> u64 {
        self.payload.span().log_offset()
    }
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.cut
    }
}
/// Values borrow the retained checked record. DTO copies need their own charge.
pub struct QueueSourceVerifiedStateValues<'r> {
    values: Vec<&'r [u8]>,
    _charge: OwnedQueueBufferCharge,
}
impl<'r> QueueSourceVerifiedStateValues<'r> {
    pub fn value(&self, position: usize) -> QueueResult<&'r [u8]> {
        self.values
            .get(position)
            .copied()
            .ok_or_else(|| unknown("verified value position differs"))
    }
}
/// Validate every requested full literal StatePut in one command walk. Unlike
/// Work Preserve bodies, the historical output version must equal the live HWM.
/// The record port separately establishes actual physical membership at offset.
pub fn verify_source_state_values<'r>(
    members: &[&QueueSourceStateValueEvidence<'_>],
    record: &CheckedQueueLogRecord<'r>,
    offset: u64,
    definition: DefinitionPin,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceVerifiedStateValues<'r>> {
    let first = members
        .first()
        .ok_or_else(|| unknown("empty value member set"))?;
    let charge = job.owned_buffer(
        (members.len() as u64)
            .checked_mul(1024)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let mut identities: BTreeMap<(&str, &str), Vec<usize>> = BTreeMap::new();
    for (position, member) in members.iter().enumerate() {
        if !member.cut.same_cut(&first.cut)
            || !member.cut.belongs_to(job)
            || !record.owner().belongs_to(job)
            || record.view().format() != QueueLogFormat::ChecksummedV2
            || record.view().address() != member.cut.address()
            || record.view().epoch() > member.cut.epoch()
            || record.view().definition_pin_bytes() != *definition.as_bytes()
            || offset >= member.cut.through()
            || member.source_offset() != offset
        {
            return Err(unknown(
                "historical record, cut, job, offset or definition differs",
            ));
        }
        identities
            .entry((&member.collection, &member.key))
            .or_default()
            .push(position);
    }
    let QueueLogPayload::Publication(publication) = record.view().payload() else {
        return Err(unknown("value points at another record domain"));
    };
    let mut found = vec![None; members.len()];
    for command in publication.commands() {
        let command = command.map_err(|_| unknown("invalid historical command"))?;
        let Some(collection) = command.identity.collection else {
            continue;
        };
        let Some(positions) = identities.get(&(collection, command.identity.key)) else {
            continue;
        };
        let EffectRef::StatePut { value, .. } = command.effect else {
            return Err(unknown(
                "exact state identity has no complete replacement value",
            ));
        };
        let actual = QueueSourcePayload::from_checked(record, offset, value)?;
        let representative = members[positions[0]];
        for &position in positions {
            let expected = members[position];
            if found[position].is_some()
                || expected.payload != actual
                || command.output_highwater != expected.highwater
                || expected.payload != representative.payload
                || expected.highwater != representative.highwater
            {
                return Err(unknown("exact state value span, version or aliases differ"));
            }
            found[position] = Some(value);
        }
        actual.span().resolve(record, offset)?;
    }
    let values = found
        .into_iter()
        .map(|value| value.ok_or_else(|| unknown("literal state value missing")))
        .collect::<QueueResult<_>>()?;
    Ok(QueueSourceVerifiedStateValues {
        values,
        _charge: charge,
    })
}
