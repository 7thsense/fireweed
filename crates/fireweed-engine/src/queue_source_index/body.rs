//! Exact historical WorkPut membership for current authenticated body facts.
use super::*;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_group_state_authority::WorkMetadata,
    queue_log_record::{CheckedQueueLogRecord, QueueLogFormat, QueueLogPayload},
    queue_publication::{BodyCommandRef, EffectRef},
    queue_schema_codec::DefinitionPin,
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::{collections::BTreeMap, sync::Arc};
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source work body: {reason}"))
}
/// Full literal work identity and body descriptor from one authenticated source
/// point. Historical membership is checked separately; this is not fresh state
/// or permission to publish. The borrowed cut preserves the original operation.
pub struct QueueSourceWorkBodyEvidence<'a> {
    key: String,
    cut: QueueSourceCut<'a>,
    metadata: WorkMetadata,
    payload: QueueSourcePayload,
    _charge: OwnedQueueBufferCharge,
}
impl<'a> QueueSourceWorkBodyEvidence<'a> {
    pub fn from_source(
        cut: QueueSourceCut<'a>,
        key: &str,
        point: QueueSourcePoint<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<Self>> {
        if !cut.belongs_to(job) || key.trim().is_empty() {
            return Err(unknown("body declaration or operation differs"));
        }
        let source_key = QueueSourceKey::Record {
            collection: None,
            key,
        };
        let charge = job.owned_buffer(
            (source_key.encoded_len()? as u64)
                .checked_add(key.len() as u64)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let encoded = source_key.encode()?;
        let Some(raw) = point.value_for(&cut, &encoded)? else {
            return Ok(None);
        };
        let fact: QueueSourceRecord = crate::queue_storage_layout::decode(raw)?;
        fact.validate(None)?;
        let Some(QueueSourceLive::Work { metadata, body }) = fact.live else {
            return Ok(None);
        };
        if body.span().log_offset() >= cut.through() || metadata.insertion_sequence == 0 {
            return Err(unknown("body lies outside current source incarnation"));
        }
        Ok(Some(Self {
            key: key.into(),
            cut,
            metadata,
            payload: body,
            _charge: charge,
        }))
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn source_offset(&self) -> u64 {
        self.payload.span().log_offset()
    }
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.cut
    }
}
/// Borrowed bodies retain the actual checked frame; copies need independent
/// admission before the frame or this container owner can be retired.
pub struct QueueSourceVerifiedWorkBodies<'r> {
    bodies: Vec<&'r [u8]>,
    _charge: OwnedQueueBufferCharge,
}
impl<'r> QueueSourceVerifiedWorkBodies<'r> {
    pub fn body(&self, position: usize) -> QueueResult<&'r [u8]> {
        self.bodies
            .get(position)
            .copied()
            .ok_or_else(|| unknown("verified body position differs"))
    }
}
/// The record port establishes physical membership at `offset` under the SAME
/// original cut. This verifies every requested full-key WorkPut in one command
/// walk and rederives its entire descriptor, including equal-byte aliases.
pub fn verify_source_work_bodies<'r>(
    members: &[&QueueSourceWorkBodyEvidence<'_>],
    record: &CheckedQueueLogRecord<'r>,
    offset: u64,
    definition: DefinitionPin,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceVerifiedWorkBodies<'r>> {
    let first = members
        .first()
        .ok_or_else(|| unknown("empty body member set"))?;
    let charge = job.owned_buffer(
        (members.len() as u64)
            .checked_mul(1024)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let mut identities: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
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
        identities.entry(&member.key).or_default().push(position);
    }
    let QueueLogPayload::Publication(publication) = record.view().payload() else {
        return Err(unknown("body points at another record domain"));
    };
    let mut found = vec![None; members.len()];
    for command in publication.commands() {
        let command = command.map_err(|_| unknown("invalid historical command"))?;
        if command.identity.collection.is_some() {
            continue;
        }
        let Some(positions) = identities.get(command.identity.key) else {
            continue;
        };
        let EffectRef::WorkPut {
            metadata,
            body: BodyCommandRef::Replace(bytes),
            ..
        } = command.effect
        else {
            return Err(unknown("exact work key has no complete replacement body"));
        };
        let actual = QueueSourcePayload::from_checked(record, offset, bytes)?;
        let representative = members[positions[0]];
        for &position in positions {
            let expected = members[position];
            if found[position].is_some()
                || expected.payload != actual
                || expected.metadata.insertion_sequence != metadata.insertion_sequence
                || command.output_highwater > expected.metadata.version
                || expected.payload != representative.payload
                || expected.metadata != representative.metadata
            {
                return Err(unknown(
                    "exact work body span, incarnation or aliases differ",
                ));
            }
            found[position] = Some(bytes);
        }
        // from_checked hashes this full replacement exactly once; descriptor
        // equality above checks that digest for every alias without rehashing.
        actual.span().resolve(record, offset)?;
    }
    let bodies = found
        .into_iter()
        .map(|body| body.ok_or_else(|| unknown("literal work body missing")))
        .collect::<QueueResult<_>>()?;
    Ok(QueueSourceVerifiedWorkBodies {
        bodies,
        _charge: charge,
    })
}
