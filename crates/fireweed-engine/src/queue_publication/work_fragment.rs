//! Disposable one-work-command preparation. This reuses the publication command
//! writer/parser; these bytes have no frame, receipt or publication authority.
use super::*;

pub struct ResolvedWorkFragment<'r> {
    bytes: Vec<u8>,
    _charge: QueueBufferCharge<'r>,
}
impl ResolvedWorkFragment<'_> {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
/// An administrative row preserves insertion order and resolves against initial
/// live authority. The initial sequence is a fragment validation bound; ordinary
/// inserts may subsequently advance the containing group's final sequence.
pub fn encode_administrative_work_fragment<'r>(
    before: &RecordSnapshot,
    delta: &CommitDelta,
    reservation: &'r QueueReservation,
) -> PublicationResult<ResolvedWorkFragment<'r>> {
    if before.work.len() != 1
        || before.versions.len() != 1
        || !before.state.is_empty()
        || delta.versions.len() != 1
        || !delta.state.is_empty()
        || !delta.deleted_state.is_empty()
        || delta.next_sequence != before.next_sequence
        || delta.versions[0].collection.is_some()
    {
        return invalid("not one initial administrative work command");
    }
    validate_delta(before, delta)?;
    let version = &delta.versions[0];
    let prior = before
        .work
        .get(&version.key)
        .ok_or(PublicationError::Invalid(
            "administrative predecessor missing",
        ))?;
    let highwater = before
        .versions
        .iter()
        .next()
        .filter(|(id, _)| id.collection.is_none() && id.key == version.key)
        .map(|(_, value)| *value)
        .ok_or(PublicationError::Invalid(
            "administrative highwater missing",
        ))?;
    let row = delta.work.first().map(|row| {
        (
            row,
            &delta.work_fields[&version.key],
            &delta.work_bodies[&version.key],
        )
    });
    let size = measure_work_mutation(version, Some(prior.record.version), highwater, row)?;
    if size == 0 {
        return invalid("administrative fragment cannot be a no-op");
    }
    let charge = reservation
        .buffer(size.checked_add(128).ok_or(PublicationError::Overflow)?)
        .map_err(PublicationError::Admission)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(size).map_err(|_| PublicationError::Overflow)?)
        .map_err(|_| PublicationError::Overflow)?;
    let mut sink = Sink::Buffer(&mut bytes);
    write_mutation_header(&mut sink, version, Some(prior.record.version), highwater)?;
    if let Some((row, fields, body)) = row {
        write_work_value(&mut sink, row, fields, body)?;
    } else {
        sink.tag(0)?;
    }
    if bytes.len() as u64 != size {
        return invalid("work fragment size changed");
    }
    parse_administrative_work_fragment(&bytes, before.next_sequence)?;
    Ok(ResolvedWorkFragment {
        bytes,
        _charge: charge,
    })
}
