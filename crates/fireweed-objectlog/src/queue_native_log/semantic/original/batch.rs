//! Exact-cut fixed facts first, then one owner per requested physical record.
use super::*;
use fireweed_engine::queue_source_index::{
    QueueSourceOriginalEvidence, QueueSourceOriginalObservation, load_source_original,
    verify_source_originals,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueNativeOriginalStatus {
    Absent,
    Conflict,
    Present,
}
enum State {
    Absent,
    Conflict,
    Present {
        record: usize,
        outcome: Range<usize>,
    },
}
struct Entry {
    request_id: String,
    domain: u8,
    observation: Option<QueueSourceOriginalObservation>,
    state: State,
}
/// This entire table belongs to one actual recovered capsule and operation.
/// A copied observation is continuity information, never cross-cut authority.
pub struct QueueNativeOriginalBatch<'a> {
    entries: Vec<Entry>,
    records: Vec<OwnedQueueLogRecord>,
    recovered: &'a QueueNativeRecovered,
    _charge: OwnedQueueBufferCharge,
}
pub struct QueueNativeOriginalView<'a> {
    entry: &'a Entry,
    record: Option<&'a OwnedQueueLogRecord>,
    recovered: &'a QueueNativeRecovered,
}
impl QueueNativeOriginalView<'_> {
    pub fn recovered(&self) -> &QueueNativeRecovered {
        self.recovered
    }
    pub fn domain(&self) -> u8 {
        self.entry.domain
    }
    pub fn request_id(&self) -> &str {
        &self.entry.request_id
    }
    pub fn observation(&self) -> Option<QueueSourceOriginalObservation> {
        self.entry.observation
    }
    pub fn status(&self) -> QueueNativeOriginalStatus {
        match self.entry.state {
            State::Absent => QueueNativeOriginalStatus::Absent,
            State::Conflict => QueueNativeOriginalStatus::Conflict,
            State::Present { .. } => QueueNativeOriginalStatus::Present,
        }
    }
    /// Historical outcomes only: domain 1 contains FWCR1 lease entries, not
    /// current claim eligibility or permission to acknowledge a usable lease.
    pub fn outcome_bytes(&self) -> QueueResult<Option<&[u8]>> {
        match &self.entry.state {
            State::Absent => Ok(None),
            State::Conflict => Err(QueueError::RequestConflict),
            State::Present { outcome, .. } => self
                .record
                .and_then(|r| r.bytes().get(outcome.clone()))
                .map(Some)
                .ok_or_else(|| unknown("verified original owner or interval differs")),
        }
    }
}
impl QueueNativeOriginalBatch<'_> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn original(&self, position: usize) -> QueueResult<QueueNativeOriginalView<'_>> {
        let entry = self
            .entries
            .get(position)
            .ok_or_else(|| unknown("original position differs"))?;
        let record = match entry.state {
            State::Present { record, .. } => Some(
                self.records
                    .get(record)
                    .ok_or_else(|| unknown("original record index differs"))?,
            ),
            _ => None,
        };
        Ok(QueueNativeOriginalView {
            entry,
            record,
            recovered: self.recovered,
        })
    }
}
struct Classified<'a> {
    entries: Vec<Entry>,
    evidence: Vec<QueueSourceOriginalEvidence<'a>>,
    groups: BTreeMap<u64, Vec<usize>>,
    charge: OwnedQueueBufferCharge,
}
type ExpectedFingerprint<'a> = Option<(u8, &'a [u8; 32])>;
type OriginalRequestParts<'a> = (u8, &'a str, ExpectedFingerprint<'a>);

pub(super) fn request_parts(
    request: QueueNativeOriginalRequest<'_>,
) -> QueueResult<OriginalRequestParts<'_>> {
    let (domain, id, expected) = match request {
        QueueNativeOriginalRequest::Replay {
            domain,
            request_id,
            fingerprint_format,
            fingerprint,
        } => {
            if !(fingerprint_format == 3 || (domain == 2 && fingerprint_format == 4)) {
                return Err(QueueError::Invalid(
                    "invalid expected fingerprint format".into(),
                ));
            }
            (domain, request_id, Some((fingerprint_format, fingerprint)))
        }
        QueueNativeOriginalRequest::Lookup { domain, request_id } => (domain, request_id, None),
    };
    if domain > 2 || id.trim().is_empty() {
        return Err(QueueError::Invalid(
            "invalid original domain or literal ID".into(),
        ));
    }
    Ok((domain, id, expected))
}
async fn classify<'a, O>(
    reader: &'a QueueNativeSourceReader<'_>,
    requests: &[QueueNativeOriginalRequest<'_>],
    job: &Arc<QueueReservation>,
    conversion_containers: u64,
    mut observe: O,
) -> QueueResult<Classified<'a>>
where
    O: for<'v> FnMut(&'v Entry) -> QueueResult<()> + Send,
{
    let mut bytes = (requests.len() as u64)
        .checked_mul(2048)
        .and_then(|n| n.checked_add(8192))
        .and_then(|n| n.checked_add(conversion_containers))
        .ok_or(QueueError::TooLarge)?;
    // Validate every declaration and fund all containers/identities before F I/O.
    for request in requests {
        let (_, id, _) = request_parts(*request)?;
        bytes = bytes
            .checked_add(id.len() as u64)
            .ok_or(QueueError::TooLarge)?;
    }
    let charge = job.owned_buffer(bytes)?;
    let mut entries = Vec::with_capacity(requests.len());
    let mut evidence = Vec::with_capacity(requests.len());
    let mut groups: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (position, request) in requests.iter().enumerate() {
        let (domain, id, expected) = request_parts(*request)?;
        let member = load_source_original(reader, domain, id, job).await?;
        let conflict = match expected
            .map(|(f, p)| member.check_fingerprint(f, p))
            .transpose()
        {
            Ok(_) => false,
            Err(QueueError::RequestConflict) => true,
            Err(error) => return Err(error),
        };
        let state = match (member.source_offset(), conflict) {
            (None, _) => State::Absent,
            (Some(_), true) => State::Conflict,
            (Some(offset), false) => {
                groups.entry(offset).or_default().push(position);
                State::Present {
                    record: usize::MAX,
                    outcome: 0..0,
                }
            }
        };
        let entry = Entry {
            request_id: member.request_id().to_owned(),
            domain: member.domain(),
            observation: member.observation(),
            state,
        };
        // Report validated fixed metadata before the next F read. This cannot
        // expose an outcome or claim membership in a physical C receipt.
        observe(&entry)?;
        entries.push(entry);
        evidence.push(member);
    }
    Ok(Classified {
        entries,
        evidence,
        groups,
        charge,
    })
}
impl QueueNativeRecovered {
    /// Classify all logical requests before any C read. Matching or Lookup
    /// members share physical owners, while conflicts never expose an outcome.
    /// Distinct retained frames each keep their full C charge, not span credit.
    pub async fn resolve_original_batch<'a>(
        &'a self,
        log: &QueueNativeLog,
        requests: &[QueueNativeOriginalRequest<'_>],
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeOriginalBatch<'a>> {
        self.as_source().semantic_scope(log, job)?;
        self.budget().scope(job)?;
        limits.read.validate()?;
        limits.record.validate()?;
        let reader = QueueNativeSourceReader::new(
            self.as_source(),
            log,
            job,
            self.budget().clone(),
            limits.read,
        )?;
        // Keep the struct intact across fallible awaits: its guard is last,
        // so every charged allocation retires before admission is released.
        let mut classified = classify(&reader, requests, job, 0, |_| Ok(())).await?;
        let mut records = Vec::with_capacity(classified.groups.len());
        for (offset, positions) in &classified.groups {
            let record = self
                .as_source()
                .read_source_record(log, *offset, limits.record, self.budget(), job)
                .await?;
            verify_group(
                &record,
                (*offset, positions),
                &classified.evidence,
                &mut classified.entries,
                records.len(),
                self.definition().pin(),
                job,
            )?;
            records.push(record);
        }
        Ok(QueueNativeOriginalBatch {
            entries: classified.entries,
            records,
            recovered: self,
            _charge: classified.charge,
        })
    }
}

fn verify_group(
    record: &OwnedQueueLogRecord,
    group: (u64, &[usize]),
    evidence: &[QueueSourceOriginalEvidence<'_>],
    entries: &mut [Entry],
    record_index: usize,
    pin: fireweed_engine::queue_schema_codec::DefinitionPin,
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    let (offset, positions) = group;
    let checked = record
        .checked()
        .map_err(|e| unknown(format!("invalid original record: {e:?}")))?;
    let members: Vec<_> = positions.iter().map(|&p| &evidence[p]).collect();
    let verified = verify_source_originals(&members, &checked, offset, pin, job)?;
    for (member, &position) in positions.iter().enumerate() {
        let raw = verified.outcome(member)?;
        let start = raw
            .as_ptr()
            .addr()
            .checked_sub(record.bytes().as_ptr().addr())
            .ok_or_else(|| unknown("verified original outcome start differs"))?;
        let end = start.checked_add(raw.len()).ok_or(QueueError::TooLarge)?;
        if end > record.bytes().len() {
            return Err(unknown("verified original outcome end differs"));
        }
        entries[position].state = State::Present {
            record: record_index,
            outcome: start..end,
        };
    }
    Ok(())
}
/// Metadata borrows the exact recovered capsule. Copied observations only detect
/// continuity violations; they cannot authorize absence or publication.
pub struct QueueNativeOriginalIdentity<'a> {
    entry: &'a Entry,
    recovered: &'a QueueNativeRecovered,
}
impl QueueNativeOriginalIdentity<'_> {
    pub fn recovered(&self) -> &QueueNativeRecovered {
        self.recovered
    }
    pub fn domain(&self) -> u8 {
        self.entry.domain
    }
    pub fn request_id(&self) -> &str {
        &self.entry.request_id
    }
    pub fn observation(&self) -> Option<QueueSourceOriginalObservation> {
        self.entry.observation
    }
    pub fn status(&self) -> QueueNativeOriginalStatus {
        match self.entry.state {
            State::Absent => QueueNativeOriginalStatus::Absent,
            State::Conflict => QueueNativeOriginalStatus::Conflict,
            State::Present { .. } => QueueNativeOriginalStatus::Present,
        }
    }
}
/// Conversion is provisional until the caller confirms freshness. Values own
/// their own heap admission; this owner funds the returned containers/identities.
/// No source record remains retained after per-offset conversion.
pub struct QueueNativeConvertedOriginals<'a, T> {
    values: Vec<T>,
    entries: Vec<Entry>,
    recovered: &'a QueueNativeRecovered,
    charge: OwnedQueueBufferCharge,
}
impl<T> QueueNativeConvertedOriginals<'_, T> {
    pub fn values(&self) -> &[T] {
        &self.values
    }
    pub fn identity(&self, position: usize) -> QueueResult<QueueNativeOriginalIdentity<'_>> {
        let entry = self
            .entries
            .get(position)
            .ok_or_else(|| unknown("original position differs"))?;
        Ok(QueueNativeOriginalIdentity {
            entry,
            recovered: self.recovered,
        })
    }
    /// Retain the returned admission owner for as long as the moved containers
    /// survive. T must independently own all copied DTO heap and its funding.
    /// This transfers data, never a source-readiness or publication proof.
    pub fn into_parts(self) -> (Vec<T>, OwnedQueueBufferCharge) {
        (self.values, self.charge)
    }
}
impl QueueNativeRecovered {
    /// Read each distinct physical frame once, verify its requested members in
    /// one stream walk, convert into independently owned provisional values, then
    /// retire the frame before the next read. The callback cannot borrow output
    /// from a source frame. Any late failure discards every provisional value;
    /// physical work remains spent. No callback or successful return is an ACK.
    /// Callbacks run for absence/conflict first, then by physical offset; only
    /// the returned values use declaration order. Callbacks must remain pure
    /// provisional conversion, not execute work or acknowledge it.
    pub async fn convert_original_batch<'a, T: Send, F>(
        &'a self,
        log: &QueueNativeLog,
        requests: &[QueueNativeOriginalRequest<'_>],
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
        convert: F,
    ) -> QueueResult<QueueNativeConvertedOriginals<'a, T>>
    where
        F: for<'v> FnMut(QueueNativeOriginalView<'v>) -> QueueResult<T> + Send,
    {
        self.convert_original_batch_observing(log, requests, limits, job, |_| Ok(()), convert)
            .await
    }

    /// Preserve fixed-fact continuity even if a later F/C read or conversion
    /// fails. The synchronous observer runs once per declaration in caller order
    /// after its fixed fact and fingerprint status validate, before the next F.
    /// Identity borrows cannot escape. Present here means fixed F presence ONLY;
    /// no outcome, freshness, publication or ACK authority is exposed.
    /// Observers may update preadmitted continuity state; their errors propagate
    /// directly and callers must never retry away a continuity contradiction.
    /// All declarations validate before I/O, and ALL F reads finish before any
    /// C read or conversion. Failure does not roll back earlier observations.
    pub async fn convert_original_batch_observing<'a, T: Send, O, F>(
        &'a self,
        log: &QueueNativeLog,
        requests: &[QueueNativeOriginalRequest<'_>],
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
        mut observe: O,
        mut convert: F,
    ) -> QueueResult<QueueNativeConvertedOriginals<'a, T>>
    where
        O: for<'v> FnMut(QueueNativeOriginalIdentity<'v>) -> QueueResult<()> + Send,
        F: for<'v> FnMut(QueueNativeOriginalView<'v>) -> QueueResult<T> + Send,
    {
        self.as_source().semantic_scope(log, job)?;
        self.budget().scope(job)?;
        limits.read.validate()?;
        limits.record.validate()?;
        let containers = (std::mem::size_of::<T>() as u64)
            .checked_add(std::mem::size_of::<Option<T>>() as u64)
            .and_then(|n| n.checked_mul(requests.len() as u64))
            .ok_or(QueueError::TooLarge)?;
        let reader = QueueNativeSourceReader::new(
            self.as_source(),
            log,
            job,
            self.budget().clone(),
            limits.read,
        )?;
        // Keep the struct intact across fallible awaits: its guard is last,
        // so every charged allocation retires before admission is released.
        let mut classified = classify(&reader, requests, job, containers, |entry| {
            observe(QueueNativeOriginalIdentity {
                entry,
                recovered: self,
            })
        })
        .await?;
        let mut values: Vec<Option<T>> = (0..requests.len()).map(|_| None).collect();
        for (position, entry) in classified.entries.iter().enumerate() {
            if !matches!(entry.state, State::Present { .. }) {
                values[position] = Some(convert(QueueNativeOriginalView {
                    entry,
                    record: None,
                    recovered: self,
                })?);
            }
        }
        for (offset, positions) in &classified.groups {
            let record = self
                .as_source()
                .read_source_record(log, *offset, limits.record, self.budget(), job)
                .await?;
            verify_group(
                &record,
                (*offset, positions),
                &classified.evidence,
                &mut classified.entries,
                0,
                self.definition().pin(),
                job,
            )?;
            for &position in positions {
                values[position] = Some(convert(QueueNativeOriginalView {
                    entry: &classified.entries[position],
                    record: Some(&record),
                    recovered: self,
                })?);
            }
            drop(record);
        }
        let values = values
            .into_iter()
            .map(|v| v.ok_or_else(|| unknown("original conversion position missing")))
            .collect::<QueueResult<Vec<_>>>()?;
        Ok(QueueNativeConvertedOriginals {
            values,
            entries: classified.entries,
            recovered: self,
            charge: classified.charge,
        })
    }
}
