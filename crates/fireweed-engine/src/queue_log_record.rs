//! One raw API-007 authoritative record per logical log offset. Borrowed parsing
//! verifies the FWQL2 checksum and structure; other versions are refused.
//! Genesis schema semantics require admitted decode.
//! Epoch fences a writer. It is neither insertion order nor an apply cursor.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_claim_publication::{
        ClaimPublicationView, OwnedClaimPublication, view_claim_publication,
    },
    queue_namespace::{AddressRef, encode_address, framed_address_len, view_address},
    queue_publication::{PublicationView, view_resolved_publication},
    queue_schema_codec::{AdmittedQueueDefinition, DefinitionPin},
};
use bytes::Bytes;
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
use sha2::{Digest, Sha256};
use std::sync::Arc;
mod bounds;
pub use bounds::QueueLogRecordBounds;
const RECORD_HEADER_BYTES: usize = 5 + 8 + 1 + 8;
const MAGIC: &[u8; 5] = b"FWQL2";
/// Checksummed framing detects corruption of this complete record, not
/// authenticated provenance or substitution of a different valid history.
const TRAILER_BYTES: usize = 32;
const GENESIS: u8 = 0;
const PUBLICATION: u8 = 1;
const CLAIM: u8 = 2;
const ENTRIES: u8 = 3;
const RETENTION: u8 = 4;
/// Bytes of an entry-list record that do not depend on its entries: the record
/// header, the checksum trailer and the entry count.
pub const ENTRIES_RECORD_FIXED_BYTES: u64 = (RECORD_HEADER_BYTES + TRAILER_BYTES) as u64 + 8;
/// Bytes each entry adds around its frame: a kind byte and a length.
pub const ENTRY_FRAMING_BYTES: u64 = 9;
fn invalid(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}

/// Structurally checked genesis. The cursor secret must never be logged or
/// replaced by a provision loser. Projection must validate definition semantics.
pub struct GenesisView<'a> {
    address: AddressRef<'a>,
    definition: &'a [u8],
    cursor_secret: &'a [u8; 32],
}
impl<'a> GenesisView<'a> {
    pub fn address(&self) -> AddressRef<'a> {
        self.address
    }
    pub fn definition_bytes(&self) -> &'a [u8] {
        self.definition
    }
    pub fn definition_pin(&self) -> DefinitionPin {
        DefinitionPin::from_persisted_bytes(self.definition)
    }
    pub fn cursor_secret(&self) -> &'a [u8; 32] {
        self.cursor_secret
    }
    pub fn validate_definition(
        &self,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueueDefinition> {
        AdmittedQueueDefinition::from_raw(self.definition, job)
    }
}
pub enum QueueLogPayload<'a> {
    Genesis(GenesisView<'a>),
    Publication(PublicationView<'a>),
    Claim(ClaimPublicationView<'a>),
    /// One or more chained entries that share one log offset.
    Entries(EntriesView<'a>),
    /// A cleanup pass. It changes no insertion sequence and no live record.
    Retention(RetentionView<'a>),
}
/// One logged cleanup pass: the store time `at` the horizon is computed from,
/// and the most rows applying it removes. Applying it reads no clock, so every
/// rebuild of the log reaches the same horizon and the same rows.
pub struct RetentionView<'a> {
    address: AddressRef<'a>,
    definition_pin: &'a [u8; 32],
    at: i64,
    budget: u64,
}
impl<'a> RetentionView<'a> {
    pub fn address(&self) -> AddressRef<'a> {
        self.address
    }
    pub fn definition_pin(&self) -> &'a [u8; 32] {
        self.definition_pin
    }
    pub fn at(&self) -> i64 {
        self.at
    }
    pub fn budget(&self) -> u64 {
        self.budget
    }
}
/// What an entry of an entry-list record holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueLogEntryKind {
    Publication,
    Claim,
}
impl QueueLogEntryKind {
    fn tag(self) -> u8 {
        match self {
            Self::Publication => PUBLICATION,
            Self::Claim => CLAIM,
        }
    }
}
/// One structurally checked entry: an ordinary publication frame or a claim.
pub enum QueueLogEntryView<'a> {
    Publication(PublicationView<'a>),
    Claim(ClaimPublicationView<'a>),
}
impl<'a> QueueLogEntryView<'a> {
    pub fn kind(&self) -> QueueLogEntryKind {
        match self {
            Self::Publication(_) => QueueLogEntryKind::Publication,
            Self::Claim(_) => QueueLogEntryKind::Claim,
        }
    }
    /// The publication frame, which a claim wraps.
    pub fn frame(&self) -> &PublicationView<'a> {
        match self {
            Self::Publication(frame) => frame,
            Self::Claim(claim) => claim.commands(),
        }
    }
    pub fn claim(&self) -> Option<&ClaimPublicationView<'a>> {
        match self {
            Self::Claim(claim) => Some(claim),
            Self::Publication(_) => None,
        }
    }
}
/// The entries of one record, in the order they apply. All describe the same
/// queue and definition, and each starts at the sequence the previous one ended.
pub struct EntriesView<'a> {
    entries: Vec<QueueLogEntryView<'a>>,
}
impl<'a> EntriesView<'a> {
    pub fn entries(&self) -> &[QueueLogEntryView<'a>] {
        &self.entries
    }
    pub fn first(&self) -> &QueueLogEntryView<'a> {
        &self.entries[0]
    }
    pub fn last(&self) -> &QueueLogEntryView<'a> {
        &self.entries[self.entries.len() - 1]
    }
}
pub struct QueueLogRecordView<'a> {
    epoch: u64,
    payload: QueueLogPayload<'a>,
}
impl<'a> QueueLogRecordView<'a> {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn payload(&self) -> &QueueLogPayload<'a> {
        &self.payload
    }
    pub fn address(&self) -> AddressRef<'a> {
        match &self.payload {
            QueueLogPayload::Genesis(genesis) => genesis.address(),
            QueueLogPayload::Claim(claim) => AddressRef {
                tenant: claim.commands().summary().queue.0,
                queue: claim.commands().summary().queue.1,
            },
            QueueLogPayload::Publication(publication) => AddressRef {
                tenant: publication.summary().queue.0,
                queue: publication.summary().queue.1,
            },
            QueueLogPayload::Entries(entries) => AddressRef {
                tenant: entries.first().frame().summary().queue.0,
                queue: entries.first().frame().summary().queue.1,
            },
            QueueLogPayload::Retention(retention) => retention.address,
        }
    }
    pub fn definition_pin_bytes(&self) -> [u8; 32] {
        match &self.payload {
            QueueLogPayload::Genesis(genesis) => *genesis.definition_pin().as_bytes(),
            QueueLogPayload::Claim(claim) => claim
                .commands()
                .summary()
                .definition_pin
                .try_into()
                .expect("validated claim pin"),
            QueueLogPayload::Publication(publication) => publication
                .summary()
                .definition_pin
                .try_into()
                .expect("validated publication pin"),
            QueueLogPayload::Entries(entries) => entries
                .first()
                .frame()
                .summary()
                .definition_pin
                .try_into()
                .expect("validated entry pin"),
            QueueLogPayload::Retention(retention) => *retention.definition_pin,
        }
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> QueueResult<&'a [u8]> {
        let bytes = self
            .0
            .get(..len)
            .ok_or_else(|| invalid("truncated queue-log record"))?;
        self.0 = &self.0[len..];
        Ok(bytes)
    }
    fn u64(&mut self) -> QueueResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| invalid("queue-log integer"))?,
        ))
    }
    fn blob(&mut self) -> QueueResult<&'a [u8]> {
        let len = usize::try_from(self.u64()?).map_err(|_| invalid("queue-log length overflow"))?;
        self.take(len)
    }
}
/// Complete borrowed structure validation and checksum verification. Only FWQL2
/// is accepted. Genesis uniqueness, schema semantics,
/// predecessor/receipt authority and atomic external cursor are store obligations.
pub fn view_queue_log_record(raw: &[u8]) -> QueueResult<QueueLogRecordView<'_>> {
    if raw.get(..5) != Some(MAGIC.as_slice()) {
        return Err(invalid("unsupported queue-log record version"));
    }
    if raw.len() < RECORD_HEADER_BYTES + TRAILER_BYTES {
        return Err(invalid("truncated checksummed queue-log record"));
    }
    let payload_len = u64::from_le_bytes(
        raw[14..RECORD_HEADER_BYTES]
            .try_into()
            .map_err(|_| invalid("queue-log payload length"))?,
    );
    let expected = usize::try_from(payload_len)
        .ok()
        .and_then(|n| n.checked_add(RECORD_HEADER_BYTES + TRAILER_BYTES))
        .ok_or_else(|| invalid("checksummed queue-log length overflow"))?;
    if raw.len() != expected {
        return Err(invalid("checksummed queue-log length mismatch"));
    }
    let (content, checksum) = raw.split_at(raw.len() - TRAILER_BYTES);
    if Sha256::digest(content).as_slice() != checksum {
        return Err(invalid("queue-log checksum mismatch"));
    }
    let mut reader = Reader(content);
    reader.take(5)?;
    let epoch = reader.u64()?;
    if epoch == 0 {
        return Err(invalid("queue-log record has no writer epoch"));
    }
    let tag = reader.take(1)?[0];
    let payload = reader.blob()?;
    if !reader.0.is_empty() {
        return Err(invalid("trailing queue-log record bytes"));
    }
    let payload = match tag {
        GENESIS => {
            let mut reader = Reader(payload);
            let address = view_address(reader.blob()?)?;
            let definition = reader.blob()?;
            // Structural/accounting scan only; the semantic state remains explicit.
            crate::queue_schema_codec::definition_headers(definition)?;
            let cursor_secret = reader
                .take(32)?
                .try_into()
                .map_err(|_| invalid("queue-log cursor secret length"))?;
            if !reader.0.is_empty() {
                return Err(invalid("trailing queue-log genesis bytes"));
            }
            QueueLogPayload::Genesis(GenesisView {
                address,
                definition,
                cursor_secret,
            })
        }
        PUBLICATION => {
            let publication = view_resolved_publication(payload)
                .map_err(|_| invalid("invalid embedded queue publication"))?;
            AddressRef {
                tenant: publication.summary().queue.0,
                queue: publication.summary().queue.1,
            }
            .validate()?;
            QueueLogPayload::Publication(publication)
        }
        CLAIM => {
            let claim = view_claim_publication(payload)?;
            AddressRef {
                tenant: claim.commands().summary().queue.0,
                queue: claim.commands().summary().queue.1,
            }
            .validate()?;
            QueueLogPayload::Claim(claim)
        }
        ENTRIES => QueueLogPayload::Entries(view_entries(payload)?),
        RETENTION => {
            let mut reader = Reader(payload);
            let address = view_address(reader.blob()?)?;
            let definition_pin = reader
                .take(32)?
                .try_into()
                .map_err(|_| invalid("queue-log retention definition pin"))?;
            let at = reader.u64()? as i64;
            let budget = reader.u64()?;
            if !reader.0.is_empty() {
                return Err(invalid("trailing queue-log retention bytes"));
            }
            QueueLogPayload::Retention(RetentionView {
                address,
                definition_pin,
                at,
                budget,
            })
        }
        _ => return Err(invalid("unknown queue-log record kind")),
    };
    Ok(QueueLogRecordView { epoch, payload })
}

fn view_entry(kind: u8, raw: &[u8]) -> QueueResult<QueueLogEntryView<'_>> {
    let (entry, summary_queue) = match kind {
        PUBLICATION => {
            let publication = view_resolved_publication(raw)
                .map_err(|_| invalid("invalid embedded queue publication"))?;
            let queue = publication.summary().queue;
            (QueueLogEntryView::Publication(publication), queue)
        }
        CLAIM => {
            let claim = view_claim_publication(raw)?;
            let queue = claim.commands().summary().queue;
            (QueueLogEntryView::Claim(claim), queue)
        }
        _ => return Err(invalid("unknown queue-log entry kind")),
    };
    AddressRef {
        tenant: summary_queue.0,
        queue: summary_queue.1,
    }
    .validate()?;
    Ok(entry)
}
fn view_entries(payload: &[u8]) -> QueueResult<EntriesView<'_>> {
    let mut reader = Reader(payload);
    let count = usize::try_from(reader.u64()?).map_err(|_| invalid("queue-log entry count"))?;
    // Each entry takes at least its framing, which bounds the allocation below.
    if count == 0 || count > payload.len() / ENTRY_FRAMING_BYTES as usize {
        return Err(invalid("queue-log entry count"));
    }
    let mut entries: Vec<QueueLogEntryView<'_>> = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = reader.take(1)?[0];
        let entry = view_entry(kind, reader.blob()?)?;
        if let Some(previous) = entries.last() {
            let (before, after) = (previous.frame().summary(), entry.frame().summary());
            if before.queue != after.queue
                || before.definition_pin != after.definition_pin
                || before.final_sequence != after.initial_sequence
            {
                return Err(invalid("queue-log entries are not one chain"));
            }
        }
        entries.push(entry);
    }
    if !reader.0.is_empty() {
        return Err(invalid("trailing queue-log entry bytes"));
    }
    Ok(EntriesView { entries })
}

/// One version-specific structurally checked view of the exact immutable owner.
/// This token is not storage, fence, or progress authority. Borrowing prevents
/// its owner/charge from disappearing while observers share the parsed bytes.
pub struct CheckedQueueLogRecord<'a> {
    owner: &'a OwnedQueueLogRecord,
    view: QueueLogRecordView<'a>,
}
impl<'a> CheckedQueueLogRecord<'a> {
    pub fn owner(&self) -> &'a OwnedQueueLogRecord {
        self.owner
    }
    /// Actual outer payload bounds, excluding framing and any checksum trailer.
    /// Consumers referencing borrowed inner values must use this codec boundary.
    pub fn payload_interval(&self) -> QueueResult<std::ops::Range<usize>> {
        let end = self
            .owner
            .bytes()
            .len()
            .checked_sub(TRAILER_BYTES)
            .filter(|end| *end >= RECORD_HEADER_BYTES)
            .ok_or_else(|| invalid("checked record payload bounds"))?;
        Ok(RECORD_HEADER_BYTES..end)
    }
    pub fn view(&self) -> &QueueLogRecordView<'a> {
        &self.view
    }
}
/// Bytes and every copied header are owned before the admission guard drops.
pub struct OwnedQueueLogRecord {
    bytes: Bytes,
    _charge: OwnedQueueBufferCharge,
}
impl OwnedQueueLogRecord {
    /// Transfer pre-admitted immutable bytes without copying or releasing their
    /// guard. Construct the whole owner before validation so failures drop bytes
    /// before capacity. This does not establish storage-source qualification.
    pub fn from_admitted_bytes(
        bytes: Bytes,
        charge: OwnedQueueBufferCharge,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        let record = Self {
            bytes,
            _charge: charge,
        };
        if !record.belongs_to(job) {
            return Err(invalid("queue record belongs to another operation"));
        }
        let required = u64::try_from(record.bytes.len())
            .map_err(|_| QueueError::TooLarge)?
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?;
        if record._charge.admitted_bytes() < required {
            return Err(QueueError::TooLarge);
        }
        record.view()?;
        Ok(record)
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn view(&self) -> QueueResult<QueueLogRecordView<'_>> {
        view_queue_log_record(&self.bytes)
    }
    pub fn checked(&self) -> QueueResult<CheckedQueueLogRecord<'_>> {
        Ok(CheckedQueueLogRecord {
            owner: self,
            view: self.view()?,
        })
    }
    pub fn belongs_to(&self, job: &QueueReservation) -> bool {
        std::ptr::eq(self._charge.reservation(), job)
    }
    /// Transfer admission into the shared payload owner. Every Bytes clone and
    /// slice keeps the charge until the final physical alias disappears.
    /// Do not send through stores that retain these aliases as persistent data:
    /// such a store would retain this operation's reservation indefinitely.
    pub fn into_admitted_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}
impl OwnedQueueLogRecord {
    /// Take the single publication or claim this record carries as an entry of a
    /// larger record. The entry keeps this record's admission alive.
    pub fn into_entry(self) -> QueueResult<OwnedQueueEntry> {
        let (kind, range) = {
            let checked = self.checked()?;
            let kind = match checked.view().payload() {
                QueueLogPayload::Publication(_) => QueueLogEntryKind::Publication,
                QueueLogPayload::Claim(_) => QueueLogEntryKind::Claim,
                _ => return Err(invalid("record is not a single publication or claim")),
            };
            (kind, checked.payload_interval()?)
        };
        let bytes = self.into_admitted_bytes().slice(range);
        let entry = OwnedQueueEntry { kind, bytes };
        entry.view()?;
        Ok(entry)
    }
}
/// One publication or claim, ready to be one entry of a record. It is not a
/// record: it has no epoch, offset or checksum.
#[derive(Clone)]
pub struct OwnedQueueEntry {
    kind: QueueLogEntryKind,
    bytes: Bytes,
}
impl OwnedQueueEntry {
    pub fn kind(&self) -> QueueLogEntryKind {
        self.kind
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn view(&self) -> QueueResult<QueueLogEntryView<'_>> {
        view_entry(self.kind.tag(), &self.bytes)
    }
}
impl std::fmt::Debug for OwnedQueueEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedQueueEntry")
            .field("kind", &self.kind)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
impl AsRef<[u8]> for OwnedQueueLogRecord {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::fmt::Debug for OwnedQueueLogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedQueueLogRecord")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
struct AdmittedOutput {
    bytes: Vec<u8>,
    charge: OwnedQueueBufferCharge,
}
fn admitted_output(
    size: usize,
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<AdmittedOutput> {
    if size as u64 > max_bytes {
        return Err(QueueError::TooLarge);
    }
    let charge = job.owned_buffer(
        (size as u64)
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    Ok(AdmittedOutput {
        bytes: Vec::with_capacity(size),
        charge,
    })
}
fn header(out: &mut Vec<u8>, epoch: u64, tag: u8, payload: usize) -> QueueResult<()> {
    if epoch == 0 {
        return Err(invalid("queue-log record has no writer epoch"));
    }
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&epoch.to_le_bytes());
    out.push(tag);
    out.extend_from_slice(
        &u64::try_from(payload)
            .map_err(|_| QueueError::TooLarge)?
            .to_le_bytes(),
    );
    Ok(())
}
fn blob(out: &mut Vec<u8>, raw: &[u8]) -> QueueResult<()> {
    out.extend_from_slice(
        &u64::try_from(raw.len())
            .map_err(|_| QueueError::TooLarge)?
            .to_le_bytes(),
    );
    out.extend_from_slice(raw);
    Ok(())
}
pub fn checked_genesis_record_len(
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
) -> QueueResult<u64> {
    address.validate()?;
    let size = framed_address_len(address)?
        .checked_add(definition.raw().len())
        .and_then(|n| n.checked_add(70 + TRAILER_BYTES))
        .ok_or(QueueError::TooLarge)?;
    u64::try_from(size).map_err(|_| QueueError::TooLarge)
}
pub fn encode_checked_genesis(
    epoch: u64,
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    cursor_secret: &[u8; 32],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    address.validate()?;
    if !definition.belongs_to(job) {
        return Err(invalid("genesis definition belongs to another job"));
    }
    let size = usize::try_from(checked_genesis_record_len(address, definition)?)
        .map_err(|_| QueueError::TooLarge)?;
    let payload = size
        .checked_sub(RECORD_HEADER_BYTES + TRAILER_BYTES)
        .ok_or(QueueError::TooLarge)?;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(&mut out.bytes, epoch, GENESIS, payload)?;
    let framed = encode_address(address)?;
    blob(&mut out.bytes, &framed)?;
    blob(&mut out.bytes, definition.raw())?;
    out.bytes.extend_from_slice(cursor_secret);
    let record = seal_record(out);
    record.view()?;
    Ok(record)
}
/// Complete checksummed publication wrapper size, separate from its inner frame.
pub fn checked_publication_record_len(publication_bytes: u64) -> QueueResult<u64> {
    publication_bytes
        .checked_add((RECORD_HEADER_BYTES + TRAILER_BYTES) as u64)
        .ok_or(QueueError::TooLarge)
}
pub fn encode_checked_publication_record(
    epoch: u64,
    publication: &[u8],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    let validated = view_resolved_publication(publication)
        .map_err(|_| invalid("invalid embedded queue publication"))?;
    AddressRef {
        tenant: validated.summary().queue.0,
        queue: validated.summary().queue.1,
    }
    .validate()?;
    let size = usize::try_from(checked_publication_record_len(publication.len() as u64)?)
        .map_err(|_| QueueError::TooLarge)?;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(&mut out.bytes, epoch, PUBLICATION, publication.len())?;
    out.bytes.extend_from_slice(publication);
    Ok(seal_record(out))
}
pub fn encode_checked_claim_record(
    epoch: u64,
    claim: &OwnedClaimPublication,
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    view_claim_publication(claim.bytes())?.validate_binding(job)?;
    let size = claim
        .bytes()
        .len()
        .checked_add(54)
        .ok_or(QueueError::TooLarge)?;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(&mut out.bytes, epoch, CLAIM, claim.bytes().len())?;
    out.bytes.extend_from_slice(claim.bytes());
    Ok(seal_record(out))
}
/// Exact size of the record that carries entries of these encoded sizes.
pub fn checked_entries_record_len(entry_bytes: impl IntoIterator<Item = u64>) -> QueueResult<u64> {
    entry_bytes
        .into_iter()
        .try_fold(ENTRIES_RECORD_FIXED_BYTES, |total, bytes| {
            total
                .checked_add(ENTRY_FRAMING_BYTES)
                .and_then(|total| total.checked_add(bytes))
                .ok_or(QueueError::TooLarge)
        })
}
/// The largest standalone checksummed publication or claim record that still
/// fits `hard_record_bytes` once `into_entry` has made it the only entry of an
/// entry-list record. A standalone record is the record header, the payload and
/// the checksum trailer; its entry is the payload alone, and the wrapping record
/// adds the entry-list fixed bytes and one entry's framing. So a record of
/// `n` bytes wraps to `n + overhead`, where the overhead is
/// `ENTRIES_RECORD_FIXED_BYTES + ENTRY_FRAMING_BYTES - (header + trailer)`.
/// `None` when nothing positive is left. Planners are bounded by this, not by
/// `hard_record_bytes`.
pub fn standalone_record_bound(hard_record_bytes: u64) -> Option<u64> {
    let wrapped = checked_entries_record_len([0]).ok()?;
    let standalone = checked_publication_record_len(0).ok()?;
    hard_record_bytes
        .checked_sub(wrapped.checked_sub(standalone)?)
        .filter(|bound| *bound > 0)
}
/// A cleanup pass bound to the queue's definition. `budget` is the most rows
/// applying it removes.
pub fn encode_checked_retention(
    epoch: u64,
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    at: i64,
    budget: u64,
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    address.validate()?;
    if !definition.belongs_to(job) {
        return Err(invalid("retention definition belongs to another job"));
    }
    let framed = encode_address(address)?;
    let payload = framed
        .len()
        .checked_add(8 + 32 + 8 + 8)
        .ok_or(QueueError::TooLarge)?;
    let size = payload
        .checked_add(RECORD_HEADER_BYTES + TRAILER_BYTES)
        .ok_or(QueueError::TooLarge)?;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(&mut out.bytes, epoch, RETENTION, payload)?;
    blob(&mut out.bytes, &framed)?;
    out.bytes
        .extend_from_slice(definition.pin().as_bytes().as_slice());
    out.bytes.extend_from_slice(&at.to_le_bytes());
    out.bytes.extend_from_slice(&budget.to_le_bytes());
    let record = seal_record(out);
    record.view()?;
    Ok(record)
}
/// One record of chained entries. The entries are validated as a chain.
pub fn encode_checked_entries_record(
    epoch: u64,
    entries: &[OwnedQueueEntry],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    if entries.is_empty() {
        return Err(invalid("entry-list record has no entries"));
    }
    let size = usize::try_from(checked_entries_record_len(
        entries.iter().map(|entry| entry.bytes.len() as u64),
    )?)
    .map_err(|_| QueueError::TooLarge)?;
    let payload = size - RECORD_HEADER_BYTES - 32;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(&mut out.bytes, epoch, ENTRIES, payload)?;
    out.bytes
        .extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.bytes.push(entry.kind.tag());
        blob(&mut out.bytes, &entry.bytes)?;
    }
    let record = seal_record(out);
    // Viewing re-checks every entry and that they chain.
    record.view()?;
    Ok(record)
}
fn seal_record(mut out: AdmittedOutput) -> OwnedQueueLogRecord {
    let checksum = Sha256::digest(&out.bytes);
    out.bytes.extend_from_slice(&checksum);
    OwnedQueueLogRecord {
        bytes: out.bytes.into(),
        _charge: out.charge,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{queue_admission::QueueAdmission, queue_schema_codec::encode_definition};
    use fireweed_core::queue_contract::{CollectionDefinition, QueueSchema};
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, BTreeSet};
    const GOLDEN: &str = "4657514c3207000000000000000068000000000000001a0000000000000046575141444452310100000000000000740100000000000000711e00000000000000ffffffffffffffffffff4657514434000000000001038080bc8ac9d21300a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a587452b75d24115ec5c5c8bae654e21b5a8125b9361c89bcc68075a8bb348bc35";
    fn reseal(mut raw: Vec<u8>) -> Vec<u8> {
        let n = raw.len() - 32;
        let checksum = Sha256::digest(&raw[..n]);
        raw[n..].copy_from_slice(&checksum);
        raw
    }
    fn hex(raw: &[u8]) -> String {
        raw.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    fn golden() -> Vec<u8> {
        GOLDEN
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::new(),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1,
            max_attempts: 3,
            retention: fireweed_core::queue_contract::QueueRetention {
                request_id_nanos: 86_400_000_000_000,
                deleted_keys: fireweed_core::queue_contract::DeletedKeyPolicy::Reusable,
            },
        }
    }
    fn address() -> QueueAddress {
        QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        }
    }
    fn job() -> Arc<QueueReservation> {
        Arc::new(QueueAdmission::new(1, 1 << 20).unwrap().reserve(0).unwrap())
    }
    #[test]
    fn admitted_genesis_bound_covers_definition_and_full_address_framing() {
        use fireweed_core::queue_contract::{
            FieldType, ProvisionRequest, QueueLimits, encoded_len,
        };
        for count in [0, 127, 128, 4096] {
            let mut schema = schema();
            for n in 0..count {
                schema
                    .work
                    .fields
                    .insert(format!("field\0雪/{n}"), FieldType::Integer);
            }
            schema
                .state
                .insert("collection\0λ/".repeat(128), schema.work.clone());
            let address = QueueAddress {
                tenant: "tenant\0雪/".repeat(129),
                queue: "queue\0λ/".repeat(130),
            };
            let request = ProvisionRequest {
                queue: address.clone(),
                schema: schema.clone(),
            };
            let bytes = encoded_len(&request).unwrap();
            let limits = QueueLimits {
                max_records: u32::try_from(count.max(1)).unwrap(),
                max_groups: 1,
                max_request_bytes: bytes,
                max_group_bytes: bytes,
                max_response_bytes: 1,
                max_body_bytes: 1,
                max_state_bytes: 1,
            };
            request.validate(&limits).unwrap();
            let admission = QueueAdmission::new(1, 64 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let raw = encode_definition(&schema).unwrap();
            let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
            let bound = QueueLogRecordBounds::for_limits(&limits).unwrap();
            let actual = checked_genesis_record_len(&address, &definition).unwrap();
            assert!(actual <= bound.genesis);
            let record =
                encode_checked_genesis(u64::MAX, &address, &definition, &[91; 32], actual, &job)
                    .unwrap();
            assert_eq!(record.bytes().len() as u64, actual);
            assert!(matches!(
                encode_checked_genesis(
                    u64::MAX,
                    &address,
                    &definition,
                    &[91; 32],
                    actual - 1,
                    &job
                ),
                Err(QueueError::TooLarge)
            ));
            drop(record);
            drop(definition);
            drop(job);
            assert_eq!(admission.usage().unwrap(), (0, 0));
        }
    }

    #[test]
    fn checked_genesis_golden_bounds_and_every_single_bit_corruption() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let n = checked_genesis_record_len(&address(), &definition).unwrap();
        assert_eq!(n, 158);
        let record =
            encode_checked_genesis(7, &address(), &definition, &[0xa5; 32], n, &job).unwrap();
        assert_eq!(hex(record.bytes()), GOLDEN);
        assert!(view_queue_log_record(&golden()).is_ok());
        let mut old_magic = golden();
        old_magic[4] = b'1';
        assert!(view_queue_log_record(&reseal(old_magic)).is_err());
        assert!(matches!(
            encode_checked_genesis(7, &address(), &definition, &[0xa5; 32], n - 1, &job),
            Err(QueueError::TooLarge)
        ));
        for offset in 0..record.bytes().len() {
            for bit in 0..8 {
                let mut changed = record.bytes().to_vec();
                changed[offset] ^= 1 << bit;
                assert!(
                    view_queue_log_record(&changed).is_err(),
                    "offset={offset}, bit={bit}"
                );
            }
        }
        let mut appended = record.bytes().to_vec();
        appended.push(0);
        assert!(view_queue_log_record(&appended).is_err());
        assert!(view_queue_log_record(&record.bytes()[..record.bytes().len() - 1]).is_err());
    }
    #[test]
    fn checked_publication_preserves_inner_versions_and_covers_complete_record() {
        use crate::{
            queue_planner::{RecordSnapshot, combine_deltas},
            queue_publication::encode_publication_pinned,
        };
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let before = RecordSnapshot::default();
        let delta = combine_deltas(&[], 0).unwrap();
        let frame = encode_publication_pinned(
            &address(),
            definition.pin(),
            &before,
            &delta,
            &[],
            4096,
            &job,
        )
        .unwrap();
        for version in *b"12" {
            let mut inner = frame.as_bytes().to_vec();
            inner[4] = version;
            let n = inner.len() as u64 + 54;
            let record = encode_checked_publication_record(9, &inner, n, &job).unwrap();
            assert_eq!(&record.bytes()[22..record.bytes().len() - 32], &inner);
            assert!(matches!(
                encode_checked_publication_record(9, &inner, n - 1, &job),
                Err(QueueError::TooLarge)
            ));
            if version == b'1' {
                assert_eq!(
                    hex(record.bytes()),
                    "4657514c320900000000000000015700000000000000465751503101000000000000007401000000000000007120e569a5228ca922ed50d018b847cbee60c8c6c482290c9c8419fddecab92932000000000000000000000000000000000000000000000000000000000000000063325621fc1d1440c310c29de66450299b2aec6efd61bdda31fe748264b9aa62"
                );
            }
            for offset in 0..record.bytes().len() {
                let mut changed = record.bytes().to_vec();
                changed[offset] ^= 1;
                assert!(view_queue_log_record(&changed).is_err(), "offset={offset}");
            }
        }
    }
    #[test]
    fn ingress_rejects_wrong_job_and_insufficient_capacity_before_releasing_owner() {
        let admission = QueueAdmission::new(2, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let other = Arc::new(admission.reserve(0).unwrap());
        let raw = golden();
        let required = raw.len() as u64 * 4 + 4096;
        let charge = job.owned_buffer(required).unwrap();
        let bytes = Bytes::copy_from_slice(&raw);
        let pointer = bytes.as_ptr();
        let record = OwnedQueueLogRecord::from_admitted_bytes(bytes, charge, &job).unwrap();
        assert_eq!(record.bytes().as_ptr(), pointer);
        assert_eq!(admission.usage().unwrap().1, required);
        drop(record);
        assert_eq!(admission.usage().unwrap().1, 0);
        let charge = job.owned_buffer(required).unwrap();
        assert!(matches!(
            OwnedQueueLogRecord::from_admitted_bytes(Bytes::copy_from_slice(&raw), charge, &other),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap().1, 0);
        let charge = job.owned_buffer(required - 1).unwrap();
        assert!(matches!(
            OwnedQueueLogRecord::from_admitted_bytes(Bytes::copy_from_slice(&raw), charge, &job),
            Err(QueueError::TooLarge)
        ));
        assert_eq!(admission.usage().unwrap().1, 0);
        drop(job);
        drop(other);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn payload_clones_and_slices_retain_final_reservation_until_last_alias() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let terminal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = std::sync::Arc::clone(&terminal);
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || {
                    signal.store(true, std::sync::atomic::Ordering::SeqCst);
                })
                .unwrap(),
        );
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record =
            encode_checked_genesis(7, &address(), &definition, &[19; 32], 4096, &job).unwrap();
        let expected = record.bytes().to_vec();
        drop(definition);
        let usage = admission.usage().unwrap();
        let bytes = record.into_admitted_bytes();
        assert_eq!(&bytes[..], &expected);
        let clone = bytes.clone();
        let slice = clone.slice(22..);
        drop(job);
        drop(bytes);
        drop(clone);
        assert_eq!(admission.usage().unwrap(), usage);
        assert!(!terminal.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(&slice[..], &expected[22..]);
        drop(slice);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert!(terminal.load(std::sync::atomic::Ordering::SeqCst));
    }
    #[test]
    fn genesis_literal_pin_secret_and_exact_boundary() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        assert_eq!(
            hex(&raw),
            "ffffffffffffffffffff4657514434000000000001038080bc8ac9d21300"
        );
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let encoded =
            encode_checked_genesis(7, &address(), &definition, &[0xa5; 32], 158, &job).unwrap();
        assert_eq!(hex(encoded.bytes()), GOLDEN);
        assert_eq!(
            hex(&Sha256::digest(encoded.bytes())),
            "bc88120eb8451a2503407eca91fe70d32a5f15937548badaacd3c7fd2a1cbad3"
        );
        assert!(matches!(
            encode_checked_genesis(7, &address(), &definition, &[0xa5; 32], 157, &job),
            Err(QueueError::TooLarge)
        ));
        let view = encoded.view().unwrap();
        assert_eq!(view.epoch(), 7);
        assert_eq!(view.address().tenant, "t");
        assert_eq!(view.definition_pin_bytes(), *definition.pin().as_bytes());
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!("genesis")
        };
        assert_eq!(genesis.cursor_secret(), &[0xa5; 32]);
        assert_eq!(genesis.definition_bytes(), raw);
        let validated = genesis.validate_definition(&job).unwrap();
        assert_eq!(validated.schema(), definition.schema());
        assert_eq!(validated.raw(), raw);
        assert!(encoded.belongs_to(&job));
        assert_eq!(
            format!("{encoded:?}"),
            "OwnedQueueLogRecord { bytes: 158, .. }"
        );
    }
    #[test]
    fn complete_framing_validation_and_semantics_are_separate() {
        let raw = golden();
        for n in 0..raw.len() {
            assert!(view_queue_log_record(&raw[..n]).is_err());
        }
        for (offset, byte) in [(4, b'9'), (13, 255)] {
            let mut invalid = raw.clone();
            invalid[offset] = byte;
            assert!(view_queue_log_record(&reseal(invalid)).is_err());
        }
        let mut zero = raw.clone();
        zero[5..13].fill(0);
        assert!(view_queue_log_record(&reseal(zero)).is_err());
        let mut length = raw.clone();
        length[14..22].fill(255);
        assert!(view_queue_log_record(&reseal(length)).is_err());
        let mut trailing = raw.clone();
        trailing.push(0);
        assert!(view_queue_log_record(&trailing).is_err());
        // The measured scanner ignores max_attempts: borrowed framing is not
        // permission to use an invalid schema as durable semantic authority.
        let mut invalid_schema = raw;
        invalid_schema[85] = 0;
        let invalid_schema = reseal(invalid_schema);
        let view = view_queue_log_record(&invalid_schema).unwrap();
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!("genesis")
        };
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        assert!(genesis.validate_definition(&job).is_err());
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn exact_definition_bytes_and_job_ownership_are_preserved() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let encoded =
            encode_checked_genesis(1, &address(), &definition, &[1; 32], 4096, &job).unwrap();
        let view = encoded.view().unwrap();
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!("genesis")
        };
        assert_eq!(genesis.definition_bytes(), raw);
        assert_eq!(
            genesis.validate_definition(&job).unwrap().pin(),
            definition.pin()
        );
        let other = Arc::new(QueueAdmission::new(1, 1 << 20).unwrap().reserve(0).unwrap());
        assert!(
            encode_checked_genesis(1, &address(), &definition, &[1; 32], 4096, &other).is_err()
        );
    }
    #[test]
    fn publication_dispatch_uses_existing_readers_and_retains_exact_bytes() {
        use crate::{
            queue_planner::{RecordSnapshot, combine_deltas},
            queue_publication::encode_publication_pinned,
        };
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let before = RecordSnapshot::default();
        let delta = combine_deltas(&[], 0).unwrap();
        let frame = encode_publication_pinned(
            &address(),
            definition.pin(),
            &before,
            &delta,
            &[],
            4096,
            &job,
        )
        .unwrap();
        for version in *b"12" {
            let mut raw = frame.as_bytes().to_vec();
            raw[4] = version;
            let encoded = encode_checked_publication_record(9, &raw, 4096, &job).unwrap();
            let view = encoded.view().unwrap();
            assert_eq!(view.epoch(), 9);
            assert_eq!(view.address().queue, "q");
            assert_eq!(view.definition_pin_bytes(), *definition.pin().as_bytes());
            assert_eq!(
                hex(&encoded.bytes()[..22]),
                "4657514c320900000000000000015700000000000000"
            );
            assert_eq!(&encoded.bytes()[22..encoded.bytes().len() - 32], raw);
            assert!(matches!(
                encode_checked_publication_record(9, &raw, encoded.bytes().len() as u64 - 1, &job),
                Err(QueueError::TooLarge)
            ));
            let mut corrupt = raw;
            corrupt.push(0);
            assert!(encode_checked_publication_record(9, &corrupt, 4096, &job).is_err());
        }
    }
    #[test]
    fn publication_wrapper_checks_the_same_borrowed_address_policy_as_genesis() {
        use crate::{
            queue_planner::{RecordSnapshot, combine_deltas},
            queue_publication::encode_publication_pinned,
        };
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let queue = QueueAddress {
            tenant: " ".into(),
            queue: "q".into(),
        };
        let before = RecordSnapshot::default();
        let delta = combine_deltas(&[], 0).unwrap();
        let frame =
            encode_publication_pinned(&queue, definition.pin(), &before, &delta, &[], 4096, &job)
                .unwrap();
        assert!(encode_checked_publication_record(1, frame.as_bytes(), 4096, &job).is_err());
        let mut wrapper = vec![];
        header(&mut wrapper, 1, PUBLICATION, frame.as_bytes().len()).unwrap();
        wrapper.extend_from_slice(frame.as_bytes());
        let checksum = Sha256::digest(&wrapper);
        wrapper.extend_from_slice(&checksum);
        assert!(view_queue_log_record(&wrapper).is_err());
    }
    #[test]
    fn invalid_epoch_drops_owned_writer_before_releasing_its_charge() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let before = admission.usage().unwrap();
        assert!(
            matches!(encode_checked_genesis(0,&address(),&definition,&[1;32],4096,&job),Err(QueueError::OutcomeUnknown(ref message)) if message.contains("writer epoch"))
        );
        assert_eq!(admission.usage().unwrap(), before);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn schema_validation_and_output_admission_precede_allocations() {
        let raw = encode_definition(&schema()).unwrap();
        let admission = QueueAdmission::new(1, 1).unwrap();
        let tiny = Arc::new(admission.reserve(0).unwrap());
        assert!(
            matches!(AdmittedQueueDefinition::from_raw(&raw,&tiny),Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted"))
        );
        assert_eq!(admission.usage().unwrap(), (1, 0));
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let occupied = admission.usage().unwrap().1;
        let _block = job.owned_buffer((1 << 20) - occupied).unwrap();
        let before = admission.usage().unwrap();
        assert!(
            matches!(encode_checked_genesis(7,&address(),&definition,&[0xa5;32],4096,&job),Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted"))
        );
        assert_eq!(admission.usage().unwrap(), before);
    }

    /// Empty frames chain at sequence zero; `at` moves a frame's sequence span.
    fn empty_frame(definition: &AdmittedQueueDefinition, job: &Arc<QueueReservation>) -> Vec<u8> {
        use crate::{
            queue_planner::{RecordSnapshot, combine_deltas},
            queue_publication::encode_publication_pinned,
        };
        encode_publication_pinned(
            &address(),
            definition.pin(),
            &RecordSnapshot::default(),
            &combine_deltas(&[], 0).unwrap(),
            &[],
            4096,
            job,
        )
        .unwrap()
        .as_bytes()
        .to_vec()
    }
    fn entry_of(frame: &[u8], job: &Arc<QueueReservation>) -> OwnedQueueEntry {
        encode_checked_publication_record(7, frame, 4096, job)
            .unwrap()
            .into_entry()
            .unwrap()
    }
    #[test]
    fn standalone_bound_is_what_a_lone_entry_wraps_to() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let frame = empty_frame(&definition, &job);
        let standalone = encode_checked_publication_record(7, &frame, 4096, &job).unwrap();
        let len = standalone.bytes().len() as u64;
        let entry = standalone.into_entry().unwrap();
        let wrapped = checked_entries_record_len([entry.bytes().len() as u64]).unwrap();
        // The bound for a hard limit equal to the wrapped size is exactly this record.
        assert_eq!(standalone_record_bound(wrapped), Some(len));
        assert_eq!(standalone_record_bound(wrapped - 1), Some(len - 1));
        assert_eq!(standalone_record_bound(wrapped - len), None);
        assert_eq!(standalone_record_bound(0), None);
    }
    #[test]
    fn entry_list_round_trips_in_order_and_sizes_exactly() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let frame = empty_frame(&definition, &job);
        let entries: Vec<_> = (0..3).map(|_| entry_of(&frame, &job)).collect();
        assert!(
            entries
                .iter()
                .all(|entry| entry.kind() == QueueLogEntryKind::Publication
                    && entry.bytes() == frame)
        );
        let size =
            checked_entries_record_len(entries.iter().map(|e| e.bytes().len() as u64)).unwrap();
        assert_eq!(
            size,
            ENTRIES_RECORD_FIXED_BYTES + 3 * (ENTRY_FRAMING_BYTES + frame.len() as u64)
        );
        let record = encode_checked_entries_record(7, &entries, size, &job).unwrap();
        assert_eq!(record.bytes().len() as u64, size);
        let view = record.view().unwrap();
        assert_eq!(view.epoch(), 7);
        assert_eq!(
            view.address(),
            view_address(&encode_address(&address()).unwrap()).unwrap()
        );
        let QueueLogPayload::Entries(list) = view.payload() else {
            panic!("entry list expected");
        };
        assert_eq!(list.entries().len(), 3);
        assert!(
            list.entries()
                .iter()
                .all(|entry| entry.kind() == QueueLogEntryKind::Publication
                    && entry.claim().is_none())
        );
        // One byte short of the exact size is refused.
        assert!(matches!(
            encode_checked_entries_record(7, &entries, size - 1, &job),
            Err(QueueError::TooLarge)
        ));
        assert!(encode_checked_entries_record(7, &[], size, &job).is_err());
        // Every single-bit change is caught by the checksum or the structure.
        for bit in 0..record.bytes().len() * 8 {
            let mut corrupt = record.bytes().to_vec();
            corrupt[bit / 8] ^= 1 << (bit % 8);
            assert!(view_queue_log_record(&corrupt).is_err(), "bit {bit}");
        }
    }
    #[test]
    fn entries_that_do_not_chain_are_refused() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let frame = empty_frame(&definition, &job);
        // The summary's initial and final sequence follow the address, the pin
        // and the fixed header: move both to one, so the frame no longer starts
        // where the previous one ended.
        let initial = 5 + 8 + address().tenant.len() + 8 + address().queue.len() + 32;
        let mut shifted = frame.clone();
        shifted[initial..initial + 8].copy_from_slice(&1u64.to_le_bytes());
        shifted[initial + 8..initial + 16].copy_from_slice(&1u64.to_le_bytes());
        let entries = vec![entry_of(&frame, &job), entry_of(&shifted, &job)];
        let size =
            checked_entries_record_len(entries.iter().map(|e| e.bytes().len() as u64)).unwrap();
        assert!(encode_checked_entries_record(7, &entries, size, &job).is_err());
        // A chain that does continue is accepted.
        let entries = vec![entry_of(&shifted, &job), entry_of(&shifted, &job)];
        assert!(encode_checked_entries_record(7, &entries, size, &job).is_ok());
    }
    #[test]
    fn only_a_publication_or_claim_record_becomes_an_entry() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let genesis =
            encode_checked_genesis(7, &address(), &definition, &[1; 32], 4096, &job).unwrap();
        assert!(genesis.into_entry().is_err());
        let frame = empty_frame(&definition, &job);
        let entry = entry_of(&frame, &job);
        let list =
            encode_checked_entries_record(7, std::slice::from_ref(&entry), 4096, &job).unwrap();
        assert!(list.into_entry().is_err());
    }
}
