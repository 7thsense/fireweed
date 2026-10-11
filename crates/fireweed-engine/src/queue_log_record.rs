//! One raw API-007 authoritative record per logical log offset. Borrowed parsing
//! verifies FWQL2 checksum and structure; FWQL1 remains structural-only.
//! Genesis schema semantics require admitted decode.
//! Epoch fences a writer. It is neither insertion order nor an apply cursor.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_namespace::{AddressRef, encode_address, framed_address_len, view_address},
    queue_publication::{PublicationView, view_resolved_publication},
    queue_schema_codec::{AdmittedQueueDefinition, DefinitionPin},
};
use bytes::Bytes;
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
use sha2::{Digest, Sha256};
use std::sync::Arc;
const MAGIC: &[u8; 5] = b"FWQL1";
const MAGIC_V2: &[u8; 5] = b"FWQL2";
/// Checksummed framing detects corruption of this complete record, not
/// authenticated provenance or substitution of a different valid history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueLogFormat {
    LegacyV1,
    ChecksummedV2,
}
impl QueueLogFormat {
    fn trailer_len(self) -> usize {
        match self {
            Self::LegacyV1 => 0,
            Self::ChecksummedV2 => 32,
        }
    }
    fn magic(self) -> &'static [u8; 5] {
        match self {
            Self::LegacyV1 => MAGIC,
            Self::ChecksummedV2 => MAGIC_V2,
        }
    }
}
const GENESIS: u8 = 0;
const PUBLICATION: u8 = 1;
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
}
pub struct QueueLogRecordView<'a> {
    format: QueueLogFormat,
    epoch: u64,
    payload: QueueLogPayload<'a>,
}
impl<'a> QueueLogRecordView<'a> {
    pub fn format(&self) -> QueueLogFormat {
        self.format
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn payload(&self) -> &QueueLogPayload<'a> {
        &self.payload
    }
    pub fn address(&self) -> AddressRef<'a> {
        match &self.payload {
            QueueLogPayload::Genesis(genesis) => genesis.address(),
            QueueLogPayload::Publication(publication) => AddressRef {
                tenant: publication.summary().queue.0,
                queue: publication.summary().queue.1,
            },
        }
    }
    pub fn definition_pin_bytes(&self) -> [u8; 32] {
        match &self.payload {
            QueueLogPayload::Genesis(genesis) => *genesis.definition_pin().as_bytes(),
            QueueLogPayload::Publication(publication) => publication
                .summary()
                .definition_pin
                .try_into()
                .expect("validated publication pin"),
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
/// Complete borrowed structure validation; FWQL2 also verifies its checksum.
/// FWQL1 remains structural-only and requires an explicit legacy policy. Genesis uniqueness, schema semantics,
/// predecessor/receipt authority and atomic external cursor are store obligations.
pub fn view_queue_log_record(raw: &[u8]) -> QueueResult<QueueLogRecordView<'_>> {
    let format = match raw.get(..5) {
        Some(magic) if magic == MAGIC => QueueLogFormat::LegacyV1,
        Some(magic) if magic == MAGIC_V2 => QueueLogFormat::ChecksummedV2,
        _ => return Err(invalid("unsupported queue-log record version")),
    };
    let content = if format == QueueLogFormat::ChecksummedV2 {
        if raw.len() < 54 {
            return Err(invalid("truncated checksummed queue-log record"));
        }
        let payload_len = u64::from_le_bytes(
            raw[14..22]
                .try_into()
                .map_err(|_| invalid("queue-log payload length"))?,
        );
        let expected = usize::try_from(payload_len)
            .ok()
            .and_then(|n| n.checked_add(54))
            .ok_or_else(|| invalid("checksummed queue-log length overflow"))?;
        if raw.len() != expected {
            return Err(invalid("checksummed queue-log length mismatch"));
        }
        let (content, checksum) = raw.split_at(raw.len() - 32);
        if Sha256::digest(content).as_slice() != checksum {
            return Err(invalid("queue-log checksum mismatch"));
        }
        content
    } else {
        raw
    };
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
        _ => return Err(invalid("unknown queue-log record kind")),
    };
    Ok(QueueLogRecordView {
        format,
        epoch,
        payload,
    })
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
fn header(
    out: &mut Vec<u8>,
    epoch: u64,
    tag: u8,
    payload: usize,
    format: QueueLogFormat,
) -> QueueResult<()> {
    if epoch == 0 {
        return Err(invalid("queue-log record has no writer epoch"));
    }
    out.extend_from_slice(format.magic());
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
/// Exact FWQL1 genesis size, shared by preflight and the admitted writer.
pub fn genesis_record_len(
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
) -> QueueResult<u64> {
    genesis_len_with_format(address, definition, QueueLogFormat::LegacyV1)
}
pub fn checked_genesis_record_len(
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
) -> QueueResult<u64> {
    genesis_len_with_format(address, definition, QueueLogFormat::ChecksummedV2)
}
fn genesis_len_with_format(
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    format: QueueLogFormat,
) -> QueueResult<u64> {
    address.validate()?;
    let size = framed_address_len(address)?
        .checked_add(definition.raw().len())
        .and_then(|n| n.checked_add(70 + format.trailer_len()))
        .ok_or(QueueError::TooLarge)?;
    u64::try_from(size).map_err(|_| QueueError::TooLarge)
}
pub fn encode_genesis(
    epoch: u64,
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    cursor_secret: &[u8; 32],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    encode_genesis_with_format(
        epoch,
        address,
        definition,
        cursor_secret,
        max_bytes,
        job,
        QueueLogFormat::LegacyV1,
    )
}
pub fn encode_checked_genesis(
    epoch: u64,
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    cursor_secret: &[u8; 32],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    encode_genesis_with_format(
        epoch,
        address,
        definition,
        cursor_secret,
        max_bytes,
        job,
        QueueLogFormat::ChecksummedV2,
    )
}
fn encode_genesis_with_format(
    epoch: u64,
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    cursor_secret: &[u8; 32],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
    format: QueueLogFormat,
) -> QueueResult<OwnedQueueLogRecord> {
    address.validate()?;
    if !definition.belongs_to(job) {
        return Err(invalid("genesis definition belongs to another job"));
    }
    let size = usize::try_from(genesis_len_with_format(address, definition, format)?)
        .map_err(|_| QueueError::TooLarge)?;
    let payload = size
        .checked_sub(22 + format.trailer_len())
        .ok_or(QueueError::TooLarge)?;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(&mut out.bytes, epoch, GENESIS, payload, format)?;
    let framed = encode_address(address)?;
    blob(&mut out.bytes, &framed)?;
    blob(&mut out.bytes, definition.raw())?;
    out.bytes.extend_from_slice(cursor_secret);
    let record = seal_record(out, format);
    record.view()?;
    Ok(record)
}
pub fn encode_publication_record(
    epoch: u64,
    publication: &[u8],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    encode_publication_with_format(epoch, publication, max_bytes, job, QueueLogFormat::LegacyV1)
}
pub fn encode_checked_publication_record(
    epoch: u64,
    publication: &[u8],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueLogRecord> {
    encode_publication_with_format(
        epoch,
        publication,
        max_bytes,
        job,
        QueueLogFormat::ChecksummedV2,
    )
}
fn encode_publication_with_format(
    epoch: u64,
    publication: &[u8],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
    format: QueueLogFormat,
) -> QueueResult<OwnedQueueLogRecord> {
    let validated = view_resolved_publication(publication)
        .map_err(|_| invalid("invalid embedded queue publication"))?;
    AddressRef {
        tenant: validated.summary().queue.0,
        queue: validated.summary().queue.1,
    }
    .validate()?;
    let size = publication
        .len()
        .checked_add(22 + format.trailer_len())
        .ok_or(QueueError::TooLarge)?;
    let mut out = admitted_output(size, max_bytes, job)?;
    header(
        &mut out.bytes,
        epoch,
        PUBLICATION,
        publication.len(),
        format,
    )?;
    out.bytes.extend_from_slice(publication);
    Ok(seal_record(out, format))
}
fn seal_record(mut out: AdmittedOutput, format: QueueLogFormat) -> OwnedQueueLogRecord {
    if format == QueueLogFormat::ChecksummedV2 {
        let checksum = Sha256::digest(&out.bytes);
        out.bytes.extend_from_slice(&checksum);
    }
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
    const GOLDEN: &str = "4657514c3107000000000000000060000000000000001a0000000000000046575141444452310100000000000000740100000000000000711600000000000000ffffffffffffffffffff465751443400000000000103a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5";
    const GOLDEN_V2: &str = "4657514c3207000000000000000060000000000000001a0000000000000046575141444452310100000000000000740100000000000000711600000000000000ffffffffffffffffffff465751443400000000000103a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5083f3d5bca758a84e221d72cbf83450855b81ea52a5faa1e4922634eccd25717";
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
    fn checked_genesis_golden_bounds_and_every_single_bit_corruption() {
        let job = job();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let n = checked_genesis_record_len(&address(), &definition).unwrap();
        assert_eq!(n, 150);
        let record =
            encode_checked_genesis(7, &address(), &definition, &[0xa5; 32], n, &job).unwrap();
        assert_eq!(hex(record.bytes()), GOLDEN_V2);
        assert_eq!(
            record.view().unwrap().format(),
            QueueLogFormat::ChecksummedV2
        );
        assert_eq!(
            view_queue_log_record(&golden()).unwrap().format(),
            QueueLogFormat::LegacyV1
        );
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
            assert_eq!(
                record.view().unwrap().format(),
                QueueLogFormat::ChecksummedV2
            );
            assert_eq!(&record.bytes()[22..record.bytes().len() - 32], &inner);
            assert!(matches!(
                encode_checked_publication_record(9, &inner, n - 1, &job),
                Err(QueueError::TooLarge)
            ));
            if version == b'1' {
                assert_eq!(
                    hex(record.bytes()),
                    "4657514c3209000000000000000157000000000000004657515031010000000000000074010000000000000071ca34cd395acac8ddffbe8b24816de31c87573a5290d20f00070e62cf8ae9dfef0000000000000000000000000000000000000000000000000000000000000000138bea4f2fd135807bcb48096e538dd43d8418ce0482c153e0685f4032f72497"
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
        let record = encode_genesis(7, &address(), &definition, &[19; 32], 4096, &job).unwrap();
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
        assert_eq!(hex(&raw), "ffffffffffffffffffff465751443400000000000103");
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let encoded = encode_genesis(7, &address(), &definition, &[0xa5; 32], 118, &job).unwrap();
        assert_eq!(hex(encoded.bytes()), GOLDEN);
        assert_eq!(
            hex(&Sha256::digest(encoded.bytes())),
            "65e549ab60c95b5d001089e48c55cfc271d423e0ad7716443a276fb6c00d9358"
        );
        assert!(matches!(
            encode_genesis(7, &address(), &definition, &[0xa5; 32], 117, &job),
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
            "OwnedQueueLogRecord { bytes: 118, .. }"
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
            assert!(view_queue_log_record(&invalid).is_err());
        }
        let mut zero = raw.clone();
        zero[5..13].fill(0);
        assert!(view_queue_log_record(&zero).is_err());
        let mut length = raw.clone();
        length[14..22].fill(255);
        assert!(view_queue_log_record(&length).is_err());
        let mut trailing = raw.clone();
        trailing.push(0);
        assert!(view_queue_log_record(&trailing).is_err());
        // The measured scanner ignores max_attempts: borrowed framing is not
        // permission to use an invalid schema as durable semantic authority.
        let mut invalid_schema = raw;
        invalid_schema[85] = 0;
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
    fn exact_legacy_definition_bytes_and_job_ownership_are_preserved() {
        let job = job();
        let raw = [0, 0, 0, 0, 1, 3];
        let legacy = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let modern_raw = encode_definition(legacy.schema()).unwrap();
        let modern = AdmittedQueueDefinition::from_raw(&modern_raw, &job).unwrap();
        assert_ne!(legacy.pin(), modern.pin());
        let encoded = encode_genesis(1, &address(), &legacy, &[1; 32], 4096, &job).unwrap();
        let view = encoded.view().unwrap();
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!("genesis")
        };
        assert_eq!(genesis.definition_bytes(), raw);
        assert_eq!(
            genesis.validate_definition(&job).unwrap().pin(),
            legacy.pin()
        );
        let other = Arc::new(QueueAdmission::new(1, 1 << 20).unwrap().reserve(0).unwrap());
        assert!(encode_genesis(1, &address(), &legacy, &[1; 32], 4096, &other).is_err());
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
            let encoded = encode_publication_record(9, &raw, 4096, &job).unwrap();
            let view = encoded.view().unwrap();
            assert_eq!(view.epoch(), 9);
            assert_eq!(view.address().queue, "q");
            assert_eq!(view.definition_pin_bytes(), *definition.pin().as_bytes());
            assert_eq!(
                hex(&encoded.bytes()[..22]),
                "4657514c310900000000000000015700000000000000"
            );
            assert_eq!(&encoded.bytes()[22..], raw);
            assert!(matches!(
                encode_publication_record(9, &raw, encoded.bytes().len() as u64 - 1, &job),
                Err(QueueError::TooLarge)
            ));
            let mut corrupt = raw;
            corrupt.push(0);
            assert!(encode_publication_record(9, &corrupt, 4096, &job).is_err());
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
        assert!(encode_publication_record(1, frame.as_bytes(), 4096, &job).is_err());
        let mut wrapper = vec![];
        header(
            &mut wrapper,
            1,
            PUBLICATION,
            frame.as_bytes().len(),
            QueueLogFormat::LegacyV1,
        )
        .unwrap();
        wrapper.extend_from_slice(frame.as_bytes());
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
            matches!(encode_genesis(0,&address(),&definition,&[1;32],4096,&job),Err(QueueError::OutcomeUnknown(ref message)) if message.contains("writer epoch"))
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
            matches!(encode_genesis(7,&address(),&definition,&[0xa5;32],4096,&job),Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted"))
        );
        assert_eq!(admission.usage().unwrap(), before);
    }
}
