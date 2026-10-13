//! Narrow FWQP1 resolved mutation frame. No claim, provision or administrative
//! result variants are implied. Sequence is insertion ordering, never an apply
//! cursor. Atomic storage still verifies raw schema, predecessors and immutable
//! receipt authority before applying all commands plus cursor in one transaction.
use crate::{
    queue_admission::{QueueBufferCharge, QueueReservation},
    queue_body::ResolvedBody,
    queue_fields::ResolvedFields,
    queue_planner::{CommitDelta, RecordIdentity, RecordSnapshot, StoredWork},
    queue_schema_codec::DefinitionPin,
};
use fireweed_core::queue_contract::*;
use sha2::{Digest, Sha256};

mod insert_binding;
pub use insert_binding::validate_insert_receipt;
mod administrative_decision;
pub use administrative_decision::*;
mod administrative_frame;
mod administrative_receipt;
pub use administrative_frame::*;
mod administrative_result;
pub use administrative_receipt::*;
mod measurement;
pub use administrative_result::*;
mod work_fragment;
pub use work_fragment::*;
mod receipt_projection;
pub use receipt_projection::{
    CommitReceiptProjection, FieldsFingerprint, ReceiptLookupProjection,
    borrowed_fields_fingerprint, owned_fields_fingerprint, receipt_outcome_heap_bound,
    state_record_encoded_len,
};
#[cfg(test)]
mod replay_tests;
mod view;
pub use measurement::*;
pub use view::*;

const MAGIC: &[u8; 5] = b"FWQP1";
const MAGIC_V2: &[u8; 5] = b"FWQP2";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationFormat {
    V1,
    V2,
}
#[derive(Debug, PartialEq, Eq)]
pub enum PublicationError {
    Invalid(&'static str),
    FrameLimit,
    Overflow,
    Admission(QueueError),
}
pub type PublicationResult<T> = Result<T, PublicationError>;
fn invalid<T>(reason: &'static str) -> PublicationResult<T> {
    Err(PublicationError::Invalid(reason))
}

/// Receipt creation is distinct from replay/conflict/expired responses. The
/// caller must hold fresh receipt authority; this type cannot establish it.
#[derive(Debug)]
pub struct PublicationReceipt<'a> {
    queue: &'a QueueAddress,
    id: &'a str,
    fingerprint: [u8; 32],
    result: ReceiptResult<'a>,
}
#[derive(Debug)]
enum ReceiptResult<'a> {
    Insert(&'a [InsertOutcome]),
    Commit(&'a GroupOutcome),
}
impl<'a> PublicationReceipt<'a> {
    pub fn commit(
        queue: &'a QueueAddress,
        group: &'a CommitGroup,
        receipt: &'a CommitReceipt,
    ) -> PublicationResult<Self> {
        if group.administration.is_some() {
            return invalid("administration requires version-4 publication");
        }
        if group.request_id != receipt.request_id
            || matches!(
                receipt.outcome,
                GroupOutcome::RequestConflict
                    | GroupOutcome::RequestExpired
                    | GroupOutcome::AdministrativeCommitted { .. }
                    | GroupOutcome::Rejected(Rejection::TooLarge)
            )
        {
            return invalid("not a fresh commit receipt");
        }
        if let GroupOutcome::Committed { versions } = &receipt.outcome {
            validate_commit_result_versions(group, versions)?;
        }
        Ok(Self {
            queue,
            id: &group.request_id,
            fingerprint: group
                .fingerprint(queue)
                .map_err(PublicationError::Admission)?,
            result: ReceiptResult::Commit(&receipt.outcome),
        })
    }
    pub fn insert(
        request: &'a InsertRequest,
        outcomes: &'a [InsertOutcome],
    ) -> PublicationResult<Self> {
        insert_binding::validate_owned(request, outcomes)?;
        Ok(Self {
            queue: &request.queue,
            id: &request.request_id,
            fingerprint: fingerprint("insert", request).map_err(PublicationError::Admission)?,
            result: ReceiptResult::Insert(outcomes),
        })
    }
    fn identity(&self) -> (u8, &str) {
        (
            match self.result {
                ReceiptResult::Insert(_) => 0,
                ReceiptResult::Commit(_) => 2,
            },
            self.id,
        )
    }
}

/// Bind an immutable retained disposition to its original complete group.
/// Metadata fingerprint/expiry resolution must precede this outcome validation.
pub fn validate_original_commit_receipt(
    queue: &QueueAddress,
    group: &CommitGroup,
    receipt: &CommitReceipt,
    reservation: &QueueReservation,
) -> PublicationResult<()> {
    let Some(admin) = &group.administration else {
        PublicationReceipt::commit(queue, group, receipt)?;
        return Ok(());
    };
    if receipt.request_id != group.request_id {
        return invalid("administrative receipt identity");
    }
    let outcome = match &receipt.outcome {
        GroupOutcome::AdministrativeCommitted {
            versions,
            administration,
        } => AdministrativeReceiptOutcome::Committed {
            versions,
            administration,
        },
        GroupOutcome::Rejected(reason) => AdministrativeReceiptOutcome::Rejected(reason),
        _ => return invalid("administrative receipt has another disposition domain"),
    };
    let binding = AdministrativePublicationReceipt::new(
        queue,
        group,
        &admin.intent,
        admin.max_result_bytes,
        &outcome,
        reservation,
    )?;
    if binding.result_budget_exceeded()? {
        return invalid("administrative receipt exceeds original result bound");
    }
    Ok(())
}

pub(crate) fn validate_commit_result_versions(
    group: &CommitGroup,
    versions: &[RecordVersion],
) -> PublicationResult<()> {
    let expected = group.modified_records();
    let count = group.modified_records().count();
    if count != versions.len()
        || expected
            .zip(versions)
            .any(|(id, v)| id != (v.collection.as_deref(), v.key.as_str()) || v.version == 0)
    {
        return invalid("commit result identity/order");
    }
    Ok(())
}

#[derive(Debug)]
pub struct PublicationFrame<'a> {
    bytes: Vec<u8>,
    _charge: QueueBufferCharge<'a>,
}
impl PublicationFrame<'_> {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Inputs already belong to an admitted planner job. This compiler allocates no
/// command sorting containers: commands and receipts must be strictly sorted.
/// Receipt validation uses an admitted temporary borrowed-identity bounds map.
/// Its frame allocation is charged before construction and retained with bytes.
pub fn encode_publication<'a>(
    queue: &QueueAddress,
    raw_definition: &[u8],
    before: &RecordSnapshot,
    delta: &CommitDelta,
    receipts: &[PublicationReceipt<'_>],
    max_frame_bytes: u64,
    reservation: &'a QueueReservation,
) -> PublicationResult<PublicationFrame<'a>> {
    encode_publication_pinned(
        queue,
        DefinitionPin::from_persisted_bytes(raw_definition),
        before,
        delta,
        receipts,
        max_frame_bytes,
        reservation,
    )
}
/// Encode using the exact persisted definition pin already held by a fenced job.
pub fn encode_publication_pinned<'a>(
    queue: &QueueAddress,
    pin: DefinitionPin,
    before: &RecordSnapshot,
    delta: &CommitDelta,
    receipts: &[PublicationReceipt<'_>],
    max_frame_bytes: u64,
    reservation: &'a QueueReservation,
) -> PublicationResult<PublicationFrame<'a>> {
    validate_delta(before, delta)?;
    if receipts.iter().any(|r| r.queue != queue) {
        return invalid("receipt queue mismatch");
    }
    for pair in receipts.windows(2) {
        if pair[0].identity() >= pair[1].identity() {
            return invalid("receipt identity order");
        }
    }
    validate_receipt_bounds(before, delta, receipts, reservation)?;
    let mut count = Sink::Count(0);
    write_frame(&mut count, queue, pin, before, delta, receipts)?;
    let Sink::Count(size) = count else {
        unreachable!()
    };
    if size > max_frame_bytes {
        return Err(PublicationError::FrameLimit);
    }
    let capacity = usize::try_from(size).map_err(|_| PublicationError::Overflow)?;
    let charge = reservation
        .buffer(size.checked_add(128).ok_or(PublicationError::Overflow)?)
        .map_err(PublicationError::Admission)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| PublicationError::Overflow)?;
    write_frame(
        &mut Sink::Buffer(&mut bytes),
        queue,
        pin,
        before,
        delta,
        receipts,
    )?;
    if bytes.len() != capacity {
        return invalid("frame size changed");
    }
    // The same full scanner used at replay also rejects any encoder defect.
    scan_publication(&bytes)?;
    Ok(PublicationFrame {
        bytes,
        _charge: charge,
    })
}

fn validate_receipt_bounds(
    before: &RecordSnapshot,
    d: &CommitDelta,
    receipts: &[PublicationReceipt<'_>],
    reservation: &QueueReservation,
) -> PublicationResult<()> {
    if receipts.is_empty() {
        return Ok(());
    }
    // The only owned validation container: borrowed identities plus two u64s.
    // Charge a 1024-byte root/node floor plus conservative per-entry capacity
    // before allocation; release it as
    // soon as receipt validation ends, before allocating the output frame.
    let bytes = (d.versions.len() as u64)
        .checked_mul(192)
        .and_then(|n| n.checked_add(1024))
        .ok_or(PublicationError::Overflow)?;
    let _scratch_charge = reservation
        .buffer(bytes)
        .map_err(PublicationError::Admission)?;
    let mut authority = InitialAuthority::new(before);
    let mut bounds = std::collections::BTreeMap::new();
    for v in &d.versions {
        let prior = authority.next(v.collection.as_deref(), &v.key);
        bounds.insert(
            (v.collection.as_deref(), v.key.as_str()),
            (prior.highwater, v.version),
        );
    }
    let check = |collection: Option<&str>, key: &str, version: u64| -> PublicationResult<()> {
        let (prior, final_version) =
            bounds
                .get(&(collection, key))
                .ok_or(PublicationError::Invalid(
                    "success receipt lacks resolved version",
                ))?;
        if version == 0 || version < *prior || version > *final_version {
            return invalid("success receipt outside resolved version bounds");
        }
        Ok(())
    };
    for receipt in receipts {
        match &receipt.result {
            ReceiptResult::Commit(GroupOutcome::Committed { versions }) => {
                for v in versions {
                    check(v.collection.as_deref(), &v.key, v.version)?;
                }
            }
            ReceiptResult::Insert(outcomes) => {
                for outcome in *outcomes {
                    if let InsertOutcome::Inserted { key, version }
                    | InsertOutcome::Replaced { key, version } = outcome
                    {
                        check(None, key, *version)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_delta(before: &RecordSnapshot, d: &CommitDelta) -> PublicationResult<()> {
    if d.next_sequence < before.next_sequence {
        return invalid("regressing insertion sequence");
    }
    if d.work
        .windows(2)
        .any(|p| p[0].record.work.key >= p[1].record.work.key)
        || d.deleted_work.windows(2).any(|p| p[0] >= p[1])
        || d.state
            .windows(2)
            .any(|p| (&p[0].collection, &p[0].key) >= (&p[1].collection, &p[1].key))
        || d.deleted_state.windows(2).any(|p| p[0] >= p[1])
        || d.versions
            .windows(2)
            .any(|p| (&p[0].collection, &p[0].key) >= (&p[1].collection, &p[1].key))
    {
        return invalid("resolved command order");
    }
    let effects = d
        .work
        .len()
        .checked_add(d.deleted_work.len())
        .and_then(|n| n.checked_add(d.state.len()))
        .and_then(|n| n.checked_add(d.deleted_state.len()))
        .ok_or(PublicationError::Overflow)?;
    if effects != d.versions.len()
        || d.work_fields.len() != d.work.len()
        || d.work_bodies.len() != d.work.len()
    {
        return invalid("resolved command/version bijection");
    }
    let mut authority = InitialAuthority::new(before);
    for v in &d.versions {
        let prior = authority.next(v.collection.as_deref(), &v.key);
        let existing = prior.version();
        let highwater = prior.highwater;
        if prior.work.is_some_and(|w| {
            w.record.work.key != v.key
                || w.record.insertion_sequence == 0
                || w.record.insertion_sequence > before.next_sequence
        }) || prior.state.is_some_and(|r| {
            Some(r.collection.as_str()) != v.collection.as_deref() || r.key != v.key
        }) {
            return invalid("corrupt initial record identity/sequence");
        }
        if existing.is_some_and(|n| n == 0 || n != highwater)
            || v.version == 0
            || v.version < highwater
            || (existing.is_none() && v.version == highwater)
        {
            return invalid("invalid predecessor highwater");
        }
        if let Some(collection) = &v.collection {
            let row = d
                .state
                .binary_search_by(|r| {
                    (r.collection.as_str(), r.key.as_str())
                        .cmp(&(collection.as_str(), v.key.as_str()))
                })
                .ok()
                .map(|i| &d.state[i]);
            let deleted = d
                .deleted_state
                .binary_search_by(|(c, k)| {
                    (c.as_str(), k.as_str()).cmp(&(collection.as_str(), v.key.as_str()))
                })
                .is_ok();
            if row.is_some() == deleted {
                return invalid("state effect identity");
            }
            if let Some(row) = row {
                if row.version != v.version {
                    return invalid("state output version");
                }
                if v.version == highwater && prior.state != Some(row) {
                    return invalid("non-advancing state change");
                }
            } else if v.version == highwater {
                return invalid("non-advancing state deletion");
            }
        } else {
            let row = d
                .work
                .binary_search_by(|r| r.record.work.key.as_str().cmp(&v.key))
                .ok()
                .map(|i| &d.work[i]);
            let deleted = d.deleted_work.binary_search(&v.key).is_ok();
            if row.is_some() == deleted {
                return invalid("work effect identity");
            }
            if let Some(row) = row {
                let fields = d
                    .work_fields
                    .get(&v.key)
                    .ok_or(PublicationError::Invalid("missing field intent"))?;
                let body = d
                    .work_bodies
                    .get(&v.key)
                    .ok_or(PublicationError::Invalid("missing body intent"))?;
                if row.record.version != v.version
                    || row.record.insertion_sequence == 0
                    || row.record.insertion_sequence > d.next_sequence
                {
                    return invalid("work output version/sequence");
                }
                if prior.work.is_none_or(|old| {
                    row.record.insertion_sequence != old.record.insertion_sequence
                }) && row.record.insertion_sequence <= before.next_sequence
                {
                    return invalid("new work sequence does not advance insertion order");
                }
                validate_lease(
                    row.record.lifecycle,
                    row.record.lease_expires_at.is_some(),
                    row.lease_hash.is_some(),
                )?;
                if existing.is_none()
                    && (!matches!(fields, ResolvedFields::Replace(_))
                        || !matches!(body, ResolvedBody::Replace(_)))
                {
                    return invalid("partial absent work creation");
                }
                crate::queue_planner::validate_work_write_view(prior.work, row, fields, body)
                    .map_err(|_| PublicationError::Invalid("resolved command/view mismatch"))?;
                if v.version == highwater {
                    let old = before
                        .work
                        .get(&v.key)
                        .ok_or(PublicationError::Invalid("absent no-op"))?;
                    if !same_metadata(old, row)
                        || !matches!(fields, ResolvedFields::Preserve)
                        || !matches!(body, ResolvedBody::Preserve)
                    {
                        return invalid("non-advancing work change");
                    }
                }
            } else if v.version == highwater {
                return invalid("non-advancing work deletion");
            }
        }
    }
    Ok(())
}
struct InitialAuthority<'a> {
    versions: std::iter::Peekable<std::collections::btree_map::Iter<'a, RecordIdentity, u64>>,
    state:
        std::iter::Peekable<std::collections::btree_map::Iter<'a, (String, String), StateRecord>>,
    work: &'a std::collections::BTreeMap<String, StoredWork>,
}
struct Prior<'a> {
    highwater: u64,
    work: Option<&'a StoredWork>,
    state: Option<&'a StateRecord>,
}
impl Prior<'_> {
    fn version(&self) -> Option<u64> {
        self.work
            .map(|w| w.record.version)
            .or_else(|| self.state.map(|s| s.version))
    }
}
impl<'a> InitialAuthority<'a> {
    fn new(before: &'a RecordSnapshot) -> Self {
        Self {
            versions: before.versions.iter().peekable(),
            state: before.state.iter().peekable(),
            work: &before.work,
        }
    }
    // Calls follow strictly ascending command identities, so each authority row
    // is visited once. All names are borrowed; no temporary key clone is needed.
    fn next(&mut self, collection: Option<&str>, key: &str) -> Prior<'a> {
        let id = (collection, key);
        while self
            .versions
            .peek()
            .is_some_and(|(v, _)| (v.collection.as_deref(), v.key.as_str()) < id)
        {
            self.versions.next();
        }
        let highwater = self
            .versions
            .peek()
            .filter(|(v, _)| (v.collection.as_deref(), v.key.as_str()) == id)
            .map_or(0, |(_, v)| **v);
        let state = if let Some(c) = collection {
            while self
                .state
                .peek()
                .is_some_and(|((name, k), _)| (name.as_str(), k.as_str()) < (c, key))
            {
                self.state.next();
            }
            self.state
                .peek()
                .filter(|((name, k), _)| (name.as_str(), k.as_str()) == (c, key))
                .map(|(_, r)| *r)
        } else {
            None
        };
        Prior {
            highwater,
            state,
            work: if collection.is_none() {
                self.work.get(key)
            } else {
                None
            },
        }
    }
}
fn same_metadata(a: &StoredWork, b: &StoredWork) -> bool {
    let (a_record, b_record) = (&a.record, &b.record);
    a_record.work.priority == b_record.work.priority
        && a_record.work.available_at == b_record.work.available_at
        && a_record.version == b_record.version
        && a_record.insertion_sequence == b_record.insertion_sequence
        && a_record.lifecycle == b_record.lifecycle
        && a_record.eligible_since == b_record.eligible_since
        && a_record.attempts == b_record.attempts
        && a_record.lease_expires_at == b_record.lease_expires_at
        && a.lease_hash == b.lease_hash
}
fn validate_lease(life: Lifecycle, expiry: bool, hash: bool) -> PublicationResult<()> {
    if (life == Lifecycle::Leased) != expiry || expiry != hash {
        invalid("malformed lease metadata")
    } else {
        Ok(())
    }
}

// FWQP1 integers/counts are fixed-width little-endian; UTF-8/byte lengths u64.
// Tags are explicit below, independent of all public serde variant ordinals.
enum Sink<'a> {
    Count(u64),
    Buffer(&'a mut Vec<u8>),
    Digest(Sha256),
}
impl Sink<'_> {
    fn raw(&mut self, bytes: &[u8]) -> PublicationResult<()> {
        match self {
            Self::Count(n) => {
                *n = n
                    .checked_add(bytes.len() as u64)
                    .ok_or(PublicationError::Overflow)?
            }
            Self::Buffer(v) => v.extend_from_slice(bytes),
            Self::Digest(hash) => hash.update(bytes),
        };
        Ok(())
    }
    fn tag(&mut self, n: u8) -> PublicationResult<()> {
        self.raw(&[n])
    }
    fn n(&mut self, n: u64) -> PublicationResult<()> {
        self.raw(&n.to_le_bytes())
    }
    fn i(&mut self, n: i64) -> PublicationResult<()> {
        self.raw(&n.to_le_bytes())
    }
    fn bytes(&mut self, b: &[u8]) -> PublicationResult<()> {
        self.n(u64::try_from(b.len()).map_err(|_| PublicationError::Overflow)?)?;
        self.raw(b)
    }
    fn text(&mut self, t: &str) -> PublicationResult<()> {
        self.bytes(t.as_bytes())
    }
}
fn value(s: &mut Sink<'_>, v: &FieldValue) -> PublicationResult<()> {
    match v {
        FieldValue::Integer(n) => {
            s.tag(0)?;
            s.i(*n)
        }
        FieldValue::Float(n) => {
            if !n.is_finite() {
                return invalid("nonfinite field");
            }
            s.tag(1)?;
            s.raw(&n.to_bits().to_le_bytes())
        }
        FieldValue::Boolean(n) => {
            s.tag(2)?;
            s.tag(u8::from(*n))
        }
        FieldValue::Text(t) => {
            s.tag(3)?;
            s.text(t)
        }
        FieldValue::Bytes(b) => {
            s.tag(4)?;
            s.bytes(b)
        }
    }
}
fn fields(s: &mut Sink<'_>, f: &Fields) -> PublicationResult<()> {
    s.n(f.len() as u64)?;
    for (n, v) in f {
        s.text(n)?;
        value(s, v)?;
    }
    Ok(())
}
fn rejection(s: &mut Sink<'_>, r: &Rejection) -> PublicationResult<()> {
    let tag = match r {
        Rejection::StaleLease => 0,
        Rejection::VersionConflict => 1,
        Rejection::AlreadyExists => 2,
        Rejection::NotFound => 3,
        Rejection::Leased => 4,
        Rejection::Invalid(_) => 5,
        Rejection::UniqueConflict => 6,
        Rejection::Terminal => 7,
        Rejection::TooLarge => return invalid("TooLarge requires version-4 publication"),
    };
    s.tag(tag)?;
    if let Rejection::Invalid(reason) = r {
        s.text(reason)?;
    }
    Ok(())
}
fn identity(s: &mut Sink<'_>, collection: Option<&str>, key: &str) -> PublicationResult<()> {
    if let Some(c) = collection {
        s.tag(1)?;
        s.text(c)?;
    } else {
        s.tag(0)?;
    }
    s.text(key)
}
fn write_state_value(s: &mut Sink<'_>, row: Option<&StateRecord>) -> PublicationResult<()> {
    if let Some(row) = row {
        s.tag(1)?;
        fields(s, &row.fields)?;
        s.bytes(&row.value)
    } else {
        s.tag(0)
    }
}
fn write_work_value(
    s: &mut Sink<'_>,
    row: &StoredWork,
    change: &ResolvedFields,
    body: &ResolvedBody,
) -> PublicationResult<()> {
    let r = &row.record;
    s.tag(1)?;
    s.i(r.work.priority)?;
    s.i(r.work.available_at)?;
    s.n(r.insertion_sequence)?;
    s.tag(match r.lifecycle {
        Lifecycle::Pending => 0,
        Lifecycle::Leased => 1,
        Lifecycle::Completed => 2,
        Lifecycle::Failed => 3,
    })?;
    s.i(r.eligible_since)?;
    s.raw(&r.attempts.to_le_bytes())?;
    s.tag(u8::from(r.lease_expires_at.is_some()))?;
    if let Some(expiry) = r.lease_expires_at {
        s.i(expiry)?;
    }
    s.tag(u8::from(row.lease_hash.is_some()))?;
    if let Some(hash) = row.lease_hash {
        s.raw(&hash)?;
    }
    match change {
        ResolvedFields::Preserve => s.tag(0)?,
        ResolvedFields::Replace(f) => {
            s.tag(1)?;
            fields(s, f)?;
        }
        ResolvedFields::Edit(edits) => {
            s.tag(2)?;
            s.n(edits.len() as u64)?;
            for (n, v) in edits {
                s.text(n)?;
                s.tag(u8::from(v.is_some()))?;
                if let Some(v) = v {
                    value(s, v)?;
                }
            }
        }
    }
    match body {
        ResolvedBody::Preserve => s.tag(0)?,
        ResolvedBody::Replace(b) => {
            s.tag(1)?;
            s.bytes(b)?;
        }
    }
    Ok(())
}
fn write_receipt_header(
    s: &mut Sink<'_>,
    domain: u8,
    id: &str,
    fingerprint_version: u8,
    fingerprint: &[u8; 32],
) -> PublicationResult<()> {
    s.tag(domain)?;
    s.text(id)?;
    s.tag(fingerprint_version)?;
    s.raw(fingerprint)
}
fn write_receipt(s: &mut Sink<'_>, r: &PublicationReceipt<'_>) -> PublicationResult<()> {
    write_receipt_header(s, r.identity().0, r.id, 3, &r.fingerprint)?;
    match &r.result {
        ReceiptResult::Commit(outcome) => match outcome {
            GroupOutcome::Committed { versions } => {
                s.tag(0)?;
                s.n(versions.len() as u64)?;
                for v in versions {
                    if v.version == 0 {
                        return invalid("zero receipt version");
                    }
                    identity(s, v.collection.as_deref(), &v.key)?;
                    s.n(v.version)?;
                }
            }
            GroupOutcome::Rejected(reason) => {
                s.tag(1)?;
                rejection(s, reason)?;
            }
            _ => return invalid("non-fresh commit outcome"),
        },
        ReceiptResult::Insert(outcomes) => {
            s.n(outcomes.len() as u64)?;
            for o in *outcomes {
                match o {
                    InsertOutcome::Inserted { key, version } => {
                        s.tag(0)?;
                        s.text(key)?;
                        s.n(*version)?;
                    }
                    InsertOutcome::Replaced { key, version } => {
                        s.tag(1)?;
                        s.text(key)?;
                        s.n(*version)?;
                    }
                    InsertOutcome::Rejected { key, reason } => {
                        s.tag(2)?;
                        s.text(key)?;
                        rejection(s, reason)?;
                    }
                }
            }
        }
    }
    Ok(())
}
fn write_mutation_header(
    s: &mut Sink<'_>,
    version: &RecordVersion,
    predecessor: Option<u64>,
    highwater: u64,
) -> PublicationResult<()> {
    identity(s, version.collection.as_deref(), &version.key)?;
    s.tag(u8::from(predecessor.is_some()))?;
    if let Some(previous) = predecessor {
        s.n(previous)?;
    }
    s.n(highwater)?;
    s.n(version.version)
}
fn write_frame_header(
    s: &mut Sink<'_>,
    queue: &QueueAddress,
    pin: DefinitionPin,
    initial: u64,
    final_sequence: u64,
    commands: u64,
) -> PublicationResult<()> {
    write_frame_header_as(s, queue, pin, initial, final_sequence, commands, MAGIC)
}
fn write_frame_header_as(
    s: &mut Sink<'_>,
    queue: &QueueAddress,
    pin: DefinitionPin,
    initial: u64,
    final_sequence: u64,
    commands: u64,
    magic: &[u8; 5],
) -> PublicationResult<()> {
    s.raw(magic)?;
    s.text(&queue.tenant)?;
    s.text(&queue.queue)?;
    s.raw(pin.as_bytes())?;
    s.n(initial)?;
    s.n(final_sequence)?;
    s.n(commands)
}
fn write_frame(
    s: &mut Sink<'_>,
    queue: &QueueAddress,
    pin: DefinitionPin,
    before: &RecordSnapshot,
    d: &CommitDelta,
    receipts: &[PublicationReceipt<'_>],
) -> PublicationResult<()> {
    let mut authority = InitialAuthority::new(before);
    let mutation_count = d
        .versions
        .iter()
        .filter(|v| v.version > authority.next(v.collection.as_deref(), &v.key).highwater)
        .count();
    write_frame_header(
        s,
        queue,
        pin,
        before.next_sequence,
        d.next_sequence,
        mutation_count as u64,
    )?;
    let mut authority = InitialAuthority::new(before);
    for v in &d.versions {
        let prior = authority.next(v.collection.as_deref(), &v.key);
        if v.version == prior.highwater {
            continue;
        }
        write_delta_mutation(s, v, &prior, d)?;
    }
    s.n(receipts.len() as u64)?;
    for r in receipts {
        write_receipt(s, r)?;
    }
    Ok(())
}

fn write_delta_mutation(
    s: &mut Sink<'_>,
    version: &RecordVersion,
    prior: &Prior<'_>,
    delta: &CommitDelta,
) -> PublicationResult<()> {
    write_mutation_header(s, version, prior.version(), prior.highwater)?;
    if let Some(collection) = &version.collection {
        let row = delta
            .state
            .binary_search_by(|row| {
                (row.collection.as_str(), row.key.as_str())
                    .cmp(&(collection.as_str(), version.key.as_str()))
            })
            .ok()
            .map(|i| &delta.state[i]);
        write_state_value(s, row)
    } else if let Ok(index) = delta
        .work
        .binary_search_by(|row| row.record.work.key.cmp(&version.key))
    {
        write_work_value(
            s,
            &delta.work[index],
            &delta.work_fields[&version.key],
            &delta.work_bodies[&version.key],
        )
    } else {
        s.tag(0)
    }
}
/// Full zero-allocation scan. Length bounds come from the supplied frame slice;
/// no decoded payload, list or seen-set is retained. Receipt outcomes preserve
/// request order; only outer receipt identities are canonical sorted identities.
#[derive(Debug, PartialEq, Eq)]
pub struct PublicationSummary<'a> {
    pub queue: (&'a str, &'a str),
    pub definition_pin: &'a [u8],
    pub initial_sequence: u64,
    pub final_sequence: u64,
    pub commands: u64,
    pub receipts: u64,
}
struct Scan<'a> {
    rest: &'a [u8],
}
impl<'a> Scan<'a> {
    fn raw(&mut self, n: usize) -> PublicationResult<&'a [u8]> {
        if n > self.rest.len() {
            return invalid("truncated frame");
        }
        let (b, rest) = self.rest.split_at(n);
        self.rest = rest;
        Ok(b)
    }
    fn tag(&mut self) -> PublicationResult<u8> {
        Ok(self.raw(1)?[0])
    }
    fn n(&mut self) -> PublicationResult<u64> {
        Ok(u64::from_le_bytes(self.raw(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> PublicationResult<&'a [u8]> {
        let n = usize::try_from(self.n()?).map_err(|_| PublicationError::Overflow)?;
        self.raw(n)
    }
    fn text(&mut self) -> PublicationResult<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| PublicationError::Invalid("invalid UTF-8"))
    }
    fn count(&mut self) -> PublicationResult<u64> {
        let n = self.n()?;
        if n > self.rest.len() as u64 {
            return invalid("impossible frame count");
        }
        Ok(n)
    }
    fn presence(&mut self) -> PublicationResult<bool> {
        match self.tag()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => invalid("invalid presence tag"),
        }
    }
    fn identity(&mut self) -> PublicationResult<(Option<&'a str>, &'a str)> {
        let c = if self.presence()? {
            Some(self.text()?)
        } else {
            None
        };
        Ok((c, self.text()?))
    }
    #[cfg(test)]
    fn rejection(&mut self) -> PublicationResult<()> {
        view::parse_rejection(self).map(|_| ())
    }
}
pub fn scan_publication(raw: &[u8]) -> PublicationResult<PublicationSummary<'_>> {
    view::summary(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_fields::FieldCoverage,
        queue_planner::{combine_deltas, plan_commit_group},
        queue_schema_codec::encode_definition,
    };
    use sha2::Digest;
    use std::collections::{BTreeMap, BTreeSet};
    pub(super) fn schema() -> QueueSchema {
        let def = CollectionDefinition {
            fields: BTreeMap::from([
                ("blob".into(), FieldType::Bytes),
                ("float".into(), FieldType::Float),
                ("gate".into(), FieldType::Boolean),
                ("number".into(), FieldType::Integer),
                ("text".into(), FieldType::Text),
            ]),
            indexes: BTreeMap::new(),
            unique_indexes: BTreeSet::new(),
            additional_field_prefixes: BTreeSet::from(["/payload/".into()]),
        };
        QueueSchema {
            work: def.clone(),
            state: BTreeMap::from([("instances".into(), def)]),
            progress_bound_nanos: 1,
            max_attempts: 3,
        }
    }
    pub(super) fn queue() -> QueueAddress {
        QueueAddress {
            tenant: "t\0/α".into(),
            queue: "q\0/β".into(),
        }
    }
    pub(super) fn group(id: &str) -> CommitGroup {
        CommitGroup {
            administration: None,
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![],
        }
    }
    #[test]
    fn retained_receipt_binding_preserves_fp3_and_fp4_disposition_domains() {
        use fireweed_core::queue_administration::*;
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let mut original = group("original");
        let receipt = CommitReceipt {
            request_id: original.request_id.clone(),
            outcome: GroupOutcome::Rejected(Rejection::TooLarge),
        };
        assert!(PublicationReceipt::commit(&queue(), &original, &receipt).is_err());
        assert!(validate_original_commit_receipt(&queue(), &original, &receipt, &job).is_err());
        original.administration = Some(AdministrativeCommit {
            intent: AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation: AdministrativeOperation::SelectFirst(vec![]),
            },
            max_result_bytes: 1024,
        });
        validate_original_commit_receipt(&queue(), &original, &receipt, &job).unwrap();
        let mut wrong_id = receipt.clone();
        wrong_id.request_id = "another".into();
        assert!(validate_original_commit_receipt(&queue(), &original, &wrong_id, &job).is_err());
        let ordinary = CommitReceipt {
            request_id: "original".into(),
            outcome: GroupOutcome::Committed { versions: vec![] },
        };
        assert!(validate_original_commit_receipt(&queue(), &original, &ordinary, &job).is_err());
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: Some(BTreeSet::from(["text".into()])),
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "a".into(),
                expected_version: Some(1),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch::default(),
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        };
        let mut histogram = AdministrativeHistogram::default();
        histogram.record(&AdministrativeOutcome::NoChange).unwrap();
        let result = AdministrativeResult {
            histogram,
            clauses: vec![],
            items: vec![AdministrativeItemResult {
                key: "a".into(),
                outcome: AdministrativeOutcome::NoChange,
                before: Some(AdministrativeBefore {
                    version: 1,
                    lifecycle: Lifecycle::Pending,
                    available_at: 0,
                    eligible_since: 0,
                    attempts: 0,
                    lease_expires_at: None,
                    fields: BTreeMap::from([("text".into(), FieldValue::Text("x".repeat(512)))]),
                }),
            }],
        };
        let complete = measure_complete_administrative_receipt(
            &original.request_id,
            &AdministrativeReceiptOutcome::Committed {
                versions: &[],
                administration: &result,
            },
        )
        .unwrap();
        original.administration = Some(AdministrativeCommit {
            intent,
            max_result_bytes: complete,
        });
        original
            .validate_for(&queue(), &schema(), &QueueLimits::default())
            .unwrap();
        let mut committed = CommitReceipt {
            request_id: original.request_id.clone(),
            outcome: GroupOutcome::AdministrativeCommitted {
                versions: vec![],
                administration: result,
            },
        };
        validate_original_commit_receipt(&queue(), &original, &committed, &job).unwrap();
        original.administration.as_mut().unwrap().max_result_bytes -= 1;
        assert!(matches!(
            validate_original_commit_receipt(&queue(), &original, &committed, &job),
            Err(PublicationError::Invalid(
                "administrative receipt exceeds original result bound"
            ))
        ));
        original.administration.as_mut().unwrap().max_result_bytes += 1;
        if let GroupOutcome::AdministrativeCommitted { administration, .. } = &mut committed.outcome
        {
            administration.items[0].key = "b".into();
        }
        assert!(validate_original_commit_receipt(&queue(), &original, &committed, &job).is_err());
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    pub(super) fn seed() -> CommitGroup {
        let mut g = group("a-seed");
        g.work_inserts.push(NewWork {
            key: "key\0/λ".into(),
            priority: -7,
            available_at: i64::MIN,
            fields: BTreeMap::from([
                ("blob".into(), FieldValue::Bytes(vec![0, 255])),
                ("float".into(), FieldValue::Float(-0.0)),
                ("gate".into(), FieldValue::Boolean(true)),
                ("number".into(), FieldValue::Integer(i64::MIN)),
                ("text".into(), FieldValue::Text("\0α".into())),
            ]),
            body: vec![0, 255].into(),
        });
        g.state_updates.push(StateUpdate {
            collection: "instances".into(),
            key: "state\0/β".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: BTreeMap::new(),
                value: vec![1, 2].into(),
            },
        });
        g
    }
    #[test]
    fn frozen_roundtrip_scan_charging_and_reject_every_truncation() {
        let q = queue();
        let schema = schema();
        let raw = encode_definition(&schema).unwrap();
        let before = RecordSnapshot::default();
        let mut after = before.clone();
        let g = seed();
        let delta = plan_commit_group(&g, &mut after, 0, &schema).unwrap();
        let packed = combine_deltas(std::slice::from_ref(&delta), 0).unwrap();
        let receipt = PublicationReceipt::commit(&q, &g, &delta.receipt).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(1).unwrap();
        let frame =
            encode_publication(&q, &raw, &before, &packed, &[receipt], 10000, &job).unwrap();
        let bytes = frame.as_bytes();
        assert_eq!(bytes.len(), 489);
        let summary = scan_publication(bytes).unwrap();
        assert_eq!(summary.commands, 2);
        assert_eq!(summary.receipts, 1);
        assert_eq!(summary.queue, (q.tenant.as_str(), q.queue.as_str()));
        assert_eq!(
            summary.definition_pin,
            DefinitionPin::from_persisted_bytes(&raw).as_bytes()
        );
        assert_eq!(admission.usage().unwrap(), (1, bytes.len() as u64 + 129));
        for end in 0..bytes.len() {
            assert!(
                scan_publication(&bytes[..end]).is_err(),
                "accepted truncation {end}"
            );
        }
        let mut corrupt = bytes.to_vec();
        corrupt[4] = b'2';
        assert!(scan_publication(&corrupt).is_err());
        corrupt = bytes.to_vec();
        corrupt.push(0);
        assert!(scan_publication(&corrupt).is_err());
        let hash = sha2::Sha256::digest(bytes);
        assert_eq!(
            hash.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "aaf900b5512de2f9d137a24310256c48654c89ad8c1b5766a056319b754696b2"
        );
        drop(frame);
        assert_eq!(admission.usage().unwrap(), (1, 1));
        let receipt = PublicationReceipt::commit(&q, &g, &delta.receipt).unwrap();
        assert!(matches!(
            encode_publication(&q, &raw, &before, &packed, &[receipt], 1, &job),
            Err(PublicationError::FrameLimit)
        ));
        assert_eq!(admission.usage().unwrap(), (1, 1));
    }
    #[test]
    fn metadata_publication_does_not_encode_read_materialization() {
        let q = queue();
        let schema = schema();
        let raw = encode_definition(&schema).unwrap();
        let mut initial = RecordSnapshot::default();
        let mut g = seed();
        g.work_inserts[0].body = vec![9; 1 << 20].into();
        g.work_inserts[0]
            .fields
            .insert("blob".into(), FieldValue::Bytes(vec![8; 1 << 20]));
        plan_commit_group(&g, &mut initial, 0, &schema).unwrap();
        let mut patch = group("patch");
        patch.work_updates.push(UnclaimedUpdate {
            key: g.work_inserts[0].key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(77),
                ..WorkPatch::default()
            }),
        });
        let mut complete = initial.clone();
        let left = plan_commit_group(&patch, &mut complete, 0, &schema).unwrap();
        let left = combine_deltas(&[left], initial.next_sequence).unwrap();
        let mut selected = initial.clone();
        let old = selected.work.get_mut(&g.work_inserts[0].key).unwrap();
        old.record.work.fields.clear();
        old.field_coverage = FieldCoverage::Selected {
            names: BTreeSet::new(),
            prefixes: BTreeSet::new(),
        };
        old.record.work.body = Default::default();
        old.body_loaded = false;
        let selected_before = selected.clone();
        let right = plan_commit_group(&patch, &mut selected, 0, &schema).unwrap();
        let right = combine_deltas(&[right], selected_before.next_sequence).unwrap();
        let receipt = CommitReceipt {
            request_id: patch.request_id.clone(),
            outcome: GroupOutcome::Committed {
                versions: left.versions.clone(),
            },
        };
        let a = QueueAdmission::new(1, 4096).unwrap();
        let job = a.reserve(0).unwrap();
        let l = encode_publication(
            &q,
            &raw,
            &initial,
            &left,
            &[PublicationReceipt::commit(&q, &patch, &receipt).unwrap()],
            2048,
            &job,
        )
        .unwrap();
        let r = encode_publication(
            &q,
            &raw,
            &selected_before,
            &right,
            &[PublicationReceipt::commit(&q, &patch, &receipt).unwrap()],
            2048,
            &job,
        )
        .unwrap();
        assert_eq!(l.as_bytes(), r.as_bytes());
        assert!(l.as_bytes().len() < 512);
        let mut malformed = right.clone();
        malformed.work_bodies.clear();
        assert!(validate_delta(&selected_before, &malformed).is_err());
        malformed = right.clone();
        malformed.work[0].lease_hash = Some([0; 32]);
        assert!(validate_delta(&selected_before, &malformed).is_err());
        let mut corrupt_before = selected_before;
        corrupt_before.versions.values_mut().for_each(|v| *v += 1);
        assert!(validate_delta(&corrupt_before, &right).is_err());
    }
    #[test]
    fn packed_absent_create_delete_preserves_tombstone_highwater() {
        let q = queue();
        let schema = schema();
        let raw = encode_definition(&schema).unwrap();
        let before = RecordSnapshot::default();
        let mut after = before.clone();
        let mut create = seed();
        create.state_updates.clear();
        let first = plan_commit_group(&create, &mut after, 0, &schema).unwrap();
        let mut delete = group("b-delete");
        delete.work_updates.push(UnclaimedUpdate {
            key: create.work_inserts[0].key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let second = plan_commit_group(&delete, &mut after, 0, &schema).unwrap();
        let packed = combine_deltas(&[first.clone(), second.clone()], 0).unwrap();
        assert!(packed.work.is_empty());
        assert_eq!(packed.versions[0].version, 2);
        let a = QueueAdmission::new(1, 4096).unwrap();
        let job = a.reserve(0).unwrap();
        let frame = encode_publication(
            &q,
            &raw,
            &before,
            &packed,
            &[
                PublicationReceipt::commit(&q, &create, &first.receipt).unwrap(),
                PublicationReceipt::commit(&q, &delete, &second.receipt).unwrap(),
            ],
            4096,
            &job,
        )
        .unwrap();
        assert_eq!(scan_publication(frame.as_bytes()).unwrap().commands, 1);
        let mut conflict = first.receipt.clone();
        conflict.outcome = GroupOutcome::RequestConflict;
        assert!(PublicationReceipt::commit(&q, &create, &conflict).is_err());
        conflict.outcome = GroupOutcome::RequestExpired;
        assert!(PublicationReceipt::commit(&q, &create, &conflict).is_err());
    }
    #[test]
    fn insert_receipt_positional_binding_and_outer_identity_order() {
        let q = queue();
        let schema = schema();
        let raw = encode_definition(&schema).unwrap();
        let request = InsertRequest {
            queue: q.clone(),
            request_id: "insert".into(),
            mode: InsertMode::InsertOnly,
            items: seed().work_inserts,
        };
        let outcomes = vec![InsertOutcome::Rejected {
            key: request.items[0].key.clone(),
            reason: Rejection::Invalid("reason\0α".into()),
        }];
        let receipt = PublicationReceipt::insert(&request, &outcomes).unwrap();
        let empty = combine_deltas(&[], 0).unwrap();
        let before = RecordSnapshot::default();
        let a = QueueAdmission::new(1, 4096).unwrap();
        let job = a.reserve(0).unwrap();
        let frame = encode_publication(&q, &raw, &before, &empty, &[receipt], 4096, &job).unwrap();
        assert_eq!(scan_publication(frame.as_bytes()).unwrap().receipts, 1);
        let duplicate = [
            PublicationReceipt::insert(&request, &outcomes).unwrap(),
            PublicationReceipt::insert(&request, &outcomes).unwrap(),
        ];
        assert!(encode_publication(&q, &raw, &before, &empty, &duplicate, 4096, &job).is_err());
        let replaced = vec![InsertOutcome::Replaced {
            key: request.items[0].key.clone(),
            version: 1,
        }];
        assert!(PublicationReceipt::insert(&request, &replaced).is_err());
        let bad = vec![InsertOutcome::Inserted {
            key: "wrong".into(),
            version: 1,
        }];
        assert!(PublicationReceipt::insert(&request, &bad).is_err());
        let wrong = QueueAddress {
            tenant: "other".into(),
            queue: q.queue.clone(),
        };
        assert!(
            encode_publication(
                &wrong,
                &raw,
                &before,
                &empty,
                &[PublicationReceipt::insert(&request, &outcomes).unwrap()],
                4096,
                &job
            )
            .is_err()
        );
    }
    #[test]
    fn malformed_preserve_edit_authority_and_noop_elision() {
        let q = queue();
        let schema = schema();
        let raw = encode_definition(&schema).unwrap();
        let create = seed();
        let mut before = RecordSnapshot::default();
        plan_commit_group(&create, &mut before, 0, &schema).unwrap();
        let key = create.work_inserts[0].key.clone();
        let mut patch = group("metadata");
        patch.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(88),
                ..WorkPatch::default()
            }),
        });
        let mut after = before.clone();
        let delta = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
        let delta = combine_deltas(&[delta], before.next_sequence).unwrap();
        let mut bad = delta.clone();
        bad.work[0].record.work.body = vec![3].into();
        assert!(validate_delta(&before, &bad).is_err());
        bad = delta.clone();
        bad.work[0]
            .record
            .work
            .fields
            .insert("gate".into(), FieldValue::Boolean(false));
        assert!(validate_delta(&before, &bad).is_err());
        patch.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            fields: BTreeMap::from([("gate".into(), Some(FieldValue::Boolean(false)))]),
            ..WorkPatch::default()
        });
        after = before.clone();
        let edit = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
        let mut bad = combine_deltas(&[edit], before.next_sequence).unwrap();
        bad.work[0]
            .record
            .work
            .fields
            .insert("number".into(), FieldValue::Integer(5));
        assert!(validate_delta(&before, &bad).is_err());
        let mut aged_before = before.clone();
        aged_before.next_sequence = 10;
        let mut aged_delta = delta.clone();
        aged_delta.next_sequence = 10;
        aged_delta.work[0].record.insertion_sequence = 2;
        assert!(validate_delta(&aged_before, &aged_delta).is_err());
        let absent_before = RecordSnapshot {
            next_sequence: 10,
            ..Default::default()
        };
        let mut new_snapshot = absent_before.clone();
        let new_delta = plan_commit_group(&create, &mut new_snapshot, 0, &schema).unwrap();
        let mut reused = combine_deltas(&[new_delta], 10).unwrap();
        reused.work[0].record.insertion_sequence = 10;
        assert!(validate_delta(&absent_before, &reused).is_err());
        let mut corrupt = before.clone();
        corrupt.work.get_mut(&key).unwrap().record.work.key = "wrong".into();
        assert!(validate_delta(&corrupt, &delta).is_err());
        corrupt = before.clone();
        corrupt
            .work
            .get_mut(&key)
            .unwrap()
            .record
            .insertion_sequence = 0;
        assert!(validate_delta(&corrupt, &delta).is_err());
        let mut noop = group("noop");
        noop.work_updates.push(UnclaimedUpdate {
            key,
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Administrative {
                patch: WorkPatch::default(),
                lifecycle: AdministrativeLifecycle::Keep,
                lease_guard: AdministrativeLeaseGuard::RejectActive,
            },
        });
        after = before.clone();
        let planned = plan_commit_group(&noop, &mut after, 0, &schema).unwrap();
        let packed = combine_deltas(std::slice::from_ref(&planned), before.next_sequence).unwrap();
        assert_eq!(packed.versions[0].version, 1);
        let a = QueueAdmission::new(1, 4096).unwrap();
        let job = a.reserve(0).unwrap();
        let frame = encode_publication(
            &q,
            &raw,
            &before,
            &packed,
            &[PublicationReceipt::commit(&q, &noop, &planned.receipt).unwrap()],
            4096,
            &job,
        )
        .unwrap();
        let summary = scan_publication(frame.as_bytes()).unwrap();
        assert_eq!(summary.commands, 0);
        assert_eq!(summary.receipts, 1);
    }
    #[test]
    fn leased_edit_removal_and_hand_mutated_wire_invariants() {
        let q = queue();
        let schema = schema();
        let raw = encode_definition(&schema).unwrap();
        let create = seed();
        let mut before = RecordSnapshot::default();
        plan_commit_group(&create, &mut before, 0, &schema).unwrap();
        let key = create.work_inserts[0].key.clone();
        let old = before.work.get_mut(&key).unwrap();
        old.record.version = 2;
        old.record.lifecycle = Lifecycle::Leased;
        old.record.lease_expires_at = Some(100);
        old.lease_hash = Some(crate::queue_planner::lease_hash(&[4]));
        *before
            .versions
            .get_mut(&RecordIdentity {
                collection: None,
                key: key.clone(),
            })
            .unwrap() = 2;
        let mut patch = group("leased-edit");
        patch.claimed_updates.push(ClaimedUpdate {
            key,
            lease: LeaseCredential { token: vec![4] },
            expected_version: 2,
            patch: WorkPatch {
                fields: BTreeMap::from([
                    ("gate".into(), Some(FieldValue::Boolean(false))),
                    ("text".into(), None),
                ]),
                body: Some(Default::default()),
                ..WorkPatch::default()
            },
            disposition: Disposition::Retain { renew_nanos: None },
        });
        let mut after = before.clone();
        let planned = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
        assert!(matches!(
            planned.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        let packed = combine_deltas(std::slice::from_ref(&planned), before.next_sequence).unwrap();
        let a = QueueAdmission::new(1, 8192).unwrap();
        let job = a.reserve(0).unwrap();
        let frame = encode_publication(
            &q,
            &raw,
            &before,
            &packed,
            &[PublicationReceipt::commit(&q, &patch, &planned.receipt).unwrap()],
            4096,
            &job,
        )
        .unwrap();
        let bytes = frame.as_bytes();
        assert_eq!(scan_publication(bytes).unwrap().commands, 1);
        // Walk the valid wire to mutate exactly the predecessor/output/scalar tags.
        let mut scan = Scan { rest: bytes };
        scan.raw(5).unwrap();
        scan.text().unwrap();
        scan.text().unwrap();
        scan.raw(32).unwrap();
        scan.n().unwrap();
        scan.n().unwrap();
        scan.count().unwrap();
        scan.identity().unwrap();
        let presence_offset = bytes.len() - scan.rest.len();
        assert!(scan.presence().unwrap());
        scan.n().unwrap();
        let prior = scan.n().unwrap();
        let output_offset = bytes.len() - scan.rest.len();
        scan.n().unwrap();
        assert_eq!(scan.tag().unwrap(), 1);
        scan.raw(24).unwrap();
        assert_eq!(scan.tag().unwrap(), 1);
        scan.raw(12).unwrap();
        assert!(scan.presence().unwrap());
        scan.raw(8).unwrap();
        assert!(scan.presence().unwrap());
        scan.raw(32).unwrap();
        assert_eq!(scan.tag().unwrap(), 2);
        assert_eq!(scan.count().unwrap(), 2);
        assert_eq!(scan.text().unwrap(), "gate");
        assert!(scan.presence().unwrap());
        let scalar_offset = bytes.len() - scan.rest.len();
        let mut bad = bytes.to_vec();
        bad[output_offset..output_offset + 8].copy_from_slice(&prior.to_le_bytes());
        assert!(scan_publication(&bad).is_err());
        bad = bytes.to_vec();
        bad[presence_offset] = 2;
        assert!(scan_publication(&bad).is_err());
        bad = bytes.to_vec();
        bad[scalar_offset] = 255;
        assert!(scan_publication(&bad).is_err());
        bad = bytes.to_vec();
        bad[scalar_offset + 1] = 2;
        assert!(scan_publication(&bad).is_err());
        bad = bytes.to_vec();
        bad[5..13].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(scan_publication(&bad).is_err());
        bad = bytes.to_vec();
        bad[13] = 255;
        assert!(scan_publication(&bad).is_err());
        let huge = u64::MAX.to_le_bytes();
        assert!(Scan { rest: &huge }.count().is_err());
        let mut receipt = planned.receipt.clone();
        if let GroupOutcome::Committed { versions } = &mut receipt.outcome {
            versions[0].version = 999;
        }
        assert!(
            encode_publication(
                &q,
                &raw,
                &before,
                &packed,
                &[PublicationReceipt::commit(&q, &patch, &receipt).unwrap()],
                4096,
                &job
            )
            .is_err()
        );
        let tiny = QueueAdmission::new(1, 400).unwrap();
        let tiny_job = tiny.reserve(0).unwrap();
        assert!(matches!(
            encode_publication(
                &q,
                &raw,
                &before,
                &packed,
                &[PublicationReceipt::commit(&q, &patch, &planned.receipt).unwrap()],
                4096,
                &tiny_job
            ),
            Err(PublicationError::Admission(_))
        ));
        assert_eq!(tiny.usage().unwrap(), (1, 0));
    }
    #[test]
    fn frozen_scalar_and_rejection_tags() {
        let mut encoded = vec![];
        value(&mut Sink::Buffer(&mut encoded), &FieldValue::Integer(-1)).unwrap();
        assert_eq!(encoded, [0, 255, 255, 255, 255, 255, 255, 255, 255]);
        encoded.clear();
        value(&mut Sink::Buffer(&mut encoded), &FieldValue::Float(-0.0)).unwrap();
        assert_eq!(encoded, [1, 0, 0, 0, 0, 0, 0, 0, 128]);
        encoded.clear();
        value(&mut Sink::Buffer(&mut encoded), &FieldValue::Boolean(false)).unwrap();
        assert_eq!(encoded, [2, 0]);
        assert!(value(&mut Sink::Count(0), &FieldValue::Float(f64::NAN)).is_err());
        let reasons = [
            Rejection::StaleLease,
            Rejection::VersionConflict,
            Rejection::AlreadyExists,
            Rejection::NotFound,
            Rejection::Leased,
            Rejection::Invalid("\0α".into()),
            Rejection::UniqueConflict,
            Rejection::Terminal,
        ];
        for (tag, reason) in reasons.iter().enumerate() {
            encoded.clear();
            rejection(&mut Sink::Buffer(&mut encoded), reason).unwrap();
            assert_eq!(encoded[0], tag as u8);
            let mut scan = Scan { rest: &encoded };
            scan.rejection().unwrap();
            assert!(scan.rest.is_empty());
        }
    }
}
