//! Borrowed FWQP1 consumption. This is also the sole structural parser used by
//! scan_publication; only a fully validated immutable frame exposes iterators.
use super::{MAGIC, PublicationResult, PublicationSummary, Scan, invalid, validate_lease};
use fireweed_core::queue_contract::{FieldType, Lifecycle};
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(&'a str),
    Bytes(&'a [u8]),
}
impl ValueRef<'_> {
    pub fn field_type(self) -> FieldType {
        match self {
            Self::Integer(_) => FieldType::Integer,
            Self::Float(_) => FieldType::Float,
            Self::Boolean(_) => FieldType::Boolean,
            Self::Text(_) => FieldType::Text,
            Self::Bytes(_) => FieldType::Bytes,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityRef<'a> {
    pub collection: Option<&'a str>,
    pub key: &'a str,
}
impl<'a> IdentityRef<'a> {
    fn order(self) -> (Option<&'a str>, &'a str) {
        (self.collection, self.key)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct FieldsRef<'a> {
    entries: &'a [u8],
    count: u64,
    edits: bool,
}
impl<'a> FieldsRef<'a> {
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn iter(self) -> FieldsIter<'a> {
        FieldsIter {
            scan: Scan { rest: self.entries },
            left: self.count,
            edits: self.edits,
        }
    }
}
pub struct FieldsIter<'a> {
    scan: Scan<'a>,
    left: u64,
    edits: bool,
}
impl<'a> Iterator for FieldsIter<'a> {
    type Item = PublicationResult<(&'a str, Option<ValueRef<'a>>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = (|| {
            let name = self.scan.text()?;
            let value = if !self.edits || self.scan.presence()? {
                Some(parse_value(&mut self.scan)?)
            } else {
                None
            };
            Ok((name, value))
        })();
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
#[derive(Debug, Clone, Copy)]
pub enum FieldsCommandRef<'a> {
    Preserve,
    Replace(FieldsRef<'a>),
    Edit(FieldsRef<'a>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyCommandRef<'a> {
    Preserve,
    Replace(&'a [u8]),
}
#[derive(Debug, Clone, Copy)]
pub struct WorkMetadataRef<'a> {
    pub priority: i64,
    pub available_at: i64,
    pub insertion_sequence: u64,
    pub lifecycle: Lifecycle,
    pub eligible_since: i64,
    pub attempts: u32,
    pub lease_expires_at: Option<i64>,
    pub lease_hash: Option<&'a [u8; 32]>,
}
#[derive(Debug, Clone, Copy)]
pub enum EffectRef<'a> {
    Delete,
    WorkPut {
        metadata: WorkMetadataRef<'a>,
        fields: FieldsCommandRef<'a>,
        body: BodyCommandRef<'a>,
    },
    StatePut {
        fields: FieldsRef<'a>,
        value: &'a [u8],
    },
}
#[derive(Debug, Clone, Copy)]
pub struct CommandRef<'a> {
    pub identity: IdentityRef<'a>,
    pub expected_version: Option<u64>,
    pub prior_highwater: u64,
    pub output_highwater: u64,
    pub effect: EffectRef<'a>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionRef<'a> {
    StaleLease,
    VersionConflict,
    AlreadyExists,
    NotFound,
    Leased,
    Invalid(&'a str),
    UniqueConflict,
    Terminal,
}
#[derive(Debug, Clone, Copy)]
pub struct VersionRef<'a> {
    pub identity: IdentityRef<'a>,
    pub version: u64,
}
#[derive(Debug, Clone, Copy)]
pub struct VersionsRef<'a> {
    entries: &'a [u8],
    count: u64,
}
impl<'a> VersionsRef<'a> {
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn iter(self) -> VersionsIter<'a> {
        VersionsIter {
            scan: Scan { rest: self.entries },
            left: self.count,
        }
    }
}
pub struct VersionsIter<'a> {
    scan: Scan<'a>,
    left: u64,
}
impl<'a> Iterator for VersionsIter<'a> {
    type Item = PublicationResult<VersionRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = parse_version(&mut self.scan);
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
#[derive(Debug, Clone, Copy)]
pub enum CommitOutcomeRef<'a> {
    Committed(VersionsRef<'a>),
    Rejected(RejectionRef<'a>),
}
#[derive(Debug, Clone, Copy)]
pub enum InsertOutcomeRef<'a> {
    Inserted {
        key: &'a str,
        version: u64,
    },
    Replaced {
        key: &'a str,
        version: u64,
    },
    Rejected {
        key: &'a str,
        reason: RejectionRef<'a>,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct InsertsRef<'a> {
    entries: &'a [u8],
    count: u64,
}
impl<'a> InsertsRef<'a> {
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn iter(self) -> InsertsIter<'a> {
        InsertsIter {
            scan: Scan { rest: self.entries },
            left: self.count,
        }
    }
}
pub struct InsertsIter<'a> {
    scan: Scan<'a>,
    left: u64,
}
impl<'a> Iterator for InsertsIter<'a> {
    type Item = PublicationResult<InsertOutcomeRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = parse_insert(&mut self.scan);
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ReceiptOutcomeRef<'a> {
    Insert(InsertsRef<'a>),
    Commit(CommitOutcomeRef<'a>),
}
#[derive(Debug, Clone, Copy)]
pub struct ReceiptRef<'a> {
    pub domain: u8,
    pub request_id: &'a str,
    pub fingerprint_format: u8,
    pub fingerprint: &'a [u8; 32],
    pub outcome: ReceiptOutcomeRef<'a>,
    encoded_outcome: &'a [u8],
}
impl<'a> ReceiptRef<'a> {
    /// Frozen FWQP1 result bytes, useful for immutable authority comparison.
    pub fn encoded_outcome(self) -> &'a [u8] {
        self.encoded_outcome
    }
}

/// Constructed only by full structural validation. Borrowed slices cannot be
/// mutated while this view or its restartable iterators exist. No payload clones,
/// list allocation, sorting scratch or from-start per-command rescans occur.
#[derive(Debug)]
pub struct PublicationView<'a> {
    summary: PublicationSummary<'a>,
    commands: &'a [u8],
    receipts: &'a [u8],
}
impl<'a> PublicationView<'a> {
    pub fn summary(&self) -> &PublicationSummary<'a> {
        &self.summary
    }
    pub fn commands(&self) -> CommandsIter<'a> {
        CommandsIter {
            scan: Scan {
                rest: self.commands,
            },
            left: self.summary.commands,
            initial: self.summary.initial_sequence,
            final_sequence: self.summary.final_sequence,
        }
    }
    pub fn receipts(&self) -> ReceiptsIter<'a> {
        ReceiptsIter {
            scan: Scan {
                rest: self.receipts,
            },
            left: self.summary.receipts,
        }
    }
}
pub struct CommandsIter<'a> {
    scan: Scan<'a>,
    left: u64,
    initial: u64,
    final_sequence: u64,
}
impl<'a> Iterator for CommandsIter<'a> {
    type Item = PublicationResult<CommandRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = parse_command(&mut self.scan, self.initial, self.final_sequence);
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
pub struct ReceiptsIter<'a> {
    scan: Scan<'a>,
    left: u64,
}
impl<'a> Iterator for ReceiptsIter<'a> {
    type Item = PublicationResult<ReceiptRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = parse_receipt(&mut self.scan);
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}

pub(super) fn parse_value<'a>(s: &mut Scan<'a>) -> PublicationResult<ValueRef<'a>> {
    Ok(match s.tag()? {
        0 => ValueRef::Integer(i64::from_le_bytes(s.raw(8)?.try_into().unwrap())),
        1 => {
            let value = f64::from_bits(s.n()?);
            if !value.is_finite() {
                return invalid("nonfinite field");
            }
            ValueRef::Float(value)
        }
        2 => ValueRef::Boolean(s.presence()?),
        3 => ValueRef::Text(s.text()?),
        4 => ValueRef::Bytes(s.bytes()?),
        _ => return invalid("unknown field tag"),
    })
}
fn identity<'a>(s: &mut Scan<'a>) -> PublicationResult<IdentityRef<'a>> {
    let (collection, key) = s.identity()?;
    Ok(IdentityRef { collection, key })
}
pub(super) fn fields<'a>(s: &mut Scan<'a>, edits: bool) -> PublicationResult<FieldsRef<'a>> {
    let count = s.count()?;
    let start = s.rest;
    let mut last = None;
    for _ in 0..count {
        let name = s.text()?;
        if last.is_some_and(|last| last >= name) {
            return invalid("field identity order");
        }
        last = Some(name);
        if !edits || s.presence()? {
            parse_value(s)?;
        }
    }
    let len = start.len() - s.rest.len();
    Ok(FieldsRef {
        entries: &start[..len],
        count,
        edits,
    })
}
pub(super) fn parse_rejection<'a>(s: &mut Scan<'a>) -> PublicationResult<RejectionRef<'a>> {
    Ok(match s.tag()? {
        0 => RejectionRef::StaleLease,
        1 => RejectionRef::VersionConflict,
        2 => RejectionRef::AlreadyExists,
        3 => RejectionRef::NotFound,
        4 => RejectionRef::Leased,
        5 => RejectionRef::Invalid(s.text()?),
        6 => RejectionRef::UniqueConflict,
        7 => RejectionRef::Terminal,
        _ => return invalid("unknown rejection tag"),
    })
}
fn parse_version<'a>(s: &mut Scan<'a>) -> PublicationResult<VersionRef<'a>> {
    let identity = identity(s)?;
    let version = s.n()?;
    if version == 0 {
        return invalid("zero receipt version");
    }
    Ok(VersionRef { identity, version })
}
fn parse_insert<'a>(s: &mut Scan<'a>) -> PublicationResult<InsertOutcomeRef<'a>> {
    let tag = s.tag()?;
    let key = s.text()?;
    Ok(match tag {
        0 | 1 => {
            let version = s.n()?;
            if version == 0 {
                return invalid("zero insert result version");
            }
            if tag == 0 {
                InsertOutcomeRef::Inserted { key, version }
            } else {
                InsertOutcomeRef::Replaced { key, version }
            }
        }
        2 => InsertOutcomeRef::Rejected {
            key,
            reason: parse_rejection(s)?,
        },
        _ => return invalid("unknown insert result"),
    })
}
fn parse_command<'a>(
    s: &mut Scan<'a>,
    initial: u64,
    final_sequence: u64,
) -> PublicationResult<CommandRef<'a>> {
    let identity = identity(s)?;
    let expected_version = if s.presence()? { Some(s.n()?) } else { None };
    let prior_highwater = s.n()?;
    let output_highwater = s.n()?;
    if expected_version.is_some_and(|v| v == 0 || v != prior_highwater)
        || output_highwater == 0
        || output_highwater <= prior_highwater
    {
        return invalid("invalid predecessor highwater");
    }
    let effect = match s.tag()? {
        0 => EffectRef::Delete,
        1 if identity.collection.is_some() => EffectRef::StatePut {
            fields: fields(s, false)?,
            value: s.bytes()?,
        },
        1 => {
            let priority = i64::from_le_bytes(s.raw(8)?.try_into().unwrap());
            let available_at = i64::from_le_bytes(s.raw(8)?.try_into().unwrap());
            let insertion_sequence = s.n()?;
            if insertion_sequence == 0 || insertion_sequence > final_sequence {
                return invalid("invalid work sequence");
            }
            if expected_version.is_none() && insertion_sequence <= initial {
                return invalid("absent work sequence reuses insertion order");
            }
            let lifecycle = match s.tag()? {
                0 => Lifecycle::Pending,
                1 => Lifecycle::Leased,
                2 => Lifecycle::Completed,
                3 => Lifecycle::Failed,
                _ => return invalid("unknown lifecycle"),
            };
            let eligible_since = i64::from_le_bytes(s.raw(8)?.try_into().unwrap());
            let attempts = u32::from_le_bytes(s.raw(4)?.try_into().unwrap());
            let lease_expires_at = if s.presence()? {
                Some(i64::from_le_bytes(s.raw(8)?.try_into().unwrap()))
            } else {
                None
            };
            let lease_hash = if s.presence()? {
                Some(s.raw(32)?.try_into().unwrap())
            } else {
                None
            };
            validate_lease(lifecycle, lease_expires_at.is_some(), lease_hash.is_some())?;
            let fields = match s.tag()? {
                0 => FieldsCommandRef::Preserve,
                1 => FieldsCommandRef::Replace(fields(s, false)?),
                2 => FieldsCommandRef::Edit(fields(s, true)?),
                _ => return invalid("unknown field command"),
            };
            let body = match s.tag()? {
                0 => BodyCommandRef::Preserve,
                1 => BodyCommandRef::Replace(s.bytes()?),
                _ => return invalid("unknown body command"),
            };
            if expected_version.is_none()
                && (!matches!(fields, FieldsCommandRef::Replace(_))
                    || !matches!(body, BodyCommandRef::Replace(_)))
            {
                return invalid("partial absent work creation");
            }
            EffectRef::WorkPut {
                metadata: WorkMetadataRef {
                    priority,
                    available_at,
                    insertion_sequence,
                    lifecycle,
                    eligible_since,
                    attempts,
                    lease_expires_at,
                    lease_hash,
                },
                fields,
                body,
            }
        }
        _ => return invalid("unknown effect tag"),
    };
    Ok(CommandRef {
        identity,
        expected_version,
        prior_highwater,
        output_highwater,
        effect,
    })
}
fn parse_receipt<'a>(s: &mut Scan<'a>) -> PublicationResult<ReceiptRef<'a>> {
    let domain = s.tag()?;
    if !matches!(domain, 0 | 2) {
        return invalid("unknown receipt domain");
    }
    let request_id = s.text()?;
    let fingerprint_format = s.tag()?;
    if fingerprint_format != 3 {
        return invalid("unsupported fingerprint format");
    }
    let fingerprint = s.raw(32)?.try_into().unwrap();
    let start = s.rest;
    let outcome = if domain == 2 {
        ReceiptOutcomeRef::Commit(match s.tag()? {
            0 => {
                let count = s.count()?;
                let entries = s.rest;
                for _ in 0..count {
                    parse_version(s)?;
                }
                CommitOutcomeRef::Committed(VersionsRef {
                    entries: &entries[..entries.len() - s.rest.len()],
                    count,
                })
            }
            1 => CommitOutcomeRef::Rejected(parse_rejection(s)?),
            _ => return invalid("non-fresh commit outcome"),
        })
    } else {
        let count = s.count()?;
        let entries = s.rest;
        for _ in 0..count {
            parse_insert(s)?;
        }
        ReceiptOutcomeRef::Insert(InsertsRef {
            entries: &entries[..entries.len() - s.rest.len()],
            count,
        })
    };
    Ok(ReceiptRef {
        domain,
        request_id,
        fingerprint_format,
        fingerprint,
        outcome,
        encoded_outcome: &start[..start.len() - s.rest.len()],
    })
}
/// Validate the entire frozen frame before exposing any replay commands.
pub fn view_publication(raw: &[u8]) -> PublicationResult<PublicationView<'_>> {
    let mut s = Scan { rest: raw };
    if s.raw(MAGIC.len())? != MAGIC {
        return invalid("unsupported publication version");
    }
    let queue = (s.text()?, s.text()?);
    let definition_pin = s.raw(32)?;
    let initial_sequence = s.n()?;
    let final_sequence = s.n()?;
    if final_sequence < initial_sequence {
        return invalid("regressing insertion sequence");
    }
    let commands = s.count()?;
    let command_start = s.rest;
    let mut last = None;
    for _ in 0..commands {
        let command = parse_command(&mut s, initial_sequence, final_sequence)?;
        if last.is_some_and(|last| last >= command.identity.order()) {
            return invalid("command identity order");
        }
        last = Some(command.identity.order());
    }
    let command_bytes = &command_start[..command_start.len() - s.rest.len()];
    let receipts = s.count()?;
    let receipt_start = s.rest;
    let mut last = None;
    for _ in 0..receipts {
        let receipt = parse_receipt(&mut s)?;
        if last.is_some_and(|last| last >= (receipt.domain, receipt.request_id)) {
            return invalid("receipt identity order");
        }
        last = Some((receipt.domain, receipt.request_id));
    }
    let receipt_bytes = &receipt_start[..receipt_start.len() - s.rest.len()];
    if !s.rest.is_empty() {
        return invalid("trailing publication bytes");
    }
    Ok(PublicationView {
        summary: PublicationSummary {
            queue,
            definition_pin,
            initial_sequence,
            final_sequence,
            commands,
            receipts,
        },
        commands: command_bytes,
        receipts: receipt_bytes,
    })
}
pub(super) fn summary(raw: &[u8]) -> PublicationResult<PublicationSummary<'_>> {
    view_publication(raw).map(|v| v.summary)
}

/// Disposable initial-image administrative command, not a standalone publication.
/// It must contain one advancing live work command and preserve insertion order.
pub fn parse_administrative_work_fragment(
    bytes: &[u8],
    initial_sequence: u64,
) -> PublicationResult<CommandRef<'_>> {
    let mut scan = Scan { rest: bytes };
    let command = parse_command(&mut scan, initial_sequence, initial_sequence)?;
    if !scan.rest.is_empty()
        || command.identity.collection.is_some()
        || command.expected_version.is_none()
    {
        return invalid("not one initial administrative work fragment");
    }
    Ok(command)
}
