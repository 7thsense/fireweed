//! FWAR1 canonical administrative result component. These bytes are neither a
//! durable receipt nor publication authority. Postcard receipts have a separate
//! complete-layout measurement; never substitute this binary size for that one.
use super::*;
use fireweed_core::queue_administration::*;
const RESULT_MAGIC: &[u8; 5] = b"FWAR1";
const KINDS: usize = 11;
fn kind_tag(kind: AdministrativeOutcomeKind) -> u8 {
    match kind {
        AdministrativeOutcomeKind::Changed => 0,
        AdministrativeOutcomeKind::Deleted => 1,
        AdministrativeOutcomeKind::NoChange => 2,
        AdministrativeOutcomeKind::NotFound => 3,
        AdministrativeOutcomeKind::Conflict => 4,
        AdministrativeOutcomeKind::Leased => 5,
        AdministrativeOutcomeKind::LeaseRequired => 6,
        AdministrativeOutcomeKind::PreconditionFailed => 7,
        AdministrativeOutcomeKind::Terminal => 8,
        AdministrativeOutcomeKind::Invalid => 9,
        AdministrativeOutcomeKind::UniqueConflict => 10,
    }
}
fn kind(tag: u8) -> PublicationResult<AdministrativeOutcomeKind> {
    Ok(match tag {
        0 => AdministrativeOutcomeKind::Changed,
        1 => AdministrativeOutcomeKind::Deleted,
        2 => AdministrativeOutcomeKind::NoChange,
        3 => AdministrativeOutcomeKind::NotFound,
        4 => AdministrativeOutcomeKind::Conflict,
        5 => AdministrativeOutcomeKind::Leased,
        6 => AdministrativeOutcomeKind::LeaseRequired,
        7 => AdministrativeOutcomeKind::PreconditionFailed,
        8 => AdministrativeOutcomeKind::Terminal,
        9 => AdministrativeOutcomeKind::Invalid,
        10 => AdministrativeOutcomeKind::UniqueConflict,
        _ => return invalid("unknown administrative outcome kind"),
    })
}
fn add_kind(
    counts: &mut [u64; KINDS],
    kind: AdministrativeOutcomeKind,
    n: u64,
) -> PublicationResult<()> {
    let slot = &mut counts[kind_tag(kind) as usize];
    *slot = slot.checked_add(n).ok_or(PublicationError::Overflow)?;
    Ok(())
}
fn histogram_counts(hist: &AdministrativeHistogram) -> PublicationResult<[u64; KINDS]> {
    let mut counts = [0; KINDS];
    let mut total = 0u64;
    for (kind, count) in &hist.outcomes {
        if *count == 0 {
            return invalid("zero administrative histogram count");
        }
        add_kind(&mut counts, *kind, *count)?;
        total = total
            .checked_add(*count)
            .ok_or(PublicationError::Overflow)?;
    }
    if total != hist.matched {
        return invalid("administrative histogram total mismatch");
    }
    Ok(counts)
}
fn check_before(
    outcome: &AdministrativeOutcome,
    version: u64,
    life: Lifecycle,
    expiry: Option<i64>,
) -> PublicationResult<()> {
    if version == 0 || (life == Lifecycle::Leased) != expiry.is_some() {
        return invalid("malformed administrative before metadata");
    }
    match outcome {
        AdministrativeOutcome::Changed { version: next }
        | AdministrativeOutcome::Deleted { version: next }
            if version.checked_add(1) != Some(*next) =>
        {
            return invalid("administrative before/output version mismatch");
        }
        AdministrativeOutcome::Conflict { actual_version } if *actual_version != version => {
            return invalid("administrative conflict/before version mismatch");
        }
        AdministrativeOutcome::NotFound => {
            return invalid("missing administrative result has before metadata");
        }
        _ => {}
    }
    Ok(())
}
fn validate_result(result: &AdministrativeResult) -> PublicationResult<()> {
    if !result.items.is_empty() && !result.clauses.is_empty() {
        return invalid("mixed addressed and broad administrative result");
    }
    let expected = histogram_counts(&result.histogram)?;
    let mut actual = [0; KINDS];
    for item in &result.items {
        if item.key.trim().is_empty() {
            return invalid("empty administrative result key");
        }
        add_kind(&mut actual, item.outcome.kind(), 1)?;
        match &item.outcome {
            AdministrativeOutcome::Changed { version }
            | AdministrativeOutcome::Deleted { version }
            | AdministrativeOutcome::Conflict {
                actual_version: version,
            } if *version == 0 => return invalid("zero administrative outcome version"),
            _ => {}
        }
        if let Some(before) = &item.before {
            check_before(
                &item.outcome,
                before.version,
                before.lifecycle,
                before.lease_expires_at,
            )?;
            if before.fields.keys().any(|name| name.trim().is_empty()) {
                return invalid("empty administrative before field");
            }
        }
    }
    for clause in &result.clauses {
        if clause.id.trim().is_empty() {
            return invalid("empty administrative result clause ID");
        }
        let counts = histogram_counts(&clause.histogram)?;
        for i in 0..KINDS {
            actual[i] = actual[i]
                .checked_add(counts[i])
                .ok_or(PublicationError::Overflow)?;
        }
    }
    if actual != expected {
        return invalid("administrative child/root histogram mismatch");
    }
    Ok(())
}
fn write_histogram(s: &mut Sink<'_>, hist: &AdministrativeHistogram) -> PublicationResult<()> {
    let counts = histogram_counts(hist)?;
    s.n(hist.matched)?;
    s.n(hist.outcomes.len() as u64)?;
    for (tag, count) in counts.iter().enumerate() {
        if *count != 0 {
            s.tag(tag as u8)?;
            s.n(*count)?;
        }
    }
    Ok(())
}

fn write_outcome(s: &mut Sink<'_>, outcome: &AdministrativeOutcome) -> PublicationResult<()> {
    s.tag(kind_tag(outcome.kind()))?;
    match outcome {
        AdministrativeOutcome::Changed { version } | AdministrativeOutcome::Deleted { version } => {
            s.n(*version)
        }
        AdministrativeOutcome::Conflict { actual_version } => s.n(*actual_version),
        _ => Ok(()),
    }
}
fn write_result(s: &mut Sink<'_>, result: &AdministrativeResult) -> PublicationResult<()> {
    validate_result(result)?;
    s.raw(RESULT_MAGIC)?;
    write_histogram(s, &result.histogram)?;
    s.n(result.items.len() as u64)?;
    for item in &result.items {
        s.text(&item.key)?;
        write_outcome(s, &item.outcome)?;
        s.tag(u8::from(item.before.is_some()))?;
        if let Some(before) = &item.before {
            s.n(before.version)?;
            s.tag(match before.lifecycle {
                Lifecycle::Pending => 0,
                Lifecycle::Leased => 1,
                Lifecycle::Completed => 2,
                Lifecycle::Failed => 3,
            })?;
            s.i(before.available_at)?;
            s.i(before.eligible_since)?;
            s.n(u64::from(before.attempts))?;
            s.tag(u8::from(before.lease_expires_at.is_some()))?;
            if let Some(expiry) = before.lease_expires_at {
                s.i(expiry)?;
            }
            fields(s, &before.fields)?;
        }
    }
    s.n(result.clauses.len() as u64)?;
    for clause in &result.clauses {
        s.text(&clause.id)?;
        write_histogram(s, &clause.histogram)?;
    }
    Ok(())
}
pub fn measure_administrative_result(result: &AdministrativeResult) -> PublicationResult<u64> {
    let mut sink = Sink::Count(0);
    write_result(&mut sink, result)?;
    let Sink::Count(bytes) = sink else {
        unreachable!()
    };
    Ok(bytes)
}
pub struct EncodedAdministrativeResult<'r> {
    bytes: Vec<u8>,
    _charge: QueueBufferCharge<'r>,
}
impl EncodedAdministrativeResult<'_> {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
pub fn encode_administrative_result<'r>(
    result: &AdministrativeResult,
    reservation: &'r QueueReservation,
) -> PublicationResult<EncodedAdministrativeResult<'r>> {
    let size = measure_administrative_result(result)?;
    let charge = reservation
        .buffer(size.checked_add(128).ok_or(PublicationError::Overflow)?)
        .map_err(PublicationError::Admission)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(size).map_err(|_| PublicationError::Overflow)?)
        .map_err(|_| PublicationError::Overflow)?;
    write_result(&mut Sink::Buffer(&mut bytes), result)?;
    if bytes.len() as u64 != size {
        return invalid("administrative result size changed");
    }
    view_administrative_result(&bytes)?;
    Ok(EncodedAdministrativeResult {
        bytes,
        _charge: charge,
    })
}
#[derive(Debug, Clone, Copy)]
pub struct AdministrativeHistogramRef<'a> {
    pub matched: u64,
    entries: &'a [u8],
    count: u64,
}
impl<'a> AdministrativeHistogramRef<'a> {
    pub fn iter(self) -> AdministrativeHistogramIter<'a> {
        AdministrativeHistogramIter {
            scan: Scan { rest: self.entries },
            left: self.count,
        }
    }
}
pub struct AdministrativeHistogramIter<'a> {
    scan: Scan<'a>,
    left: u64,
}
impl Iterator for AdministrativeHistogramIter<'_> {
    type Item = PublicationResult<(AdministrativeOutcomeKind, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = (|| Ok((kind(self.scan.tag()?)?, self.scan.n()?)))();
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
fn parse_histogram<'a>(
    s: &mut Scan<'a>,
) -> PublicationResult<(AdministrativeHistogramRef<'a>, [u64; KINDS])> {
    let matched = s.n()?;
    let count = s.count()?;
    if count > KINDS as u64 {
        return invalid("too many administrative histogram kinds");
    }
    let start = s.rest;
    let mut previous = None;
    let mut counts = [0; KINDS];
    let mut total = 0u64;
    for _ in 0..count {
        let tag = s.tag()?;
        let k = kind(tag)?;
        if previous.is_some_and(|previous| previous >= tag) {
            return invalid("administrative histogram kind order");
        }
        previous = Some(tag);
        let n = s.n()?;
        if n == 0 {
            return invalid("zero administrative histogram count");
        }
        add_kind(&mut counts, k, n)?;
        total = total.checked_add(n).ok_or(PublicationError::Overflow)?;
    }
    if total != matched {
        return invalid("administrative histogram total mismatch");
    }
    Ok((
        AdministrativeHistogramRef {
            matched,
            entries: &start[..start.len() - s.rest.len()],
            count,
        },
        counts,
    ))
}
fn parse_outcome(s: &mut Scan<'_>) -> PublicationResult<AdministrativeOutcome> {
    let outcome = match kind(s.tag()?)? {
        AdministrativeOutcomeKind::Changed => AdministrativeOutcome::Changed { version: s.n()? },
        AdministrativeOutcomeKind::Deleted => AdministrativeOutcome::Deleted { version: s.n()? },
        AdministrativeOutcomeKind::NoChange => AdministrativeOutcome::NoChange,
        AdministrativeOutcomeKind::NotFound => AdministrativeOutcome::NotFound,
        AdministrativeOutcomeKind::Conflict => AdministrativeOutcome::Conflict {
            actual_version: s.n()?,
        },
        AdministrativeOutcomeKind::Leased => AdministrativeOutcome::Leased,
        AdministrativeOutcomeKind::LeaseRequired => AdministrativeOutcome::LeaseRequired,
        AdministrativeOutcomeKind::PreconditionFailed => AdministrativeOutcome::PreconditionFailed,
        AdministrativeOutcomeKind::Terminal => AdministrativeOutcome::Terminal,
        AdministrativeOutcomeKind::Invalid => AdministrativeOutcome::Invalid,
        AdministrativeOutcomeKind::UniqueConflict => AdministrativeOutcome::UniqueConflict,
    };
    match outcome {
        AdministrativeOutcome::Changed { version }
        | AdministrativeOutcome::Deleted { version }
        | AdministrativeOutcome::Conflict {
            actual_version: version,
        } if version == 0 => return invalid("zero administrative outcome version"),
        _ => {}
    }
    Ok(outcome)
}
#[derive(Debug, Clone, Copy)]
pub struct AdministrativeBeforeRef<'a> {
    pub version: u64,
    pub lifecycle: Lifecycle,
    pub available_at: i64,
    pub eligible_since: i64,
    pub attempts: u32,
    pub lease_expires_at: Option<i64>,
    pub fields: FieldsRef<'a>,
}
#[derive(Debug, Clone)]
pub struct AdministrativeItemRef<'a> {
    pub key: &'a str,
    pub outcome: AdministrativeOutcome,
    pub before: Option<AdministrativeBeforeRef<'a>>,
}
fn parse_item<'a>(s: &mut Scan<'a>) -> PublicationResult<AdministrativeItemRef<'a>> {
    let key = s.text()?;
    if key.trim().is_empty() {
        return invalid("empty administrative result key");
    }
    let outcome = parse_outcome(s)?;
    let before = if s.presence()? {
        let version = s.n()?;
        let lifecycle = match s.tag()? {
            0 => Lifecycle::Pending,
            1 => Lifecycle::Leased,
            2 => Lifecycle::Completed,
            3 => Lifecycle::Failed,
            _ => return invalid("unknown administrative lifecycle"),
        };
        let available_at = s.n()? as i64;
        let eligible_since = s.n()? as i64;
        let attempts = u32::try_from(s.n()?)
            .map_err(|_| PublicationError::Invalid("administrative attempts overflow"))?;
        let lease_expires_at = if s.presence()? {
            Some(s.n()? as i64)
        } else {
            None
        };
        check_before(&outcome, version, lifecycle, lease_expires_at)?;
        let fields = view::fields(s, false)?;
        for entry in fields.iter() {
            if entry?.0.trim().is_empty() {
                return invalid("empty administrative before field");
            }
        }
        Some(AdministrativeBeforeRef {
            version,
            lifecycle,
            available_at,
            eligible_since,
            attempts,
            lease_expires_at,
            fields,
        })
    } else {
        None
    };
    Ok(AdministrativeItemRef {
        key,
        outcome,
        before,
    })
}
#[derive(Debug, Clone, Copy)]
pub struct AdministrativeItemsRef<'a> {
    entries: &'a [u8],
    count: u64,
}
impl<'a> AdministrativeItemsRef<'a> {
    pub fn len(self) -> u64 {
        self.count
    }
    pub fn is_empty(self) -> bool {
        self.count == 0
    }
    pub fn iter(self) -> AdministrativeItemsIter<'a> {
        AdministrativeItemsIter {
            scan: Scan { rest: self.entries },
            left: self.count,
        }
    }
}
pub struct AdministrativeItemsIter<'a> {
    scan: Scan<'a>,
    left: u64,
}
impl<'a> Iterator for AdministrativeItemsIter<'a> {
    type Item = PublicationResult<AdministrativeItemRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = parse_item(&mut self.scan);
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct AdministrativeClauseRef<'a> {
    pub id: &'a str,
    pub histogram: AdministrativeHistogramRef<'a>,
}
fn parse_clause<'a>(
    s: &mut Scan<'a>,
) -> PublicationResult<(AdministrativeClauseRef<'a>, [u64; KINDS])> {
    let id = s.text()?;
    if id.trim().is_empty() {
        return invalid("empty administrative result clause ID");
    }
    let (histogram, counts) = parse_histogram(s)?;
    Ok((AdministrativeClauseRef { id, histogram }, counts))
}
#[derive(Debug, Clone, Copy)]
pub struct AdministrativeClausesRef<'a> {
    entries: &'a [u8],
    count: u64,
}
impl<'a> AdministrativeClausesRef<'a> {
    pub fn len(self) -> u64 {
        self.count
    }
    pub fn is_empty(self) -> bool {
        self.count == 0
    }
    pub fn iter(self) -> AdministrativeClausesIter<'a> {
        AdministrativeClausesIter {
            scan: Scan { rest: self.entries },
            left: self.count,
        }
    }
}
pub struct AdministrativeClausesIter<'a> {
    scan: Scan<'a>,
    left: u64,
}
impl<'a> Iterator for AdministrativeClausesIter<'a> {
    type Item = PublicationResult<AdministrativeClauseRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = parse_clause(&mut self.scan).map(|(clause, _)| clause);
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct AdministrativeResultRef<'a> {
    pub histogram: AdministrativeHistogramRef<'a>,
    pub items: AdministrativeItemsRef<'a>,
    pub clauses: AdministrativeClausesRef<'a>,
}
/// Exhaust the complete component before exposing restartable borrowed views.
/// Validation retains a fixed number of 11-entry counter arrays and one transient item/field.
pub fn view_administrative_result(raw: &[u8]) -> PublicationResult<AdministrativeResultRef<'_>> {
    let mut s = Scan { rest: raw };
    if s.raw(RESULT_MAGIC.len())? != RESULT_MAGIC {
        return invalid("unsupported administrative result format");
    }
    let (histogram, expected) = parse_histogram(&mut s)?;
    let mut actual = [0u64; KINDS];
    let items = s.count()?;
    let start = s.rest;
    for _ in 0..items {
        let item = parse_item(&mut s)?;
        add_kind(&mut actual, item.outcome.kind(), 1)?;
    }
    let item_bytes = &start[..start.len() - s.rest.len()];
    let clauses = s.count()?;
    if items != 0 && clauses != 0 {
        return invalid("mixed addressed and broad administrative result");
    }
    let start = s.rest;
    for _ in 0..clauses {
        let (_, counts) = parse_clause(&mut s)?;
        for i in 0..KINDS {
            actual[i] = actual[i]
                .checked_add(counts[i])
                .ok_or(PublicationError::Overflow)?;
        }
    }
    let clause_bytes = &start[..start.len() - s.rest.len()];
    if actual != expected {
        return invalid("administrative child/root histogram mismatch");
    }
    if !s.rest.is_empty() {
        return invalid("trailing administrative result bytes");
    }
    Ok(AdministrativeResultRef {
        histogram,
        items: AdministrativeItemsRef {
            entries: item_bytes,
            count: items,
        },
        clauses: AdministrativeClausesRef {
            entries: clause_bytes,
            count: clauses,
        },
    })
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    use sha2::Digest;
    use std::collections::BTreeMap;
    pub(in crate::queue_publication) fn rich_result() -> AdministrativeResult {
        let before = AdministrativeBefore {
            version: 1,
            lifecycle: Lifecycle::Leased,
            available_at: i64::MIN,
            eligible_since: i64::MAX,
            attempts: u32::MAX,
            lease_expires_at: Some(-1),
            fields: BTreeMap::from([
                ("blob".into(), FieldValue::Bytes(vec![0, 255])),
                ("bool".into(), FieldValue::Boolean(true)),
                ("float".into(), FieldValue::Float(-0.0)),
                ("number".into(), FieldValue::Integer(i64::MIN)),
                ("text".into(), FieldValue::Text("\0α".into())),
            ]),
        };
        let items = vec![
            AdministrativeItemResult {
                key: "same\0α".into(),
                outcome: AdministrativeOutcome::Changed { version: 2 },
                before: Some(before.clone()),
            },
            AdministrativeItemResult {
                key: "same\0α".into(),
                outcome: AdministrativeOutcome::Invalid,
                before: Some(before.clone()),
            },
            AdministrativeItemResult {
                key: "missing".into(),
                outcome: AdministrativeOutcome::NotFound,
                before: None,
            },
            AdministrativeItemResult {
                key: "same\0α".into(),
                outcome: AdministrativeOutcome::Conflict { actual_version: 1 },
                before: Some(before),
            },
        ];
        let mut histogram = AdministrativeHistogram::default();
        for item in &items {
            histogram.record(&item.outcome).unwrap();
        }
        AdministrativeResult {
            histogram,
            items,
            clauses: vec![],
        }
    }
    #[test]
    fn empty_and_rich_binary_goldens_preserve_order_types_and_restartable_views() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let empty = AdministrativeResult {
            histogram: Default::default(),
            items: vec![],
            clauses: vec![],
        };
        let encoded = encode_administrative_result(&empty, &job).unwrap();
        assert_eq!(encoded.as_bytes(), [b"FWAR1".as_slice(), &[0; 32]].concat());
        assert_eq!(encoded.as_bytes().len(), 37);
        let result = rich_result();
        let encoded = encode_administrative_result(&result, &job).unwrap();
        assert_eq!(
            measure_administrative_result(&result).unwrap(),
            encoded.as_bytes().len() as u64
        );
        assert_eq!(encoded.as_bytes().len(), 625);
        assert_eq!(
            sha2::Sha256::digest(encoded.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "829e5da1a26c446c237cf15abbe91fb823fa458155fe67eb4efd27e20e50e480"
        );
        let view = view_administrative_result(encoded.as_bytes()).unwrap();
        assert_eq!(view.items.len(), 4);
        assert!(view.clauses.is_empty());
        for _ in 0..2 {
            let items = view
                .items
                .iter()
                .collect::<PublicationResult<Vec<_>>>()
                .unwrap();
            assert_eq!(
                items.iter().map(|item| item.key).collect::<Vec<_>>(),
                vec!["same\0α", "same\0α", "missing", "same\0α"]
            );
            for (actual, expected) in items.iter().zip(&result.items) {
                assert_eq!(actual.outcome, expected.outcome);
                if let Some(before) = actual.before {
                    assert_eq!(before.version, 1);
                    assert_eq!(before.attempts, u32::MAX);
                    assert_eq!(before.lease_expires_at, Some(-1));
                    assert_eq!(before.available_at, i64::MIN);
                    assert_eq!(before.eligible_since, i64::MAX);
                    let fields = before
                        .fields
                        .iter()
                        .collect::<PublicationResult<Vec<_>>>()
                        .unwrap();
                    assert_eq!(fields[0], ("blob", Some(ValueRef::Bytes(&[0, 255]))));
                    assert_eq!(fields[1], ("bool", Some(ValueRef::Boolean(true))));
                    let Some(ValueRef::Float(value)) = fields[2].1 else {
                        panic!("float")
                    };
                    assert_eq!(value.to_bits(), (-0.0f64).to_bits());
                    assert_eq!(fields[3], ("number", Some(ValueRef::Integer(i64::MIN))));
                    assert_eq!(fields[4], ("text", Some(ValueRef::Text("\0α"))));
                }
            }
        }
    }
    #[test]
    fn broad_empty_clauses_and_duplicate_ids_keep_positional_order() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let mut one = AdministrativeHistogram::default();
        one.record(&AdministrativeOutcome::NoChange).unwrap();
        let result = AdministrativeResult {
            histogram: one.clone(),
            items: vec![],
            clauses: vec![
                AdministrativeClauseResult {
                    id: "duplicate".into(),
                    histogram: Default::default(),
                },
                AdministrativeClauseResult {
                    id: "duplicate".into(),
                    histogram: one,
                },
                AdministrativeClauseResult {
                    id: "zero".into(),
                    histogram: Default::default(),
                },
            ],
        };
        let encoded = encode_administrative_result(&result, &job).unwrap();
        let view = view_administrative_result(encoded.as_bytes()).unwrap();
        let clauses = view
            .clauses
            .iter()
            .collect::<PublicationResult<Vec<_>>>()
            .unwrap();
        assert_eq!(
            clauses
                .iter()
                .map(|clause| (clause.id, clause.histogram.matched))
                .collect::<Vec<_>>(),
            vec![("duplicate", 0), ("duplicate", 1), ("zero", 0)]
        );
        assert!(view.items.is_empty());
        assert_eq!(view.histogram.matched, 1);
    }
    #[test]
    fn every_outcome_has_explicit_fixed_wire_tag() {
        let outcomes = [
            AdministrativeOutcome::Changed { version: 2 },
            AdministrativeOutcome::Deleted { version: 2 },
            AdministrativeOutcome::NoChange,
            AdministrativeOutcome::NotFound,
            AdministrativeOutcome::Conflict { actual_version: 1 },
            AdministrativeOutcome::Leased,
            AdministrativeOutcome::LeaseRequired,
            AdministrativeOutcome::PreconditionFailed,
            AdministrativeOutcome::Terminal,
            AdministrativeOutcome::Invalid,
            AdministrativeOutcome::UniqueConflict,
        ];
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        for (tag, outcome) in outcomes.into_iter().enumerate() {
            let mut histogram = AdministrativeHistogram::default();
            histogram.record(&outcome).unwrap();
            let result = AdministrativeResult {
                histogram,
                items: vec![AdministrativeItemResult {
                    key: "r".into(),
                    outcome: outcome.clone(),
                    before: None,
                }],
                clauses: vec![],
            };
            let encoded = encode_administrative_result(&result, &job).unwrap();
            assert_eq!(encoded.as_bytes()[21], tag as u8);
            assert_eq!(encoded.as_bytes()[47], tag as u8);
            assert_eq!(
                view_administrative_result(encoded.as_bytes())
                    .unwrap()
                    .items
                    .iter()
                    .next()
                    .unwrap()
                    .unwrap()
                    .outcome,
                outcome
            );
        }
    }
    #[test]
    fn inconsistent_histograms_versions_presence_and_nonfinite_fields_fail_closed() {
        let result = rich_result();
        for case in 0..9 {
            let mut bad = result.clone();
            match case {
                0 => bad.histogram.matched += 1,
                1 => {
                    bad.histogram
                        .outcomes
                        .insert(AdministrativeOutcomeKind::NoChange, 0);
                }
                2 => bad.items[0].outcome = AdministrativeOutcome::Changed { version: 3 },
                3 => bad.items[3].outcome = AdministrativeOutcome::Conflict { actual_version: 2 },
                4 => bad.items[0].before.as_mut().unwrap().lease_expires_at = None,
                5 => bad.items[0].before.as_mut().unwrap().version = 0,
                6 => bad.items[0]
                    .before
                    .as_mut()
                    .unwrap()
                    .fields
                    .insert("bad".into(), FieldValue::Float(f64::NAN))
                    .map(|_| ())
                    .unwrap_or(()),
                7 => bad.items[2].before = bad.items[1].before.clone(),
                8 => bad.clauses.push(AdministrativeClauseResult {
                    id: "mixed".into(),
                    histogram: Default::default(),
                }),
                _ => unreachable!(),
            }
            assert!(measure_administrative_result(&bad).is_err());
        }
        let mut overflow = AdministrativeHistogram {
            matched: u64::MAX,
            outcomes: BTreeMap::from([
                (AdministrativeOutcomeKind::NoChange, u64::MAX),
                (AdministrativeOutcomeKind::Invalid, 1),
            ]),
        };
        assert!(histogram_counts(&overflow).is_err());
        overflow
            .outcomes
            .remove(&AdministrativeOutcomeKind::Invalid);
        assert!(histogram_counts(&overflow).is_ok());
    }
    #[test]
    fn every_truncation_trailing_bad_tag_count_and_nan_is_rejected() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let encoded = encode_administrative_result(&rich_result(), &job).unwrap();
        let raw = encoded.as_bytes();
        for n in 0..raw.len() {
            assert!(
                view_administrative_result(&raw[..n]).is_err(),
                "accepted truncation {n}"
            );
        }
        let mut trailing = raw.to_vec();
        trailing.push(0);
        assert!(view_administrative_result(&trailing).is_err());
        for offset in [0, 21] {
            let mut bad = raw.to_vec();
            bad[offset] = 255;
            assert!(view_administrative_result(&bad).is_err());
        }
        let mut huge = raw.to_vec();
        huge[13..21].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(view_administrative_result(&huge).is_err());
        let pattern = [
            b"float".as_slice(),
            &[1],
            &(-0.0f64).to_bits().to_le_bytes(),
        ]
        .concat();
        let offset = raw
            .windows(pattern.len())
            .position(|window| window == pattern)
            .unwrap();
        let mut nan = raw.to_vec();
        nan[offset + 6..offset + 14].copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
        assert!(view_administrative_result(&nan).is_err());
    }
    #[test]
    fn multi_mebibyte_before_payload_is_borrowed_and_encoding_capacity_failure_leaks_nothing() {
        let mut result = rich_result();
        result.items.truncate(1);
        result.histogram = Default::default();
        result.histogram.record(&result.items[0].outcome).unwrap();
        result.items[0]
            .before
            .as_mut()
            .unwrap()
            .fields
            .insert("large".into(), FieldValue::Bytes(vec![7; 4 << 20]));
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let encoded = encode_administrative_result(&result, &job).unwrap();
        let saturated = QueueAdmission::new(1, 128).unwrap();
        let _held = saturated.reserve(128).unwrap();
        let view = view_administrative_result(encoded.as_bytes()).unwrap();
        let item = view.items.iter().next().unwrap().unwrap();
        let value = item
            .before
            .unwrap()
            .fields
            .iter()
            .find_map(|entry| match entry.unwrap() {
                ("large", Some(ValueRef::Bytes(value))) => Some(value),
                _ => None,
            })
            .unwrap();
        assert_eq!(value.len(), 4 << 20);
        assert_eq!(value[0], 7);
        let raw = encoded.as_bytes();
        assert!(
            value.as_ptr() >= raw.as_ptr() && value.as_ptr() < raw.as_ptr().wrapping_add(raw.len())
        );
        assert_eq!(saturated.usage().unwrap(), (1, 128));
        let limited = QueueAdmission::new(1, 1 << 20).unwrap();
        let limited_job = limited.reserve(0).unwrap();
        assert!(matches!(
            encode_administrative_result(&result, &limited_job),
            Err(PublicationError::Admission(QueueError::OutcomeUnknown(_)))
        ));
        assert_eq!(limited.usage().unwrap(), (1, 0));
    }
}
