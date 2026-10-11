//! Borrowed projection of frozen results into the public DTO serialization.
//! These serializers count public responses without allocating their owned
//! strings/vectors. They do not encode FWQP or establish publication authority.
use super::*;
use fireweed_core::queue_administration::*;
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeMap, SerializeSeq, SerializeStructVariant},
};

fn parse_error<E: serde::ser::Error>(error: PublicationError) -> E {
    E::custom(format!("{error:?}"))
}
impl Serialize for RejectionRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (tag, name) = match self {
            Self::StaleLease => (0, "StaleLease"),
            Self::VersionConflict => (1, "VersionConflict"),
            Self::AlreadyExists => (2, "AlreadyExists"),
            Self::NotFound => (3, "NotFound"),
            Self::Leased => (4, "Leased"),
            Self::Invalid(message) => {
                return s.serialize_newtype_variant("Rejection", 5, "Invalid", message);
            }
            Self::UniqueConflict => (6, "UniqueConflict"),
            Self::Terminal => (7, "Terminal"),
            Self::TooLarge => (8, "TooLarge"),
        };
        s.serialize_unit_variant("Rejection", tag, name)
    }
}
impl Serialize for ValueRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Integer(n) => s.serialize_newtype_variant("FieldValue", 0, "Integer", n),
            Self::Float(n) => s.serialize_newtype_variant("FieldValue", 1, "Float", n),
            Self::Boolean(n) => s.serialize_newtype_variant("FieldValue", 2, "Boolean", n),
            Self::Text(n) => s.serialize_newtype_variant("FieldValue", 3, "Text", n),
            Self::Bytes(n) => s.serialize_newtype_variant("FieldValue", 4, "Bytes", n),
        }
    }
}
#[derive(Serialize)]
struct PublicVersion<'a> {
    collection: Option<&'a str>,
    key: &'a str,
    version: u64,
}
impl Serialize for VersionsRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq =
            s.serialize_seq(Some(usize::try_from(self.len()).map_err(S::Error::custom)?))?;
        for version in self.iter() {
            let version = version.map_err(parse_error)?;
            seq.serialize_element(&PublicVersion {
                collection: version.identity.collection,
                key: version.identity.key,
                version: version.version,
            })?;
        }
        seq.end()
    }
}
impl Serialize for InsertOutcomeRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Inserted { key, version } | Self::Replaced { key, version } => {
                let (tag, name) = if matches!(self, Self::Inserted { .. }) {
                    (0, "Inserted")
                } else {
                    (1, "Replaced")
                };
                let mut v = s.serialize_struct_variant("InsertOutcome", tag, name, 2)?;
                v.serialize_field("key", key)?;
                v.serialize_field("version", version)?;
                v.end()
            }
            Self::Rejected { key, reason } => {
                let mut v = s.serialize_struct_variant("InsertOutcome", 2, "Rejected", 2)?;
                v.serialize_field("key", key)?;
                v.serialize_field("reason", reason)?;
                v.end()
            }
        }
    }
}
impl Serialize for InsertsRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq =
            s.serialize_seq(Some(usize::try_from(self.len()).map_err(S::Error::custom)?))?;
        for outcome in self.iter() {
            seq.serialize_element(&outcome.map_err(parse_error)?)?;
        }
        seq.end()
    }
}
struct PublicFields<'a>(FieldsRef<'a>);
impl Serialize for PublicFields<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(
            usize::try_from(self.0.len()).map_err(S::Error::custom)?,
        ))?;
        for field in self.0.iter() {
            let (name, value) = field.map_err(parse_error)?;
            let value =
                value.ok_or_else(|| S::Error::custom("result field cannot be a deletion"))?;
            map.serialize_entry(name, &value)?;
        }
        map.end()
    }
}
struct PublicHistogram<'a>(AdministrativeHistogramRef<'a>);
impl Serialize for PublicHistogram<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let count = self.0.iter().count();
        let mut map = s.serialize_map(Some(count))?;
        for entry in self.0.iter() {
            let (kind, count) = entry.map_err(parse_error)?;
            map.serialize_entry(&kind, &count)?;
        }
        map.end()
    }
}
#[derive(Serialize)]
struct Histogram<'a> {
    matched: u64,
    outcomes: PublicHistogram<'a>,
}
fn histogram(value: AdministrativeHistogramRef<'_>) -> Histogram<'_> {
    Histogram {
        matched: value.matched,
        outcomes: PublicHistogram(value),
    }
}
#[derive(Serialize)]
struct Before<'a> {
    version: u64,
    lifecycle: Lifecycle,
    available_at: i64,
    eligible_since: i64,
    attempts: u32,
    lease_expires_at: Option<i64>,
    fields: PublicFields<'a>,
}
fn before(value: AdministrativeBeforeRef<'_>) -> Before<'_> {
    Before {
        version: value.version,
        lifecycle: value.lifecycle,
        available_at: value.available_at,
        eligible_since: value.eligible_since,
        attempts: value.attempts,
        lease_expires_at: value.lease_expires_at,
        fields: PublicFields(value.fields),
    }
}
#[derive(Serialize)]
struct Item<'a> {
    key: &'a str,
    outcome: AdministrativeOutcome,
    before: Option<Before<'a>>,
}
struct Items<'a>(AdministrativeItemsRef<'a>);
impl Serialize for Items<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(
            usize::try_from(self.0.len()).map_err(S::Error::custom)?,
        ))?;
        for item in self.0.iter() {
            let item = item.map_err(parse_error)?;
            seq.serialize_element(&Item {
                key: item.key,
                outcome: item.outcome,
                before: item.before.map(before),
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct Clause<'a> {
    id: &'a str,
    histogram: Histogram<'a>,
}
struct Clauses<'a>(AdministrativeClausesRef<'a>);
impl Serialize for Clauses<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(
            usize::try_from(self.0.len()).map_err(S::Error::custom)?,
        ))?;
        for clause in self.0.iter() {
            let clause = clause.map_err(parse_error)?;
            seq.serialize_element(&Clause {
                id: clause.id,
                histogram: histogram(clause.histogram),
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct Administration<'a> {
    histogram: Histogram<'a>,
    items: Items<'a>,
    clauses: Clauses<'a>,
}
impl Serialize for CommitOutcomeRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Committed(versions) => {
                let mut v = s.serialize_struct_variant("GroupOutcome", 2, "Committed", 1)?;
                v.serialize_field("versions", versions)?;
                v.end()
            }
            Self::Rejected(reason) => {
                s.serialize_newtype_variant("GroupOutcome", 3, "Rejected", reason)
            }
            Self::AdministrativeCommitted {
                versions,
                administration,
            } => {
                let mut v =
                    s.serialize_struct_variant("GroupOutcome", 4, "AdministrativeCommitted", 2)?;
                v.serialize_field("versions", versions)?;
                v.serialize_field(
                    "administration",
                    &Administration {
                        histogram: histogram(administration.histogram),
                        items: Items(administration.items),
                        clauses: Clauses(administration.clauses),
                    },
                )?;
                v.end()
            }
        }
    }
}

/// Public receipt framing borrowed from an independently validated stored result.
#[derive(Serialize)]
pub struct CommitReceiptProjection<'a> {
    pub request_id: &'a str,
    pub outcome: CommitOutcomeRef<'a>,
}
/// The serde layout is exactly ReceiptLookup; this is not a stored FWQP format.
#[derive(Serialize)]
pub enum ReceiptLookupProjection<'a> {
    Found(CommitReceiptProjection<'a>),
    Absent,
    Expired,
}
/// Allocation upper bound for conversion, separate from public serialized size.
/// Includes container headers even when fixed-format values are only a few bytes.
pub fn receipt_outcome_heap_bound(
    outcome: ReceiptOutcomeRef<'_>,
    raw_bytes: u64,
) -> PublicationResult<u64> {
    let mut headers = 0u64;
    let mut add = |count: u64, bytes: u64| -> PublicationResult<()> {
        headers = headers
            .checked_add(count.checked_mul(bytes).ok_or(PublicationError::Overflow)?)
            .ok_or(PublicationError::Overflow)?;
        Ok(())
    };
    match outcome {
        ReceiptOutcomeRef::Insert(outcomes) => add(outcomes.len(), 1024)?,
        ReceiptOutcomeRef::Commit(CommitOutcomeRef::Committed(versions)) => {
            add(versions.len(), 1024)?
        }
        ReceiptOutcomeRef::Commit(CommitOutcomeRef::Rejected(_)) => {}
        ReceiptOutcomeRef::Commit(CommitOutcomeRef::AdministrativeCommitted {
            versions,
            administration,
        }) => {
            add(versions.len(), 1024)?;
            add(administration.items.len(), 2048)?;
            add(administration.clauses.len(), 8192)?;
            for item in administration.items.iter() {
                if let Some(before) = item?.before {
                    add(before.fields.len(), 512)?;
                }
            }
        }
    }
    raw_bytes
        .checked_mul(8)
        .and_then(|n| n.checked_add(headers))
        .and_then(|n| n.checked_add(65536))
        .ok_or(PublicationError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    fn ordinary(outcome: &GroupOutcome) -> Vec<u8> {
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let receipt = PublicationReceipt {
            queue: &queue,
            id: "r",
            fingerprint: [7; 32],
            result: ReceiptResult::Commit(outcome),
        };
        let mut bytes = Vec::new();
        write_receipt(&mut Sink::Buffer(&mut bytes), &receipt).unwrap();
        bytes[43..].to_vec() // Frozen domain + length-framed one-byte ID + FP3/hash.
    }
    fn strict(domain: u8, format: u8, raw: &[u8]) {
        assert!(view_receipt_outcome(domain, format, raw).is_ok());
        for end in 0..raw.len() {
            assert!(
                view_receipt_outcome(domain, format, &raw[..end]).is_err(),
                "accepted truncated result {end}"
            );
        }
        let mut trailing = raw.to_vec();
        trailing.push(0);
        assert!(view_receipt_outcome(domain, format, &trailing).is_err());
    }
    #[test]
    fn borrowed_ordinary_and_insert_results_match_real_dto_bytes_and_frozen_gates() {
        for count in [0, 1, 127, 128, 129, 257] {
            let versions = (0..count)
                .map(|n| RecordVersion {
                    collection: Some("state\0λ".into()),
                    key: format!("item/{n}~β"),
                    version: if n % 2 == 0 { u64::MAX } else { 1 },
                })
                .collect();
            let owned = GroupOutcome::Committed { versions };
            let raw = ordinary(&owned);
            strict(2, 3, &raw);
            let ReceiptOutcomeRef::Commit(view) = view_receipt_outcome(2, 3, &raw).unwrap() else {
                panic!("wrong domain")
            };
            assert_eq!(
                postcard::to_allocvec(&view).unwrap(),
                postcard::to_allocvec(&owned).unwrap()
            );
            assert_eq!(
                encoded_len(&view).unwrap(),
                postcard::to_allocvec(&owned).unwrap().len() as u64
            );
        }
        for reason in [
            Rejection::StaleLease,
            Rejection::VersionConflict,
            Rejection::AlreadyExists,
            Rejection::NotFound,
            Rejection::Leased,
            Rejection::Invalid("reason\0λ".into()),
            Rejection::UniqueConflict,
            Rejection::Terminal,
        ] {
            let owned = GroupOutcome::Rejected(reason);
            let raw = ordinary(&owned);
            strict(2, 3, &raw);
            let ReceiptOutcomeRef::Commit(view) = view_receipt_outcome(2, 3, &raw).unwrap() else {
                panic!("wrong domain")
            };
            assert_eq!(
                postcard::to_allocvec(&view).unwrap(),
                postcard::to_allocvec(&owned).unwrap()
            );
        }
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let outcomes = vec![
            InsertOutcome::Inserted {
                key: "dup\0λ".into(),
                version: u64::MAX,
            },
            InsertOutcome::Replaced {
                key: "dup\0λ".into(),
                version: 2,
            },
            InsertOutcome::Rejected {
                key: "dup\0λ".into(),
                reason: Rejection::AlreadyExists,
            },
        ];
        let receipt = PublicationReceipt {
            queue: &queue,
            id: "r",
            fingerprint: [7; 32],
            result: ReceiptResult::Insert(&outcomes),
        };
        let mut bytes = Vec::new();
        write_receipt(&mut Sink::Buffer(&mut bytes), &receipt).unwrap();
        let raw = &bytes[43..];
        strict(0, 3, raw);
        let ReceiptOutcomeRef::Insert(view) = view_receipt_outcome(0, 3, raw).unwrap() else {
            panic!("wrong domain")
        };
        assert_eq!(
            postcard::to_allocvec(&view).unwrap(),
            postcard::to_allocvec(&outcomes).unwrap()
        );
        for (domain, format) in [(1, 3), (3, 3), (0, 4), (2, 0), (2, 2), (2, 5)] {
            assert!(view_receipt_outcome(domain, format, raw).is_err());
        }
        for tag in [2, 3, 4, 255] {
            assert!(view_receipt_outcome(2, 3, &[tag]).is_err());
        }
        assert!(view_receipt_outcome(2, 3, &[1, 8]).is_err());
        strict(2, 4, &[1, 8]);
        let ReceiptOutcomeRef::Commit(view) = view_receipt_outcome(2, 4, &[1, 8]).unwrap() else {
            panic!("wrong domain")
        };
        assert_eq!(
            postcard::to_allocvec(&view).unwrap(),
            postcard::to_allocvec(&GroupOutcome::Rejected(Rejection::TooLarge)).unwrap()
        );
    }
    #[test]
    fn borrowed_rich_administrative_receipt_matches_dto_and_counts_nested_headers() {
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let mut administration = super::super::administrative_result::tests::rich_result();
        let first = administration
            .items
            .iter_mut()
            .find(|item| item.before.is_some())
            .unwrap();
        first
            .before
            .as_mut()
            .unwrap()
            .fields
            .extend((0..5000).map(|n| (format!("tiny/{n}"), FieldValue::Boolean(n % 2 == 0))));
        let versions = vec![RecordVersion {
            collection: Some("state\0λ".into()),
            key: "k/~β".into(),
            version: u64::MAX,
        }];
        let component = encode_administrative_result(&administration, &job).unwrap();
        let mut raw = Vec::new();
        let mut sink = Sink::Buffer(&mut raw);
        sink.tag(4).unwrap();
        sink.n(versions.len() as u64).unwrap();
        for version in &versions {
            identity(&mut sink, version.collection.as_deref(), &version.key).unwrap();
            sink.n(version.version).unwrap();
        }
        sink.bytes(component.as_bytes()).unwrap();
        assert!(view_receipt_outcome(2, 4, &raw).is_ok());
        for end in [0, 1, raw.len() / 2, raw.len() - 1] {
            assert!(view_receipt_outcome(2, 4, &raw[..end]).is_err());
        }
        let mut trailing = raw.clone();
        trailing.push(0);
        assert!(view_receipt_outcome(2, 4, &trailing).is_err());
        assert!(view_receipt_outcome(2, 3, &raw).is_err());
        let outcome = view_receipt_outcome(2, 4, &raw).unwrap();
        let ReceiptOutcomeRef::Commit(view) = outcome else {
            panic!("wrong domain")
        };
        let projection = CommitReceiptProjection {
            request_id: "rich\0β",
            outcome: view,
        };
        let owned = CommitReceipt {
            request_id: "rich\0β".into(),
            outcome: GroupOutcome::AdministrativeCommitted {
                versions,
                administration,
            },
        };
        assert_eq!(
            postcard::to_allocvec(&projection).unwrap(),
            postcard::to_allocvec(&owned).unwrap()
        );
        assert_eq!(
            encoded_len(&projection).unwrap(),
            postcard::to_allocvec(&owned).unwrap().len() as u64
        );
        assert!(receipt_outcome_heap_bound(outcome, raw.len() as u64).unwrap() > 5000 * 512);
        let lookup = vec![
            ReceiptLookupProjection::Found(projection),
            ReceiptLookupProjection::Absent,
            ReceiptLookupProjection::Expired,
        ];
        let owned_lookup = vec![
            ReceiptLookup::Found(owned),
            ReceiptLookup::Absent,
            ReceiptLookup::Expired,
        ];
        assert_eq!(
            postcard::to_allocvec(&lookup).unwrap(),
            postcard::to_allocvec(&owned_lookup).unwrap()
        );
    }
}
