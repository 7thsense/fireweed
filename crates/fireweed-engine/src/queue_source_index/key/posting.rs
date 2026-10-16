//! Bounds for persisted scalar/named postings. Sort bytes already use the v1
//! ordered-key codec and receive the same outer escaping as the producer.
use super::{
    QueueResult, Sink, VERSION,
    literal::Literal,
    membership::{QueueSourceDecodedIdentity, collection_suffix, marker, record_key},
};
use crate::queue_source_index::{QueueSourceCut, QueueSourcePoint};
use fireweed_core::queue_contract::{FieldType, QueueError};

#[derive(Clone, Copy)]
pub enum QueueSourcePostingFamily<'a> {
    Scalar {
        collection: Option<&'a str>,
        field: &'a str,
    },
    Named {
        collection: Option<&'a str>,
        index: &'a str,
    },
}
impl<'a> QueueSourcePostingFamily<'a> {
    /// Validate one original-cut membership and its complete typed key. This is
    /// neither a range-completeness seal nor correspondence to a live record.
    /// `types` must come from the admitted declaration/typed predicate range.
    pub fn decode<'b>(
        self,
        original: &QueueSourceCut<'_>,
        point: QueueSourcePoint<'b>,
        types: &[FieldType],
    ) -> QueueResult<QueueSourceDecodedPosting<'a, 'b>> {
        marker(original, &point)?;
        let (tag, collection, name, missing) = match self {
            Self::Scalar { collection, field } => {
                if types.len() != 1 {
                    return Err(unknown("scalar arity differs"));
                }
                (4, collection, field, false)
            }
            Self::Named { collection, index } => {
                if types.is_empty() || types.len() > 8 {
                    return Err(unknown("named arity differs"));
                }
                (5, collection, index, true)
            }
        };
        if name.trim().is_empty() {
            return Err(unknown("blank posting name"));
        }
        let input = point
            .key()
            .strip_prefix(&[VERSION, tag])
            .ok_or_else(|| unknown("posting family differs"))?;
        let input = collection_suffix(input, collection)?;
        let (actual_name, input) = Literal::parse(input)?;
        if !actual_name.matches(name.as_bytes()) {
            return Err(unknown("posting name differs"));
        }
        let (sort, input) = Literal::parse(input)?;
        super::ordered::validate(sort.bytes(), types, missing)?;
        let key = record_key(input)?;
        Ok(QueueSourceDecodedPosting {
            identity: QueueSourceDecodedIdentity::new(collection, key, point),
            sort,
        })
    }

    fn write(self, sink: &mut Sink<'_>, sort: Option<&[u8]>) -> QueueResult<()> {
        let (tag, collection, name) = match self {
            Self::Scalar { collection, field } => (4, collection, field),
            Self::Named { collection, index } => (5, collection, index),
        };
        sink.raw(&[VERSION, tag])?;
        sink.collection(collection)?;
        sink.literal(name.as_bytes())?;
        if let Some(sort) = sort {
            sink.literal(sort)?;
        }
        Ok(())
    }
    fn length(self, sort: Option<&[u8]>) -> QueueResult<usize> {
        let mut sink = Sink { out: None, len: 0 };
        self.write(&mut sink, sort)?;
        Ok(sink.len)
    }
    fn encode(self, sort: Option<&[u8]>) -> QueueResult<Vec<u8>> {
        let mut out = Vec::with_capacity(self.length(sort)?);
        self.write(
            &mut Sink {
                out: Some(&mut out),
                len: 0,
            },
            sort,
        )?;
        Ok(out)
    }
    /// Admit the measured prefix and upper-bound copy before allocating either.
    pub fn prefix_len(self) -> QueueResult<usize> {
        self.length(None)
    }
    pub fn prefix(self) -> QueueResult<Vec<u8>> {
        self.encode(None)
    }
    /// Prefix of the entire equal-value tie group, without a literal record key.
    /// Its successor excludes every key at this value. Do not append a synthetic
    /// record key or use unescaped `ordered_key` bytes as persisted bounds.
    /// The caller validates the ordered-key bytes. This writer alone establishes
    /// neither their type/schema validity nor source membership or authority.
    pub fn value_prefix_len(self, sort: &[u8]) -> QueueResult<usize> {
        self.length(Some(sort))
    }
    pub fn value_prefix(self, sort: &[u8]) -> QueueResult<Vec<u8>> {
        self.encode(Some(sort))
    }
}

/// Borrowed candidate syntax and exact-cut membership only. Copying bytes into
/// scratch requires admission first; selection must still prove range coverage
/// and same-cut live metadata/field correspondence before returning any row.
pub struct QueueSourceDecodedPosting<'a, 'b> {
    identity: QueueSourceDecodedIdentity<'a, 'b>,
    sort: Literal<'b>,
}
impl QueueSourceDecodedPosting<'_, '_> {
    pub fn identity(&self) -> &QueueSourceDecodedIdentity<'_, '_> {
        &self.identity
    }
    pub fn sort_len(&self) -> usize {
        self.sort.len()
    }
    pub fn sort_bytes(&self) -> impl Iterator<Item = u8> + Clone + '_ {
        self.sort.bytes()
    }
}

fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source posting: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_index::ordered_key,
        queue_source_index::{QueueSourceKey, prefix_successor},
    };
    use fireweed_core::queue_contract::FieldValue;

    #[test]
    fn posting_boundaries_match_full_literal_value_groups() {
        let values = [
            None,
            Some(FieldValue::Integer(i64::MIN)),
            Some(FieldValue::Integer(0)),
            Some(FieldValue::Integer(i64::MAX)),
            Some(FieldValue::Float(-0.1)),
            Some(FieldValue::Float(0.0)),
            Some(FieldValue::Boolean(false)),
            Some(FieldValue::Boolean(true)),
            Some(FieldValue::Text(String::new())),
            Some(FieldValue::Text("a\0é".into())),
            Some(FieldValue::Bytes(vec![0, 255])),
        ];
        for collection in [None, Some(""), Some("state\0é")] {
            for family in [
                QueueSourcePostingFamily::Scalar {
                    collection,
                    field: "f\0é",
                },
                QueueSourcePostingFamily::Named {
                    collection,
                    index: "f\0é",
                },
            ] {
                let lower_family = family.prefix().unwrap();
                assert_eq!(lower_family.len(), family.prefix_len().unwrap());
                let mut upper_family = lower_family.clone();
                assert!(prefix_successor(&mut upper_family));
                for value in &values {
                    let sort = ordered_key(&[value.as_ref()]).unwrap();
                    let lower = family.value_prefix(&sort).unwrap();
                    assert_eq!(lower.len(), family.value_prefix_len(&sort).unwrap());
                    let mut upper = lower.clone();
                    assert!(prefix_successor(&mut upper));
                    for other in &values {
                        let other_sort = ordered_key(&[other.as_ref()]).unwrap();
                        for key in ["", "\0", "a", "a\0é", "🦊"] {
                            let encoded = match family {
                                QueueSourcePostingFamily::Scalar { collection, field } => {
                                    QueueSourceKey::ScalarPosting {
                                        collection,
                                        field,
                                        sort: &other_sort,
                                        key,
                                    }
                                }
                                QueueSourcePostingFamily::Named { collection, index } => {
                                    QueueSourceKey::NamedPosting {
                                        collection,
                                        index,
                                        sort: &other_sort,
                                        key,
                                    }
                                }
                            }
                            .encode()
                            .unwrap();
                            assert!(lower_family <= encoded && encoded < upper_family);
                            assert_eq!(lower <= encoded && encoded < upper, sort == other_sort);
                            assert_eq!(encoded < lower, other_sort < sort);
                            assert_eq!(encoded >= upper, other_sort > sort);
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn named_missing_components_and_family_names_remain_disjoint() {
        let family = QueueSourcePostingFamily::Named {
            collection: Some("state"),
            index: "i",
        };
        let tuple = ordered_key(&[None, Some(&FieldValue::Text("x\0".into()))]).unwrap();
        let lower = family.value_prefix(&tuple).unwrap();
        let mut upper = lower.clone();
        assert!(prefix_successor(&mut upper));
        let good = QueueSourceKey::NamedPosting {
            collection: Some("state"),
            index: "i",
            sort: &tuple,
            key: "k",
        }
        .encode()
        .unwrap();
        assert!(lower <= good && good < upper);
        for bad in [
            QueueSourceKey::NamedPosting {
                collection: Some("state\0"),
                index: "i",
                sort: &tuple,
                key: "k",
            },
            QueueSourceKey::NamedPosting {
                collection: Some("state"),
                index: "i\0",
                sort: &tuple,
                key: "k",
            },
            QueueSourceKey::ScalarPosting {
                collection: Some("state"),
                field: "i",
                sort: &tuple,
                key: "k",
            },
        ] {
            let bad = bad.encode().unwrap();
            assert!(!(lower <= bad && bad < upper));
        }
    }
}

#[cfg(test)]
#[path = "posting/decode_tests.rs"]
mod decode_tests;

#[cfg(test)]
mod family_exclusions {
    use super::*;
    use crate::{
        queue_index::ordered_key,
        queue_source_index::{QueueSourceKey, prefix_successor},
    };
    use fireweed_core::queue_contract::FieldValue;
    #[test]
    fn complete_family_bounds_exclude_literal_extensions_and_foreign_domains() {
        let sort = ordered_key(&[Some(&FieldValue::Integer(1))]).unwrap();
        for collection in [None, Some(""), Some("s"), Some("s\0")] {
            for named in [false, true] {
                let family = if named {
                    QueueSourcePostingFamily::Named {
                        collection,
                        index: "i",
                    }
                } else {
                    QueueSourcePostingFamily::Scalar {
                        collection,
                        field: "i",
                    }
                };
                let lower = family.prefix().unwrap();
                let mut upper = lower.clone();
                assert!(prefix_successor(&mut upper));
                for other_collection in [None, Some(""), Some("s"), Some("s\0")] {
                    for other_named in [false, true] {
                        for name in ["i", "i\0", "ij"] {
                            let raw = if other_named {
                                QueueSourceKey::NamedPosting {
                                    collection: other_collection,
                                    index: name,
                                    sort: &sort,
                                    key: "k",
                                }
                            } else {
                                QueueSourceKey::ScalarPosting {
                                    collection: other_collection,
                                    field: name,
                                    sort: &sort,
                                    key: "k",
                                }
                            }
                            .encode()
                            .unwrap();
                            assert_eq!(
                                lower <= raw && raw < upper,
                                collection == other_collection
                                    && named == other_named
                                    && name == "i"
                            );
                        }
                    }
                }
            }
        }
    }
}
