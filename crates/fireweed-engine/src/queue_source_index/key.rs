//! Versioned, disjoint semantic key families. Full literals remain in every key;
//! no hash, schema ordinal or prefix stands in for identity or scalar equality.
use crate::queue_index;
use fireweed_core::queue_contract::{Lifecycle, QueueError, QueueResult};

pub(super) const VERSION: u8 = 1;
#[derive(Clone, Copy)]
pub enum QueueSourceKey<'a> {
    Genesis,
    Sequence,
    Record {
        collection: Option<&'a str>,
        key: &'a str,
    },
    Field {
        collection: Option<&'a str>,
        key: &'a str,
        field: &'a str,
    },
    ScalarPosting {
        collection: Option<&'a str>,
        field: &'a str,
        sort: &'a [u8],
        key: &'a str,
    },
    NamedPosting {
        collection: Option<&'a str>,
        index: &'a str,
        sort: &'a [u8],
        key: &'a str,
    },
    UniqueOwner {
        collection: Option<&'a str>,
        index: &'a str,
        sort: &'a [u8],
    },
    Original {
        domain: u8,
        request_id: &'a str,
    },
    LifecycleCount(Lifecycle),
    LiveKey {
        collection: Option<&'a str>,
        key: &'a str,
    },
    WorkIntrinsic {
        posting: super::QueueSourceWorkPosting,
        key: &'a str,
    },
}
struct Sink<'a> {
    out: Option<&'a mut Vec<u8>>,
    len: usize,
}
impl Sink<'_> {
    fn raw(&mut self, bytes: &[u8]) -> QueueResult<()> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(QueueError::TooLarge)?;
        if let Some(out) = &mut self.out {
            out.extend_from_slice(bytes);
        }
        Ok(())
    }
    fn literal(&mut self, bytes: &[u8]) -> QueueResult<()> {
        let len = bytes
            .len()
            .checked_add(bytes.iter().filter(|byte| **byte == 0).count())
            .and_then(|n| n.checked_add(2))
            .ok_or(QueueError::TooLarge)?;
        self.len = self.len.checked_add(len).ok_or(QueueError::TooLarge)?;
        if let Some(out) = &mut self.out {
            queue_index::escaped(out, bytes);
        }
        Ok(())
    }
    fn collection(&mut self, collection: Option<&str>) -> QueueResult<()> {
        match collection {
            None => self.raw(&[0]),
            Some(name) => {
                self.raw(&[1])?;
                self.literal(name.as_bytes())
            }
        }
    }
}
impl QueueSourceKey<'_> {
    fn write(self, sink: &mut Sink<'_>) -> QueueResult<()> {
        let family = match self {
            Self::Genesis => 0,
            Self::Sequence => 1,
            Self::Record { .. } => 2,
            Self::Field { .. } => 3,
            Self::ScalarPosting { .. } => 4,
            Self::NamedPosting { .. } => 5,
            Self::UniqueOwner { .. } => 6,
            Self::Original { .. } => 7,
            Self::LifecycleCount(_) => 8,
            // Family 9 is reserved; its former opaque kinds were never sealed.
            Self::LiveKey { .. } => 10,
            Self::WorkIntrinsic { .. } => 11,
        };
        sink.raw(&[VERSION, family])?;
        match self {
            Self::Genesis | Self::Sequence => Ok(()),
            Self::Record { collection, key } => {
                sink.collection(collection)?;
                sink.literal(key.as_bytes())
            }
            Self::Field {
                collection,
                key,
                field,
            } => {
                sink.collection(collection)?;
                sink.literal(key.as_bytes())?;
                sink.literal(field.as_bytes())
            }
            Self::ScalarPosting {
                collection,
                field,
                sort,
                key,
            } => {
                sink.collection(collection)?;
                sink.literal(field.as_bytes())?;
                sink.literal(sort)?;
                sink.literal(key.as_bytes())
            }
            Self::NamedPosting {
                collection,
                index,
                sort,
                key,
            } => {
                sink.collection(collection)?;
                sink.literal(index.as_bytes())?;
                sink.literal(sort)?;
                sink.literal(key.as_bytes())
            }
            Self::UniqueOwner {
                collection,
                index,
                sort,
            } => {
                sink.collection(collection)?;
                sink.literal(index.as_bytes())?;
                sink.literal(sort)
            }
            Self::Original { domain, request_id } => {
                if domain > 2 {
                    return Err(QueueError::OutcomeUnknown(
                        "invalid source receipt domain".into(),
                    ));
                }
                sink.raw(&[domain])?;
                sink.literal(request_id.as_bytes())
            }
            Self::LiveKey { collection, key } => {
                sink.collection(collection)?;
                sink.literal(key.as_bytes())
            }
            Self::WorkIntrinsic { posting, key } => {
                sink.raw(&[posting.index().tag()])?;
                sink.raw(posting.sort())?;
                sink.literal(key.as_bytes())
            }
            Self::LifecycleCount(lifecycle) => sink.raw(&[match lifecycle {
                Lifecycle::Pending => 0,
                Lifecycle::Leased => 1,
                Lifecycle::Completed => 2,
                Lifecycle::Failed => 3,
            }]),
        }
    }
    /// Exact output length without allocations. The compiler must charge this
    /// PLUS edit/container overhead before calling encode, including deletions.
    pub fn encoded_len(self) -> QueueResult<usize> {
        let mut sink = Sink { out: None, len: 0 };
        self.write(&mut sink)?;
        Ok(sink.len)
    }
    pub fn encode(self) -> QueueResult<Vec<u8>> {
        let len = self.encoded_len()?;
        let mut out = Vec::with_capacity(len);
        self.write(&mut Sink {
            out: Some(&mut out),
            len: 0,
        })?;
        Ok(out)
    }
}
/// Complete per-record field membership is an interval, not an unbounded list
/// embedded in each work metadata update. Prefix output also needs admission.
pub fn record_fields_prefix_len(collection: Option<&str>, key: &str) -> QueueResult<usize> {
    let mut sink = Sink { out: None, len: 0 };
    sink.raw(&[VERSION, 3])?;
    sink.collection(collection)?;
    sink.literal(key.as_bytes())?;
    Ok(sink.len)
}
pub fn record_fields_prefix(collection: Option<&str>, key: &str) -> QueueResult<Vec<u8>> {
    let mut out = Vec::with_capacity(record_fields_prefix_len(collection, key)?);
    let mut sink = Sink {
        out: Some(&mut out),
        len: 0,
    };
    sink.raw(&[VERSION, 3])?;
    sink.collection(collection)?;
    sink.literal(key.as_bytes())?;
    Ok(out)
}
/// Lower bound for fields whose literal names start with `field_prefix`.
/// Unlike a complete field key this omits the field terminator. Together with
/// `prefix_successor` it selects exactly that prefix within one record.
/// Call the length function and admit both bounds before allocating them.
pub fn record_field_name_prefix_len(
    collection: Option<&str>,
    key: &str,
    field_prefix: &str,
) -> QueueResult<usize> {
    record_fields_prefix_len(collection, key)?
        .checked_add(field_prefix.len())
        .and_then(|n| n.checked_add(field_prefix.bytes().filter(|b| *b == 0).count()))
        .ok_or(QueueError::TooLarge)
}
pub fn record_field_name_prefix(
    collection: Option<&str>,
    key: &str,
    field_prefix: &str,
) -> QueueResult<Vec<u8>> {
    let mut out = Vec::with_capacity(record_field_name_prefix_len(collection, key, field_prefix)?);
    let mut sink = Sink {
        out: Some(&mut out),
        len: 0,
    };
    sink.raw(&[VERSION, 3])?;
    sink.collection(collection)?;
    sink.literal(key.as_bytes())?;
    for byte in field_prefix.bytes() {
        if byte == 0 {
            sink.raw(&[0, 255])?;
        } else {
            sink.raw(&[byte])?;
        }
    }
    Ok(out)
}
/// Complete live-key namespace, including State collection identity.
pub fn live_keys_prefix_len(collection: Option<&str>) -> QueueResult<usize> {
    let mut sink = Sink { out: None, len: 0 };
    sink.raw(&[VERSION, 10])?;
    sink.collection(collection)?;
    Ok(sink.len)
}
pub fn live_keys_prefix(collection: Option<&str>) -> QueueResult<Vec<u8>> {
    let mut out = Vec::with_capacity(live_keys_prefix_len(collection)?);
    let mut sink = Sink {
        out: Some(&mut out),
        len: 0,
    };
    sink.raw(&[VERSION, 10])?;
    sink.collection(collection)?;
    Ok(out)
}
/// Exclusive upper bound for a byte prefix. No successor means unbounded.
/// Operates in place so the caller can reuse admitted bound storage.
pub fn prefix_successor(prefix: &mut Vec<u8>) -> bool {
    match prefix.iter().rposition(|byte| *byte != 255) {
        Some(index) => {
            prefix[index] += 1;
            prefix.truncate(index + 1);
            true
        }
        None => {
            prefix.clear();
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_publication::ValueRef;
    #[test]
    fn literal_families_and_domains_are_unambiguous_and_lengths_exact() {
        let sort = queue_index::ordered_key_ref(&[Some(ValueRef::Text("é\0"))]).unwrap();
        let keys = [
            QueueSourceKey::Genesis,
            QueueSourceKey::Sequence,
            QueueSourceKey::Record {
                collection: None,
                key: "a\0b",
            },
            QueueSourceKey::Record {
                collection: Some(""),
                key: "a\0b",
            },
            QueueSourceKey::Record {
                collection: Some("a"),
                key: "b",
            },
            QueueSourceKey::Record {
                collection: Some("a\0"),
                key: "b",
            },
            QueueSourceKey::Field {
                collection: None,
                key: "a",
                field: "x",
            },
            QueueSourceKey::ScalarPosting {
                collection: None,
                field: "x",
                sort: &sort,
                key: "a",
            },
            QueueSourceKey::NamedPosting {
                collection: None,
                index: "x",
                sort: &sort,
                key: "a",
            },
            QueueSourceKey::UniqueOwner {
                collection: None,
                index: "x",
                sort: &sort,
            },
            QueueSourceKey::Original {
                domain: 0,
                request_id: "same\0id",
            },
            QueueSourceKey::Original {
                domain: 1,
                request_id: "same\0id",
            },
            QueueSourceKey::Original {
                domain: 2,
                request_id: "same\0id",
            },
            QueueSourceKey::LifecycleCount(Lifecycle::Pending),
        ];
        let mut seen = std::collections::BTreeSet::new();
        for key in keys {
            let encoded = key.encode().unwrap();
            assert_eq!(encoded.len(), key.encoded_len().unwrap());
            assert!(seen.insert(encoded));
        }
        assert!(
            QueueSourceKey::Original {
                domain: 3,
                request_id: "id"
            }
            .encode()
            .is_err()
        );
        let long = "é\0".repeat(4096);
        let key = QueueSourceKey::Record {
            collection: Some(&long),
            key: &long,
        };
        assert_eq!(key.encode().unwrap().len(), key.encoded_len().unwrap());
    }
    #[test]
    fn field_name_prefix_ranges_match_literal_oracle() {
        let names = ["", "a", "ab", "a\0", "a\0b", "a\0\0", "b", "é", "éx", "🦊"];
        for collection in [None, Some(""), Some("state\0é")] {
            for record in ["", "item", "item\0"] {
                for prefix in names {
                    let lower = record_field_name_prefix(collection, record, prefix).unwrap();
                    assert_eq!(
                        lower.len(),
                        record_field_name_prefix_len(collection, record, prefix).unwrap()
                    );
                    let mut upper = lower.clone();
                    assert!(prefix_successor(&mut upper));
                    for other_collection in [None, Some(""), Some("state\0é")] {
                        for other_record in ["", "item", "item\0", "itemx"] {
                            for name in names {
                                let encoded = QueueSourceKey::Field {
                                    collection: other_collection,
                                    key: other_record,
                                    field: name,
                                }
                                .encode()
                                .unwrap();
                                assert_eq!(
                                    encoded >= lower && encoded < upper,
                                    collection == other_collection
                                        && record == other_record
                                        && name.starts_with(prefix),
                                    "{collection:?}/{record:?}/{prefix:?} vs {other_collection:?}/{other_record:?}/{name:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn full_prefix_intervals_and_scalar_tuple_order_preserve_shared_semantics() {
        let prefix = record_fields_prefix(Some("state\0é"), "item\0").unwrap();
        assert_eq!(
            prefix.len(),
            record_fields_prefix_len(Some("state\0é"), "item\0").unwrap()
        );
        let mut upper = prefix.clone();
        assert!(prefix_successor(&mut upper));
        for field in ["", "\0", "a", "é", "🦊"] {
            let key = QueueSourceKey::Field {
                collection: Some("state\0é"),
                key: "item\0",
                field,
            }
            .encode()
            .unwrap();
            assert!(key.starts_with(&prefix) && key < upper);
        }
        let other = QueueSourceKey::Field {
            collection: Some("state\0é"),
            key: "item",
            field: "x",
        }
        .encode()
        .unwrap();
        assert!(!other.starts_with(&prefix));
        let values = [
            ValueRef::Integer(i64::MIN),
            ValueRef::Integer(-1),
            ValueRef::Integer(0),
            ValueRef::Integer(i64::MAX),
        ];
        let mut previous = None;
        for value in values {
            let sort = queue_index::ordered_key_ref(&[Some(value)]).unwrap();
            let key = QueueSourceKey::NamedPosting {
                collection: None,
                index: "index",
                sort: &sort,
                key: "record",
            }
            .encode()
            .unwrap();
            if let Some(old) = previous {
                assert!(old < key);
            }
            previous = Some(key);
        }
        let mut all_ff = vec![255, 255];
        assert!(!prefix_successor(&mut all_ff));
        let mut bound = vec![1, 255, 255];
        assert!(prefix_successor(&mut bound));
        assert_eq!(bound, [2]);
    }
}
