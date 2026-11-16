//! Order-preserving v1 index keys, separate from request fingerprint encoding.
//! Full values remain authoritative. Storage may use bounded prefixes/hash probes
//! to find candidates, but must compare complete keys and never truncate semantics.
use fireweed_core::queue_contract::{FieldType, FieldValue, QueueError, QueueResult};

pub(crate) fn escaped(out: &mut Vec<u8>, bytes: &[u8]) {
    escaped_prefix(out, bytes);
    out.extend_from_slice(&[0, 0]);
}
pub(crate) fn escaped_prefix(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            out.extend_from_slice(&[0, 255]);
        } else {
            out.push(*byte);
        }
    }
}
/// Inclusive lower prefix for exactly one present scalar type. The next tag is
/// its exclusive upper prefix, allowing mixed typed property-bag range probes.
pub fn scalar_type_prefix(value: FieldType) -> [u8; 3] {
    [
        1,
        1,
        match value {
            FieldType::Integer => 0,
            FieldType::Float => 1,
            FieldType::Boolean => 2,
            FieldType::Text => 3,
            FieldType::Bytes => 4,
        },
    ]
}
/// Missing values precede present values. A queue declares each column's type.
/// Length prefixes are intentionally absent, preserving lexical tuple order.
pub fn ordered_key(values: &[Option<&FieldValue>]) -> QueueResult<Vec<u8>> {
    encode_values(values.iter().map(|value| value.map(borrowed_value)))
}

pub(crate) fn borrowed_value(value: &FieldValue) -> crate::queue_publication::ValueRef<'_> {
    use crate::queue_publication::ValueRef;
    match value {
        FieldValue::Integer(n) => ValueRef::Integer(*n),
        FieldValue::Float(n) => ValueRef::Float(*n),
        FieldValue::Boolean(n) => ValueRef::Boolean(*n),
        FieldValue::Text(s) => ValueRef::Text(s),
        FieldValue::Bytes(b) => ValueRef::Bytes(b),
    }
}

/// Encode checked publication field values without materializing owned fields.
/// Identical persisted ordering to ordered_key; callers admit the output first.
pub fn ordered_key_ref(
    values: &[Option<crate::queue_publication::ValueRef<'_>>],
) -> QueueResult<Vec<u8>> {
    encode_values(values.iter().copied())
}

/// Exact allocation-free size of the existing v1 ordered codec. Validation
/// and emission use the same writer, so admission never relies on another codec.
pub fn ordered_key_ref_len(
    values: &[Option<crate::queue_publication::ValueRef<'_>>],
) -> QueueResult<usize> {
    write_values(values.iter().copied(), None)
}

struct IndexSink<'a> {
    bytes: Option<&'a mut Vec<u8>>,
    len: usize,
}
impl IndexSink<'_> {
    fn byte(&mut self, byte: u8) -> QueueResult<()> {
        self.raw(&[byte])
    }
    fn raw(&mut self, bytes: &[u8]) -> QueueResult<()> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(QueueError::TooLarge)?;
        if let Some(out) = &mut self.bytes {
            out.extend_from_slice(bytes);
        }
        Ok(())
    }
    fn literal(&mut self, bytes: &[u8]) -> QueueResult<()> {
        let length = bytes
            .len()
            .checked_add(bytes.iter().filter(|byte| **byte == 0).count())
            .and_then(|n| n.checked_add(2))
            .ok_or(QueueError::TooLarge)?;
        self.len = self.len.checked_add(length).ok_or(QueueError::TooLarge)?;
        if let Some(out) = &mut self.bytes {
            escaped(out, bytes);
        }
        Ok(())
    }
}
pub(crate) fn encode_values<'a>(
    values: impl Iterator<Item = Option<crate::queue_publication::ValueRef<'a>>> + Clone,
) -> QueueResult<Vec<u8>> {
    let length = write_values(values.clone(), None)?;
    let mut out = Vec::with_capacity(length);
    write_values(values, Some(&mut out))?;
    debug_assert_eq!(out.len(), length);
    Ok(out)
}
fn write_values<'a>(
    values: impl Iterator<Item = Option<crate::queue_publication::ValueRef<'a>>>,
    bytes: Option<&mut Vec<u8>>,
) -> QueueResult<usize> {
    use crate::queue_publication::ValueRef;
    let mut out = IndexSink { bytes, len: 0 };
    out.byte(1)?; // codec version, identical for all compared keys
    for value in values {
        let Some(value) = value else {
            out.byte(0)?;
            continue;
        };
        out.byte(1)?;
        match value {
            ValueRef::Integer(n) => {
                out.byte(0)?;
                out.raw(&((n as u64) ^ (1 << 63)).to_be_bytes())?;
            }
            ValueRef::Float(n) => {
                if !n.is_finite() {
                    return Err(QueueError::Invalid("noncanonical index float".into()));
                }
                out.byte(1)?;
                let bits = if n == 0.0 {
                    0.0f64.to_bits()
                } else {
                    n.to_bits()
                };
                let ordered = if bits >> 63 == 1 {
                    !bits
                } else {
                    bits ^ (1 << 63)
                };
                out.raw(&ordered.to_be_bytes())?;
            }
            ValueRef::Boolean(n) => {
                out.byte(2)?;
                out.byte(u8::from(n))?;
            }
            ValueRef::Text(s) => {
                out.byte(3)?;
                out.literal(s.as_bytes())?;
            }
            ValueRef::Bytes(b) => {
                out.byte(4)?;
                out.literal(b)?;
            }
        }
    }
    Ok(out.len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::compare_optional_fields;
    #[test]
    fn measured_codec_preserves_frozen_v1_bytes_and_rejects_nonfinite_values() {
        use crate::queue_publication::ValueRef;
        for (values, expected) in [
            (vec![None], vec![1, 0]),
            (
                vec![Some(ValueRef::Integer(0))],
                vec![1, 1, 0, 128, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                vec![Some(ValueRef::Float(-0.0))],
                vec![1, 1, 1, 128, 0, 0, 0, 0, 0, 0, 0],
            ),
            (vec![Some(ValueRef::Boolean(true))], vec![1, 1, 2, 1]),
            (
                vec![Some(ValueRef::Text("\0β"))],
                vec![1, 1, 3, 0, 255, 206, 178, 0, 0],
            ),
            (vec![Some(ValueRef::Bytes(&[]))], vec![1, 1, 4, 0, 0]),
            (
                vec![
                    None,
                    Some(ValueRef::Boolean(false)),
                    Some(ValueRef::Bytes(&[0, 255])),
                ],
                vec![1, 0, 1, 2, 0, 1, 4, 0, 255, 255, 0, 0],
            ),
        ] {
            assert_eq!(ordered_key_ref_len(&values).unwrap(), expected.len());
            assert_eq!(ordered_key_ref(&values).unwrap(), expected);
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                ordered_key_ref_len(&[Some(ValueRef::Float(value))]),
                Err(QueueError::Invalid(_))
            ));
            assert!(matches!(
                ordered_key_ref(&[Some(ValueRef::Float(value))]),
                Err(QueueError::Invalid(_))
            ));
        }
    }
    #[test]
    fn scalar_keys_match_shared_order_for_all_edge_shapes() {
        let domains = vec![
            vec![
                FieldValue::Integer(i64::MIN),
                FieldValue::Integer(-1),
                FieldValue::Integer(0),
                FieldValue::Integer(i64::MAX),
            ],
            vec![
                FieldValue::Float(-100.0),
                FieldValue::Float(-0.1),
                FieldValue::Float(-0.0),
                FieldValue::Float(0.0),
                FieldValue::Float(0.1),
                FieldValue::Float(100.0),
            ],
            vec![FieldValue::Boolean(false), FieldValue::Boolean(true)],
            vec![
                FieldValue::Text("".into()),
                FieldValue::Text("\0".into()),
                FieldValue::Text("a".into()),
                FieldValue::Text("a\0".into()),
                FieldValue::Text("aa".into()),
                FieldValue::Text("z".into()),
                FieldValue::Text("é".into()),
                FieldValue::Text("🦊".into()),
            ],
            vec![
                FieldValue::Bytes(vec![]),
                FieldValue::Bytes(vec![0]),
                FieldValue::Bytes(vec![0, 0]),
                FieldValue::Bytes(vec![0, 255]),
                FieldValue::Bytes(vec![255]),
            ],
        ];
        for domain in domains {
            let values: Vec<_> = std::iter::once(None)
                .chain(domain.iter().map(Some))
                .collect();
            for a in &values {
                let borrowed = [a.map(borrowed_value)];
                assert_eq!(
                    ordered_key_ref_len(&borrowed).unwrap(),
                    ordered_key_ref(&borrowed).unwrap().len()
                );
                for b in &values {
                    assert_eq!(
                        ordered_key_ref(&[a.map(borrowed_value)])
                            .unwrap()
                            .cmp(&ordered_key_ref(&[b.map(borrowed_value)]).unwrap()),
                        compare_optional_fields(*a, *b).unwrap()
                    );
                    assert_eq!(
                        ordered_key(&[*a])
                            .unwrap()
                            .cmp(&ordered_key(&[*b]).unwrap()),
                        compare_optional_fields(*a, *b).unwrap()
                    );
                }
            }
        }
    }
    #[test]
    fn variable_length_composites_are_not_length_prefixed() {
        let a = FieldValue::Text("z".into());
        let b = FieldValue::Text("aa".into());
        let second = FieldValue::Integer(1);
        assert!(
            ordered_key(&[Some(&a), Some(&second)]).unwrap()
                > ordered_key(&[Some(&b), Some(&second)]).unwrap()
        );
        let a = FieldValue::Text("a".into());
        let b = FieldValue::Text("a\0".into());
        assert!(
            ordered_key(&[Some(&a), Some(&second)]).unwrap()
                < ordered_key(&[Some(&b), None]).unwrap()
        );
    }
    #[test]
    fn oversized_strings_and_keys_are_preserved_exactly() {
        let a = FieldValue::Text("x".repeat(1025));
        let b = FieldValue::Text(format!("{}y", "x".repeat(1025)));
        let encoded = ordered_key(&[Some(&a)]).unwrap();
        assert!(encoded.len() > 1024);
        assert!(encoded < ordered_key(&[Some(&b)]).unwrap());
    }
    #[test]
    fn signed_zero_has_one_ordered_unique_key() {
        assert_eq!(
            ordered_key(&[Some(&FieldValue::Float(-0.0))]).unwrap(),
            ordered_key(&[Some(&FieldValue::Float(0.0))]).unwrap()
        );
    }
}
